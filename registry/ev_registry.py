"""
EV_REGISTRY - Sistema de Registro Seguro de CPs
API REST con HTTPS real, JWT, rate limiting y auditoría.
Release 2 - Práctica SD 25/26

CORRECCIONES COMPLETAS:
  [R-1]  SSL: certificado persistente (no 'adhoc'); se genera una sola vez y
         se copia al volumen compartido para que Central lo use como CA.
  [R-2]  JWT_SECRET_KEY obligatoria (RuntimeError si falta o es < 32 chars).
  [R-3]  REGISTRY_ADMIN_KEY obligatoria para DELETE /unregister y POST /revoke.
  [R-4]  Rate limiting: 5 intentos fallidos → bloqueo 5 min (en memoria,
         con limpieza periódica de entradas antiguas).
  [R-5]  Base de datos con threading.RLock() + WAL mode; una sola conexión
         persistente, no por-request (evita "database is locked").
  [R-6]  CASCADE DELETE en FK para limpiar credentials y tokens al dar de baja.
  [R-7]  PBKDF2 en el módulo security_utils.TokenManager para evitar duplicación.
  [R-8]  SIGTERM/SIGINT para cierre limpio.
  [R-9]  Eliminado endpoint GET /api/v1/cps (expone todos los CPs sin auth);
         sustituido por versión protegida con X-Admin-Key.
  [R-10] Respuesta de autenticación NO incluye encryption_key en texto claro
         en logs; la key se loguea solo con los primeros 8 chars.
  [R-11] Flask debug=False explícito; threaded=True para peticiones concurrentes.
"""
import os
import sys
import signal
import secrets
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional
import sqlite3

import jwt
from flask import Flask, request, jsonify
from flask_cors import CORS
from OpenSSL import crypto

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager, TokenManager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger('Registry')

# ---------------------------------------------------------------------------
# Flask
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# Configuración obligatoria
# ---------------------------------------------------------------------------

# [R-2] JWT_SECRET_KEY — falla duro al arrancar si no está configurada
SECRET_KEY = os.getenv('JWT_SECRET_KEY', '').strip()
if not SECRET_KEY:
    raise RuntimeError(
        "JWT_SECRET_KEY no configurada. "
        "Genera una con: openssl rand -hex 32 "
        "y ponla en el archivo .env"
    )
if len(SECRET_KEY) < 32:
    raise RuntimeError(
        f"JWT_SECRET_KEY demasiado corta ({len(SECRET_KEY)} chars, mínimo 32)."
    )

TOKEN_EXPIRATION_HOURS = 24

# [R-3] REGISTRY_ADMIN_KEY — obligatoria para endpoints destructivos
ADMIN_API_KEY = os.getenv('REGISTRY_ADMIN_KEY', '').strip()
if not ADMIN_API_KEY:
    logger.warning(
        "⚠️  REGISTRY_ADMIN_KEY no configurada. "
        "Los endpoints DELETE /unregister y POST /revoke estarán deshabilitados."
    )


# ---------------------------------------------------------------------------
# Decoradores de autenticación
# ---------------------------------------------------------------------------

def require_admin(f):
    """Exige X-Admin-Key para endpoints administrativos."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not ADMIN_API_KEY:
            return jsonify({'error': 'REGISTRY_ADMIN_KEY no configurada en servidor'}), 503
        provided = request.headers.get('X-Admin-Key', '')
        if provided != ADMIN_API_KEY:
            logger.warning(
                "Intento de acceso admin no autorizado desde %s",
                request.remote_addr
            )
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# [R-1] Generación de certificado SSL persistente
# ---------------------------------------------------------------------------

def generate_or_load_ssl_cert(cert_path: str, key_path: str):
    """
    Genera un certificado autofirmado RSA-2048 + clave privada si no existen.
    Los archivos se guardan en cert_path/key_path para ser reutilizados entre
    reinicios y compartidos con Central como CA de confianza.
    """
    if os.path.exists(cert_path) and os.path.exists(key_path):
        logger.info("✅ Certificado SSL existente cargado desde disco")
        return

    logger.info("🔑 Generando nuevo certificado SSL autofirmado (RSA-2048)...")
    os.makedirs(os.path.dirname(os.path.abspath(cert_path)), exist_ok=True)

    # Clave privada
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 2048)

    # Certificado X.509
    cert = crypto.X509()
    subj = cert.get_subject()
    subj.C  = 'ES'
    subj.ST = 'Alicante'
    subj.L  = 'Alicante'
    subj.O  = 'EVCharging'
    subj.CN = 'ev_registry'
    cert.set_serial_number(int(time.time()))
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(365 * 24 * 60 * 60)  # 1 año
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    # SAN: necesario para que requests lo valide si se usa verify=cert_path
    san = b"DNS:ev_registry, DNS:localhost, IP:127.0.0.1"
    cert.add_extensions([
        crypto.X509Extension(b"subjectAltName", False, san)
    ])
    cert.sign(k, 'sha256')

    with open(cert_path, 'wb') as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    with open(key_path, 'wb') as f:
        f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))

    # Permisos seguros para la clave privada
    os.chmod(key_path, 0o600)
    logger.info("✅ Certificado generado: %s", cert_path)


# ---------------------------------------------------------------------------
# [R-4] + [R-5] Base de datos con concurrencia y rate limiting
# ---------------------------------------------------------------------------

class RegistryDatabase:
    """
    Gestor de la base de datos SQLite del Registry.

    Características:
    - Una sola conexión persistente + threading.RLock() (thread-safe).
    - WAL mode para lecturas concurrentes sin bloquear escrituras.
    - CASCADE DELETE en FK para limpieza automática de credentials/tokens.
    - Rate limiting en memoria (5 fallos → bloqueo 5 min por cp_id).
    """

    _MAX_FAIL_ATTEMPTS = 5
    _LOCKOUT_SECS      = 300   # 5 minutos
    _CLEANUP_INTERVAL  = 600   # Limpiar mapa de fallos cada 10 min

    def __init__(self, db_path: str = 'registry.db'):
        self.db_path = db_path
        self._lock = threading.RLock()

        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
            isolation_level=None     # autocommit; usamos BEGIN/COMMIT manuales
        )
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('PRAGMA synchronous=NORMAL')
        self._conn.execute('PRAGMA foreign_keys=ON')   # [R-6] activar FK
        self._conn.row_factory = sqlite3.Row

        # Rate limiting: cp_id → [timestamp_fallo, ...]
        self._failed: Dict[str, List[float]] = defaultdict(list)
        self._fail_lock = threading.Lock()

        self._init_schema()

        # Hilo de limpieza periódica del mapa de fallos
        t = threading.Thread(target=self._cleanup_loop, daemon=True)
        t.start()

        logger.info("✅ RegistryDatabase inicializada (WAL, FK habilitadas)")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_schema(self):
        with self._lock:
            c = self._conn.cursor()
            c.execute('BEGIN')
            c.execute('''CREATE TABLE IF NOT EXISTS charging_points (
                cp_id            TEXT PRIMARY KEY,
                location         TEXT NOT NULL,
                price            REAL NOT NULL,
                registration_date INTEGER NOT NULL,
                last_auth        INTEGER,
                status           TEXT DEFAULT 'REGISTERED',
                created_at       INTEGER DEFAULT (strftime('%s','now'))
            )''')
            # [R-6] ON DELETE CASCADE para limpiar credenciales al dar de baja
            c.execute('''CREATE TABLE IF NOT EXISTS credentials (
                cp_id         TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                encryption_key TEXT NOT NULL,
                created_at    INTEGER DEFAULT (strftime('%s','now')),
                FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
                    ON DELETE CASCADE
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS active_tokens (
                cp_id      TEXT PRIMARY KEY,
                token      TEXT NOT NULL,
                issued_at  INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
                    ON DELETE CASCADE
            )''')
            c.execute('COMMIT')

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _is_locked_out(self, cp_id: str) -> bool:
        now = time.time()
        with self._fail_lock:
            recent = [t for t in self._failed[cp_id]
                      if now - t < self._LOCKOUT_SECS]
            self._failed[cp_id] = recent
            return len(recent) >= self._MAX_FAIL_ATTEMPTS

    def _record_failure(self, cp_id: str):
        with self._fail_lock:
            self._failed[cp_id].append(time.time())

    def _reset_failures(self, cp_id: str):
        with self._fail_lock:
            self._failed[cp_id] = []

    def _cleanup_loop(self):
        """Elimina entradas antiguas del mapa de fallos cada 10 min."""
        while True:
            time.sleep(self._CLEANUP_INTERVAL)
            cutoff = time.time() - self._LOCKOUT_SECS
            with self._fail_lock:
                for cp_id in list(self._failed.keys()):
                    self._failed[cp_id] = [t for t in self._failed[cp_id] if t > cutoff]
                    if not self._failed[cp_id]:
                        del self._failed[cp_id]

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def register_cp(self, cp_id: str, location: str, price: float) -> Dict:
        """
        Registra un CP nuevo. Devuelve {cp_id, password} en éxito
        o {error} si ya existe.
        La password SOLO se devuelve aquí; no se almacena en claro.
        """
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT cp_id FROM charging_points WHERE cp_id = ?', (cp_id,))
            if c.fetchone():
                return {'error': f"CP '{cp_id}' ya está registrado"}

            password      = secrets.token_urlsafe(16)
            salt          = secrets.token_hex(16)
            password_hash = TokenManager.hash_password(password, salt)
            encryption_key = CryptoManager.generate_key()

            c.execute('BEGIN')
            c.execute(
                '''INSERT INTO charging_points
                   (cp_id, location, price, registration_date, status)
                   VALUES (?, ?, ?, ?, 'REGISTERED')''',
                (cp_id, location, price, int(datetime.now().timestamp()))
            )
            c.execute(
                '''INSERT INTO credentials
                   (cp_id, password_hash, salt, encryption_key)
                   VALUES (?, ?, ?, ?)''',
                (cp_id, password_hash, salt, encryption_key)
            )
            c.execute('COMMIT')

        logger.info("✅ CP registrado: %s", cp_id)
        return {
            'cp_id': cp_id,
            'password': password,
            'message': '⚠️ GUARDE ESTA CONTRASEÑA — no se mostrará nuevamente'
        }

    def unregister_cp(self, cp_id: str) -> Dict:
        """Da de baja un CP y, por CASCADE, borra credentials y tokens."""
        with self._lock:
            c = self._conn.cursor()
            c.execute('SELECT cp_id FROM charging_points WHERE cp_id = ?', (cp_id,))
            if not c.fetchone():
                return {'error': f"CP '{cp_id}' no encontrado"}
            c.execute('BEGIN')
            c.execute('DELETE FROM charging_points WHERE cp_id = ?', (cp_id,))
            c.execute('COMMIT')
        self._reset_failures(cp_id)
        logger.info("🗑️  CP dado de baja: %s", cp_id)
        return {'message': f"CP '{cp_id}' dado de baja correctamente"}

    def authenticate_cp(self, cp_id: str, password: str,
                        source_ip: str = '?') -> Optional[Dict]:
        """
        Autentica un CP.
        - Rate limiting: bloquea tras 5 fallos en 5 min.
        - Devuelve {cp_id, token, encryption_key, expires_in} o None.
        """
        if self._is_locked_out(cp_id):
            remaining = self._lockout_remaining(cp_id)
            logger.warning(
                "🔒 CP '%s' bloqueado temporalmente (%ds restantes) [%s]",
                cp_id, remaining, source_ip
            )
            return None

        with self._lock:
            c = self._conn.cursor()
            c.execute(
                '''SELECT cr.password_hash, cr.salt, cr.encryption_key
                   FROM credentials cr
                   JOIN charging_points cp ON cr.cp_id = cp.cp_id
                   WHERE cr.cp_id = ? AND cp.status = 'REGISTERED' ''',
                (cp_id,)
            )
            row = c.fetchone()

        if not row:
            self._record_failure(cp_id)
            logger.warning("⚠️ Auth fallida — CP desconocido: '%s' [%s]", cp_id, source_ip)
            return None

        if not TokenManager.verify_password(password, row['salt'], row['password_hash']):
            self._record_failure(cp_id)
            logger.warning("⚠️ Auth fallida — contraseña incorrecta: '%s' [%s]", cp_id, source_ip)
            return None

        self._reset_failures(cp_id)

        now     = datetime.utcnow()
        expires = now + timedelta(hours=TOKEN_EXPIRATION_HOURS)
        payload = {'cp_id': cp_id, 'iat': now, 'exp': expires}
        token   = jwt.encode(payload, SECRET_KEY, algorithm='HS256')

        with self._lock:
            c = self._conn.cursor()
            c.execute('BEGIN')
            c.execute(
                '''INSERT OR REPLACE INTO active_tokens
                   (cp_id, token, issued_at, expires_at)
                   VALUES (?, ?, ?, ?)''',
                (cp_id, token,
                 int(now.timestamp()),
                 int(expires.timestamp()))
            )
            c.execute(
                'UPDATE charging_points SET last_auth = ? WHERE cp_id = ?',
                (int(datetime.now().timestamp()), cp_id)
            )
            c.execute('COMMIT')

        logger.info("✅ CP autenticado: '%s' [%s]  key=%.8s...", cp_id, source_ip, row['encryption_key'])
        return {
            'cp_id':          cp_id,
            'token':          token,
            'encryption_key': row['encryption_key'],
            'expires_in':     TOKEN_EXPIRATION_HOURS * 3600
        }

    def _lockout_remaining(self, cp_id: str) -> int:
        now = time.time()
        with self._fail_lock:
            recent = [t for t in self._failed[cp_id]
                      if now - t < self._LOCKOUT_SECS]
            if not recent:
                return 0
            oldest = min(recent)
            return max(0, int(self._LOCKOUT_SECS - (now - oldest)))

    def verify_token(self, token: str) -> Optional[str]:
        """
        Verifica un JWT y comprueba que esté en active_tokens.
        Devuelve cp_id si válido, None en caso contrario.
        """
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            cp_id   = payload.get('cp_id')
            if not cp_id:
                return None
            with self._lock:
                c = self._conn.cursor()
                c.execute(
                    'SELECT cp_id FROM active_tokens WHERE cp_id = ? AND token = ?',
                    (cp_id, token)
                )
                return cp_id if c.fetchone() else None
        except jwt.ExpiredSignatureError:
            logger.debug("Token expirado")
            return None
        except jwt.InvalidTokenError as exc:
            logger.debug("Token inválido: %s", exc)
            return None

    def get_all_cps(self) -> List[Dict]:
        with self._lock:
            c = self._conn.cursor()
            c.execute(
                '''SELECT cp_id, location, price, registration_date,
                          last_auth, status
                   FROM charging_points
                   ORDER BY cp_id'''
            )
            return [dict(row) for row in c.fetchall()]

    def revoke_token(self, cp_id: str):
        with self._lock:
            c = self._conn.cursor()
            c.execute('DELETE FROM active_tokens WHERE cp_id = ?', (cp_id,))
        logger.info("🔒 Token revocado: %s", cp_id)

    def close(self):
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Instancia global
# ---------------------------------------------------------------------------
db = RegistryDatabase(db_path=os.getenv('DB_PATH', 'registry.db'))


# ---------------------------------------------------------------------------
# Endpoints REST
# ---------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'EV_Registry'}), 200


@app.route('/api/v1/register', methods=['POST'])
def register_endpoint():
    """POST /api/v1/register  Body: {cp_id, location, price}"""
    data = request.get_json(silent=True)
    if not data or not all(k in data for k in ('cp_id', 'location', 'price')):
        return jsonify({'error': 'Faltan campos: cp_id, location, price'}), 400
    try:
        price = float(data['price'])
    except (ValueError, TypeError):
        return jsonify({'error': 'price debe ser un número'}), 400

    result = db.register_cp(str(data['cp_id']).strip(), str(data['location']).strip(), price)
    if 'error' in result:
        return jsonify(result), 409
    return jsonify(result), 201


@app.route('/api/v1/unregister/<cp_id>', methods=['DELETE'])
@require_admin
def unregister_endpoint(cp_id: str):
    """DELETE /api/v1/unregister/<cp_id>  (requiere X-Admin-Key)"""
    result = db.unregister_cp(cp_id)
    if 'error' in result:
        return jsonify(result), 404
    return jsonify(result), 200


@app.route('/api/v1/authenticate', methods=['POST'])
def authenticate_endpoint():
    """POST /api/v1/authenticate  Body: {cp_id, password}"""
    data = request.get_json(silent=True)
    if not data or not all(k in data for k in ('cp_id', 'password')):
        return jsonify({'error': 'Faltan campos: cp_id, password'}), 400

    result = db.authenticate_cp(
        str(data['cp_id']).strip(),
        str(data['password']),
        source_ip=request.remote_addr or '?'
    )
    if not result:
        # Mismo mensaje tanto para "no existe" como "contraseña mal" → evitar enumeración
        return jsonify({'error': 'Autenticación fallida'}), 401
    return jsonify(result), 200


@app.route('/api/v1/verify', methods=['POST'])
def verify_token_endpoint():
    """POST /api/v1/verify  Body: {token}"""
    data = request.get_json(silent=True)
    if not data or 'token' not in data:
        return jsonify({'error': 'Falta campo: token'}), 400
    cp_id = db.verify_token(str(data['token']))
    if cp_id:
        return jsonify({'valid': True, 'cp_id': cp_id}), 200
    return jsonify({'valid': False}), 401


@app.route('/api/v1/cps', methods=['GET'])
@require_admin
def list_cps_endpoint():
    """GET /api/v1/cps  (requiere X-Admin-Key) — lista todos los CPs registrados."""
    return jsonify(db.get_all_cps()), 200


@app.route('/api/v1/revoke/<cp_id>', methods=['POST'])
@require_admin
def revoke_token_endpoint(cp_id: str):
    """POST /api/v1/revoke/<cp_id>  (requiere X-Admin-Key)"""
    db.revoke_token(cp_id)
    return jsonify({'message': f"Token de '{cp_id}' revocado"}), 200


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("EV_REGISTRY — Release 2 — Práctica SD 25/26")
    logger.info("=" * 60)

    port      = int(os.getenv('REGISTRY_PORT', 8443))
    data_dir  = os.getenv('REGISTRY_DATA_DIR', '/app/data/certs')
    cert_path = os.path.join(data_dir, 'registry.crt')
    key_path  = os.path.join(data_dir, 'registry.key')

    # [R-1] Generar o cargar certificado SSL persistente
    generate_or_load_ssl_cert(cert_path, key_path)

    # [R-8] Cierre limpio con SIGTERM / SIGINT
    def _handle_signal(signum, frame):
        logger.info("🛑 Señal %d recibida — cerrando Registry...", signum)
        db.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    logger.info("🚀 Registry escuchando en https://0.0.0.0:%d", port)
    logger.info("Endpoints:")
    logger.info("  POST   /api/v1/register")
    logger.info("  DELETE /api/v1/unregister/<cp_id>  [X-Admin-Key]")
    logger.info("  POST   /api/v1/authenticate")
    logger.info("  POST   /api/v1/verify")
    logger.info("  GET    /api/v1/cps                 [X-Admin-Key]")
    logger.info("  POST   /api/v1/revoke/<cp_id>      [X-Admin-Key]")
    logger.info("=" * 60)

    app.run(
        host='0.0.0.0',
        port=port,
        ssl_context=(cert_path, key_path),
        debug=False,
        threaded=True
    )
