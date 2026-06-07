"""
EV_REGISTRY - Sistema de Registro Seguro
API REST con HTTPS real, autenticación JWT y gestión de credenciales.
Release 2 - Práctica SD 25/26

CORRECCIONES APLICADAS:
  [1.3] SSL con certificado persistente (no 'adhoc'); cert distribuido como volumen
  [1.4] JWT_SECRET_KEY obligatoria; falla al arrancar si no está configurada
  [1.6] Endpoints de revocación y baja protegidos con X-Admin-Key
  [1.7] Base de datos con threading.RLock() y WAL mode
  [1.8] Rate limiting: máx 5 intentos fallidos → bloqueo 5 min
  [5.4] Manejo de SIGTERM para cierre limpio
"""
import os
import json
import sqlite3
import secrets
import hashlib
import logging
import threading
import time
import sys
import signal
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, Optional

from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt
from OpenSSL import crypto

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger('Registry')

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# Configuración de seguridad — JWT_SECRET_KEY obligatoria
# ---------------------------------------------------------------------------
SECRET_KEY = os.getenv('JWT_SECRET_KEY')
if not SECRET_KEY:
    raise RuntimeError(
        "JWT_SECRET_KEY no configurada. "
        "Genera una con: openssl rand -hex 32 "
        "y añádela al archivo .env"
    )
if len(SECRET_KEY) < 32:
    raise RuntimeError(
        "JWT_SECRET_KEY demasiado corta (mínimo 32 caracteres hex)."
    )

TOKEN_EXPIRATION_HOURS = 24

# Clave de administración para endpoints destructivos
ADMIN_API_KEY = os.getenv('REGISTRY_ADMIN_KEY')
if not ADMIN_API_KEY:
    logger.warning(
        "⚠️  REGISTRY_ADMIN_KEY no configurada. "
        "Los endpoints de revocación y baja estarán deshabilitados."
    )

# ---------------------------------------------------------------------------
# Helpers de seguridad
# ---------------------------------------------------------------------------

def require_admin(f):
    """Decorador: exige X-Admin-Key en la cabecera."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not ADMIN_API_KEY:
            return jsonify({'error': 'REGISTRY_ADMIN_KEY no configurada en servidor'}), 500
        provided = request.headers.get('X-Admin-Key', '')
        if provided != ADMIN_API_KEY:
            logger.warning(
                f"Intento de acceso admin no autorizado desde {request.remote_addr}"
            )
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Generación de certificado SSL persistente
# ---------------------------------------------------------------------------

def generate_self_signed_cert(cert_path: str, key_path: str):
    """Genera un certificado autofirmado persistente (solo si no existe)."""
    if os.path.exists(cert_path) and os.path.exists(key_path):
        logger.info("✅ Certificado SSL existente reutilizado")
        return

    logger.info("🔑 Generando certificado SSL autofirmado...")
    os.makedirs(os.path.dirname(cert_path), exist_ok=True)

    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 2048)

    cert = crypto.X509()
    cert.get_subject().CN = "ev_registry"
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(365 * 24 * 60 * 60)  # 1 año
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    cert.sign(k, 'sha256')

    with open(cert_path, 'wb') as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    with open(key_path, 'wb') as f:
        f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))

    logger.info(f"✅ Certificado generado: {cert_path}")


# ---------------------------------------------------------------------------
# Base de datos con concurrencia y rate limiting
# ---------------------------------------------------------------------------

class RegistryDatabase:
    """Base de datos del Registry con RLock, WAL mode y rate limiting."""

    _MAX_ATTEMPTS = 5
    _LOCKOUT_SECONDS = 300  # 5 minutos

    def __init__(self, db_path: str = 'registry.db'):
        self.db_path = db_path
        self.lock = threading.RLock()

        self.conn = sqlite3.connect(
            db_path, check_same_thread=False, timeout=30.0,
            isolation_level=None
        )
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.row_factory = sqlite3.Row

        # Rate limiting en memoria: cp_id → [timestamp, ...]
        self._failed_attempts: dict = defaultdict(list)

        self._init_schema()
        logger.info("✅ Base de datos del Registry inicializada (WAL mode)")

    def _init_schema(self):
        with self.lock:
            c = self.conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS charging_points (
                cp_id TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                price REAL NOT NULL,
                registration_date INTEGER NOT NULL,
                last_auth INTEGER,
                status TEXT DEFAULT 'REGISTERED',
                created_at INTEGER DEFAULT (strftime('%s','now'))
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS credentials (
                cp_id TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                encryption_key TEXT NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s','now')),
                FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
                    ON DELETE CASCADE
            )''')
            c.execute('''CREATE TABLE IF NOT EXISTS active_tokens (
                cp_id TEXT PRIMARY KEY,
                token TEXT NOT NULL,
                issued_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
                    ON DELETE CASCADE
            )''')
            self.conn.commit()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _is_locked_out(self, cp_id: str) -> bool:
        now = time.time()
        recent = [t for t in self._failed_attempts[cp_id]
                  if now - t < self._LOCKOUT_SECONDS]
        self._failed_attempts[cp_id] = recent
        return len(recent) >= self._MAX_ATTEMPTS

    def _record_failure(self, cp_id: str):
        self._failed_attempts[cp_id].append(time.time())

    def _reset_failures(self, cp_id: str):
        self._failed_attempts[cp_id] = []

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def register_cp(self, cp_id: str, location: str, price: float) -> Dict:
        with self.lock:
            c = self.conn.cursor()
            c.execute('SELECT cp_id FROM charging_points WHERE cp_id = ?', (cp_id,))
            if c.fetchone():
                return {'error': 'CP_ID ya registrado'}

            password = secrets.token_urlsafe(16)
            salt = secrets.token_hex(16)
            password_hash = hashlib.pbkdf2_hmac(
                'sha256', password.encode(), salt.encode(), 100000
            ).hex()
            encryption_key = CryptoManager.generate_key()

            c.execute('''INSERT INTO charging_points
                (cp_id, location, price, registration_date, status)
                VALUES (?, ?, ?, ?, 'REGISTERED')''',
                (cp_id, location, price, int(datetime.now().timestamp())))
            c.execute('''INSERT INTO credentials
                (cp_id, password_hash, salt, encryption_key)
                VALUES (?, ?, ?, ?)''',
                (cp_id, password_hash, salt, encryption_key))
            self.conn.commit()

        logger.info(f"✅ CP {cp_id} registrado")
        return {
            'cp_id': cp_id,
            'password': password,
            'message': '⚠️ GUARDE ESTA CONTRASEÑA - No se mostrará nuevamente'
        }

    def unregister_cp(self, cp_id: str) -> Dict:
        with self.lock:
            c = self.conn.cursor()
            c.execute('SELECT cp_id FROM charging_points WHERE cp_id = ?', (cp_id,))
            if not c.fetchone():
                return {'error': 'CP_ID no encontrado'}
            c.execute('DELETE FROM charging_points WHERE cp_id = ?', (cp_id,))
            self.conn.commit()
        logger.info(f"❌ CP {cp_id} dado de baja")
        return {'message': f'CP {cp_id} dado de baja exitosamente'}

    def authenticate_cp(self, cp_id: str, password: str,
                        source_ip: str = '?') -> Optional[Dict]:
        """Autenticar CP. Aplica rate limiting y devuelve token + encryption_key."""
        if self._is_locked_out(cp_id):
            logger.warning(
                f"🔒 CP {cp_id} bloqueado temporalmente "
                f"(demasiados intentos fallidos desde {source_ip})"
            )
            return None

        with self.lock:
            c = self.conn.cursor()
            c.execute('''SELECT c.password_hash, c.salt, c.encryption_key
                FROM credentials c
                INNER JOIN charging_points cp ON c.cp_id = cp.cp_id
                WHERE c.cp_id = ? AND cp.status = 'REGISTERED' ''', (cp_id,))
            row = c.fetchone()

        if not row:
            self._record_failure(cp_id)
            logger.warning(f"⚠️ Autenticación fallida (CP desconocido): {cp_id} desde {source_ip}")
            return None

        computed = hashlib.pbkdf2_hmac(
            'sha256', password.encode(), row['salt'].encode(), 100000
        ).hex()
        if computed != row['password_hash']:
            self._record_failure(cp_id)
            logger.warning(f"⚠️ Contraseña incorrecta para {cp_id} desde {source_ip}")
            return None

        self._reset_failures(cp_id)

        payload = {
            'cp_id': cp_id,
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(hours=TOKEN_EXPIRATION_HOURS)
        }
        token = jwt.encode(payload, SECRET_KEY, algorithm='HS256')

        with self.lock:
            c = self.conn.cursor()
            c.execute('''INSERT OR REPLACE INTO active_tokens
                (cp_id, token, issued_at, expires_at) VALUES (?, ?, ?, ?)''',
                (cp_id, token, int(datetime.now().timestamp()),
                 int((datetime.now() + timedelta(hours=TOKEN_EXPIRATION_HOURS)).timestamp())))
            c.execute('UPDATE charging_points SET last_auth = ? WHERE cp_id = ?',
                      (int(datetime.now().timestamp()), cp_id))
            self.conn.commit()

        logger.info(f"✅ CP {cp_id} autenticado desde {source_ip}")
        return {
            'cp_id': cp_id,
            'token': token,
            'encryption_key': row['encryption_key'],
            'expires_in': TOKEN_EXPIRATION_HOURS * 3600
        }

    def verify_token(self, token: str) -> Optional[str]:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            cp_id = payload.get('cp_id')
            with self.lock:
                c = self.conn.cursor()
                c.execute(
                    'SELECT cp_id FROM active_tokens WHERE cp_id = ? AND token = ?',
                    (cp_id, token)
                )
                return cp_id if c.fetchone() else None
        except jwt.ExpiredSignatureError:
            logger.warning("⚠️ Token expirado")
            return None
        except jwt.InvalidTokenError:
            logger.warning("⚠️ Token inválido")
            return None

    def get_all_cps(self) -> list:
        with self.lock:
            c = self.conn.cursor()
            c.execute(
                'SELECT cp_id, location, price, registration_date, '
                'last_auth, status FROM charging_points'
            )
            return [dict(row) for row in c.fetchall()]

    def revoke_token(self, cp_id: str):
        with self.lock:
            c = self.conn.cursor()
            c.execute('DELETE FROM active_tokens WHERE cp_id = ?', (cp_id,))
            self.conn.commit()
        logger.info(f"🔒 Token de {cp_id} revocado")


# ---------------------------------------------------------------------------
# Instancia global
# ---------------------------------------------------------------------------
db = RegistryDatabase(
    db_path=os.getenv('DB_PATH', 'registry.db')
)

# ---------------------------------------------------------------------------
# Endpoints REST
# ---------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'EV_Registry'}), 200


@app.route('/api/v1/register', methods=['POST'])
def register_endpoint():
    """POST /api/v1/register — registrar nuevo CP."""
    try:
        data = request.get_json()
        if not data or not all(k in data for k in ('cp_id', 'location', 'price')):
            return jsonify({'error': 'Faltan campos: cp_id, location, price'}), 400

        result = db.register_cp(data['cp_id'], data['location'], float(data['price']))
        if 'error' in result:
            return jsonify(result), 409
        return jsonify(result), 201
    except Exception as e:
        logger.exception(f"❌ Error en registro: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/unregister/<cp_id>', methods=['DELETE'])
@require_admin
def unregister_endpoint(cp_id: str):
    """DELETE /api/v1/unregister/<cp_id> — dar de baja CP (requiere X-Admin-Key)."""
    try:
        result = db.unregister_cp(cp_id)
        if 'error' in result:
            return jsonify(result), 404
        return jsonify(result), 200
    except Exception as e:
        logger.exception(f"❌ Error en baja: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/authenticate', methods=['POST'])
def authenticate_endpoint():
    """POST /api/v1/authenticate — autenticar CP y obtener token + encryption_key."""
    try:
        data = request.get_json()
        if not data or not all(k in data for k in ('cp_id', 'password')):
            return jsonify({'error': 'Faltan campos: cp_id, password'}), 400

        result = db.authenticate_cp(
            data['cp_id'], data['password'],
            source_ip=request.remote_addr or '?'
        )
        if not result:
            return jsonify({'error': 'Autenticación fallida'}), 401
        return jsonify(result), 200
    except Exception as e:
        logger.exception(f"❌ Error en autenticación: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/verify', methods=['POST'])
def verify_token_endpoint():
    """POST /api/v1/verify — verificar token JWT."""
    try:
        data = request.get_json()
        if not data or 'token' not in data:
            return jsonify({'error': 'Falta campo: token'}), 400

        cp_id = db.verify_token(data['token'])
        if cp_id:
            return jsonify({'valid': True, 'cp_id': cp_id}), 200
        return jsonify({'valid': False}), 401
    except Exception as e:
        logger.exception(f"❌ Error en verificación: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/cps', methods=['GET'])
def list_cps_endpoint():
    """GET /api/v1/cps — listar todos los CPs registrados."""
    try:
        return jsonify(db.get_all_cps()), 200
    except Exception as e:
        logger.exception(f"❌ Error listando CPs: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/revoke/<cp_id>', methods=['POST'])
@require_admin
def revoke_token_endpoint(cp_id: str):
    """POST /api/v1/revoke/<cp_id> — revocar token (requiere X-Admin-Key)."""
    try:
        db.revoke_token(cp_id)
        return jsonify({'message': f'Token de {cp_id} revocado'}), 200
    except Exception as e:
        logger.exception(f"❌ Error revocando token: {e}")
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("EV_REGISTRY - Sistema de Registro Seguro")
    logger.info("Release 2 - Práctica SD 25/26")
    logger.info("=" * 60)

    port = int(os.getenv('REGISTRY_PORT', 8443))

    # Certificado SSL persistente
    data_dir = os.getenv('REGISTRY_DATA_DIR', '/app/data')
    cert_path = os.path.join(data_dir, 'registry.crt')
    key_path = os.path.join(data_dir, 'registry.key')
    generate_self_signed_cert(cert_path, key_path)

    # Manejo de SIGTERM para cierre limpio
    def handle_sigterm(signum, frame):
        logger.info("🛑 SIGTERM recibido, cerrando Registry limpiamente...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    logger.info(f"🚀 Registry escuchando en https://0.0.0.0:{port}")
    logger.info("📋 Endpoints:")
    logger.info("   POST   /api/v1/register           - Registrar CP")
    logger.info("   DELETE /api/v1/unregister/:id     - Baja CP (admin)")
    logger.info("   POST   /api/v1/authenticate       - Autenticar CP")
    logger.info("   POST   /api/v1/verify             - Verificar token")
    logger.info("   GET    /api/v1/cps                - Listar CPs")
    logger.info("   POST   /api/v1/revoke/:id         - Revocar token (admin)")
    logger.info("=" * 60)

    app.run(
        host='0.0.0.0',
        port=port,
        ssl_context=(cert_path, key_path),
        debug=False
    )
