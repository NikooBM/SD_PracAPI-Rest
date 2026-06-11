"""EV_CENTRAL — Central de Control EVCharging
Release 2 - Práctica SD 25/26
"""
import os
import sys
import socket
import threading
import json
import time
import logging
import sqlite3
import signal
import stat
import hashlib
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog
from datetime import datetime
from typing import Optional, Dict, Any, List
from queue import Queue, Empty
import uuid

import requests
import urllib3
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager, AuditLogger

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger('Central')

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
_CRED_DIR       = os.getenv('CP_DATA_DIR', '/app/data/cp_credentials')
_MACHINE_SECRET = os.getenv('MACHINE_SECRET', 'evcharging-lab-secret')

def _cred_path(cp_id: str) -> str:
    os.makedirs(_CRED_DIR, mode=0o700, exist_ok=True)
    return os.path.join(_CRED_DIR, f'{cp_id}.cred')

def _save_password(cp_id: str, password: str):
    key   = hashlib.sha256(f"{_MACHINE_SECRET}:{cp_id}".encode()).hexdigest()
    key_b = (key * 10).encode()
    pw_b  = password.encode('utf-8')
    enc   = bytes(a ^ b for a, b in zip(pw_b, key_b[:len(pw_b)]))
    path  = _cred_path(cp_id)
    import tempfile
    dir_name = os.path.dirname(os.path.abspath(path))
    with tempfile.NamedTemporaryFile('wb', dir=dir_name, delete=False, suffix='.tmp') as tmp:
        tmp.write(enc)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

def _load_password(cp_id: str) -> Optional[str]:
    path = _cred_path(cp_id)
    if not os.path.exists(path):
        return None
    key   = hashlib.sha256(f"{_MACHINE_SECRET}:{cp_id}".encode()).hexdigest()
    key_b = (key * 10).encode()
    with open(path, 'rb') as f:
        enc = f.read()
    dec = bytes(a ^ b for a, b in zip(enc, key_b[:len(enc)]))
    return dec.decode('utf-8')

def _delete_password(cp_id: str):
    path = _cred_path(cp_id)
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """SQLite con RLock + WAL. Conexión persistente."""

    def __init__(self, db_path: str = 'evcharging.db'):
        self.db_path = db_path
        self.lock    = threading.RLock()
        self.conn    = sqlite3.connect(
            db_path, check_same_thread=False,
            timeout=30.0, isolation_level=None
        )
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('PRAGMA foreign_keys=ON')
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        logger.info("✅ BD inicializada (WAL)")

    def _init_schema(self):
        with self.lock:
            c = self.conn.cursor()
            try:
                c.execute('BEGIN')
                c.execute('''CREATE TABLE IF NOT EXISTS charging_points (
                    cp_id         TEXT PRIMARY KEY,
                    location      TEXT NOT NULL,
                    price         REAL NOT NULL,
                    status        TEXT DEFAULT 'DISCONNECTED',
                    last_seen     INTEGER,
                    registered    INTEGER DEFAULT 0,
                    authenticated INTEGER DEFAULT 0,
                    created_at    INTEGER DEFAULT (strftime('%s','now')))''')
                c.execute('''CREATE TABLE IF NOT EXISTS sessions (
                    session_id  TEXT PRIMARY KEY,
                    cp_id       TEXT NOT NULL,
                    driver_id   TEXT NOT NULL,
                    start_time  INTEGER NOT NULL,
                    end_time    INTEGER,
                    kw_consumed REAL DEFAULT 0,
                    total_cost  REAL DEFAULT 0,
                    exitosa     INTEGER DEFAULT 1,
                    razon       TEXT)''')
                c.execute('''CREATE TABLE IF NOT EXISTS cp_credentials (
                    cp_id          TEXT PRIMARY KEY,
                    registry_token TEXT,
                    encryption_key TEXT NOT NULL,
                    created_at     INTEGER DEFAULT (strftime('%s','now')))''')
                c.execute('CREATE INDEX IF NOT EXISTS idx_sessions_cp '
                          'ON sessions(cp_id)')
                c.execute('CREATE INDEX IF NOT EXISTS idx_sessions_active '
                          'ON sessions(end_time) WHERE end_time IS NULL')
                c.execute('COMMIT')
            except Exception as exc:
                c.execute('ROLLBACK')
                raise RuntimeError(f"Error inicializando schema: {exc}") from exc

    def save_cp_credentials(self, cp_id: str, encryption_key: str,
                            registry_token: Optional[str] = None):
        with self.lock:
            self.conn.execute(
                '''INSERT OR REPLACE INTO cp_credentials
                   (cp_id, encryption_key, registry_token) VALUES (?,?,?)''',
                (cp_id, encryption_key, registry_token)
            )

    def get_cp_encryption_key(self, cp_id: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute(
                'SELECT encryption_key FROM cp_credentials WHERE cp_id=?', (cp_id,)
            ).fetchone()
            return row['encryption_key'] if row else None

    def delete_cp_credentials(self, cp_id: str):
        with self.lock:
            self.conn.execute('DELETE FROM cp_credentials WHERE cp_id=?', (cp_id,))

    def save_cp(self, cp_id: str, location: str, price: float):
        with self.lock:
            self.conn.execute(
                '''INSERT OR REPLACE INTO charging_points
                   (cp_id, location, price, last_seen, registered)
                   VALUES (?,?,?,?,1)''',
                (cp_id, location, price, int(time.time()))
            )

    def mark_cp_authenticated(self, cp_id: str):
        with self.lock:
            self.conn.execute(
                'UPDATE charging_points SET authenticated=1 WHERE cp_id=?', (cp_id,)
            )

    def update_cp_status(self, cp_id: str, status: str):
        with self.lock:
            self.conn.execute(
                'UPDATE charging_points SET status=?,last_seen=? WHERE cp_id=?',
                (status, int(time.time()), cp_id)
            )

    def get_all_cps(self) -> List[Dict]:
        with self.lock:
            return [dict(r) for r in
                    self.conn.execute('SELECT * FROM charging_points').fetchall()]

    def save_session(self, s: Dict):
        with self.lock:
            self.conn.execute(
                '''INSERT OR REPLACE INTO sessions
                   (session_id,cp_id,driver_id,start_time,end_time,
                    kw_consumed,total_cost,exitosa,razon)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (s.get('session_id',''), s.get('cp_id',''),
                 s.get('driver_id',''),  s.get('start_time', 0),
                 s.get('end_time'),      s.get('kw_consumed', 0),
                 s.get('total_cost', 0),
                 1 if s.get('exitosa', True) else 0,
                 s.get('razon'))
            )

    def update_session_realtime(self, session_id: str, kw: float, cost: float):
        with self.lock:
            self.conn.execute(
                'UPDATE sessions SET kw_consumed=?,total_cost=? WHERE session_id=?',
                (kw, cost, session_id)
            )

    def get_driver_id_for_session(self, session_id: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute(
                'SELECT driver_id FROM sessions WHERE session_id=?', (session_id,)
            ).fetchone()
            return row['driver_id'] if row else None

    def close(self):
        with self.lock:
            try:
                self.conn.close()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# CPWidget
# ---------------------------------------------------------------------------

class CPWidget(tk.Frame):
    COLORS = {
        'AVAILABLE':    '#2ecc71',
        'CHARGING':     '#27ae60',
        'STOPPED':      '#f39c12',
        'BROKEN':       '#e74c3c',
        'DISCONNECTED': '#95a5a6'
    }
    LABELS = {
        'AVAILABLE':    'DISPONIBLE',
        'CHARGING':     'CARGANDO...',
        'STOPPED':      'FUERA DE SERVICIO',
        'BROKEN':       'AVERIADO',
        'DISCONNECTED': 'DESCONECTADO'
    }

    def __init__(self, parent, cp_id: str, location: str, price: float):
        super().__init__(parent, relief=tk.RAISED, borderwidth=2,
                         bg=self.COLORS['DISCONNECTED'])
        self.cp_id = cp_id
        self._build(location, price)

    def _build(self, location: str, price: float):
        bg = self.COLORS['DISCONNECTED']
        tk.Label(self, text=self.cp_id, font=('Arial', 14, 'bold'),
                 fg='white', bg=bg).pack(pady=5)
        tk.Label(self, text=location, font=('Arial', 9), fg='white',
                 bg=bg, wraplength=200).pack()
        tk.Label(self, text=f"{price:.2f}€/kWh", font=('Arial', 10),
                 fg='white', bg=bg).pack(pady=5)
        tk.Label(self, text="─" * 30, bg=bg, fg='white').pack()
        self.lbl_auth   = tk.Label(self, text='', font=('Arial', 8), fg='yellow', bg=bg)
        self.lbl_auth.pack()
        self.lbl_estado = tk.Label(self, text='DESCONECTADO',
                                   font=('Arial', 11, 'bold'), fg='white', bg=bg)
        self.lbl_estado.pack(pady=10)
        self.frame_carga = tk.Frame(self, bg=bg)
        self.lbl_driver  = tk.Label(self.frame_carga, text='',
                                    font=('Arial', 9, 'bold'), fg='yellow', bg=bg)
        self.lbl_driver.pack()
        self.lbl_consumo = tk.Label(self.frame_carga, text='',
                                    font=('Arial', 11, 'bold'), fg='white', bg=bg)
        self.lbl_consumo.pack(pady=2)
        self.lbl_coste   = tk.Label(self.frame_carga, text='',
                                    font=('Arial', 11, 'bold'), fg='white', bg=bg)
        self.lbl_coste.pack()
        self.lbl_weather = tk.Label(self, text='', font=('Arial', 8),
                                    fg='white', bg=bg)
        self.lbl_weather.pack(pady=2)
        self.config(width=220, height=320)
        self.pack_propagate(False)

    def _set_bg(self, color: str):
        self.config(bg=color)
        self._recurse_bg(self, color)

    def _recurse_bg(self, w, color: str):
        for child in w.winfo_children():
            try:
                child.config(bg=color)
            except tk.TclError:
                pass
            self._recurse_bg(child, color)

    def actualizar(self, status: str, driver_id: str = '',
                   kw: float = 0.0, cost: float = 0.0,
                   authenticated: bool = False):
        try:
            color = self.COLORS.get(status, self.COLORS['DISCONNECTED'])
            self._set_bg(color)
            self.lbl_auth.config(
                text='🔒 Autenticado' if authenticated else '⚠️ Sin autenticar'
            )
            if status == 'CHARGING' and driver_id:
                self.lbl_driver.config(text=f"👤 {driver_id}")
                self.lbl_consumo.config(text=f"⚡ {kw:.2f} kWh")
                self.lbl_coste.config(text=f"💶 {cost:.2f} €")
                self.lbl_estado.pack_forget()
                self.frame_carga.pack(pady=5)
            else:
                self.frame_carga.pack_forget()
                self.lbl_estado.config(text=self.LABELS.get(status, status))
                self.lbl_estado.pack(pady=10)
        except Exception as exc:
            logger.error("Error actualizando widget %s: %s", self.cp_id, exc)

    def set_weather_location(self, city: str):
        try:
            self.lbl_weather.config(text=f"🌡️ {city}")
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Central
# ---------------------------------------------------------------------------

class Central:

    def __init__(self, socket_port: int = 5001,
                 kafka_servers: str = 'localhost:9092',
                 db_path: str = 'evcharging.db'):
        self.socket_port   = socket_port
        self.kafka_servers = (
            kafka_servers if isinstance(kafka_servers, list)
            else [s.strip() for s in kafka_servers.split(',')]
        )
        self.db    = Database(db_path)
        self.audit = AuditLogger(os.getenv('AUDIT_LOG', 'audit.log'))

        self.weather_alerts:   Dict[str, Dict] = {}
        self.weather_locations: Dict[str, str]  = {}
        self.charging_points:   Dict[str, Dict[str, Any]] = {}
        self.sessions:          Dict[str, Dict[str, Any]] = {}
        self.pending_commands:  Dict[str, str]             = {}
        self.lock = threading.RLock()

        self.gui_queue: Queue = Queue()
        self.server_socket: Optional[socket.socket] = None
        self.producer:      Optional[KafkaProducer]  = None
        self.consumer:      Optional[KafkaConsumer]  = None
        self.running = True

        self.root:           Optional[tk.Tk]                     = None
        self.cp_widgets:     Dict[str, CPWidget]                 = {}
        self.log_text:       Optional[scrolledtext.ScrolledText] = None
        self.requests_table: Optional[ttk.Treeview]              = None
        self.frame_cps:      Optional[tk.Frame]                  = None
        self.request_items:  Dict[str, str]                      = {}

        self.registry_cert = os.getenv('REGISTRY_CERT_PATH', '/app/certs/registry.crt')

    # ------------------------------------------------------------------
    # Arranque
    # ------------------------------------------------------------------

    def start(self) -> bool:
        logger.info("=" * 60)
        logger.info("SISTEMA CENTRAL — RELEASE 2")
        logger.info("=" * 60)
        self._load_cps_from_db()
        if not self._init_kafka():
            logger.error("❌ No se pudo inicializar Kafka")
            return False
        if not self._init_socket_server():
            logger.error("❌ No se pudo iniciar socket server")
            return False
        threading.Thread(target=self._monitor_connections, daemon=True).start()
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL', 'System startup',
                             'Central iniciada', True)
        logger.info("✅ Sistema listo")
        self._init_gui()
        return True

    def _load_cps_from_db(self):
        for cp in self.db.get_all_cps():
            self.charging_points[cp['cp_id']] = {
                'location':             cp['location'],
                'price':                cp['price'],
                'status':               'DISCONNECTED',
                'socket':               None,
                'session':              None,
                'last_seen':            0,
                'monitor_alive':        False,
                'engine_alive':         False,
                'consecutive_failures': 0,
                'authenticated':        bool(cp.get('authenticated', 0))
            }

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                logger.info("🔄 Kafka intento %d/15...", attempt)
                admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_servers,
                    request_timeout_ms=5000
                )
                admin.list_topics()
                admin.close()
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5,
                    request_timeout_ms=30_000,
                    max_block_ms=10_000
                )
                self._init_consumer()
                if self.consumer:
                    threading.Thread(target=self._kafka_consumer_loop,
                                     daemon=True).start()
                    logger.info("✅ Kafka OK")
                    return True
            except Exception as exc:
                logger.warning("⚠️ Kafka error (%d/15): %s", attempt, exc)
                if attempt < 15:
                    time.sleep(5)
        return False

    def _init_consumer(self):
        try:
            self.consumer = KafkaConsumer(
                'service_requests', 'charging_data',
                'charging_complete', 'weather_sync',
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='central-group',
                enable_auto_commit=True,
                session_timeout_ms=30_000,
                consumer_timeout_ms=1000
            )
        except Exception as exc:
            logger.error("❌ No se pudo crear consumer: %s", exc)
            self.consumer = None

    def _kafka_consumer_loop(self):
        while self.running:
            try:
                if self.consumer is None:
                    self._init_consumer()
                    time.sleep(2)
                    continue
                for msg in self.consumer:
                    if not self.running:
                        break
                    try:
                        if msg.topic == 'service_requests':
                            self._handle_service_request(msg.value)
                        elif msg.topic == 'weather_sync':
                            v = msg.value
                            self.handle_weather_alert(
                                v['cp_id'], v['alert_type'],
                                v['temperature'], v['city']
                            )
                        elif msg.topic == 'charging_data':
                            self._handle_charging_data(msg.value)
                        elif msg.topic == 'charging_complete':
                            self._handle_charging_complete(msg.value)
                    except Exception as exc:
                        logger.exception("Error procesando msg [%s]: %s", msg.topic, exc)
            except Exception as exc:
                if self.running and 'timed out' not in str(exc).lower():
                    logger.error("❌ Consumer error, reiniciando: %s", exc)
                    try:
                        if self.consumer:
                            self.consumer.close()
                    except Exception:
                        pass
                    self.consumer = None
                    time.sleep(5)

    # ------------------------------------------------------------------
    # Socket server
    # ------------------------------------------------------------------

    def _init_socket_server(self) -> bool:
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.socket_port))
            self.server_socket.settimeout(1.0)
            self.server_socket.listen(10)
            threading.Thread(target=self._accept_monitors, daemon=True).start()
            logger.info("✅ Socket servidor en :%d", self.socket_port)
            return True
        except Exception as exc:
            logger.exception("❌ Socket error: %s", exc)
            return False

    def _accept_monitors(self):
        while self.running:
            try:
                if not self.server_socket:
                    break
                client_socket, address = self.server_socket.accept()
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.audit.log_event('CONNECTION', address[0], 'MONITOR',
                                     'Connection attempt', f'From {address}', True)
                threading.Thread(
                    target=self._handle_monitor,
                    args=(client_socket, address[0]),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except Exception as exc:
                if self.running:
                    logger.error("Error en accept: %s", exc)
                    time.sleep(1)

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _verify_cp_credentials(self, cp_id: str,
                                password: Optional[str] = None) -> bool:
        registry_url = os.getenv('REGISTRY_URL', 'https://ev_registry:8443')
        verify_ssl: Any = False
        if os.path.exists(self.registry_cert):
            verify_ssl = self.registry_cert
        else:
            logger.warning(
                "⚠️ Cert del Registry no encontrado (%s); usando verify=False",
                self.registry_cert
            )
        try:
            if not password:
                password = _load_password(cp_id)
                if not password:
                    logger.error("❌ No hay contraseña almacenada para '%s'", cp_id)
                    return False

            response = requests.post(
                f"{registry_url}/api/v1/authenticate",
                json={'cp_id': cp_id, 'password': password},
                verify=verify_ssl,
                timeout=10
            )
            if response.status_code == 200:
                _save_password(cp_id, password)
                logger.info("✅ Credenciales verificadas: %s", cp_id)
                self.audit.log_authentication(cp_id, '0.0.0.0', True, 'PASSWORD_REGISTRY')
                return True
            logger.warning("⚠️ Registry rechazó '%s' (HTTP %d)", cp_id, response.status_code)
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'PASSWORD_INVALID')
            return False
        except requests.exceptions.ConnectionError as exc:
            logger.error("❌ Registry inalcanzable — CP '%s' RECHAZADO: %s", cp_id, exc)
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'REGISTRY_UNAVAILABLE')
            return False
        except requests.exceptions.Timeout:
            logger.error("❌ Timeout con Registry — CP '%s' RECHAZADO", cp_id)
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'REGISTRY_TIMEOUT')
            return False
        except Exception as exc:
            logger.error("❌ Error inesperado verificando '%s': %s", cp_id, exc)
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'REGISTRY_ERROR')
            return False

    # ------------------------------------------------------------------
    # Handler del Monitor
    # ------------------------------------------------------------------

    def _handle_monitor(self, sock: socket.socket, client_ip: str):
        cp_id: Optional[str] = None
        try:
            sock.settimeout(30)   # Margen amplio para negociación con Registry
            raw = sock.recv(2048).decode('utf-8', errors='replace').strip()
            if not raw.startswith('REGISTER'):
                logger.warning("Mensaje inesperado desde %s: %s", client_ip, raw[:80])
                return
            parts = raw.split('|')
            if len(parts) < 4:
                logger.warning("REGISTER incompleto desde %s", client_ip)
                return
            _, cp_id, location, price_str = parts[:4]
            password = parts[4] if len(parts) > 4 else None
            try:
                price = float(price_str)
            except ValueError:
                sock.send(b'ERROR|BAD_REQUEST|precio invalido')
                return

            if not self._verify_cp_credentials(cp_id, password):
                sock.send(b'ERROR|INVALID_CREDENTIALS|CP no autorizado por Registry')
                logger.warning("🚫 Autenticación rechazada: %s [%s]", cp_id, client_ip)
                self._enqueue_gui_action('log', f"🚫 {cp_id} rechazado [{client_ip}]")
                self.audit.log_authentication(cp_id, client_ip, False, 'PASSWORD_INVALID')
                return

            with self.lock:
                is_new = cp_id not in self.charging_points
                if is_new:
                    self.charging_points[cp_id] = {
                        'location': location, 'price': price,
                        'status': 'AVAILABLE', 'socket': sock,
                        'session': None, 'last_seen': time.time(),
                        'monitor_alive': True, 'engine_alive': False,
                        'consecutive_failures': 0, 'authenticated': False
                    }
                    self.db.save_cp(cp_id, location, price)
                    self._enqueue_gui_action('log', f"✅ {cp_id} registrado")
                    self._enqueue_gui_action('add_cp', cp_id, location, price)
                else:
                    cp_data = self.charging_points[cp_id]
                    if cp_data.get('session'):
                        self._abort_session(cp_id, 'CP reconectado')
                    cp_data.update({
                        'socket': sock, 'status': 'AVAILABLE',
                        'last_seen': time.time(), 'monitor_alive': True,
                        'engine_alive': False, 'consecutive_failures': 0
                    })
                    logger.info("🔄 %s reconectado", cp_id)

            encryption_key = self.db.get_cp_encryption_key(cp_id)
            if not encryption_key:
                encryption_key = CryptoManager.generate_key()
                self.db.save_cp_credentials(cp_id, encryption_key)
                logger.info("🔑 Nueva clave generada: %s", cp_id)

            self.db.mark_cp_authenticated(cp_id)
            with self.lock:
                self.charging_points[cp_id]['authenticated'] = True

            sock.send(f'OK|REGISTERED|{encryption_key}'.encode('utf-8'))
            self.audit.log_authentication(cp_id, client_ip, True, 'FULL_AUTH')
            self._enqueue_gui_action('log', f"🔐 {cp_id} autenticado")
            self._enqueue_gui_action('update_cp', cp_id, 'AVAILABLE', '', 0, 0, True)
            self.db.update_cp_status(cp_id, 'AVAILABLE')
            self._monitor_health_loop(cp_id, sock)

        except Exception as exc:
            logger.exception("❌ Monitor %s: %s", cp_id, exc)
            if cp_id:
                self.audit.log_error('MONITOR_ERROR', cp_id, str(exc))
        finally:
            if cp_id and cp_id in self.charging_points:
                with self.lock:
                    cp_data = self.charging_points[cp_id]
                    if cp_data.get('socket') == sock:
                        if cp_data.get('session'):
                            self._abort_session(cp_id, 'Monitor desconectado')
                        old = cp_data['status']
                        cp_data.update({
                            'status': 'DISCONNECTED', 'socket': None,
                            'monitor_alive': False, 'engine_alive': False,
                            'authenticated': False
                        })
                        self._enqueue_gui_action('update_cp', cp_id, 'DISCONNECTED',
                                                 '', 0, 0, False)
                        self.db.update_cp_status(cp_id, 'DISCONNECTED')
                        self._enqueue_gui_action('log', f"❌ {cp_id} desconectado")
                        self.audit.log_cp_status_change(
                            cp_id, old, 'DISCONNECTED', 'Monitor disconnected'
                        )
            try:
                sock.close()
            except Exception:
                pass

    def _monitor_health_loop(self, cp_id: str, sock: socket.socket):
        sock.settimeout(5)
        last_health_ok = time.time()
        while self.running:
            try:
                msg = sock.recv(64).decode('utf-8', errors='replace').strip()
                if not msg:
                    break
                with self.lock:
                    if cp_id not in self.charging_points:
                        break
                    self.charging_points[cp_id]['last_seen'] = time.time()
                    self.charging_points[cp_id]['monitor_alive'] = True

                if 'HEALTH_OK' in msg:
                    last_health_ok = time.time()
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive']         = True
                            cp_data['consecutive_failures'] = 0
                            if cp_data['status'] == 'BROKEN':
                                cmd = self.pending_commands.pop(cp_id, None)
                                new_status = 'STOPPED' if cmd == 'STOP' else 'AVAILABLE'
                                if cmd:
                                    self._send_kafka(
                                        'central_commands',
                                        {'cp_id': cp_id, 'command': cmd,
                                         'timestamp': time.time()},
                                        encrypt_for_cp=cp_id
                                    )
                                cp_data['status'] = new_status
                                self._enqueue_gui_action(
                                    'update_cp', cp_id, new_status,
                                    '', 0, 0, cp_data.get('authenticated', False)
                                )
                                self.db.update_cp_status(cp_id, new_status)
                                self._enqueue_gui_action('log', f"✅ {cp_id} recuperado")

                elif 'HEALTH_FAIL' in msg:
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive']          = False
                            cp_data['consecutive_failures'] += 1
                            if (cp_data['consecutive_failures'] >= 3
                                    and cp_data['monitor_alive']
                                    and cp_data['status'] != 'BROKEN'):
                                self._handle_cp_failure(cp_id)

            except socket.timeout:
                if time.time() - last_health_ok > 6:
                    logger.warning("⏰ Timeout health: %s", cp_id)
                    break
                continue
            except Exception as exc:
                if self.running:
                    logger.error("❌ Health loop %s: %s", cp_id, exc)
                break

    # ------------------------------------------------------------------
    # Handlers Kafka
    # ------------------------------------------------------------------

    def _decrypt_message(self, data: Dict, cp_id: str) -> Optional[Dict]:
        if isinstance(data, dict) and data.get('encrypted') and data.get('data'):
            key = self.db.get_cp_encryption_key(cp_id)
            if key:
                try:
                    return CryptoManager.decrypt_json(data['data'], key)
                except Exception as exc:
                    logger.error("Error descifrando de %s: %s", cp_id, exc)
                    self._enqueue_gui_action(
                        'log',
                        f"❌ Mensajes no comprensibles de {cp_id} (clave incorrecta)"
                    )
                    return None
            logger.error("No hay clave de descifrado para %s", cp_id)
            return None
        return data

    def _handle_service_request(self, data: Dict):
        driver_id = data.get('driver_id', '')
        cp_id     = data.get('cp_id', '')
        self._enqueue_gui_action('log', f"📨 {driver_id} → {cp_id}")
        self.audit.log_service_request(driver_id, cp_id, '0.0.0.0')

        with self.lock:
            if cp_id not in self.charging_points:
                self._send_notification(driver_id, 'DENIED', cp_id, 'CP no existe')
                self.audit.log_service_auth(driver_id, cp_id, False)
                return
            cp = self.charging_points[cp_id]
            if cp['status'] != 'AVAILABLE':
                reasons = {
                    'DISCONNECTED': 'CP desconectado',
                    'BROKEN':       'CP averiado',
                    'STOPPED':      'CP fuera de servicio',
                    'CHARGING':     'CP ocupado'
                }
                self._send_notification(
                    driver_id, 'DENIED', cp_id,
                    reasons.get(cp['status'], 'No disponible')
                )
                self.audit.log_service_auth(driver_id, cp_id, False)
                return

            session_id = f"SES_{cp_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
            cp['status'] = 'CHARGING'
            cp['session'] = {
                'session_id':  session_id,
                'driver_id':   driver_id,
                'start_time':  int(time.time()),
                'kw_consumed': 0.0,
                'total_cost':  0.0
            }
            self.sessions[session_id] = {**cp['session'], 'cp_id': cp_id}

            self._send_kafka('service_authorizations', {
                'cp_id':      cp_id,
                'driver_id':  driver_id,
                'session_id': session_id,
                'price':      cp['price'],
                'timestamp':  time.time()
            }, encrypt_for_cp=cp_id)

            self._send_notification(driver_id, 'AUTHORIZED', cp_id, 'Autorizado')
            self.audit.log_service_auth(driver_id, cp_id, True)

            self._enqueue_gui_action('update_cp', cp_id, 'CHARGING', driver_id,
                                     0.0, 0.0, cp.get('authenticated', False))
            self._enqueue_gui_action(
                'add_request', session_id,
                datetime.now().strftime('%d/%m/%y'),
                datetime.now().strftime('%H:%M'),
                driver_id, cp_id
            )
            self.db.update_cp_status(cp_id, 'CHARGING')
            self._enqueue_gui_action('log', f"✅ {driver_id} en {cp_id}")

    def _handle_charging_data(self, data: Dict):
        cp_id = data.get('cp_id', '')
        if data.get('encrypted'):
            data = self._decrypt_message(data, cp_id)
            if data is None:
                return
        with self.lock:
            if cp_id not in self.charging_points:
                return
            cp = self.charging_points[cp_id]
            if not cp.get('session'):
                return
            kw   = float(data.get('kw',   0.0))
            cost = float(data.get('cost', 0.0))
            cp['session']['kw_consumed'] = kw
            cp['session']['total_cost']  = cost
            driver_id  = cp['session'].get('driver_id', '')
            session_id = cp['session'].get('session_id')

            if session_id:
                self.db.update_session_realtime(session_id, kw, cost)

            if driver_id:
                self._send_kafka('driver_notifications', {
                    'driver_id': driver_id,
                    'cp_id':     cp_id,
                    'kw':        kw,
                    'cost':      cost,
                    'type':      'CHARGING_UPDATE',
                    'timestamp': time.time()
                })
            self._enqueue_gui_action(
                'update_cp', cp_id, 'CHARGING', driver_id,
                kw, cost, cp.get('authenticated', False)
            )

    def _handle_charging_complete(self, data: Dict):
        cp_id = data.get('cp_id', '')
        if data.get('encrypted'):
            data = self._decrypt_message(data, cp_id)
            if data is None:
                return

        session_id = data.get('session_id', '')
        driver_id  = data.get('driver_id', '')
        exitosa    = data.get('exitosa', True)
        razon      = data.get('razon', '')
        kw_total   = float(data.get('kw_total',   0))
        cost_total = float(data.get('cost_total', 0))

        logger.info("📋 Finalizando sesión %s: %s", session_id, cp_id)

        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                if cp.get('session'):
                    session = cp['session']
                    session.update({
                        'end_time':    int(time.time()),
                        'cp_id':       cp_id,
                        'kw_consumed': kw_total,
                        'total_cost':  cost_total,
                        'exitosa':     exitosa,
                        'razon':       razon
                    })
                    self.db.save_session(session)
                cp['session'] = None
                if cp['status'] == 'CHARGING':
                    cp['status'] = 'AVAILABLE'
                    self.db.update_cp_status(cp_id, 'AVAILABLE')
                self._enqueue_gui_action(
                    'update_cp', cp_id, cp['status'], '', 0, 0,
                    cp.get('authenticated', False)
                )
            if session_id in self.sessions:
                del self.sessions[session_id]
                self._enqueue_gui_action('remove_request', session_id)

        if not driver_id:
            driver_id = self.db.get_driver_id_for_session(session_id) or ''

        if driver_id:
            ticket = {
                'driver_id':  driver_id,
                'cp_id':      cp_id,
                'session_id': session_id,
                'kw_total':   kw_total,
                'cost_total': cost_total,
                'exitosa':    exitosa,
                'razon':      razon,
                'type':       'FINAL_TICKET',
                'timestamp':  time.time()
            }
            self._send_kafka('driver_notifications', ticket, require_ack=True)
            logger.info("✅ Ticket final enviado a %s", driver_id)
        else:
            logger.error("❌ driver_id desconocido para sesión %s", session_id)

        self.audit.log_event(
            'SESSION', '0.0.0.0', driver_id or 'UNKNOWN', 'Charging complete',
            f'Session:{session_id} CP:{cp_id} Status:{"OK" if exitosa else razon}',
            exitosa
        )

    # ------------------------------------------------------------------
    # Weather
    # ------------------------------------------------------------------

    def handle_weather_alert(self, cp_id: str, alert_type: str,
                             temperature: float, city: str):
        with self.lock:
            if city and cp_id:
                self.weather_locations[cp_id] = city
                self._enqueue_gui_action('update_weather_location', cp_id, city)
            if alert_type == 'REGISTER':
                logger.info("📍 Localización registrada: %s → %s", cp_id, city)
                return
            if alert_type == 'START':
                self.weather_alerts[cp_id] = {
                    'temperature': temperature, 'city': city,
                    'started_at': time.time()
                }
                logger.warning("❄️ ALERTA: %s (%s) — %.1f°C", cp_id, city, temperature)
                self._send_command(cp_id, 'STOP')
                self.audit.log_weather_alert(cp_id, 'START', temperature)
                self._enqueue_gui_action('log', f"❄️ Alerta: {cp_id} ({temperature:.1f}°C)")
            elif alert_type == 'END':
                self.weather_alerts.pop(cp_id, None)
                logger.info("☀️ ALERTA CANCELADA: %s (%s)", cp_id, city)
                self._send_command(cp_id, 'RESUME')
                self.audit.log_weather_alert(cp_id, 'END', temperature)
                self._enqueue_gui_action('log', f"☀️ Alerta cancelada: {cp_id}")

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    def _send_command(self, cp_id: str, command: str):
        with self.lock:
            if cp_id not in self.charging_points:
                logger.warning("⚠️ CP '%s' no existe para enviar %s", cp_id, command)
                return
            cp_data = self.charging_points[cp_id]
            self.pending_commands[cp_id] = command
            if command == 'STOP':
                if cp_data.get('session'):
                    self._abort_session(cp_id, 'Detenido por Central (STOP)')
                old = cp_data['status']
                cp_data['status'] = 'STOPPED'
                self._enqueue_gui_action('update_cp', cp_id, 'STOPPED', '', 0, 0,
                                         cp_data.get('authenticated', False))
                self._enqueue_gui_action('log', f"⛔ {cp_id} PARADO")
                self.db.update_cp_status(cp_id, 'STOPPED')
                self.audit.log_cp_status_change(cp_id, old, 'STOPPED', command)
            elif command == 'RESUME':
                old = cp_data['status']
                cp_data['status'] = 'AVAILABLE'
                self._enqueue_gui_action('update_cp', cp_id, 'AVAILABLE', '', 0, 0,
                                         cp_data.get('authenticated', False))
                self._enqueue_gui_action('log', f"▶️ {cp_id} REANUDADO")
                self.db.update_cp_status(cp_id, 'AVAILABLE')
                self.audit.log_cp_status_change(cp_id, old, 'AVAILABLE', command)

        self._send_kafka(
            'central_commands',
            {'cp_id': cp_id, 'command': command, 'timestamp': time.time()},
            encrypt_for_cp=cp_id
        )
        self.audit.log_command(cp_id, command)
        logger.info("📤 Comando %s → %s", command, cp_id)

    def _abort_session(self, cp_id: str, razon: str):
        """Aborta sesión activa. Llamar con self.lock tomado."""
        cp_data = self.charging_points.get(cp_id)
        if not cp_data or not cp_data.get('session'):
            return
        session = cp_data['session']
        session.update({'end_time': int(time.time()), 'exitosa': False,
                        'razon': razon, 'cp_id': cp_id})
        self.db.save_session(session)
        # No adquirimos self.lock de nuevo aquí
        self._send_kafka('driver_notifications', {
            'driver_id':  session['driver_id'],
            'cp_id':      cp_id,
            'session_id': session['session_id'],
            'kw_total':   session['kw_consumed'],
            'cost_total': session['total_cost'],
            'exitosa':    False,
            'razon':      razon,
            'type':       'FINAL_TICKET',
            'timestamp':  time.time()
        })
        cp_data['session'] = None
        self._enqueue_gui_action('remove_request', session['session_id'])
        self.audit.log_error('SESSION_ABORTED', cp_id, f'Razón: {razon}')

    def _handle_cp_failure(self, cp_id: str):
        if cp_id not in self.charging_points:
            return
        cp = self.charging_points[cp_id]
        if not cp.get('monitor_alive') or cp['status'] == 'BROKEN':
            return
        old = cp['status']
        cp['status'] = 'BROKEN'
        cp['engine_alive'] = False
        if cp.get('session'):
            self._abort_session(cp_id, 'Avería del Engine')
        self._enqueue_gui_action('update_cp', cp_id, 'BROKEN', '', 0, 0,
                                 cp.get('authenticated', False))
        self.db.update_cp_status(cp_id, 'BROKEN')
        self._enqueue_gui_action('log', f"💥 {cp_id} AVERIADO")
        self.audit.log_cp_status_change(cp_id, old, 'BROKEN',
                                        'Consecutive engine failures')
        logger.warning("💥 CP %s marcado como AVERIADO", cp_id)

    def _monitor_connections(self):
        while self.running:
            try:
                now = time.time()
                with self.lock:
                    for cp_id, cp_data in list(self.charging_points.items()):
                        if (cp_data and
                                now - cp_data.get('last_seen', 0) > 15 and
                                cp_data['status'] != 'DISCONNECTED'):
                            if cp_data.get('session'):
                                self._abort_session(cp_id, 'Timeout')
                            old = cp_data['status']
                            cp_data.update({
                                'status': 'DISCONNECTED', 'socket': None,
                                'monitor_alive': False, 'engine_alive': False,
                                'authenticated': False
                            })
                            self._enqueue_gui_action('update_cp', cp_id,
                                                     'DISCONNECTED', '', 0, 0, False)
                            self.db.update_cp_status(cp_id, 'DISCONNECTED')
                            self._enqueue_gui_action('log', f"❌ {cp_id} TIMEOUT")
                            self.audit.log_cp_status_change(
                                cp_id, old, 'DISCONNECTED', 'Connection timeout'
                            )
                time.sleep(2)
            except Exception as exc:
                if self.running:
                    logger.error("Error en _monitor_connections: %s", exc)
                    time.sleep(2)

    # ------------------------------------------------------------------
    # Kafka send
    # ------------------------------------------------------------------

    def _send_kafka(self, topic: str, payload: Dict,
                    encrypt_for_cp: Optional[str] = None,
                    require_ack: bool = False):
        for attempt in range(3):
            try:
                if not self.producer:
                    return
                final = payload
                if encrypt_for_cp:
                    key = self.db.get_cp_encryption_key(encrypt_for_cp)
                    if key:
                        try:
                            final = {
                                'encrypted': True,
                                'data':      CryptoManager.encrypt_json(payload, key),
                                'cp_id':     encrypt_for_cp
                            }
                        except Exception as exc:
                            logger.warning("⚠️ Error cifrando: %s", exc)
                future = self.producer.send(topic, final)
                if require_ack:
                    future.get(timeout=5)
                return
            except Exception as exc:
                logger.error("Error Kafka [%s] intento %d: %s", topic, attempt + 1, exc)
                time.sleep(1)

    def _send_notification(self, driver_id: str, status: str,
                           cp_id: str, message: str):
        self._send_kafka('driver_notifications', {
            'driver_id': driver_id, 'status': status,
            'cp_id':     cp_id,     'message': message,
            'timestamp': time.time()
        })

    # ------------------------------------------------------------------
    # Revocación de claves
    # ------------------------------------------------------------------

    def revoke_cp_encryption_key(self, cp_id: str) -> bool:
        with self.lock:
            if cp_id not in self.charging_points:
                return False
            cp_data = self.charging_points[cp_id]
            if cp_data.get('session'):
                self._abort_session(cp_id, 'Clave revocada')
            if cp_data.get('socket'):
                try:
                    cp_data['socket'].close()
                except Exception:
                    pass
                cp_data['socket'] = None
            old = cp_data['status']
            cp_data.update({
                'status': 'DISCONNECTED', 'authenticated': False,
                'monitor_alive': False, 'engine_alive': False
            })
            self.db.delete_cp_credentials(cp_id)
            _delete_password(cp_id)
            self._enqueue_gui_action('update_cp', cp_id, 'DISCONNECTED', '', 0, 0, False)
            self._enqueue_gui_action('log', f"🔒 Clave revocada: {cp_id}")
            self.audit.log_event('SECURITY', '0.0.0.0', 'CENTRAL',
                                 'Key revocation', f'CP: {cp_id}', True)
            self.audit.log_cp_status_change(cp_id, old, 'DISCONNECTED',
                                            'Encryption key revoked')
        return True

    def revoke_all_encryption_keys(self) -> int:
        with self.lock:
            cp_ids = list(self.charging_points.keys())
        return sum(1 for cp_id in cp_ids if self.revoke_cp_encryption_key(cp_id))

    # ------------------------------------------------------------------
    # GUI — cola thread-safe
    # ------------------------------------------------------------------

    def _enqueue_gui_action(self, action: str, *args):
        if self.root:
            self.gui_queue.put((action, args))

    def _process_gui_queue(self):
        try:
            for _ in range(20):
                try:
                    action, args = self.gui_queue.get_nowait()
                except Empty:
                    break
                if   action == 'log':
                    self._do_log(args[0])
                elif action == 'add_cp':
                    self._do_gui_add_cp(args[0], args[1], args[2])
                elif action == 'update_cp':
                    self._do_gui_update_cp(*args)
                elif action == 'add_request':
                    self._do_gui_add_request(*args)
                elif action == 'remove_request':
                    self._do_gui_remove_request(args[0])
                elif action == 'update_weather_location':
                    self._do_gui_update_weather_location(args[0], args[1])
        finally:
            if self.root and self.running:
                self.root.after(100, self._process_gui_queue)

    def _init_gui(self):
        self.root = tk.Tk()
        self.root.title("EVCharging — CENTRAL (RELEASE 2)")
        self.root.geometry("1400x950")
        self.root.config(bg='#2c3e50')
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

        header = tk.Frame(self.root, bg='#1a252f', height=70)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        tk.Label(header, text="*** EV CHARGING — CENTRAL (RELEASE 2) ***",
                 font=('Arial', 16, 'bold'), bg='#1a252f', fg='#ecf0f1').pack(pady=20)

        cp_container = tk.Frame(self.root, bg='#34495e')
        cp_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        canvas    = tk.Canvas(cp_container, bg='#34495e', highlightthickness=0)
        scrollbar = ttk.Scrollbar(cp_container, orient='vertical', command=canvas.yview)
        inner = tk.Frame(canvas, bg='#34495e')
        inner.bind('<Configure>',
                   lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.create_window((0, 0), window=inner, anchor='nw')
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.frame_cps = tk.Frame(inner, bg='#34495e')
        self.frame_cps.pack(padx=10, pady=10)

        req_frame = tk.Frame(self.root, bg='#1a252f', height=130)
        req_frame.pack(fill=tk.X, padx=10, pady=5)
        req_frame.pack_propagate(False)
        tk.Label(req_frame, text="*** ON GOING REQUESTS ***",
                 font=('Arial', 11, 'bold'), bg='#1a252f', fg='white').pack(pady=5)
        self.requests_table = ttk.Treeview(
            req_frame, columns=('DATE', 'TIME', 'USER', 'CP'),
            show='headings', height=3
        )
        for col in ('DATE', 'TIME', 'USER', 'CP'):
            self.requests_table.heading(col, text=col)
            self.requests_table.column(col, width=150, anchor=tk.CENTER)
        self.requests_table.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        cmd_frame = tk.Frame(self.root, bg='#1a252f', height=120)
        cmd_frame.pack(fill=tk.X, padx=10, pady=5)
        cmd_frame.pack_propagate(False)
        tk.Label(cmd_frame, text="*** CENTRAL COMMANDS ***",
                 font=('Arial', 11, 'bold'), bg='#1a252f', fg='white').pack(pady=5)
        bf1 = tk.Frame(cmd_frame, bg='#1a252f')
        bf1.pack()
        for text, cmd, color in [
            ("⛔ PARAR CP",        self._cmd_stop_cp,    '#e74c3c'),
            ("▶️ REANUDAR CP",    self._cmd_resume_cp,  '#2ecc71'),
            ("⛔ PARAR TODOS",    self._cmd_stop_all,   '#c0392b'),
            ("▶️ REANUDAR TODOS", self._cmd_resume_all, '#27ae60'),
        ]:
            tk.Button(bf1, text=text, command=cmd, bg=color, fg='white',
                      font=('Arial', 10, 'bold'), width=14).pack(side=tk.LEFT, padx=5)
        bf2 = tk.Frame(cmd_frame, bg='#1a252f')
        bf2.pack(pady=5)
        for text, cmd, color in [
            ("🔒 REVOCAR CLAVE CP",  self._cmd_revoke_key,      '#9b59b6'),
            ("🔒 REVOCAR TODAS",     self._cmd_revoke_all_keys, '#8e44ad'),
            ("🔐 LISTAR CLAVES",     self._cmd_list_keys,       '#3498db'),
        ]:
            tk.Button(bf2, text=text, command=cmd, bg=color, fg='white',
                      font=('Arial', 10, 'bold'), width=18).pack(side=tk.LEFT, padx=5)

        log_frame = tk.Frame(self.root, bg='#1a252f', height=100)
        log_frame.pack(fill=tk.X, padx=10, pady=5)
        log_frame.pack_propagate(False)
        tk.Label(log_frame, text="*** MESSAGES ***",
                 font=('Arial', 11, 'bold'), bg='#1a252f', fg='white').pack(pady=5)
        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=3, bg='#2c3e50', fg='#ecf0f1', font=('Courier', 9)
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        with self.lock:
            for cp_id, cp_data in self.charging_points.items():
                w = CPWidget(self.frame_cps, cp_id,
                             cp_data['location'], cp_data['price'])
                n = len(self.cp_widgets)
                w.grid(row=n // 5, column=n % 5, padx=10, pady=10)
                self.cp_widgets[cp_id] = w
                w.actualizar(cp_data['status'], '', 0, 0,
                             cp_data.get('authenticated', False))

        logger.info("✅ GUI lista — %d CPs", len(self.cp_widgets))
        self.root.after(100, self._process_gui_queue)
        self.root.mainloop()

    def _do_log(self, msg: str):
        if self.log_text:
            try:
                self.log_text.insert(
                    tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"
                )
                self.log_text.see(tk.END)
            except Exception:
                pass

    def _do_gui_add_cp(self, cp_id: str, location: str, price: float):
        if cp_id in self.cp_widgets or not self.frame_cps:
            return
        try:
            w = CPWidget(self.frame_cps, cp_id, location, price)
            n = len(self.cp_widgets)
            w.grid(row=n // 5, column=n % 5, padx=10, pady=10)
            self.cp_widgets[cp_id] = w
        except Exception as exc:
            logger.exception("Error creando widget %s: %s", cp_id, exc)

    def _do_gui_update_cp(self, cp_id: str, status: str, driver: str = '',
                          kw: float = 0.0, cost: float = 0.0,
                          authenticated: bool = False):
        if cp_id in self.cp_widgets:
            try:
                self.cp_widgets[cp_id].actualizar(status, driver, kw, cost, authenticated)
                with self.lock:
                    city = self.weather_locations.get(cp_id)
                if city:
                    self.cp_widgets[cp_id].set_weather_location(city)
            except Exception as exc:
                logger.error("Error actualizando widget %s: %s", cp_id, exc)

    def _do_gui_update_weather_location(self, cp_id: str, city: str):
        if cp_id in self.cp_widgets:
            try:
                self.cp_widgets[cp_id].set_weather_location(city)
            except Exception:
                pass
        self._do_log(f"🌡️ EV_W monitoreando {city} para {cp_id}")

    def _do_gui_add_request(self, sid, date, time_str, user, cp):
        if self.requests_table:
            try:
                item = self.requests_table.insert(
                    '', tk.END, values=(date, time_str, user, cp)
                )
                self.request_items[sid] = item
            except Exception:
                pass

    def _do_gui_remove_request(self, sid: str):
        if self.requests_table and sid in self.request_items:
            try:
                self.requests_table.delete(self.request_items.pop(sid))
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Botones de comando
    # ------------------------------------------------------------------

    def _cmd_stop_cp(self):
        cp_id = simpledialog.askstring("Parar CP", "ID del CP:")
        if cp_id:
            cp_id = cp_id.strip().upper()
            if cp_id in self.charging_points:
                self._send_command(cp_id, 'STOP')
                messagebox.showinfo("OK", f"STOP → {cp_id}")
            else:
                messagebox.showerror("Error", f"CP '{cp_id}' no existe")

    def _cmd_resume_cp(self):
        cp_id = simpledialog.askstring("Reanudar CP", "ID del CP:")
        if cp_id:
            cp_id = cp_id.strip().upper()
            if cp_id in self.charging_points:
                self._send_command(cp_id, 'RESUME')
                messagebox.showinfo("OK", f"RESUME → {cp_id}")
            else:
                messagebox.showerror("Error", f"CP '{cp_id}' no existe")

    def _cmd_stop_all(self):
        if messagebox.askyesno("Confirmar", "¿Parar TODOS los CPs?"):
            with self.lock:
                for cp_id in list(self.charging_points.keys()):
                    self._send_command(cp_id, 'STOP')

    def _cmd_resume_all(self):
        if messagebox.askyesno("Confirmar", "¿Reanudar TODOS los CPs?"):
            with self.lock:
                for cp_id in list(self.charging_points.keys()):
                    self._send_command(cp_id, 'RESUME')

    def _cmd_revoke_key(self):
        cp_id = simpledialog.askstring("Revocar Clave", "ID del CP:")
        if cp_id:
            cp_id = cp_id.strip().upper()
            if cp_id in self.charging_points:
                if messagebox.askyesno(
                    "Confirmar",
                    f"¿Revocar clave de {cp_id}?\nEl CP deberá re-autenticarse."
                ):
                    if self.revoke_cp_encryption_key(cp_id):
                        messagebox.showinfo("Éxito", f"✅ Clave revocada: {cp_id}")
                    else:
                        messagebox.showerror("Error", "❌ Error revocando clave")
            else:
                messagebox.showerror("Error", f"CP '{cp_id}' no existe")

    def _cmd_revoke_all_keys(self):
        if messagebox.askyesno(
            "⚠️ EMERGENCIA",
            "¿REVOCAR TODAS LAS CLAVES?\nTodos los CPs quedarán fuera de servicio."
        ):
            count = self.revoke_all_encryption_keys()
            messagebox.showinfo("Completado", f"🔒 {count} claves revocadas")

    def _cmd_list_keys(self):
        with self.lock:
            cps = list(self.charging_points.items())
        lines = ["=" * 60, "ESTADO DE CLAVES DE CIFRADO", "=" * 60]
        for cp_id, cp_data in cps:
            auth       = "🔐 Autenticado" if cp_data.get('authenticated') else "⚠️ No autenticado"
            key        = self.db.get_cp_encryption_key(cp_id)
            key_status = "✅ Presente" if key else "❌ Ausente"
            lines += [f"\n{cp_id}:", f"  Estado: {auth}", f"  Clave:  {key_status}"]
            if key:
                lines.append(f"  Hash:   {key[:20]}...")
        lines.append("\n" + "=" * 60)
        dlg = tk.Toplevel(self.root)
        dlg.title("Estado de Claves")
        dlg.geometry("600x500")
        tw = scrolledtext.ScrolledText(dlg, width=70, height=25, font=('Courier', 9))
        tw.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        tw.insert(tk.END, "\n".join(lines))
        tw.config(state=tk.DISABLED)
        tk.Button(dlg, text="Cerrar", command=dlg.destroy,
                  font=('Arial', 10, 'bold')).pack(pady=10)

    def _on_closing(self):
        if messagebox.askyesno("Cerrar", "¿Cerrar la Central?"):
            self.shutdown()
            if self.root:
                self.root.destroy()

    def shutdown(self):
        self.running = False
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL',
                             'System shutdown', 'Central detenida', True)
        for resource in (self.server_socket, self.consumer, self.producer):
            try:
                if resource:
                    if hasattr(resource, 'flush'):
                        resource.flush(timeout=3)
                    resource.close()
            except Exception:
                pass
        self.db.close()
        logger.info("✅ Central cerrada")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    from typing import Any

    socket_port   = int(os.getenv('SOCKET_PORT',         '5001'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    db_path       = os.getenv('DB_PATH',                 'evcharging.db')

    central = Central(socket_port, kafka_servers, db_path)

    def _handle_signal(signum, frame):
        logger.info("🛑 Señal %d recibida — cerrando Central...", signum)
        central.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    central.start()
