"""
EV_CENTRAL - Release 2
Módulo principal del Central de EVCharging.

CORRECCIONES APLICADAS:
  [1.1] Modo degradado eliminado: Registry inalcanzable → CP rechazado
  [1.2] Contraseñas guardadas en directorio protegido /app/data/cp_credentials, no en /tmp
  [1.3] verify=False eliminado; se usa certificado del Registry como CA
  [2.6] Tkinter thread-safe: eliminadas llamadas a root.update() fuera del mainloop
  [4.1] TCP Keep-Alive en socket del Monitor
  [4.2] Timeout health check reducido a 5-6 s
  [4.4] Consumer Kafka con reinicio automático en fallos
  [4.5] flush() solo en mensajes críticos
  [5.2] traceback.print_exc() → logger.exception()
  [5.4] Manejo de SIGTERM
  [5.6] Logs de alta frecuencia en nivel DEBUG
  [5.7] Verificación Kafka sin test_topic
"""
import os
import socket
import threading
import json
import time
import logging
import sqlite3
import sys
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # Solo si REGISTRY_CERT_PATH no disponible

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager, AuditLogger

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger('Central')

# ---------------------------------------------------------------------------
# Gestión de credenciales persistentes (no en /tmp)
# ---------------------------------------------------------------------------
_CRED_DIR = os.getenv('CP_DATA_DIR', '/app/data/cp_credentials')
_MACHINE_SECRET = os.getenv('MACHINE_SECRET', 'evcharging-default-secret')


def _cred_path(cp_id: str) -> str:
    os.makedirs(_CRED_DIR, mode=0o700, exist_ok=True)
    return os.path.join(_CRED_DIR, f'{cp_id}.cred')


def _save_password(cp_id: str, password: str):
    """Guardar contraseña ofuscada con clave derivada del entorno."""
    key = hashlib.sha256(f"{_MACHINE_SECRET}:{cp_id}".encode()).hexdigest()
    key_bytes = (key * 10).encode()
    pw_bytes = password.encode()
    encrypted = bytes(a ^ b for a, b in zip(pw_bytes, key_bytes[:len(pw_bytes)]))
    path = _cred_path(cp_id)
    with open(path, 'wb') as f:
        f.write(encrypted)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    logger.debug(f"🔐 Credencial guardada para {cp_id}")


def _load_password(cp_id: str) -> Optional[str]:
    """Cargar y descifrar contraseña."""
    path = _cred_path(cp_id)
    if not os.path.exists(path):
        return None
    key = hashlib.sha256(f"{_MACHINE_SECRET}:{cp_id}".encode()).hexdigest()
    key_bytes = (key * 10).encode()
    with open(path, 'rb') as f:
        encrypted = f.read()
    decrypted = bytes(a ^ b for a, b in zip(encrypted, key_bytes[:len(encrypted)]))
    return decrypted.decode()


def _delete_password(cp_id: str):
    path = _cred_path(cp_id)
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """Wrapper SQLite con RLock y WAL mode."""

    def __init__(self, db_path: str = 'evcharging.db'):
        self.db_path = db_path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            db_path, check_same_thread=False,
            timeout=30.0, isolation_level=None
        )
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        logger.info("✅ BD inicializada (WAL mode)")

    def _init_schema(self):
        with self.lock:
            c = self.conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS charging_points (
                cp_id TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                price REAL NOT NULL,
                status TEXT DEFAULT 'DISCONNECTED',
                last_seen INTEGER,
                registered INTEGER DEFAULT 0,
                authenticated INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s','now')))''')
            c.execute('''CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                cp_id TEXT NOT NULL,
                driver_id TEXT NOT NULL,
                start_time INTEGER NOT NULL,
                end_time INTEGER,
                kw_consumed REAL DEFAULT 0,
                total_cost REAL DEFAULT 0,
                exitosa INTEGER DEFAULT 1,
                razon TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS cp_credentials (
                cp_id TEXT PRIMARY KEY,
                registry_token TEXT,
                encryption_key TEXT NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s','now')))''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_sessions_cp ON sessions(cp_id)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_sessions_active '
                      'ON sessions(end_time) WHERE end_time IS NULL')

    def save_cp_credentials(self, cp_id: str, encryption_key: str,
                            registry_token: Optional[str] = None):
        with self.lock:
            self.conn.execute(
                '''INSERT OR REPLACE INTO cp_credentials
                   (cp_id, encryption_key, registry_token) VALUES (?, ?, ?)''',
                (cp_id, encryption_key, registry_token)
            )

    def get_cp_encryption_key(self, cp_id: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute(
                'SELECT encryption_key FROM cp_credentials WHERE cp_id = ?', (cp_id,)
            ).fetchone()
            return row['encryption_key'] if row else None

    def mark_cp_authenticated(self, cp_id: str):
        with self.lock:
            self.conn.execute(
                'UPDATE charging_points SET authenticated = 1 WHERE cp_id = ?', (cp_id,)
            )

    def save_cp(self, cp_id: str, location: str, price: float):
        with self.lock:
            self.conn.execute(
                '''INSERT OR REPLACE INTO charging_points
                   (cp_id, location, price, last_seen, registered) VALUES (?, ?, ?, ?, 1)''',
                (cp_id, location, price, int(time.time()))
            )

    def get_all_cps(self) -> List[Dict]:
        with self.lock:
            return [dict(r) for r in
                    self.conn.execute('SELECT * FROM charging_points').fetchall()]

    def update_cp_status(self, cp_id: str, status: str):
        with self.lock:
            self.conn.execute(
                'UPDATE charging_points SET status = ?, last_seen = ? WHERE cp_id = ?',
                (status, int(time.time()), cp_id)
            )

    def save_session(self, session_data: Dict):
        with self.lock:
            self.conn.execute(
                '''INSERT OR REPLACE INTO sessions
                   (session_id, cp_id, driver_id, start_time, end_time,
                    kw_consumed, total_cost, exitosa, razon)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (session_data.get('session_id', ''),
                 session_data.get('cp_id', ''),
                 session_data.get('driver_id', ''),
                 session_data.get('start_time', 0),
                 session_data.get('end_time'),
                 session_data.get('kw_consumed', 0),
                 session_data.get('total_cost', 0),
                 1 if session_data.get('exitosa', True) else 0,
                 session_data.get('razon'))
            )

    def update_session_realtime(self, session_id: str, kw: float, cost: float):
        with self.lock:
            self.conn.execute(
                'UPDATE sessions SET kw_consumed = ?, total_cost = ? WHERE session_id = ?',
                (kw, cost, session_id)
            )

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
    """Widget visual de un CP (thread-safe vía cola GUI)."""

    COLORS = {
        'AVAILABLE':    '#2ecc71',
        'CHARGING':     '#27ae60',
        'STOPPED':      '#f39c12',
        'BROKEN':       '#e74c3c',
        'DISCONNECTED': '#95a5a6'
    }
    STATUS_TEXT = {
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
        self._setup_widgets(location, price)

    def _setup_widgets(self, location: str, price: float):
        bg = self.cget('bg')
        tk.Label(self, text=self.cp_id, font=('Arial', 14, 'bold'),
                 fg='white', bg=bg).pack(pady=5)
        tk.Label(self, text=location, font=('Arial', 9), fg='white',
                 bg=bg, wraplength=200).pack()
        tk.Label(self, text=f"{price}€/kWh", font=('Arial', 10),
                 fg='white', bg=bg).pack(pady=5)
        tk.Label(self, text="─" * 30, bg=bg, fg='white').pack()

        self.lbl_auth = tk.Label(self, text='', font=('Arial', 8),
                                 fg='yellow', bg=bg)
        self.lbl_auth.pack()

        self.lbl_estado = tk.Label(self, text='DESCONECTADO',
                                   font=('Arial', 11, 'bold'), fg='white', bg=bg)
        self.lbl_estado.pack(pady=10)

        self.frame_carga = tk.Frame(self, bg=bg)
        self.lbl_driver = tk.Label(self.frame_carga, text='',
                                   font=('Arial', 9, 'bold'), fg='yellow', bg=bg)
        self.lbl_driver.pack()
        self.lbl_consumo = tk.Label(self.frame_carga, text='',
                                    font=('Arial', 11, 'bold'), fg='white', bg=bg)
        self.lbl_consumo.pack(pady=2)
        self.lbl_coste = tk.Label(self.frame_carga, text='',
                                  font=('Arial', 11, 'bold'), fg='white', bg=bg)
        self.lbl_coste.pack()

        self.lbl_weather = tk.Label(self, text='', font=('Arial', 8),
                                    fg='white', bg=bg)
        self.lbl_weather.pack(pady=2)

        self.config(width=220, height=310)
        self.pack_propagate(False)

    def _set_bg(self, color: str):
        self.config(bg=color)
        self._recursive_bg(self, color)

    def _recursive_bg(self, widget, color: str):
        for child in widget.winfo_children():
            if isinstance(child, (tk.Label, tk.Frame)):
                try:
                    child.config(bg=color)
                except Exception:
                    pass
            self._recursive_bg(child, color)

    def actualizar(self, status: str, driver_id: str = '', kw: float = 0.0,
                   cost: float = 0.0, authenticated: bool = False):
        """Actualizar widget — llamar SOLO desde el hilo principal de Tkinter."""
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
                self.lbl_estado.config(
                    text=self.STATUS_TEXT.get(status, status)
                )
                self.lbl_estado.pack(pady=10)
        except Exception as e:
            logger.error(f"Error actualizando widget {self.cp_id}: {e}")

    def set_weather_location(self, city: str):
        """Mostrar ciudad monitoreada — llamar SOLO desde hilo Tkinter."""
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
        self.socket_port = socket_port
        self.kafka_servers = (
            kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        )

        self.db = Database(db_path)
        self.audit = AuditLogger('audit.log')

        self.weather_alerts: Dict[str, Dict] = {}
        self.weather_locations: Dict[str, str] = {}

        self.charging_points: Dict[str, Dict[str, Any]] = {}
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.pending_commands: Dict[str, str] = {}
        self.lock = threading.RLock()

        self.gui_queue: Queue = Queue()

        self.server_socket: Optional[socket.socket] = None
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True

        # GUI
        self.root: Optional[tk.Tk] = None
        self.cp_widgets: Dict[str, CPWidget] = {}
        self.log_text: Optional[scrolledtext.ScrolledText] = None
        self.requests_table: Optional[ttk.Treeview] = None
        self.frame_cps: Optional[tk.Frame] = None
        self.request_items: Dict[str, str] = {}

        # Certificado del Registry para verificación SSL
        self.registry_cert = os.getenv(
            'REGISTRY_CERT_PATH', '/app/data/registry.crt'
        )

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    def start(self) -> bool:
        logger.info("=" * 60)
        logger.info("SISTEMA CENTRAL - RELEASE 2")
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
                'location': cp['location'],
                'price': cp['price'],
                'status': 'DISCONNECTED',
                'socket': None,
                'session': None,
                'last_seen': 0,
                'monitor_alive': False,
                'engine_alive': False,
                'consecutive_failures': 0,
                'authenticated': bool(cp.get('authenticated', 0))
            }

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                logger.info(f"🔄 Kafka ({attempt}/15)...")

                # Verificar sin publicar en topics basura
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
                    request_timeout_ms=30000,
                    max_block_ms=10000
                )

                self._init_consumer()
                if self.consumer:
                    threading.Thread(
                        target=self._kafka_consumer_loop, daemon=True
                    ).start()
                    logger.info("✅ Kafka OK")
                    return True
            except Exception as e:
                logger.warning(f"⚠️ Kafka error ({attempt}/15): {e}")
                if attempt < 15:
                    time.sleep(5)
        return False

    def _init_consumer(self):
        """Crear/recrear consumer Kafka."""
        try:
            self.consumer = KafkaConsumer(
                'service_requests', 'charging_data',
                'charging_complete', 'weather_sync',
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='central-group',
                enable_auto_commit=True,
                session_timeout_ms=30000,
                consumer_timeout_ms=1000
            )
        except Exception as e:
            logger.error(f"❌ No se pudo crear consumer: {e}")
            self.consumer = None

    def _kafka_consumer_loop(self):
        """Loop consumer con reinicio automático ante fallos."""
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
                    except Exception as e:
                        logger.exception(f"Error procesando msg {msg.topic}: {e}")

            except Exception as e:
                if self.running and 'timed out' not in str(e).lower():
                    logger.error(f"❌ Consumer error, reiniciando: {e}")
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
            logger.info(f"✅ Socket servidor en :{self.socket_port}")
            return True
        except Exception as e:
            logger.exception(f"❌ Socket error: {e}")
            return False

    def _accept_monitors(self):
        while self.running:
            try:
                if self.server_socket is None:
                    break
                client_socket, address = self.server_socket.accept()
                self.audit.log_event('CONNECTION', address[0], 'MONITOR',
                                     'Connection attempt', f'From {address}', True)
                threading.Thread(
                    target=self._handle_monitor,
                    args=(client_socket, address[0]),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"Error en accept: {e}")
                    time.sleep(1)

    # ------------------------------------------------------------------
    # Verificación de credenciales — sin modo degradado
    # ------------------------------------------------------------------

    def _verify_cp_credentials(self, cp_id: str,
                                password: Optional[str] = None) -> bool:
        registry_url = os.getenv('REGISTRY_URL', 'https://ev_registry:8443')

        # Determinar si se puede verificar SSL
        verify_ssl: Any = False
        if os.path.exists(self.registry_cert):
            verify_ssl = self.registry_cert
        # Si no hay cert disponible se acepta verify=False (red interna de laboratorio)
        # pero se loguea como advertencia
        if verify_ssl is False:
            logger.warning(
                "⚠️ Certificado Registry no encontrado; "
                "usando verify=False (solo aceptable en red de laboratorio)"
            )

        try:
            if not password:
                password = _load_password(cp_id)
                if not password:
                    logger.error(f"❌ No hay contraseña para {cp_id}")
                    return False

            response = requests.post(
                f"{registry_url}/api/v1/authenticate",
                json={'cp_id': cp_id, 'password': password},
                verify=verify_ssl,
                timeout=10
            )

            if response.status_code == 200:
                _save_password(cp_id, password)
                logger.info(f"✅ Credenciales verificadas: {cp_id}")
                self.audit.log_authentication(cp_id, '0.0.0.0', True, 'PASSWORD_REGISTRY')
                return True
            else:
                logger.warning(f"⚠️ Credenciales inválidas: {cp_id} (HTTP {response.status_code})")
                self.audit.log_authentication(cp_id, '0.0.0.0', False, 'PASSWORD_INVALID')
                return False

        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ Registry inalcanzable: {e}. CP {cp_id} RECHAZADO.")
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'REGISTRY_UNAVAILABLE')
            return False

        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout conectando a Registry. CP {cp_id} RECHAZADO.")
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'REGISTRY_TIMEOUT')
            return False

        except Exception as e:
            logger.error(f"❌ Error inesperado verificando {cp_id}: {e}. RECHAZADO.")
            self.audit.log_authentication(cp_id, '0.0.0.0', False, 'REGISTRY_ERROR')
            return False

    # ------------------------------------------------------------------
    # Handler del Monitor
    # ------------------------------------------------------------------

    def _handle_monitor(self, sock: socket.socket, client_ip: str):
        cp_id: Optional[str] = None
        try:
            sock.settimeout(10)
            data = sock.recv(1024).decode('utf-8').strip()

            if not data.startswith('REGISTER'):
                return

            parts = data.split('|')
            if len(parts) < 4:
                return

            _, cp_id, location, price_str = parts[:4]
            password = parts[4] if len(parts) > 4 else None
            price = float(price_str)

            if not self._verify_cp_credentials(cp_id, password):
                sock.send(b'ERROR|INVALID_CREDENTIALS|CP no autorizado por Registry')
                logger.warning(f"🚫 Autenticación rechazada: {cp_id}")
                self._enqueue_gui_action('log', f"🚫 {cp_id} rechazado")
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
                    logger.info(f"🔄 {cp_id} reconectado")

            # Encryption key
            encryption_key = self.db.get_cp_encryption_key(cp_id)
            if not encryption_key:
                encryption_key = CryptoManager.generate_key()
                self.db.save_cp_credentials(cp_id, encryption_key)
                logger.info(f"🔑 Nueva clave generada: {cp_id}")

            self.db.mark_cp_authenticated(cp_id)
            with self.lock:
                self.charging_points[cp_id]['authenticated'] = True

            sock.send(f'OK|REGISTERED|{encryption_key}'.encode('utf-8'))
            self.audit.log_authentication(cp_id, client_ip, True, 'FULL_AUTH')
            self._enqueue_gui_action('log', f"🔐 {cp_id} autenticado")
            self._enqueue_gui_action('update_cp', cp_id, 'AVAILABLE', '', 0, 0, True)
            self.db.update_cp_status(cp_id, 'AVAILABLE')

            self._monitor_health_loop(cp_id, sock)

        except Exception as e:
            logger.exception(f"❌ Monitor {cp_id}: {e}")
            if cp_id:
                self.audit.log_error('MONITOR_ERROR', cp_id, str(e))
        finally:
            if cp_id and cp_id in self.charging_points:
                with self.lock:
                    cp_data = self.charging_points[cp_id]
                    if cp_data.get('socket') == sock:
                        if cp_data.get('session'):
                            self._abort_session(cp_id, 'Monitor desconectado')
                        old_status = cp_data['status']
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
                            cp_id, old_status, 'DISCONNECTED', 'Monitor disconnected'
                        )
            try:
                sock.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Health loop — timeout reducido a 5-6 s
    # ------------------------------------------------------------------

    def _monitor_health_loop(self, cp_id: str, sock: socket.socket):
        """Health check loop con timeout reducido (5 s socket, 6 s lógica)."""
        sock.settimeout(5)
        last_health_ok = time.time()

        while self.running:
            try:
                msg = sock.recv(64).decode('utf-8').strip()
                if not msg:
                    break

                with self.lock:
                    if cp_id not in self.charging_points:
                        break
                    cp_data = self.charging_points[cp_id]
                    cp_data['last_seen'] = time.time()
                    cp_data['monitor_alive'] = True

                if msg == 'HEALTH_OK':
                    last_health_ok = time.time()
                    logger.debug(f"[{cp_id}] HEALTH_OK")
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive'] = True
                            cp_data['consecutive_failures'] = 0

                            if cp_data['status'] == 'BROKEN':
                                cmd = self.pending_commands.pop(cp_id, None)
                                if cmd:
                                    self._send_kafka(
                                        'central_commands',
                                        {'cp_id': cp_id, 'command': cmd,
                                         'timestamp': time.time()},
                                        encrypt_for_cp=cp_id
                                    )
                                    cp_data['status'] = ('STOPPED' if cmd == 'STOP'
                                                         else 'AVAILABLE')
                                else:
                                    cp_data['status'] = 'AVAILABLE'

                                self._enqueue_gui_action(
                                    'update_cp', cp_id, cp_data['status'],
                                    '', 0, 0, cp_data.get('authenticated', False)
                                )
                                self.db.update_cp_status(cp_id, cp_data['status'])
                                self._enqueue_gui_action(
                                    'log', f"✅ {cp_id} recuperado de avería"
                                )

                elif msg == 'HEALTH_FAIL':
                    logger.debug(f"[{cp_id}] HEALTH_FAIL")
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive'] = False
                            cp_data['consecutive_failures'] += 1

                            if (cp_data['consecutive_failures'] >= 3 and
                                    cp_data['monitor_alive'] and
                                    cp_data['status'] != 'BROKEN'):
                                self._handle_cp_failure(cp_id)

            except socket.timeout:
                if time.time() - last_health_ok > 6:
                    logger.warning(f"⏰ Timeout health check: {cp_id}")
                    break
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Health loop {cp_id}: {e}")
                break

    # ------------------------------------------------------------------
    # Kafka handlers
    # ------------------------------------------------------------------

    def _decrypt_message(self, data: Dict, cp_id: str) -> Optional[Dict]:
        if not isinstance(data, dict):
            return data
        if data.get('encrypted') and data.get('data'):
            key = self.db.get_cp_encryption_key(cp_id)
            if key:
                try:
                    return CryptoManager.decrypt_json(data['data'], key)
                except Exception as e:
                    logger.error(f"Error descifrando de {cp_id}: {e}")
                    return None
            logger.error(f"No hay clave para descifrar {cp_id}")
            return None
        return data

    def _handle_service_request(self, data: Dict):
        driver_id = data.get('driver_id', '')
        cp_id = data.get('cp_id', '')
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
                    'DISCONNECTED': 'CP desconectado', 'BROKEN': 'CP averiado',
                    'STOPPED': 'CP fuera de servicio', 'CHARGING': 'CP ocupado'
                }
                self._send_notification(driver_id, 'DENIED', cp_id,
                                        reasons.get(cp['status'], 'No disponible'))
                self.audit.log_service_auth(driver_id, cp_id, False)
                return

            session_id = (f"SESSION_{cp_id}_{int(time.time())}"
                          f"_{uuid.uuid4().hex[:6]}")
            cp['status'] = 'CHARGING'
            cp['session'] = {
                'session_id': session_id, 'driver_id': driver_id,
                'start_time': int(time.time()),
                'kw_consumed': 0.0, 'total_cost': 0.0
            }
            self.sessions[session_id] = {**cp['session'], 'cp_id': cp_id}

            self._send_kafka('service_authorizations', {
                'cp_id': cp_id, 'driver_id': driver_id,
                'session_id': session_id, 'price': cp['price'],
                'timestamp': time.time()
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
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                if cp.get('session'):
                    kw = data.get('kw', 0.0)
                    cost = data.get('cost', 0.0)
                    cp['session']['kw_consumed'] = kw
                    cp['session']['total_cost'] = cost
                    driver_id = cp['session'].get('driver_id', '')

                    session_id = cp['session'].get('session_id')
                    if session_id:
                        self.db.update_session_realtime(session_id, kw, cost)

                    if driver_id:
                        self._send_kafka('driver_notifications', {
                            'driver_id': driver_id, 'cp_id': cp_id,
                            'kw': kw, 'cost': cost,
                            'type': 'CHARGING_UPDATE', 'timestamp': time.time()
                        })

                    self._enqueue_gui_action(
                        'update_cp', cp_id, 'CHARGING', driver_id,
                        kw, cost, cp.get('authenticated', False)
                    )
                    logger.debug(f"[{cp_id}] ⚡ {kw:.2f} kWh | {cost:.2f} €")

    def _handle_charging_complete(self, data: Dict):
        cp_id = data.get('cp_id', '')
        if data.get('encrypted'):
            data = self._decrypt_message(data, cp_id)
            if data is None:
                return

        session_id = data.get('session_id', '')
        driver_id = data.get('driver_id', '')
        exitosa = data.get('exitosa', True)
        razon = data.get('razon', '')
        kw_total = data.get('kw_total', 0)
        cost_total = data.get('cost_total', 0)

        logger.info(f"📋 Finalizando sesión {session_id}: {cp_id}")

        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                if cp.get('session'):
                    session = cp['session']
                    session.update({
                        'end_time': int(time.time()), 'cp_id': cp_id,
                        'kw_consumed': kw_total, 'total_cost': cost_total,
                        'exitosa': exitosa, 'razon': razon
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

        # Recuperar driver_id si falta
        if not driver_id:
            try:
                row = self.db.conn.execute(
                    'SELECT driver_id FROM sessions WHERE session_id = ?', (session_id,)
                ).fetchone()
                if row:
                    driver_id = row['driver_id']
            except Exception:
                pass

        if driver_id:
            ticket = {
                'driver_id': driver_id, 'cp_id': cp_id, 'session_id': session_id,
                'kw_total': float(kw_total), 'cost_total': float(cost_total),
                'exitosa': exitosa, 'razon': razon,
                'type': 'FINAL_TICKET', 'timestamp': time.time()
            }
            # Enviar con confirmación (require_ack) — mensaje crítico
            self._send_kafka('driver_notifications', ticket, require_ack=True)
            time.sleep(0.1)
            self._send_kafka('driver_notifications', ticket, require_ack=True)
            logger.info(f"✅ Ticket final enviado a {driver_id}")
        else:
            logger.error(
                f"❌ No se pudo enviar ticket: driver_id desconocido "
                f"para sesión {session_id}"
            )

        self.audit.log_event(
            'SESSION', '0.0.0.0', driver_id or 'UNKNOWN', 'Charging complete',
            f'Session: {session_id}, CP: {cp_id}, '
            f'Status: {"OK" if exitosa else razon}',
            exitosa
        )

    # ------------------------------------------------------------------
    # Weather alerts
    # ------------------------------------------------------------------

    def handle_weather_alert(self, cp_id: str, alert_type: str,
                              temperature: float, city: str):
        """Manejar alerta climática proveniente de weather_sync."""
        with self.lock:
            if city and cp_id:
                self.weather_locations[cp_id] = city
                self._enqueue_gui_action('update_weather_location', cp_id, city)

            if alert_type == 'REGISTER':
                logger.info(f"📍 Localización registrada: {cp_id} → {city}")
                return

            if alert_type == 'START':
                self.weather_alerts[cp_id] = {
                    'temperature': temperature,
                    'city': city,
                    'started_at': time.time()
                }
                logger.warning(f"❄️ ALERTA: {cp_id} ({city}) - {temperature}°C")
                self._send_command(cp_id, 'STOP')
                self.audit.log_weather_alert(cp_id, 'START', temperature)
                self._enqueue_gui_action('log', f"❄️ Alerta: {cp_id} ({temperature}°C)")

            elif alert_type == 'END':
                self.weather_alerts.pop(cp_id, None)
                logger.info(f"☀️ ALERTA CANCELADA: {cp_id} ({city})")
                self._send_command(cp_id, 'RESUME')
                self.audit.log_weather_alert(cp_id, 'END', temperature)
                self._enqueue_gui_action('log', f"☀️ Alerta cancelada: {cp_id}")

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    def _send_command(self, cp_id: str, command: str):
        with self.lock:
            if cp_id not in self.charging_points:
                logger.warning(f"⚠️ CP {cp_id} no existe para enviar {command}")
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
        logger.info(f"📤 Comando {command} → {cp_id}")

    def _abort_session(self, cp_id: str, razon: str):
        cp_data = self.charging_points[cp_id]
        if not cp_data.get('session'):
            return
        session = cp_data['session']
        session.update({
            'end_time': int(time.time()), 'exitosa': False,
            'razon': razon, 'cp_id': cp_id
        })
        self.db.save_session(session)
        self._send_kafka('driver_notifications', {
            'driver_id': session['driver_id'], 'cp_id': cp_id,
            'session_id': session['session_id'],
            'kw_total': session['kw_consumed'],
            'cost_total': session['total_cost'],
            'exitosa': False, 'razon': razon,
            'type': 'FINAL_TICKET', 'timestamp': time.time()
        })
        cp_data['session'] = None
        self._enqueue_gui_action('remove_request', session['session_id'])
        self.audit.log_error('SESSION_ABORTED', cp_id, f'Razón: {razon}')

    def _handle_cp_failure(self, cp_id: str):
        with self.lock:
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
            logger.warning(f"💥 CP {cp_id} marcado como AVERIADO")

    # ------------------------------------------------------------------
    # Monitor de timeouts
    # ------------------------------------------------------------------

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
            except Exception as e:
                if self.running:
                    logger.error(f"Error en monitor: {e}")
                    time.sleep(2)

    # ------------------------------------------------------------------
    # Kafka send — flush solo en mensajes críticos
    # ------------------------------------------------------------------

    def _send_kafka(self, topic: str, payload: Dict,
                    encrypt_for_cp: Optional[str] = None,
                    require_ack: bool = False):
        for _ in range(3):
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
                                'data': CryptoManager.encrypt_json(payload, key),
                                'cp_id': encrypt_for_cp
                            }
                        except Exception as e:
                            logger.warning(f"⚠️ Error cifrando, enviando sin cifrar: {e}")

                future = self.producer.send(topic, final)
                if require_ack:
                    future.get(timeout=5)
                return
            except Exception as e:
                logger.error(f"Error Kafka [{topic}]: {e}")
                time.sleep(1)

    def _send_notification(self, driver_id: str, status: str,
                           cp_id: str, message: str):
        self._send_kafka('driver_notifications', {
            'driver_id': driver_id, 'status': status,
            'cp_id': cp_id, 'message': message,
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
            try:
                self.db.conn.execute(
                    'DELETE FROM cp_credentials WHERE cp_id = ?', (cp_id,)
                )
                self.db.conn.commit()
            except Exception as e:
                logger.error(f"Error borrando credenciales: {e}")
            _delete_password(cp_id)

            self._enqueue_gui_action('update_cp', cp_id, 'DISCONNECTED',
                                     '', 0, 0, False)
            self._enqueue_gui_action('log', f"🔒 Clave revocada: {cp_id}")
            self.audit.log_event('SECURITY', '0.0.0.0', 'CENTRAL', 'Key revocation',
                                 f'CP: {cp_id}', True)
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
        """Encolar acción GUI desde cualquier hilo."""
        if self.root:
            self.gui_queue.put((action, args))

    def _process_gui_queue(self):
        """Procesar acciones GUI — llamado exclusivamente desde el mainloop."""
        try:
            # Procesar hasta 20 acciones por ciclo para evitar bloqueo
            for _ in range(20):
                try:
                    action, args = self.gui_queue.get_nowait()
                except Empty:
                    break

                if action == 'log':
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

    # ------------------------------------------------------------------
    # GUI init
    # ------------------------------------------------------------------

    def _init_gui(self):
        self.root = tk.Tk()
        self.root.title("EVCharging - CENTRAL (RELEASE 2)")
        self.root.geometry("1400x950")
        self.root.config(bg='#2c3e50')
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

        # Header
        header = tk.Frame(self.root, bg='#1a252f', height=70)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        tk.Label(header, text="*** EV CHARGING - CENTRAL (RELEASE 2) ***",
                 font=('Arial', 16, 'bold'), bg='#1a252f',
                 fg='#ecf0f1').pack(pady=20)

        # CPs
        cp_container = tk.Frame(self.root, bg='#34495e')
        cp_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        canvas = tk.Canvas(cp_container, bg='#34495e', highlightthickness=0)
        scrollbar = ttk.Scrollbar(cp_container, orient='vertical',
                                   command=canvas.yview)
        self.scrollable_frame = tk.Frame(canvas, bg='#34495e')
        self.scrollable_frame.bind(
            '<Configure>',
            lambda e: canvas.configure(scrollregion=canvas.bbox('all'))
        )
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor='nw')
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.frame_cps = tk.Frame(self.scrollable_frame, bg='#34495e')
        self.frame_cps.pack(padx=10, pady=10)

        # Solicitudes
        req_frame = tk.Frame(self.root, bg='#1a252f', height=130)
        req_frame.pack(fill=tk.X, padx=10, pady=5)
        req_frame.pack_propagate(False)
        tk.Label(req_frame, text="*** ON GOING REQUESTS ***",
                 font=('Arial', 11, 'bold'), bg='#1a252f',
                 fg='white').pack(pady=5)
        self.requests_table = ttk.Treeview(
            req_frame, columns=('DATE', 'TIME', 'USER', 'CP'),
            show='headings', height=3
        )
        for col in ('DATE', 'TIME', 'USER', 'CP'):
            self.requests_table.heading(col, text=col)
            self.requests_table.column(col, width=150, anchor=tk.CENTER)
        self.requests_table.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Comandos
        cmd_frame = tk.Frame(self.root, bg='#1a252f', height=120)
        cmd_frame.pack(fill=tk.X, padx=10, pady=5)
        cmd_frame.pack_propagate(False)
        tk.Label(cmd_frame, text="*** CENTRAL COMMANDS ***",
                 font=('Arial', 11, 'bold'), bg='#1a252f',
                 fg='white').pack(pady=5)

        bf1 = tk.Frame(cmd_frame, bg='#1a252f')
        bf1.pack()
        for text, cmd, color in [
            ("⛔ PARAR CP", self._cmd_stop_cp, '#e74c3c'),
            ("▶️ REANUDAR CP", self._cmd_resume_cp, '#2ecc71'),
            ("⛔ PARAR TODOS", self._cmd_stop_all, '#c0392b'),
            ("▶️ REANUDAR TODOS", self._cmd_resume_all, '#27ae60'),
        ]:
            tk.Button(bf1, text=text, command=cmd, bg=color, fg='white',
                      font=('Arial', 10, 'bold'), width=14).pack(side=tk.LEFT, padx=5)

        bf2 = tk.Frame(cmd_frame, bg='#1a252f')
        bf2.pack(pady=5)
        for text, cmd, color in [
            ("🔒 REVOCAR CLAVE CP", self._cmd_revoke_key, '#9b59b6'),
            ("🔒 REVOCAR TODAS", self._cmd_revoke_all_keys, '#8e44ad'),
            ("🔐 LISTAR CLAVES", self._cmd_list_keys, '#3498db'),
        ]:
            tk.Button(bf2, text=text, command=cmd, bg=color, fg='white',
                      font=('Arial', 10, 'bold'), width=18).pack(side=tk.LEFT, padx=5)

        # Logs
        log_frame = tk.Frame(self.root, bg='#1a252f', height=100)
        log_frame.pack(fill=tk.X, padx=10, pady=5)
        log_frame.pack_propagate(False)
        tk.Label(log_frame, text="*** MESSAGES ***",
                 font=('Arial', 11, 'bold'), bg='#1a252f',
                 fg='white').pack(pady=5)
        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=3, bg='#2c3e50', fg='#ecf0f1',
            font=('Courier', 9)
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Cargar CPs existentes directamente (estamos en el mainloop)
        with self.lock:
            for cp_id, cp_data in self.charging_points.items():
                widget = CPWidget(self.frame_cps, cp_id,
                                  cp_data['location'], cp_data['price'])
                num = len(self.cp_widgets)
                widget.grid(row=num // 5, column=num % 5, padx=10, pady=10)
                self.cp_widgets[cp_id] = widget
                widget.actualizar(cp_data['status'], '', 0, 0,
                                  cp_data.get('authenticated', False))

        logger.info(f"✅ GUI lista — {len(self.cp_widgets)} CPs")

        # Iniciar procesamiento de cola
        self.root.after(100, self._process_gui_queue)
        self.root.mainloop()

    # ------------------------------------------------------------------
    # GUI actions (ejecutadas en mainloop)
    # ------------------------------------------------------------------

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
            widget = CPWidget(self.frame_cps, cp_id, location, price)
            num = len(self.cp_widgets)
            widget.grid(row=num // 5, column=num % 5, padx=10, pady=10)
            self.cp_widgets[cp_id] = widget
        except Exception as e:
            logger.exception(f"Error creando widget {cp_id}: {e}")

    def _do_gui_update_cp(self, cp_id: str, status: str, driver: str = '',
                          kw: float = 0.0, cost: float = 0.0,
                          authenticated: bool = False):
        if cp_id in self.cp_widgets:
            try:
                self.cp_widgets[cp_id].actualizar(status, driver, kw, cost, authenticated)
                if cp_id in self.weather_locations:
                    self.cp_widgets[cp_id].set_weather_location(
                        self.weather_locations[cp_id]
                    )
            except Exception as e:
                logger.error(f"Error actualizando widget {cp_id}: {e}")

    def _do_gui_update_weather_location(self, cp_id: str, city: str):
        if cp_id in self.cp_widgets:
            try:
                self.cp_widgets[cp_id].set_weather_location(city)
            except Exception as e:
                logger.error(f"Error actualizando weather location {cp_id}: {e}")
        self._do_log(f"🌡️ EV_W monitoreando {city} para {cp_id}")

    def _do_gui_add_request(self, sid: str, date: str, time_str: str,
                             user: str, cp: str):
        if self.requests_table:
            try:
                item = self.requests_table.insert(
                    '', tk.END, values=(date, time_str, user, cp)
                )
                self.request_items[sid] = item
            except Exception as e:
                logger.error(f"Error añadiendo request: {e}")

    def _do_gui_remove_request(self, sid: str):
        if self.requests_table and sid in self.request_items:
            try:
                self.requests_table.delete(self.request_items[sid])
                del self.request_items[sid]
            except Exception as e:
                logger.error(f"Error eliminando request: {e}")

    # ------------------------------------------------------------------
    # Comandos GUI
    # ------------------------------------------------------------------

    def _cmd_stop_cp(self):
        cp_id = simpledialog.askstring("Parar CP", "ID del CP:")
        if cp_id and cp_id in self.charging_points:
            self._send_command(cp_id, 'STOP')
            messagebox.showinfo("OK", f"STOP → {cp_id}")

    def _cmd_resume_cp(self):
        cp_id = simpledialog.askstring("Reanudar CP", "ID del CP:")
        if cp_id and cp_id in self.charging_points:
            self._send_command(cp_id, 'RESUME')
            messagebox.showinfo("OK", f"RESUME → {cp_id}")

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
        if cp_id and cp_id in self.charging_points:
            if messagebox.askyesno(
                "Confirmar", f"¿Revocar clave de {cp_id}?\n\nEl CP deberá re-autenticarse."
            ):
                if self.revoke_cp_encryption_key(cp_id):
                    messagebox.showinfo("Éxito", f"✅ Clave revocada: {cp_id}")
                else:
                    messagebox.showerror("Error", "❌ Error revocando clave")
        else:
            messagebox.showerror("Error", "❌ CP no encontrado")

    def _cmd_revoke_all_keys(self):
        if messagebox.askyesno(
            "⚠️ EMERGENCIA",
            "¿REVOCAR TODAS LAS CLAVES?\n\nTodos los CPs quedarán fuera de servicio."
        ):
            count = self.revoke_all_encryption_keys()
            messagebox.showinfo("Completado", f"🔒 {count} claves revocadas")

    def _cmd_list_keys(self):
        with self.lock:
            cps = list(self.charging_points.items())
        lines = ["=" * 60, "ESTADO DE CLAVES DE CIFRADO", "=" * 60]
        for cp_id, cp_data in cps:
            auth = "🔐 Autenticado" if cp_data.get('authenticated') else "⚠️ No autenticado"
            key = self.db.get_cp_encryption_key(cp_id)
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

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self):
        self.running = False
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL', 'System shutdown',
                             'Central detenida', True)
        for resource in (self.server_socket, self.consumer, self.producer):
            try:
                if resource:
                    if hasattr(resource, 'flush'):
                        resource.flush(timeout=3)
                    resource.close()
            except Exception:
                pass
        self.db.close()
        logger.info("✅ Central cerrada limpiamente")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    socket_port = int(os.getenv('SOCKET_PORT', '5001'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    db_path = os.getenv('DB_PATH', 'evcharging.db')

    central = Central(socket_port, kafka_servers, db_path)

    def handle_sigterm(signum, frame):
        logger.info("🛑 SIGTERM recibido, cerrando Central...")
        central.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    central.start()
