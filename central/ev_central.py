"""
EV_CENTRAL - Release 2
Módulo principal del Central de EVCharging
"""
import socket
import threading
import json
import time
import logging
import sqlite3
import tkinter as tk
import sys
import requests
import urllib3
from tkinter import ttk, scrolledtext, messagebox, simpledialog
from datetime import datetime
from typing import Optional, Dict, Any, List
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from queue import Queue
import os
import uuid

# Deshabilitar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Añadir path para security_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager, AuditLogger

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger('Central')


class Database:
    """Base de datos con manejo de concurrencia"""
    def __init__(self, db_path='evcharging.db'):
        self.db_path = db_path
        self.lock = threading.RLock()  # Usar RLock para permitir re-entrada
        
        # Conexión con WAL mode para mejor concurrencia
        self.conn = sqlite3.connect(db_path, check_same_thread=False, 
                                    timeout=30.0, isolation_level=None)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        logger.info("✅ BD inicializada con WAL mode")
    
    def _init_schema(self):
        with self.lock:
            cursor = self.conn.cursor()
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS charging_points (
                cp_id TEXT PRIMARY KEY, 
                location TEXT NOT NULL, 
                price REAL NOT NULL,
                status TEXT DEFAULT 'DISCONNECTED', 
                last_seen INTEGER,
                registered INTEGER DEFAULT 0, 
                authenticated INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now')))''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY, 
                cp_id TEXT NOT NULL, 
                driver_id TEXT NOT NULL,
                start_time INTEGER NOT NULL, 
                end_time INTEGER, 
                kw_consumed REAL DEFAULT 0,
                total_cost REAL DEFAULT 0, 
                exitosa INTEGER DEFAULT 1, 
                razon TEXT)''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS cp_credentials (
                cp_id TEXT PRIMARY KEY,
                registry_token TEXT,
                encryption_key TEXT NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now')))''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_cp ON sessions(cp_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(end_time) WHERE end_time IS NULL')
    
    def save_cp_credentials(self, cp_id: str, encryption_key: str, registry_token: Optional[str] = None) -> None:
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('''INSERT OR REPLACE INTO cp_credentials 
                    (cp_id, encryption_key, registry_token) VALUES (?, ?, ?)''',
                    (cp_id, encryption_key, registry_token))
            except Exception as e:
                logger.error(f"Error guardando credenciales: {e}")

    def get_cp_encryption_key(self, cp_id: str) -> Optional[str]:
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('SELECT encryption_key FROM cp_credentials WHERE cp_id = ?', (cp_id,))
                row = cursor.fetchone()
                return row['encryption_key'] if row else None
            except Exception as e:
                logger.error(f"Error obteniendo clave: {e}")
                return None
    
    def mark_cp_authenticated(self, cp_id: str):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('UPDATE charging_points SET authenticated = 1 WHERE cp_id = ?', (cp_id,))
            except Exception as e:
                logger.error(f"Error marcando autenticación: {e}")
            
    def save_cp(self, cp_id: str, location: str, price: float):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('''INSERT OR REPLACE INTO charging_points 
                    (cp_id, location, price, last_seen, registered) VALUES (?, ?, ?, ?, 1)''',
                    (cp_id, location, price, int(time.time())))
            except Exception as e:
                logger.error(f"Error guardando CP: {e}")
    
    def get_all_cps(self) -> List[Dict]:
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('SELECT * FROM charging_points')
                return [dict(row) for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Error obteniendo CPs: {e}")
                return []
    
    def update_cp_status(self, cp_id: str, status: str):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('UPDATE charging_points SET status = ?, last_seen = ? WHERE cp_id = ?',
                                (status, int(time.time()), cp_id))
            except Exception as e:
                logger.error(f"Error actualizando: {e}")
    
    def save_session(self, session_data: Dict):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('''INSERT OR REPLACE INTO sessions
                    (session_id, cp_id, driver_id, start_time, end_time, kw_consumed, total_cost, exitosa, razon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (session_data.get('session_id', ''), session_data.get('cp_id', ''),
                     session_data.get('driver_id', ''), session_data.get('start_time', 0),
                     session_data.get('end_time'), session_data.get('kw_consumed', 0),
                     session_data.get('total_cost', 0), 1 if session_data.get('exitosa', True) else 0,
                     session_data.get('razon')))
            except Exception as e:
                logger.error(f"Error guardando sesión: {e}")


class CPWidget(tk.Frame):
    """Widget de CP thread-safe"""
    COLORS = {'AVAILABLE': '#2ecc71', 'CHARGING': '#27ae60', 'STOPPED': '#f39c12',
              'BROKEN': '#e74c3c', 'DISCONNECTED': '#95a5a6'}
    
    def __init__(self, parent, cp_id: str, location: str, price: float):
        super().__init__(parent, relief=tk.RAISED, borderwidth=2, bg='#95a5a6')
        self.cp_id = cp_id
        self._setup_widgets(location, price)
        
    def _setup_widgets(self, location: str, price: float):
        tk.Label(self, text=self.cp_id, font=('Arial', 14, 'bold'), 
                fg='white', bg=self.cget('bg')).pack(pady=5)
        tk.Label(self, text=location, font=('Arial', 9), fg='white', 
                bg=self.cget('bg'), wraplength=200).pack()
        tk.Label(self, text=f"{price}€/kWh", font=('Arial', 10), 
                fg='white', bg=self.cget('bg')).pack(pady=5)
        tk.Label(self, text="─"*30, bg=self.cget('bg'), fg='white').pack()
        
        self.lbl_estado = tk.Label(self, text="DESCONECTADO", 
                                   font=('Arial', 11, 'bold'), 
                                   fg='white', bg=self.cget('bg'))
        self.lbl_estado.pack(pady=10)
        
        self.lbl_auth = tk.Label(self, text="", font=('Arial', 8), 
                                fg='yellow', bg=self.cget('bg'))
        self.lbl_auth.pack()
        
        self.frame_carga = tk.Frame(self, bg=self.cget('bg'))
        self.lbl_driver = tk.Label(self.frame_carga, text="", 
                                   font=('Arial', 9, 'bold'), 
                                   fg='yellow', bg=self.cget('bg'))
        self.lbl_driver.pack()
        self.lbl_consumo = tk.Label(self.frame_carga, text="", 
                                    font=('Arial', 11, 'bold'), 
                                    fg='white', bg=self.cget('bg'))
        self.lbl_consumo.pack(pady=2)
        self.lbl_coste = tk.Label(self.frame_carga, text="", 
                                  font=('Arial', 11, 'bold'), 
                                  fg='white', bg=self.cget('bg'))
        self.lbl_coste.pack()
        
        self.config(width=220, height=300)
        self.pack_propagate(False)
    
    def actualizar(self, status: str, driver_id: str = "", kw: float = 0.0, 
                cost: float = 0.0, authenticated: bool = False):
        """Actualizar widget - CORREGIDO: orden correcto de etiquetas"""
        try:
            color = self.COLORS.get(status, '#95a5a6')
            self.config(bg=color)
            self._update_bg_recursive(self, color)
            
            # Textos de estado
            texto = {
                'AVAILABLE': 'DISPONIBLE', 
                'CHARGING': 'CARGANDO...', 
                'STOPPED': 'FUERA DE SERVICIO',
                'BROKEN': 'AVERIADO', 
                'DISCONNECTED': 'DESCONECTADO'
            }
            
            # CRÍTICO: Mostrar autenticación PRIMERO (arriba)
            if authenticated:
                self.lbl_auth.config(text="🔒 Autenticado")
            else:
                self.lbl_auth.config(text="⚠️ Sin autenticar")
            
            # CRÍTICO: Estado DESPUÉS (abajo)
            if status == 'CHARGING' and driver_id:
                # Durante carga: mostrar datos en tiempo real
                self.lbl_driver.config(text=f"👤 {driver_id}")
                self.lbl_consumo.config(text=f"⚡ {kw:.2f} kWh")
                self.lbl_coste.config(text=f"💶 {cost:.2f} €")
                
                # Ocultar label de estado, mostrar frame de carga
                self.lbl_estado.pack_forget()
                self.frame_carga.pack(pady=5)
            else:
                # No está cargando: mostrar estado
                self.frame_carga.pack_forget()
                self.lbl_estado.config(text=texto.get(status, status))
                self.lbl_estado.pack(pady=10)
            
        except Exception as e:
            logger.error(f"Error actualizando widget {self.cp_id}: {e}")
    
    def _update_bg_recursive(self, widget, color):
        for child in widget.winfo_children():
            if isinstance(child, (tk.Label, tk.Frame)):
                try:
                    child.config(bg=color)
                except:
                    pass
            self._update_bg_recursive(child, color)
            
class Central:
    def __init__(self, socket_port: int = 5001, kafka_servers: str = 'localhost:9092', 
                 db_path: str = 'evcharging.db'):
        self.socket_port = socket_port
        self.kafka_servers = kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        
        # CRÍTICO: Inicializar logger
        self.logger = logging.getLogger('Central')
        
        self.db = Database(db_path)
        self.audit = AuditLogger('audit.log')
        
        self.weather_alerts = {}
        self.charging_points: Dict[str, Dict[str, Any]] = {}
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.pending_commands: Dict[str, str] = {}
        self.lock = threading.RLock()
        
        # Cola para actualizaciones de GUI (thread-safe)
        self.gui_queue = Queue()
        
        self.server_socket: Optional[socket.socket] = None
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True
        
        self.root: Optional[tk.Tk] = None
        self.cp_widgets: Dict[str, CPWidget] = {}
        self.log_text: Optional[scrolledtext.ScrolledText] = None
        self.requests_table: Optional[ttk.Treeview] = None
        self.frame_cps: Optional[tk.Frame] = None
        self.request_items: Dict[str, str] = {}

    def start(self) -> bool:
        self.logger.info("="*60)
        self.logger.info("SISTEMA CENTRAL - VERSIÓN COMPLETA Y CORREGIDA")
        self.logger.info("="*60)
        
        self._load_cps_from_db()
        
        if not self._init_kafka():
            self.logger.error("❌ Error Kafka")
            return False
        
        if not self._init_socket_server():
            self.logger.error("❌ Error Socket")
            return False
        
        threading.Thread(target=self._monitor_connections, daemon=True).start()
        
        self.logger.info("✅ Sistema listo")
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL', 'System startup', 
                            'Central iniciada correctamente', True)
        
        self._init_gui()
        return True
    
    def _load_cps_from_db(self):
        cps = self.db.get_all_cps()
        with self.lock:
            for cp in cps:
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
    
    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                self.logger.info(f"🔄 Kafka ({attempt}/15)...")
                
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5, 
                    request_timeout_ms=30000, 
                    max_block_ms=10000)
                
                self.producer.send('test_topic', {'test': 'connection'}).get(timeout=10)
                
                self.consumer = KafkaConsumer(
                    'service_requests', 'charging_data', 'charging_complete',
                    bootstrap_servers=self.kafka_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest', 
                    group_id='central-group',
                    enable_auto_commit=True, 
                    session_timeout_ms=30000,
                    consumer_timeout_ms=1000)
                
                threading.Thread(target=self._kafka_consumer_loop, daemon=True).start()
                self.logger.info("✅ Kafka OK")
                return True
            except Exception as e:
                self.logger.error(f"❌ Kafka: {e}")
                if attempt < 15:
                    time.sleep(5)
        return False
    
    def _kafka_consumer_loop(self):
        while self.running:
            try:
                if self.consumer is None:
                    time.sleep(5)
                    continue
                
                for msg in self.consumer:
                    if not self.running:
                        break
                    
                    try:
                        if msg.topic == 'service_requests':
                            self._handle_service_request(msg.value)
                        elif msg.topic == 'charging_data':
                            self._handle_charging_data(msg.value)
                        elif msg.topic == 'charging_complete':
                            self._handle_charging_complete(msg.value)
                    except Exception as e:
                        self.logger.error(f"Error procesando mensaje {msg.topic}: {e}")
            except Exception as e:
                if self.running and "timed out" not in str(e).lower():
                    self.logger.error(f"❌ Consumer: {e}")
                    time.sleep(1)
    
    def _init_socket_server(self) -> bool:
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.socket_port))
            self.server_socket.settimeout(1.0)
            self.server_socket.listen(10)
            
            threading.Thread(target=self._accept_monitors, daemon=True).start()
            self.logger.info(f"✅ Socket {self.socket_port}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Socket: {e}")
            return False
    
    def _accept_monitors(self):
        while self.running:
            try:
                if self.server_socket is None:
                    break
                client_socket, address = self.server_socket.accept()
                self.audit.log_event('CONNECTION', address[0], 'MONITOR', 
                                    'Connection attempt', f'From {address}', True)
                threading.Thread(target=self._handle_monitor, 
                               args=(client_socket, address[0]), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    time.sleep(1)
    def _verify_cp_credentials(self, cp_id: str, password: Optional[str] = None) -> bool:
        """Verificar credenciales del CP contra Registry"""
        registry_url = os.getenv('REGISTRY_URL', 'https://ev_registry:8443')
        
        try:
            if not password:
                password_file = f'/tmp/{cp_id}_verified_password.txt'
                if os.path.exists(password_file):
                    with open(password_file, 'r') as f:
                        password = f.read().strip()
                else:
                    self.logger.warning(f"⚠️ No password para {cp_id}")
                    return False
            
            verify_url = f"{registry_url}/api/v1/authenticate"
            payload = {'cp_id': cp_id, 'password': password}
            
            response = requests.post(verify_url, json=payload, verify=False, timeout=10)
            
            if response.status_code == 200:
                try:
                    with open(f'/tmp/{cp_id}_verified_password.txt', 'w') as f:
                        f.write(password)
                except:
                    pass
                
                self.logger.info(f"✅ Credenciales verificadas: {cp_id}")
                self.audit.log_authentication(cp_id, '0.0.0.0', True, 'PASSWORD_REGISTRY')
                return True
            else:
                self.logger.warning(f"⚠️ Credenciales inválidas: {cp_id}")
                self.audit.log_authentication(cp_id, '0.0.0.0', False, 'PASSWORD_REGISTRY')
                return False
        
        except Exception as e:
            self.logger.warning(f"⚠️ No se pudo verificar con Registry: {e}")
            return True  # Modo degradado
    
    def _handle_monitor(self, sock: socket.socket, client_ip: str):
        """Handler de Monitor con verificación de credenciales"""
        cp_id: Optional[str] = None
        
        try:
            sock.settimeout(10)
            data = sock.recv(1024).decode('utf-8').strip()
            
            if data.startswith('REGISTER'):
                parts = data.split('|')
                if len(parts) >= 4:
                    _, cp_id, location, price = parts[:4]
                    password = parts[4] if len(parts) > 4 else None
                    price = float(price)
                    
                    # VERIFICAR CREDENCIALES
                    if not self._verify_cp_credentials(cp_id, password):
                        response = 'ERROR|INVALID_CREDENTIALS|CP no autorizado por Registry'
                        sock.send(response.encode('utf-8'))
                        self.logger.warning(f"🚫 Autenticación rechazada: {cp_id}")
                        self._enqueue_gui_action('log', f"🚫 {cp_id} rechazado")
                        self.audit.log_authentication(cp_id, client_ip, False, 'PASSWORD_INVALID')
                        return
                    
                    with self.lock:
                        if cp_id not in self.charging_points:
                            self.charging_points[cp_id] = {
                                'location': location, 'price': price, 'status': 'AVAILABLE',
                                'socket': sock, 'session': None, 'last_seen': time.time(),
                                'monitor_alive': True, 'engine_alive': False, 
                                'consecutive_failures': 0, 'authenticated': False
                            }
                            self.db.save_cp(cp_id, location, price)
                            self.logger.info(f"✅ {cp_id} NUEVO y VERIFICADO")
                            self._enqueue_gui_action('log', f"✅ {cp_id} registrado")
                            self._enqueue_gui_action('add_cp', cp_id, location, price)
                        else:
                            cp_data = self.charging_points[cp_id]
                            if cp_data.get('session'):
                                self._abort_session(cp_id, 'CP reconectado')
                            
                            cp_data.update({
                                'socket': sock, 'status': 'AVAILABLE', 'last_seen': time.time(),
                                'monitor_alive': True, 'engine_alive': False, 'consecutive_failures': 0
                            })
                            self.logger.info(f"🔄 {cp_id} RECONECTADO")
                    
                    # Generar/recuperar encryption key
                    encryption_key = self.db.get_cp_encryption_key(cp_id)
                    if not encryption_key:
                        encryption_key = CryptoManager.generate_key()
                        self.db.save_cp_credentials(cp_id, encryption_key)
                        self.logger.info(f"🔑 Nueva clave: {cp_id}")
                    
                    self.db.mark_cp_authenticated(cp_id)
                    with self.lock:
                        self.charging_points[cp_id]['authenticated'] = True
                    
                    response = f'OK|REGISTERED|{encryption_key}'
                    sock.send(response.encode('utf-8'))
                    
                    self.audit.log_authentication(cp_id, client_ip, True, 'FULL_AUTH')
                    self._enqueue_gui_action('log', f"🔐 {cp_id} autenticado")
                    self._enqueue_gui_action('update_cp', cp_id, 'AVAILABLE', '', 0, 0, True)
                    self.db.update_cp_status(cp_id, 'AVAILABLE')
                    
                    self._monitor_health_loop(cp_id, sock)
        
        except Exception as e:
            self.logger.error(f"❌ Monitor {cp_id}: {e}")
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
                        cp_data['status'] = 'DISCONNECTED'
                        cp_data['socket'] = None
                        cp_data['monitor_alive'] = False
                        cp_data['engine_alive'] = False
                        cp_data['authenticated'] = False
                        
                        self._enqueue_gui_action('update_cp', cp_id, 'DISCONNECTED', '', 0, 0, False)
                        self.db.update_cp_status(cp_id, 'DISCONNECTED')
                        self._enqueue_gui_action('log', f"❌ {cp_id} desconectado")
                        self.audit.log_cp_status_change(cp_id, old_status, 'DISCONNECTED', 'Monitor disconnected')
            try:
                sock.close()
            except:
                pass
    
    def _monitor_health_loop(self, cp_id: str, sock: socket.socket):
        """Loop de health checks - CORREGIDO: detección robusta de averías"""
        sock.settimeout(15)
        last_health_ok = time.time()
        last_failure_log = 0  # Para evitar spam de logs
        
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
                    
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive'] = True
                            cp_data['consecutive_failures'] = 0
                            
                            # CRÍTICO: Recuperar de BROKEN solo UNA VEZ
                            if cp_data['status'] == 'BROKEN':
                                # Aplicar comando pendiente si existe
                                if cp_id in self.pending_commands:
                                    cmd = self.pending_commands.pop(cp_id)
                                    self._send_kafka('central_commands', 
                                                {'cp_id': cp_id, 'command': cmd, 
                                                    'timestamp': time.time()}, 
                                                encrypt_for_cp=cp_id)
                                    cp_data['status'] = 'STOPPED' if cmd == 'STOP' else 'AVAILABLE'
                                else:
                                    cp_data['status'] = 'AVAILABLE'
                                
                                self._enqueue_gui_action('update_cp', cp_id, cp_data['status'], 
                                                        '', 0, 0, cp_data.get('authenticated', False))
                                self.db.update_cp_status(cp_id, cp_data['status'])
                                self._enqueue_gui_action('log', f"✅ {cp_id} recuperado de avería")
                                self.logger.info(f"✅ {cp_id} recuperado de BROKEN")
                
                elif msg == 'HEALTH_FAIL':
                    current_time = time.time()
                    
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive'] = False
                            cp_data['consecutive_failures'] += 1
                            
                            # CRÍTICO: Solo marcar como BROKEN al alcanzar 3 fallos
                            # Y solo procesarlo UNA VEZ
                            if (cp_data['consecutive_failures'] >= 3 and 
                                cp_data['monitor_alive'] and 
                                cp_data['status'] != 'BROKEN'):
                                
                                # Log solo si han pasado 5s desde último log (evitar spam)
                                if current_time - last_failure_log > 5:
                                    self.logger.warning(
                                        f"⚠️ {cp_id}: {cp_data['consecutive_failures']} fallos consecutivos"
                                    )
                                    last_failure_log = current_time
                                
                                # Marcar como averiado
                                self._handle_cp_failure(cp_id)
            
            except socket.timeout:
                if time.time() - last_health_ok > 20:
                    self.logger.warning(f"⏰ Timeout health check: {cp_id}")
                    break
                continue
            
            except Exception as e:
                if self.running:
                    self.logger.error(f"❌ Health loop error {cp_id}: {e}")
                break
            
    def _decrypt_message(self, data: Dict[str, Any], cp_id: str) -> Optional[Dict[str, Any]]:
        """Descifrar mensaje si está cifrado"""
        if not isinstance(data, dict):
            return data
            
        if data.get('encrypted') and data.get('data'):
            encryption_key = self.db.get_cp_encryption_key(cp_id)
            if encryption_key:
                try:
                    decrypted = CryptoManager.decrypt_json(data['data'], encryption_key)
                    return decrypted
                except Exception as e:
                    self.logger.error(f"Error descifrando de {cp_id}: {e}")
                    return None
            else:
                self.logger.error(f"No hay clave para {cp_id}")
                return None
        return data
    
    def _handle_service_request(self, data: Dict[str, Any]):
        """Handler de solicitudes de servicio"""
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
                razon = {'DISCONNECTED': 'CP desconectado', 'BROKEN': 'CP averiado',
                        'STOPPED': 'CP fuera de servicio', 'CHARGING': 'CP ocupado'}
                self._send_notification(driver_id, 'DENIED', cp_id, 
                                       razon.get(cp['status'], 'No disponible'))
                self.audit.log_service_auth(driver_id, cp_id, False)
                return
            
            session_id = f"SESSION_{cp_id}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
            
            cp['status'] = 'CHARGING'
            cp['session'] = {
                'session_id': session_id, 'driver_id': driver_id,
                'start_time': int(time.time()), 'kw_consumed': 0.0, 'total_cost': 0.0
            }
            
            self.sessions[session_id] = cp['session'].copy()
            self.sessions[session_id]['cp_id'] = cp_id
            
            self._send_kafka('service_authorizations', {
                'cp_id': cp_id, 'driver_id': driver_id, 'session_id': session_id,
                'price': cp['price'], 'timestamp': time.time()
            }, encrypt_for_cp=cp_id)
            
            self._send_notification(driver_id, 'AUTHORIZED', cp_id, 'Autorizado')
            self.audit.log_service_auth(driver_id, cp_id, True)
            
            self._enqueue_gui_action('update_cp', cp_id, 'CHARGING', driver_id, 0.0, 0.0, 
                                    cp.get('authenticated', False))
            self._enqueue_gui_action('add_request', session_id, 
                                    datetime.now().strftime("%d/%m/%y"),
                                    datetime.now().strftime("%H:%M"), driver_id, cp_id)
            
            self.db.update_cp_status(cp_id, 'CHARGING')
            self._enqueue_gui_action('log', f"✅ {driver_id} en {cp_id}")
    
    def _handle_charging_data(self, data: Dict[str, Any]):
        """Handler de datos de carga - CORREGIDO para actualizar BD en tiempo real"""
        cp_id = data.get('cp_id', '')
        
        if data.get('encrypted'):
            decrypted_data = self._decrypt_message(data, cp_id)
            if decrypted_data is None:
                return
            data = decrypted_data
        
        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                if cp.get('session'):
                    # Actualizar sesión en memoria
                    cp['session']['kw_consumed'] = data.get('kw', 0.0)
                    cp['session']['total_cost'] = data.get('cost', 0.0)
                    
                    # CRÍTICO: Actualizar también en base de datos
                    try:
                        session_id = cp['session'].get('session_id')
                        if session_id:
                            cursor = self.db.conn.cursor()
                            cursor.execute('''UPDATE sessions 
                                            SET kw_consumed = ?, total_cost = ? 
                                            WHERE session_id = ?''',
                                        (data.get('kw', 0.0), data.get('cost', 0.0), session_id))
                            self.db.conn.commit()
                    except Exception as e:
                        self.logger.error(f"Error actualizando sesión en BD: {e}")
                    
                    # Actualizar GUI con progreso
                    self._enqueue_gui_action('update_cp', cp_id, 'CHARGING', 
                                            data.get('driver_id', ''),
                                            data.get('kw', 0.0), data.get('cost', 0.0),
                                            cp.get('authenticated', False))
                    
    def _handle_charging_complete(self, data: Dict[str, Any]):
        """Handler de finalización de carga - CORREGIDO para limpiar correctamente"""
        cp_id = data.get('cp_id', '')
        
        if data.get('encrypted'):
            decrypted_data = self._decrypt_message(data, cp_id)
            if decrypted_data is None:
                return
            data = decrypted_data
        
        session_id = data.get('session_id', '')
        driver_id = data.get('driver_id', '')
        exitosa = data.get('exitosa', True)
        razon = data.get('razon', '')
        
        self.logger.info(f"📋 Finalizando sesión {session_id}: {cp_id}")
        
        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                
                # Guardar sesión completa
                if cp.get('session'):
                    session = cp['session']
                    session.update({
                        'end_time': int(time.time()), 
                        'cp_id': cp_id,
                        'kw_consumed': data.get('kw_total', session.get('kw_consumed', 0)),
                        'total_cost': data.get('cost_total', session.get('total_cost', 0)),
                        'exitosa': exitosa, 
                        'razon': razon
                    })
                    self.db.save_session(session)
                    self.logger.info(f"✅ Sesión guardada: {session_id}")
                
                # Limpiar sesión del CP
                cp['session'] = None
                
                # Actualizar estado del CP
                if cp['status'] in ['CHARGING', 'AVAILABLE']:
                    # No cambiar si está STOPPED o BROKEN
                    if cp['status'] == 'CHARGING':
                        cp['status'] = 'AVAILABLE'
                        self.db.update_cp_status(cp_id, 'AVAILABLE')
                
                # Actualizar GUI
                self._enqueue_gui_action('update_cp', cp_id, cp['status'], '', 0, 0, 
                                        cp.get('authenticated', False))
            
            # Limpiar de ongoing sessions
            if session_id in self.sessions:
                del self.sessions[session_id]
                # CRÍTICO: Eliminar de la tabla visual
                self._enqueue_gui_action('remove_request', session_id)
                self.logger.info(f"🗑️ Sesión {session_id} eliminada de ongoing")
        
        # Enviar ticket final al driver
        self._send_kafka('driver_notifications', {
            'driver_id': driver_id, 
            'cp_id': cp_id, 
            'session_id': session_id,
            'kw_total': data.get('kw_total', 0), 
            'cost_total': data.get('cost_total', 0),
            'exitosa': exitosa, 
            'razon': razon, 
            'type': 'FINAL_TICKET', 
            'timestamp': time.time()
        })
        
        self.logger.info(f"✅ Carga completada: {session_id}")
    
    def revoke_cp_encryption_key(self, cp_id: str):
        """Revocar encryption key de un CP"""
        with self.lock:
            if cp_id not in self.charging_points:
                self.logger.error(f"❌ CP {cp_id} no encontrado")
                return False
            
            cp_data = self.charging_points[cp_id]
            
            if cp_data.get('session'):
                self._abort_session(cp_id, 'Clave de cifrado revocada')
            
            if cp_data.get('socket'):
                try:
                    cp_data['socket'].close()
                except:
                    pass
                cp_data['socket'] = None
            
            old_status = cp_data['status']
            cp_data['status'] = 'DISCONNECTED'
            cp_data['authenticated'] = False
            cp_data['monitor_alive'] = False
            cp_data['engine_alive'] = False
            
            try:
                self.db.conn.execute('DELETE FROM cp_credentials WHERE cp_id = ?', (cp_id,))
                self.db.conn.commit()
            except Exception as e:
                self.logger.error(f"Error borrando credenciales: {e}")
            
            try:
                password_file = f'/tmp/{cp_id}_verified_password.txt'
                if os.path.exists(password_file):
                    os.remove(password_file)
            except:
                pass
            
            self._enqueue_gui_action('update_cp', cp_id, 'DISCONNECTED', '', 0, 0, False)
            self._enqueue_gui_action('log', f"🔒 Clave revocada: {cp_id}")
            
            self.audit.log_event('SECURITY', '0.0.0.0', 'CENTRAL', 'Key revocation',
                                f'CP: {cp_id}', True)
            self.audit.log_cp_status_change(cp_id, old_status, 'DISCONNECTED', 'Encryption key revoked')
            
            self.logger.info(f"🔒 Clave revocada: {cp_id}")
            return True
    
    def revoke_all_encryption_keys(self):
        """Revocar todas las claves (emergencia)"""
        with self.lock:
            cp_ids = list(self.charging_points.keys())
        
        count = 0
        for cp_id in cp_ids:
            if self.revoke_cp_encryption_key(cp_id):
                count += 1
        
        self._enqueue_gui_action('log', f"🔒 Revocadas {count} claves")
        self.logger.warning(f"🔒 EMERGENCIA: {count} claves revocadas")
        return count
    
    def handle_weather_alert(self, cp_id: str, alert_type: str, temperature: float, city: str):
        """Manejar alerta climática"""
        with self.lock:
            if alert_type == 'START':
                self.weather_alerts[cp_id] = {
                    'temperature': temperature, 'city': city, 'started_at': time.time()
                }
                self.logger.warning(f"❄️ ALERTA: {cp_id} ({city}) - {temperature}°C")
                self._send_command(cp_id, 'STOP')
                self.audit.log_weather_alert(cp_id, 'START', temperature)
                self._enqueue_gui_action('log', f"❄️ Alerta: {cp_id} ({temperature}°C)")
            
            elif alert_type == 'END':
                if cp_id in self.weather_alerts:
                    del self.weather_alerts[cp_id]
                self.logger.info(f"☀️ ALERTA CANCELADA: {cp_id}")
                self._send_command(cp_id, 'RESUME')
                self.audit.log_weather_alert(cp_id, 'END', temperature)
                self._enqueue_gui_action('log', f"☀️ Alerta cancelada: {cp_id}")
    
    def _send_command(self, cp_id: str, command: str):
        """Enviar comando a CP - CORREGIDO para notificar estado"""
        with self.lock:
            if cp_id not in self.charging_points:
                self.logger.warning(f"⚠️ CP {cp_id} no existe")
                return
            
            cp_data = self.charging_points[cp_id]
            self.pending_commands[cp_id] = command
            
            if command == 'STOP':
                # Finalizar sesión activa si existe
                if cp_data.get('session'):
                    self._abort_session(cp_id, 'Detenido por Central (comando STOP)')
                
                # Cambiar estado
                old_status = cp_data['status']
                cp_data['status'] = 'STOPPED'
                
                # CRÍTICO: Actualizar GUI con estado STOPPED
                self._enqueue_gui_action('update_cp', cp_id, 'STOPPED', '', 0, 0, 
                                        cp_data.get('authenticated', False))
                self._enqueue_gui_action('log', f"⛔ {cp_id} PARADO por Central")
                
                self.db.update_cp_status(cp_id, 'STOPPED')
                self.audit.log_cp_status_change(cp_id, old_status, 'STOPPED', f'Command: {command}')
                
            elif command == 'RESUME':
                old_status = cp_data['status']
                cp_data['status'] = 'AVAILABLE'
                
                # CRÍTICO: Actualizar GUI con estado AVAILABLE
                self._enqueue_gui_action('update_cp', cp_id, 'AVAILABLE', '', 0, 0, 
                                        cp_data.get('authenticated', False))
                self._enqueue_gui_action('log', f"▶️ {cp_id} REANUDADO por Central")
                
                self.db.update_cp_status(cp_id, 'AVAILABLE')
                self.audit.log_cp_status_change(cp_id, old_status, 'AVAILABLE', f'Command: {command}')
        
        # Enviar comando por Kafka
        self._send_kafka('central_commands', 
                        {'cp_id': cp_id, 'command': command, 'timestamp': time.time()},
                        encrypt_for_cp=cp_id)
        
        self.audit.log_command(cp_id, command)
        self.logger.info(f"📤 Comando {command} enviado a {cp_id}")
    
    def _abort_session(self, cp_id: str, razon: str):
        """Abortar sesión"""
        cp_data = self.charging_points[cp_id]
        if not cp_data.get('session'):
            return
        
        session = cp_data['session']
        session['end_time'] = int(time.time())
        session['exitosa'] = False
        session['razon'] = razon
        session['cp_id'] = cp_id
        self.db.save_session(session)
        
        self._send_kafka('driver_notifications', {
            'driver_id': session['driver_id'], 'cp_id': cp_id,
            'session_id': session['session_id'],
            'kw_total': session['kw_consumed'], 'cost_total': session['total_cost'],
            'exitosa': False, 'razon': razon, 'type': 'FINAL_TICKET',
            'timestamp': time.time()
        })
        
        cp_data['session'] = None
        self._enqueue_gui_action('remove_request', session['session_id'])
        self.audit.log_error('SESSION_ABORTED', cp_id, f'Razón: {razon}')
    
    def _handle_cp_failure(self, cp_id: str):
        """Manejar fallo de CP - CORREGIDO para evitar spam de avisos"""
        with self.lock:
            if cp_id not in self.charging_points:
                return
            
            cp = self.charging_points[cp_id]
            if not cp.get('monitor_alive'):
                return
            
            # CRÍTICO: Solo procesar si no está ya en estado BROKEN
            if cp['status'] == 'BROKEN':
                return  # Ya está marcado como averiado, no hacer nada más
            
            old_status = cp['status']
            cp['status'] = 'BROKEN'
            cp['engine_alive'] = False
            
            # Abortar sesión si existe
            if cp.get('session'):
                self._abort_session(cp_id, 'Avería del Engine')
            
            # Actualizar GUI UNA SOLA VEZ
            self._enqueue_gui_action('update_cp', cp_id, 'BROKEN', '', 0, 0, 
                                    cp.get('authenticated', False))
            self.db.update_cp_status(cp_id, 'BROKEN')
            self._enqueue_gui_action('log', f"💥 {cp_id} AVERIADO")
            
            # Auditar UNA SOLA VEZ
            self.audit.log_cp_status_change(cp_id, old_status, 'BROKEN', 
                                        'Consecutive Engine failures')
            
            self.logger.warning(f"💥 CP {cp_id} marcado como AVERIADO")
    
    def _monitor_connections(self):
        """Monitor de timeouts"""
        while self.running:
            try:
                current = time.time()
                with self.lock:
                    for cp_id, cp_data in list(self.charging_points.items()):
                        if cp_data and (current - cp_data.get('last_seen', 0)) > 15:
                            if cp_data['status'] != 'DISCONNECTED':
                                if cp_data.get('session'):
                                    self._abort_session(cp_id, 'Timeout')
                                
                                old_status = cp_data['status']
                                cp_data['status'] = 'DISCONNECTED'
                                cp_data['socket'] = None
                                cp_data['monitor_alive'] = False
                                cp_data['engine_alive'] = False
                                cp_data['authenticated'] = False
                                
                                self._enqueue_gui_action('update_cp', cp_id, 'DISCONNECTED', 
                                                        '', 0, 0, False)
                                self.db.update_cp_status(cp_id, 'DISCONNECTED')
                                self._enqueue_gui_action('log', f"❌ {cp_id} TIMEOUT")
                                
                                self.audit.log_cp_status_change(cp_id, old_status, 
                                                                'DISCONNECTED', 'Connection timeout')
                
                time.sleep(2)
            except Exception as e:
                if self.running:
                    self.logger.error(f"Error en monitor: {e}")
                    time.sleep(2)
    
    def _send_kafka(self, topic: str, payload: Dict[str, Any], encrypt_for_cp: Optional[str] = None):
        """Enviar mensaje a Kafka"""
        for _ in range(3):
            try:
                if self.producer:
                    final_payload = payload
                    
                    if encrypt_for_cp and encrypt_for_cp in self.charging_points:
                        encryption_key = self.db.get_cp_encryption_key(encrypt_for_cp)
                        if encryption_key:
                            try:
                                encrypted_data = CryptoManager.encrypt_json(payload, encryption_key)
                                final_payload = {
                                    'encrypted': True, 'data': encrypted_data, 'cp_id': encrypt_for_cp
                                }
                            except Exception as e:
                                self.logger.error(f"Error cifrando: {e}")
                    
                    self.producer.send(topic, final_payload)
                    self.producer.flush(timeout=5)
                return
            except Exception as e:
                self.logger.error(f"Error Kafka: {e}")
                time.sleep(1)
    
    def _send_notification(self, driver_id: str, status: str, cp_id: str, message: str):
        """Enviar notificación a driver"""
        self._send_kafka('driver_notifications', {
            'driver_id': driver_id, 'status': status, 'cp_id': cp_id,
            'message': message, 'timestamp': time.time()
        })
    def _enqueue_gui_action(self, action: str, *args):
        """Encolar acción para GUI (thread-safe)"""
        if self.root:
            self.gui_queue.put((action, args))
    
    def _process_gui_queue(self):
        """Procesar acciones de GUI desde la cola"""
        try:
            while True:
                try:
                    action, args = self.gui_queue.get_nowait()
                    
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
                except:
                    break
        finally:
            if self.root and self.running:
                self.root.after(100, self._process_gui_queue)
    
    def _init_gui(self):
        """Inicializar GUI completa"""
        self.root = tk.Tk()
        self.root.title("EVCharging - CENTRAL (RELEASE 2 - COMPLETO)")
        self.root.geometry("1400x950")
        self.root.config(bg='#2c3e50')
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # HEADER
        header = tk.Frame(self.root, bg='#1a252f', height=70)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        tk.Label(header, text="*** EV CHARGING - CENTRAL (RELEASE 2 - COMPLETO) ***",
                font=('Arial', 16, 'bold'), bg='#1a252f', fg='#ecf0f1').pack(pady=20)
        
        # CPs
        cp_container = tk.Frame(self.root, bg='#34495e')
        cp_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        canvas = tk.Canvas(cp_container, bg='#34495e', highlightthickness=0)
        scrollbar = ttk.Scrollbar(cp_container, orient="vertical", command=canvas.yview)
        self.scrollable_frame = tk.Frame(canvas, bg='#34495e')
        self.scrollable_frame.bind("<Configure>", 
                                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.frame_cps = tk.Frame(self.scrollable_frame, bg='#34495e')
        self.frame_cps.pack(padx=10, pady=10)
        
        # SOLICITUDES
        req_frame = tk.Frame(self.root, bg='#1a252f', height=130)
        req_frame.pack(fill=tk.X, padx=10, pady=5)
        req_frame.pack_propagate(False)
        tk.Label(req_frame, text="*** ON GOING REQUESTS ***", 
                font=('Arial', 11, 'bold'), bg='#1a252f', fg='white').pack(pady=5)
        
        self.requests_table = ttk.Treeview(req_frame, 
                                          columns=("DATE", "TIME", "USER", "CP"),
                                          show='headings', height=3)
        for col in ["DATE", "TIME", "USER", "CP"]:
            self.requests_table.heading(col, text=col)
            self.requests_table.column(col, width=150, anchor=tk.CENTER)
        self.requests_table.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # COMANDOS (CON SEGURIDAD)
        cmd_frame = tk.Frame(self.root, bg='#1a252f', height=120)
        cmd_frame.pack(fill=tk.X, padx=10, pady=5)
        cmd_frame.pack_propagate(False)
        tk.Label(cmd_frame, text="*** CENTRAL COMMANDS ***", 
                font=('Arial', 11, 'bold'), bg='#1a252f', fg='white').pack(pady=5)
        
        # Primera fila
        btn_frame1 = tk.Frame(cmd_frame, bg='#1a252f')
        btn_frame1.pack()
        tk.Button(btn_frame1, text="⛔ PARAR CP", command=self._cmd_stop_cp, 
                 bg='#e74c3c', fg='white', font=('Arial', 10, 'bold'), 
                 width=14).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame1, text="▶️ REANUDAR CP", command=self._cmd_resume_cp, 
                 bg='#2ecc71', fg='white', font=('Arial', 10, 'bold'), 
                 width=14).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame1, text="⛔ PARAR TODOS", command=self._cmd_stop_all, 
                 bg='#c0392b', fg='white', font=('Arial', 10, 'bold'), 
                 width=14).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame1, text="▶️ REANUDAR TODOS", command=self._cmd_resume_all, 
                 bg='#27ae60', fg='white', font=('Arial', 10, 'bold'), 
                 width=14).pack(side=tk.LEFT, padx=5)
        
        # Segunda fila (SEGURIDAD)
        btn_frame2 = tk.Frame(cmd_frame, bg='#1a252f')
        btn_frame2.pack(pady=5)
        tk.Button(btn_frame2, text="🔒 REVOCAR CLAVE CP", command=self._cmd_revoke_key, 
                 bg='#9b59b6', fg='white', font=('Arial', 10, 'bold'), 
                 width=18).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame2, text="🔒 REVOCAR TODAS", command=self._cmd_revoke_all_keys, 
                 bg='#8e44ad', fg='white', font=('Arial', 10, 'bold'), 
                 width=18).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame2, text="🔐 LISTAR CLAVES", command=self._cmd_list_keys, 
                 bg='#3498db', fg='white', font=('Arial', 10, 'bold'), 
                 width=18).pack(side=tk.LEFT, padx=5)
        
        # LOGS
        log_frame = tk.Frame(self.root, bg='#1a252f', height=100)
        log_frame.pack(fill=tk.X, padx=10, pady=5)
        log_frame.pack_propagate(False)
        tk.Label(log_frame, text="*** MESSAGES ***", 
                font=('Arial', 11, 'bold'), bg='#1a252f', fg='white').pack(pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=3, bg='#2c3e50',
                                                  fg='#ecf0f1', font=('Courier', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Cargar CPs existentes
        with self.lock:
            for cp_id, cp_data in self.charging_points.items():
                self._do_gui_add_cp(cp_id, cp_data['location'], cp_data['price'])
                self._do_gui_update_cp(cp_id, 'DISCONNECTED', '', 0, 0, False)
        
        # Iniciar procesamiento de cola
        self.root.after(100, self._process_gui_queue)
        
        self.root.mainloop()
    
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
        if messagebox.askyesno("Confirmar", "¿Parar TODOS?"):
            with self.lock:
                for cp_id in list(self.charging_points.keys()):
                    self._send_command(cp_id, 'STOP')
    
    def _cmd_resume_all(self):
        if messagebox.askyesno("Confirmar", "¿Reanudar TODOS?"):
            with self.lock:
                for cp_id in list(self.charging_points.keys()):
                    self._send_command(cp_id, 'RESUME')
    
    def _cmd_revoke_key(self):
        """Revocar clave de un CP"""
        cp_id = simpledialog.askstring("Revocar Clave", "ID del CP:")
        if cp_id and cp_id in self.charging_points:
            if messagebox.askyesno("Confirmar", 
                f"¿Revocar clave de {cp_id}?\n\nEl CP debe re-autenticarse."):
                if self.revoke_cp_encryption_key(cp_id):
                    messagebox.showinfo("Éxito", f"✅ Clave revocada: {cp_id}")
                else:
                    messagebox.showerror("Error", f"❌ Error revocando clave")
        else:
            messagebox.showerror("Error", f"❌ CP no encontrado")
    
    def _cmd_revoke_all_keys(self):
        """Revocar todas las claves"""
        if messagebox.askyesno("⚠️ EMERGENCIA", 
            "¿REVOCAR TODAS LAS CLAVES?\n\nTodos los CPs quedarán fuera de servicio."):
            count = self.revoke_all_encryption_keys()
            messagebox.showinfo("Completado", f"🔒 {count} claves revocadas")
    
    def _cmd_list_keys(self):
        """Listar estado de claves"""
        with self.lock:
            cps = list(self.charging_points.items())
        
        info_lines = ["="*60, "ESTADO DE CLAVES DE CIFRADO", "="*60]
        
        for cp_id, cp_data in cps:
            status = "🔐 Autenticado" if cp_data.get('authenticated') else "⚠️ No autenticado"
            encryption_key = self.db.get_cp_encryption_key(cp_id)
            key_status = "✅ Presente" if encryption_key else "❌ Ausente"
            
            info_lines.extend([
                f"\n{cp_id}:",
                f"  Estado:    {status}",
                f"  Clave:     {key_status}"
            ])
            if encryption_key:
                info_lines.append(f"  Key hash:  {encryption_key[:20]}...")
        
        info_lines.append("\n" + "="*60)
        
        dialog = tk.Toplevel(self.root)
        dialog.title("Estado de Claves")
        dialog.geometry("600x500")
        
        text_widget = scrolledtext.ScrolledText(dialog, width=70, height=25, font=('Courier', 9))
        text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text_widget.insert(tk.END, "\n".join(info_lines))
        text_widget.config(state=tk.DISABLED)
        
        tk.Button(dialog, text="Cerrar", command=dialog.destroy,
                 font=('Arial', 10, 'bold')).pack(pady=10)
    
    def _do_log(self, msg: str):
        if self.log_text:
            try:
                self.log_text.insert(tk.END, 
                                    f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
                self.log_text.see(tk.END)
            except:
                pass
    
    def _do_gui_add_cp(self, cp_id: str, location: str, price: float):
        if cp_id in self.cp_widgets or not self.frame_cps:
            return
        try:
            widget = CPWidget(self.frame_cps, cp_id, location, price)
            num = len(self.cp_widgets)
            widget.grid(row=num//5, column=num%5, padx=10, pady=10)
            self.cp_widgets[cp_id] = widget
        except Exception as e:
            self.logger.error(f"Error añadiendo widget {cp_id}: {e}")
    
    def _do_gui_update_cp(self, cp_id: str, status: str, driver: str = "", 
                          kw: float = 0.0, cost: float = 0.0, authenticated: bool = False):
        if cp_id in self.cp_widgets:
            try:
                self.cp_widgets[cp_id].actualizar(status, driver, kw, cost, authenticated)
            except Exception as e:
                self.logger.error(f"Error actualizando widget {cp_id}: {e}")
    
    def _do_gui_add_request(self, sid: str, date: str, time_str: str, user: str, cp: str):
        if self.requests_table:
            try:
                item = self.requests_table.insert("", tk.END, values=(date, time_str, user, cp))
                self.request_items[sid] = item
            except Exception as e:
                self.logger.error(f"Error añadiendo request: {e}")
    
    def _do_gui_remove_request(self, sid: str):
        if self.requests_table and sid in self.request_items:
            try:
                self.requests_table.delete(self.request_items[sid])
                del self.request_items[sid]
            except Exception as e:
                self.logger.error(f"Error eliminando request: {e}")
    
    def _on_closing(self):
        if messagebox.askyesno("Cerrar", "¿Cerrar Central?"):
            self.shutdown()
            if self.root:
                self.root.destroy()
    
    def shutdown(self):
        self.running = False
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL', 'System shutdown', 
                            'Central detenida', True)
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass
        if self.producer:
            try:
                self.producer.close()
            except:
                pass
        
        try:
            self.db.conn.close()
        except:
            pass

# MAIN

if __name__ == '__main__':
    socket_port = int(os.getenv('SOCKET_PORT', '5001'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    db_path = os.getenv('DB_PATH', 'evcharging.db')
    
    central = Central(socket_port, kafka_servers, db_path)
    central.start()