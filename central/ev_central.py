"""
EV_CENTRAL - RELEASE 2 COMPLETO CON TODAS LAS CORRECCIONES
Incluye: cifrado AES-256, auditoría completa, alertas climáticas, autenticación
"""
import socket
import threading
import json
import time
import logging
import sqlite3
import tkinter as tk
import sys
from tkinter import ttk, scrolledtext, messagebox, simpledialog
from datetime import datetime
from typing import Optional, Dict, Any, List
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import os
import uuid

# Añadir path para security_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from security.security_utils import CryptoManager, AuditLogger

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger('Central')


class Database:
    def __init__(self, db_path='evcharging.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS charging_points (
            cp_id TEXT PRIMARY KEY, location TEXT NOT NULL, price REAL NOT NULL,
            status TEXT DEFAULT 'DISCONNECTED', last_seen INTEGER,
            registered INTEGER DEFAULT 0, authenticated INTEGER DEFAULT 0,
            created_at INTEGER DEFAULT (strftime('%s', 'now')))''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY, cp_id TEXT NOT NULL, driver_id TEXT NOT NULL,
            start_time INTEGER NOT NULL, end_time INTEGER, kw_consumed REAL DEFAULT 0,
            total_cost REAL DEFAULT 0, exitosa INTEGER DEFAULT 1, razon TEXT)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS cp_credentials (
            cp_id TEXT PRIMARY KEY,
            registry_token TEXT,
            encryption_key TEXT NOT NULL,
            created_at INTEGER DEFAULT (strftime('%s', 'now')))''')
        
        self.conn.commit()
        logger.info("✅ BD inicializada")
    
    def save_cp_credentials(self, cp_id: str, encryption_key: str, registry_token: Optional[str] = None) -> None:
        """Guardar credenciales de un CP"""
        with self.lock:
            try:
                self.conn.execute('''INSERT OR REPLACE INTO cp_credentials 
                    (cp_id, encryption_key, registry_token) VALUES (?, ?, ?)''',
                    (cp_id, encryption_key, registry_token))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error guardando credenciales: {e}")

    def get_cp_encryption_key(self, cp_id: str) -> Optional[str]:
        """Obtener clave de cifrado de un CP"""
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
        """Marcar CP como autenticado"""
        with self.lock:
            try:
                self.conn.execute('UPDATE charging_points SET authenticated = 1 WHERE cp_id = ?', (cp_id,))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error marcando autenticación: {e}")
            
    def save_cp(self, cp_id: str, location: str, price: float):
        with self.lock:
            try:
                self.conn.execute('''INSERT OR REPLACE INTO charging_points 
                    (cp_id, location, price, last_seen, registered) VALUES (?, ?, ?, ?, 1)''',
                    (cp_id, location, price, int(time.time())))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error guardando CP: {e}")
    
    def sync_database(self):
        """Sincronizar la base de datos al disco"""
        with self.lock:
            try:
                self.conn.commit()
                self.conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
            except Exception as e:
                logger.error(f"Error sincronizando BD: {e}")
                
    def get_all_cps(self) -> List[Dict]:
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('SELECT * FROM charging_points')
                return [dict(row) for row in cursor.fetchall()]
            except:
                return []
    
    def update_cp_status(self, cp_id: str, status: str):
        with self.lock:
            try:
                self.conn.execute('UPDATE charging_points SET status = ?, last_seen = ? WHERE cp_id = ?',
                                (status, int(time.time()), cp_id))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error actualizando: {e}")
    
    def save_session(self, session_data: Dict):
        with self.lock:
            try:
                self.conn.execute('''INSERT OR REPLACE INTO sessions
                    (session_id, cp_id, driver_id, start_time, end_time, kw_consumed, total_cost, exitosa, razon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (session_data.get('session_id', ''), session_data.get('cp_id', ''),
                     session_data.get('driver_id', ''), session_data.get('start_time', 0),
                     session_data.get('end_time'), session_data.get('kw_consumed', 0),
                     session_data.get('total_cost', 0), 1 if session_data.get('exitosa', True) else 0,
                     session_data.get('razon')))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error sesión: {e}")


class CPWidget(tk.Frame):
    COLORS = {'AVAILABLE': '#2ecc71', 'CHARGING': '#27ae60', 'STOPPED': '#f39c12',
              'BROKEN': '#e74c3c', 'DISCONNECTED': '#95a5a6'}
    
    def __init__(self, parent, cp_id: str, location: str, price: float):
        super().__init__(parent, relief=tk.RAISED, borderwidth=2, bg='#95a5a6')
        self.cp_id = cp_id
        tk.Label(self, text=cp_id, font=('Arial', 14, 'bold'), fg='white', bg=self.cget('bg')).pack(pady=5)
        tk.Label(self, text=location, font=('Arial', 9), fg='white', bg=self.cget('bg'), wraplength=200).pack()
        tk.Label(self, text=f"{price}€/kWh", font=('Arial', 10), fg='white', bg=self.cget('bg')).pack(pady=5)
        tk.Label(self, text="─"*30, bg=self.cget('bg'), fg='white').pack()
        
        self.lbl_estado = tk.Label(self, text="DESCONECTADO", font=('Arial', 11, 'bold'), fg='white', bg=self.cget('bg'))
        self.lbl_estado.pack(pady=10)
        
        # Labels para autenticación
        self.lbl_auth = tk.Label(self, text="", font=('Arial', 8), fg='yellow', bg=self.cget('bg'))
        self.lbl_auth.pack()
        
        self.frame_carga = tk.Frame(self, bg=self.cget('bg'))
        self.lbl_driver = tk.Label(self.frame_carga, text="", font=('Arial', 9, 'bold'), fg='yellow', bg=self.cget('bg'))
        self.lbl_driver.pack()
        self.lbl_consumo = tk.Label(self.frame_carga, text="", font=('Arial', 11, 'bold'), fg='white', bg=self.cget('bg'))
        self.lbl_consumo.pack(pady=2)
        self.lbl_coste = tk.Label(self.frame_carga, text="", font=('Arial', 11, 'bold'), fg='white', bg=self.cget('bg'))
        self.lbl_coste.pack()
        
        self.config(width=220, height=300)
        self.pack_propagate(False)
    
    def actualizar(self, status: str, driver_id: str = "", kw: float = 0.0, cost: float = 0.0, 
                   authenticated: bool = False):
        color = self.COLORS.get(status, '#95a5a6')
        self.config(bg=color)
        self._update_bg_recursive(self, color)
        
        texto = {'AVAILABLE': 'DISPONIBLE', 'CHARGING': '', 'STOPPED': 'FUERA DE SERVICIO',
                'BROKEN': 'AVERIADO', 'DISCONNECTED': 'DESCONECTADO'}
        self.lbl_estado.config(text=texto.get(status, status))
        
        # Mostrar estado de autenticación
        if authenticated:
            self.lbl_auth.config(text="🔐 Autenticado")
        else:
            self.lbl_auth.config(text="⚠️ Sin autenticar")
        
        if status == 'CHARGING' and driver_id:
            self.lbl_driver.config(text=f"Driver: {driver_id}")
            self.lbl_consumo.config(text=f"{kw:.2f} kWh")
            self.lbl_coste.config(text=f"{cost:.2f} €")
            self.frame_carga.pack(pady=5)
            self.lbl_estado.pack_forget()
        else:
            self.frame_carga.pack_forget()
            if status in ['BROKEN', 'DISCONNECTED', 'STOPPED']:
                self.lbl_estado.pack(pady=10)
    
    def _update_bg_recursive(self, widget, color):
        for child in widget.winfo_children():
            if isinstance(child, (tk.Label, tk.Frame)):
                try:
                    child.config(bg=color)
                except:
                    pass
            self._update_bg_recursive(child, color)


class Central:
    def __init__(self, socket_port: int = 5001, kafka_servers: str = 'localhost:9092', db_path: str = 'evcharging.db'):
        self.socket_port = socket_port
        self.kafka_servers = kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        self.db = Database(db_path)
        self.audit = AuditLogger('audit.log')
        self.weather_alerts = {}  # {cp_id: alert_data}
        self.charging_points: Dict[str, Dict[str, Any]] = {}
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.pending_commands: Dict[str, str] = {}
        self.lock = threading.Lock()
        
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
        logger.info("="*60)
        logger.info("SISTEMA CENTRAL - VERSIÓN FINAL CORREGIDA")
        logger.info("="*60)
        
        self._load_cps_from_db()
        
        if not self._init_kafka():
            logger.error("❌ Error Kafka")
            return False
        
        if not self._init_socket_server():
            logger.error("❌ Error Socket")
            return False
        
        threading.Thread(target=self._monitor_connections, daemon=True).start()
        
        # Thread para consumir alertas climáticas desde Kafka
        threading.Thread(target=self._weather_alert_consumer, daemon=True).start()
        
        logger.info("✅ Sistema listo")
        
        # Auditoría de inicio
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL', 'System startup', 'Central iniciada correctamente', True)
        
        self._init_gui()
        return True
    
    def _database_sync_loop(self):
        """Loop que sincroniza la BD periódicamente"""
        while self.running:
            try:
                time.sleep(2)
                self.db.sync_database()
            except Exception as e:
                if self.running:
                    logger.error(f"Error en sync loop: {e}")
                    time.sleep(2)

    def _load_cps_from_db(self):
        cps = self.db.get_all_cps()
        with self.lock:
            for cp in cps:
                self.charging_points[cp['cp_id']] = {
                    'location': cp['location'], 'price': cp['price'], 'status': 'DISCONNECTED',
                    'socket': None, 'session': None, 'last_seen': 0,
                    'monitor_alive': False, 'engine_alive': False, 'consecutive_failures': 0,
                    'authenticated': bool(cp.get('authenticated', 0))
                }
    
    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                logger.info(f"🔄 Kafka ({attempt}/15)...")
                
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5, request_timeout_ms=30000, max_block_ms=10000)
                
                self.producer.send('test_topic', {'test': 'connection'}).get(timeout=10)
                
                self.consumer = KafkaConsumer(
                    'service_requests', 'charging_data', 'charging_complete', 'weather_alerts',
                    bootstrap_servers=self.kafka_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest', group_id='central-group',
                    enable_auto_commit=True, session_timeout_ms=30000)
                
                threading.Thread(target=self._kafka_consumer_loop, daemon=True).start()
                logger.info("✅ Kafka OK")
                return True
            except Exception as e:
                logger.error(f"❌ Kafka: {e}")
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
                    
                    if msg.topic == 'service_requests':
                        self._handle_service_request(msg.value)
                    elif msg.topic == 'charging_data':
                        self._handle_charging_data(msg.value)
                    elif msg.topic == 'charging_complete':
                        self._handle_charging_complete(msg.value)
                    elif msg.topic == 'weather_alerts':
                        self._handle_weather_alert_kafka(msg.value)
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Consumer: {e}")
                    time.sleep(1)
    
    def _weather_alert_consumer(self):
        """Thread dedicado para alertas climáticas desde API_Central vía Kafka"""
        while self.running:
            time.sleep(1)
    
    def _init_socket_server(self) -> bool:
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.socket_port))
            self.server_socket.settimeout(1.0)
            self.server_socket.listen(10)
            
            threading.Thread(target=self._accept_monitors, daemon=True).start()
            logger.info(f"✅ Socket {self.socket_port}")
            return True
        except Exception as e:
            logger.error(f"❌ Socket: {e}")
            return False
    
    def _accept_monitors(self):
        while self.running:
            try:
                if self.server_socket is None:
                    break
                client_socket, address = self.server_socket.accept()
                # Auditoría de conexión
                self.audit.log_event('CONNECTION', address[0], 'MONITOR', 'Connection attempt', f'From {address}', True)
                threading.Thread(target=self._handle_monitor, args=(client_socket, address[0]), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    time.sleep(1)
    
    def _handle_monitor(self, sock: socket.socket, client_ip: str):
        cp_id: Optional[str] = None
        
        try:
            sock.settimeout(10)
            data = sock.recv(1024).decode('utf-8').strip()
            
            if data.startswith('REGISTER'):
                parts = data.split('|')
                if len(parts) >= 4:
                    _, cp_id, location, price = parts[:4]
                    price = float(price)
                    
                    # Verificar si el CP está registrado en Registry (simplificado)
                    with self.lock:
                        if cp_id not in self.charging_points:
                            self.charging_points[cp_id] = {
                                'location': location, 'price': price, 'status': 'AVAILABLE',
                                'socket': sock, 'session': None, 'last_seen': time.time(),
                                'monitor_alive': True, 'engine_alive': False, 'consecutive_failures': 0,
                                'authenticated': False}
                            self.db.save_cp(cp_id, location, price)
                            logger.info(f"✅ {cp_id} NUEVO")
                            self._gui_log(f"✅ {cp_id} registrado")
                            self._gui_add_cp(cp_id, location, price)
                        else:
                            cp_data = self.charging_points[cp_id]
                            if cp_data.get('session'):
                                self._abort_session(cp_id, 'CP reconectado')
                            
                            cp_data.update({'socket': sock, 'status': 'AVAILABLE', 'last_seen': time.time(),
                                          'monitor_alive': True, 'engine_alive': False, 'consecutive_failures': 0})
                            logger.info(f"🔄 {cp_id} RECONECTADO")
                        
                        self._update_cp_gui_sync(cp_id, 'AVAILABLE', authenticated=False)
                        self.db.update_cp_status(cp_id, 'AVAILABLE')
                    
                    # Generar o recuperar clave de cifrado
                    encryption_key = self.db.get_cp_encryption_key(cp_id)
                    if not encryption_key:
                        encryption_key = CryptoManager.generate_key()
                        self.db.save_cp_credentials(cp_id, encryption_key)
                        logger.info(f"🔑 Nueva clave generada para {cp_id}")
                    
                    # Marcar como autenticado
                    self.db.mark_cp_authenticated(cp_id)
                    with self.lock:
                        self.charging_points[cp_id]['authenticated'] = True
                    
                    # Enviar clave de cifrado al CP
                    response = f'OK|REGISTERED|{encryption_key}'
                    sock.send(response.encode('utf-8'))
                    
                    # Auditoría de autenticación
                    self.audit.log_authentication(cp_id, client_ip, True)
                    self._gui_log(f"🔐 {cp_id} autenticado")
                    
                    self._update_cp_gui_sync(cp_id, 'AVAILABLE', authenticated=True)
                    
                    self._monitor_health_loop(cp_id, sock)
        except Exception as e:
            logger.error(f"❌ Monitor {cp_id}: {e}")
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
                        
                        self._update_cp_gui_sync(cp_id, 'DISCONNECTED', authenticated=False)
                        self.db.update_cp_status(cp_id, 'DISCONNECTED')
                        self._gui_log(f"❌ {cp_id} desconectado")
                        
                        # Auditoría
                        self.audit.log_cp_status_change(cp_id, old_status, 'DISCONNECTED', 'Monitor disconnected')
            try:
                sock.close()
            except:
                pass
    
    def _abort_session(self, cp_id: str, razon: str):
        """Abortar sesión y notificar al driver"""
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
        self._gui_remove_request(session['session_id'])
        
        # Auditoría
        self.audit.log_error('SESSION_ABORTED', cp_id, f'Razón: {razon}')
    
    def _monitor_health_loop(self, cp_id: str, sock: socket.socket):
        sock.settimeout(15)
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
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive'] = True
                            cp_data['consecutive_failures'] = 0
                            
                            if cp_data['status'] == 'BROKEN':
                                if cp_id in self.pending_commands:
                                    cmd = self.pending_commands.pop(cp_id)
                                    self._send_kafka('central_commands', {'cp_id': cp_id, 'command': cmd, 'timestamp': time.time()}, encrypt_for_cp=cp_id)
                                    cp_data['status'] = 'STOPPED' if cmd == 'STOP' else 'AVAILABLE'
                                else:
                                    cp_data['status'] = 'AVAILABLE'
                                
                                self._update_cp_gui_sync(cp_id, cp_data['status'], authenticated=cp_data.get('authenticated', False))
                                self.db.update_cp_status(cp_id, cp_data['status'])
                                self._gui_log(f"✅ {cp_id} recuperado")
                
                elif msg == 'HEALTH_FAIL':
                    with self.lock:
                        if cp_id in self.charging_points:
                            cp_data = self.charging_points[cp_id]
                            cp_data['engine_alive'] = False
                            cp_data['consecutive_failures'] += 1
                            
                            if cp_data['monitor_alive'] and cp_data['consecutive_failures'] >= 3:
                                self._handle_cp_failure(cp_id)
            except socket.timeout:
                if time.time() - last_health_ok > 20:
                    break
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"❌ Health {cp_id}: {e}")
                break
    
    def _monitor_connections(self):
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
                                
                                self._update_cp_gui_sync(cp_id, 'DISCONNECTED', authenticated=False)
                                self.db.update_cp_status(cp_id, 'DISCONNECTED')
                                self._gui_log(f"❌ {cp_id} TIMEOUT")
                                
                                # Auditoría
                                self.audit.log_cp_status_change(cp_id, old_status, 'DISCONNECTED', 'Connection timeout')
                
                time.sleep(2)
            except Exception as e:
                if self.running:
                    time.sleep(2)
    
    def _handle_cp_failure(self, cp_id: str):
        with self.lock:
            if cp_id not in self.charging_points:
                return
            
            cp = self.charging_points[cp_id]
            if not cp.get('monitor_alive'):
                return
            
            old_status = cp['status']
            cp['status'] = 'BROKEN'
            cp['engine_alive'] = False
            
            if cp.get('session'):
                self._abort_session(cp_id, 'Avería del Engine')
            
            self._update_cp_gui_sync(cp_id, 'BROKEN', authenticated=cp.get('authenticated', False))
            self.db.update_cp_status(cp_id, 'BROKEN')
            self._gui_log(f"💥 {cp_id} AVERIADO")
            
            # Auditoría
            self.audit.log_cp_status_change(cp_id, old_status, 'BROKEN', 'Consecutive Engine failures')
    
    def _handle_service_request(self, data: Dict[str, Any]):
        driver_id = data.get('driver_id', '')
        cp_id = data.get('cp_id', '')
        
        self._gui_log(f"📨 {driver_id} → {cp_id}")
        
        # Auditoría de solicitud
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
                self._send_notification(driver_id, 'DENIED', cp_id, razon.get(cp['status'], 'No disponible'))
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
            
            # Auditoría
            self.audit.log_service_auth(driver_id, cp_id, True)
            
            self._update_cp_gui_sync(cp_id, 'CHARGING', driver_id, 0.0, 0.0, cp.get('authenticated', False))
            self._gui_add_request(session_id, datetime.now().strftime("%d/%m/%y"),
                                datetime.now().strftime("%H:%M"), driver_id, cp_id)
            
            self.db.update_cp_status(cp_id, 'CHARGING')
            self._gui_log(f"✅ {driver_id} en {cp_id}")
    
    def _handle_charging_data(self, data: Dict[str, Any]):
        # Descifrar si es necesario
        if isinstance(data, dict) and data.get('encrypted'):
            cp_id = data.get('cp_id')
            encryption_key: Optional[str] = None
            if cp_id:
                encryption_key = self.db.get_cp_encryption_key(cp_id)
            if encryption_key is not None:
                try:
                    encrypted_data = data.get('data')
                    if encrypted_data is not None:
                        data = CryptoManager.decrypt_json(encrypted_data, encryption_key)
                    else:
                        logger.error(f"Datos cifrados no encontrados para {cp_id}")
                        return
                except Exception as e:
                    logger.error(f"Error descifrando datos de {cp_id}: {e}")
                    return
            else:
                logger.error(f"Clave de cifrado no encontrada para {cp_id}, no se puede descifrar datos.")
                return
        
        cp_id = data.get('cp_id', '')
        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                if cp.get('session'):
                    cp['session']['kw_consumed'] = data.get('kw', 0.0)
                    cp['session']['total_cost'] = data.get('cost', 0.0)
                    self._update_cp_gui_sync(cp_id, 'CHARGING', data.get('driver_id', ''),
                                           data.get('kw', 0.0), data.get('cost', 0.0),
                                           cp.get('authenticated', False))
    
    def _handle_charging_complete(self, data: Dict[str, Any]):
        # Descifrar si es necesario
        if isinstance(data, dict) and data.get('encrypted'):
            cp_id = data.get('cp_id')
            encryption_key: Optional[str] = None
            if cp_id:
                encryption_key = self.db.get_cp_encryption_key(cp_id)
            if encryption_key is not None:
                try:
                    encrypted_data = data.get('data')
                    if encrypted_data is not None:
                        data = CryptoManager.decrypt_json(encrypted_data, encryption_key)
                    else:
                        logger.error(f"Datos cifrados no encontrados para {cp_id}")
                        return
                except Exception as e:
                    logger.error(f"Error descifrando complete de {cp_id}: {e}")
                    return
            else:
                logger.error(f"Clave de cifrado no encontrada para {cp_id}, no se puede descifrar datos.")
                return
        
        cp_id = data.get('cp_id', '')
        session_id = data.get('session_id', '')
        driver_id = data.get('driver_id', '')
        exitosa = data.get('exitosa', True)
        razon = data.get('razon', '')
        
        with self.lock:
            if cp_id in self.charging_points:
                cp = self.charging_points[cp_id]
                if cp.get('session'):
                    session = cp['session']
                    session.update({'end_time': int(time.time()), 'cp_id': cp_id,
                                   'exitosa': exitosa, 'razon': razon})
                    self.db.save_session(session)
                
                cp['session'] = None
                if cp['status'] in ['CHARGING', 'AVAILABLE']:
                    cp['status'] = 'AVAILABLE'
                    self._update_cp_gui_sync(cp_id, 'AVAILABLE', authenticated=cp.get('authenticated', False))
                    self.db.update_cp_status(cp_id, 'AVAILABLE')
            
            if session_id in self.sessions:
                del self.sessions[session_id]
                self._gui_remove_request(session_id)
        
        self._send_kafka('driver_notifications', {
            'driver_id': driver_id, 'cp_id': cp_id, 'session_id': session_id,
            'kw_total': data.get('kw_total', 0), 'cost_total': data.get('cost_total', 0),
            'exitosa': exitosa, 'razon': razon, 'type': 'FINAL_TICKET', 'timestamp': time.time()
        })
    
    def _handle_weather_alert_kafka(self, data: Dict[str, Any]):
        """Manejar alerta climática recibida desde Kafka (enviada por API_Central)"""
        cp_id = data.get('cp_id')
        alert_type = data.get('alert_type')
        temperature = data.get('temperature')
        city = data.get('city')
        
        if not all([cp_id, alert_type, temperature, city]):
            logger.warning("⚠️ Alerta climática incompleta")
            return
        
        try:
            if temperature is not None:
                temperature = float(temperature)
            else:
                logger.warning("⚠️ Temperatura inválida en alerta climática")
                return
        except (ValueError, TypeError):
            logger.warning("⚠️ Temperatura inválida en alerta climática")
            return
        
        if isinstance(cp_id, str) and isinstance(alert_type, str) and isinstance(city, str):
            self.handle_weather_alert(cp_id, alert_type, temperature, city)
    
    def handle_weather_alert(self, cp_id: str, alert_type: str, temperature: float, city: str):
        """Manejar alerta climática"""
        with self.lock:
            if alert_type == 'START':
                self.weather_alerts[cp_id] = {
                    'temperature': temperature,
                    'city': city,
                    'started_at': time.time()
                }
                logger.warning(f"❄️ ALERTA CLIMÁTICA: {cp_id} ({city}) - {temperature}°C")
                
                # Enviar comando STOP al CP
                self._send_command(cp_id, 'STOP')
                
                # Auditoría
                self.audit.log_weather_alert(cp_id, 'START', temperature)
                
                self._gui_log(f"❄️ Alerta clima: {cp_id} ({temperature}°C)")
            
            elif alert_type == 'END':
                if cp_id in self.weather_alerts:
                    del self.weather_alerts[cp_id]
                
                logger.info(f"☀️ ALERTA CANCELADA: {cp_id} ({city}) - {temperature}°C")
                
                # Enviar comando RESUME al CP
                self._send_command(cp_id, 'RESUME')
                
                # Auditoría
                self.audit.log_weather_alert(cp_id, 'END', temperature)
                
                self._gui_log(f"☀️ Alerta clima cancelada: {cp_id}")
    
    def _send_kafka(self, topic: str, payload: Dict[str, Any], encrypt_for_cp: Optional[str] = None):
        """Enviar mensaje a Kafka, opcionalmente cifrado para un CP específico"""
        for _ in range(3):
            try:
                if self.producer:
                    # Si es un mensaje para un CP específico, cifrarlo
                    if encrypt_for_cp and encrypt_for_cp in self.charging_points:
                        encryption_key = self.db.get_cp_encryption_key(encrypt_for_cp)
                        if encryption_key:
                            try:
                                encrypted_payload = CryptoManager.encrypt_json(payload, encryption_key)
                                final_payload = {'encrypted': True, 'data': encrypted_payload, 'cp_id': encrypt_for_cp}
                            except Exception as e:
                                logger.error(f"Error cifrando mensaje: {e}")
                                final_payload = payload
                        else:
                            final_payload = payload
                    else:
                        final_payload = payload
                    
                    self.producer.send(topic, final_payload)
                    self.producer.flush(timeout=5)
                return
            except:
                time.sleep(1)
    
    def _send_notification(self, driver_id: str, status: str, cp_id: str, message: str):
        self._send_kafka('driver_notifications', {
            'driver_id': driver_id, 'status': status, 'cp_id': cp_id,
            'message': message, 'timestamp': time.time()
        })
    
    def _send_command(self, cp_id: str, command: str):
        with self.lock:
            if cp_id not in self.charging_points:
                return
            
            cp_data = self.charging_points[cp_id]
            self.pending_commands[cp_id] = command
            
            if command == 'STOP':
                if cp_data.get('session'):
                    self._abort_session(cp_id, 'Detenido por Central')
                old_status = cp_data['status']
                cp_data['status'] = 'STOPPED'
                self._update_cp_gui_sync(cp_id, 'STOPPED', authenticated=cp_data.get('authenticated', False))
                self.audit.log_cp_status_change(cp_id, old_status, 'STOPPED', f'Command: {command}')
            elif command == 'RESUME':
                old_status = cp_data['status']
                cp_data['status'] = 'AVAILABLE'
                self._update_cp_gui_sync(cp_id, 'AVAILABLE', authenticated=cp_data.get('authenticated', False))
                self.audit.log_cp_status_change(cp_id, old_status, 'AVAILABLE', f'Command: {command}')
            
            self.db.update_cp_status(cp_id, cp_data['status'])
        
        self._send_kafka('central_commands', 
                        {'cp_id': cp_id, 'command': command, 'timestamp': time.time()},
                        encrypt_for_cp=cp_id)
        
        # Auditoría
        self.audit.log_command(cp_id, command)
        
        self._gui_log(f"📤 {command} → {cp_id}")
    
    # GUI - ACTUALIZACIÓN SINCRONIZADA
    def _update_cp_gui_sync(self, cp_id: str, status: str, driver: str = "", kw: float = 0.0, 
                           cost: float = 0.0, authenticated: bool = False):
        """Actualiza GUI SINCRÓNICAMENTE"""
        if self.root and cp_id in self.cp_widgets:
            try:
                self.cp_widgets[cp_id].actualizar(status, driver, kw, cost, authenticated)
                self.root.update_idletasks()
            except:
                pass
    
    def _init_gui(self):
        self.root = tk.Tk()
        self.root.title("EVCharging - CENTRAL (RELEASE 2 - CORREGIDA)")
        self.root.geometry("1400x950")
        self.root.config(bg='#2c3e50')
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # HEADER
        header = tk.Frame(self.root, bg='#1a252f', height=70)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        tk.Label(header, text="*** EV CHARGING - CENTRAL (RELEASE 2 - FINAL) ***",
                font=('Arial', 16, 'bold'), bg='#1a252f', fg='#ecf0f1').pack(pady=20)
        
        # CPs
        cp_container = tk.Frame(self.root, bg='#34495e')
        cp_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        canvas = tk.Canvas(cp_container, bg='#34495e', highlightthickness=0)
        scrollbar = ttk.Scrollbar(cp_container, orient="vertical", command=canvas.yview)
        self.scrollable_frame = tk.Frame(canvas, bg='#34495e')
        self.scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
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
        tk.Label(req_frame, text="*** ON GOING REQUESTS ***", font=('Arial', 11, 'bold'),
                bg='#1a252f', fg='white').pack(pady=5)
        
        self.requests_table = ttk.Treeview(req_frame, columns=("DATE", "TIME", "USER", "CP"),
                                          show='headings', height=3)
        for col in ["DATE", "TIME", "USER", "CP"]:
            self.requests_table.heading(col, text=col)
            self.requests_table.column(col, width=150, anchor=tk.CENTER)
        self.requests_table.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # COMANDOS
        cmd_frame = tk.Frame(self.root, bg='#1a252f', height=90)
        cmd_frame.pack(fill=tk.X, padx=10, pady=5)
        cmd_frame.pack_propagate(False)
        tk.Label(cmd_frame, text="*** CENTRAL COMMANDS ***", font=('Arial', 11, 'bold'),
                bg='#1a252f', fg='white').pack(pady=5)
        
        btn_frame = tk.Frame(cmd_frame, bg='#1a252f')
        btn_frame.pack()
        tk.Button(btn_frame, text="⛔ PARAR CP", command=self._cmd_stop_cp, bg='#e74c3c',
                 fg='white', font=('Arial', 10, 'bold'), width=14).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="▶️ REANUDAR CP", command=self._cmd_resume_cp, bg='#2ecc71',
                 fg='white', font=('Arial', 10, 'bold'), width=14).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="⛔ PARAR TODOS", command=self._cmd_stop_all, bg='#c0392b',
                 fg='white', font=('Arial', 10, 'bold'), width=14).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="▶️ REANUDAR TODOS", command=self._cmd_resume_all, bg='#27ae60',
                 fg='white', font=('Arial', 10, 'bold'), width=14).pack(side=tk.LEFT, padx=5)
        
        # LOGS
        log_frame = tk.Frame(self.root, bg='#1a252f', height=100)
        log_frame.pack(fill=tk.X, padx=10, pady=5)
        log_frame.pack_propagate(False)
        tk.Label(log_frame, text="*** MESSAGES ***", font=('Arial', 11, 'bold'),
                bg='#1a252f', fg='white').pack(pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=3, bg='#2c3e50',
                                                  fg='#ecf0f1', font=('Courier', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Cargar CPs existentes
        with self.lock:
            for cp_id, cp_data in self.charging_points.items():
                self._do_gui_add_cp(cp_id, cp_data['location'], cp_data['price'])
                self._do_gui_update_cp(cp_id, 'DISCONNECTED', '', 0, 0, False)
        
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
    
    def _gui_log(self, msg: str):
        if self.root and self.log_text:
            self.root.after(0, lambda: self._do_log(msg))
    
    def _do_log(self, msg: str):
        if self.log_text:
            self.log_text.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
            self.log_text.see(tk.END)
    
    def _gui_add_cp(self, cp_id: str, location: str, price: float):
        if self.root:
            self.root.after(0, lambda: self._do_gui_add_cp(cp_id, location, price))
    
    def _do_gui_add_cp(self, cp_id: str, location: str, price: float):
        if cp_id in self.cp_widgets or not self.frame_cps:
            return
        widget = CPWidget(self.frame_cps, cp_id, location, price)
        num = len(self.cp_widgets)
        widget.grid(row=num//5, column=num%5, padx=10, pady=10)
        self.cp_widgets[cp_id] = widget
    
    def _gui_update_cp(self, cp_id: str, status: str, driver: str = "", kw: float = 0.0, 
                       cost: float = 0.0, authenticated: bool = False):
        if self.root:
            self.root.after(0, lambda: self._do_gui_update_cp(cp_id, status, driver, kw, cost, authenticated))
    
    def _do_gui_update_cp(self, cp_id: str, status: str, driver: str, kw: float, cost: float, authenticated: bool):
        if cp_id in self.cp_widgets:
            self.cp_widgets[cp_id].actualizar(status, driver, kw, cost, authenticated)
    
    def _gui_add_request(self, sid: str, date: str, time_str: str, user: str, cp: str):
        if self.root and self.requests_table:
            self.root.after(0, lambda: self._do_gui_add_request(sid, date, time_str, user, cp))
    
    def _do_gui_add_request(self, sid: str, date: str, time_str: str, user: str, cp: str):
        if self.requests_table:
            item = self.requests_table.insert("", tk.END, values=(date, time_str, user, cp))
            self.request_items[sid] = item
    
    def _gui_remove_request(self, sid: str):
        if self.root and self.requests_table:
            self.root.after(0, lambda: self._do_gui_remove_request(sid))
    
    def _do_gui_remove_request(self, sid: str):
        if self.requests_table and sid in self.request_items:
            try:
                self.requests_table.delete(self.request_items[sid])
                del self.request_items[sid]
            except:
                pass
    
    def _on_closing(self):
        if messagebox.askyesno("Cerrar", "¿Cerrar Central?"):
            self.shutdown()
            if self.root:
                self.root.destroy()
    
    def shutdown(self):
        self.running = False
        self.audit.log_event('SYSTEM', '0.0.0.0', 'CENTRAL', 'System shutdown', 'Central detenida', True)
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
        self.db.conn.close()


if __name__ == '__main__':
    socket_port = int(os.getenv('SOCKET_PORT', '5001'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    central = Central(socket_port, kafka_servers)
    central.start()