"""
EV_CP_E - Engine Corregido
Correcciones: carga robusta de encryption key, manejo mejorado de sesiones recuperadas, cifrado condicional robusto
"""
import socket
import threading
import json
import time
import os
import logging
import random
import pickle
from typing import Optional, Dict, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import sys

# Añadir path para security
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import condicional de CryptoManager
try:
    from security.security_utils import CryptoManager
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logging.warning("⚠️ CryptoManager no disponible")

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')


class ChargingPointEngine:
    def __init__(self, cp_id: str, listen_port: int, price_per_kwh: float, kafka_servers: str):
        self.cp_id = cp_id
        self.listen_port = listen_port
        self.price_per_kwh = price_per_kwh
        self.kafka_servers = kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        
        self.state = 'IDLE'
        self.is_healthy = True
        self.is_stopped_by_central = False
        self.current_session: Optional[Dict[str, Any]] = None
        self.last_central_contact = 0
        
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.health_server: Optional[socket.socket] = None
        self.running = True
        self.charging_active = False
        
        self.session_backup_file = f'/tmp/cp_{cp_id}_session.pkl'
        self.logger = logging.getLogger(f"Engine-{cp_id}")
        self.lock = threading.Lock()
        
        # Cifrado
        self.encryption_key = None
        self.encryption_key_loaded = threading.Event()

    def _load_encryption_key(self):
        """Cargar encryption key del Monitor con timeout"""
        key_file = f'/tmp/{self.cp_id}_encryption_key.txt'
        
        self.logger.info("⏳ Esperando encryption key del Monitor...")
        
        for i in range(45):  # 45 segundos de espera
            if os.path.exists(key_file):
                try:
                    with open(key_file, 'r') as f:
                        self.encryption_key = f.read().strip()
                    
                    if self.encryption_key and len(self.encryption_key) > 0:
                        self.logger.info(f"🔑 Encryption key cargada: {self.encryption_key[:20]}...")
                        self.encryption_key_loaded.set()
                        return True
                except Exception as e:
                    self.logger.error(f"❌ Error leyendo key: {e}")
            
            time.sleep(1)
        
        self.logger.warning("⚠️ No se pudo cargar encryption key en 45s")
        self.logger.warning("⚠️ Continuando sin cifrado")
        self.encryption_key_loaded.set()  # Permitir continuar
        return False
        
    def start(self) -> bool:
        """Iniciar Engine"""
        self.logger.info("="*60)
        self.logger.info(f"ENGINE {self.cp_id}")
        self.logger.info("="*60)
        
        # 1. Cargar sesión guardada
        recovered_session = self._load_session_backup()
        
        # 2. Iniciar health server
        if not self._init_health_server():
            return False
        
        # 3. Iniciar Kafka
        if not self._init_kafka():
            return False
        
        # 4. Cargar encryption key (en paralelo, no bloquea)
        threading.Thread(target=self._load_encryption_key, daemon=True).start()
        
        # 5. Enviar sesión recuperada después de cargar key
        if recovered_session:
            threading.Thread(target=self._send_recovered_session_when_ready, 
                           args=(recovered_session,), daemon=True).start()
        
        self.logger.info(f"✅ Engine {self.cp_id} listo")
        self._interactive_mode()
        return True

    def _send_recovered_session_when_ready(self, session: Dict[str, Any]):
        """Enviar sesión recuperada cuando la key esté lista"""
        # Esperar a que Kafka esté listo
        time.sleep(3)
        
        # Esperar a que la encryption key esté cargada (o timeout)
        if not self.encryption_key_loaded.wait(timeout=50):
            self.logger.warning("⚠️ Timeout esperando encryption key, enviando sin cifrar")
        
        try:
            payload = {
                'cp_id': self.cp_id,
                'session_id': session['session_id'],
                'driver_id': session['driver_id'],
                'kw_total': session['kw_consumed'],
                'cost_total': session['total_cost'],
                'exitosa': False,
                'razon': session['razon'],
                'timestamp': time.time()
            }
            
            self._send_kafka('charging_complete', payload, encrypt=True)
            
            self.logger.info("✅ Sesión recuperada enviada a Central")
            
            # Eliminar backup
            if os.path.exists(self.session_backup_file):
                try:
                    os.remove(self.session_backup_file)
                except:
                    pass
        except Exception as e:
            self.logger.error(f"❌ Error enviando sesión recuperada: {e}")

    def _load_session_backup(self) -> Optional[Dict[str, Any]]:
        """Cargar sesión guardada"""
        if os.path.exists(self.session_backup_file):
            try:
                with open(self.session_backup_file, 'rb') as f:
                    backup = pickle.load(f)
                    
                    self.logger.info("="*60)
                    self.logger.info("📂 SESIÓN RECUPERADA")
                    self.logger.info(f"   Sesión: {backup['session_id']}")
                    self.logger.info(f"   Driver: {backup['driver_id']}")
                    self.logger.info(f"   Consumo: {backup['kw_consumed']:.2f} kWh")
                    self.logger.info(f"   Coste: {backup['total_cost']:.2f} €")
                    self.logger.info("="*60)
                    
                    backup['exitosa'] = False
                    backup['razon'] = 'Engine cayó durante carga'
                    return backup
            except Exception as e:
                self.logger.error(f"❌ Error cargando backup: {e}")
        
        return None

    def _save_session_backup(self):
        """Guardar sesión"""
        if self.current_session:
            try:
                with open(self.session_backup_file, 'wb') as f:
                    pickle.dump(self.current_session, f)
            except Exception as e:
                self.logger.error(f"❌ Error guardando backup: {e}")

    def _init_health_server(self) -> bool:
        """Iniciar servidor health"""
        try:
            self.health_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.health_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.health_server.bind(('0.0.0.0', self.listen_port))
            self.health_server.listen(5)
            self.health_server.settimeout(1.0)
            
            threading.Thread(target=self._health_server_loop, daemon=True).start()
            self.logger.info(f"✅ Health server: {self.listen_port}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Health server error: {e}")
            return False

    def _health_server_loop(self):
        """Loop health server"""
        while self.running:
            try:
                if self.health_server is None:
                    break
                
                client, addr = self.health_server.accept()
                client.settimeout(2.0)
                
                try:
                    data = client.recv(64).decode('utf-8').strip()
                    if data == 'PING':
                        response = b'PONG' if self.is_healthy else b'KO'
                        client.send(response)
                finally:
                    client.close()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    pass  # Evitar spam de logs

    def _init_kafka(self) -> bool:
        """Inicializar Kafka"""
        for attempt in range(1, 16):
            try:
                self.logger.info(f"🔄 Kafka ({attempt}/15)...")
                
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5, 
                    request_timeout_ms=30000,
                    max_block_ms=10000)
                
                self.consumer = KafkaConsumer(
                    'service_authorizations', 'central_commands',
                    bootstrap_servers=self.kafka_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest', 
                    group_id=f'cp-{self.cp_id}',
                    enable_auto_commit=True,
                    session_timeout_ms=30000,
                    consumer_timeout_ms=1000)
                
                threading.Thread(target=self._kafka_consumer_loop, daemon=True).start()
                self.logger.info("✅ Kafka conectado")
                return True
            except Exception as e:
                self.logger.error(f"❌ Kafka error: {e}")
                if attempt < 15:
                    time.sleep(5)
        return False

    def _kafka_consumer_loop(self):
        """Loop consumer Kafka"""
        while self.running:
            try:
                if self.consumer is None:
                    break
                
                for msg in self.consumer:
                    if not self.running:
                        break
                    
                    data = msg.value
                    
                    # Descifrar si necesario
                    if isinstance(data, dict) and data.get('encrypted') and data.get('cp_id') == self.cp_id:
                        if self.encryption_key and CRYPTO_AVAILABLE:
                            try:
                                encrypted_data = data.get('data')
                                if encrypted_data:
                                    data = CryptoManager.decrypt_json(encrypted_data, self.encryption_key)
                                else:
                                    self.logger.error("❌ No encrypted data found")
                                    continue
                            except Exception as e:
                                self.logger.error(f"❌ Error descifrando: {e}")
                                continue
                    
                    if msg.topic == 'service_authorizations':
                        self._handle_authorization(data)
                    elif msg.topic == 'central_commands':
                        self._handle_command(data)
            except Exception as e:
                if self.running and "timed out" not in str(e).lower():
                    self.logger.error(f"❌ Consumer error: {e}")
                    time.sleep(1)

    def _handle_authorization(self, data: Dict[str, Any]):
        """Manejar autorización"""
        if data.get('cp_id') != self.cp_id:
            return
        
        self.last_central_contact = time.time()
        
        with self.lock:
            if self.is_stopped_by_central:
                self.logger.warning("⚠️ CP parado por Central")
                return
            
            if self.current_session is not None:
                self.logger.warning("⚠️ Ya hay sesión activa")
                return
        
        driver_id = data.get('driver_id', '')
        session_id = data.get('session_id', '')
        price = data.get('price', self.price_per_kwh)
        
        self.logger.info(f"✅ AUTORIZACIÓN: {driver_id}")
        
        with self.lock:
            self.state = 'AUTHORIZED'
            self.current_session = {
                'session_id': session_id, 
                'driver_id': driver_id, 
                'price': price,
                'start_time': None, 
                'kw_consumed': 0.0, 
                'total_cost': 0.0
            }
        
        print("\n" + "="*60)
        print("⚡ SERVICIO AUTORIZADO")
        print("="*60)
        print(f"Driver:  {driver_id}")
        print(f"Sesión:  {session_id}")
        print("\n👉 Pulsa '1' para iniciar carga")
        print("="*60 + "\n")

    def iniciar_carga(self) -> bool:
        """Iniciar carga"""
        with self.lock:
            if self.state != 'AUTHORIZED':
                print(f"\n❌ Estado: {self.state}. Necesitas autorización.\n")
                return False
            
            if self.current_session is None:
                return False
            
            if self.is_stopped_by_central:
                print("\n❌ CP parado por Central\n")
                return False
            
            self.state = 'CHARGING'
            self.current_session['start_time'] = time.time()
            self.charging_active = True
        
        print("\n⚡ CARGA INICIADA")
        print(f"Driver: {self.current_session['driver_id']}")
        print("👉 Pulsa '2' para finalizar\n")
        
        threading.Thread(target=self._charging_loop, daemon=True).start()
        return True

    def iniciar_carga_manual(self) -> bool:
        """Carga manual (emergencia) - CORREGIDO para notificar a Central"""
        # Verificar conexión reciente con Central
        if time.time() - self.last_central_contact > 30:
            print("\n⚠️ Central no responde hace más de 30s")
            print("   Iniciando en modo emergencia...")
        else:
            print(f"\n⚠️ Central activa (último contacto: {int(time.time() - self.last_central_contact)}s)")
            resp = input("   ¿Continuar con carga manual de emergencia? (s/n): ").lower()
            if resp != 's':
                return False
        
        driver_id = input("ID del conductor: ").strip()
        if not driver_id:
            print("❌ ID de conductor requerido")
            return False
        
        with self.lock:
            if self.current_session:
                print("❌ Ya hay una sesión activa")
                return False
            
            if self.is_stopped_by_central:
                print("❌ CP parado por Central - No se puede iniciar carga manual")
                return False
            
            # Crear sesión manual
            session_id = f"MANUAL_{self.cp_id}_{int(time.time())}_{os.getpid()}"
            self.state = 'CHARGING'
            self.current_session = {
                'session_id': session_id, 
                'driver_id': driver_id, 
                'price': self.price_per_kwh,
                'start_time': time.time(), 
                'kw_consumed': 0.0, 
                'total_cost': 0.0, 
                'manual': True
            }
            self.charging_active = True
        
        print("\n" + "="*60)
        print("⚡ CARGA MANUAL INICIADA (MODO EMERGENCIA)")
        print("="*60)
        print(f"Driver:  {driver_id}")
        print(f"Sesión:  {session_id}")
        print("\n💡 Esta sesión NO está autorizada por Central")
        print("💡 Pulsa '2' para finalizar")
        print("="*60 + "\n")
        
        # CRÍTICO: Notificar a Central si es posible
        # Esto permite que aparezca en Central y Front
        if time.time() - self.last_central_contact < 30:
            try:
                # Enviar datos de carga inmediatamente
                payload = {
                    'cp_id': self.cp_id,
                    'session_id': session_id,
                    'driver_id': driver_id,
                    'kw': 0.0,
                    'cost': 0.0,
                    'manual': True,
                    'timestamp': time.time()
                }
                self._send_kafka('charging_data', payload, encrypt=True)
                self.logger.info("📤 Sesión manual notificada a Central")
            except:
                pass
        
        # Iniciar thread de carga
        threading.Thread(target=self._charging_loop, daemon=True).start()
        return True

    def _charging_loop(self):
        """Loop de carga - CORREGIDO para manejar cargas manuales"""
        last_log_time = 0
        last_send_time = 0
        
        while self.charging_active and self.running:
            with self.lock:
                if self.current_session is None:
                    break
                
                if self.is_stopped_by_central:
                    self.logger.warning("⛔ Carga detenida por Central")
                    break
                
                # Simular consumo
                kw_rate = random.uniform(7.0, 22.0) / 3600  # kW por segundo
                self.current_session['kw_consumed'] += kw_rate
                self.current_session['total_cost'] = (
                    self.current_session['kw_consumed'] * self.current_session['price']
                )
                
                # Guardar backup
                self._save_session_backup()
                
                # Determinar si enviar a Central
                current_time = time.time()
                is_manual = self.current_session.get('manual', False)
                central_alive = (current_time - self.last_central_contact < 30)
                
                # CRÍTICO: Enviar datos cada 2s (manual o autorizada si Central está viva)
                should_send = (current_time - last_send_time >= 2)
                
                if should_send:
                    # Enviar siempre, incluso si es manual
                    # Central/Front decidirán cómo mostrarlo
                    payload = {
                        'cp_id': self.cp_id,
                        'session_id': self.current_session['session_id'],
                        'driver_id': self.current_session['driver_id'],
                        'kw': self.current_session['kw_consumed'],
                        'cost': self.current_session['total_cost'],
                        'manual': is_manual,
                        'timestamp': time.time()
                    }
                    
                    self._send_kafka('charging_data', payload, encrypt=True)
                    last_send_time = current_time
                
                # Log local cada 5s
                if current_time - last_log_time >= 5:
                    elapsed = int(current_time - self.current_session['start_time'])
                    status_icon = "🔧" if is_manual else "⚡"
                    self.logger.info(
                        f"{status_icon} {self.current_session['kw_consumed']:.2f} kWh | "
                        f"{self.current_session['total_cost']:.2f} € | {elapsed}s"
                    )
                    last_log_time = current_time
            
            time.sleep(1)
        
    def finalizar_carga(self, razon: str = 'Finalizada por conductor') -> bool:
        """Finalizar carga"""
        with self.lock:
            if self.state != 'CHARGING':
                print(f"\n❌ Estado: {self.state}\n")
                return False
            
            if self.current_session is None:
                return False
            
            self.charging_active = False
        
        time.sleep(0.5)
        
        with self.lock:
            if self.current_session is None:
                return False
            
            session_id = self.current_session['session_id']
            driver_id = self.current_session['driver_id']
            kw_total = self.current_session['kw_consumed']
            cost_total = self.current_session['total_cost']
            is_manual = self.current_session.get('manual', False)
            
            self.state = 'IDLE'
            self.current_session = None
        
        # Ticket local
        self._print_ticket(session_id, driver_id, kw_total, cost_total, True, razon, is_manual)
        
        # Eliminar backup
        if os.path.exists(self.session_backup_file):
            try:
                os.remove(self.session_backup_file)
            except:
                pass
        
        # Notificar Central
        payload = {
            'cp_id': self.cp_id, 
            'session_id': session_id, 
            'driver_id': driver_id,
            'kw_total': kw_total, 
            'cost_total': cost_total, 
            'exitosa': True,
            'razon': razon, 
            'manual': is_manual, 
            'timestamp': time.time()
        }
        
        self._send_kafka('charging_complete', payload, encrypt=True)
        
        self.logger.info(f"✅ Finalizada: {session_id}")
        return True
    
    def _print_ticket(self, session_id: str, driver_id: str, kw: float, cost: float,
                     exitosa: bool, razon: str = '', is_manual: bool = False):
        """Imprimir ticket"""
        print("\n" + "="*60)
        print(f"🎫 TICKET - {self.cp_id}")
        if is_manual:
            print("    ⚠️ MODO MANUAL")
        print("="*60)
        print(f"Conductor:      {driver_id}")
        print(f"Sesión:         {session_id}")
        print(f"Energía:        {kw:.2f} kWh")
        print(f"Importe:        {cost:.2f} €")
        if exitosa:
            print("Estado:         ✅ COMPLETADA")
        else:
            print(f"Estado:         ⚠️ INTERRUMPIDA - {razon}")
        print("="*60 + "\n")

    def _handle_command(self, data: Dict[str, Any]):
        """Manejar comando"""
        if data.get('cp_id') != self.cp_id:
            return
        
        self.last_central_contact = time.time()
        command = data.get('command', '')
        self.logger.info(f"📨 Comando: {command}")
        
        if command == 'STOP':
            self._stop_by_central()
        elif command == 'RESUME':
            self._resume_by_central()

    def _stop_by_central(self):
        """Detener por Central"""
        print("\n" + "="*60)
        print("⛔ CP DETENIDO POR CENTRAL")
        print("="*60 + "\n")
        
        with self.lock:
            self.is_stopped_by_central = True
            
            if self.state == 'CHARGING' and self.current_session:
                self.charging_active = False
                time.sleep(0.5)
                
                session_id = self.current_session['session_id']
                driver_id = self.current_session['driver_id']
                kw_total = self.current_session['kw_consumed']
                cost_total = self.current_session['total_cost']
                
                self._print_ticket(session_id, driver_id, kw_total, cost_total,
                                 False, 'Detenido por Central')
                
                payload = {
                    'cp_id': self.cp_id, 
                    'session_id': session_id, 
                    'driver_id': driver_id,
                    'kw_total': kw_total, 
                    'cost_total': cost_total, 
                    'exitosa': False,
                    'razon': 'Detenido por Central', 
                    'timestamp': time.time()
                }
                
                self._send_kafka('charging_complete', payload, encrypt=True)
                
                self.current_session = None
                
                if os.path.exists(self.session_backup_file):
                    try:
                        os.remove(self.session_backup_file)
                    except:
                        pass
            
            self.state = 'STOPPED'
        
        self.logger.warning("⛔ CP PARADO")

    def _resume_by_central(self):
        """Reanudar"""
        with self.lock:
            self.is_stopped_by_central = False
            self.state = 'IDLE'
        
        print("\n▶️ CP REANUDADO\n")
        self.logger.info("▶️ REANUDADO")

    def simular_averia(self):
        """Simular avería"""
        print("\n" + "="*60)
        print("💥 SIMULANDO AVERÍA")
        print("="*60 + "\n")
        
        with self.lock:
            self.is_healthy = False
            
            if self.state == 'CHARGING' and self.current_session:
                self.charging_active = False
                time.sleep(0.5)
                
                # NO eliminar backup para poder recuperar
                self.current_session = None
            
            self.state = 'IDLE'
        
        self.logger.error("💥 AVERÍA")

    def resolver_averia(self):
        """Resolver avería"""
        with self.lock:
            self.is_healthy = True
            self.state = 'IDLE'
        
        print("\n🔧 AVERÍA RESUELTA\n")
        self.logger.info("🔧 RESUELTA")

    def _send_kafka(self, topic: str, payload: Dict[str, Any], encrypt: bool = False):
        """Enviar a Kafka con cifrado condicional robusto"""
        try:
            if self.producer:
                final_payload = payload
                
                if encrypt and self.encryption_key and CRYPTO_AVAILABLE:
                    try:
                        encrypted_data = CryptoManager.encrypt_json(payload, self.encryption_key)
                        final_payload = {
                            'encrypted': True,
                            'data': encrypted_data,
                            'cp_id': self.cp_id
                        }
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error cifrando, enviando sin cifrar: {e}")
                        # Continuar con payload sin cifrar
                
                self.producer.send(topic, final_payload)
                self.producer.flush(timeout=5)
        except Exception as e:
            self.logger.error(f"❌ Error Kafka: {e}")
    
    def _show_help(self):
        """Mostrar ayuda"""
        print("\n" + "="*60)
        print(f"ENGINE {self.cp_id} - COMANDOS")
        print("="*60)
        print("  1    - Enchufar vehículo (iniciar carga)")
        print("  1m   - Carga MANUAL (emergencia)")
        print("  2    - Desenchufar (finalizar carga)")
        print("  3    - Simular avería")
        print("  4    - Resolver avería")
        print("  5    - Ver estado")
        print("  help - Ayuda")
        print("  q    - Salir")
        print("="*60)
    
    def _interactive_mode(self):
        """Modo interactivo - CORREGIDO: manejo robusto de 'q' durante carga"""
        self._show_help()
        
        try:
            while self.running:
                cmd = input(f"\n[{self.cp_id}]> ").strip().lower()
                
                if not cmd:
                    continue
                
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == '1':
                    self.iniciar_carga()
                elif command == '1m':
                    self.iniciar_carga_manual()
                elif command == '2':
                    self.finalizar_carga()
                elif command == '3':
                    self.simular_averia()
                elif command == '4':
                    self.resolver_averia()
                elif command == '5':
                    with self.lock:
                        print("\n" + "="*60)
                        print("ESTADO DEL ENGINE")
                        print("="*60)
                        print(f"Estado:          {self.state}")
                        print(f"Salud:           {'✅ OK' if self.is_healthy else '❌ AVERIADO'}")
                        print(f"Parado Central:  {'✅ Sí' if self.is_stopped_by_central else '❌ No'}")
                        print(f"Cifrado:         {'✅ Activo' if self.encryption_key else '❌ Sin clave'}")
                        
                        if self.current_session:
                            print(f"\n🔋 SESIÓN ACTIVA:")
                            print(f"  ID:          {self.current_session['session_id']}")
                            print(f"  Driver:      {self.current_session['driver_id']}")
                            print(f"  Consumo:     {self.current_session['kw_consumed']:.2f} kWh")
                            print(f"  Coste:       {self.current_session['total_cost']:.2f} €")
                            print(f"  Manual:      {'✅ Sí' if self.current_session.get('manual') else '❌ No'}")
                        else:
                            print("\n🔋 Sin sesión activa")
                        
                        # Info de backup
                        if os.path.exists(self.session_backup_file):
                            print("\n💾 Hay sesión guardada en disco")
                        
                        print("="*60 + "\n")
                
                elif command == 'help':
                    self._show_help()
                
                elif command in ('q', 'quit', 'exit'):
                    # CRÍTICO: Manejo correcto de salida durante carga
                    with self.lock:
                        if self.state == 'CHARGING' and self.current_session:
                            print("\n⚠️ HAY UNA CARGA EN PROGRESO")
                            resp = input("¿Detener carga y salir? (s/n): ").lower()
                            
                            if resp == 's':
                                print("\n🛑 Deteniendo carga...")
                                self.charging_active = False
                                time.sleep(0.5)
                                
                                # Guardar backup ANTES de salir
                                self._save_session_backup()
                                
                                # NO notificar a Central - dejar que se recupere al reiniciar
                                self.logger.info("💾 Sesión guardada para recuperación")
                                break
                            else:
                                print("Cancelado. Carga continúa.")
                                continue
                        else:
                            break
            
        except (KeyboardInterrupt, EOFError):
            print("\n\n🛑 Interrupción detectada...")
            
            # CRÍTICO: Guardar sesión si existe
            with self.lock:
                if self.state == 'CHARGING' and self.current_session:
                    print("💾 Guardando sesión en progreso...")
                    self.charging_active = False
                    time.sleep(0.5)
                    self._save_session_backup()
                    self.logger.info("✅ Sesión guardada para recuperación posterior")
        
        finally:
            self.shutdown()
        
    def shutdown(self):
        """Apagar Engine - CORREGIDO: preservar sesión activa"""
        self.logger.info("🛑 Apagando Engine...")
        self.running = False
        self.charging_active = False
        
        # CRÍTICO: Guardar sesión activa antes de cerrar
        with self.lock:
            if self.current_session:
                try:
                    self._save_session_backup()
                    self.logger.info("💾 Sesión guardada en backup para recuperación")
                except Exception as e:
                    self.logger.error(f"❌ Error guardando sesión: {e}")
        
        # Cerrar conexiones
        if self.health_server:
            try:
                self.health_server.close()
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
        
        self.logger.info("✅ Engine apagado correctamente")


if __name__ == '__main__':
    cp_id = os.getenv('CP_ID', 'CP001')
    listen_port = int(os.getenv('LISTEN_PORT', '6000'))
    price = float(os.getenv('PRICE_PER_KWH', '0.50'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    engine = ChargingPointEngine(cp_id, listen_port, price, kafka_servers)
    engine.start()