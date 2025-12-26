"""
EV_CP_E - Engine Completo y Funcional
Con persistencia, cifrado y recuperación automática
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
        self._load_encryption_key()

    def _load_encryption_key(self):
        """Cargar encryption key del Monitor"""
        key_file = f'/tmp/{self.cp_id}_encryption_key.txt'
        
        for i in range(30):
            if os.path.exists(key_file):
                try:
                    with open(key_file, 'r') as f:
                        self.encryption_key = f.read().strip()
                    self.logger.info(f"🔑 Key cargada: {self.encryption_key[:20]}...")
                    return
                except Exception as e:
                    self.logger.error(f"❌ Error cargando key: {e}")
            time.sleep(1)
        
        self.logger.warning("⚠️ No hay encryption key")
        
    def start(self) -> bool:
        """Iniciar Engine"""
        self.logger.info("="*60)
        self.logger.info(f"ENGINE {self.cp_id} - VERSIÓN COMPLETA")
        self.logger.info("="*60)
        
        # 1. Cargar sesión guardada
        self._load_session_backup()
        
        # 2. Iniciar health server
        if not self._init_health_server():
            return False
        
        # 3. Iniciar Kafka
        if not self._init_kafka():
            return False
        
        # 4. Enviar sesión recuperada
        if hasattr(self, 'recovered_session') and self.recovered_session:
            threading.Timer(3.0, self._send_recovered_session).start()
        
        self.logger.info(f"✅ Engine {self.cp_id} listo")
        self._interactive_mode()
        return True

    def _load_session_backup(self):
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
                    self.recovered_session = backup
            except Exception as e:
                self.logger.error(f"❌ Error cargando backup: {e}")
                self.recovered_session = None
        else:
            self.recovered_session = None

    def _send_recovered_session(self):
        """Enviar sesión recuperada a Central"""
        if not hasattr(self, 'recovered_session') or not self.recovered_session:
            return
        
        try:
            payload = {
                'cp_id': self.cp_id,
                'session_id': self.recovered_session['session_id'],
                'driver_id': self.recovered_session['driver_id'],
                'kw_total': self.recovered_session['kw_consumed'],
                'cost_total': self.recovered_session['total_cost'],
                'exitosa': False,
                'razon': self.recovered_session['razon'],
                'timestamp': time.time()
            }
            
            self._send_kafka('charging_complete', payload, encrypt=True)
            
            self.logger.info("✅ Sesión recuperada enviada")
            
            if os.path.exists(self.session_backup_file):
                os.remove(self.session_backup_file)
            
            self.recovered_session = None
        except Exception as e:
            self.logger.error(f"❌ Error enviando sesión: {e}")

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
                    self.logger.error(f"❌ Health error: {e}")

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
                                    decrypted_data = CryptoManager.decrypt_json(encrypted_data, self.encryption_key)
                                    data = decrypted_data
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
                if self.running:
                    if "timed out" not in str(e).lower():
                        self.logger.error(f"❌ Consumer error: {e}")
                    time.sleep(1)

    def _handle_authorization(self, data: Dict[str, Any]):
        """Manejar autorización"""
        if data.get('cp_id') != self.cp_id:
            return
        
        self.last_central_contact = time.time()
        
        with self.lock:
            if self.is_stopped_by_central:
                self.logger.warning("⚠️ CP parado")
                return
            
            if self.current_session is not None:
                self.logger.warning("⚠️ Ya hay sesión")
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
        """Carga manual (emergencia)"""
        if time.time() - self.last_central_contact > 30:
            print("\n⚠️ Central no responde. Modo emergencia...")
        else:
            resp = input("Central activa. ¿Continuar manual? (s/n): ").lower()
            if resp != 's':
                return False
        
        driver_id = input("ID conductor: ").strip()
        if not driver_id:
            return False
        
        with self.lock:
            if self.current_session or self.is_stopped_by_central:
                print("❌ No se puede iniciar manual\n")
                return False
            
            session_id = f"MANUAL_{self.cp_id}_{int(time.time())}"
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
        
        print("\n⚡ CARGA MANUAL (EMERGENCIA)")
        print(f"Driver: {driver_id}")
        print("👉 Pulsa '2' para finalizar\n")
        
        threading.Thread(target=self._charging_loop, daemon=True).start()
        return True

    def _charging_loop(self):
        """Loop de carga"""
        last_log_time = 0
        last_send_time = 0
        
        while self.charging_active and self.running:
            with self.lock:
                if self.current_session is None:
                    break
                
                if self.is_stopped_by_central:
                    break
                
                # Simular consumo
                kw_rate = random.uniform(7.0, 22.0) / 3600
                self.current_session['kw_consumed'] += kw_rate
                self.current_session['total_cost'] = self.current_session['kw_consumed'] * self.current_session['price']
                
                # Guardar backup
                self._save_session_backup()
                
                # Enviar datos cada 2s
                current_time = time.time()
                is_manual = self.current_session.get('manual', False)
                central_alive = (current_time - self.last_central_contact < 30)
                
                if (not is_manual or central_alive) and (current_time - last_send_time >= 2):
                    payload = {
                        'cp_id': self.cp_id,
                        'session_id': self.current_session['session_id'],
                        'driver_id': self.current_session['driver_id'],
                        'kw': self.current_session['kw_consumed'],
                        'cost': self.current_session['total_cost'],
                        'timestamp': time.time()
                    }
                    
                    self._send_kafka('charging_data', payload, encrypt=True)
                    last_send_time = current_time
                
                # Log cada 5s
                if current_time - last_log_time >= 5:
                    elapsed = int(current_time - self.current_session['start_time'])
                    self.logger.info(f"📊 {self.current_session['kw_consumed']:.2f} kWh | "
                                   f"{self.current_session['total_cost']:.2f} € | {elapsed}s")
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
                
                session_id = self.current_session['session_id']
                driver_id = self.current_session['driver_id']
                kw_total = self.current_session['kw_consumed']
                cost_total = self.current_session['total_cost']
                
                self._print_ticket(session_id, driver_id, kw_total, cost_total,
                                 False, 'Avería del Engine')
                
                payload = {
                    'cp_id': self.cp_id, 
                    'session_id': session_id, 
                    'driver_id': driver_id,
                    'kw_total': kw_total, 
                    'cost_total': cost_total, 
                    'exitosa': False,
                    'razon': 'Avería del Engine', 
                    'timestamp': time.time()
                }
                
                self._send_kafka('charging_complete', payload, encrypt=True)
                
                # NO eliminar backup
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
        
        # Cargar sesión
        self._load_session_backup()
        if hasattr(self, 'recovered_session') and self.recovered_session:
            self._send_recovered_session()

    def _send_kafka(self, topic: str, payload: Dict[str, Any], encrypt: bool = False):
        """Enviar a Kafka"""
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
                        self.logger.warning(f"⚠️ Error cifrando: {e}")
                
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
        """Modo interactivo"""
        self._show_help()
        
        try:
            while self.running:
                cmd = input(f"\n[{self.cp_id}]> ").strip().lower()
                
                if cmd == '1':
                    self.iniciar_carga()
                elif cmd == '1m':
                    self.iniciar_carga_manual()
                elif cmd == '2':
                    self.finalizar_carga()
                elif cmd == '3':
                    self.simular_averia()
                elif cmd == '4':
                    self.resolver_averia()
                elif cmd == '5':
                    with self.lock:
                        print("\n" + "="*60)
                        print("ESTADO")
                        print("="*60)
                        print(f"Estado:   {self.state}")
                        print(f"Salud:    {'✅ OK' if self.is_healthy else '❌ AVERIADO'}")
                        print(f"Parado:   {'✅' if self.is_stopped_by_central else '❌'}")
                        print(f"Cifrado:  {'✅' if self.encryption_key else '❌'}")
                        
                        if self.current_session:
                            print(f"\n📋 SESIÓN:")
                            print(f"  ID:      {self.current_session['session_id']}")
                            print(f"  Driver:  {self.current_session['driver_id']}")
                            print(f"  Consumo: {self.current_session['kw_consumed']:.2f} kWh")
                            print(f"  Coste:   {self.current_session['total_cost']:.2f} €")
                        else:
                            print("\n📋 Sin sesión")
                        print("="*60 + "\n")
                elif cmd == 'help':
                    self._show_help()
                elif cmd in ('q', 'quit', 'exit'):
                    break
        except (KeyboardInterrupt, EOFError):
            print("\n\n🛑 Saliendo...\n")
        finally:
            self.shutdown()

    def shutdown(self):
        """Apagar Engine"""
        self.running = False
        self.charging_active = False
        
        if self.current_session:
            self._save_session_backup()
        
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
        
        self.logger.info("✅ Engine apagado")


if __name__ == '__main__':
    cp_id = os.getenv('CP_ID', 'CP001')
    listen_port = int(os.getenv('LISTEN_PORT', '6000'))
    price = float(os.getenv('PRICE_PER_KWH', '0.50'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    engine = ChargingPointEngine(cp_id, listen_port, price, kafka_servers)
    engine.start()