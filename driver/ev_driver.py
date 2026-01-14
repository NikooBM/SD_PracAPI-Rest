"""
EV_Driver
"""
import json
import time
import os
import threading
import logging
import pickle
from collections import deque
from typing import Optional, Dict, Any, List
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')


class Driver:
    def __init__(self, driver_id: str, kafka_servers: str):
        self.driver_id = driver_id
        self.kafka_servers = kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        
        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True
        
        self.current_service: Optional[Dict[str, Any]] = None
        self.pending_services: List[str] = []
        
        # Datos en tiempo real
        self.realtime_lock = threading.Lock()
        self.realtime_data: Dict[str, Any] = {}
        self.last_realtime_update = 0
        
        self.message_buffer = deque(maxlen=50)
        self.message_lock = threading.Lock()
        
        self.processed_messages = set()
        self.processed_lock = threading.Lock()
        
        self.state_file = f'/tmp/driver_{driver_id}_state.pkl'
        
        self.logger = logging.getLogger(f"Driver-{driver_id}")
        
        self.show_clean_prompt = threading.Event()

    def start(self) -> bool:
        self.logger.info("="*60)
        self.logger.info(f"DRIVER {self.driver_id} INICIANDO...")
        self.logger.info("="*60)
        
        self._load_state()
        
        if not self._init_kafka():
            self.logger.error("❌ Kafka no disponible")
            return False
        
        time.sleep(1)
        self.logger.info("✅ Driver listo")
        
        if self.current_service:
            self.logger.info(f"📋 Servicio recuperado: {self.current_service}")
        
        threading.Thread(target=self._realtime_display_loop, daemon=True).start()
        
        self._interactive_mode()
        return True

    def _load_state(self):
        """Cargar estado guardado - MEJORADO con recuperación de sesión activa"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'rb') as f:
                    state = pickle.load(f)
                    self.current_service = state.get('current_service')
                    self.pending_services = state.get('pending_services', [])
                    self.message_buffer = deque(state.get('messages', []), maxlen=50)
                    
                    # NUEVO: Si había sesión activa, intentar recuperarla
                    if self.current_service:
                        status = self.current_service.get('status', '')
                        if status == 'authorized':
                            self.logger.info(f"📂 Sesión autorizada recuperada: {self.current_service}")
                            
                            # Re-solicitar datos de carga actuales
                            threading.Timer(2.0, self._request_session_update).start()
                    
                    self.logger.info("📂 Estado recuperado")
            except Exception as e:
                self.logger.error(f"❌ Error cargando estado: {e}")
                
    def _request_session_update(self):
        """Solicitar actualización de sesión activa (después de reconexión)"""
        if not self.current_service:
            return
        
        cp_id = self.current_service.get('cp_id')
        if not cp_id:
            return
        
        self.logger.info(f"🔄 Solicitando actualización de sesión en {cp_id}")
        
        # La Central enviará datos automáticamente si la carga está activa
        # Solo necesitamos asegurarnos de estar escuchando
        with self.message_lock:
            msg = f"[{time.strftime('%H:%M:%S')}] 🔄 Reconectado - esperando datos de {cp_id}"
            self.message_buffer.append(msg)
            print(f"\n{msg}\n")


    def _save_state(self):
        """Guardar estado actual"""
        try:
            state = {
                'current_service': self.current_service,
                'pending_services': self.pending_services,
                'messages': list(self.message_buffer)
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(state, f)
        except Exception as e:
            self.logger.error(f"❌ Error guardando estado: {e}")

    def _init_kafka(self) -> bool:
        """Inicializar conexión Kafka"""
        for attempt in range(1, 16):
            try:
                self.logger.info(f"🔄 Conectando a Kafka ({attempt}/15)...")
                
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5, 
                    request_timeout_ms=30000,
                    max_block_ms=10000)
                
                self.consumer = KafkaConsumer(
                    'driver_notifications', 'charging_data',
                    bootstrap_servers=self.kafka_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='earliest',
                    group_id=f'driver-{self.driver_id}',
                    enable_auto_commit=True,
                    session_timeout_ms=30000,
                    consumer_timeout_ms=1000)
                
                threading.Thread(target=self._listen_notifications, daemon=True).start()
                self.logger.info("✅ Kafka conectado")
                return True
            except KafkaError as e:
                self.logger.error(f"❌ Error Kafka: {e}")
                if attempt < 15:
                    time.sleep(5)
        return False

    def _listen_notifications(self):
        """Thread que escucha notificaciones de Kafka"""
        while self.running:
            try:
                if self.consumer is None:
                    break
                
                for msg in self.consumer:
                    if not self.running:
                        break
                    
                    try:
                        data = msg.value
                        
                        if msg.topic == 'charging_data':
                            self._process_realtime_data(data)
                        elif msg.topic == 'driver_notifications':
                            if isinstance(data, dict) and data.get('driver_id') == self.driver_id:
                                self._process_notification(data)
                    except Exception as e:
                        self.logger.error(f"Error procesando mensaje: {e}")
            except Exception as e:
                if self.running and "timed out" not in str(e).lower():
                    self.logger.error(f"❌ Consumer error: {e}")
                    time.sleep(1)

    def _process_realtime_data(self, data: Dict[str, Any]):
        """Procesar datos de carga en tiempo real - CORREGIDO"""
        if self.current_service is None:
            return
        
        driver_id = data.get('driver_id', '')
        if driver_id != self.driver_id:
            return
        
        cp_id = data.get('cp_id', '')
        if self.current_service.get('cp_id') != cp_id:
            return
        
        with self.realtime_lock:
            self.realtime_data = {
                'cp_id': cp_id,
                'kw': float(data.get('kw', 0.0)),
                'cost': float(data.get('cost', 0.0)),
                'timestamp': time.time()
            }
            self.last_realtime_update = time.time()

    def _realtime_display_loop(self):
        """
        Loop para mostrar progreso en tiempo real - CORREGIDO
        Muestra SIEMPRE que hay datos de carga
        """
        last_display = 0
        last_line_length = 0
        
        while self.running:
            try:
                current = time.time()
                
                with self.realtime_lock:
                    if self.realtime_data and self.current_service:
                        # Mostrar cada segundo
                        if current - last_display >= 1:
                            # Limpiar línea anterior
                            if last_line_length > 0:
                                print('\r' + ' ' * last_line_length + '\r', end='', flush=True)
                            
                            # MOSTRAR PROGRESO CLARAMENTE
                            line = (f"\r⚡ CARGANDO: {self.realtime_data['kw']:.2f} kWh | "
                                f"💶 {self.realtime_data['cost']:.2f} € | "
                                f"CP: {self.realtime_data['cp_id']}")
                            
                            print(line, end='', flush=True)
                            last_line_length = len(line)
                            last_display = current
                        
                        # Timeout si no hay actualización en 15s
                        if current - self.last_realtime_update > 15:
                            if last_line_length > 0:
                                print('\r' + ' ' * last_line_length + '\r', end='', flush=True)
                                last_line_length = 0
                            self.realtime_data = {}
                            self.show_clean_prompt.set()
                    
                    elif last_line_length > 0:
                        # Limpiar si no hay datos
                        print('\r' + ' ' * last_line_length + '\r', end='', flush=True)
                        last_line_length = 0
                
                time.sleep(0.5)
            except Exception as e:
                if self.running:
                    pass

    def _process_notification(self, data: Dict[str, Any]):
        """Procesar notificación - MEJORADO con reconexión"""
        # Generar ID único
        msg_id = f"{data.get('status')}_{data.get('cp_id')}_{data.get('session_id', '')}_{int(data.get('timestamp', 0))}"
        
        with self.processed_lock:
            if msg_id in self.processed_messages:
                return
            self.processed_messages.add(msg_id)
            if len(self.processed_messages) > 100:
                self.processed_messages.clear()
        
        timestamp = time.strftime("%H:%M:%S")
        
        with self.message_lock:
            status = data.get('status', '').upper()
            msg_type = data.get('type', '')
            cp_id = data.get('cp_id', 'N/A')
            
            # CRÍTICO: Si recibimos CHARGING_UPDATE y no teníamos sesión activa,
            # significa que nos reconectamos durante una carga
            if msg_type == 'CHARGING_UPDATE':
                if not self.current_service or self.current_service.get('status') != 'authorized':
                    # Recuperar sesión activa
                    driver_id = data.get('driver_id')
                    if driver_id == self.driver_id:
                        self.logger.info(f"🔄 Sesión activa detectada en {cp_id} - recuperando...")
                        self.current_service = {
                            'cp_id': cp_id,
                            'status': 'authorized',
                            'authorized_at': time.time()
                        }
                        self._save_state()
                        
                        print(f"\n{'='*60}")
                        print(f"🔄 SESIÓN RECUPERADA EN {cp_id}")
                        print(f"{'='*60}")
                        print("⚡ Carga en progreso detectada")
                        print(f"{'='*60}\n")
                
                # Actualizar datos en tiempo real
                with self.realtime_lock:
                    self.realtime_data = {
                        'cp_id': cp_id,
                        'kw': float(data.get('kw', 0.0)),
                        'cost': float(data.get('cost', 0.0)),
                        'timestamp': time.time()
                    }
                    self.last_realtime_update = time.time()
                return  # No mostrar mensaje, solo actualizar display

            if status == 'AUTHORIZED':
                msg = f"[{timestamp}] ✅ AUTORIZADO en {cp_id}"
                self.message_buffer.append(msg)
                
                if self.current_service:
                    old_cp = self.current_service.get('cp_id')
                    if old_cp != cp_id:
                        self.logger.warning(f"⚠️ Nueva autorización en {cp_id} (anterior: {old_cp})")
                
                self.current_service = {
                    'cp_id': cp_id, 
                    'status': 'authorized',
                    'authorized_at': time.time()
                }
                
                with self.realtime_lock:
                    self.realtime_data = {}
                
                self._save_state()
                
                print(f"\n\n{'='*60}")
                print(f"{msg}")
                print(f"{'='*60}")
                print("⏳ Esperando que el CP inicie la carga...")
                print("💡 El operador debe conectar el vehículo y pulsar '1' en el Engine")
                print(f"{'='*60}\n")
                
                self.show_clean_prompt.set()
            
            elif status == 'DENIED':
                msg = f"[{timestamp}] ❌ DENEGADO en {cp_id}: {data.get('message', '')}"
                self.message_buffer.append(msg)
                
                if self.current_service and self.current_service.get('cp_id') == cp_id:
                    self.current_service = None
                
                with self.realtime_lock:
                    self.realtime_data = {}
                
                self._save_state()
                print(f"\n{msg}")
                self.show_clean_prompt.set()
                self._schedule_next_service()
            
            # TICKET FINAL - MEJORADO con validación
            elif msg_type == 'FINAL_TICKET' or 'kw_total' in data:
                # Limpiar línea de progreso
                print("\n" + " "*100 + "\r", end='', flush=True)
                
                # VALIDAR que el ticket es para este driver
                ticket_driver = data.get('driver_id')
                if ticket_driver != self.driver_id:
                    self.logger.warning(f"⚠️ Ticket recibido para otro driver: {ticket_driver}")
                    return
                
                # Guardar ticket
                ticket_data = {
                    'timestamp': timestamp,
                    'cp_id': data.get('cp_id'),
                    'session_id': data.get('session_id'),
                    'kw_total': float(data.get('kw_total', 0)),
                    'cost_total': float(data.get('cost_total', 0)),
                    'exitosa': data.get('exitosa', True),
                    'razon': data.get('razon', '')
                }
                
                try:
                    with open(f'/tmp/driver_{self.driver_id}_tickets.json', 'a') as f:
                        json.dump(ticket_data, f)
                        f.write('\n')
                except:
                    pass
                
                # IMPRIMIR TICKET
                self._print_ticket(data, timestamp)
                
                # CRÍTICO: Limpiar estado completamente
                self.current_service = None
                
                with self.realtime_lock:
                    self.realtime_data = {}
                
                self._save_state()
                self.show_clean_prompt.set()
                self._schedule_next_service()
            
    def _print_ticket(self, data: Dict[str, Any], timestamp: str):
        """Imprimir ticket de recarga - MEJORADO"""
        cp_id = data.get('cp_id', 'N/A')
        session_id = data.get('session_id', 'N/A')
        kw_total = data.get('kw_total', 0)
        cost_total = data.get('cost_total', 0)
        exitosa = data.get('exitosa', True)
        razon = data.get('razon', '')
        
        print("\n")
        print("="*60)
        print("🎫 TICKET DE RECARGA - EVCharging")
        print("="*60)
        print(f"Hora:           {timestamp}")
        print(f"Conductor:      {self.driver_id}")
        print(f"CP:             {cp_id}")
        print(f"Sesión:         {session_id}")
        print("-"*60)
        print(f"Energía:        {kw_total:.2f} kWh")
        print(f"Importe:        {cost_total:.2f} €")
        print("-"*60)
        
        if exitosa:
            print("Estado:         ✅ COMPLETADA")
            print("\n¡Gracias por usar EVCharging!")
        else:
            print(f"Estado:         ⚠️ INTERRUMPIDA")
            print(f"Motivo:         {razon}")
        
        print("="*60 + "\n")
        
        # Guardar en buffer
        ticket_lines = [
            f"[{timestamp}] 🎫 TICKET FINAL",
            f"    CP: {cp_id}, Sesión: {session_id}",
            f"    {kw_total:.2f} kWh, {cost_total:.2f} €",
            f"    {'✅ Completada' if exitosa else f'⚠️ {razon}'}"
        ]
        
        with self.message_lock:
            for line in ticket_lines:
                self.message_buffer.append(line)

    def _schedule_next_service(self):
        """Programar próximo servicio"""
        if self.pending_services:
            threading.Timer(4.0, self._request_next_service).start()

    def solicitar_servicio(self, cp_id: str) -> bool:
        """Solicitar servicio en un CP"""
        if not cp_id:
            print("❌ CP_ID inválido")
            return False
        
        print(f"\n📤 Solicitando carga en {cp_id}...")
        
        try:
            if self.producer is None:
                print("❌ Kafka no conectado")
                return False
            
            self.producer.send('service_requests', {
                'driver_id': self.driver_id,
                'cp_id': cp_id,
                'timestamp': time.time()
            })
            self.producer.flush()
            
            print("✅ Solicitud enviada")
            self.current_service = {'cp_id': cp_id, 'status': 'waiting'}
            self._save_state()
            return True
        except Exception as e:
            print(f"❌ Error: {e}")
            return False

    def cargar_servicios_desde_archivo(self, filepath: str) -> bool:
        """Cargar lista de servicios desde archivo"""
        try:
            if not os.path.exists(filepath):
                print(f"❌ Archivo no encontrado: {filepath}")
                return False
            
            with open(filepath, 'r') as f:
                lines = [line.strip() for line in f if line.strip()]
            
            if not lines:
                print("❌ Archivo vacío")
                return False
            
            self.pending_services = lines
            self._save_state()
            print(f"✅ {len(lines)} servicios cargados")
            self._request_next_service()
            return True
        except Exception as e:
            self.logger.error(f"❌ Error: {e}")
            return False

    def _request_next_service(self):
        """Solicitar próximo servicio de la lista"""
        if not self.pending_services:
            print("\n✅ Todos los servicios completados")
            self._save_state()
            return
        
        cp_id = self.pending_services.pop(0)
        self._save_state()
        print(f"\n📋 Siguiente: {cp_id} ({len(self.pending_services)} restantes)")
        self.solicitar_servicio(cp_id)

    def mostrar_mensajes(self):
        """Mostrar mensajes recibidos"""
        with self.message_lock:
            if not self.message_buffer:
                print("\n📭 No hay mensajes")
            else:
                print("\n" + "="*60)
                print("📨 MENSAJES RECIBIDOS")
                print("="*60)
                for msg in self.message_buffer:
                    print(msg)
                print("="*60)

    def mostrar_estado(self):
        """Mostrar estado completo"""
        print("\n" + "="*60)
        print(f"ESTADO DE {self.driver_id}")
        print("="*60)
        
        if self.current_service:
            print(f"Servicio actual:")
            print(f"  CP:     {self.current_service.get('cp_id', 'N/A')}")
            print(f"  Estado: {self.current_service.get('status', 'N/A')}")
            
            with self.realtime_lock:
                if self.realtime_data:
                    print(f"\n📊 Datos en tiempo real:")
                    print(f"  Consumo: {self.realtime_data.get('kw', 0):.2f} kWh")
                    print(f"  Coste:   {self.realtime_data.get('cost', 0):.2f} €")
        else:
            print("Sin servicio activo")
        
        if self.pending_services:
            print(f"\nPendientes: {len(self.pending_services)}")
            print(f"Próximos: {', '.join(self.pending_services[:3])}")
        
        with self.message_lock:
            if self.message_buffer:
                print(f"\nMensajes: {len(self.message_buffer)}")
        
        print("="*60)

    def _show_help(self):
        """Mostrar ayuda"""
        print("\n" + "="*60)
        print("COMANDOS DISPONIBLES")
        print("="*60)
        print("  request <CP_ID>     - Solicitar carga en un CP")
        print("  file <ruta>         - Cargar lista de servicios")
        print("  msg                 - Ver mensajes recibidos")
        print("  status              - Ver estado completo")
        print("  help                - Mostrar esta ayuda")
        print("  clear               - Limpiar pantalla")
        print("  quit                - Salir")
        print("="*60)
        print("\nEjemplos:")
        print("  request CP001")
        print("  file services/servicios.txt")
        print("="*60)

    def _interactive_mode(self):
        """Modo interactivo"""
        self._show_help()
        
        while self.running:
            try:
                if self.show_clean_prompt.is_set():
                    self.show_clean_prompt.clear()
                    time.sleep(0.2)
                
                prompt = f"\n[{self.driver_id}]> "
                cmd = input(prompt).strip()
                
                if not cmd:
                    continue
                
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()
                
                if command == 'msg':
                    self.mostrar_mensajes()
                
                elif command == 'request' and len(parts) == 2:
                    cp_id = parts[1].upper()
                    self.solicitar_servicio(cp_id)
                
                elif command == 'file' and len(parts) == 2:
                    filepath = parts[1]
                    if not os.path.isabs(filepath):
                        base_path = '/app/driver' if os.path.exists('/app/driver') else '.'
                        filepath = os.path.join(base_path, filepath)
                    self.cargar_servicios_desde_archivo(filepath)
                
                elif command == 'status':
                    self.mostrar_estado()
                
                elif command == 'help':
                    self._show_help()
                
                elif command == 'clear':
                    os.system('clear' if os.name != 'nt' else 'cls')
                
                elif command in ('quit', 'exit', 'q'):
                    with self.realtime_lock:
                        if self.realtime_data:
                            print("\n⚠️ HAY DATOS DE CARGA EN TIEMPO REAL")
                            print("   La carga continuará en el CP")
                            resp = input("¿Desconectar driver? (s/n): ").lower()
                            
                            if resp == 's':
                                print("\n🛑 Guardando estado y saliendo...")
                                print("💡 La carga continúa en el CP")
                                self._save_state()
                                break
                            else:
                                print("Cancelado.")
                                continue
                        else:
                            print("\n🛑 Guardando estado y saliendo...")
                            self._save_state()
                            break
                
                else:
                    print(f"❌ Comando desconocido: '{command}'")
                    print("💡 Usa 'help' para ver comandos disponibles")
            
            except (KeyboardInterrupt, EOFError):
                print("\n\n🛑 Interrumpido por usuario")
                print("Guardando estado...")
                self._save_state()
                break
            
            except Exception as e:
                if self.running:
                    print(f"❌ Error: {e}")
        
        self.shutdown()


    def shutdown(self):
        """Apagar Driver"""
        self.logger.info("🛑 Apagando Driver...")
        self.running = False
        
        try:
            self._save_state()
            self.logger.info("💾 Estado guardado correctamente")
        except Exception as e:
            self.logger.error(f"❌ Error guardando estado: {e}")
        
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
        
        self.logger.info("✅ Driver apagado")


if __name__ == '__main__':
    driver_id = os.getenv('DRIVER_ID', 'Driver_001')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    driver = Driver(driver_id, kafka_servers)
    driver.start()