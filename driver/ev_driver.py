"""EV_Driver — Aplicación del conductor
Release 2 - Práctica SD 25/26
"""
import json
import time
import os
import threading
import logging
import signal
import sys
import tempfile
from collections import deque
from typing import Optional, Dict, Any, List
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

class Driver:
    def __init__(self, driver_id: str, kafka_servers: str):
        self.driver_id = driver_id
        self.kafka_servers = (
            kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        )

        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True

        self.current_service: Optional[Dict[str, Any]] = None
        self.pending_services: List[str] = []

        # Datos en tiempo real (actualizados por CHARGING_UPDATE de driver_notifications)
        self.realtime_lock = threading.Lock()
        self.realtime_data: Dict[str, Any] = {}
        self.last_realtime_update = 0.0

        self.message_buffer: deque = deque(maxlen=50)
        self.message_lock = threading.Lock()
        self.processed_messages: set = set()
        self.processed_lock = threading.Lock()
        self.state_file = f'/tmp/driver_{driver_id}_state.json'

        self.logger = logging.getLogger(f"Driver-{driver_id}")
        self.show_clean_prompt = threading.Event()

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _load_state(self):
        """Carga estado desde JSON. Ignora estado 'authorized' sin datos."""
        if not os.path.exists(self.state_file):
            # Compatibilidad: intentar leer .pkl legado y migrarlo
            pkl_file = self.state_file.replace('.json', '.pkl')
            if os.path.exists(pkl_file):
                try:
                    import pickle
                    with open(pkl_file, 'rb') as f:
                        state = pickle.load(f)
                    self.pending_services = state.get('pending_services', [])
                    # No restauramos current_service desde pkl: puede ser stale
                    self.logger.info("📂 Estado pkl migrado a JSON (current_service descartado)")
                    os.remove(pkl_file)
                    self._save_state()
                except Exception as e:
                    self.logger.warning(f"⚠️ No se pudo migrar estado pkl: {e}")
            return

        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)

            self.pending_services = state.get('pending_services', [])

            saved_service = state.get('current_service')
            # reenviará la autorización tras un reinicio → descartamos para evitar
            # que el driver quede bloqueado esperando una autorización que no llega.
            if saved_service and saved_service.get('status') not in ('authorized', 'waiting'):
                self.current_service = saved_service
            else:
                if saved_service:
                    self.logger.info(
                        "⚠️ Estado '%s' descartado tras reinicio (Central no reenviará auth)",
                        saved_service.get('status')
                    )

            saved_msgs = state.get('messages', [])
            self.message_buffer = deque(saved_msgs, maxlen=50)

            self.logger.info("📂 Estado JSON restaurado (%d pendientes)", len(self.pending_services))
        except Exception as e:
            self.logger.error("❌ Error cargando estado JSON: %s", e)

    def _save_state(self):
        """Escritura atómica del estado en JSON."""
        try:
            state = {
                'current_service': self.current_service,
                'pending_services': self.pending_services,
                'messages': list(self.message_buffer)
            }
            dir_name = os.path.dirname(os.path.abspath(self.state_file))
            with tempfile.NamedTemporaryFile(
                'w', dir=dir_name, delete=False, suffix='.tmp', encoding='utf-8'
            ) as tmp:
                json.dump(state, tmp, ensure_ascii=False)
                tmp_path = tmp.name
            os.replace(tmp_path, self.state_file)
        except Exception as e:
            self.logger.error("❌ Error guardando estado: %s", e)

    # ------------------------------------------------------------------
    # Arranque
    # ------------------------------------------------------------------

    def start(self) -> bool:
        self.logger.info("=" * 60)
        self.logger.info("DRIVER %s INICIANDO...", self.driver_id)
        self.logger.info("=" * 60)

        self._load_state()

        if not self._init_kafka():
            self.logger.error("❌ Kafka no disponible")
            return False

        time.sleep(1)
        self.logger.info("✅ Driver listo")

        threading.Thread(target=self._realtime_display_loop, daemon=True).start()

        self._interactive_mode()
        return True

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                self.logger.info("🔄 Conectando a Kafka (%d/15)...", attempt)

                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5,
                    request_timeout_ms=30_000,
                    max_block_ms=10_000
                )
                self._init_consumer()
                if self.consumer:
                    threading.Thread(
                        target=self._kafka_consumer_loop, daemon=True
                    ).start()
                    self.logger.info("✅ Kafka conectado")
                    return True
            except KafkaError as e:
                self.logger.error("❌ Error Kafka: %s", e)
                if attempt < 15:
                    time.sleep(5)
        return False

    def _init_consumer(self):
        """Solo suscribe a driver_notifications — ya no a charging_data directamente."""
        try:
            self.consumer = KafkaConsumer(
                'driver_notifications',
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=f'driver-{self.driver_id}',
                enable_auto_commit=True,
                session_timeout_ms=30_000,
                consumer_timeout_ms=1000
            )
        except Exception as e:
            self.logger.error("❌ No se pudo crear consumer: %s", e)
            self.consumer = None

    def _kafka_consumer_loop(self):
        """Loop con reinicio automático ante fallos de Kafka."""
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
                        data = msg.value
                        if isinstance(data, dict) and data.get('driver_id') == self.driver_id:
                            self._process_notification(data)
                    except Exception as e:
                        self.logger.error("Error procesando mensaje: %s", e)
            except Exception as e:
                if self.running and 'timed out' not in str(e).lower():
                    self.logger.error("❌ Consumer error, reiniciando: %s", e)
                    try:
                        if self.consumer:
                            self.consumer.close()
                    except Exception:
                        pass
                    self.consumer = None
                    time.sleep(5)

    # ------------------------------------------------------------------
    # Procesamiento de notificaciones
    # ------------------------------------------------------------------

    def _process_notification(self, data: Dict[str, Any]):
        """Deduplicación por session_id + tipo (no por timestamp)."""
        session_id = data.get('session_id', '')
        msg_type = data.get('type', data.get('status', ''))
        # Clave de deduplicación: session_id + tipo de mensaje
        # Para mensajes sin session_id (AUTHORIZED/DENIED), usamos cp_id + status
        if session_id:
            dedup_key = f"{session_id}|{msg_type}"
        else:
            dedup_key = f"{data.get('cp_id', '')}|{msg_type}|{int(data.get('timestamp', 0))}"

        with self.processed_lock:
            if dedup_key in self.processed_messages:
                self.logger.debug("Mensaje duplicado ignorado: %s", dedup_key)
                return
            self.processed_messages.add(dedup_key)
            # Limpiar set cuando supera 200 entradas
            if len(self.processed_messages) > 200:
                self.processed_messages.clear()

        timestamp_str = time.strftime("%H:%M:%S")
        status = data.get('status', '').upper()
        cp_id = data.get('cp_id', 'N/A')

        with self.message_lock:
            # CHARGING_UPDATE: actualizar display en tiempo real
            if msg_type == 'CHARGING_UPDATE':
                with self.realtime_lock:
                    self.realtime_data = {
                        'cp_id': cp_id,
                        'kw': float(data.get('kw', 0.0)),
                        'cost': float(data.get('cost', 0.0)),
                        'timestamp': time.time()
                    }
                    self.last_realtime_update = time.time()
                return

            # AUTHORIZED
            if status == 'AUTHORIZED':
                msg = f"[{timestamp_str}] ✅ AUTORIZADO en {cp_id}"
                self.message_buffer.append(msg)
                self.current_service = {
                    'cp_id': cp_id,
                    'status': 'authorized',
                    'authorized_at': time.time()
                }
                with self.realtime_lock:
                    self.realtime_data = {}
                self._save_state()
                print(f"\n\n{'='*60}")
                print(msg)
                print(f"{'='*60}")
                print("⏳ Esperando que el CP inicie la carga...")
                print("💡 El operador debe conectar el vehículo y pulsar '1' en el Engine")
                print(f"{'='*60}\n")
                self.show_clean_prompt.set()

            # DENIED
            elif status == 'DENIED':
                msg = f"[{timestamp_str}] ❌ DENEGADO en {cp_id}: {data.get('message', '')}"
                self.message_buffer.append(msg)
                if self.current_service and self.current_service.get('cp_id') == cp_id:
                    self.current_service = None
                with self.realtime_lock:
                    self.realtime_data = {}
                self._save_state()
                print(f"\n{msg}")
                self.show_clean_prompt.set()
                self._schedule_next_service()

            # FINAL_TICKET
            elif msg_type == 'FINAL_TICKET' or 'kw_total' in data:
                # Limpiar línea de progreso
                print("\n" + " " * 80 + "\r", end='', flush=True)
                self._print_ticket(data, timestamp_str)
                self.current_service = None
                with self.realtime_lock:
                    self.realtime_data = {}
                # Guardar ticket en disco
                try:
                    with open(f'/tmp/driver_{self.driver_id}_tickets.json', 'a',
                              encoding='utf-8') as f:
                        json.dump({**data, 'logged_at': timestamp_str}, f, ensure_ascii=False)
                        f.write('\n')
                except Exception:
                    pass
                self._save_state()
                self.show_clean_prompt.set()
                self._schedule_next_service()

    def _print_ticket(self, data: Dict[str, Any], timestamp_str: str):
        cp_id = data.get('cp_id', 'N/A')
        session_id = data.get('session_id', 'N/A')
        kw_total = float(data.get('kw_total', 0))
        cost_total = float(data.get('cost_total', 0))
        exitosa = data.get('exitosa', True)
        razon = data.get('razon', '')

        print("\n" + "=" * 60)
        print("🎫 TICKET DE RECARGA - EVCharging")
        print("=" * 60)
        print(f"Hora:           {timestamp_str}")
        print(f"Conductor:      {self.driver_id}")
        print(f"CP:             {cp_id}")
        print(f"Sesión:         {session_id}")
        print("-" * 60)
        print(f"Energía:        {kw_total:.2f} kWh")
        print(f"Importe:        {cost_total:.2f} €")
        print("-" * 60)
        if exitosa:
            print("Estado:         ✅ COMPLETADA")
            print("\n¡Gracias por usar EVCharging!")
        else:
            print(f"Estado:         ⚠️ INTERRUMPIDA")
            print(f"Motivo:         {razon}")
        print("=" * 60 + "\n")

        with self.message_lock:
            self.message_buffer.extend([
                f"[{timestamp_str}] 🎫 TICKET FINAL",
                f"    CP:{cp_id} | {kw_total:.2f}kWh | {cost_total:.2f}€ | "
                f"{'✅' if exitosa else '⚠️'}"
            ])

    # ------------------------------------------------------------------
    # Realtime display
    # ------------------------------------------------------------------

    def _realtime_display_loop(self):
        last_display = 0.0
        last_line_length = 0

        while self.running:
            try:
                current = time.time()
                with self.realtime_lock:
                    has_data = bool(self.realtime_data) and bool(self.current_service)
                    if has_data and current - last_display >= 1.0:
                        if last_line_length > 0:
                            print('\r' + ' ' * last_line_length + '\r', end='', flush=True)
                        line = (f"\r⚡ CARGANDO: {self.realtime_data['kw']:.2f} kWh | "
                                f"💶 {self.realtime_data['cost']:.2f} € | "
                                f"CP: {self.realtime_data['cp_id']}")
                        print(line, end='', flush=True)
                        last_line_length = len(line)
                        last_display = current

                    if has_data and current - self.last_realtime_update > 15.0:
                        if last_line_length > 0:
                            print('\r' + ' ' * last_line_length + '\r', end='', flush=True)
                            last_line_length = 0
                        self.realtime_data = {}
                        self.show_clean_prompt.set()
                    elif not has_data and last_line_length > 0:
                        print('\r' + ' ' * last_line_length + '\r', end='', flush=True)
                        last_line_length = 0

                time.sleep(0.5)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Servicios
    # ------------------------------------------------------------------

    def solicitar_servicio(self, cp_id: str) -> bool:
        if not cp_id:
            print("❌ CP_ID inválido")
            return False
        if not self.producer:
            print("❌ Kafka no conectado")
            return False
        print(f"\n📤 Solicitando carga en {cp_id}...")
        try:
            self.producer.send('service_requests', {
                'driver_id': self.driver_id,
                'cp_id': cp_id,
                'timestamp': time.time()
            })
            self.producer.flush(timeout=5)
            print("✅ Solicitud enviada")
            self.current_service = {'cp_id': cp_id, 'status': 'waiting'}
            self._save_state()
            return True
        except Exception as e:
            print(f"❌ Error: {e}")
            return False

    def cargar_servicios_desde_archivo(self, filepath: str) -> bool:
        try:
            if not os.path.exists(filepath):
                print(f"❌ Archivo no encontrado: {filepath}")
                return False
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            if not lines:
                print("❌ Archivo vacío o sin CPs válidos")
                return False
            self.pending_services = lines
            self._save_state()
            print(f"✅ {len(lines)} servicios cargados")
            self._request_next_service()
            return True
        except Exception as e:
            self.logger.error("❌ Error cargando archivo: %s", e)
            return False

    def _schedule_next_service(self):
        if self.pending_services:
            threading.Timer(4.0, self._request_next_service).start()

    def _request_next_service(self):
        if not self.pending_services:
            print("\n✅ Todos los servicios completados")
            self._save_state()
            return
        cp_id = self.pending_services.pop(0)
        self._save_state()
        print(f"\n📋 Siguiente: {cp_id} ({len(self.pending_services)} restantes)")
        self.solicitar_servicio(cp_id)

    # ------------------------------------------------------------------
    # Menú interactivo
    # ------------------------------------------------------------------

    def _show_help(self):
        print("\n" + "=" * 60)
        print("COMANDOS DISPONIBLES")
        print("=" * 60)
        print("  request <CP_ID>     - Solicitar carga en un CP")
        print("  file <ruta>         - Cargar lista de CPs desde archivo")
        print("  msg                 - Ver mensajes recibidos")
        print("  status              - Ver estado completo")
        print("  clear               - Limpiar pantalla")
        print("  help                - Mostrar esta ayuda")
        print("  quit                - Salir")
        print("=" * 60)
        print("Ejemplos:")
        print("  request CP001")
        print("  file services/servicios.txt")
        print("=" * 60)

    def mostrar_mensajes(self):
        with self.message_lock:
            if not self.message_buffer:
                print("\n📭 No hay mensajes")
                return
            print("\n" + "=" * 60)
            print("📨 MENSAJES RECIBIDOS")
            print("=" * 60)
            for msg in self.message_buffer:
                print(msg)
            print("=" * 60)

    def mostrar_estado(self):
        print("\n" + "=" * 60)
        print(f"ESTADO DE {self.driver_id}")
        print("=" * 60)
        if self.current_service:
            print(f"Servicio actual:  CP={self.current_service.get('cp_id')} "
                  f"| Estado={self.current_service.get('status')}")
            with self.realtime_lock:
                if self.realtime_data:
                    print(f"Consumo actual:   {self.realtime_data.get('kw', 0):.2f} kWh "
                          f"| {self.realtime_data.get('cost', 0):.2f} €")
        else:
            print("Sin servicio activo")
        if self.pending_services:
            print(f"Pendientes:       {len(self.pending_services)} "
                  f"({', '.join(self.pending_services[:5])}{'...' if len(self.pending_services)>5 else ''})")
        print("=" * 60)

    def _interactive_mode(self):
        self._show_help()
        try:
            while self.running:
                if self.show_clean_prompt.is_set():
                    self.show_clean_prompt.clear()
                    time.sleep(0.2)
                try:
                    cmd = input(f"\n[{self.driver_id}]> ").strip()
                except EOFError:
                    break
                if not cmd:
                    continue
                parts = cmd.split(maxsplit=1)
                command = parts[0].lower()

                if command == 'msg':
                    self.mostrar_mensajes()
                elif command == 'request' and len(parts) == 2:
                    self.solicitar_servicio(parts[1].upper())
                elif command == 'file' and len(parts) == 2:
                    filepath = parts[1]
                    if not os.path.isabs(filepath):
                        base = '/app/driver' if os.path.exists('/app/driver') else '.'
                        filepath = os.path.join(base, filepath)
                    self.cargar_servicios_desde_archivo(filepath)
                elif command == 'status':
                    self.mostrar_estado()
                elif command == 'help':
                    self._show_help()
                elif command == 'clear':
                    os.system('clear' if os.name != 'nt' else 'cls')
                elif command in ('quit', 'exit', 'q'):
                    with self.realtime_lock:
                        active = bool(self.realtime_data)
                    if active:
                        print("\n⚠️ HAY DATOS DE CARGA EN TIEMPO REAL")
                        resp = input("¿Desconectar driver? (s/n): ").lower()
                        if resp != 's':
                            print("Cancelado.")
                            continue
                    print("\n🛑 Guardando estado y saliendo...")
                    self._save_state()
                    break
                else:
                    print(f"❌ Comando desconocido: '{command}'. Usa 'help'.")
        except KeyboardInterrupt:
            print("\n\n🛑 Interrumpido por usuario")
            self._save_state()
        finally:
            self.shutdown()

    # ------------------------------------------------------------------
    # Shutdown # ------------------------------------------------------------------

    def shutdown(self):
        self.logger.info("🛑 Apagando Driver...")
        self.running = False
        try:
            self._save_state()
        except Exception:
            pass
        for resource in (self.consumer, self.producer):
            try:
                if resource:
                    resource.close()
            except Exception:
                pass
        self.logger.info("✅ Driver apagado")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    driver_id = os.getenv('DRIVER_ID', 'Driver_001')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    driver = Driver(driver_id, kafka_servers)
    def _handle_signal(signum, frame):
        logging.info("🛑 Señal %d recibida — cerrando Driver...", signum)
        driver.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    driver.start()
