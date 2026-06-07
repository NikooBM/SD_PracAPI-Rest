"""
EV_CP_E - Engine del Charging Point
Release 2 - Práctica SD 25/26

CORRECCIONES APLICADAS:
  [2.5] Backup de sesión con escritura atómica (JSON + tempfile + os.replace)
  [4.3] Engine espera indefinidamente la encryption_key (sin timeout de 45s)
  [4.4] Consumer Kafka con reinicio automático en fallos
  [4.5] flush() solo en mensajes críticos
  [5.1] Persistencia migrada de pickle a JSON
  [5.4] Manejo de SIGTERM para cierre limpio
  [5.6] Logs de alta frecuencia a nivel DEBUG
"""
import socket
import threading
import json
import time
import os
import logging
import random
import tempfile
import signal
import sys
from typing import Optional, Dict, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

# Añadir path para security
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from security.security_utils import CryptoManager
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logging.warning("⚠️ CryptoManager no disponible")

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s: %(message)s'
)


class ChargingPointEngine:
    def __init__(self, cp_id: str, listen_port: int,
                 price_per_kwh: float, kafka_servers: str):
        self.cp_id = cp_id
        self.listen_port = listen_port
        self.price_per_kwh = price_per_kwh
        self.kafka_servers = (
            kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        )

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

        # FIX [5.1]: backup en JSON en lugar de pickle; en directorio de datos
        self.data_dir = os.getenv('CP_DATA_DIR', '/tmp')
        self.session_backup_file = os.path.join(self.data_dir, f'cp_{cp_id}_session.json')

        self.logger = logging.getLogger(f"Engine-{cp_id}")
        self.lock = threading.Lock()

        self.encryption_key: Optional[str] = None
        self.encryption_key_loaded = threading.Event()

        # Archivo donde el Monitor escribe la key (compartido con Monitor)
        self.key_file = f'/tmp/{self.cp_id}_encryption_key.txt'

    # ------------------------------------------------------------------
    # FIX [4.3]: Espera indefinida de la encryption_key
    # ------------------------------------------------------------------

    def _load_encryption_key(self):
        """
        Esperar indefinidamente la encryption_key del Monitor.
        Sin la clave no es posible descifrar las autorizaciones de servicio.
        """
        self.logger.info("⏳ Esperando encryption key del Monitor...")
        attempt = 0

        while self.running:
            attempt += 1
            if os.path.exists(self.key_file):
                try:
                    with open(self.key_file, 'r') as f:
                        key = f.read().strip()
                    if key:
                        self.encryption_key = key
                        self.logger.info(
                            f"🔑 Encryption key cargada (intento {attempt}): {key[:20]}..."
                        )
                        self.encryption_key_loaded.set()
                        return True
                except Exception as e:
                    self.logger.error(f"❌ Error leyendo key: {e}")

            # Log periódico cada 30s para no saturar
            if attempt % 30 == 0:
                self.logger.warning(f"⏳ Esperando encryption key... ({attempt}s)")

            time.sleep(1)

        # Sólo llega aquí si running=False
        self.encryption_key_loaded.set()
        return False

    # ------------------------------------------------------------------
    # FIX [5.1]: Backup de sesión en JSON con escritura atómica
    # ------------------------------------------------------------------

    def _save_session_backup(self):
        """FIX [2.5] + [5.1]: Escritura atómica en JSON (no pickle)."""
        if not self.current_session:
            return
        try:
            data = {
                'session_id': self.current_session.get('session_id'),
                'driver_id': self.current_session.get('driver_id'),
                'price': self.current_session.get('price'),
                'start_time': self.current_session.get('start_time'),
                'kw_consumed': self.current_session.get('kw_consumed', 0.0),
                'total_cost': self.current_session.get('total_cost', 0.0),
                'manual': self.current_session.get('manual', False),
            }
            dir_name = os.path.dirname(self.session_backup_file) or '.'
            # FIX [2.5]: escritura en temporal + rename atómico
            with tempfile.NamedTemporaryFile(
                'w', dir=dir_name, delete=False, suffix='.tmp', encoding='utf-8'
            ) as tmp:
                json.dump(data, tmp)
                tmp_path = tmp.name
            os.replace(tmp_path, self.session_backup_file)
        except Exception as e:
            self.logger.error(f"❌ Error guardando backup: {e}")

    def _load_session_backup(self) -> Optional[Dict[str, Any]]:
        """FIX [5.1]: Cargar backup JSON. Compatibilidad con .pkl legados."""
        # Intentar JSON primero
        if os.path.exists(self.session_backup_file):
            try:
                with open(self.session_backup_file, 'r', encoding='utf-8') as f:
                    backup = json.load(f)
                backup['exitosa'] = False
                backup['razon'] = 'Engine cayó durante carga'
                self.logger.info("=" * 60)
                self.logger.info("📂 SESIÓN JSON RECUPERADA")
                self.logger.info(f"   Sesión: {backup.get('session_id')}")
                self.logger.info(f"   Driver: {backup.get('driver_id')}")
                self.logger.info(f"   Consumo: {backup.get('kw_consumed', 0):.2f} kWh")
                self.logger.info(f"   Coste:   {backup.get('total_cost', 0):.2f} €")
                self.logger.info("=" * 60)
                return backup
            except Exception as e:
                self.logger.error(f"❌ Error cargando backup JSON: {e}")
                # Borrar backup corrupto
                try:
                    os.remove(self.session_backup_file)
                except Exception:
                    pass

        # Compatibilidad: intentar .pkl si existe
        pkl_file = self.session_backup_file.replace('.json', '.pkl')
        if os.path.exists(pkl_file):
            try:
                import pickle
                with open(pkl_file, 'rb') as f:
                    backup = pickle.load(f)
                backup['exitosa'] = False
                backup['razon'] = 'Engine cayó durante carga'
                self.logger.info("📂 SESIÓN pkl RECUPERADA (migrando a JSON)")
                self._save_session_backup_from(backup)  # Migrar a JSON
                os.remove(pkl_file)
                return backup
            except Exception as e:
                self.logger.error(f"❌ Error cargando backup pkl: {e}")
                try:
                    os.remove(pkl_file)
                except Exception:
                    pass

        return None

    def _save_session_backup_from(self, session: Dict):
        """Guardar datos de sesión directamente (para migración pkl→json)."""
        try:
            dir_name = os.path.dirname(self.session_backup_file) or '.'
            with tempfile.NamedTemporaryFile(
                'w', dir=dir_name, delete=False, suffix='.tmp', encoding='utf-8'
            ) as tmp:
                json.dump(session, tmp)
                tmp_path = tmp.name
            os.replace(tmp_path, self.session_backup_file)
        except Exception as e:
            self.logger.error(f"❌ Error en save_from: {e}")

    def _delete_session_backup(self):
        """Eliminar archivos de backup."""
        for f in [self.session_backup_file,
                  self.session_backup_file.replace('.json', '.pkl')]:
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Arranque
    # ------------------------------------------------------------------

    def start(self) -> bool:
        self.logger.info("=" * 60)
        self.logger.info(f"ENGINE {self.cp_id}")
        self.logger.info("=" * 60)

        recovered_session = self._load_session_backup()

        if not self._init_health_server():
            return False

        if not self._init_kafka():
            return False

        # FIX [4.3]: cargar key en hilo paralelo — sin timeout
        threading.Thread(target=self._load_encryption_key, daemon=True).start()

        if recovered_session:
            threading.Thread(
                target=self._send_recovered_session_when_ready,
                args=(recovered_session,), daemon=True
            ).start()

        self.logger.info(f"✅ Engine {self.cp_id} listo")
        self._interactive_mode()
        return True

    def _send_recovered_session_when_ready(self, session: Dict[str, Any]):
        """Enviar sesión recuperada cuando la key esté disponible."""
        time.sleep(3)
        # Esperar key sin timeout (puede tardar si el Monitor reinicia)
        self.encryption_key_loaded.wait()

        try:
            payload = {
                'cp_id': self.cp_id,
                'session_id': session.get('session_id'),
                'driver_id': session.get('driver_id'),
                'kw_total': session.get('kw_consumed', 0),
                'cost_total': session.get('total_cost', 0),
                'exitosa': False,
                'razon': session.get('razon', 'Engine cayó durante carga'),
                'timestamp': time.time()
            }
            self._send_kafka('charging_complete', payload, encrypt=True, require_ack=True)
            self.logger.info("✅ Sesión recuperada enviada a Central")
            self._delete_session_backup()
        except Exception as e:
            self.logger.error(f"❌ Error enviando sesión recuperada: {e}")

    # ------------------------------------------------------------------
    # Health server
    # ------------------------------------------------------------------

    def _init_health_server(self) -> bool:
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
        while self.running:
            try:
                client, addr = self.health_server.accept()
                client.settimeout(2.0)
                try:
                    data = client.recv(64).decode('utf-8').strip()
                    if data == 'PING':
                        client.send(b'PONG' if self.is_healthy else b'KO')
                finally:
                    client.close()
            except socket.timeout:
                continue
            except Exception:
                if self.running:
                    pass  # Evitar spam de logs

    # ------------------------------------------------------------------
    # FIX [4.4]: Kafka con reinicio automático del consumer
    # ------------------------------------------------------------------

    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                self.logger.info(f"🔄 Kafka ({attempt}/15)...")
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
                    self.logger.info("✅ Kafka conectado")
                    return True
            except Exception as e:
                self.logger.error(f"❌ Kafka error: {e}")
                if attempt < 15:
                    time.sleep(5)
        return False

    def _init_consumer(self):
        """Crear/recrear consumer Kafka."""
        try:
            self.consumer = KafkaConsumer(
                'service_authorizations', 'central_commands',
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=f'cp-{self.cp_id}',
                enable_auto_commit=True,
                session_timeout_ms=30000,
                consumer_timeout_ms=1000
            )
        except Exception as e:
            self.logger.error(f"❌ No se pudo crear consumer: {e}")
            self.consumer = None

    def _kafka_consumer_loop(self):
        """FIX [4.4]: Loop consumer con reinicio automático ante fallos."""
        while self.running:
            try:
                if self.consumer is None:
                    self._init_consumer()
                    time.sleep(2)
                    continue

                for msg in self.consumer:
                    if not self.running:
                        break
                    data = msg.value

                    # Descifrar si aplica
                    if (isinstance(data, dict) and data.get('encrypted')
                            and data.get('cp_id') == self.cp_id):
                        if self.encryption_key and CRYPTO_AVAILABLE:
                            try:
                                data = CryptoManager.decrypt_json(
                                    data['data'], self.encryption_key
                                )
                            except Exception as e:
                                self.logger.error(f"❌ Error descifrando: {e}")
                                continue
                        else:
                            self.logger.warning(
                                "⚠️ Mensaje cifrado recibido pero sin key disponible; ignorando"
                            )
                            continue

                    if msg.topic == 'service_authorizations':
                        self._handle_authorization(data)
                    elif msg.topic == 'central_commands':
                        self._handle_command(data)

            except Exception as e:
                if self.running and 'timed out' not in str(e).lower():
                    self.logger.error(f"❌ Consumer error, reiniciando: {e}")
                    try:
                        if self.consumer:
                            self.consumer.close()
                    except Exception:
                        pass
                    self.consumer = None
                    time.sleep(5)

    # ------------------------------------------------------------------
    # Handlers de negocio
    # ------------------------------------------------------------------

    def _handle_authorization(self, data: Dict[str, Any]):
        if data.get('cp_id') != self.cp_id:
            return
        self.last_central_contact = time.time()

        with self.lock:
            if self.is_stopped_by_central:
                self.logger.warning("⚠️ CP parado por Central; autorización ignorada")
                return
            if self.current_session is not None:
                self.logger.warning("⚠️ Ya hay sesión activa; autorización ignorada")
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

        print("\n" + "=" * 60)
        print("⚡ SERVICIO AUTORIZADO")
        print("=" * 60)
        print(f"Driver:  {driver_id}")
        print(f"Sesión:  {session_id}")
        print("\n👉 Pulsa '1' para iniciar carga")
        print("=" * 60 + "\n")

    def iniciar_carga(self) -> bool:
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
        """Carga manual (emergencia) — sin autorización de Central."""
        if time.time() - self.last_central_contact > 30:
            print("\n⚠️ Central no responde hace más de 30s")
            print("   Iniciando en modo emergencia...")
        else:
            print(f"\n⚠️ Central activa (último contacto: "
                  f"{int(time.time() - self.last_central_contact)}s)")
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
                print("❌ CP parado por Central — No se puede iniciar carga manual")
                return False

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

        print("\n" + "=" * 60)
        print("⚡ CARGA MANUAL INICIADA (MODO EMERGENCIA)")
        print("=" * 60)
        print(f"Driver:  {driver_id}")
        print(f"Sesión:  {session_id}")
        print("\n💡 Esta sesión NO está autorizada por Central")
        print("💡 Pulsa '2' para finalizar")
        print("=" * 60 + "\n")

        threading.Thread(target=self._charging_loop, daemon=True).start()
        return True

    def _charging_loop(self):
        """Loop de carga. FIX [4.5]: flush solo en envíos críticos."""
        last_log_time = 0
        last_send_time = 0

        while self.charging_active and self.running:
            with self.lock:
                if self.current_session is None:
                    break
                if self.is_stopped_by_central:
                    self.logger.warning("⛔ Carga detenida por Central")
                    break

                kw_rate = random.uniform(7.0, 22.0) / 3600
                self.current_session['kw_consumed'] += kw_rate
                self.current_session['total_cost'] = (
                    self.current_session['kw_consumed'] * self.current_session['price']
                )

                self._save_session_backup()

                current_time = time.time()
                if current_time - last_send_time >= 2:
                    payload = {
                        'cp_id': self.cp_id,
                        'session_id': self.current_session['session_id'],
                        'driver_id': self.current_session['driver_id'],
                        'kw': self.current_session['kw_consumed'],
                        'cost': self.current_session['total_cost'],
                        'manual': self.current_session.get('manual', False),
                        'timestamp': current_time
                    }
                    # FIX [4.5]: sin require_ack en datos de telemetría
                    self._send_kafka('charging_data', payload, encrypt=True)
                    last_send_time = current_time

                # FIX [5.6]: log de datos de carga a DEBUG
                if current_time - last_log_time >= 5:
                    elapsed = int(current_time - (self.current_session['start_time'] or current_time))
                    icon = "🔧" if self.current_session.get('manual') else "⚡"
                    self.logger.debug(
                        f"{icon} {self.current_session['kw_consumed']:.2f} kWh | "
                        f"{self.current_session['total_cost']:.2f} € | {elapsed}s"
                    )
                    last_log_time = current_time

            time.sleep(1)

    def finalizar_carga(self, razon: str = 'Finalizada por conductor') -> bool:
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

        self._print_ticket(session_id, driver_id, kw_total, cost_total, True, razon, is_manual)
        self._delete_session_backup()

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
        # FIX [4.5]: charging_complete es crítico → require_ack=True
        self._send_kafka('charging_complete', payload, encrypt=True, require_ack=True)
        self.logger.info(f"✅ Finalizada: {session_id}")
        return True

    def _print_ticket(self, session_id: str, driver_id: str, kw: float,
                      cost: float, exitosa: bool, razon: str = '',
                      is_manual: bool = False):
        print("\n" + "=" * 60)
        print(f"🎫 TICKET - {self.cp_id}")
        if is_manual:
            print("    ⚠️ MODO MANUAL")
        print("=" * 60)
        print(f"Conductor:      {driver_id}")
        print(f"Sesión:         {session_id}")
        print(f"Energía:        {kw:.2f} kWh")
        print(f"Importe:        {cost:.2f} €")
        if exitosa:
            print("Estado:         ✅ COMPLETADA")
        else:
            print(f"Estado:         ⚠️ INTERRUMPIDA - {razon}")
        print("=" * 60 + "\n")

    def _handle_command(self, data: Dict[str, Any]):
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
        print("\n" + "=" * 60)
        print("⛔ CP DETENIDO POR CENTRAL")
        print("=" * 60 + "\n")

        with self.lock:
            self.is_stopped_by_central = True
            if self.state == 'CHARGING' and self.current_session:
                self.charging_active = False
                time.sleep(0.5)
                session_id = self.current_session['session_id']
                driver_id = self.current_session['driver_id']
                kw_total = self.current_session['kw_consumed']
                cost_total = self.current_session['total_cost']
                self._print_ticket(
                    session_id, driver_id, kw_total, cost_total,
                    False, 'Detenido por Central'
                )
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
                # Fuera del lock para no bloquear
                threading.Thread(
                    target=lambda: self._send_kafka(
                        'charging_complete', payload, encrypt=True, require_ack=True
                    ), daemon=True
                ).start()
                self.current_session = None
                self._delete_session_backup()
            self.state = 'STOPPED'

        self.logger.warning("⛔ CP PARADO")

    def _resume_by_central(self):
        with self.lock:
            self.is_stopped_by_central = False
            self.state = 'IDLE'
        print("\n▶️ CP REANUDADO\n")
        self.logger.info("▶️ REANUDADO")

    def simular_averia(self):
        print("\n" + "=" * 60)
        print("💥 SIMULANDO AVERÍA")
        print("=" * 60 + "\n")
        with self.lock:
            self.is_healthy = False
            if self.state == 'CHARGING' and self.current_session:
                self.charging_active = False
                time.sleep(0.5)
                self._save_session_backup()
                self.current_session = None
            self.state = 'IDLE'
        self.logger.error("💥 AVERÍA")

    def resolver_averia(self):
        with self.lock:
            self.is_healthy = True
            self.state = 'IDLE'
        print("\n🔧 AVERÍA RESUELTA\n")
        self.logger.info("🔧 RESUELTA")

    # ------------------------------------------------------------------
    # FIX [4.5]: Kafka send — flush solo en mensajes críticos
    # ------------------------------------------------------------------

    def _send_kafka(self, topic: str, payload: Dict[str, Any],
                    encrypt: bool = False, require_ack: bool = False):
        """
        Enviar a Kafka con cifrado condicional.
        FIX [4.5]: flush/ack solo cuando require_ack=True (mensajes críticos).
        """
        try:
            if self.producer:
                final_payload = payload
                if encrypt and self.encryption_key and CRYPTO_AVAILABLE:
                    try:
                        encrypted_data = CryptoManager.encrypt_json(
                            payload, self.encryption_key
                        )
                        final_payload = {
                            'encrypted': True,
                            'data': encrypted_data,
                            'cp_id': self.cp_id
                        }
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error cifrando, enviando sin cifrar: {e}")

                future = self.producer.send(topic, final_payload)
                if require_ack:
                    future.get(timeout=5)
        except Exception as e:
            self.logger.error(f"❌ Error Kafka [{topic}]: {e}")

    # ------------------------------------------------------------------
    # Modo interactivo
    # ------------------------------------------------------------------

    def _show_help(self):
        print("\n" + "=" * 60)
        print(f"ENGINE {self.cp_id} - COMANDOS")
        print("=" * 60)
        print("  1    - Enchufar vehículo (iniciar carga)")
        print("  1m   - Carga MANUAL (emergencia)")
        print("  2    - Desenchufar (finalizar carga)")
        print("  3    - Simular avería")
        print("  4    - Resolver avería")
        print("  5    - Ver estado")
        print("  help - Ayuda")
        print("  q    - Salir")
        print("=" * 60)

    def _interactive_mode(self):
        self._show_help()
        try:
            while self.running:
                cmd = input(f"\n[{self.cp_id}]> ").strip().lower()
                if not cmd:
                    continue

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
                        print("\n" + "=" * 60)
                        print("ESTADO DEL ENGINE")
                        print("=" * 60)
                        print(f"Estado:          {self.state}")
                        print(f"Salud:           {'✅ OK' if self.is_healthy else '❌ AVERIADO'}")
                        print(f"Parado Central:  {'✅ Sí' if self.is_stopped_by_central else '❌ No'}")
                        print(f"Cifrado:         {'✅ Activo' if self.encryption_key else '⏳ Esperando key'}")
                        if self.current_session:
                            print(f"\n🔋 SESIÓN ACTIVA:")
                            print(f"  ID:      {self.current_session['session_id']}")
                            print(f"  Driver:  {self.current_session['driver_id']}")
                            print(f"  Consumo: {self.current_session['kw_consumed']:.2f} kWh")
                            print(f"  Coste:   {self.current_session['total_cost']:.2f} €")
                        else:
                            print("\n🔋 Sin sesión activa")
                        if os.path.exists(self.session_backup_file):
                            print("\n💾 Hay sesión guardada en disco (backup)")
                        print("=" * 60 + "\n")
                elif cmd == 'help':
                    self._show_help()
                elif cmd in ('q', 'quit', 'exit'):
                    with self.lock:
                        if self.state == 'CHARGING' and self.current_session:
                            print("\n⚠️ HAY UNA CARGA EN PROGRESO")
                            resp = input("¿Detener carga y salir? (s/n): ").lower()
                            if resp == 's':
                                print("\n🛑 Deteniendo carga...")
                                self.charging_active = False
                                time.sleep(0.5)
                                self._save_session_backup()
                                self.logger.info("💾 Sesión guardada para recuperación")
                                break
                            else:
                                print("Cancelado.")
                                continue
                        else:
                            break
        except (KeyboardInterrupt, EOFError):
            print("\n\n🛑 Interrupción detectada...")
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
        self.logger.info("🛑 Apagando Engine...")
        self.running = False
        self.charging_active = False

        with self.lock:
            if self.current_session:
                try:
                    self._save_session_backup()
                    self.logger.info("💾 Sesión guardada en backup")
                except Exception as e:
                    self.logger.error(f"❌ Error guardando sesión: {e}")

        for resource in [self.health_server, self.consumer, self.producer]:
            try:
                if resource:
                    resource.close()
            except Exception:
                pass

        self.logger.info("✅ Engine apagado correctamente")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    cp_id = os.getenv('CP_ID', 'CP001')
    listen_port = int(os.getenv('LISTEN_PORT', '6000'))
    price = float(os.getenv('PRICE_PER_KWH', '0.50'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    engine = ChargingPointEngine(cp_id, listen_port, price, kafka_servers)

    # FIX [5.4]: Manejo de SIGTERM
    def handle_sigterm(signum, frame):
        logger.info("🛑 SIGTERM recibido, cerrando Engine...")
        engine.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    engine.start()
