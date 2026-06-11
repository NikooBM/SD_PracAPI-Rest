"""EV_CP_E — Engine del Charging Point
Release 2 - Práctica SD 25/26
"""
import socket
import threading
import json
import time
import os
import stat
import logging
import random
import tempfile
import signal
import sys
from typing import Optional, Dict, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from security.security_utils import CryptoManager
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logging.warning("⚠️ CryptoManager no disponible — mensajes sin cifrar")

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
        self.last_central_contact = 0.0

        self.producer: Optional[KafkaProducer] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.health_server: Optional[socket.socket] = None
        self.running = True
        self.charging_active = False
        self.data_dir = os.getenv('CP_DATA_DIR', '/tmp')
        os.makedirs(self.data_dir, mode=0o700, exist_ok=True)
        self.key_file = os.path.join(self.data_dir, f'{cp_id}_encryption_key.txt')
        self.session_backup_file = os.path.join(self.data_dir, f'cp_{cp_id}_session.json')

        self.logger = logging.getLogger(f"Engine-{cp_id}")
        self.lock = threading.Lock()

        self.encryption_key: Optional[str] = None
        self.encryption_key_loaded = threading.Event()

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _load_encryption_key(self):
        """Espera indefinidamente hasta que Monitor escriba la key en CP_DATA_DIR.
        Sin timeout: el Monitor puede tardar mientras negocia con el Registry.
        """
        self.logger.info("⏳ Esperando encryption key en %s ...", self.key_file)
        attempt = 0

        while self.running:
            attempt += 1
            if os.path.exists(self.key_file):
                try:
                    with open(self.key_file, 'r', encoding='utf-8') as f:
                        key = f.read().strip()
                    if key:
                        self.encryption_key = key
                        self.logger.info(
                            "🔑 Encryption key cargada (intento %d): %.20s...", attempt, key
                        )
                        self.encryption_key_loaded.set()
                        return True
                except Exception as e:
                    self.logger.error("❌ Error leyendo key: %s", e)

            if attempt % 30 == 0:
                self.logger.warning("⏳ Esperando encryption key... (%ds)", attempt)

            time.sleep(1)

        self.encryption_key_loaded.set()
        return False

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _get_session_snapshot(self) -> Optional[Dict[str, Any]]:
        """Obtiene copia del estado de sesión bajo lock, para guardar fuera del lock."""
        with self.lock:
            if not self.current_session:
                return None
            return {
                'session_id':   self.current_session.get('session_id'),
                'driver_id':    self.current_session.get('driver_id'),
                'price':        self.current_session.get('price'),
                'start_time':   self.current_session.get('start_time'),
                'kw_consumed':  self.current_session.get('kw_consumed', 0.0),
                'total_cost':   self.current_session.get('total_cost', 0.0),
                'manual':       self.current_session.get('manual', False),
            }

    def _save_session_backup(self):
        """Escritura atómica en JSON — NUNCA llamar mientras se tiene self.lock."""
        snapshot = self._get_session_snapshot()
        if not snapshot:
            return
        try:
            dir_name = os.path.dirname(os.path.abspath(self.session_backup_file))
            with tempfile.NamedTemporaryFile(
                'w', dir=dir_name, delete=False, suffix='.tmp', encoding='utf-8'
            ) as tmp:
                json.dump(snapshot, tmp, ensure_ascii=False)
                tmp_path = tmp.name
            os.replace(tmp_path, self.session_backup_file)
        except Exception as e:
            self.logger.error("❌ Error guardando backup: %s", e)

    def _load_session_backup(self) -> Optional[Dict[str, Any]]:
        """Carga backup JSON. Compatibilidad con .pkl legados."""
        if os.path.exists(self.session_backup_file):
            try:
                with open(self.session_backup_file, 'r', encoding='utf-8') as f:
                    backup = json.load(f)
                backup['exitosa'] = False
                backup['razon'] = 'Engine reiniciado durante carga'
                self.logger.info("📂 SESIÓN RECUPERADA: %s", backup.get('session_id'))
                return backup
            except Exception as e:
                self.logger.error("❌ Error cargando backup JSON: %s", e)
                try:
                    os.remove(self.session_backup_file)
                except Exception:
                    pass

        # Compatibilidad .pkl
        pkl_file = self.session_backup_file.replace('.json', '.pkl')
        if os.path.exists(pkl_file):
            try:
                import pickle
                with open(pkl_file, 'rb') as f:
                    backup = pickle.load(f)
                backup['exitosa'] = False
                backup['razon'] = 'Engine reiniciado durante carga'
                self.logger.info("📂 Sesión pkl recuperada (migrando a JSON)")
                os.remove(pkl_file)
                return backup
            except Exception as e:
                self.logger.error("❌ Error cargando backup pkl: %s", e)
                try:
                    os.remove(pkl_file)
                except Exception:
                    pass
        return None

    def _delete_session_backup(self):
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
        self.logger.info("ENGINE %s", self.cp_id)
        self.logger.info("=" * 60)

        recovered = self._load_session_backup()

        if not self._init_health_server():
            return False
        if not self._init_kafka():
            return False
        threading.Thread(target=self._load_encryption_key, daemon=True).start()

        if recovered:
            threading.Thread(
                target=self._send_recovered_session_when_ready,
                args=(recovered,), daemon=True
            ).start()

        self.logger.info("✅ Engine %s listo", self.cp_id)
        self._interactive_mode()
        return True

    def _send_recovered_session_when_ready(self, session: Dict[str, Any]):
        time.sleep(3)
        self.encryption_key_loaded.wait()
        try:
            payload = {
                'cp_id':      self.cp_id,
                'session_id': session.get('session_id'),
                'driver_id':  session.get('driver_id'),
                'kw_total':   session.get('kw_consumed', 0),
                'cost_total': session.get('total_cost', 0),
                'exitosa':    False,
                'razon':      session.get('razon', 'Engine reiniciado'),
                'timestamp':  time.time()
            }
            self._send_kafka('charging_complete', payload, encrypt=True, require_ack=True)
            self.logger.info("✅ Sesión recuperada enviada a Central")
            self._delete_session_backup()
        except Exception as e:
            self.logger.error("❌ Error enviando sesión recuperada: %s", e)

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
            self.logger.info("✅ Health server: :%d", self.listen_port)
            return True
        except Exception as e:
            self.logger.error("❌ Health server: %s", e)
            return False

    def _health_server_loop(self):
        while self.running:
            try:
                client, _ = self.health_server.accept()
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
                pass

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _init_kafka(self) -> bool:
        for attempt in range(1, 16):
            try:
                self.logger.info("🔄 Kafka (%d/15)...", attempt)
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
            except Exception as e:
                self.logger.error("❌ Kafka error: %s", e)
                if attempt < 15:
                    time.sleep(5)
        return False

    def _init_consumer(self):
        try:
            self.consumer = KafkaConsumer(
                'service_authorizations', 'central_commands',
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=f'cp-{self.cp_id}',
                enable_auto_commit=True,
                session_timeout_ms=30_000,
                consumer_timeout_ms=1000
            )
        except Exception as e:
            self.logger.error("❌ No se pudo crear consumer: %s", e)
            self.consumer = None

    def _kafka_consumer_loop(self):
        """Loop con reinicio automático."""
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
                            except ValueError as e:
                                self.logger.error(
                                    "❌ Error descifrando (clave incorrecta o datos corruptos): %s", e
                                )
                                self.logger.error(
                                    "   Imposible descifrar mensaje de Central. "
                                    "Verifique que la clave sea correcta."
                                )
                                continue
                        else:
                            self.logger.warning(
                                "⚠️ Mensaje cifrado sin key disponible; ignorando"
                            )
                            continue

                    if msg.topic == 'service_authorizations':
                        self._handle_authorization(data)
                    elif msg.topic == 'central_commands':
                        self._handle_command(data)

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
    # Handlers de negocio
    # ------------------------------------------------------------------

    def _handle_authorization(self, data: Dict[str, Any]):
        if data.get('cp_id') != self.cp_id:
            return
        self.last_central_contact = time.time()

        with self.lock:
            if self.is_stopped_by_central:
                self.logger.warning("⚠️ CP parado; autorización ignorada")
                return
            if self.current_session:
                self.logger.warning("⚠️ Ya hay sesión activa; autorización ignorada")
                return
            driver_id  = data.get('driver_id', '')
            session_id = data.get('session_id', '')
            price      = data.get('price', self.price_per_kwh)
            self.state = 'AUTHORIZED'
            self.current_session = {
                'session_id': session_id,
                'driver_id':  driver_id,
                'price':      price,
                'start_time': None,
                'kw_consumed': 0.0,
                'total_cost':  0.0
            }

        print("\n" + "=" * 60)
        print("⚡ SERVICIO AUTORIZADO")
        print(f"Driver:  {data.get('driver_id')}")
        print(f"Sesión:  {data.get('session_id')}")
        print("\n👉 Pulsa '1' para iniciar carga")
        print("=" * 60 + "\n")

    def iniciar_carga(self) -> bool:
        with self.lock:
            if self.state != 'AUTHORIZED':
                print(f"\n❌ Estado: {self.state}. Necesitas autorización primero.\n")
                return False
            if not self.current_session or self.is_stopped_by_central:
                print("\n❌ CP parado por Central o sin sesión.\n")
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
        if time.time() - self.last_central_contact > 30:
            print("\n⚠️ Central no responde hace más de 30s")
            print("   Iniciando en modo emergencia...")
        else:
            resp = input("¿Continuar con carga manual de emergencia? (s/n): ").lower()
            if resp != 's':
                return False

        driver_id = input("ID del conductor: ").strip()
        if not driver_id:
            print("❌ ID de conductor requerido")
            return False

        with self.lock:
            if self.current_session or self.is_stopped_by_central:
                print("❌ Hay sesión activa o CP parado")
                return False
            session_id = f"MANUAL_{self.cp_id}_{int(time.time())}_{os.getpid()}"
            self.state = 'CHARGING'
            self.current_session = {
                'session_id': session_id, 'driver_id': driver_id,
                'price': self.price_per_kwh, 'start_time': time.time(),
                'kw_consumed': 0.0, 'total_cost': 0.0, 'manual': True
            }
            self.charging_active = True

        print("\n⚡ CARGA MANUAL INICIADA")
        print(f"Driver: {driver_id} | Sesión: {session_id}")
        print("👉 Pulsa '2' para finalizar\n")
        threading.Thread(target=self._charging_loop, daemon=True).start()
        return True

    def _charging_loop(self):
        """File I/O fuera del lock. flush() solo en envíos críticos."""
        last_send_time = 0.0
        last_log_time  = 0.0

        while self.charging_active and self.running:
            # Obtener snapshot bajo lock, luego trabajar fuera
            snapshot = None
            stopped = False

            with self.lock:
                if not self.current_session:
                    break
                if self.is_stopped_by_central:
                    stopped = True
                else:
                    kw_rate = random.uniform(7.0, 22.0) / 3600
                    self.current_session['kw_consumed'] += kw_rate
                    self.current_session['total_cost'] = (
                        self.current_session['kw_consumed'] * self.current_session['price']
                    )
                    snapshot = dict(self.current_session)

            if stopped:
                self.logger.warning("⛔ Carga detenida por Central")
                break

            if snapshot is None:
                break
            self._save_session_backup()

            current_time = time.time()
            if current_time - last_send_time >= 2:
                self._send_kafka('charging_data', {
                    'cp_id':      self.cp_id,
                    'session_id': snapshot['session_id'],
                    'driver_id':  snapshot['driver_id'],
                    'kw':         snapshot['kw_consumed'],
                    'cost':       snapshot['total_cost'],
                    'manual':     snapshot.get('manual', False),
                    'timestamp':  current_time
                }, encrypt=True, require_ack=False)
                last_send_time = current_time
            if current_time - last_log_time >= 5:
                elapsed = int(current_time - (snapshot.get('start_time') or current_time))
                self.logger.debug(
                    "⚡ %.2f kWh | %.2f € | %ds",
                    snapshot['kw_consumed'], snapshot['total_cost'], elapsed
                )
                last_log_time = current_time

            time.sleep(1)

    def finalizar_carga(self, razon: str = 'Finalizada por conductor') -> bool:
        with self.lock:
            if self.state != 'CHARGING' or not self.current_session:
                print(f"\n❌ Estado: {self.state}\n")
                return False
            self.charging_active = False

        time.sleep(0.5)

        with self.lock:
            if not self.current_session:
                return False
            session_id = self.current_session['session_id']
            driver_id  = self.current_session['driver_id']
            kw_total   = self.current_session['kw_consumed']
            cost_total = self.current_session['total_cost']
            is_manual  = self.current_session.get('manual', False)
            self.state = 'IDLE'
            self.current_session = None

        self._print_ticket(session_id, driver_id, kw_total, cost_total, True, razon, is_manual)
        self._delete_session_backup()
        self._send_kafka('charging_complete', {
            'cp_id':      self.cp_id,
            'session_id': session_id,
            'driver_id':  driver_id,
            'kw_total':   kw_total,
            'cost_total': cost_total,
            'exitosa':    True,
            'razon':      razon,
            'manual':     is_manual,
            'timestamp':  time.time()
        }, encrypt=True, require_ack=True)

        self.logger.info("✅ Carga finalizada: %s", session_id)
        return True

    def _print_ticket(self, session_id, driver_id, kw, cost, exitosa, razon='', manual=False):
        print("\n" + "=" * 60)
        print(f"🎫 TICKET — {self.cp_id}{'  ⚠️ MANUAL' if manual else ''}")
        print("=" * 60)
        print(f"Conductor: {driver_id}")
        print(f"Sesión:    {session_id}")
        print(f"Energía:   {kw:.2f} kWh")
        print(f"Importe:   {cost:.2f} €")
        print(f"Estado:    {'✅ COMPLETADA' if exitosa else '⚠️ INTERRUMPIDA — ' + razon}")
        print("=" * 60 + "\n")

    def _handle_command(self, data: Dict[str, Any]):
        if data.get('cp_id') != self.cp_id:
            return
        self.last_central_contact = time.time()
        command = data.get('command', '')
        self.logger.info("📨 Comando: %s", command)
        if command == 'STOP':
            self._stop_by_central()
        elif command == 'RESUME':
            self._resume_by_central()

    def _stop_by_central(self):
        print("\n" + "=" * 60)
        print("⛔ CP DETENIDO POR CENTRAL")
        print("=" * 60 + "\n")

        session_to_finalize = None
        with self.lock:
            self.is_stopped_by_central = True
            if self.state == 'CHARGING' and self.current_session:
                self.charging_active = False
                session_to_finalize = dict(self.current_session)
                self.current_session = None
            self.state = 'STOPPED'

        if session_to_finalize:
            time.sleep(0.5)
            self._print_ticket(
                session_to_finalize['session_id'],
                session_to_finalize['driver_id'],
                session_to_finalize['kw_consumed'],
                session_to_finalize['total_cost'],
                False, 'Detenido por Central'
            )
            threading.Thread(
                target=lambda: self._send_kafka('charging_complete', {
                    'cp_id':      self.cp_id,
                    'session_id': session_to_finalize['session_id'],
                    'driver_id':  session_to_finalize['driver_id'],
                    'kw_total':   session_to_finalize['kw_consumed'],
                    'cost_total': session_to_finalize['total_cost'],
                    'exitosa':    False,
                    'razon':      'Detenido por Central',
                    'timestamp':  time.time()
                }, encrypt=True, require_ack=True),
                daemon=True
            ).start()
            self._delete_session_backup()

        self.logger.warning("⛔ CP PARADO")

    def _resume_by_central(self):
        with self.lock:
            self.is_stopped_by_central = False
            self.state = 'IDLE'
        print("\n▶️ CP REANUDADO\n")
        self.logger.info("▶️ REANUDADO")

    def simular_averia(self):
        print("\n💥 SIMULANDO AVERÍA\n")
        with self.lock:
            self.is_healthy = False
            if self.state == 'CHARGING' and self.current_session:
                self.charging_active = False
                time.sleep(0.5)
            self.state = 'IDLE'
            self.current_session = None
        self._save_session_backup()
        self.logger.error("💥 AVERÍA")

    def resolver_averia(self):
        with self.lock:
            self.is_healthy = True
            self.state = 'IDLE'
        print("\n🔧 AVERÍA RESUELTA\n")
        self.logger.info("🔧 RESUELTA")

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _send_kafka(self, topic: str, payload: Dict[str, Any],
                    encrypt: bool = False, require_ack: bool = False):
        try:
            if not self.producer:
                return
            final = payload
            if encrypt and self.encryption_key and CRYPTO_AVAILABLE:
                try:
                    final = {
                        'encrypted': True,
                        'data':      CryptoManager.encrypt_json(payload, self.encryption_key),
                        'cp_id':     self.cp_id
                    }
                except Exception as e:
                    self.logger.warning("⚠️ Error cifrando, enviando sin cifrar: %s", e)
            future = self.producer.send(topic, final)
            if require_ack:
                future.get(timeout=5)
        except Exception as e:
            self.logger.error("❌ Error Kafka [%s]: %s", topic, e)

    # ------------------------------------------------------------------
    # Modo interactivo
    # ------------------------------------------------------------------

    def _show_help(self):
        print("\n" + "=" * 60)
        print(f"ENGINE {self.cp_id} — COMANDOS")
        print("=" * 60)
        print("  1    - Enchufar vehículo (iniciar carga autorizada)")
        print("  1m   - Carga MANUAL (emergencia, sin autorización)")
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
                try:
                    cmd = input(f"\n[{self.cp_id}]> ").strip().lower()
                except EOFError:
                    break
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
                            print(f"Sesión activa:   {self.current_session['session_id']}")
                            print(f"Consumo:         {self.current_session['kw_consumed']:.2f} kWh")
                            print(f"Coste:           {self.current_session['total_cost']:.2f} €")
                        else:
                            print("Sin sesión activa")
                        print("=" * 60 + "\n")
                elif cmd == 'help':
                    self._show_help()
                elif cmd in ('q', 'quit', 'exit'):
                    with self.lock:
                        has_session = bool(self.current_session)
                    if has_session:
                        resp = input("\n⚠️ HAY CARGA EN PROGRESO. ¿Guardar y salir? (s/n): ").lower()
                        if resp != 's':
                            print("Cancelado.")
                            continue
                        self.charging_active = False
                        time.sleep(0.5)
                    # Backup fuera del lock
                    self._save_session_backup()
                    break
        except (KeyboardInterrupt, EOFError):
            print("\n\n🛑 Interrupción detectada...")
            self.charging_active = False
            time.sleep(0.5)
            self._save_session_backup()
        finally:
            self.shutdown()

    # ------------------------------------------------------------------
    # Shutdown # ------------------------------------------------------------------

    def shutdown(self):
        self.logger.info("🛑 Apagando Engine...")
        self.running = False
        self.charging_active = False
        time.sleep(0.2)
        # Backup fuera del lock
        self._save_session_backup()
        for resource in (self.health_server, self.consumer, self.producer):
            try:
                if resource:
                    resource.close()
            except Exception:
                pass
        self.logger.info("✅ Engine apagado")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    cp_id        = os.getenv('CP_ID',                      'CP001')
    listen_port  = int(os.getenv('LISTEN_PORT',            '6000'))
    price        = float(os.getenv('PRICE_PER_KWH',        '0.50'))
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS',   'kafka:9092')

    engine = ChargingPointEngine(cp_id, listen_port, price, kafka_servers)
    def _handle_signal(signum, frame):
        logging.info("🛑 Señal %d recibida — cerrando Engine...", signum)
        engine.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    engine.start()
