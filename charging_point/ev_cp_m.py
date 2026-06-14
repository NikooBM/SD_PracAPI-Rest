"""EV_CP_M — Monitor del Charging Point
Release 2 - Práctica SD 25/26
"""
import socket
import time
import os
import stat
import hashlib
import logging
import threading
import signal
import sys
import tempfile
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------

_MACHINE_SECRET = os.getenv('MACHINE_SECRET', 'evcharging-lab-secret')

def _pw_path(cp_id: str, data_dir: str) -> str:
    os.makedirs(data_dir, mode=0o700, exist_ok=True)
    return os.path.join(data_dir, f'{cp_id}.pw')

def _save_password_secure(cp_id: str, password: str, data_dir: str):
    """
    Guarda contraseña con XOR derivado de entorno + permisos 0o600.
    """
    key = hashlib.sha256(f"{_MACHINE_SECRET}:{cp_id}".encode()).hexdigest()
    key_b = (key * 10).encode()
    pw_b = password.encode('utf-8')
    enc = bytes(a ^ b for a, b in zip(pw_b, key_b[:len(pw_b)]))
    path = _pw_path(cp_id, data_dir)
    # Escritura atómica
    dir_name = os.path.dirname(os.path.abspath(path))
    with tempfile.NamedTemporaryFile('wb', dir=dir_name, delete=False, suffix='.tmp') as tmp:
        tmp.write(enc)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)

def _load_password_secure(cp_id: str, data_dir: str) -> str | None:
    path = _pw_path(cp_id, data_dir)
    if not os.path.exists(path):
        return None
    key = hashlib.sha256(f"{_MACHINE_SECRET}:{cp_id}".encode()).hexdigest()
    key_b = (key * 10).encode()
    with open(path, 'rb') as f:
        enc = f.read()
    dec = bytes(a ^ b for a, b in zip(enc, key_b[:len(enc)]))
    return dec.decode('utf-8')

def _delete_password_secure(cp_id: str, data_dir: str):
    path = _pw_path(cp_id, data_dir)
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass

# ---------------------------------------------------------------------------
# CPMonitor
# ---------------------------------------------------------------------------

class CPMonitor:
    def __init__(self, cp_id: str, location: str, price: float,
                 central_host: str, central_port: int,
                 engine_host: str, engine_port: int):
        self.cp_id = cp_id
        self.location = location
        self.price = price
        self.central_host = central_host
        self.central_port = central_port
        self.engine_host = engine_host
        self.engine_port = engine_port

        self.central_socket: socket.socket | None = None
        self.running = True
        self.is_healthy = True
        self.consecutive_failures = 0

        self.logger = logging.getLogger(f"Monitor-{cp_id}")

        # Registry
        self.registry_url = os.getenv('REGISTRY_URL', 'https://ev_registry:8443')
        self.registry_cert = os.getenv('REGISTRY_CERT_PATH', '/app/certs/registry.crt')

        self.cp_password: str | None = None
        self.encryption_key: str | None = None
        self.data_dir = os.getenv('CP_DATA_DIR', '/tmp')
        # Archivo de encryption key: en CP_DATA_DIR compartido con Engine
        self.key_file = os.path.join(self.data_dir, f'{cp_id}_encryption_key.txt')

        self.interactive_mode = False
        self.reauth_on_disconnect = os.getenv('REAUTH_ON_DISCONNECT', 'false').lower() == 'true'

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _get_verify(self):
        if os.path.exists(self.registry_cert):
            return self.registry_cert
        self.logger.warning(
            "⚠️ Cert del Registry no encontrado (%s); usando verify=False "
            "(aceptable en laboratorio con cert autofirmado)", self.registry_cert
        )
        return False

    # ------------------------------------------------------------------
    # Arranque
    # ------------------------------------------------------------------

    def start(self, interactive: bool = False) -> bool:
        self.interactive_mode = interactive
        self.logger.info("=" * 60)
        self.logger.info("MONITOR %s", self.cp_id)
        self.logger.info("=" * 60)

        # Intentar recuperar credenciales guardadas
        saved_pw = _load_password_secure(self.cp_id, self.data_dir)
        saved_key = self._load_key_file()

        if saved_pw and saved_key:
            self.cp_password = saved_pw
            self.encryption_key = saved_key
            self.logger.info("✅ Credenciales recuperadas de disco")
            if not self._wait_for_engine(timeout=60):
                self.logger.error("❌ Engine no responde")
                return False
            if self._authenticate_with_central():
                self.logger.info("✅ Monitor %s listo (credenciales recuperadas)", self.cp_id)
                self._run_loop()
                return True
            self.logger.warning("⚠️ Credenciales guardadas no funcionan → re-registro")

        # Registro en Registry
        self.logger.info("🔐 Registrando en Registry...")
        if not self._register_in_registry():
            self.logger.error("❌ No se pudo obtener credenciales del Registry")
            return False

        if not self._wait_for_engine(timeout=60):
            self.logger.error("❌ Engine no responde")
            return False

        self.logger.info("🔐 Autenticando en Central...")
        if not self._authenticate_with_central():
            self.logger.error("❌ No se pudo autenticar en Central")
            return False

        self.logger.info("✅ Monitor %s listo y autenticado", self.cp_id)
        self._run_loop()
        return True

    def _run_loop(self):
        try:
            if self.interactive_mode:
                self._interactive_loop()
            else:
                self._health_check_loop()
        except KeyboardInterrupt:
            self.logger.info("Interrumpido por usuario")
        finally:
            self.shutdown()

    # ------------------------------------------------------------------
    # Registry
    # ------------------------------------------------------------------

    def _register_in_registry(self) -> bool:
        """Registra el CP en Registry y obtiene la password."""
        verify = self._get_verify()

        # Intentar cargar password guardada
        saved_pw = _load_password_secure(self.cp_id, self.data_dir)
        if saved_pw:
            self.cp_password = saved_pw
            if self._verify_password_with_registry():
                self.logger.info("✅ Password guardada verificada con Registry")
                return True
            self.logger.warning("⚠️ Password guardada no válida, re-registrando")
            _delete_password_secure(self.cp_id, self.data_dir)

        url = f"{self.registry_url}/api/v1/register"
        payload = {'cp_id': self.cp_id, 'location': self.location, 'price': self.price}
        self.logger.info("Conectando a Registry: %s", url)

        try:
            response = requests.post(url, json=payload, verify=verify, timeout=15)

            if response.status_code == 201:
                data = response.json()
                self.cp_password = data.get('password')
                self.logger.info("✅ CP registrado en Registry")
                _save_password_secure(self.cp_id, self.cp_password, self.data_dir)
                self.logger.info("🔒 Password guardada de forma segura")
                return True

            elif response.status_code == 409:
                self.logger.info("⚠️ CP ya registrado en Registry")
                self.logger.error("❌ No hay password guardada y CP ya existe. "
                                  "Elimina el CP del Registry o restaura la password.")
                return False

            else:
                self.logger.error("❌ Registry respondió: %d — %s",
                                  response.status_code, response.text[:200])
                return False

        except requests.exceptions.ConnectionError as e:
            self.logger.error("❌ No se pudo conectar a Registry: %s", e)
            return False
        except requests.exceptions.Timeout:
            self.logger.error("❌ Timeout conectando a Registry")
            return False
        except Exception as e:
            self.logger.error("❌ Error en Registry: %s", e)
            return False

    def _verify_password_with_registry(self) -> bool:
        """Verifica que la password almacenada es válida."""
        if not self.cp_password:
            return False
        try:
            response = requests.post(
                f"{self.registry_url}/api/v1/authenticate",
                json={'cp_id': self.cp_id, 'password': self.cp_password},
                verify=self._get_verify(),
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Engine
    # ------------------------------------------------------------------

    def _wait_for_engine(self, timeout: int = 60) -> bool:
        start = time.time()
        attempts = 0
        while time.time() - start < timeout:
            attempts += 1
            if self._ping_engine():
                self.logger.info("✅ Engine disponible (intento %d)", attempts)
                return True
            if attempts % 10 == 0:
                self.logger.info("⏳ Esperando Engine... (%d intentos)", attempts)
            time.sleep(1)
        return False

    def _ping_engine(self) -> bool:
        try:
            with socket.create_connection(
                (self.engine_host, self.engine_port), timeout=3
            ) as s:
                s.sendall(b'PING')
                resp = s.recv(64).decode('utf-8').strip()
                return resp in ('PONG', 'KO')
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Central — autenticación # ------------------------------------------------------------------

    def _authenticate_with_central(self) -> bool:
        for attempt in range(1, 11):
            try:
                self.logger.info("🔄 Intento %d/10 autenticación en Central", attempt)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(30)
                sock.connect((self.central_host, self.central_port))

                msg = f"REGISTER|{self.cp_id}|{self.location}|{self.price}|{self.cp_password}"
                sock.sendall(msg.encode('utf-8'))
                self.logger.info("📤 Enviando credenciales a Central...")

                resp = sock.recv(1024).decode('utf-8').strip()
                self.logger.info("📥 Respuesta Central: %s", resp[:60])

                parts = resp.split('|')
                if parts[0] == 'OK' and len(parts) >= 3:
                    self.encryption_key = parts[2]
                    self.logger.info("🔑 Encryption key recibida: %.20s...", self.encryption_key)
                    self._save_key_file(self.encryption_key)
                    self.logger.info("✅ Autenticado en Central — key guardada en %s", self.key_file)
                    sock.settimeout(None)
                    self.central_socket = sock
                    return True

                elif parts[0] == 'ERROR':
                    error_type = parts[1] if len(parts) > 1 else 'UNKNOWN'
                    error_msg = parts[2] if len(parts) > 2 else ''
                    self.logger.error("❌ Central rechazó: %s — %s", error_type, error_msg)
                    sock.close()
                    if error_type == 'INVALID_CREDENTIALS':
                        # Credenciales inválidas: no reintentar
                        return False
                else:
                    self.logger.error("❌ Respuesta inesperada: %s", resp[:80])
                    sock.close()

            except socket.timeout:
                self.logger.error("❌ Timeout conectando a Central")
            except ConnectionRefusedError:
                self.logger.error("❌ Conexión rechazada por Central")
            except Exception as e:
                self.logger.error("❌ Error: %s", e)

            if attempt < 10:
                self.logger.info("⏳ Reintentando en 5s...")
                time.sleep(5)

        return False

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------

    def _save_key_file(self, key: str):
        """Guarda la encryption_key en CP_DATA_DIR con permisos 0o600."""
        os.makedirs(self.data_dir, mode=0o700, exist_ok=True)
        dir_name = os.path.dirname(os.path.abspath(self.key_file))
        with tempfile.NamedTemporaryFile(
            'w', dir=dir_name, delete=False, suffix='.tmp', encoding='utf-8'
        ) as tmp:
            tmp.write(key)
            tmp_path = tmp.name
        os.replace(tmp_path, self.key_file)
        os.chmod(self.key_file, stat.S_IRUSR | stat.S_IWUSR)

    def _load_key_file(self) -> str | None:
        if os.path.exists(self.key_file):
            try:
                with open(self.key_file, 'r', encoding='utf-8') as f:
                    key = f.read().strip()
                return key if key else None
            except Exception:
                return None
        return None

    def _delete_key_file(self):
        try:
            if os.path.exists(self.key_file):
                os.remove(self.key_file)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Re-autenticación # ------------------------------------------------------------------

    def re_authenticate(self) -> bool:
        self.logger.info("=" * 60)
        self.logger.info("🔄 RE-AUTENTICACIÓN")
        self.logger.info("=" * 60)

        if self.central_socket:
            try:
                self.central_socket.close()
            except Exception:
                pass
            self.central_socket = None

        self._delete_key_file()
        self.encryption_key = None

        if not self.cp_password:
            self.logger.error("❌ No hay password del Registry para re-autenticarse")
            return False

        if self._authenticate_with_central():
            self.logger.info("✅ RE-AUTENTICACIÓN EXITOSA")
            if not self.interactive_mode:
                threading.Thread(target=self._health_check_loop, daemon=True).start()
            return True

        self.logger.error("❌ RE-AUTENTICACIÓN FALLIDA")
        return False

    # ------------------------------------------------------------------
    # Health check loop # ------------------------------------------------------------------

    def _health_check_loop(self):
        self.logger.info("🩺 Health checks iniciados")
        consecutive_socket_failures = 0

        while self.running:
            try:
                engine_ok = self._check_engine_health()

                if engine_ok:
                    if self.consecutive_failures >= 3:
                        self.logger.info("✅ Engine RECUPERADO")
                    self.is_healthy = True
                    self.consecutive_failures = 0
                else:
                    self.consecutive_failures += 1
                    self.logger.warning("⚠️ Engine fallo %d/3", self.consecutive_failures)
                    if self.consecutive_failures >= 3:
                        self.is_healthy = False

                # Enviar health a Central
                if not self._send_health_to_central():
                    consecutive_socket_failures += 1
                    if consecutive_socket_failures == 1:
                        self.logger.warning("⚠️ Conexión perdida con Central")

                    if consecutive_socket_failures >= 5:
                        self.logger.error(
                            "❌ Conexión con Central perdida (%d intentos fallidos)",
                            consecutive_socket_failures
                        )
                        if self.reauth_on_disconnect:
                            self.logger.info("🔄 Intentando re-autenticación automática...")
                            if self.re_authenticate():
                                consecutive_socket_failures = 0
                                continue
                        else:
                            self.logger.error(
                                "   Ejecuta 'reauth' en modo interactivo para reconectar"
                            )
                        time.sleep(10)
                        continue
                else:
                    if consecutive_socket_failures > 0:
                        self.logger.info("✅ Conexión con Central restaurada")
                        consecutive_socket_failures = 0

                time.sleep(1)

            except Exception as e:
                if self.running:
                    self.logger.error("❌ Error en health loop: %s", e)
                    time.sleep(1)

    def _check_engine_health(self) -> bool:
        try:
            with socket.create_connection(
                (self.engine_host, self.engine_port), timeout=3
            ) as s:
                s.sendall(b'PING')
                resp = s.recv(64).decode('utf-8').strip()
                return resp == 'PONG'
        except Exception:
            return False

    def _send_health_to_central(self) -> bool:
        if not self.central_socket:
            return False
        try:
            status = b'HEALTH_OK' if self.is_healthy else b'HEALTH_FAIL'
            self.central_socket.sendall(status)
            return True
        except (OSError, BrokenPipeError, ConnectionResetError):
            return False
        except Exception as e:
            self.logger.error("❌ Error enviando health: %s", e)
            return False

    # ------------------------------------------------------------------
    # Modo interactivo
    # ------------------------------------------------------------------

    def _interactive_loop(self):
        self.logger.info("\n" + "=" * 60)
        self.logger.info("MODO INTERACTIVO — %s", self.cp_id)
        self.logger.info("=" * 60)
        self.logger.info("Comandos: reauth | status | health | quit")
        self.logger.info("=" * 60 + "\n")

        # Health en paralelo
        threading.Thread(target=self._health_check_loop, daemon=True).start()

        try:
            while self.running:
                try:
                    cmd = input(f"[{self.cp_id}]> ").strip().lower()
                except EOFError:
                    break

                if cmd == 'reauth':
                    self.re_authenticate()
                elif cmd == 'status':
                    print("\n" + "=" * 60)
                    print(f"CP: {self.cp_id}")
                    print(f"Password Registry: {'✅' if self.cp_password else '❌'}")
                    print(f"Encryption Key:    {'✅ ' + self.encryption_key[:20] + '...' if self.encryption_key else '❌'}")
                    print(f"Socket Central:    {'✅ Conectado' if self.central_socket else '❌'}")
                    print(f"Key file:          {self.key_file}")
                    print(f"Key file existe:   {os.path.exists(self.key_file)}")
                    print("=" * 60)
                elif cmd == 'health':
                    engine_ok = self._check_engine_health()
                    print(f"\nEngine: {'✅ OK' if engine_ok else '❌ FAIL'}")
                    print(f"Fallos consecutivos: {self.consecutive_failures}")
                    print(f"Estado: {'✅ Healthy' if self.is_healthy else '❌ Broken'}\n")
                elif cmd in ('quit', 'exit', 'q'):
                    break
                elif cmd:
                    print(f"Comando desconocido: {cmd}")
        except KeyboardInterrupt:
            print("\nSaliendo...")

    # ------------------------------------------------------------------
    # Shutdown # ------------------------------------------------------------------

    def shutdown(self):
        self.logger.info("🛑 Apagando Monitor...")
        self.running = False
        if self.central_socket:
            try:
                self.central_socket.close()
            except Exception:
                pass
        # NO borrar key file: Engine la necesita mientras esté corriendo
        self.logger.info("💾 Encryption key preservada en %s", self.key_file)
        self.logger.info("✅ Monitor apagado")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    cp_id = os.getenv('CP_ID', 'CP001')
    location = os.getenv('CP_LOCATION', 'Ubicación Desconocida')
    price = float(os.getenv('CP_PRICE', '0.50'))
    central_host = os.getenv('CENTRAL_HOST', '192.168.1.100')
    central_port = int(os.getenv('CENTRAL_PORT', '5001'))
    engine_host = os.getenv('ENGINE_HOST', 'localhost')
    engine_port = int(os.getenv('ENGINE_PORT', '6000'))

    interactive = '--interactive' in sys.argv or '-i' in sys.argv

    monitor = CPMonitor(
        cp_id, location, price,
        central_host, central_port,
        engine_host, engine_port
    )
    def _handle_signal(signum, frame):
        logging.info("🛑 Señal %d recibida — cerrando Monitor...", signum)
        monitor.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    monitor.start(interactive=interactive)
