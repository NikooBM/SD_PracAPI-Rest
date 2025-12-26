"""
EV_CP_M - Monitor Completo y Funcional
Con registro en Registry HTTPS y reconexión automática
"""
import socket
import time
import os
import logging
import requests
import urllib3

# Deshabilitar warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')


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
        
        self.central_socket = None
        self.running = True
        self.is_healthy = True
        self.consecutive_failures = 0
        
        self.logger = logging.getLogger(f"Monitor-{cp_id}")
        
        # Registry
        self.registry_url = os.getenv('REGISTRY_URL', 'https://ev_registry:8443')
        self.cp_password = None
        self.cp_token = None
        self.encryption_key = None

    def start(self) -> bool:
        """Iniciar Monitor"""
        self.logger.info("="*60)
        self.logger.info(f"MONITOR {self.cp_id} - VERSIÓN COMPLETA")
        self.logger.info("="*60)
        
        # 1. Intentar registro en Registry (opcional)
        self.logger.info("🔐 Intentando registro en Registry...")
        try:
            if self._register_in_registry():
                self.logger.info("✅ Registro en Registry exitoso")
            else:
                self.logger.warning("⚠️ Registry no disponible - Continuando")
        except Exception as e:
            self.logger.warning(f"⚠️ Error Registry: {e} - Continuando")
        
        # 2. Esperar a Engine
        self.logger.info("⏳ Esperando Engine...")
        if not self._wait_for_engine(timeout=60):
            self.logger.error("❌ Engine no responde")
            return False
        
        # 3. Registrar en Central
        self.logger.info("📝 Registrando en Central...")
        if not self._register_with_central():
            self.logger.error("❌ No se pudo registrar en Central")
            return False
        
        self.logger.info(f"✅ Monitor {self.cp_id} listo")
        
        try:
            self._health_check_loop()
        except KeyboardInterrupt:
            self.logger.info("Interrumpido")
        finally:
            self.shutdown()
        
        return True

    def _register_in_registry(self) -> bool:
        """Registrar CP en Registry"""
        try:
            url = f"{self.registry_url}/api/v1/register"
            payload = {
                'cp_id': self.cp_id,
                'location': self.location,
                'price': self.price
            }
            
            response = requests.post(
                url, 
                json=payload, 
                verify=False,
                timeout=10
            )
            
            if response.status_code == 201:
                data = response.json()
                self.cp_password = data.get('password')
                self.logger.info(f"✅ CP registrado. Password: {self.cp_password}")
                
                # Guardar password
                try:
                    os.makedirs('/tmp', exist_ok=True)
                    with open(f'/tmp/{self.cp_id}_password.txt', 'w') as f:
                        f.write(self.cp_password)
                except:
                    pass
                
                return True
            
            elif response.status_code == 409:
                # Ya existe
                self.logger.info("⚠️ CP ya registrado")
                try:
                    with open(f'/tmp/{self.cp_id}_password.txt', 'r') as f:
                        self.cp_password = f.read().strip()
                    self.logger.info("✅ Password cargada")
                    return True
                except FileNotFoundError:
                    self.logger.warning("⚠️ Password no encontrada")
                    return False
            
            else:
                self.logger.warning(f"⚠️ Registry código: {response.status_code}")
                return False
        
        except requests.exceptions.ConnectionError:
            self.logger.warning("⚠️ No se pudo conectar a Registry")
            return False
        except requests.exceptions.Timeout:
            self.logger.warning("⚠️ Timeout Registry")
            return False
        except Exception as e:
            self.logger.warning(f"⚠️ Error Registry: {e}")
            return False
    
    def _wait_for_engine(self, timeout: int = 60) -> bool:
        """Esperar a que Engine esté listo"""
        start = time.time()
        while time.time() - start < timeout:
            if self._ping_engine():
                self.logger.info("✅ Engine disponible")
                return True
            time.sleep(1)
        return False

    def _ping_engine(self) -> bool:
        """Verificar si Engine responde"""
        try:
            with socket.create_connection((self.engine_host, self.engine_port), timeout=3) as s:
                s.sendall(b'PING')
                response = s.recv(1024).decode('utf-8').strip()
                return response in ('PONG', 'KO')
        except:
            return False

    def _register_with_central(self) -> bool:
        """Registrar en Central"""
        for attempt in range(1, 11):
            try:
                self.logger.info(f"🔄 Intento {attempt}/10 conexión a Central")
                
                self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.central_socket.settimeout(10)
                
                self.central_socket.connect((self.central_host, self.central_port))
                self.logger.info(f"✅ Conectado a {self.central_host}:{self.central_port}")
                
                message = f"REGISTER|{self.cp_id}|{self.location}|{self.price}"
                self.central_socket.sendall(message.encode('utf-8'))
                self.logger.info(f"📤 Enviado: {message}")
                
                response = self.central_socket.recv(1024).decode('utf-8').strip()
                self.logger.info(f"📥 Recibido: {response}")
                
                # Extraer encryption_key
                parts = response.split('|')
                if len(parts) >= 3 and parts[0] == 'OK':
                    self.encryption_key = parts[2]
                    self.logger.info(f"🔑 Encryption key: {self.encryption_key[:20]}...")
                    
                    # Guardar para Engine
                    try:
                        os.makedirs('/tmp', exist_ok=True)
                        with open(f'/tmp/{self.cp_id}_encryption_key.txt', 'w') as f:
                            f.write(self.encryption_key)
                        self.logger.info("✅ Key guardada para Engine")
                    except Exception as e:
                        self.logger.warning(f"⚠️ No se pudo guardar key: {e}")
                
                if response.startswith("OK"):
                    self.logger.info(f"✅ Registrado en Central")
                    self.central_socket.settimeout(None)
                    return True
                else:
                    self.logger.error(f"❌ Central respondió: {response}")
                    if self.central_socket:
                        self.central_socket.close()
                    self.central_socket = None
            
            except socket.timeout:
                self.logger.error(f"❌ Timeout Central")
            except ConnectionRefusedError:
                self.logger.error(f"❌ Conexión rechazada por Central")
            except Exception as e:
                self.logger.error(f"❌ Error: {e}")
            
            if self.central_socket:
                try:
                    self.central_socket.close()
                except:
                    pass
                self.central_socket = None
            
            if attempt < 10:
                self.logger.info(f"⏳ Reintentando en 5s...")
                time.sleep(5)
        
        return False

    def _health_check_loop(self):
        """Loop principal de health checks"""
        self.logger.info("🩺 Health checks iniciados")
        
        while self.running:
            try:
                # Verificar Engine
                engine_ok = self._check_engine_health()
                
                if engine_ok:
                    if self.consecutive_failures > 0:
                        self.logger.info("✅ Engine RECUPERADO")
                        self.is_healthy = True
                        self.consecutive_failures = 0
                    else:
                        self.is_healthy = True
                else:
                    self.consecutive_failures += 1
                    self.logger.warning(f"⚠️ Engine fallo {self.consecutive_failures}/3")
                    
                    if self.consecutive_failures >= 3:
                        if self.is_healthy:
                            self.logger.error("💥 AVERÍA DETECTADA")
                            self.is_healthy = False
                
                # Enviar health a Central
                if not self._send_health_to_central():
                    self.logger.warning("⚠️ Conexión perdida con Central")
                    self.logger.info("🔄 Intentando reconectar...")
                    
                    if not self._register_with_central():
                        self.logger.error("❌ No se pudo reconectar")
                        break
                
                time.sleep(1)
            
            except Exception as e:
                if self.running:
                    self.logger.error(f"❌ Error health loop: {e}")
                    time.sleep(1)

    def _check_engine_health(self) -> bool:
        """Verificar salud de Engine"""
        try:
            with socket.create_connection((self.engine_host, self.engine_port), timeout=3) as s:
                s.sendall(b'PING')
                response = s.recv(1024).decode('utf-8').strip()
                return response == 'PONG'
        except:
            return False

    def _send_health_to_central(self) -> bool:
        """Enviar estado de salud a Central"""
        if self.central_socket is None:
            return False
        
        try:
            status = 'HEALTH_OK' if self.is_healthy else 'HEALTH_FAIL'
            self.central_socket.sendall(status.encode('utf-8'))
            return True
        except (OSError, BrokenPipeError, ConnectionResetError):
            return False
        except Exception as e:
            self.logger.error(f"❌ Error enviando health: {e}")
            return False

    def shutdown(self):
        """Apagar Monitor"""
        self.logger.info("🛑 Apagando Monitor...")
        self.running = False
        
        if self.central_socket:
            try:
                self.central_socket.close()
            except:
                pass
        
        self.logger.info("✅ Monitor apagado")


if __name__ == '__main__':
    cp_id = os.getenv('CP_ID', 'CP001')
    location = os.getenv('CP_LOCATION', 'Ubicación Desconocida')
    price = float(os.getenv('CP_PRICE', '0.50'))
    central_host = os.getenv('CENTRAL_HOST', '192.168.1.100')
    central_port = int(os.getenv('CENTRAL_PORT', '5001'))
    engine_host = os.getenv('ENGINE_HOST', 'localhost')
    engine_port = int(os.getenv('ENGINE_PORT', '6000'))
    
    monitor = CPMonitor(cp_id, location, price, central_host, central_port, 
                       engine_host, engine_port)
    monitor.start()