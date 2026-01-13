"""
EV_CP_M - Monitor con Autenticación Completa según Enunciado
Incluye: verificación con Registry, envío de password a Central, re-autenticación
"""
import socket
import time
import os
import logging
import requests
import urllib3
import threading

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
        
        # Archivos
        self.key_file = f'/tmp/{cp_id}_encryption_key.txt'
        self.password_file = f'/tmp/{cp_id}_password.txt'
        
        # Para modo interactivo (re-autenticación manual)
        self.interactive_mode = False

    def start(self, interactive: bool = False) -> bool:
        """
        Iniciar Monitor - CORREGIDO: verificar encryption key persistente
        
        Args:
            interactive: Si True, permite comandos interactivos para re-autenticación
        """
        self.interactive_mode = interactive
        
        self.logger.info("="*60)
        self.logger.info(f"MONITOR {self.cp_id} - VERSIÓN CORREGIDA")
        self.logger.info("="*60)
        
        # 1. Verificar si hay credenciales guardadas
        self.logger.info("🔍 Verificando credenciales guardadas...")
        
        has_password = os.path.exists(self.password_file)
        has_key = os.path.exists(self.key_file)
        
        if has_password and has_key:
            # CRÍTICO: Intentar cargar credenciales existentes
            try:
                with open(self.password_file, 'r') as f:
                    self.cp_password = f.read().strip()
                with open(self.key_file, 'r') as f:
                    self.encryption_key = f.read().strip()
                
                if self.cp_password and self.encryption_key:
                    self.logger.info("✅ Credenciales recuperadas de archivos locales")
                    self.logger.info("   Intentando conectar directamente...")
                    
                    # 2. Esperar Engine
                    self.logger.info("⏳ Esperando Engine...")
                    if not self._wait_for_engine(timeout=60):
                        self.logger.error("❌ Engine no responde")
                        return False
                    
                    # 3. Conectar a Central con credenciales existentes
                    if self._authenticate_with_central():
                        self.logger.info(f"✅ Monitor {self.cp_id} listo (credenciales recuperadas)")
                        
                        # 4. Health check loop o modo interactivo
                        try:
                            if interactive:
                                self._interactive_loop()
                            else:
                                self._health_check_loop()
                        except KeyboardInterrupt:
                            self.logger.info("Interrumpido por usuario")
                        finally:
                            self.shutdown()
                        
                        return True
                    else:
                        self.logger.warning("⚠️ Credenciales guardadas no funcionan")
                        self.logger.warning("   Procediendo a re-registro...")
            except Exception as e:
                self.logger.warning(f"⚠️ Error cargando credenciales: {e}")
        
        # Si no hay credenciales o no funcionan, proceder con registro normal
        self.logger.info("🔐 Registrando en Registry para obtener credenciales...")
        if not self._register_in_registry():
            self.logger.error("❌ No se pudo obtener credenciales del Registry")
            return False
        
        # 2. Esperar Engine
        self.logger.info("⏳ Esperando Engine...")
        if not self._wait_for_engine(timeout=60):
            self.logger.error("❌ Engine no responde después de 60s")
            return False
        
        # 3. Autenticar en Central
        self.logger.info("🔐 Autenticando en Central...")
        if not self._authenticate_with_central():
            self.logger.error("❌ No se pudo autenticar en Central")
            return False
        
        # 4. Verificar encryption key
        if self.encryption_key:
            if not self._verify_encryption_key_saved():
                self.logger.error("❌ Encryption key no se guardó correctamente")
                return False
        
        self.logger.info(f"✅ Monitor {self.cp_id} listo y autenticado")
        
        # 5. Health check loop o modo interactivo
        try:
            if interactive:
                self._interactive_loop()
            else:
                self._health_check_loop()
        except KeyboardInterrupt:
            self.logger.info("Interrumpido por usuario")
        finally:
            self.shutdown()
        
        return True

    def _register_in_registry(self) -> bool:
        """
        Registrar CP en Registry para obtener credenciales (password)
        Este paso es OBLIGATORIO según el enunciado
        """
        try:
            # Intentar cargar password guardado
            if os.path.exists(self.password_file):
                try:
                    with open(self.password_file, 'r') as f:
                        self.cp_password = f.read().strip()
                    self.logger.info("✅ Password recuperada de archivo local")
                    return True
                except:
                    pass
            
            # Registrarse en Registry
            url = f"{self.registry_url}/api/v1/register"
            payload = {
                'cp_id': self.cp_id,
                'location': self.location,
                'price': self.price
            }
            
            self.logger.info(f"Conectando a Registry: {url}")
            
            response = requests.post(
                url, 
                json=payload, 
                verify=False,
                timeout=15
            )
            
            if response.status_code == 201:
                data = response.json()
                self.cp_password = data.get('password')
                self.logger.info(f"✅ CP registrado en Registry")
                self.logger.info(f"   ⚠️ Password: {self.cp_password}")
                self.logger.info(f"   ⚠️ GUARDAR ESTA CONTRASEÑA - No se mostrará de nuevo")
                
                # Guardar password
                try:
                    os.makedirs('/tmp', exist_ok=True)
                    with open(self.password_file, 'w') as f:
                        f.write(self.cp_password)
                    self.logger.info("   Password guardada en archivo")
                except Exception as e:
                    self.logger.warning(f"⚠️ No se pudo guardar password: {e}")
                
                return True
            
            elif response.status_code == 409:
                # Ya registrado, intentar autenticar para verificar password
                self.logger.info("⚠️ CP ya registrado en Registry")
                
                # Intentar cargar password de archivo
                if os.path.exists(self.password_file):
                    with open(self.password_file, 'r') as f:
                        self.cp_password = f.read().strip()
                    
                    # Verificar que la password es válida
                    if self._verify_password_with_registry():
                        self.logger.info("✅ Password verificada con Registry")
                        return True
                    else:
                        self.logger.error("❌ Password guardada no es válida")
                        return False
                else:
                    self.logger.error("❌ CP ya registrado pero no hay password guardada")
                    self.logger.error("   Elimina el CP del Registry o proporciona la password")
                    return False
            
            else:
                self.logger.error(f"❌ Registry respondió con código: {response.status_code}")
                self.logger.error(f"   Respuesta: {response.text[:200]}")
                return False
        
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"❌ No se pudo conectar a Registry: {e}")
            self.logger.error("   El Registry debe estar corriendo para obtener credenciales")
            return False
        except requests.exceptions.Timeout:
            self.logger.error("❌ Timeout conectando a Registry")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error en Registry: {e}")
            return False
    
    def _verify_password_with_registry(self) -> bool:
        """Verificar que la password es válida autenticando con Registry"""
        try:
            url = f"{self.registry_url}/api/v1/authenticate"
            payload = {
                'cp_id': self.cp_id,
                'password': self.cp_password
            }
            
            response = requests.post(url, json=payload, verify=False, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def _wait_for_engine(self, timeout: int = 60) -> bool:
        """Esperar a que Engine esté listo"""
        start = time.time()
        attempts = 0
        
        while time.time() - start < timeout:
            attempts += 1
            if self._ping_engine():
                self.logger.info(f"✅ Engine disponible (intento {attempts})")
                return True
            
            if attempts % 10 == 0:
                self.logger.info(f"   Esperando Engine... ({attempts} intentos)")
            
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

    def _authenticate_with_central(self) -> bool:
        """
        Autenticarse en Central enviando las credenciales del Registry
        Central verificará las credenciales antes de entregar la encryption key
        """
        for attempt in range(1, 11):
            try:
                self.logger.info(f"🔄 Intento {attempt}/10 autenticación en Central")
                
                self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.central_socket.settimeout(10)
                
                self.central_socket.connect((self.central_host, self.central_port))
                self.logger.info(f"✅ Conectado a {self.central_host}:{self.central_port}")
                
                # Enviar mensaje de registro CON PASSWORD del Registry
                message = f"REGISTER|{self.cp_id}|{self.location}|{self.price}|{self.cp_password}"
                self.central_socket.sendall(message.encode('utf-8'))
                self.logger.info(f"📤 Enviando credenciales de Registry...")
                
                response = self.central_socket.recv(1024).decode('utf-8').strip()
                self.logger.info(f"📥 Respuesta de Central: {response[:50]}...")
                
                # Procesar respuesta
                parts = response.split('|')
                
                if parts[0] == 'OK' and len(parts) >= 3:
                    # Autenticación exitosa
                    self.encryption_key = parts[2]
                    self.logger.info(f"🔑 Encryption key recibida: {self.encryption_key[:20]}...")
                    
                    # Guardar encryption key
                    try:
                        if self.encryption_key is not None and isinstance(self.encryption_key, str):
                            os.makedirs('/tmp', exist_ok=True)
                            with open(self.key_file, 'w') as f:
                                f.write(self.encryption_key)
                            
                            # Verificar escritura
                            with open(self.key_file, 'r') as f:
                                saved_key = f.read().strip()
                            
                            if saved_key == self.encryption_key:
                                self.logger.info("✅ Encryption key guardada y verificada")
                            else:
                                self.logger.error("❌ Encryption key no se guardó correctamente")
                                # Reintentar
                                with open(self.key_file, 'w') as f:
                                    f.write(self.encryption_key)
                        else:
                            self.logger.error("❌ Encryption key es None")
                            return False
                    
                    except Exception as e:
                        self.logger.error(f"❌ Error guardando encryption key: {e}")
                        return False
                    
                    self.logger.info(f"✅ Autenticado en Central exitosamente")
                    self.central_socket.settimeout(None)
                    return True
                
                elif parts[0] == 'ERROR':
                    error_type = parts[1] if len(parts) > 1 else 'UNKNOWN'
                    error_msg = parts[2] if len(parts) > 2 else 'Error desconocido'
                    
                    self.logger.error(f"❌ Central rechazó autenticación: {error_type}")
                    self.logger.error(f"   Mensaje: {error_msg}")
                    
                    if error_type == 'INVALID_CREDENTIALS':
                        self.logger.error("   Las credenciales del Registry no son válidas")
                        self.logger.error("   Verifica que el CP esté registrado en Registry")
                        return False  # No reintentar, credenciales inválidas
                    
                    # Cerrar socket
                    if self.central_socket:
                        self.central_socket.close()
                    self.central_socket = None
                
                else:
                    self.logger.error(f"❌ Respuesta inesperada: {response}")
                    if self.central_socket:
                        self.central_socket.close()
                    self.central_socket = None
            
            except socket.timeout:
                self.logger.error(f"❌ Timeout conectando a Central")
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
    
    def _verify_encryption_key_saved(self) -> bool:
        """Verificar que la encryption key se guardó correctamente"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                if os.path.exists(self.key_file):
                    with open(self.key_file, 'r') as f:
                        saved_key = f.read().strip()
                    
                    if saved_key == self.encryption_key and len(saved_key) > 0:
                        self.logger.info("✅ Encryption key verificada")
                        return True
                    else:
                        self.logger.warning(f"⚠️ Key guardada no coincide (intento {attempt+1}/{max_attempts})")
                        if self.encryption_key is not None:
                            with open(self.key_file, 'w') as f:
                                f.write(self.encryption_key)
                        time.sleep(0.5)
                else:
                    self.logger.warning(f"⚠️ Archivo de key no existe (intento {attempt+1}/{max_attempts})")
                    if self.encryption_key is not None:
                        with open(self.key_file, 'w') as f:
                            f.write(self.encryption_key)
                    time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Error verificando key: {e}")
                time.sleep(0.5)
        
        return False

    def re_authenticate(self) -> bool:
        """
        RE-AUTENTICARSE en Central
        Usado cuando Central revoca la encryption key
        """
        self.logger.info("="*60)
        self.logger.info("🔄 INICIANDO RE-AUTENTICACIÓN")
        self.logger.info("="*60)
        
        # Cerrar socket existente
        if self.central_socket:
            try:
                self.central_socket.close()
            except:
                pass
            self.central_socket = None
        
        # Limpiar encryption key antigua
        try:
            if os.path.exists(self.key_file):
                os.remove(self.key_file)
        except:
            pass
        
        self.encryption_key = None
        
        # Verificar que tenemos password del Registry
        if not self.cp_password:
            self.logger.error("❌ No hay password del Registry")
            self.logger.error("   Ejecuta primero el registro en Registry")
            return False
        
        # Autenticar de nuevo
        if self._authenticate_with_central():
            self.logger.info("="*60)
            self.logger.info("✅ RE-AUTENTICACIÓN EXITOSA")
            self.logger.info("="*60)
            
            # Reanudar health checks
            if not self.interactive_mode:
                threading.Thread(target=self._health_check_loop, daemon=True).start()
            
            return True
        else:
            self.logger.error("="*60)
            self.logger.error("❌ RE-AUTENTICACIÓN FALLIDA")
            self.logger.error("="*60)
            return False

    def _health_check_loop(self):
        """Loop principal de health checks - CORREGIDO: NO re-autenticación automática"""
        self.logger.info("🩺 Health checks iniciados")
        
        consecutive_connection_failures = 0
        
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
                            self.logger.error("💥 AVERÍA DETECTADA en Engine")
                            self.is_healthy = False
                
                # Enviar health a Central
                if not self._send_health_to_central():
                    consecutive_connection_failures += 1
                    
                    if consecutive_connection_failures == 1:
                        self.logger.warning("⚠️ Conexión perdida con Central")
                    
                    # CRÍTICO: NO RE-AUTENTICARSE AUTOMÁTICAMENTE
                    # Según PDF: "debe ser algo manual"
                    if consecutive_connection_failures >= 5:
                        self.logger.error("❌ Conexión con Central perdida definitivamente")
                        self.logger.error("   Ejecuta comando 'reauth' para re-autenticarse")
                        
                        # En modo interactivo, el usuario usa el comando
                        # En modo no-interactivo, simplemente esperar
                        if not self.interactive_mode:
                            # Esperar sin intentar re-autenticar
                            time.sleep(5)
                    
                    time.sleep(2)
                    continue
                else:
                    # Conexión OK
                    if consecutive_connection_failures > 0:
                        self.logger.info("✅ Conexión con Central restaurada")
                        consecutive_connection_failures = 0
                
                time.sleep(1)
            
            except Exception as e:
                if self.running:
                    self.logger.error(f"❌ Error en health loop: {e}")
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

    def _interactive_loop(self):
        """
        Modo interactivo para pruebas y re-autenticación manual
        """
        self.logger.info("\n" + "="*60)
        self.logger.info("MODO INTERACTIVO")
        self.logger.info("="*60)
        self.logger.info("Comandos disponibles:")
        self.logger.info("  reauth   - Re-autenticarse en Central")
        self.logger.info("  status   - Ver estado de autenticación")
        self.logger.info("  health   - Ver estado de health checks")
        self.logger.info("  quit     - Salir")
        self.logger.info("="*60 + "\n")
        
        # Iniciar health checks en paralelo
        threading.Thread(target=self._health_check_loop, daemon=True).start()
        
        try:
            while self.running:
                cmd = input(f"[{self.cp_id}]> ").strip().lower()
                
                if cmd == 'reauth':
                    self.re_authenticate()
                
                elif cmd == 'status':
                    print("\n" + "="*60)
                    print("ESTADO DE AUTENTICACIÓN")
                    print("="*60)
                    print(f"CP ID:              {self.cp_id}")
                    print(f"Password Registry:  {'✅ Presente' if self.cp_password else '❌ Ausente'}")
                    print(f"Encryption Key:     {'✅ Presente' if self.encryption_key else '❌ Ausente'}")
                    print(f"Socket Central:     {'✅ Conectado' if self.central_socket else '❌ Desconectado'}")
                    
                    if self.encryption_key:
                        print(f"Key hash:           {self.encryption_key[:30]}...")
                    
                    print("="*60 + "\n")
                
                elif cmd == 'health':
                    engine_ok = self._check_engine_health()
                    print("\n" + "="*60)
                    print("ESTADO DE HEALTH")
                    print("="*60)
                    print(f"Engine:             {'✅ OK' if engine_ok else '❌ FAIL'}")
                    print(f"Fallos consecutivos: {self.consecutive_failures}")
                    print(f"Estado:             {'✅ Healthy' if self.is_healthy else '❌ Broken'}")
                    print("="*60 + "\n")
                
                elif cmd in ('quit', 'exit', 'q'):
                    break
                
                else:
                    print(f"Comando desconocido: {cmd}")
        
        except (KeyboardInterrupt, EOFError):
            print("\nSaliendo...")

    def shutdown(self):
        """Apagar Monitor - CORREGIDO: NO eliminar encryption key automáticamente"""
        self.logger.info("🛑 Apagando Monitor...")
        self.running = False
        
        if self.central_socket:
            try:
                self.central_socket.close()
            except:
                pass
        
        # CRÍTICO: NO eliminar encryption key para permitir reconexión
        # Solo eliminarla cuando se revoca explícitamente
        self.logger.info("💾 Encryption key preservada para próxima reconexión")
        
        self.logger.info("✅ Monitor apagado")


if __name__ == '__main__':
    import sys
    
    cp_id = os.getenv('CP_ID', 'CP001')
    location = os.getenv('CP_LOCATION', 'Ubicación Desconocida')
    price = float(os.getenv('CP_PRICE', '0.50'))
    central_host = os.getenv('CENTRAL_HOST', '192.168.1.100')
    central_port = int(os.getenv('CENTRAL_PORT', '5001'))
    engine_host = os.getenv('ENGINE_HOST', 'localhost')
    engine_port = int(os.getenv('ENGINE_PORT', '6000'))
    
    # Modo interactivo si se pasa argumento --interactive
    interactive = '--interactive' in sys.argv or '-i' in sys.argv
    
    monitor = CPMonitor(cp_id, location, price, central_host, central_port, 
                       engine_host, engine_port)
    monitor.start(interactive=interactive)