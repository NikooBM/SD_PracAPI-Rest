"""
EV_W - Weather Control Office
Monitoriza condiciones climáticas y notifica alertas a Central.
Release 2 - Práctica SD 25/26

CORRECCIONES APLICADAS:
  [1.5] Envía X-API-Key en cada petición a API_Central
  [2.3] CENTRAL_API_URL ya apunta a 8082 desde docker-compose
  [3.3] Carga localizaciones desde archivo JSON al arrancar
  [3.3] Guarda localizaciones cuando se añaden/eliminan (persistencia entre reinicios)
  [3.5] Valida que la ciudad existe en OpenWeather al añadirla
  [5.4] Manejo de SIGTERM para cierre limpio
  [5.6] Logs de alta frecuencia a nivel DEBUG
"""
import os
import time
import logging
import json
import threading
import signal
import sys
from typing import Dict, List, Optional
from datetime import datetime

import requests

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger('Weather')


class WeatherControlOffice:
    """
    Monitoriza el clima en localizaciones de CPs y notifica alertas
    cuando la temperatura está por debajo de 0°C.
    """

    def __init__(self, api_key: str, central_api_url: str,
                 weather_api_key: str = '', check_interval: int = 4):
        self.api_key = api_key                          # OpenWeather key
        self.central_api_url = central_api_url.rstrip('/')
        self.weather_api_key = weather_api_key          # FIX [1.5]: key para API_Central
        self.check_interval = check_interval

        self.locations: Dict[str, str] = {}             # {cp_id: city}
        self.alerts: Dict[str, bool] = {}               # {cp_id: in_alert}
        self.last_temperatures: Dict[str, Optional[float]] = {}

        self.running = True
        self.monitor_thread: Optional[threading.Thread] = None

        self.openweather_base_url = "https://api.openweathermap.org/data/2.5/weather"

        # FIX [3.3]: archivo de persistencia de localizaciones
        self.locations_file = os.getenv(
            'WEATHER_LOCATIONS_FILE', '/app/weather/locations.json'
        )

    # ------------------------------------------------------------------
    # FIX [3.3]: Persistencia de localizaciones
    # ------------------------------------------------------------------

    def _load_locations_from_file(self):
        """Cargar localizaciones desde archivo JSON al arrancar."""
        if not os.path.exists(self.locations_file):
            logger.info(f"📭 No hay archivo de localizaciones: {self.locations_file}")
            return

        try:
            with open(self.locations_file, 'r', encoding='utf-8') as f:
                locs = json.load(f)
            for cp_id, city in locs.items():
                # Añadir sin validar para no bloquear el arranque
                self.locations[cp_id] = city
                self.alerts[cp_id] = False
                self.last_temperatures[cp_id] = None
                logger.info(f"📍 Localización cargada desde archivo: {cp_id} → {city}")
            logger.info(f"✅ {len(locs)} localizaciones cargadas desde {self.locations_file}")
        except Exception as e:
            logger.error(f"❌ Error cargando localizaciones: {e}")

    def _save_locations_to_file(self):
        """Guardar localizaciones en archivo JSON."""
        try:
            # Escritura atómica
            tmp = self.locations_file + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self.locations, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.locations_file)
        except Exception as e:
            logger.error(f"❌ Error guardando localizaciones: {e}")

    # ------------------------------------------------------------------
    # Localización
    # ------------------------------------------------------------------

    def add_location(self, cp_id: str, city: str) -> bool:
        """
        Añadir localización. FIX [3.5]: valida que la ciudad existe en OpenWeather.
        """
        # FIX [3.5]: Validar ciudad antes de añadir
        weather = self.get_weather(city)
        if weather is None:
            logger.error(f"❌ Ciudad '{city}' no encontrada en OpenWeather. No se añade.")
            return False

        self.locations[cp_id] = city
        self.alerts[cp_id] = False
        self.last_temperatures[cp_id] = weather['temp']
        logger.info(f"📍 Añadida localización: {cp_id} → {city} ({weather['temp']}°C)")

        # FIX [3.3]: persistir
        self._save_locations_to_file()

        # Notificar registro a Central
        self._notify_central(cp_id, 'REGISTER', weather['temp'], city)

        # Si ya está bajo cero, alertar inmediatamente
        if weather['temp'] < 0.0:
            logger.warning(f"🥶 ALERTA INMEDIATA: {city} ({cp_id}) - {weather['temp']}°C")
            self._notify_central(cp_id, 'START', weather['temp'], city)
            self.alerts[cp_id] = True

        return True

    def remove_location(self, cp_id: str):
        """Eliminar localización y persistir cambio."""
        if cp_id in self.locations:
            del self.locations[cp_id]
            self.alerts.pop(cp_id, None)
            self.last_temperatures.pop(cp_id, None)
            self._save_locations_to_file()
            logger.info(f"📍 Eliminada localización: {cp_id}")

    # ------------------------------------------------------------------
    # Consulta OpenWeather
    # ------------------------------------------------------------------

    def get_weather(self, city: str) -> Optional[Dict]:
        """Consultar clima de una ciudad en OpenWeather."""
        try:
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric',
                'lang': 'es'
            }
            response = requests.get(
                self.openweather_base_url, params=params, timeout=10
            )
            if response.status_code == 404:
                logger.error(f"❌ Ciudad no encontrada: {city}")
                return None
            response.raise_for_status()
            data = response.json()
            return {
                'city': data['name'],
                'temp': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'temp_min': data['main']['temp_min'],
                'temp_max': data['main']['temp_max'],
                'description': data['weather'][0]['description'],
                'humidity': data['main']['humidity'],
                'wind_speed': data['wind']['speed']
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error consultando clima de {city}: {e}")
            return None
        except (KeyError, IndexError) as e:
            logger.error(f"❌ Error parseando respuesta para {city}: {e}")
            return None

    # ------------------------------------------------------------------
    # FIX [1.5]: Notificación a Central con X-API-Key
    # ------------------------------------------------------------------

    def _notify_central(self, cp_id: str, alert_type: str,
                        temperature: float, city: str) -> bool:
        """Notificar alerta a Central vía API_Central (con autenticación)."""
        try:
            endpoint = f"{self.central_api_url}/api/v1/weather/alert"
            payload = {
                'cp_id': cp_id,
                'alert_type': alert_type,
                'temperature': temperature,
                'city': city,
                'timestamp': datetime.now().isoformat()
            }
            # FIX [1.5]: cabecera de autenticación
            headers = {
                'X-API-Key': self.weather_api_key,
                'Content-Type': 'application/json'
            }
            response = requests.post(
                endpoint, json=payload, headers=headers, timeout=10
            )
            response.raise_for_status()
            logger.info(f"✅ Notificación enviada: {alert_type} para {cp_id} ({temperature}°C)")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error notificando a Central: {e}")
            return False

    # Alias público
    def notify_central(self, cp_id: str, alert_type: str,
                       temperature: float, city: str) -> bool:
        return self._notify_central(cp_id, alert_type, temperature, city)

    # ------------------------------------------------------------------
    # Loop de monitorización
    # ------------------------------------------------------------------

    def check_all_locations(self):
        """Verificar clima de todas las localizaciones."""
        for cp_id, city in list(self.locations.items()):
            weather = self.get_weather(city)
            if weather is None:
                logger.warning(f"⚠️ No se pudo obtener clima de {city} ({cp_id})")
                continue

            temperature = weather['temp']
            self.last_temperatures[cp_id] = temperature

            is_below_zero = temperature < 0.0
            was_in_alert = self.alerts.get(cp_id, False)

            if is_below_zero and not was_in_alert:
                logger.warning(f"🥶 ALERTA: {city} ({cp_id}) - {temperature}°C")
                self._notify_central(cp_id, 'START', temperature, city)
                self.alerts[cp_id] = True

            elif not is_below_zero and was_in_alert:
                logger.info(f"☀️ Alerta cancelada: {city} ({cp_id}) - {temperature}°C")
                self._notify_central(cp_id, 'END', temperature, city)
                self.alerts[cp_id] = False

            else:
                # FIX [5.6]: log de estado rutinario a DEBUG
                status = "❄️ ALERTA ACTIVA" if is_below_zero else "✅ OK"
                logger.debug(
                    f"{status}: {city} ({cp_id}) - {temperature}°C - {weather['description']}"
                )

    def monitor_loop(self):
        """Loop principal de monitorización."""
        logger.info(f"🔄 Iniciando monitorización cada {self.check_interval}s")
        while self.running:
            try:
                if self.locations:
                    self.check_all_locations()
                else:
                    logger.debug("⏳ No hay localizaciones para monitorizar")
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"❌ Error en monitor loop: {e}")
                time.sleep(self.check_interval)

    def start(self):
        """Iniciar monitorización en segundo plano."""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.running = True
            self.monitor_thread = threading.Thread(
                target=self.monitor_loop, daemon=True
            )
            self.monitor_thread.start()
            logger.info("🚀 Weather Control Office iniciado")

    def stop(self):
        """Detener monitorización."""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        logger.info("🛑 Weather Control Office detenido")

    def get_status(self) -> Dict:
        return {
            'locations': dict(self.locations),
            'alerts': dict(self.alerts),
            'temperatures': dict(self.last_temperatures),
            'monitoring': self.running
        }

    # ------------------------------------------------------------------
    # Menú interactivo
    # ------------------------------------------------------------------

    def interactive_menu(self):
        self._print_help()
        while True:
            try:
                cmd = input("\n[Weather]> ").strip().lower()

                if cmd == 'add':
                    cp_id = input("CP_ID: ").strip().upper()
                    city = input("Ciudad: ").strip()
                    if cp_id and city:
                        if self.add_location(cp_id, city):
                            print(f"✅ {cp_id} → {city} añadido correctamente")
                        else:
                            print(f"❌ No se pudo añadir {city} (ciudad no válida o no encontrada)")
                    else:
                        print("❌ Datos inválidos")

                elif cmd == 'remove':
                    cp_id = input("CP_ID: ").strip().upper()
                    self.remove_location(cp_id)

                elif cmd == 'list':
                    self._print_locations()

                elif cmd == 'status':
                    self._print_status()

                elif cmd == 'check':
                    print("\n🔄 Verificando clima...")
                    self.check_all_locations()

                elif cmd == 'help':
                    self._print_help()

                elif cmd in ('quit', 'exit', 'q'):
                    print("\nSaliendo...")
                    break

                else:
                    print(f"❌ Comando desconocido: '{cmd}'")

            except (KeyboardInterrupt, EOFError):
                print("\n\nSaliendo...")
                break
            except Exception as e:
                logger.error(f"❌ Error: {e}")

        self.stop()

    def _print_help(self):
        print("\n" + "=" * 60)
        print("EV_W - WEATHER CONTROL OFFICE")
        print("=" * 60)
        print("Comandos disponibles:")
        print("  add      - Añadir localización (valida ciudad en OpenWeather)")
        print("  remove   - Eliminar localización")
        print("  list     - Listar localizaciones con temperatura actual")
        print("  status   - Ver estado actual del sistema")
        print("  check    - Forzar verificación de clima ahora")
        print("  help     - Mostrar esta ayuda")
        print("  quit     - Salir")
        print("=" * 60)

    def _print_locations(self):
        if not self.locations:
            print("\n📭 No hay localizaciones configuradas")
            return
        print("\n" + "=" * 60)
        print("📍 LOCALIZACIONES MONITORIZADAS")
        print("=" * 60)
        for cp_id, city in self.locations.items():
            temp = self.last_temperatures.get(cp_id)
            alert = self.alerts.get(cp_id, False)
            status = "❄️ ALERTA" if alert else "✅ OK"
            temp_str = f"{temp:.1f}°C" if temp is not None else "N/A"
            print(f"{cp_id:10} → {city:20} | {temp_str:10} | {status}")
        print("=" * 60)

    def _print_status(self):
        print("\n" + "=" * 60)
        print("📊 ESTADO DEL SISTEMA")
        print("=" * 60)
        print(f"Localizaciones:    {len(self.locations)}")
        print(f"Alertas activas:   {sum(self.alerts.values())}")
        print(f"Monitorización:    {'🟢 ACTIVA' if self.running else '🔴 INACTIVA'}")
        print(f"Intervalo:         {self.check_interval}s")
        print("=" * 60)
        if self.locations:
            self._print_locations()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("EV_W - Weather Control Office")
    logger.info("Release 2 - Práctica SD 25/26")
    logger.info("=" * 60)

    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        logger.error("❌ Error: OPENWEATHER_API_KEY no configurada")
        logger.info("   Obtén tu API key en: https://openweathermap.org/api")
        sys.exit(1)

    central_api_url = os.getenv('CENTRAL_API_URL', 'http://localhost:8082')
    weather_api_key = os.getenv('WEATHER_API_KEY', '')
    check_interval = int(os.getenv('CHECK_INTERVAL', '4'))

    logger.info(f"🔑 OpenWeather API Key: {api_key[:8]}...")
    logger.info(f"🌐 Central API: {central_api_url}")
    logger.info(f"⏱️  Intervalo: {check_interval}s")

    if not weather_api_key:
        logger.warning("⚠️  WEATHER_API_KEY no configurada; las peticiones a API_Central serán rechazadas")

    weather_office = WeatherControlOffice(
        api_key, central_api_url, weather_api_key, check_interval
    )

    # FIX [3.3]: Cargar localizaciones persistidas al arrancar
    weather_office._load_locations_from_file()

    # FIX [5.4]: Manejo de SIGTERM
    def handle_sigterm(signum, frame):
        logger.info("🛑 SIGTERM recibido, cerrando EV_W...")
        weather_office._save_locations_to_file()
        weather_office.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    weather_office.start()
    weather_office.interactive_menu()
