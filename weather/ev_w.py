"""
EV_W - Weather Control Office
Monitoriza condiciones climáticas y notifica alertas a Central
Release 2 - Práctica SD 25/26
"""
import os
import time
import logging
import requests
import json
import threading
from typing import Dict, List, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger('Weather')

class WeatherControlOffice:
    """
    Monitoriza el clima en localizaciones de CPs y notifica alertas
    cuando la temperatura está por debajo de 0°C
    """
    
    def __init__(self, api_key: str, central_api_url: str, check_interval: int = 4):
        self.api_key = api_key
        self.central_api_url = central_api_url.rstrip('/')
        self.check_interval = check_interval  # segundos
        
        self.locations: Dict[str, str] = {}  # {cp_id: city_name}
        self.alerts: Dict[str, bool] = {}  # {cp_id: is_in_alert}
        self.last_temperatures: Dict[str, Optional[float]] = {}

        
        self.running = True
        self.monitor_thread: Optional[threading.Thread] = None
        
        self.openweather_base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    def add_location(self, cp_id: str, city: str):
        """Añadir una localización para monitorizar"""
        self.locations[cp_id] = city
        self.alerts[cp_id] = False
        self.last_temperatures[cp_id] = None
        logger.info(f"📍 Añadida localización: {cp_id} → {city}")
    
    def remove_location(self, cp_id: str):
        """Eliminar una localización"""
        if cp_id in self.locations:
            del self.locations[cp_id]
            if cp_id in self.alerts:
                del self.alerts[cp_id]
            if cp_id in self.last_temperatures:
                del self.last_temperatures[cp_id]
            logger.info(f"📍 Eliminada localización: {cp_id}")
    
    def get_weather(self, city: str) -> Optional[Dict]:
        """
        Consultar clima de una ciudad en OpenWeather
        Returns: {'temp': float, 'description': str, 'city': str}
        """
        try:
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric',  # Celsius
                'lang': 'es'
            }
            
            response = requests.get(self.openweather_base_url, params=params, timeout=10)
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
    
    def notify_central(self, cp_id: str, alert_type: str, temperature: float, city: str) -> bool:
        """
        Notificar alerta a Central vía API
        alert_type: 'START' | 'END'
        """
        try:
            endpoint = f"{self.central_api_url}/api/v1/weather/alert"
            
            payload = {
                'cp_id': cp_id,
                'alert_type': alert_type,
                'temperature': temperature,
                'city': city,
                'timestamp': datetime.now().isoformat()
            }
            
            response = requests.post(endpoint, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"✅ Notificación enviada: {alert_type} para {cp_id} ({temperature}°C)")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error notificando a Central: {e}")
            return False
    
    def check_all_locations(self):
        """Verificar clima de todas las localizaciones"""
        for cp_id, city in self.locations.items():
            weather = self.get_weather(city)
            
            if weather is None:
                logger.warning(f"⚠️ No se pudo obtener clima de {city} ({cp_id})")
                continue
            
            temperature = weather['temp']
            self.last_temperatures[cp_id] = temperature
            
            # Verificar si está por debajo de 0°C
            is_below_zero = temperature < 0.0
            was_in_alert = self.alerts.get(cp_id, False)
            
            if is_below_zero and not was_in_alert:
                # NUEVA ALERTA
                logger.warning(f"🥶 ALERTA: {city} ({cp_id}) - {temperature}°C")
                self.notify_central(cp_id, 'START', temperature, city)
                self.alerts[cp_id] = True
            
            elif not is_below_zero and was_in_alert:
                # CANCELAR ALERTA
                logger.info(f"☀️ Alerta cancelada: {city} ({cp_id}) - {temperature}°C")
                self.notify_central(cp_id, 'END', temperature, city)
                self.alerts[cp_id] = False
            
            else:
                # Sin cambios
                status = "❄️ ALERTA ACTIVA" if is_below_zero else "✅ OK"
                logger.info(f"{status}: {city} ({cp_id}) - {temperature}°C - {weather['description']}")
    
    def monitor_loop(self):
        """Loop principal de monitorización"""
        logger.info(f"🔄 Iniciando monitorización cada {self.check_interval}s")
        
        while self.running:
            try:
                if self.locations:
                    self.check_all_locations()
                else:
                    logger.info("⏳ No hay localizaciones para monitorizar")
                
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"❌ Error en monitor loop: {e}")
                time.sleep(self.check_interval)
    
    def start(self):
        """Iniciar monitorización en segundo plano"""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.running = True
            self.monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info("🚀 Weather Control Office iniciado")
    
    def stop(self):
        """Detener monitorización"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=10)
        logger.info("🛑 Weather Control Office detenido")
    
    def get_status(self) -> Dict:
        """Obtener estado actual del sistema"""
        return {
            'locations': dict(self.locations),
            'alerts': dict(self.alerts),
            'temperatures': dict(self.last_temperatures),
            'monitoring': self.running
        }
    
    def interactive_menu(self):
        """Menú interactivo para gestionar localizaciones"""
        self._print_help()
        
        while True:
            try:
                cmd = input(f"\n[Weather]> ").strip().lower()
                
                if cmd == 'add':
                    cp_id = input("CP_ID: ").strip().upper()
                    city = input("Ciudad: ").strip()
                    if cp_id and city:
                        self.add_location(cp_id, city)
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
        print("\n" + "="*60)
        print("EV_W - WEATHER CONTROL OFFICE")
        print("="*60)
        print("Comandos disponibles:")
        print("  add      - Añadir localización")
        print("  remove   - Eliminar localización")
        print("  list     - Listar localizaciones")
        print("  status   - Ver estado actual")
        print("  check    - Forzar verificación de clima")
        print("  help     - Mostrar ayuda")
        print("  quit     - Salir")
        print("="*60)
    
    def _print_locations(self):
        if not self.locations:
            print("\n📭 No hay localizaciones configuradas")
            return
        
        print("\n" + "="*60)
        print("📍 LOCALIZACIONES MONITORIZADAS")
        print("="*60)
        for cp_id, city in self.locations.items():
            temp = self.last_temperatures.get(cp_id)
            alert = self.alerts.get(cp_id, False)
            
            status = "❄️ ALERTA" if alert else "✅ OK"
            temp_str = f"{temp:.1f}°C" if temp is not None else "N/A"
            
            print(f"{cp_id:10} → {city:20} | {temp_str:10} | {status}")
        print("="*60)
    
    def _print_status(self):
        print("\n" + "="*60)
        print("📊 ESTADO DEL SISTEMA")
        print("="*60)
        print(f"Localizaciones:    {len(self.locations)}")
        print(f"Alertas activas:   {sum(self.alerts.values())}")
        print(f"Monitorización:    {'🟢 ACTIVA' if self.running else '🔴 INACTIVA'}")
        print(f"Intervalo:         {self.check_interval}s")
        print("="*60)
        
        if self.locations:
            self._print_locations()

# MAIN

if __name__ == '__main__':
    logger.info("="*60)
    logger.info("EV_W - Weather Control Office")
    logger.info("Release 2 - Práctica SD 25/26")
    logger.info("="*60)
    
    # Configuración
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        logger.error("❌ Error: OPENWEATHER_API_KEY no configurada")
        logger.info("📋 Obtén tu API key en: https://openweathermap.org/api")
        logger.info("   Luego configura: export OPENWEATHER_API_KEY='tu_clave'")
        exit(1)
    
    central_api_url = os.getenv('CENTRAL_API_URL', 'http://localhost:8080')
    check_interval = int(os.getenv('CHECK_INTERVAL', '4'))
    
    logger.info(f"🔑 API Key configurada: {api_key[:8]}...")
    logger.info(f"🌐 Central API: {central_api_url}")
    logger.info(f"⏱️  Intervalo: {check_interval}s")
    
    # Crear instancia
    weather_office = WeatherControlOffice(api_key, central_api_url, check_interval)
    
    # Iniciar monitorización
    weather_office.start()
    
    # Menú interactivo
    weather_office.interactive_menu()