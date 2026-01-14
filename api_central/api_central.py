"""
API_CENTRAL - API REST para consultar estado del sistema EVCharging
Expone información de CPs, Drivers, Transacciones y recibe alertas climáticas
Release 2 - Práctica SD 25/26
"""
import os
import sqlite3
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional
from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.errors import KafkaError
import threading
import time

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger('API-Central')

app = Flask(__name__)
CORS(app)  # Permitir CORS para el Front

class CentralAPI:
    """API para acceso al estado del sistema"""
    
    def __init__(self, db_path='evcharging.db', kafka_servers='localhost:9092'):
        self.db_path = db_path
        self.kafka_servers = kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        
        # Alertas climáticas en memoria
        self.weather_alerts: Dict[str, Dict] = {}
        
        # Inicializar Kafka Producer
        self.producer = None
        self._init_kafka()
        
        self._start_db_sync()
        self.weather_locations: Dict[str, str] = {}  # {cp_id: city_name}

    
    def _start_db_sync(self):
        """Thread que sincroniza la BD periódicamente"""
        def sync_loop():
            while True:
                try:
                    time.sleep(1)  # Sincronizar cada segundo
                    # Forzar commit de cualquier transacción pendiente
                    conn = self.get_db_connection()
                    conn.commit()
                    conn.close()
                except Exception as e:
                    logger.error(f"Error en sync loop: {e}")
                    time.sleep(1)
        
        sync_thread = threading.Thread(target=sync_loop, daemon=True)
        sync_thread.start()
        logger.info("✅ DB sync thread iniciado")


    def _init_kafka(self):
        """Inicializar conexión con Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            logger.info("✅ Kafka conectado")
        except KafkaError as e:
            logger.error(f"❌ Error conectando a Kafka: {e}")
    
    def get_db_connection(self):
        """Obtener conexión a la base de datos"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    # MÉTODOS DE CONSULTA
    
    def get_all_cps(self) -> List[Dict]:
        """Obtener todos los CPs - MEJORADO con info weather"""
        try:
            conn = self.get_db_connection()
            conn.execute('PRAGMA journal_mode=WAL')
            cursor = conn.cursor()
            cursor.execute('''SELECT cp_id, location, price, status, last_seen,
                            registered, authenticated 
                            FROM charging_points ORDER BY cp_id''')
            cps = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            # Agregar alertas climáticas Y localizaciones
            for cp in cps:
                cp_id = cp['cp_id']
                
                # Alerta activa
                if cp_id in self.weather_alerts:
                    cp['weather_alert'] = self.weather_alerts[cp_id]
                else:
                    cp['weather_alert'] = None
                
                # NUEVO: Localización monitoreada
                if cp_id in self.weather_locations:
                    cp['weather_location'] = self.weather_locations[cp_id]
                else:
                    cp['weather_location'] = None
            
            return cps
        except Exception as e:
            logger.error(f"❌ Error obteniendo CPs: {e}")
            return []


    
    def get_cp_by_id(self, cp_id: str) -> Optional[Dict]:
        """Obtener un CP específico"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''SELECT cp_id, location, price, status, last_seen 
                             FROM charging_points WHERE cp_id = ?''', (cp_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                cp = dict(row)
                if cp_id in self.weather_alerts:
                    cp['weather_alert'] = self.weather_alerts[cp_id]
                return cp
            return None
        except Exception as e:
            logger.error(f"❌ Error obteniendo CP {cp_id}: {e}")
            return None
    
    def get_active_sessions(self) -> List[Dict]:
        """Obtener sesiones de carga activas - CORREGIDO con datos en tiempo real"""
        try:
            conn = self.get_db_connection()
            conn.execute('PRAGMA journal_mode=WAL')
            cursor = conn.cursor()
            
            cursor.execute('''SELECT session_id, cp_id, driver_id, start_time, 
                            kw_consumed, total_cost 
                            FROM sessions 
                            WHERE end_time IS NULL 
                            ORDER BY start_time DESC''')
            
            sessions = []
            for row in cursor.fetchall():
                session = dict(row)
                # Formatear timestamp
                if session.get('start_time'):
                    session['start_time_formatted'] = datetime.fromtimestamp(
                        session['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                # Asegurar valores numéricos
                session['kw_consumed'] = float(session.get('kw_consumed', 0))
                session['total_cost'] = float(session.get('total_cost', 0))
                sessions.append(session)
            
            conn.close()
            return sessions
        except Exception as e:
            logger.error(f"❌ Error obteniendo sesiones activas: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_session_history(self, limit: int = 50) -> List[Dict]:
        """Obtener historial de sesiones - CORREGIDO"""
        try:
            conn = self.get_db_connection()
            conn.execute('PRAGMA journal_mode=WAL')
            cursor = conn.cursor()
            
            # CRÍTICO: Ordenar por start_time si end_time es NULL
            cursor.execute('''SELECT session_id, cp_id, driver_id, 
                            start_time, end_time, kw_consumed, total_cost, 
                            exitosa, razon
                            FROM sessions 
                            WHERE end_time IS NOT NULL 
                            ORDER BY COALESCE(end_time, start_time) DESC 
                            LIMIT ?''', (limit,))
            
            sessions = []
            for row in cursor.fetchall():
                session = dict(row)
                # Convertir timestamps a formato legible
                if session.get('start_time'):
                    session['start_time_formatted'] = datetime.fromtimestamp(
                        session['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                if session.get('end_time'):
                    session['end_time_formatted'] = datetime.fromtimestamp(
                        session['end_time']).strftime('%Y-%m-%d %H:%M:%S')
                sessions.append(session)
            
            conn.close()
            return sessions
        except Exception as e:
            logger.error(f"❌ Error obteniendo historial: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_stats(self) -> Dict:
        """Obtener estadísticas generales"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Total CPs
            cursor.execute('SELECT COUNT(*) as total FROM charging_points')
            total_cps = cursor.fetchone()['total']
            
            # CPs por estado
            cursor.execute('''SELECT status, COUNT(*) as count 
                             FROM charging_points 
                             GROUP BY status''')
            cps_by_status = {row['status']: row['count'] for row in cursor.fetchall()}
            
            # Sesiones activas
            cursor.execute('SELECT COUNT(*) as total FROM sessions WHERE end_time IS NULL')
            active_sessions = cursor.fetchone()['total']
            
            # Total sesiones completadas
            cursor.execute('SELECT COUNT(*) as total FROM sessions WHERE end_time IS NOT NULL')
            total_sessions = cursor.fetchone()['total']
            
            # Energía total suministrada
            cursor.execute('SELECT SUM(kw_consumed) as total FROM sessions WHERE exitosa = 1')
            row = cursor.fetchone()
            total_energy = row['total'] if row['total'] else 0.0
            
            # Ingresos totales
            cursor.execute('SELECT SUM(total_cost) as total FROM sessions WHERE exitosa = 1')
            row = cursor.fetchone()
            total_revenue = row['total'] if row['total'] else 0.0
            
            conn.close()
            
            return {
                'total_cps': total_cps,
                'cps_by_status': cps_by_status,
                'active_sessions': active_sessions,
                'total_sessions_completed': total_sessions,
                'total_energy_kwh': round(total_energy, 2),
                'total_revenue_eur': round(total_revenue, 2),
                'weather_alerts': len(self.weather_alerts)
            }
        except Exception as e:
            logger.error(f"❌ Error obteniendo stats: {e}")
            return {}
    
    def get_weather_alerts(self) -> List[Dict]:
        """Obtener todas las alertas climáticas activas"""
        return [
            {
                'cp_id': cp_id,
                **alert_data
            }
            for cp_id, alert_data in self.weather_alerts.items()
        ]
    
    # MÉTODOS PARA ALERTAS CLIMÁTICAS
    
    def process_weather_alert(self, cp_id: str, alert_type: str, 
                             temperature: float, city: str) -> bool:
        """
        Procesar alerta climática - MEJORADO con registro de localizaciones
        """
        try:
            # NUEVO: Registrar localización
            if city and cp_id:
                self.weather_locations[cp_id] = city
                logger.info(f"📍 Localización weather registrada: {cp_id} → {city}")
            
            if alert_type == 'REGISTER':
                    return True
                
            if alert_type == 'START':
                # Iniciar alerta
                self.weather_alerts[cp_id] = {
                    'temperature': temperature,
                    'city': city,
                    'started_at': datetime.now().isoformat(),
                    'active': True
                }
                
                logger.warning(f"❄️ ALERTA INICIADA: {cp_id} ({city}) - {temperature}°C")
                
                # Enviar comando STOP al CP via Kafka
                if self.producer:
                    self.producer.send('central_commands', {
                        'cp_id': cp_id,
                        'command': 'STOP',
                        'reason': f'Temperatura baja: {temperature}°C en {city}',
                        'timestamp': datetime.now().timestamp()
                    })
                    self.producer.flush()
                    logger.info(f"📤 Comando STOP enviado a {cp_id}")
            
            elif alert_type == 'END':
                # Finalizar alerta
                if cp_id in self.weather_alerts:
                    self.weather_alerts[cp_id]['active'] = False
                    self.weather_alerts[cp_id]['ended_at'] = datetime.now().isoformat()
                    
                    logger.info(f"☀️ ALERTA FINALIZADA: {cp_id} ({city}) - {temperature}°C")
                    
                    # Enviar comando RESUME al CP via Kafka
                    if self.producer:
                        self.producer.send('central_commands', {
                            'cp_id': cp_id,
                            'command': 'RESUME',
                            'reason': f'Temperatura normalizada: {temperature}°C en {city}',
                            'timestamp': datetime.now().timestamp()
                        })
                        self.producer.flush()
                        logger.info(f"📤 Comando RESUME enviado a {cp_id}")
                    
                    # Eliminar de alertas activas después
                    del self.weather_alerts[cp_id]
            
            return True
        except Exception as e:
            logger.error(f"❌ Error procesando alerta: {e}")
            return False

    def get_weather_info(self) -> Dict:
        """
        NUEVO: Obtener información completa de weather
        """
        return {
            'alerts': self.get_weather_alerts(),
            'monitored_locations': dict(self.weather_locations),
            'total_monitored': len(self.weather_locations),
            'active_alerts': len(self.weather_alerts)
        }


# Instancia global de la API
api = CentralAPI(
    db_path=os.getenv('DB_PATH', 'evcharging.db'),
    kafka_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
)

# ENDPOINTS REST

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de salud"""
    return jsonify({'status': 'healthy', 'service': 'API_Central'}), 200

@app.route('/api/v1/cps', methods=['GET'])
def get_all_cps():
    """GET /api/v1/cps - Obtener todos los CPs"""
    cps = api.get_all_cps()
    return jsonify(cps), 200

@app.route('/api/v1/cps/<cp_id>', methods=['GET'])
def get_cp(cp_id: str):
    """GET /api/v1/cps/:id - Obtener un CP específico"""
    cp = api.get_cp_by_id(cp_id)
    if cp:
        return jsonify(cp), 200
    return jsonify({'error': 'CP not found'}), 404

@app.route('/api/v1/sessions/active', methods=['GET'])
def get_active_sessions():
    """GET /api/v1/sessions/active - Obtener sesiones activas"""
    sessions = api.get_active_sessions()
    return jsonify(sessions), 200

@app.route('/api/v1/sessions/history', methods=['GET'])
def get_session_history():
    """GET /api/v1/sessions/history?limit=50 - Obtener historial"""
    limit = request.args.get('limit', default=50, type=int)
    sessions = api.get_session_history(limit)
    return jsonify(sessions), 200

@app.route('/api/v1/stats', methods=['GET'])
def get_stats():
    """GET /api/v1/stats - Obtener estadísticas generales"""
    stats = api.get_stats()
    return jsonify(stats), 200

@app.route('/api/v1/weather/alerts', methods=['GET'])
def get_weather_alerts():
    """GET /api/v1/weather/alerts - Obtener alertas climáticas activas"""
    alerts = api.get_weather_alerts()
    return jsonify(alerts), 200

@app.route('/api/v1/weather/alert', methods=['POST'])
def weather_alert():
    """
    POST /api/v1/weather/alert
    Body: {
        "cp_id": "CP001",
        "alert_type": "START" | "END",
        "temperature": -5.2,
        "city": "Madrid"
    }
    
    Endpoint para que EV_W envíe alertas climáticas
    """
    try:
        data = request.get_json()
        
        required_fields = ['cp_id', 'alert_type', 'temperature', 'city']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        success = api.process_weather_alert(
            data['cp_id'],
            data['alert_type'],
            data['temperature'],
            data['city']
        )
        
        if success:
            return jsonify({'message': 'Alert processed successfully'}), 200
        else:
            return jsonify({'error': 'Failed to process alert'}), 500
    
    except Exception as e:
        logger.error(f"❌ Error en endpoint alert: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/weather/info', methods=['GET'])
def get_weather_info():
    """
    GET /api/v1/weather/info
    Obtener información completa del sistema weather
    """
    try:
        info = api.get_weather_info()
        return jsonify(info), 200
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/system/status', methods=['GET'])
def get_system_status():
    """
    GET /api/v1/system/status
    Obtener estado completo del sistema (para el Front)
    """
    try:
        return jsonify({
            'cps': api.get_all_cps(),
            'active_sessions': api.get_active_sessions(),
            'stats': api.get_stats(),
            'weather_alerts': api.get_weather_alerts(),
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"❌ Error obteniendo estado: {e}")
        return jsonify({'error': str(e)}), 500

# MAIN

if __name__ == '__main__':
    logger.info("="*60)
    logger.info("API_CENTRAL - API REST para EVCharging")
    logger.info("Release 2 - Práctica SD 25/26")
    logger.info("="*60)
    
    port = int(os.getenv('API_PORT', 8080))
    
    logger.info(f"🚀 API escuchando en http://0.0.0.0:{port}")
    logger.info("📋 Endpoints disponibles:")
    logger.info("   GET    /api/v1/cps               - Listar CPs")
    logger.info("   GET    /api/v1/cps/:id           - Obtener CP")
    logger.info("   GET    /api/v1/sessions/active   - Sesiones activas")
    logger.info("   GET    /api/v1/sessions/history  - Historial")
    logger.info("   GET    /api/v1/stats             - Estadísticas")
    logger.info("   GET    /api/v1/weather/alerts    - Alertas climáticas")
    logger.info("   POST   /api/v1/weather/alert     - Recibir alerta")
    logger.info("   GET    /api/v1/system/status     - Estado completo")
    logger.info("="*60)
    
    app.run(host='0.0.0.0', port=port, debug=False)