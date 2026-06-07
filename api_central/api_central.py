"""
API_CENTRAL - API REST para consultar estado del sistema EVCharging.
Expone información de CPs, Drivers, Transacciones y recibe alertas climáticas.
Release 2 - Práctica SD 25/26

CORRECCIONES APLICADAS:
  [1.5] Endpoint POST /api/v1/weather/alert protegido con X-API-Key
  [2.2] api_central ya NO publica en central_commands; solo notifica en weather_sync
  [2.3] Puerto por defecto corregido a 8082; CENTRAL_API_URL apunta a 8082
  [5.4] Manejo de SIGTERM para cierre limpio
  [5.6] Logs de alta frecuencia a nivel DEBUG
  [5.7] Verificación Kafka sin publicar en test_topic
"""
import os
import sqlite3
import logging
import json
import sys
import signal
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
from functools import wraps

from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger('API-Central')

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# FIX [1.5]: Autenticación Weather API Key
# ---------------------------------------------------------------------------
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')
if not WEATHER_API_KEY:
    logger.warning(
        "⚠️  WEATHER_API_KEY no configurada. "
        "El endpoint POST /api/v1/weather/alert estará deshabilitado."
    )


def require_weather_api_key(f):
    """Decorador: exige X-API-Key correcta para endpoints de escritura weather."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not WEATHER_API_KEY:
            return jsonify({'error': 'WEATHER_API_KEY no configurada en servidor'}), 500
        provided = request.headers.get('X-API-Key', '')
        if provided != WEATHER_API_KEY:
            logger.warning(
                f"Acceso no autorizado a weather alert desde {request.remote_addr}"
            )
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# CentralAPI
# ---------------------------------------------------------------------------

class CentralAPI:
    """API para acceso al estado del sistema."""

    def __init__(self, db_path: str = 'evcharging.db',
                 kafka_servers: str = 'localhost:9092'):
        self.db_path = db_path
        self.kafka_servers = (
            kafka_servers if isinstance(kafka_servers, list) else [kafka_servers]
        )

        # Alertas y localizaciones en memoria
        self.weather_alerts: Dict[str, Dict] = {}
        self.weather_locations: Dict[str, str] = {}

        self.producer: Optional[KafkaProducer] = None
        self._init_kafka()
        self._start_db_sync()

    # ------------------------------------------------------------------
    # FIX [5.7]: Kafka — verificar sin publicar en test_topic
    # ------------------------------------------------------------------

    def _init_kafka(self):
        """Inicializar Kafka Producer verificando conexión sin crear topics basura."""
        for attempt in range(1, 11):
            try:
                # FIX [5.7]: Verificar disponibilidad con admin client, sin publicar
                admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_servers,
                    request_timeout_ms=5000
                )
                admin.list_topics()
                admin.close()

                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5
                )
                logger.info("✅ Kafka Producer conectado")
                return
            except Exception as e:
                logger.warning(f"⚠️ Kafka intento {attempt}/10: {e}")
                if attempt < 10:
                    time.sleep(5)
        logger.error("❌ No se pudo conectar a Kafka tras 10 intentos")

    def _send_kafka(self, topic: str, payload: dict):
        """
        Enviar mensaje a Kafka.
        FIX [4.5]: sin flush síncrono salvo mensajes críticos.
        """
        try:
            if self.producer:
                self.producer.send(topic, payload)
        except Exception as e:
            logger.error(f"❌ Error enviando a Kafka [{topic}]: {e}")

    # ------------------------------------------------------------------
    # BD sync — keep-alive WAL
    # ------------------------------------------------------------------

    def _start_db_sync(self):
        """Thread que mantiene la conexión SQLite activa (WAL keep-alive)."""
        def sync_loop():
            while True:
                try:
                    time.sleep(2)
                    conn = self._get_db()
                    conn.commit()
                    conn.close()
                except Exception as e:
                    logger.debug(f"DB sync: {e}")
                    time.sleep(1)

        t = threading.Thread(target=sync_loop, daemon=True)
        t.start()
        logger.debug("DB sync thread iniciado")

    def _get_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Consultas (solo lectura de BD compartida)
    # ------------------------------------------------------------------

    def get_all_cps(self) -> List[Dict]:
        try:
            conn = self._get_db()
            cursor = conn.cursor()
            cursor.execute('''SELECT cp_id, location, price, status, last_seen,
                              registered, authenticated
                              FROM charging_points ORDER BY cp_id''')
            cps = [dict(row) for row in cursor.fetchall()]
            conn.close()
            for cp in cps:
                cp_id = cp['cp_id']
                cp['weather_alert'] = self.weather_alerts.get(cp_id)
                cp['weather_location'] = self.weather_locations.get(cp_id)
            return cps
        except Exception as e:
            logger.exception(f"❌ Error obteniendo CPs: {e}")
            return []

    def get_cp_by_id(self, cp_id: str) -> Optional[Dict]:
        try:
            conn = self._get_db()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT cp_id, location, price, status, last_seen '
                'FROM charging_points WHERE cp_id = ?', (cp_id,)
            )
            row = cursor.fetchone()
            conn.close()
            if row:
                cp = dict(row)
                cp['weather_alert'] = self.weather_alerts.get(cp_id)
                return cp
            return None
        except Exception as e:
            logger.exception(f"❌ Error obteniendo CP {cp_id}: {e}")
            return None

    def get_active_sessions(self) -> List[Dict]:
        try:
            conn = self._get_db()
            cursor = conn.cursor()
            cursor.execute('''SELECT session_id, cp_id, driver_id, start_time,
                              kw_consumed, total_cost
                              FROM sessions
                              WHERE end_time IS NULL
                              ORDER BY start_time DESC''')
            sessions = []
            for row in cursor.fetchall():
                s = dict(row)
                if s.get('start_time'):
                    s['start_time_formatted'] = datetime.fromtimestamp(
                        s['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                s['kw_consumed'] = float(s.get('kw_consumed', 0))
                s['total_cost'] = float(s.get('total_cost', 0))
                sessions.append(s)
            conn.close()
            return sessions
        except Exception as e:
            logger.exception(f"❌ Error obteniendo sesiones activas: {e}")
            return []

    def get_session_history(self, limit: int = 50) -> List[Dict]:
        try:
            conn = self._get_db()
            cursor = conn.cursor()
            cursor.execute('''SELECT session_id, cp_id, driver_id,
                              start_time, end_time, kw_consumed, total_cost,
                              exitosa, razon
                              FROM sessions
                              WHERE end_time IS NOT NULL
                              ORDER BY end_time DESC
                              LIMIT ?''', (limit,))
            sessions = []
            for row in cursor.fetchall():
                s = dict(row)
                if s.get('start_time'):
                    s['start_time_formatted'] = datetime.fromtimestamp(
                        s['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                if s.get('end_time'):
                    s['end_time_formatted'] = datetime.fromtimestamp(
                        s['end_time']).strftime('%Y-%m-%d %H:%M:%S')
                sessions.append(s)
            conn.close()
            return sessions
        except Exception as e:
            logger.exception(f"❌ Error obteniendo historial: {e}")
            return []

    def get_stats(self) -> Dict:
        try:
            conn = self._get_db()
            cursor = conn.cursor()

            cursor.execute('SELECT COUNT(*) as total FROM charging_points')
            total_cps = cursor.fetchone()['total']

            cursor.execute('SELECT status, COUNT(*) as count '
                           'FROM charging_points GROUP BY status')
            cps_by_status = {row['status']: row['count']
                             for row in cursor.fetchall()}

            cursor.execute('SELECT COUNT(*) as total FROM sessions WHERE end_time IS NULL')
            active_sessions = cursor.fetchone()['total']

            cursor.execute('SELECT COUNT(*) as total FROM sessions WHERE end_time IS NOT NULL')
            total_sessions = cursor.fetchone()['total']

            cursor.execute('SELECT SUM(kw_consumed) as total FROM sessions WHERE exitosa=1')
            row = cursor.fetchone()
            total_energy = row['total'] if row['total'] else 0.0

            cursor.execute('SELECT SUM(total_cost) as total FROM sessions WHERE exitosa=1')
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
            logger.exception(f"❌ Error obteniendo stats: {e}")
            return {}

    def get_weather_alerts(self) -> List[Dict]:
        return [{'cp_id': k, **v} for k, v in self.weather_alerts.items()]

    # ------------------------------------------------------------------
    # FIX [2.2]: Procesamiento de alertas weather — SOLO notifica a Central
    #            vía weather_sync. NO publica en central_commands.
    # ------------------------------------------------------------------

    def process_weather_alert(self, cp_id: str, alert_type: str,
                               temperature: float, city: str) -> bool:
        """
        Procesar alerta climática.

        IMPORTANTE: api_central SOLO actualiza su estado interno y
        publica en weather_sync para que ev_central tome las acciones.
        NO publica en central_commands para evitar duplicación (fix 2.2).
        """
        try:
            # Registrar localización siempre
            if city and cp_id:
                self.weather_locations[cp_id] = city

            if alert_type == 'REGISTER':
                # Solo registro de localización — notificar a ev_central
                if self.producer:
                    self._send_kafka('weather_sync', {
                        'cp_id': cp_id,
                        'alert_type': 'REGISTER',
                        'temperature': temperature,
                        'city': city
                    })
                logger.info(f"📍 Localización registrada: {cp_id} → {city}")
                return True

            if alert_type == 'START':
                self.weather_alerts[cp_id] = {
                    'temperature': temperature,
                    'city': city,
                    'started_at': datetime.now().isoformat(),
                    'active': True
                }
                logger.warning(f"❄️ ALERTA INICIADA: {cp_id} ({city}) - {temperature}°C")

                # FIX [2.2]: Solo notificar a ev_central vía weather_sync.
                # ev_central es quien publica en central_commands.
                if self.producer:
                    self._send_kafka('weather_sync', {
                        'cp_id': cp_id,
                        'alert_type': 'START',
                        'temperature': temperature,
                        'city': city
                    })

            elif alert_type == 'END':
                if cp_id in self.weather_alerts:
                    self.weather_alerts[cp_id]['active'] = False
                    self.weather_alerts[cp_id]['ended_at'] = datetime.now().isoformat()
                    del self.weather_alerts[cp_id]

                logger.info(f"☀️ ALERTA FINALIZADA: {cp_id} ({city}) - {temperature}°C")

                if self.producer:
                    self._send_kafka('weather_sync', {
                        'cp_id': cp_id,
                        'alert_type': 'END',
                        'temperature': temperature,
                        'city': city
                    })

            return True
        except Exception as e:
            logger.exception(f"❌ Error procesando alerta: {e}")
            return False

    def get_weather_info(self) -> Dict:
        return {
            'alerts': self.get_weather_alerts(),
            'monitored_locations': dict(self.weather_locations),
            'total_monitored': len(self.weather_locations),
            'active_alerts': len(self.weather_alerts)
        }

    def shutdown(self):
        if self.producer:
            try:
                self.producer.flush(timeout=5)
                self.producer.close()
            except Exception:
                pass
        logger.info("✅ API_Central cerrada limpiamente")


# ---------------------------------------------------------------------------
# Instancia global
# ---------------------------------------------------------------------------
api = CentralAPI(
    db_path=os.getenv('DB_PATH', 'evcharging.db'),
    kafka_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
)

# ---------------------------------------------------------------------------
# Endpoints REST
# ---------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'API_Central'}), 200


@app.route('/api/v1/cps', methods=['GET'])
def get_all_cps():
    return jsonify(api.get_all_cps()), 200


@app.route('/api/v1/cps/<cp_id>', methods=['GET'])
def get_cp(cp_id: str):
    cp = api.get_cp_by_id(cp_id)
    if cp:
        return jsonify(cp), 200
    return jsonify({'error': 'CP not found'}), 404


@app.route('/api/v1/sessions/active', methods=['GET'])
def get_active_sessions():
    return jsonify(api.get_active_sessions()), 200


@app.route('/api/v1/sessions/history', methods=['GET'])
def get_session_history():
    limit = request.args.get('limit', default=50, type=int)
    return jsonify(api.get_session_history(limit)), 200


@app.route('/api/v1/stats', methods=['GET'])
def get_stats():
    return jsonify(api.get_stats()), 200


@app.route('/api/v1/weather/alerts', methods=['GET'])
def get_weather_alerts():
    return jsonify(api.get_weather_alerts()), 200


# FIX [1.5]: Endpoint protegido con X-API-Key
@app.route('/api/v1/weather/alert', methods=['POST'])
@require_weather_api_key
def weather_alert():
    """
    POST /api/v1/weather/alert
    Requiere cabecera: X-API-Key: <WEATHER_API_KEY>
    Body: {cp_id, alert_type, temperature, city}
    """
    try:
        data = request.get_json()
        required = ['cp_id', 'alert_type', 'temperature', 'city']
        if not data or not all(f in data for f in required):
            return jsonify({'error': 'Missing required fields'}), 400

        success = api.process_weather_alert(
            data['cp_id'], data['alert_type'],
            data['temperature'], data['city']
        )
        if success:
            return jsonify({'message': 'Alert processed successfully'}), 200
        return jsonify({'error': 'Failed to process alert'}), 500
    except Exception as e:
        logger.exception(f"❌ Error en endpoint alert: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/weather/info', methods=['GET'])
def get_weather_info():
    try:
        return jsonify(api.get_weather_info()), 200
    except Exception as e:
        logger.exception(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/system/status', methods=['GET'])
def get_system_status():
    """GET /api/v1/system/status — estado completo (para el Front)."""
    try:
        return jsonify({
            'cps': api.get_all_cps(),
            'active_sessions': api.get_active_sessions(),
            'stats': api.get_stats(),
            'weather_alerts': api.get_weather_alerts(),
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.exception(f"❌ Error obteniendo estado: {e}")
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("API_CENTRAL - API REST para EVCharging")
    logger.info("Release 2 - Práctica SD 25/26")
    logger.info("=" * 60)

    # FIX [5.4]: Manejo de SIGTERM
    def handle_sigterm(signum, frame):
        logger.info("🛑 SIGTERM recibido, cerrando API_Central...")
        api.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    # FIX [2.3]: Puerto por defecto 8082
    port = int(os.getenv('API_PORT', 8082))
    logger.info(f"🚀 API escuchando en http://0.0.0.0:{port}")
    logger.info("=" * 60)

    app.run(host='0.0.0.0', port=port, debug=False)
