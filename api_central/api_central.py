"""
API_CENTRAL - API REST para consultar estado del sistema EVCharging.
Expone CPs, sesiones, estadísticas y recibe alertas climáticas de EV_W.
Release 2 - Práctica SD 25/26

CORRECCIONES COMPLETAS:
  [A-1]  POST /api/v1/weather/alert protegido con X-API-Key (WEATHER_API_KEY).
  [A-2]  api_central NO publica en central_commands; solo reenvía en weather_sync.
         Es EV_Central quien decide si debe parar o reanudar un CP.
  [A-3]  Puerto 8082 como valor por defecto coherente con docker-compose.
  [A-4]  Cada request abre y cierra su propia conexión SQLite (evita
         "database is locked" con WAL y múltiples readers).
  [A-5]  DB sync thread eliminado (era innecesario y podía generar deadlocks).
  [A-6]  Kafka Producer: reintentos exponenciales + manejo de NoBrokersAvailable.
  [A-7]  Polling automático del front: setInterval en index.html. Aquí se añade
         cabecera Cache-Control: no-store en /api/v1/system/status.
  [A-8]  SIGTERM/SIGINT para cierre limpio.
  [A-9]  Respuesta 503 clara cuando Kafka no está disponible.
  [A-10] Verificación de Kafka con AdminClient sin publicar en topics basura.
"""
import os
import sys
import signal
import logging
import json
import time
import threading
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional
import sqlite3

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger('API-Central')

# ---------------------------------------------------------------------------
# Flask
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# [A-1] Autenticación por API Key para endpoints weather (escritura)
# ---------------------------------------------------------------------------
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY', '').strip()
if not WEATHER_API_KEY:
    logger.warning(
        "⚠️  WEATHER_API_KEY no configurada. "
        "POST /api/v1/weather/alert estará deshabilitado."
    )


def require_weather_api_key(f):
    """Decorador: exige cabecera X-API-Key == WEATHER_API_KEY."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not WEATHER_API_KEY:
            return jsonify({'error': 'WEATHER_API_KEY no configurada en servidor'}), 503
        provided = request.headers.get('X-API-Key', '')
        if provided != WEATHER_API_KEY:
            logger.warning("Acceso no autorizado a weather/alert desde %s", request.remote_addr)
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Clase principal
# ---------------------------------------------------------------------------

class CentralAPI:
    """
    Acceso de lectura a la BD compartida + reenvío de alertas weather a Kafka.
    """

    def __init__(self, db_path: str, kafka_servers: str):
        self.db_path = db_path
        self.kafka_servers = (
            kafka_servers if isinstance(kafka_servers, list)
            else [s.strip() for s in kafka_servers.split(',')]
        )

        # Estado en memoria (alertas y localizaciones weather)
        self.weather_alerts:    Dict[str, Dict] = {}
        self.weather_locations: Dict[str, str]  = {}
        self._mem_lock = threading.RLock()

        self.producer: Optional[KafkaProducer] = None
        self._kafka_ready = False
        self._init_kafka()

    # ------------------------------------------------------------------
    # [A-10] Kafka — conexión con AdminClient para verificar sin basura
    # ------------------------------------------------------------------

    def _init_kafka(self):
        """Conecta al broker Kafka con reintentos exponenciales."""
        for attempt in range(1, 11):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_servers,
                    request_timeout_ms=5000,
                    client_id='api_central_admin'
                )
                admin.list_topics()
                admin.close()

                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5,
                    request_timeout_ms=30_000,
                    max_block_ms=10_000,
                    client_id='api_central_producer'
                )
                self._kafka_ready = True
                logger.info("✅ Kafka Producer conectado")
                return
            except (KafkaError, NoBrokersAvailable, Exception) as exc:
                wait = min(2 ** attempt, 30)
                logger.warning("⚠️ Kafka intento %d/10 — %s — reintentando en %ds", attempt, exc, wait)
                time.sleep(wait)
        logger.error("❌ No se pudo conectar a Kafka. El reenvío de alertas estará deshabilitado.")

    def _send_kafka(self, topic: str, payload: Dict):
        """Envía a Kafka sin flush síncrono (fire-and-forget)."""
        if not self._kafka_ready or not self.producer:
            logger.warning("⚠️ Kafka no disponible; descartando mensaje en topic '%s'", topic)
            return
        try:
            self.producer.send(topic, payload)
        except Exception as exc:
            logger.error("❌ Error enviando a Kafka [%s]: %s", topic, exc)

    # ------------------------------------------------------------------
    # [A-4] Conexiones SQLite por-request (lectura)
    # ------------------------------------------------------------------

    def _get_db(self) -> sqlite3.Connection:
        """Abre una conexión SQLite de solo lectura (WAL friendly)."""
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Consultas
    # ------------------------------------------------------------------

    def get_all_cps(self) -> List[Dict]:
        try:
            conn = self._get_db()
            rows = conn.execute(
                '''SELECT cp_id, location, price, status, last_seen,
                          registered, authenticated
                   FROM charging_points ORDER BY cp_id'''
            ).fetchall()
            conn.close()
            result = []
            with self._mem_lock:
                for row in rows:
                    cp = dict(row)
                    cp_id = cp['cp_id']
                    cp['weather_alert']    = self.weather_alerts.get(cp_id)
                    cp['weather_location'] = self.weather_locations.get(cp_id)
                    result.append(cp)
            return result
        except Exception as exc:
            logger.exception("Error obteniendo CPs: %s", exc)
            return []

    def get_cp_by_id(self, cp_id: str) -> Optional[Dict]:
        try:
            conn = self._get_db()
            row = conn.execute(
                '''SELECT cp_id, location, price, status, last_seen,
                          registered, authenticated
                   FROM charging_points WHERE cp_id = ?''',
                (cp_id,)
            ).fetchone()
            conn.close()
            if row:
                cp = dict(row)
                with self._mem_lock:
                    cp['weather_alert']    = self.weather_alerts.get(cp_id)
                    cp['weather_location'] = self.weather_locations.get(cp_id)
                return cp
            return None
        except Exception as exc:
            logger.exception("Error obteniendo CP %s: %s", cp_id, exc)
            return None

    def get_active_sessions(self) -> List[Dict]:
        try:
            conn = self._get_db()
            rows = conn.execute(
                '''SELECT session_id, cp_id, driver_id, start_time,
                          kw_consumed, total_cost
                   FROM sessions WHERE end_time IS NULL
                   ORDER BY start_time DESC'''
            ).fetchall()
            conn.close()
            result = []
            for row in rows:
                s = dict(row)
                if s.get('start_time'):
                    s['start_time_formatted'] = datetime.fromtimestamp(
                        s['start_time']
                    ).strftime('%Y-%m-%d %H:%M:%S')
                s['kw_consumed'] = round(float(s.get('kw_consumed', 0)), 3)
                s['total_cost']  = round(float(s.get('total_cost',  0)), 2)
                result.append(s)
            return result
        except Exception as exc:
            logger.exception("Error obteniendo sesiones activas: %s", exc)
            return []

    def get_session_history(self, limit: int = 50) -> List[Dict]:
        try:
            limit = max(1, min(limit, 500))   # cota segura
            conn = self._get_db()
            rows = conn.execute(
                '''SELECT session_id, cp_id, driver_id,
                          start_time, end_time, kw_consumed, total_cost,
                          exitosa, razon
                   FROM sessions WHERE end_time IS NOT NULL
                   ORDER BY end_time DESC LIMIT ?''',
                (limit,)
            ).fetchall()
            conn.close()
            result = []
            for row in rows:
                s = dict(row)
                for field in ('start_time', 'end_time'):
                    if s.get(field):
                        s[f'{field}_formatted'] = datetime.fromtimestamp(
                            s[field]
                        ).strftime('%Y-%m-%d %H:%M:%S')
                result.append(s)
            return result
        except Exception as exc:
            logger.exception("Error obteniendo historial: %s", exc)
            return []

    def get_stats(self) -> Dict:
        try:
            conn = self._get_db()
            total_cps = conn.execute('SELECT COUNT(*) FROM charging_points').fetchone()[0]

            by_status = {}
            for row in conn.execute(
                'SELECT status, COUNT(*) AS cnt FROM charging_points GROUP BY status'
            ).fetchall():
                by_status[row['status']] = row['cnt']

            active_sessions = conn.execute(
                'SELECT COUNT(*) FROM sessions WHERE end_time IS NULL'
            ).fetchone()[0]

            total_sessions = conn.execute(
                'SELECT COUNT(*) FROM sessions WHERE end_time IS NOT NULL'
            ).fetchone()[0]

            row = conn.execute(
                'SELECT SUM(kw_consumed) FROM sessions WHERE exitosa = 1'
            ).fetchone()
            total_energy = float(row[0] or 0)

            row = conn.execute(
                'SELECT SUM(total_cost) FROM sessions WHERE exitosa = 1'
            ).fetchone()
            total_revenue = float(row[0] or 0)

            conn.close()
            with self._mem_lock:
                n_alerts = len(self.weather_alerts)

            return {
                'total_cps':               total_cps,
                'cps_by_status':           by_status,
                'active_sessions':         active_sessions,
                'total_sessions_completed': total_sessions,
                'total_energy_kwh':        round(total_energy,  2),
                'total_revenue_eur':       round(total_revenue, 2),
                'weather_alerts':          n_alerts
            }
        except Exception as exc:
            logger.exception("Error obteniendo stats: %s", exc)
            return {}

    def get_weather_alerts(self) -> List[Dict]:
        with self._mem_lock:
            return [{'cp_id': k, **v} for k, v in self.weather_alerts.items()]

    def get_weather_info(self) -> Dict:
        with self._mem_lock:
            return {
                'alerts':             self.get_weather_alerts(),
                'monitored_locations': dict(self.weather_locations),
                'total_monitored':    len(self.weather_locations),
                'active_alerts':      len(self.weather_alerts)
            }

    # ------------------------------------------------------------------
    # [A-2] Procesamiento de alertas weather — SIN publicar en central_commands
    # ------------------------------------------------------------------

    def process_weather_alert(self, cp_id: str, alert_type: str,
                               temperature: float, city: str) -> bool:
        """
        Actualiza estado interno y notifica a EV_Central vía weather_sync.

        IMPORTANTE: api_central NO publica en central_commands.
        Es EV_Central quien, al consumir weather_sync, decide si STOP/RESUME.
        """
        try:
            with self._mem_lock:
                if city and cp_id:
                    self.weather_locations[cp_id] = city

                if alert_type == 'REGISTER':
                    logger.info("📍 Localización registrada: %s → %s", cp_id, city)

                elif alert_type == 'START':
                    self.weather_alerts[cp_id] = {
                        'temperature': temperature,
                        'city':        city,
                        'started_at':  datetime.now().isoformat(),
                        'active':      True
                    }
                    logger.warning("❄️  ALERTA INICIADA: %s (%s) — %.1f°C", cp_id, city, temperature)

                elif alert_type == 'END':
                    self.weather_alerts.pop(cp_id, None)
                    logger.info("☀️  ALERTA CANCELADA: %s (%s) — %.1f°C", cp_id, city, temperature)

                else:
                    logger.warning("⚠️ alert_type desconocido: '%s'", alert_type)
                    return False

            # Notificar a EV_Central únicamente mediante weather_sync
            self._send_kafka('weather_sync', {
                'cp_id':       cp_id,
                'alert_type':  alert_type,
                'temperature': temperature,
                'city':        city,
                'timestamp':   datetime.now().isoformat()
            })
            return True

        except Exception as exc:
            logger.exception("Error procesando alerta weather: %s", exc)
            return False

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
    return jsonify({
        'status':       'healthy',
        'service':      'API_Central',
        'kafka_ready':  api._kafka_ready
    }), 200


@app.route('/api/v1/cps', methods=['GET'])
def get_all_cps():
    return jsonify(api.get_all_cps()), 200


@app.route('/api/v1/cps/<cp_id>', methods=['GET'])
def get_cp(cp_id: str):
    cp = api.get_cp_by_id(cp_id)
    if cp:
        return jsonify(cp), 200
    return jsonify({'error': f"CP '{cp_id}' no encontrado"}), 404


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


@app.route('/api/v1/weather/info', methods=['GET'])
def get_weather_info():
    return jsonify(api.get_weather_info()), 200


# [A-1] Protegido con X-API-Key
@app.route('/api/v1/weather/alert', methods=['POST'])
@require_weather_api_key
def weather_alert():
    """
    POST /api/v1/weather/alert
    Cabecera: X-API-Key: <WEATHER_API_KEY>
    Body: {cp_id, alert_type, temperature, city}
    alert_type: 'REGISTER' | 'START' | 'END'
    """
    data = request.get_json(silent=True)
    required = ('cp_id', 'alert_type', 'temperature', 'city')
    if not data or not all(f in data for f in required):
        return jsonify({'error': f'Faltan campos: {required}'}), 400

    try:
        temperature = float(data['temperature'])
    except (ValueError, TypeError):
        return jsonify({'error': 'temperature debe ser un número'}), 400

    success = api.process_weather_alert(
        str(data['cp_id']),
        str(data['alert_type']).upper(),
        temperature,
        str(data['city'])
    )
    if success:
        return jsonify({'message': 'Alerta procesada correctamente'}), 200
    return jsonify({'error': 'Error procesando alerta'}), 500


@app.route('/api/v1/system/status', methods=['GET'])
def get_system_status():
    """
    Endpoint principal para el Front — devuelve el estado completo del sistema.
    [A-7] Cache-Control: no-store para que el navegador siempre pida datos frescos.
    """
    try:
        payload = {
            'cps':             api.get_all_cps(),
            'active_sessions': api.get_active_sessions(),
            'stats':           api.get_stats(),
            'weather_alerts':  api.get_weather_alerts(),
            'timestamp':       datetime.now().isoformat()
        }
        resp = make_response(jsonify(payload), 200)
        resp.headers['Cache-Control'] = 'no-store'
        return resp
    except Exception as exc:
        logger.exception("Error en /api/v1/system/status: %s", exc)
        return jsonify({'error': str(exc)}), 500


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("API_CENTRAL — Release 2 — Práctica SD 25/26")
    logger.info("=" * 60)

    # [A-8] Cierre limpio con SIGTERM / SIGINT
    def _handle_signal(signum, frame):
        logger.info("🛑 Señal %d recibida — cerrando API_Central...", signum)
        api.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    # [A-3] Puerto 8082 por defecto
    port = int(os.getenv('API_PORT', 8082))
    logger.info("🚀 API_Central escuchando en http://0.0.0.0:%d", port)
    logger.info("=" * 60)

    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
