"""API_CENTRAL — API REST para consultar el estado del sistema EVCharging.
Release 2 - Práctica SD 25/26
"""
import os, sys, signal, logging, json, time, threading
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional
import sqlite3

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('API-Central')

app = Flask(__name__)
CORS(app)
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY', '').strip()
if not WEATHER_API_KEY:
    logger.warning("⚠️ WEATHER_API_KEY no configurada. POST /api/v1/weather/alert deshabilitado.")

def require_weather_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not WEATHER_API_KEY:
            return jsonify({'error': 'WEATHER_API_KEY no configurada'}), 503
        provided = request.headers.get('X-API-Key', '')
        if provided != WEATHER_API_KEY:
            logger.warning("Acceso no autorizado a weather/alert desde %s", request.remote_addr)
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

class CentralAPI:
    def __init__(self, db_path: str, kafka_servers: str):
        self.db_path = db_path
        self.kafka_servers = (kafka_servers if isinstance(kafka_servers, list)
                              else [s.strip() for s in kafka_servers.split(',')])
        self.weather_alerts:    Dict[str, Dict] = {}
        self.weather_locations: Dict[str, str]  = {}
        self._mem_lock = threading.RLock()
        self.producer = None
        self._kafka_ready = False

    def _get_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.row_factory = sqlite3.Row
        return conn

    def get_all_cps(self) -> List[Dict]:
        try:
            conn = self._get_db()
            rows = conn.execute(
                'SELECT cp_id, location, price, status, last_seen, registered, authenticated '
                'FROM charging_points ORDER BY cp_id'
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

    def get_active_sessions(self) -> List[Dict]:
        try:
            conn = self._get_db()
            rows = conn.execute(
                'SELECT session_id, cp_id, driver_id, start_time, kw_consumed, total_cost '
                'FROM sessions WHERE end_time IS NULL ORDER BY start_time DESC'
            ).fetchall()
            conn.close()
            result = []
            for row in rows:
                s = dict(row)
                if s.get('start_time'):
                    s['start_time_formatted'] = datetime.fromtimestamp(
                        s['start_time']).strftime('%Y-%m-%d %H:%M:%S')
                result.append(s)
            return result
        except Exception:
            return []

    def get_session_history(self, limit: int = 50) -> List[Dict]:
        try:
            limit = max(1, min(limit, 500))
            conn = self._get_db()
            rows = conn.execute(
                'SELECT session_id, cp_id, driver_id, start_time, end_time, '
                'kw_consumed, total_cost, exitosa, razon '
                'FROM sessions WHERE end_time IS NOT NULL '
                'ORDER BY end_time DESC LIMIT ?', (limit,)
            ).fetchall()
            conn.close()
            result = []
            for row in rows:
                s = dict(row)
                for field in ('start_time', 'end_time'):
                    if s.get(field):
                        s[f'{field}_formatted'] = datetime.fromtimestamp(
                            s[field]).strftime('%Y-%m-%d %H:%M:%S')
                result.append(s)
            return result
        except Exception:
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
            active = conn.execute(
                'SELECT COUNT(*) FROM sessions WHERE end_time IS NULL'
            ).fetchone()[0]
            total = conn.execute(
                'SELECT COUNT(*) FROM sessions WHERE end_time IS NOT NULL'
            ).fetchone()[0]
            energy = float(conn.execute(
                'SELECT SUM(kw_consumed) FROM sessions WHERE exitosa = 1'
            ).fetchone()[0] or 0)
            revenue = float(conn.execute(
                'SELECT SUM(total_cost) FROM sessions WHERE exitosa = 1'
            ).fetchone()[0] or 0)
            conn.close()
            with self._mem_lock:
                n_alerts = len(self.weather_alerts)
            return {
                'total_cps': total_cps, 'cps_by_status': by_status,
                'active_sessions': active, 'total_sessions_completed': total,
                'total_energy_kwh': round(energy, 2),
                'total_revenue_eur': round(revenue, 2),
                'weather_alerts': n_alerts
            }
        except Exception:
            return {}

    def get_weather_alerts(self) -> List[Dict]:
        with self._mem_lock:
            return [{'cp_id': k, **v} for k, v in self.weather_alerts.items()]

    def _send_kafka(self, topic: str, payload: Dict):
        if not self._kafka_ready or not self.producer:
            return
        try:
            self.producer.send(topic, payload)
        except Exception as exc:
            logger.error("❌ Error Kafka [%s]: %s", topic, exc)
    def process_weather_alert(self, cp_id: str, alert_type: str,
                               temperature: float, city: str) -> bool:
        try:
            with self._mem_lock:
                if city and cp_id:
                    self.weather_locations[cp_id] = city
                if alert_type == 'REGISTER':
                    logger.info("📍 Localización: %s → %s", cp_id, city)
                elif alert_type == 'START':
                    self.weather_alerts[cp_id] = {
                        'temperature': temperature, 'city': city,
                        'started_at': datetime.now().isoformat(), 'active': True
                    }
                    logger.warning("❄️ ALERTA: %s (%s) %.1f°C", cp_id, city, temperature)
                elif alert_type == 'END':
                    self.weather_alerts.pop(cp_id, None)
                    logger.info("☀️ Alerta cancelada: %s", cp_id)
                else:
                    return False
            self._send_kafka('weather_sync', {
                'cp_id': cp_id, 'alert_type': alert_type,
                'temperature': temperature, 'city': city,
                'timestamp': datetime.now().isoformat()
            })
            return True
        except Exception as exc:
            logger.exception("Error procesando alerta: %s", exc)
            return False

    def shutdown(self):
        if self.producer:
            try:
                self.producer.flush(timeout=5)
                self.producer.close()
            except Exception:
                pass

api = CentralAPI(
    db_path=os.getenv('DB_PATH', 'evcharging.db'),
    kafka_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'API_Central',
                    'kafka_ready': api._kafka_ready}), 200

@app.route('/api/v1/cps', methods=['GET'])
def get_all_cps():
    return jsonify(api.get_all_cps()), 200

@app.route('/api/v1/cps/<cp_id>', methods=['GET'])
def get_cp(cp_id):
    cps = {cp['cp_id']: cp for cp in api.get_all_cps()}
    if cp_id in cps:
        return jsonify(cps[cp_id]), 200
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
@app.route('/api/v1/weather/alert', methods=['POST'])
@require_weather_api_key
def weather_alert():
    data = request.get_json(silent=True)
    if not data or not all(f in data for f in ('cp_id', 'alert_type', 'temperature', 'city')):
        return jsonify({'error': 'Faltan campos requeridos'}), 400
    try:
        temperature = float(data['temperature'])
    except (ValueError, TypeError):
        return jsonify({'error': 'temperature debe ser número'}), 400
    success = api.process_weather_alert(
        str(data['cp_id']), str(data['alert_type']).upper(),
        temperature, str(data['city'])
    )
    if success:
        return jsonify({'message': 'Alerta procesada'}), 200
    return jsonify({'error': 'Error procesando alerta'}), 500
@app.route('/api/v1/system/status', methods=['GET'])
def get_system_status():
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
        logger.exception("Error en /system/status: %s", exc)
        return jsonify({'error': str(exc)}), 500

if __name__ == '__main__':
    def _handle_signal(signum, frame):
        logger.info("🛑 Señal %d — cerrando...", signum)
        api.shutdown()
        sys.exit(0)
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)
    port = int(os.getenv('API_PORT', 8082))
    logger.info("🚀 API_Central en http://0.0.0.0:%d", port)
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
