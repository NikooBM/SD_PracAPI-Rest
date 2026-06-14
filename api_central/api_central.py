"""
API_CENTRAL — API REST para consultar el estado del sistema EVCharging.
Incluye endpoints de auditoría de seguridad con soporte SSE (tiempo real).
Release 2 / C4 - Práctica SD 25/26
"""
import os, sys, signal, logging, json, time, threading, re, queue
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional, Generator
import sqlite3

from flask import Flask, request, jsonify, make_response, Response, stream_with_context
from flask_cors import CORS

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('API-Central')

app = Flask(__name__)
CORS(app)

WEATHER_API_KEY = os.getenv('WEATHER_API_KEY', '').strip()
if not WEATHER_API_KEY:
    logger.warning("⚠️ WEATHER_API_KEY no configurada.")

# ── Autenticación endpoints weather ─────────────────────────────────────────

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


# ── Gestor de suscriptores SSE ───────────────────────────────────────────────

class SSEBroker:
    """Distribuye eventos de auditoría en tiempo real a todos los clientes SSE conectados."""

    def __init__(self):
        self._subscribers: List[queue.Queue] = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=100)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self._lock:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass

    def publish(self, event: Dict):
        """Envía un evento a todos los suscriptores. Descarta si el buffer está lleno."""
        with self._lock:
            dead = []
            for q in self._subscribers:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    dead.append(q)
            for q in dead:
                self._subscribers.remove(q)

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subscribers)


sse_broker = SSEBroker()


# ── Parser del audit.log ─────────────────────────────────────────────────────

AUDIT_LOG_PATH = os.getenv('AUDIT_LOG', '/app/audit/audit.log')

# Patrón: "2025-12-01 10:23:45,123 | INFO | [AUTH] SOURCE=... | ACTOR=... | ..."
_LOG_RE = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)\s*\|\s*'
    r'(?P<level>\w+)\s*\|\s*'
    r'\[(?P<event_type>[^\]]+)\]\s*'
    r'SOURCE=(?P<source>[^\|]+?)\s*\|\s*'
    r'ACTOR=(?P<actor>[^\|]+?)\s*\|\s*'
    r'ACTION=(?P<action>[^\|]+?)\s*\|\s*'
    r'STATUS=(?P<status>\w+)'
    r'(?:\s*\|\s*DETAILS=(?P<details>.+))?$'
)


def _parse_log_line(line: str) -> Optional[Dict]:
    """Parsea una línea del audit.log y devuelve un dict o None si no coincide."""
    line = line.strip()
    if not line:
        return None
    m = _LOG_RE.match(line)
    if not m:
        return None
    return {
        'timestamp':  m.group('timestamp'),
        'level':      m.group('level'),
        'event_type': m.group('event_type').strip(),
        'source':     m.group('source').strip(),
        'actor':      m.group('actor').strip(),
        'action':     m.group('action').strip(),
        'status':     m.group('status').strip(),
        'details':    (m.group('details') or '').strip(),
    }


class AuditReader:
    """Lee y filtra eventos del audit.log con soporte de tail en tiempo real."""

    def __init__(self, log_path: str):
        self.log_path = log_path

    def _log_exists(self) -> bool:
        return os.path.exists(self.log_path)

    def get_events(self, limit: int = 100, event_type: Optional[str] = None,
                   actor: Optional[str] = None, status: Optional[str] = None,
                   since: Optional[str] = None) -> List[Dict]:
        """
        Devuelve hasta `limit` eventos del log, los más recientes primero.
        Filtra opcionalmente por event_type, actor, status y timestamp >= since.
        """
        if not self._log_exists():
            return []
        limit = max(1, min(limit, 1000))
        events = []
        try:
            with open(self.log_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    parsed = _parse_log_line(line)
                    if parsed is None:
                        continue
                    if event_type and parsed['event_type'].upper() != event_type.upper():
                        continue
                    if actor and actor.lower() not in parsed['actor'].lower():
                        continue
                    if status and parsed['status'].upper() != status.upper():
                        continue
                    if since and parsed['timestamp'] < since:
                        continue
                    events.append(parsed)
        except OSError as e:
            logger.error("Error leyendo audit.log: %s", e)
        # Más recientes primero, limitar
        return list(reversed(events))[:limit]

    def get_stats(self) -> Dict:
        """Resumen estadístico de los eventos de auditoría."""
        if not self._log_exists():
            return {'total': 0, 'by_type': {}, 'by_status': {}, 'auth_failures': 0}
        by_type:   Dict[str, int] = {}
        by_status: Dict[str, int] = {}
        auth_failures = 0
        total = 0
        try:
            with open(self.log_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    parsed = _parse_log_line(line)
                    if parsed is None:
                        continue
                    total += 1
                    et = parsed['event_type']
                    st = parsed['status']
                    by_type[et]   = by_type.get(et, 0) + 1
                    by_status[st] = by_status.get(st, 0) + 1
                    if et == 'AUTH' and st == 'FAILED':
                        auth_failures += 1
        except OSError as e:
            logger.error("Error leyendo audit.log para stats: %s", e)
        return {
            'total':         total,
            'by_type':       by_type,
            'by_status':     by_status,
            'auth_failures': auth_failures,
        }

    def tail(self) -> Generator[Dict, None, None]:
        """
        Generador que hace tail del log: primero devuelve las últimas 20 líneas
        y luego espera nuevas entradas indefinidamente.
        """
        if not self._log_exists():
            time.sleep(1)

        # Leer las últimas 20 líneas para dar contexto inicial
        initial = self.get_events(limit=20)
        for event in reversed(initial):
            yield event

        # Tail en tiempo real
        try:
            with open(self.log_path, 'r', encoding='utf-8', errors='replace') as f:
                f.seek(0, 2)  # ir al final del archivo
                while True:
                    line = f.readline()
                    if line:
                        parsed = _parse_log_line(line)
                        if parsed:
                            yield parsed
                    else:
                        time.sleep(0.5)
        except OSError:
            pass


audit_reader = AuditReader(AUDIT_LOG_PATH)


# ── Hilo que hace tail del log y publica en SSEBroker ───────────────────────

def _audit_tail_thread():
    """Hilo daemon: hace tail del audit.log y publica cada nuevo evento en el SSEBroker."""
    logger.info("🔍 Iniciando tail de audit.log: %s", AUDIT_LOG_PATH)
    # Esperar a que el archivo exista
    while not os.path.exists(AUDIT_LOG_PATH):
        time.sleep(2)

    try:
        with open(AUDIT_LOG_PATH, 'r', encoding='utf-8', errors='replace') as f:
            f.seek(0, 2)  # ir al final
            while True:
                line = f.readline()
                if line:
                    parsed = _parse_log_line(line)
                    if parsed:
                        sse_broker.publish(parsed)
                else:
                    time.sleep(0.3)
    except Exception as e:
        logger.error("Error en audit_tail_thread: %s", e)


threading.Thread(target=_audit_tail_thread, daemon=True).start()


# ── CentralAPI ───────────────────────────────────────────────────────────────

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


# ── Rutas del sistema existentes ─────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'API_Central',
                    'kafka_ready': api._kafka_ready,
                    'audit_log_exists': os.path.exists(AUDIT_LOG_PATH),
                    'sse_subscribers': sse_broker.subscriber_count}), 200

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


# ── Rutas de auditoría ───────────────────────────────────────────────────────

@app.route('/api/v1/audit/events', methods=['GET'])
def get_audit_events():
    """
    Devuelve eventos de auditoría con filtros opcionales.
    Query params:
      limit      — número máximo de eventos (default 100, max 1000)
      event_type — filtrar por tipo (AUTH, SERVICE, STATUS, COMMAND, WEATHER, ERROR, SYSTEM)
      actor      — filtrar por actor (cp_id, driver_id…)
      status     — filtrar por estado (SUCCESS, FAILED)
      since      — solo eventos desde esta fecha (formato: YYYY-MM-DD HH:MM:SS)
    """
    limit      = request.args.get('limit',      default=100,  type=int)
    event_type = request.args.get('event_type', default=None, type=str)
    actor      = request.args.get('actor',      default=None, type=str)
    status     = request.args.get('status',     default=None, type=str)
    since      = request.args.get('since',      default=None, type=str)

    events = audit_reader.get_events(
        limit=limit, event_type=event_type,
        actor=actor, status=status, since=since
    )
    resp = make_response(jsonify(events), 200)
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@app.route('/api/v1/audit/stats', methods=['GET'])
def get_audit_stats():
    """Resumen estadístico del log de auditoría."""
    stats = audit_reader.get_stats()
    resp  = make_response(jsonify(stats), 200)
    resp.headers['Cache-Control'] = 'no-store'
    return resp


@app.route('/api/v1/audit/stream', methods=['GET'])
def audit_stream():
    """
    Server-Sent Events: stream de eventos de auditoría en tiempo real.
    El cliente recibe las últimas 20 entradas al conectar y luego
    cada nuevo evento conforme se genera.
    Formato SSE estándar — compatible con EventSource de JavaScript.
    """
    def generate():
        # Enviar las últimas 20 líneas del log como contexto inicial
        initial = audit_reader.get_events(limit=20)
        for event in reversed(initial):
            data = json.dumps(event, ensure_ascii=False)
            yield f"event: audit\ndata: {data}\n\n"

        # Suscribirse al broker SSE para recibir nuevos eventos en tiempo real
        q = sse_broker.subscribe()
        # Keepalive: comentario SSE cada 15s para evitar timeout del navegador
        last_keepalive = time.time()
        try:
            while True:
                try:
                    event = q.get(timeout=15)
                    data  = json.dumps(event, ensure_ascii=False)
                    yield f"event: audit\ndata: {data}\n\n"
                    last_keepalive = time.time()
                except queue.Empty:
                    # Keepalive
                    yield ': keepalive\n\n'
                    last_keepalive = time.time()
        except GeneratorExit:
            pass
        finally:
            sse_broker.unsubscribe(q)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':  'no-cache',
            'X-Accel-Buffering': 'no',   # Desactiva buffer de nginx
            'Connection':     'keep-alive',
        }
    )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    def _handle_signal(signum, frame):
        logger.info("🛑 Señal %d — cerrando...", signum)
        api.shutdown()
        sys.exit(0)
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    port = int(os.getenv('API_PORT', 8082))
    logger.info("🚀 API_Central en http://0.0.0.0:%d", port)
    logger.info("📋 Audit log:   %s", AUDIT_LOG_PATH)
    logger.info("📡 SSE stream:  http://0.0.0.0:%d/api/v1/audit/stream", port)
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
