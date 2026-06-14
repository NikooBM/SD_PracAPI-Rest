#!/bin/bash
# launch_cps.sh — PC2: CPs (Engine + Monitor) + EV_W
set -euo pipefail

echo "=============================================="
echo " EVCharging — Lanzamiento PC2 (CPs + EV_W)"
echo "=============================================="

# ── Limpieza ────────────────────────────────────────────────────────
echo "🧹 Limpiando contenedores CP anteriores..."
docker ps -a --format '{{.Names}}' | grep -E '^(CP[0-9]|ev_w)' | xargs -r docker rm -f 2>/dev/null || true

# ── IP de PC1 ────────────────────────────────────────────────────────
read -rp "Introduce la IP del PC1 (Central/Kafka): " IP_CENTRAL
[ -z "$IP_CENTRAL" ] && { echo "❌ IP requerida."; exit 1; }

# ── Número de CPs ───────────────────────────────────────────────────
read -rp "Número de CPs a desplegar [default: 3]: " NUM_CPS
NUM_CPS="${NUM_CPS:-3}"

# ── Imagen Docker ────────────────────────────────────────────────────
# Intentar usar imagen ya construida; si no, construir aquí
IMAGE="evcharging-app:latest"
if ! docker image inspect "$IMAGE" &>/dev/null; then
    echo "🛠️  Imagen no encontrada, construyendo..."
    docker build -t "$IMAGE" .
fi

# ── OpenWeather vars para EV_W ───────────────────────────────────────
read -rp "OpenWeather API Key (para EV_W): " OPENWEATHER_KEY
OPENWEATHER_KEY="${OPENWEATHER_KEY:-dummy_key_not_set}"
read -rp "WEATHER_API_KEY (debe coincidir con la del PC1): " WEATHER_API_KEY
WEATHER_API_KEY="${WEATHER_API_KEY:-weather-api-key-change-in-production}"

# ── Lanzar CPs ──────────────────────────────────────────────────────
echo ""
echo "🚀 Desplegando $NUM_CPS Charging Points..."

for i in $(seq 1 "$NUM_CPS"); do
    CP_ID=$(printf "CP%03d" "$i")
    LOCATION="Punto de Carga ${i}"
    PRICE=$(python3 -c "print(f'{0.40 + $i * 0.05:.2f}')")
    ENGINE_PORT=$((6000 + i))
    # Engine y Monitor del mismo CP vía bind-mount.
    CP_DATA_HOST="$(pwd)/cp_data/${CP_ID}"
    mkdir -p "$CP_DATA_HOST"

    echo ""
    echo "--- ${CP_ID} (puerto engine: ${ENGINE_PORT}, precio: ${PRICE}€/kWh) ---"

    # Engine
    docker run -d -it \
        --name "${CP_ID}-engine" \
        --network host \
        -e CP_ID="$CP_ID" \
        -e LISTEN_PORT="$ENGINE_PORT" \
        -e PRICE_PER_KWH="$PRICE" \
        -e KAFKA_BOOTSTRAP_SERVERS="${IP_CENTRAL}:9092" \
        -e CP_DATA_DIR="/app/cp_data" \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/charging_point:/app/charging_point:ro" \
        -v "$(pwd)/security:/app/security:ro" \
        -v "${CP_DATA_HOST}:/app/cp_data" \
        "$IMAGE" \
        python charging_point/ev_cp_e.py

    echo "✅ Engine ${CP_ID} lanzado"
    sleep 2

    # Monitor
    docker run -d -it \
        --name "${CP_ID}-monitor" \
        --network host \
        -e CP_ID="$CP_ID" \
        -e CP_LOCATION="$LOCATION" \
        -e CP_PRICE="$PRICE" \
        -e CENTRAL_HOST="$IP_CENTRAL" \
        -e CENTRAL_PORT=5001 \
        -e ENGINE_HOST=127.0.0.1 \
        -e ENGINE_PORT="$ENGINE_PORT" \
        -e REGISTRY_URL="https://${IP_CENTRAL}:8443" \
        -e CP_DATA_DIR="/app/cp_data" \
        -e MACHINE_SECRET="${MACHINE_SECRET:-evcharging-lab-secret}" \
        -e REAUTH_ON_DISCONNECT=true \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/charging_point:/app/charging_point:ro" \
        -v "$(pwd)/security:/app/security:ro" \
        -v "${CP_DATA_HOST}:/app/cp_data" \
        "$IMAGE" \
        bash -c "sleep 3 && python charging_point/ev_cp_m.py"

    echo "✅ Monitor ${CP_ID} lanzado"
    echo "   Engine:  docker attach ${CP_ID}-engine"
    echo "   Monitor: docker attach ${CP_ID}-monitor"
    sleep 1
done

# ── Lanzar EV_W (Weather) ────────────────────────────────────────────
echo ""
echo "🌡️  Lanzando EV_W (Weather Control Office)..."

docker run -d -it \
    --name ev_w \
    --network host \
    -e OPENWEATHER_API_KEY="$OPENWEATHER_KEY" \
    -e CENTRAL_API_URL="http://${IP_CENTRAL}:8082" \
    -e WEATHER_API_KEY="$WEATHER_API_KEY" \
    -e CHECK_INTERVAL=4 \
    -e WEATHER_LOCATIONS_FILE=/app/weather/locations.json \
    -e PYTHONUNBUFFERED=1 \
    -v "$(pwd)/weather:/app/weather:rw" \
    -v "$(pwd)/security:/app/security:ro" \
    "$IMAGE" \
    python weather/ev_w.py

echo "✅ EV_W lanzado"
echo "   Interactuar: docker attach ev_w"

echo ""
echo "=============================================="
echo " ✅ PC2 listo — $NUM_CPS CPs + EV_W"
echo "=============================================="
echo " 💡 Comandos útiles:"
echo "   Ver CPs:       docker ps --filter 'name=CP'"
echo "   Logs Engine:   docker logs -f CP001-engine"
echo "   Conectar:      docker attach CP001-engine"
echo "   Desconectar:   CTRL+P  CTRL+Q  (no mata el contenedor)"
echo "   Parar todos:   docker stop \$(docker ps -q --filter 'name=CP')"
echo "=============================================="
