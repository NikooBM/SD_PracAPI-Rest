#!/bin/bash
# launch_local.sh — Todo en UN solo PC (pruebas locales)
# Lanza Central (via docker compose) + CPs + Drivers + EV_W
set -euo pipefail

echo "=============================================="
echo " EVCharging — Lanzamiento LOCAL (1 PC)"
echo "=============================================="

IMAGE="evcharging-app:latest"

# Verificar que la imagen existe (debe haberse construido con docker compose build)
if ! docker image inspect "$IMAGE" &>/dev/null; then
    echo "⚠️  Imagen '$IMAGE' no encontrada. Construyendo..."
    docker compose build
    if ! docker image inspect "$IMAGE" &>/dev/null; then
        echo "❌ No se pudo construir la imagen. Asegúrate de estar en el directorio del proyecto."
        exit 1
    fi
fi
echo "✅ Imagen '$IMAGE' disponible"

# Número de CPs y Drivers
read -rp "Número de CPs [default: 2]: "     NUM_CPS;     NUM_CPS="${NUM_CPS:-2}"
read -rp "Número de Drivers [default: 2]: " NUM_DRIVERS; NUM_DRIVERS="${NUM_DRIVERS:-2}"

# Limpiar contenedores anteriores de CPs y Drivers
echo ""
echo "🧹 Limpiando contenedores CP/Driver anteriores..."
docker ps -a --format '{{.Names}}' | grep -E '^(CP[0-9]|driver-)' | xargs -r docker rm -f 2>/dev/null || true
docker ps -a --format '{{.Names}}' | grep '^ev_w$' | xargs -r docker rm -f 2>/dev/null || true

# Verificar que la Central está corriendo
if ! docker ps --format '{{.Names}}' | grep -q '^ev_central$'; then
    echo "⚠️  Central no está corriendo. Levantando servicios base..."
    docker compose up -d
    echo "⏳ Esperando 20s a que los servicios arranquen..."
    sleep 20
fi

echo ""
echo "🚀 Desplegando $NUM_CPS CPs..."
for i in $(seq 1 "$NUM_CPS"); do
    CP_ID=$(printf "CP%03d" "$i")
    LOCATION="Punto de Carga ${i}"
    PRICE=$(python3 -c "print(f'{0.40 + $i * 0.05:.2f}')")
    ENGINE_PORT=$((6000 + i))

    # Directorio compartido entre Engine y Monitor del mismo CP
    CP_DATA_HOST="$(pwd)/cp_data/${CP_ID}"
    mkdir -p "$CP_DATA_HOST"

    echo "--- ${CP_ID} (engine port: ${ENGINE_PORT}, precio: ${PRICE}€/kWh) ---"

    # Engine — en la red evcharging_net para hablar con Kafka y Central
    docker run -d -it \
        --name "${CP_ID}-engine" \
        --network evcharging_net \
        -e CP_ID="$CP_ID" \
        -e LISTEN_PORT="$ENGINE_PORT" \
        -e PRICE_PER_KWH="$PRICE" \
        -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
        -e CP_DATA_DIR="/app/cp_data" \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/charging_point:/app/charging_point:ro" \
        -v "$(pwd)/security:/app/security:ro" \
        -v "${CP_DATA_HOST}:/app/cp_data" \
        "$IMAGE" \
        python charging_point/ev_cp_e.py
    echo "  ✅ Engine ${CP_ID} lanzado"
    sleep 2

    # Monitor — misma red, ENGINE_HOST es el nombre del contenedor del Engine
    docker run -d -it \
        --name "${CP_ID}-monitor" \
        --network evcharging_net \
        -e CP_ID="$CP_ID" \
        -e CP_LOCATION="$LOCATION" \
        -e CP_PRICE="$PRICE" \
        -e CENTRAL_HOST="ev_central" \
        -e CENTRAL_PORT=5001 \
        -e ENGINE_HOST="${CP_ID}-engine" \
        -e ENGINE_PORT="$ENGINE_PORT" \
        -e REGISTRY_URL="https://ev_registry:8443" \
        -e CP_DATA_DIR="/app/cp_data" \
        -e MACHINE_SECRET="${MACHINE_SECRET:-evcharging-lab-secret}" \
        -e REAUTH_ON_DISCONNECT=true \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/charging_point:/app/charging_point:ro" \
        -v "$(pwd)/security:/app/security:ro" \
        -v "${CP_DATA_HOST}:/app/cp_data" \
        "$IMAGE" \
        bash -c "sleep 5 && python charging_point/ev_cp_m.py"
    echo "  ✅ Monitor ${CP_ID} lanzado"
    sleep 1
done

# EV_W
echo ""
echo "🌡️  Lanzando EV_W..."
WEATHER_API_KEY_VAL="${WEATHER_API_KEY:-weather-api-key-change-in-production}"
OPENWEATHER_KEY="${OPENWEATHER_API_KEY:-dummy_key}"

docker run -d -it \
    --name ev_w \
    --network evcharging_net \
    -e OPENWEATHER_API_KEY="$OPENWEATHER_KEY" \
    -e CENTRAL_API_URL="http://api_central:8082" \
    -e WEATHER_API_KEY="$WEATHER_API_KEY_VAL" \
    -e CHECK_INTERVAL=4 \
    -e WEATHER_LOCATIONS_FILE=/app/weather/locations.json \
    -e PYTHONUNBUFFERED=1 \
    -v "$(pwd)/weather:/app/weather:rw" \
    -v "$(pwd)/security:/app/security:ro" \
    "$IMAGE" \
    python weather/ev_w.py
echo "  ✅ EV_W lanzado"

echo ""
echo "🚀 Desplegando $NUM_DRIVERS Drivers..."
for i in $(seq 1 "$NUM_DRIVERS"); do
    DRIVER_ID=$(printf "Driver_%03d" "$i")
    CONTAINER=$(echo "$DRIVER_ID" | tr '_' '-' | tr '[:upper:]' '[:lower:]')

    docker run -d -it \
        --name "$CONTAINER" \
        --network evcharging_net \
        -e DRIVER_ID="$DRIVER_ID" \
        -e KAFKA_BOOTSTRAP_SERVERS="kafka:29092" \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/driver:/app/driver:rw" \
        -v "$(pwd)/security:/app/security:ro" \
        "$IMAGE" \
        python driver/ev_driver.py
    echo "  ✅ ${DRIVER_ID} lanzado"
    sleep 1
done

echo ""
echo "=============================================="
echo " ✅ Sistema completo desplegado (1 PC)"
echo "=============================================="
echo ""
echo " Servicios:"
echo "   Central GUI   → pantalla Tkinter (docker attach ev_central si headless)"
echo "   Front Web     → http://localhost:80"
echo "   API Central   → http://localhost:8082"
echo "   Registry      → https://localhost:8443"
echo ""
echo " Interactuar con contenedores:"
echo "   docker attach CP001-engine     (pulsa '1' para cargar, '2' para parar)"
echo "   docker attach CP001-monitor"
echo "   docker attach driver-001       (escribe 'request CP001')"
echo "   docker attach ev_w             (escribe 'add' para añadir ciudad)"
echo "   Salir sin matar: CTRL+P  CTRL+Q"
echo ""
echo " Ver logs:"
echo "   docker compose logs -f central"
echo "   docker logs -f CP001-engine"
echo ""
echo " Parar todo:"
echo "   docker compose down && docker rm -f \$(docker ps -aq --filter 'name=CP') \$(docker ps -aq --filter 'name=driver') ev_w 2>/dev/null || true"
echo "=============================================="
