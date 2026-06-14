#!/bin/bash
# launch_drivers.sh — PC3: Drivers
set -euo pipefail

echo "=============================================="
echo " EVCharging — Lanzamiento PC3 (Drivers)"
echo "=============================================="

# ── Limpieza ────────────────────────────────────────────────────────
echo "🧹 Limpiando Drivers anteriores..."
docker ps -a --format '{{.Names}}' | grep '^driver-' | xargs -r docker rm -f 2>/dev/null || true

# ── IP de PC1 ────────────────────────────────────────────────────────
read -rp "Introduce la IP del PC1 (Central/Kafka): " IP_CENTRAL
[ -z "$IP_CENTRAL" ] && { echo "❌ IP requerida."; exit 1; }

# ── Número de Drivers ────────────────────────────────────────────────
read -rp "Número de Drivers a desplegar [default: 2]: " NUM_DRIVERS
NUM_DRIVERS="${NUM_DRIVERS:-2}"

# ── Imagen Docker ────────────────────────────────────────────────────
IMAGE="evcharging-app:latest"
if ! docker image inspect "$IMAGE" &>/dev/null; then
    echo "🛠️  Imagen no encontrada, construyendo..."
    docker build -t "$IMAGE" .
fi

# ── Lanzar Drivers ───────────────────────────────────────────────────
echo ""
echo "🚀 Desplegando $NUM_DRIVERS Drivers..."

for i in $(seq 1 "$NUM_DRIVERS"); do
    DRIVER_ID=$(printf "Driver_%03d" "$i")
    # Nombre de contenedor: driver-001, driver-002, …
    CONTAINER=$(echo "$DRIVER_ID" | tr '_' '-' | tr '[:upper:]' '[:lower:]')

    echo ""
    echo "--- ${DRIVER_ID} ---"

    docker run -d -it \
        --name "$CONTAINER" \
        --network host \
        -e DRIVER_ID="$DRIVER_ID" \
        -e KAFKA_BOOTSTRAP_SERVERS="${IP_CENTRAL}:9092" \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/driver:/app/driver:rw" \
        -v "$(pwd)/security:/app/security:ro" \
        "$IMAGE" \
        python driver/ev_driver.py

    echo "✅ ${DRIVER_ID} lanzado"
    echo "   Conectar: docker attach ${CONTAINER}"
    sleep 1
done

echo ""
echo "=============================================="
echo " ✅ PC3 listo — $NUM_DRIVERS Drivers"
echo "=============================================="
echo " 💡 Comandos útiles:"
echo "   Ver Drivers:   docker ps --filter 'name=driver'"
echo "   Conectar:      docker attach driver-001"
echo "   Desconectar:   CTRL+P  CTRL+Q"
echo "   Parar todos:   docker stop \$(docker ps -q --filter 'name=driver')"
echo ""
echo " 💡 Comandos del Driver (tras docker attach):"
echo "   request CP001              — solicitar carga en CP001"
echo "   file services/servicios.txt — cargar lista desde archivo"
echo "   status                     — ver estado"
echo "   msg                        — ver mensajes"
echo "   quit                       — salir"
echo "=============================================="
