#!/bin/bash
# launch_central.sh — PC1: Kafka + Central + Registry + API_Central + Front + Weather
set -euo pipefail

echo "=============================================="
echo " EVCharging — Lanzamiento PC1 (CENTRAL)"
echo "=============================================="

# ── IP del PC1 ──────────────────────────────────────────────────────
read -rp "Introduce la IP de este PC (PC1) [default: localhost]: " IP_PC1
IP_PC1="${IP_PC1:-localhost}"

# ── API Key OpenWeather ─────────────────────────────────────────────
read -rp "OpenWeather API Key (entra en openweathermap.org): " OPENWEATHER_KEY
if [ -z "$OPENWEATHER_KEY" ]; then
    echo "⚠️  Sin API Key de OpenWeather — EV_W no podrá consultar el clima."
    OPENWEATHER_KEY="dummy_key_not_set"
fi

# ── Generar .env ─────────────────────────────────────────────────────
JWT_SECRET=$(openssl rand -hex 32)
MACHINE_SECRET=$(openssl rand -hex 24)
WEATHER_API_KEY=$(openssl rand -hex 16)

cat > .env << ENVEOF
# Generado por launch_central.sh — NO subir a git
OPENWEATHER_API_KEY=${OPENWEATHER_KEY}
JWT_SECRET_KEY=${JWT_SECRET}
MACHINE_SECRET=${MACHINE_SECRET}
REGISTRY_ADMIN_KEY=$(openssl rand -hex 16)
WEATHER_API_KEY=${WEATHER_API_KEY}
# En local usa localhost; en distribuido, pon aquí la IP real del PC1.
KAFKA_EXTERNAL_IP=${IP_PC1}
ENVEOF

echo "✅ Archivo .env generado"
echo "   KAFKA_EXTERNAL_IP=${IP_PC1}"

# ── Limpiar y construir ──────────────────────────────────────────────
echo ""
echo "🧹 Limpiando contenedores anteriores..."
docker compose down -v 2>/dev/null || true

echo "🛠️  Construyendo imagen Docker..."
docker compose build

echo "🚀 Lanzando servicios en PC1..."
docker compose up -d

echo ""
echo "=============================================="
echo " ✅ PC1 lanzado correctamente"
echo "=============================================="
echo " Servicios:"
echo "   Central Socket  → ${IP_PC1}:5001"
echo "   Kafka externo   → ${IP_PC1}:9092"
echo "   API Central     → http://${IP_PC1}:8082"
echo "   Registry HTTPS  → https://${IP_PC1}:8443"
echo "   Front Web       → http://${IP_PC1}:80"
echo "=============================================="
echo ""
echo " 📋 En PC2 ejecuta:   ./launch_cps.sh"
echo "    Cuando pregunte la IP de PC1 introduce: ${IP_PC1}"
echo " 📋 En PC3 ejecuta:   ./launch_drivers.sh"
echo "    Cuando pregunte la IP de PC1 introduce: ${IP_PC1}"
echo ""
echo " 💡 El weather (EV_W) está en PC2 junto a los CPs."
echo "    Para interactuar: docker attach ev_w"
echo "=============================================="
echo ""

read -rp "¿Ver logs de la Central? (s/n): " VER_LOGS
if [ "${VER_LOGS}" = "s" ]; then
    docker compose logs -f central
fi
