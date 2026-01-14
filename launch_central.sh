#!/bin/bash
# launch_central.sh - CORREGIDO

echo "=============================================="
echo "EVCharging - Lanzamiento PC1 (CENTRAL)"
echo "=============================================="

# Solicitar IP del PC1
read -p "Introduce la IP de este PC (PC1): " IP_PC1

if [ -z "$IP_PC1" ]; then
    echo "❌ Error: Se necesita una IP."
    exit 1
fi

# Usar backup file y sed con -i.bak para compatibilidad
echo "📝 Configurando Kafka con IP: $IP_PC1"

# Crear backup
cp docker-compose.yml docker-compose.yml.bak

# CORRECCIÓN: sed con sintaxis correcta
sed -i.tmp "s/TU_IP_PC1/${IP_PC1}/g" docker-compose.yml

# Limpiar archivos temporales
rm -f docker-compose.yml.tmp docker-compose.yml.bak

echo "✅ docker-compose.yml actualizado"

# Solicitar API Key de OpenWeather
read -p "Introduce tu OpenWeather API Key: " OPENWEATHER_KEY

if [ -z "$OPENWEATHER_KEY" ]; then
    echo "⚠️  Advertencia: No se proporcionó API Key de OpenWeather"
    echo "   EV_W no funcionará correctamente"
    OPENWEATHER_KEY="dummy_key"
fi

# Crear archivo .env
cat > .env << ENVEOF
OPENWEATHER_API_KEY=$OPENWEATHER_KEY
JWT_SECRET_KEY=$(openssl rand -hex 32)
ENVEOF

echo "✅ Configuración guardada en .env"

# Limpiar contenedores anteriores
echo "🧹 Limpiando sistema anterior..."
docker-compose down -v 2>/dev/null

# Construir imagen
echo "🛠️  Construyendo imagen Docker..."
docker-compose build

if [ $? -ne 0 ]; then
    echo "❌ Error en build"
    exit 1
fi

# Lanzar servicios
echo "🚀 Lanzando servicios en PC1..."
docker-compose up -d

echo ""
echo "=============================================="
echo "✅ PC1 (Central) lanzado correctamente"
echo "=============================================="
echo "Servicios disponibles:"
echo "  - Central Socket:  ${IP_PC1}:5001"
echo "  - Kafka:           ${IP_PC1}:9092"
echo "  - API Central:     http://${IP_PC1}:8080"
echo "  - Registry HTTPS:  https://${IP_PC1}:8443"
echo "  - Front Web:       http://${IP_PC1}:80"
echo "=============================================="
echo ""
echo "📋 Próximos pasos:"
echo "  1. En PC2: Ejecutar ./launch_cps.sh"
echo "  2. En PC3: Ejecutar ./launch_drivers.sh"
echo "  3. Usar la IP: $IP_PC1 cuando se solicite"
echo "=============================================="

# Mostrar logs de Central
echo ""
read -p "¿Ver logs de Central? (s/n): " VER_LOGS
if [ "$VER_LOGS" = "s" ]; then
    docker-compose logs -f central
fi