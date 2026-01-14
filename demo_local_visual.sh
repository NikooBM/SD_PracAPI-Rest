#!/bin/bash
# demo_local_visual.sh

echo "=============================================="
echo "EVCharging - Demo LOCAL (1 PC)"
echo "=============================================="

read -p "Número de CPs (default: 3): " NUM_CPS
NUM_CPS=${NUM_CPS:-3}

read -p "Número de Drivers (default: 2): " NUM_DRIVERS
NUM_DRIVERS=${NUM_DRIVERS:-2}

# Limpiar
echo "🧹 Limpiando sistema..."
docker-compose down -v 2>/dev/null
docker rm -f $(docker ps -a -q --filter "name=CP") 2>/dev/null
docker rm -f $(docker ps -a -q --filter "name=driver-") 2>/dev/null

# CORRECCIÓN: Configurar para localhost con sed correcto
echo "📝 Configurando para localhost..."
cp docker-compose.yml docker-compose.yml.bak
sed -i.tmp 's/TU_IP_PC1/localhost/g' docker-compose.yml
rm -f docker-compose.yml.tmp docker-compose.yml.bak

# API Key
read -p "OpenWeather API Key: " OPENWEATHER_KEY
cat > .env << ENVEOF
OPENWEATHER_API_KEY=${OPENWEATHER_KEY:-dummy_key}
JWT_SECRET_KEY=$(openssl rand -hex 32)
ENVEOF

# Build y lanzar infraestructura
echo "🛠️  Construyendo..."
docker-compose build
# docker build -t principal-kafka-init .

echo "🚀 Lanzando infraestructura..."
docker-compose up -d

echo "⏳ Esperando servicios..."
sleep 20

# Lanzar CPs
echo "🔌 Lanzando $NUM_CPS CPs..."
for i in $(seq 1 $NUM_CPS); do
    CP_ID=$(printf "CP%03d" $i)
    PRICE=$(LC_NUMERIC=C awk -v i="$i" 'BEGIN{printf "%.2f", 0.40 + i*0.05}')
    ENGINE_PORT=$((6000 + i))
    
    docker run -d -it \
        --name ${CP_ID}-engine \
        --network evcharging_net \
        -e CP_ID=$CP_ID \
        -e LISTEN_PORT=$ENGINE_PORT \
        -e PRICE_PER_KWH=$PRICE \
        -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
        -v $(pwd)/charging_point:/app/charging_point:ro \
        -v $(pwd)/security:/app/security:ro \
        principal-kafka-init \
        python charging_point/ev_cp_e.py
    
    sleep 2
    
    docker run -d -it \
        --name ${CP_ID}-monitor \
        --network evcharging_net \
        -e CP_ID=$CP_ID \
        -e CP_LOCATION="CP Local $i" \
        -e CP_PRICE=$PRICE \
        -e CENTRAL_HOST=ev_central \
        -e CENTRAL_PORT=5001 \
        -e ENGINE_HOST=${CP_ID}-engine \
        -e ENGINE_PORT=$ENGINE_PORT \
        -e REGISTRY_URL=https://ev_registry:8443 \
        -v $(pwd)/charging_point:/app/charging_point:ro \
        -v $(pwd)/security:/app/security:ro \
        principal-kafka-init \
        bash -c "sleep 3 && python charging_point/ev_cp_m.py"
    
    echo "✅ $CP_ID lanzado"
    sleep 1
done

# Lanzar Drivers
echo "🚗 Lanzando $NUM_DRIVERS Drivers..."
for i in $(seq 1 $NUM_DRIVERS); do
    DRIVER_ID=$(printf "Driver_%03d" $i)
    CONTAINER_NAME=$(echo $DRIVER_ID | tr '_' '-' | tr '[:upper:]' '[:lower:]')
    
    docker run -d -it \
        --name $CONTAINER_NAME \
        --network evcharging_net \
        -e DRIVER_ID=$DRIVER_ID \
        -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
        -v $(pwd)/driver:/app/driver:rw \
        -v $(pwd)/security:/app/security:ro \
        principal-kafka-init \
        python driver/ev_driver.py
    
    echo "✅ $DRIVER_ID lanzado"
    sleep 1
done

echo ""
echo "=============================================="
echo "✅ Demo LOCAL completada"
echo "=============================================="
echo "Servicios:"
echo "  - Front:      http://localhost"
echo "  - API:        http://localhost:8080"
echo "  - Registry:   https://localhost:8443"
echo "=============================================="
echo ""
echo "💡 COMANDOS ÚTILES:"
echo "  - Ver CPs:        docker ps --filter 'name=CP'"
echo "  - Ver Drivers:    docker ps --filter 'name=driver'"
echo "  - Logs Central:   docker-compose logs -f central"
echo "  - Conectar CP:    docker attach CP001-engine"
echo "  - Conectar Driver: docker attach driver-001"
echo "  - Salir sin matar: CTRL+P seguido de CTRL+Q"
echo "=============================================="