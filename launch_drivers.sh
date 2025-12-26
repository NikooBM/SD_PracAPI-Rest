#!/bin/bash
echo "=============================================="
echo "EVCharging - Lanzamiento PC3 (Drivers)"
echo "=============================================="

# Limpiar Drivers anteriores
echo "🧹 Limpiando Drivers antiguos..."
docker rm -f $(docker ps -a -q --filter "name=driver-") 2>/dev/null || true

# Solicitar IP de PC1
read -p "Introduce la IP del PC1 (Central): " IP_CENTRAL

if [ -z "$IP_CENTRAL" ]; then
    echo "❌ Error: Se necesita la IP de PC1."
    exit 1
fi

# Número de Drivers
read -p "Número de Drivers a desplegar (default: 2): " NUM_DRIVERS
NUM_DRIVERS=${NUM_DRIVERS:-2}

echo ""
echo "🚀 Desplegando $NUM_DRIVERS Drivers..."
echo "=============================================="

for i in $(seq 1 $NUM_DRIVERS); do
    DRIVER_ID=$(printf "Driver_%03d" $i)
    CONTAINER_NAME=$(echo $DRIVER_ID | tr '_' '-' | tr '[:upper:]' '[:lower:]')
    
    echo ""
    echo "--- Lanzando $DRIVER_ID ---"
    
    docker run -d -it \
        --name $CONTAINER_NAME \
        --network host \
        -e DRIVER_ID=$DRIVER_ID \
        -e KAFKA_BOOTSTRAP_SERVERS=${IP_CENTRAL}:9092 \
        -e PYTHONUNBUFFERED=1 \
        -v $(pwd)/driver:/app/driver:rw \
        -v $(pwd)/security:/app/security:ro \
        principal-kafka-init \
        python driver/ev_driver.py
    
    if [ $? -ne 0 ]; then
        echo "❌ Error desplegando Driver $DRIVER_ID"
        continue
    fi
    
    echo "✅ $DRIVER_ID lanzado"
    echo "   Para interactuar: docker attach $CONTAINER_NAME"
    sleep 1
done

echo ""
echo "=============================================="
echo "✅ $NUM_DRIVERS Drivers desplegados en PC3"
echo "=============================================="
echo ""
echo "📋 Comandos útiles:"
echo "  - Ver Drivers activos:  docker ps --filter 'name=driver'"
echo "  - Logs de un Driver:    docker logs -f driver-001"
echo "  - Conectar a un Driver: docker attach driver-001"
echo "  - Parar todos:          docker stop \$(docker ps -q --filter 'name=driver')"
echo "=============================================="
echo ""
echo "💡 IMPORTANTE: Para interactuar con un Driver:"
echo "   1. docker attach driver-001"
echo "   2. Usar comandos: request CP001, file services/servicios.txt, etc."
echo "   3. Salir: CTRL+P seguido de CTRL+Q (sin detener el contenedor)"
echo "=============================================="
