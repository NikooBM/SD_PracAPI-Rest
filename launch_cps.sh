#!/bin/bash
echo "=============================================="
echo "EVCharging - Lanzamiento PC2 (CPs)"
echo "=============================================="

# Limpiar CPs anteriores
echo "🧹 Limpiando CPs antiguos..."
docker rm -f $(docker ps -a -q --filter "name=CP") 2>/dev/null || true

# Solicitar IP de PC1
read -p "Introduce la IP del PC1 (Central): " IP_CENTRAL

if [ -z "$IP_CENTRAL" ]; then
    echo "❌ Error: Se necesita la IP de PC1."
    exit 1
fi

# Número de CPs
read -p "Número de CPs a desplegar (default: 3): " NUM_CPS
NUM_CPS=${NUM_CPS:-3}

echo ""
echo "🚀 Desplegando $NUM_CPS Charging Points..."
echo "=============================================="

for i in $(seq 1 $NUM_CPS); do
    CP_ID=$(printf "CP%03d" $i)
    LOCATION="Punto de Carga $i"
    PRICE=$(LC_NUMERIC=C awk -v i="$i" 'BEGIN{printf "%.2f", 0.40 + i*0.05}')
    ENGINE_PORT=$((6000 + i))
    CONTAINER_NAME="${CP_ID}-engine"
    
    echo ""
    echo "--- Lanzando $CP_ID ---"
    echo "  Ubicación: $LOCATION"
    echo "  Precio: ${PRICE}€/kWh"
    
    # Lanzar Engine
    docker run -d -it \
        --name $CONTAINER_NAME \
        --network host \
        -e CP_ID=$CP_ID \
        -e LISTEN_PORT=$ENGINE_PORT \
        -e PRICE_PER_KWH=$PRICE \
        -e KAFKA_BOOTSTRAP_SERVERS=${IP_CENTRAL}:9092 \
        -e PYTHONUNBUFFERED=1 \
        -v $(pwd)/charging_point:/app/charging_point:ro \
        -v $(pwd)/security:/app/security:ro \
        principal-kafka-init \
        python charging_point/ev_cp_e.py
    
    if [ $? -ne 0 ]; then
        echo "❌ Error desplegando Engine $CP_ID"
        continue
    fi
    
    echo "✅ Engine $CP_ID lanzado (puerto $ENGINE_PORT)"
    sleep 3
    
    # Lanzar Monitor
    docker run -d -it \
        --name ${CP_ID}-monitor \
        --network host \
        -e CP_ID=$CP_ID \
        -e CP_LOCATION="$LOCATION" \
        -e CP_PRICE=$PRICE \
        -e CENTRAL_HOST=$IP_CENTRAL \
        -e CENTRAL_PORT=5001 \
        -e ENGINE_HOST=127.0.0.1 \
        -e ENGINE_PORT=$ENGINE_PORT \
        -e REGISTRY_URL=https://${IP_CENTRAL}:8443 \
        -v $(pwd)/charging_point:/app/charging_point:ro \
        -v $(pwd)/security:/app/security:ro \
        principal-kafka-init \
        bash -c "sleep 3 && python charging_point/ev_cp_m.py"
    
    if [ $? -ne 0 ]; then
        echo "❌ Error desplegando Monitor $CP_ID"
        continue
    fi
    
    echo "✅ Monitor $CP_ID lanzado"
    echo "   Para interactuar: docker attach $CONTAINER_NAME"
    sleep 1
done

echo ""
echo "=============================================="
echo "✅ $NUM_CPS CPs desplegados en PC2"
echo "=============================================="
echo ""
echo "📋 Comandos útiles:"
echo "  - Ver CPs activos:    docker ps --filter 'name=CP'"
echo "  - Logs de un CP:      docker logs -f CP001-engine"
echo "  - Conectar a un CP:   docker attach CP001-engine"
echo "  - Parar todos:        docker stop \$(docker ps -q --filter 'name=CP')"
echo "=============================================="
EOF