FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    netcat-openbsd \
    tk-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar kafka-topics CLI
RUN curl -L https://archive.apache.org/dist/kafka/3.5.0/kafka_2.13-3.5.0.tgz | \
    tar xz && \
    mv kafka_2.13-3.5.0 /opt/kafka && \
    ln -s /opt/kafka/bin/kafka-topics /usr/local/bin/kafka-topics

# Copiar requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Crear estructura de directorios
RUN mkdir -p /app/central /app/charging_point /app/driver \
             /app/registry /app/api_central /app/weather /app/security /app/front

# Copiar código (se sobrescribirá con volumes en docker-compose)
COPY central/ ./central/
COPY charging_point/ ./charging_point/
COPY driver/ ./driver/
COPY registry/ ./registry/
COPY api_central/ ./api_central/
COPY weather/ ./weather/
COPY security/ ./security/

# Crear directorio para servicios de drivers
RUN mkdir -p /app/driver/services

CMD ["bash"]