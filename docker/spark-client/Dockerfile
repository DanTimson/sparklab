# eclipse-temurin:17-jdk-noble = Ubuntu 22.04 + Java 17, official image
FROM eclipse-temurin:17-jdk-noble

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip procps && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

CMD ["bash"]