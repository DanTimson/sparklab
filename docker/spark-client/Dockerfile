FROM eclipse-temurin:17-jdk-noble

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip procps && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir --break-system-packages pyspark==3.5.1

COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

CMD ["bash"]