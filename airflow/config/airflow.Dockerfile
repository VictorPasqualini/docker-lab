FROM apache/spark:3.5.1 AS spark-base
FROM apache/airflow:2.9.2-python3.11

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark-base /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements.txt .

RUN pip install uv --break-system-packages || python3 -m pip install uv --break-system-packages || \
    curl -LsSf https://astral.sh/uv/install.sh | sh

RUN uv pip install -r requirements.txt --system

USER airflow