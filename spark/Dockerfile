# Imagen oficial de Airflow + Java (Spark corre sobre la JVM) + PySpark.
# La usan dos contenedores: spark-streaming y airflow.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "pyspark==3.5.1"

# Conector Spark <-> Kafka (se descarga una vez aquí, no en cada arranque)
RUN JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
 && M=https://repo1.maven.org/maven2 \
 && curl -fsSL -o $JARS/spark-sql-kafka-0-10_2.12-3.5.1.jar  $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSL -o $JARS/spark-token-provider-kafka-0-10_2.12-3.5.1.jar $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
 && curl -fsSL -o $JARS/kafka-clients-3.4.1.jar $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSL -o $JARS/commons-pool2-2.11.1.jar $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
