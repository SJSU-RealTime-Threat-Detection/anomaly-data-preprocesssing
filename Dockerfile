FROM openjdk:11-slim

# Install required system tools and Python
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    python3-pip \
    python3-setuptools \
    python3-venv \
    && apt-get clean

# Install Spark manually
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3

RUN curl -fSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
    tar -xzf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy application files
COPY . .

# Install Python dependencies
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Command to run Spark job with Kafka connector
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4", "kafka_consumer.py"]
