FROM apache/airflow:3.0.0

USER root

# Install Java 17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Download PostgreSQL JDBC driver for PySpark
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.1.jar