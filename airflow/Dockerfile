FROM apache/airflow:2.7.0-python3.11

USER root
# Install system-level dependencies needed for compiling C-extensions (like gssapi/kerberos)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libkrb5-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Upgrade pip
RUN pip install --upgrade pip

# 2. Install providers and libraries
# The HDFS provider pulls in security dependencies that now have 
# the required system headers from the 'root' step above.
RUN pip install --no-cache-dir \
    "apache-airflow-providers-mongo" \
    "apache-airflow-providers-apache-hdfs" \
    "pymongo" \
    "kafka-python" \
    "requests" \
    "pandas" \
    "pyarrow" \
    "hdfs" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-3.11.txt"