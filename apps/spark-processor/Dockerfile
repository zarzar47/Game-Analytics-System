# Using a Python base image
FROM python:3.11-slim-bullseye

# Set the working directory
WORKDIR /app

# --- FIX START ---
# Install Java (required for PySpark) and procps (required for background checks)
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark knows where to find Java
ENV JAVA_HOME=/usr/lib/jvm/default-java
# --- FIX END ---

# Install dependencies
COPY apps/spark-processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY apps/spark-processor/main.py .

# The entrypoint will be overridden by the spark-submit command in docker-compose
CMD ["python", "main.py"]