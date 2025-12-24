# Using Python base with Java support
FROM python:3.11-slim-bullseye

WORKDIR /app

# Install Java (required for PySpark) and other utilities
RUN apt-get update && \
    apt-get install -y \
    default-jre \
    default-jdk \
    procps \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python dependencies
COPY apps/spark-processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install kafka-python for connectivity checks
RUN pip install kafka-python

# Copy the application code
COPY apps/spark-processor/main.py .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD pgrep -f "python.*main.py" || exit 1

# Run as Python script (not spark-submit)
CMD ["python", "-u", "main.py"]