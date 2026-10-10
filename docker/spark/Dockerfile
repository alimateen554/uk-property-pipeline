# Base image: small Debian Linux with Python 3.11
FROM python:3.11-slim-bookworm

# Install Java 17 (Spark runs on the JVM) and clean up apt cache
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first so this layer is cached
COPY requirements-spark.txt .
RUN pip install --no-cache-dir -r requirements-spark.txt

# Copy the pipeline code
COPY src/ src/

# Default command: run the cleaning job
CMD ["python", "src/transform/clean_price_paid.py"]