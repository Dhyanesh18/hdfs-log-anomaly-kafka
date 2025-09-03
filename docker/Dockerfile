FROM python:3.10-slim

# Install system deps (only essentials)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ./services /app/services
COPY ./utils /app/utils
COPY ./models /app/models
COPY ./preprocessed /app/preprocessed

ENV PYTHONPATH=/app
