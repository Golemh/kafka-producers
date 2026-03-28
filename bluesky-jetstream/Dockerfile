FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better caching
COPY bluesky-jetstream/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared core framework
COPY core/ ./core/

# Copy application code
COPY bluesky-jetstream/src/ ./

# Run the producer
CMD ["python", "producer.py"]
