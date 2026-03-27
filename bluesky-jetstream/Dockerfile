FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared core framework
COPY ../../core/ ./core/

# Copy application code
COPY src/ ./

# Run the producer
CMD ["python", "producer.py"]
