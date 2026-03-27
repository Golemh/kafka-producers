FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY stress_test.py .

ENTRYPOINT ["python", "stress_test.py"]
