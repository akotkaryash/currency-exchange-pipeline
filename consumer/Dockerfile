FROM python:3.11-slim

RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY load_to_bigquery.py .
COPY consumer.py .

CMD ["python", "consumer.py"]