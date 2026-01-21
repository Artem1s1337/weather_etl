FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    libpq-dev \
    libxml2-dev libxslt-dev python3-lxml \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/extract /app/src/extract
COPY scripts /app/scripts

RUN chmod +x /app/scripts/wait-for-postgres.sh

CMD ["python", "/app/src/extract/extract.py"]
