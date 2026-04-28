FROM python:3.11.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ ./pipeline/
COPY config/   ./config/
COPY api/       ./api/

# Directories populated at runtime via docker-compose volume mounts
RUN mkdir -p /app/data/output /app/models /app/mlflow

ENV MLFLOW_TRACKING_URI=sqlite:///app/mlflow/mlflow.db
ENV API_HOST=0.0.0.0
ENV API_PORT=8003

EXPOSE 8003

CMD ["python", "api/app.py"]
