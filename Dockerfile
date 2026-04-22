FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ ./pipeline/
COPY api/       ./api/
COPY data/      ./data/
COPY models/    ./models/
COPY mlflow/    ./mlflow/

ENV MLFLOW_TRACKING_URI=sqlite:///app/mlflow/mlflow.db
ENV API_HOST=0.0.0.0
ENV API_PORT=8080

EXPOSE 8080

CMD ["python", "api/app.py"]
