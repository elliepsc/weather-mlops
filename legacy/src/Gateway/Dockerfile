# Dockerfile pour api_securite
FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8001

# Commande de lancement
CMD ["uvicorn", "api_securite:app", "--host", "0.0.0.0", "--port", "8001"]
