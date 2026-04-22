# Dockerfile pour Streamlit
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
#RUN pip install altair

COPY . .

EXPOSE 8002

CMD ["streamlit", "run", "app.py", "--server.port", "8002", "--server.address", "0.0.0.0"]

