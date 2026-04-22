FROM python:3.11.8

# Créer les dossiers nécessaires
RUN mkdir -p /app/config /app/data

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires
COPY . /app

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

RUN dvc init --no-scm && \
    dvc remote add origin https://dagshub.com/bmle-aug24/Meteo_group.dvc && \
    dvc remote modify origin auth basic && \
    dvc remote modify origin user bmle-aug24 && \
    dvc remote modify origin password 80f0fd7d1ab6a2b1e95664936d78045d71c78e17 && \
    dvc pull # Récupération des fichiers versionnés

CMD ["sh", "-c", "dvc pull && python train.py && dvc add /app/mlflow/latest_model.pkl && dvc push"]