from fastapi import FastAPI, HTTPException, Request
import mlflow
import pandas as pd
from input_classes import InputData, ModelData
from datetime import datetime
import os
import time

# Import du module pour Prometheus
from prometheus_fastapi_instrumentator import Instrumentator


# Import du logger
from logger import logger
import traceback

def process_data():
    logger.info("Début du traitement des données...")
    try:
        # Simuler une erreur
        raise ValueError("Erreur de test")
    except Exception as e:
        logger.error(f"Erreur rencontrée : {e}")
        logger.debug(traceback.format_exc())

if __name__ == "__main__":
    process_data()



# Création de l'API FastAPI
api = FastAPI(title="Model API")

# Initialisation de Prometheus Instrumentator
instrumentator = Instrumentator()
instrumentator.instrument(api)
# Ceci va automatiquement ajouter l'endpoint /metrics à votre API
instrumentator.expose(api)

# Configuration de MLflow
mlflow.set_tracking_uri("http://mlflow:8100")

print("Loading model...")
# Chargement du modèle depuis le model registry de MLflow
model = mlflow.xgboost.load_model("models:/model@model_last", dst_path="mlflow/")

# Endpoint pour vérifier si l'API est en ligne
@api.get('/check')
def verify_status():
    return {"message": "API is online."}

# Endpoint pour la prédiction
@api.post('/predict')
async def predict(input_data: InputData, request: Request):
    global model
    start_time = time.time()  # Début de la mesure du temps de réponse
    try:
        # Convertir l'entrée en DataFrame
        X = pd.DataFrame.from_dict([input_data.dict()])
        
        # Faire la prédiction
        prediction = model.predict(X)
        
        # Log des requêtes en production (CSV)
        X['prediction'] = prediction
        X['timestamp'] = datetime.now().isoformat()

        # Sauvegarde des logs de requêtes
        log_file = "data/production_logs/requests_log.csv"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        write_header = not os.path.exists(log_file)
        X.to_csv(log_file, mode='a', header=write_header, index=False)
        
        # La durée d'exécution est mesurée, et sera éventuellement visible via /metrics
        duration = time.time() - start_time
        
        return {"prediction": prediction.tolist(), "duration": duration}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint pour la prédiction avec probabilités
@api.post('/predict_proba')
async def predict_proba(input_data: InputData):
    global model
    try:
        X = pd.DataFrame.from_dict([input_data.dict()])
        prediction = model.predict_proba(X)
        return {"prediction": prediction[:, 1].tolist()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint pour recharger le modèle
@api.post('/reload')
async def reload_model(model_data: ModelData):
    global model

    if model_data.version and model_data.alias:
        raise HTTPException(status_code=400, detail="Only one of version or alias can be provided.")

    if not model_data.version and not model_data.alias:
        print("No version or alias provided. Using last version.")
        model_data.alias = "model_last"
    
    print("Loading model...")
    try:
        if model_data.alias:
            model = mlflow.xgboost.load_model(
                f"models:/{model_data.model_name}@{model_data.alias}",
                dst_path="mlflow/"
            )
        else:
            model = mlflow.xgboost.load_model(
                f"models:/{model_data.model_name}/{model_data.version}",
                dst_path="mlflow/"
            )
        return {"message": f"Model '{model_data}' reloaded."}
    except mlflow.exceptions.RestException as e:
        raise HTTPException(status_code=404, detail=f"Model '{model_data}' not found. {str(e)}")

# Lancement du serveur avec uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app_model:api", host="0.0.0.0", port=8000, reload=True)
