import os
import yaml
import pandas as pd
import json
import mlflow
import mlflow.xgboost
import xgboost as xgb

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from mlflow.tracking import MlflowClient  # <-- pour le Model Registry

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



def load_config(config_path="config/config.yaml"):
    """
    Cette fonction permet de charger la configuration (chemins, hyperparamètres, etc.)
    depuis un fichier YAML, que je vais utiliser ensuite dans tout le script.
    """
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def train_and_evaluate(X_train, y_train, X_test, y_test, config):
    """
    Je définis ici la logique d'entraînement et d'évaluation du modèle XGBoost,
    tout en loggant les paramètres et les métriques dans MLflow, et en sauvegardant
    localement le modèle et un fichier de métriques au format JSON.
    """
    # 1) Configuration MLflow : on définit l'URI et le nom de l'expérience
    mlflow.set_tracking_uri(config["mlflow"]["tracking_uri"])
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    # 2) Je vérifie si l'expérience existe, sinon je la crée
    experiment_name = config["mlflow"]["experiment_name"]
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(experiment_name)
        print(f"Expérience créée : {experiment_name}")

    # 3) On démarre un nouveau run MLflow
    with mlflow.start_run() as run:
        # a) Je récupère les hyperparamètres XGBoost depuis la config
        xgb_params = config["model"]["xgboost"]

        # b) J'instancie et entraîne le XGBClassifier
        model = xgb.XGBClassifier(**xgb_params)
        model.fit(X_train, y_train)

        # c) Je fais la prédiction
        y_pred = model.predict(X_test)

        # d) Je calcule les métriques de classification
        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, zero_division=0),
            "recall": recall_score(y_test, y_pred, zero_division=0),
            "f1_score": f1_score(y_test, y_pred, zero_division=0),
        }

        # e) Je loggue les hyperparams et les métriques dans MLflow
        mlflow.log_params(xgb_params)
        mlflow.log_metrics(metrics)

        # f) J'enregistre les métriques dans un fichier JSON (ex: metrics/metrics.json)
        metrics_path = config["output"]["metrics_path"]
        os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=4)

        # g) Je sauvegarde le modèle localement (en format XGBoost .json)
        model_path = config["output"]["model_path"]
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        model.save_model(model_path)

        # h) Je loggue le modèle XGBoost dans MLflow (pour le retrouver via l’UI MLflow)
        artifact_path = "xgboost_model"
        mlflow.xgboost.log_model(
            xgb_model=model,
            artifact_path=artifact_path,
            input_example=X_test.iloc[:1]
        )

        # (Nouveau) Enregistrement dans la Model Registry sous le nom "model" + alias "model_last"
        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/{artifact_path}"

        # 1) On enregistre le modèle dans le registry
        registered_model = mlflow.register_model(
            model_uri=model_uri,
            name="model"  # Sergio souhaitait le nom "model"
        )

        # 2) On définit l’alias "model_last" sur cette nouvelle version
        client = MlflowClient()
        client.set_registered_model_alias(
            name="model",
            alias="model_last",
            version=registered_model.version
        )

        print(f"Métriques enregistrées dans MLflow: {metrics}")
        print(f"Modèle sauvegardé à l'emplacement : {model_path}")
        print(f"Registered model: 'model' (version {registered_model.version}) avec alias 'model_last'")

def main():
    """
    Fonction principale qui lit la config, charge les données prétraitées,
    lance l'entraînement XGBoost et enregistre le tout.
    """
    # 1) Je charge la config YAML
    config = load_config()

    # 2) Je récupère le répertoire où se trouvent X_train.csv, X_test.csv, y_train.csv, y_test.csv
    processed_dir = config["data"]["processed_dir"]

    # 3) Je charge ces fichiers CSV en DataFrames
    X_train = pd.read_csv(os.path.join(processed_dir, "X_train.csv"))
    X_test = pd.read_csv(os.path.join(processed_dir, "X_test.csv"))
    y_train = pd.read_csv(os.path.join(processed_dir, "y_train.csv")).squeeze()
    y_test = pd.read_csv(os.path.join(processed_dir, "y_test.csv")).squeeze()

    # 4) J'entraîne et évalue le modèle
    train_and_evaluate(X_train, y_train, X_test, y_test, config)

if __name__ == "__main__":
    main()
