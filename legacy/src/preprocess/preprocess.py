import pandas as pd
import yaml
from sklearn.model_selection import train_test_split
import os


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



def load_config(yaml_path):
    with open(yaml_path, "r") as file:
        return yaml.safe_load(file)


def remove_nan_target(df, target_column):
    cleaned_data = df.dropna(subset=[target_column]).copy()
    print(f"Lignes supprimées (NaN dans cible) : {len(df) - len(cleaned_data)}")
    return cleaned_data


def preprocess_data(df, target_column):
    df = remove_nan_target(df, target_column)
    numerical_columns = df.select_dtypes(include=["float", "int"]).columns
    df[numerical_columns] = df[numerical_columns].fillna(df[numerical_columns].mean())

    # Gestion des NaN dans les colonnes catégoriques
    categorical_columns = df.select_dtypes(include=["object"]).columns
    df[categorical_columns] = df[categorical_columns].fillna("Inconnu")

    # Encodage des colonnes catégoriques
    for col in categorical_columns:
        df[col] = df[col].astype("category").cat.codes.astype("int64")

    # Conversion de la colonne Date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["Date"] = df["Date"].apply(lambda x: x.toordinal() if pd.notnull(x) else pd.NA)
        df["Date"] = df["Date"].astype("Int64")

    return df


def save_processed_data(X_train, X_test, y_train, y_test, config):
    processed_dir = config["data"]["processed_dir"]
    print(f"Processed dir: {processed_dir}")
    os.makedirs(processed_dir, exist_ok=True)

    # Sauvegarder les fichiers
    X_train.to_csv(os.path.join(processed_dir, "X_train.csv"), index=False)
    X_test.to_csv(os.path.join(processed_dir, "X_test.csv"), index=False)
    y_train.to_csv(os.path.join(processed_dir, "y_train.csv"), index=False)
    y_test.to_csv(os.path.join(processed_dir, "y_test.csv"), index=False)
    print(f"Files saved in {processed_dir}")


if __name__ == "__main__":
    # Charger la configuration
    config = load_config("/app/config/config.yaml")

    # Charger les données brutes
    raw_data_path = config["data"]["raw_data_path"]
    target_column = config["model"]["target_column"]
    df = pd.read_csv(raw_data_path)

    # Prétraiter les données
    df = preprocess_data(df, target_column)

    # Séparer les caractéristiques (X) et la cible (y)
    X = df.drop(columns=[target_column]).copy()
    y = df[target_column].copy()

    # Diviser en ensembles d'entraînement et de test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config["model"]["test_size"],
        random_state=config["model"]["random_state"]
    )

    # Sauvegarder les ensembles traités
    print("Calling save_processed_data...")
    save_processed_data(X_train, X_test, y_train, y_test, config)
    print("save_processed_data executed")
