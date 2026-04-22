import logging
import os

# Définition du chemin du dossier logs principal
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../logs"))
os.makedirs(LOG_DIR, exist_ok=True)  # Création du dossier logs s'il n'existe pas

# Chemin du fichier log spécifique à ingest
LOG_FILE = os.path.join(LOG_DIR, "preprocess.log")

# Création du logger
logger = logging.getLogger("preprocess")
logger.setLevel(logging.INFO)

# Vérifier si un handler existe déjà (évite la duplication des logs)
if not logger.hasHandlers():
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

logger.info("Logger configuré avec succès pour preprocess !")

