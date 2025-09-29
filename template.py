import os
from pathlib import Path
import logging

os.makedirs('artifacts/logs', exist_ok=True)


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('artifacts', 'logs', 'logfile.txt')),
        logging.StreamHandler()
    ]
)

project_name = 'AI-Powered-Supply-Chain-Risk-Advisor-Chatbot'


list_of_paths = [
    # src/components/ for key pipeline stages
    "src/components/data_ingestion.py",
    "src/components/data_cleaning.py",
    "src/components/feature_engineering.py",
    "src/components/model_nlp_intent.py",
    "src/components/model_risk_predictor.py",
    "src/components/recommendation_engine.py",
    "src/components/api_gnews_fetcher.py",
    "src/components/api_weather_fetcher.py",

    # src/config, src/constants, src/entity
    "src/config/config.py",
    "src/constants/constants.py",
    "src/entity/event_entity.py",
    "src/entity/prediction_entity.py",

    # src/pipeline/ for orchestration scripts
    "src/pipeline/train_pipeline.py",
    "src/pipeline/predict_pipeline.py",
    "src/pipeline/inference_pipeline.py",

    # src/utils/
    "src/utils/helpers.py",
    "src/utils/logger.py",

    # src/app/ for Chainlit chatbot logic
    "src/app/chatbot.py",
    "src/app/session_manager.py",
    "src/app/response_templates.py",

    # tests/
    "tests/test_data_ingestion.py",
    "tests/test_model_nlp_intent.py",
    "tests/test_model_risk_predictor.py",
    "tests/test_chainlit_app.py",

    # artifacts/
    "artifacts/data/raw/",
    "artifacts/data/processed/",
    "artifacts/models/nlp_intent_entity/",
    "artifacts/models/risk_predictor/",
    "artifacts/logs/",

    # Root files
    "requirements.txt",
    "Dockerfile",
    "README.md",
    ".env"          
]

for path_str in list_of_paths:
    p = Path(path_str)

   
    if p.suffix == "" and "." not in p.name and p.name.lower() not in {
        "dockerfile", "license", "readme", "makefile"
    }:
        os.makedirs(p, exist_ok=True)
        logging.info(f"Created directory: {p}")
    else:
     
        os.makedirs(p.parent, exist_ok=True)
        logging.info(f"Created directory: {p.parent}")
        
        if (not p.exists()) or (p.stat().st_size == 0):
            p.touch()
            logging.info(f"Created empty file: {p}")
        else:
            logging.info(f"File already exists: {p} and is not empty.")
