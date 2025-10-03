import pandas as pd
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier 
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, RocCurveDisplay

import joblib
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

def main():
    processed_dir = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "processed"
    model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "risk_predictor"
    model_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(processed_dir / "supply_chain_disruptions_features.csv")


    target = "is_late"
    if target not in df.columns:
        logger.error(f"Target column {target} not found.")
        return
 
    feature_cols = [
        c for c in df.columns if c != target and df[c].dtype in [np.float64, np.int64, np.bool_, np.int32]
    ]
    X = df[feature_cols]
    y = df[target].astype(int) 

   
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    logger.info(f"Training data shape: {X_train.shape}, Test data shape: {X_test.shape}")

 
    model = RandomForestClassifier(
        n_estimators=100,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

 
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    report = classification_report(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_proba)
    logger.info("Classification Report:\n" + report)
    logger.info(f"Confusion Matrix:\n{cm}")
    logger.info(f"ROC-AUC: {roc_auc}")

  

    # --- Save Model ---
    model_path = model_dir / "random_forest_risk_predictor.joblib"
    joblib.dump(model, model_path)
    logger.info(f"Model saved to {model_path}")

if __name__ == "__main__":
    main()
