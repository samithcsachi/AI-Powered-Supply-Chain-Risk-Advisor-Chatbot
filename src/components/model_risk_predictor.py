import pandas as pd
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.inspection import permutation_importance
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

    exclude_cols = [
        target, "Customer Id", "Order Id", "Order Item Id", "Order Customer Id",
        "Late_delivery_risk", "Late Delivery Risk", "Delivery Status",
        "lead_time_days", "Days for shipping (real)", "Days for shipment (scheduled)"
    ]
    feature_cols = [
        c for c in df.columns 
        if c not in exclude_cols and df[c].dtype in [np.float64, np.int64, np.bool_, np.int32]
    ]

    X = df[feature_cols]
    y = df[target].astype(int) 
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    logger.info(f"Training data shape: {X_train.shape}, Test data shape: {X_test.shape}")

    model = HistGradientBoostingClassifier(
        max_iter=100, learning_rate=1.0, max_depth=1, random_state=42
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

    result = permutation_importance(model, X_test, y_test, n_repeats=10, random_state=42, n_jobs=-1)
    importances = result.importances_mean
    feature_importance = pd.DataFrame({'feature': X_test.columns, 'importance': importances}).sort_values('importance', ascending=False)
    logger.info("Top 10 Most Important Features (Permutation Importance):")
    logger.info(feature_importance.head(10).to_string())
    max_importance = feature_importance['importance'].max()
    if max_importance > 0.8:
        logger.warning(f"Potential data leakage: One feature has {max_importance:.3f} importance")

    model_path = model_dir / "hist_gradient_boosting_risk_predictor.joblib"
    joblib.dump(model, model_path)
    logger.info(f"Model saved to {model_path}")


def build_feature_row(feature_cols, query_dict, reference_row=None):
    
    if reference_row is None:
        reference_row = pd.Series({col: 0 for col in feature_cols})

    row = reference_row.copy()
    
    
    shipping_mode = query_dict.get('shipping_mode', 'Standard Class')
    for col in feature_cols:
        if 'Shipping_Mode' in col and shipping_mode in col:
            row[col] = 1
            logger.debug(f"Set shipping mode: {col} = 1")
    
   
    region = query_dict.get('region', '')
    for col in feature_cols:
        if 'Order_Country' in col or 'Order_Region' in col:
            if region in col:
                row[col] = 1
    
    
    for col in feature_cols:
        if 'Order_Status_COMPLETE' in col:
            row[col] = 1
    
    return row



REGION_BASE_RISKS = {
    "Shanghai": 0.55,
    "Singapore": 0.30,
    "Mumbai": 0.45,
    "Dubai": 0.35,
    "UAE": 0.35,
    "USA": 0.30,
    "Germany": 0.25,
    "China": 0.55,
    "India": 0.45,
    "Hong Kong": 0.50,
    "Rotterdam": 0.28,
    "Los Angeles": 0.40,
}


EVENT_RISK_MULTIPLIERS = {
    "strike": 0.30,
    "port strike": 0.35,
    "typhoon": 0.35,
    "hurricane": 0.35,
    "earthquake": 0.40,
    "flood": 0.25,
    "port closure": 0.45,
    "supplier outage": 0.25,
    "customs delay": 0.15,
    "congestion": 0.20,
    "pandemic": 0.30,
    "war": 0.50,
    "sanctions": 0.40,
}


def calculate_rule_based_risk(region, days, incidents):
    
    base_risk = REGION_BASE_RISKS.get(region, 0.40)
    

    event_risk = 0.0
    if incidents:
        for incident in incidents:
            incident_lower = str(incident).lower()
            for event_keyword, multiplier in EVENT_RISK_MULTIPLIERS.items():
                if event_keyword in incident_lower:
                    event_risk += multiplier
                    logger.debug(f"Event '{event_keyword}' detected in '{incident}', adding {multiplier}")
    
   
    time_factor = max(0.1, 1.0 - (days / 30.0))
    
  
    rule_risk = (base_risk * 0.5 + event_risk * 0.4 + time_factor * 0.1)
    
    return min(1.0, rule_risk)


def predict_risk(region: str, days: int = 5, origin=None, destination=None, 
                 event_type=None, incidents=None, shipping_mode=None):

    try:
        import joblib
        import pandas as pd
        from pathlib import Path

        model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "risk_predictor"
        model_path = model_dir / "hist_gradient_boosting_risk_predictor.joblib"
        
        
        if shipping_mode is None:
            shipping_mode = "Standard Class"
        
      
        rule_risk = calculate_rule_based_risk(region, days, incidents or [])
        logger.info(f"Rule-based risk for {region}: {rule_risk:.3f}")
        
  
        ml_risk = 0.40  
        
        if model_path.exists():
            try:
                model = joblib.load(model_path)
                logger.debug(f"Loaded ML model from {model_path}")

                data_dir = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "processed"
                feature_csv_path = data_dir / "supply_chain_disruptions_features.csv"
                
                if feature_csv_path.exists():
                    feature_csv = pd.read_csv(feature_csv_path)
                    feature_cols = list(model.feature_names_in_) if hasattr(model, "feature_names_in_") else list(feature_csv.columns)

                    reference_row = feature_csv[feature_cols].median()

                    query_dict = {
                        "region": region,
                        "days": days,
                        "origin": origin,
                        "destination": destination,
                        "shipping_mode": shipping_mode,
                    }
                    
                    test_features = pd.DataFrame([build_feature_row(feature_cols, query_dict, reference_row)])
                    ml_risk = float(model.predict_proba(test_features)[0, 1])
                    logger.info(f"ML model risk for {region}: {ml_risk:.3f}")
            except Exception as e:
                logger.warning(f"Could not get ML prediction: {e}")
        
        
        if incidents and len(incidents) > 0:
           
            final_risk = (ml_risk * 0.40) + (rule_risk * 0.60)
            logger.info(f"Hybrid risk (with incidents): ML={ml_risk:.3f}*0.4 + Rule={rule_risk:.3f}*0.6 = {final_risk:.3f}")
        else:
           
            final_risk = (ml_risk * 0.70) + (rule_risk * 0.30)
            logger.info(f"Hybrid risk (no incidents): ML={ml_risk:.3f}*0.7 + Rule={rule_risk:.3f}*0.3 = {final_risk:.3f}")
        
      
        final_risk = float(np.clip(final_risk, 0.0, 1.0))
        
        return round(final_risk, 2)
    
    except Exception as e:
        logger.error(f"Error in predict_risk: {e}", exc_info=True)
        return 0.50


if __name__ == "__main__":
    main()
    

    print("\n" + "="*60)
    print("Testing HYBRID Risk Predictions (ML + Rules)")
    print("="*60)
    

    print("\n1. UAE with no events:")
    risk1 = predict_risk("UAE", days=5, incidents=[])
    print(f"   → Risk Score: {risk1:.2f}")
    
 
    print("\n2. Shanghai with port strike:")
    risk2 = predict_risk("Shanghai", days=5, incidents=["port strike"])
    print(f"   → Risk Score: {risk2:.2f}")
    print(f"   → Increase: +{(risk2-risk1)*100:.1f}%")
    
  
    print("\n3. Mumbai with typhoon and port congestion:")
    risk3 = predict_risk("Mumbai", days=3, incidents=["typhoon", "port congestion"])
    print(f"   → Risk Score: {risk3:.2f}")
    print(f"   → Increase: +{(risk3-risk1)*100:.1f}%")
    

    print("\n4. USA to Singapore route (no events):")
    risk4 = predict_risk("Singapore", days=7, origin="USA", destination="Singapore", incidents=[])
    print(f"   → Risk Score: {risk4:.2f}")
    

    print("\n5. USA to Singapore with equipment failure:")
    risk5 = predict_risk("Singapore", days=7, origin="USA", destination="Singapore", 
                        incidents=["equipment failure", "customs delay"])
    print(f"   → Risk Score: {risk5:.2f}")
    print(f"   → Increase: +{(risk5-risk4)*100:.1f}%")
    

    print("\n6. Shanghai with multiple critical events:")
    risk6 = predict_risk("Shanghai", days=2, incidents=["typhoon", "port strike", "port closure"])
    print(f"   → Risk Score: {risk6:.2f} ")
    
    print("\n" + "="*60)
    print("Hybrid approach combines:")
    print("   - ML Model: Historical shipping patterns")
    print("   - Rules: Real-time events and regional factors")
    print("="*60)