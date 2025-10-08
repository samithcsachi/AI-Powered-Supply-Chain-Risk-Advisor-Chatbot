import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

data_path = Path(__file__).resolve().parents[2] / "artifacts" / "data" / "processed" / "supply_chain_disruptions_features.csv"
df = pd.read_csv(data_path)

# --- For demo, pick only one city/region ---
region_col = "Order City"
region_name = "Shanghai"   # Change to cities/regions as needed!
df_region = df[df[region_col] == region_name].copy()
if len(df_region) < 20:
    df_region = df.head(50)  # Fallback: use first 50 for a demo

# --- Select features & label ---
feature_cols = ["Days for shipping (real)", "Sales per customer", "Order Item Discount", "Order Item Product Price", "Order Item Quantity"]
label_col = "Late_delivery_risk"
seq_length = 7  # 1 week window

# --- Clean numeric features ---
X_all = df_region[feature_cols].fillna(0).astype(float).values
y_all = df_region[label_col].fillna(0).astype(int).values

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_all)

X_seq = []
y_seq = []
for i in range(len(X_scaled) - seq_length):
    X_seq.append(X_scaled[i:i+seq_length])
    y_seq.append(y_all[i+seq_length])

X_seq = np.array(X_seq)
y_seq = np.array(y_seq)

logger.info(f"Sequence shape: {X_seq.shape}; Labels: {y_seq.shape}")

if len(X_seq) < 2:
    logger.error("Not enough sequences. Add more data or lower seq_length.")
    exit()

X_train, X_test, y_train, y_test = train_test_split(X_seq, y_seq, test_size=0.2, random_state=42, stratify=y_seq)

model = tf.keras.Sequential([
    tf.keras.layers.Input(shape=(seq_length, len(feature_cols))),
    tf.keras.layers.LSTM(32, return_sequences=True),
    tf.keras.layers.LSTM(16),
    tf.keras.layers.Dense(1, activation="sigmoid")
])
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

logger.info("Training LSTM risk model.")
model.fit(X_train, y_train, epochs=8, batch_size=8, validation_split=0.1)

test_loss, test_acc = model.evaluate(X_test, y_test)
logger.info(f"Test Accuracy: {test_acc:.4f}")

model_dir = Path(__file__).resolve().parents[2] / "artifacts" / "models" / "timeseries_risk"
model_dir.mkdir(parents=True, exist_ok=True)
model.save(model_dir / "lstm_risk_model.keras")
joblib.dump(scaler, model_dir / "scaler.joblib")
logger.info(f"Saved LSTM model and scaler to {model_dir}")

def predict_risk_for_next_day(sequence):
    seq = scaler.transform(sequence)
    seq_window = np.expand_dims(seq, axis=0)
    pred = model.predict(seq_window)[0][0]
    logger.info(f"Predicted next-day risk score: {pred:.3f} (region: {region_name})")
    return pred

if X_test.shape[0] > 0:
    logger.info("Demo prediction for next-day risk using last window of test set:")
    predict_risk_for_next_day(X_test[0])
