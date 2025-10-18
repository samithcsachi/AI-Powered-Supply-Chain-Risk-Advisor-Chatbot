import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.utils import class_weight
import joblib
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


base_dir = Path(__file__).resolve().parents[2]
data_path = base_dir / "artifacts" / "data" / "processed" / "supply_chain_disruptions_features.csv"


df = pd.read_csv(data_path)
region_col = "Order City"
region_name = "Shanghai"


df_region = df[df[region_col] == region_name].copy()
if len(df_region) < 100:
    logger.warning("Region sample is small, upsampling/cropping to 200 rows from full dataset.")
    df_region = df.sample(200, random_state=42) if len(df) >= 200 else df

feature_cols = [
    "Days for shipping (real)", "Sales per customer", "Order Item Discount",
    "Order Item Product Price", "Order Item Quantity"
]
label_col = "Late_delivery_risk"
seq_length = 7

X_all = df_region[feature_cols].fillna(0).astype(float).values
y_all = df_region[label_col].fillna(0).astype(int).values

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_all)

X_seq, y_seq = [], []
for i in range(len(X_scaled) - seq_length):
    X_seq.append(X_scaled[i:i+seq_length])
    y_seq.append(y_all[i+seq_length])
X_seq = np.array(X_seq)
y_seq = np.array(y_seq)
logger.info(f"Sequence shape: {X_seq.shape}; Labels: {y_seq.shape}")

if len(X_seq) < 2:
    logger.error("Not enough sequences. Add more data or lower seq_length.")
    exit()


test_size = int(0.2 * len(X_seq))
X_train, X_test = X_seq[:-test_size], X_seq[-test_size:]
y_train, y_test = y_seq[:-test_size], y_seq[-test_size:]


weights = class_weight.compute_class_weight(class_weight="balanced",
                                            classes=np.unique(y_train),
                                            y=y_train)
class_weight_dict = dict(zip(np.unique(y_train), weights))


model = tf.keras.Sequential([
    tf.keras.layers.Input(shape=(seq_length, len(feature_cols))),
    tf.keras.layers.LSTM(64, return_sequences=True),
    tf.keras.layers.Dropout(0.25),
    tf.keras.layers.LSTM(32),
    tf.keras.layers.Dropout(0.25),
    tf.keras.layers.Dense(1, activation="sigmoid")
])
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

logger.info("Training LSTM risk model with weighted loss and dropout.")
model.fit(X_train, y_train, epochs=12, batch_size=8,
          validation_split=0.1, class_weight=class_weight_dict)

test_loss, test_acc = model.evaluate(X_test, y_test)
logger.info(f"Test Accuracy: {test_acc:.4f}")


model_dir = base_dir / "artifacts" / "models" / "timeseries_risk"
model_dir.mkdir(parents=True, exist_ok=True)
model.save(model_dir / "lstm_risk_model.keras")
joblib.dump(scaler, model_dir / "scaler.joblib")
logger.info(f"Saved LSTM model and scaler to {model_dir}")

def predict_risk_for_next_day(sequence, threshold=0.5):
    seq = scaler.transform(sequence)
    seq_window = np.expand_dims(seq, axis=0)
    pred_prob = model.predict(seq_window)[0][0]
    pred_label = int(pred_prob > threshold)
    logger.info(f"Predicted next-day risk score: {pred_prob:.3f} (region: {region_name}), label: {pred_label}")
    return pred_prob, pred_label

if X_test.shape[0] > 0:
    logger.info("Demo prediction for next-day risk using last window of test set:")
    predict_risk_for_next_day(X_test[0], threshold=0.5)
