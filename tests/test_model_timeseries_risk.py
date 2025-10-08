import pytest
import joblib
import numpy as np
import tensorflow as tf
from pathlib import Path

class TestTimeseriesRiskLSTM:
    @classmethod
    def setup_class(cls):
        cls.model_dir = Path(__file__).resolve().parents[1] / "artifacts" / "models" / "timeseries_risk"
        cls.model_path = cls.model_dir / "lstm_risk_model.keras"
        cls.scaler_path = cls.model_dir / "scaler.joblib"
        assert cls.model_path.exists(), "LSTM risk model not found. Train and save model_timeseries_risk first."
        assert cls.scaler_path.exists(), "Scaler not found. Train and save model_timeseries_risk first."
        cls.model = tf.keras.models.load_model(cls.model_path)
        cls.scaler = joblib.load(cls.scaler_path)
        # Example window, must have same feature count as training
        cls.demo_window = np.array([
            [2.0, 320, 0, 327.75, 1],
            [3.0, 325, 0.05, 327.75, 2],
            [4.0, 318, 0.02, 327.75, 1],
            [3.0, 300, 0.10, 325.00, 1],
            [2.0, 310, 0, 327.75, 2],
            [4.0, 330, 0.04, 327.75, 1],
            [3.0, 315, 0.03, 327.75, 1],
        ])  # shape: (seq_length, n_features)

    def test_model_loading(self):
        assert self.model is not None
        assert self.scaler is not None

    def test_demo_prediction(self):
        demo_scaled = self.scaler.transform(self.demo_window)
        demo_seq = np.expand_dims(demo_scaled, axis=0)
        pred = self.model.predict(demo_seq)[0][0]
        assert 0.0 <= pred <= 1.0
        print(f"Demo score: {pred:.3f}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
