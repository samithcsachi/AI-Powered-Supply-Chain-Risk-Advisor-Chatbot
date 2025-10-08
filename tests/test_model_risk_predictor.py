import pytest
import joblib
import pandas as pd
import numpy as np
from pathlib import Path

class TestRiskPredictor:
    @classmethod
    def setup_class(cls):
        cls.model_dir = Path(__file__).resolve().parents[1] / "artifacts" / "models" / "risk_predictor"
        cls.model_path = cls.model_dir / "hist_gradient_boosting_risk_predictor.joblib"
        assert cls.model_path.exists(), "Risk model not found. Train and save risk predictor first."
        cls.model = joblib.load(cls.model_path)
        # Load data sample
        data_path = Path(__file__).resolve().parents[1] / "artifacts" / "data" / "processed" / "supply_chain_disruptions_features.csv"
        df = pd.read_csv(data_path)
        # Use same feature selection as training
        exclude_cols = [
            "is_late", "Customer Id", "Order Id", "Order Item Id", "Order Customer Id",
            "Late_delivery_risk", "Late Delivery Risk", "Delivery Status",
            "lead_time_days", "Days for shipping (real)", "Days for shipment (scheduled)"
        ]
        cls.feature_cols = [c for c in df.columns if c not in exclude_cols and df[c].dtype in [np.float64, np.int64, np.bool_, np.int32]]
        cls.df = df

    def test_model_loading(self):
        assert self.model is not None

    def test_prediction_output(self):
        # Use first batch of 5 rows
        X = self.df[self.feature_cols].fillna(0).astype(float).head(5)
        y_pred = self.model.predict(X)
        assert y_pred.shape == (5,)
        # Predictions should be binary
        assert set(np.unique(y_pred)).issubset({0, 1})

    def test_predict_proba(self):
        X = self.df[self.feature_cols].fillna(0).astype(float).head(5)
        y_proba = self.model.predict_proba(X)
        assert y_proba.shape == (5, 2)
        # Each row should sum to 1.0
        for row in y_proba:
            assert np.isclose(row.sum(), 1.0)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
