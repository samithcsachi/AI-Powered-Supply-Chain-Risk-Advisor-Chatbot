import pytest
from pathlib import Path
import sys

# Add src/components to sys.path for import
sys.path.append(str(Path(__file__).resolve().parents[1] / "src" / "components"))
from recommendation_engine import generate_recommendation

class TestRecommendationEngine:
    def test_high_risk_reply(self):
        result = generate_recommendation(
            risk_score=0.9,
            region="Shanghai",
            recent_incidents=['port strike', 'supplier outage', 'heavy rain'],
            weather_alert='Typhoon warning',
            intent="mitigation_help"
        )
        assert "High risk" in result["message"]
        assert result["action"].startswith("reroute")

    def test_medium_risk_reply(self):
        result = generate_recommendation(
            risk_score=0.55,
            region="Delhi",
            recent_incidents=['route accident', 'moderate rain'],
            weather_alert=None
        )
        assert "Moderate risk" in result["message"]
        assert "continue" in result["action"]

    def test_low_risk_reply(self):
        result = generate_recommendation(
            risk_score=0.1,
            region="Mumbai"
        )
        assert "Risk is low" in result["message"]
        assert result["action"] == "proceed"

    def test_weather_integration(self):
        result = generate_recommendation(
            risk_score=0.65,
            region="Manila",
            weather_alert="Flood risk"
        )
        assert "Weather Alert: Flood risk" in result["message"]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
