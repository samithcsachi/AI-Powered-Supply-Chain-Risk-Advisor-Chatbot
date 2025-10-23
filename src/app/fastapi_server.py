from fastapi import FastAPI, Query
from typing import Optional, List

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from components.model_nlp_intent import predict_intent
from components.model_nlp_ner import extract_entities_pipeline
from components.model_risk_predictor import predict_risk
from components.recommendation_engine import generate_recommendation

app = FastAPI(
    title="Supply Chain Risk Advisor API",
    description="Provides risk prediction, event queries, and mitigation recommendations.",
    version="1.0"
)

@app.get("/health/")
def health():
    return {"status": "ok"}

@app.get("/nlp/")
def nlp_analysis(query: str):
    """Run both intent and entity extraction on a user query."""
    intent_result = predict_intent(query)
    entities = extract_entities_pipeline(query)
    return {
        "query": query,
        "intent": intent_result["intent"],
        "confidence": intent_result["confidence"],
        "entities": entities
    }

@app.get("/predict-risk/")
def predict_risk_api(region: str, days: Optional[int] = 5):
    """Return risk prediction for a region next N days."""
    risk_score = predict_risk(region, days)
    return {"region": region, "risk_score": risk_score, "days": days}

@app.get("/events/")
def events_api(region: Optional[str] = None):
    """Query past incidents/events for a region or all regions."""
    # Replace this with real event loading (e.g., from your snapshot/data files)
    sample_events = [
        {"region": "Germany", "event": "railway strike", "date": "2025-09-23"},
        {"region": "Mumbai", "event": "weather alert", "date": "2025-10-05"},
        {"region": "Shanghai", "event": "typhoon", "date": "2025-09-30"},
    ]
    if region:
        filtered = [ev for ev in sample_events if ev["region"].lower() == region.lower()]
        return {"events": filtered}
    return {"events": sample_events}

@app.get("/recommendation/")
def recommendation_api(
    region: str,
    risk: float,
    intent: Optional[str] = None,
    recent_incidents: Optional[List[str]] = Query(None),
    weather_alert: Optional[str] = None
):
    """Get mitigation recommendation for region and risk."""
    advice = generate_recommendation(
        risk_score=risk,
        region=region,
        recent_incidents=recent_incidents,
        weather_alert=weather_alert,
        intent=intent
    )
    return advice

@app.get("/bot/")
def chatbot_api(query: str):
    """Full pipeline: intent, entities, risk prediction and recommendation."""
    intent_result = predict_intent(query)
    entities = extract_entities_pipeline(query)
    # Use the first location found or default to Mumbai for demo if missing
    region = None
    if entities.get("location"):
        region = entities["location"][0] if isinstance(entities["location"], list) and entities["location"] else entities["location"]
    if not region:
        region = "Mumbai"
    risk_score = predict_risk(region, 5)
    recent_incidents = ["port strike", "supplier outage"] if region else []
    weather_alert = "Typhoon warning" if region == "Shanghai" else None
    advice = generate_recommendation(
        risk_score=risk_score,
        region=region,
        recent_incidents=recent_incidents,
        weather_alert=weather_alert,
        intent=intent_result.get("intent")
    )
    return {
        "query": query,
        "intent": intent_result["intent"],
        "confidence": intent_result["confidence"],
        "entities": entities,
        "region": region,
        "risk_score": risk_score,
        "advice": advice
    }


