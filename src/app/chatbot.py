import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chainlit as cl

from components.model_nlp_intent import predict_intent
from components.model_nlp_ner import extract_entities
from components.model_risk_predictor import predict_risk
from components.recommendation_engine import generate_recommendation

# Optionally add: from app.session_manager import ... if you need session/context tracking

@cl.on_message
async def handle_message(msg: cl.Message):
    # --- Get user query ---
    query = msg.content
    session = cl.user_session  # Chainlit session/state object

    # --- Intent Detection ---
    intent_result = predict_intent(query)
    intent = intent_result["intent"]
    confidence = intent_result["confidence"]

    # --- NER Extraction ---
    entities = extract_entities(query)

    # Choose region for risk/recommendation
    region = None
    if entities.get("location"):
        region = entities["location"][0] if isinstance(entities["location"], list) and entities["location"] else entities["location"]
    if not region:
        region = "Mumbai"

    # --- Risk Prediction ---
    risk_score = predict_risk(region, days=5)

    # --- Recommendation Advice ---
    recent_incidents = ["port strike", "supplier outage"] if region else []
    weather_alert = "Typhoon warning" if region == "Shanghai" else None
    advice = generate_recommendation(
        risk_score=risk_score,
        region=region,
        recent_incidents=recent_incidents,
        weather_alert=weather_alert,
        intent=intent
    )

    # --- Compose and stream chatbot response ---
    response = (
        f"*Region:* **{region}**\n"
        f"*Intent:* **{intent}** (Conf: {confidence:.2f})\n"
        f"*Entities:* {entities}\n"
        f"*Risk Score:* {risk_score:.2f}\n\n"
        f"**Recommendation:**\n{advice['message']}\n"
    )

    # Stream response to user
    await cl.Message(
        content=response,
        author="Risk Advisor Bot"
    ).send()

    # Optionally show structured alert/cards
    await cl.Message(
        content=f"Alert level: **{advice['action']}**",
        author="Risk Advisor Bot"
    ).send()

  
@cl.on_chat_start
async def welcome():
    await cl.Message(
        content="Welcome to the AI-powered Supply Chain Risk Advisor Chatbot!\nType your logistics question or region to get real-time risk analysis and advice.",
        author="Risk Advisor Bot"
    ).send()

