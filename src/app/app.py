import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chainlit as cl

from components.model_nlp_intent import predict_intent
from components.model_nlp_ner import extract_entities_pipeline
from components.model_risk_predictor import predict_risk
from components.recommendation_engine import generate_recommendation


@cl.on_message
async def handle_message(msg: cl.Message):
    query = msg.content
    session = cl.user_session

    
    intent_result = predict_intent(query)
    intent = intent_result["intent"]
    confidence = intent_result["confidence"]

  
    entities = extract_entities_pipeline(query)

   
    region = None
    origin = None
    destination = None
    
    if entities.get("location"):
        locations = entities["location"]
        if isinstance(locations, list) and len(locations) > 0:
            region = locations[0]
            
            if len(locations) > 1:
                origin = locations[0]
                destination = locations[1]
        else:
            region = locations
    
    if not region:
        region = "Mumbai"

    
    incidents = []
    event_type = None
    
    if entities.get("event"):
        events = entities["event"]
        if isinstance(events, list):
            incidents = events
            event_type = events[0] if events else None
        else:
            incidents = [events]
            event_type = events
    
   
    risk_score = predict_risk(
        region=region,
        days=5,
        origin=origin,              
        destination=destination,
        event_type=event_type,
        incidents=incidents
    )

  
    recent_incidents = incidents if incidents else ["port strike", "supplier outage"]
    weather_alert = "Typhoon warning" if region == "Shanghai" else None
    
    advice = generate_recommendation(
        risk_score=risk_score,
        region=region,
        recent_incidents=recent_incidents,
        weather_alert=weather_alert,
        intent=intent
    )


    if risk_score >= 0.7:
        risk_emoji = "🔴"
        risk_level = "High"
    elif risk_score >= 0.4:
        risk_emoji = "🟡"
        risk_level = "Medium"
    else:
        risk_emoji = "🟢"
        risk_level = "Low"

    
    response = (
        f"### 📊 Supply Chain Risk Analysis\n\n"
        f"**Region:** {region}\n"
        f"**Intent:** {intent} (Confidence: {confidence:.2%})\n"
        f"**Entities:** {entities}\n"
    )
    
 
    if origin and destination:
        response += f"**Route:** {origin} → {destination}\n"
    
   
    if incidents:
        response += f"**⚠️ Detected Events:** {', '.join(incidents)}\n"
    
    response += f"**Risk Score:** {risk_emoji} **{risk_level}** ({risk_score:.2f})\n\n"
    response += f"**💡 Recommendation:**\n{advice['message']}\n"

    await cl.Message(
        content=response,
        author="Supply Chain Risk Analysis"
    ).send()

    # Send Alert Level
    alert_emoji = "🚨" if risk_score >= 0.7 else "⚠️" if risk_score >= 0.4 else "✅"
    await cl.Message(
        content=f"{alert_emoji} **Alert Level:** {advice['action'].upper()}",
        author="Alert Level"
    ).send()


@cl.on_chat_start
async def welcome():
    await cl.Message(
        content=(
            "# 🌐 Welcome to AI-Powered Supply Chain Risk Advisor\n\n"
            "I provide **real-time risk analysis** and **mitigation strategies** "
            "based on:\n"
            "- 🌍 **Regional factors** (port congestion, infrastructure)\n"
            "- ⚠️ **Active events** (strikes, typhoons, disruptions)\n"
            "- 🚢 **Route analysis** (origin to destination)\n"
            "- 🤖 **ML-powered predictions** (trained on historical data)\n\n"
            "### 💬 Example Questions:\n\n"
            "- \"Is there any delay in vessels from USA to UAE?\"\n"
            "- \"What should I do about the port strike in Shanghai?\"\n"
            "- \"Are there weather problems affecting shipments to Germany?\"\n"
            "- \"Risk level for Mumbai to Singapore route?\"\n\n"
            "**Ask me anything about your supply chain risks!** 🚀"
        ),
        author="Risk Advisor Bot"
    ).send()