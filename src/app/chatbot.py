import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import chainlit as cl
import logging

from components.model_nlp_intent import predict_intent
from components.model_nlp_ner import extract_entities_pipeline
from components.model_risk_predictor import predict_risk
from components.recommendation_engine import generate_recommendation


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@cl.on_message
async def handle_message(msg: cl.Message):
    
    query = msg.content
    session = cl.user_session
    
    logger.info(f"Processing query: {query}")
    
    try:
       
        try:
            intent_result = predict_intent(query)
            intent = intent_result["intent"]
            confidence = intent_result["confidence"]
            logger.info(f"Intent: {intent}, Confidence: {confidence:.2f}")
        except Exception as e:
            logger.error(f"Intent prediction failed: {e}")
            intent = "unknown"
            confidence = 0.0
            await cl.Message(
                content="Note: Intent classification encountered an issue. Proceeding with default analysis.",
                author="Risk Advisor Bot"
            ).send()

 
        try:
            entities = extract_entities_pipeline(query)
            logger.info(f"Extracted entities: {entities}")
        except Exception as e:
            logger.error(f"Entity extraction failed: {e}")
            entities = {"location": [], "event": []}
            await cl.Message(
                content="Note: Could not extract specific locations. Using default region.",
                author="Risk Advisor Bot"
            ).send()

       
        region = None
        if entities.get("location"):
            loc = entities["location"]
            region = loc[0] if isinstance(loc, list) and loc else loc
        
        if not region:
            region = "Mumbai"
            logger.info(f"No region found, defaulting to {region}")

       
        try:
            risk_score = predict_risk(region, days=5)
            logger.info(f"Risk score for {region}: {risk_score:.2f}")
        except Exception as e:
            logger.error(f"Risk prediction failed for {region}: {e}")
            risk_score = 0.5  
            await cl.Message(
                content="Note: Risk calculation encountered an issue. Using estimated risk level.",
                author="Risk Advisor Bot"
            ).send()

 
        try:
            recent_incidents = ["port strike", "supplier outage"] if region else []
            weather_alert = "Typhoon warning" if region == "Shanghai" else None
            
            advice = generate_recommendation(
                risk_score=risk_score,
                region=region,
                recent_incidents=recent_incidents,
                weather_alert=weather_alert,
                intent=intent
            )
            logger.info(f"Generated recommendation: {advice['action']}")
        except Exception as e:
            logger.error(f"Recommendation generation failed: {e}")
            advice = {
                "message": "Unable to generate specific recommendation. Please monitor the situation closely.",
                "action": "monitor"
            }

      
        try:
            # Get risk level emoji
            if risk_score > 0.7:
                risk_emoji = "🔴"
                risk_level = "High"
            elif risk_score > 0.4:
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
                f"**Risk Score:** {risk_emoji} {risk_level} ({risk_score:.2f})\n\n"
                f"**💡 Recommendation:**\n{advice['message']}\n"
            )

            await cl.Message(
                content=response,
                author="Risk Advisor Bot"
            ).send()

            
            alert_emoji = "🚨" if risk_score > 0.7 else "⚠️" if risk_score > 0.4 else "✅"
            await cl.Message(
                content=f"{alert_emoji} **Alert Level:** {advice['action'].upper()}",
                author="Risk Advisor Bot"
            ).send()
            
            logger.info("Response sent successfully")
            
        except Exception as e:
            logger.error(f"Failed to send response: {e}")
            await cl.Message(
                content="An error occurred while formatting the response. Please try again.",
                author="Risk Advisor Bot"
            ).send()
    
    except Exception as e:
      
        logger.error(f"Unexpected error in handle_message: {e}", exc_info=True)
        await cl.Message(
            content="An unexpected error occurred. Please try again or rephrase your question.",
            author="Risk Advisor Bot"
        ).send()


@cl.on_chat_start
async def welcome():
    
    try:
        welcome_msg = """
# 🌐 Welcome to AI-Powered Supply Chain Risk Advisor

I'm your intelligent assistant for **real-time supply chain risk analysis** and **mitigation strategies**.

### 🎯 What I Can Help You With:

- 🔍 **Risk Assessment** - Check delays, disruptions, or issues in specific regions
- ⚡ **Real-time Alerts** - Get notifications about strikes, weather events, and disruptions  
- 💡 **Smart Recommendations** - Receive actionable advice for supply chain challenges
- 🌍 **Regional Analysis** - Analyze risk levels for countries, ports, and routes

### 💬 Example Questions:

- "Is there any delay in vessels from USA to UAE?"
- "What should I do about the port strike in Shanghai?"
- "Are there weather problems affecting shipments to Germany?"
- "What's the risk level for Mumbai port?"

**How can I assist you today?** 🚀
"""
        await cl.Message(
            content=welcome_msg,
            author="Risk Advisor Bot"
        ).send()
        logger.info("Welcome message sent successfully")
        
    except Exception as e:
        logger.error(f"Failed to send welcome message: {e}")
       
        await cl.Message(
            content="Welcome to the Supply Chain Risk Advisor! How can I help you today?",
            author="Risk Advisor Bot"
        ).send()


@cl.on_chat_end
async def on_chat_end():
    """
    Handle chat session end
    """
    logger.info("Chat session ended")




@cl.on_settings_update
async def on_settings_update(settings):
   
    logger.info(f"Settings updated: {settings}")
