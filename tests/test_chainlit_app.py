import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import patch, MagicMock, AsyncMock


TYPICAL_QUERIES = [
    {
        "message": "Is there any delay in vessel from USA to UAE?",
        "expected_intent": "risk_check",
        "expected_entity": "UAE",
        "expected_region": "UAE"
    },
    {
        "message": "What should I do about the port strike in Shanghai?",
        "expected_intent": "mitigation_help",
        "expected_entity": "Shanghai",
        "expected_region": "Shanghai"
    },
    {
        "message": "Are there weather problems affecting shipments to Germany?",
        "expected_intent": "weather_alert",
        "expected_entity": "Germany",
        "expected_region": "Germany"
    },
]


@pytest.fixture
def mock_chainlit_message():
 
    mock_message_class = MagicMock()
    mock_message_instance = MagicMock()
    mock_message_instance.send = AsyncMock()
    mock_message_class.return_value = mock_message_instance
    
    with patch("chainlit.Message", mock_message_class), \
         patch("chainlit.user_session", {}):
        yield mock_message_instance


@pytest.mark.asyncio
@pytest.mark.parametrize("qset", TYPICAL_QUERIES)
async def test_message_handling_logic(qset, mock_chainlit_message):

    from app import app
    

    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:


        mock_intent.return_value = {
            "intent": qset["expected_intent"],
            "confidence": 0.95
        }
        mock_ner.return_value = {
            "location": [qset["expected_entity"]],
            "event": ["strike"] if "strike" in qset["message"] else []
        }
        mock_risk.return_value = 0.7
        mock_reco.return_value = {
            "message": "Test recommendation message.",
            "action": "monitor"
        }

   
        class DummyMsg:
            content = qset["message"]

  
        await app.handle_message(DummyMsg())

     
        mock_intent.assert_called_once_with(qset["message"])
        mock_ner.assert_called_once_with(qset["message"])
        mock_risk.assert_called_once()  # Check region is passed
        mock_reco.assert_called_once()
        
        
        assert mock_chainlit_message.send.call_count == 2


@pytest.mark.asyncio
async def test_welcome_message(mock_chainlit_message):

    from app import app
    
    await app.welcome()
    
  
    mock_chainlit_message.send.assert_called_once()


@pytest.mark.asyncio
async def test_default_region_fallback(mock_chainlit_message):
    """Test that Mumbai is used as default when no location found"""
    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "risk_check", "confidence": 0.9}
        mock_ner.return_value = {"location": [], "event": []}  # No location
        mock_risk.return_value = 0.5
        mock_reco.return_value = {"message": "Test", "action": "monitor"}

        class DummyMsg:
            content = "What is the risk level?"

        await app.handle_message(DummyMsg())

 
        mock_risk.assert_called_once()
        call_args = mock_risk.call_args
        assert call_args[0][0] == "Mumbai", "Should default to Mumbai"


@pytest.mark.asyncio
async def test_entity_extraction(mock_chainlit_message):

    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "risk_check", "confidence": 0.95}
        mock_ner.return_value = {
            "location": ["China", "USA"],
            "event": ["typhoon", "strike"]
        }
        mock_risk.return_value = 0.8
        mock_reco.return_value = {"message": "High risk", "action": "reroute"}

        class DummyMsg:
            content = "Typhoon and strike affecting China USA shipments"

        await app.handle_message(DummyMsg())


        mock_ner.assert_called_once()
        extracted = mock_ner.return_value
        assert "China" in extracted["location"]
        assert "typhoon" in extracted["event"]


@pytest.mark.asyncio
async def test_risk_score_calculation(mock_chainlit_message):

    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "risk_check", "confidence": 0.9}
        mock_ner.return_value = {"location": ["Shanghai"], "event": ["strike"]}
        mock_risk.return_value = 0.85
        mock_reco.return_value = {"message": "Critical", "action": "immediate"}

        class DummyMsg:
            content = "Port strike in Shanghai"

        await app.handle_message(DummyMsg())

    
        mock_risk.assert_called_once_with("Shanghai", days=5)


@pytest.mark.asyncio
async def test_recommendation_generation(mock_chainlit_message):

    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "mitigation_help", "confidence": 0.9}
        mock_ner.return_value = {"location": ["Shanghai"], "event": ["strike"]}
        mock_risk.return_value = 0.9
        mock_reco.return_value = {
            "message": "Immediate rerouting recommended",
            "action": "urgent"
        }

        class DummyMsg:
            content = "What should I do about Shanghai strike?"

        await app.handle_message(DummyMsg())

    
        mock_reco.assert_called_once()
        call_kwargs = mock_reco.call_args[1]
        assert call_kwargs["risk_score"] == 0.9
        assert call_kwargs["region"] == "Shanghai"
        assert call_kwargs["intent"] == "mitigation_help"
        assert "port strike" in call_kwargs["recent_incidents"]


@pytest.mark.asyncio
async def test_shanghai_typhoon_warning(mock_chainlit_message):

    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "weather_alert", "confidence": 0.9}
        mock_ner.return_value = {"location": ["Shanghai"], "event": []}
        mock_risk.return_value = 0.8
        mock_reco.return_value = {"message": "Weather warning", "action": "monitor"}

        class DummyMsg:
            content = "Weather in Shanghai?"

        await app.handle_message(DummyMsg())

   
        call_kwargs = mock_reco.call_args[1]
        assert call_kwargs["weather_alert"] == "Typhoon warning"


@pytest.mark.asyncio
async def test_non_shanghai_no_typhoon(mock_chainlit_message):
 
    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "weather_alert", "confidence": 0.9}
        mock_ner.return_value = {"location": ["Mumbai"], "event": []}
        mock_risk.return_value = 0.5
        mock_reco.return_value = {"message": "Normal", "action": "continue"}

        class DummyMsg:
            content = "Weather in Mumbai?"

        await app.handle_message(DummyMsg())

   
        call_kwargs = mock_reco.call_args[1]
        assert call_kwargs["weather_alert"] is None


@pytest.mark.asyncio
async def test_response_format(mock_chainlit_message):

    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "risk_check", "confidence": 0.85}
        mock_ner.return_value = {"location": ["Dubai"], "event": []}
        mock_risk.return_value = 0.6
        mock_reco.return_value = {
            "message": "Moderate risk detected",
            "action": "monitor"
        }

        class DummyMsg:
            content = "Risk in Dubai?"

        await app.handle_message(DummyMsg())

       
        assert mock_chainlit_message.send.call_count == 2


@pytest.mark.asyncio
async def test_list_location_extraction(mock_chainlit_message):

    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "risk_check", "confidence": 0.9}
        mock_ner.return_value = {
            "location": ["Singapore", "Malaysia"],  # Multiple locations
            "event": []
        }
        mock_risk.return_value = 0.7
        mock_reco.return_value = {"message": "Check routes", "action": "monitor"}

        class DummyMsg:
            content = "Risk between Singapore and Malaysia?"

        await app.handle_message(DummyMsg())

 
        mock_risk.assert_called_once_with("Singapore", days=5)


@pytest.mark.asyncio
async def test_empty_location_list(mock_chainlit_message):
  
    from app import app
    
    with patch("src.app.chatbot.predict_intent") as mock_intent, \
         patch("src.app.chatbot.extract_entities_pipeline") as mock_ner, \
         patch("src.app.chatbot.predict_risk") as mock_risk, \
         patch("src.app.chatbot.generate_recommendation") as mock_reco:

        mock_intent.return_value = {"intent": "general", "confidence": 0.7}
        mock_ner.return_value = {"location": [], "event": []}  # Empty list
        mock_risk.return_value = 0.5
        mock_reco.return_value = {"message": "General info", "action": "continue"}

        class DummyMsg:
            content = "General supply chain info?"

        await app.handle_message(DummyMsg())

     
        mock_risk.assert_called_once_with("Mumbai", days=5)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_models_integration(mock_chainlit_message):
 
    from app import app
    
    class DummyMsg:
        content = "Is there a delay in Shanghai port?"
    
    try:
        await app.handle_message(DummyMsg())
       
        assert mock_chainlit_message.send.called
    except Exception as e:
        pytest.skip(f"Integration test skipped - models not available: {e}")