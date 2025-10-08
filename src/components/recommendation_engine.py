
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

def generate_recommendation(risk_score, region, recent_incidents=None, weather_alert=None, intent=None):
    """
    Generate mitigation advice based on risk prediction and context.
    
    Args:
        risk_score (float): Predicted risk score (0–1) for region/route.
        region (str): Region/city/route name.
        recent_incidents (list[str]): Summary of latest incidents (optional).
        weather_alert (str): Short weather alert or status (optional).
        intent (str): Dialog/user intent (optional).
    
    Returns:
        dict: Recommendation message and advice action.
    """
    # --- Rule-based logic ---
    if risk_score >= 0.8:
        message = (f"🚨 High risk for {region}! " +
                   "Recent incidents or delays detected. " +
                   "It is recommended to reroute shipments, switch to an alternate supplier, or delay dispatch.")
        action = "reroute/switch_supplier/delay"
    elif risk_score >= 0.6:
        message = (f"⚠️ Risk is elevated for {region}. " +
                   "Monitor the region closely and prioritize suppliers/routes with better reliability." )
        action = "monitor_prioritize"
    elif risk_score >= 0.3:
        message = (f"➔ Moderate risk for {region}. " +
                   "Standard operation is acceptable, but stay alert for new events." )
        action = "continue_monitor"
    else:
        message = (f"✅ Risk is low for {region}. " +
                   "Proceed with regular operations.")
        action = "proceed"

    if weather_alert:
        message += f"\nWeather Alert: {weather_alert}"
    if recent_incidents:
        message += f"\nRecent incidents: {', '.join(recent_incidents[:3])}"

    # --- Intent-sensitive advice ---
    if intent == "mitigation_help" and risk_score >= 0.5:
        message += "\nWould you like to view alternate routes or suppliers for mitigation?"

    logger.info(f"Recommendation for {region} (risk: {risk_score:.2f}): {action}")
    return {"message": message, "action": action, "risk_score": risk_score, "region": region}


# --- Example/demo usage ---
if __name__ == "__main__":
    ex1 = generate_recommendation(
        risk_score=0.85,
        region="Shanghai",
        recent_incidents=['port strike', 'supplier outage', 'heavy rain'],
        weather_alert='Typhoon warning',
        intent="mitigation_help"
    )
    print("\n--- Example Recommendation ---")
    print(ex1["message"])

    ex2 = generate_recommendation(
        risk_score=0.55,
        region="Delhi",
        recent_incidents=['route accident', 'moderate rain'],
        weather_alert=None,
        intent="risk_check"
    )
    print("\n--- Example Recommendation ---")
    print(ex2["message"])

    ex3 = generate_recommendation(
        risk_score=0.15,
        region="Mumbai",
        recent_incidents=[],
        intent=None
    )
    print("\n--- Example Recommendation ---")
    print(ex3["message"])
