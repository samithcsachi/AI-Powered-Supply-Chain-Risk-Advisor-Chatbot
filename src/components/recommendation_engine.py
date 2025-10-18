import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from utils.logger import *

import logging
logger = logging.getLogger(__name__)

def generate_recommendation(
    risk_score,
    region,
    recent_incidents=None,
    weather_alert=None,
    intent=None,
    origin=None,
    destination=None
):
   
    if origin and destination:
        region_str = f"{origin} to {destination}"
    else:
        region_str = region

    if risk_score >= 0.8:
        level = "High risk"
        message = (
            f"{level} detected for {region_str}! Recent incidents or delays increase disruption probability. "
            "Immediate mitigation advised—consider rerouting, switching suppliers, or delaying shipment."
        )
        action = "reroute/switch_supplier/delay"
    elif risk_score >= 0.6:
        level = "Elevated risk"
        message = (
            f"{level} in {region_str}. Monitor closely and prioritize more reliable suppliers and routes."
        )
        action = "monitor_prioritize"
    elif risk_score >= 0.3:
        level = "Moderate risk"
        message = (
            f"{level} for {region_str}. Standard operations are feasible, but stay alert for escalating risks."
        )
        action = "continue_monitor"
    else:
        level = "Low risk"
        message = f"{level} for {region_str}. Proceed with routine operations."
        action = "proceed"

 
    if weather_alert:
        message += f"\nWeather Alert: {weather_alert}"
    if recent_incidents:
        message += f"\nRecent incidents: {', '.join(recent_incidents[:3])}"


    if recent_incidents and risk_score >= 0.8:
        message += "\nSupply chain disruption likely due to recent incidents. Take immediate action to mitigate risk."

 
    if intent == "mitigation_help" and risk_score >= 0.5:
        message += "\nWould you like to view alternate routes or suppliers for mitigation?"

    logger.info(f"Recommendation for {region_str} (risk: {risk_score:.2f}): {action}")
    return {
        "message": message,
        "action": action,
        "risk_score": risk_score,
        "region": region_str
    }

if __name__ == "__main__":
  
    ex1 = generate_recommendation(
        risk_score=0.85,
        region="Shanghai",
        recent_incidents=['port strike', 'supplier outage', 'heavy rain'],
        weather_alert='Typhoon warning',
        intent="mitigation_help",
        origin="Shanghai",
        destination="Los Angeles"
    )
    print("\n--- Example Recommendation ---")
    print(ex1["message"])

    ex2 = generate_recommendation(
        risk_score=0.55,
        region="Delhi",
        recent_incidents=['route accident', 'moderate rain'],
        weather_alert=None,
        intent="risk_check",
        origin="Delhi",
        destination="Dubai"
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
