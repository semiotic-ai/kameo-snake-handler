import json
import sys
from typing import Dict, Any

def log_debug(msg: str) -> None:
    """Log debug messages to stderr."""
    print(msg, file=sys.stderr, flush=True)

def handle_message(command: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle incoming messages from da Rust side.
    
    Args:
        command: Dictionary containing command type and parameters
        
    Returns:
        Dictionary with result and message
    """
    log_debug(f"RECEIVED COMMAND: {json.dumps(command, indent=2)}")
    
    if isinstance(command, str):
        try:
            command = json.loads(command)
        except json.JSONDecodeError as e:
            log_debug(f"ERROR DECODING JSON: {e}")
            return {"result": 0, "message": f"JSON decode error: {e}"}
    
    # Calculate WAAAGH! power bonus based on klan
    klan_bonus = {
        "Evil Sunz": 1.5,
        "Goffs": 2.0,
        "Bad Moons": 1.8,
        "Deathskulls": 1.3,
        "Blood Axes": 1.4,
        "Snakebites": 1.6,
    }.get(command.get("klan", ""), 1.0)
    
    # Calculate final result using WAAAGH! power and teef
    result = int(command.get("power", 0) * klan_bonus)
    
    # Generate appropriate message
    msg = f"WAAAGH! {command.get('klan', 'Unknown')} boyz got {result} from {command.get('power', 0)} power!"
    
    log_debug(f"SENDING RESPONSE: {{'result': {result}, 'message': '{msg}'}}")
    return {
        "result": result,
        "message": msg
    } 