"""
WAAAGH! Da Ork Logic Module!

This module contains all da fancy calculations for da Ork Mob system.
It handles things like WAAAGH! power, klan bonuses, and fightin' results.
"""

import random
import json
import sys
from typing import Dict, Any, Tuple

def log_debug(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)

def apply_klan_bonus(power: int, klan: str) -> int:
    """
    Apply klan-specific power bonuses.
    
    Args:
        power: Base power level
        klan: Which klan da boy belongs to
        
    Returns:
        Modified power level
    """
    klan_bonuses = {
        'Goffs': 1.5,      # Best at krumpin'
        'EvilSunz': 1.3,   # Fast but not as strong
        'BadMoons': 1.4,   # Good gear helps
        'DeathSkulls': 1.2 + random.random() * 0.4,  # Random luck!
        'BloodAxes': 1.35,  # Taktikul advantage
    }
    
    multiplier = klan_bonuses.get(klan, 1.0)
    return int(power * multiplier)

def calculate_scrap_result(boy1_power: int, boy2_power: int) -> Tuple[int, int]:
    """
    Calculate da result of a scrap between two boyz.
    
    Args:
        boy1_power: Power of first boy
        boy2_power: Power of second boy
        
    Returns:
        Tuple of (winner (1 or 2), power_boost)
    """
    # Add some randomness
    boy1_roll = random.random() * 0.4 + 0.8  # 0.8 to 1.2
    boy2_roll = random.random() * 0.4 + 0.8  # 0.8 to 1.2
    
    boy1_final = int(boy1_power * boy1_roll)
    boy2_final = int(boy2_power * boy2_roll)
    
    power_boost = abs(boy1_final - boy2_final) // 2
    
    if boy1_final > boy2_final:
        return (1, power_boost)
    else:
        return (2, power_boost)

def loot_calculation(base_teef: int) -> int:
    """
    Calculate how much extra teef a boy gets from lootin'.
    
    Args:
        base_teef: How much they're trying to loot
        
    Returns:
        Actual teef looted
    """
    success_roll = random.random()
    if success_roll > 0.5:
        return int(base_teef * (1.0 + success_roll))
    else:
        return int(base_teef * success_roll)

def calculate_waaagh_power(boyz_count: int) -> int:
    """
    Calculate total WAAAGH! power based on number of boyz.
    
    Args:
        boyz_count: Number of boyz in da mob
        
    Returns:
        Total WAAAGH! power
    """
    if boyz_count < 0:
        raise ValueError("YA KANT 'AVE NEGATIVE BOYZ, YA GIT!")
        
    base_power = boyz_count * 100
    waaagh_multiplier = 1.0 + (boyz_count / 10.0)  # More boyz = more WAAAGH!
    return int(base_power * waaagh_multiplier)

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
    
    # Try to get command type from either format
    command_type = command.get("type")
    if not command_type:
        # Try the old format where command type is a top-level key
        for key in command:
            if key not in ["result", "message"]:
                command_type = key
                # Extract parameters from nested dict
                command.update(command[key])
                break
    
    if not command_type:
        return {"result": 0, "message": "No command type found"}
    
    log_debug(f"COMMAND TYPE: {command_type}")
    
    if command_type == "KlanBonus":
        power = command.get("power", 0)
        klan = command.get("klan", "")
        result = apply_klan_bonus(power, klan)
        response = {"result": result, "message": f"Klan bonus for {klan}: {result}"}
        log_debug(f"SENDING RESPONSE: {json.dumps(response, indent=2)}")
        return response
        
    elif command_type == "ScrapResult":
        boy1_power = command.get("boy1_power", 0)
        boy2_power = command.get("boy2_power", 0)
        winner, power_boost = calculate_scrap_result(boy1_power, boy2_power)
        response = {"result": winner, "message": f"Boy {winner} won with {power_boost} power boost!"}
        log_debug(f"SENDING RESPONSE: {json.dumps(response, indent=2)}")
        return response
        
    elif command_type == "LootTeef":
        base_teef = command.get("base_teef", 0)
        result = loot_calculation(base_teef)
        response = {"result": result, "message": f"Looted {result} teef!"}
        log_debug(f"SENDING RESPONSE: {json.dumps(response, indent=2)}")
        return response
        
    elif command_type == "WaaaghPower":
        boyz_count = command.get("boyz_count", 0)
        result = calculate_waaagh_power(boyz_count)
        response = {"result": result, "message": f"WAAAGH! power is {result}!"}
        log_debug(f"SENDING RESPONSE: {json.dumps(response, indent=2)}")
        return response
        
    else:
        response = {"result": 0, "message": f"Unknown command type: {command_type}"}
        log_debug(f"SENDING RESPONSE: {json.dumps(response, indent=2)}")
        return response 