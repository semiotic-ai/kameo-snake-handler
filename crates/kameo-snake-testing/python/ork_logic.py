"""
WAAAGH! Da Ork Logic Module!

This module contains all da fancy calculations for da Ork Mob system.
It handles things like WAAAGH! power, klan bonuses, and fightin' results.
"""

import random
import json
import sys
import traceback
from typing import Dict, Any, Union

def log_debug(msg: str) -> None:
    """Log debug messages to stderr."""
    print(msg, file=sys.stderr, flush=True)

class OrkError(Exception):
    """Base class for Ork-related errors."""
    pass

class InvalidBoyzCountError(OrkError):
    """Raised when boyz count is invalid."""
    pass

class InvalidKlanError(OrkError):
    """Raised when klan name is invalid."""
    pass

class InvalidTeefError(OrkError):
    """Raised when teef amount is invalid."""
    pass

class InvalidPowerError(OrkError):
    """Raised when power level is invalid."""
    pass

def calculate_waaagh_power(boyz_count: int) -> int:
    """Calculate da WAAAGH! power based on number of boyz."""
    if not isinstance(boyz_count, int):
        raise InvalidBoyzCountError("Boyz count must be a whole number, ya git!")
    if boyz_count < 0:
        raise InvalidBoyzCountError(f"Can't have {boyz_count} boyz, ya git!")
    if boyz_count > 429496729:  # Ensure result won't exceed u32::MAX after multiplication and random addition
        raise InvalidBoyzCountError(f"Too many boyz ({boyz_count})! Da WAAAGH! would overflow!")
    return boyz_count * 10 + random.randint(1, 100)

def calculate_klan_bonus(klan_name: str, base_power: int) -> int:
    """Calculate da klan bonus for a given power level."""
    klan_bonuses = {
        "Evil Sunz": 1.2,
        "Bad Moons": 1.1,
        "Goffs": 1.3,
        "Deathskulls": 1.15,
        "Blood Axes": 1.1,
        "Snakebites": 1.25
    }
    
    if not isinstance(base_power, int):
        raise InvalidPowerError("Power must be a whole number, ya git!")
    if klan_name not in klan_bonuses:
        raise InvalidKlanError(f"Never heard of da {klan_name} klan!")
    if base_power < 0:
        raise InvalidPowerError(f"Can't have {base_power} power, ya git!")
    if base_power > 3435973836:  # Ensure result won't exceed u32::MAX after applying max bonus (1.3)
        raise InvalidPowerError(f"Too much power ({base_power})! It would overflow!")
        
    return int(base_power * klan_bonuses[klan_name])

def calculate_scrap_result(attacker_power: int, defender_power: int) -> bool:
    """Calculate da result of a scrap between two orks."""
    if not isinstance(attacker_power, int) or not isinstance(defender_power, int):
        raise InvalidPowerError("Power must be whole numbers, ya git!")
    if attacker_power < 0 or defender_power < 0:
        raise InvalidPowerError("Can't have negative power in a scrap!")
    if attacker_power > 4294967275 or defender_power > 4294967275:  # u32::MAX - 20 for the random roll
        raise InvalidPowerError("Power too high! It would overflow!")
        
    # Add some random factor for da fun of it
    attacker_roll = random.randint(1, 20)
    defender_roll = random.randint(1, 20)
    
    return (attacker_power + attacker_roll) > (defender_power + defender_roll)

def calculate_loot(teef: int, victory_points: int) -> Dict[str, int]:
    """Calculate da total loot from a battle."""
    if not isinstance(teef, int) or not isinstance(victory_points, int):
        raise InvalidTeefError("Teef and victory points must be whole numbers, ya git!")
    if teef < 0:
        raise InvalidTeefError(f"Can't have {teef} teef, ya git!")
    if victory_points < 0:
        raise InvalidTeefError("Can't have negative victory points, ya git!")
    if teef > 4294967295 - (victory_points * 15):  # Ensure we won't overflow with max bonus
        raise InvalidTeefError("Too much teef! It would overflow!")
        
    # More victory points means more teef!
    bonus = victory_points * random.randint(5, 15)
    total = teef + bonus
    return {"total_teef": total, "bonus_teef": bonus}

def handle_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle incoming messages from da Rust code.
    
    Args:
        message: Dictionary containing message data
        
    Returns:
        Dictionary containing response data
    """
    try:
        log_debug(f"PROCESSING MESSAGE: {json.dumps(message, indent=2)}")
        
        # Extract message fields
        if not isinstance(message, dict):
            raise OrkError("INVALID MESSAGE FORMAT, YA GIT!")
            
        # Handle message types directly
        if "CalculateWaaaghPower" in message:
            # CalculateWaaaghPower
            boyz_count = message["CalculateWaaaghPower"]["boyz_count"]
            power = calculate_waaagh_power(int(boyz_count))
            return {"WaaaghPower": {"power": power}}
            
        elif "CalculateKlanBonus" in message:
            # CalculateKlanBonus
            params = message["CalculateKlanBonus"]
            bonus = calculate_klan_bonus(
                str(params["klan_name"]), 
                int(params["base_power"])
            )
            return {"KlanBonus": {"bonus": bonus}}
            
        elif "CalculateScrapResult" in message:
            # CalculateScrapResult
            params = message["CalculateScrapResult"]
            result = calculate_scrap_result(
                int(params["attacker_power"]), 
                int(params["defender_power"])
            )
            return {"ScrapResult": {"victory": result}}
            
        elif "CalculateLoot" in message:
            # CalculateLoot
            params = message["CalculateLoot"]
            result = calculate_loot(
                int(params["teef"]), 
                int(params["victory_points"])
            )
            return {"LootResult": {"total_teef": result["total_teef"], "bonus_teef": result["bonus_teef"]}}
            
        else:
            raise OrkError("INVALID MESSAGE TYPE, YA GIT!")
            
    except OrkError as e:
        log_debug(f"ORK ERROR: {str(e)}")
        return {"Error": {"error": str(e)}}
            
    except Exception as e:
        log_debug(f"ERROR: {str(e)}\n{traceback.format_exc()}")
        return {"Error": {"error": str(e)}}

if __name__ == "__main__":
    # Set up logging
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )
    logger = logging.getLogger(__name__)
    
    # Main loop
    for line in sys.stdin:
        try:
            message = line.strip()
            if not message:
                continue
                
            if isinstance(message, str):
                message = json.loads(message)
                
            response = handle_message(message)
            print(json.dumps(response))
            sys.stdout.flush()
            
        except Exception as e:
            logger.exception("Error in main loop")
            print(json.dumps({"Error": {"error": f"FATAL ERROR: {str(e)}"}}))
            sys.stdout.flush()
