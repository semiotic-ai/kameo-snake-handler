"""Async Ork logic module for handling various Ork-themed calculations."""

import json
import sys
import logging
import asyncio
from typing import Dict, Any
from ork_logic import (
    calculate_waaagh_power, calculate_klan_bonus, 
    calculate_scrap_result, calculate_loot,
    OrkError, log_debug
)

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def process_async_calculation(func, *args):
    """
    Process a calculation asynchronously with a small delay to simulate work.
    """
    await asyncio.sleep(0.1)  # Simulate some async work
    return func(*args)

async def handle_message_async(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle incoming messages from da Rust code, but ASYNC-LIKE!
    
    Args:
        message: Dictionary containing message data
        
    Returns:
        Dictionary containing response data
    """
    try:
        log_debug(f"PROCESSING ASYNC MESSAGE: {json.dumps(message, indent=2)}")
        
        # Handle message types directly
        if "CalculateWaaaghPower" in message:
            boyz_count = message["CalculateWaaaghPower"]["boyz_count"]
            result = await process_async_calculation(calculate_waaagh_power, int(boyz_count))
            return {"WaaaghPower": {"power": result}}
            
        elif "CalculateKlanBonus" in message:
            klan_name = message["CalculateKlanBonus"]["klan_name"]
            base_power = message["CalculateKlanBonus"]["base_power"]
            result = await process_async_calculation(calculate_klan_bonus, klan_name, int(base_power))
            return {"KlanBonus": {"bonus": result}}
            
        elif "CalculateScrapResult" in message:
            attacker = message["CalculateScrapResult"]["attacker_power"]
            defender = message["CalculateScrapResult"]["defender_power"]
            result = await process_async_calculation(calculate_scrap_result, int(attacker), int(defender))
            return {"ScrapResult": {"victory": result}}
            
        elif "CalculateLoot" in message:
            teef = message["CalculateLoot"]["teef"]
            victory_points = message["CalculateLoot"]["victory_points"]
            result = await process_async_calculation(calculate_loot, int(teef), int(victory_points))
            # Convert dictionary values to integers
            return {"LootResult": {"total_teef": int(result["total_teef"]), "bonus_teef": int(result["bonus_teef"])}}
            
        else:
            raise OrkError(f"Unknown message type: {message}")
            
    except OrkError as e:
        log_debug(f"ORK ERROR: {str(e)}")
        return {"Error": {"error": str(e)}}
    except Exception as e:
        log_debug(f"UNEXPECTED ERROR: {str(e)}")
        return {"Error": {"error": str(e)}}

if __name__ == "__main__":
    # For testing
    async def test():
        result = await handle_message_async({"CalculateWaaaghPower": {"boyz_count": 100}})
        print(result)
    
    asyncio.run(test()) 