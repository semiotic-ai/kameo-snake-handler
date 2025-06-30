"""Async Ork logic module for handling various Ork-themed calculations."""

import json
import sys
import logging
import asyncio
from typing import Dict, Any
import os

from ork_logic import (
    calculate_waaagh_power, calculate_klan_bonus, 
    calculate_scrap_result, calculate_loot,
    OrkError
)

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='[PYTHON ASYNC] %(levelname)s %(message)s')

async def process_async_calculation(func, *args):
    """
    Process a calculation asynchronously with a small delay to simulate work.
    """
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
        logging.info(f"Received message: {message}")
        # Accept both CalculatePower and CalculateWaaaghPower for power calc
        if "CalculatePower" in message or "CalculateWaaaghPower" in message:
            key = "CalculatePower" if "CalculatePower" in message else "CalculateWaaaghPower"
            boyz_count = message[key]["boyz_count"]
            result = await process_async_calculation(calculate_waaagh_power, int(boyz_count))
            resp = {"Power": {"power": result}}
            logging.info(f"Returning: {resp}")
            return resp
        
        elif "CalculateKlanBonus" in message:
            klan_name = message["CalculateKlanBonus"]["klan_name"]
            base_power = message["CalculateKlanBonus"]["base_power"]
            result = await process_async_calculation(calculate_klan_bonus, klan_name, int(base_power))
            resp = {"KlanBonus": {"bonus": result}}
            logging.info(f"Returning: {resp}")
            return resp
        
        elif "CalculateScrapResult" in message:
            attacker = message["CalculateScrapResult"]["attacker_power"]
            defender = message["CalculateScrapResult"]["defender_power"]
            result = await process_async_calculation(calculate_scrap_result, int(attacker), int(defender))
            resp = {"ScrapResult": {"victory": result}}
            logging.info(f"Returning: {resp}")
            return resp
        
        elif "CalculateLoot" in message:
            teef = message["CalculateLoot"]["teef"]
            victory_points = message["CalculateLoot"]["victory_points"]
            result = await process_async_calculation(calculate_loot, int(teef), int(victory_points))
            resp = {"LootResult": {"total_teef": int(result["total_teef"]), "bonus_teef": int(result["bonus_teef"])}}
            logging.info(f"Returning: {resp}")
            return resp
        
        else:
            raise OrkError(f"Unknown message type: {message}")
    except Exception as e:
        import traceback
        logging.error(f"Exception: {e}")
        traceback.print_exc()
        raise
