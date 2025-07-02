"""Async logic module for handling various calculations."""

import json
import sys
import logging
import asyncio
from typing import Dict, Any
import os
from logic import (
    calculate_power, calculate_category_bonus, 
    calculate_competition_result, calculate_reward,
    LogicError
)

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='[PYTHON ASYNC] %(levelname)s %(message)s')

async def process_async_calculation(func, *args):
    """
    Process a calculation asynchronously with a small delay to simulate work.
    """
    return func(*args)

async def handle_message_async(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle incoming messages from the Rust code, asynchronously.
    Args:
        message: Dictionary containing message data
    Returns:
        Dictionary containing response data
    """
    import kameo
    try:
        logging.info(f"Received message: {message}")
        # Accept both old and new keys for each operation
        if "CalculatePower" in message or "CalculateWaaaghPower" in message:
            key = "CalculatePower" if "CalculatePower" in message else "CalculateWaaaghPower"
            count = message[key].get("count", message[key].get("boyz_count"))
            if count is None or (isinstance(count, str) and count.strip() == ""):
                raise LogicError("Missing or empty required field: count/boyz_count")
            try:
                count_val = int(count)
            except Exception:
                raise LogicError(f"Invalid value for count/boyz_count: {count}")
            if count_val <= 0:
                raise LogicError(f"count/boyz_count must be positive, got {count_val}")
            result = await process_async_calculation(calculate_power, count_val)
            resp = {"Power": {"power": result}}
            logging.info(f"Returning: {resp}")
            return resp
        elif "CalculateCategoryBonus" in message or "CalculateKlanBonus" in message:
            key = "CalculateCategoryBonus" if "CalculateCategoryBonus" in message else "CalculateKlanBonus"
            params = message[key]
            category_name = params.get("category_name", params.get("klan_name"))
            base_power = params.get("base_power")
            if category_name is None or (isinstance(category_name, str) and category_name.strip() == ""):
                raise LogicError("Missing or empty required field: category_name/klan_name")
            if base_power is None or (isinstance(base_power, str) and base_power.strip() == ""):
                raise LogicError("Missing or empty required field: base_power")
            try:
                base_power_val = int(base_power)
            except Exception:
                raise LogicError(f"Invalid value for base_power: {base_power}")
            if base_power_val <= 0:
                raise LogicError(f"base_power must be positive, got {base_power_val}")
            result = await process_async_calculation(calculate_category_bonus, category_name, base_power_val)
            resp = {"CategoryBonus": {"bonus": result}}
            logging.info(f"Returning: {resp}")
            return resp
        elif "CalculateCompetitionResult" in message or "CalculateScrapResult" in message:
            key = "CalculateCompetitionResult" if "CalculateCompetitionResult" in message else "CalculateScrapResult"
            params = message[key]
            attacker = params.get("attacker_power")
            defender = params.get("defender_power")
            if attacker is None or defender is None:
                raise LogicError("Missing required field: attacker_power or defender_power")
            try:
                attacker_val = int(attacker)
                defender_val = int(defender)
            except Exception:
                raise LogicError(f"Invalid value for attacker_power or defender_power: {attacker}, {defender}")
            if attacker_val < 0 or defender_val < 0:
                raise LogicError(f"attacker_power and defender_power must be non-negative, got {attacker_val}, {defender_val}")
            result = await process_async_calculation(calculate_competition_result, attacker_val, defender_val)
            resp = {"CompetitionResult": {"victory": result}}
            logging.info(f"Returning: {resp}")
            return resp
        elif "CalculateReward" in message or "CalculateLoot" in message:
            key = "CalculateReward" if "CalculateReward" in message else "CalculateLoot"
            params = message[key]
            currency = params.get("currency", params.get("teef"))
            points = params.get("points", params.get("victory_points"))
            if currency is None or (isinstance(currency, str) and currency.strip() == "") or \
               points is None or (isinstance(points, str) and points.strip() == ""):
                raise LogicError("Missing or empty required field: currency/teef or points/victory_points")
            try:
                currency_val = int(currency)
                points_val = int(points)
            except Exception:
                raise LogicError(f"Invalid value for currency/teef or points/victory_points: {currency}, {points}")
            if currency_val < 0 or points_val < 0:
                raise LogicError(f"currency/teef and points/victory_points must be non-negative, got {currency_val}, {points_val}")
            result = await process_async_calculation(calculate_reward, currency_val, points_val)
            resp = {"RewardResult": {"total_currency": int(result["total_currency"]), "bonus_currency": int(result["bonus_currency"])} }
            logging.info(f"Returning: {resp}")
            return resp
        elif "CallbackRoundtrip" in message:
            value = message["CallbackRoundtrip"]["value"]
            result = await kameo.callback_handle({"value": value})
            return {"CallbackRoundtripResult": {"value": result}}
        else:
            raise LogicError(f"Unknown message type: {message}")
    except Exception as e:
        import traceback
        logging.error(f"Exception: {e}")
        traceback.print_exc()
        raise
