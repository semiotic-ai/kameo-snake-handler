"""Async streaming logic module for handling various streaming calculations."""

import json
import sys
import logging
import asyncio
import random
import time
from typing import Dict, Any, AsyncGenerator
import os
from logic import (
    calculate_power, calculate_category_bonus, 
    calculate_competition_result, calculate_reward,
    LogicError
)
import kameo
print("Imported kameo, has callback_handle:", hasattr(kameo, "callback_handle"))

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='[PYTHON STREAMING] %(levelname)s %(message)s')

async def fibonacci_generator(count: int) -> AsyncGenerator[Dict[str, Any], None]:
    """Generate Fibonacci numbers as a stream."""
    a, b = 0, 1
    for i in range(count):
        yield {
            "StreamItem": {
                "index": i,
                "value": a
            }
        }
        a, b = b, a + b
        await asyncio.sleep(0.01)  # Small delay to simulate work

async def random_numbers_generator(count: int, max_value: int) -> AsyncGenerator[Dict[str, Any], None]:
    """Generate random numbers as a stream."""
    for i in range(count):
        value = random.randint(1, max_value)
        yield {
            "StreamItem": {
                "index": i,
                "value": value
            }
        }
        await asyncio.sleep(0.01)  # Small delay to simulate work

async def delayed_stream_generator(count: int, delay_ms: int) -> AsyncGenerator[Dict[str, Any], None]:
    """Generate a stream with configurable delays."""
    for i in range(count):
        yield {
            "StreamItem": {
                "index": i,
                "value": i * 10
            }
        }
        await asyncio.sleep(delay_ms / 1000.0)  # Convert ms to seconds

async def error_stream_generator(count: int, error_at: int = None) -> AsyncGenerator[Dict[str, Any], None]:
    """Generate a stream that may error at a specific index."""
    for i in range(count):
        if error_at is not None and i == error_at:
            yield {
                "StreamError": {
                    "index": i,
                    "error": f"Simulated error at index {i}"
                }
            }
            return  # Stop the stream on error
        
        yield {
            "StreamItem": {
                "index": i,
                "value": i * 100
            }
        }
        await asyncio.sleep(0.01)

async def large_dataset_generator(count: int) -> AsyncGenerator[Dict[str, Any], None]:
    """Generate a large dataset as a stream."""
    for i in range(count):
        # Simulate processing a large dataset
        data_size = random.randint(100, 1000)
        yield {
            "StreamItem": {
                "index": i,
                "value": data_size
            }
        }
        await asyncio.sleep(0.005)  # Faster processing for large datasets

async def handle_message_streaming(message: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Handle incoming messages from the Rust code, returning streaming responses.
    Args:
        message: Dictionary containing message data
    Yields:
        Dictionary containing streaming response items
    """
    try:
        logging.info(f"Received streaming message: {message}")
        
        if "StreamFibonacci" in message:
            count = message["StreamFibonacci"]["count"]
            if count <= 0:
                raise LogicError("Count must be positive for Fibonacci stream")
            if count > 1000:
                raise LogicError("Count too large for Fibonacci stream")
            
            # Yield all items from the Fibonacci stream
            async for item in fibonacci_generator(count):
                yield item
                
        elif "StreamRandomNumbers" in message:
            count = message["StreamRandomNumbers"]["count"]
            max_value = message["StreamRandomNumbers"]["max_value"]
            if count <= 0:
                raise LogicError("Count must be positive for random numbers stream")
            if max_value <= 0:
                raise LogicError("Max value must be positive for random numbers stream")
            if count > 10000:
                raise LogicError("Count too large for random numbers stream")
            
            # Yield all items from the random numbers stream
            async for item in random_numbers_generator(count, max_value):
                yield item
                
        elif "StreamWithDelays" in message:
            count = message["StreamWithDelays"]["count"]
            delay_ms = message["StreamWithDelays"]["delay_ms"]
            if count <= 0:
                raise LogicError("Count must be positive for delayed stream")
            if delay_ms > 5000:
                raise LogicError("Delay too large for delayed stream")
            
            # Yield all items from the delayed stream
            async for item in delayed_stream_generator(count, delay_ms):
                yield item
                
        elif "StreamWithErrors" in message:
            count = message["StreamWithErrors"]["count"]
            error_at = message["StreamWithErrors"].get("error_at")
            if count <= 0:
                raise LogicError("Count must be positive for error stream")
            if error_at is not None and (error_at < 0 or error_at >= count):
                raise LogicError("Error index out of range")
            
            # Yield all items from the error stream
            async for item in error_stream_generator(count, error_at):
                yield item
                
        elif "StreamLargeDataset" in message:
            count = message["StreamLargeDataset"]["count"]
            if count <= 0:
                raise LogicError("Count must be positive for large dataset stream")
            if count > 100000:
                raise LogicError("Count too large for large dataset stream")
            
            # Yield all items from the large dataset stream
            async for item in large_dataset_generator(count):
                yield item
                
        else:
            raise LogicError(f"Unknown streaming message type: {message}")
            
    except Exception as e:
        import traceback
        logging.error(f"Exception in streaming handler: {e}")
        traceback.print_exc()
        yield {
            "StreamError": {
                "index": 0,
                "error": str(e)
            }
        }

# For backward compatibility, also support the old non-streaming messages
async def handle_message_async(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle incoming messages from the Rust code, asynchronously.
    Args:
        message: Dictionary containing message data
    Returns:
        Dictionary containing response data
    """
    try:
        logging.info(f"Received async message: {message}")
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
            result = await asyncio.sleep(0.01)  # Simulate async work
            result = calculate_power(count_val)
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
            result = await asyncio.sleep(0.01)  # Simulate async work
            result = calculate_category_bonus(category_name, base_power_val)
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
            result = await asyncio.sleep(0.01)  # Simulate async work
            result = calculate_competition_result(attacker_val, defender_val)
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
            result = await asyncio.sleep(0.01)  # Simulate async work
            result = calculate_reward(currency_val, points_val)
            resp = {"RewardResult": {"total_currency": int(result["total_currency"]), "bonus_currency": int(result["bonus_currency"])} }
            logging.info(f"Returning: {resp}")
            return resp
        elif "CallbackRoundtrip" in message:
            value = message["CallbackRoundtrip"]["value"]
            await kameo.callback_handle({'value': value})
            return {'CallbackRoundtripResult': {'value': value + 1}}
        else:
            raise LogicError(f"Unknown message type: {message}")
    except Exception as e:
        import traceback
        logging.error(f"Exception: {e}")
        traceback.print_exc()
        raise

print("[DEBUG] MODULE LOADED:", __name__, "sys.path:", __import__('sys').path) 