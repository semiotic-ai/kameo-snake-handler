"""
General Logic Module

This module contains calculation logic for the test system.
It handles things like power, category bonuses, competition results, and rewards.
"""

import random
from typing import Dict, Any

class LogicError(Exception):
    """Base class for logic-related errors."""
    pass

class InvalidCountError(LogicError):
    """Raised when count is invalid."""
    pass

class InvalidCategoryError(LogicError):
    """Raised when category name is invalid."""
    pass

class InvalidCurrencyError(LogicError):
    """Raised when currency amount is invalid."""
    pass

class InvalidPowerError(LogicError):
    """Raised when power level is invalid."""
    pass

def calculate_power(count: int) -> int:
    """Calculate power based on a count value."""
    if not isinstance(count, int):
        raise InvalidCountError("Count must be an integer.")
    if count < 0:
        raise InvalidCountError(f"Cannot have negative count: {count}")
    if count > 429496729:  # Ensure result won't exceed u32::MAX after multiplication and random addition
        raise InvalidCountError(f"Count too large: {count}")
    return count * 10 + random.randint(1, 100)

def calculate_category_bonus(category_name: str, base_power: int) -> int:
    """Calculate a category bonus for a given power level."""
    category_bonuses = {
        "Alpha": 1.2,
        "Beta": 1.1,
        "Gamma": 1.3,
        "Delta": 1.15,
        "Epsilon": 1.1,
        "Zeta": 1.25
    }
    if not isinstance(base_power, int):
        raise InvalidPowerError("Power must be an integer.")
    if category_name not in category_bonuses:
        raise InvalidCategoryError(f"Unknown category: {category_name}")
    if base_power < 0:
        raise InvalidPowerError(f"Cannot have negative power: {base_power}")
    if base_power > 3435973836:  # Ensure result won't exceed u32::MAX after applying max bonus (1.3)
        raise InvalidPowerError(f"Power too high: {base_power}")
    return int(base_power * category_bonuses[category_name])

def calculate_competition_result(attacker_power: int, defender_power: int) -> bool:
    """Calculate the result of a competition between two entities."""
    if not isinstance(attacker_power, int) or not isinstance(defender_power, int):
        raise InvalidPowerError("Power values must be integers.")
    if attacker_power < 0 or defender_power < 0:
        raise InvalidPowerError("Cannot have negative power in a competition.")
    if attacker_power > 4294967275 or defender_power > 4294967275:  # u32::MAX - 20 for the random roll
        raise InvalidPowerError("Power too high.")
    # Add some random factor for variety
    attacker_roll = random.randint(1, 20)
    defender_roll = random.randint(1, 20)
    return (attacker_power + attacker_roll) > (defender_power + defender_roll)

def calculate_reward(currency: int, points: int) -> Dict[str, int]:
    """Calculate the total reward from a competition."""
    if not isinstance(currency, int) or not isinstance(points, int):
        raise InvalidCurrencyError("Currency and points must be integers.")
    if currency < 0:
        raise InvalidCurrencyError(f"Cannot have negative currency: {currency}")
    if points < 0:
        raise InvalidCurrencyError("Cannot have negative points.")
    if currency > 4294967295 - (points * 15):  # Ensure we won't overflow with max bonus
        raise InvalidCurrencyError("Currency too high.")
    # More points means more bonus!
    bonus = points * random.randint(5, 15)
    total = currency + bonus
    return {"total_currency": total, "bonus_currency": bonus}

def handle_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle incoming messages from the Rust code.
    Args:
        message: Dictionary containing message data
    Returns:
        Dictionary containing response data
    """
    if not isinstance(message, dict):
        raise LogicError("Invalid message format.")
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
        power = calculate_power(count_val)
        return {"Power": {"power": power}}
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
        bonus = calculate_category_bonus(str(category_name), base_power_val)
        return {"CategoryBonus": {"bonus": bonus}}
    elif "CalculateCompetitionResult" in message or "CalculateScrapResult" in message:
        key = "CalculateCompetitionResult" if "CalculateCompetitionResult" in message else "CalculateScrapResult"
        params = message[key]
        attacker_power = params.get("attacker_power")
        defender_power = params.get("defender_power")
        if attacker_power is None or defender_power is None:
            raise LogicError("Missing required field: attacker_power or defender_power")
        try:
            attacker_val = int(attacker_power)
            defender_val = int(defender_power)
        except Exception:
            raise LogicError(f"Invalid value for attacker_power or defender_power: {attacker_power}, {defender_power}")
        if attacker_val < 0 or defender_val < 0:
            raise LogicError(f"attacker_power and defender_power must be non-negative, got {attacker_val}, {defender_val}")
        result = calculate_competition_result(attacker_val, defender_val)
        return {"CompetitionResult": {"victory": result}}
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
        result = calculate_reward(currency_val, points_val)
        return {"RewardResult": {"total_currency": result["total_currency"], "bonus_currency": result["bonus_currency"]}}
    else:
        raise LogicError("Invalid message type.")
