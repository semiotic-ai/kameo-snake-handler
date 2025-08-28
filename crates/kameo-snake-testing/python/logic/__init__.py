"""
General Logic Module (packaged)

Uses generated types written to this package directory at child startup.
"""

import random
from typing import Dict, Any
from . import invocation_generated_types as gen


class LogicError(Exception):
    pass


class InvalidCountError(LogicError):
    pass


class InvalidCategoryError(LogicError):
    pass


class InvalidCurrencyError(LogicError):
    pass


class InvalidPowerError(LogicError):
    pass


def calculate_power(count: int) -> int:
    if not isinstance(count, int):
        raise InvalidCountError("Count must be an integer.")
    if count < 0:
        raise InvalidCountError(f"Cannot have negative count: {count}")
    if count > 429496729:
        raise InvalidCountError(f"Count too large: {count}")
    return count * 10 + random.randint(1, 100)


def calculate_category_bonus(category_name: str, base_power: int) -> int:
    category_bonuses = {
        "Alpha": 1.2,
        "Beta": 1.1,
        "Gamma": 1.3,
        "Delta": 1.15,
        "Epsilon": 1.1,
        "Zeta": 1.25,
    }
    if not isinstance(base_power, int):
        raise InvalidPowerError("Power must be an integer.")
    if category_name not in category_bonuses:
        raise InvalidCategoryError(f"Unknown category: {category_name}")
    if base_power < 0:
        raise InvalidPowerError(f"Cannot have negative power: {base_power}")
    if base_power > 3435973836:
        raise InvalidPowerError(f"Power too high: {base_power}")
    return int(base_power * category_bonuses[category_name])


def calculate_competition_result(attacker_power: int, defender_power: int) -> bool:
    if not isinstance(attacker_power, int) or not isinstance(defender_power, int):
        raise InvalidPowerError("Power values must be integers.")
    if attacker_power < 0 or defender_power < 0:
        raise InvalidPowerError("Cannot have negative power in a competition.")
    if attacker_power > 4294967275 or defender_power > 4294967275:
        raise InvalidPowerError("Power too high.")
    attacker_roll = random.randint(1, 20)
    defender_roll = random.randint(1, 20)
    return (attacker_power + attacker_roll) > (defender_power + defender_roll)


def calculate_reward(currency: int, points: int) -> Dict[str, int]:
    if not isinstance(currency, int) or not isinstance(points, int):
        raise InvalidCurrencyError("Currency and points must be integers.")
    if currency < 0:
        raise InvalidCurrencyError(f"Cannot have negative currency: {currency}")
    if points < 0:
        raise InvalidCurrencyError("Cannot have negative points.")
    if currency > 4294967295 - (points * 15):
        raise InvalidCurrencyError("Currency too high.")
    bonus = points * random.randint(5, 15)
    total = currency + bonus
    return {"total_currency": total, "bonus_currency": bonus}


def handle_message(message: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(message, dict):
        raise LogicError("Invalid message format.")

    tm = gen.from_wire_test_message(message)

    def on_calc_power(count: int) -> gen.TestResponse:
        if count <= 0:
            raise LogicError(f"count/boyz_count must be positive, got {count}")
        power = calculate_power(int(count))
        return gen.test_response_power(power=int(power))

    def on_category_bonus(category_name: str, base_power: int) -> gen.TestResponse:
        if (not isinstance(category_name, str)) or (category_name.strip() == ""):
            raise LogicError("Missing or empty required field: category_name/klan_name")
        if base_power <= 0:
            raise LogicError(f"base_power must be positive, got {base_power}")
        bonus = calculate_category_bonus(str(category_name), int(base_power))
        return gen.test_response_category_bonus(bonus=int(bonus))

    def on_comp_result(attacker_power: int, defender_power: int) -> gen.TestResponse:
        if attacker_power < 0 or defender_power < 0:
            raise LogicError(
                f"attacker_power and defender_power must be non-negative, got {attacker_power}, {defender_power}"
            )
        victory = calculate_competition_result(int(attacker_power), int(defender_power))
        return gen.test_response_competition_result(victory=bool(victory))

    def on_reward(currency: int, points: int) -> gen.TestResponse:
        if currency < 0 or points < 0:
            raise LogicError(
                f"currency/teef and points/victory_points must be non-negative, got {currency}, {points}"
            )
        result = calculate_reward(int(currency), int(points))
        return gen.test_response_reward_result(
            total_currency=int(result["total_currency"]),
            bonus_currency=int(result["bonus_currency"]),
        )

    resp = gen.match_test_message(
        tm,
        CalculatePower=lambda count: on_calc_power(int(count)),
        CalculateCategoryBonus=lambda category_name, base_power: on_category_bonus(str(category_name), int(base_power)),
        CalculateCompetitionResult=lambda attacker_power, defender_power: on_comp_result(int(attacker_power), int(defender_power)),
        CalculateReward=lambda currency, points: on_reward(int(currency), int(points)),
        default=lambda _m: (_ for _ in ()).throw(LogicError("Invalid message type (no matching tag)")),
    )
    return gen.to_wire_test_response(resp)


