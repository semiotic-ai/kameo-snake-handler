"""Async logic module for handling various calculations (packaged)."""

import json
import sys
import logging
import asyncio
from typing import Dict, Any
import os
import importlib
from logic import (
    calculate_power, calculate_category_bonus,
    calculate_competition_result, calculate_reward,
    LogicError,
)
import kameo  # type: ignore[import-not-found]

# Generated invocation/callback modules for this package
from . import invocation_generated_types as gen
from . import callback_generated_types as cb

# Generated callback types are required for this package

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='[PYTHON ASYNC] %(levelname)s %(message)s')

async def process_async_calculation(func, *args) -> Any:
    return func(*args)

async def handle_message_async(message: Dict[str, Any]) -> Dict[str, Any]:
    try:
        import opentelemetry.trace as trace
        tracer = trace.get_tracer("kameo_snake_handler")

        with tracer.start_as_current_span("handle_message") as span:
            span.set_attribute("message.type", str(type(message)))
            span.set_attribute("message.keys", str(list(message.keys())))

            logging.info(f"Received message: {message}")
            # Parse to generated TestMessage
            tm = gen.from_wire_test_message(message)

            async def on_calc_power(count: int) -> gen.TestResponse:
                if count <= 0:
                    raise LogicError(f"count/boyz_count must be positive, got {count}")
                result = await process_async_calculation(calculate_power, count)
                return gen.test_response_power(power=int(result))

            async def on_category_bonus(category_name: str, base_power: int) -> gen.TestResponse:
                if (not isinstance(category_name, str)) or (category_name.strip() == ""):
                    raise LogicError("Missing or empty required field: category_name/klan_name")
                if base_power <= 0:
                    raise LogicError(f"base_power must be positive, got {base_power}")
                result = await process_async_calculation(calculate_category_bonus, category_name, base_power)
                return gen.test_response_category_bonus(bonus=int(result))

            async def on_comp_result(attacker_power: int, defender_power: int) -> gen.TestResponse:
                if attacker_power < 0 or defender_power < 0:
                    raise LogicError(
                        f"attacker_power and defender_power must be non-negative, got {attacker_power}, {defender_power}"
                    )
                result = await process_async_calculation(calculate_competition_result, attacker_power, defender_power)
                return gen.test_response_competition_result(victory=bool(result))

            async def on_reward(currency: int, points: int) -> gen.TestResponse:
                if currency < 0 or points < 0:
                    raise LogicError(
                        f"currency/teef and points/victory_points must be non-negative, got {currency}, {points}"
                    )
                result = await process_async_calculation(calculate_reward, currency, points)
                return gen.test_response_reward_result(
                    total_currency=int(result["total_currency"]),
                    bonus_currency=int(result["bonus_currency"]),
                )

            async def on_callback_roundtrip(value: int) -> gen.TestResponse:
                from .callback_request_types import TestCallbackMessage
                iterator = cb.test__test_callback(TestCallbackMessage(value=int(value)))
                async for _ in iterator:
                    break
                return gen.test_response_callback_roundtrip_result(value=int(value) + 1)

            # Match on the generated TestMessage and dispatch to handlers
            match_result = await gen.match_test_message(
                tm,
                CalculatePower=lambda count: on_calc_power(int(count)),
                CalculateCategoryBonus=lambda category_name, base_power: on_category_bonus(str(category_name), int(base_power)),
                CalculateCompetitionResult=lambda attacker_power, defender_power: on_comp_result(int(attacker_power), int(defender_power)),
                CalculateReward=lambda currency, points: on_reward(int(currency), int(points)),
                CallbackRoundtrip=lambda value: on_callback_roundtrip(int(value)),
                default=lambda _m: (_ for _ in ()).throw(LogicError("Invalid message type")),
            )

            # Serialize generated TestResponse to wire dict
            return gen.to_wire_test_response(match_result)
            raise LogicError(f"Unknown message type: {message}")
    except Exception as e:
        import traceback
        logging.error(f"Exception: {e}")
        traceback.print_exc()
        raise


