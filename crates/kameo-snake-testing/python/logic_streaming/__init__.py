"""Async streaming logic module (packaged)."""

import json
import sys
import logging
import asyncio
import random
import time
from typing import Dict, Any, AsyncGenerator
import os
import inspect

# Generated types for this package
from . import invocation_generated_types as gen
# Explicit generated callback types import for this module's package (may be unused here)
try:
    from . import callback_generated_types as _generated_stubs  # noqa: F401
except Exception:
    _generated_stubs = None  # noqa: F401

from logic import LogicError
import kameo  # type: ignore[import-not-found]

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='[PYTHON STREAMING] %(levelname)s %(message)s')

async def fibonacci_generator(count: int) -> AsyncGenerator[gen.TestResponse, None]:
    a, b = 0, 1
    for i in range(count):
        yield gen.test_response_stream_item(index=int(i), value=int(a))
        a, b = b, a + b
        await asyncio.sleep(0.01)

async def random_numbers_generator(count: int, max_value: int) -> AsyncGenerator[gen.TestResponse, None]:
    for i in range(count):
        value = random.randint(1, max_value)
        yield gen.test_response_stream_item(index=int(i), value=int(value))
        await asyncio.sleep(0.01)

async def delayed_stream_generator(count: int, delay_ms: int) -> AsyncGenerator[gen.TestResponse, None]:
    for i in range(count):
        yield gen.test_response_stream_item(index=int(i), value=int(i * 10))
        await asyncio.sleep(delay_ms / 1000.0)

async def error_stream_generator(count: int, error_at: int = None) -> AsyncGenerator[gen.TestResponse, None]:
    for i in range(count):
        if error_at is not None and i == error_at:
            yield gen.test_response_stream_error(index=int(i), error=f"Simulated error at index {i}")
            return
        yield gen.test_response_stream_item(index=int(i), value=int(i * 100))
        await asyncio.sleep(0.01)

async def large_dataset_generator(count: int) -> AsyncGenerator[gen.TestResponse, None]:
    for i in range(count):
        data_size = random.randint(100, 1000)
        yield gen.test_response_stream_item(index=int(i), value=int(data_size))
        await asyncio.sleep(0.005)

async def handle_message_streaming(message: gen.TestMessage) -> AsyncGenerator[gen.TestResponse, None]:
    try:
        logging.info(f"Received streaming message: {message}")
        # Parse to typed union
        tm = gen.from_wire_test_message(message)

        async def on_stream_fibonacci(count: int):
            nonlocal message
            if count <= 0:
                raise LogicError("Count must be positive for Fibonacci stream")
            if count > 1000:
                raise LogicError("Count too large for Fibonacci stream")
            async for item in fibonacci_generator(count):
                yield gen.to_wire_test_response(item)

        async def on_stream_random_numbers(count: int, max_value: int):
            if count <= 0:
                raise LogicError("Count must be positive for random numbers stream")
            if max_value <= 0:
                raise LogicError("Max value must be positive for random numbers stream")
            if count > 10000:
                raise LogicError("Count too large for random numbers stream")
            async for item in random_numbers_generator(count, max_value):
                yield gen.to_wire_test_response(item)

        async def on_stream_with_delays(count: int, delay_ms: int):
            if count <= 0:
                raise LogicError("Count must be positive for delayed stream")
            if delay_ms > 5000:
                raise LogicError("Delay too large for delayed stream")
            async for item in delayed_stream_generator(count, delay_ms):
                yield gen.to_wire_test_response(item)

        async def on_stream_with_errors(count: int, error_at: int | None):
            if count <= 0:
                raise LogicError("Count must be positive for error stream")
            if error_at is not None and (error_at < 0 or error_at >= count):
                raise LogicError("Error index out of range")
            async for item in error_stream_generator(count, error_at):
                yield gen.to_wire_test_response(item)

        async def on_stream_large_dataset(count: int):
            if count <= 0:
                raise LogicError("Count must be positive for large dataset stream")
            if count > 100000:
                raise LogicError("Count too large for large dataset stream")
            async for item in large_dataset_generator(count):
                yield gen.to_wire_test_response(item)

        # Use generated matcher to route the stream and forward async generator items
        selected = gen.match_test_message(
            tm,
            StreamFibonacci=lambda count: on_stream_fibonacci(int(count)),
            StreamRandomNumbers=lambda count, max_value: on_stream_random_numbers(int(count), int(max_value)),
            StreamWithDelays=lambda count, delay_ms: on_stream_with_delays(int(count), int(delay_ms)),
            StreamWithErrors=lambda count, error_at: on_stream_with_errors(int(count), None if error_at is None else int(error_at)),
            StreamLargeDataset=lambda count: on_stream_large_dataset(int(count)),
            default=lambda _m: (_ for _ in ()).throw(LogicError("Unknown streaming message type")),
        )
        iterator = await selected if inspect.isawaitable(selected) else selected
        async for item in iterator:
            yield item
    except Exception as e:
        import traceback
        logging.error(f"Exception in streaming handler: {e}")
        traceback.print_exc()
        yield gen.to_wire_test_response(gen.test_response_stream_error(index=0, error=str(e)))

logging.debug("MODULE LOADED: %s", __name__)


