"""
Test for dynamic callbacks - Python calls different callback handlers by name (packaged).
"""

import asyncio
import logging
import sys

from typing import Dict, Any, AsyncGenerator
from . import invocation_generated_types as gen
from . import callback_generated_types as callbacks  # noqa: F401

import kameo  # type: ignore[import-not-found]

# Configure logging: set levels only, let Rust bridge handle handlers
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def handle_message_async(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Test handler using generated types only. Parses wire -> typed message, dispatches via match_*,
    invokes generated callbacks with generated request dataclasses, and returns a typed response serialized to wire.
    """
    try:
        logger.info(f"📨 Received message: {message}")

        # Parse incoming wire dict into generated union type
        tm = gen.from_wire_test_message(message)

        from .callback_request_types import (
            ComplexCallbackMessage,
            TestCallbackMessage,
            TraderCallbackMessage,
        )

        async def on_callback_roundtrip(value: int) -> gen.TestResponse:
            logger.info(f"🔄 Starting dynamic callback demonstration with value={value}")

            # 1) Streaming callback
            logger.info("📞 Calling generated test__streaming_callback()")
            streaming_iterator: AsyncGenerator[gen.TestResponse, None] = callbacks.test__streaming_callback(
                ComplexCallbackMessage(
                    value=int(value),
                    labels=['alpha', 'beta', 'gamma'],
                    metadata={'foo': 'bar', 'id': str(value)},
                    dimensions={'width': 3, 'height': 5},
                    events=[
                        {'kind': 'start', 'weight': 1},
                        {'kind': 'tick', 'weight': 2},
                        {'kind': 'stop', 'weight': 1},
                    ],
                )
            )
            async for _ in streaming_iterator:
                pass

            # 2) Basic callback
            logger.info("📞 Calling generated basic__test_callback()")
            basic_iterator: AsyncGenerator[gen.TestResponse, None] = callbacks.basic__test_callback(
                TestCallbackMessage(value=int(value + 10))
            )
            async for _ in basic_iterator:
                pass

            # 3) Trader callback
            logger.info("📞 Calling generated trader__trader_callback()")
            trader_iterator: AsyncGenerator[gen.TestResponse, None] = callbacks.trader__trader_callback(
                TraderCallbackMessage(value=int(value + 20))
            )
            async for _ in trader_iterator:
                pass

            return gen.test_response_callback_roundtrip_result(value=int(value) + 1)

        # Dispatch via generated matcher
        resp = gen.match_test_message(
            tm,
            CallbackRoundtrip=lambda v: on_callback_roundtrip(int(v)),
            default=lambda _m: (_ for _ in ()).throw(ValueError("Unknown message type")),
        )
        resp = await resp if asyncio.iscoroutine(resp) else resp
        return gen.to_wire_test_response(resp)

    except Exception as e:
        import traceback
        logger.error(f"❌ Exception: {e}")
        traceback.print_exc()
        raise


