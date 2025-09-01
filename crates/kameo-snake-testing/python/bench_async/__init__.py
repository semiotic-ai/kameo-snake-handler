import asyncio
import random
import time
import sys
import logging

from typing import Dict, Any, AsyncGenerator
from . import invocation_generated_types as gen
from . import callback_generated_types as callbacks

async def handle_bench_message(message: Dict[str, Any]) -> Dict[str, Any]:
    import kameo  # type: ignore[import-not-found]
    py_sleep = message.get('py_sleep_ms', 100) / 1000.0
    await asyncio.sleep(py_sleep)

    bm = gen.from_wire_bench_message(message)

    # Dispatch: this module only handles a struct BenchMessage, so just perform callback and return a generated response
    from .callback_request_types import BenchCallback
    callback_iterator: AsyncGenerator[gen.BenchResponse, None] = callbacks.bench__bench_callback(
        BenchCallback(id=int(bm.id), rust_sleep_ms=int(getattr(bm, 'rust_sleep_ms', 0)))
    )

    # Process the callback response (gracefully handle parent disconnect)
    try:
        async for _ in callback_iterator:
            break
    except Exception as e:
        # Parent may have closed early; treat as best-effort fire-and-forget for bench
        logging.warning("[bench_async] callback iteration ended early: %s", e, exc_info=True)

    return gen.to_wire_bench_response(gen.bench_response_callback_roundtrip_result(value=int(bm.id)))


