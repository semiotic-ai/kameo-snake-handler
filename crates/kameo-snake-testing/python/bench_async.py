import asyncio
import random
import time

async def handle_bench_message(message):
    import kameo
    py_sleep = message.get('py_sleep_ms', 100) / 1000.0
    msg_id = message.get('id', -1)
    start = time.time()
    await asyncio.sleep(py_sleep)
    cb = {'id': message['id'], 'rust_sleep_ms': message.get('rust_sleep_ms', 100)}
    callback_iterator = await kameo.bench.BenchCallback(cb)
    
    # Process the callback response
    callback_result = None
    async for response in callback_iterator:
        callback_result = response
        break  # Just take the first response for now
    end = time.time()
    # Return a valid BenchResponse variant for Rust
    return {"CallbackRoundtripResult": {"value": message["id"]}} 