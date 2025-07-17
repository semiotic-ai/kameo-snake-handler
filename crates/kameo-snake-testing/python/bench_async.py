import asyncio
import random
import time

async def handle_bench_message(message):
    import kameo
    py_sleep = message.get('py_sleep_ms', 100) / 1000.0
    msg_id = message.get('id', -1)
    start = time.time()
    print(f"[PYTHON ASYNC] START id={msg_id} t={start:.6f}")
    await asyncio.sleep(py_sleep)
    cb = {'id': message['id'], 'rust_sleep_ms': message.get('rust_sleep_ms', 100)}
    await kameo.callback_handle(cb)
    end = time.time()
    print(f"[PYTHON ASYNC] END   id={msg_id} t={end:.6f} dt={end-start:.3f}")
    # Return a valid BenchResponse variant for Rust
    return {"CallbackRoundtripResult": {"value": message["id"]}} 