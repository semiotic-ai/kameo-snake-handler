import sys, os
print("[ORKY DEBUG] PYTHON EXECUTABLE:", sys.executable)
print("[ORKY DEBUG] PYTHONPATH:", os.environ.get("PYTHONPATH"))
print("[ORKY DEBUG] sys.path:", sys.path)
import dspy
import mlflow.dspy
import asyncio
import os
import logging
import threading


# --- Kameo message types matching Rust enums ---
from dataclasses import dataclass
from typing import Any, Dict, Union

@dataclass
class OrderKustomGubbinz:
    item: str
    teef: int

@dataclass
class OrkyTraderMessage:
    OrderKustomGubbinz: OrderKustomGubbinz = None

@dataclass
class OrkyTraderResponse:
    OrderResult: str = None
    Error: str = None


print(f"[ORKY DEBUG] OPENROUTER_API_KEY: {os.environ.get('OPENROUTER_API_KEY')}")

import dspy
import mlflow.dspy
logging.getLogger("mlflow").setLevel(logging.DEBUG)
try:
    lm = dspy.LM(
        model="openrouter/openai/gpt-3.5-turbo",
        api_key=os.environ.get("OPENROUTER_API_KEY"),
        allow_tool_async_sync_conversion=True,
    )
    dspy.configure(lm=lm)
    print("[ORKY DEBUG] DSPy LM initialized successfully!")
except Exception as e:
    print(f"[ORKY DEBUG] DSPy LM initialization failed: {e}")

# Only one tool for now: order_kustom_gubbinz
async def order_kustom_gubbinz(item: str, teef: int):
    print(f"[ORKY DEBUG] order_kustom_gubbinz called with item={item}, teef={teef}")
    import kameo
    try:
        result = await kameo.callback_handle.ask({"OrderKustomGubbinz": {"item": item, "teef": teef}})
        print(f"[ORKY DEBUG] order_kustom_gubbinz result: {result}")
        return f"Order for {item} ({teef} teef) complete: {result}"
    except Exception as e:
        print(f"[ORKY DEBUG] order_kustom_gubbinz exception: {e}")
        raise

async def handle_message(message):
    import asyncio
    import sys
    print(f"[ORKY DEBUG] handle_message ENTRY: thread={threading.get_ident()} sys.version={sys.version}")
    try:
        loop = asyncio.get_running_loop()
        print(f"[ORKY DEBUG] Running loop: {loop} thread={threading.get_ident()} loop.is_running={loop.is_running()} loop.id={id(loop)}")
    except RuntimeError:
        print(f"[ORKY DEBUG] No running event loop! thread={threading.get_ident()}")
    try:
        task = asyncio.current_task()
        print(f"[ORKY DEBUG] Current asyncio task: {task}")
    except Exception as e:
        print(f"[ORKY DEBUG] No current asyncio task: {e}")
    print(f"[ORKY DEBUG] handle_message called with: {message}")
    try:
        if "OrderKustomGubbinz" in message:
            args = message["OrderKustomGubbinz"]
            result = await order_kustom_gubbinz(args["item"], args["teef"])
            print(f"[ORKY DEBUG] handle_message returning: {{'OrderResult': result}}")
            return {"OrderResult": result}
        else:
            print(f"[ORKY DEBUG] handle_message unknown message: {message}")
            return {"Error": f"Unknown message: {message}"}
    except Exception as e:
        print(f"[ORKY DEBUG] handle_message exception: {e}")
        return {"Error": str(e)}

# if __name__ == "__main__":
#     main()

print("[ORKY DEBUG] MODULE LOADED:", __name__, "sys.path:", sys.path) 