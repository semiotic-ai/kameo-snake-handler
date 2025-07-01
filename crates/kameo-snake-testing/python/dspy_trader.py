import sys, os
print("[TEST DEBUG] PYTHON EXECUTABLE:", sys.executable)
print("[TEST DEBUG] PYTHONPATH:", os.environ.get("PYTHONPATH"))
print("[TEST DEBUG] sys.path:", sys.path)
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
class OrderDetails:
    item: str
    currency: int

@dataclass
class TraderMessage:
    OrderDetails: OrderDetails = None

@dataclass
class TraderResponse:
    OrderResult: str = None
    Error: str = None


print(f"[TEST DEBUG] OPENROUTER_API_KEY: {os.environ.get('OPENROUTER_API_KEY')}")

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
    print("[TEST DEBUG] DSPy LM initialized successfully!")
except Exception as e:
    print(f"[TEST DEBUG] DSPy LM initialization failed: {e}")

# Only one tool for now: order_item
async def order_item(item: str, currency: int):
    print(f"[TEST DEBUG] order_item called with item={item}, currency={currency}")
    import kameo
    try:
        result = await kameo.callback_handle.ask({"OrderDetails": {"item": item, "currency": currency}})
        print(f"[TEST DEBUG] order_item result: {result}")
        return f"Order for {item} ({currency} units) complete: {result}"
    except Exception as e:
        print(f"[TEST DEBUG] order_item exception: {e}")
        raise

async def handle_message(message):
    import asyncio
    import sys
    print(f"[TEST DEBUG] handle_message ENTRY: thread={threading.get_ident()} sys.version={sys.version}")
    try:
        loop = asyncio.get_running_loop()
        print(f"[TEST DEBUG] Running loop: {loop} thread={threading.get_ident()} loop.is_running={loop.is_running()} loop.id={id(loop)}")
    except RuntimeError:
        print(f"[TEST DEBUG] No running event loop! thread={threading.get_ident()}")
    try:
        task = asyncio.current_task()
        print(f"[TEST DEBUG] Current asyncio task: {task}")
    except Exception as e:
        print(f"[TEST DEBUG] No current asyncio task: {e}")
    print(f"[TEST DEBUG] handle_message called with: {message}")
    try:
        if "OrderDetails" in message:
            args = message["OrderDetails"]
            result = await order_item(args["item"], args["currency"])
            print(f"[TEST DEBUG] handle_message returning: {{'OrderResult': result}}")
            return {"OrderResult": result}
        else:
            print(f"[TEST DEBUG] handle_message unknown message: {message}")
            return {"Error": f"Unknown message: {message}"}
    except Exception as e:
        print(f"[TEST DEBUG] handle_message exception: {e}")
        return {"Error": str(e)}

# if __name__ == "__main__":
#     main()

print("[TEST DEBUG] MODULE LOADED:", __name__, "sys.path:", sys.path) 