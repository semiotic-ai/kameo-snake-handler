import sys, os
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

print(f"[DEBUG] OPENROUTER_API_KEY: {os.environ.get('OPENROUTER_API_KEY')}")

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
    print("[DEBUG] DSPy LM initialized successfully!")
except Exception as e:
    print(f"[DEBUG] DSPy LM initialization failed: {e}")


# Async order item tool for TraderMessage
async def order_item_tool(item: str, currency: int) -> str:
    import kameo
    print(f"[DEBUG] order_item TOOL called with item={item}, currency={currency}")
    # Call the callback with the correct protocol: {"value": currency}
    callback_result = await kameo.callback_handle({"value": currency})
    print(f"[DEBUG] order_item TOOL got callback_result={callback_result}")
    return f"Order for {item} ({currency} units) complete: callback returned {callback_result}"

order_item = dspy.Tool(order_item_tool)

# Async callback roundtrip tool for TraderCallbackMessage
async def callback_roundtrip_tool(message: dict) -> dict:
    import kameo
    value = message["CallbackRoundtrip"]["value"]
    print(f"[DEBUG] callback_roundtrip TOOL called with value={value}")
    result = await kameo.callback_handle({"value": value})
    print(f"[DEBUG] callback_roundtrip TOOL got result={result}")
    return {"CallbackRoundtripResult": {"value": result}}

callback_roundtrip = dspy.Tool(callback_roundtrip_tool)

class TraderAgent(dspy.Module):
    def __init__(self):
        super().__init__()
        self.order_item = order_item
        self.callback_roundtrip = callback_roundtrip

    async def forward(self, message):
        print(f"[DEBUG] TraderAgent.forward ENTRY: message={{message}}")
        if "OrderDetails" in message:
            try:
                item = message["OrderDetails"]["item"]
                currency = message["OrderDetails"]["currency"]
                result = await self.order_item.acall(item=item, currency=currency)
                print(f"[DEBUG] TraderAgent returning: {{'OrderResult': {{'result': result}}}}")
                return {"OrderResult": {"result": result}}
            except Exception as e:
                print(f"[DEBUG] TraderAgent exception: {e}")
                return {"Error": {"error": str(e)}}
        elif "CallbackRoundtrip" in message:
            return await self.callback_roundtrip.acall(message)
        else:
            print(f"[DEBUG] TraderAgent unknown message: {message}")
            return {"Error": {"error": f"Unknown message: {message}"}}

async def handle_message(message):
    print(f"[DEBUG] handle_message ENTRY: thread={{threading.get_ident()}} message={{message}}")
    agent = TraderAgent()
    return await agent.forward(message)

# if __name__ == "__main__":
#     main()

print("[DEBUG] MODULE LOADED:", __name__, "sys.path:", __import__('sys').path) 