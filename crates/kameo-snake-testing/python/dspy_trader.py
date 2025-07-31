import sys, os
import dspy
import mlflow.dspy
import asyncio
import os
import logging
import threading
import mlflow

# Configure MLflow for OTEL integration
mlflow.set_experiment("dspy-otel-integration")

# Defer DSPy autologging setup until after OTEL context is established
# This will be called from the handle_message function
def setup_dspy_autologging():
# Enable MLflow DSPy autologging with OTEL
mlflow.dspy.autolog(
    log_traces=True,
    log_traces_from_compile=False,
    log_traces_from_eval=True,
    log_compiles=False,
    log_evals=False,
    disable=False,
    silent=False
)
print("[DEBUG] DSPy autologging configured with OTEL context")

logging.getLogger("mlflow").setLevel(logging.DEBUG)

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

# Async order item tool for TraderMessage
async def order_item_tool(item: str, currency: int) -> str:
        import kameo
        print(f"[DEBUG] order_item TOOL called with item={item}, currency={currency}")
        
    # Set callback attributes on the tool span
            callback_result = await kameo.callback_handle({"value": currency})
            print(f"[DEBUG] order_item TOOL got callback_result={callback_result}")
        
        result = f"Order for {item} ({currency} units) complete: callback returned {callback_result}"
        return result

order_item = dspy.Tool(order_item_tool)

# --- ReAct agent setup ---
class OrderSignature(dspy.Signature):
    """Signature for ordering an item."""
    item = dspy.InputField(desc="The item to order")
    currency = dspy.InputField(desc="The amount of currency to use")

try:
    lm = dspy.LM(
        model="openrouter/deepseek/deepseek-r1:free",
        api_key=os.environ.get("OPENROUTER_API_KEY"),
    )
    dspy.configure(lm=lm)
    print("[DEBUG] DSPy LM initialized successfully!")
    react_agent = dspy.ReAct(signature=OrderSignature, tools=[order_item])
    print("[DEBUG] DSPy ReAct agent initialized!")
except Exception as e:
    print(f"[DEBUG] DSPy LM/ReAct initialization failed: {e}")
    react_agent = None

class TraderAgent(dspy.Module):
    def __init__(self):
        super().__init__()
        self.react_agent = react_agent

    async def forward(self, message):
            print(f"[DEBUG] TraderAgent.forward ENTRY: message={message}")
            
            if self.react_agent is None:
                return {"Error": {"error": "ReAct agent not initialized"}}
                
            if "OrderDetails" in message:
                item = message["OrderDetails"].get("item")
                currency = message["OrderDetails"].get("currency")
                
                try:
                        result = await self.react_agent.aforward(item=item, currency=currency)
                        print(f"[DEBUG] TraderAgent ReAct result: {result}")
                        return {"OrderResult": {"result": str(result)}}
                except Exception as e:
                    print(f"[DEBUG] TraderAgent exception: {e}")
                    return {"Error": {"error": str(e)}}
            elif "CallbackRoundtrip" in message:
                return {"Error": {"error": "CallbackRoundtrip not implemented in ReAct agent"}}
            else:
                print(f"[DEBUG] TraderAgent unknown message: {message}")
                return {"Error": {"error": f"Unknown message: {message}"}}

async def handle_message(message):
    # Call deferred DSPy autologging setup
    setup_dspy_autologging()
    print("[DEBUG] DSPy autologging configured with OTEL context")
    
    # Process the message
    print(f"[DEBUG] handle_message ENTRY: thread={threading.get_ident()} message={message}")
    
    # Instantiate the trader agent
    agent = TraderAgent()
        
    # Process the message
    result = await agent.forward(message)
        
    print(f"[DEBUG] handle_message EXIT: result={result}")
        
    return result
    


