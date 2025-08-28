import sys
import os
import asyncio
import logging
import threading
from typing import Dict, Any, AsyncGenerator

# DSPy / MLflow integrations
import dspy
import mlflow
import mlflow.dspy

# Configure MLflow for OTEL integration
mlflow.set_experiment("dspy-otel-integration")

def setup_dspy_autologging() -> None:
    mlflow.dspy.autolog(
        log_traces=True,
        log_traces_from_compile=False,
        log_traces_from_eval=True,
        log_compiles=False,
        log_evals=False,
        disable=False,
        silent=False,
    )
    print("[DEBUG] DSPy autologging configured with OTEL context")

logging.getLogger("mlflow").setLevel(logging.DEBUG)

# Generated invocation/callback modules
from . import invocation_generated_types as gen
from . import callback_generated_types as callbacks

print(f"[DEBUG] OPENROUTER_API_KEY: {os.environ.get('OPENROUTER_API_KEY')}")

# Async order item tool for TraderMessage using strongly-typed callback
async def order_item_tool(item: str, currency: int) -> str:
    from .callback_request_types import TraderCallbackMessage
    print(f"[DEBUG] order_item TOOL called with item={item}, currency={currency}")
    iterator: AsyncGenerator[gen.TraderResponse, None] = callbacks.trader__trader_callback(
        TraderCallbackMessage(value=int(currency))
    )
    callback_result: gen.TraderResponse | None = None
    async for response in iterator:
        print(f"[DEBUG] order_item TOOL callback response: {response}")
        callback_result = response
        break
    return f"Order for {item} ({currency} units) complete: callback returned {callback_result}"

order_item = dspy.Tool(order_item_tool)

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
    def __init__(self) -> None:
        super().__init__()
        self.react_agent = react_agent

    async def forward(self, *, item: str, currency: int) -> str:
        print(f"[DEBUG] TraderAgent.forward ENTRY: item={item}, currency={currency}")
        if self.react_agent is None:
            return "ReAct agent not initialized"
        try:
            result = await self.react_agent.aforward(item=item, currency=currency)
            print(f"[DEBUG] TraderAgent ReAct result: {result}")
            return str(result)
        except Exception as e:
            print(f"[DEBUG] TraderAgent exception: {e}")
            return f"Error: {e}"

async def handle_message(message: Dict[str, Any]) -> Dict[str, Any]:
    # Call deferred DSPy autologging setup
    setup_dspy_autologging()
    print("[DEBUG] DSPy autologging configured with OTEL context")

    print(f"[DEBUG] handle_message ENTRY: thread={threading.get_ident()} message={message}")

    # Parse to generated TraderMessage
    tm: gen.TraderMessage = gen.from_wire_trader_message(message)

    agent = TraderAgent()

    async def on_order_details(item: str, currency: int) -> gen.TraderResponse:
        res = await agent.forward(item=str(item), currency=int(currency))
        if res.startswith("Error:"):
            return gen.trader_response_error(error=res)
        return gen.trader_response_order_result(result=res)

    # Dispatch via generated matcher and serialize response
    resp = gen.match_trader_message(
        tm,
        OrderDetails=lambda item, currency: on_order_details(str(item), int(currency)),
        default=lambda _m: gen.trader_response_error(error="Unknown message"),
    )
    result = await resp if asyncio.iscoroutine(resp) else resp
    print(f"[DEBUG] handle_message EXIT: result={result}")
    return gen.to_wire_trader_response(result)


