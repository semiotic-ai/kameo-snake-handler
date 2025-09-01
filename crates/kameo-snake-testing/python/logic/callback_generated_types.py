# Generated callback for callback_generated_types
# pyright: reportMissingImports=false
from __future__ import annotations
from typing import AsyncGenerator, TYPE_CHECKING, Any
import dataclasses
import inspect
import kameo  # type: ignore[import-not-found]
from . import invocation_generated_types as inv
if TYPE_CHECKING:
    from .callback_request_types import *  # for editor type awareness
    from .invocation_generated_types import *  # for editor type awareness

def _to_wire(obj):
    if dataclasses.is_dataclass(obj):
        return {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
    return obj

try:
    from .invocation_generated_types import TestResponse as TestResponse
except Exception:
    TestResponse = Any

from .callback_request_types import TestCallbackMessage as TestCallbackMessage

async def test__test_callback(req: 'TestCallbackMessage') -> AsyncGenerator['TestResponse', None]:
    it = getattr(kameo, "test").__getattribute__("TestCallback")( _to_wire(req) )
    iterator = await it if inspect.isawaitable(it) else it
    async for item in iterator:
        yield item

