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
    from .invocation_generated_types import BenchResponse as BenchResponse
except Exception:
    BenchResponse = Any

from .callback_request_types import BenchCallback as BenchCallback

async def bench__bench_callback(req: 'BenchCallback') -> AsyncGenerator['BenchResponse', None]:
    it = getattr(kameo, "bench").__getattribute__("BenchCallback")( _to_wire(req) )
    iterator = await it if inspect.isawaitable(it) else it
    async for item in iterator:
        yield item

