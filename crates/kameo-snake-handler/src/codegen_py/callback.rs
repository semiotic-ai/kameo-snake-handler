use std::fmt::Write as _;

// No direct IR types needed here; callback generation is driven by CallbackStub inputs

/// Callback stub description: fully qualified path and request/response types
#[derive(Debug, Clone)]
pub struct CallbackStub {
    /// e.g., "data.DataFetchCallback"
    pub path: String,
    /// Request type name generated in the types module
    pub request_type: String,
    /// Response type name generated in the types module
    pub response_type: String,
}

/// Emit a Python stubs module that provides typed callback functions aligned with DynamicCallbackModule
///
/// This generates a module with:
/// - typed function wrappers that call kameo.<module>.<HandlerType>(req)
/// - precise AsyncGenerator[UnionType, None] annotations
/// - match_* re-exports from invocation module when available
pub fn emit_callback_module(module_name: &str, _types_module: &str, stubs: &[CallbackStub]) -> String {
    let mut out = String::new();
    writeln!(out, "# Generated callback for {}", module_name).unwrap();
    // Suppress dynamic module import warnings for editors (pyright/pylance)
    writeln!(out, "# pyright: reportMissingImports=false").unwrap();
    writeln!(out, "from __future__ import annotations").unwrap();
    writeln!(out, "from typing import AsyncGenerator, TYPE_CHECKING, Any").unwrap();
    writeln!(out, "import dataclasses").unwrap();
    writeln!(out, "import inspect").unwrap();
    writeln!(out, "import kameo  # type: ignore[import-not-found]").unwrap();
    writeln!(out, "from . import invocation_generated_types as inv").unwrap();
    writeln!(out, "if TYPE_CHECKING:").unwrap();
    writeln!(out, "    from .callback_request_types import *  # for editor type awareness").unwrap();
    writeln!(out, "    from .invocation_generated_types import *  # for editor type awareness").unwrap();
    writeln!(out).unwrap();
    // helper: convert dataclass to wire dict when needed
    writeln!(out, "def _to_wire(obj):").unwrap();
    writeln!(out, "    if dataclasses.is_dataclass(obj):").unwrap();
    writeln!(out, "        return {{f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}}").unwrap();
    writeln!(out, "    return obj").unwrap();
    writeln!(out).unwrap();

    // Helper to map Rust type path to local generated union name if available
    fn py_resp_alias(rust_ty: &str) -> String {
        let name = rust_ty.rsplit("::").next().unwrap_or("");
        if name.is_empty() { return "Any".to_string(); }
        name.to_string()
    }

    // Helper to map Rust request type path to a valid Python identifier (use last segment)
    fn py_req_alias(rust_ty: &str) -> String {
        let name = rust_ty.rsplit("::").next().unwrap_or("");
        if name.is_empty() { return "object".to_string(); }
        name.to_string()
    }

    use std::collections::BTreeSet;
    let mut resp_names: BTreeSet<String> = BTreeSet::new();
    let mut req_names: BTreeSet<String> = BTreeSet::new();
    for stub in stubs {
        let resp_ty = py_resp_alias(&stub.response_type);
        resp_names.insert(resp_ty);
        let req_ty = py_req_alias(&stub.request_type);
        req_names.insert(req_ty);
    }

    // Provide fallbacks for response union names not present in this package's inv module
    for name in resp_names {
        if name == "Any" { continue; }
        writeln!(out, "try:").unwrap();
        writeln!(out, "    from .invocation_generated_types import {} as {}", name, name).unwrap();
        writeln!(out, "except Exception:").unwrap();
        writeln!(out, "    {} = Any", name).unwrap();
    }
    writeln!(out).unwrap();

    // Re-export request types so user code can import from this module
    for name in req_names {
        if name == "object" { continue; }
        writeln!(out, "from .callback_request_types import {} as {}", name, name).unwrap();
    }
    writeln!(out).unwrap();

    for stub in stubs {
        let snake = to_snake(stub.path.replace('.', "_").as_str());
        let resp_ty = py_resp_alias(&stub.response_type);
        let req_ty = py_req_alias(&stub.request_type);
        // Function header with forward-ref request and response types
        writeln!(
            out,
            "async def {}(req: '{}') -> AsyncGenerator['{}', None]:",
            snake, req_ty, resp_ty
        ).unwrap();
        // Body: call kameo.<module>.<HandlerType>(req), await if needed, then iterate
        let parts: Vec<&str> = stub.path.split('.').collect();
        let (module, handler) = (parts[0], parts[1]);
        writeln!(out, "    it = getattr(kameo, \"{}\").__getattribute__(\"{}\")( _to_wire(req) )", module, handler).unwrap();
        writeln!(out, "    iterator = await it if inspect.isawaitable(it) else it").unwrap();
        writeln!(out, "    async for item in iterator:").unwrap();
        writeln!(out, "        yield item").unwrap();
        writeln!(out).unwrap();
    }

    // No additional re-exports; consumers should import helpers from invocation_generated_types explicitly

    out
}

fn to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() * 2);
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 { out.push('_'); }
            for lc in ch.to_lowercase() { out.push(lc); }
        } else {
            out.push(ch);
        }
    }
    out
}



