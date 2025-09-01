//! Python dynamic module creation and generated file emission used by the macro.
//!
//! This module runs inside the child process and is called from the macro
//! expansion to:
//! - Ensure `kameo` module exists in `sys.modules`
//! - Create deterministic submodules for each callback module (`kameo.<module>`)
//! - Inject Python callables for each handler type which call back into Rust
//! - Generate and write `callback_generated_types.py` and `invocation_generated_types.py`
//!
//! Inputs are passed via environment variables set by the parent prior to
//! spawning the child, including the callback registry, request/response type
//! names, and optional IR for generating dataclasses and helpers.

use pyo3::types::{PyAnyMethods, PyTupleMethods};

#[tracing::instrument(skip(py, config))]
pub fn create_dynamic_modules(
    py: pyo3::Python<'_>,
    config: &crate::PythonConfig,
) -> pyo3::PyResult<()> {
    use pyo3::types::PyModule;
    use std::collections::HashMap;

    let callback_registry_json = std::env::var("KAMEO_CALLBACK_REGISTRY").expect("KAMEO_CALLBACK_REGISTRY must be set in child");
    tracing::debug!("Raw callback registry JSON: {}", callback_registry_json);
    let callback_registry: HashMap<String, Vec<String>> = serde_json::from_str(&callback_registry_json).expect("Failed to parse KAMEO_CALLBACK_REGISTRY");
    tracing::debug!("Parsed callback registry: {:?}", callback_registry);

    let resp_types_json = std::env::var("KAMEO_CALLBACK_RESP_TYPES").unwrap_or_else(|_| "{}".to_string());
    let resp_types: HashMap<String, String> = serde_json::from_str(&resp_types_json).unwrap_or_default();
    let req_types_json = std::env::var("KAMEO_CALLBACK_REQ_TYPES").unwrap_or_else(|_| "{}".to_string());
    let req_types: HashMap<String, String> = serde_json::from_str(&req_types_json).unwrap_or_default();
    let req_ir_json = std::env::var("KAMEO_CALLBACK_REQ_IR").unwrap_or_else(|_| "{}".to_string());
    let req_ir_map: HashMap<String, Vec<crate::codegen_py::Decl>> = serde_json::from_str(&req_ir_json).unwrap_or_default();
    tracing::debug!(target: "kameo_snake_handler::macros::codegen", req_ir_map_keys = ?req_ir_map.keys().collect::<Vec<_>>(), "Parsed callback request IR");
    let resp_ir_json = std::env::var("KAMEO_CALLBACK_RESP_IR").unwrap_or_else(|_| "{}".to_string());
    let resp_ir_map: HashMap<String, Vec<crate::codegen_py::Decl>> = serde_json::from_str(&resp_ir_json).unwrap_or_default();
    tracing::debug!(target: "kameo_snake_handler::macros::codegen", resp_ir_map_keys = ?resp_ir_map.keys().collect::<Vec<_>>(), "Parsed callback response IR");

    let sys = py.import("sys").expect("import sys");
    let modules = sys.getattr("modules").expect("get sys.modules");
    let kameo_mod = match modules.get_item("kameo") {
        Ok(m) => m.downcast::<PyModule>().unwrap().clone(),
        Err(_) => {
            let m = PyModule::new(py, "kameo").expect("create kameo module");
            modules.set_item("kameo", &m).expect("inject kameo into sys.modules");
            tracing::debug!("Injected kameo module into sys.modules BEFORE user import");
            m.clone()
        }
    };

    tracing::debug!("Starting dynamic module creation for {} modules", callback_registry.len());
    for (module_name, handler_types) in &callback_registry {
        tracing::debug!("Creating Python module: kameo.{} with {} handler types", module_name, handler_types.len());
        let submodule = match kameo_mod.getattr(module_name) {
            Ok(existing) => existing.downcast::<PyModule>().unwrap().clone(),
            Err(_) => {
                let new_module = PyModule::new(py, module_name)?;
                kameo_mod.setattr(module_name, &new_module)?;
                new_module.clone()
            }
        };

        for handler_type in handler_types {
            tracing::debug!("Creating callback function: kameo.{}.{}", module_name, handler_type);
            let callback_path = format!("{}.{}", module_name, handler_type);
            let callback_path_clone = callback_path.clone();
            // Create a small Python callable which forwards into Rust's `callback_handle_inner`.
            let callback_fn = pyo3::types::PyCFunction::new_closure(
                py,
                None,
                None,
                move |args: &pyo3::Bound<'_, pyo3::types::PyTuple>, _kwargs: Option<&pyo3::Bound<'_, pyo3::types::PyDict>>| {
                    let py = args.py();
                    if args.len() != 1 { return Err(pyo3::exceptions::PyTypeError::new_err("Expected exactly one argument")); }
                    let py_msg = args.get_item(0)?;
                    crate::macros::py_callback::callback_handle_inner(py, &callback_path_clone, &py_msg)
                }
            )?;
            submodule.setattr(handler_type, callback_fn)?;
            tracing::info!("Created callback function: kameo.{}.{}", module_name, handler_type);
        }
        tracing::info!("Completed module: kameo.{}", module_name);
    }
    tracing::info!("Dynamic module creation completed for all {} modules", callback_registry.len());

    let sys_path = py.import("sys").expect("import sys").getattr("path").expect("get sys.path");
    for path in &config.python_path { sys_path.call_method1("append", (path,)).expect("append python_path"); tracing::debug!(added_path = %path, "Appended to sys.path"); }
    let path0 = sys_path.get_item(0).expect("sys.path[0]").extract::<String>().expect("path str");
    let default_dir = if let Some(dir) = config.python_path.last() { std::path::PathBuf::from(dir) } else { std::path::PathBuf::from(&path0) };
    let package_name = config.module_name.split('.').next().unwrap_or(&config.module_name);
    let module_dir = default_dir.join(package_name);
    std::fs::create_dir_all(&module_dir).expect("mkdir module package dir");
    tracing::debug!(target: "kameo_snake_handler::macros::codegen", dir = %module_dir.display(), module = %config.module_name, package = %package_name, "Ensured Python package directory for generated files");

    let types_mod = "invocation_generated_types".to_string();
    let stubs_mod = "callback_generated_types".to_string();
    let stubs_path = module_dir.join(format!("{}.py", stubs_mod));
    let mut callback_stubs: Vec<crate::codegen_py::CallbackStub> = Vec::new();
    for (module_name, handler_types) in &callback_registry {
        for handler_type in handler_types {
            let full_path = format!("{}.{}", module_name, handler_type);
            let request_type = req_types.get(&full_path).cloned().unwrap_or_else(|| "Any".to_string());
            let response_type = resp_types.get(&full_path).cloned().unwrap_or_else(|| "Any".to_string());
            callback_stubs.push(crate::codegen_py::CallbackStub { path: full_path, request_type, response_type });
        }
    }
    if config.enable_codegen {
        // Generate wrapper callables and typing stubs for callbacks.
        let stubs_src = crate::codegen_py::emit_callback_module(&stubs_mod, &types_mod, &callback_stubs);
        std::fs::write(&stubs_path, stubs_src).expect("write stubs module");
    }

    let _has_callback_ir = !req_ir_map.is_empty() || !resp_ir_map.is_empty();
    tracing::debug!(target: "kameo_snake_handler::macros::codegen", has_callback_ir = %_has_callback_ir, req_ir_map_empty = %req_ir_map.is_empty(), resp_ir_map_empty = %resp_ir_map.is_empty(), "Callback IR presence detected");
    // Emit callback request types if IR provided
    if config.enable_codegen && !req_ir_map.is_empty() {
        let mut all_reqs: Vec<crate::codegen_py::Decl> = Vec::new();
        for (_path, decls) in &req_ir_map {
            all_reqs.extend_from_slice(decls);
        }
        if !all_reqs.is_empty() {
            let req_mod = "callback_request_types".to_string();
            let req_src = crate::codegen_py::emit_python_module(&req_mod, &all_reqs);
            let req_path = module_dir.join(format!("{}.py", req_mod));
            std::fs::write(&req_path, req_src).expect("write callback request types module");
            tracing::info!(target: "kameo_snake_handler::macros::codegen", file = %req_path.display(), count = all_reqs.len(), "Emitted callback request types");
        }
    }
    std::env::set_var("KAMEO_GENERATED_CALLBACK_TYPES", &stubs_mod);
    std::env::set_var("KAMEO_GENERATED_INVOCATION_TYPES", &types_mod);

    if config.enable_codegen {
        let inv_ir_json = std::env::var("KAMEO_INVOCATION_IR").unwrap_or_else(|_| "[]".to_string());
        let decls: Vec<crate::codegen_py::Decl> = serde_json::from_str(&inv_ir_json).unwrap_or_default();
        // Emit invocation types used by the user’s Python package.
        let types_src = crate::codegen_py::emit_python_module(&types_mod, &decls);
        let types_path = module_dir.join(format!("{}.py", types_mod));
        std::fs::write(&types_path, types_src).expect("write types module");
    }

    Ok(())
}


