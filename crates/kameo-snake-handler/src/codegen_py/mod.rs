//! Python type code generation (static, annotated dataclasses and unions).
//!
//! This module defines a minimal intermediate representation (IR) for Rust-like
//! types and an emitter that generates a Python module containing:
//! - `@dataclass` structs
//! - sum-types (Rust enums) as a union of per-variant dataclasses with a Literal tag
//!
//! The goal is to enable static typing (mypy/pyright) without any runtime monkey patching.

mod ir;
mod emitter;
pub mod callback;

pub use ir::{Decl, EnumDecl, EnumVariant, StructDecl, TypeRef};
pub use emitter::emit_python_module;
pub use callback::{CallbackStub, emit_callback_module};

/// Trait allowing types to provide compile-time IR via a proc-macro derive.
pub trait ProvideIr {
    fn provide_ir() -> Vec<Decl>;
}
// --- ProvideIr blanket impls for primitives and common containers ---
impl ProvideIr for bool { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for i8 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for i16 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for i32 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for i64 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for i128 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for u8 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for u16 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for u32 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for u64 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for u128 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for f32 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for f64 { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for String { fn provide_ir() -> Vec<Decl> { vec![] } }
impl ProvideIr for serde_json::Value { fn provide_ir() -> Vec<Decl> { vec![] } }

impl<T: ProvideIr> ProvideIr for Option<T> {
    fn provide_ir() -> Vec<Decl> {
        T::provide_ir()
    }
}
impl<T: ProvideIr> ProvideIr for Vec<T> {
    fn provide_ir() -> Vec<Decl> { T::provide_ir() }
}
impl<T: ProvideIr> ProvideIr for std::collections::HashMap<String, T> {
    fn provide_ir() -> Vec<Decl> { T::provide_ir() }
}
impl<T: ProvideIr> ProvideIr for std::collections::BTreeMap<String, T> {
    fn provide_ir() -> Vec<Decl> { T::provide_ir() }
}

#[cfg(test)]
mod tests {
    use super::{emit_python_module, Decl, EnumDecl, EnumVariant, StructDecl, TypeRef};
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyTuple, PyCFunction};
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn pyo3_imports_generated_module_and_instantiates_types() {
        let user = Decl::Struct(StructDecl {
            name: "User".to_string(),
            fields: vec![
                ("id".to_string(), TypeRef::Builtin("int".to_string())),
                ("name".to_string(), TypeRef::Builtin("str".to_string())),
                ("meta".to_string(), TypeRef::Builtin("any".to_string())),
            ],
        });

        let evt = Decl::Enum(EnumDecl {
            name: "Message".to_string(),
            variants: vec![
                EnumVariant::Unit { name: "Started".to_string() },
                EnumVariant::Newtype { name: "UserJoined".to_string(), ty: TypeRef::Named("User".to_string()) },
                EnumVariant::Tuple { name: "Scored".to_string(), tys: vec![TypeRef::Builtin("int".to_string()), TypeRef::Builtin("float".to_string())] },
                EnumVariant::Struct { name: "Tagged".to_string(), fields: vec![
                    ("tag".to_string(), TypeRef::Builtin("str".to_string())),
                    ("props".to_string(), TypeRef::Dict(Box::new(TypeRef::Builtin("any".to_string())))),
                ]},
            ],
        });

        let module_name = format!("kameo_types_{}", Uuid::new_v4().simple());
        let py_src = emit_python_module(&module_name, &[user, evt]);

        // Write module to a temporary directory as kameo_types.py
        let mut dir = std::env::temp_dir();
        dir.push(format!("kameo_codegen_test_{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("create temp dir");
        let module_path: PathBuf = dir.join(format!("{}.py", module_name));
        let mut f = fs::File::create(&module_path).expect("create module file");
        f.write_all(py_src.as_bytes()).expect("write module");
        f.flush().expect("flush module");

        // Import and use generated module via PyO3
        Python::with_gil(|py| {
            let sys = py.import("sys").expect("import sys");
            let sys_path = sys.getattr("path").expect("get sys.path");
            let path_str = module_path.parent().unwrap().to_string_lossy().to_string();
            sys_path
                .call_method1("insert", (0, &path_str))
                .expect("insert temp path");

            let importlib = py.import("importlib").expect("import importlib");
            let m = importlib
                .call_method1("import_module", (&module_name,))
                .expect("import kameo_types");

            // Access generated helpers for Message
            let from_wire = m.getattr("from_wire_message").expect("from_wire_message exists");
            let to_wire = m.getattr("to_wire_message").expect("to_wire_message exists");
            let match_fn = m.getattr("match_message").expect("match_message exists");

            // Wire dict for unit variant: {"Started": {}}
            let py_dict = PyDict::new(py);
            let empty = PyDict::new(py);
            py_dict.set_item("Started", empty).unwrap();

            // Parse and match
            let msg_obj = from_wire.call1((py_dict,)).expect("from_wire ok");

            // Define handler for unit variant returning a constant
            let handlers = PyDict::new(py);
            let started_handler = PyCFunction::new_closure(
                py,
                None,
                None,
                |args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>| -> PyResult<PyObject> {
                    let bound_any = 7i64.into_pyobject(args.py())?;
                    Ok(bound_any.unbind().into())
                },
            )
            .unwrap();
            handlers.set_item("Started", started_handler).unwrap();

            let result = match_fn
                .call((msg_obj.clone(),), Some(&handlers))
                .expect("match ok");
            let value = result.extract::<i64>().unwrap();
            assert_eq!(value, 7);

            // Roundtrip
            let wire_back = to_wire.call1((msg_obj,),).expect("to_wire ok");
            let tag = wire_back.get_item("Started").unwrap();
            let inner = tag.downcast::<PyDict>().unwrap();
            assert!(inner.is_empty());
        });

        // Cleanup best-effort
        let _ = fs::remove_file(&module_path);
        let _ = fs::remove_dir(&dir);
    }

    #[test]
    fn pyo3_match_tuple_struct_newtype_and_helpers() {
        let user = Decl::Struct(StructDecl {
            name: "User".to_string(),
            fields: vec![
                ("id".to_string(), TypeRef::Builtin("int".to_string())),
                ("name".to_string(), TypeRef::Builtin("str".to_string())),
            ],
        });

        let evt = Decl::Enum(EnumDecl {
            name: "Message".to_string(),
            variants: vec![
                EnumVariant::Unit { name: "Started".to_string() },
                EnumVariant::Newtype { name: "UserJoined".to_string(), ty: TypeRef::Named("User".to_string()) },
                EnumVariant::Tuple { name: "Scored".to_string(), tys: vec![TypeRef::Builtin("int".to_string()), TypeRef::Builtin("float".to_string())] },
                EnumVariant::Struct { name: "Tagged".to_string(), fields: vec![
                    ("tag".to_string(), TypeRef::Builtin("str".to_string())),
                    ("props".to_string(), TypeRef::Dict(Box::new(TypeRef::Builtin("any".to_string())))),
                ]},
            ],
        });

        let module_name = format!("kameo_types_{}", Uuid::new_v4().simple());
        let py_src = emit_python_module(&module_name, &[user, evt]);

        let mut dir = std::env::temp_dir();
        dir.push(format!("kameo_codegen_test_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let module_path: PathBuf = dir.join(format!("{}.py", module_name));
        let mut f = std::fs::File::create(&module_path).expect("create module file");
        f.write_all(py_src.as_bytes()).expect("write module");
        f.flush().expect("flush module");

        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            sys.getattr("path").unwrap().call_method1("insert", (0, module_path.parent().unwrap().to_string_lossy().to_string(),)).unwrap();
            let m = py.import("importlib").unwrap().call_method1("import_module", (&module_name,)).unwrap();

            let from_wire = m.getattr("from_wire_message").unwrap();
            let to_wire = m.getattr("to_wire_message").unwrap();
            let match_fn = m.getattr("match_message").unwrap();
            let mk_scored = m.getattr("message_scored").unwrap();

            // Tuple variant via constructor -> to_wire -> from_wire
            let msg_tuple = mk_scored.call1((3i64, 2.5f64)).unwrap();
            let wire_tuple = to_wire.call1((msg_tuple,),).unwrap();
            let msg_tuple = from_wire.call1((wire_tuple,)).unwrap();

            let handlers = PyDict::new(py);
            let scored = pyo3::types::PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| -> PyResult<PyObject> {
                let x: i64 = args.get_item(0)?.extract()?;
                let y: f64 = args.get_item(1)?.extract()?;
                Ok((x as f64 + y).into_pyobject(args.py())?.unbind().into())
            }).unwrap();
            handlers.set_item("Scored", scored).unwrap();
            let r = match_fn.call((msg_tuple,), Some(&handlers)).unwrap();
            let sum = r.extract::<f64>().unwrap();
            assert!((sum - 5.5).abs() < 1e-6);

            // Struct variant: {"Tagged": {"tag":"x","props":{"a":1}}}
            let wire_struct = PyDict::new(py);
            let inner = PyDict::new(py);
            let props = PyDict::new(py);
            props.set_item("a", 1i64).unwrap();
            inner.set_item("tag", "x").unwrap();
            inner.set_item("props", props).unwrap();
            wire_struct.set_item("Tagged", inner).unwrap();
            let msg_struct = from_wire.call1((wire_struct,)).unwrap();
            let handlers2 = PyDict::new(py);
            let tagged = pyo3::types::PyCFunction::new_closure(py, None, None, |args: &Bound<'_, PyTuple>, _| -> PyResult<PyObject> {
                // Ignore details; return constant to assert handler dispatched
                Ok(1i64.into_pyobject(args.py())?.unbind().into())
            }).unwrap();
            handlers2.set_item("Tagged", tagged).unwrap();
            let r2 = match_fn.call((msg_struct,), Some(&handlers2)).unwrap();
            assert_eq!(r2.extract::<i64>().unwrap(), 1);

            // Newtype variant via constructor and helpers
            let user_cls = m.getattr("User").unwrap();
            let mk_joined = m.getattr("message_user_joined").unwrap();
            let is_joined = m.getattr("is_message_user_joined").unwrap();
            let expect_joined = m.getattr("expect_message_user_joined").unwrap();

            let d = PyDict::new(py);
            d.set_item("id", 7i64).unwrap();
            d.set_item("name", "alice").unwrap();
            // No meta field in this module's User
            let u = user_cls.call((), Some(&d)).unwrap();
            let msg_n = mk_joined.call1((u,)).unwrap();
            let truthy = is_joined.call1((msg_n.clone(),),).unwrap();
            assert!(truthy.extract::<bool>().unwrap());
            let v = expect_joined.call1((msg_n.clone(),),).unwrap();
            let got_name = v.getattr("value").unwrap().getattr("name").unwrap().extract::<String>().unwrap();
            assert_eq!(got_name, "alice");

            // Unit constructor
            let mk_started = m.getattr("message_started").unwrap();
            let ms = mk_started.call0().unwrap();
            let w = to_wire.call1((ms,)).unwrap();
            let started_payload = w.get_item("Started").unwrap();
            assert!(started_payload.downcast::<PyDict>().unwrap().is_empty());
        });

        let _ = std::fs::remove_file(&module_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn pyo3_struct_optionals_strict_and_enum_decode() {
        // Define a struct with fields, and an enum with mixed variants
        let cfg = Decl::Struct(StructDecl {
            name: "Config".to_string(),
            fields: vec![
                ("name".to_string(), TypeRef::Builtin("str".to_string())),
                ("extra".to_string(), TypeRef::Builtin("any".to_string())),
            ],
        });

        let resp = Decl::Enum(EnumDecl {
            name: "Response".to_string(),
            variants: vec![
                EnumVariant::Unit { name: "Ok".to_string() },
                EnumVariant::Newtype { name: "Err".to_string(), ty: TypeRef::Builtin("str".to_string()) },
                EnumVariant::Tuple { name: "Point".to_string(), tys: vec![TypeRef::Builtin("int".to_string()), TypeRef::Builtin("float".to_string())] },
                EnumVariant::Struct { name: "Meta".to_string(), fields: vec![
                    ("a".to_string(), TypeRef::Builtin("int".to_string())),
                    ("b".to_string(), TypeRef::Builtin("str".to_string())),
                ]},
            ],
        });

        let module_name = format!("kameo_types_{}", Uuid::new_v4().simple());
        let py_src = emit_python_module(&module_name, &[cfg, resp]);

        let mut dir = std::env::temp_dir();
        dir.push(format!("kameo_codegen_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let module_path: PathBuf = dir.join(format!("{}.py", module_name));
        let mut f = std::fs::File::create(&module_path).expect("create module file");
        f.write_all(py_src.as_bytes()).expect("write module");
        f.flush().expect("flush module");

        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            sys.getattr("path").unwrap().call_method1("insert", (0, module_path.parent().unwrap().to_string_lossy().to_string(),)).unwrap();
            let m = py.import("importlib").unwrap().call_method1("import_module", (&module_name,)).unwrap();

            // Struct helpers
            let from_wire_cfg = m.getattr("from_wire_config").unwrap();
            let from_wire_cfg_strict = m.getattr("from_wire_config_strict").unwrap();
            let to_wire_cfg = m.getattr("to_wire_config").unwrap();

            // Missing optional extra -> None
            let d = PyDict::new(py);
            d.set_item("name", "svc").unwrap();
            let cfg_obj = from_wire_cfg.call1((d,)).unwrap();
            assert_eq!(cfg_obj.getattr("name").unwrap().extract::<String>().unwrap(), "svc");
            assert!(cfg_obj.getattr("extra").unwrap().is_none());

            // Unknown key strict mode -> error
            let d2 = PyDict::new(py);
            d2.set_item("name", "svc").unwrap();
            d2.set_item("bogus", 1i64).unwrap();
            let strict_err = from_wire_cfg_strict.call1((d2,));
            assert!(strict_err.is_err());

            // Roundtrip to_wire
            let wire_back = to_wire_cfg.call1((cfg_obj.clone(),)).unwrap();
            assert_eq!(wire_back.get_item("name").unwrap().extract::<String>().unwrap(), "svc");
            assert!(wire_back.get_item("extra").unwrap().is_none());

            // Enum decode helper
            let decode = m.getattr("decode_response").unwrap();

            // Unit
            let ok = decode.call1(("Ok", py.None())).unwrap();
            assert_eq!(ok.getattr("kind").unwrap().extract::<String>().unwrap(), "Ok");
            // Newtype
            let err = decode.call1(("Err", "bad")).unwrap();
            assert_eq!(err.getattr("value").unwrap().extract::<String>().unwrap(), "bad");
            // Tuple payload with heterogeneous types via PyObject elements
            let p0: PyObject = 3i64.into_pyobject(py).unwrap().unbind().into();
            let p1: PyObject = 2.5f64.into_pyobject(py).unwrap().unbind().into();
            let items: Vec<PyObject> = vec![p0, p1];
            let point_payload = pyo3::types::PyTuple::new(py, &items).unwrap();
            let point = decode.call1(("Point", &point_payload)).unwrap();
            assert_eq!(point.getattr("field_0").unwrap().extract::<i64>().unwrap(), 3);
            // Struct
            let meta_payload = PyDict::new(py);
            meta_payload.set_item("a", 9i64).unwrap();
            meta_payload.set_item("b", "z").unwrap();
            let meta = decode.call1(("Meta", meta_payload)).unwrap();
            assert_eq!(meta.getattr("a").unwrap().extract::<i64>().unwrap(), 9);
            assert_eq!(meta.getattr("b").unwrap().extract::<String>().unwrap(), "z");
        });

        let _ = std::fs::remove_file(&module_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn pyo3_invalid_enum_and_roundtrip_constructors() {
        // Enum with all variant shapes
        let resp = Decl::Enum(EnumDecl {
            name: "Resp".to_string(),
            variants: vec![
                EnumVariant::Unit { name: "U".to_string() },
                EnumVariant::Newtype { name: "N".to_string(), ty: TypeRef::Builtin("int".to_string()) },
                EnumVariant::Tuple { name: "T".to_string(), tys: vec![TypeRef::Builtin("int".to_string()), TypeRef::Builtin("float".to_string())] },
                EnumVariant::Struct { name: "S".to_string(), fields: vec![
                    ("x".to_string(), TypeRef::Builtin("str".to_string())),
                ]},
            ],
        });

        let module_name = format!("kameo_types_{}", Uuid::new_v4().simple());
        let py_src = emit_python_module(&module_name, &[resp]);
        let mut dir = std::env::temp_dir();
        dir.push(format!("kameo_codegen_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let module_path: PathBuf = dir.join(format!("{}.py", module_name));
        let mut f = std::fs::File::create(&module_path).unwrap();
        f.write_all(py_src.as_bytes()).unwrap();
        f.flush().unwrap();

        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            sys.getattr("path").unwrap().call_method1("insert", (0, module_path.parent().unwrap().to_string_lossy().to_string(),)).unwrap();
            let m = py.import("importlib").unwrap().call_method1("import_module", (&module_name,)).unwrap();

            let from_wire = m.getattr("from_wire_resp").unwrap();
            let to_wire = m.getattr("to_wire_resp").unwrap();
            let decode = m.getattr("decode_resp").unwrap();

            // Invalid tag
            let bad = PyDict::new(py);
            bad.set_item("ZZ", 1i64).unwrap();
            assert!(from_wire.call1((bad,)).is_err());
            assert!(decode.call1(("ZZ", 1i64)).is_err());

            // Invalid tuple arity
            let t_bad = PyDict::new(py);
            let tuple1 = pyo3::types::PyTuple::new(py, [1i64]).unwrap();
            t_bad.set_item("T", tuple1).unwrap();
            assert!(from_wire.call1((t_bad,)).is_err());

            // Roundtrip all constructors
            let mk_u = m.getattr("resp_u").unwrap();
            let mk_n = m.getattr("resp_n").unwrap();
            let mk_t = m.getattr("resp_t").unwrap();
            let mk_s = m.getattr("resp_s").unwrap();

            let u = mk_u.call0().unwrap();
            let w_u = to_wire.call1((u.clone(),)).unwrap();
            let u2 = from_wire.call1((w_u,)).unwrap();
            assert_eq!(u.getattr("kind").unwrap().extract::<String>().unwrap(), u2.getattr("kind").unwrap().extract::<String>().unwrap());

            let n = mk_n.call1((42i64,)).unwrap();
            let w_n = to_wire.call1((n.clone(),)).unwrap();
            let n2 = from_wire.call1((w_n,)).unwrap();
            assert_eq!(n.getattr("value").unwrap().extract::<i64>().unwrap(), n2.getattr("value").unwrap().extract::<i64>().unwrap());

            let _p0: PyObject = 2i64.into_pyobject(py).unwrap().unbind().into();
            let _p1: PyObject = 3.5f64.into_pyobject(py).unwrap().unbind().into();
            let t = mk_t.call1((2i64, 3.5f64)).unwrap();
            let w_t = to_wire.call1((t.clone(),)).unwrap();
            let t2 = from_wire.call1((w_t,)).unwrap();
            assert_eq!(t.getattr("field_0").unwrap().extract::<i64>().unwrap(), t2.getattr("field_0").unwrap().extract::<i64>().unwrap());

            let s = mk_s.call1(("z",)).unwrap();
            let w_s = to_wire.call1((s.clone(),)).unwrap();
            let s2 = from_wire.call1((w_s,)).unwrap();
            assert_eq!(s.getattr("x").unwrap().extract::<String>().unwrap(), s2.getattr("x").unwrap().extract::<String>().unwrap());
        });

        let _ = std::fs::remove_file(&module_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn pyo3_nested_recursive_from_to_wire() {
        // Define nested types to validate recursive from_wire/to_wire generation
        let inner = Decl::Struct(StructDecl {
            name: "Inner".to_string(),
            fields: vec![
                ("id".to_string(), TypeRef::Builtin("int".to_string())),
                ("name".to_string(), TypeRef::Builtin("str".to_string())),
                ("tags".to_string(), TypeRef::List(Box::new(TypeRef::Builtin("str".to_string())))),
                ("meta".to_string(), TypeRef::Dict(Box::new(TypeRef::Builtin("str".to_string())))),
            ],
        });

        let outer = Decl::Enum(EnumDecl {
            name: "Outer".to_string(),
            variants: vec![
                EnumVariant::Unit { name: "Zero".to_string() },
                EnumVariant::Newtype { name: "Single".to_string(), ty: TypeRef::Named("Inner".to_string()) },
                EnumVariant::Tuple { name: "Pair".to_string(), tys: vec![
                    TypeRef::Option(Box::new(TypeRef::Named("Inner".to_string()))),
                    TypeRef::List(Box::new(TypeRef::Named("Inner".to_string()))),
                ]},
                EnumVariant::Struct { name: "WithMap".to_string(), fields: vec![
                    ("items".to_string(), TypeRef::Dict(Box::new(TypeRef::Named("Inner".to_string())))),
                    ("extra".to_string(), TypeRef::Option(Box::new(TypeRef::Builtin("int".to_string())))),
                ]},
            ],
        });

        let module_name = format!("kameo_types_{}", Uuid::new_v4().simple());
        let py_src = emit_python_module(&module_name, &[inner, outer]);

        let mut dir = std::env::temp_dir();
        dir.push(format!("kameo_codegen_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let module_path: PathBuf = dir.join(format!("{}.py", module_name));
        let mut f = std::fs::File::create(&module_path).expect("create module file");
        f.write_all(py_src.as_bytes()).expect("write module");
        f.flush().expect("flush module");

        Python::with_gil(|py| {
            let sys = py.import("sys").unwrap();
            sys.getattr("path").unwrap().call_method1("insert", (0, module_path.parent().unwrap().to_string_lossy().to_string(),)).unwrap();
            let m = py.import("importlib").unwrap().call_method1("import_module", (&module_name,)).unwrap();

            let from_wire_inner = m.getattr("from_wire_inner").unwrap();
            let to_wire_inner = m.getattr("to_wire_inner").unwrap();
            let from_wire_outer = m.getattr("from_wire_outer").unwrap();
            let to_wire_outer = m.getattr("to_wire_outer").unwrap();

            // Build nested wire dict for Outer::Struct WithMap
            let inner1 = PyDict::new(py);
            inner1.set_item("id", 1i64).unwrap();
            inner1.set_item("name", "a").unwrap();
            inner1.set_item("tags", vec!["x", "y"]).unwrap();
            let meta1 = PyDict::new(py); meta1.set_item("k1", "v1").unwrap();
            inner1.set_item("meta", meta1).unwrap();

            let inner2 = PyDict::new(py);
            inner2.set_item("id", 2i64).unwrap();
            inner2.set_item("name", "b").unwrap();
            inner2.set_item("tags", vec!["z"]).unwrap();
            let meta2 = PyDict::new(py); meta2.set_item("k2", "v2").unwrap();
            inner2.set_item("meta", meta2).unwrap();

            let items = PyDict::new(py);
            items.set_item("first", inner1).unwrap();
            items.set_item("second", inner2).unwrap();

            let payload = PyDict::new(py);
            payload.set_item("items", items).unwrap();
            payload.set_item("extra", 99i64).unwrap();

            let wire_outer = PyDict::new(py);
            wire_outer.set_item("WithMap", payload).unwrap();

            // Parse -> to_wire roundtrip
            let outer_obj = from_wire_outer.call1((wire_outer,)).unwrap();
            let wire_back = to_wire_outer.call1((outer_obj.clone(),)).unwrap();
            // Ensure keys present and match shapes
            let tag = wire_back.get_item("WithMap").unwrap();
            let tag_dict = tag.downcast::<PyDict>().unwrap();
            assert!(tag_dict.contains("items").unwrap());
            assert!(tag_dict.contains("extra").unwrap());

            // Also test Newtype Single with recursive inner from_wire/to_wire
            let wire_inner = PyDict::new(py);
            wire_inner.set_item("id", 7i64).unwrap();
            wire_inner.set_item("name", "nest").unwrap();
            wire_inner.set_item("tags", vec!["a"]).unwrap();
            let meta = PyDict::new(py); meta.set_item("m", "n").unwrap();
            wire_inner.set_item("meta", meta).unwrap();
            let inner_obj = from_wire_inner.call1((wire_inner,)).unwrap();
            let inner_back = to_wire_inner.call1((inner_obj,)).unwrap();
            assert_eq!(inner_back.get_item("id").unwrap().extract::<i64>().unwrap(), 7);
        });

        let _ = std::fs::remove_file(&module_path);
        let _ = std::fs::remove_dir(&dir);
    }

}


