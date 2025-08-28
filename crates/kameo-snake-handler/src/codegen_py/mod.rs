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

// --- Automatic IR derivation from serde-reflection ---
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_reflection::{ContainerFormat, Format, Registry, Tracer, TracerConfig, VariantFormat, Samples};

fn builtin_of(format: &Format) -> Option<String> {
    Some(match format {
        Format::Unit => "any".to_string(),
        Format::Bool => "bool".to_string(),
        Format::I8 | Format::I16 | Format::I32 | Format::I64 | Format::I128 |
        Format::U8 | Format::U16 | Format::U32 | Format::U64 | Format::U128 => "int".to_string(),
        Format::F32 | Format::F64 => "float".to_string(),
        Format::Char | Format::Str => "str".to_string(),
        Format::Bytes => "bytes".to_string(),
        _ => return None,
    })
}

fn to_typeref(_registry: &Registry, f: &Format) -> TypeRef {
    if let Some(b) = builtin_of(f) { return TypeRef::Builtin(b); }
    match f {
        Format::Option(inner) => TypeRef::Option(Box::new(to_typeref(_registry, inner))),
        Format::Seq(inner) => TypeRef::List(Box::new(to_typeref(_registry, inner))),
        Format::Map{key, value} => {
            // Constrain to dict[str, T] when key can be string; else Dict[Any]
            let v = to_typeref(_registry, value);
            if matches!(key.as_ref(), Format::Str | Format::Char) { TypeRef::Dict(Box::new(v)) } else { TypeRef::Dict(Box::new(TypeRef::Builtin("any".to_string()))) }
        }
        Format::Tuple(_fields) => {
            // Represent as Named tuple class if needed; for now, use List[Any]
            TypeRef::List(Box::new(TypeRef::Builtin("any".to_string())))
        }
        Format::TupleArray{ content, .. } => TypeRef::List(Box::new(to_typeref(_registry, content))),
        Format::TypeName(name) => TypeRef::Named(short_name(name)),
        _ => TypeRef::Builtin("any".to_string()),
    }
}

fn short_name(full: &str) -> String { full.rsplit("::").next().unwrap_or(full).to_string() }

fn container_to_decl(name: &str, c: &ContainerFormat) -> Decl {
    let nm = short_name(name);
    match c {
        ContainerFormat::Struct(fields) => {
            let mut f = Vec::with_capacity(fields.len());
            for field in fields {
                f.push((field.name.clone(), to_typeref(&Registry::default(), &field.value)));
            }
            Decl::Struct(StructDecl{ name: nm, fields: f })
        }
        ContainerFormat::Enum(variants) => {
            let mut vs = Vec::new();
            // variants is a map from index to Named<VariantFormat>
            for (_idx, named) in variants.iter() {
                let vname = named.name.to_string();
                match &named.value {
                    VariantFormat::Unit => vs.push(EnumVariant::Unit{ name: vname }),
                    VariantFormat::NewType(fmt) => vs.push(EnumVariant::Newtype{ name: vname, ty: to_typeref(&Registry::default(), fmt) }),
                    VariantFormat::Tuple(fmts) => {
                        let mut tys = Vec::with_capacity(fmts.len());
                        for fmt in fmts { tys.push(to_typeref(&Registry::default(), fmt)); }
                        vs.push(EnumVariant::Tuple{ name: named.name.to_string(), tys });
                    }
                    VariantFormat::Struct(fields) => {
                        let mut f = Vec::with_capacity(fields.len());
                        for fld in fields { f.push((fld.name.clone(), to_typeref(&Registry::default(), &fld.value))); }
                        vs.push(EnumVariant::Struct{ name: named.name.to_string(), fields: f });
                    }
                    VariantFormat::Variable(_) => {
                        // Fallback: treat as unit for unknown-at-this-stage
                        vs.push(EnumVariant::Unit{ name: vname });
                    }
                }
            }
            Decl::Enum(EnumDecl{ name: nm, variants: vs })
        }
        _ => {
            // Fallback: treat as opaque struct
            Decl::Struct(StructDecl{ name: nm, fields: vec![] })
        }
    }
}

/// Derive IR for all named containers reachable from the given type `T` using serde-reflection.
pub fn derive_decls_for<T: Serialize + DeserializeOwned>() -> Vec<Decl> {
    let mut tracer = Tracer::new(TracerConfig::default());
    let samples = Samples::new();
    let _ = tracer.trace_type::<T>(&samples);
    let registry = tracer.registry().unwrap_or_default();
    registry.iter().map(|(name, cf)| container_to_decl(name, cf)).collect()
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
}


