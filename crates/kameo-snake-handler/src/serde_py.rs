use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::conversion::IntoPyObjectExt;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Python error: {0}")]
    Py(#[from] PyErr),
    #[error("Serde error: {0}")]
    Serde(String),
    #[error("Conversion error: {0}")]
    Conversion(String),
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
    #[error("Invalid value: {0}")]
    InvalidValue(String),
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Trait for converting Python objects to Rust types
pub trait FromPyAny: Sized {
    /// Convert a Python object into Self
    fn from_py_object(obj: &Bound<'_, PyAny>) -> Result<Self>;
}

// Implement FromPyAny for any type that implements Deserialize
impl<T> FromPyAny for T
where
    T: for<'de> Deserialize<'de>,
{
    fn from_py_object(obj: &Bound<'_, PyAny>) -> Result<Self> {
        from_pyobject(obj)
    }
}

/// Convert a Rust value to a Python object
pub fn to_pyobject<'py, T>(py: Python<'py>, value: &T) -> Result<PyObject>
where
    T: Serialize,
{
    let json_value = serde_json::to_value(value)
        .map_err(|e| Error::Serde(e.to_string()))?;
    value_to_py(py, &json_value)
}

/// Convert a Python object to a Rust value
pub fn from_pyobject<T>(obj: &Bound<'_, PyAny>) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let value = py_to_value(obj)?;
    serde_json::from_value(value)
        .map_err(|e| Error::Serde(e.to_string()))
}

fn value_to_py(py: Python, value: &serde_json::Value) -> Result<PyObject> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_bound_py_any(py)?.into()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_bound_py_any(py)?.into())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_bound_py_any(py)?.into())
            } else {
                Err(Error::UnsupportedType("unsupported number type".to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(s.clone().into_bound_py_any(py)?.into()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (key, value) in map {
                dict.set_item(key, value_to_py(py, value)?)?;
            }
            Ok(dict.into())
        }
    }
}

fn py_to_value(obj: &Bound<'_, PyAny>) -> Result<serde_json::Value> {
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(b) = bool::extract_bound(obj) {
        return Ok(serde_json::Value::Bool(b));
    }

    if let Ok(i) = i64::extract_bound(obj) {
        return Ok(serde_json::Value::Number(i.into()));
    }

    if let Ok(f) = f64::extract_bound(obj) {
        return Ok(serde_json::json!(f));
    }

    if let Ok(s) = String::extract_bound(obj) {
        return Ok(serde_json::Value::String(s));
    }

    if let Ok(list) = obj.downcast::<PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(serde_json::Value::Array(arr));
    }

    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let key = String::extract_bound(&key)?;
            map.insert(key, py_to_value(&value)?);
        }
        return Ok(serde_json::Value::Object(map));
    }

    Err(Error::UnsupportedType(format!(
        "unsupported Python type: {}",
        obj.get_type().name()?
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Helper to create nested objects strategy
    fn nested_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|n| Value::Number(n.into())),
            "[a-zA-Z0-9]*".prop_map(Value::String)
        ];

        leaf.prop_recursive(
            3,   // Max depth
            256, // Max size
            10,  // Max number of items
            |inner| {
                prop_oneof![
                    // Generate arrays of values
                    proptest::collection::vec(inner.clone(), 0..5)
                        .prop_map(Value::Array),
                    // Generate objects with string keys and values
                    proptest::collection::hash_map("[a-zA-Z]+", inner, 0..5)
                        .prop_map(|map| Value::Object(map.into_iter().collect()))
                ]
            }
        )
    }

    proptest! {
        #[test]
        fn test_bool_roundtrip(bool_val in any::<bool>()) {
            Python::with_gil(|py| {
                let bool_json = Value::Bool(bool_val);
                let py_bool = value_to_py(py, &bool_json).unwrap();
                let bool_back = py_to_value(&py_bool.bind(py)).unwrap();
                assert_eq!(bool_json, bool_back);
            });
        }

        #[test]
        fn test_int_roundtrip(int_val in any::<i64>()) {
            Python::with_gil(|py| {
                let int_json = Value::Number(int_val.into());
                let py_int = value_to_py(py, &int_json).unwrap();
                let int_back = py_to_value(&py_int.bind(py)).unwrap();
                assert_eq!(int_json, int_back);
            });
        }

        #[test]
        fn test_string_roundtrip(s in "[a-zA-Z0-9]*") {
            Python::with_gil(|py| {
                let string_json = Value::String(s);
                let py_string = value_to_py(py, &string_json).unwrap();
                let string_back = py_to_value(&py_string.bind(py)).unwrap();
                assert_eq!(string_json, string_back);
            });
        }

        #[test]
        fn test_array_roundtrip(vec_items in proptest::collection::vec(any::<i64>(), 0..10)) {
            Python::with_gil(|py| {
                let array_json = Value::Array(vec_items.iter().map(|n| Value::Number((*n).into())).collect());
                let py_array = value_to_py(py, &array_json).unwrap();
                let array_back = py_to_value(&py_array.bind(py)).unwrap();
                assert_eq!(array_json, array_back);
            });
        }

        #[test]
        fn test_object_roundtrip(map_items in proptest::collection::hash_map("[a-zA-Z]+", any::<i64>(), 0..10)) {
            Python::with_gil(|py| {
                let object_json = Value::Object(
                    map_items.iter()
                        .map(|(k, v)| (k.clone(), Value::Number((*v).into())))
                        .collect()
                );
                let py_object = value_to_py(py, &object_json).unwrap();
                let object_back = py_to_value(&py_object.bind(py)).unwrap();
                assert_eq!(object_json, object_back);
            });
        }

        #[test]
        fn test_nested_array_roundtrip(arr in proptest::collection::vec(nested_value(), 0..5)) {
            Python::with_gil(|py| {
                let array_json = Value::Array(arr);
                let py_array = value_to_py(py, &array_json).unwrap();
                let array_back = py_to_value(&py_array.bind(py)).unwrap();
                assert_eq!(array_json, array_back);
            });
        }

        #[test]
        fn test_nested_object_roundtrip(map in proptest::collection::hash_map("[a-zA-Z]+", nested_value(), 0..5)) {
            Python::with_gil(|py| {
                let object_json = Value::Object(map.into_iter().collect());
                let py_object = value_to_py(py, &object_json).unwrap();
                let object_back = py_to_value(&py_object.bind(py)).unwrap();
                assert_eq!(object_json, object_back);
            });
        }

        #[test]
        fn test_mixed_array_roundtrip(
            bools in proptest::collection::vec(any::<bool>(), 0..3),
            ints in proptest::collection::vec(any::<i64>(), 0..3),
            strings in proptest::collection::vec("[a-zA-Z0-9]*", 0..3)
        ) {
            Python::with_gil(|py| {
                let mut array = Vec::new();
                // Add bools
                array.extend(bools.into_iter().map(Value::Bool));
                // Add numbers
                array.extend(ints.into_iter().map(|n| Value::Number(n.into())));
                // Add strings
                array.extend(strings.into_iter().map(Value::String));
                
                let array_json = Value::Array(array);
                let py_array = value_to_py(py, &array_json).unwrap();
                let array_back = py_to_value(&py_array.bind(py)).unwrap();
                assert_eq!(array_json, array_back);
            });
        }

        #[test]
        fn test_complex_object_roundtrip(
            key1 in "[a-zA-Z]+",
            key2 in "[a-zA-Z]+",
            nested_arr in proptest::collection::vec(nested_value(), 0..3),
            nested_obj in proptest::collection::hash_map("[a-zA-Z]+", nested_value(), 0..3)
        ) {
            Python::with_gil(|py| {
                let mut map = serde_json::Map::new();
                // Add a nested array
                map.insert(key1, Value::Array(nested_arr));
                // Add a nested object
                map.insert(key2, Value::Object(nested_obj.into_iter().collect()));
                
                let object_json = Value::Object(map);
                let py_object = value_to_py(py, &object_json).unwrap();
                let object_back = py_to_value(&py_object.bind(py)).unwrap();
                assert_eq!(object_json, object_back);
            });
        }

        #[test]
        fn test_null_values_roundtrip(
            key in "[a-zA-Z]+",
            arr_size in 0..5usize
        ) {
            Python::with_gil(|py| {
                // Test null in object
                let mut map = serde_json::Map::new();
                map.insert(key, Value::Null);
                let object_json = Value::Object(map);
                let py_object = value_to_py(py, &object_json).unwrap();
                let object_back = py_to_value(&py_object.bind(py)).unwrap();
                assert_eq!(object_json, object_back);

                // Test null in array
                let array = vec![Value::Null; arr_size];
                let array_json = Value::Array(array);
                let py_array = value_to_py(py, &array_json).unwrap();
                let array_back = py_to_value(&py_array.bind(py)).unwrap();
                assert_eq!(array_json, array_back);
            });
        }
    }
} 