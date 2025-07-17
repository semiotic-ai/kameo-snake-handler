use pyo3::{prelude::*, types::PyAny};
use serde::Deserialize;

mod de;
mod ser;

pub use de::from_pyobject;
pub use ser::to_pyobject;

/// Trait for types that can be converted from PyAny
pub trait FromPyAny: Sized {
    fn from_py_any(obj: Bound<'_, PyAny>) -> Result<Self>;
}

impl<T> FromPyAny for T
where
    T: for<'de> Deserialize<'de>,
{
    fn from_py_any(obj: Bound<'_, PyAny>) -> Result<Self> {
        from_pyobject(&obj)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Py(PyErr),
    Serialization(String),
    Deserialization(String),
    UnsupportedType(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Py(err) => write!(f, "Python error: {}", err),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::Deserialization(msg) => write!(f, "Deserialization error: {}", msg),
            Error::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Serialization(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Deserialization(msg.to_string())
    }
}

impl From<serde::de::value::Error> for Error {
    fn from(err: serde::de::value::Error) -> Self {
        Error::Deserialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::{hash_map, vec};
    use proptest::prelude::*;
    use proptest::strategy::{BoxedStrategy, Strategy};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    mod nested_option_helper {
        use serde::{self, Deserialize, Deserializer, Serializer};

        pub fn serialize<S>(option: &Option<Option<i32>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match option {
                None => serializer.serialize_unit_variant("Option", 0, "None"),
                Some(None) => serializer.serialize_unit_variant("Option", 1, "SomeNone"),
                Some(Some(val)) => serializer.serialize_newtype_variant("Option", 2, "Some", val),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Option<i32>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(rename_all = "PascalCase")]
            enum OptionOption {
                None,
                SomeNone,
                Some(i32),
            }

            match OptionOption::deserialize(deserializer)? {
                OptionOption::None => Ok(None),
                OptionOption::SomeNone => Ok(Some(None)),
                OptionOption::Some(val) => Ok(Some(Some(val))),
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct AllPrimitivesStruct {
        // Integer types
        i8_field: i8,
        i16_field: i16,
        i32_field: i32,
        i64_field: i64,
        u8_field: u8,
        u16_field: u16,
        u32_field: u32,
        u64_field: u64,
        // Floating point
        f32_field: f32,
        f64_field: f64,
        // Other primitives
        char_field: char,
        bool_field: bool,
        string_field: String,
        bytes_field: Vec<u8>,
    }

    impl PartialEq for AllPrimitivesStruct {
        fn eq(&self, other: &Self) -> bool {
            // Compare integer fields
            self.i8_field == other.i8_field &&
            self.i16_field == other.i16_field &&
            self.i32_field == other.i32_field &&
            self.i64_field == other.i64_field &&
            self.u8_field == other.u8_field &&
            self.u16_field == other.u16_field &&
            self.u32_field == other.u32_field &&
            self.u64_field == other.u64_field &&
            // Compare float fields with special handling for NaN
            (self.f32_field.is_nan() && other.f32_field.is_nan() ||
             self.f32_field == other.f32_field) &&
            (self.f64_field.is_nan() && other.f64_field.is_nan() ||
             self.f64_field == other.f64_field) &&
            // Compare other fields
            self.char_field == other.char_field &&
            self.bool_field == other.bool_field &&
            self.string_field == other.string_field &&
            self.bytes_field == other.bytes_field
        }
    }

    fn integer_strategy() -> BoxedStrategy<(i8, i16, i32, i64, u8, u16, u32, u64)> {
        (
            any::<i8>(),
            any::<i16>(),
            any::<i32>(),
            any::<i64>(),
            any::<u8>(),
            any::<u16>(),
            any::<u32>(),
            any::<u64>(),
        )
            .boxed()
    }

    fn float_strategy() -> BoxedStrategy<(f32, f64)> {
        (
            prop_oneof![
                Just(f32::INFINITY),
                Just(f32::NEG_INFINITY),
                Just(f32::NAN),
                Just(f32::MIN),
                Just(f32::MAX),
                any::<f32>()
            ],
            prop_oneof![
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                Just(f64::NAN),
                Just(f64::MIN),
                Just(f64::MAX),
                any::<f64>()
            ],
        )
            .boxed()
    }

    fn other_primitives_strategy() -> BoxedStrategy<(char, bool, String, Vec<u8>)> {
        (
            any::<char>(),
            any::<bool>(),
            any::<String>(),
            vec(any::<u8>(), 0..100),
        )
            .boxed()
    }

    fn all_primitives_strategy() -> BoxedStrategy<AllPrimitivesStruct> {
        (
            integer_strategy(),
            float_strategy(),
            other_primitives_strategy(),
        )
            .prop_map(
                |(
                    (i8_f, i16_f, i32_f, i64_f, u8_f, u16_f, u32_f, u64_f),
                    (f32_f, f64_f),
                    (char_f, bool_f, string_f, bytes_f),
                )| {
                    AllPrimitivesStruct {
                        i8_field: i8_f,
                        i16_field: i16_f,
                        i32_field: i32_f,
                        i64_field: i64_f,
                        u8_field: u8_f,
                        u16_field: u16_f,
                        u32_field: u32_f,
                        u64_field: u64_f,
                        f32_field: f32_f,
                        f64_field: f64_f,
                        char_field: char_f,
                        bool_field: bool_f,
                        string_field: string_f,
                        bytes_field: bytes_f,
                    }
                },
            )
            .boxed()
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct EdgeCaseStruct {
        // Empty collections
        empty_vec: Vec<i32>,
        empty_map: HashMap<String, i32>,
        // Nested options
        #[serde(with = "nested_option_helper")]
        nested_option: Option<Option<i32>>,
        // Unit struct
        unit: UnitStruct,
        // Newtype struct
        newtype: NewtypeStruct,
        // Recursive type with max depth 3
        recursive: Option<Box<EdgeCaseStruct>>,
        // Special string cases
        empty_string: String,
        unicode_string: String,
        // Special collection cases
        single_item_vec: Vec<i32>,
        single_item_map: HashMap<String, i32>,
        // Deep nesting
        nested_vec: Vec<Vec<Vec<i32>>>,
        nested_map: HashMap<String, HashMap<String, i32>>,
    }

    impl PartialEq for EdgeCaseStruct {
        fn eq(&self, other: &Self) -> bool {
            // Compare all fields explicitly for better debugging
            let empty_vec_eq = self.empty_vec == other.empty_vec;
            let empty_map_eq = self.empty_map == other.empty_map;
            let nested_option_eq = self.nested_option == other.nested_option;
            let unit_eq = self.unit == other.unit;
            let newtype_eq = self.newtype == other.newtype;
            let recursive_eq = match (&self.recursive, &other.recursive) {
                (None, None) => true,
                (Some(a), Some(b)) => a == b,
                _ => false,
            };
            let empty_string_eq = self.empty_string == other.empty_string;
            let unicode_string_eq = self.unicode_string == other.unicode_string;
            let single_item_vec_eq = self.single_item_vec == other.single_item_vec;
            let single_item_map_eq = self.single_item_map == other.single_item_map;
            let nested_vec_eq = self.nested_vec == other.nested_vec;
            let nested_map_eq = self.nested_map == other.nested_map;

            // Print debug info if any field doesn't match
            if !empty_vec_eq {
                println!(
                    "empty_vec mismatch: {:?} != {:?}",
                    self.empty_vec, other.empty_vec
                );
            }
            if !empty_map_eq {
                println!(
                    "empty_map mismatch: {:?} != {:?}",
                    self.empty_map, other.empty_map
                );
            }
            if !nested_option_eq {
                println!(
                    "nested_option mismatch: {:?} != {:?}",
                    self.nested_option, other.nested_option
                );
            }
            if !unit_eq {
                println!("unit mismatch");
            }
            if !newtype_eq {
                println!(
                    "newtype mismatch: {:?} != {:?}",
                    self.newtype, other.newtype
                );
            }
            if !recursive_eq {
                println!(
                    "recursive mismatch: {:?} != {:?}",
                    self.recursive, other.recursive
                );
            }
            if !empty_string_eq {
                println!(
                    "empty_string mismatch: {:?} != {:?}",
                    self.empty_string, other.empty_string
                );
            }
            if !unicode_string_eq {
                println!(
                    "unicode_string mismatch: {:?} != {:?}",
                    self.unicode_string, other.unicode_string
                );
            }
            if !single_item_vec_eq {
                println!(
                    "single_item_vec mismatch: {:?} != {:?}",
                    self.single_item_vec, other.single_item_vec
                );
            }
            if !single_item_map_eq {
                println!(
                    "single_item_map mismatch: {:?} != {:?}",
                    self.single_item_map, other.single_item_map
                );
            }
            if !nested_vec_eq {
                println!(
                    "nested_vec mismatch: {:?} != {:?}",
                    self.nested_vec, other.nested_vec
                );
            }
            if !nested_map_eq {
                println!(
                    "nested_map mismatch: {:?} != {:?}",
                    self.nested_map, other.nested_map
                );
            }

            empty_vec_eq
                && empty_map_eq
                && nested_option_eq
                && unit_eq
                && newtype_eq
                && recursive_eq
                && empty_string_eq
                && unicode_string_eq
                && single_item_vec_eq
                && single_item_map_eq
                && nested_vec_eq
                && nested_map_eq
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct UnitStruct;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct NewtypeStruct(i32);

    fn special_string_strategy() -> BoxedStrategy<(String, String)> {
        (
            Just(String::new()),
            prop_oneof![
                // Basic ASCII
                Just("Hello, World!".to_string()),
                // Unicode
                Just("Hello, 世界!".to_string()),
                // Emojis
                Just("🦀 Rust 🚀".to_string()),
                // Control characters
                Just("\n\t\r".to_string()),
                // Very long string
                Just("x".repeat(1000)),
            ],
        )
            .boxed()
    }

    fn special_collection_strategy() -> BoxedStrategy<(Vec<i32>, HashMap<String, i32>)> {
        (
            Just(vec![42]),
            hash_map(Just("key".to_string()), Just(42), 1..=1),
        )
            .boxed()
    }

    fn nested_collection_strategy(
    ) -> BoxedStrategy<(Vec<Vec<Vec<i32>>>, HashMap<String, HashMap<String, i32>>)> {
        let nested_vec = Just(vec![vec![vec![1, 2, 3]], vec![vec![4, 5, 6]]]);
        let nested_map = {
            let mut inner = HashMap::new();
            inner.insert("inner".to_string(), 42);
            let mut outer = HashMap::new();
            outer.insert("outer".to_string(), inner);
            Just(outer)
        };
        (nested_vec, nested_map).boxed()
    }

    fn edge_case_strategy() -> BoxedStrategy<EdgeCaseStruct> {
        let leaf = (
            Just(Vec::new()),
            Just(HashMap::new()),
            prop_oneof![
                Just(None),
                any::<i32>().prop_map(|x| Some(Some(x))),
                Just(Some(None))
            ],
            Just(UnitStruct),
            any::<i32>().prop_map(NewtypeStruct),
            special_string_strategy(),
            special_collection_strategy(),
            nested_collection_strategy(),
        )
            .prop_map(
                |(
                    empty_vec,
                    empty_map,
                    nested_option,
                    unit,
                    newtype,
                    (empty_string, unicode_string),
                    (single_item_vec, single_item_map),
                    (nested_vec, nested_map),
                )| {
                    EdgeCaseStruct {
                        empty_vec,
                        empty_map,
                        nested_option,
                        unit,
                        newtype,
                        recursive: None,
                        empty_string,
                        unicode_string,
                        single_item_vec,
                        single_item_map,
                        nested_vec,
                        nested_map,
                    }
                },
            );

        leaf.prop_recursive(
            3,   // depth
            256, // total elements
            10,  // items per collection
            |inner| {
                (
                    Just(Vec::new()),
                    Just(HashMap::new()),
                    prop_oneof![
                        Just(None),
                        any::<i32>().prop_map(|x| Some(Some(x))),
                        Just(Some(None))
                    ],
                    Just(UnitStruct),
                    any::<i32>().prop_map(NewtypeStruct),
                    prop::option::of(inner.prop_map(Box::new)),
                    special_string_strategy(),
                    special_collection_strategy(),
                    nested_collection_strategy(),
                )
                    .prop_map(
                        |(
                            empty_vec,
                            empty_map,
                            nested_option,
                            unit,
                            newtype,
                            recursive,
                            (empty_string, unicode_string),
                            (single_item_vec, single_item_map),
                            (nested_vec, nested_map),
                        )| {
                            EdgeCaseStruct {
                                empty_vec,
                                empty_map,
                                nested_option,
                                unit,
                                newtype,
                                recursive,
                                empty_string,
                                unicode_string,
                                single_item_vec,
                                single_item_map,
                                nested_vec,
                                nested_map,
                            }
                        },
                    )
            },
        )
        .boxed()
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum ComplexEnum {
        Unit,
        NewType(i32),
        Tuple(i32, String, bool),
        Struct { x: i32, y: String, z: bool },
    }

    fn complex_enum_strategy() -> BoxedStrategy<ComplexEnum> {
        prop_oneof![
            Just(ComplexEnum::Unit),
            any::<i32>().prop_map(ComplexEnum::NewType),
            (any::<i32>(), any::<String>(), any::<bool>())
                .prop_map(|(i, s, b)| ComplexEnum::Tuple(i, s, b)),
            (any::<i32>(), any::<String>(), any::<bool>())
                .prop_map(|(x, y, z)| ComplexEnum::Struct { x, y, z })
        ]
        .boxed()
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct AsyncTestStruct {
        future_result: Option<i32>,
        stream_results: Vec<String>,
        nested_futures: Vec<Option<ComplexEnum>>,
    }

    fn async_test_struct_strategy() -> BoxedStrategy<AsyncTestStruct> {
        (
            prop::option::of(any::<i32>()),
            vec(any::<String>(), 0..10),
            vec(prop::option::of(complex_enum_strategy()), 0..5),
        )
            .prop_map(
                |(future_result, stream_results, nested_futures)| AsyncTestStruct {
                    future_result,
                    stream_results,
                    nested_futures,
                },
            )
            .boxed()
    }

    proptest! {
        #[test]
        fn test_all_primitives_roundtrip(test_struct in all_primitives_strategy()) {
            let py_obj = Python::with_gil(|py| to_pyobject(py, &test_struct).unwrap());
            let roundtrip = Python::with_gil(|py| from_pyobject(py_obj.bind(py)).unwrap());
            prop_assert_eq!(test_struct, roundtrip)
        }

        #[test]
        fn test_edge_cases_roundtrip(test_struct in edge_case_strategy()) {
            let py_obj = Python::with_gil(|py| to_pyobject(py, &test_struct).unwrap());
            let roundtrip = Python::with_gil(|py| from_pyobject(py_obj.bind(py)).unwrap());
            prop_assert_eq!(test_struct, roundtrip)
        }

        #[test]
        fn test_complex_enum_roundtrip(value in complex_enum_strategy()) {
            Python::with_gil(|py| {
                let py_obj = to_pyobject(py, &value).unwrap();
                let roundtrip: ComplexEnum = from_pyobject(py_obj.bind(py)).unwrap();
                assert_eq!(value, roundtrip);
            });
        }

        #[test]
        fn test_async_struct_roundtrip(value in async_test_struct_strategy()) {
            Python::with_gil(|py| {
                let py_obj = to_pyobject(py, &value).unwrap();
                let roundtrip: AsyncTestStruct = from_pyobject(py_obj.bind(py)).unwrap();
                assert_eq!(value, roundtrip);
            });
        }

        #[test]
        fn test_deep_recursive_enum(depth in 0usize..5) {
            #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
            enum RecursiveEnum {
                Leaf(i32),
                Node(Box<RecursiveEnum>, String, Box<RecursiveEnum>),
            }

            fn make_recursive_enum(depth: usize) -> RecursiveEnum {
                if depth == 0 {
                    RecursiveEnum::Leaf(42)
                } else {
                    RecursiveEnum::Node(
                        Box::new(make_recursive_enum(depth - 1)),
                        format!("Level {}", depth),
                        Box::new(make_recursive_enum(depth - 1))
                    )
                }
            }

            let value = make_recursive_enum(depth);
            Python::with_gil(|py| {
                let py_obj = to_pyobject(py, &value).unwrap();
                let roundtrip: RecursiveEnum = from_pyobject(py_obj.bind(py)).unwrap();
                assert_eq!(value, roundtrip);
            });
        }
    }
}
