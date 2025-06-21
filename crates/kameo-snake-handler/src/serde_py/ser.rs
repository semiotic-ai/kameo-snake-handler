use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
    IntoPyObjectExt,
    Bound,
    Python,
    PyObject,
};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant, SerializeStruct, SerializeStructVariant};

use super::Error;
use super::Result;

/// Convert a Rust value to a Python object
pub fn to_pyobject<'py, T>(py: Python<'py>, value: &T) -> Result<PyObject>
where
    T: Serialize,
{
    value.serialize(PythonSerializer::new(py))
}

pub struct PythonSerializer<'py> {
    py: Python<'py>,
}

impl<'py> PythonSerializer<'py> {
    pub fn new(py: Python<'py>) -> Self {
        Self { py }
    }
}

impl<'py> serde::Serializer for PythonSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    type SerializeSeq = PythonSeqSerializer<'py>;
    type SerializeTuple = PythonSeqSerializer<'py>;
    type SerializeTupleStruct = PythonSeqSerializer<'py>;
    type SerializeTupleVariant = PythonSeqSerializer<'py>;
    type SerializeMap = PythonMapSerializer<'py>;
    type SerializeStruct = PythonMapSerializer<'py>;
    type SerializeStructVariant = PythonMapSerializer<'py>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        Ok(v.to_string().into_bound_py_any(self.py)?.into())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        Ok(v.into_bound_py_any(self.py)?.into())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Ok(self.py.None().into())
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(self.py.None())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        Ok(variant.into_bound_py_any(self.py)?.into())
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        let dict = PyDict::new(self.py);
        dict.set_item(variant, value.serialize(Self::new(self.py))?)?;
        Ok(dict.into_bound_py_any(self.py)?.into())
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(PythonSeqSerializer {
            py: self.py,
            list: PyList::empty(self.py).into(),
            next_key: None,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        let mut seq = PythonSeqSerializer::new(self.py, Some(len))?;
        seq.next_key = Some(variant.to_string());
        Ok(seq)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        PythonMapSerializer::new(self.py)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct> {
        let dict = PyDict::new(self.py);
        Ok(PythonMapSerializer {
            py: self.py,
            dict: dict.into(),
            next_key: None,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        let mut map = PythonMapSerializer::new(self.py)?;
        map.next_key = Some(variant.to_string());
        Ok(map)
    }
}

pub struct PythonSeqSerializer<'py> {
    py: Python<'py>,
    list: Bound<'py, PyList>,
    next_key: Option<String>,
}

impl<'py> PythonSeqSerializer<'py> {
    fn new(py: Python<'py>, _len: Option<usize>) -> Result<Self> {
        Ok(Self {
            py,
            list: PyList::empty(py).into(),
            next_key: None,
        })
    }
}

impl<'py> SerializeSeq for PythonSeqSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where T: Serialize {
        let element = value.serialize(PythonSerializer::new(self.py))?;
        self.list.append(element)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        if let Some(variant) = self.next_key {
            let dict = PyDict::new(self.py);
            dict.set_item(variant, self.list)?;
            Ok(dict.into_bound_py_any(self.py)?.into())
        } else {
            Ok(self.list.into_bound_py_any(self.py)?.into())
        }
    }
}

impl<'py> SerializeTuple for PythonSeqSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where T: Serialize {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok> {
        SerializeSeq::end(self)
    }
}

impl<'py> SerializeTupleStruct for PythonSeqSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where T: Serialize {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok> {
        SerializeSeq::end(self)
    }
}

impl<'py> SerializeTupleVariant for PythonSeqSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Self::Ok> {
        let variant = self.next_key.ok_or_else(|| Error::Serialization("missing variant key".to_string()))?;
        let outer = PyDict::new(self.py);
        outer.set_item(variant, self.list)?;
        Ok(outer.into_bound_py_any(self.py)?.into())
    }
}

pub struct PythonMapSerializer<'py> {
    py: Python<'py>,
    dict: Bound<'py, PyDict>,
    next_key: Option<String>,
}

impl<'py> PythonMapSerializer<'py> {
    fn new(py: Python<'py>) -> Result<Self> {
        Ok(Self {
            py,
            dict: PyDict::new(py).into(),
            next_key: None,
        })
    }
}

impl<'py> SerializeMap for PythonMapSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<()>
    where T: Serialize {
        self.next_key = Some(key.serialize(PythonSerializer::new(self.py))?.extract(self.py)
            .map_err(|e| Error::Serialization(format!("failed to extract key: {}", e)))?);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<()>
    where T: Serialize {
        let key = self.next_key.take()
            .ok_or_else(|| Error::Serialization("missing key in map serialization".to_string()))?;
        let value = value.serialize(PythonSerializer::new(self.py))?;
        self.dict.set_item(key, value)
            .map_err(|e| Error::Serialization(format!("failed to set dict item: {}", e)))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.dict.into_bound_py_any(self.py)
            .map_err(|e| Error::Serialization(format!("failed to convert dict to PyAny: {}", e)))?.into())
    }
}

impl<'py> SerializeStruct for PythonMapSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()>
    where T: Serialize {
        let value = value.serialize(PythonSerializer::new(self.py))?;
        self.dict.set_item(key, value)
            .map_err(|e| Error::Serialization(format!("failed to set struct field '{}': {}", key, e)))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.dict.into_bound_py_any(self.py)
            .map_err(|e| Error::Serialization(format!("failed to convert struct to PyAny: {}", e)))?.into())
    }
}

impl<'py> SerializeStructVariant for PythonMapSerializer<'py> {
    type Ok = PyObject;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let value = value.serialize(PythonSerializer::new(self.py))?;
        self.dict.set_item(key, value)
            .map_err(|e| Error::Serialization(format!("failed to set variant field '{}': {}", key, e)))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        let variant = self.next_key
            .ok_or_else(|| Error::Serialization("missing variant key in enum serialization".to_string()))?;
        let outer = PyDict::new(self.py);
        outer.set_item(variant, self.dict)
            .map_err(|e| Error::Serialization(format!("failed to set variant dict: {}", e)))?;
        Ok(outer.into_bound_py_any(self.py)
            .map_err(|e| Error::Serialization(format!("failed to convert variant to PyAny: {}", e)))?.into())
    }
} 