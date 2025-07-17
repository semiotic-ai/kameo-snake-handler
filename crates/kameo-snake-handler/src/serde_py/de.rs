use pyo3::{
    prelude::*,
    types::{PyAny, PyBool, PyDict, PyFloat, PyInt, PyList, PyString},
    Bound, FromPyObject,
};
use serde::de::{
    self, Deserialize, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use tracing::{error, instrument, trace};

use super::{Error, Result};

impl From<PyErr> for Error {
    fn from(err: PyErr) -> Self {
        Error::Deserialization(format!("Python error: {}", err))
    }
}

impl From<pyo3::DowncastError<'_, '_>> for Error {
    fn from(e: pyo3::DowncastError<'_, '_>) -> Self {
        Error::Deserialization(e.to_string())
    }
}

/// Convert a Python object to a Rust value
#[instrument(skip(obj))]
pub fn from_pyobject<'py, T>(obj: &Bound<'py, PyAny>) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let deserializer = PythonDeserializer { input: obj.clone() };
    match T::deserialize(deserializer) {
        Ok(value) => {
            trace!(status = "success");
            Ok(value)
        }
        Err(e) => {
            error!(status = "failure", error = %e);
            Err(e)
        }
    }
}

pub struct PythonDeserializer<'py> {
    input: Bound<'py, PyAny>,
}

pub struct SeqDeserializer<'py> {
    seq: Bound<'py, PyList>,
    idx: usize,
}

pub struct MapDeserializer<'py> {
    map: Bound<'py, PyDict>,
    keys: Vec<Bound<'py, PyAny>>,
    current_idx: usize,
}

pub struct EnumDeserializer<'py> {
    input: Bound<'py, PyAny>,
}

pub struct UnitEnumDeserializer {
    variant: String,
}

impl<'de, 'py> de::Deserializer<'de> for PythonDeserializer<'py>
where
    'py: 'de,
{
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.input.is_none() {
            visitor.visit_unit()
        } else {
            let input = &self.input;
            if input.is_instance_of::<PyBool>() {
                visitor.visit_bool(bool::extract_bound(input)?)
            } else if input.is_instance_of::<PyInt>() {
                visitor.visit_i64(i64::extract_bound(input)?)
            } else if input.is_instance_of::<PyFloat>() {
                visitor.visit_f64(f64::extract_bound(input)?)
            } else if input.is_instance_of::<PyString>() {
                visitor.visit_string(String::extract_bound(input)?)
            } else if input.is_instance_of::<PyList>() {
                let list = input.downcast::<PyList>()?;
                visitor.visit_seq(SeqDeserializer { seq: list.clone(), idx: 0 })
            } else if input.is_instance_of::<PyDict>() {
                let dict = input.downcast::<PyDict>()?;
                let mut keys = Vec::new();
                for k in dict.keys().iter() {
                    keys.push(k.clone());
                }
                visitor.visit_map(MapDeserializer { map: dict.clone(), keys, current_idx: 0 })
            } else {
                Err(Error::Deserialization(format!("unsupported Python type: {}", input.get_type().name()?)))
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(bool::extract_bound(&self.input)?)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(i8::extract_bound(&self.input)?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(i16::extract_bound(&self.input)?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(i32::extract_bound(&self.input)?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(i64::extract_bound(&self.input)?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(u8::extract_bound(&self.input)?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(u16::extract_bound(&self.input)?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(u32::extract_bound(&self.input)?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(u64::extract_bound(&self.input)?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(f32::extract_bound(&self.input)?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(f64::extract_bound(&self.input)?)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let s = String::extract_bound(&self.input)?;
        let mut chars = s.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => visitor.visit_char(c),
            _ => Err(Error::Deserialization(
                "expected single character string".to_string(),
            )),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(String::extract_bound(&self.input)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let bytes = Vec::<u8>::extract_bound(&self.input)?;
        visitor.visit_bytes(&bytes)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.input.is_none() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.input.is_none() {
            visitor.visit_unit()
        } else {
            Err(Error::Deserialization("expected None".to_string()))
        }
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if !self.input.is_instance_of::<PyList>() {
            return Err(Error::Deserialization(format!(
                "expected list, got {}",
                self.input.get_type().name()?
            )));
        }

        let list = self
            .input
            .downcast::<PyList>()
            .map_err(|_| Error::Deserialization("expected list".to_string()))?;
        visitor.visit_seq(SeqDeserializer {
            seq: list.clone(),
            idx: 0,
        })
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if !self.input.is_instance_of::<PyDict>() {
            return Err(Error::Deserialization(format!(
                "expected dict, got {}",
                self.input.get_type().name()?
            )));
        }

        let dict = self
            .input
            .downcast::<PyDict>()
            .map_err(|_| Error::Deserialization("expected dict".to_string()))?;
        let mut keys = Vec::new();
        for k in dict.keys().iter() {
            keys.push(k.clone());
        }

        visitor.visit_map(MapDeserializer {
            map: dict.clone(),
            keys,
            current_idx: 0,
        })
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let _py = self.input.py();

        if let Ok(dict) = self.input.downcast::<PyDict>() {
            if dict.len() == 1 {
                return visitor.visit_enum(EnumDeserializer { input: self.input });
            }
        }

        if let Ok(s) = String::extract_bound(&self.input) {
            return visitor.visit_enum(UnitEnumDeserializer { variant: s });
        }

        Err(Error::Deserialization("invalid enum value".to_string()))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

impl<'de, 'py> SeqAccess<'de> for SeqDeserializer<'py>
where
    'py: 'de,
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.idx >= self.seq.len() {
            return Ok(None);
        }

        let element = self.seq.get_item(self.idx)?;
        self.idx += 1;

        seed.deserialize(PythonDeserializer { input: element })
            .map(Some)
    }
}

impl<'de, 'py> MapAccess<'de> for MapDeserializer<'py>
where
    'py: 'de,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if self.current_idx >= self.keys.len() {
            return Ok(None);
        }

        let key = &self.keys[self.current_idx];
        seed.deserialize(PythonDeserializer { input: key.clone() })
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let key = &self.keys[self.current_idx];
        let value = self
            .map
            .get_item(key)
            .map_err(|e| Error::Deserialization(format!("failed to get dict item: {}", e)))?
            .ok_or_else(|| Error::Deserialization("missing value for key".to_string()))?;
        self.current_idx += 1;

        seed.deserialize(PythonDeserializer { input: value })
    }
}

impl<'de, 'py> EnumAccess<'de> for EnumDeserializer<'py>
where
    'py: 'de,
{
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let dict = self
            .input
            .downcast::<PyDict>()
            .map_err(|_| Error::Deserialization("expected dict for enum variant".to_string()))?;

        let (key, _) = dict
            .iter()
            .next()
            .ok_or_else(|| Error::Deserialization("empty dict for enum variant".to_string()))?;

        let key_str = String::extract_bound(&key)?;
        let variant = seed.deserialize(
            serde::de::value::StringDeserializer::<Self::Error>::new(key_str),
        )?;
        Ok((variant, self))
    }
}

impl<'de, 'py> VariantAccess<'de> for EnumDeserializer<'py>
where
    'py: 'de,
{
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        let dict = self
            .input
            .downcast::<PyDict>()
            .map_err(|_| Error::Deserialization("expected dict for enum variant".to_string()))?;

        let (_, value) = dict
            .iter()
            .next()
            .ok_or_else(|| Error::Deserialization("empty dict for enum variant".to_string()))?;

        seed.deserialize(PythonDeserializer {
            input: value.clone(),
        })
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let dict = self
            .input
            .downcast::<PyDict>()
            .map_err(|_| Error::Deserialization("expected dict for enum variant".to_string()))?;

        let (_, value) = dict
            .iter()
            .next()
            .ok_or_else(|| Error::Deserialization("empty dict for enum variant".to_string()))?;

        if let Ok(list) = value.downcast::<PyList>() {
            visitor.visit_seq(SeqDeserializer {
                seq: list.clone(),
                idx: 0,
            })
        } else {
            Err(Error::Deserialization(
                "expected list for tuple variant".to_string(),
            ))
        }
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let dict = self
            .input
            .downcast::<PyDict>()
            .map_err(|_| Error::Deserialization("expected dict for enum variant".to_string()))?;

        let (_, value) = dict
            .iter()
            .next()
            .ok_or_else(|| Error::Deserialization("empty dict for enum variant".to_string()))?;

        if let Ok(dict) = value.downcast::<PyDict>() {
            let mut keys = Vec::new();
            for k in dict.keys().iter() {
                keys.push(k.clone());
            }
            visitor.visit_map(MapDeserializer {
                map: dict.clone(),
                keys,
                current_idx: 0,
            })
        } else {
            Err(Error::Deserialization(
                "expected dict for struct variant".to_string(),
            ))
        }
    }
}

impl<'de> EnumAccess<'de> for UnitEnumDeserializer {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(mut self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = std::mem::take(&mut self.variant);
        let variant_value = seed.deserialize(
            serde::de::value::StringDeserializer::<Self::Error>::new(variant),
        )?;
        Ok((variant_value, self))
    }
}

impl<'de> VariantAccess<'de> for UnitEnumDeserializer {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        Err(Error::Deserialization(
            "unit enum deserializer cannot deserialize newtype variant".to_string(),
        ))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::Deserialization(
            "unit enum deserializer cannot deserialize tuple variant".to_string(),
        ))
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        Err(Error::Deserialization(
            "unit enum deserializer cannot deserialize struct variant".to_string(),
        ))
    }
}
