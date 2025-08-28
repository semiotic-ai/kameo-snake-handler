/// Reference to a type used in fields/variants
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeRef {
    /// Builtins: "int", "float", "bool", "str", "bytes", "any"
    Builtin(String),
    /// Optional[T]
    Option(Box<TypeRef>),
    /// list[T]
    List(Box<TypeRef>),
    /// dict[str, T] (Python's JSON-friendly map)
    Dict(Box<TypeRef>),
    /// Named type (struct or enum alias)
    Named(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructDecl {
    pub name: String,
    /// Vec<(field_name, type)>
    pub fields: Vec<(String, TypeRef)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnumVariant {
    /// Unit variant: tag only
    Unit { name: String },
    /// Newtype variant: single unnamed payload
    Newtype { name: String, ty: TypeRef },
    /// Tuple variant: T0, T1, ...
    Tuple { name: String, tys: Vec<TypeRef> },
    /// Struct variant: named fields
    Struct { name: String, fields: Vec<(String, TypeRef)> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumDecl {
    pub name: String,
    pub variants: Vec<EnumVariant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decl {
    Struct(StructDecl),
    Enum(EnumDecl),
}


