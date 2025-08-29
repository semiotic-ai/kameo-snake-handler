use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DataStruct, DeriveInput, Fields, Type, TypePath};

#[proc_macro_derive(PythonIr)]
pub fn derive_python_ir(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Build IR for the container
    let decl_expr = match &input.data {
        Data::Struct(ds) => build_struct_decl(&ident.to_string(), ds),
        Data::Enum(de) => build_enum_decl(&ident.to_string(), de),
        Data::Union(_) => {
            return syn::Error::new_spanned(ident, "PythonIr cannot be derived for unions")
                .to_compile_error()
                .into();
        }
    };

    // Recursively collect nested IR from field/variant types
    let nested_extends = match &input.data {
        Data::Struct(ds) => extend_nested_for_struct(ds),
        Data::Enum(de) => extend_nested_for_enum(de),
        Data::Union(_) => quote! {},
    };

    let output = quote! {
        impl #impl_generics kameo_snake_handler::codegen_py::ProvideIr for #ident #ty_generics #where_clause {
            fn provide_ir() -> Vec<kameo_snake_handler::codegen_py::Decl> {
                let mut __decls: Vec<kameo_snake_handler::codegen_py::Decl> = Vec::new();
                #nested_extends
                __decls.push(#decl_expr);
                __decls
            }
        }
    };
    output.into()
}

fn build_struct_decl(name: &str, ds: &DataStruct) -> proc_macro2::TokenStream {
    let fields = match &ds.fields {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|f| {
                let fname = f.ident.as_ref().unwrap().to_string();
                let tref = type_to_typeref(&f.ty);
                quote! { (#fname.to_string(), #tref) }
            })
            .collect::<Vec<_>>(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let fname = format!("field_{}", i);
                let tref = type_to_typeref(&f.ty);
                quote! { (#fname.to_string(), #tref) }
            })
            .collect::<Vec<_>>(),
        Fields::Unit => Vec::new(),
    };
    quote! {
        kameo_snake_handler::codegen_py::Decl::Struct(
            kameo_snake_handler::codegen_py::StructDecl{
                name: #name.to_string(),
                fields: vec![ #( #fields ),* ],
            }
        )
    }
}

fn build_enum_decl(name: &str, de: &DataEnum) -> proc_macro2::TokenStream {
    let mut variants_ts: Vec<proc_macro2::TokenStream> = Vec::new();
    for v in &de.variants {
        let vname = v.ident.to_string();
        match &v.fields {
            Fields::Unit => {
                variants_ts.push(quote! { kameo_snake_handler::codegen_py::EnumVariant::Unit{ name: #vname.to_string() } });
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.len() == 1 {
                    let ty = &unnamed.unnamed.first().unwrap().ty;
                    let tref = type_to_typeref(ty);
                    variants_ts.push(quote! { kameo_snake_handler::codegen_py::EnumVariant::Newtype{ name: #vname.to_string(), ty: #tref } });
                } else {
                    let mut elems: Vec<proc_macro2::TokenStream> = Vec::new();
                    for f in &unnamed.unnamed {
                        elems.push(type_to_typeref(&f.ty));
                    }
                    variants_ts.push(quote! { kameo_snake_handler::codegen_py::EnumVariant::Tuple{ name: #vname.to_string(), tys: vec![ #( #elems ),* ] } });
                }
            }
            Fields::Named(named) => {
                let mut nfields: Vec<proc_macro2::TokenStream> = Vec::new();
                for f in &named.named {
                    let fname = f.ident.as_ref().unwrap().to_string();
                    let tref = type_to_typeref(&f.ty);
                    nfields.push(quote! { (#fname.to_string(), #tref) });
                }
                variants_ts.push(quote! { kameo_snake_handler::codegen_py::EnumVariant::Struct{ name: #vname.to_string(), fields: vec![ #( #nfields ),* ] } });
            }
        }
    }
    quote! {
        kameo_snake_handler::codegen_py::Decl::Enum(
            kameo_snake_handler::codegen_py::EnumDecl{
                name: #name.to_string(),
                variants: vec![ #( #variants_ts ),* ],
            }
        )
    }
}

fn extend_nested_for_struct(ds: &DataStruct) -> proc_macro2::TokenStream {
    match &ds.fields {
        Fields::Named(named) => {
            let mut exts = Vec::new();
            for f in &named.named { exts.push(extend_nested_for_type(&f.ty)); }
            quote! { #( #exts )* }
        }
        Fields::Unnamed(unnamed) => {
            let mut exts = Vec::new();
            for f in &unnamed.unnamed { exts.push(extend_nested_for_type(&f.ty)); }
            quote! { #( #exts )* }
        }
        Fields::Unit => quote! {},
    }
}

fn extend_nested_for_enum(de: &DataEnum) -> proc_macro2::TokenStream {
    let mut exts = Vec::new();
    for v in &de.variants {
        match &v.fields {
            Fields::Unit => {}
            Fields::Unnamed(unnamed) => {
                for f in &unnamed.unnamed { exts.push(extend_nested_for_type(&f.ty)); }
            }
            Fields::Named(named) => {
                for f in &named.named { exts.push(extend_nested_for_type(&f.ty)); }
            }
        }
    }
    quote! { #( #exts )* }
}

fn extend_nested_for_type(ty: &Type) -> proc_macro2::TokenStream {
    match ty {
        Type::Reference(r) => extend_nested_for_type(&r.elem),
        Type::Path(tp) => {
            if let Some(seg) = tp.path.segments.first() {
                let id = seg.ident.to_string();
                if id == "Option" || id == "Vec" || id == "Box" {
                    if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                        if let Some(syn::GenericArgument::Type(inner)) = ab.args.first() {
                            let nest = extend_nested_for_type(inner);
                            return quote! { #nest };
                        }
                    }
                } else if id == "HashMap" || id == "BTreeMap" {
                    if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                        if ab.args.len() == 2 {
                            if let Some(syn::GenericArgument::Type(inner_v)) = ab.args.iter().nth(1) {
                                let nest = extend_nested_for_type(inner_v);
                                return quote! { #nest };
                            }
                        }
                    }
                } else if is_builtin_path(tp) || is_json_value(tp) || is_string_path(tp) {
                    return quote! {};
                }
            }
            quote! {
                {
                    let mut __v = <#tp as kameo_snake_handler::codegen_py::ProvideIr>::provide_ir();
                    __decls.append(&mut __v);
                }
            }
        }
        Type::Array(a) => extend_nested_for_type(&a.elem),
        Type::Tuple(tup) => {
            let mut exts = Vec::new();
            for elem in &tup.elems { exts.push(extend_nested_for_type(elem)); }
            quote! { #( #exts )* }
        }
        _ => quote! {},
    }
}

fn type_to_typeref(ty: &Type) -> proc_macro2::TokenStream {
    match ty {
        Type::Reference(r) => type_to_typeref(&r.elem),
        Type::Path(tp) => path_to_typeref(tp),
        Type::Array(arr) => {
            let inner = type_to_typeref(&arr.elem);
            quote! { kameo_snake_handler::codegen_py::TypeRef::List(Box::new(#inner)) }
        }
        Type::Tuple(_tup) => {
            quote! { kameo_snake_handler::codegen_py::TypeRef::List(Box::new(kameo_snake_handler::codegen_py::TypeRef::Builtin("any".to_string()))) }
        }
        _ => quote! { kameo_snake_handler::codegen_py::TypeRef::Builtin("any".to_string()) },
    }
}

fn path_to_typeref(tp: &TypePath) -> proc_macro2::TokenStream {
    if let Some(seg) = tp.path.segments.first() {
        let id = seg.ident.to_string();
        if id == "Option" {
            if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = ab.args.first() {
                    let inner_ts = type_to_typeref(inner);
                    return quote! { kameo_snake_handler::codegen_py::TypeRef::Option(Box::new(#inner_ts)) };
                }
            }
        }
        if id == "Vec" {
            if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = ab.args.first() {
                    let inner_ts = type_to_typeref(inner);
                    return quote! { kameo_snake_handler::codegen_py::TypeRef::List(Box::new(#inner_ts)) };
                }
            }
        }
        if id == "HashMap" || id == "BTreeMap" {
            if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                if ab.args.len() == 2 {
                    let mut args_iter = ab.args.iter();
                    let _key = args_iter.next().unwrap();
                    let val = args_iter.next().unwrap();
                    let v_ts = if let syn::GenericArgument::Type(vt) = val { type_to_typeref(vt) } else { quote! { kameo_snake_handler::codegen_py::TypeRef::Builtin("any".to_string()) } };
                    return quote! { kameo_snake_handler::codegen_py::TypeRef::Dict(Box::new(#v_ts)) };
                }
            }
        }
        if is_builtin_path(tp) {
            let name = builtin_name(&id);
            return quote! { kameo_snake_handler::codegen_py::TypeRef::Builtin(#name.to_string()) };
        }
        if is_json_value(tp) {
            return quote! { kameo_snake_handler::codegen_py::TypeRef::Builtin("any".to_string()) };
        }
        if is_string_path(tp) {
            return quote! { kameo_snake_handler::codegen_py::TypeRef::Builtin("str".to_string()) };
        }
        let last = tp.path.segments.last().unwrap().ident.to_string();
        return quote! { kameo_snake_handler::codegen_py::TypeRef::Named(#last.to_string()) };
    }
    quote! { kameo_snake_handler::codegen_py::TypeRef::Builtin("any".to_string()) }
}

fn is_builtin_path(tp: &TypePath) -> bool {
    tp.path.segments.len() == 1 && is_builtin_ident(&tp.path.segments.first().unwrap().ident.to_string())
}

fn is_builtin_ident(id: &str) -> bool {
    matches!(id, "bool"|"i8"|"i16"|"i32"|"i64"|"i128"|"u8"|"u16"|"u32"|"u64"|"u128"|"f32"|"f64"|"char")
}

fn builtin_name(id: &str) -> &str {
    match id {
        "bool" => "bool",
        "i8"|"i16"|"i32"|"i64"|"i128"|"u8"|"u16"|"u32"|"u64"|"u128" => "int",
        "f32"|"f64" => "float",
        "char" => "str",
        _ => "any",
    }
}

fn is_string_path(tp: &TypePath) -> bool {
    tp.path.segments.len() == 1 && tp.path.segments.first().unwrap().ident == "String"
}

fn is_json_value(tp: &TypePath) -> bool {
    let segs = &tp.path.segments;
    if segs.len() >= 2 {
        let a = segs.iter().nth(segs.len()-2).unwrap().ident.to_string();
        let b = segs.last().unwrap().ident.to_string();
        return (a == "json" || a == "serde_json") && b == "Value";
    }
    false
}


