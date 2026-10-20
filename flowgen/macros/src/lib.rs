//! Derive macros for flowgen task configs, re-exported by `flowgen_core`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Implements `flowgen_core::task::TaskConfig` from the struct's fields.
///
/// `name` is required. `depends_on`, `retry` and `max_in_flight` return `None`
/// when the struct has no such field. A newtype delegates to its inner config.
#[proc_macro_derive(TaskConfig)]
pub fn derive_task_config(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let fields = match &input.data {
        Data::Struct(data) => &data.fields,
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "TaskConfig can only be derived for a struct",
            ))
        }
    };

    let body = match fields {
        Fields::Named(named) => {
            let has = |name: &str| {
                named
                    .named
                    .iter()
                    .any(|field| field.ident.as_ref().is_some_and(|ident| ident == name))
            };
            if !has("name") {
                return Err(syn::Error::new_spanned(
                    ident,
                    "TaskConfig needs a `name: String` field",
                ));
            }
            let depends_on = match has("depends_on") {
                true => quote! { self.depends_on.as_deref() },
                false => quote! { None },
            };
            let retry = match has("retry") {
                true => quote! { self.retry.as_ref() },
                false => quote! { None },
            };
            let max_in_flight = match has("max_in_flight") {
                true => quote! { self.max_in_flight },
                false => quote! { None },
            };
            quote! {
                fn name(&self) -> &str {
                    &self.name
                }
                fn depends_on(&self) -> Option<&[String]> {
                    #depends_on
                }
                fn retry(&self) -> Option<&::flowgen_core::retry::RetryConfig> {
                    #retry
                }
                fn max_in_flight(&self) -> Option<::std::num::NonZeroUsize> {
                    #max_in_flight
                }
            }
        }
        Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => quote! {
            fn name(&self) -> &str {
                ::flowgen_core::task::TaskConfig::name(&self.0)
            }
            fn depends_on(&self) -> Option<&[String]> {
                ::flowgen_core::task::TaskConfig::depends_on(&self.0)
            }
            fn retry(&self) -> Option<&::flowgen_core::retry::RetryConfig> {
                ::flowgen_core::task::TaskConfig::retry(&self.0)
            }
            fn max_in_flight(&self) -> Option<::std::num::NonZeroUsize> {
                ::flowgen_core::task::TaskConfig::max_in_flight(&self.0)
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "TaskConfig needs named fields or a single wrapped config",
            ))
        }
    };

    Ok(quote! {
        impl #impl_generics ::flowgen_core::task::TaskConfig for #ident #type_generics #where_clause {
            #body
        }
    })
}
