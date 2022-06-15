extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use proc_macro_error::abort;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(GroupByItem)]
pub fn derive_group_by_predicate(input: TokenStream) -> TokenStream {
    let DeriveInput { attrs: _, vis: _, ident, generics: _, data } = parse_macro_input!(input);

    let e = match data {
        Data::Enum(e) => e,
        Data::Struct(_) => abort!(ident, "GroupByItem derive not supported for structs"),
        Data::Union(_) => abort!(ident, "GroupByItem derive not supported for unions"),
    };

    let variant_patterns: Vec<_> = e.variants.iter()
        .map(|variant| {
            let name = &variant.ident;
            match variant.fields {
                Fields::Named(_) => quote! { #ident::#name { .. } },
                Fields::Unnamed(_) => quote! { #ident::#name(..) },
                Fields::Unit => quote! { #ident::#name },
            }
        })
        .collect();
    let vec_types: Vec<_> = e.variants.iter()
        .map(|_| quote! { ::std::vec::Vec<S::Item> })
        .collect();
    let vec_inits: Vec<_> = e.variants.iter()
        .map(|_| quote! { ::std::vec::Vec::new() })
        .collect();
    let arg_names: Vec<_> = e.variants.iter().enumerate()
        .map(|(i, _)| format!("arg{i}"))
        .collect();
    let arg_names: Vec<_> = arg_names.iter().map(|name| Ident::new(name, Span::call_site())).collect();

    (quote! {
        impl<'a, S: ::joins::__private::Stream + 'a> ::joins::group_by::GroupByItem<'a, S> for #ident {
            type Output = (#(#vec_types,)*);
            type Future = ::std::boxed::Box<dyn ::joins::__private::Future<Item = Self::Output, Error = S::Error> + 'a>;

            fn consume<P: ::joins::group_by::GroupByPredicate<'a, S, Self> + 'a>(mut predicate: P, stream: S) -> Self::Future {
                ::std::boxed::Box::new(stream.fold((#(#vec_inits,)*), move |(#(mut #arg_names,)*), item| {
                    match predicate.extract(&item) {
                        #(
                            #variant_patterns => #arg_names.push(item),
                        )*
                    }
                    Ok((#(#arg_names),*))
                }))
            }
        }
    }).into()
}
