#![feature(type_alias_impl_trait)]
#![deny(unsafe_code)]

extern crate self as joins;

pub mod predicate;
pub mod join;
pub mod group_by;
mod value_skimmer;
mod in_memory;

pub use join::*;
pub use predicate::*;
pub use in_memory::*;
#[cfg(feature = "derive")]
pub use joins_derive::GroupByItem;

/// reexports for joins-derive
#[doc(hidden)]
pub mod __private {
    pub use ::futures::{Stream, Future};
}
