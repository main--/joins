//! Generic join predicates.
//!
//! A join predicate joins tuples. In general, a join implementation will invoke
//! a `JoinPredicate`'s `eq` function for all pairs of input tuples. The join predicate
//! can then produce output tuples whenever two input tuples match according to the join definition.
//!
//! Without any additional functionality, this means that a bare `JoinPredicate` can only be used
//! for a nested-loop style join that actually compares every single pair of tuples.
//! In other words, given two input relations of size `n` any join algorithm is forced to invoke
//! the join predicate at least `O(n²)` times.
//!
//! In order to improve this, a `JoinPredicate` may optionally implement the `MergePredicate`
//! and `HashPredicate` traits:
//!
//! * Implementing the `MergePredicate` trait allows the join implementation to rely on
//!   an **order** between tuples. The classic example would be the sort-merge join,
//!   which only invokes the join predicate `O(n log(n))` times.
//! * Implementing the `HashPredicate` trait allows the join implementation to restrict join
//!   partners using a hash value. In a theoretical best case, a hash join only needs to
//!   invoke the join predicate `O(n)` times.

use std::cmp::Ordering;

mod equijoin;
pub use equijoin::EquiJoin;

pub trait JoinPredicate {
    type Left;
    type Right;
    type Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output>;
}

pub trait MergePredicate: JoinPredicate {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering>;
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering;
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering;
}

pub trait HashPredicate: JoinPredicate {
    fn hash_left(&self, x: &Self::Left) -> u64;
    fn hash_right(&self, x: &Self::Right) -> u64;
}
