use std::cmp::Ordering;
use crate::{InnerJoinPredicate, OuterJoinPredicate};

use super::{JoinPredicate, MergePredicate, HashPredicate};

pub struct SwapPredicate<P>(P);

impl<P: JoinPredicate> SwapPredicate<P> {
    pub fn new(predicate: P) -> Self {
        SwapPredicate(predicate)
    }
}

impl<P: JoinPredicate> JoinPredicate for SwapPredicate<P> {
    type Left = P::Right;
    type Right = P::Left;
}
impl<P: InnerJoinPredicate> InnerJoinPredicate for SwapPredicate<P> {
    type Output = P::Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        self.0.eq(right, left)
    }
}
impl<P: OuterJoinPredicate> OuterJoinPredicate for SwapPredicate<P> {
    fn eq(&self, left: &Self::Left, right: &Self::Right) -> bool {
        self.0.eq(right, left)
    }
}
impl<P: MergePredicate> MergePredicate for SwapPredicate<P> {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        self.0.cmp(right, left)
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        self.0.cmp_right(a, b)
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        self.0.cmp_left(a, b)
    }
}
impl<P: HashPredicate> HashPredicate for SwapPredicate<P> {
    fn hash_left(&self, x: &Self::Left) -> u64 { self.0.hash_right(x) }
    fn hash_right(&self, x: &Self::Right) -> u64 { self.0.hash_left(x) }
}
