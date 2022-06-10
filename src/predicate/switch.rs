use std::cmp::Ordering;

use super::{JoinPredicate, MergePredicate, HashPredicate};

pub struct SwitchPredicate<P>(pub P);

impl<P: JoinPredicate> JoinPredicate for SwitchPredicate<P> {
    type Left = P::Right;
    type Right = P::Left;
    type Output = P::Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        self.0.eq(right, left)
    }
}

impl<P: MergePredicate> MergePredicate for SwitchPredicate<P> {
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
impl<P: HashPredicate> HashPredicate for SwitchPredicate<P> {
    fn hash_left(&self, x: &Self::Left) -> u64 { self.0.hash_right(x) }
    fn hash_right(&self, x: &Self::Right) -> u64 { self.0.hash_left(x) }
}
