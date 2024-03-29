use std::cmp::Ordering;
use std::borrow::Borrow;
use std::marker::PhantomData;
use crate::{InnerJoinPredicate, OuterJoinPredicate};

use super::{JoinPredicate, MergePredicate, HashPredicate};

#[derive(Clone)]
pub struct MapLeftPredicate<P, F, T, O> {
    predicate: P,
    mapping: F,
    _phantom: PhantomData<fn(&T) -> O>,
}

impl<P, F, T, O> MapLeftPredicate<P, F, T, O>
    where
        P: JoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Left>,
{
    pub fn new(predicate: P, mapping: F) -> Self {
        MapLeftPredicate {
            predicate,
            mapping,
            _phantom: PhantomData,
        }
    }
}

impl<P, F, T, O> JoinPredicate for MapLeftPredicate<P, F, T, O>
    where
        P: JoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Left>,
{
    type Left = T;
    type Right = P::Right;
}
impl<P, F, T, O> InnerJoinPredicate for MapLeftPredicate<P, F, T, O>
where
    P: InnerJoinPredicate,
    F: Fn(&T) -> O,
    O: Borrow<P::Left>,
{
    type Output = P::Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        self.predicate.eq((self.mapping)(left).borrow(), right)
    }
}
impl<P, F, T, O> OuterJoinPredicate for MapLeftPredicate<P, F, T, O>
    where
        P: OuterJoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Left>,
{
    fn eq(&self, left: &Self::Left, right: &Self::Right) -> bool {
        self.predicate.eq((self.mapping)(left).borrow(), right)
    }
}
impl<P, F, T, O> MergePredicate for MapLeftPredicate<P, F, T, O>
    where
        P: MergePredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Left>,
{
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        self.predicate.cmp((self.mapping)(left).borrow(), right)
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        self.predicate.cmp_left((self.mapping)(a).borrow(), (self.mapping)(b).borrow())
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        self.predicate.cmp_right(a, b)
    }
}
impl<P, F, T, O> HashPredicate for MapLeftPredicate<P, F, T, O>
    where
        P: HashPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Left>,
{
    fn hash_left(&self, x: &Self::Left) -> u64 { self.predicate.hash_left((self.mapping)(x).borrow()) }
    fn hash_right(&self, x: &Self::Right) -> u64 { self.predicate.hash_right(x) }
}

#[derive(Clone)]
pub struct MapRightPredicate<P, F, T, O> {
    predicate: P,
    mapping: F,
    _phantom: PhantomData<fn(&T) -> O>,
}

impl<P, F, T, O> MapRightPredicate<P, F, T, O>
    where
        P: JoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Right>,
{
    pub fn new(predicate: P, mapping: F) -> Self {
        MapRightPredicate {
            predicate,
            mapping,
            _phantom: PhantomData,
        }
    }
}

impl<P, F, T, O> JoinPredicate for MapRightPredicate<P, F, T, O>
    where
        P: JoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Right>,
{
    type Left = P::Left;
    type Right = T;
}
impl<P, F, T, O> InnerJoinPredicate for MapRightPredicate<P, F, T, O>
    where
        P: InnerJoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Right>,
{
    type Output = P::Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        self.predicate.eq(left, (self.mapping)(right).borrow())
    }
}
impl<P, F, T, O> OuterJoinPredicate for MapRightPredicate<P, F, T, O>
    where
        P: OuterJoinPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Right>,
{
    fn eq(&self, left: &Self::Left, right: &Self::Right) -> bool {
        self.predicate.eq(left, (self.mapping)(right).borrow())
    }
}
impl<P, F, T, O> MergePredicate for MapRightPredicate<P, F, T, O>
    where
        P: MergePredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Right>,
{
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        self.predicate.cmp(left, (self.mapping)(right).borrow())
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        self.predicate.cmp_left(a, b)
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        self.predicate.cmp_right((self.mapping)(a).borrow(), (self.mapping)(b).borrow())
    }
}
impl<P, F, T, O> HashPredicate for MapRightPredicate<P, F, T, O>
    where
        P: HashPredicate,
        F: Fn(&T) -> O,
        O: Borrow<P::Right>,
{
    fn hash_left(&self, x: &Self::Left) -> u64 { self.predicate.hash_left(x) }
    fn hash_right(&self, x: &Self::Right) -> u64 { self.predicate.hash_right((self.mapping)(x).borrow()) }
}

#[derive(Clone)]
pub struct MapOutputPredicate<P, F, T, O> {
    predicate: P,
    mapping: F,
    _phantom: PhantomData<fn(T) -> O>,
}

impl<P, F, T, O> MapOutputPredicate<P, F, T, O>
    where
        P: InnerJoinPredicate,
        F: Fn(T) -> O,
{
    pub fn new(predicate: P, mapping: F) -> Self {
        MapOutputPredicate {
            predicate,
            mapping,
            _phantom: PhantomData,
        }
    }
}

impl<P, F, T, O> JoinPredicate for MapOutputPredicate<P, F, T, O>
    where
        P: JoinPredicate,
        F: Fn(T) -> O,
{
    type Left = P::Left;
    type Right = P::Right;
}
impl<P, F, T, O> InnerJoinPredicate for MapOutputPredicate<P, F, T, O>
    where
        P: InnerJoinPredicate<Output = T>,
        F: Fn(T) -> O,
{
    type Output = O;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        self.predicate.eq(left, right).map(&self.mapping)
    }
}
impl<P, F, T, O> MergePredicate for MapOutputPredicate<P, F, T, O>
    where
        P: MergePredicate,
        F: Fn(T) -> O,
{
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> { self.predicate.cmp(left, right) }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering { self.predicate.cmp_left(a, b) }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering { self.predicate.cmp_right(a, b) }
}
impl<P, F, T, O> HashPredicate for MapOutputPredicate<P, F, T, O>
    where
        P: HashPredicate,
        F: Fn(T) -> O,
{
    fn hash_left(&self, x: &Self::Left) -> u64 { self.predicate.hash_left(x) }
    fn hash_right(&self, x: &Self::Right) -> u64 { self.predicate.hash_right(x) }
}
