use std::mem;
use std::rc::Rc;
use std::collections::VecDeque;
use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;
use itertools::{Itertools, MinMaxResult};
use multimap::MultiMap;
use debug_everything::Debuggable;

use super::{Join, ExternalStorage, External};
use crate::predicate::{JoinPredicate, HashPredicate};

#[derive(NamedType)]
pub enum HashMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream,
        D: JoinPredicate,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    definition: D,
    storage: E,
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    partitions_left: Vec<Partition<L::Item, E>>,
    partitions_right: Vec<Partition<R::Item, E>>,
    stage2_cursor: usize,
    overflow_memory: usize,
    memory_limit: usize,
    timer: u64,
    output_buffer: VecDeque<D::Output>,
}

impl<L, R, D, E> Stream for HashMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        
    }
}
impl<L, R, D, E> Join<L, R, D, E> for HashMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    fn build(left: L, right: R, definition: D, storage: E, memory_limit: usize) -> Self {
        HashMergeJoin::MainPhase(MainPhase {
            definition,
            storage,
            left: left.fuse(),
            right: right.fuse(),
            memory_limit,
        })
    }
}

