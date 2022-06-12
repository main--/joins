use std::collections::VecDeque;
use futures::{Stream, Poll, Async, stream};
use multimap::MultiMap;
use named_type::NamedType;
use named_type_derive::*;

use super::Join;
use crate::predicate::{HashPredicate, InnerJoinPredicate};

#[derive(Debug)]
pub enum Error<E> {
    Underlying(E),
    OutOfMemory,
}
impl<E> From<E> for Error<E> {
    fn from(e: E) -> Error<E> {
        Error::Underlying(e)
    }
}

#[derive(NamedType)]
pub struct SymmetricHashJoin<L: Stream, R: Stream, D: InnerJoinPredicate + HashPredicate> {
    definition: D,
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    table_left: MultiMap<u64, L::Item>,
    table_right: MultiMap<u64, R::Item>,
    tuple_count: usize,
    memory_limit: usize,
    output_buffer: VecDeque<D::Output>,
}
impl<L, R, D> Stream for SymmetricHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: InnerJoinPredicate + HashPredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = Error<L::Error>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            // carry-over buffer
            if let Some(buffered) = self.output_buffer.pop_front() {
                return Ok(Async::Ready(Some(buffered)));
            }

            let left = self.left.poll()?;
            let right = self.right.poll()?;

            match (left, right) {
                (Async::Ready(None), Async::Ready(None)) => return Ok(Async::Ready(None)),
                (Async::NotReady, Async::NotReady) | (Async::Ready(None), Async::NotReady) | (Async::NotReady, Async::Ready(None)) => return Ok(Async::NotReady),
                (l, r) => {
                    let definition = &self.definition;
                    if let Async::Ready(Some(l)) = l {
                        let hash = definition.hash_left(&l);
                        self.output_buffer.extend(
                            self.table_right.get_vec(&hash).into_iter().flatten()
                                .filter_map(|r| definition.eq(&l, r)));
                        self.table_left.insert(hash, l);
                        self.tuple_count += 1;
                    }
                    if let Async::Ready(Some(r)) = r {
                        let hash = definition.hash_right(&r);
                        self.output_buffer.extend(
                            self.table_left.get_vec(&hash).into_iter().flatten()
                                .filter_map(|l| definition.eq(l, &r)));
                        self.table_right.insert(hash, r);
                        self.tuple_count += 1;
                    }
                    if self.tuple_count > self.memory_limit {
                        return Err(Error::OutOfMemory);
                    }
                }
            }
        }
    }
}
impl<L, R, D, E> Join<L, R, D, E, usize> for SymmetricHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: InnerJoinPredicate + HashPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, _: E, main_memory: usize) -> Self {
        SymmetricHashJoin {
            definition,
            left: left.fuse(),
            right: right.fuse(),
            table_left: MultiMap::new(),
            table_right: MultiMap::new(),
            output_buffer: VecDeque::new(),
            memory_limit: main_memory,
            tuple_count: 0,
        }
    }
}

