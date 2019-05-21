use std::collections::VecDeque;
use futures::{Stream, Poll, Async, stream};
use multimap::MultiMap;
use named_type::NamedType;
use named_type_derive::*;

use super::Join;
use crate::definition::{JoinDefinition, HashJoinDefinition};

#[derive(NamedType)]
pub struct SymmetricHashJoin<L: Stream, R: Stream, D: JoinDefinition> {
    definition: D,
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    table_left: MultiMap<u64, L::Item>,
    table_right: MultiMap<u64, R::Item>,
    output_buffer: VecDeque<D::Output>,
}
impl<L, R, D> Stream for SymmetricHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: HashJoinDefinition<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

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
                    if let Async::Ready(Some(l)) = l {
                        let hash = self.definition.hash_left(&l);
                        for candidate in self.table_right.get_vec(&hash).into_iter().flatten() {
                            if let Some(x) = self.definition.eq(&l, candidate) {
                                self.output_buffer.push_back(x);
                            }
                        }
                        self.table_left.insert(hash, l);
                    }
                    if let Async::Ready(Some(r)) = r {
                        let hash = self.definition.hash_right(&r);
                        for candidate in self.table_left.get_vec(&hash).into_iter().flatten() {
                            if let Some(x) = self.definition.eq(candidate, &r) {
                                self.output_buffer.push_back(x);
                            }
                        }
                        self.table_right.insert(hash, r);
                    }
                }
            }
        }
    }
}
impl<L, R, D> Join<L, R, D> for SymmetricHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: HashJoinDefinition<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        SymmetricHashJoin { definition, left: left.fuse(), right: right.fuse(), table_left: MultiMap::new(), table_right: MultiMap::new(), output_buffer: VecDeque::new() }
    }
}

