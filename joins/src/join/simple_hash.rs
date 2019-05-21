use std::collections::VecDeque;
use futures::{Stream, Poll, Async, try_ready, stream};
use multimap::MultiMap;
use named_type::NamedType;
use named_type_derive::*;

use super::Join;
use crate::predicate::HashPredicate;

#[derive(NamedType)]
pub struct SimpleHashJoin<L: Stream, R: Stream, D: HashPredicate> {
    definition: D,
    left: stream::Fuse<L>,
    right: R,
    table: MultiMap<u64, L::Item>,
    output_buffer: VecDeque<D::Output>,
}
impl<L, R, D> Stream for SimpleHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: HashPredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // build phase
        while let Some(left) = try_ready!(self.left.poll()) {
            self.table.insert(self.definition.hash_left(&left), left);
        }

        // probe phase


        // carry-over buffer
        while let Some(buffered) = self.output_buffer.pop_front() {
            return Ok(Async::Ready(Some(buffered)));
        }
        // actual probing (spills excess candidates to buffer)
        while let Some(right) = try_ready!(self.right.poll()) {
            for candidate in self.table.get_vec(&self.definition.hash_right(&right)).into_iter().flatten() {
                if let Some(x) = self.definition.eq(candidate, &right) {
                    self.output_buffer.push_back(x);
                }
            }
            if let Some(buffered) = self.output_buffer.pop_front() {
                return Ok(Async::Ready(Some(buffered)));
            }
        }

        // done
        Ok(Async::Ready(None))
    }
}
impl<L, R, D, E> Join<L, R, D, E> for SimpleHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: HashPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, _: E) -> Self {
        SimpleHashJoin { definition, left: left.fuse(), right, table: MultiMap::new(), output_buffer: VecDeque::new() }
    }
}

