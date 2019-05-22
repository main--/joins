use std::collections::VecDeque;
use futures::{Stream, Poll, Async, try_ready, stream};
use multimap::MultiMap;
use named_type::NamedType;
use named_type_derive::*;

use super::{Join, Rescan};
use crate::predicate::HashPredicate;

#[derive(NamedType)]
pub struct SimpleHashJoin<L: Stream, R: Stream, D: HashPredicate> {
    definition: D,
    left: stream::Fuse<L>,
    right: R,
    table: MultiMap<u64, L::Item>,
    table_entries: usize,
    memory_limit: usize,
    output_buffer: VecDeque<D::Output>,
}
impl<L, R, D> Stream for SimpleHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: HashPredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            if let Some(buffered) = self.output_buffer.pop_front() {
                // pending buffered tuples
                return Ok(Async::Ready(Some(buffered)));
            } else if (self.table_entries < self.memory_limit) && !self.left.is_done() {
                // build phase
                if let Some(left) =  try_ready!(self.left.poll()) {
                    self.table.insert(self.definition.hash_left(&left), left);
                    self.table_entries += 1;
                }
            } else if let Some(right) = try_ready!(self.right.poll()) {
                // probe phase
                for candidate in self.table.get_vec(&self.definition.hash_right(&right)).into_iter().flatten() {
                    if let Some(x) = self.definition.eq(candidate, &right) {
                        self.output_buffer.push_back(x);
                    }
                }
            } else if self.left.is_done() {
                // all complete
                return Ok(Async::Ready(None));
            } else {
                // probe phase complete, return to build phase
                self.right.rescan();
                self.table.clear();
                self.table_entries = 0;
            }
        }
    }
}
impl<L, R, D, E> Join<L, R, D, E> for SimpleHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: HashPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, _: E, main_memory: usize) -> Self {
        SimpleHashJoin {
            definition,
            left: left.fuse(),
            right,
            table: MultiMap::new(),
            table_entries: 0,
            memory_limit: main_memory,
            output_buffer: VecDeque::new(),
        }
    }
}

