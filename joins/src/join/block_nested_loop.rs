use std::collections::VecDeque;
use futures::{Stream, Poll, try_ready, Async, stream};
use named_type::NamedType;
use named_type_derive::*;

use super::{Join, Rescan};
use crate::predicate::JoinPredicate;

#[derive(NamedType)]
pub struct BlockNestedLoopJoin<L: Stream, R: Stream, D: JoinPredicate> {
    left: stream::Fuse<L>,
    right: R,
    definition: D,
    buffer: Vec<L::Item>,
    output_buffer: VecDeque<D::Output>,
}

impl<L, R, D> Stream for BlockNestedLoopJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: JoinPredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            if let Some(out) = self.output_buffer.pop_front() {
                return Ok(Async::Ready(Some(out)));
            } else if (self.buffer.len() < self.buffer.capacity()) && !self.left.is_done() {
                if let Some(left) =  try_ready!(self.left.poll()) {
                    self.buffer.push(left);
                }
            } else if let Some(right) = try_ready!(self.right.poll()) {
                let definition = &self.definition;
                self.output_buffer.extend(self.buffer.iter().filter_map(|l| definition.eq(l, &right)));
            } else if self.left.is_done() {
                return Ok(Async::Ready(None));
            } else {
                self.buffer.clear();
                self.right.rescan();
            }
        }
    }
}
/*
TODO: need to reset Fuse somehow
impl<L, R, D> Rescan for BlockNestedLoopJoin<L, R, D>
    where L: Stream + Rescan,
          R: Stream<Error=L::Error> + Rescan,
          D: JoinPredicate<Left=L::Item, Right=R::Item> {
    fn rescan(&mut self) {
        self.left.rescan();
        self.right.rescan();
        self.buffer.clear();
        self.output_buffer.clear();
    }
}
*/


impl<L, R, D, E> Join<L, R, D, E, usize> for BlockNestedLoopJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: JoinPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, _: E, memory_size: usize) -> Self {
        BlockNestedLoopJoin { left: left.fuse(), right, definition, buffer: Vec::with_capacity(memory_size), output_buffer: VecDeque::new() }
    }
}

