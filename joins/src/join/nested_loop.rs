use futures::{Stream, Poll, try_ready, Async, stream};
use named_type::NamedType;
use named_type_derive::*;

use super::{Join, Rescan};
use crate::predicate::JoinPredicate;

#[derive(NamedType)]
pub struct NestedLoopJoin<L: Stream, R: Stream, D> {
    left: stream::Peekable<L>,
    right: R,
    definition: D,
}

impl<L, R, D> Stream for NestedLoopJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: JoinPredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match try_ready!(self.left.peek()) {
                None => return Ok(Async::Ready(None)),
                Some(left) => {
                    match try_ready!(self.right.poll()) {
                        None => {
                            match self.left.poll()? {
                                Async::Ready(Some(_)) => (),
                                _ => unreachable!(),
                            }
                            self.right.rescan();
                        }
                        Some(right) => {
                            if let Some(out) = self.definition.eq(left, &right) {
                                return Ok(Async::Ready(Some(out)));
                            }
                        }
                    }
                }
            }
        }
    }
}
// TODO: we /could/ support rescan ourselves iff our left side ALSO has rescan (need it for right side anyways)
// however, Peekable<S> does not offer a way to access the underlying stream.
// we would have to implement our own peek adapter probably - which is trivial but meh


impl<L, R, D> NestedLoopJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: JoinPredicate<Left=L::Item, Right=R::Item> {
    pub fn new(left: L, right: R, definition: D) -> Self {
        NestedLoopJoin { left: left.peekable(), right, definition }
    }
}

impl<L, R, D, E> Join<L, R, D, E> for NestedLoopJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: JoinPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, _: E, _: usize) -> Self {
        NestedLoopJoin::new(left, right, definition)
    }
}

