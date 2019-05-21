use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;

use super::{Join, OrderedMergeJoin};
use crate::predicate::MergePredicate;

#[derive(NamedType)]
pub enum SortMergeJoin<L: Stream, R: Stream, D> {
    InputPhase {
        definition: D,
        left: stream::Fuse<L>,
        right: stream::Fuse<R>,

        left_buf: Vec<L::Item>,
        right_buf: Vec<R::Item>,
    },
    OutputPhase(OrderedMergeJoin<stream::IterOk<std::vec::IntoIter<L::Item>, L::Error>, stream::IterOk<std::vec::IntoIter<R::Item>, R::Error>, D>),
    Tmp,
}
impl<L, R, D> Stream for SortMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: MergePredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self {
                SortMergeJoin::InputPhase { left, right, left_buf, right_buf, .. } => {
                    let l = left.poll()?;
                    let r = right.poll()?;

                    match (l, r) {
                        (Async::Ready(None), Async::Ready(None)) => {
                            // fall out of the match in order to replace self
                        }
                        (Async::NotReady, Async::NotReady) | (Async::Ready(None), Async::NotReady) | (Async::NotReady, Async::Ready(None)) => return Ok(Async::NotReady),
                        (l, r) => {
                            if let Async::Ready(Some(l)) = l { left_buf.push(l); }
                            if let Async::Ready(Some(r)) = r { right_buf.push(r); }

                            continue;
                        }
                    }
                }
                SortMergeJoin::OutputPhase(omj) => return omj.poll(),
                SortMergeJoin::Tmp => unreachable!(),
            }

            *self = match std::mem::replace(self, SortMergeJoin::Tmp) {
                SortMergeJoin::InputPhase { mut left_buf, mut right_buf, definition, .. } => {
                    left_buf.sort_by(|a, b| definition.cmp_left(a, b));
                    right_buf.sort_by(|a, b| definition.cmp_right(a, b));
                    SortMergeJoin::OutputPhase(OrderedMergeJoin::build(stream::iter_ok(left_buf), stream::iter_ok(right_buf), definition))
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<L, R, D> Join<L, R, D> for SortMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: MergePredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        SortMergeJoin::InputPhase { left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new(), definition }
    }
}
