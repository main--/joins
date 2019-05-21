use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;

use super::{Join, OrderedMergeJoin};
use crate::predicate::MergePredicate;
use crate::{External, ExternalStorage};

#[derive(NamedType)]
pub enum SortMergeJoin<L: Stream, R: Stream, D, E>
    where
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    InputPhase {
        definition: D,
        storage: E,
        left: stream::Fuse<L>,
        right: stream::Fuse<R>,

        left_buf: Vec<L::Item>,
        right_buf: Vec<R::Item>,
    },
    OutputPhase(OrderedMergeJoin<stream::IterOk<<<E as ExternalStorage<L::Item>>::External as External<L::Item>>::Iter, L::Error>, stream::IterOk<<<E as ExternalStorage<R::Item>>::External as External<R::Item>>::Iter, R::Error>, D>),
    Tmp,
}
impl<L, R, D, E> Stream for SortMergeJoin<L, R, D, E>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: MergePredicate<Left=L::Item, Right=R::Item>,
          E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
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
                SortMergeJoin::InputPhase { mut left_buf, mut right_buf, definition, mut storage, .. } => {
                    left_buf.sort_by(|a, b| definition.cmp_left(a, b));
                    right_buf.sort_by(|a, b| definition.cmp_right(a, b));
                    SortMergeJoin::OutputPhase(OrderedMergeJoin::build(stream::iter_ok(storage.store(left_buf).fetch()), stream::iter_ok(storage.store(right_buf).fetch()), definition, ()))
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<L, R, D, E> Join<L, R, D, E> for SortMergeJoin<L, R, D, E>
    where L: Stream,
          R: Stream<Error=L::Error>,
          D: MergePredicate<Left=L::Item, Right=R::Item>,
          E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    fn build(left: L, right: R, definition: D, storage: E) -> Self {
        SortMergeJoin::InputPhase { left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new(), definition, storage }
    }
}
