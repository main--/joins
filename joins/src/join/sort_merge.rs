use std::iter::Peekable;
use std::rc::Rc;
use std::cmp::Ordering;
use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;
use itertools::{Itertools, MinMaxResult};

use super::{Join, OrderedMergeJoin};
use crate::predicate::MergePredicate;
use crate::{External, ExternalStorage};

#[derive(NamedType)]
pub enum SortMergeJoin<L: Stream, R: Stream, D: MergePredicate<Left=L::Item, Right=R::Item>, E>
    where
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    InputPhase {
        definition: D,
        storage: E,
        left: stream::Fuse<L>,
        right: stream::Fuse<R>,

        left_buf: Vec<L::Item>,
        right_buf: Vec<R::Item>,
        buf_limit: usize,
        left_blocks: Vec<<E as ExternalStorage<L::Item>>::External>,
        right_blocks: Vec<<E as ExternalStorage<R::Item>>::External>,
    },
    OutputPhase(OrderedMergeJoin<SortMerger<L::Item, <E as ExternalStorage<L::Item>>::External, CmpLeft<Rc<D>>>, SortMerger<R::Item, <E as ExternalStorage<R::Item>>::External, CmpRight<Rc<D>>>, Rc<D>>),
    Tmp,
}

pub trait Compare<T> {
    fn cmp(&self, a: &T, b: &T) -> Ordering;
}

pub struct CmpLeft<D>(D);
impl<D: MergePredicate> Compare<D::Left> for CmpLeft<D> {
    fn cmp(&self, a: &D::Left, b: &D::Left) -> Ordering {
        self.0.cmp_left(a, b)
    }
}
pub struct CmpRight<D>(D);
impl<D: MergePredicate> Compare<D::Right> for CmpRight<D> {
    fn cmp(&self, a: &D::Right, b: &D::Right) -> Ordering {
        self.0.cmp_right(a, b)
    }
}


pub struct SortMerger<T, E: External<T>, C> {
    ways: Vec<Peekable<E::Iter>>,
    sort: C,
}
impl<T, E: External<T>, C> SortMerger<T, E, C> {
    fn new(e: Vec<E>, c: C) -> Self {
        SortMerger {
            ways: e.into_iter().map(|x| x.fetch().peekable()).collect(),
            sort: c,
        }
    }
}
impl<T, E: External<T>, C: Compare<T>> Stream for SortMerger<T, E, C> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        let sort = &self.sort;
        Ok(Async::Ready(match self.ways.iter_mut().enumerate().filter_map(|(i, x)| x.peek().map(|x| (i, x))).minmax_by(|(_, a), (_, b)| sort.cmp(a, b)) {
            MinMaxResult::NoElements => None,
            MinMaxResult::OneElement((i, _)) | MinMaxResult::MinMax((i, _), _) => {
                Some(self.ways[i].next().unwrap())
            }
        }))
    }
}

fn manage_buf<T, E: ExternalStorage<T>, F: Fn(&T, &T) -> std::cmp::Ordering>(
    value: Async<Option<T>>,
    buffer: &mut Vec<T>,
    size_limit: usize,
    storage: &mut E,
    blocks: &mut Vec<E::External>,
    sort: F) {
    if let Async::Ready(Some(v)) = value {
        buffer.push(v);
    }
    if buffer.len() >= size_limit {
        buffer.sort_by(sort);
        println!("flush");
        blocks.push(storage.store(std::mem::replace(buffer, Vec::new())));
    }
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
                SortMergeJoin::InputPhase { left, right, left_buf, right_buf, buf_limit, storage, left_blocks, right_blocks, definition, .. } => {
                    let l = left.poll()?;
                    let r = right.poll()?;

                    match (l, r) {
                        (Async::Ready(None), Async::Ready(None)) => {
                            // fall out of the match in order to replace self
                        }
                        (Async::NotReady, Async::NotReady) | (Async::Ready(None), Async::NotReady) | (Async::NotReady, Async::Ready(None)) => return Ok(Async::NotReady),
                        (l, r) => {
                            manage_buf(l, left_buf, *buf_limit, storage, left_blocks, |a, b| definition.cmp_left(a, b));
                            manage_buf(r, right_buf, *buf_limit, storage, right_blocks, |a, b| definition.cmp_right(a, b));

                            continue;
                        }
                    }
                }
                SortMergeJoin::OutputPhase(omj) => return omj.poll().map_err(|_| unreachable!()),
                SortMergeJoin::Tmp => unreachable!(),
            }

            *self = match std::mem::replace(self, SortMergeJoin::Tmp) {
                SortMergeJoin::InputPhase { mut left_buf, mut right_buf, definition, mut storage, mut left_blocks, mut right_blocks, .. } => {
                    manage_buf(Async::NotReady, &mut left_buf, 0, &mut storage, &mut left_blocks, |a, b| definition.cmp_left(a, b));
                    manage_buf(Async::NotReady, &mut right_buf, 0, &mut storage, &mut right_blocks, |a, b| definition.cmp_right(a, b));
                    assert!(left_buf.is_empty());
                    assert!(right_buf.is_empty());

                    let definition = Rc::new(definition);

                    let left = SortMerger::new(left_blocks, CmpLeft(definition.clone()));
                    let right = SortMerger::new(right_blocks, CmpRight(definition.clone()));

                    SortMergeJoin::OutputPhase(OrderedMergeJoin::new(left, right, definition))
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
    fn build(left: L, right: R, definition: D, storage: E, main_memory: usize) -> Self {
        SortMergeJoin::InputPhase {
            left: left.fuse(),
            right: right.fuse(),
            left_buf: Vec::new(),
            right_buf: Vec::new(),
            left_blocks: Vec::new(),
            right_blocks: Vec::new(),
            buf_limit: main_memory / 2,

            definition,
            storage
        }
    }
}
