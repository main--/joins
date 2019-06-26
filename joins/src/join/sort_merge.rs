use std::rc::Rc;
use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;

use super::{Join, OrderedMergeJoin, External, ExternalStorage};
use crate::predicate::{JoinPredicate, MergePredicate, SwitchPredicate};

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
    OutputPhase(OrderedMergeJoin<Merger<Rc<D>, E>, Merger<SwitchPredicate<Rc<D>>, E>, Rc<D>>),
    Tmp,
}
type Merger<D, E> = SortMergerNoIndex<<D as JoinPredicate>::Left, SortMerger<D, <E as ExternalStorage<<D as JoinPredicate>::Left>>::External>>;

fn without_index<T, S: Stream<Item=(usize, T), Error=()>>(s: S) -> SortMergerNoIndex<T, S> {
    s.map(|(_, x)| x)
}
existential type SortMergerNoIndex<T, S>: Stream<Item=T, Error=()>;


use std::cmp::Ordering;
struct SortMergerItem<D: MergePredicate, E: External<D::Left>> {
    id: usize,
    iter: E::Iter,
    item: <E::Iter as Iterator>::Item,
    predicate: Rc<D>,
}
impl<D: MergePredicate, E: External<D::Left>> SortMergerItem<D, E> {
    fn new(id: usize, e: &E, predicate: &Rc<D>) -> Option<Self> {
        let mut iter = e.fetch();
        if let Some(item) = iter.next() {
            Some(SortMergerItem {
                id,
                iter,
                item,
                predicate: Rc::clone(&predicate),
            })
        } else {
            None
        }
    }
    fn next(mut self) -> ((usize, <E::Iter as Iterator>::Item), Option<Self>) {
        let ret = self.item;
        let me = if let Some(next) = self.iter.next() {
            Some(SortMergerItem {
                id: self.id,
                item: next,
                iter: self.iter,
                predicate: self.predicate,
            })
        } else {
            None
        };
        ((self.id, ret), me)
    }
}
impl<D: MergePredicate, E: External<D::Left>> PartialEq for SortMergerItem<D, E> {
    fn eq(&self, rhs: &Self) -> bool {
        self.cmp(rhs) == Ordering::Equal
    }
}
impl<D: MergePredicate, E: External<D::Left>> Eq for SortMergerItem<D, E> {}
impl<D: MergePredicate, E: External<D::Left>> PartialOrd for SortMergerItem<D, E> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}
impl<D: MergePredicate, E: External<D::Left>> Ord for SortMergerItem<D, E> {
    fn cmp(&self, rhs: &Self) -> Ordering {
        self.predicate.cmp_left(&self.item, &rhs.item).reverse() // reverse order to get a min-heap
    }
}
pub struct SortMerger<D: MergePredicate, E: External<D::Left>> {
    ways: std::collections::BinaryHeap<SortMergerItem<D, E>>,
}
impl<D: MergePredicate, E: External<D::Left>> SortMerger<D, E> {
    pub fn new(e: Vec<E>, predicate: D) -> Self {
        let rc = Rc::new(predicate);
        let ways = e.into_iter().enumerate().filter_map(|(i, x)| SortMergerItem::new(i, &x, &rc)).collect();
        SortMerger { ways }
    }
}
impl<D: MergePredicate, E: External<D::Left>> Stream for SortMerger<D, E> {
    type Item = (usize, D::Left);
    type Error = ();

    fn poll(&mut self) -> Poll<Option<(usize, D::Left)>, ()> {
        Ok(Async::Ready(if let Some(x) = self.ways.pop() {
            let (item, way) = x.next();
            if let Some(way) = way {
                self.ways.push(way);
            }
            Some(item)
        } else {
            None
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
        //println!("flush");
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

                    let left = without_index(SortMerger::new(left_blocks, definition.clone()));
                    let right = without_index(SortMerger::new(right_blocks, definition.clone().switch()));

                    SortMergeJoin::OutputPhase(OrderedMergeJoin::new(left, right, definition))
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<L, R, D, E> Join<L, R, D, E, usize> for SortMergeJoin<L, R, D, E>
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
