use std::mem;
use std::collections::VecDeque;
use std::rc::Rc;
use std::cmp::Ordering;
use futures::{Future, Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;
use debug_everything::Debuggable;

use super::{Join, Rescan, OrderedMergeJoin, ExternalStorage};
use super::sort_merge::{SortMerger, CmpLeft, CmpRight};
use crate::predicate::{JoinPredicate, MergePredicate};

pub struct InputPhase<L, R, D, E> 
    where
        L: Stream,
        R: Stream,
        D: MergePredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    definition: D,
    storage: E,
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    left_runs: Vec<<E as ExternalStorage<L::Item>>::External>,
    right_runs: Vec<<E as ExternalStorage<R::Item>>::External>,
    left_buf: Vec<L::Item>,
    right_buf: Vec<R::Item>,
    memory_limit: usize,
    output_buffer: VecDeque<D::Output>,
}

#[derive(NamedType)]
pub enum ProgressiveMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream,
        D: MergePredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    InputPhase(InputPhase<L, R, D, E>),
    OutputPhase {
        output_buffer: VecDeque<D::Output>,
        omj: OrderedMergeJoin<Merger<L, E, CmpLeft<Rc<D>>>, Merger<R, E, CmpRight<Rc<D>>>, IgnoreIndexPredicate<Rc<D>>>,
    },
    Tmp,
}
type Merger<S, E, C> = SortMerger<<S as Stream>::Item, <E as ExternalStorage<<S as Stream>::Item>>::External, C>;

impl<L, R, D, E> InputPhase<L, R, D, E>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          E: ExternalStorage<L::Item> + ExternalStorage<R::Item>,
          D: MergePredicate<Left=L::Item, Right=R::Item> {
    fn flush_buffers(&mut self) {
        let definition = &self.definition;
        
        // sort
        self.left_buf.sort_by(|a, b| definition.cmp_left(a, b));
        self.right_buf.sort_by(|a, b|definition.cmp_right(a, b));
        
        // join
        match OrderedMergeJoin::new(stream::iter_ok::<_, ()>(&self.left_buf), stream::iter_ok::<_, ()>(&self.right_buf), definition).collect().poll().unwrap() {
            Async::NotReady => unreachable!(),
            Async::Ready(v) => self.output_buffer.extend(v.into_iter()),
        }
        
        // flush
        self.left_runs.push(self.storage.store(mem::replace(&mut self.left_buf, Vec::new())));
        self.right_runs.push(self.storage.store(mem::replace(&mut self.right_buf, Vec::new())));
    }
}


pub struct IgnoreIndexPredicate<P>(pub P);
impl<P: JoinPredicate> JoinPredicate for IgnoreIndexPredicate<P> {
    type Left = (usize, P::Left);
    type Right = (usize, P::Right);
    type Output = P::Output;
    
    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<P::Output> {
        // Progressive merge join: ignore previously joined tuples in merging phase
        if left.0 == right.0 {
            return None;
        }
    
        self.0.eq(&left.1, &right.1)
    }
}
impl<P: MergePredicate> MergePredicate for IgnoreIndexPredicate<P> {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        self.0.cmp(&left.1, &right.1)
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        self.0.cmp_left(&a.1, &b.1)
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        self.0.cmp_right(&a.1, &b.1)
    }
}

impl<L, R, D, E> Stream for ProgressiveMergeJoin<L, R, D, E>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          E: ExternalStorage<L::Item> + ExternalStorage<R::Item>,
          D: MergePredicate<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self {
                ProgressiveMergeJoin::InputPhase(i) => {
                    if let Some(buffered) = i.output_buffer.pop_front() {
                        println!("yielding {:?} from buffer", buffered.debug());
                        // pending buffered tuples
                        return Ok(Async::Ready(Some(buffered)));
                    }
                    
                    match (i.left.poll()?, i.right.poll()?) {
                        (Async::Ready(None), Async::Ready(None)) => {
                            // cleanup phase
                            // fall through to replace self
                        }
                        (Async::NotReady, Async::NotReady)
                            | (Async::Ready(None), Async::NotReady)
                            | (Async::NotReady, Async::Ready(None)) => {
                            return Ok(Async::NotReady);
                        }
                        (l, r) => {
                            if let Async::Ready(Some(l)) = l {
                                i.left_buf.push(l);
                            }
                            if let Async::Ready(Some(r)) = r {
                                i.right_buf.push(r);
                            }
                            if i.left_buf.len() + i.right_buf.len() >= i.memory_limit {
                                i.flush_buffers();
                            }
                            continue;
                        }
                    }
                }
                ProgressiveMergeJoin::OutputPhase { output_buffer, omj } => {
                    if let Some(buffered) = output_buffer.pop_front() {
                        // pending buffered tuples
                        println!("yielding {:?} from buffer 2", buffered.debug());
                        return Ok(Async::Ready(Some(buffered)));
                    }
                    
                    match omj.poll().unwrap() {
                        Async::Ready(None) => return Ok(Async::Ready(None)),
                        Async::Ready(Some(item)) => {
                            println!("yielding {:?} from MERGE", item.debug());
                            return Ok(Async::Ready(Some(item)));
                        }
                        Async::NotReady => unreachable!(),
                    }
                }
                ProgressiveMergeJoin::Tmp => unreachable!(),
            }
            
            *self = match mem::replace(self, ProgressiveMergeJoin::Tmp) {
                ProgressiveMergeJoin::InputPhase(mut i) => {
                    i.flush_buffers();
                    let InputPhase { left_buf, right_buf, definition, left_runs, right_runs, output_buffer, .. } = i;
                    assert!(left_buf.is_empty());
                    assert!(right_buf.is_empty());

                    let definition = Rc::new(definition);

                    let left = SortMerger::new(left_runs, CmpLeft(definition.clone()));
                    let right = SortMerger::new(right_runs, CmpRight(definition.clone()));

                    println!("merge phase!");
                    ProgressiveMergeJoin::OutputPhase {
                        output_buffer,
                        omj: OrderedMergeJoin::new(left, right, IgnoreIndexPredicate(definition)),
                    }
                }
                _ => unreachable!(),
            };
        }
    }
}
impl<L, R, D, E> Join<L, R, D, E, usize> for ProgressiveMergeJoin<L, R, D, E>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          E: ExternalStorage<L::Item> + ExternalStorage<R::Item>,
          D: MergePredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, storage: E, main_memory: usize) -> Self {
        ProgressiveMergeJoin::InputPhase(InputPhase {
            definition,
            storage,
            left: left.fuse(),
            right: right.fuse(),
            left_runs: Vec::new(),
            right_runs: Vec::new(),
            left_buf: Vec::new(),
            right_buf: Vec::new(),
            memory_limit: main_memory,
            output_buffer: VecDeque::new(),
        })
    }
}

