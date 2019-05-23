use std::{cmp, mem};
use std::rc::Rc;
use std::collections::VecDeque;
use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;
use itertools::{Itertools, MinMaxResult};
use multimap::MultiMap;
use debug_everything::Debuggable;

use super::{Join, ExternalStorage, External};
use crate::predicate::{JoinPredicate, HashPredicate};

#[derive(NamedType)]
pub enum XJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream,
        D: HashPredicate,
        E: ExternalStorage<Timestamped<L::Item>> + ExternalStorage<Timestamped<R::Item>> {
    MainPhase(MainPhase<L, R, D, E>),
    CleanupPhase(CleanupPhase<L, R, D, E>),
    Tmp,
}

pub struct MainPhase<L, R, D, E>
    where
        L: Stream,
        R: Stream,
        D: HashPredicate,
        E: ExternalStorage<Timestamped<L::Item>> + ExternalStorage<Timestamped<R::Item>> {
    definition: D,
    storage: E,
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    partitions_left: Vec<Partition<L::Item, E>>,
    partitions_right: Vec<Partition<R::Item, E>>,
    overflow_memory: usize,
    memory_limit: usize,
    timer: u64,
    output_buffer: VecDeque<D::Output>,
}

#[derive(Clone)]
pub struct Timestamped<T> {
    t_in: u64,
    t_out: u64,
    item: T,
}

struct Partition<T, E: ExternalStorage<Timestamped<T>>> {
    in_memory: Vec<(u64, T)>,
    on_disk: Vec<E::External>,
}
// why can't derive figure this out?  :(
impl<T, E: ExternalStorage<Timestamped<T>>> Default for Partition<T, E> {
    fn default() -> Self {
        Partition { in_memory: Vec::new(), on_disk: Vec::new() }
    }
}
impl<T, E: ExternalStorage<Timestamped<T>>> Partition<T, E> {
    fn evict(&mut self, storage: &mut E, t_out: u64, mem: &mut usize) {
        *mem -= self.in_memory.len() - 1;
        self.on_disk.push(storage.store(mem::replace(&mut self.in_memory, Vec::new()).into_iter().map(|(t_in, item)| Timestamped { t_in, t_out, item }).collect()));
    }
}

fn manage_side<T, U, O, E: ExternalStorage<Timestamped<T>> + ExternalStorage<Timestamped<U>>, F: FnOnce(&T) -> u64, G: Fn(&T, &U) -> Option<O>>(
        v: Async<Option<T>>,
        insert_partitions: &mut Vec<Partition<T, E>>,
        probe_partitions: &Vec<Partition<U, E>>,
        output_buffer: &mut VecDeque<O>,
        overflow_memory: &mut usize,
        timer: u64,
        hasher: F,
        joiner: G) {
    if let Async::Ready(Some(v)) = v {
        let hash = hasher(&v);
        let hash = (hash % (insert_partitions.len() as u64)) as usize;
        let partition = &mut insert_partitions[hash];
        if !partition.in_memory.is_empty() {
            *overflow_memory += 1;
        }
        output_buffer.extend(probe_partitions[hash].in_memory.iter().filter_map(|(_, c)| joiner(&v, c)));
        partition.in_memory.push((timer, v));
    }
}
impl<L, R, D, E> MainPhase<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<Timestamped<L::Item>> + ExternalStorage<Timestamped<R::Item>> {
    fn manage_eviction(&mut self) {
        self.timer += 1; // TODO: is this necessary?
        
        if self.overflow_memory >= self.memory_limit {
            // evict largest partition (no matter if left or right)
            let largest_left = match self.partitions_left.iter_mut().minmax_by_key(|p| p.in_memory.len()) {
                MinMaxResult::NoElements => unreachable!(),
                MinMaxResult::OneElement(x) | MinMaxResult::MinMax(_, x) => x,
            };
            let largest_right = match self.partitions_right.iter_mut().minmax_by_key(|p| p.in_memory.len()) {
                MinMaxResult::NoElements => unreachable!(),
                MinMaxResult::OneElement(x) | MinMaxResult::MinMax(_, x) => x,
            };
            if largest_left.in_memory.len() > largest_right.in_memory.len() {
                largest_left.evict(&mut self.storage, self.timer, &mut self.overflow_memory);
            } else {
                largest_right.evict(&mut self.storage, self.timer, &mut self.overflow_memory);
            }
        }
    }

    fn switch_to_cleanup(self) -> CleanupPhase<L, R, D, E> {
        let t_out = self.timer;
        let definition = Rc::new(self.definition);
        self.partitions_left.into_iter().zip(self.partitions_right).flat_map(move |(l, r)| {
            let left = l.in_memory.into_iter().map(move |(t_in, item)| Timestamped { t_in, t_out, item })
                .chain(l.on_disk.into_iter().flat_map(|x| x.fetch()));
            let right = r.in_memory.into_iter().map(move |(t_in, item)| Timestamped { t_in, t_out, item })
                .chain(r.on_disk.into_iter().flat_map(|x| x.fetch()));
                
            let table: MultiMap<_, Timestamped<L::Item>> = left.map(|x| (definition.hash_left(&x.item), x)).collect();
            let definition = Rc::clone(&definition);
            right.flat_map(move |r| {
                let hash = definition.hash_right(&r.item);
                let matches: Vec<_> = table.get_vec(&hash).into_iter().flat_map(|l| l.iter()).flat_map(|l| {
                    // did we join these already?
                    if (l.t_in <= r.t_out) && (l.t_out > r.t_in) {
                        println!("rejecting {:?} ({}, {}) {:?} ({}, {})", l.item.debug(), l.t_in, l.t_out, r.item.debug(), r.t_in, r.t_out);
                        return None;
                    }
                    
                    definition.eq(&l.item, &r.item)
                }).collect();
                matches.into_iter()
            })
        })
    }
}

existential type CleanupPhase<L, R, D: JoinPredicate, E>: Iterator<Item=D::Output>;

impl<L, R, D, E> Stream for XJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<Timestamped<L::Item>> + ExternalStorage<Timestamped<R::Item>> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self {
                XJoin::MainPhase(this) => {
                    this.timer += 1;

                    // carry-over buffer
                    if let Some(buffered) = this.output_buffer.pop_front() {
                        return Ok(Async::Ready(Some(buffered)));
                    }

                    match (this.left.poll()?, this.right.poll()?) {
                        (Async::Ready(None), Async::Ready(None)) => {
                            // cleanup phase
                            // fall through to switch to cleanup phase
                        }
                        (Async::NotReady, Async::NotReady)
                            | (Async::Ready(None), Async::NotReady)
                            | (Async::NotReady, Async::Ready(None)) => {
                            // both inputs blocked: phase 2
                            return Ok(Async::NotReady);
                        }
                        (l, r) => {
                            // input ready: phase 1
                            {
                                let definition = &this.definition;
                                manage_side(l, &mut this.partitions_left, &this.partitions_right, &mut this.output_buffer, &mut this.overflow_memory, this.timer, |x| definition.hash_left(x), |x, y| definition.eq(x, y));
                            }
                            this.manage_eviction();
                            {
                                let definition = &this.definition;
                                manage_side(r, &mut this.partitions_right, &this.partitions_left, &mut this.output_buffer, &mut this.overflow_memory, this.timer, |x| definition.hash_right(x), |y, x| definition.eq(x, y));
                            }
                            this.manage_eviction();
                            continue;
                        }
                    }
                }
                XJoin::CleanupPhase(cp) => return Ok(Async::Ready(cp.next())),
                XJoin::Tmp => unreachable!(),
            }
            
            *self = match mem::replace(self, XJoin::Tmp) {
                XJoin::MainPhase(mp) => XJoin::CleanupPhase(mp.switch_to_cleanup()),
                _ => unreachable!(),
            }
        }
    }
}
impl<L, R, D, E> Join<L, R, D, E> for XJoin<L, R, D, E>
    where L: Stream,
          R: Stream<Error=L::Error>,
          E: ExternalStorage<Timestamped<L::Item>> + ExternalStorage<Timestamped<R::Item>>,
          D: HashPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, storage: E, memory_limit: usize) -> Self {
        assert!(memory_limit >= 3);
        let num_partitions = memory_limit / 3; // TODO: is this good?
        let mut partitions_left = Vec::new();
        let mut partitions_right = Vec::new();
        partitions_left.resize_with(num_partitions, Default::default);
        partitions_right.resize_with(num_partitions, Default::default);
        XJoin::MainPhase(MainPhase {
            definition,
            storage,
            left: left.fuse(),
            right: right.fuse(),
            partitions_left,
            partitions_right,
            output_buffer: VecDeque::new(),
            memory_limit: memory_limit - num_partitions * 2, // = num_partitions + division remainder
            overflow_memory: 0,
            timer: 0,
        })
    }
}

