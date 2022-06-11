use std::{cmp, mem};
use std::rc::Rc;
use std::collections::VecDeque;
use futures::{Stream, Poll, Async, stream};
use named_type::NamedType;
use named_type_derive::*;
use itertools::Itertools;

use super::{Join, ExternalStorage, OrderedMergeJoin};
use super::sort_merge::SortMerger;
use super::progressive_merge::IgnoreIndexPredicate;
use crate::predicate::{JoinPredicate, HashPredicate, MergePredicate, SwapPredicate};

pub mod flush;
use self::flush::{FlushingPolicy, PartitionStats};
use crate::value_skimmer::{ValueSink, ValueSinkRecv};

#[derive(NamedType)]
pub struct HashMergeJoin<L, R, D, E, F>
    where
        L: Stream,
        R: Stream,
        D: MergePredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    parts_l: Partitions<L::Item, E>,
    parts_r: Partitions<R::Item, E>,
    definition: Rc<D>,
    common: Common<D::Output, E, F>,

    // is there currently a merge going on?
    merge: Option<MergePhase<L, R, D, E>>,
}
type Merger<D, E> = ValueSink<SortMerger<D, <E as ExternalStorage<<D as JoinPredicate>::Left>>::External>>;

pub struct MergePhase<L, R, D, E>
    where
        L: Stream,
        R: Stream,
        D: MergePredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    omj: OrderedMergeJoin<Merger<Rc<D>, E>, Merger<SwapPredicate<Rc<D>>, E>, IgnoreIndexPredicate<Rc<D>>>,
    recv_left: ValueSinkRecv<(usize, L::Item), ()>,
    recv_right: ValueSinkRecv<(usize, R::Item), ()>,
    disk_partition: usize,
}

struct Common<O, E, F> {
    storage: E,
    total_inmemory: usize,
    config: HMJConfig<F>,
    output_buffer: VecDeque<O>,
}

struct Partitions<T, E: ExternalStorage<T>> {
    mem: Vec<Vec<T>>,
    in_memory_tuples: Vec<usize>,
    disk: Vec<Vec<E::External>>,
}
impl<T, E: ExternalStorage<T>> Partitions<T, E> {
    fn new<F>(config: &HMJConfig<F>) -> Self {
       let mut x = Partitions { mem: Vec::new(), disk: Vec::new(), in_memory_tuples: vec![0; config.num_partitions / config.mem_parts_per_disk_part] };
       x.mem.resize_with(config.num_partitions, Default::default);
       x.disk.resize_with(config.num_partitions / config.mem_parts_per_disk_part, Default::default);
       x
    }
    fn evict<D: MergePredicate<Left=T>, F: FlushingPolicy>(&mut self, partition_to_evict: usize, definition: &D, common: &mut Common<D::Output, E, F>) {
        let mut eviction: Vec<_> = self.mem.iter_mut().enumerate()
            .filter(|(i, _)| (i / common.config.mem_parts_per_disk_part) == partition_to_evict)
            .flat_map(|(_, x)| mem::replace(x, Vec::new())).collect();
        //println!("evicting {} tuples", eviction.len());
        eviction.sort_by(|a, b| definition.cmp_left(a, b));
        common.total_inmemory -= eviction.len();
        self.in_memory_tuples[partition_to_evict] -= eviction.len();
        assert_eq!(0, self.in_memory_tuples[partition_to_evict]);
        self.disk[partition_to_evict].push(common.storage.store(eviction));
    }

    fn insert<D, F: FlushingPolicy>(
        &mut self,
        item: T,
        other: &mut Partitions<D::Right, E>,
        definition: &D,
        common: &mut Common<D::Output, E, F>)
    where
        D: MergePredicate + HashPredicate<Left=T>,
        E: ExternalStorage<D::Right>
    {
        let hash = (definition.hash_left(&item) % (self.mem.len() as u64)) as usize;
        //println!("hash({:?}) = {}", item.debug(), hash);
        //println!("disks = {}", self.disk.len());
        common.output_buffer.extend(other.mem[hash].iter().filter_map(|x| definition.eq(&item, x)));
        self.mem[hash].push(item);
        common.total_inmemory += 1;
        self.in_memory_tuples[hash / common.config.mem_parts_per_disk_part] += 1;
    }
}

impl<L, R, D, E, F> HashMergeJoin<L, R, D, E, F>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        F: FlushingPolicy,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    fn check_eviction(&mut self) {
        if self.common.total_inmemory >= self.common.config.memory_limit {
            // if out of space, go evict
            let memory_table: Vec<_> = self.parts_l.in_memory_tuples.iter().zip(&self.parts_r.in_memory_tuples)
                .map(|(l, r)| PartitionStats { left: *l, right: *r }).collect();
            let partition_to_evict = self.common.config.flushing_policy.flush(&memory_table); // FIXME dont hardcode
            //println!("EVICTING {} because of {:?}", partition_to_evict, memory_table);
            self.parts_l.evict(partition_to_evict, &self.definition, &mut self.common);
            self.parts_r.evict(partition_to_evict, &self.definition.by_ref().swap(), &mut self.common);
        }
    }

    #[cfg(feature = "debug")]
    #[allow(dead_code)]
    fn print_memory(&self) {
        use debug_everything::Debuggable;
        for (i, (l, r)) in self.parts_l.mem.iter().zip(&self.parts_r.mem).enumerate() {
            println!("PARTITION#{}", i);
            print!("left: ");
            for l in l { print!("{:?}, ", l.debug()); }
            println!();
            print!("right: ");
            for r in r { print!("{:?}, ", r.debug()); }
            println!();
        }
    }
}
impl<L, R, D, E, F> Stream for HashMergeJoin<L, R, D, E, F>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        F: FlushingPolicy,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            if let Some(x) = self.common.output_buffer.pop_front() {
                return Ok(Async::Ready(Some(x)));
            }

            // PAPER UNCLEAR: do we finish the merge first? or poll more input asap?
            if let Some(mut merge) = self.merge.take() {
                match merge.omj.poll().unwrap() {
                    Async::Ready(Some(x)) => {
                        // merge ongoing, yield tuple
                        self.merge = Some(merge);
                        return Ok(Async::Ready(Some(x)));
                    }
                    Async::Ready(None) => {
                        // merge complete, write merged partitions back to disk
                        drop(merge.omj);

                        if !self.left.is_done() || !self.right.is_done() {
                            self.parts_l.disk[merge.disk_partition].push(self.common.storage.store(merge.recv_left.unpack().into_iter().map(|(_, x)| x).collect()));
                            self.parts_r.disk[merge.disk_partition].push(self.common.storage.store(merge.recv_right.unpack().into_iter().map(|(_, x)| x).collect()));
                        }
                    }
                    Async::NotReady => unreachable!(),
                }
            }

            match (self.left.poll()?, self.right.poll()?) {
                (l @ Async::Ready(Some(_)), r) | (l, r @ Async::Ready(Some(_))) => {
                    // we have inputs => hash phase
                    if let Async::Ready(Some(l)) = l {
                        self.check_eviction();
                        self.parts_l.insert(l, &mut self.parts_r, &self.definition, &mut self.common);
                    }
                    if let Async::Ready(Some(r)) = r {
                        self.check_eviction();
                        self.parts_r.insert(r, &mut self.parts_l, &self.definition.by_ref().swap(), &mut self.common);
                    }
                }
                (Async::Ready(None), Async::Ready(None)) if self.common.total_inmemory != 0 => {
                    // inputs complete => flush all
                    for i in 0..self.parts_l.disk.len() {
                        self.parts_l.evict(i, &self.definition, &mut self.common);
                        self.parts_r.evict(i, &self.definition.by_ref().swap(), &mut self.common);
                    }
                }
                (l, r) => {
                    // we don't => merge phase

                    // what to pick ??
                    // for now: find the biggest, then merge up to the given fan-in
                    // PAPER UNCLEAR: perhaps we should decline to merge unless we're in cleanup phase?
                    let fan_in = self.common.config.fan_in;
                    let merge = self.parts_l.disk.iter_mut().zip(self.parts_r.disk.iter_mut())
                        .enumerate().filter(|(_, (l, _))| l.len() > 1)
                        .sorted_by_key(|(_, (l, _))| l.len()).rev()
                        .map(|(i, (l, r))| (i, l.drain(..cmp::min(l.len(), fan_in)).collect(), r.drain(..cmp::min(r.len(), fan_in))
                        .collect())).next();
                    if let Some((i, l, r)) = merge {
                        let (send_left, recv_left) = ValueSink::new(SortMerger::new(l, Rc::clone(&self.definition)));
                        let (send_right, recv_right) = ValueSink::new(SortMerger::new(r, Rc::clone(&self.definition).swap()));

                        self.merge = Some(MergePhase {
                            disk_partition: i,
                            recv_left,
                            recv_right,
                            omj: OrderedMergeJoin::new(send_left, send_right, IgnoreIndexPredicate(Rc::clone(&self.definition))),
                        });
                    } else {
                        // none found, nothing to do!
                        match (l, r) {
                            (Async::Ready(None), Async::Ready(None)) => {
                                assert_eq!(0, self.common.total_inmemory);
                                return Ok(Async::Ready(None));
                            }
                            _ => return Ok(Async::NotReady),
                        }
                    }
                }
            }
        }
    }
}

pub struct HMJConfig<F> {
    pub memory_limit: usize,
    pub num_partitions: usize, // paper has no idea regarding memory_limit vs num_partitions

    // parameter p
    // aka memory partitions per disk partition
    pub mem_parts_per_disk_part: usize, // paper recommends: 5% * num_partitions

    // parameter f
    pub fan_in: usize,

    pub flushing_policy: F,
}


impl<L, R, D, E, F> Join<L, R, D, E, HMJConfig<F>> for HashMergeJoin<L, R, D, E, F>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        F: FlushingPolicy,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    fn build(left: L, right: R, definition: D, storage: E, config: HMJConfig<F>) -> Self {
        // assert!(config.num_partitions <= (config.memory_limit / 2)); // not sure if this /actually/ must hold?

        // mem_parts_per_disk_part must evenly divide num_partitions
        assert_eq!(0, config.num_partitions % config.mem_parts_per_disk_part);

        assert!(config.fan_in > 1);

        HashMergeJoin {
            definition: Rc::new(definition),
            parts_l: Partitions::new(&config),
            parts_r: Partitions::new(&config),
            common: Common {
                storage,
                config,
                total_inmemory: 0,
                output_buffer: VecDeque::new(),
            },
            left: left.fuse(),
            right: right.fuse(),

            merge: None,
        }
    }
}

