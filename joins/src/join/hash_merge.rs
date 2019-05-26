use std::{cmp, mem};
use std::fmt::Debug;
use std::rc::Rc;
use std::collections::VecDeque;
use futures::{Future, Stream, Poll, Async, stream, try_ready};
use named_type::NamedType;
use named_type_derive::*;
use itertools::{Itertools, MinMaxResult};
use debug_everything::Debuggable;

use super::{Join, ExternalStorage, OrderedMergeJoin};
use super::sort_merge::SortMerger;
use super::progressive_merge::IgnoreIndexPredicate;
use crate::predicate::{JoinPredicate, HashPredicate, MergePredicate, SwitchPredicate};

#[derive(NamedType)]
pub struct HashMergeJoin<L, R, D, E>
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
    common: Common<D::Output, E>,

    // is there currently a merge going on?
    merge: Option<MergePhase<L, R, D, E>>,
}
type Merger<D, E> = ValueSink<SortMerger<D, <E as ExternalStorage<<D as JoinPredicate>::Left>>::External>>;
pub struct ValueSink<S: Stream> {
    underlying: stream::Fuse<S>,
    sink: futures::unsync::mpsc::UnboundedSender<Result<Rc<S::Item>, S::Error>>,
}
struct ValueSinkRecv<T, E> {
    recv: futures::unsync::mpsc::UnboundedReceiver<Result<Rc<T>, E>>,
}
impl<T, E> ValueSinkRecv<T, E> {
    fn unpack(self) -> Vec<T> where E: Debug {
        match self.recv.map(|r| match Rc::try_unwrap(r.unwrap()) { Ok(x) => x, _ => unreachable!() }).collect().poll().unwrap() {
            Async::Ready(v) => v,
            Async::NotReady => unreachable!(),
        }
    }
}
impl<S: Stream> ValueSink<S> {
    fn new(underlying: S) -> (Self, ValueSinkRecv<S::Item, S::Error>) {
        let (send, recv) = futures::unsync::mpsc::unbounded();
        (ValueSink { underlying: underlying.fuse(), sink: send }, ValueSinkRecv { recv })
    }
}
impl<S: Stream> Stream for ValueSink<S> {
    type Item = Rc<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, S::Error> {
        Ok(Async::Ready(try_ready!(self.underlying.poll()).map(|x| {
            let x = Rc::new(x);
            self.sink.unbounded_send(Ok(Rc::clone(&x))).unwrap();
            x
        })))
    }
}
impl<S: Stream> Drop for ValueSink<S> {
    fn drop(&mut self) {
        loop {
            match self.poll() {
                Ok(Async::Ready(None)) => break,
                Ok(Async::Ready(Some(_))) => (),
                Ok(Async::NotReady) => if !std::thread::panicking() { panic!("we lost"); },
                Err(e) => drop(self.sink.unbounded_send(Err(e))),
            }
        }
    }
}

pub struct MergePhase<L, R, D, E>
    where
        L: Stream,
        R: Stream,
        D: MergePredicate<Left=L::Item, Right=R::Item>,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    omj: OrderedMergeJoin<Merger<Rc<D>, E>, Merger<SwitchPredicate<Rc<D>>, E>, IgnoreIndexPredicate<Rc<D>>>,
    recv_left: ValueSinkRecv<(usize, L::Item), ()>,
    recv_right: ValueSinkRecv<(usize, R::Item), ()>,
    disk_partition: usize,
}

// TODO: self type, parameter system, etc etc
trait FlushingPolicy {
    fn flush(memory_tuples: &[PartitionStats]) -> usize;
}
struct FlushSmallest;
impl FlushingPolicy for FlushSmallest {
    fn flush(memory_tuples: &[PartitionStats]) -> usize {
        //println!("eviction? {:?}", memory_tuples);
        match memory_tuples.iter().enumerate().minmax_by_key(|(_, &PartitionStats { left, right })| if (left + right) == 0 { std::usize::MAX } else { left + right }) {
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement((i, _)) | MinMaxResult::MinMax((i, _), _) => i,
        }
    }
}

#[derive(Default, Clone, Debug)]
struct PartitionStats {
    left: usize,
    right: usize,
}

struct Common<O, E> {
    storage: E,
    total_inmemory: usize,
    config: HMJConfig,
    output_buffer: VecDeque<O>,
}

struct Partitions<T, E: ExternalStorage<T>> {
    mem: Vec<Vec<T>>,
    in_memory_tuples: Vec<usize>,
    disk: Vec<Vec<E::External>>,
}
impl<T, E: ExternalStorage<T>> Partitions<T, E> {
    fn new(config: &HMJConfig) -> Self {
       let mut x = Partitions { mem: Vec::new(), disk: Vec::new(), in_memory_tuples: vec![0; config.num_partitions / config.mem_parts_per_disk_part] };
       x.mem.resize_with(config.num_partitions, Default::default);
       x.disk.resize_with(config.num_partitions / config.mem_parts_per_disk_part, Default::default);
       x
    }
    fn evict<D: MergePredicate<Left=T>>(&mut self, partition_to_evict: usize, definition: &D, common: &mut Common<D::Output, E>) {
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

    fn insert<D>(&mut self,
                 item: T,
                 other: &mut Partitions<D::Right, E>,
                 definition: &D,
                 common: &mut Common<D::Output, E>)
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

impl<L, R, D, E> HashMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    fn check_eviction(&mut self) {
        if self.common.total_inmemory >= self.common.config.memory_limit {
            // if out of space, go evict
            let memory_table: Vec<_> = self.parts_l.in_memory_tuples.iter().zip(&self.parts_r.in_memory_tuples).map(|(&left, &right)| PartitionStats { left, right }).collect();
            let partition_to_evict = FlushSmallest::flush(&memory_table); // FIXME dont hardcode
            //println!("EVICTING {} because of {:?}", partition_to_evict, memory_table);
            self.parts_l.evict(partition_to_evict, &self.definition, &mut self.common);
            self.parts_r.evict(partition_to_evict, &self.definition.by_ref().switch(), &mut self.common);
        }
    }

    #[allow(dead_code)]
    fn print_memory(&self) {
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
impl<L, R, D, E> Stream for HashMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
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
                        self.parts_l.disk[merge.disk_partition].push(self.common.storage.store(merge.recv_left.unpack().into_iter().map(|(_, x)| x).collect()));
                        self.parts_r.disk[merge.disk_partition].push(self.common.storage.store(merge.recv_right.unpack().into_iter().map(|(_, x)| x).collect()));
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
                        self.parts_r.insert(r, &mut self.parts_l, &self.definition.by_ref().switch(), &mut self.common);
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
                        let (send_right, recv_right) = ValueSink::new(SortMerger::new(r, Rc::clone(&self.definition).switch()));

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
                                // inputs complete => flush all
                                if self.common.total_inmemory == 0 {
                                    return Ok(Async::Ready(None));
                                }

                                // else go back and try for another merge
                                for i in 0..self.parts_l.disk.len() {
                                    self.parts_l.evict(i, &self.definition, &mut self.common);
                                    self.parts_r.evict(i, &self.definition.by_ref().switch(), &mut self.common);
                                }
                            }
                            _ => return Ok(Async::NotReady),
                        }
                    }
                }
            }
        }
    }
}

pub struct HMJConfig {
    pub memory_limit: usize,
    pub num_partitions: usize,
    // parameter p
    // aka memory partitions per disk partition
    pub mem_parts_per_disk_part: usize,
    pub fan_in: usize,
}

impl<L, R, D, E> Join<L, R, D, E, HMJConfig> for HashMergeJoin<L, R, D, E>
    where
        L: Stream,
        R: Stream<Error=L::Error>,
        D: HashPredicate<Left=L::Item, Right=R::Item> + MergePredicate,
        E: ExternalStorage<L::Item> + ExternalStorage<R::Item> {
    fn build(left: L, right: R, definition: D, storage: E, config: HMJConfig) -> Self {
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

