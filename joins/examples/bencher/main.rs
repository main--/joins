#![feature(type_alias_impl_trait)]

use std::fmt::Debug;
use std::rc::Rc;
use std::cmp::Ordering;
use std::cell::RefCell;
use futures::{Async, Future, Poll, Stream};
use named_type::NamedType;
use rand::prelude::*;

use joins::*;


pub struct BenchStorage(Rc<RefCell<IoSimulator>>);
impl<T: Clone> ExternalStorage<T> for BenchStorage {
    type External = BenchExternal<T>;
    fn store(&mut self, tuples: Vec<T>) -> BenchExternal<T> {
        self.0.borrow_mut().notify_disk_io(tuples.len(), true);
        BenchExternal(Rc::new(tuples), Rc::clone(&self.0))
    }
}
pub struct BenchExternal<T>(Rc<Vec<T>>, Rc<RefCell<IoSimulator>>);
impl<T: Clone> External<T> for BenchExternal<T> {
    type Iter = BenchIter<T>;
    fn fetch<'a>(&'a self) -> Self::Iter {
        BenchIter {
            data: Rc::clone(&self.0),
            sim: Rc::clone(&self.1),
            index: 0,
        }
    }
}
pub struct BenchIter<T> {
    data: Rc<Vec<T>>,
    index: usize,
    sim: Rc<RefCell<IoSimulator>>,
}
impl<T: Clone> Iterator for BenchIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.data.get(self.index).map(Clone::clone).map(|x| {
            self.sim.borrow_mut().notify_disk_io(1, false);
            self.index += 1;
            x
        })
    }
}


#[derive(Clone, Debug)]
#[allow(unused)]
struct Tuple { a: i32, b: i32 }

type Fraction = fraction::GenericFraction<usize>;

#[derive(Debug, Copy, Clone)]
enum Side { Left, Right }

struct IoSimulator {
    right_to_left: Fraction,
    input_batch_size: Fraction,
    disk_ops_per_refill: usize,

    left_budget: Fraction,
    right_budget: Fraction,

    read_tuple_count: usize,
    disk_ops_count: usize,

    disk_ops_out: usize,
    disk_ops_in: usize,

    predicate_calls: usize,
    cmp_calls: usize,
    hash_calls: usize,
}
impl IoSimulator {
    fn add_input_budget(&mut self) {
        let input = self.input_batch_size;
        self.left_budget += input;
        self.right_budget += input * self.right_to_left;
    }
    fn read_tuple(&mut self, side: Side) -> bool {
        let budget = match side {
            Side::Left => &mut self.left_budget,
            Side::Right => &mut self.right_budget,
        };
        let one = Fraction::from(1);
        if *budget >= one {
            *budget -= one;
            self.read_tuple_count += 1;
            //println!("read tuple {:?}", side);
            true
        } else {
            false
        }
    }
    fn notify_disk_io(&mut self, amount: usize, out: bool) {
        if out {
            self.disk_ops_out += amount;
        } else {
            self.disk_ops_in += amount;
        }
        if self.disk_ops_per_refill == 0 {
            // disabled - never refill for disk IO
            return;
        }

        let refills = (amount + (self.disk_ops_count % self.disk_ops_per_refill)) / self.disk_ops_per_refill;
        for _ in 0..refills {
            self.add_input_budget();
        }
        self.disk_ops_count += amount;
    }

    fn new() -> Rc<RefCell<IoSimulator>> {
        Rc::new(RefCell::new(IoSimulator {
            right_to_left: Fraction::from(1),
            input_batch_size: Fraction::from(1),
            disk_ops_per_refill: 0,

            left_budget: Fraction::neg_zero(),
            right_budget: Fraction::neg_zero(),
            read_tuple_count: 0,
            disk_ops_count: 0,

            disk_ops_out: 0,
            disk_ops_in: 0,

            predicate_calls: 0,
            cmp_calls: 0,
            hash_calls: 0,
        }))
    }
}
struct TupleInputThrottle<T> {
    underlying: T,
    side: Side,
    simulator: Rc<RefCell<IoSimulator>>,
}
impl<T: Stream> Stream for TupleInputThrottle<T> {
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<T::Item>, T::Error> {
        if self.simulator.borrow_mut().read_tuple(self.side) {
            self.underlying.poll()
        } else {
            Ok(Async::NotReady)
        }
    }
}
impl<T: Rescan> Rescan for TupleInputThrottle<T> {
    fn rescan(&mut self) {
        self.underlying.rescan();
    }
}
#[derive(Debug)]
pub struct IterVec<T> {
    data: Vec<T>,
    index: usize,
}
impl<T: Clone> Stream for IterVec<T> {
    type Item = T;
    type Error = ();
    
    fn poll(&mut self) -> Poll<Option<T>, ()> {
        let index = self.index;
        self.index = index + 1;
        Ok(Async::Ready(self.data.get(index).map(Clone::clone)))
    }
}
impl<T: Clone> Rescan for IterVec<T> {
    fn rescan(&mut self) {
        self.index = 0;
    }
}

fn bench_source<T: Clone>(data: Vec<T>, simulator: &Rc<RefCell<IoSimulator>>, side: Side) -> BenchSource<T> {
    let rc = Rc::clone(&simulator);
    TupleInputThrottle {
        underlying: IterVec { data, index: 0 },
        side,
        simulator: rc,
    }
}

// TODO: delet this
type BenchSource<T: Clone> = impl Stream<Item=T, Error=()> + Rescan;

struct BenchPredicate<P>(P, Rc<RefCell<IoSimulator>>);
impl<P: JoinPredicate> JoinPredicate for BenchPredicate<P> {
    type Left = P::Left;
    type Right = P::Right;
}
impl<P: InnerJoinPredicate> InnerJoinPredicate for BenchPredicate<P> {
    type Output = P::Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        self.1.borrow_mut().predicate_calls += 1;
        self.0.eq(left, right)
    }
}
impl<P: MergePredicate> MergePredicate for BenchPredicate<P> {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        self.1.borrow_mut().cmp_calls += 1;
        self.0.cmp(left, right)
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        self.1.borrow_mut().cmp_calls += 1;
        self.0.cmp_left(a, b)
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        self.1.borrow_mut().cmp_calls += 1;
        self.0.cmp_right(a, b)
    }
}
impl<P: HashPredicate> HashPredicate for BenchPredicate<P> {
    fn hash_left(&self, x: &Self::Left) -> u64 {
        self.1.borrow_mut().hash_calls += 1;
        self.0.hash_left(x)
    }
    fn hash_right(&self, x: &Self::Right) -> u64 {
        self.1.borrow_mut().hash_calls += 1;
        self.0.hash_right(x)
    }
}

fn bencher<J, D, C>(data_left: Vec<D::Left>, data_right: Vec<D::Right>, definition: D, config: C)
where
    J: Join<BenchSource<D::Left>, BenchSource<D::Right>, BenchPredicate<D>, BenchStorage, C> + NamedType,
    D: InnerJoinPredicate,
    D::Left: Clone,
    D::Right: Clone,
    D::Output: Debug,
    J::Error: Debug {
    let simulator = IoSimulator::new();
    simulator.borrow_mut().right_to_left = Fraction::new(2usize, 1usize);
    //simulator.borrow_mut().input_batch_size = Fraction::new(1_000_000_000usize ,1usize);
    //simulator.borrow_mut().input_batch_size = Fraction::new(20_000usize ,1usize);
    simulator.borrow_mut().disk_ops_per_refill = 15;
    //simulator.borrow_mut().disk_ops_per_refill = 10;

    let left = bench_source(data_left, &simulator, Side::Left);
    let right = bench_source(data_right, &simulator, Side::Right);

    let join = J::build(left, right, BenchPredicate(definition, Rc::clone(&simulator)), BenchStorage(Rc::clone(&simulator)), config);

    //let mut timings = Vec::new();
    let mut i = 0;
    let timed = join.inspect(|_| {
    let simulator = simulator.borrow();
    //timings.push((simulator.read_tuple_count, simulator.disk_ops_out, simulator.disk_ops_in, simulator.predicate_calls));
    println!("{} {} {} {} {} {} {} {}", i, J::short_type_name(), simulator.read_tuple_count, simulator.disk_ops_out, simulator.disk_ops_in, simulator.predicate_calls, simulator.cmp_calls, simulator.hash_calls);
    i += 1;
    });

    //println!("Running {} ...", J::short_type_name());
    let mut collector = timed.collect();
    loop {
        match collector.poll() {
            Ok(Async::Ready(_result)) => {
                //println!("{} RESULTS {:?} ({} items)", J::short_type_name(), (), result.len());
                break;
            }
            Ok(Async::NotReady) => {
                //println!("=== REFILL ===");
                simulator.borrow_mut().add_input_budget();
            }
            Err(e) => {
                eprintln!("{} error: {:?}", J::short_type_name(), e);
                return;
            }
        }
    }
    //println!("timings {}: {:?}", J::short_type_name(), timings);
    /*
    for (i, (intuples, dout, din, predicate)) in timings.into_iter().enumerate() {
        println!("{} {} {} {} {} {}", i, J::short_type_name(), intuples, dout, din, predicate);
    }*/
}

fn bench_all<D>(data_left: Vec<D::Left>, data_right: Vec<D::Right>, definition: D)
where
    D: InnerJoinPredicate + HashPredicate + MergePredicate + Clone,
    D::Left: Clone + Debug,
    D::Right: Clone + Debug,
    D::Output: Debug {
    let memory = 10000;
    println!("Index Algorithm TuplesIn DiskOut DiskIn PredicateCalls CmpCalls HashCalls");
    //bencher::<NestedLoopJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), ());
    //bencher::<BlockNestedLoopJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), memory);
    //bencher::<OrderedMergeJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), ());
    bencher::<SortMergeJoin<_, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), memory);
    //bencher::<SimpleHashJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), memory);
    //bencher::<SymmetricHashJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), memory);
    bencher::<ProgressiveMergeJoin<_, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), memory);
    bencher::<XJoin<_, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), memory);
    bencher::<HashMergeJoin<_, _, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), hash_merge::HMJConfig {
        memory_limit: memory,
        mem_parts_per_disk_part: memory / 20,
        num_partitions: memory / 2,
        fan_in: 256,
        flushing_policy: hash_merge::flush::Adaptive { a: 10, b: 0.25 },
    });
    // TODO: hybrid hash join
}

fn main() {
    //let left_sorted: Vec<i32> = vec![0,0,0,0,0,0];
    //let right_sorted: Vec<i32> = vec![0,0,0,0,0,0];
    //let left_sorted: Vec<i32> = vec![1,3,3,3,3,3,3,3,3,4,7,18];
    //let right_sorted: Vec<i32> = vec![0, 1, 3, 3,3,7,42,45];
    //let left_sorted: Vec<i32> = (0..20).collect();
    //let right_sorted: Vec<i32> = (0..20).rev().collect();
    
    //let left_sorted: Vec<i32> = (0..20).collect();
    //let right_sorted: Vec<i32> = (10..20).chain(0..10).collect();
    
    let mut left_sorted: Vec<i32> = (0..1_000_000).collect();
    let mut right_sorted: Vec<i32> = (0..1_00_000).collect();
    
    let mut rng = SmallRng::seed_from_u64(42);
    left_sorted.shuffle(&mut rng);
    right_sorted.shuffle(&mut rng);
    eprintln!("shuffle done !!");
    

    let definition = EquiJoin::new(|&x: &i32| x, |&x| x);
    //let definition = definition.map_left(|&x| x * 2);
    //let definition = predicate::MapLeftPredicate { predicate: definition, mapping: |x| x, phantom: std::marker::PhantomData };


    //let left_sorted: Vec<Tuple> = vec![1,3,3,3,3,3,3,3,3,4,7,18].into_iter().map(|x| Tuple { a: x, b: 0 }).collect();
    //let right_sorted: Vec<Tuple> = vec![0, 1, 3, 3,3,7,42,45].into_iter().map(|x| Tuple { a: 0, b: x }).collect();
    //let definition = EquiJoin::new(|x: &Tuple| (x.a, x.b), |x: &Tuple| (x.b, x.a));
    bench_all(left_sorted, right_sorted, definition);
}

