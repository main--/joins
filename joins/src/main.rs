#![feature(existential_type)]

use std::fmt::Debug;
use std::rc::Rc;
use std::cell::RefCell;
use futures::{Future, Stream, Async, Poll};
use named_type::NamedType;


mod predicate;
use predicate::*;
mod join;
use join::*;


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
existential type BenchSource<T>: Stream<Item=T, Error=()> + Rescan;

fn bencher<J, D, C>(data_left: Vec<D::Left>, data_right: Vec<D::Right>, definition: D, config: C)
where
    J: Join<BenchSource<D::Left>, BenchSource<D::Right>, D, BenchStorage, C> + NamedType,
    D: JoinPredicate,
    D::Left: Clone,
    D::Right: Clone,
    D::Output: Debug,
    J::Error: Debug {
    let simulator = IoSimulator::new();
    simulator.borrow_mut().right_to_left = Fraction::new(1usize, 2usize);
    //simulator.borrow_mut().input_batch_size = Fraction::new(1000usize ,1usize);
    simulator.borrow_mut().disk_ops_per_refill = 20;

    let left = bench_source(data_left, &simulator, Side::Left);
    let right = bench_source(data_right, &simulator, Side::Right);

    let join = J::build(left, right, definition, BenchStorage(Rc::clone(&simulator)), config);

    let mut timings = Vec::new();
    let timed = join.inspect(|_| timings.push((simulator.borrow().read_tuple_count, simulator.borrow().disk_ops_out, simulator.borrow().disk_ops_in)));

    let mut collector = timed.collect();
    loop {
        match collector.poll() {
            Ok(Async::Ready(result)) => {
                println!("{} RESULTS {:?} ({} items)", J::short_type_name(), result, result.len());
                break;
            }
            Ok(Async::NotReady) => simulator.borrow_mut().add_input_budget(),
            Err(e) => {
                println!("{} error: {:?}", J::short_type_name(), e);
                return;
            }
        }
    }
    println!("timings {}: {:?}", J::short_type_name(), timings);
}

fn bench_all<D>(data_left: Vec<D::Left>, data_right: Vec<D::Right>, definition: D)
where
    D: HashPredicate + MergePredicate + Clone,
    D::Left: Clone + Debug,
    D::Right: Clone + Debug,
    D::Output: Debug {
    bencher::<NestedLoopJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), ());
    bencher::<BlockNestedLoopJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), 4);
    //bencher::<OrderedMergeJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SortMergeJoin<_, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), 4);
    bencher::<SimpleHashJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), 4);
    bencher::<SymmetricHashJoin<_, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), 4);
    bencher::<ProgressiveMergeJoin<_, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), 4);
    bencher::<XJoin<_, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), 4);
    bencher::<HashMergeJoin<_, _, _, _, _>, _, _>(data_left.clone(), data_right.clone(), definition.clone(), join::hash_merge::HMJConfig { memory_limit: 10, mem_parts_per_disk_part: 1, num_partitions: 5, fan_in: 4, flushing_policy: join::hash_merge::FlushSmallest });
    // TODO: hybrid hash join
}

fn main() {
    //let left_sorted: Vec<i32> = vec![0,0,0,0,0,0];
    //let right_sorted: Vec<i32> = vec![0,0,0,0,0,0];
    //let left_sorted: Vec<i32> = vec![1,3,3,3,3,3,3,3,3,4,7,18];
    //let right_sorted: Vec<i32> = vec![0, 1, 3, 3,3,7,42,45];
    //let left_sorted: Vec<i32> = (0..20).collect();
    //let right_sorted: Vec<i32> = (0..20).rev().collect();
    let left_sorted: Vec<i32> = (0..20).collect();
    let right_sorted: Vec<i32> = (10..20).chain(0..10).collect();

    let definition = EquiJoin::new(|&x: &i32| x, |&x| x);
    //let definition = definition.map_left(|&x| x * 2);
    //let definition = predicate::MapLeftPredicate { predicate: definition, mapping: |x| x, phantom: std::marker::PhantomData };


    //let left_sorted: Vec<Tuple> = vec![1,3,3,3,3,3,3,3,3,4,7,18].into_iter().map(|x| Tuple { a: x, b: 0 }).collect();
    //let right_sorted: Vec<Tuple> = vec![0, 1, 3, 3,3,7,42,45].into_iter().map(|x| Tuple { a: 0, b: x }).collect();
    //let definition = EquiJoin::new(|x: &Tuple| (x.a, x.b), |x: &Tuple| (x.b, x.a));
    bench_all(left_sorted, right_sorted, definition);
}

