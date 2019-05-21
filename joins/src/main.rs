#![feature(existential_type)]

use std::fmt::Debug;
use std::rc::Rc;
use std::cell::RefCell;
use futures::{Future, Stream, Async, stream, Poll};
use named_type::NamedType;


mod predicate;
use predicate::*;
mod join;
use join::*;



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
            println!("read tuple {:?}", side);
            true
        } else {
            false
        }
    }
    fn notify_disk_io(&mut self, amount: usize) {
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
            disk_ops_per_refill: 0, // not implemented

            left_budget: Fraction::neg_zero(),
            right_budget: Fraction::neg_zero(),
            read_tuple_count: 0,
            disk_ops_count: 0,
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


fn bench_source<T>(data: Vec<T>, simulator: &Rc<RefCell<IoSimulator>>, side: Side) -> BenchSource<T> {
    let rc = Rc::clone(&simulator);
    TupleInputThrottle {
        underlying: stream::iter_ok::<_, ()>(data),
        side,
        simulator: rc,
    }
}

existential type BenchSource<T>: Stream<Item=T, Error=()>;

fn bencher<J, D>(data_left: Vec<D::Left>, data_right: Vec<D::Right>, definition: D)
where
    J: Join<BenchSource<D::Left>, BenchSource<D::Right>, D> + NamedType,
    D: JoinPredicate,
    D::Output: Debug {
    let simulator = IoSimulator::new();
    simulator.borrow_mut().right_to_left = Fraction::new(2usize, 1usize);

    let left = bench_source(data_left, &simulator, Side::Left);
    let right = bench_source(data_right, &simulator, Side::Right);

    let join = J::build(left, right, definition);

    let mut timings = Vec::new();
    let timed = join.inspect(|_| timings.push(simulator.borrow().read_tuple_count));

    let mut collector = timed.collect();
    loop {
        match collector.poll().unwrap() {
            Async::Ready(result) => {
                println!("result {:?}", result);
                break;
            }
            Async::NotReady => simulator.borrow_mut().add_input_budget(),
        }
    }
    println!("timings {}: {:?}", J::short_type_name(), timings);
}

fn bench_all<D>(data_left: Vec<D::Left>, data_right: Vec<D::Right>, definition: D)
where
    D: HashPredicate + MergePredicate + Clone,
    D::Left: Clone,
    D::Right: Clone,
    D::Output: Debug {
    bencher::<OrderedMergeJoin<_, _, _>, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SortMergeJoin<_, _, _>, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SimpleHashJoin<_, _, _>, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SymmetricHashJoin<_, _, _>, _>(data_left.clone(), data_right.clone(), definition.clone());
}

fn main() {
    let left_sorted: Vec<i32> = vec![1,3,3,3,3,3,3,3,3,4,7,18];
    let right_sorted: Vec<i32> = vec![0, 1, 3, 3,3,7,42,45];
    let definition = EquiJoin::new(|&x| x, |&x| x);
    //let left_sorted: Vec<Tuple> = vec![1,3,3,3,3,3,3,3,3,4,7,18].into_iter().map(|x| Tuple { a: x, b: 0 }).collect();
    //let right_sorted: Vec<Tuple> = vec![0, 1, 3, 3,3,7,42,45].into_iter().map(|x| Tuple { a: 0, b: x }).collect();
    //let definition = EquiJoin::new(|x: &Tuple| (x.a, x.b), |x: &Tuple| (x.b, x.a));
    bench_all(left_sorted, right_sorted, definition);
}

