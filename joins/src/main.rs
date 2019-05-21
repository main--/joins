#![feature(existential_type)]

use std::fmt::Debug;
use std::rc::Rc;
use std::cell::Cell;
use futures::{Future, Stream, Async, stream};
use named_type::NamedType;


mod definition;
use definition::*;
mod join;
use join::*;



#[derive(Clone, Debug)]
struct Tuple { a: i32, b: i32 }

// additional constraints (PartialCmp, Hash, Eq) as needed by the join implementation



struct IoSimulator {
    //read_left:
}


fn bench_source<T>(data: Vec<T>, counter: &Rc<Cell<usize>>) -> BenchSource<T> {
    let rc = Rc::clone(&counter);
    stream::iter_ok::<_, ()>(data).inspect(move |_| { rc.set(rc.get() + 1); })
}

existential type BenchSource<T>: Stream<Item=T, Error=()>;

fn bencher<J, L: Debug, R: Debug, D>(data_left: Vec<L>, data_right: Vec<R>, definition: D)
where
    J: Join<BenchSource<L>, BenchSource<R>, D> + NamedType,
    D: JoinDefinition<Left=L, Right=R>,
    D::Output: Debug {
    let tuples_read = Rc::new(Cell::new(0));

    let left = bench_source(data_left, &tuples_read);
    let right = bench_source(data_right, &tuples_read);

    let join = J::build(left, right, definition);

    let mut timings = Vec::new();
    let timed = join.inspect(|_| timings.push(tuples_read.get()));

    let results = match timed.collect().poll().unwrap() {
        Async::Ready(x) => x,
        Async::NotReady => unimplemented!(),
    };
    println!("timings {}: {:?}", J::short_type_name(), timings);
}

fn bench_all<L: Debug + Clone, R: Debug + Clone, D>(data_left: Vec<L>, data_right: Vec<R>, definition: D)
where
    D: JoinDefinition<Left=L, Right=R> + HashJoinDefinition + OrdJoinDefinition + Clone,
    D::Output: Debug {
    bencher::<OrderedMergeJoin<_, _, _>, _, _, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SortMergeJoin<_, _, _>, _, _, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SimpleHashJoin<_, _, _>, _, _, _>(data_left.clone(), data_right.clone(), definition.clone());
    bencher::<SymmetricHashJoin<_, _, _>, _, _, _>(data_left.clone(), data_right.clone(), definition.clone());
}

fn main() {
    let left_sorted: Vec<Tuple> = vec![1,3,3,3,3,3,3,3,3,4,7,18].into_iter().map(|x| Tuple { a: x, b: 0 }).collect();
    let right_sorted: Vec<Tuple> = vec![0, 1, 3, 3,3,7,42,45].into_iter().map(|x| Tuple { a: 0, b: x }).collect();
    let definition = EquiJoin::new(|x: &Tuple| (x.a, x.b), |x: &Tuple| (x.b, x.a));
    bench_all(left_sorted, right_sorted, definition);
}

