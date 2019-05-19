use std::rc::Rc;
use std::cell::Cell;
use futures::{Future, Stream, Poll, try_ready, Async, stream};

// TODO: futures
//trait Stream {
//    fn next(&mut self) -> Option<Option<T>>; // None = not ready; Some(None) = end
//}

equi_join! { WrapperTypeLeft(TupleTypeLeft => attribute_left) == WrapperTypeRight(TupleTypeRight => attribute_right) }
macro_rules! equi_join {
    ($nl:ident ( $tl:ty => $ml:ident ) == $nr:ident ( $tr:ty => $mr:ident )) => {
        #[derive(Clone, Debug)] struct $nl($tl);
        #[derive(Clone, Debug)] struct $nr($tr);
        impl PartialEq<$nr> for $nl {
            fn eq(&self, rhs: &JoinWrapperRight) -> bool {
                (self.0).$ml == (rhs.0).$mr
            }
        }
        impl PartialOrd<$nr> for $nl {
            fn partial_cmp(&self, rhs: &JoinWrapperRight) -> Option<std::cmp::Ordering> {
                (self.0).$ml.partial_cmp(&(rhs.0).$mr)
            }
        }
        impl std::hash::Hash for $nl {
            fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
                (self.0).$ml.hash(h)
            }
        }
        impl std::hash::Hash for $nr {
            fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
                (self.0).$mr.hash(h)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Tuple { a: i32, b: i32 }
equi_join! { JoinWrapperLeft(Tuple => a) == JoinWrapperRight(Tuple => b) }

/*
struct JoinWrapperLeft(Tuple);
struct JoinWrapperRight(Tuple);
impl PartialEq<JoinWrapperRight> for JoinWrapperLeft {
    fn eq(&self, rhs: &JoinWrapperRight) -> bool {
        self.0.a == rhs.0.b
    }
}
impl PartialOrd<JoinWrapperRight> for JoinWrapperLeft {
    fn partial_cmp(&self, rhs: &JoinWrapperRight) -> Option<std::cmp::Ordering> {
        self.0.a.partial_cmp(&rhs.0.b)
    }
}
impl std::hash::Hash for JoinWrapperLeft {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.0.a.hash(h)
    }
}
impl std::hash::Hash for JoinWrapperRight {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.0.b.hash(h)
    }
}*/


// additional constraints (PartialCmp, Hash, Eq) as needed by the join implementation
trait Join<Left, Right> : Stream<Item=(Left::Item, Right::Item), Error=Left::Error>
    where Left: Stream,
          Right: Stream<Error=Left::Error> {
    fn build(left: Left, right: Right) -> Self;
}

struct OrderedMergeJoin<L: Stream, R: Stream> {
    left: stream::Peekable<L>,
    right: stream::Peekable<R>,
}

impl<L, R> Stream for OrderedMergeJoin<L, R>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item>,
          R::Item: Clone {
    type Item = (L::Item, R::Item);
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut ret = None;
        while ret.is_none() {
            let poll_left = {
                let left = try_ready!(self.left.peek());
                let right = try_ready!(self.right.peek());
                match (left, right) {
                    (Some(l), Some(r)) => {
                        ret = if l == r { Some((l.clone(), r.clone())) } else { None };
                        l < r
                    }
                    _ => break,
                }
            };

            if poll_left {
                if let Async::NotReady = self.left.poll()? {
                    unreachable!();
                }
            } else {
                if let Async::NotReady = self.right.poll()? {
                    unreachable!();
                }
            }
        }
        Ok(Async::Ready(ret))
    }
}


impl<L, R> OrderedMergeJoin<L, R>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item>,
          R::Item: Clone {
    fn build(left: L, right: R) -> Self {
        OrderedMergeJoin { left: left.peekable(), right: right.peekable() }
    }
}



enum SortMergeJoin<L: Stream, R: Stream> {
    InputPhase {
        left: stream::Fuse<L>,
        right: stream::Fuse<R>,

        left_buf: Vec<L::Item>,
        right_buf: Vec<R::Item>,
    },
    OutputPhase(OrderedMergeJoin<stream::IterOk<std::vec::IntoIter<L::Item>, L::Error>, stream::IterOk<std::vec::IntoIter<R::Item>, R::Error>>),
    Tmp,
}
impl<L, R> Stream for SortMergeJoin<L, R>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item> + Ord,
          R::Item: Clone + Ord {
    type Item = (L::Item, R::Item);
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self {
                SortMergeJoin::InputPhase { left, right, left_buf, right_buf, .. } => {
                    let l = left.poll()?;
                    let r = right.poll()?;

                    match (l, r) {
                        (Async::Ready(None), Async::Ready(None)) => {
                            // fall out of the match in order to replace self
                        }
                        (Async::NotReady, Async::NotReady) => return Ok(Async::NotReady),
                        (l, r) => {
                            if let Async::Ready(Some(l)) = l { left_buf.push(l); }
                            if let Async::Ready(Some(r)) = r { right_buf.push(r); }

                            continue;
                        }
                    }
                }
                SortMergeJoin::OutputPhase(omj) => return omj.poll(),
                SortMergeJoin::Tmp => unreachable!(),
            }

            *self = match std::mem::replace(self, SortMergeJoin::Tmp) {
                SortMergeJoin::InputPhase { mut left_buf, mut right_buf, .. } => {
                    left_buf.sort();
                    right_buf.sort();
                    SortMergeJoin::OutputPhase(OrderedMergeJoin::build(stream::iter_ok(left_buf), stream::iter_ok(right_buf)))
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<L, R> Join<L, R> for SortMergeJoin<L, R>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item> + Ord,
          R::Item: Clone + Ord {
    fn build(left: L, right: R) -> Self {
        SortMergeJoin::InputPhase { left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new() }
    }
}

/*
TODO: impossible to implement, requires stream restart
struct NestedLoopJoin<P, L: Stream, R> {
    predicate: P,
    left: L,
    current_left: Option<L::Item>,
    right: R,
}

impl<P, L, R> Stream for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone {
    type Item = P::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.current_left.is_none() {
            self.current_left = try_ready!(self.left.poll());
        }
        while let Some(left) = self.current_left.clone() {
            while let Some(right) = try_ready!(self.right.poll()) {
                if let Some(result) = self.predicate.call(left.clone(), right) {
                    return Ok(Async::Ready(Some(result)));
                }
            }

            self.right.restart();
            self.current_left = None;
            self.current_left = try_ready!(self.left.poll());
        }

        Ok(Async::Ready(None))
    }
}


impl<P, L, R> Join<P, L, R> for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone {
    fn build(predicate: P, mut left: L, right: R) -> Self {
        NestedLoopJoin { current_left: None, predicate, left, right }
    }
}
*/


/*
TODO: useless, only provides stream restart
struct MemorySource<T> {
    vec: Vec<T>,
    index: usize,
}

impl<T: Clone> Stream for MemorySource<T> {
    type Item = T;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, ()> {
        Ok(Async::Ready(if self.index >= self.vec.len() {
            None
        } else {
            let element = self.vec[self.index].clone();
            self.index += 1;
            Some(element)
        }))
    }
}
*/


/*
struct BenchPredicate;
impl JoinPredicate<i32, i32> for BenchPredicate {
    type Output = i32;
    fn call(&self, a: i32, b: i32) -> Option<i32> {
        if a == b { Some(a) } else { None }
    }
}
*/

//struct BenchDataSource;

fn bench_source<T>(data: Vec<T>, counter: &Rc<Cell<usize>>) -> impl Stream<Item=T, Error=()> {
    let rc = Rc::clone(&counter);
    stream::iter_ok::<_, ()>(data).inspect(move |_| { rc.set(rc.get() + 1); })
}

fn bencher() {
    let tuples_read = Rc::new(Cell::new(0));

    let left = bench_source(vec![1,3,4,7,18].into_iter().map(|x| Tuple { a: x, b: 0 }).collect(), &tuples_read);
    let right = bench_source(vec![0, 1, 3, 3,7,42,45].into_iter().map(|x| Tuple { a: 0, b: x }).collect(), &tuples_read);
    //let left = bench_source(vec![1,3,4,7,18], &tuples_read);
    //let right = bench_source(vec![0, 1, 3, 3,7,42,45], &tuples_read);

    let join = OrderedMergeJoin::build(left.map(JoinWrapperLeft), right.map(JoinWrapperRight));

    let mut res = join.and_then(|x| {
        Ok((x, tuples_read.get()))
    });


    loop {
        match res.poll().unwrap() {
            Async::Ready(None) => break,
            Async::NotReady => unimplemented!(),
            x => println!("{:?}", x),
        }
    }
}

fn main() {
/*    let left = stream::iter_ok::<_, ()>(vec![1,3,4,7,18]);
    let right = stream::iter_ok(vec![88, 0, 1, 3, 3,7,42,45]);

    let mut join = SortMergeJoin::build(EquiJoin::make(|x, y| x == y), left, right);

    loop {
        match join.poll().unwrap() {
            Async::Ready(None) => break,
            x => println!("{:?}", x),
        }
    }*/
    bencher();
}
