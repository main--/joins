use std::rc::Rc;
use std::cell::Cell;
use futures::{Future, Stream, Poll, try_ready, Async, stream};

// equi_join! { WrapperTypeLeft(TupleTypeLeft => attribute_left) == WrapperTypeRight(TupleTypeRight => attribute_right) }
macro_rules! equi_join {
    ($nl:ident ( $tl:ty => $ml:ident ) == $nr:ident ( $tr:ty => $mr:ident )) => {
        #[derive(Clone, Debug)] struct $nl($tl);
        #[derive(Clone, Debug)] struct $nr($tr);
        impl PartialEq<$nl> for $nl {
            fn eq(&self, rhs: &$nl) -> bool {
                (self.0).$ml == (rhs.0).$ml
            }
        }
        impl Eq for $nl {}
        impl PartialEq<$nr> for $nr {
            fn eq(&self, rhs: &$nr) -> bool {
                (self.0).$mr == (rhs.0).$mr
            }
        }
        impl Eq for $nr {}
        impl PartialEq<$nr> for $nl {
            fn eq(&self, rhs: &$nr) -> bool {
                (self.0).$ml == (rhs.0).$mr
            }
        }
        impl PartialOrd<$nl> for $nl {
            fn partial_cmp(&self, rhs: &$nl) -> Option<std::cmp::Ordering> {
                (self.0).$ml.partial_cmp(&(rhs.0).$ml)
            }
        }
        impl Ord for $nl {
            fn cmp(&self, rhs: &$nl) -> std::cmp::Ordering {
                (self.0).$ml.cmp(&(rhs.0).$ml)
            }
        }
        impl PartialOrd<$nr> for $nr {
            fn partial_cmp(&self, rhs: &$nr) -> Option<std::cmp::Ordering> {
                (self.0).$mr.partial_cmp(&(rhs.0).$mr)
            }
        }
        impl Ord for $nr {
            fn cmp(&self, rhs: &$nr) -> std::cmp::Ordering {
                (self.0).$mr.cmp(&(rhs.0).$mr)
            }
        }
        impl PartialOrd<$nr> for $nl {
            fn partial_cmp(&self, rhs: &$nr) -> Option<std::cmp::Ordering> {
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


// additional constraints (PartialCmp, Hash, Eq) as needed by the join implementation
/*
trait Join<Left, Right> : Stream<Item=(Left::Item, Right::Item), Error=Left::Error>
    where Left: Stream,
          Right: Stream<Error=Left::Error> {
    fn build(left: Left, right: Right) -> Self;
}*/

struct OrderedMergeJoin<L: Stream, R: Stream, KL, KR> {
    left: stream::Peekable<L>,
    right: stream::Peekable<R>,
    key_left: KL,
    key_right: KR,
}

impl<L, R, KL, KR, KKL, KKR> Stream for OrderedMergeJoin<L, R, KL, KR>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          KL: for<'a> Fn(&'a L::Item) -> KKL,
          KR: for<'a> Fn(&'a R::Item) -> KKR,
          KKL: PartialOrd<KKR> {
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
                        let kl = (self.key_left)(l);
                        let kr = (self.key_right)(r);
                        ret = if kl == kr { Some((l.clone(), r.clone())) } else { None };
                        kl < kr
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


impl<L, R, KL, KR> OrderedMergeJoin<L, R, KL, KR>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone {
    fn build(left: L, right: R, key_left: KL, key_right: KR) -> Self {
        OrderedMergeJoin { left: left.peekable(), right: right.peekable(), key_left, key_right }
    }
}


/*
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

impl<L, R> SortMergeJoin<L, R>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item> + Ord,
          R::Item: Clone + Ord {
    fn build(left: L, right: R) -> Self {
        SortMergeJoin::InputPhase { left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new() }
    }
}
*/

//struct SymmetricHashJoin<L: Stream, R: Stream> {}

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

    let join = OrderedMergeJoin::build(left, right, |x: &Tuple| x.a, |x: &Tuple| x.b);

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
