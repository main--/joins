use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::rc::Rc;
use std::cmp::Ordering;
use std::cell::Cell;
use futures::{Future, Stream, Poll, try_ready, Async, stream};

trait JoinDefinition {
    type Left;
    type Right;
    type Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output>;
}
trait OrdJoinDefinition: JoinDefinition {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering>;
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering;
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering;
}
trait HashJoinDefinition: JoinDefinition {
    fn hash_left(&self, x: &Self::Left) -> u64;
    fn hash_right(&self, x: &Self::Right) -> u64;
}
struct EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight {
    get_key_left: GetKeyLeft,
    get_key_right: GetKeyRight,

    left: std::marker::PhantomData<Left>,
    right: std::marker::PhantomData<Right>,
}
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: PartialEq<KeyRight>,
      Left: Clone,
      Right: Clone {
    fn new(get_key_left: GetKeyLeft, get_key_right: GetKeyRight) -> Self {
        EquiJoin { get_key_left, get_key_right, left: std::marker::PhantomData, right: std::marker::PhantomData }
    }
}
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> JoinDefinition for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: PartialEq<KeyRight>,
      Left: Clone,
      Right: Clone {
    type Left = Left;
    type Right = Right;
    type Output = (Left, Right);

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        if (self.get_key_left)(left) == (self.get_key_right)(right) {
            Some((left.clone(), right.clone()))
        } else {
            None
        }
    }
}
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> OrdJoinDefinition for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: Ord + PartialOrd<KeyRight>,
      KeyRight: Ord,
      Left: Clone,
      Right: Clone {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        (self.get_key_left)(left).partial_cmp(&(self.get_key_right)(right))
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        (self.get_key_left)(a).cmp(&(self.get_key_left)(b))
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        (self.get_key_right)(a).cmp(&(self.get_key_right)(b))
    }
}
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> HashJoinDefinition for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: PartialEq<KeyRight> + Hash,
      KeyRight: Hash,
      Left: Clone,
      Right: Clone {
    fn hash_left(&self, x: &Self::Left) -> u64 {
        let mut hasher = DefaultHasher::new();
        (self.get_key_left)(x).hash(&mut hasher);
        hasher.finish()
    }
    fn hash_right(&self, x: &Self::Right) -> u64 {
        let mut hasher = DefaultHasher::new();
        (self.get_key_right)(x).hash(&mut hasher);
        hasher.finish()
    }
}




#[derive(Clone, Debug)]
struct Tuple { a: i32, b: i32 }

// additional constraints (PartialCmp, Hash, Eq) as needed by the join implementation
/*
trait Join<Left, Right> : Stream<Item=(Left::Item, Right::Item), Error=Left::Error>
    where Left: Stream,
          Right: Stream<Error=Left::Error> {
    fn build(left: Left, right: Right) -> Self;
}*/

struct OrderedMergeJoin<L: Stream, R: Stream, D> {
    left: stream::Peekable<L>,
    right: stream::Peekable<R>,
    definition: D,
}

impl<L, R, D> Stream for OrderedMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: OrdJoinDefinition<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut ret = None;
        while ret.is_none() {
            let poll_left = {
                let left = try_ready!(self.left.peek());
                let right = try_ready!(self.right.peek());
                match (left, right) {
                    (Some(l), Some(r)) => {
                        ret = self.definition.eq(l, r);
                        self.definition.cmp(l, r) == Some(Ordering::Less)
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


impl<L, R, D> OrderedMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone {
    fn build(left: L, right: R, definition: D) -> Self {
        OrderedMergeJoin { left: left.peekable(), right: right.peekable(), definition }
    }
}


enum SortMergeJoin<L: Stream, R: Stream, D> {
    InputPhase {
        definition: D,
        left: stream::Fuse<L>,
        right: stream::Fuse<R>,

        left_buf: Vec<L::Item>,
        right_buf: Vec<R::Item>,
    },
    OutputPhase(OrderedMergeJoin<stream::IterOk<std::vec::IntoIter<L::Item>, L::Error>, stream::IterOk<std::vec::IntoIter<R::Item>, R::Error>, D>),
    Tmp,
}
impl<L, R, D> Stream for SortMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: OrdJoinDefinition<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
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
                SortMergeJoin::InputPhase { mut left_buf, mut right_buf, definition, .. } => {
                    left_buf.sort_by(|a, b| definition.cmp_left(a, b));
                    right_buf.sort_by(|a, b| definition.cmp_right(a, b));
                    SortMergeJoin::OutputPhase(OrderedMergeJoin::build(stream::iter_ok(left_buf), stream::iter_ok(right_buf), definition))
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<L, R, D> SortMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: OrdJoinDefinition<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        SortMergeJoin::InputPhase { left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new(), definition }
    }
}

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

    let join = SortMergeJoin::build(left, right, EquiJoin::new(|x: &Tuple| x.a, |x: &Tuple| x.b));

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
