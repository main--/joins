#![feature(existential_type)]

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;
use std::collections::hash_map::DefaultHasher;
use std::rc::Rc;
use std::cmp::Ordering;
use std::cell::Cell;
use futures::{Future, Stream, Poll, try_ready, Async, stream};
use multimap::MultiMap;

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

trait Join<Left, Right, Definition> : Stream<Item=Definition::Output, Error=Left::Error>
    where Left: Stream,
          Right: Stream<Error=Left::Error>,
          Definition: JoinDefinition<Left=Left::Item, Right=Right::Item> {
    fn build(left: Left, right: Right, definition: Definition) -> Self;
}

struct OrderedMergeJoin<L: Stream, R: Stream, D> {
    left: stream::Peekable<L>,
    right: stream::Peekable<R>,
    definition: D,
    eq_buffer: Vec<L::Item>,
    eq_cursor: usize,
    replay_mode: bool,
}

impl<L, R, D> Stream for OrderedMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + Debug,
          R::Item: Clone + Debug,
          D: OrdJoinDefinition<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut ret = None;
        while ret.is_none() {
            let order = {
                let left = if self.replay_mode {
                    let x = &self.eq_buffer[self.eq_cursor];
                    self.eq_cursor += 1;
                    Some(x)
                } else {
                    try_ready!(self.left.peek())
                };
                let right = try_ready!(self.right.peek());
                //println!("matching {:?} vs {:?}", left, right);
                match (left, right) {
                    (Some(l), Some(r)) => {
                        ret = self.definition.eq(l, r);
                        self.definition.cmp(l, r).unwrap()
                    }
                    _ => break,
                }
            };

            match order {
                Ordering::Less => {
                    if self.replay_mode {
                        self.eq_buffer.clear();
                        self.eq_cursor = 0;
                        self.replay_mode = false;
                    } else {
                        if let Async::NotReady = self.left.poll()? {
                            unreachable!();
                        }
                    }
                }
                Ordering::Greater => {
                    assert!(!self.replay_mode);
                    if !self.eq_buffer.is_empty() {
                        println!("entering replay mode with {:?}", self.eq_buffer);
                        self.replay_mode = true;
                    }

                    if let Async::NotReady = self.right.poll()? {
                        unreachable!();
                    }
                }
                Ordering::Equal => {
                    if self.replay_mode {
                        if self.eq_cursor >= self.eq_buffer.len() {
                            if let Async::NotReady = self.right.poll()? {
                                unreachable!();
                            }
                            self.eq_cursor = 0;
                        }
                    } else {
                        match self.left.poll()? {
                            Async::Ready(Some(left)) => self.eq_buffer.push(left),
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }
        Ok(Async::Ready(ret))
    }
}


impl<L, R, D> Join<L, R, D> for OrderedMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + Debug,
          R::Item: Clone + Debug,
          D: OrdJoinDefinition<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        OrderedMergeJoin { left: left.peekable(), right: right.peekable(), definition, eq_buffer: Vec::new(), eq_cursor: 0, replay_mode: false }
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
          L::Item: Clone + Debug,
          R::Item: Clone + Debug,
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

impl<L, R, D> Join<L, R, D> for SortMergeJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + Debug,
          R::Item: Clone + Debug,
          D: OrdJoinDefinition<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        SortMergeJoin::InputPhase { left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new(), definition }
    }
}

struct SimpleHashJoin<L: Stream, R: Stream, D: JoinDefinition> {
    definition: D,
    left: stream::Fuse<L>,
    right: R,
    table: MultiMap<u64, L::Item>,
    output_buffer: VecDeque<D::Output>,
}
impl<L, R, D> Stream for SimpleHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: HashJoinDefinition<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // build phase
        while let Some(left) = try_ready!(self.left.poll()) {
            self.table.insert(self.definition.hash_left(&left), left);
        }

        // probe phase


        // carry-over buffer
        while let Some(buffered) = self.output_buffer.pop_front() {
            return Ok(Async::Ready(Some(buffered)));
        }
        // actual probing (spills excess candidates to buffer)
        while let Some(right) = try_ready!(self.right.poll()) {
            for candidate in self.table.get_vec(&self.definition.hash_right(&right)).into_iter().flatten() {
                if let Some(x) = self.definition.eq(candidate, &right) {
                    self.output_buffer.push_back(x);
                }
            }
            if let Some(buffered) = self.output_buffer.pop_front() {
                return Ok(Async::Ready(Some(buffered)));
            }
        }

        // done
        Ok(Async::Ready(None))
    }
}
impl<L, R, D> Join<L, R, D> for SimpleHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: HashJoinDefinition<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        SimpleHashJoin { definition, left: left.fuse(), right, table: MultiMap::new(), output_buffer: VecDeque::new() }
    }
}




struct SymmetricHashJoin<L: Stream, R: Stream, D: JoinDefinition> {
    definition: D,
    left: stream::Fuse<L>,
    right: stream::Fuse<R>,
    table_left: MultiMap<u64, L::Item>,
    table_right: MultiMap<u64, R::Item>,
    output_buffer: VecDeque<D::Output>,
}
impl<L, R, D> Stream for SymmetricHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: HashJoinDefinition<Left=L::Item, Right=R::Item> {
    type Item = D::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            // carry-over buffer
            if let Some(buffered) = self.output_buffer.pop_front() {
                return Ok(Async::Ready(Some(buffered)));
            }

            let left = self.left.poll()?;
            let right = self.right.poll()?;

            match (left, right) {
                (Async::Ready(None), Async::Ready(None)) => return Ok(Async::Ready(None)),
                (Async::NotReady, Async::NotReady) | (Async::Ready(None), Async::NotReady) | (Async::NotReady, Async::Ready(None)) => return Ok(Async::NotReady),
                (l, r) => {
                    if let Async::Ready(Some(l)) = l {
                        let hash = self.definition.hash_left(&l);
                        for candidate in self.table_right.get_vec(&hash).into_iter().flatten() {
                            if let Some(x) = self.definition.eq(&l, candidate) {
                                self.output_buffer.push_back(x);
                            }
                        }
                        self.table_left.insert(hash, l);
                    }
                    if let Async::Ready(Some(r)) = r {
                        let hash = self.definition.hash_right(&r);
                        for candidate in self.table_left.get_vec(&hash).into_iter().flatten() {
                            if let Some(x) = self.definition.eq(candidate, &r) {
                                self.output_buffer.push_back(x);
                            }
                        }
                        self.table_right.insert(hash, r);
                    }
                }
            }
        }
    }
}
impl<L, R, D> Join<L, R, D> for SymmetricHashJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone,
          R::Item: Clone,
          D: HashJoinDefinition<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D) -> Self {
        SymmetricHashJoin { definition, left: left.fuse(), right: right.fuse(), table_left: MultiMap::new(), table_right: MultiMap::new(), output_buffer: VecDeque::new() }
    }
}




fn bench_source<T>(data: Vec<T>, counter: &Rc<Cell<usize>>) -> BenchSource<T> {
    let rc = Rc::clone(&counter);
    stream::iter_ok::<_, ()>(data).inspect(move |_| { rc.set(rc.get() + 1); })
}

fn bench_join() -> BenchJoin {
    EquiJoin::new(|x: &Tuple| (x.a, x.b), |x: &Tuple| (x.b, x.a))
}

existential type BenchSource<T>: Stream<Item=T, Error=()>;
existential type BenchJoin: HashJoinDefinition<Left=Tuple, Right=Tuple, Output=(Tuple, Tuple)> + OrdJoinDefinition;

fn bencher<J>() where J: Join<BenchSource<Tuple>, BenchSource<Tuple>, BenchJoin> {
    let tuples_read = Rc::new(Cell::new(0));

    let left = bench_source(vec![1,3,3,3,3,3,3,3,3,4,7,18].into_iter().map(|x| Tuple { a: x, b: 0 }).collect(), &tuples_read);
    let right = bench_source(vec![0, 1, 3, 3,3,7,42,45].into_iter().map(|x| Tuple { a: 0, b: x }).collect(), &tuples_read);

    let join = J::build(left, right, bench_join());

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
    bencher::<OrderedMergeJoin<_, _, _>>();
    bencher::<SortMergeJoin<_, _, _>>();
    bencher::<SimpleHashJoin<_, _, _>>();
    bencher::<SymmetricHashJoin<_, _, _>>();
}
