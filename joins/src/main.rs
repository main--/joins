use futures::{Future, Stream, Poll, try_ready, Async, stream};

// TODO: futures
//trait Stream {
//    fn next(&mut self) -> Option<Option<T>>; // None = not ready; Some(None) = end
//}



trait JoinPredicate<Left, Right> {
    type Output;
    fn call(&self, l: Left, r: Right) -> Option<Self::Output>;
}

struct EquiJoin<F>(F);

impl<F> EquiJoin<F> {
    fn make<L, R>(f: F) -> EquiJoin<F> where F: for<'a, 'b> Fn(&'a L, &'b R) -> bool {
        EquiJoin(f)
    }
}

impl<F, L, R> JoinPredicate<L, R> for EquiJoin<F> where F: for<'a, 'b> Fn(&'a L, &'b R) -> bool {
    type Output = (L, R);
    fn call(&self, l: L, r: R) -> Option<(L, R)> {
        if self.0(&l, &r) { Some((l, r)) } else { None }
    }
}

impl<F, L, R, O> JoinPredicate<L, R> for F where F: Fn(L, R) -> Option<O> {
    type Output = O;
    fn call(&self, l: L, r: R) -> Option<O> {
        self(l, r)
    }
}



trait Join<Predicate, Left, Right> : Stream<Item=Predicate::Output, Error=Left::Error>
    where Predicate: JoinPredicate<Left::Item, Right::Item>,
          Left: Stream,
          Right: Stream<Error=Left::Error> {
    fn build(predicate: Predicate, left: Left, right: Right) -> Self;
}

struct OrderedMergeJoin<P, L: Stream, R: Stream> {
    predicate: P,
    left: stream::Peekable<L>,
    right: stream::Peekable<R>,
}

impl<P, L, R> Stream for OrderedMergeJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item>,
          R::Item: Clone {
    type Item = P::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut ret = None;
        while ret.is_none() {
            let poll_left = {
                let left = try_ready!(self.left.peek());
                let right = try_ready!(self.right.peek());
                match (left, right) {
                    (Some(l), Some(r)) => {
                        ret = self.predicate.call(l.clone(), r.clone());
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


impl<P, L, R> Join<P, L, R> for OrderedMergeJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item>,
          R::Item: Clone {
    fn build(predicate: P, left: L, right: R) -> Self {
        OrderedMergeJoin { predicate, left: left.peekable(), right: right.peekable() }
    }
}



enum SortMergeJoin<P, L: Stream, R: Stream> {
    InputPhase {
        predicate: P,
        left: stream::Fuse<L>,
        right: stream::Fuse<R>,

        left_buf: Vec<L::Item>,
        right_buf: Vec<R::Item>,
    },
    OutputPhase(OrderedMergeJoin<P, stream::IterOk<std::vec::IntoIter<L::Item>, L::Error>, stream::IterOk<std::vec::IntoIter<R::Item>, R::Error>>),
    Tmp,
}
impl<P, L, R> Stream for SortMergeJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item> + Ord,
          R::Item: Clone + Ord {
    type Item = P::Output;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self {
                SortMergeJoin::InputPhase { predicate, left, right, left_buf, right_buf } => {
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
                SortMergeJoin::InputPhase { predicate, left, right, mut left_buf, mut right_buf } => {
                    left_buf.sort();
                    right_buf.sort();
                    SortMergeJoin::OutputPhase(OrderedMergeJoin::build(predicate, stream::iter_ok(left_buf), stream::iter_ok(right_buf)))
                }
                _ => unreachable!(),
            }
        }
    }
}

impl<P, L, R> Join<P, L, R> for SortMergeJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: Stream,
          R: Stream<Error=L::Error>,
          L::Item: Clone + PartialOrd<R::Item> + Ord,
          R::Item: Clone + Ord {
    fn build(predicate: P, mut left: L, right: R) -> Self {
        SortMergeJoin::InputPhase { predicate, left: left.fuse(), right: right.fuse(), left_buf: Vec::new(), right_buf: Vec::new() }
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


fn main() {
    let left = stream::iter_ok::<_, ()>(vec![1,3,4,7,18]);
    let right = stream::iter_ok(vec![88, 0, 1, 3, 3,7,42,45]);

    let mut join = SortMergeJoin::build(EquiJoin::make(|x, y| x == y), left, right);

/*
    let left = MemorySource { vec: vec![1,3,18,4,7], index: 0 };
    let right = MemorySource { vec: vec![42, 1, 45, 3, 0, 3,7], index: 0 };


    //let mut join = NestedLoopJoin::build(JP, left, right);
    //let mut join = NestedLoopJoin::build(|x, y| if x == y { Some(x) } else { None }, left, right);
    let mut join = NestedLoopJoin::build(EquiJoin::make(|&x, &y| x == (y+0)), left, right);

    */
    loop {
        match join.poll().unwrap() {
            Async::Ready(None) => break,
            x => println!("{:?}", x),
        }
    }
}
