use futures::{Future, Stream, Poll, try_ready, Async, stream};

// TODO: futures
//trait TupleSource {
//    fn next(&mut self) -> Option<Option<T>>; // None = not ready; Some(None) = end
//}

trait TupleSource: Stream {
    fn restart(&mut self);
}



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



trait Join<Predicate, Left, Right> : TupleSource<Item=Predicate::Output, Error=Left::Error>
    where Predicate: JoinPredicate<Left::Item, Right::Item>,
          Left: TupleSource,
          Right: TupleSource<Error=Left::Error> {
    fn build(predicate: Predicate, left: Left, right: Right) -> Self;
}


struct NestedLoopJoin<P, L: TupleSource, R> {
    predicate: P,
    left: L,
    current_left: Option<L::Item>,
    right: R,
}

impl<P, L, R> Stream for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: TupleSource,
          R: TupleSource<Error=L::Error>,
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

impl<P, L, R> TupleSource for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: TupleSource,
          R: TupleSource<Error=L::Error>,
          L::Item: Clone {
    fn restart(&mut self) {
        unimplemented!();
    }
}

impl<P, L, R> Join<P, L, R> for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: TupleSource,
          R: TupleSource<Error=L::Error>,
          L::Item: Clone {
    fn build(predicate: P, mut left: L, right: R) -> Self {
        NestedLoopJoin { current_left: None, predicate, left, right }
    }
}


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
impl<T: Clone> TupleSource for MemorySource<T> {
    fn restart(&mut self) {
        self.index = 0;
    }
}

fn main() {
    let left = MemorySource { vec: vec![1,3,18,4,7], index: 0 };
    let right = MemorySource { vec: vec![42, 1, 45, 3, 0, 3,7], index: 0 };

    let asdf = MemorySource { vec: vec![0,1,2,3,4,5,6], index: 0 };

    //let mut join = NestedLoopJoin::build(JP, left, right);
    //let mut join = NestedLoopJoin::build(|x, y| if x == y { Some(x) } else { None }, left, right);
    let mut join = NestedLoopJoin::build(EquiJoin::make(|&x, &y| x == (y+0)), left, right);
    let mut join = NestedLoopJoin::build(EquiJoin::make(|(x, _), y| x == y), join, asdf);
    while let Async::Ready(Some(x)) = join.poll().unwrap() {
        println!("{:?}", x);
    }
}
