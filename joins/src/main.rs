use futures::Future;

// TODO: futures
//trait TupleSource {
//    fn next(&mut self) -> Option<Option<T>>; // None = not ready; Some(None) = end
//}

trait TupleSource {
    type Item;
    fn next(&mut self) -> Option<Self::Item>;
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



trait Join<Predicate, Left, Right> : TupleSource<Item=Predicate::Output>
    where Predicate: JoinPredicate<Left::Item, Right::Item>,
          Left: TupleSource,
          Right: TupleSource {
    fn build(predicate: Predicate, left: Left, right: Right) -> Self;
}


struct NestedLoopJoin<P, L: TupleSource, R> {
    predicate: P,
    left: L,
    current_left: Option<L::Item>,
    right: R,
}

impl<P, L, R> TupleSource for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: TupleSource,
          R: TupleSource,
          L::Item: Clone {
    type Item = P::Output;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(left) = self.current_left.clone() {
            while let Some(right) = self.right.next() {
                if let Some(result) = self.predicate.call(left.clone(), right) {
                    return Some(result);
                }
            }

            self.current_left = self.left.next();
        }

        None
    }
}

impl<P, L, R> Join<P, L, R> for NestedLoopJoin<P, L, R>
    where P: JoinPredicate<L::Item, R::Item>,
          L: TupleSource,
          R: TupleSource,
          L::Item: Clone {
    fn build(predicate: P, mut left: L, right: R) -> Self {
        NestedLoopJoin { current_left: left.next(), predicate, left, right }
    }
}

/*impl<P, L, R, Output> TupleSource for NestedLoopJoin<P, L, R>
    where L: TupleSource, R: TupleSource, P: Fn(L::Item, R::Item) -> Option<Output> {*/

/*impl<P, L, R> TupleSource for NestedLoopJoin<P, L, R>
    where L: TupleSource<Item=P::, R: TupleSource, P: Fn {
    type Item = Output;
    fn next(&mut self) -> Option<Option<Output>> {
        unimplemented!();
    }
}

impl<P, L, R> Join<P, L, R> for NestedLoopJoin<P, L, R>
    where L: TupleSource, R: TupleSource, P: Fn(L::Item, R::Item) -> Option<Self::Item> {
}*/

struct MemorySource<T> {
    vec: Vec<T>,
    index: usize,
}

impl<T: Clone> TupleSource for MemorySource<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vec.len() {
            self.index = 0;
            None
        } else {
            let element = self.vec[self.index].clone();
            self.index += 1;
            Some(element)
        }
    }
}

/*
struct JP;

impl JoinPredicate<i32, i32> for JP {
    type Output = i32;
    fn call(&self, l: i32, r: i32) -> Option<i32> {
    //println!("{}=={}", l, r);
        if l == r { Some(l) } else { None }
    }
}
*/

fn main() {
    let left = MemorySource { vec: vec![1,3,18,4,7], index: 0 };
    let right = MemorySource { vec: vec![42, 1, 45, 3, 0, 3,7], index: 0 };

    //let mut join = NestedLoopJoin::build(JP, left, right);
    //let mut join = NestedLoopJoin::build(|x, y| if x == y { Some(x) } else { None }, left, right);
    let mut join = NestedLoopJoin::build(EquiJoin::make(|&x, &y| x == (y+1)), left, right);
    while let Some(x) = join.next() {
        println!("{:?}", x);
    }
}
