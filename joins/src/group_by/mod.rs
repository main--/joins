use futures::{Future, Stream};
use std::collections::HashMap;

pub trait GroupBy<'a>: Stream + Sized + 'a {
    fn group_by<I: GroupByItem<'a, Self>, P: GroupByPredicate<'a, Self, I> + 'a>(self, predicate: P) -> I::Future {
        I::consume(predicate, self)
    }
}

impl<'a, S: Stream + Sized + 'a> GroupBy<'a> for S {}

// TODO: GroupBy in-memory lazy returning (Iter<…>, …)
// TODO: GroupBy on streams

pub trait GroupByPredicate<'a, S, I>: Sized
where
    S: Stream + 'a,
    I: GroupByItem<'a, S>,
{
    fn extract(&mut self, item: &S::Item) -> I;
}

pub trait GroupByItem<'a, S: Stream + 'a>: Sized {
    type Output;
    type Future: Future<Item = Self::Output, Error = S::Error> + 'a;
    fn consume<P: GroupByPredicate<'a, S, Self> + 'a>(predicate: P, stream: S) -> Self::Future;
}

impl<'a, S, F, I> GroupByPredicate<'a, S, I> for F
where
    S: Stream + 'a,
    F: Fn(&S::Item) -> I + 'a,
    I: GroupByItem<'a, S> + 'a,
{
    fn extract(&mut self, item: &S::Item) -> I {
        self(item)
    }
}

impl<'a, S: Stream + 'a> GroupByItem<'a, S> for bool {
    type Output = (Vec<S::Item>, Vec<S::Item>);
    type Future = Box<dyn Future<Item = Self::Output, Error = S::Error> + 'a>;

    fn consume<P: GroupByPredicate<'a, S, Self> + 'a>(mut predicate: P, stream: S) -> Self::Future {
        Box::new(stream.fold((Vec::new(), Vec::new()), move |(mut l, mut r), item| {
            match predicate.extract(&item) {
                true => l.push(item),
                false => r.push(item),
            }
            Ok((l, r))
        }))
    }
}

macro_rules! impl_for_int {
    ($($typ:ty),*) => {
        $(
            impl<'a, S: Stream + 'a> GroupByItem<'a, S> for $typ {
                type Output = HashMap<$typ, Vec<S::Item>>;
                type Future = Box<dyn Future<Item = Self::Output, Error = S::Error> + 'a>;

                fn consume<P: GroupByPredicate<'a, S, Self> + 'a>(mut predicate: P, stream: S) -> Self::Future {
                    Box::new(stream.fold(HashMap::new(), move |mut map: Self::Output, item| {
                        let key = predicate.extract(&item);
                        map.entry(key).or_default().push(item);
                        Ok(map)
                    }))
                }
            }
        )*
    }
}

impl_for_int!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, usize, isize);

#[cfg(test)]
mod test {
    use crate::{IterSource, NowOrNever, GroupByItem};
    use crate::group_by::GroupBy;

    #[test]
    fn test_condition() {
        let a = vec![0, -1, 1, -2, 2, -3, 3];
        let stream = IterSource::new(a);
        let (l, r) = stream.group_by(|&i: &i32| i < 0).now_or_never().unwrap();
        assert_eq!(vec![-1, -2, -3], l);
        assert_eq!(vec![0, 1, 2, 3], r);
    }

    #[test]
    fn test_int() {
        #[derive(Debug, Clone, Copy, PartialEq)]
        struct Foo { i: i32, foo: &'static str }
        let a = Foo { i: 0, foo: "a" };
        let b = Foo { i: 0, foo: "b" };
        let c = Foo { i: 1, foo: "c" };
        let d = Foo { i: 2, foo: "d" };
        let e = Foo { i: 0, foo: "e" };
        let f = Foo { i: 2, foo: "f" };
        let g = Foo { i: 0, foo: "g" };
        let vec = vec![a, b, c, d, e, f, g];
        let stream = IterSource::new(vec);
        let mut map = stream.group_by(|foo: &Foo| foo.i).now_or_never().unwrap();
        assert_eq!(vec![a, b, e, g], map.remove(&0).unwrap());
        assert_eq!(vec![c], map.remove(&1).unwrap());
        assert_eq!(vec![d, f], map.remove(&2).unwrap());
        assert!(map.is_empty());
    }

    #[test]
    fn test_enum() {
        #[derive(Debug, Clone, PartialEq)]
        struct Foo {
            i: i32,
            kind: Kind,
        }
        #[derive(Debug, Clone, Copy, PartialEq, GroupByItem)]
        enum Kind { A, B, C }

        let a = Foo { i: 0, kind: Kind::A };
        let b = Foo { i: 1, kind: Kind::A };
        let c = Foo { i: 2, kind: Kind::B };
        let d = Foo { i: 3, kind: Kind::C };
        let e = Foo { i: 4, kind: Kind::A };
        let f = Foo { i: 5, kind: Kind::C };
        let g = Foo { i: 6, kind: Kind::A };
        let vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone() ];

        // borrowed
        let stream = IterSource::new(&vec);
        let (veca, vecb, vecc) = stream.group_by(|foo: &&Foo| foo.kind).now_or_never().unwrap();
        assert_eq!(vec![&a, &b, &e, &g], veca);
        assert_eq!(vec![&c], vecb);
        assert_eq!(vec![&d, &f], vecc);

        // borrow-cloned
        let stream = IterSource::new(vec.iter().cloned());
        let (veca, vecb, vecc) = stream.group_by(|foo: &Foo| foo.kind).now_or_never().unwrap();
        assert_eq!(vec![a.clone(), b.clone(), e.clone(), g.clone()], veca);
        assert_eq!(vec![c.clone()], vecb);
        assert_eq!(vec![d.clone(), f.clone()], vecc);

        // owned
        let stream = IterSource::new(vec);
        let (veca, vecb, vecc) = stream.group_by(|foo: &Foo| foo.kind).now_or_never().unwrap();
        assert_eq!(vec![a, b, e, g], veca);
        assert_eq!(vec![c], vecb);
        assert_eq!(vec![d, f], vecc);
    }
}
