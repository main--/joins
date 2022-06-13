use futures::{Future, Stream};

pub trait GroupBy: Stream + Sized {
    fn group_by<P: GroupByPredicate<Self>>(self, predicate: P) -> P::Future {
        predicate.consume(self)
    }
}

impl<S: Stream + Sized> GroupBy for S {}

// TODO: GroupBy in-memory lazy returning (Iter<…>, …)
// TODO: GroupBy on streams

pub trait GroupByPredicate<S: Stream> {
    type Output;
    type Future: Future<Item = Self::Output, Error = S::Error>;

    fn consume(self, stream: S) -> Self::Future;
}

impl<S, F> GroupByPredicate<S> for F
where
    S: Stream + 'static,
    S::Item: 'static,
    S::Error: 'static,
    F: Fn(&S::Item) -> bool + 'static,
{
    type Output = (Vec<S::Item>, Vec<S::Item>);
    type Future = Box<dyn Future<Item = Self::Output, Error = S::Error>>;

    fn consume(self, stream: S) -> Self::Future {
        Box::new(stream.fold((Vec::new(), Vec::new()), move |(mut l, mut r), item| {
            match self(&item) {
                true => l.push(item),
                false => r.push(item),
            }
            Ok((l, r))
        }))
    }
}


#[cfg(test)]
mod test {
    use std::marker::PhantomData;
    use futures::{Future, Stream};
    use crate::group_by::{GroupBy, GroupByPredicate};
    use crate::{IterSource, NowOrNever};

    #[test]
    fn test() {
        let a = vec![0, -1, 1, -2, 2, -3, 3];
        let stream = IterSource::new(a);
        let (l, r) = stream.group_by(|&i: &i32| i < 0).now_or_never().unwrap();
        assert_eq!(vec![-1, -2, -3], l);
        assert_eq!(vec![0, 1, 2, 3], r);
    }

    #[test]
    fn test_enum() {
        #[derive(Debug, Clone, PartialEq)]
        struct Foo {
            i: i32,
            kind: Kind,
        }
        #[derive(Debug, Clone, PartialEq)]
        enum Kind { A, B, C }

        struct GroupByKindPredicate<T, F: Fn(&T) -> &Kind> {
            mapper: F,
            _phantom: PhantomData<fn(&T) -> &Kind>,
        }
        impl<T, F: Fn(&T) -> &Kind> GroupByKindPredicate<T, F> {
            fn new(mapper: F) -> GroupByKindPredicate<T, F> {
                GroupByKindPredicate { mapper, _phantom: PhantomData }
            }
        }
        impl<S, F> GroupByPredicate<S> for GroupByKindPredicate<S::Item, F>
            where
                S: Stream + 'static,
                S::Item: 'static,
                S::Error: 'static,
                F: Fn(&S::Item) -> &Kind + 'static,
        {
            type Output = (Vec<S::Item>, Vec<S::Item>, Vec<S::Item>);
            type Future = Box<dyn Future<Item = Self::Output, Error = S::Error>>;

            fn consume(self, stream: S) -> Self::Future {
                Box::new(stream.fold((Vec::new(), Vec::new(), Vec::new()), move |(mut a, mut b, mut c), item| {
                    match (self.mapper)(&item) {
                        Kind::A => a.push(item),
                        Kind::B => b.push(item),
                        Kind::C => c.push(item),
                    }
                    Ok((a, b, c))
                }))
            }
        }

        let a = Foo { i: 0, kind: Kind::A };
        let b = Foo { i: 1, kind: Kind::A };
        let c = Foo { i: 2, kind: Kind::B };
        let d = Foo { i: 3, kind: Kind::C };
        let e = Foo { i: 4, kind: Kind::A };
        let f = Foo { i: 5, kind: Kind::C };
        let g = Foo { i: 6, kind: Kind::A };
        let vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone() ];
        let stream = IterSource::new(vec);
        let (veca, vecb, vecc) = stream.group_by(GroupByKindPredicate::new(|foo: &Foo| &foo.kind)).now_or_never().unwrap();
        assert_eq!(vec![a, b, e, g], veca);
        assert_eq!(vec![c], vecb);
        assert_eq!(vec![d, f], vecc);
    }
}
