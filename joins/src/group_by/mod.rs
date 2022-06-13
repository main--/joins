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
    use crate::group_by::GroupBy;
    use crate::{IterSource, NowOrNever};

    #[test]
    fn test() {
        let a = vec![0, -1, 1, -2, 2, -3, 3];
        let stream = IterSource::new(a);
        let (l, r) = stream.group_by(|&i: &i32| i < 0).now_or_never().unwrap();
        assert_eq!(vec![-1, -2, -3], l);
        assert_eq!(vec![0, 1, 2, 3], r);
    }
}
