use std::convert::Infallible;
use futures::{Async, Poll, Stream};
use crate::Rescan;

pub struct IterSource<I: Iterator + Clone> {
    saved: I,
    used: I,
}

impl<I: Iterator + Clone> IterSource<I> {
   pub fn new(i: impl IntoIterator<IntoIter = I>) -> IterSource<I> {
       let i = i.into_iter();
       IterSource {
           saved: i.clone(),
           used: i,
       }
   }
}

impl<I: Iterator + Clone> Stream for IterSource<I> {
    type Item = I::Item;
    type Error = Infallible;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(self.used.next()))
    }
}


impl<I: Iterator + Clone> Rescan for IterSource<I> {
    fn rescan(&mut self) {
        self.used = self.saved.clone();
    }
}

pub struct IterReady<S>(S);

impl<S: Stream<Error = Infallible>> Iterator for IterReady<S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.poll() {
            Ok(Async::Ready(e)) => e,
            Ok(Async::NotReady) => None,
            Err(x) => match x {},
        }
    }
}

pub trait IntoIterReady {
    fn iter_ready(self) -> IterReady<Self> where Self: Sized;
}
impl<S: Stream<Error = Infallible>> IntoIterReady for S {
    fn iter_ready(self) -> IterReady<Self> {
        IterReady(self)
    }
}
