use std::fmt::Debug;
use std::rc::Rc;
use futures::{Future, Stream, Poll, Async, stream, try_ready};

pub struct ValueSink<S: Stream> {
    underlying: stream::Fuse<S>,
    sink: futures::unsync::mpsc::UnboundedSender<Result<Rc<S::Item>, S::Error>>,
}
pub struct ValueSinkRecv<T, E> {
    recv: futures::unsync::mpsc::UnboundedReceiver<Result<Rc<T>, E>>,
}
impl<T, E> ValueSinkRecv<T, E> {
    pub fn unpack(self) -> Vec<T> where E: Debug {
        match self.recv.map(|r| match Rc::try_unwrap(r.unwrap()) { Ok(x) => x, _ => unreachable!() }).collect().poll().unwrap() {
            Async::Ready(v) => v,
            Async::NotReady => unreachable!(),
        }
    }
}
impl<S: Stream> ValueSink<S> {
    pub fn new(underlying: S) -> (Self, ValueSinkRecv<S::Item, S::Error>) {
        let (send, recv) = futures::unsync::mpsc::unbounded();
        (ValueSink { underlying: underlying.fuse(), sink: send }, ValueSinkRecv { recv })
    }
}
impl<S: Stream> Stream for ValueSink<S> {
    type Item = Rc<S::Item>;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, S::Error> {
        Ok(Async::Ready(try_ready!(self.underlying.poll()).map(|x| {
            let x = Rc::new(x);
            self.sink.unbounded_send(Ok(Rc::clone(&x))).unwrap();
            x
        })))
    }
}
impl<S: Stream> Drop for ValueSink<S> {
    fn drop(&mut self) {
        loop {
            match self.poll() {
                Ok(Async::Ready(None)) => break,
                Ok(Async::Ready(Some(_))) => (),
                Ok(Async::NotReady) => if !std::thread::panicking() { panic!("we lost"); },
                Err(e) => drop(self.sink.unbounded_send(Err(e))),
            }
        }
    }
}