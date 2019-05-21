use std::fmt::Debug;
use std::cmp::Ordering;
use futures::{Stream, Poll, try_ready, Async, stream};
use named_type::NamedType;
use named_type_derive::*;

use super::Join;
use crate::definition::OrdJoinDefinition;

#[derive(NamedType)]
pub struct OrderedMergeJoin<L: Stream, R: Stream, D> {
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
                        //println!("entering replay mode with {:?}", self.eq_buffer);
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

