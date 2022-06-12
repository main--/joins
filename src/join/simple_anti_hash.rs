use std::vec;
use futures::{Stream, Poll, Async, try_ready, stream};
use multimap::MultiMap;
use named_type::NamedType;
use named_type_derive::*;
use crate::OuterJoinPredicate;

use super::{Join, Rescan};
use crate::predicate::HashPredicate;

enum State<T> {
    Join(MultiMap<u64, T>),
    Drain(MultiMap<u64, T>, vec::IntoIter<T>),
}

#[derive(NamedType)]
pub struct SimpleHashAntiJoin<L: Stream, R: Stream, D: HashPredicate> {
    definition: D,
    left: stream::Fuse<L>,
    right: R,
    state: State<L::Item>,
    table_entries: usize,
    memory_limit: usize,
}
impl<L, R, D> Stream for SimpleHashAntiJoin<L, R, D>
where
    L: Stream,
    R: Stream<Error=L::Error> + Rescan,
    D: HashPredicate + OuterJoinPredicate<Left=L::Item, Right=R::Item>
{
    type Item = L::Item;
    type Error = L::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let table = match &mut self.state {
                State::Drain(map, iter) => {
                    if let Some(item) = iter.next() {
                        return Ok(Async::Ready(Some(item)));
                    }
                    if let Some(&key) = map.keys().next() {
                        *iter = map.remove(&key).unwrap().into_iter();
                        continue;
                    }
                    if self.left.is_done() {
                        return Ok(Async::Ready(None));
                    }
                    assert!(map.is_empty());
                    // reuse already allocated map
                    let map = std::mem::replace(map, MultiMap::new());
                    self.state = State::Join(map);
                    self.table_entries = 0;
                    continue;
                }
                State::Join(table) => table,
            };

            if (self.table_entries < self.memory_limit) && !self.left.is_done() {
                // build phase
                if let Some(left) = try_ready!(self.left.poll()) {
                    table.insert(self.definition.hash_left(&left), left);
                    self.table_entries += 1;
                }
            } else if let Some(right) = try_ready!(self.right.poll()) {
                // probe phase
                let definition = &self.definition;
                let hash = definition.hash_right(&right);
                let vec = match table.get_vec_mut(&hash) {
                    Some(vec) => vec,
                    None => continue,
                };
                if let Some(pos) = vec.iter().position(|left| definition.eq(left, &right)) {
                    vec.swap_remove(pos);
                };
            } else {
                // probe phase complete, return to drain phase
                self.right.rescan();
                let map = std::mem::replace(table, MultiMap::new());
                self.state = State::Drain(map, Vec::new().into_iter());
            }
        }
    }
}
impl<L, R, D, E> Join<L, R, D, E, usize> for SimpleHashAntiJoin<L, R, D>
    where L: Stream,
          R: Stream<Error=L::Error> + Rescan,
          D: HashPredicate + OuterJoinPredicate<Left=L::Item, Right=R::Item> {
    fn build(left: L, right: R, definition: D, _: E, main_memory: usize) -> Self {
        SimpleHashAntiJoin {
            definition,
            left: left.fuse(),
            right,
            state: State::Join(MultiMap::new()),
            table_entries: 0,
            memory_limit: main_memory,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{EquiJoin, JoinInMemory, SimpleHashAntiJoin};

    #[test]
    fn simple_anti_hash() {
        let join = SimpleHashAntiJoin::build_in_memory(
            0..60,
            0..50,
            EquiJoin::new(|&l: &i32| l, |&r: &i32| r),
            usize::MAX,
        );
        let mut results: Vec<_> = join.collect();
        results.sort_unstable();
        assert_eq!(vec![50, 51, 52, 53, 54, 55, 56, 57, 58, 59], results);
    }
}
