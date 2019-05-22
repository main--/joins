use futures::Stream;

mod nested_loop;
pub use self::nested_loop::NestedLoopJoin;
mod ordered_merge;
pub use self::ordered_merge::OrderedMergeJoin;
mod sort_merge;
pub use self::sort_merge::SortMergeJoin;
mod simple_hash;
pub use self::simple_hash::SimpleHashJoin;
mod symmetric_hash;
pub use self::symmetric_hash::SymmetricHashJoin;


use crate::predicate::JoinPredicate;

pub trait Rescan: Stream {
    fn rescan(&mut self);
}

pub trait Join<Left, Right, Definition, ExtStorage>: Stream<Item=Definition::Output, Error=Left::Error>
    where Left: Stream,
          Right: Stream<Error=Left::Error>,
          Definition: JoinPredicate<Left=Left::Item, Right=Right::Item> {
    fn build(
        left: Left,
        right: Right,
        definition: Definition,
        storage: ExtStorage,
        main_memory: usize) -> Self;
}

