use std::borrow::Borrow;
use futures::Stream;

mod nested_loop;
pub use self::nested_loop::NestedLoopJoin;
mod block_nested_loop;
pub use self::block_nested_loop::BlockNestedLoopJoin;
mod ordered_merge;
pub use self::ordered_merge::OrderedMergeJoin;
mod sort_merge;
pub use self::sort_merge::SortMergeJoin;
mod simple_hash;
pub use self::simple_hash::SimpleHashJoin;
mod symmetric_hash;
pub use self::symmetric_hash::SymmetricHashJoin;
mod progressive_merge;
pub use self::progressive_merge::ProgressiveMergeJoin;
mod xjoin;
pub use self::xjoin::XJoin;
pub mod hash_merge;
pub use self::hash_merge::HashMergeJoin;


use crate::predicate::JoinPredicate;

pub trait Rescan: Stream {
    fn rescan(&mut self);
}

pub trait Join<Left, Right, Definition, ExtStorage, Config>: Stream<Item=Definition::Output>
    where Left: Stream,
          Right: Stream<Error=Left::Error>,
          Left::Item: Borrow<Definition::Left>,
          Right::Item: Borrow<Definition::Right>,
          Definition: JoinPredicate {
    fn build(
        left: Left,
        right: Right,
        definition: Definition,
        storage: ExtStorage,
        config: Config) -> Self;
}

pub trait ExternalStorage<T> {
    type External: External<T>;
    fn store(&mut self, tuples: Vec<T>) -> Self::External;
}
pub trait External<T> {
    type Iter: Iterator<Item=T>;
    fn fetch(&self) -> Self::Iter;
}
