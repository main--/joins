use std::borrow::Borrow;
use std::convert::Infallible;
use futures::Stream;
use crate::{IntoIterReady, IterReady, IterSource};

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
mod simple_anti_hash;
pub use self::simple_anti_hash::SimpleAntiHashJoin;
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
pub trait JoinInMemory<Left, Right, Definition, Config>
where
    Left: IntoIterator,
    Left::IntoIter: Clone,
    Right: IntoIterator,
    Right::IntoIter: Clone,
    Left::Item: Borrow<Definition::Left>,
    Right::Item: Borrow<Definition::Right>,
    Definition: JoinPredicate,
    Self: Join<IterSource<Left::IntoIter>, IterSource<Right::IntoIter>, Definition, (), Config>,
    Self: Stream<Error = Infallible>,
    Self: Sized,
{
    fn build_in_memory(left: Left, right: Right, definition: Definition, config: Config,) -> IterReady<Self>{
        Self::build(IterSource::new(left), IterSource::new(right), definition, (), config).iter_ready()
    }
}
impl<J, Left, Right, Definition, Config> JoinInMemory<Left, Right, Definition, Config> for J
where
    J: Join<IterSource<Left::IntoIter>, IterSource<Right::IntoIter>, Definition, (), Config>,
    J: Stream<Error = Infallible>,
    Left: IntoIterator,
    Left::IntoIter: Clone,
    Right: IntoIterator,
    Right::IntoIter: Clone,
    Left::Item: Borrow<Definition::Left>,
    Right::Item: Borrow<Definition::Right>,
    Definition: JoinPredicate
{}

pub trait ExternalStorage<T> {
    type External: External<T>;
    fn store(&mut self, tuples: Vec<T>) -> Self::External;
}
pub trait External<T> {
    type Iter: Iterator<Item=T>;
    fn fetch(&self) -> Self::Iter;
}

