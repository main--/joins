use futures::Stream;

mod ordered_merge;
mod sort_merge;
mod simple_hash;
mod symmetric_hash;

pub use self::ordered_merge::OrderedMergeJoin;
pub use self::sort_merge::SortMergeJoin;
pub use self::simple_hash::SimpleHashJoin;
pub use self::symmetric_hash::SymmetricHashJoin;

use crate::predicate::JoinPredicate;

pub trait Join<Left, Right, Definition, ExtStorage>: Stream<Item=Definition::Output, Error=Left::Error>
    where Left: Stream,
          Right: Stream<Error=Left::Error>,
          Definition: JoinPredicate<Left=Left::Item, Right=Right::Item> {
    fn build(left: Left, right: Right, definition: Definition, storage: ExtStorage) -> Self;
}

