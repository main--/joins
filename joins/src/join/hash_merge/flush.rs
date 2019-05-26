use std::usize;
use itertools::{Itertools, MinMaxResult};

#[derive(Default, Clone, Debug)]
pub struct PartitionStats {
    pub left: usize,
    pub right: usize,
}

pub trait FlushingPolicy {
    fn flush(&mut self, memory_tuples: &[PartitionStats]) -> usize;
}

pub struct FlushSmallest;
impl FlushingPolicy for FlushSmallest {
    fn flush(&mut self, memory_tuples: &[PartitionStats]) -> usize {
        //println!("eviction? {:?}", memory_tuples);
        match memory_tuples.iter().enumerate().minmax_by_key(|(_, &PartitionStats { left, right })| if (left + right) == 0 { usize::MAX } else { left + right }) {
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement((i, _)) | MinMaxResult::MinMax((i, _), _) => i,
        }
    }
}
