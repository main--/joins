use std::usize;
use itertools::{Itertools, MinMaxResult};
use either::Either;

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
        let minmax = memory_tuples.iter().enumerate()
            .filter(|(_, &PartitionStats { left, right })| (left + right) > 0)
            .minmax_by_key(|(_, &PartitionStats { left, right })| left + right);
        match minmax {
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement((i, _)) | MinMaxResult::MinMax((i, _), _) => i,
        }
    }
}
pub struct FlushLargest;
impl FlushingPolicy for FlushLargest {
    fn flush(&mut self, memory_tuples: &[PartitionStats]) -> usize {
        //println!("eviction? {:?}", memory_tuples);
        match memory_tuples.iter().enumerate().minmax_by_key(|(_, &PartitionStats { left, right })| left + right) {
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement((i, _)) | MinMaxResult::MinMax(_, (i, _)) => i,
        }
    }
}
pub struct Adaptive {
    /// Smallest acceptable size for a certain bucket to be flushed.
    pub a: usize,
    /// Memory balancing. If |A| and |B| are the ratios of tuples from A and B, respectively,
    /// to all memory tuples, then we say that the memory is balanced only if `abs(|A| - |B|) < b`.
    pub b: f32,
}
impl Adaptive {
    fn is_balanced<'a, I: IntoIterator<Item=&'a PartitionStats>>(&self, all: usize, iter: I) -> bool {
        let (left, right) = sum_tuples(iter);
        // let all = (left + right) as f32;
        // Note: This turns out to be incorrect. The paper appears to assume that for the
        // "does removing this ruin balance?"-check the ratios are calculated relative to the
        // (old) total number - not the new total once the bucket has been removed.
        let all = all as f32;
        let ratio_left = (left as f32) / all;
        let ratio_right = (right as f32) / all;

        (ratio_left - ratio_right).abs() < self.b
    }
}
fn sum_tuples<'a, I: IntoIterator<Item=&'a PartitionStats>>(iter: I) -> (usize, usize) {
    iter.into_iter().fold((0, 0), |(l, r), &PartitionStats { left, right }| (l + left, r + right))
}
fn try_filter<I: Iterator + Clone, P: for<'a> Fn(&'a I::Item) -> bool>(iterator: I, predicate: P) -> impl Iterator<Item=I::Item> + Clone where I::Item: Clone, P: Clone {
    let mut peek = iterator.clone().filter(predicate).peekable();
    if peek.peek().is_some() {
        Either::Left(peek)
    } else {
        Either::Right(iterator)
    }
}
impl FlushingPolicy for Adaptive {
    fn flush(&mut self, memory_tuples: &[PartitionStats]) -> usize {
        let (left, right) = sum_tuples(memory_tuples);
        let all = left + right;

        let candidates = if self.is_balanced(all, memory_tuples) {
            // memory is balanced
            let acceptable = try_filter(memory_tuples.iter().enumerate(), |(_, &PartitionStats { left, right })| (left >= self.a) && (right >= self.a));
            Either::Left(try_filter(acceptable, |&(i, _)| self.is_balanced(all, memory_tuples.iter().take(i).chain(memory_tuples.iter().skip(i + 1)))))
        } else {
            let candidates = memory_tuples.iter().enumerate()
                // not sure why the paper does it exactly like /this/ but this is what they propose
                .filter(move |&(_, x)| if left >= right { x.left >= x.right } else { x.right >= x.left });
            let acceptable = try_filter(candidates, |(_, &PartitionStats { left, right })| (left >= self.a) && (right >= self.a));
            Either::Right(acceptable)
        };
        match candidates.minmax_by_key(|(_, &PartitionStats { left, right })| left + right) {
            MinMaxResult::NoElements => unreachable!(),
            MinMaxResult::OneElement((i, _)) | MinMaxResult::MinMax(_, (i, _)) => i,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn example() {
        // example from the paper:
        let layout = vec![
            PartitionStats { left: 4, right: 12 },
            PartitionStats { left: 11, right: 13 },
            PartitionStats { left: 13, right: 10 },
            PartitionStats { left: 6, right: 4 },
            PartitionStats { left: 25, right: 2 },
        ];
        assert_eq!(1, Adaptive { a: 10, b: 0.25 }.flush(&layout));
        assert_eq!(2, Adaptive { a: 10, b: 0.10 }.flush(&layout));
        assert_eq!(4, Adaptive { a: 1, b: 0.10 }.flush(&layout));
    }
}

