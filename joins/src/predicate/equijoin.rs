use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::cmp::Ordering;
use std::marker::PhantomData;

use super::*;

/// A generic equality join.
///
/// The `JoinPredicate` family of traits is extremely versatile. This versatility comes at
/// a cost however: implementing them for every single join operation by hand is clearly infeasible.
/// In practice, most joins are equi-joins and don't actually require all of this versatility.
/// That's what this type is for.
///
/// # Example
///
/// ```
/// use joins::predicate::EquiJoin;
/// #[derive(Clone, Debug)]
/// struct Left { a: i32, b: i32, c: i32 }
/// #[derive(Clone, Debug)]
/// struct Right { x: i32, y: i32, z: i32 }
///
/// EquiJoin::new(|x: &Left| (x.a, x.c), |x: &Right| (x.z, x.y * 2));
/// ```
///
/// This example shows the join predicate `Left.a = Right.z AND Left.c = Right.y * 2`.
#[derive(Clone, Copy)]
pub struct EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight {
    get_key_left: GetKeyLeft,
    get_key_right: GetKeyRight,

    left: PhantomData<fn(&Left)>,
    right: PhantomData<fn(&Right)>,
}

impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: PartialEq<KeyRight>,
      Left: Clone,
      Right: Clone {
    pub fn new(get_key_left: GetKeyLeft, get_key_right: GetKeyRight) -> Self {
        EquiJoin { get_key_left, get_key_right, left: PhantomData, right: PhantomData }
    }
}

impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> JoinPredicate for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: PartialEq<KeyRight>,
      Left: Clone,
      Right: Clone {
    type Left = Left;
    type Right = Right;
    type Output = (Left, Right);

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output> {
        if (self.get_key_left)(left) == (self.get_key_right)(right) {
            Some((left.clone(), right.clone()))
        } else {
            None
        }
    }
}

impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> MergePredicate for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: Ord + PartialOrd<KeyRight>,
      KeyRight: Ord,
      Left: Clone,
      Right: Clone {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering> {
        (self.get_key_left)(left).partial_cmp(&(self.get_key_right)(right))
    }
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering {
        (self.get_key_left)(a).cmp(&(self.get_key_left)(b))
    }
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering {
        (self.get_key_right)(a).cmp(&(self.get_key_right)(b))
    }
}

impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> HashPredicate for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight,
      KeyLeft: PartialEq<KeyRight> + Hash,
      KeyRight: Hash,
      Left: Clone,
      Right: Clone {
    fn hash_left(&self, x: &Self::Left) -> u64 {
        let mut hasher = DefaultHasher::new();
        (self.get_key_left)(x).hash(&mut hasher);
        hasher.finish()
    }
    fn hash_right(&self, x: &Self::Right) -> u64 {
        let mut hasher = DefaultHasher::new();
        (self.get_key_right)(x).hash(&mut hasher);
        hasher.finish()
    }
}
