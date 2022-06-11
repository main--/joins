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
/// It joins tuples by a **key** which is specified using closures.
/// It returns `(left, right)` for tuples with equal keys.
///
/// # Example
///
/// ```
/// use joins::EquiJoin;
/// #[derive(Clone, Debug)]
/// struct Left { a: i32, b: i32, c: i32 }
/// #[derive(Clone, Debug)]
/// struct Right { x: i32, y: i32, z: i32 }
///
/// EquiJoin::new(|l: &Left| (l.a, l.c), |r: &Right| (r.z, r.y * 2));
/// ```
///
/// This example shows the join predicate `Left.a = Right.z AND Left.c = Right.y * 2`.
/// Note that the type annotations can usually be omitted once you actually pass the `EquiJoin`
/// to a join implementation (assuming the input tuple types are unambiguous).
///
/// # Requirements
///
/// In order to use this type, your tuples need to implement the following traits
///
/// * `KeyLeft: PartialEq<KeyRight>`: this is the bare minimum so we can actually join tuples
/// * `KeyLeft: Clone, KeyRight: Clone`: this is required to produce output tuples since any given
///   input tuple may match several times
/// * `KeyLeft: Ord + PartialOrd<KeyRight>, KeyRight: Ord`: this is **optional** and enables the `MergePredicate` implementation for this join
/// * `KeyLeft: Hash, KeyRight: Hash`: this is **optional** and enables the `HashPredicate` implementation for this join
#[derive(Clone, Copy)]
pub struct EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
where GetKeyLeft: Fn(&Left) -> KeyLeft,
      GetKeyRight: Fn(&Right) -> KeyRight {
    get_key_left: GetKeyLeft,
    get_key_right: GetKeyRight,

    // why is this necessary ???
    left: PhantomData<fn(&Left) -> KeyLeft>,
    right: PhantomData<fn(&Right) -> KeyLeft>,
}

// TODO: support non-Clone by delegating Output generation to a trait parameter
//       (which defaults to a clone-dependent implementation that generates tuples)
// TODO: allow passing in a different hasher maybe?

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
