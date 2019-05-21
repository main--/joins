use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::cmp::Ordering;
use std::marker::PhantomData;

pub trait JoinDefinition {
    type Left;
    type Right;
    type Output;

    fn eq(&self, left: &Self::Left, right: &Self::Right) -> Option<Self::Output>;
}
pub trait OrdJoinDefinition: JoinDefinition {
    fn cmp(&self, left: &Self::Left, right: &Self::Right) -> Option<Ordering>;
    fn cmp_left(&self, a: &Self::Left, b: &Self::Left) -> Ordering;
    fn cmp_right(&self, a: &Self::Right, b: &Self::Right) -> Ordering;
}
pub trait HashJoinDefinition: JoinDefinition {
    fn hash_left(&self, x: &Self::Left) -> u64;
    fn hash_right(&self, x: &Self::Right) -> u64;
}
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
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> JoinDefinition for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
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
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> OrdJoinDefinition for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
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
impl<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight> HashJoinDefinition for EquiJoin<Left, Right, KeyLeft, KeyRight, GetKeyLeft, GetKeyRight>
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
