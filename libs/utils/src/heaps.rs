use std::ops::{Deref, DerefMut};


mod skew_binomial_queue {
    use std::mem::swap;

    struct NodeData<'a, T: TreeElement> {
        rank: Rank,
        first_child: Option<&'a mut T>,
        next_sibling: Option<&'a mut T>,
    }
    
    impl<'a, T> NodeData<'a, T> where T: TreeElement {
        fn rank_mut(&mut self) -> &mut Rank {
            return &mut self.rank;
        }

        fn rank(&self) -> &Rank {
            return &self.rank;
        }

        fn next_sibling(&self) -> Option<&'a T> {
            return self.next_sibling.map(|it| &it);
        }

        fn first_child(&self) -> Option<&'a T> {
            return self.first_child.map(|it| &it);
        }

        fn next_sibling_mut(&mut self) -> Option<&mut Self> {
            return self.next_sibling.as_deref_mut();
        }

        fn first_child_mut(&mut self) -> Option<&mut Self> {
            return self.first_child.as_deref_mut();
        }
    }

    impl Default for NodeData<T>
        where T: TreeElement {
        fn default() -> Self {
            return  Self {
                rank: 0,
                first_child: None,
                next_sibling: None,
            }
        }
    }

    trait TreeElement : Ord {
        fn node(&self) -> &NodeData<Self>;
        fn node_mut(&self) -> &mut NodeData<Self>;
    }

    trait SkewBinQueue: TreeElement {
        #[inline]
        fn ins(t: &mut Self, head: Option<&mut Self>) -> &mut Self;
        #[inline]
        fn skew_link(mut t0: &mut Self, mut t1: &mut Self, mut t2: &mut Self) -> &mut Self;
        fn link(mut t1: &mut Self, mut t2: &mut Self) -> &mut Self;
        fn pop_sibling(self: &mut Self) -> (&mut Self, Option<&mut Self>);
        fn after(self: &mut Self, front: &mut Self) -> &mut Self;
        fn push_child(&mut self, other: &mut Self);
        fn rank(&self) -> &Rank;
        fn rank_mut(&mut self) -> &mut Rank;
        fn uniqify(data: Option<&mut Self>) -> Option<&mut Self>;
        fn meld_uniq(mut t1s: Option<&mut Self>, mut t2s: Option<&mut Self>) -> Option<&mut Self>;
        fn siblings(self: &mut Self, tail: Option<&mut Self>) -> &mut Self;
        fn insert(queue: Option<&mut Self>, new_item: &mut Self) -> Option<&mut Self>;
        fn meld(left: Option<&mut Self>, right: Option<&mut Self>) -> Option<&mut Self>;
        fn find_min(queue: Option<&mut Self>) -> Option<&Self>;
        fn delete_min(queue: Option<&mut Self>) -> Option<(&mut Self, Option<&mut Self>)>;
    }

    impl<T> SkewBinQueue for T where T: TreeElement {
        fn rank_mut(&mut self) -> &mut Rank {
            return &mut self.node_mut().rank;
        }
        fn rank(&self) -> &Rank {
            return &self.node().rank;
        }

        fn push_child(&mut self, other: &mut Self) {
            let node = self.node_mut();
            let other_node = &mut other.node_mut();

            assert!(other_node.next_sibling.is_none());

            swap(&mut other_node.next_sibling, &mut node.first_child);

            let mut new_first_child = Some(other);

            swap(&mut node.first_child, &mut new_first_child);
        }

        fn after(self: &mut Self, after: &mut Self) -> &mut Self {
            let node = after.node_mut();
            assert!(node.next_sibling.is_none());
            node.next_sibling = Some(self);
            return after;
        }
        
        fn siblings(self: &mut Self, tail: Option<&mut Self>) -> &mut Self {
            let node = self.node_mut();
            assert!(node.next_sibling.is_none());
            node.next_sibling = tail;
            return self;
        }

        fn pop_sibling(self: &mut Self) -> (&mut Self, Option<&mut Self>) {
            let mut tail: Option<&mut Self> = None;
            swap(&mut tail, &mut self.node_mut().next_sibling);
            return (self, tail);
        }

        fn link(mut t1: &mut Self, mut t2: &mut Self) -> &mut Self {
            assert_eq!(t1.rank(), t2.rank());
            return if Ord::le(&t1, &t2) {
                t1.rank_mut() += 1;
                t1.push_child(t2);
                t1
            } else {
                t2.node_mut().rank() += 1;
                t2.push_child(t1);
                t2
            }
        }

        #[inline]
        fn skew_link(mut t0: &mut Self, mut t1: &mut Self, mut t2: &mut Self) -> &mut Self {
            return if Ord::le(&t1, &t0) && Ord::le(&t1, &t2)  {
                t1.rank_mut() += 1;
                t1.push_child(t2);
                t1.push_child(t0);
                t1
            } else if Ord::le(&t2, &t0) && Ord::le(&t2, &t1) {
                t2.rank_mut() += 1;
                t2.push_child(t1);
                t2.push_child(t0);
                t2
            } else {
                t0.rank_mut() += 1;
                t0.push_child(t2);
                t0.push_child(t1);
                t0
            }
        }

        #[inline]
        fn ins(t: &mut Self, head: Option<&mut Self>) -> &mut Self {
            assert!(t.node().next_sibling.is_none());
            return if let Some(mut tprime) = head {
                if t.rank() < tprime.rank() {
                    t.after(tprime)
                } else {
                    let (tprime, tail) = tprime.pop_sibling();

                    Self::ins(
                        Self::link(t, tprime),
                        tail
                    )
                }
            } else {
                t
            };
        }

        fn uniqify(data: Option<&mut Self>) -> Option<&mut Self> {
            return if let Some(item) = data {
                let (t,tail) = item.pop_sibling();
                Some(Self::ins(t, tail))
            } else {
                data
            }
        }

        fn meld_uniq(mut t1s: Option<&mut Self>, mut t2s: Option<&mut Self>) -> Option<&mut Self> {
            return match (t1s, t2s) {
                (None, some) => some,
                (some, None) => some,
                (Some(t1s), Some(t2s)) => {
                    let (t1, t1tail) = t1s.pop_sibling();
                    let (t2, t2tail) = t2s.pop_sibling();

                    if t1.rank() < t2.rank() {
                        Some(t1.siblings(
                            Self::meld_uniq(
                                t1tail,
                                Some(t2.siblings(t2tail))
                            )
                        ))
                    } else if t2.rank() < t1.rank() {
                        Some(t2.siblings(
                            Self::meld_uniq(
                                Some(t1.siblings(t1tail)),
                                t2tail
                            )
                        ))
                    } else {
                        Some(Self::ins(
                            Self::link(t1, t2),
                            Self::meld_uniq(t1tail, t2tail)
                        ))
                    }
                }
            }
        }

        fn insert(queue: Option<&mut Self>, new_item: &mut Self) -> Option<&mut Self> {
            if let Some(queue) = &queue {
                if let Some(sibling) = queue.node_mut().next_sibling {
                    if sibling.node_mut().next_sibling.is_some() {
                        let (sibling, s_tail) = queue.pop_sibling();
                        let (next_sibling, s_tail) = s_tail.unwrap().pop_sibling();
                        return Some(Self::skew_link(new_item, sibling, next_sibling)
                            .siblings(s_tail));
                    }
                }
            }
            return Some(new_item.siblings(queue));
        }

        fn meld(left: Option<&mut Self>, right: Option<&mut Self>) -> Option<&mut Self> {
            return Self::meld_uniq(
                Self::uniqify(left),
                Self::uniqify(right),
            )
        }

        fn find_min(queue: Option<&Self>) -> Option<&Self> {
            return match queue {
                None => None,
                Some(queue) => {
                    match Self::find_min(queue.node().next_sibling.as_deref()) {
                        None => Some(queue),
                        Some(x) => {
                            if queue < x {
                                Some(queue)
                            } else {
                                Some(x)
                            }
                        }
                    }
                }
            }
        }

        fn delete_min(queue: Option<&mut Self>) -> Option<(&mut Self, Option<&mut Self>)> {
            const GETMIN: fn(&Self) -> &Self = |node: &Self| {
                let mut min = node;
                let mut cursor = node;

                while let Some(candidate) = cursor.node().next_sibling {
                    if &candidate < min {
                        min = candidate;
                    }
                    cursor = candidate;
                }
                min
            };
            
            let SPLIT = |mut ts: Option<&mut Self>, mut xs: Option<&mut Self>, mut t_c: Option<&mut Self>| {
                
                while let Some(candidate) = t_c {
                    if candidate.rank() == 0 {
                        ts
                    }
                }
            }

            Self::insert(xs, )

            match queue {
                None => None,
                Some(head) => {
                }
            }
        }
    }

    struct Node<E: Ord> {
        elem: E,
        rank: Rank,
        list: Vec<Node<E>>,
    }

    type Tree<E: Ord> = Node<E>;

    type Rank = u32;
    
    impl Node<E> where E: Ord {
        fn root_mut(&mut self) -> &mut E {
            return &mut self.elem;
        }
        fn root(&self) -> &E {
            return &self.elem;
        }

        fn rank_mut(&mut self) -> &mut Rank {
            return &mut self.rank;
        }
        fn rank(&self) -> &Rank {
            return &self.rank;
        }
        
        fn link(mut t1: Self, mut t2: Self) -> Self {
            assert_eq!(t1.rank(), t2.rank());
            return if Ord::le(t1.elem, t2.elem) {
                *Self::rank_mut(&mut t1) += 1;
                t1.list.push(t2);
                t1
            } else {
                *Self::rank_mut(&mut t2) += 1;
                t2.list.push(t1);
                t2
            }
        }
        
        fn skew_link(mut t0: Self, mut t1: Self, mut t2: Self) -> Self {
            return if Ord::le(t1.elem, t0.elem) && Ord::le(t1.elem, t2.elem)  {
                *Self::rank_mut(&mut t1) += 1;
                t1.list.push(t2);
                t1.list.push(t0);
                t1
            } else if Ord::le(t2.elem, t0.elem) && Ord::le(t2.elem, t1.elem) {
                *Self::rank_mut(&mut t2) += 1;
                t2.list.push(t1);
                t2.list.push(t0);
                t2
            } else {
                *Self::rank_mut(&mut t0) += 1;
                t0.list = vec![t1, t2];
                t0
            }
        }
        
        fn ins(t: Self, mut vec: Vec<Self>) -> Vec<Self> {
            return if vec.is_empty() {
                vec.push(t);
                vec
            } else {
                /* at least one item in the Vec, so this is always safe */
                let tprime = vec.pop().unwrap();

                assert!(Self::rank(&t) <= Self::rank(&tprime));

                if rank(&t) < rank(&tprime) {
                    vec.push(tprime);
                    vec.push(t);
                    vec
                } else {
                    let linked = Self::link(t, tprime);
                    insert(it, vec);
                }
            }
        }
        
        fn uniqify(mut vec: Vec<Self>) -> Vec<Self> {
            return if vec.is_empty() {
                vec
            } else {
                let t = vec.pop().unwrap();
                Self::ins(t, vec);
            }
        }
        
        fn meld_uniq(mut t1s: Vec<Self>, mut t2s: Vec<Self>) -> Vec<Self> {
            return if t1s.is_empty() {
                t2s
            } else if t2s.is_empty() {
                t1s
            } else {
                let t1 = t1s.pop().unwrap();
                let t2 = t2s.pop().unwrap();
                
                if Self::rank(&t1) < Self::rank(&t2) {
                    t2s.push(t2);
                    t1s = Self::meld_uniq(t1s, t2s);
                    t1s.push(t1);
                    t1s
                } else if Self::rank(&t2) < Self::rank(&t1) {
                    t1s.push(t1);
                    t2s = Self::meld_uniq(t1s, t2s);
                    t2s.push(t2);
                    t2s
                } else {
                    Self::ins(
                        Self::link(t1, t2),
                        Self::meld_uniq(t1s, t2s)
                    )
                }
            }
        }
        
        fn empty() -> Vec<Self> {
            return Vec::new();
        }

        fn insert(e: E, mut vec: Vec<Self>) -> Vec<Self> {
            if vec.len() > 2 && vec[0].rank() == vec[1].rank() {
                let t1 = vec.pop().unwrap();
                let t2 = vec.pop().unwrap();
                vec.push(Self::skew_link(
                    Node {
                        elem: e,
                        rank: 0,
                        list: vec![],
                    },
                    t1,
                    t2,
                ));
            } else {
                vec.push(Self {
                    elem: e,
                    rank: 0,
                    list: vec![],
                });
            }
            return vec;
        }
        
        fn find_min(vec: &[Self]) -> Option<&E> {
            if vec.is_empty() {
                return None;
            }
            if vec.len() == 1 {
                return Some(&vec[0].elem)
            }

            let t = &vec[0];
            let rest = &vec[1..];
            let x: &E = Self::find_min(rest).unwrap();

            return if Ord::le(t.root(), x) {
                Some(t.root())
            } else {
                Some(x)
            }
        }
    }
}
