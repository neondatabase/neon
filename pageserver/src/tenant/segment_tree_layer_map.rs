use persistent_range_query::naive::{IndexableKey, NaiveVecStorage};
use persistent_range_query::ops::SameElementsInitializer;
use persistent_range_query::segment_tree::{MidpointableKey, PersistentSegmentTree};
use persistent_range_query::{
    LazyRangeInitializer, PersistentVecStorage, RangeModification, RangeQueryResult,
    VecReadableVersion,
};
use std::cmp::Ordering;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PageIndex(u32);
type LayerId = String;

impl IndexableKey for PageIndex {
    fn index(all_keys: &Range<Self>, key: &Self) -> usize {
        (key.0 as usize) - (all_keys.start.0 as usize)
    }

    fn element_range(all_keys: &Range<Self>, index: usize) -> Range<Self> {
        PageIndex(all_keys.start.0 + index as u32)..PageIndex(all_keys.start.0 + index as u32 + 1)
    }
}

impl MidpointableKey for PageIndex {
    fn midpoint(range: &Range<Self>) -> Self {
        PageIndex(range.start.0 + (range.end.0 - range.start.0) / 2)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LayerMapInformation {
    // Only make sense for a range of length 1.
    last_layer: Option<LayerId>,
    last_image_layer: Option<LayerId>,
    // Work for all ranges
    max_delta_layers: (usize, Range<PageIndex>),
}

impl LayerMapInformation {
    fn last_layers(&self) -> (&Option<LayerId>, &Option<LayerId>) {
        (&self.last_layer, &self.last_image_layer)
    }

    fn max_delta_layers(&self) -> &(usize, Range<PageIndex>) {
        &self.max_delta_layers
    }
}

fn merge_ranges(left: &Range<PageIndex>, right: &Range<PageIndex>) -> Range<PageIndex> {
    if left.is_empty() {
        right.clone()
    } else if right.is_empty() {
        left.clone()
    } else if left.end == right.start {
        left.start..right.end
    } else {
        left.clone()
    }
}

impl RangeQueryResult<PageIndex> for LayerMapInformation {
    fn new_for_empty_range() -> Self {
        LayerMapInformation {
            last_layer: None,
            last_image_layer: None,
            max_delta_layers: (0, PageIndex(0)..PageIndex(0)),
        }
    }

    fn combine(
        left: &Self,
        _left_range: &Range<PageIndex>,
        right: &Self,
        _right_range: &Range<PageIndex>,
    ) -> Self {
        // Note that either range may be empty.
        LayerMapInformation {
            last_layer: left
                .last_layer
                .as_ref()
                .or_else(|| right.last_layer.as_ref())
                .cloned(),
            last_image_layer: left
                .last_image_layer
                .as_ref()
                .or_else(|| right.last_image_layer.as_ref())
                .cloned(),
            max_delta_layers: match left.max_delta_layers.0.cmp(&right.max_delta_layers.0) {
                Ordering::Less => right.max_delta_layers.clone(),
                Ordering::Greater => left.max_delta_layers.clone(),
                Ordering::Equal => (
                    left.max_delta_layers.0,
                    merge_ranges(&left.max_delta_layers.1, &right.max_delta_layers.1),
                ),
            },
        }
    }

    fn add(
        left: &mut Self,
        left_range: &Range<PageIndex>,
        right: &Self,
        right_range: &Range<PageIndex>,
    ) {
        *left = Self::combine(&left, left_range, right, right_range);
    }
}

#[derive(Clone, Debug)]
struct AddDeltaLayers {
    last_layer: LayerId,
    count: usize,
}

#[derive(Clone, Debug)]
struct LayerMapModification {
    add_image_layer: Option<LayerId>,
    add_delta_layers: Option<AddDeltaLayers>,
}

impl LayerMapModification {
    fn add_image_layer(layer: impl Into<LayerId>) -> Self {
        LayerMapModification {
            add_image_layer: Some(layer.into()),
            add_delta_layers: None,
        }
    }

    fn add_delta_layer(layer: impl Into<LayerId>) -> Self {
        LayerMapModification {
            add_image_layer: None,
            add_delta_layers: Some(AddDeltaLayers {
                last_layer: layer.into(),
                count: 1,
            }),
        }
    }
}

impl RangeModification<PageIndex> for LayerMapModification {
    type Result = LayerMapInformation;

    fn no_op() -> Self {
        LayerMapModification {
            add_image_layer: None,
            add_delta_layers: None,
        }
    }

    fn is_no_op(&self) -> bool {
        self.add_image_layer.is_none() && self.add_delta_layers.is_none()
    }

    fn is_reinitialization(&self) -> bool {
        self.add_image_layer.is_some()
    }

    fn apply(&self, result: &mut Self::Result, range: &Range<PageIndex>) {
        if let Some(layer) = &self.add_image_layer {
            result.last_layer = Some(layer.clone());
            result.last_image_layer = Some(layer.clone());
            result.max_delta_layers = (0, range.clone());
        }
        if let Some(AddDeltaLayers { last_layer, count }) = &self.add_delta_layers {
            result.last_layer = Some(last_layer.clone());
            result.max_delta_layers.0 += count;
        }
    }

    fn compose(later: &Self, earlier: &mut Self) {
        if later.add_image_layer.is_some() {
            *earlier = later.clone();
            return;
        }
        if let Some(AddDeltaLayers { last_layer, count }) = &later.add_delta_layers {
            let res = earlier.add_delta_layers.get_or_insert(AddDeltaLayers {
                last_layer: LayerId::default(),
                count: 0,
            });
            res.last_layer = last_layer.clone();
            res.count += count;
        }
    }
}

impl LazyRangeInitializer<LayerMapInformation, PageIndex> for SameElementsInitializer<()> {
    fn get(&self, range: &Range<PageIndex>) -> LayerMapInformation {
        LayerMapInformation {
            last_layer: None,
            last_image_layer: None,
            max_delta_layers: (0, range.clone()),
        }
    }
}

pub struct STLM {
    s: PersistentVecStorage<LayerMapModification, SameElementsInitializer<()>, PageIndex>,
}

impl STLM {
    pub fn new() -> Self {
        STLM {
            s: PersistentVecStorage::new(
                PageIndex(0)..PageIndex(100),
                SameElementsInitializer::new(()),
            ),
        }
    }

    pub fn insert(key_begin: i32, key_end: i32) {
        s.modify(
            &(PageIndex(key_begin)..PageIndex(key_end)),
            &LayerMapModification::add_image_layer("Img0..70"),
        );
    }
}

fn test_layer_map<
    S: PersistentVecStorage<LayerMapModification, SameElementsInitializer<()>, PageIndex>,
>() {
    let mut s = S::new(
        PageIndex(0)..PageIndex(100),
        SameElementsInitializer::new(()),
    );
    s.modify(
        &(PageIndex(0)..PageIndex(70)),
        &LayerMapModification::add_image_layer("Img0..70"),
    );
    s.modify(
        &(PageIndex(50)..PageIndex(100)),
        &LayerMapModification::add_image_layer("Img50..100"),
    );
    s.modify(
        &(PageIndex(10)..PageIndex(60)),
        &LayerMapModification::add_delta_layer("Delta10..60"),
    );
    let s_before_last_delta = s.freeze();
    s.modify(
        &(PageIndex(20)..PageIndex(80)),
        &LayerMapModification::add_delta_layer("Delta20..80"),
    );

    assert_eq!(
        s.get(&(PageIndex(5)..PageIndex(6))).last_layers(),
        (&Some("Img0..70".to_owned()), &Some("Img0..70".to_owned()))
    );
    assert_eq!(
        s.get(&(PageIndex(15)..PageIndex(16))).last_layers(),
        (
            &Some("Delta10..60".to_owned()),
            &Some("Img0..70".to_owned())
        )
    );
    assert_eq!(
        s.get(&(PageIndex(25)..PageIndex(26))).last_layers(),
        (
            &Some("Delta20..80".to_owned()),
            &Some("Img0..70".to_owned())
        )
    );
    assert_eq!(
        s.get(&(PageIndex(65)..PageIndex(66))).last_layers(),
        (
            &Some("Delta20..80".to_owned()),
            &Some("Img50..100".to_owned())
        )
    );
    assert_eq!(
        s.get(&(PageIndex(95)..PageIndex(96))).last_layers(),
        (
            &Some("Img50..100".to_owned()),
            &Some("Img50..100".to_owned())
        )
    );

    assert_eq!(
        s.get(&(PageIndex(0)..PageIndex(100))).max_delta_layers(),
        &(2, PageIndex(20)..PageIndex(60)),
    );
    assert_eq!(
        *s_before_last_delta
            .get(&(PageIndex(0)..PageIndex(100)))
            .max_delta_layers(),
        (1, PageIndex(10)..PageIndex(60)),
    );

    assert_eq!(
        *s.get(&(PageIndex(10)..PageIndex(30))).max_delta_layers(),
        (2, PageIndex(20)..PageIndex(30))
    );
    assert_eq!(
        *s.get(&(PageIndex(10)..PageIndex(20))).max_delta_layers(),
        (1, PageIndex(10)..PageIndex(20))
    );

    assert_eq!(
        *s.get(&(PageIndex(70)..PageIndex(80))).max_delta_layers(),
        (1, PageIndex(70)..PageIndex(80))
    );
    assert_eq!(
        *s_before_last_delta
            .get(&(PageIndex(70)..PageIndex(80)))
            .max_delta_layers(),
        (0, PageIndex(70)..PageIndex(80))
    );
}

#[test]
fn test_naive() {
    test_layer_map::<NaiveVecStorage<_, _, _>>();
}

#[test]
fn test_segment_tree() {
    test_layer_map::<PersistentSegmentTree<_, _, _>>();
}
