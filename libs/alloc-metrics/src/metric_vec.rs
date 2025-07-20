//! Dense metric vec

use std::marker::PhantomData;

use measured::{
    FixedCardinalityLabel, LabelGroup,
    label::{LabelGroupSet, StaticLabelSet},
    metric::{
        MetricEncoding, MetricFamilyEncoding, MetricType, counter::CounterState, group::Encoding,
        name::MetricNameEncoder,
    },
};
use metrics::{CounterPairAssoc, MeasuredCounterPairState};

pub struct DenseMetricVec<M: MetricType, L: FixedCardinalityLabel + LabelGroup> {
    metrics: VecInner<M>,
    metadata: M::Metadata,
    label_set: StaticLabelSet<L>,
}

enum VecInner<M: MetricType> {
    Dense(Box<[M]>),
}

fn new_dense<M: MetricType>(c: usize) -> Box<[M]> {
    let mut vec = Vec::with_capacity(c);
    vec.resize_with(c, M::default);
    vec.into_boxed_slice()
}

impl<M: MetricType, L: FixedCardinalityLabel + LabelGroup> DenseMetricVec<M, L>
where
    M::Metadata: Default,
{
    /// Create a new metric vec with the given label set and metric metadata
    pub fn new() -> Self {
        Self::with_metadata(<M::Metadata>::default())
    }
}

impl<M: MetricType, L: FixedCardinalityLabel + LabelGroup> Default for DenseMetricVec<M, L>
where
    M::Metadata: Default,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<M: MetricType> VecInner<M> {
    fn get_metric(&self, id: usize) -> &M {
        match self {
            VecInner::Dense(metrics) => &metrics[id],
        }
    }

    fn get_metric_mut(&mut self, id: usize) -> &mut M {
        match self {
            VecInner::Dense(metrics) => &mut metrics[id],
        }
    }
}

impl<M: MetricType, L: FixedCardinalityLabel + LabelGroup> DenseMetricVec<M, L> {
    /// Create a new metric vec with the given label set and metric metadata
    pub fn with_metadata(metadata: M::Metadata) -> Self {
        let metrics = VecInner::Dense(new_dense(L::cardinality()));

        Self {
            metrics,
            metadata,
            label_set: StaticLabelSet::new(),
        }
    }

    // /// View the metric metadata
    // pub fn metadata(&self) -> &M::Metadata {
    //     &self.metadata
    // }

    /// Get an identifier for the specific metric identified by this label group
    ///
    /// # Panics
    /// Panics if the label group is not contained within the label set.
    pub fn with_labels(&self, label: L) -> usize {
        self.try_with_labels(label)
            .expect("label group was not contained within this label set")
    }

    /// Get an identifier for the specific metric identified by this label group
    ///
    /// # Errors
    /// Returns None if the label group is not contained within the label set.
    pub fn try_with_labels(&self, label: L) -> Option<usize> {
        self.label_set.encode(label)
    }

    /// Get the individual metric at the given identifier.
    ///
    /// # Panics
    /// Can panic or cause strange behaviour if the label ID comes from a different metric family.
    pub fn get_metric(&self, id: usize) -> &M {
        self.metrics.get_metric(id)
    }

    /// Get the individual metric at the given identifier.
    ///
    /// # Panics
    /// Can panic or cause strange behaviour if the label ID comes from a different metric family.
    pub fn get_metric_mut(&mut self, id: usize) -> &mut M {
        self.metrics.get_metric_mut(id)
    }
}

impl<M: MetricEncoding<T>, L: FixedCardinalityLabel + LabelGroup, T: Encoding>
    MetricFamilyEncoding<T> for DenseMetricVec<M, L>
{
    fn collect_family_into(&self, name: impl MetricNameEncoder, enc: &mut T) -> Result<(), T::Err> {
        M::write_type(&name, enc)?;
        match &self.metrics {
            VecInner::Dense(m) => {
                for (index, value) in m.iter().enumerate() {
                    value.collect_into(
                        &self.metadata,
                        self.label_set.decode_dense(index),
                        &name,
                        enc,
                    )?;
                }
            }
        }
        Ok(())
    }
}

pub struct DenseCounterPairVec<
    A: CounterPairAssoc<LabelGroupSet = StaticLabelSet<L>>,
    L: FixedCardinalityLabel + LabelGroup,
> {
    pub vec: DenseMetricVec<MeasuredCounterPairState, L>,
    pub _marker: PhantomData<A>,
}

impl<A: CounterPairAssoc<LabelGroupSet = StaticLabelSet<L>>, L: FixedCardinalityLabel + LabelGroup>
    Default for DenseCounterPairVec<A, L>
{
    fn default() -> Self {
        Self {
            vec: DenseMetricVec::new(),
            _marker: PhantomData,
        }
    }
}

// impl<A: CounterPairAssoc<LabelGroupSet = StaticLabelSet<L>>, L: FixedCardinalityLabel + LabelGroup>
//     DenseCounterPairVec<A, L>
// {
//     #[inline]
//     pub fn inc(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>) {
//         let id = self.vec.with_labels(labels);
//         self.vec.get_metric(id).inc.inc();
//     }

//     #[inline]
//     pub fn dec(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>) {
//         let id = self.vec.with_labels(labels);
//         self.vec.get_metric(id).dec.inc();
//     }

//     #[inline]
//     pub fn inc_by(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>, x: u64) {
//         let id = self.vec.with_labels(labels);
//         self.vec.get_metric(id).inc.inc_by(x);
//     }

//     #[inline]
//     pub fn dec_by(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>, x: u64) {
//         let id = self.vec.with_labels(labels);
//         self.vec.get_metric(id).dec.inc_by(x);
//     }
// }

impl<T, A, L> ::measured::metric::group::MetricGroup<T> for DenseCounterPairVec<A, L>
where
    T: ::measured::metric::group::Encoding,
    ::measured::metric::counter::CounterState: ::measured::metric::MetricEncoding<T>,
    A: CounterPairAssoc<LabelGroupSet = StaticLabelSet<L>>,
    L: FixedCardinalityLabel + LabelGroup,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), T::Err> {
        // write decrement first to avoid a race condition where inc - dec < 0
        T::write_help(enc, A::DEC_NAME, A::DEC_HELP)?;
        self.vec
            .collect_family_into(A::DEC_NAME, &mut Dec(&mut *enc))?;

        T::write_help(enc, A::INC_NAME, A::INC_HELP)?;
        self.vec
            .collect_family_into(A::INC_NAME, &mut Inc(&mut *enc))?;

        Ok(())
    }
}

/// [`MetricEncoding`] for [`MeasuredCounterPairState`] that only writes the inc counter to the inner encoder.
struct Inc<T>(T);
/// [`MetricEncoding`] for [`MeasuredCounterPairState`] that only writes the dec counter to the inner encoder.
struct Dec<T>(T);

impl<T: Encoding> Encoding for Inc<T> {
    type Err = T::Err;

    fn write_help(&mut self, name: impl MetricNameEncoder, help: &str) -> Result<(), Self::Err> {
        self.0.write_help(name, help)
    }
}

impl<T: Encoding> MetricEncoding<Inc<T>> for MeasuredCounterPairState
where
    CounterState: MetricEncoding<T>,
{
    fn write_type(name: impl MetricNameEncoder, enc: &mut Inc<T>) -> Result<(), T::Err> {
        CounterState::write_type(name, &mut enc.0)
    }
    fn collect_into(
        &self,
        metadata: &(),
        labels: impl LabelGroup,
        name: impl MetricNameEncoder,
        enc: &mut Inc<T>,
    ) -> Result<(), T::Err> {
        self.inc.collect_into(metadata, labels, name, &mut enc.0)
    }
}

impl<T: Encoding> Encoding for Dec<T> {
    type Err = T::Err;

    fn write_help(&mut self, name: impl MetricNameEncoder, help: &str) -> Result<(), Self::Err> {
        self.0.write_help(name, help)
    }
}

/// Write the dec counter to the encoder
impl<T: Encoding> MetricEncoding<Dec<T>> for MeasuredCounterPairState
where
    CounterState: MetricEncoding<T>,
{
    fn write_type(name: impl MetricNameEncoder, enc: &mut Dec<T>) -> Result<(), T::Err> {
        CounterState::write_type(name, &mut enc.0)
    }
    fn collect_into(
        &self,
        metadata: &(),
        labels: impl LabelGroup,
        name: impl MetricNameEncoder,
        enc: &mut Dec<T>,
    ) -> Result<(), T::Err> {
        self.dec.collect_into(metadata, labels, name, &mut enc.0)
    }
}
