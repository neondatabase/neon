//! Dense metric vec

use measured::{
    FixedCardinalityLabel, LabelGroup,
    label::{LabelGroupSet, StaticLabelSet},
    metric::{
        MetricEncoding, MetricFamilyEncoding, MetricType, group::Encoding, name::MetricNameEncoder,
    },
};

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
