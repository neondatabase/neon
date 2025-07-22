//! Dense metric vec

use measured::{
    FixedCardinalityLabel, LabelGroup,
    label::StaticLabelSet,
    metric::{
        MetricEncoding, MetricFamilyEncoding, MetricType, group::Encoding, name::MetricNameEncoder,
    },
};

pub struct DenseMetricVec<M: MetricType, L: FixedCardinalityLabel + LabelGroup> {
    metrics: Box<[M]>,
    metadata: M::Metadata,
    _label_set: StaticLabelSet<L>,
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

impl<M: MetricType, L: FixedCardinalityLabel + LabelGroup> DenseMetricVec<M, L> {
    /// Create a new metric vec with the given label set and metric metadata
    pub fn with_metadata(metadata: M::Metadata) -> Self {
        Self {
            metrics: new_dense(L::cardinality()),
            metadata,
            _label_set: StaticLabelSet::new(),
        }
    }

    /// Get the individual metric at the given identifier.
    ///
    /// # Panics
    /// Can panic or cause strange behaviour if the label ID comes from a different metric family.
    pub fn get_metric(&self, label: L) -> &M {
        // safety: The caller has guarantees that the label encoding is valid.
        unsafe { self.metrics.get_unchecked(label.encode()) }
    }

    /// Get the individual metric at the given identifier.
    ///
    /// # Panics
    /// Can panic or cause strange behaviour if the label ID comes from a different metric family.
    pub fn get_metric_mut(&mut self, label: L) -> &mut M {
        // safety: The caller has guarantees that the label encoding is valid.
        unsafe { self.metrics.get_unchecked_mut(label.encode()) }
    }
}

impl<M: MetricEncoding<T>, L: FixedCardinalityLabel + LabelGroup, T: Encoding>
    MetricFamilyEncoding<T> for DenseMetricVec<M, L>
{
    fn collect_family_into(&self, name: impl MetricNameEncoder, enc: &mut T) -> Result<(), T::Err> {
        M::write_type(&name, enc)?;
        for (index, value) in self.metrics.iter().enumerate() {
            value.collect_into(&self.metadata, L::decode(index), &name, enc)?;
        }
        Ok(())
    }
}
