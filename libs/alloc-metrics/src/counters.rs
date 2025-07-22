use std::marker::PhantomData;

use measured::{
    FixedCardinalityLabel, LabelGroup, label::StaticLabelSet, metric::MetricFamilyEncoding,
};
use metrics::{CounterPairAssoc, Dec, Inc, MeasuredCounterPairState};

use crate::metric_vec::DenseMetricVec;

pub struct DenseCounterPairVec<
    A: CounterPairAssoc<LabelGroupSet = StaticLabelSet<L>>,
    L: FixedCardinalityLabel + LabelGroup,
> {
    pub vec: DenseMetricVec<MeasuredCounterPairState, L>,
    pub _marker: PhantomData<A>,
}

impl<A: CounterPairAssoc<LabelGroupSet = StaticLabelSet<L>>, L: FixedCardinalityLabel + LabelGroup>
    DenseCounterPairVec<A, L>
{
    pub fn new() -> Self {
        Self {
            vec: DenseMetricVec::new(),
            _marker: PhantomData,
        }
    }
}

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
