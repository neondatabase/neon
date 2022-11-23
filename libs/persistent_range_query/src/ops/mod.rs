pub mod rsq;

#[derive(Copy, Clone, Debug)]
pub struct SameElementsInitializer<T> {
    initial_element_value: T,
}

impl<T> SameElementsInitializer<T> {
    pub fn new(initial_element_value: T) -> Self {
        SameElementsInitializer {
            initial_element_value,
        }
    }
}
