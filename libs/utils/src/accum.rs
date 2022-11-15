/// A helper to "accumulate" a value similar to `Iterator::reduce`, but lets you
/// feed the accumulated values by calling the 'accum' function, instead of having an
/// iterator.
///
/// For example, to calculate the smallest value among some integers:
///
/// ```
/// use utils::accum::Accum;
///
/// let values = [1, 2, 3];
///
/// let mut min_value: Accum<u32> = Accum(None);
/// for new_value in &values {
///     min_value.accum(std::cmp::min, *new_value);
/// }
///
/// assert_eq!(min_value.0.unwrap(), 1);
/// ```
pub struct Accum<T>(pub Option<T>);
impl<T: Copy> Accum<T> {
    pub fn accum<F>(&mut self, func: F, new_value: T)
    where
        F: FnOnce(T, T) -> T,
    {
        // If there is no previous value, just store the new value.
        // Otherwise call the function to decide which one to keep.
        self.0 = Some(if let Some(accum) = self.0 {
            func(accum, new_value)
        } else {
            new_value
        });
    }
}
