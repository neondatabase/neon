/// Useful type for asserting that expected bytes match reporting the bytes more readable
/// array-syntax compatible hex bytes.
///
/// # Usage
///
/// ```
/// use utils::Hex;
///
/// let actual = serialize_something();
/// let expected = [0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64];
///
/// // the type implements PartialEq and on mismatch, both sides are printed in 16 wide multiline
/// // output suffixed with an array style length for easier comparisons.
/// assert_eq!(Hex(&actual), Hex(&expected));
///
/// // with `let expected = [0x68];` the error would had been:
/// // assertion `left == right` failed
/// //  left: [0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64; 11]
/// // right: [0x68; 1]
/// # fn serialize_something() -> Vec<u8> { "hello world".as_bytes().to_vec() }
/// ```
pub struct Hex<S>(pub S);

impl<S: AsRef<[u8]>> std::fmt::Debug for Hex<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let chunks = self.0.as_ref().chunks(16);
        for (i, c) in chunks.enumerate() {
            if i > 0 && !c.is_empty() {
                writeln!(f, ", ")?;
            }
            for (j, b) in c.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "0x{b:02x}")?;
            }
        }
        write!(f, "; {}]", self.0.as_ref().len())
    }
}

impl<R: AsRef<[u8]>, L: AsRef<[u8]>> PartialEq<Hex<R>> for Hex<L> {
    fn eq(&self, other: &Hex<R>) -> bool {
        let left = self.0.as_ref();
        let right = other.0.as_ref();

        left == right
    }
}
