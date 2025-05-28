//! For postgres password authentication, we need to perform a PBKDF2 using
//! PRF=HMAC-SHA2-256, producing only 1 block (32 bytes) of output key.

use hmac::digest::consts::U32;
use hmac::digest::generic_array::GenericArray;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use x509_cert::der::zeroize::Zeroize;

type Prf = Hmac<Sha256>;
pub(crate) type Block = GenericArray<u8, U32>;

pub(crate) struct Pbkdf2 {
    hmac: Hmac<Sha256>,
    /// U{r-1} for whatever iteration r we are currently on.
    prev: Block,
    /// the output of `fold(xor, U{1}..U{r})` for whatever iteration r we are currently on.
    hi: Block,
    /// number of iterations left
    iterations: u32,
}

impl Drop for Pbkdf2 {
    fn drop(&mut self) {
        self.prev.zeroize();
        self.hi.zeroize();
    }
}

// inspired from <https://github.com/neondatabase/rust-postgres/blob/20031d7a9ee1addeae6e0968e3899ae6bf01cee2/postgres-protocol/src/authentication/sasl.rs#L36-L61>
impl Pbkdf2 {
    pub(crate) fn start(pw: &[u8], salt: &[u8], iterations: u32) -> Self {
        // key the HMAC and derive the first block in-place
        let mut hmac = Prf::new_from_slice(pw).expect("HMAC is able to accept all key sizes");

        // U1 = PRF(Password, Salt + INT_32_BE(i))
        // i = 1 since we only need 1 block of output.
        hmac.update(salt);
        hmac.update(&1u32.to_be_bytes());
        let init_block = hmac.finalize_reset().into_bytes();

        Self {
            hmac,
            // one iteration spent above
            iterations: iterations - 1,
            hi: init_block,
            prev: init_block,
        }
    }

    pub(crate) fn cost(&self) -> u32 {
        (self.iterations).clamp(0, 4096)
    }

    /// For "fairness", we implement PBKDF2 with cooperative yielding, which is why we use this `turn`
    /// function that only executes a fixed number of iterations before continuing.
    pub(crate) fn turn(&mut self) -> std::task::Poll<Block> {
        let Self {
            hmac,
            prev,
            hi,
            iterations,
        } = self;

        // only do up to 4096 iterations per turn for fairness
        let n = (*iterations).clamp(0, 4096);
        for _ in 0..n {
            let next = single_round(hmac, prev);
            xor(hi, &next);
            *prev = next;
        }

        *iterations -= n;
        if *iterations == 0 {
            std::task::Poll::Ready(*hi)
        } else {
            std::task::Poll::Pending
        }
    }
}

#[inline(always)]
pub fn xor(x: &mut Block, y: &Block) {
    for (x, &y) in std::iter::zip(x, y) {
        *x ^= y;
    }
}

#[inline(always)]
fn single_round(prf: &mut Prf, ui: &Block) -> Block {
    // Ui = PRF(Password, Ui-1)
    prf.update(ui);
    prf.finalize_reset().into_bytes()
}

#[cfg(test)]
mod tests {
    use pbkdf2::pbkdf2_hmac_array;
    use sha2::Sha256;

    use super::Pbkdf2;

    #[test]
    fn works() {
        let salt = b"sodium chloride";
        let pass = b"Ne0n_!5_50_C007";

        let mut job = Pbkdf2::start(pass, salt, 60000);
        let hash: [u8; 32] = loop {
            let std::task::Poll::Ready(hash) = job.turn() else {
                continue;
            };
            break hash.into();
        };

        let expected = pbkdf2_hmac_array::<Sha256, 32>(pass, salt, 60000);
        assert_eq!(hash, expected);
    }
}
