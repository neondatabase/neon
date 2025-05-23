use hmac::digest::consts::U32;
use hmac::digest::generic_array::GenericArray;
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub(crate) struct Pbkdf2 {
    hmac: Hmac<Sha256>,
    prev: GenericArray<u8, U32>,
    hi: GenericArray<u8, U32>,
    iterations: u32,
}

pub type Pbkdf2Output = [u8; 32];

// inspired from <https://github.com/neondatabase/rust-postgres/blob/20031d7a9ee1addeae6e0968e3899ae6bf01cee2/postgres-protocol/src/authentication/sasl.rs#L36-L61>
impl Pbkdf2 {
    pub(crate) fn start(str: &[u8], salt: &[u8], iterations: u32) -> Self {
        // key the HMAC and derive the first block in-place
        let mut hmac =
            Hmac::<Sha256>::new_from_slice(str).expect("HMAC is able to accept all key sizes");
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

    pub(crate) fn turn(&mut self) -> std::task::Poll<Pbkdf2Output> {
        let Self {
            hmac,
            prev,
            hi,
            iterations,
        } = self;

        // only do up to 4096 iterations per turn for fairness
        let n = (*iterations).clamp(0, 4096);
        for _ in 0..n {
            hmac.update(prev);
            let block = hmac.finalize_reset().into_bytes();

            for (hi_byte, &b) in hi.iter_mut().zip(block.iter()) {
                *hi_byte ^= b;
            }

            *prev = block;
        }

        *iterations -= n;
        if *iterations == 0 {
            std::task::Poll::Ready((*hi).into())
        } else {
            std::task::Poll::Pending
        }
    }
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
        let hash = loop {
            let std::task::Poll::Ready(hash) = job.turn() else {
                continue;
            };
            break hash;
        };

        let expected = pbkdf2_hmac_array::<Sha256, 32>(pass, salt, 60000);
        assert_eq!(hash, expected);
    }
}
