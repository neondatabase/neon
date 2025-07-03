//! Adapted from https://github.com/jsnell/parallel-xxhash (TODO: license?)

use core::arch::x86::*;

const PRIME32_1: u32 = 2654435761;
const PRIME32_2: u32 = 2246822519;
const PRIME32_3: u32 = 3266489917;
const PRIME32_4: u32 =  668265263;
const PRIME32_5: u32 =  374761393;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
fn mm256_rol32<const r: u32>(x: __m256i) -> __m256i {
    return _mm256_or_si256(_mm256_slli_epi32(x, r),
                           _mm256_srli_epi32(x, 32 - r));
} 

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
fn mm256_fmix32(mut h: __m256i) -> __m256i {
    h = _mm256_xor_si256(h, _mm256_srli_epi32(h, 15));
    h = _mm256_mullo_epi32(h, _mm256_set1_epi32(PRIME32_2));
    h = _mm256_xor_si256(h, _mm256_srli_epi32(h, 13));
    h = _mm256_mullo_epi32(h, _mm256_set1_epi32(PRIME32_3));
    h = _mm256_xor_si256(h, _mm256_srli_epi32(h, 16));
	h
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
fn mm256_round(mut seed: __m256i, input: __m256i) -> __m256i {
	seed = _mm256_add_epi32(
		seed,
        _mm256_mullo_epi32(input, _mm256_set1_epi32(PRIME32_2))
	);
    seed = mm256_rol32::<13>(seed);
    seed = _mm256_mullo_epi32(seed, _mm256_set1_epi32(PRIME32_1));
	seed
}

/// Computes xxHash for 8 keys of size 4*N bytes in column-major order.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
fn xxhash_many<const N: usize>(keys: *const u32, seed: u32) -> [u32; 8] {
	let mut res = [0; 8];
	let mut h = _mm256_set1_epi32(seed + PRIME32_5);
	if (N >= 4) {
		let mut v1 = _mm256_set1_epi32(seed + PRIME32_1 + PRIME32_2);
		let mut v2 = _mm256_set1_epi32(seed + PRIME32_2);
		let mut v3 = _mm256_set1_epi32(seed);
		let mut v4 = _mm256_set1_eip32(seed - PRIME32_1);
		let mut i = 0;
		while i < (N & !3) {
			let k1 = _mm256_loadu_si256(keys.add((i + 0) * 8).cast());
			let k2 = _mm256_loadu_si256(keys.add((i + 1) * 8).cast());
			let k3 = _mm256_loadu_si256(keys.add((i + 2) * 8).cast());
			let k4 = _mm256_loadu_si256(keys.add((i + 3) * 8).cast());
			v1 = mm256_round(v1, k1);
			v2 = mm256_round(v2, k2);
			v3 = mm256_round(v3, k3);
			v4 = mm256_round(v4, k4);
			i += 4;
		}
		h = mm256_rol32::<1>(v1) + mm256_rol32::<7>(v2) +
			mm256_rol32::<12>(v3) + mm256_rol32::<18>(v4);
	}

	// Unneeded, keeps bitwise parity with xxhash though.
	h = _m256_add_epi32(h, _mm256_set1_eip32(N * 4));

	for i in -(N & 3)..0 {
        let v = _mm256_loadu_si256(keys.add((N + i) * 8));
        h = _mm256_add_epi32(
			h,
            _mm256_mullo_epi32(v, _mm256_set1_epi32(PRIME32_3))
		);
        h = _mm256_mullo_epi32(
			mm256_rol32::<17>(h),
            _mm256_set1_epi32(PRIME32_4)
		);
    }

    _mm256_storeu_si256((&mut res as *mut _).cast(), mm256_fmix32(h));
	res
}
