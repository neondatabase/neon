///
/// Rust implementation of Postgres pg_checksum_page
/// See: https://github.com/postgres/postgres/blob/88210542106de5b26fe6aa088d1811b68502d224/src/include/storage/checksum_impl.h
/// for additional comments.
///
/// This is not a direct port of pg_checksum_page from Postgres, though.
/// For example, in the current state it can only produce a valid result
/// on the little-endian platform and with the standard 8 KB page size.
///

const BLCKSZ: usize = 8192;
const N_SUMS: usize = 32;
// Prime multiplier of FNV-1a hash
const FNV_PRIME: u32 = 16777619;

// Base offsets to initialize each of the parallel FNV hashes into a
// different initial state.
const CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
    0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
    0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E, 0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
];

// Calculate one round of the checksum.
fn checksum_comp(checksum: u32, value: u32) -> u32 {
    let tmp = checksum ^ value;
    tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17)
}

/// Compute the checksum for a Postgres page.
///
/// The page must be adequately aligned (at least on a 4-byte boundary).
///
/// The checksum includes the block number (to detect the case where a page is
/// somehow moved to a different location), the page header (excluding the
/// checksum itself), and the page data.
///
/// As in C implementation in Postgres, the checksum attribute on the page is
/// excluded from the calculation and preserved.
///
/// NB: after doing any modifications run `cargo bench`. The baseline on the more
/// or less recent Intel laptop is around 700ns. If it's significantly higher,
/// then it's worth looking into.
///
/// # Arguments
/// * `data` - the page to checksum
/// * `blkno` - the block number of the page
///
/// # Safety
/// This function is safe to call only if:
/// * `data` is strictly a standard 8 KB Postgres page
/// * it's called on the little-endian platform
pub unsafe fn pg_checksum_page(data: &[u8], blkno: u32) -> u16 {
    let page = std::mem::transmute::<&[u8], &[u32]>(data);
    let mut checksum: u32 = 0;
    let mut sums = CHECKSUM_BASE_OFFSETS;

    // Calculate the checksum of the first 'row' of the page. Do it separately as
    // we do an expensive comparison here, which is not required for the rest of the
    // page. Putting it into the main loop slows it down ~3 times.
    for (j, sum) in sums.iter_mut().enumerate().take(N_SUMS) {
        // Third 32-bit chunk of the page contains the checksum in the lower half
        // (assuming we are on little-endian machine), which we need to zero out.
        // See also `PageHeaderData` for reference.
        let chunk: u32 = if j == 2 {
            page[j] & 0xFFFF_0000
        } else {
            page[j]
        };

        *sum = checksum_comp(*sum, chunk);
    }

    // Main checksum calculation loop
    for i in 1..(BLCKSZ / (4 * N_SUMS)) {
        for (j, sum) in sums.iter_mut().enumerate().take(N_SUMS) {
            *sum = checksum_comp(*sum, page[i * N_SUMS + j]);
        }
    }

    // Finally, add in two rounds of zeroes for additional mixing
    for _i in 0..2 {
        for s in sums.iter_mut().take(N_SUMS) {
            *s = checksum_comp(*s, 0);
        }
    }

    // Xor fold partial checksums together
    for sum in sums {
        checksum ^= sum;
    }

    // Mix in the block number to detect transposed pages
    checksum ^= blkno;

    // Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
    // one. That avoids checksums of zero, which seems like a good idea.
    ((checksum % 65535) + 1) as u16
}

#[cfg(test)]
mod tests {
    use super::{pg_checksum_page, BLCKSZ};

    #[test]
    fn page_with_and_without_checksum() {
        // Create a page with some content and without a correct checksum.
        let mut page: [u8; BLCKSZ] = [0; BLCKSZ];
        for (i, byte) in page.iter_mut().enumerate().take(BLCKSZ) {
            *byte = i as u8;
        }

        // Calculate the checksum.
        let checksum = unsafe { pg_checksum_page(&page[..], 0) };

        // Zero the checksum attribute on the page.
        page[8..10].copy_from_slice(&[0u8; 2]);

        // Calculate the checksum again, should be the same.
        let new_checksum = unsafe { pg_checksum_page(&page[..], 0) };
        assert_eq!(checksum, new_checksum);

        // Set the correct checksum into the page.
        page[8..10].copy_from_slice(&checksum.to_le_bytes());

        // Calculate the checksum again, should be the same.
        let new_checksum = unsafe { pg_checksum_page(&page[..], 0) };
        assert_eq!(checksum, new_checksum);

        // Check that we protect from the page transposition, i.e. page is the
        // same, but in the wrong place.
        let wrong_blockno_checksum = unsafe { pg_checksum_page(&page[..], 1) };
        assert_ne!(checksum, wrong_blockno_checksum);
    }
}
