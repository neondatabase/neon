///
///  Port of Postgres pg_checksum_page
///

const BLCKSZ: usize = 8192;
const N_SUMS: usize = 32;
/* prime multiplier of FNV-1a hash */
const FNV_PRIME: u32 = 16777619;

/*
 * Base offsets to initialize each of the parallel FNV hashes into a
 * different initial state.
 */
const CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
    0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2, 0x83E13D2C,
    0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C, 0xF718902A, 0x0B1C9CDB,
    0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979, 0xCCA6C0B2, 0x304A0979, 0x85AA43D4,
    0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E, 0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
];

/*
 * Calculate one round of the checksum.
 */
fn checksum_comp(checksum: u32, value: u32) -> u32 {
    let tmp = checksum ^ value;
    tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17)
}

/*
 * Compute the checksum for a Postgres page.
 *
 * The page must be adequately aligned (at least on a 4-byte boundary).
 * Beware also that the checksum field of the page is transiently zeroed.
 *
 * The checksum includes the block number (to detect the case where a page is
 * somehow moved to a different location), the page header (excluding the
 * checksum itself), and the page data.
 */
pub fn pg_checksum_page(data: &[u8], blkno: u32) -> u16 {
    let page = unsafe { std::mem::transmute::<&[u8], &[u32]>(data) };
    let mut checksum: u32 = 0;
    let mut sums = CHECKSUM_BASE_OFFSETS;

    /* main checksum calculation */
    for i in 0..(BLCKSZ / (4 * N_SUMS)) {
        for j in 0..N_SUMS {
            sums[j] = checksum_comp(sums[j], page[i * N_SUMS + j]);
        }
    }
    /* finally add in two rounds of zeroes for additional mixing */
    for _i in 0..2 {
        for s in sums.iter_mut().take(N_SUMS) {
            *s = checksum_comp(*s, 0);
        }
    }

    /* xor fold partial checksums together */
    for sum in sums {
        checksum ^= sum;
    }

    /* Mix in the block number to detect transposed pages */
    checksum ^= blkno;

    /*
     * Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
     * one. That avoids checksums of zero, which seems like a good idea.
     */
    ((checksum % 65535) + 1) as u16
}
