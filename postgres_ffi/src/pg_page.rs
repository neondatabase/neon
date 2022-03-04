use crate::pg_constants;
use crate::{ItemIdData, PageHeaderData};
use bytes::BytesMut;
use zenith_utils::lsn::Lsn;

fn page_get_max_offset_number(pg: &PageHeaderData) -> u16 {
    if pg.pd_lower as usize <= pg_constants::SIZEOF_PAGE_HEADER_DATA {
        0
    } else {
        (pg.pd_lower as usize - pg_constants::SIZEOF_PAGE_HEADER_DATA) as u16
            / pg_constants::ITEM_ID_DATA_SIZE
    }
}

fn offset_number_is_valid(offnum: u16) -> bool {
    offnum != pg_constants::INVALID_OFFSET_NUMBER && offnum < pg_constants::MAX_OFFSET_NUMBER
}

pub fn max_align(x: usize) -> usize {
    (x + 7) & !7
}

pub fn item_id_set_normal(item_id: &mut ItemIdData, offs: u16, len: usize) {
    item_id.set_lp_off(offs as u32);
    item_id.set_lp_flags(pg_constants::LP_NORMAL);
    item_id.set_lp_len(len as u32);
}

pub fn add_item(page: &mut BytesMut, offnum: u16, rec: &[u8], is_heap: bool) {
    let pg = unsafe { std::mem::transmute::<*mut u8, &mut PageHeaderData>(page[..].as_mut_ptr()) };
    assert!(
        pg.pd_lower as usize >= pg_constants::SIZEOF_PAGE_HEADER_DATA
            && pg.pd_lower <= pg.pd_upper
            && pg.pd_upper <= pg.pd_special
            && pg.pd_special <= pg_constants::BLCKSZ
    );
    assert!(offset_number_is_valid(offnum));

    let size = rec.len();
    let limit = 1 + page_get_max_offset_number(&pg);
    assert!(offnum <= limit);
    assert!(!is_heap || offnum as usize <= pg_constants::MAX_HEAP_TUPLES_PER_PAGE);

    if offnum < limit {
        let items = unsafe { pg.pd_linp.as_slice(offnum as usize) };
        let item_id = &items[offnum as usize - 1];
        assert!(item_id.lp_flags() == pg_constants::LP_UNUSED && item_id.lp_len() == 0);
    }

    let aligned_size = max_align(size) as u16;
    let lower = if offnum == limit {
        pg.pd_lower + pg_constants::ITEM_ID_DATA_SIZE as u16
    } else {
        pg.pd_lower
    };
    let upper = pg.pd_upper - aligned_size;
    assert!(lower <= upper);

	/* set the line pointer */
    let items = unsafe { pg.pd_linp.as_mut_slice(offnum as usize) };
    let item_id = &mut items[offnum as usize - 1];
    item_id_set_normal(item_id, upper, size);

	/* copy the item's data onto the page */
    let dst = upper as usize;
    page[dst..dst + size].copy_from_slice(&rec);

	/* adjust page header */
    pg.pd_lower = lower;
    pg.pd_upper = upper;
}

pub fn init(page: &mut BytesMut, lsn: Lsn, special_size: u16) {
    let mut image = [0u8; pg_constants::BLCKSZ as usize];
    let mut pg =
        unsafe { std::mem::transmute::<*mut u8, &mut PageHeaderData>(&mut image as *mut u8) };
    pg.pd_lsn.xlogid = (lsn.0 >> 32) as u32;
    pg.pd_lsn.xrecoff = lsn.0 as u32;
    pg.pd_flags = 0;
    pg.pd_lower = pg_constants::SIZEOF_PAGE_HEADER_DATA as u16;
    pg.pd_upper = pg_constants::BLCKSZ as u16 - special_size;
    pg.pd_special = pg_constants::BLCKSZ as u16 - special_size;
    pg.pd_pagesize_version = pg_constants::BLCKSZ | pg_constants::PG_PAGE_LAYOUT_VERSION;
    page.extend_from_slice(&image);
}

pub fn set_flags(page: &mut BytesMut, flags: u16) {
    let pg = unsafe { std::mem::transmute::<*mut u8, &mut PageHeaderData>(page[..].as_mut_ptr()) };
    pg.pd_flags |= flags;
}

pub fn clear_flags(page: &mut BytesMut, flags: u16) {
    let pg = unsafe { std::mem::transmute::<*mut u8, &mut PageHeaderData>(page[..].as_mut_ptr()) };
    pg.pd_flags &= !flags;
}

pub fn set_lsn(page: &mut BytesMut, lsn: Lsn) {
    let pg = unsafe { std::mem::transmute::<*mut u8, &mut PageHeaderData>(page[..].as_mut_ptr()) };
    pg.pd_lsn.xlogid = (lsn.0 >> 32) as u32;
    pg.pd_lsn.xrecoff = lsn.0 as u32;
}
