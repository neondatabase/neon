use safekeeper::simlib::proto::{AnyMessage, ReplCell};
use std::cell::RefCell;

pub(crate) fn anymessage_tag(msg: &AnyMessage) -> AnyMessageTag {
    match msg {
        AnyMessage::None => AnyMessageTag::None,
        AnyMessage::InternalConnect => AnyMessageTag::InternalConnect,
        AnyMessage::Just32(_) => AnyMessageTag::Just32,
        AnyMessage::ReplCell(_) => AnyMessageTag::ReplCell,
        AnyMessage::Bytes(_) => AnyMessageTag::Bytes,
        AnyMessage::LSN(_) => AnyMessageTag::LSN,
    }
}

thread_local! {
    pub static MESSAGE_BUF: RefCell<AnyMessage> = RefCell::new(AnyMessage::None);
}

#[no_mangle]
/// Get tag of the current message.
pub extern "C" fn sim_msg_tag() -> AnyMessageTag {
    MESSAGE_BUF.with(|cell| anymessage_tag(&*cell.borrow()))
}

#[no_mangle]
/// Read AnyMessage::Just32 message.
pub extern "C" fn sim_msg_get_just_u32(val: &mut u32) {
    MESSAGE_BUF.with(|cell| match &*cell.borrow() {
        AnyMessage::Just32(v) => {
            *val = *v;
        }
        _ => panic!("expected Just32 message"),
    });
}

#[no_mangle]
/// Read AnyMessage::LSN message.
pub extern "C" fn sim_msg_get_lsn(val: &mut u64) {
    MESSAGE_BUF.with(|cell| match &*cell.borrow() {
        AnyMessage::LSN(v) => {
            *val = *v;
        }
        _ => panic!("expected LSN message"),
    });
}

#[no_mangle]
/// Write AnyMessage::ReplCell message.
pub extern "C" fn sim_msg_set_repl_cell(value: u32, client_id: u32, seqno: u32) {
    MESSAGE_BUF.with(|cell| {
        *cell.borrow_mut() = AnyMessage::ReplCell(ReplCell {
            value,
            client_id,
            seqno,
        });
    });
}

#[no_mangle]
/// Write AnyMessage::Bytes message.
pub extern "C" fn sim_msg_set_bytes(bytes: *const u8, len: usize) {
    MESSAGE_BUF.with(|cell| {
        // copy bytes to a Rust Vec
        let mut v = Vec::with_capacity(len);
        unsafe {
            v.set_len(len);
            std::ptr::copy_nonoverlapping(bytes, v.as_mut_ptr(), len);
        }
        *cell.borrow_mut() = AnyMessage::Bytes(v.into());
    });
}

#[no_mangle]
/// Read AnyMessage::Bytes message.
pub extern "C" fn sim_msg_get_bytes(len: *mut usize) -> *const u8 {
    MESSAGE_BUF.with(|cell| match &*cell.borrow() {
        AnyMessage::Bytes(v) => {
            unsafe {
                *len = v.len();
                v.as_ptr()
            }
        }
        _ => panic!("expected Bytes message"),
    })
}

#[repr(C)]
/// Event returned by epoll_recv.
pub struct Event {
    pub tag: EventTag,
    pub tcp: i64,
    pub any_message: AnyMessageTag,
}

#[repr(u8)]
/// List of all possible NodeEvent.
pub enum EventTag {
    Timeout,
    Accept,
    Closed,
    Message,
    Internal,
}

#[repr(u8)]
/// List of all possible AnyMessage.
pub enum AnyMessageTag {
    None,
    InternalConnect,
    Just32,
    ReplCell,
    Bytes,
    LSN,
}
