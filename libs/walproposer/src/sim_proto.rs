use safekeeper::simlib::proto::{AnyMessage, ReplCell};
use std::cell::RefCell;

thread_local! {
    pub static MESSAGE_BUF: RefCell<AnyMessage> = RefCell::new(AnyMessage::None);
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
    Accept,
    Closed,
    Message,
}

#[repr(u8)]
/// List of all possible AnyMessage.
pub enum AnyMessageTag {
    None,
    InternalConnect,
    Just32,
    ReplCell,
}
