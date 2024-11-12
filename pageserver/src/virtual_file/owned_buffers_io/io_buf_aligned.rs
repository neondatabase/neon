use tokio_epoll_uring::{IoBuf, IoBufMut};

use crate::virtual_file::{IoBuffer, IoBufferMut, PageWriteGuardBuf};

pub trait IoBufAlignedMut: IoBufMut {}

pub trait IoBufAligned: IoBuf {}

impl IoBufAlignedMut for IoBufferMut {}

impl IoBufAligned for IoBuffer {}

impl IoBufAlignedMut for PageWriteGuardBuf {}
