#![allow(unused)]

use tokio_epoll_uring::{IoBuf, IoBufMut};

use crate::virtual_file::{IoBufferMut, PageWriteGuardBuf};

pub trait IoBufAligned: IoBuf {}

pub trait IoBufAlignedMut: IoBufMut {}

impl IoBufAligned for IoBufferMut {}

impl IoBufAlignedMut for IoBufferMut {}

impl IoBufAlignedMut for PageWriteGuardBuf {}
