#![allow(unused)]

use tokio_epoll_uring::IoBufMut;

use crate::virtual_file::{IoBufferMut, PageWriteGuardBuf};

pub trait IoBufAlignedMut: IoBufMut {}

impl IoBufAlignedMut for IoBufferMut {}

impl IoBufAlignedMut for PageWriteGuardBuf {}
