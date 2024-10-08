#![allow(unused)]

use tokio_epoll_uring::IoBufMut;

use crate::virtual_file::IoBufferMut;

pub(crate) trait IoBufAlignedMut: IoBufMut {}

impl IoBufAlignedMut for IoBufferMut {}
