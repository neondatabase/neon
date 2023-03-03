

pub trait Storage {
    fn flush_pos(&self) -> u32;
    fn flush(&mut self) -> Result<()>;
    fn write(&mut self, )
}