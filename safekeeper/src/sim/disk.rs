use anyhow::Result;

pub trait Storage<T> {
    fn flush_pos(&self) -> u32;
    fn flush(&mut self) -> Result<()>;
    fn write(&mut self, t: T);
}
