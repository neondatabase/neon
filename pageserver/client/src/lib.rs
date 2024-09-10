pub mod mgmt_api;
pub mod page_service;

/// For timeline_block_unblock_gc, distinguish the two different operations. This could be a bool.
// If file structure is per-kind not per-feature then where to put this?
#[derive(Clone, Copy)]
pub enum BlockUnblock {
    Block,
    Unblock,
}

impl std::fmt::Display for BlockUnblock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BlockUnblock::Block => "block",
            BlockUnblock::Unblock => "unblock",
        };
        f.write_str(s)
    }
}
