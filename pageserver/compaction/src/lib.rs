// The main module implementing the compaction algorithm
pub mod compact_tiered;
pub(crate) mod identify_levels;

// Traits that the caller of the compaction needs to implement
pub mod interface;

// Utility functions, useful for the implementation
pub mod helpers;

// A simulator with mock implementations of 'interface'
pub mod simulator;
