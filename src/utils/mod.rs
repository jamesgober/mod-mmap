//! Utility functions for memory mapping operations.
//!
//! This module provides various utility functions for working with memory maps.

pub mod alignment;
pub mod metrics;
pub mod concurrency;

pub use alignment::{align_up, align_down, is_aligned, get_alignment, page_size, cache_line_size};
pub use metrics::{MemoryStats, record_operation, get_stats};
pub use concurrency::{RwLock, AtomicPtr, fence};
