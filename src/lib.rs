//! # Mod-MMap
//!
//! `membase` is a high-performance memory mapping library designed for extreme efficiency.
//! It provides cross-platform memory mapping with advanced features like huge page support,
//! NUMA awareness, and SIMD-optimized operations.
//!
//! ## Features
//!
//! - Zero-copy memory mapping with minimal overhead
//! - Cross-platform support (Linux, macOS, Windows)
//! - Huge page support for improved TLB efficiency
//! - NUMA-aware memory allocation
//! - Prefetching optimizations for sequential access
//! - Thread-safe operations with minimal synchronization
//! - Comprehensive error handling with detailed diagnostics
//!
//! ## Example
//!
//! ```
//! use membase::{MmapOptions, Mmap};
//! use std::fs::File;
//! use std::io::Write;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a temporary file
//! let mut file = tempfile::tempfile()?;
//! 
//! // Write some data to the file
//! file.write_all(b"Hello, Mod-MMap!")?;
//! file.sync_all()?;
//!
//! // Create a memory map with default options
//! let map = unsafe { MmapOptions::new().map(&file)? };
//!
//! // Access the memory map
//! if map.len() >= 8 {
//!     let value = unsafe { *(map.as_ptr() as *const u64) };
//!     println!("First 8 bytes as u64: {}", value);
//! }
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod mmap;
pub mod platform;
pub mod advanced;
pub mod columnar;
pub mod utils;

pub use error::{Error, Result};
pub use mmap::{Mmap, MmapMut, MmapOptions, MmapRaw};
pub use advanced::{HugePageSize, NumaPolicy, PrefetchStrategy};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Feature detection at runtime
#[inline]
pub fn has_huge_page_support() -> bool {
    advanced::huge_pages::is_supported()
}

/// Check if NUMA is available on this system
#[inline]
pub fn has_numa_support() -> bool {
    advanced::numa::is_supported()
}

/// Check if SIMD acceleration is available
#[inline]
pub fn has_simd_support() -> bool {
    #[cfg(any(target_feature = "sse2", target_feature = "neon", target_feature = "simd128"))]
    {
        true
    }
    #[cfg(not(any(target_feature = "sse2", target_feature = "neon", target_feature = "simd128")))]
    {
        false
    }
}