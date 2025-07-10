//! Advanced memory mapping features.
//!
//! This module provides advanced features for memory mapping, such as huge page
//! support, NUMA awareness, and prefetching optimizations.

pub mod huge_pages;
pub mod numa;
pub mod prefetch;

/// Huge page sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HugePageSize {
    /// 2MB huge pages.
    TwoMB,
    
    /// 1GB huge pages.
    OneGB,
}

/// NUMA policy for memory allocation.
#[derive(Debug, Clone, Copy)] 
pub enum NumaPolicy {
    /// Interleave memory across the specified NUMA nodes.
    Interleave([u32; 4], usize),
    
    /// Bind memory to the specified NUMA node.
    Bind(u32),
    
    /// Prefer allocating memory on the specified NUMA node.
    Preferred(u32),
}

/// Prefetching strategy for memory access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchStrategy {
    /// No prefetching.
    None,
    
    /// Sequential prefetching.
    Sequential,
    
    /// Random prefetching.
    Random,
    
    /// Custom prefetching with specified lookahead.
    Custom(usize),
}



/// Check if huge pages are supported on the current system.
#[inline]
pub fn has_huge_page_support() -> bool {
    huge_pages::is_supported()
}

/// Check if NUMA is available on this system.
#[inline]
pub fn has_numa_support() -> bool {
    numa::is_supported()
}

pub use prefetch::apply_strategy;
