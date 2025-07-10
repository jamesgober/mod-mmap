//! Platform-specific memory mapping implementations.
//!
//! This module provides platform-specific implementations of memory mapping
//! operations for Linux, macOS, and Windows.

use std::fs::File;

use crate::error::Result;
use crate::advanced::{HugePageSize, NumaPolicy};

/// Memory access advice for the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Advice {
    /// Normal access pattern.
    Normal,
    
    /// Random access pattern.
    Random,
    
    /// Sequential access pattern.
    Sequential,
    
    /// Will need soon.
    WillNeed,
    
    /// Don't need anymore.
    DontNeed,
    
    /// Access data only once.
    SequentialOnce,  // Changed from Sequential_once
    
    /// Access data in random order once.
    RandomOnce,      // Changed from Random_once
    
    /// Do not make pages eligible for reclamation.
    Free,
}

// Re-export platform-specific implementations
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use self::linux::*;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use self::macos::*;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use self::windows::*;

// Provide a default implementation for unsupported platforms
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
mod unsupported;
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub use self::unsupported::*;

// Platform-agnostic functions that delegate to platform-specific implementations

/// Map a file into memory.
///
/// # Safety
///
/// This function is unsafe because it creates a memory map that can be accessed
/// and potentially modified, which might lead to undefined behavior if not used
/// correctly.
pub unsafe fn map_file(
    file: &File,
    offset: u64,
    len: usize,
    readable: bool,
    writable: bool,
    executable: bool,
    huge_pages: Option<HugePageSize>,
    numa_policy: Option<NumaPolicy>,
    stack: bool,
    copy_on_write: bool,
    populate: bool,
    alignment: Option<usize>,
) -> Result<crate::mmap::MmapRaw> {
    #[cfg(target_os = "linux")]
    return linux::map_file(file, offset, len, readable, writable, executable, huge_pages, numa_policy, stack, copy_on_write, populate, alignment);
    
    #[cfg(target_os = "macos")]
    return macos::map_file(file, offset, len, readable, writable, executable, huge_pages, numa_policy, stack, copy_on_write, populate, alignment);
    
    #[cfg(windows)]
    return windows::map_file(file, offset, len, readable, writable, executable, huge_pages, numa_policy, stack, copy_on_write, populate, alignment);
    
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    return unsupported::map_file(file, offset, len, readable, writable, executable, huge_pages, numa_policy, stack, copy_on_write, populate, alignment);
}

/// Create an anonymous memory map.
///
/// # Safety
///
/// This function is unsafe because it creates a memory map that can be accessed
/// and modified, which might lead to undefined behavior if not used correctly.
pub unsafe fn map_anon(
    len: usize,
    readable: bool,
    writable: bool,
    executable: bool,
    huge_pages: Option<HugePageSize>,
    numa_policy: Option<NumaPolicy>,
    stack: bool,
    populate: bool,
    alignment: Option<usize>,
) -> Result<crate::mmap::MmapRaw> {
    #[cfg(target_os = "linux")]
    return linux::map_anon(len, readable, writable, executable, huge_pages, numa_policy, stack, populate, alignment);
    
    #[cfg(target_os = "macos")]
    return macos::map_anon(len, readable, writable, executable, huge_pages, numa_policy, stack, populate, alignment);
    
    #[cfg(windows)]
    return windows::map_anon(len, readable, writable, executable, huge_pages, numa_policy, stack, populate, alignment);
    
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    return unsupported::map_anon(len, readable, writable, executable, huge_pages, numa_policy, stack, populate, alignment);
}

/// Flush memory map changes to disk.
///
/// # Safety
///
/// This function is unsafe because it operates on raw memory.
pub unsafe fn flush(addr: *mut u8, len: usize, async_flush: bool) -> Result<()> {
    #[cfg(target_os = "linux")]
    return linux::flush(addr, len, async_flush);
    
    #[cfg(target_os = "macos")]
    return macos::flush(addr, len, async_flush);
    
    #[cfg(windows)]
    return windows::flush(addr, len, async_flush);
    
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    return unsupported::flush(addr, len, async_flush);
}

/// Unmap memory.
///
/// # Safety
///
/// This function is unsafe because it unmaps memory that might still be in use.
pub unsafe fn unmap(addr: *mut u8, len: usize) -> Result<()> {
    #[cfg(target_os = "linux")]
    return linux::unmap(addr, len);
    
    #[cfg(target_os = "macos")]
    return macos::unmap(addr, len);
    
    #[cfg(windows)]
    return windows::unmap(addr, len);
    
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    return unsupported::unmap(addr, len);
}

/// Advise the kernel about how the memory map will be accessed.
///
/// # Safety
///
/// This function is unsafe because it operates on raw memory.
pub unsafe fn advise(addr: *mut u8, len: usize, advice: Advice) -> Result<()> {
    #[cfg(target_os = "linux")]
    return linux::advise(addr, len, advice);
    
    #[cfg(target_os = "macos")]
    return macos::advise(addr, len, advice);
    
    #[cfg(windows)]
    return windows::advise(addr, len, advice);
    
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    return unsupported::advise(addr, len, advice);
}
