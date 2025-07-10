//! Memory alignment utilities.
//!
//! This module provides utilities for working with memory alignment.

/// Round up to the next multiple of `align`.
///
/// # Arguments
///
/// * `value` - The value to round up.
/// * `align` - The alignment, which must be a power of two.
///
/// # Returns
///
/// The smallest multiple of `align` that is greater than or equal to `value`.
#[inline]
pub fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two(), "alignment must be a power of two");
    (value + align - 1) & !(align - 1)
}

/// Round down to the previous multiple of `align`.
///
/// # Arguments
///
/// * `value` - The value to round down.
/// * `align` - The alignment, which must be a power of two.
///
/// # Returns
///
/// The largest multiple of `align` that is less than or equal to `value`.
#[inline]
pub fn align_down(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two(), "alignment must be a power of two");
    value & !(align - 1)
}

/// Check if a value is aligned to a given alignment.
///
/// # Arguments
///
/// * `value` - The value to check.
/// * `align` - The alignment, which must be a power of two.
///
/// # Returns
///
/// `true` if `value` is a multiple of `align`, `false` otherwise.
#[inline]
pub fn is_aligned(value: usize, align: usize) -> bool {
    debug_assert!(align.is_power_of_two(), "alignment must be a power of two");
    (value & (align - 1)) == 0
}

/// Get the alignment of a pointer.
///
/// # Arguments
///
/// * `ptr` - The pointer to check.
///
/// # Returns
///
/// The largest power of two that divides the pointer address.
#[inline]
pub fn get_alignment(ptr: *const u8) -> usize {
    let addr = ptr as usize;
    addr & (!addr + 1)
}

/// Get the system page size.
#[inline]
pub fn page_size() -> usize {
    #[cfg(unix)]
    {
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    }
    
    #[cfg(windows)]
    {
        use winapi::um::sysinfoapi::{GetSystemInfo, SYSTEM_INFO};
        
        unsafe {
            let mut system_info: SYSTEM_INFO = std::mem::zeroed();
            GetSystemInfo(&mut system_info);
            system_info.dwPageSize as usize
        }
    }
    
    #[cfg(not(any(unix, windows)))]
    {
        // Default to 4KB for unknown platforms
        4096
    }
}

/// Get the cache line size.
#[inline]
pub fn cache_line_size() -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        // Try to get the cache line size from CPUID
        if let Some(size) = x86_64_cache_line_size() {
            return size;
        }
    }
    
    // Default to 64 bytes, which is common on most modern CPUs
    64
}

/// Get the cache line size on x86_64 using CPUID.
#[cfg(target_arch = "x86_64")]
fn x86_64_cache_line_size() -> Option<usize> {
    // This is a simplified implementation
    // In a real-world scenario, you would use CPUID to query the cache line size
    
    // For now, just return a common value
    Some(64)
}
