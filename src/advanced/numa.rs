//! NUMA support for memory mapping.
//!
//! This module provides functionality for NUMA-aware memory mapping.

/// Check if NUMA is supported on the current system.
#[inline]
pub fn is_supported() -> bool {
    #[cfg(target_os = "linux")]
    {
        // Check if NUMA is available on Linux
        std::path::Path::new("/sys/devices/system/node/node0").exists()
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        // NUMA is primarily a Linux feature in our implementation
        false
    }
}

/// Get the number of NUMA nodes on the current system.
#[inline]
pub fn node_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        // Count NUMA nodes on Linux
        let mut count = 0;
        while std::path::Path::new(&format!("/sys/devices/system/node/node{}", count)).exists() {
            count += 1;
        }
        count
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        // NUMA is primarily a Linux feature in our implementation
        1
    }
}

/// Get the NUMA node for the current thread.
#[inline]
pub fn current_node() -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        // This is a simplified implementation
        // In a real-world scenario, you would use libnuma or direct syscalls
        Some(0)
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        // NUMA is primarily a Linux feature in our implementation
        None
    }
}

/// Get the preferred NUMA node for the current thread.
#[inline]
pub fn preferred_node() -> Option<u32> {
    current_node()
}