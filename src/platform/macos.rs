//! macOS-specific memory mapping implementation.
use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::ptr;

use libc::{
    c_void, mmap, munmap, msync, madvise, PROT_NONE, PROT_READ, PROT_WRITE, PROT_EXEC,
    MAP_SHARED, MAP_PRIVATE, MAP_ANON, MS_ASYNC, MS_SYNC,
    MADV_NORMAL, MADV_RANDOM, MADV_SEQUENTIAL, MADV_WILLNEED, MADV_DONTNEED, MADV_FREE,
};

use crate::error::{Error, Result};
use crate::mmap::MmapRaw;
use crate::advanced::{HugePageSize, NumaPolicy};
use crate::platform::Advice;

/// Map a file into memory on macOS.
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
    _huge_pages: Option<HugePageSize>,
    _numa_policy: Option<NumaPolicy>,
    _stack: bool,
    copy_on_write: bool,
    populate: bool,
    alignment: Option<usize>,
) -> Result<MmapRaw> {
    // Calculate protection flags
    let mut prot = PROT_NONE;
    if readable {
        prot |= PROT_READ;
    }
    if writable {
        prot |= PROT_WRITE;
    }
    if executable {
        prot |= PROT_EXEC;
    }

    // Calculate mapping flags
    let flags = if copy_on_write {
        MAP_PRIVATE
    } else {
        MAP_SHARED
    };

    // Note: macOS doesn't support MAP_STACK, MAP_POPULATE, or MAP_HUGETLB
    // We'll silently ignore these options on macOS

    // Calculate page-aligned offset
    let page_size = page_size();
    let aligned_offset = offset & !(page_size as u64 - 1);
    let offset_delta = offset - aligned_offset;
    let aligned_len = len + offset_delta as usize;

    // Apply custom alignment if requested
    let mut aligned_addr: *mut c_void = ptr::null_mut();
    if let Some(align) = alignment {
        if align > page_size {
            // For custom alignment, we'll allocate extra space and then adjust the pointer
            let extra = align - 1;
            let map_len = aligned_len + extra;
            
            aligned_addr = mmap(
                ptr::null_mut(),
                map_len,
                prot,
                flags,
                file.as_raw_fd(),
                aligned_offset as i64,
            );
            
            if aligned_addr == libc::MAP_FAILED {
                return Err(Error::Io(io::Error::last_os_error()));
            }
            
            // Calculate aligned address
            let addr_value = aligned_addr as usize;
            let aligned_value = (addr_value + extra) & !(align - 1);
            
            // Unmap the extra portions
            let prefix_size = aligned_value - addr_value;
            if prefix_size > 0 {
                munmap(aligned_addr, prefix_size);
            }
            
            let suffix_size = extra - prefix_size;
            if suffix_size > 0 {
                munmap(
                    (aligned_addr as usize + prefix_size + aligned_len) as *mut c_void,
                    suffix_size,
                );
            }
            
            aligned_addr = aligned_value as *mut c_void;
        }
    }

    // Perform the actual mapping
    let addr = if aligned_addr.is_null() {
        mmap(
            ptr::null_mut(),
            aligned_len,
            prot,
            flags,
            file.as_raw_fd(),
            aligned_offset as i64,
        )
    } else {
        // We already have an aligned address
        aligned_addr
    };

    if addr == libc::MAP_FAILED {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    // NUMA is not supported on macOS, so we ignore the numa_policy parameter

    // If populate is requested, we can simulate it by touching the pages
    if populate {
        // Touch each page to force it into memory
        for i in (0..aligned_len).step_by(page_size) {
            ptr::read_volatile((addr as usize + i) as *const u8);
        }
    }

    // Adjust pointer for the offset delta
    let ptr = (addr as usize + offset_delta as usize) as *mut u8;

    Ok(MmapRaw { ptr, len })
}

/// Create an anonymous memory map on macOS.
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
    _huge_pages: Option<HugePageSize>,
    _numa_policy: Option<NumaPolicy>,
    _stack: bool,
    populate: bool,
    alignment: Option<usize>,
) -> Result<MmapRaw> {
    // Calculate protection flags
    let mut prot = PROT_NONE;
    if readable {
        prot |= PROT_READ;
    }
    if writable {
        prot |= PROT_WRITE;
    }
    if executable {
        prot |= PROT_EXEC;
    }

    // Calculate mapping flags
    let flags = MAP_PRIVATE | MAP_ANON;

    // Note: macOS doesn't support MAP_STACK, MAP_POPULATE, or MAP_HUGETLB
    // We'll silently ignore these options on macOS

    // Apply custom alignment if requested
    let mut aligned_len = len;
    let mut aligned_addr: *mut c_void = ptr::null_mut();
    
    if let Some(align) = alignment {
        let page_size = page_size();
        if align > page_size {
            // For custom alignment, we'll allocate extra space and then adjust the pointer
            let extra = align - 1;
            aligned_len = len + extra;
            
            aligned_addr = mmap(
                ptr::null_mut(),
                aligned_len,
                prot,
                flags,
                -1,
                0,
            );
            
            if aligned_addr == libc::MAP_FAILED {
                return Err(Error::Io(io::Error::last_os_error()));
            }
            
            // Calculate aligned address
            let addr_value = aligned_addr as usize;
            let aligned_value = (addr_value + extra) & !(align - 1);
            
            // Unmap the extra portions
            let prefix_size = aligned_value - addr_value;
            if prefix_size > 0 {
                munmap(aligned_addr, prefix_size);
            }
            
            let suffix_size = extra - prefix_size;
            if suffix_size > 0 {
                munmap(
                    (aligned_addr as usize + prefix_size + len) as *mut c_void,
                    suffix_size,
                );
            }
            
            aligned_addr = aligned_value as *mut c_void;
            aligned_len = len;
        }
    }

    // Perform the actual mapping
    let addr = if aligned_addr.is_null() {
        mmap(
            ptr::null_mut(),
            len,
            prot,
            flags,
            -1,
            0,
        )
    } else {
        // We already have an aligned address
        aligned_addr
    };

    if addr == libc::MAP_FAILED {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    // NUMA is not supported on macOS, so we ignore the numa_policy parameter

    // If populate is requested, we can simulate it by touching the pages
    if populate {
        let page_size = page_size();
        // Touch each page to force it into memory
        for i in (0..len).step_by(page_size) {
            ptr::read_volatile((addr as usize + i) as *const u8);
        }
    }

    Ok(MmapRaw { ptr: addr as *mut u8, len: aligned_len })
}

/// Flush memory map changes to disk on macOS.
///
/// # Safety
///
/// This function is unsafe because it operates on raw memory.
pub unsafe fn flush(addr: *mut u8, len: usize, async_flush: bool) -> Result<()> {
    let flags = if async_flush { MS_ASYNC } else { MS_SYNC };
    
    let result = msync(addr as *mut c_void, len, flags);
    
    if result == 0 {
        Ok(())
    } else {
        Err(Error::Io(io::Error::last_os_error()))
    }
}

/// Unmap memory on macOS.
///
/// # Safety
///
/// This function is unsafe because it unmaps memory that might still be in use.
pub unsafe fn unmap(addr: *mut u8, len: usize) -> Result<()> {
    let result = munmap(addr as *mut c_void, len);
    
    if result == 0 {
        Ok(())
    } else {
        Err(Error::Io(io::Error::last_os_error()))
    }
}

/// Advise the kernel about how the memory map will be accessed on macOS.
///
/// # Safety
///
/// This function is unsafe because it operates on raw memory.
pub unsafe fn advise(addr: *mut u8, len: usize, advice: Advice) -> Result<()> {
    let advice_flag = match advice {
        Advice::Normal => MADV_NORMAL,
        Advice::Random => MADV_RANDOM,
        Advice::Sequential => MADV_SEQUENTIAL,
        Advice::WillNeed => MADV_WILLNEED,
        Advice::DontNeed => MADV_DONTNEED,
        Advice::SequentialOnce => MADV_SEQUENTIAL,  // Updated
        Advice::RandomOnce => MADV_RANDOM,          // Updated
        Advice::Free => MADV_FREE,
    };
    
    let result = madvise(addr as *mut c_void, len, advice_flag);
    
    if result == 0 {
        Ok(())
    } else {
        Err(Error::Io(io::Error::last_os_error()))
    }
}

/// Get the system page size.
#[inline]
fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}