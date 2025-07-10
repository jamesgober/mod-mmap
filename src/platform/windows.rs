//! Windows-specific memory mapping implementation.

use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;
use std::ptr;

use winapi::um::memoryapi::{
    CreateFileMappingW, MapViewOfFileEx, FlushViewOfFile, UnmapViewOfFile,
    FILE_MAP_READ, FILE_MAP_WRITE, FILE_MAP_EXECUTE, FILE_MAP_COPY,
};
use winapi::um::winnt::{
    PAGE_READONLY, PAGE_READWRITE, PAGE_EXECUTE_READ, PAGE_EXECUTE_READWRITE,
    SEC_COMMIT, SEC_RESERVE, SEC_LARGE_PAGES, MEM_COMMIT, MEM_RESERVE,
};
use winapi::um::handleapi::CloseHandle;
use winapi::um::sysinfoapi::GetSystemInfo;
use winapi::um::winbase::VirtualAlloc;
use winapi::shared::minwindef::{DWORD, LPVOID};
use winapi::shared::basetsd::SIZE_T;

use crate::error::{Error, Result};
use crate::mmap::MmapRaw;
use crate::advanced::{HugePageSize, NumaPolicy};
use crate::platform::Advice;
use crate::utils::alignment;

/// Map a file into memory on Windows.
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
) -> Result<MmapRaw> {
    // Calculate protection flags
    let page_protection = if executable {
        if writable {
            PAGE_EXECUTE_READWRITE
        } else {
            PAGE_EXECUTE_READ
        }
    } else if writable {
        PAGE_READWRITE
    } else {
        PAGE_READONLY
    };

    // Calculate access flags
    let mut desired_access = 0;
    if readable {
        desired_access |= FILE_MAP_READ;
    }
    if writable {
        if copy_on_write {
            desired_access |= FILE_MAP_COPY;
        } else {
            desired_access |= FILE_MAP_WRITE;
        }
    }
    if executable {
        desired_access |= FILE_MAP_EXECUTE;
    }

    // Calculate additional flags
    let mut additional_flags = SEC_COMMIT;
    if huge_pages.is_some() {
        additional_flags |= SEC_LARGE_PAGES;
    }

    // Calculate page-aligned offset
    let system_info = get_system_info();
    let page_size = system_info.dwPageSize as u64;
    let aligned_offset = offset & !(page_size - 1);
    let offset_delta = offset - aligned_offset;
    let aligned_len = len + offset_delta as usize;

    // Calculate maximum size for the file mapping
    let maximum_size_high = ((aligned_offset + aligned_len as u64) >> 32) as DWORD;
    let maximum_size_low = ((aligned_offset + aligned_len as u64) & 0xFFFFFFFF) as DWORD;

    // Create file mapping
    let file_mapping = CreateFileMappingW(
        file.as_raw_handle(),
        ptr::null_mut(),
        page_protection | additional_flags,
        maximum_size_high,
        maximum_size_low,
        ptr::null(),
    );

    if file_mapping.is_null() {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    // Calculate offset for MapViewOfFileEx
    let offset_high = (aligned_offset >> 32) as DWORD;
    let offset_low = (aligned_offset & 0xFFFFFFFF) as DWORD;

    // Apply custom alignment if requested
    let mut aligned_addr: LPVOID = ptr::null_mut();
    if let Some(align) = alignment {
        if align > page_size as usize {
            // Windows doesn't provide direct support for custom alignment in MapViewOfFileEx
            // We'll need to map with extra space and then adjust the pointer
            
            // This is a simplified approach - in a real implementation, you'd need
            // to handle this more carefully, possibly with VirtualAlloc and explicit
            // memory management
            
            // For now, we'll just return an error if custom alignment is requested
            // beyond the system page size
            return Err(Error::InvalidArgument("Custom alignment beyond page size is not supported on Windows".into()));
        }
    }

    // Map view of file
    let addr = MapViewOfFileEx(
        file_mapping,
        desired_access,
        offset_high,
        offset_low,
        aligned_len as SIZE_T,
        aligned_addr,
    );

    // Close file mapping handle (the view will remain valid)
    CloseHandle(file_mapping);

    if addr.is_null() {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    // NUMA is not directly supported through this API on Windows
    // We'll ignore the numa_policy parameter

    // If populate is requested, we can simulate it by touching the pages
    if populate {
        // Touch each page to force it into memory
        for i in (0..aligned_len).step_by(page_size as usize) {
            ptr::read_volatile((addr as usize + i) as *const u8);
        }
    }

    // Adjust pointer for the offset delta
    let ptr = (addr as usize + offset_delta as usize) as *mut u8;

    Ok(MmapRaw { ptr, len })
}

/// Create an anonymous memory map on Windows.
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
) -> Result<MmapRaw> {
    // Calculate protection flags
    let page_protection = if executable {
        if writable {
            PAGE_EXECUTE_READWRITE
        } else {
            PAGE_EXECUTE_READ
        }
    } else if writable {
        PAGE_READWRITE
    } else {
        PAGE_READONLY
    };

    // Calculate allocation type
    let mut allocation_type = MEM_RESERVE | MEM_COMMIT;
    if huge_pages.is_some() {
        allocation_type |= SEC_LARGE_PAGES as DWORD;
    }

    // Get system info for page size
    let system_info = get_system_info();
    let page_size = system_info.dwPageSize as usize;

    // Apply custom alignment if requested
    let mut aligned_len = len;
    let mut aligned_addr: LPVOID = ptr::null_mut();
    
    if let Some(align) = alignment {
        if align > page_size {
            // For custom alignment, we'll allocate extra space and then adjust the pointer
            let extra = align - 1;
            aligned_len = len + extra;
            
            // Windows doesn't provide direct support for custom alignment in VirtualAlloc
            // beyond the system page size. We'll need to allocate extra space and then
            // adjust the pointer ourselves.
            
            // This is a simplified approach - in a real implementation, you'd need
            // to handle this more carefully
            
            // For now, we'll just return an error if custom alignment is requested
            // beyond the system page size
            return Err(Error::InvalidArgument("Custom alignment beyond page size is not supported on Windows".into()));
        }
    }

    // Allocate memory
    let addr = VirtualAlloc(
        aligned_addr,
        aligned_len as SIZE_T,
        allocation_type,
        page_protection,
    );

    if addr.is_null() {
        return Err(Error::Io(io::Error::last_os_error()));
    }

    // NUMA is not directly supported through this API on Windows
    // We'll ignore the numa_policy parameter

    // If populate is requested, we can simulate it by touching the pages
    if populate {
        // Touch each page to force it into memory
        for i in (0..aligned_len).step_by(page_size) {
            ptr::read_volatile((addr as usize + i) as *const u8);
        }
    }

    Ok(MmapRaw { ptr: addr as *mut u8, len: aligned_len })
}

/// Flush memory map changes to disk on Windows.
///
/// # Safety
///
/// This function is unsafe because it operates on raw memory.
pub unsafe fn flush(addr: *mut u8, len: usize, _async_flush: bool) -> Result<()> {
    // Windows doesn't have a direct equivalent to async flush
    // FlushViewOfFile is always synchronous
    let result = FlushViewOfFile(addr as LPVOID, len as SIZE_T);
    
    if result != 0 {
        Ok(())
    } else {
        Err(Error::Io(io::Error::last_os_error()))
    }
}

/// Unmap memory on Windows.
///
/// # Safety
///
/// This function is unsafe because it unmaps memory that might still be in use.
pub unsafe fn unmap(addr: *mut u8, _len: usize) -> Result<()> {
    // Windows doesn't use the length parameter for unmapping
    let result = UnmapViewOfFile(addr as LPVOID);
    
    if result != 0 {
        Ok(())
    } else {
        Err(Error::Io(io::Error::last_os_error()))
    }
}

/// Advise the kernel about how the memory map will be accessed on Windows.
///
/// # Safety
///
/// This function is unsafe because it operates on raw memory.
pub unsafe fn advise(_addr: *mut u8, _len: usize, _advice: Advice) -> Result<()> {
    // Windows doesn't have a direct equivalent to madvise
    // We'll just silently ignore the advice
    Ok(())
}

/// Get system information.
#[inline]
fn get_system_info() -> winapi::um::sysinfoapi::SYSTEM_INFO {
    let mut system_info: winapi::um::sysinfoapi::SYSTEM_INFO = unsafe { std::mem::zeroed() };
    unsafe { GetSystemInfo(&mut system_info) };
    system_info
}
