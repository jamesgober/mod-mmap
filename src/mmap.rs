//! Core memory mapping functionality.
//!
//! This module provides the primary interfaces for memory mapping operations,
//! with a focus on performance and safety.

use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{Error, Result};
use crate::platform;
use crate::advanced::{HugePageSize, NumaPolicy, PrefetchStrategy};
use crate::utils::alignment;

/// Statistics for memory mapping operations
static TOTAL_MAPPED_MEMORY: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_MAPPINGS: AtomicUsize = AtomicUsize::new(0);

/// Configuration options for memory mapping.
#[derive(Debug, Clone)]
pub struct MmapOptions {
    /// The offset within the file at which the memory map will start.
    offset: u64,
    
    /// The length of the memory map.
    len: Option<usize>,
    
    /// Whether the memory map should be readable.
    pub readable: bool,
    
    /// Whether the memory map should be writable.
    pub writable: bool,
    
    /// Whether the memory map should be executable.
    pub executable: bool,
    
    /// Whether the memory map should be backed by huge pages.
    pub huge_pages: Option<HugePageSize>,
    
    /// NUMA policy for the memory map.
    pub numa_policy: Option<NumaPolicy>,
    
    /// Prefetching strategy for the memory map.
    pub prefetch: Option<PrefetchStrategy>,
    
    /// Stack support (MAP_STACK on Unix).
    pub stack: bool,
    
    /// Whether to use copy-on-write semantics.
    pub copy_on_write: bool,
    
    /// Whether to populate (prefault) the map.
    pub populate: bool,
    
    /// Custom alignment for the memory map.
    pub alignment: Option<usize>,
}

impl Default for MmapOptions {
    fn default() -> MmapOptions {
        MmapOptions {
            offset: 0,
            len: None,
            readable: true,
            writable: false,
            executable: false,
            huge_pages: None,
            numa_policy: None,
            prefetch: None,
            stack: false,
            copy_on_write: false,
            populate: false,
            alignment: None,
        }
    }
}

impl MmapOptions {
    /// Create a new set of options for configuring memory maps.
    #[inline]
    pub fn new() -> MmapOptions {
        MmapOptions::default()
    }

    /// Set the offset from the start of the file to start the memory map.
    ///
    /// This offset will be rounded down to the nearest multiple of the page size.
    #[inline]
    pub fn offset(mut self, offset: u64) -> MmapOptions {
        self.offset = offset;
        self
    }

    /// Set the length of the memory map.
    #[inline]
    pub fn len(mut self, len: usize) -> MmapOptions {
        self.len = Some(len);
        self
    }

    /// Configure the memory map to be readable.
    #[inline]
    pub fn read(mut self, readable: bool) -> MmapOptions {
        self.readable = readable;
        self
    }

    /// Configure the memory map to be writable.
    #[inline]
    pub fn write(mut self, writable: bool) -> MmapOptions {
        self.writable = writable;
        self
    }

    /// Configure the memory map to be executable.
    #[inline]
    pub fn exec(mut self, executable: bool) -> MmapOptions {
        self.executable = executable;
        self
    }

    /// Configure the memory map to use huge pages.
    #[inline]
    pub fn huge_pages(mut self, size: HugePageSize) -> MmapOptions {
        self.huge_pages = Some(size);
        self
    }

    /// Configure the memory map with a NUMA policy.
    #[inline]
    pub fn numa_policy(mut self, policy: NumaPolicy) -> MmapOptions {
        self.numa_policy = Some(policy);
        self
    }

    /// Configure the prefetching strategy.
    #[inline]
    pub fn prefetch(mut self, strategy: PrefetchStrategy) -> MmapOptions {
        self.prefetch = Some(strategy);
        self
    }

    /// Configure the memory map for stack usage (MAP_STACK on Unix).
    #[inline]
    pub fn stack(mut self, stack: bool) -> MmapOptions {
        self.stack = stack;
        self
    }

    /// Configure the memory map to use copy-on-write semantics.
    #[inline]
    pub fn copy_on_write(mut self, cow: bool) -> MmapOptions {
        self.copy_on_write = cow;
        self
    }

    /// Configure the memory map to be pre-populated (prefaulted).
    #[inline]
    pub fn populate(mut self, populate: bool) -> MmapOptions {
        self.populate = populate;
        self
    }

    /// Configure a custom alignment for the memory map.
    #[inline]
    pub fn alignment(mut self, alignment: usize) -> MmapOptions {
        self.alignment = Some(alignment);
        self
    }

    /// Create a read-only memory map backed by a file.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a memory map that can be accessed
    /// while the file is being modified by external processes, which might lead to
    /// undefined behavior.
    #[inline]
    pub unsafe fn map(&self, file: &File) -> Result<Mmap> {
        self.map_impl(file).map(|raw| Mmap { inner: raw })
    }

    /// Create a writable memory map backed by a file.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a memory map that can modify the
    /// file, which might lead to undefined behavior if the file is being accessed
    /// by other processes.
    #[inline]
    pub unsafe fn map_mut(&self, file: &File) -> Result<MmapMut> {
        let mut options = self.clone();
        options.writable = true;
        options.map_impl(file).map(|raw| MmapMut { inner: raw })
    }

    /// Create an anonymous memory map not backed by a file.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a memory map that can be accessed
    /// and modified, which might lead to undefined behavior if not used correctly.
    #[inline]
    pub unsafe fn map_anon(&self, len: usize) -> Result<MmapMut> {
        let mut options = self.clone();
        options.len = Some(len);
        options.map_anon_impl().map(|raw| MmapMut { inner: raw })
    }

    /// Implementation of file-backed memory mapping.
    unsafe fn map_impl(&self, file: &File) -> Result<MmapRaw> {
        // Validate options
        if let Some(len) = self.len {
            if len == 0 {
                return Err(Error::ZeroSizedMapping);
            }
        }

        // Get file length if not specified
        let len = match self.len {
            Some(len) => len,
            None => {
                let metadata = file.metadata()?;
                metadata.len().try_into().map_err(|_| Error::SizeExceedsSystemLimit)?
            }
        };

        // Perform platform-specific mapping
        let raw = platform::map_file(
            file,
            self.offset,
            len,
            self.readable,
            self.writable,
            self.executable,
            self.huge_pages,
            self.numa_policy.clone(),  // Clone here
            self.stack,
            self.copy_on_write,
            self.populate,
            self.alignment,
        )?;

        // Update statistics
        TOTAL_MAPPED_MEMORY.fetch_add(len, Ordering::Relaxed);
        ACTIVE_MAPPINGS.fetch_add(1, Ordering::Relaxed);

        // Apply prefetching if requested
        if let Some(strategy) = self.prefetch {
            crate::advanced::prefetch::apply_strategy(raw.ptr, len, strategy);
        }

        Ok(raw)
    }

    /// Implementation of anonymous memory mapping.
    unsafe fn map_anon_impl(&self) -> Result<MmapRaw> {
        let len = self.len.ok_or(Error::InvalidArgument("Length must be specified for anonymous mapping".into()))?;
        
        if len == 0 {
            return Err(Error::ZeroSizedMapping);
        }

        // Perform platform-specific anonymous mapping
        let raw = platform::map_anon(
            len,
            self.readable,
            self.writable,
            self.executable,
            self.huge_pages,
            self.numa_policy,
            self.stack,
            self.populate,
            self.alignment,
        )?;

        // Update statistics
        TOTAL_MAPPED_MEMORY.fetch_add(len, Ordering::Relaxed);
        ACTIVE_MAPPINGS.fetch_add(1, Ordering::Relaxed);

        // Apply prefetching if requested
        if let Some(strategy) = self.prefetch {
            crate::advanced::prefetch::apply_strategy(raw.ptr, len, strategy);
        }

        Ok(raw)
    }
}

/// Raw memory map handle.
#[derive(Debug)]
pub struct MmapRaw {
    /// Pointer to the mapped memory.
    pub(crate) ptr: *mut u8,
    
    /// Length of the mapped memory.
    pub(crate) len: usize,
}

impl MmapRaw {
    /// Flush the memory map to disk.
    ///
    /// This function will flush the entire memory map to disk, ensuring that all
    /// changes are persisted.
    #[inline]
    pub fn flush(&self) -> Result<()> {
        unsafe { platform::flush(self.ptr, self.len, false) }
    }

    /// Flush the memory map to disk asynchronously.
    ///
    /// This function will initiate an asynchronous flush of the entire memory map
    /// to disk, but may return before the flush is complete.
    #[inline]
    pub fn flush_async(&self) -> Result<()> {
        unsafe { platform::flush(self.ptr, self.len, true) }
    }

    /// Advise the kernel about how the memory map will be accessed.
    #[inline]
    pub fn advise(&self, advice: platform::Advice) -> Result<()> {
        unsafe { platform::advise(self.ptr, self.len, advice) }
    }
}

impl Drop for MmapRaw {
    fn drop(&mut self) {
        // Safety: We're ensuring proper cleanup of the memory map
        unsafe {
            if !self.ptr.is_null() {
                // Update statistics
                ACTIVE_MAPPINGS.fetch_sub(1, Ordering::Relaxed);
                TOTAL_MAPPED_MEMORY.fetch_sub(self.len, Ordering::Relaxed);
                
                // Unmap the memory
                let _ = platform::unmap(self.ptr, self.len);
            }
        }
    }
}

/// A read-only memory map.
#[derive(Debug)]
pub struct Mmap {
    inner: MmapRaw,
}

impl Mmap {
    /// Create a read-only memory map backed by a file.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a memory map that can be accessed
    /// while the file is being modified by external processes, which might lead to
    /// undefined behavior.
    #[inline]
    pub unsafe fn map(file: &File) -> Result<Mmap> {
        MmapOptions::new().map(file)
    }

    /// Return the length of the memory map.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len
    }

    /// Return true if the memory map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    /// Return a pointer to the memory map.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the memory map outlives any pointers derived
    /// from this function.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.inner.ptr
    }

    /// Flush the memory map to disk.
    #[inline]
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    /// Flush the memory map to disk asynchronously.
    #[inline]
    pub fn flush_async(&self) -> Result<()> {
        self.inner.flush_async()
    }

    /// Advise the kernel about how the memory map will be accessed.
    #[inline]
    pub fn advise(&self, advice: platform::Advice) -> Result<()> {
        self.inner.advise(advice)
    }
}

impl Deref for Mmap {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.inner.ptr, self.inner.len) }
    }
}

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// A writable memory map.
#[derive(Debug)]
pub struct MmapMut {
    inner: MmapRaw,
}

impl MmapMut {
    /// Create a writable memory map backed by a file.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a memory map that can modify the
    /// file, which might lead to undefined behavior if the file is being accessed
    /// by other processes.
    #[inline]
    pub unsafe fn map(file: &File) -> Result<MmapMut> {
        MmapOptions::new().write(true).map_mut(file)
    }

    /// Create an anonymous memory map not backed by a file.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it creates a memory map that can be accessed
    /// and modified, which might lead to undefined behavior if not used correctly.
    #[inline]
    pub unsafe fn map_anon(len: usize) -> Result<MmapMut> {
        MmapOptions::new().write(true).map_anon(len)
    }

    /// Return the length of the memory map.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len
    }

    /// Return true if the memory map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    /// Return a pointer to the memory map.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the memory map outlives any pointers derived
    /// from this function.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.inner.ptr
    }

    /// Return a mutable pointer to the memory map.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the memory map outlives any pointers derived
    /// from this function, and that no data races occur.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.inner.ptr
    }

    /// Flush the memory map to disk.
    #[inline]
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    /// Flush the memory map to disk asynchronously.
    #[inline]
    pub fn flush_async(&self) -> Result<()> {
        self.inner.flush_async()
    }

    /// Advise the kernel about how the memory map will be accessed.
    #[inline]
    pub fn advise(&self, advice: platform::Advice) -> Result<()> {
        self.inner.advise(advice)
    }
}

impl Deref for MmapMut {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.inner.ptr, self.inner.len) }
    }
}

impl DerefMut for MmapMut {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.inner.ptr, self.inner.len) }
    }
}

impl AsRef<[u8]> for MmapMut {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for MmapMut {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

/// Get the total amount of memory currently mapped.
#[inline]
pub fn total_mapped_memory() -> usize {
    TOTAL_MAPPED_MEMORY.load(Ordering::Relaxed)
}

/// Get the number of active memory mappings.
#[inline]
pub fn active_mappings() -> usize {
    ACTIVE_MAPPINGS.load(Ordering::Relaxed)
}