//! Performance metrics for memory mapping operations.
//!
//! This module provides functionality for tracking and reporting performance
//! metrics for memory mapping operations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Operation types for metrics tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Map a file.
    MapFile,
    
    /// Map anonymous memory.
    MapAnon,
    
    /// Unmap memory.
    Unmap,
    
    /// Flush memory to disk.
    Flush,
    
    /// Advise the kernel about memory usage.
    Advise,
}

/// Memory mapping statistics.
#[derive(Debug, Default)]
pub struct MemoryStats {
    /// Total number of map operations.
    pub map_count: u64,
    
    /// Total number of unmap operations.
    pub unmap_count: u64,
    
    /// Total number of flush operations.
    pub flush_count: u64,
    
    /// Total number of advise operations.
    pub advise_count: u64,
    
    /// Total bytes mapped.
    pub bytes_mapped: u64,
    
    /// Total bytes unmapped.
    pub bytes_unmapped: u64,
    
    /// Total bytes flushed.
    pub bytes_flushed: u64,
    
    /// Average map operation time in microseconds.
    pub avg_map_time_us: u64,
    
    /// Average unmap operation time in microseconds.
    pub avg_unmap_time_us: u64,
    
    /// Average flush operation time in microseconds.
    pub avg_flush_time_us: u64,
}

// Atomic counters for tracking metrics
static MAP_COUNT: AtomicU64 = AtomicU64::new(0);
static UNMAP_COUNT: AtomicU64 = AtomicU64::new(0);
static FLUSH_COUNT: AtomicU64 = AtomicU64::new(0);
static ADVISE_COUNT: AtomicU64 = AtomicU64::new(0);
static BYTES_MAPPED: AtomicU64 = AtomicU64::new(0);
static BYTES_UNMAPPED: AtomicU64 = AtomicU64::new(0);
static BYTES_FLUSHED: AtomicU64 = AtomicU64::new(0);
static TOTAL_MAP_TIME_US: AtomicU64 = AtomicU64::new(0);
static TOTAL_UNMAP_TIME_US: AtomicU64 = AtomicU64::new(0);
static TOTAL_FLUSH_TIME_US: AtomicU64 = AtomicU64::new(0);

/// Record a memory mapping operation for metrics tracking.
///
/// # Arguments
///
/// * `op` - The operation type.
/// * `size` - The size of the memory region in bytes.
/// * `duration` - The duration of the operation.
#[inline]
pub fn record_operation(op: Operation, size: usize, duration: Duration) {
    let duration_us = duration.as_micros() as u64;
    
    match op {
        Operation::MapFile | Operation::MapAnon => {
            MAP_COUNT.fetch_add(1, Ordering::Relaxed);
            BYTES_MAPPED.fetch_add(size as u64, Ordering::Relaxed);
            TOTAL_MAP_TIME_US.fetch_add(duration_us, Ordering::Relaxed);
        },
        Operation::Unmap => {
            UNMAP_COUNT.fetch_add(1, Ordering::Relaxed);
            BYTES_UNMAPPED.fetch_add(size as u64, Ordering::Relaxed);
            TOTAL_UNMAP_TIME_US.fetch_add(duration_us, Ordering::Relaxed);
        },
        Operation::Flush => {
            FLUSH_COUNT.fetch_add(1, Ordering::Relaxed);
            BYTES_FLUSHED.fetch_add(size as u64, Ordering::Relaxed);
            TOTAL_FLUSH_TIME_US.fetch_add(duration_us, Ordering::Relaxed);
        },
        Operation::Advise => {
            ADVISE_COUNT.fetch_add(1, Ordering::Relaxed);
        },
    }
}

/// Get the current memory mapping statistics.
///
/// # Returns
///
/// The current memory mapping statistics.
#[inline]
pub fn get_stats() -> MemoryStats {
    let map_count = MAP_COUNT.load(Ordering::Relaxed);
    let unmap_count = UNMAP_COUNT.load(Ordering::Relaxed);
    let flush_count = FLUSH_COUNT.load(Ordering::Relaxed);
    
    let avg_map_time_us = if map_count > 0 {
        TOTAL_MAP_TIME_US.load(Ordering::Relaxed) / map_count
    } else {
        0
    };
    
    let avg_unmap_time_us = if unmap_count > 0 {
        TOTAL_UNMAP_TIME_US.load(Ordering::Relaxed) / unmap_count
    } else {
        0
    };
    
    let avg_flush_time_us = if flush_count > 0 {
        TOTAL_FLUSH_TIME_US.load(Ordering::Relaxed) / flush_count
    } else {
        0
    };
    
    MemoryStats {
        map_count,
        unmap_count,
        flush_count,
        advise_count: ADVISE_COUNT.load(Ordering::Relaxed),
        bytes_mapped: BYTES_MAPPED.load(Ordering::Relaxed),
        bytes_unmapped: BYTES_UNMAPPED.load(Ordering::Relaxed),
        bytes_flushed: BYTES_FLUSHED.load(Ordering::Relaxed),
        avg_map_time_us,
        avg_unmap_time_us,
        avg_flush_time_us,
    }
}

/// Reset all memory mapping statistics.
#[inline]
pub fn reset_stats() {
    MAP_COUNT.store(0, Ordering::Relaxed);
    UNMAP_COUNT.store(0, Ordering::Relaxed);
    FLUSH_COUNT.store(0, Ordering::Relaxed);
    ADVISE_COUNT.store(0, Ordering::Relaxed);
    BYTES_MAPPED.store(0, Ordering::Relaxed);
    BYTES_UNMAPPED.store(0, Ordering::Relaxed);
    BYTES_FLUSHED.store(0, Ordering::Relaxed);
    TOTAL_MAP_TIME_US.store(0, Ordering::Relaxed);
    TOTAL_UNMAP_TIME_US.store(0, Ordering::Relaxed);
    TOTAL_FLUSH_TIME_US.store(0, Ordering::Relaxed);
}

/// Measure the duration of an operation and record it.
///
/// # Arguments
///
/// * `op` - The operation type.
/// * `size` - The size of the memory region in bytes.
/// * `f` - The function to measure.
///
/// # Returns
///
/// The result of the function.
#[inline]
pub fn measure<F, T>(op: Operation, size: usize, f: F) -> T
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = f();
    let duration = start.elapsed();
    
    record_operation(op, size, duration);
    
    result
}