//! Advanced features example for the membase library.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use membase::{MmapOptions, Mmap, MmapMut, Error};
use membase::{HugePageSize, NumaPolicy, PrefetchStrategy};
use membase::platform::Advice;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("membase Advanced Features Example");
    println!("=================================");

    // Check for feature support
    println!("Feature Support:");
    println!("  Huge Pages: {}", membase::has_huge_page_support());
    println!("  NUMA: {}", membase::has_numa_support());
    println!("  SIMD: {}", membase::has_simd_support());
    
    // Create a large temporary file
    let path = Path::new("large_data.bin");
    let mut file = File::create(&path)?;
    
    // Write 10MB of data to the file
    let size = 10 * 1024 * 1024;
    let data = vec![0u8; size];
    file.write_all(&data)?;
    file.sync_all()?;
    
    // Close the file
    drop(file);
    
    // Open the file for reading
    let file = File::open(&path)?;
    
    // Create a memory map with advanced options
    println!("\nCreating memory map with advanced options...");
    let map = unsafe {
        MmapOptions::new()
            .read(true)
            .write(true)
            // Use huge pages if available
            .huge_pages(HugePageSize::TwoMB)
            // Use sequential prefetching
            .prefetch(PrefetchStrategy::Sequential)
            // Populate the mapping immediately
            .populate(true)
            // Map the file
            .map_mut(&file)?
    };
    
    println!("Memory map created successfully:");
    println!("  Size: {} bytes", map.len());
    
    // Advise the kernel about our access pattern
    println!("\nAdvising the kernel about access pattern...");
    map.advise(Advice::Sequential)?;
    
    // Perform some operations on the memory map
    println!("Performing sequential read...");
    let mut sum = 0u64;
    for chunk in map.chunks(8) {
        if chunk.len() == 8 {
            let value = u64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3],
                chunk[4], chunk[5], chunk[6], chunk[7],
            ]);
            sum = sum.wrapping_add(value);
        }
    }
    println!("Checksum: {}", sum);
    
    // Flush asynchronously
    println!("\nFlushing asynchronously...");
    map.flush_async()?;
    
    // Get memory mapping statistics
    println!("\nMemory Mapping Statistics:");
    let stats = membase::utils::metrics::get_stats();
    println!("  Map operations: {}", stats.map_count);
    println!("  Bytes mapped: {}", stats.bytes_mapped);
    println!("  Average map time: {}µs", stats.avg_map_time_us);
    
    // Clean up
    drop(map);
    std::fs::remove_file(&path)?;
    
    // Try to create an anonymous memory map with huge pages
    println!("\nCreating anonymous memory map with huge pages...");
    if membase::has_huge_page_support() {
        match unsafe {
            MmapOptions::new()
                .read(true)
                .write(true)
                .huge_pages(HugePageSize::TwoMB)
                .map_anon(2 * 1024 * 1024) // 2MB
        } {
            Ok(map) => {
                println!("Successfully created huge page mapping of {} bytes", map.len());
                // Write some data to the memory map
                if map.len() >= 8 {
                    unsafe {
                        let ptr = map.as_ptr() as *mut u64;
                        *ptr = 0xDEADBEEFCAFEBABE;
                    }
                }
            },
            Err(e) => {
                println!("Failed to create huge page mapping: {}", e);
                println!("Falling back to regular pages...");
                
                let map = unsafe {
                    MmapOptions::new()
                        .read(true)
                        .write(true)
                        .map_anon(2 * 1024 * 1024) // 2MB
                }?;
                
                println!("Successfully created regular page mapping of {} bytes", map.len());
            }
        }
    } else {
        println!("Huge pages are not supported on this system");
    }
    
    println!("\nExample completed successfully!");
    
    Ok(())
}