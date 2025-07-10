//! Performance benchmark example for the membase library.

use std::fs::File;
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::{Instant, Duration};

use membase::{MmapOptions, Mmap, MmapMut, Error};
use membase::platform::Advice;

const FILE_SIZE: usize = 100 * 1024 * 1024; // 100MB
const CHUNK_SIZE: usize = 4096;
const ITERATIONS: usize = 100;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("membase Performance Benchmark");
    println!("=============================");

    // Create a large temporary file
    println!("Creating a {}MB test file...", FILE_SIZE / 1024 / 1024);
    let path = Path::new("benchmark_data.bin");
    let mut file = File::create(&path)?;
    
    // Write random data to the file
    let mut buffer = vec![0u8; CHUNK_SIZE];
    for i in 0..(FILE_SIZE / CHUNK_SIZE) {
        // Generate some deterministic pattern
        for j in 0..CHUNK_SIZE {
            buffer[j] = ((i * j) % 256) as u8;
        }
        file.write_all(&buffer)?;
    }
    file.sync_all()?;
    
    // Close the file
    drop(file);
    
    // Benchmark 1: Sequential read with standard I/O
    println!("\nBenchmark 1: Sequential read with standard I/O");
    let mut file = File::open(&path)?;
    let mut buffer = vec![0u8; CHUNK_SIZE];
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        file.seek(SeekFrom::Start(0))?;
        let mut total_bytes = 0;
        loop {
            match file.read(&mut buffer)? {
                0 => break,
                n => {
                    total_bytes += n;
                    // Do something with the data to prevent optimization
                    buffer[0] = buffer[0].wrapping_add(1);
                }
            }
        }
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (FILE_SIZE as f64 * ITERATIONS as f64) / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Benchmark 2: Sequential read with memory mapping
    println!("\nBenchmark 2: Sequential read with memory mapping");
    let file = File::open(&path)?;
    let map = unsafe { MmapOptions::new().map(&file)? };
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut checksum = 0u64;
        for chunk in map.chunks(CHUNK_SIZE) {
            // Do something with the data to prevent optimization
            checksum = checksum.wrapping_add(chunk[0] as u64);
        }
        // Use checksum to prevent optimization
        if checksum == 0 {
            println!("  Checksum: {}", checksum);
        }
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (FILE_SIZE as f64 * ITERATIONS as f64) / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Benchmark 3: Random access with standard I/O
    println!("\nBenchmark 3: Random access with standard I/O");
    let mut file = File::open(&path)?;
    let mut buffer = vec![0u8; CHUNK_SIZE];
    
    let start = Instant::now();
    for i in 0..ITERATIONS {
        for j in 0..100 {
            let offset = ((i * j * 997) % (FILE_SIZE / CHUNK_SIZE)) * CHUNK_SIZE;
            file.seek(SeekFrom::Start(offset as u64))?;
            file.read_exact(&mut buffer)?;
            // Do something with the data to prevent optimization
            buffer[0] = buffer[0].wrapping_add(1);
        }
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Operations per second: {:.2}", 
        (ITERATIONS * 100) as f64 / elapsed.as_secs_f64());
    
    // Benchmark 4: Random access with memory mapping
    println!("\nBenchmark 4: Random access with memory mapping");
    let file = File::open(&path)?;
    let map = unsafe { MmapOptions::new().map(&file)? };
    
    let start = Instant::now();
    for i in 0..ITERATIONS {
        let mut checksum = 0u64;
        for j in 0..100 {
            let offset = ((i * j * 997) % (FILE_SIZE / CHUNK_SIZE)) * CHUNK_SIZE;
            let chunk = &map[offset..offset + CHUNK_SIZE];
            // Do something with the data to prevent optimization
            checksum = checksum.wrapping_add(chunk[0] as u64);
        }
        // Use checksum to prevent optimization
        if checksum == 0 {
            println!("  Checksum: {}", checksum);
        }
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Operations per second: {:.2}", 
        (ITERATIONS * 100) as f64 / elapsed.as_secs_f64());
    
    // Benchmark 5: Write with standard I/O
    println!("\nBenchmark 5: Write with standard I/O");
    let mut file = File::create(&path)?;
    let buffer = vec![42u8; CHUNK_SIZE];
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        file.seek(SeekFrom::Start(0))?;
        for _ in 0..(FILE_SIZE / CHUNK_SIZE) {
            file.write_all(&buffer)?;
        }
        file.sync_all()?;
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (FILE_SIZE as f64 * ITERATIONS as f64) / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Benchmark 6: Write with memory mapping
    println!("\nBenchmark 6: Write with memory mapping");
    let file = File::open(&path)?;
    let mut map = unsafe { MmapOptions::new().write(true).map_mut(&file)? };
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for chunk in map.chunks_mut(CHUNK_SIZE) {
            // Fill with a pattern
            for i in 0..chunk.len() {
                chunk[i] = 42;
            }
        }
        map.flush()?;
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (FILE_SIZE as f64 * ITERATIONS as f64) / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Benchmark 7: Sequential read with optimized memory mapping
    println!("\nBenchmark 7: Sequential read with optimized memory mapping");
    let file = File::open(&path)?;
    let map = unsafe { 
        MmapOptions::new()
            .prefetch(membase::PrefetchStrategy::Sequential)
            .populate(true)
            .map(&file)? 
    };
    
    // Advise the kernel about our access pattern
    map.advise(Advice::Sequential)?;
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut checksum = 0u64;
        for chunk in map.chunks(CHUNK_SIZE) {
            // Do something with the data to prevent optimization
            checksum = checksum.wrapping_add(chunk[0] as u64);
        }
        // Use checksum to prevent optimization
        if checksum == 0 {
            println!("  Checksum: {}", checksum);
        }
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (FILE_SIZE as f64 * ITERATIONS as f64) / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Benchmark 8: Random access with optimized memory mapping
    println!("\nBenchmark 8: Random access with optimized memory mapping");
    let file = File::open(&path)?;
    let map = unsafe { 
        MmapOptions::new()
            .populate(true)
            .map(&file)? 
    };
    
    // Advise the kernel about our access pattern
    map.advise(Advice::Random)?;
    
    let start = Instant::now();
    for i in 0..ITERATIONS {
        let mut checksum = 0u64;
        for j in 0..100 {
            let offset = ((i * j * 997) % (FILE_SIZE / CHUNK_SIZE)) * CHUNK_SIZE;
            let chunk = &map[offset..offset + CHUNK_SIZE];
            // Do something with the data to prevent optimization
            checksum = checksum.wrapping_add(chunk[0] as u64);
        }
        // Use checksum to prevent optimization
        if checksum == 0 {
            println!("  Checksum: {}", checksum);
        }
    }
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Operations per second: {:.2}", 
        (ITERATIONS * 100) as f64 / elapsed.as_secs_f64());
    
    // Clean up
    drop(map);
    std::fs::remove_file(&path)?;
    
    println!("\nBenchmark completed successfully!");
    
    Ok(())
}
            