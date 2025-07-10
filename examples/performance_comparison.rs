//! Performance comparison between standard I/O, memory mapping, and columnar database.

use std::fs::{File, create_dir_all};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::time::{Instant, Duration};
use membase::{MmapOptions, Mmap, MmapMut};
use membase::columnar::{
    Schema, Field, DataType, ColumnBuilder, CompressionType
};

const ROW_COUNT: usize = 1_000_000;
const BATCH_SIZE: usize = 10_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("membase Performance Comparison");
    println!("===============================");
    println!("Comparing performance of standard I/O, memory mapping, and columnar database");
    println!("with {} rows of data", ROW_COUNT);
    
    // Create test directories
    create_dir_all("perf_test")?;
    
    // Generate test data
    println!("\nGenerating test data...");
    let mut ids = Vec::with_capacity(ROW_COUNT);
    let mut values = Vec::with_capacity(ROW_COUNT);
    
    for i in 0..ROW_COUNT {
        ids.push(i as u64);
        values.push((i % 100) as f64);
    }
    
    // Test 1: Standard I/O - Write
    println!("\nTest 1: Standard I/O - Write");
    let start = Instant::now();
    
    {
        let mut file = File::create("perf_test/standard_io.dat")?;
        
        for i in 0..ROW_COUNT {
            file.write_all(&ids[i].to_le_bytes())?;
            file.write_all(&values[i].to_le_bytes())?;
        }
        
        file.sync_all()?;
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 16) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Test 2: Memory Mapping - Write
    println!("\nTest 2: Memory Mapping - Write");
    let start = Instant::now();
    
    {
        let file = File::create("perf_test/mmap.dat")?;
        file.set_len((ROW_COUNT * 16) as u64)?;
        
        let mut map = unsafe { MmapOptions::new().write(true).map_mut(&file)? };
        
        for i in 0..ROW_COUNT {
            let offset = i * 16;
            map[offset..offset + 8].copy_from_slice(&ids[i].to_le_bytes());
            map[offset + 8..offset + 16].copy_from_slice(&values[i].to_le_bytes());
        }
        
        map.flush()?;
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 16) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Test 3: Columnar Database - Write
    println!("\nTest 3: Columnar Database - Write");
    let start = Instant::now();
    
    {
        // Create column builders
        let id_field = Field::new("id", DataType::UInt64, false);
        let value_field = Field::new("value", DataType::Float64, false);
        
        let mut id_builder = ColumnBuilder::new(id_field, CompressionType::None);
        let mut value_builder = ColumnBuilder::new(value_field, CompressionType::None);
        
        // Add data
        for i in 0..ROW_COUNT {
            id_builder.append_u64(ids[i])?;
            value_builder.append_f64(values[i])?;
        }
        
        // Write columns to files
        id_builder.write_to_file("perf_test/id.col")?;
        value_builder.write_to_file("perf_test/value.col")?;
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 16) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Test 4: Standard I/O - Read Sequential
    println!("\nTest 4: Standard I/O - Read Sequential");
    let start = Instant::now();
    
    {
        let mut file = File::open("perf_test/standard_io.dat")?;
        let mut buffer = [0u8; 16];
        let mut sum = 0.0;
        
        for _ in 0..ROW_COUNT {
            file.read_exact(&mut buffer)?;
            let value = f64::from_le_bytes([
                buffer[8], buffer[9], buffer[10], buffer[11],
                buffer[12], buffer[13], buffer[14], buffer[15],
            ]);
            sum += value;
        }
        
        println!("  Sum: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 16) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Test 5: Memory Mapping - Read Sequential
    println!("\nTest 5: Memory Mapping - Read Sequential");
    let start = Instant::now();
    
    {
        let file = File::open("perf_test/mmap.dat")?;
        let map = unsafe { MmapOptions::new().map(&file)? };
        let mut sum = 0.0;
        
        for i in 0..ROW_COUNT {
            let offset = i * 16 + 8;
            let value = f64::from_le_bytes([
                map[offset], map[offset + 1], map[offset + 2], map[offset + 3],
                map[offset + 4], map[offset + 5], map[offset + 6], map[offset + 7],
            ]);
            sum += value;
        }
        
        println!("  Sum: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 16) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Test 6: Columnar Database - Read Sequential
    println!("\nTest 6: Columnar Database - Read Sequential");
    let start = Instant::now();
    
    {
        let value_column = membase::columnar::Column::open("perf_test/value.col")?;
        let mut sum = 0.0;
        
        for i in 0..value_column.row_count() {
            if let Some(value) = value_column.get_f64(i) {
                sum += value;
            }
        }
        
        println!("  Sum: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 8) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    // Test 7: Standard I/O - Read Random
    println!("\nTest 7: Standard I/O - Read Random");
    let start = Instant::now();
    
    {
        let mut file = File::open("perf_test/standard_io.dat")?;
        let mut buffer = [0u8; 16];
        let mut sum = 0.0;
        
        for i in 0..10000 {
            let index = (i * 97) % ROW_COUNT;
            file.seek(SeekFrom::Start((index * 16) as u64))?;
            file.read_exact(&mut buffer)?;
            let value = f64::from_le_bytes([
                buffer[8], buffer[9], buffer[10], buffer[11],
                buffer[12], buffer[13], buffer[14], buffer[15],
            ]);
            sum += value;
        }
        
        println!("  Sum: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Operations per second: {:.2}", 
        10000.0 / elapsed.as_secs_f64());
    
    // Test 8: Memory Mapping - Read Random
    println!("\nTest 8: Memory Mapping - Read Random");
    let start = Instant::now();
    
    {
        let file = File::open("perf_test/mmap.dat")?;
        let map = unsafe { MmapOptions::new().map(&file)? };
        let mut sum = 0.0;
        
        for i in 0..10000 {
            let index = (i * 97) % ROW_COUNT;
            let offset = index * 16 + 8;
            let value = f64::from_le_bytes([
                map[offset], map[offset + 1], map[offset + 2], map[offset + 3],
                map[offset + 4], map[offset + 5], map[offset + 6], map[offset + 7],
            ]);
            sum += value;
        }
        
        println!("  Sum: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Operations per second: {:.2}", 
        10000.0 / elapsed.as_secs_f64());
    
    // Test 9: Columnar Database - Read Random
    println!("\nTest 9: Columnar Database - Read Random");
    let start = Instant::now();
    
    {
        let value_column = membase::columnar::Column::open("perf_test/value.col")?;
        let mut sum = 0.0;
        
        for i in 0..10000 {
            let index = (i * 97) % ROW_COUNT;
            if let Some(value) = value_column.get_f64(index as u64) {
                sum += value;
            }
        }
        
        println!("  Sum: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Operations per second: {:.2}", 
        10000.0 / elapsed.as_secs_f64());
    
    // Test 10: Columnar Database - Batch Processing
    println!("\nTest 10: Columnar Database - Batch Processing");
    let start = Instant::now();
    
    {
        let _id_column = membase::columnar::Column::open("perf_test/id.col")?;
        let value_column = membase::columnar::Column::open("perf_test/value.col")?;
        let mut sum = 0.0;
        
        // Process in batches
        for batch_start in (0..ROW_COUNT).step_by(BATCH_SIZE) {
            let batch_end = std::cmp::min(batch_start + BATCH_SIZE, ROW_COUNT);
            
            for i in batch_start..batch_end {
                if let Some(value) = value_column.get_f64(i as u64) {
                    if value > 50.0 {
                        sum += value;
                    }
                }
            }
        }
        
        println!("  Sum of values > 50: {}", sum);
    }
    
    let elapsed = start.elapsed();
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", 
        (ROW_COUNT * 8) as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64()));
    
    println!("\nPerformance comparison completed successfully!");
    
    Ok(())
}