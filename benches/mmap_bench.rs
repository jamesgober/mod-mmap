//! Comprehensive benchmarks for the membase library.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;
use tempfile::tempdir;

use membase::{MmapOptions, Mmap, MmapMut, PrefetchStrategy};
use membase::platform::Advice;

const SMALL_SIZE: usize = 4 * 1024;        // 4KB
const MEDIUM_SIZE: usize = 1 * 1024 * 1024; // 1MB
const LARGE_SIZE: usize = 64 * 1024 * 1024; // 64MB

fn setup_file(path: &Path, size: usize) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    
    // Write data in chunks to avoid excessive memory usage
    let chunk_size = 64 * 1024;
    let mut buffer = vec![0u8; chunk_size];
    
    for i in 0..(size / chunk_size) {
        // Fill with a pattern
        for j in 0..chunk_size {
            buffer[j] = ((i * j) % 256) as u8;
        }
        file.write_all(&buffer)?;
    }
    
    // Write any remaining bytes
    let remaining = size % chunk_size;
    if remaining > 0 {
        buffer.truncate(remaining);
        for j in 0..remaining {
            buffer[j] = ((size / chunk_size * j) % 256) as u8;
        }
        file.write_all(&buffer)?;
    }
    
    file.sync_all()
}

fn bench_sequential_read_std_io(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Sequential Read (Standard IO)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        let file_path = dir.path().join(format!("seq_read_std_{}", size));
        setup_file(&file_path, size).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut file = File::open(&file_path).unwrap();
                let mut buffer = vec![0u8; 4096];
                let mut total = 0u64;
                
                loop {
                    match file.read(&mut buffer).unwrap() {
                        0 => break,
                        n => {
                            // Do something with the data
                            for byte in &buffer[0..n] {
                                total = total.wrapping_add(*byte as u64);
                            }
                        }
                    }
                }
                
                black_box(total)
            });
        });
    }
    
    group.finish();
}

fn bench_sequential_read_mmap(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Sequential Read (Memory Map)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        let file_path = dir.path().join(format!("seq_read_mmap_{}", size));
        setup_file(&file_path, size).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let file = File::open(&file_path).unwrap();
            let map = unsafe { MmapOptions::new().map(&file).unwrap() };
            
            b.iter(|| {
                let mut total = 0u64;
                
                for byte in &map[..] {
                    total = total.wrapping_add(*byte as u64);
                }
                
                black_box(total)
            });
        });
    }
    
    group.finish();
}

fn bench_sequential_read_optimized_mmap(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Sequential Read (Optimized Memory Map)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        let file_path = dir.path().join(format!("seq_read_opt_mmap_{}", size));
        setup_file(&file_path, size).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let file = File::open(&file_path).unwrap();
            let map = unsafe { 
                MmapOptions::new()
                    .prefetch(PrefetchStrategy::Sequential)
                    .populate(true)
                    .map(&file)
                    .unwrap() 
            };
            
            // Advise the kernel about our access pattern
            map.advise(Advice::Sequential).unwrap();
            
            b.iter(|| {
                let mut total = 0u64;
                
                for byte in &map[..] {
                    total = total.wrapping_add(*byte as u64);
                }
                
                black_box(total)
            });
        });
    }
    
    group.finish();
}

fn bench_random_access_std_io(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Random Access (Standard IO)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        let file_path = dir.path().join(format!("rand_access_std_{}", size));
        setup_file(&file_path, size).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut file = File::open(&file_path).unwrap();
                let mut buffer = [0u8; 8];
                let mut total = 0u64;
                
                // Perform 1000 random reads
                for i in 0..1000 {
                    let offset = (i * 997) % (size - 8);
                    file.seek(SeekFrom::Start(offset as u64)).unwrap();
                    file.read_exact(&mut buffer).unwrap();
                    
                    let value = u64::from_le_bytes(buffer);
                    total = total.wrapping_add(value);
                }
                
                black_box(total)
            });
        });
    }
    
    group.finish();
}

fn bench_random_access_mmap(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Random Access (Memory Map)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        let file_path = dir.path().join(format!("rand_access_mmap_{}", size));
        setup_file(&file_path, size).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let file = File::open(&file_path).unwrap();
            let map = unsafe { MmapOptions::new().map(&file).unwrap() };
            
            b.iter(|| {
                let mut total = 0u64;
                
                // Perform 1000 random reads
                for i in 0..1000 {
                    let offset = (i * 997) % (size - 8);
                    let value = unsafe {
                        *(map.as_ptr().add(offset) as *const u64)
                    };
                    total = total.wrapping_add(value);
                }
                
                black_box(total)
            });
        });
    }
    
    group.finish();
}

fn bench_random_access_optimized_mmap(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Random Access (Optimized Memory Map)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        let file_path = dir.path().join(format!("rand_access_opt_mmap_{}", size));
        setup_file(&file_path, size).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let file = File::open(&file_path).unwrap();
            let map = unsafe { 
                MmapOptions::new()
                    .populate(true)
                    .map(&file)
                    .unwrap() 
            };
            
            // Advise the kernel about our access pattern
            map.advise(Advice::Random).unwrap();
            
            b.iter(|| {
                let mut total = 0u64;
                
                // Perform 1000 random reads
                for i in 0..1000 {
                    let offset = (i * 997) % (size - 8);
                    let value = unsafe {
                        *(map.as_ptr().add(offset) as *const u64)
                    };
                    total = total.wrapping_add(value);
                }
                
                black_box(total)
            });
        });
    }
    
    group.finish();
}

fn bench_write_std_io(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Write (Standard IO)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE] {  // Skip LARGE_SIZE for write tests
        let file_path = dir.path().join(format!("write_std_{}", size));
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut file = File::create(&file_path).unwrap();
                let chunk_size = 4096;
                let mut buffer = vec![42u8; chunk_size];
                
                for i in 0..(size / chunk_size) {
                    // Modify the buffer to prevent optimization
                    buffer[0] = (i % 256) as u8;
                    file.write_all(&buffer).unwrap();
                }
                
                file.sync_all().unwrap();
            });
        });
    }
    
    group.finish();
}

fn bench_write_mmap(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut group = c.benchmark_group("Write (Memory Map)");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE] {  // Skip LARGE_SIZE for write tests
        let file_path = dir.path().join(format!("write_mmap_{}", size));
        
        // Create and pre-size the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();
        file.set_len(size as u64).unwrap();
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut map = unsafe { 
                    MmapOptions::new()
                        .write(true)
                        .map_mut(&file)
                        .unwrap() 
                };
                
                for i in 0..size {
                    map[i] = 42;
                }
                
                map.flush().unwrap();
            });
        });
    }
    
    group.finish();
}

fn bench_copy_on_write(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("cow_test");
    setup_file(&file_path, MEDIUM_SIZE).unwrap();
    
    let mut group = c.benchmark_group("Copy-on-Write");
    
    group.bench_function("Standard Copy", |b| {
        let file = File::open(&file_path).unwrap();
        let map = unsafe { MmapOptions::new().map(&file).unwrap() };
        
        b.iter(|| {
            // Create a copy of the data
            let copy = map.to_vec();
            black_box(copy)
        });
    });
    
    group.bench_function("Copy-on-Write", |b| {
        let file = File::open(&file_path).unwrap();
        
        b.iter(|| {
            // Create a copy-on-write mapping
            let map = unsafe { 
                MmapOptions::new()
                    .copy_on_write(true)
                    .map(&file)
                    .unwrap() 
            };
            black_box(map)
        });
    });
    
    group.finish();
}

fn bench_anonymous_mapping(c: &mut Criterion) {
    let mut group = c.benchmark_group("Anonymous Mapping");
    
    for &size in &[SMALL_SIZE, MEDIUM_SIZE, LARGE_SIZE] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let map = unsafe { 
                    MmapOptions::new()
                        .write(true)
                        .map_anon(size)
                        .unwrap() 
                };
                black_box(map)
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_read_std_io,
    bench_sequential_read_mmap,
    bench_sequential_read_optimized_mmap,
    bench_random_access_std_io,
    bench_random_access_mmap,
    bench_random_access_optimized_mmap,
    bench_write_std_io,
    bench_write_mmap,
    bench_copy_on_write,
    bench_anonymous_mapping,
);
criterion_main!(benches);