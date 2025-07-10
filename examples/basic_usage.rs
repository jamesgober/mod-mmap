//! Basic usage example for the membase library.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use membase::{MmapOptions, Mmap, MmapMut, Error};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("membase Basic Usage Example");
    println!("===========================");

    // Create a temporary file
    let path = Path::new("example_data.bin");
    let mut file = File::create(&path)?;
    
    // Write some data to the file
    let data = b"Hello, membase!";
    file.write_all(data)?;
    file.sync_all()?;
    
    // Close the file
    drop(file);
    
    // Open the file for reading
    let file = File::open(&path)?;
    
    // Create a read-only memory map
    println!("Creating read-only memory map...");
    let map = unsafe { MmapOptions::new().map(&file)? };
    
    // Read the data from the memory map
    let contents = std::str::from_utf8(&map)?;
    println!("Read from memory map: {}", contents);
    
    // Close the memory map
    drop(map);
    
    // Open the file for writing
    let file = File::open(&path)?;
    
    // Create a writable memory map
    println!("\nCreating writable memory map...");
    let mut map = unsafe { MmapOptions::new().write(true).map_mut(&file)? };
    
    // Modify the data in the memory map
    if map.len() >= 7 {
        map[7..13].copy_from_slice(b"membase");
    }
    
    // Flush the changes to disk
    println!("Flushing changes to disk...");
    map.flush()?;
    
    // Close the memory map
    drop(map);
    
    // Read the file again to verify the changes
    let file = File::open(&path)?;
    let map = unsafe { Mmap::map(&file)? };
    let contents = std::str::from_utf8(&map)?;
    println!("Read after modification: {}", contents);
    
    // Clean up
    drop(map);
    std::fs::remove_file(&path)?;
    
    // Anonymous memory map example
    println!("\nCreating anonymous memory map...");
    let mut map = unsafe { MmapMut::map_anon(1024)? };
    
    // Write some data to the anonymous memory map
    let message = b"This is an anonymous memory map";
    map[0..message.len()].copy_from_slice(message);
    
    // Read the data back
    let contents = std::str::from_utf8(&map[0..message.len()])?;
    println!("Read from anonymous map: {}", contents);
    
    println!("\nExample completed successfully!");
    
    Ok(())
}