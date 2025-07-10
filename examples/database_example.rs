//! Simple database example using memory mapping.
//!
//! This example demonstrates how to build a simple key-value store
//! using memory mapping for storage.
use std::fs::{File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom};
use std::path::Path;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use membase::{MmapOptions, MmapMut, Error};

// Simple header for our database file
#[repr(C)]
struct DbHeader {
    magic: [u8; 8],       // Magic number to identify our database
    version: u32,         // Database format version
    record_count: u32,    // Number of records in the database
    data_size: u64,       // Total size of data section
    index_offset: u64,    // Offset to the index section
}

// Record structure
#[repr(C)]
struct Record {
    key_length: u32,      // Length of the key
    value_length: u32,    // Length of the value
    key_offset: u64,      // Offset to the key data
    value_offset: u64,    // Offset to the value data
}

// Simple memory-mapped key-value database
struct MmapDatabase {
    file_path: String,
    mmap: MmapMut,
    index: Arc<RwLock<HashMap<String, (u64, u32)>>>, // Key -> (offset, length)
    header: *mut DbHeader,
    data_offset: u64,
}

impl MmapDatabase {
    // Open or create a database file
    fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file_path = path.to_string();
        let path = Path::new(path);
        
        let file_exists = path.exists();
        
        // Open or create the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        
        // Initialize the file if it's new
        if !file_exists {
            println!("Creating new database file...");
            
            // Create header
            let header = DbHeader {
                magic: *b"MMAPDBV1",
                version: 1,
                record_count: 0,
                data_size: 0,
                index_offset: 0,
            };
            
            // Write header
            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    &header as *const _ as *const u8,
                    std::mem::size_of::<DbHeader>(),
                )
            };
            file.write_all(header_bytes)?;
            
            // Ensure the file is at least 1MB
            file.set_len(1024 * 1024)?;
            file.sync_all()?;
        }
        
        // Memory map the file
        let mmap = unsafe { MmapOptions::new().write(true).map_mut(&file)? };
        
        // Get header pointer
        let header = unsafe { mmap.as_ptr() as *mut DbHeader };
        
        // Validate magic number if file existed
        if file_exists {
            let magic = unsafe { &(*header).magic };
            if magic != b"MMAPDBV1" {
                return Err("Invalid database file format".into());
            }
        }
        
        // Calculate data offset (right after header)
        let data_offset = std::mem::size_of::<DbHeader>() as u64;
        
        // Build index
        let index = Arc::new(RwLock::new(HashMap::new()));
        
        // If the file existed, load the index
        if file_exists {
            println!("Loading existing database...");
            
            let record_count = unsafe { (*header).record_count };
            let index_offset = unsafe { (*header).index_offset };
            
            if record_count > 0 && index_offset > 0 {
                // Read records from the index
                for i in 0..record_count {
                    let record_ptr = unsafe { 
                        (mmap.as_ptr().add(index_offset as usize) as *const Record).add(i as usize)
                    };
                    
                    let record = unsafe { &*record_ptr };
                    
                    // Read key
                    let key_bytes = &mmap[record.key_offset as usize..(record.key_offset + record.key_length as u64) as usize];
                    let key = String::from_utf8_lossy(key_bytes).to_string();
                    
                    // Add to index
                    index.write().unwrap().insert(key, (record.value_offset, record.value_length));
                }
            }
        }
        
        Ok(MmapDatabase {
            file_path,
            mmap,
            index,
            header,
            data_offset,
        })
    }
    
    // Get a value by key
    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let index = self.index.read().unwrap();
        
        if let Some(&(offset, length)) = index.get(key) {
            let value = self.mmap[offset as usize..(offset + length as u64) as usize].to_vec();
            Some(value)
        } else {
            None
        }
    }
    
    // Put a key-value pair
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        // Check if we need to resize the file
        let key_bytes = key.as_bytes();
        let required_space = key_bytes.len() + value.len() + std::mem::size_of::<Record>();
        
        let current_size = self.mmap.len();
        let used_size = unsafe { (*self.header).data_size } as usize + 
                        unsafe { (*self.header).record_count } as usize * std::mem::size_of::<Record>();
        
        if used_size + required_space > current_size {
            // Resize the file
            let new_size = (current_size * 2).max(current_size + required_space);
            println!("Resizing database from {} to {} bytes", current_size, new_size);
            
            // We need to unmap, resize, and remap
            drop(std::mem::replace(&mut self.mmap, unsafe { MmapMut::map_anon(0)? }));
            
            // Open the file and resize it
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.file_path)?;
            
            file.set_len(new_size as u64)?;
            file.sync_all()?;
            
            // Remap the file
            self.mmap = unsafe { MmapOptions::new().write(true).map_mut(&file)? };
            
            // Update header pointer
            self.header = self.mmap.as_ptr() as *mut DbHeader;
        }
        
        // Check if the key already exists
        let mut index = self.index.write().unwrap();
        
        if let Some(&(offset, length)) = index.get(key) {
            // Key exists, check if we can overwrite in place
            if length as usize >= value.len() {
                // We can overwrite in place
                self.mmap[offset as usize..(offset + value.len() as u64) as usize].copy_from_slice(value);
                
                // Update the index if the length changed
                if length as usize != value.len() {
                    index.insert(key.to_string(), (offset, value.len() as u32));
                }
                
                return Ok(());
            }
            
            // We can't overwrite in place, so we'll append the new value
            // and update the index
        }
        
        // Append the key and value to the data section
        let data_size = unsafe { (*self.header).data_size };
        let key_offset = self.data_offset + data_size;
        let value_offset = key_offset + key_bytes.len() as u64;
        
        // Write key
        self.mmap[key_offset as usize..(key_offset + key_bytes.len() as u64) as usize]
            .copy_from_slice(key_bytes);
        
        // Write value
        self.mmap[value_offset as usize..(value_offset + value.len() as u64) as usize]
            .copy_from_slice(value);
        
        // Update index
        index.insert(key.to_string(), (value_offset, value.len() as u32));
        
        // Update data size
        unsafe {
            (*self.header).data_size = data_size + key_bytes.len() as u64 + value.len() as u64;
        }
        
        // Update record count
        unsafe {
            (*self.header).record_count += 1;
        }
        
        // Drop the index lock before calling flush_index
        drop(index);
        
        // Write index to file
        self.flush_index()?;
        
        Ok(())
    }

    
    // Flush the index to the file
    fn flush_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index = self.index.read().unwrap();
        let record_count = index.len();
        
        // Calculate index offset (after data section)
        let index_offset = self.data_offset + unsafe { (*self.header).data_size };
        
        // Update header
        unsafe {
            (*self.header).index_offset = index_offset;
            (*self.header).record_count = record_count as u32;
        }
        
        // Write records
        let mut i = 0;
        for (key, &(value_offset, value_length)) in index.iter() {
            let key_bytes = key.as_bytes();
            let key_offset = self.data_offset + unsafe { (*self.header).data_size } - 
                            (key_bytes.len() as u64 + value_length as u64);
            
            let record = Record {
                key_length: key_bytes.len() as u32,
                value_length,
                key_offset,
                value_offset,
            };
            
            let record_ptr = unsafe { 
                (self.mmap.as_ptr().add(index_offset as usize) as *mut Record).add(i)
            };
            
            unsafe {
                *record_ptr = record;
            }
            
            i += 1;
        }
        
        // Flush changes to disk
        self.mmap.flush()?;
        
        Ok(())
    }
    
    // Flush changes to disk
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.mmap.flush()?;
        Ok(())
    }
    
    // Get the number of records
    fn record_count(&self) -> u32 {
        unsafe { (*self.header).record_count }
    }
    
    // Get the total data size
    fn data_size(&self) -> u64 {
        unsafe { (*self.header).data_size }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("membase Simple Database Example");
    println!("===============================");

    // Open or create a database
    let mut db = MmapDatabase::open("example_db.mmdb")?;
    
    println!("Database opened successfully");
    println!("  Record count: {}", db.record_count());
    println!("  Data size: {} bytes", db.data_size());
    
    // Store some values
    println!("\nStoring values...");
    db.put("name", b"membase Database")?;
    db.put("version", b"1.0.0")?;
    db.put("author", b"membase Team")?;
    db.put("description", b"A high-performance memory-mapped database example")?;
    
    // Flush changes to disk
    db.flush()?;
    
    // Retrieve values
    println!("\nRetrieving values:");
    for key in &["name", "version", "author", "description"] {
        if let Some(value) = db.get(key) {
            let value_str = String::from_utf8_lossy(&value);
            println!("  {}: {}", key, value_str);
        } else {
            println!("  {}: <not found>", key);
        }
    }
    
    // Update a value
    println!("\nUpdating a value...");
    db.put("version", b"1.0.1")?;
    
    // Retrieve the updated value
    if let Some(value) = db.get("version") {
        let value_str = String::from_utf8_lossy(&value);
        println!("  version: {}", value_str);
    }
    
    // Store a large value
    println!("\nStoring a large value...");
    let large_value = vec![42u8; 1024 * 1024]; // 1MB
    db.put("large_value", &large_value)?;
    
    // Retrieve the large value
    if let Some(value) = db.get("large_value") {
        println!("  large_value: {} bytes, first byte: {}", value.len(), value[0]);
    }
    
    println!("\nFinal database statistics:");
    println!("  Record count: {}", db.record_count());
    println!("  Data size: {} bytes", db.data_size());
    
    println!("\nExample completed successfully!");
    
    Ok(())
}