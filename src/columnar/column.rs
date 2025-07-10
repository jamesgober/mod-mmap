//! Column implementation for columnar database.
use std::sync::Arc;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use crate::error::{Error, Result};
use crate::mmap::{Mmap, MmapMut, MmapOptions};
use crate::columnar::schema::{DataType, Field};
use crate::columnar::compression::{Compression, CompressionType};

/// Column header stored at the beginning of each column file.
#[repr(C)]
struct ColumnHeader {
    /// Magic number to identify column files
    magic: [u8; 8],
    
    /// Column format version
    version: u32,
    
    /// Number of rows in the column
    row_count: u64,
    
    /// Data type of the column
    data_type: u32,
    
    /// Whether the column is nullable
    nullable: u8,
    
    /// Compression type used
    compression: u32,
    
    /// Offset to the data section
    data_offset: u64,
    
    /// Size of the data section
    data_size: u64,
    
    /// Offset to the null bitmap (if nullable)
    null_bitmap_offset: u64,
    
    /// Size of the null bitmap
    null_bitmap_size: u64,
    
    /// Offset to the offsets section (for variable-length types)
    offsets_offset: u64,
    
    /// Size of the offsets section
    offsets_size: u64,
    
    /// Reserved for future use
    reserved: [u8; 24],
}

impl ColumnHeader {
    /// Create a new column header.
    fn new(
        row_count: u64,
        data_type: &DataType,
        nullable: bool,
        compression: CompressionType,
    ) -> Self {
        ColumnHeader {
            magic: *b"ULTRACOL",
            version: 1,
            row_count,
            data_type: Self::data_type_to_u32(data_type),
            nullable: if nullable { 1 } else { 0 },
            compression: compression as u32,
            data_offset: 0,
            data_size: 0,
            null_bitmap_offset: 0,
            null_bitmap_size: 0,
            offsets_offset: 0,
            offsets_size: 0,
            reserved: [0; 24],
        }
    }
    
    /// Convert a data type to u32 for storage.
    fn data_type_to_u32(data_type: &DataType) -> u32 {
        match data_type {
            DataType::Boolean => 1,
            DataType::Int8 => 2,
            DataType::UInt8 => 3,
            DataType::Int16 => 4,
            DataType::UInt16 => 5,
            DataType::Int32 => 6,
            DataType::UInt32 => 7,
            DataType::Int64 => 8,
            DataType::UInt64 => 9,
            DataType::Float32 => 10,
            DataType::Float64 => 11,
            DataType::String => 12,
            DataType::Binary => 13,
            DataType::Date => 14,
            DataType::Timestamp => 15,
            DataType::FixedBinary(size) => 16 | ((*size as u32) << 8),
            DataType::Decimal(precision, scale) => 17 | ((*precision as u32) << 8) | ((*scale as u32) << 16),
        }
    }
    
    /// Convert a u32 to data type.
    fn u32_to_data_type(value: u32) -> DataType {
        match value & 0xFF {
            1 => DataType::Boolean,
            2 => DataType::Int8,
            3 => DataType::UInt8,
            4 => DataType::Int16,
            5 => DataType::UInt16,
            6 => DataType::Int32,
            7 => DataType::UInt32,
            8 => DataType::Int64,
            9 => DataType::UInt64,
            10 => DataType::Float32,
            11 => DataType::Float64,
            12 => DataType::String,
            13 => DataType::Binary,
            14 => DataType::Date,
            15 => DataType::Timestamp,
            16 => DataType::FixedBinary((value >> 8) as usize),
            17 => DataType::Decimal((value >> 8) as usize & 0xFF, (value >> 16) as usize & 0xFF),
            _ => panic!("Invalid data type value: {}", value),
        }
    }
}

/// A column in a columnar database.
pub struct Column {
    /// Field definition
    field: Field,
    
    /// Memory-mapped column data
    mmap: Mmap,
    
    /// Header information
    header: *const ColumnHeader,
    
    /// Pointer to the data section
    data_ptr: *const u8,
    
    /// Pointer to the null bitmap (if nullable)
    null_bitmap_ptr: *const u8,
    
    /// Pointer to the offsets section (for variable-length types)
    offsets_ptr: *const u64,
    
    /// Compression used for this column
    compression: CompressionType,
}

impl Column {
    /// Open a column from a file.
    pub fn open(path: &str) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        if mmap.len() < std::mem::size_of::<ColumnHeader>() {
            return Err(Error::InvalidArgument("File too small to be a valid column".into()));
        }
        
        let header = mmap.as_ptr() as *const ColumnHeader;
        
        // Validate magic number
        let magic = unsafe { &(*header).magic };
        if magic != b"ULTRACOL" {
            return Err(Error::InvalidArgument("Invalid column file format".into()));
        }
        
        // Get pointers to the various sections
        let data_ptr = unsafe { mmap.as_ptr().add((*header).data_offset as usize) };
        
        let null_bitmap_ptr = if unsafe { (*header).nullable } != 0 {
            unsafe { mmap.as_ptr().add((*header).null_bitmap_offset as usize) }
        } else {
            std::ptr::null()
        };
        
        let offsets_ptr = if unsafe { (*header).offsets_size } > 0 {
            unsafe { mmap.as_ptr().add((*header).offsets_offset as usize) as *const u64 }
        } else {
            std::ptr::null()
        };
        
        // Create field definition
        let data_type = unsafe { ColumnHeader::u32_to_data_type((*header).data_type) };
        let nullable = unsafe { (*header).nullable != 0 };
        let compression = unsafe { std::mem::transmute::<u32, CompressionType>((*header).compression) };
        
        let field = Field::new("column", data_type, nullable);
        
        Ok(Column {
            field,
            mmap,
            header,
            data_ptr,
            null_bitmap_ptr,
            offsets_ptr,
            compression,
        })
    }
    
    /// Get the number of rows in the column.
    pub fn row_count(&self) -> u64 {
        unsafe { (*self.header).row_count }
    }
    
    /// Get the data type of the column.
    pub fn data_type(&self) -> DataType {
        self.field.data_type.clone()
    }
    
    /// Check if the column is nullable.
    pub fn is_nullable(&self) -> bool {
        self.field.nullable
    }
    
    /// Get the compression type used for this column.
    pub fn compression(&self) -> CompressionType {
        self.compression
    }
    
    /// Check if a value at the given row index is null.
    pub fn is_null(&self, row_index: u64) -> bool {
        if !self.field.nullable || self.null_bitmap_ptr.is_null() {
            return false;
        }
        
        let byte_index = (row_index / 8) as usize;
        let bit_index = (row_index % 8) as u8;
        
        unsafe {
            let byte = *self.null_bitmap_ptr.add(byte_index);
            (byte & (1 << bit_index)) != 0
        }
    }
    
    /// Get a value at the given row index as bytes.
    pub fn get_bytes(&self, row_index: u64) -> Option<&[u8]> {
        if self.is_null(row_index) {
            return None;
        }
        
        match self.field.data_type {
            DataType::Boolean | DataType::Int8 | DataType::UInt8 => {
                unsafe {
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(row_index as usize),
                        1,
                    ))
                }
            },
            DataType::Int16 | DataType::UInt16 => {
                unsafe {
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(row_index as usize * 2),
                        2,
                    ))
                }
            },
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date => {
                unsafe {
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(row_index as usize * 4),
                        4,
                    ))
                }
            },
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Timestamp => {
                unsafe {
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(row_index as usize * 8),
                        8,
                    ))
                }
            },
            DataType::FixedBinary(size) => {
                unsafe {
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(row_index as usize * size),
                        size,
                    ))
                }
            },
            DataType::Decimal(_, _) => {
                unsafe {
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(row_index as usize * 16),
                        16,
                    ))
                }
            },
            DataType::String | DataType::Binary => {
                if self.offsets_ptr.is_null() {
                    return None;
                }
                
                unsafe {
                    let start_offset = *self.offsets_ptr.add(row_index as usize);
                    let end_offset = *self.offsets_ptr.add(row_index as usize + 1);
                    
                    Some(std::slice::from_raw_parts(
                        self.data_ptr.add(start_offset as usize),
                        (end_offset - start_offset) as usize,
                    ))
                }
            },
        }
    }
    
    /// Get a boolean value at the given row index.
    pub fn get_bool(&self, row_index: u64) -> Option<bool> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Boolean = self.field.data_type {
            unsafe {
                let value = *self.data_ptr.add(row_index as usize);
                Some(value != 0)
            }
        } else {
            None
        }
    }
    
    /// Get an i8 value at the given row index.
    pub fn get_i8(&self, row_index: u64) -> Option<i8> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Int8 = self.field.data_type {
            unsafe {
                let value = *self.data_ptr.add(row_index as usize) as i8;
                Some(value)
            }
        } else {
            None
        }
    }
    
    /// Get a u8 value at the given row index.
    pub fn get_u8(&self, row_index: u64) -> Option<u8> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::UInt8 = self.field.data_type {
            unsafe {
                let value = *self.data_ptr.add(row_index as usize);
                Some(value)
            }
        } else {
            None
        }
    }
    
    /// Get an i16 value at the given row index.
    pub fn get_i16(&self, row_index: u64) -> Option<i16> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Int16 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 2) as *const i16;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get a u16 value at the given row index.
    pub fn get_u16(&self, row_index: u64) -> Option<u16> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::UInt16 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 2) as *const u16;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get an i32 value at the given row index.
    pub fn get_i32(&self, row_index: u64) -> Option<i32> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Int32 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 4) as *const i32;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get a u32 value at the given row index.
    pub fn get_u32(&self, row_index: u64) -> Option<u32> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::UInt32 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 4) as *const u32;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get an i64 value at the given row index.
    pub fn get_i64(&self, row_index: u64) -> Option<i64> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Int64 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 8) as *const i64;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get a u64 value at the given row index.
    pub fn get_u64(&self, row_index: u64) -> Option<u64> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::UInt64 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 8) as *const u64;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get an f32 value at the given row index.
    pub fn get_f32(&self, row_index: u64) -> Option<f32> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Float32 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 4) as *const f32;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get an f64 value at the given row index.
    pub fn get_f64(&self, row_index: u64) -> Option<f64> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Float64 = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 8) as *const f64;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get a string value at the given row index.
    pub fn get_string(&self, row_index: u64) -> Option<&str> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::String = self.field.data_type {
            if let Some(bytes) = self.get_bytes(row_index) {
                std::str::from_utf8(bytes).ok()
            } else {
                None
            }
        } else {
            None
        }
    }
    
    /// Get a binary value at the given row index.
    pub fn get_binary(&self, row_index: u64) -> Option<&[u8]> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Binary | DataType::FixedBinary(_) = self.field.data_type {
            self.get_bytes(row_index)
        } else {
            None
        }
    }
    
    /// Get a date value at the given row index.
    pub fn get_date(&self, row_index: u64) -> Option<i32> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Date = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 4) as *const i32;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Get a timestamp value at the given row index.
    pub fn get_timestamp(&self, row_index: u64) -> Option<i64> {
        if self.is_null(row_index) {
            return None;
        }
        
        if let DataType::Timestamp = self.field.data_type {
            unsafe {
                let ptr = self.data_ptr.add(row_index as usize * 8) as *const i64;
                Some(*ptr)
            }
        } else {
            None
        }
    }
    
    /// Create an iterator over the column.
    pub fn iter(&self) -> ColumnIterator {
        ColumnIterator {
            column: self,
            current_row: 0,
            row_count: self.row_count(),
        }
    }
}

/// Iterator over a column.
pub struct ColumnIterator<'a> {
    column: &'a Column,
    current_row: u64,
    row_count: u64,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = Option<&'a [u8]>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row >= self.row_count {
            return None;
        }
        
        let row = self.current_row;
        self.current_row += 1;
        
        Some(self.column.get_bytes(row))
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.row_count - self.current_row) as usize;
        (remaining, Some(remaining))
    }
}

/// Builder for creating a column.
pub struct ColumnBuilder {
    field: Field,
    data: Vec<u8>,
    null_bitmap: Vec<u8>,
    offsets: Vec<u64>,
    row_count: u64,
    compression: CompressionType,
}

impl ColumnBuilder {
    /// Create a new column builder.
    pub fn new(field: Field, compression: CompressionType) -> Self {
        ColumnBuilder {
            field,
            data: Vec::new(),
            null_bitmap: Vec::new(),
            offsets: Vec::new(),
            row_count: 0,
            compression,
        }
    }
    
    /// Append a null value to the column.
    pub fn append_null(&mut self) -> Result<()> {
        if !self.field.nullable {
            return Err(Error::InvalidArgument("Column is not nullable".into()));
        }
        
        // Set the null bit
        let byte_index = (self.row_count / 8) as usize;
        let bit_index = (self.row_count % 8) as u8;
        
        if byte_index >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        
        self.null_bitmap[byte_index] |= 1 << bit_index;
        
        // For variable-length types, we need to update offsets
        if self.field.data_type.is_variable_length() {
            let current_offset = if self.offsets.is_empty() {
                0
            } else {
                self.offsets[self.offsets.len() - 1]
            };
            
            self.offsets.push(current_offset);
        } else {
            // For fixed-length types, we need to add padding
            if let Some(size) = self.field.data_type.size() {
                self.data.resize(self.data.len() + size, 0);
            }
        }
        
        self.row_count += 1;
        Ok(())
    }
    
    /// Append a boolean value to the column.
    pub fn append_bool(&mut self, value: bool) -> Result<()> {
        if let DataType::Boolean = self.field.data_type {
            self.data.push(if value { 1 } else { 0 });
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of boolean type".into()))
        }
    }
    
    /// Append an i8 value to the column.
    pub fn append_i8(&mut self, value: i8) -> Result<()> {
        if let DataType::Int8 = self.field.data_type {
            self.data.push(value as u8);
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of i8 type".into()))
        }
    }
    
    /// Append a u8 value to the column.
    pub fn append_u8(&mut self, value: u8) -> Result<()> {
        if let DataType::UInt8 = self.field.data_type {
            self.data.push(value);
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of u8 type".into()))
        }
    }
    
    /// Append an i16 value to the column.
    pub fn append_i16(&mut self, value: i16) -> Result<()> {
        if let DataType::Int16 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of i16 type".into()))
        }
    }
    
    /// Append a u16 value to the column.
    pub fn append_u16(&mut self, value: u16) -> Result<()> {
        if let DataType::UInt16 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of u16 type".into()))
        }
    }
    
    /// Append an i32 value to the column.
    pub fn append_i32(&mut self, value: i32) -> Result<()> {
        if let DataType::Int32 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of i32 type".into()))
        }
    }
    
    /// Append a u32 value to the column.
    pub fn append_u32(&mut self, value: u32) -> Result<()> {
        if let DataType::UInt32 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of u32 type".into()))
        }
    }
    
    /// Append an i64 value to the column.
    pub fn append_i64(&mut self, value: i64) -> Result<()> {
        if let DataType::Int64 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of i64 type".into()))
        }
    }
    
    /// Append a u64 value to the column.
    pub fn append_u64(&mut self, value: u64) -> Result<()> {
        if let DataType::UInt64 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of u64 type".into()))
        }
    }
    
    /// Append an f32 value to the column.
    pub fn append_f32(&mut self, value: f32) -> Result<()> {
        if let DataType::Float32 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of f32 type".into()))
        }
    }
    
    /// Append an f64 value to the column.
    pub fn append_f64(&mut self, value: f64) -> Result<()> {
        if let DataType::Float64 = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of f64 type".into()))
        }
    }
    
    /// Append a string value to the column.
    pub fn append_string(&mut self, value: &str) -> Result<()> {
        if let DataType::String = self.field.data_type {
            let current_offset = if self.offsets.is_empty() {
                0
            } else {
                self.offsets[self.offsets.len() - 1]
            };
            
            self.data.extend_from_slice(value.as_bytes());
            self.offsets.push(current_offset + value.len() as u64);
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of string type".into()))
        }
    }
    
    /// Append a binary value to the column.
    pub fn append_binary(&mut self, value: &[u8]) -> Result<()> {
        match self.field.data_type {
            DataType::Binary => {
                let current_offset = if self.offsets.is_empty() {
                    0
                } else {
                    self.offsets[self.offsets.len() - 1]
                };
                
                self.data.extend_from_slice(value);
                self.offsets.push(current_offset + value.len() as u64);
                self.row_count += 1;
                Ok(())
            },
            DataType::FixedBinary(size) => {
                if value.len() != size {
                    return Err(Error::InvalidArgument(format!(
                        "Binary value length {} does not match fixed size {}",
                        value.len(),
                        size
                    )));
                }
                
                self.data.extend_from_slice(value);
                self.row_count += 1;
                Ok(())
            },
            _ => Err(Error::InvalidArgument("Column is not of binary type".into())),
        }
    }
    
    /// Append a date value to the column.
    pub fn append_date(&mut self, value: i32) -> Result<()> {
        if let DataType::Date = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of date type".into()))
        }
    }
    
    /// Append a timestamp value to the column.
    pub fn append_timestamp(&mut self, value: i64) -> Result<()> {
        if let DataType::Timestamp = self.field.data_type {
            self.data.extend_from_slice(&value.to_le_bytes());
            self.row_count += 1;
            Ok(())
        } else {
            Err(Error::InvalidArgument("Column is not of timestamp type".into()))
        }
    }
    
    /// Write the column to a file.
    pub fn write_to_file(&self, path: &str) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        
        // Calculate section sizes and offsets
        let header_size = std::mem::size_of::<ColumnHeader>();
        let mut data_offset = header_size as u64;
        
        let null_bitmap_size = if self.field.nullable {
            (self.row_count + 7) / 8
        } else {
            0
        };
        
        let null_bitmap_offset = if null_bitmap_size > 0 {
            data_offset
        } else {
            0
        };
        
        data_offset += null_bitmap_size;
        
        let offsets_size = if self.field.data_type.is_variable_length() {
            (self.row_count + 1) * std::mem::size_of::<u64>() as u64
        } else {
            0
        };
        
        let offsets_offset = if offsets_size > 0 {
            data_offset
        } else {
            0
        };
        
        data_offset += offsets_size;
        
        // Create and write header
        let mut header = ColumnHeader::new(
            self.row_count,
            &self.field.data_type,
            self.field.nullable,
            self.compression,
        );
        
        header.data_offset = data_offset;
        header.data_size = self.data.len() as u64;
        header.null_bitmap_offset = null_bitmap_offset;
        header.null_bitmap_size = null_bitmap_size;
        header.offsets_offset = offsets_offset;
        header.offsets_size = offsets_size;
        
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &header as *const _ as *const u8,
                std::mem::size_of::<ColumnHeader>(),
            )
        };
        
        file.write_all(header_bytes)?;
        
        // Write null bitmap if needed
        if null_bitmap_size > 0 {
            file.write_all(&self.null_bitmap)?;
        }
        
        // Write offsets if needed
        if offsets_size > 0 {
            for offset in &self.offsets {
                file.write_all(&offset.to_le_bytes())?;
            }
        }
        
        // Write data
        file.write_all(&self.data)?;
        
        Ok(())
    }
}
