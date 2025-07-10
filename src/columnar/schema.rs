//! Schema definition for columnar database.

use std::collections::HashMap;
use std::sync::Arc;

/// Data types supported by the columnar database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    /// Boolean type (1 byte)
    Boolean,
    
    /// 8-bit signed integer
    Int8,
    
    /// 8-bit unsigned integer
    UInt8,
    
    /// 16-bit signed integer
    Int16,
    
    /// 16-bit unsigned integer
    UInt16,
    
    /// 32-bit signed integer
    Int32,
    
    /// 32-bit unsigned integer
    UInt32,
    
    /// 64-bit signed integer
    Int64,
    
    /// 64-bit unsigned integer
    UInt64,
    
    /// 32-bit floating point
    Float32,
    
    /// 64-bit floating point
    Float64,
    
    /// UTF-8 string (variable length)
    String,
    
    /// Binary data (variable length)
    Binary,
    
    /// Date (32-bit integer representing days since epoch)
    Date,
    
    /// Timestamp (64-bit integer representing milliseconds since epoch)
    Timestamp,
    
    /// Fixed-size binary data
    FixedBinary(usize),
    
    /// Decimal (precision, scale)
    Decimal(usize, usize),
}

impl DataType {
    /// Get the size of the data type in bytes.
    ///
    /// For fixed-size types, returns the size in bytes.
    /// For variable-length types, returns None.
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::Boolean => Some(1),
            DataType::Int8 => Some(1),
            DataType::UInt8 => Some(1),
            DataType::Int16 => Some(2),
            DataType::UInt16 => Some(2),
            DataType::Int32 => Some(4),
            DataType::UInt32 => Some(4),
            DataType::Int64 => Some(8),
            DataType::UInt64 => Some(8),
            DataType::Float32 => Some(4),
            DataType::Float64 => Some(8),
            DataType::String => None,
            DataType::Binary => None,
            DataType::Date => Some(4),
            DataType::Timestamp => Some(8),
            DataType::FixedBinary(size) => Some(*size),
            DataType::Decimal(_, _) => Some(16),
        }
    }
    
    /// Check if the data type is numeric.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::UInt8
                | DataType::Int16
                | DataType::UInt16
                | DataType::Int32
                | DataType::UInt32
                | DataType::Int64
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal(_, _)
        )
    }
    
    /// Check if the data type is integer.
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::UInt8
                | DataType::Int16
                | DataType::UInt16
                | DataType::Int32
                | DataType::UInt32
                | DataType::Int64
                | DataType::UInt64
        )
    }
    
    /// Check if the data type is floating point.
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }
    
    /// Check if the data type is variable length.
    pub fn is_variable_length(&self) -> bool {
        matches!(self, DataType::String | DataType::Binary)
    }
}

/// Field definition in a schema.
#[derive(Debug, Clone)]
pub struct Field {
    /// Field name
    pub name: String,
    
    /// Field data type
    pub data_type: DataType,
    
    /// Whether the field can be null
    pub nullable: bool,
    
    /// Default value for the field (if any)
    pub default_value: Option<Vec<u8>>,
    
    /// Additional metadata for the field
    pub metadata: HashMap<String, String>,
}

impl Field {
    /// Create a new field.
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            default_value: None,
            metadata: HashMap::new(),
        }
    }
    
    /// Create a new field with a default value.
    pub fn with_default(name: &str, data_type: DataType, nullable: bool, default_value: Vec<u8>) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            default_value: Some(default_value),
            metadata: HashMap::new(),
        }
    }
    
    /// Add metadata to the field.
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

/// Schema definition for a table.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Fields in the schema
    pub fields: Vec<Field>,
    
    /// Field name to index mapping
    pub field_indices: HashMap<String, usize>,
    
    /// Additional metadata for the schema
    pub metadata: HashMap<String, String>,
}

impl Schema {
    /// Create a new schema.
    pub fn new(fields: Vec<Field>) -> Self {
        let mut field_indices = HashMap::new();
        for (i, field) in fields.iter().enumerate() {
            field_indices.insert(field.name.clone(), i);
        }
        
        Schema {
            fields,
            field_indices,
            metadata: HashMap::new(),
        }
    }
    
    /// Add metadata to the schema.
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
    
    /// Get a field by name.
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.field_indices.get(name).map(|&i| &self.fields[i])
    }
    
    /// Get a field by index.
    pub fn field_by_index(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }
    
    /// Get the index of a field by name.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.field_indices.get(name).copied()
    }
    
    /// Get the number of fields in the schema.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
    
    /// Check if the schema contains a field with the given name.
    pub fn contains_field(&self, name: &str) -> bool {
        self.field_indices.contains_key(name)
    }
}