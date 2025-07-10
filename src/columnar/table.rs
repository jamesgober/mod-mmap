//! Table implementation for columnar database.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::columnar::schema::{Schema, Field, DataType};
use crate::columnar::column::{Column, ColumnBuilder};
use crate::columnar::compression::CompressionType;

/// A table in a columnar database.
pub struct Table {
    /// Table name
    name: String,
    
    /// Table schema
    schema: Schema,
    
    /// Columns in the table
    columns: HashMap<String, Column>,
    
    /// Base directory for table files
    base_dir: PathBuf,
    
    /// Number of rows in the table
    row_count: u64,
}

impl Table {
    /// Open a table from a directory.
    pub fn open(name: &str, base_dir: &str) -> Result<Self> {
        let base_path = Path::new(base_dir).join(name);
        
        // Check if the directory exists
        if !base_path.exists() || !base_path.is_dir() {
            return Err(Error::InvalidArgument(format!(
                "Table directory {} does not exist",
                base_path.display()
            )));
        }
        
        // Load schema
        let schema_path = base_path.join("schema.json");
        if !schema_path.exists() {
            return Err(Error::InvalidArgument(format!(
                "Schema file {} does not exist",
                schema_path.display()
            )));
        }
        
        // In a real implementation, we would parse the schema from the file
        // For this example, we'll create a simple schema
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::String, false),
            Field::new("age", DataType::UInt8, true),
            Field::new("email", DataType::String, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("created_at", DataType::Timestamp, false),
        ]);
        
        // Load columns
        let mut columns = HashMap::new();
        let mut row_count = 0;
        
        for field in &schema.fields {
            let column_path = base_path.join(format!("{}.col", field.name));
            if !column_path.exists() {
                return Err(Error::InvalidArgument(format!(
                    "Column file {} does not exist",
                    column_path.display()
                )));
            }
            
            let column = Column::open(column_path.to_str().unwrap())?;
            
            // Verify that all columns have the same number of rows
            if columns.is_empty() {
                row_count = column.row_count();
            } else if column.row_count() != row_count {
                return Err(Error::InvalidArgument(format!(
                    "Column {} has {} rows, but expected {}",
                    field.name,
                    column.row_count(),
                    row_count
                )));
            }
            
            columns.insert(field.name.clone(), column);
        }
        
        Ok(Table {
            name: name.to_string(),
            schema,
            columns,
            base_dir: base_path,
            row_count,
        })
    }
    
    /// Get the table name.
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the table schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    
    /// Get the number of rows in the table.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }
    
    /// Get a column by name.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }
    
    /// Get all columns.
    pub fn columns(&self) -> &HashMap<String, Column> {
        &self.columns
    }
}

/// Builder for creating a table.
pub struct TableBuilder {
    /// Table name
    name: String,
    
    /// Table schema
    schema: Schema,
    
    /// Column builders
    column_builders: HashMap<String, ColumnBuilder>,
    
    /// Base directory for table files
    base_dir: PathBuf,
    
    /// Compression type to use for columns
    compression: CompressionType,
}

impl TableBuilder {
    /// Create a new table builder.
    pub fn new(name: &str, schema: Schema, base_dir: &str, compression: CompressionType) -> Self {
        let base_path = Path::new(base_dir).join(name);
        
        // Create column builders
        let mut column_builders = HashMap::new();
        for field in &schema.fields {
            let builder = ColumnBuilder::new(field.clone(), compression);
            column_builders.insert(field.name.clone(), builder);
        }
        
        TableBuilder {
            name: name.to_string(),
            schema,
            column_builders,
            base_dir: base_path,
            compression,
        }
    }
    
    /// Add a row to the table.
    pub fn add_row(&mut self, values: &HashMap<String, Option<Vec<u8>>>) -> Result<()> {
        // Check that all required fields are present
        for field in &self.schema.fields {
            if !field.nullable && !values.contains_key(&field.name) {
                return Err(Error::InvalidArgument(format!(
                    "Missing required field: {}",
                    field.name
                )));
            }
        }
        
        // Add values to column builders
        for (name, value_opt) in values {
            if let Some(builder) = self.column_builders.get_mut(name) {
                if let Some(value) = value_opt {
                    match self.schema.field(name).unwrap().data_type {
                        DataType::Boolean => {
                            if value.len() != 1 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid boolean value for field {}",
                                    name
                                )));
                            }
                            builder.append_bool(value[0] != 0)?;
                        },
                        DataType::Int8 => {
                            if value.len() != 1 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid i8 value for field {}",
                                    name
                                )));
                            }
                            builder.append_i8(value[0] as i8)?;
                        },
                        DataType::UInt8 => {
                            if value.len() != 1 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid u8 value for field {}",
                                    name
                                )));
                            }
                            builder.append_u8(value[0])?;
                        },
                        DataType::Int16 => {
                            if value.len() != 2 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid i16 value for field {}",
                                    name
                                )));
                            }
                            let val = i16::from_le_bytes([value[0], value[1]]);
                            builder.append_i16(val)?;
                        },
                        DataType::UInt16 => {
                            if value.len() != 2 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid u16 value for field {}",
                                    name
                                )));
                            }
                            let val = u16::from_le_bytes([value[0], value[1]]);
                            builder.append_u16(val)?;
                        },
                        DataType::Int32 => {
                            if value.len() != 4 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid i32 value for field {}",
                                    name
                                )));
                            }
                            let val = i32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                            builder.append_i32(val)?;
                        },
                        DataType::UInt32 => {
                            if value.len() != 4 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid u32 value for field {}",
                                    name
                                )));
                            }
                            let val = u32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                            builder.append_u32(val)?;
                        },
                        DataType::Int64 => {
                            if value.len() != 8 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid i64 value for field {}",
                                    name
                                )));
                            }
                            let val = i64::from_le_bytes([
                                value[0], value[1], value[2], value[3],
                                value[4], value[5], value[6], value[7],
                            ]);
                            builder.append_i64(val)?;
                        },
                        DataType::UInt64 => {
                            if value.len() != 8 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid u64 value for field {}",
                                    name
                                )));
                            }
                            let val = u64::from_le_bytes([
                                value[0], value[1], value[2], value[3],
                                value[4], value[5], value[6], value[7],
                            ]);
                            builder.append_u64(val)?;
                        },
                        DataType::Float32 => {
                            if value.len() != 4 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid f32 value for field {}",
                                    name
                                )));
                            }
                            let val = f32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                            builder.append_f32(val)?;
                        },
                        DataType::Float64 => {
                            if value.len() != 8 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid f64 value for field {}",
                                    name
                                )));
                            }
                            let val = f64::from_le_bytes([
                                value[0], value[1], value[2], value[3],
                                value[4], value[5], value[6], value[7],
                            ]);
                            builder.append_f64(val)?;
                        },
                        DataType::String => {
                            let s = String::from_utf8(value.clone())
                                .map_err(|_| Error::InvalidArgument(format!(
                                    "Invalid UTF-8 string for field {}",
                                    name
                                )))?;
                            builder.append_string(&s)?;
                        },
                        DataType::Binary => {
                            builder.append_binary(value)?;
                        },
                        DataType::FixedBinary(size) => {
                            if value.len() != size {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid fixed binary value for field {}: expected size {}, got {}",
                                    name,
                                    size,
                                    value.len()
                                )));
                            }
                            builder.append_binary(value)?;
                        },
                        DataType::Date => {
                            if value.len() != 4 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid date value for field {}",
                                    name
                                )));
                            }
                            let val = i32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                            builder.append_date(val)?;
                        },
                        DataType::Timestamp => {
                            if value.len() != 8 {
                                return Err(Error::InvalidArgument(format!(
                                    "Invalid timestamp value for field {}",
                                    name
                                )));
                            }
                            let val = i64::from_le_bytes([
                                value[0], value[1], value[2], value[3],
                                value[4], value[5], value[6], value[7],
                            ]);
                            builder.append_timestamp(val)?;
                        },
                        DataType::Decimal(_, _) => {
                            // For simplicity, we'll skip decimal implementation in this example
                            return Err(Error::InvalidArgument(format!(
                                "Decimal type not implemented for field {}",
                                name
                            )));
                        },
                    }
                } else {
                    builder.append_null()?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Build the table and write it to disk.
    pub fn build(&self) -> Result<Table> {
        // Create the table directory
        std::fs::create_dir_all(&self.base_dir)?;
        
        // Write schema to file
        // In a real implementation, we would serialize the schema to JSON
        // For this example, we'll skip this step
        
        // Write columns to files
        let mut columns = HashMap::new();
        let mut row_count = 0;
        
        for (name, builder) in &self.column_builders {
            let column_path = self.base_dir.join(format!("{}.col", name));
            builder.write_to_file(column_path.to_str().unwrap())?;
            
            let column = Column::open(column_path.to_str().unwrap())?;
            
            // Verify that all columns have the same number of rows
            if columns.is_empty() {
                row_count = column.row_count();
            } else if column.row_count() != row_count {
                return Err(Error::InvalidArgument(format!(
                    "Column {} has {} rows, but expected {}",
                    name,
                    column.row_count(),
                    row_count
                )));
            }
            
            columns.insert(name.clone(), column);
        }
        
        Ok(Table {
            name: self.name.clone(),
            schema: self.schema.clone(),
            columns,
            base_dir: self.base_dir.clone(),
            row_count,
        })
    }
}