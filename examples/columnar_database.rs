//! Columnar database example using the membase library.
use std::fs::{File, create_dir_all};
use std::io::{self, Write};
use std::path::Path;
use membase::columnar::{
    Schema, Field, DataType, Table, TableBuilder, Column, ColumnBuilder,
    CompressionType, Query, Predicate, Operator
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("membase Columnar Database Example");
    println!("==================================");

    // Create a directory for our database files
    let db_dir = "columnar_db";
    create_dir_all(db_dir)?;
    
    // Define a schema for our user table
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::String, false),
        Field::new("age", DataType::UInt8, true),
        Field::new("email", DataType::String, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("created_at", DataType::Timestamp, false),
    ]);
    
    println!("Creating user table with schema:");
    
    for (_i, field) in schema.fields.iter().enumerate() {
        println!("  {}: {} ({}nullable)", 
            field.name, 
            format!("{:?}", field.data_type),
            if field.nullable { "" } else { "not " }
        );
    }
    
    // Create column builders
    let mut id_builder = ColumnBuilder::new(
        schema.fields[0].clone(),
        CompressionType::None,
    );
    
    let mut name_builder = ColumnBuilder::new(
        schema.fields[1].clone(),
        CompressionType::None,
    );
    
    let mut age_builder = ColumnBuilder::new(
        schema.fields[2].clone(),
        CompressionType::None,
    );
    
    let mut email_builder = ColumnBuilder::new(
        schema.fields[3].clone(),
        CompressionType::None,
    );
    
    let mut active_builder = ColumnBuilder::new(
        schema.fields[4].clone(),
        CompressionType::None,
    );
    
    let mut created_at_builder = ColumnBuilder::new(
        schema.fields[5].clone(),
        CompressionType::None,
    );
    
    // Add some sample data
    println!("\nAdding sample data...");
    
    // User 1
    id_builder.append_u64(1)?;
    name_builder.append_string("Alice Smith")?;
    age_builder.append_u8(32)?;
    email_builder.append_string("alice@example.com")?;
    active_builder.append_bool(true)?;
    created_at_builder.append_timestamp(1625097600000)?; // 2021-07-01
    
    // User 2
    id_builder.append_u64(2)?;
    name_builder.append_string("Bob Johnson")?;
    age_builder.append_u8(45)?;
    email_builder.append_string("bob@example.com")?;
    active_builder.append_bool(true)?;
    created_at_builder.append_timestamp(1625184000000)?; // 2021-07-02
    
    // User 3
    id_builder.append_u64(3)?;
    name_builder.append_string("Charlie Brown")?;
    age_builder.append_u8(28)?;
    email_builder.append_null()?;
    active_builder.append_bool(false)?;
    created_at_builder.append_timestamp(1625270400000)?; // 2021-07-03
    
    // User 4
    id_builder.append_u64(4)?;
    name_builder.append_string("Diana Prince")?;
    age_builder.append_null()?;
    email_builder.append_string("diana@example.com")?;
    active_builder.append_bool(true)?;
    created_at_builder.append_timestamp(1625356800000)?; // 2021-07-04
    
    // User 5
    id_builder.append_u64(5)?;
    name_builder.append_string("Ethan Hunt")?;
    age_builder.append_u8(39)?;
    email_builder.append_string("ethan@example.com")?;
    active_builder.append_bool(true)?;
    created_at_builder.append_timestamp(1625443200000)?; // 2021-07-05
    
    // Write columns to files
    println!("\nWriting columns to disk...");
    id_builder.write_to_file(&format!("{}/id.col", db_dir))?;
    name_builder.write_to_file(&format!("{}/name.col", db_dir))?;
    age_builder.write_to_file(&format!("{}/age.col", db_dir))?;
    email_builder.write_to_file(&format!("{}/email.col", db_dir))?;
    active_builder.write_to_file(&format!("{}/active.col", db_dir))?;
    created_at_builder.write_to_file(&format!("{}/created_at.col", db_dir))?;
    
    // Read columns back
    println!("\nReading columns from disk...");
    let id_column = Column::open(&format!("{}/id.col", db_dir))?;
    let name_column = Column::open(&format!("{}/name.col", db_dir))?;
    let age_column = Column::open(&format!("{}/age.col", db_dir))?;
    let email_column = Column::open(&format!("{}/email.col", db_dir))?;
    let active_column = Column::open(&format!("{}/active.col", db_dir))?;
    let created_at_column = Column::open(&format!("{}/created_at.col", db_dir))?;
    
    println!("Columns loaded successfully:");
    println!("  id: {} rows", id_column.row_count());
    println!("  name: {} rows", name_column.row_count());
    println!("  age: {} rows", age_column.row_count());
    println!("  email: {} rows", email_column.row_count());
    println!("  active: {} rows", active_column.row_count());
    println!("  created_at: {} rows", created_at_column.row_count());
    
    // Print all users
    println!("\nAll users:");
    for i in 0..id_column.row_count() {
        let id = id_column.get_u64(i).unwrap();
        let name = name_column.get_string(i).unwrap();
        let age = match age_column.get_u8(i) {
            Some(age) => age.to_string(),
            None => "NULL".to_string(),
        };
        let email = match email_column.get_string(i) {
            Some(email) => email.to_string(),
            None => "NULL".to_string(),
        };
        let active = active_column.get_bool(i).unwrap();
        let created_at = created_at_column.get_timestamp(i).unwrap();
        
        println!("  User {}: {} (age: {}, email: {}, active: {}, created: {})",
            id, name, age, email, active, created_at);
    }
    
    // Perform a simple query: find active users
    println!("\nActive users:");
    for i in 0..active_column.row_count() {
        if active_column.get_bool(i).unwrap() {
            let id = id_column.get_u64(i).unwrap();
            let name = name_column.get_string(i).unwrap();
            println!("  User {}: {}", id, name);
        }
    }
    
    // Perform a more complex query: find users over 30 years old with an email
    println!("\nUsers over 30 with email:");
    for i in 0..id_column.row_count() {
        if let Some(age) = age_column.get_u8(i) {
            if age > 30 && email_column.get_string(i).is_some() {
                let id = id_column.get_u64(i).unwrap();
                let name = name_column.get_string(i).unwrap();
                let email = email_column.get_string(i).unwrap();
                println!("  User {}: {} (age: {}, email: {})", id, name, age, email);
            }
        }
    }
    
    // Demonstrate SIMD-accelerated operations (simulated)
    println!("\nPerforming SIMD-accelerated aggregation:");
    
    // Calculate average age (excluding nulls)
    let mut sum = 0u64;
    let mut count = 0u64;
    
    for i in 0..age_column.row_count() {
        if let Some(age) = age_column.get_u8(i) {
            sum += age as u64;
            count += 1;
        }
    }
    
    let avg_age = if count > 0 { sum as f64 / count as f64 } else { 0.0 };
    println!("  Average age: {:.1}", avg_age);
    
    // Count active vs. inactive users
    let mut active_count = 0u64;
    let mut inactive_count = 0u64;
    
    for i in 0..active_column.row_count() {
        if active_column.get_bool(i).unwrap() {
            active_count += 1;
        } else {
            inactive_count += 1;
        }
    }
    
    println!("  Active users: {}", active_count);
    println!("  Inactive users: {}", inactive_count);
    
    // Find the earliest and latest created_at timestamps
    let mut earliest = i64::MAX;
    let mut latest = i64::MIN;
    
    for i in 0..created_at_column.row_count() {
        let timestamp = created_at_column.get_timestamp(i).unwrap();
        earliest = earliest.min(timestamp);
        latest = latest.max(timestamp);
    }
    
    println!("  Earliest user created at: {}", earliest);
    println!("  Latest user created at: {}", latest);
    
    println!("\nExample completed successfully!");
    
    Ok(())
}