//! Columnar database functionality built on top of the memory mapping library.
//!
//! This module provides a high-performance columnar database implementation
//! that uses memory mapping for storage and efficient data access.

pub mod schema;
pub mod column;
pub mod table;
pub mod query;
pub mod index;
pub mod compression;
pub mod storage;

pub use schema::{Schema, Field, DataType};
pub use column::{Column, ColumnBuilder, ColumnIterator};
pub use table::{Table, TableBuilder};
pub use query::{Query, QueryBuilder, Predicate, Operator};
pub use index::{Index, IndexType};
pub use compression::{Compression, CompressionType};
pub use storage::{Storage, StorageOptions};