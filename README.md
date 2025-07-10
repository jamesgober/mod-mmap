<h1 align="center">
    <img width="90px" height="auto" src="https://raw.githubusercontent.com/jamesgober/jamesgober/main/media/icons/hexagon-3.svg" alt="Triple Hexagon">
    <br>
    <b>MemBase</b>
    <br>
    <sub>
        <sup>memory data mapping</sup>
    </sub>
    <br>
</h1>
<h2 align="center">
    Ultra-high-performance memmap library for Rust
    <br><br>
</h2>
<div align="center">
    <div>
        <a href="https://crates.io/crates/mod-mmap" alt="Mod MMap on Crates.io"><img alt="Crates.io" src="https://img.shields.io/crates/v/mod-mmap"></a>
        <span>&nbsp;</span>
        <a href="https://crates.io/crates/mod-mmap" alt="Download Mod MMap"><img alt="Crates.io Downloads" src="https://img.shields.io/crates/d/mod-mmap?color=%230099ff"></a>
        <span>&nbsp;</span>
        <a href="https://docs.rs/mod-mmap" title="Mod MMap Documentation"><img alt="docs.rs" src="https://img.shields.io/docsrs/mod-mmap"></a>
        <span>&nbsp;</span>
        <img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/jamesgober/cycle?mod-mmap=%23347d39" alt="last commit badge">
    </div>
    <br><br>
</div>
<div>
    <p>
    <strong>MemBase</strong> is a cross-platform, memory-mapped I/O library engineered for performance-critical systems. Optimized to deliver maximum throughput for extreme memory mapping operations and heavy data manipulation with minimal overhead.
    </p>
</div>

<br>

<h2>Performance Benchmarks</h2>

**Sequential Read Performance:**
- Standard I/O: 2,784 MB/s
- MemBase: 26,277 MB/s
- **9.4x faster** than traditional I/O

**Random Access Performance:**
- Standard I/O: 397,634 ops/s
- MemBase: 1,509,185 ops/s
- **3.8x faster** random access

<br>
<h2>Key Features</h2>

<h3>Zero-Copy Architecture</h3>
<p>
    Direct memory access eliminates buffer copying and system call overhead, delivering raw performance for data-intensive applications.
</p>


<h3>Advanced Memory Management</h3>
<p>
    Intelligent memory mapping strategies optimize for both sequential and random access patterns, automatically adapting to workload characteristics.
</p>

<h3>Cross-Platform Compatibility</h3>
<p>
    Native support across Windows, macOS, and Linux with platform-specific optimizations under a unified API.
</p>

<h3>Safety-First Design</h3>
<p>
    Memory-safe operations with Rust's ownership model, preventing common pitfalls like buffer overflows and use-after-free errors.
</p>

<h3>Integrated Database Operations</h3>
<p>
    Built-in database functionality with optimized query execution and in-place file modifications for persistent data structures.
</p>


<br>
<h2>Use Cases</h2>

- **High-Frequency Trading Systems** - Microsecond-critical market data processing
- **Database Engines** - Storage layer optimization for OLTP and OLAP workloads  
- **Real-Time Analytics** - Stream processing with persistent state management
- **Game Engines** - Asset loading and world state persistence
- **Scientific Computing** - Large dataset manipulation and analysis
- **Log Processing** - High-throughput log ingestion and analysis

<br>
<h2>Architecture</h2>

MemBase leverages OS-level memory mapping primitives with intelligent prefetching and cache-aware algorithms. The library automatically handles:

- Page fault optimization
- Memory pressure management  
- Multi-threaded access coordination
- Atomic operations for concurrent modifications

<br>
<h2>Usage</h2>

Add this to your `Cargo.toml`:

```toml
[dependencies]
MemMap = "0.1.0"
```

### Basic Example
```rust
use MemMap::{MmapOptions, Mmap};
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open a file
    let file = File::open("data.bin")?;

    // Create a memory map with default options
    let map = unsafe { MmapOptions::new().map(&file)? };

    // Access the memory map
    if map.len() >= 8 {
        let value = unsafe { *(map.as_ptr() as *const u64) };
        println!("First 8 bytes as u64: {}", value);
    }

    Ok(())
}
```

### Advanced Example
```rust
use MemMap::{MmapOptions, Mmap, HugePageSize, PrefetchStrategy};
use MemMap::platform::Advice;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open a file
    let file = File::open("large_data.bin")?;

    // Create a memory map with advanced options
    let map = unsafe {
        MmapOptions::new()
            .read(true)
            .write(true)
            // Use huge pages if available
            .huge_pages(HugePageSize::TwoMB)
            // Use sequential prefetching
            .prefetch(PrefetchStrategy::Sequential)
            // Populate the mapping immediately
            .populate(true)
            // Map the file
            .map_mut(&file)?
    };

    // Advise the kernel about our access pattern
    map.advise(Advice::Sequential)?;

    // Use the memory map
    // ...

    // Flush changes to disk
    map.flush()?;

    Ok(())
}
```


<br>
<!--
:: LICENSE
============================================================================ -->
<div id="license">
    <hr>
    <h2>📌 License</h2>
    <p>Licensed under the <b>Apache License</b>, version 2.0 (the <b>"License"</b>); you may not use this software, including, but not limited to the source code, media files, ideas, techniques, or any other associated property or concept belonging to, associated with, or otherwise packaged with this software except in compliance with the <b>License</b>.</p>
    <p>You may obtain a copy of the <b>License</b> at: <a href="http://www.apache.org/licenses/LICENSE-2.0" title="Apache-2.0 License" target="_blank">http://www.apache.org/licenses/LICENSE-2.0</a>.</p>
    <p>Unless required by applicable law or agreed to in writing, software distributed under the <b>License</b> is distributed on an "<b>AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND</b>, either express or implied.</p>
    <p>See the <a href="./LICENSE" title="Software License file">LICENSE</a> file included with this project for the specific language governing permissions and limitations under the <b>License</b>.</p>
    <br>
</div>



<!--
:: COPYRIGHT
============================================================================ -->
<div align="center">
  <br>
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2025 <strong>JAMES GOBER.</strong></sup>
</div>
