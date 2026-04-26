# LevelDB: Range Operations & Full Compaction Implementation

**Authors**: Aditya Anand & Neelkanth Mishra (IIT Delhi)

This project implements several key enhancements to the LevelDB storage engine, focusing on efficient range-based operations and manual maintenance capabilities. These features are critical for modern database workloads requiring bulk deletions, range queries, and explicit control over storage layout.

## Overview of Assignment Goals

The primary objective was to extend LevelDB's core functionality with:
1.  **Efficient Range Scanning**: A way to retrieve all key-value pairs within a specific interval.
2.  **Atomic Range Deletion**: A high-performance mechanism to delete blocks of keys without individual point deletions.
3.  **Manual Full Compaction**: A maintenance interface to optimize the entire database state on demand, with detailed performance reporting.

---

## Detailed Feature Report

### 1. Range Scan (`Scan`)
The `Scan` operation allows users to efficiently iterate through a range of keys.

-   **API**: `Status Scan(const ReadOptions& options, const Slice& start_key, const Slice& end_key, std::vector<std::pair<std::string, std::string>>* result)`
-   **Implementation**: 
    -   Utilizes LevelDB's internal `Iterator` to traverse from `start_key` to `end_key`.
    -   Designed to be thread-safe; it respects concurrent `ForceFullCompaction` operations by waiting if a maintenance task is in progress.
    -   Handles boundary conditions where `start_key >= end_key` by returning immediately, ensuring robust performance.

### 2. Range Delete (`DeleteRange`)
Instead of deleting keys one by one, `DeleteRange` provides a way to mark an entire range as deleted in a single O(1) operation (in terms of write volume).

-   **API**: `Status DeleteRange(const WriteOptions& options, const Slice& start_key, const Slice& end_key)`
-   **Core Innovations**:
    -   **Range Tombstones**: Introduced a new `ValueType`: `kTypeRangeDeletion` (0x2).
    -   **Efficiency**: Deletions are appended to the MemTable as "range tombstones" rather than scanning and deleting individual keys. This converts what would be an O(N) operation into an O(1) write.
    -   **Compaction Integration**: The compaction process was modified to recognize these tombstones. During SSTable creation, boundaries are automatically expanded to encompass range deletions, ensuring point lookups correctly identify deleted data even if the point data exists in lower levels.

### 3. Full Compaction (`ForceFullCompaction`)
A powerful maintenance tool that allows users to manually trigger a systematic cleanup of the entire database.

-   **API**: `Status ForceFullCompaction()`
-   **Mechanics**:
    -   **MemTable Flush**: Immediately flushes the current MemTable to Level 0 to ensure recent writes are included.
    -   **Global Lockdown**: Temporarily blocks concurrent reads and writes to ensure the database reaches a perfectly consistent and optimized state.
    -   **Sequential Cascading**: Iterates through every level of the LSM tree, merging files and purging obsolete data (overwritten values and deleted keys).
    -   **Trivial Moves**: Optimized to use "trivial moves" where possible (moving files between levels without rewriting) to minimize I/O overhead.
-   **Performance Reporting**:
    After execution, it prints a comprehensive `Compaction Report` to stdout, including:
    -   Number of compactions executed
    -   Number of input/output files processed
    -   Total bytes read and written
    -   Total elapsed time in milliseconds

---

## Technical Implementation Summary

-   **`WriteBatch` Enhancement**: Updated to support `DeleteRange` operations, allowing atomic bulk deletions within a single write batch.
-   **`DBImpl` Synchronization**: Added logic to handle the state transition during `ForceFullCompaction`, preventing deadlocks with background compaction threads.
-   **`VersionSet` Updates**: Enhanced the boundary detection logic to account for range tombstones, preventing data corruption during SSTable merging.

---

## Compilation and Build Instructions

The project uses CMake for building. Follow these steps to compile:

### 1. Build the Project
```bash
cd leveldb
mkdir -p build
cd build
cmake ..
make leveldb_tests
```

### 2. Run the Test Suite
We have added a comprehensive set of tests in `db_test.cc` that validate isolation, concurrency, and correctness of the new features.

To run all related compaction and range tests:
```bash
./leveldb_tests --gtest_filter="*FullCompaction*"
```

To run range scan specific tests:
```bash
./leveldb_tests --gtest_filter="*Scan*"
```