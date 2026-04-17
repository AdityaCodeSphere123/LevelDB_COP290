# LevelDB 

### 1. Build
The project uses CMake. To build the test suite:

```bash
cd leveldb
mkdir -p build
cd build
cmake ..
make leveldb_tests
```

### 2. Run Tests
We have added a comprehensive test suite in `db_test.cc` that covers basic, multi-level, isolation, and concurrent manual compaction scenarios.

To run all related tests:
```bash
./leveldb_tests --gtest_filter="DBTest.ForceFullCompaction*"
```