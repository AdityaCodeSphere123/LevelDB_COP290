#include <iostream>
#include <string>

#include "leveldb/db.h"

int main() {
  leveldb::DB* db = nullptr;
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status status =
      leveldb::DB::Open(options, "/tmp/testdb_cop290_demo", &db);
  if (!status.ok()) {
    std::cerr << "Failed to open database: " << status.ToString() << std::endl;
    return 1;
  }

  leveldb::WriteOptions write_options;
  leveldb::ReadOptions read_options;

  status = db->Put(write_options, "key1", "value1");
  if (!status.ok()) {
    std::cerr << "Put(key1) failed: " << status.ToString() << std::endl;
    delete db;
    return 1;
  }

  status = db->Put(write_options, "key2", "value2");
  if (!status.ok()) {
    std::cerr << "Put(key2) failed: " << status.ToString() << std::endl;
    delete db;
    return 1;
  }

  std::string value;
  status = db->Get(read_options, "key1", &value);
  if (status.ok()) {
    std::cout << "key1 => " << value << std::endl;
  } else {
    std::cerr << "Get(key1) failed: " << status.ToString() << std::endl;
  }

  status = db->Delete(write_options, "key2");
  if (!status.ok()) {
    std::cerr << "Delete(key2) failed: " << status.ToString() << std::endl;
    delete db;
    return 1;
  }

  delete db;
  return 0;
}
