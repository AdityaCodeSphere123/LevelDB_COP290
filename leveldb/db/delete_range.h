#ifndef STORAGE_LEVELDB_DB_DELETE_RANGE_H_
#define STORAGE_LEVELDB_DB_DELETE_RANGE_H_

#include <string>
#include "db/dbformat.h"

namespace leveldb {

struct DeleteRanges{
  std::string start;
  std::string end;
  SequenceNumber seq;
};

}  // namespace leveldb

#endif 
