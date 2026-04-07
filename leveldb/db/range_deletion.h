#ifndef STORAGE_LEVELDB_DB_RANGE_DELETION_H_
#define STORAGE_LEVELDB_DB_RANGE_DELETION_H_

#include <string>
#include "db/dbformat.h"

namespace leveldb {

struct RangeDeletion{
  std::string start;
  std::string end;
  SequenceNumber seq;
};

} 

#endif  
