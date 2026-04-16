#ifndef LEVELDB_RANGE_DELETION_H
#define LEVELDB_RANGE_DELETION_H

#include "db/dbformat.h"
#include <string>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct RangeDeletion {
  std::string start_key;
  std::string end_key;
  SequenceNumber seq;
};

class RangeDeletionList {
 public:
  RangeDeletionList() = default;

  void Add(const Slice& start, const Slice& end, SequenceNumber seq);
  bool IsDeleted(const Slice& key, SequenceNumber found_seq,
                 SequenceNumber read_seq) const;

 private:
  std::vector<RangeDeletion> deletions_;
};

}  // namespace leveldb

#endif  // LEVELDB_RANGE_DELETION_H