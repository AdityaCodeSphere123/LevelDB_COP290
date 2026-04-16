#include "table/range_deletion.h"

namespace leveldb {
void RangeDeletionList::Add(const Slice& start, const Slice& end,
                            SequenceNumber seq) {
  RangeDeletion del;
  del.start_key = start.ToString();
  del.end_key = end.ToString();
  del.seq = seq;
  deletions_.push_back(del);
}

bool RangeDeletionList::IsDeleted(const Slice& key, SequenceNumber found_seq,
                                  SequenceNumber read_seq) const {
  for (const auto& del : deletions_) {
    if (key.compare(del.start_key) >= 0 && key.compare(del.end_key) < 0) {
      if (del.seq <= read_seq && del.seq > found_seq) {
        return true;
      }
    }
  }
  return false;
}
}  // namespace leveldb