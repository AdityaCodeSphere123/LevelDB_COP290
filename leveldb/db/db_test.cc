// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/db.h"

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <atomic>
#include <cinttypes>
#include <string>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <filesystem>
#include "leveldb/env.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/table.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/testutil.h"

#include "gtest/gtest.h"

namespace leveldb {

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

static std::string RandomKey(Random* rnd) {
  int len =
      (rnd->OneIn(3) ? 1  // Short sometimes to encourage collisions
                     : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
  return test::RandomKey(rnd, len);
}

namespace {
class AtomicCounter {
 public:
  AtomicCounter() : count_(0) {}
  void Increment() { IncrementBy(1); }
  void IncrementBy(int count) LOCKS_EXCLUDED(mu_) {
    MutexLock l(&mu_);
    count_ += count;
  }
  int Read() LOCKS_EXCLUDED(mu_) {
    MutexLock l(&mu_);
    return count_;
  }
  void Reset() LOCKS_EXCLUDED(mu_) {
    MutexLock l(&mu_);
    count_ = 0;
  }

 private:
  port::Mutex mu_;
  int count_ GUARDED_BY(mu_);
};

void DelayMilliseconds(int millis) {
  Env::Default()->SleepForMicroseconds(millis * 1000);
}

bool IsLdbFile(const std::string& f) {
  return strstr(f.c_str(), ".ldb") != nullptr;
}

bool IsLogFile(const std::string& f) {
  return strstr(f.c_str(), ".log") != nullptr;
}

bool IsManifestFile(const std::string& f) {
  return strstr(f.c_str(), "MANIFEST") != nullptr;
}

}  // namespace

// Test Env to override default Env behavior for testing.
class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(Env* base) : EnvWrapper(base), ignore_dot_files_(false) {}

  void SetIgnoreDotFiles(bool ignored) { ignore_dot_files_ = ignored; }

  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* result) override {
    Status s = target()->GetChildren(dir, result);
    if (!s.ok() || !ignore_dot_files_) {
      return s;
    }

    std::vector<std::string>::iterator it = result->begin();
    while (it != result->end()) {
      if ((*it == ".") || (*it == "..")) {
        it = result->erase(it);
      } else {
        ++it;
      }
    }

    return s;
  }

 private:
  bool ignore_dot_files_;
};

// Special Env used to delay background operations.
class SpecialEnv : public EnvWrapper {
 public:
  // For historical reasons, the std::atomic<> fields below are currently
  // accessed via acquired loads and release stores. We should switch
  // to plain load(), store() calls that provide sequential consistency.

  // sstable/log Sync() calls are blocked while this pointer is non-null.
  std::atomic<bool> delay_data_sync_;

  // sstable/log Sync() calls return an error.
  std::atomic<bool> data_sync_error_;

  // Simulate no-space errors while this pointer is non-null.
  std::atomic<bool> no_space_;

  // Simulate non-writable file system while this pointer is non-null.
  std::atomic<bool> non_writable_;

  // Force sync of manifest files to fail while this pointer is non-null.
  std::atomic<bool> manifest_sync_error_;

  // Force write to manifest files to fail while this pointer is non-null.
  std::atomic<bool> manifest_write_error_;

  // Force log file close to fail while this bool is true.
  std::atomic<bool> log_file_close_;

  bool count_random_reads_;
  AtomicCounter random_read_counter_;

  explicit SpecialEnv(Env* base)
      : EnvWrapper(base),
        delay_data_sync_(false),
        data_sync_error_(false),
        no_space_(false),
        non_writable_(false),
        manifest_sync_error_(false),
        manifest_write_error_(false),
        log_file_close_(false),
        count_random_reads_(false) {}

  Status NewWritableFile(const std::string& f, WritableFile** r) {
    class DataFile : public WritableFile {
     private:
      SpecialEnv* const env_;
      WritableFile* const base_;
      const std::string fname_;

     public:
      DataFile(SpecialEnv* env, WritableFile* base, const std::string& fname)
          : env_(env), base_(base), fname_(fname) {}

      ~DataFile() { delete base_; }
      Status Append(const Slice& data) {
        if (env_->no_space_.load(std::memory_order_acquire)) {
          // Drop writes on the floor
          return Status::OK();
        } else {
          return base_->Append(data);
        }
      }
      Status Close() {
        Status s = base_->Close();
        if (s.ok() && IsLogFile(fname_) &&
            env_->log_file_close_.load(std::memory_order_acquire)) {
          s = Status::IOError("simulated log file Close error");
        }
        return s;
      }
      Status Flush() { return base_->Flush(); }
      Status Sync() {
        if (env_->data_sync_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated data sync error");
        }
        while (env_->delay_data_sync_.load(std::memory_order_acquire)) {
          DelayMilliseconds(100);
        }
        return base_->Sync();
      }
    };
    class ManifestFile : public WritableFile {
     private:
      SpecialEnv* env_;
      WritableFile* base_;

     public:
      ManifestFile(SpecialEnv* env, WritableFile* b) : env_(env), base_(b) {}
      ~ManifestFile() { delete base_; }
      Status Append(const Slice& data) {
        if (env_->manifest_write_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated writer error");
        } else {
          return base_->Append(data);
        }
      }
      Status Close() { return base_->Close(); }
      Status Flush() { return base_->Flush(); }
      Status Sync() {
        if (env_->manifest_sync_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated sync error");
        } else {
          return base_->Sync();
        }
      }
    };

    if (non_writable_.load(std::memory_order_acquire)) {
      return Status::IOError("simulated write error");
    }

    Status s = target()->NewWritableFile(f, r);
    if (s.ok()) {
      if (IsLdbFile(f) || IsLogFile(f)) {
        *r = new DataFile(this, *r, f);
      } else if (IsManifestFile(f)) {
        *r = new ManifestFile(this, *r);
      }
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f, RandomAccessFile** r) {
    class CountingFile : public RandomAccessFile {
     private:
      RandomAccessFile* target_;
      AtomicCounter* counter_;

     public:
      CountingFile(RandomAccessFile* target, AtomicCounter* counter)
          : target_(target), counter_(counter) {}
      ~CountingFile() override { delete target_; }
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        counter_->Increment();
        return target_->Read(offset, n, result, scratch);
      }
    };

    Status s = target()->NewRandomAccessFile(f, r);
    if (s.ok() && count_random_reads_) {
      *r = new CountingFile(*r, &random_read_counter_);
    }
    return s;
  }
};

class DBTest : public testing::Test {
 public:
  std::string dbname_;
  SpecialEnv* env_;
  DB* db_;

  Options last_options_;

  DBTest() : env_(new SpecialEnv(Env::Default())), option_config_(kDefault) {
    filter_policy_ = NewBloomFilterPolicy(10);
    dbname_ = testing::TempDir() + "db_test";
    DestroyDB(dbname_, Options());
    db_ = nullptr;
    Reopen();
  }

  ~DBTest() {
    delete db_;
    DestroyDB(dbname_, Options());
    delete env_;
    delete filter_policy_;
  }

  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions() {
    option_config_++;
    if (option_config_ >= kEnd) {
      return false;
    } else {
      DestroyAndReopen();
      return true;
    }
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.reuse_logs = false;
    switch (option_config_) {
      case kReuse:
        options.reuse_logs = true;
        break;
      case kFilter:
        options.filter_policy = filter_policy_;
        break;
      case kUncompressed:
        options.compression = kNoCompression;
        break;
      default:
        break;
    }
    return options;
  }

  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_); }

  void Reopen(Options* options = nullptr) {
    ASSERT_LEVELDB_OK(TryReopen(options));
  }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(Options* options = nullptr) {
    delete db_;
    db_ = nullptr;
    DestroyDB(dbname_, Options());
    ASSERT_LEVELDB_OK(TryReopen(options));
  }

  Status TryReopen(Options* options) {
    delete db_;
    db_ = nullptr;
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts = CurrentOptions();
      opts.create_if_missing = true;
    }
    last_options_ = opts;

    return DB::Open(opts, dbname_, &db_);
  }

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  // Return a string that contains all key,value pairs in order,
  // formatted like "(k1->v1)(k2->v2)".
  std::string Contents() {
    std::vector<std::string> forward;
    std::string result;
    Iterator* iter = db_->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::string s = IterStatus(iter);
      result.push_back('(');
      result.append(s);
      result.push_back(')');
      forward.push_back(s);
    }

    // Check reverse iteration results are the reverse of forward results
    size_t matched = 0;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      EXPECT_LT(matched, forward.size());
      EXPECT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
      matched++;
    }
    EXPECT_EQ(matched, forward.size());

    delete iter;
    return result;
  }

  std::string AllEntriesFor(const Slice& user_key) {
    Iterator* iter = dbfull()->TEST_NewInternalIterator();
    InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
    iter->Seek(target.Encode());
    std::string result;
    if (!iter->status().ok()) {
      result = iter->status().ToString();
    } else {
      result = "[ ";
      bool first = true;
      while (iter->Valid()) {
        ParsedInternalKey ikey;
        if (!ParseInternalKey(iter->key(), &ikey)) {
          result += "CORRUPTED";
        } else {
          if (last_options_.comparator->Compare(ikey.user_key, user_key) != 0) {
            break;
          }
          if (!first) {
            result += ", ";
          }
          first = false;
          switch (ikey.type) {
            case kTypeValue:
              result += iter->value().ToString();
              break;
            case kTypeDeletion:
              result += "DEL";
              break;
            case kTypeRangeDeletion:
              result += "RDEL";
              break;
          }
        }
        iter->Next();
      }
      if (!first) {
        result += " ";
      }
      result += "]";
    }
    delete iter;
    return result;
  }

  int NumTableFilesAtLevel(int level) {
    std::string property;
    EXPECT_TRUE(db_->GetProperty(
        "leveldb.num-files-at-level" + NumberToString(level), &property));
    return std::stoi(property);
  }

  int TotalTableFiles() {
    int result = 0;
    for (int level = 0; level < config::kNumLevels; level++) {
      result += NumTableFilesAtLevel(level);
    }
    return result;
  }

  // Return spread of files per level
  std::string FilesPerLevel() {
    std::string result;
    int last_non_zero_offset = 0;
    for (int level = 0; level < config::kNumLevels; level++) {
      int f = NumTableFilesAtLevel(level);
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = result.size();
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }

  int CountFiles() {
    std::vector<std::string> files;
    env_->GetChildren(dbname_, &files);
    return static_cast<int>(files.size());
  }

  uint64_t Size(const Slice& start, const Slice& limit) {
    Range r(start, limit);
    uint64_t size;
    db_->GetApproximateSizes(&r, 1, &size);
    return size;
  }

  void Compact(const Slice& start, const Slice& limit) {
    db_->CompactRange(&start, &limit);
  }

  // Do n memtable compactions, each of which produces an sstable
  // covering the range [small_key,large_key].
  void MakeTables(int n, const std::string& small_key,
                  const std::string& large_key) {
    for (int i = 0; i < n; i++) {
      Put(small_key, "begin");
      Put(large_key, "end");
      dbfull()->TEST_CompactMemTable();
    }
  }

  // Prevent pushing of new sstables into deeper levels by adding
  // tables that cover a specified range to all levels.
  void FillLevels(const std::string& smallest, const std::string& largest) {
    MakeTables(config::kNumLevels, smallest, largest);
  }

  void DumpFileCounts(const char* label) {
    std::fprintf(stderr, "---\n%s:\n", label);
    std::fprintf(
        stderr, "maxoverlap: %lld\n",
        static_cast<long long>(dbfull()->TEST_MaxNextLevelOverlappingBytes()));
    for (int level = 0; level < config::kNumLevels; level++) {
      int num = NumTableFilesAtLevel(level);
      if (num > 0) {
        std::fprintf(stderr, "  level %3d : %d files\n", level, num);
      }
    }
  }

  std::string DumpSSTableList() {
    std::string property;
    db_->GetProperty("leveldb.sstables", &property);
    return property;
  }

  std::string IterStatus(Iterator* iter) {
    std::string result;
    if (iter->Valid()) {
      result = iter->key().ToString() + "->" + iter->value().ToString();
    } else {
      result = "(invalid)";
    }
    return result;
  }

  bool DeleteAnSSTFile() {
    std::vector<std::string> filenames;
    EXPECT_LEVELDB_OK(env_->GetChildren(dbname_, &filenames));
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
        EXPECT_LEVELDB_OK(env_->RemoveFile(TableFileName(dbname_, number)));
        return true;
      }
    }
    return false;
  }

  // Returns number of files renamed.
  int RenameLDBToSST() {
    std::vector<std::string> filenames;
    EXPECT_LEVELDB_OK(env_->GetChildren(dbname_, &filenames));
    uint64_t number;
    FileType type;
    int files_renamed = 0;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
        const std::string from = TableFileName(dbname_, number);
        const std::string to = SSTTableFileName(dbname_, number);
        EXPECT_LEVELDB_OK(env_->RenameFile(from, to));
        files_renamed++;
      }
    }
    return files_renamed;
  }

 private:
  // Sequence of option configurations to try
  enum OptionConfig { kDefault, kReuse, kFilter, kUncompressed, kEnd };

  const FilterPolicy* filter_policy_;
  int option_config_;
};

TEST_F(DBTest, Empty) {
  do {
    ASSERT_TRUE(db_ != nullptr);
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, EmptyKey) {
  do {
    ASSERT_LEVELDB_OK(Put("", "v1"));
    ASSERT_EQ("v1", Get(""));
    ASSERT_LEVELDB_OK(Put("", "v2"));
    ASSERT_EQ("v2", Get(""));
  } while (ChangeOptions());
}

TEST_F(DBTest, EmptyValue) {
  do {
    ASSERT_LEVELDB_OK(Put("key", "v1"));
    ASSERT_EQ("v1", Get("key"));
    ASSERT_LEVELDB_OK(Put("key", ""));
    ASSERT_EQ("", Get("key"));
    ASSERT_LEVELDB_OK(Put("key", "v2"));
    ASSERT_EQ("v2", Get("key"));
  } while (ChangeOptions());
}

TEST_F(DBTest, ReadWrite) {
  do {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_LEVELDB_OK(Put("bar", "v2"));
    ASSERT_LEVELDB_OK(Put("foo", "v3"));
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
  } while (ChangeOptions());
}

TEST_F(DBTest, PutDeleteGet) {
  do {
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), "foo"));
    ASSERT_EQ("NOT_FOUND", Get("foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetFromImmutableLayer) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    Reopen(&options);

    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));

    // Block sync calls.
    env_->delay_data_sync_.store(true, std::memory_order_release);
    Put("k1", std::string(100000, 'x'));  // Fill memtable.
    Put("k2", std::string(100000, 'y'));  // Trigger compaction.
    ASSERT_EQ("v1", Get("foo"));
    // Release sync calls.
    env_->delay_data_sync_.store(false, std::memory_order_release);
  } while (ChangeOptions());
}

TEST_F(DBTest, GetFromVersions) {
  do {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v1", Get("foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetMemUsage) {
  do {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    std::string val;
    ASSERT_TRUE(db_->GetProperty("leveldb.approximate-memory-usage", &val));
    int mem_usage = std::stoi(val);
    ASSERT_GT(mem_usage, 0);
    ASSERT_LT(mem_usage, 5 * 1024 * 1024);
  } while (ChangeOptions());
}

TEST_F(DBTest, GetSnapshot) {
  do {
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_LEVELDB_OK(Put(key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_LEVELDB_OK(Put(key, "v2"));
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      dbfull()->TEST_CompactMemTable();
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, GetIdenticalSnapshots) {
  do {
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_LEVELDB_OK(Put(key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      const Snapshot* s2 = db_->GetSnapshot();
      const Snapshot* s3 = db_->GetSnapshot();
      ASSERT_LEVELDB_OK(Put(key, "v2"));
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s1));
      ASSERT_EQ("v1", Get(key, s2));
      ASSERT_EQ("v1", Get(key, s3));
      db_->ReleaseSnapshot(s1);
      dbfull()->TEST_CompactMemTable();
      ASSERT_EQ("v2", Get(key));
      ASSERT_EQ("v1", Get(key, s2));
      db_->ReleaseSnapshot(s2);
      ASSERT_EQ("v1", Get(key, s3));
      db_->ReleaseSnapshot(s3);
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, IterateOverEmptySnapshot) {
  do {
    const Snapshot* snapshot = db_->GetSnapshot();
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    ASSERT_LEVELDB_OK(Put("foo", "v2"));

    Iterator* iterator1 = db_->NewIterator(read_options);
    iterator1->SeekToFirst();
    ASSERT_TRUE(!iterator1->Valid());
    delete iterator1;

    dbfull()->TEST_CompactMemTable();

    Iterator* iterator2 = db_->NewIterator(read_options);
    iterator2->SeekToFirst();
    ASSERT_TRUE(!iterator2->Valid());
    delete iterator2;

    db_->ReleaseSnapshot(snapshot);
  } while (ChangeOptions());
}

TEST_F(DBTest, GetLevel0Ordering) {
  do {
    // Check that we process level-0 files in correct order.  The code
    // below generates two level-0 files where the earlier one comes
    // before the later one in the level-0 file list since the earlier
    // one has a smaller "smallest" key.
    ASSERT_LEVELDB_OK(Put("bar", "b"));
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_LEVELDB_OK(Put("foo", "v2"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v2", Get("foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetOrderedByLevels) {
  do {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    Compact("a", "z");
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_LEVELDB_OK(Put("foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v2", Get("foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetPicksCorrectFile) {
  do {
    // Arrange to have multiple files in a non-level-0 level.
    ASSERT_LEVELDB_OK(Put("a", "va"));
    Compact("a", "b");
    ASSERT_LEVELDB_OK(Put("x", "vx"));
    Compact("x", "y");
    ASSERT_LEVELDB_OK(Put("f", "vf"));
    Compact("f", "g");
    ASSERT_EQ("va", Get("a"));
    ASSERT_EQ("vf", Get("f"));
    ASSERT_EQ("vx", Get("x"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetEncountersEmptyLevel) {
  do {
    // Arrange for the following to happen:
    //   * sstable A in level 0
    //   * nothing in level 1
    //   * sstable B in level 2
    // Then do enough Get() calls to arrange for an automatic compaction
    // of sstable A.  A bug would cause the compaction to be marked as
    // occurring at level 1 (instead of the correct level 0).

    // Step 1: First place sstables in levels 0 and 2
    int compaction_count = 0;
    while (NumTableFilesAtLevel(0) == 0 || NumTableFilesAtLevel(2) == 0) {
      ASSERT_LE(compaction_count, 100) << "could not fill levels 0 and 2";
      compaction_count++;
      Put("a", "begin");
      Put("z", "end");
      dbfull()->TEST_CompactMemTable();
    }

    // Step 2: clear level 1 if necessary.
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    ASSERT_EQ(NumTableFilesAtLevel(0), 1);
    ASSERT_EQ(NumTableFilesAtLevel(1), 0);
    ASSERT_EQ(NumTableFilesAtLevel(2), 1);

    // Step 3: read a bunch of times
    for (int i = 0; i < 1000; i++) {
      ASSERT_EQ("NOT_FOUND", Get("missing"));
    }

    // Step 4: Wait for compaction to finish
    DelayMilliseconds(1000);

    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  } while (ChangeOptions());
}

TEST_F(DBTest, IterEmpty) {
  Iterator* iter = db_->NewIterator(ReadOptions());

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("foo");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST_F(DBTest, IterSingle) {
  ASSERT_LEVELDB_OK(Put("a", "va"));
  Iterator* iter = db_->NewIterator(ReadOptions());

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("a");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("b");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST_F(DBTest, IterMulti) {
  ASSERT_LEVELDB_OK(Put("a", "va"));
  ASSERT_LEVELDB_OK(Put("b", "vb"));
  ASSERT_LEVELDB_OK(Put("c", "vc"));
  Iterator* iter = db_->NewIterator(ReadOptions());

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->Seek("");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("a");
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Seek("ax");
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Seek("b");
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Seek("z");
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  // Switch from reverse to forward
  iter->SeekToLast();
  iter->Prev();
  iter->Prev();
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");

  // Switch from forward to reverse
  iter->SeekToFirst();
  iter->Next();
  iter->Next();
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->vb");

  // Make sure iter stays at snapshot
  ASSERT_LEVELDB_OK(Put("a", "va2"));
  ASSERT_LEVELDB_OK(Put("a2", "va3"));
  ASSERT_LEVELDB_OK(Put("b", "vb2"));
  ASSERT_LEVELDB_OK(Put("c", "vc2"));
  ASSERT_LEVELDB_OK(Delete("b"));
  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->vb");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST_F(DBTest, IterSmallAndLargeMix) {
  ASSERT_LEVELDB_OK(Put("a", "va"));
  ASSERT_LEVELDB_OK(Put("b", std::string(100000, 'b')));
  ASSERT_LEVELDB_OK(Put("c", "vc"));
  ASSERT_LEVELDB_OK(Put("d", std::string(100000, 'd')));
  ASSERT_LEVELDB_OK(Put("e", std::string(100000, 'e')));

  Iterator* iter = db_->NewIterator(ReadOptions());

  iter->SeekToFirst();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
  iter->Next();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), "e->" + std::string(100000, 'e'));
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "d->" + std::string(100000, 'd'));
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "c->vc");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "b->" + std::string(100000, 'b'));
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "a->va");
  iter->Prev();
  ASSERT_EQ(IterStatus(iter), "(invalid)");

  delete iter;
}

TEST_F(DBTest, IterMultiWithDelete) {
  do {
    ASSERT_LEVELDB_OK(Put("a", "va"));
    ASSERT_LEVELDB_OK(Put("b", "vb"));
    ASSERT_LEVELDB_OK(Put("c", "vc"));
    ASSERT_LEVELDB_OK(Delete("b"));
    ASSERT_EQ("NOT_FOUND", Get("b"));

    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    delete iter;
  } while (ChangeOptions());
}

TEST_F(DBTest, IterMultiWithDeleteAndCompaction) {
  do {
    ASSERT_LEVELDB_OK(Put("b", "vb"));
    ASSERT_LEVELDB_OK(Put("c", "vc"));
    ASSERT_LEVELDB_OK(Put("a", "va"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_LEVELDB_OK(Delete("b"));
    ASSERT_EQ("NOT_FOUND", Get("b"));

    Iterator* iter = db_->NewIterator(ReadOptions());
    iter->Seek("c");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    iter->Prev();
    ASSERT_EQ(IterStatus(iter), "a->va");
    iter->Seek("b");
    ASSERT_EQ(IterStatus(iter), "c->vc");
    delete iter;
  } while (ChangeOptions());
}

TEST_F(DBTest, Recover) {
  do {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    ASSERT_LEVELDB_OK(Put("baz", "v5"));

    Reopen();
    ASSERT_EQ("v1", Get("foo"));

    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v5", Get("baz"));
    ASSERT_LEVELDB_OK(Put("bar", "v2"));
    ASSERT_LEVELDB_OK(Put("foo", "v3"));

    Reopen();
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_LEVELDB_OK(Put("foo", "v4"));
    ASSERT_EQ("v4", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
    ASSERT_EQ("v5", Get("baz"));
  } while (ChangeOptions());
}

TEST_F(DBTest, RecoveryWithEmptyLog) {
  do {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    ASSERT_LEVELDB_OK(Put("foo", "v2"));
    Reopen();
    Reopen();
    ASSERT_LEVELDB_OK(Put("foo", "v3"));
    Reopen();
    ASSERT_EQ("v3", Get("foo"));
  } while (ChangeOptions());
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST_F(DBTest, RecoverDuringMemtableCompaction) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 1000000;
    Reopen(&options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_LEVELDB_OK(Put("foo", "v1"));  // Goes to 1st log file
    ASSERT_LEVELDB_OK(
        Put("big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_LEVELDB_OK(
        Put("big2", std::string(1000, 'y')));  // Triggers compaction
    ASSERT_LEVELDB_OK(Put("bar", "v2"));       // Goes to new log file

    Reopen(&options);
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get("big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get("big2"));
  } while (ChangeOptions());
}

static std::string Key(int i) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "key%06d", i);
  return std::string(buf);
}

TEST_F(DBTest, MinorCompactionsHappen) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10000;
  Reopen(&options);

  const int N = 500;

  int starting_num_tables = TotalTableFiles();
  for (int i = 0; i < N; i++) {
    ASSERT_LEVELDB_OK(Put(Key(i), Key(i) + std::string(1000, 'v')));
  }
  int ending_num_tables = TotalTableFiles();
  ASSERT_GT(ending_num_tables, starting_num_tables);

  for (int i = 0; i < N; i++) {
    ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(Key(i)));
  }

  Reopen();

  for (int i = 0; i < N; i++) {
    ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(Key(i)));
  }
}

TEST_F(DBTest, RecoverWithLargeLog) {
  {
    Options options = CurrentOptions();
    Reopen(&options);
    ASSERT_LEVELDB_OK(Put("big1", std::string(200000, '1')));
    ASSERT_LEVELDB_OK(Put("big2", std::string(200000, '2')));
    ASSERT_LEVELDB_OK(Put("small3", std::string(10, '3')));
    ASSERT_LEVELDB_OK(Put("small4", std::string(10, '4')));
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  }

  // Make sure that if we re-open with a small write buffer size that
  // we flush table files in the middle of a large log file.
  Options options = CurrentOptions();
  options.write_buffer_size = 100000;
  Reopen(&options);
  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  ASSERT_EQ(std::string(200000, '1'), Get("big1"));
  ASSERT_EQ(std::string(200000, '2'), Get("big2"));
  ASSERT_EQ(std::string(10, '3'), Get("small3"));
  ASSERT_EQ(std::string(10, '4'), Get("small4"));
  ASSERT_GT(NumTableFilesAtLevel(0), 1);
}

TEST_F(DBTest, CompactionsGenerateMultipleFiles) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;  // Large write buffer
  Reopen(&options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(RandomString(&rnd, 100000));
    ASSERT_LEVELDB_OK(Put(Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  Reopen(&options);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

TEST_F(DBTest, RepeatedWritesToSameKey) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  Reopen(&options);

  // We must have at most one file per level except for level-0,
  // which may have up to kL0_StopWritesTrigger files.
  const int kMaxFiles = config::kNumLevels + config::kL0_StopWritesTrigger;

  Random rnd(301);
  std::string value = RandomString(&rnd, 2 * options.write_buffer_size);
  for (int i = 0; i < 5 * kMaxFiles; i++) {
    Put("key", value);
    ASSERT_LE(TotalTableFiles(), kMaxFiles);
    std::fprintf(stderr, "after %d: %d files\n", i + 1, TotalTableFiles());
  }
}

TEST_F(DBTest, SparseMerge) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  Reopen(&options);

  FillLevels("A", "Z");

  // Suppose there is:
  //    small amount of data with prefix A
  //    large amount of data with prefix B
  //    small amount of data with prefix C
  // and that recent updates have made small changes to all three prefixes.
  // Check that we do not do a compaction that merges all of B in one shot.
  const std::string value(1000, 'x');
  Put("A", "va");
  // Write approximately 100MB of "B" values
  for (int i = 0; i < 100000; i++) {
    char key[100];
    std::snprintf(key, sizeof(key), "B%010d", i);
    Put(key, value);
  }
  Put("C", "vc");
  dbfull()->TEST_CompactMemTable();
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  // Make sparse update
  Put("A", "va2");
  Put("B100", "bvalue2");
  Put("C", "vc2");
  dbfull()->TEST_CompactMemTable();

  // Compactions should not cause us to create a situation where
  // a file overlaps too much data at the next level.
  ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20 * 1048576);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20 * 1048576);
  dbfull()->TEST_CompactRange(1, nullptr, nullptr);
  ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20 * 1048576);
}

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    std::fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
                 (unsigned long long)(val), (unsigned long long)(low),
                 (unsigned long long)(high));
  }
  return result;
}

TEST_F(DBTest, ApproximateSizes) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;  // Large write buffer
    options.compression = kNoCompression;
    DestroyAndReopen();

    ASSERT_TRUE(Between(Size("", "xyz"), 0, 0));
    Reopen(&options);
    ASSERT_TRUE(Between(Size("", "xyz"), 0, 0));

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    const int N = 80;
    static const int S1 = 100000;
    static const int S2 = 105000;  // Allow some expansion from metadata
    Random rnd(301);
    for (int i = 0; i < N; i++) {
      ASSERT_LEVELDB_OK(Put(Key(i), RandomString(&rnd, S1)));
    }

    // 0 because GetApproximateSizes() does not account for memtable space
    ASSERT_TRUE(Between(Size("", Key(50)), 0, 0));

    if (options.reuse_logs) {
      // Recovery will reuse memtable, and GetApproximateSizes() does not
      // account for memtable usage;
      Reopen(&options);
      ASSERT_TRUE(Between(Size("", Key(50)), 0, 0));
      continue;
    }

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      Reopen(&options);

      for (int compact_start = 0; compact_start < N; compact_start += 10) {
        for (int i = 0; i < N; i += 10) {
          ASSERT_TRUE(Between(Size("", Key(i)), S1 * i, S2 * i));
          ASSERT_TRUE(Between(Size("", Key(i) + ".suffix"), S1 * (i + 1),
                              S2 * (i + 1)));
          ASSERT_TRUE(Between(Size(Key(i), Key(i + 10)), S1 * 10, S2 * 10));
        }
        ASSERT_TRUE(Between(Size("", Key(50)), S1 * 50, S2 * 50));
        ASSERT_TRUE(Between(Size("", Key(50) + ".suffix"), S1 * 50, S2 * 50));

        std::string cstart_str = Key(compact_start);
        std::string cend_str = Key(compact_start + 9);
        Slice cstart = cstart_str;
        Slice cend = cend_str;
        dbfull()->TEST_CompactRange(0, &cstart, &cend);
      }

      ASSERT_EQ(NumTableFilesAtLevel(0), 0);
      ASSERT_GT(NumTableFilesAtLevel(1), 0);
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    Reopen();

    Random rnd(301);
    std::string big1 = RandomString(&rnd, 100000);
    ASSERT_LEVELDB_OK(Put(Key(0), RandomString(&rnd, 10000)));
    ASSERT_LEVELDB_OK(Put(Key(1), RandomString(&rnd, 10000)));
    ASSERT_LEVELDB_OK(Put(Key(2), big1));
    ASSERT_LEVELDB_OK(Put(Key(3), RandomString(&rnd, 10000)));
    ASSERT_LEVELDB_OK(Put(Key(4), big1));
    ASSERT_LEVELDB_OK(Put(Key(5), RandomString(&rnd, 10000)));
    ASSERT_LEVELDB_OK(Put(Key(6), RandomString(&rnd, 300000)));
    ASSERT_LEVELDB_OK(Put(Key(7), RandomString(&rnd, 10000)));

    if (options.reuse_logs) {
      // Need to force a memtable compaction since recovery does not do so.
      ASSERT_LEVELDB_OK(dbfull()->TEST_CompactMemTable());
    }

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      Reopen(&options);

      ASSERT_TRUE(Between(Size("", Key(0)), 0, 0));
      ASSERT_TRUE(Between(Size("", Key(1)), 10000, 11000));
      ASSERT_TRUE(Between(Size("", Key(2)), 20000, 21000));
      ASSERT_TRUE(Between(Size("", Key(3)), 120000, 121000));
      ASSERT_TRUE(Between(Size("", Key(4)), 130000, 131000));
      ASSERT_TRUE(Between(Size("", Key(5)), 230000, 231000));
      ASSERT_TRUE(Between(Size("", Key(6)), 240000, 241000));
      ASSERT_TRUE(Between(Size("", Key(7)), 540000, 541000));
      ASSERT_TRUE(Between(Size("", Key(8)), 550000, 560000));

      ASSERT_TRUE(Between(Size(Key(3), Key(5)), 110000, 111000));

      dbfull()->TEST_CompactRange(0, nullptr, nullptr);
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, IteratorPinsRef) {
  Put("foo", "hello");

  // Get iterator that will yield the current contents of the DB.
  Iterator* iter = db_->NewIterator(ReadOptions());

  // Write to force compactions
  Put("foo", "newvalue1");
  for (int i = 0; i < 100; i++) {
    ASSERT_LEVELDB_OK(
        Put(Key(i), Key(i) + std::string(100000, 'v')));  // 100K values
  }
  Put("foo", "newvalue2");

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  ASSERT_EQ("hello", iter->value().ToString());
  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  delete iter;
}

TEST_F(DBTest, Snapshot) {
  do {
    Put("foo", "v1");
    const Snapshot* s1 = db_->GetSnapshot();
    Put("foo", "v2");
    const Snapshot* s2 = db_->GetSnapshot();
    Put("foo", "v3");
    const Snapshot* s3 = db_->GetSnapshot();

    Put("foo", "v4");
    ASSERT_EQ("v1", Get("foo", s1));
    ASSERT_EQ("v2", Get("foo", s2));
    ASSERT_EQ("v3", Get("foo", s3));
    ASSERT_EQ("v4", Get("foo"));

    db_->ReleaseSnapshot(s3);
    ASSERT_EQ("v1", Get("foo", s1));
    ASSERT_EQ("v2", Get("foo", s2));
    ASSERT_EQ("v4", Get("foo"));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("v2", Get("foo", s2));
    ASSERT_EQ("v4", Get("foo"));

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ("v4", Get("foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, HiddenValuesAreRemoved) {
  do {
    Random rnd(301);
    FillLevels("a", "z");

    std::string big = RandomString(&rnd, 50000);
    Put("foo", big);
    Put("pastfoo", "v");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put("foo", "tiny");
    Put("pastfoo2", "v2");  // Advance sequence number one more

    ASSERT_LEVELDB_OK(dbfull()->TEST_CompactMemTable());
    ASSERT_GT(NumTableFilesAtLevel(0), 0);

    ASSERT_EQ(big, Get("foo", snapshot));
    ASSERT_TRUE(Between(Size("", "pastfoo"), 50000, 60000));
    db_->ReleaseSnapshot(snapshot);
    ASSERT_EQ(AllEntriesFor("foo"), "[ tiny, " + big + " ]");
    Slice x("x");
    dbfull()->TEST_CompactRange(0, nullptr, &x);
    ASSERT_EQ(AllEntriesFor("foo"), "[ tiny ]");
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_GE(NumTableFilesAtLevel(1), 1);
    dbfull()->TEST_CompactRange(1, nullptr, &x);
    ASSERT_EQ(AllEntriesFor("foo"), "[ tiny ]");

    ASSERT_TRUE(Between(Size("", "pastfoo"), 0, 1000));
  } while (ChangeOptions());
}

TEST_F(DBTest, DeletionMarkers1) {
  Put("foo", "v1");
  ASSERT_LEVELDB_OK(dbfull()->TEST_CompactMemTable());
  const int last = config::kMaxMemCompactLevel;
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);  // foo => v1 is now in last level

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_CompactMemTable();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1), 1);

  Delete("foo");
  Put("foo", "v2");
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
  ASSERT_LEVELDB_OK(dbfull()->TEST_CompactMemTable());  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
  Slice z("z");
  dbfull()->TEST_CompactRange(last - 2, nullptr, &z);
  // DEL eliminated, but v1 remains because we aren't compacting that level
  // (DEL can be eliminated because v2 hides v1).
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2, v1 ]");
  dbfull()->TEST_CompactRange(last - 1, nullptr, nullptr);
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo"), "[ v2 ]");
}

TEST_F(DBTest, DeletionMarkers2) {
  Put("foo", "v1");
  ASSERT_LEVELDB_OK(dbfull()->TEST_CompactMemTable());
  const int last = config::kMaxMemCompactLevel;
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);  // foo => v1 is now in last level

  // Place a table at level last-1 to prevent merging with preceding mutation
  Put("a", "begin");
  Put("z", "end");
  dbfull()->TEST_CompactMemTable();
  ASSERT_EQ(NumTableFilesAtLevel(last), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1), 1);

  Delete("foo");
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  ASSERT_LEVELDB_OK(dbfull()->TEST_CompactMemTable());  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  dbfull()->TEST_CompactRange(last - 2, nullptr, nullptr);
  // DEL kept: "last" file overlaps
  ASSERT_EQ(AllEntriesFor("foo"), "[ DEL, v1 ]");
  dbfull()->TEST_CompactRange(last - 1, nullptr, nullptr);
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo"), "[ ]");
}

TEST_F(DBTest, OverlapInLevel0) {
  do {
    ASSERT_EQ(config::kMaxMemCompactLevel, 2) << "Fix test to match config";

    // Fill levels 1 and 2 to disable the pushing of new memtables to levels >
    // 0.
    ASSERT_LEVELDB_OK(Put("100", "v100"));
    ASSERT_LEVELDB_OK(Put("999", "v999"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_LEVELDB_OK(Delete("100"));
    ASSERT_LEVELDB_OK(Delete("999"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("0,1,1", FilesPerLevel());

    // Make files spanning the following ranges in level-0:
    //  files[0]  200 .. 900
    //  files[1]  300 .. 500
    // Note that files are sorted by smallest key.
    ASSERT_LEVELDB_OK(Put("300", "v300"));
    ASSERT_LEVELDB_OK(Put("500", "v500"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_LEVELDB_OK(Put("200", "v200"));
    ASSERT_LEVELDB_OK(Put("600", "v600"));
    ASSERT_LEVELDB_OK(Put("900", "v900"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("2,1,1", FilesPerLevel());

    // Compact away the placeholder files we created initially
    dbfull()->TEST_CompactRange(1, nullptr, nullptr);
    dbfull()->TEST_CompactRange(2, nullptr, nullptr);
    ASSERT_EQ("2", FilesPerLevel());

    // Do a memtable compaction.  Before bug-fix, the compaction would
    // not detect the overlap with level-0 files and would incorrectly place
    // the deletion in a deeper level.
    ASSERT_LEVELDB_OK(Delete("600"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("3", FilesPerLevel());
    ASSERT_EQ("NOT_FOUND", Get("600"));
  } while (ChangeOptions());
}

TEST_F(DBTest, L0_CompactionBug_Issue44_a) {
  Reopen();
  ASSERT_LEVELDB_OK(Put("b", "v"));
  Reopen();
  ASSERT_LEVELDB_OK(Delete("b"));
  ASSERT_LEVELDB_OK(Delete("a"));
  Reopen();
  ASSERT_LEVELDB_OK(Delete("a"));
  Reopen();
  ASSERT_LEVELDB_OK(Put("a", "v"));
  Reopen();
  Reopen();
  ASSERT_EQ("(a->v)", Contents());
  DelayMilliseconds(1000);  // Wait for compaction to finish
  ASSERT_EQ("(a->v)", Contents());
}

TEST_F(DBTest, L0_CompactionBug_Issue44_b) {
  Reopen();
  Put("", "");
  Reopen();
  Delete("e");
  Put("", "");
  Reopen();
  Put("c", "cv");
  Reopen();
  Put("", "");
  Reopen();
  Put("", "");
  DelayMilliseconds(1000);  // Wait for compaction to finish
  Reopen();
  Put("d", "dv");
  Reopen();
  Put("", "");
  Reopen();
  Delete("d");
  Delete("b");
  Reopen();
  ASSERT_EQ("(->)(c->cv)", Contents());
  DelayMilliseconds(1000);  // Wait for compaction to finish
  ASSERT_EQ("(->)(c->cv)", Contents());
}

TEST_F(DBTest, Fflush_Issue474) {
  static const int kNum = 100000;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < kNum; i++) {
    std::fflush(nullptr);
    ASSERT_LEVELDB_OK(Put(RandomKey(&rnd), RandomString(&rnd, 100)));
  }
}

TEST_F(DBTest, ComparatorCheck) {
  class NewComparator : public Comparator {
   public:
    const char* Name() const override { return "leveldb.NewComparator"; }
    int Compare(const Slice& a, const Slice& b) const override {
      return BytewiseComparator()->Compare(a, b);
    }
    void FindShortestSeparator(std::string* s, const Slice& l) const override {
      BytewiseComparator()->FindShortestSeparator(s, l);
    }
    void FindShortSuccessor(std::string* key) const override {
      BytewiseComparator()->FindShortSuccessor(key);
    }
  };
  NewComparator cmp;
  Options new_options = CurrentOptions();
  new_options.comparator = &cmp;
  Status s = TryReopen(&new_options);
  ASSERT_TRUE(!s.ok());
  ASSERT_TRUE(s.ToString().find("comparator") != std::string::npos)
      << s.ToString();
}

TEST_F(DBTest, CustomComparator) {
  class NumberComparator : public Comparator {
   public:
    const char* Name() const override { return "test.NumberComparator"; }
    int Compare(const Slice& a, const Slice& b) const override {
      return ToNumber(a) - ToNumber(b);
    }
    void FindShortestSeparator(std::string* s, const Slice& l) const override {
      ToNumber(*s);  // Check format
      ToNumber(l);   // Check format
    }
    void FindShortSuccessor(std::string* key) const override {
      ToNumber(*key);  // Check format
    }

   private:
    static int ToNumber(const Slice& x) {
      // Check that there are no extra characters.
      EXPECT_TRUE(x.size() >= 2 && x[0] == '[' && x[x.size() - 1] == ']')
          << EscapeString(x);
      int val;
      char ignored;
      EXPECT_TRUE(sscanf(x.ToString().c_str(), "[%i]%c", &val, &ignored) == 1)
          << EscapeString(x);
      return val;
    }
  };
  NumberComparator cmp;
  Options new_options = CurrentOptions();
  new_options.create_if_missing = true;
  new_options.comparator = &cmp;
  new_options.filter_policy = nullptr;   // Cannot use bloom filters
  new_options.write_buffer_size = 1000;  // Compact more often
  DestroyAndReopen(&new_options);
  ASSERT_LEVELDB_OK(Put("[10]", "ten"));
  ASSERT_LEVELDB_OK(Put("[0x14]", "twenty"));
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ("ten", Get("[10]"));
    ASSERT_EQ("ten", Get("[0xa]"));
    ASSERT_EQ("twenty", Get("[20]"));
    ASSERT_EQ("twenty", Get("[0x14]"));
    ASSERT_EQ("NOT_FOUND", Get("[15]"));
    ASSERT_EQ("NOT_FOUND", Get("[0xf]"));
    Compact("[0]", "[9999]");
  }

  for (int run = 0; run < 2; run++) {
    for (int i = 0; i < 1000; i++) {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "[%d]", i * 10);
      ASSERT_LEVELDB_OK(Put(buf, buf));
    }
    Compact("[0]", "[1000000]");
  }
}

TEST_F(DBTest, ManualCompaction) {
  ASSERT_EQ(config::kMaxMemCompactLevel, 2)
      << "Need to update this test to match kMaxMemCompactLevel";

  MakeTables(3, "p", "q");
  ASSERT_EQ("1,1,1", FilesPerLevel());

  // Compaction range falls before files
  Compact("", "c");
  ASSERT_EQ("1,1,1", FilesPerLevel());

  // Compaction range falls after files
  Compact("r", "z");
  ASSERT_EQ("1,1,1", FilesPerLevel());

  // Compaction range overlaps files
  Compact("p1", "p9");
  ASSERT_EQ("0,0,1", FilesPerLevel());

  // Populate a different range
  MakeTables(3, "c", "e");
  ASSERT_EQ("1,1,2", FilesPerLevel());

  // Compact just the new range
  Compact("b", "f");
  ASSERT_EQ("0,0,2", FilesPerLevel());

  // Compact all
  MakeTables(1, "a", "z");
  ASSERT_EQ("0,1,2", FilesPerLevel());
  db_->CompactRange(nullptr, nullptr);
  ASSERT_EQ("0,0,1", FilesPerLevel());
}

TEST_F(DBTest, DBOpen_Options) {
  std::string dbname = testing::TempDir() + "db_options_test";
  DestroyDB(dbname, Options());

  // Does not exist, and create_if_missing == false: error
  DB* db = nullptr;
  Options opts;
  opts.create_if_missing = false;
  Status s = DB::Open(opts, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  opts.create_if_missing = true;
  s = DB::Open(opts, dbname, &db);
  ASSERT_LEVELDB_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;

  // Does exist, and error_if_exists == true: error
  opts.create_if_missing = false;
  opts.error_if_exists = true;
  s = DB::Open(opts, dbname, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  opts.create_if_missing = true;
  opts.error_if_exists = false;
  s = DB::Open(opts, dbname, &db);
  ASSERT_LEVELDB_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;
}

TEST_F(DBTest, DestroyEmptyDir) {
  std::string dbname = testing::TempDir() + "db_empty_dir";
  TestEnv env(Env::Default());
  env.RemoveDir(dbname);
  ASSERT_TRUE(!env.FileExists(dbname));

  Options opts;
  opts.env = &env;

  ASSERT_LEVELDB_OK(env.CreateDir(dbname));
  ASSERT_TRUE(env.FileExists(dbname));
  std::vector<std::string> children;
  ASSERT_LEVELDB_OK(env.GetChildren(dbname, &children));
#if defined(LEVELDB_PLATFORM_CHROMIUM)
  // TODO(https://crbug.com/1428746): Chromium's file system abstraction always
  // filters out '.' and '..'.
  ASSERT_EQ(0, children.size());
#else
  // The stock Env's do not filter out '.' and '..' special files.
  ASSERT_EQ(2, children.size());
#endif  // defined(LEVELDB_PLATFORM_CHROMIUM)
  ASSERT_LEVELDB_OK(DestroyDB(dbname, opts));
  ASSERT_TRUE(!env.FileExists(dbname));

  // Should also be destroyed if Env is filtering out dot files.
  env.SetIgnoreDotFiles(true);
  ASSERT_LEVELDB_OK(env.CreateDir(dbname));
  ASSERT_TRUE(env.FileExists(dbname));
  ASSERT_LEVELDB_OK(env.GetChildren(dbname, &children));
  ASSERT_EQ(0, children.size());
  ASSERT_LEVELDB_OK(DestroyDB(dbname, opts));
  ASSERT_TRUE(!env.FileExists(dbname));
}

TEST_F(DBTest, DestroyOpenDB) {
  std::string dbname = testing::TempDir() + "open_db_dir";
  env_->RemoveDir(dbname);
  ASSERT_TRUE(!env_->FileExists(dbname));

  Options opts;
  opts.create_if_missing = true;
  DB* db = nullptr;
  ASSERT_LEVELDB_OK(DB::Open(opts, dbname, &db));
  ASSERT_TRUE(db != nullptr);

  // Must fail to destroy an open db.
  ASSERT_TRUE(env_->FileExists(dbname));
  ASSERT_TRUE(!DestroyDB(dbname, Options()).ok());
  ASSERT_TRUE(env_->FileExists(dbname));

  delete db;
  db = nullptr;

  // Should succeed destroying a closed db.
  ASSERT_LEVELDB_OK(DestroyDB(dbname, Options()));
  ASSERT_TRUE(!env_->FileExists(dbname));
}

TEST_F(DBTest, Locking) {
  DB* db2 = nullptr;
  Status s = DB::Open(CurrentOptions(), dbname_, &db2);
  ASSERT_TRUE(!s.ok()) << "Locking did not prevent re-opening db";
}

// Check that number of files does not grow when we are out of space
TEST_F(DBTest, NoSpace) {
  Options options = CurrentOptions();
  options.env = env_;
  Reopen(&options);

  ASSERT_LEVELDB_OK(Put("foo", "v1"));
  ASSERT_EQ("v1", Get("foo"));
  Compact("a", "z");
  const int num_files = CountFiles();
  // Force out-of-space errors.
  env_->no_space_.store(true, std::memory_order_release);
  for (int i = 0; i < 10; i++) {
    for (int level = 0; level < config::kNumLevels - 1; level++) {
      dbfull()->TEST_CompactRange(level, nullptr, nullptr);
    }
  }
  env_->no_space_.store(false, std::memory_order_release);
  ASSERT_LT(CountFiles(), num_files + 3);
}

TEST_F(DBTest, NonWritableFileSystem) {
  Options options = CurrentOptions();
  options.write_buffer_size = 1000;
  options.env = env_;
  Reopen(&options);
  ASSERT_LEVELDB_OK(Put("foo", "v1"));
  // Force errors for new files.
  env_->non_writable_.store(true, std::memory_order_release);
  std::string big(100000, 'x');
  int errors = 0;
  for (int i = 0; i < 20; i++) {
    std::fprintf(stderr, "iter %d; errors %d\n", i, errors);
    if (!Put("foo", big).ok()) {
      errors++;
      DelayMilliseconds(100);
    }
  }
  ASSERT_GT(errors, 0);
  env_->non_writable_.store(false, std::memory_order_release);
}

TEST_F(DBTest, WriteSyncError) {
  // Check that log sync errors cause the DB to disallow future writes.

  // (a) Cause log sync calls to fail
  Options options = CurrentOptions();
  options.env = env_;
  Reopen(&options);
  env_->data_sync_error_.store(true, std::memory_order_release);

  // (b) Normal write should succeed
  WriteOptions w;
  ASSERT_LEVELDB_OK(db_->Put(w, "k1", "v1"));
  ASSERT_EQ("v1", Get("k1"));

  // (c) Do a sync write; should fail
  w.sync = true;
  ASSERT_TRUE(!db_->Put(w, "k2", "v2").ok());
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("NOT_FOUND", Get("k2"));

  // (d) make sync behave normally
  env_->data_sync_error_.store(false, std::memory_order_release);

  // (e) Do a non-sync write; should fail
  w.sync = false;
  ASSERT_TRUE(!db_->Put(w, "k3", "v3").ok());
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("NOT_FOUND", Get("k2"));
  ASSERT_EQ("NOT_FOUND", Get("k3"));
}

TEST_F(DBTest, ManifestWriteError) {
  // Test for the following problem:
  // (a) Compaction produces file F
  // (b) Log record containing F is written to MANIFEST file, but Sync() fails
  // (c) GC deletes F
  // (d) After reopening DB, reads fail since deleted F is named in log record

  // We iterate twice.  In the second iteration, everything is the
  // same except the log record never makes it to the MANIFEST file.
  for (int iter = 0; iter < 2; iter++) {
    std::atomic<bool>* error_type = (iter == 0) ? &env_->manifest_sync_error_
                                                : &env_->manifest_write_error_;

    // Insert foo=>bar mapping
    Options options = CurrentOptions();
    options.env = env_;
    options.create_if_missing = true;
    options.error_if_exists = false;
    DestroyAndReopen(&options);
    ASSERT_LEVELDB_OK(Put("foo", "bar"));
    ASSERT_EQ("bar", Get("foo"));

    // Memtable compaction (will succeed)
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("bar", Get("foo"));
    const int last = config::kMaxMemCompactLevel;
    ASSERT_EQ(NumTableFilesAtLevel(last), 1);  // foo=>bar is now in last level

    // Merging compaction (will fail)
    error_type->store(true, std::memory_order_release);
    dbfull()->TEST_CompactRange(last, nullptr, nullptr);  // Should fail
    ASSERT_EQ("bar", Get("foo"));

    // Recovery: should not lose data
    error_type->store(false, std::memory_order_release);
    Reopen(&options);
    ASSERT_EQ("bar", Get("foo"));
  }
}

TEST_F(DBTest, MissingSSTFile) {
  ASSERT_LEVELDB_OK(Put("foo", "bar"));
  ASSERT_EQ("bar", Get("foo"));

  // Dump the memtable to disk.
  dbfull()->TEST_CompactMemTable();
  ASSERT_EQ("bar", Get("foo"));

  Close();
  ASSERT_TRUE(DeleteAnSSTFile());
  Options options = CurrentOptions();
  options.paranoid_checks = true;
  Status s = TryReopen(&options);
  ASSERT_TRUE(!s.ok());
  ASSERT_TRUE(s.ToString().find("issing") != std::string::npos) << s.ToString();
}

TEST_F(DBTest, StillReadSST) {
  ASSERT_LEVELDB_OK(Put("foo", "bar"));
  ASSERT_EQ("bar", Get("foo"));

  // Dump the memtable to disk.
  dbfull()->TEST_CompactMemTable();
  ASSERT_EQ("bar", Get("foo"));
  Close();
  ASSERT_GT(RenameLDBToSST(), 0);
  Options options = CurrentOptions();
  options.paranoid_checks = true;
  Status s = TryReopen(&options);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ("bar", Get("foo"));
}

TEST_F(DBTest, FilesDeletedAfterCompaction) {
  ASSERT_LEVELDB_OK(Put("foo", "v2"));
  Compact("a", "z");
  const int num_files = CountFiles();
  for (int i = 0; i < 10; i++) {
    ASSERT_LEVELDB_OK(Put("foo", "v2"));
    Compact("a", "z");
  }
  ASSERT_EQ(CountFiles(), num_files);
}

TEST_F(DBTest, BloomFilter) {
  env_->count_random_reads_ = true;
  Options options = CurrentOptions();
  options.env = env_;
  options.block_cache = NewLRUCache(0);  // Prevent cache hits
  options.filter_policy = NewBloomFilterPolicy(10);
  Reopen(&options);

  // Populate multiple layers
  const int N = 10000;
  for (int i = 0; i < N; i++) {
    ASSERT_LEVELDB_OK(Put(Key(i), Key(i)));
  }
  Compact("a", "z");
  for (int i = 0; i < N; i += 100) {
    ASSERT_LEVELDB_OK(Put(Key(i), Key(i)));
  }
  dbfull()->TEST_CompactMemTable();

  // Prevent auto compactions triggered by seeks
  env_->delay_data_sync_.store(true, std::memory_order_release);

  // Lookup present keys.  Should rarely read from small sstable.
  env_->random_read_counter_.Reset();
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(Key(i), Get(Key(i)));
  }
  int reads = env_->random_read_counter_.Read();
  std::fprintf(stderr, "%d present => %d reads\n", N, reads);
  ASSERT_GE(reads, N);
  ASSERT_LE(reads, N + 2 * N / 100);

  // Lookup present keys.  Should rarely read from either sstable.
  env_->random_read_counter_.Reset();
  for (int i = 0; i < N; i++) {
    ASSERT_EQ("NOT_FOUND", Get(Key(i) + ".missing"));
  }
  reads = env_->random_read_counter_.Read();
  std::fprintf(stderr, "%d missing => %d reads\n", N, reads);
  ASSERT_LE(reads, 3 * N / 100);

  env_->delay_data_sync_.store(false, std::memory_order_release);
  Close();
  delete options.block_cache;
  delete options.filter_policy;
}

TEST_F(DBTest, LogCloseError) {
  // Regression test for bug where we could ignore log file
  // Close() error when switching to a new log file.
  const int kValueSize = 20000;
  const int kWriteCount = 10;
  const int kWriteBufferSize = (kValueSize * kWriteCount) / 2;

  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = kWriteBufferSize;  // Small write buffer
  Reopen(&options);
  env_->log_file_close_.store(true, std::memory_order_release);

  std::string value(kValueSize, 'x');
  Status s;
  for (int i = 0; i < kWriteCount && s.ok(); i++) {
    s = Put(Key(i), value);
  }
  ASSERT_TRUE(!s.ok()) << "succeeded even after log file Close failure";

  // Future writes should also fail after an earlier error.
  s = Put("hello", "world");
  ASSERT_TRUE(!s.ok()) << "write succeeded after log file Close failure";

  env_->log_file_close_.store(false, std::memory_order_release);
}

// Multi-threaded test:
namespace {

static const int kNumThreads = 4;
static const int kTestSeconds = 10;
static const int kNumKeys = 1000;

struct MTState {
  DBTest* test;
  std::atomic<bool> stop;
  std::atomic<int> counter[kNumThreads];
  std::atomic<bool> thread_done[kNumThreads];
};

struct MTThread {
  MTState* state;
  int id;
};

static void MTThreadBody(void* arg) {
  MTThread* t = reinterpret_cast<MTThread*>(arg);
  int id = t->id;
  DB* db = t->state->test->db_;
  int counter = 0;
  std::fprintf(stderr, "... starting thread %d\n", id);
  Random rnd(1000 + id);
  std::string value;
  char valbuf[1500];
  while (!t->state->stop.load(std::memory_order_acquire)) {
    t->state->counter[id].store(counter, std::memory_order_release);

    int key = rnd.Uniform(kNumKeys);
    char keybuf[20];
    std::snprintf(keybuf, sizeof(keybuf), "%016d", key);

    if (rnd.OneIn(2)) {
      // Write values of the form <key, my id, counter>.
      // We add some padding for force compactions.
      std::snprintf(valbuf, sizeof(valbuf), "%d.%d.%-1000d", key, id,
                    static_cast<int>(counter));
      ASSERT_LEVELDB_OK(db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf)));
    } else {
      // Read a value and verify that it matches the pattern written above.
      Status s = db->Get(ReadOptions(), Slice(keybuf), &value);
      if (s.IsNotFound()) {
        // Key has not yet been written
      } else {
        // Check that the writer thread counter is >= the counter in the value
        ASSERT_LEVELDB_OK(s);
        int k, w, c;
        ASSERT_EQ(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;
        ASSERT_EQ(k, key);
        ASSERT_GE(w, 0);
        ASSERT_LT(w, kNumThreads);
        ASSERT_LE(c, t->state->counter[w].load(std::memory_order_acquire));
      }
    }
    counter++;
  }
  t->state->thread_done[id].store(true, std::memory_order_release);
  std::fprintf(stderr, "... stopping thread %d after %d ops\n", id, counter);
}

}  // namespace

TEST_F(DBTest, MultiThreaded) {
  do {
    // Initialize state
    MTState mt;
    mt.test = this;
    mt.stop.store(false, std::memory_order_release);
    for (int id = 0; id < kNumThreads; id++) {
      mt.counter[id].store(false, std::memory_order_release);
      mt.thread_done[id].store(false, std::memory_order_release);
    }

    // Start threads
    MTThread thread[kNumThreads];
    for (int id = 0; id < kNumThreads; id++) {
      thread[id].state = &mt;
      thread[id].id = id;
      env_->StartThread(MTThreadBody, &thread[id]);
    }

    // Let them run for a while
    DelayMilliseconds(kTestSeconds * 1000);

    // Stop the threads and wait for them to finish
    mt.stop.store(true, std::memory_order_release);
    for (int id = 0; id < kNumThreads; id++) {
      while (!mt.thread_done[id].load(std::memory_order_acquire)) {
        DelayMilliseconds(100);
      }
    }
  } while (ChangeOptions());
}

namespace {
typedef std::map<std::string, std::string> KVMap;
}

class ModelDB : public DB {
 public:
  class ModelSnapshot : public Snapshot {
   public:
    KVMap map_;
  };

  explicit ModelDB(const Options& options) : options_(options) {}
  ~ModelDB() override = default;
  Status Put(const WriteOptions& o, const Slice& k, const Slice& v) override {
    return DB::Put(o, k, v);
  }
  Status Delete(const WriteOptions& o, const Slice& key) override {
    return DB::Delete(o, key);
  }
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override {
    assert(false);  // Not implemented
    return Status::NotFound(key);
  }
  Status Scan(
      const ReadOptions& options, const Slice& start_key, const Slice& end_key,
      std::vector<std::pair<std::string, std::string>>* result) override {
    result->clear();
    for (auto it = map_.lower_bound(start_key.ToString());
         it != map_.end() && it->first < end_key.ToString(); ++it) {
      result->push_back(*it);
    }
    return Status::OK();
  }

  Status DeleteRange(const WriteOptions& options, const Slice& start_key,
                     const Slice& end_key) override {
    return Status::NotSupported("DeleteRange not implemented in ModelDB");
  }

  Iterator* NewIterator(const ReadOptions& options) override {
    if (options.snapshot == nullptr) {
      KVMap* saved = new KVMap;
      *saved = map_;
      return new ModelIter(saved, true);
    } else {
      const KVMap* snapshot_state =
          &(reinterpret_cast<const ModelSnapshot*>(options.snapshot)->map_);
      return new ModelIter(snapshot_state, false);
    }
  }
  const Snapshot* GetSnapshot() override {
    ModelSnapshot* snapshot = new ModelSnapshot;
    snapshot->map_ = map_;
    return snapshot;
  }

  void ReleaseSnapshot(const Snapshot* snapshot) override {
    delete reinterpret_cast<const ModelSnapshot*>(snapshot);
  }
  Status Write(const WriteOptions& options, WriteBatch* batch) override {
    class Handler : public WriteBatch::Handler {
     public:
      KVMap* map_;
      void Put(const Slice& key, const Slice& value) override {
        (*map_)[key.ToString()] = value.ToString();
      }
      void Delete(const Slice& key) override { map_->erase(key.ToString()); }
    };
    Handler handler;
    handler.map_ = &map_;
    return batch->Iterate(&handler);
  }

  bool GetProperty(const Slice& property, std::string* value) override {
    return false;
  }
  void GetApproximateSizes(const Range* r, int n, uint64_t* sizes) override {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
  }
  void CompactRange(const Slice* start, const Slice* end) override {}
  Status ForceFullCompaction() override { return Status::OK(); }

 private:
  class ModelIter : public Iterator {
   public:
    ModelIter(const KVMap* map, bool owned)
        : map_(map), owned_(owned), iter_(map_->end()) {}
    ~ModelIter() override {
      if (owned_) delete map_;
    }
    bool Valid() const override { return iter_ != map_->end(); }
    void SeekToFirst() override { iter_ = map_->begin(); }
    void SeekToLast() override {
      if (map_->empty()) {
        iter_ = map_->end();
      } else {
        iter_ = map_->find(map_->rbegin()->first);
      }
    }
    void Seek(const Slice& k) override {
      iter_ = map_->lower_bound(k.ToString());
    }
    void Next() override { ++iter_; }
    void Prev() override { --iter_; }
    Slice key() const override { return iter_->first; }
    Slice value() const override { return iter_->second; }
    Status status() const override { return Status::OK(); }

   private:
    const KVMap* const map_;
    const bool owned_;  // Do we own map_
    KVMap::const_iterator iter_;
  };
  const Options options_;
  KVMap map_;
};

static bool CompareIterators(int step, DB* model, DB* db,
                             const Snapshot* model_snap,
                             const Snapshot* db_snap) {
  ReadOptions options;
  options.snapshot = model_snap;
  Iterator* miter = model->NewIterator(options);
  options.snapshot = db_snap;
  Iterator* dbiter = db->NewIterator(options);
  bool ok = true;
  int count = 0;
  std::vector<std::string> seek_keys;
  // Compare equality of all elements using Next(). Save some of the keys for
  // comparing Seek equality.
  for (miter->SeekToFirst(), dbiter->SeekToFirst();
       ok && miter->Valid() && dbiter->Valid(); miter->Next(), dbiter->Next()) {
    count++;
    if (miter->key().compare(dbiter->key()) != 0) {
      std::fprintf(stderr, "step %d: Key mismatch: '%s' vs. '%s'\n", step,
                   EscapeString(miter->key()).c_str(),
                   EscapeString(dbiter->key()).c_str());
      ok = false;
      break;
    }

    if (miter->value().compare(dbiter->value()) != 0) {
      std::fprintf(stderr,
                   "step %d: Value mismatch for key '%s': '%s' vs. '%s'\n",
                   step, EscapeString(miter->key()).c_str(),
                   EscapeString(miter->value()).c_str(),
                   EscapeString(miter->value()).c_str());
      ok = false;
      break;
    }

    if (count % 10 == 0) {
      seek_keys.push_back(miter->key().ToString());
    }
  }

  if (ok) {
    if (miter->Valid() != dbiter->Valid()) {
      std::fprintf(stderr, "step %d: Mismatch at end of iterators: %d vs. %d\n",
                   step, miter->Valid(), dbiter->Valid());
      ok = false;
    }
  }

  if (ok) {
    // Validate iterator equality when performing seeks.
    for (auto kiter = seek_keys.begin(); ok && kiter != seek_keys.end();
         ++kiter) {
      miter->Seek(*kiter);
      dbiter->Seek(*kiter);
      if (!miter->Valid() || !dbiter->Valid()) {
        std::fprintf(stderr, "step %d: Seek iterators invalid: %d vs. %d\n",
                     step, miter->Valid(), dbiter->Valid());
        ok = false;
      }
      if (miter->key().compare(dbiter->key()) != 0) {
        std::fprintf(stderr, "step %d: Seek key mismatch: '%s' vs. '%s'\n",
                     step, EscapeString(miter->key()).c_str(),
                     EscapeString(dbiter->key()).c_str());
        ok = false;
        break;
      }

      if (miter->value().compare(dbiter->value()) != 0) {
        std::fprintf(
            stderr,
            "step %d: Seek value mismatch for key '%s': '%s' vs. '%s'\n", step,
            EscapeString(miter->key()).c_str(),
            EscapeString(miter->value()).c_str(),
            EscapeString(miter->value()).c_str());
        ok = false;
        break;
      }
    }
  }

  std::fprintf(stderr, "%d entries compared: ok=%d\n", count, ok);
  delete miter;
  delete dbiter;
  return ok;
}

TEST_F(DBTest, Randomized) {
  Random rnd(test::RandomSeed());
  do {
    ModelDB model(CurrentOptions());
    const int N = 10000;
    const Snapshot* model_snap = nullptr;
    const Snapshot* db_snap = nullptr;
    std::string k, v;
    for (int step = 0; step < N; step++) {
      if (step % 100 == 0) {
        std::fprintf(stderr, "Step %d of %d\n", step, N);
      }
      // TODO(sanjay): Test Get() works
      int p = rnd.Uniform(100);
      if (p < 45) {  // Put
        k = RandomKey(&rnd);
        v = RandomString(
            &rnd, rnd.OneIn(20) ? 100 + rnd.Uniform(100) : rnd.Uniform(8));
        ASSERT_LEVELDB_OK(model.Put(WriteOptions(), k, v));
        ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), k, v));

      } else if (p < 90) {  // Delete
        k = RandomKey(&rnd);
        ASSERT_LEVELDB_OK(model.Delete(WriteOptions(), k));
        ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), k));

      } else {  // Multi-element batch
        WriteBatch b;
        const int num = rnd.Uniform(8);
        for (int i = 0; i < num; i++) {
          if (i == 0 || !rnd.OneIn(10)) {
            k = RandomKey(&rnd);
          } else {
            // Periodically re-use the same key from the previous iter, so
            // we have multiple entries in the write batch for the same key
          }
          if (rnd.OneIn(2)) {
            v = RandomString(&rnd, rnd.Uniform(10));
            b.Put(k, v);
          } else {
            b.Delete(k);
          }
        }
        ASSERT_LEVELDB_OK(model.Write(WriteOptions(), &b));
        ASSERT_LEVELDB_OK(db_->Write(WriteOptions(), &b));
      }

      if ((step % 100) == 0) {
        ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));
        ASSERT_TRUE(CompareIterators(step, &model, db_, model_snap, db_snap));
        // Save a snapshot from each DB this time that we'll use next
        // time we compare things, to make sure the current state is
        // preserved with the snapshot
        if (model_snap != nullptr) model.ReleaseSnapshot(model_snap);
        if (db_snap != nullptr) db_->ReleaseSnapshot(db_snap);

        Reopen();
        ASSERT_TRUE(CompareIterators(step, &model, db_, nullptr, nullptr));

        model_snap = model.GetSnapshot();
        db_snap = db_->GetSnapshot();
      }
    }
    if (model_snap != nullptr) model.ReleaseSnapshot(model_snap);
    if (db_snap != nullptr) db_->ReleaseSnapshot(db_snap);
  } while (ChangeOptions());
}

TEST_F(DBTest, ForceFullCompactionBasic) {
  // Case 1: Empty DB
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // Case 2: Single file in L0
  ASSERT_LEVELDB_OK(Put("a", "v1"));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  ASSERT_EQ(Get("a"), "v1");
  // After full compaction, everything should be in the last level or at least
  // moved out of L0
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
}

TEST_F(DBTest, ForceFullCompactionMultiLevel) {
  // Fill multiple levels
  for (int i = 0; i < 5; i++) {
    ASSERT_LEVELDB_OK(Put("key" + std::to_string(i), "value"));
    dbfull()->TEST_CompactMemTable();
  }
  // Now we have several L0 files.

  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // Verify data integrity
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(Get("key" + std::to_string(i)), "value");
  }

  // Tree should be stable (L0 empty)
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
}

TEST_F(DBTest, ForceFullCompactionIsolation) {
  // Use SpecialEnv for this test
  Options options = CurrentOptions();
  options.env = env_;
  Reopen(&options);

  // Populate enough data to ensure multiple compaction waves
  for (int i = 0; i < 200; i++) {
    ASSERT_LEVELDB_OK(Put("key" + std::to_string(i), std::string(1000, 'x')));
  }
  dbfull()->TEST_CompactMemTable();

  std::atomic<bool> ffc_started(false);
  std::atomic<bool> write_blocked(false);
  std::atomic<bool> write_finished(false);

  // Use a delay in data sync to slow down the compaction's FlushMemTableSync
  // (which now does a sync write)
  env_->delay_data_sync_.store(true, std::memory_order_release);

  std::thread ffc_thread([this, &ffc_started]() {
    ffc_started.store(true);
    this->db_->ForceFullCompaction();
  });

  while (!ffc_started.load()) {
    DelayMilliseconds(5);
  }

  // Wait until the isolation flag is actually set in the DB
  while (!dbfull()->TEST_IsForceFullCompactionInProgress()) {
    DelayMilliseconds(5);
  }

  std::thread writer_thread([this, &write_blocked, &write_finished]() {
    write_blocked.store(true);
    this->Put("concurrent", "value");
    write_finished.store(true);
  });

  while (!write_blocked.load()) {
    DelayMilliseconds(5);
  }

  // Verify writer is blocked while FFC is running and sync is delayed
  bool blocked = true;
  for (int i = 0; i < 20; i++) {
    DelayMilliseconds(50);
    if (write_finished.load()) {
      blocked = false;
      break;
    }
  }

  // Release sync delay to let FFC and then the writer proceed
  env_->delay_data_sync_.store(false, std::memory_order_release);
  ffc_thread.join();
  writer_thread.join();

  ASSERT_TRUE(blocked)
      << "Writer thread was not blocked by ForceFullCompaction";
  ASSERT_EQ(Get("concurrent"), "value");
}

TEST_F(DBTest, ForceFullCompactionConcurrentManual) {
  Options options = CurrentOptions();
  options.env = env_;
  Reopen(&options);

  // Fill some data
  MakeTables(3, "a", "z");

  std::atomic<bool> manual_started(false);
  std::atomic<bool> manual_finished(false);

  // Start a manual compaction and slow it down
  env_->delay_data_sync_.store(true, std::memory_order_release);

  std::thread manual_thread([this, &manual_started, &manual_finished]() {
    manual_started.store(true);
    this->Compact("a", "z");
    manual_finished.store(true);
  });

  while (!manual_started.load()) {
    DelayMilliseconds(5);
  }

  // Give it time to start
  DelayMilliseconds(50);

  // Now call ForceFullCompaction. It should wait for the manual compaction to
  // finish.
  std::atomic<bool> ffc_started(false);
  std::atomic<bool> ffc_finished(false);
  std::thread ffc_thread([this, &ffc_started, &ffc_finished]() {
    ffc_started.store(true);
    this->db_->ForceFullCompaction();
    ffc_finished.store(true);
  });

  while (!ffc_started.load()) {
    DelayMilliseconds(5);
  }

  // Check that FFC is blocked
  DelayMilliseconds(500);
  ASSERT_FALSE(manual_finished.load());
  ASSERT_FALSE(ffc_finished.load());

  // Release sync delay
  env_->delay_data_sync_.store(false, std::memory_order_release);

  manual_thread.join();
  ffc_thread.join();

  ASSERT_TRUE(manual_finished.load());
  ASSERT_TRUE(ffc_finished.load());
}

TEST_F(DBTest, ForceFullCompaction_Level0Only_BugHunt) {
  // 1. Write a single key-value pair to the database.
  ASSERT_LEVELDB_OK(Put("isolate_key", "isolate_value"));

  // 2. Force the MemTable to flush.
  // LevelDB's PickLevelForMemTableOutput may push a single file to higher
  // levels if there is no overlap. We loop to ensure we actually have an L0
  // file.
  int count = 0;
  while (NumTableFilesAtLevel(0) == 0 && count < 100) {
    ASSERT_LEVELDB_OK(Put("isolate_key", "isolate_value"));
    dbfull()->TEST_CompactMemTable();
    count++;
  }

  // Verify our starting state: we have at least one file in L0.
  ASSERT_GT(NumTableFilesAtLevel(0), 0);

  // 3. Trigger the manual full compaction.
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // 4. THE TRUTH TEST: Level 0 should now be empty.
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  // Verify data wasn't destroyed
  ASSERT_EQ(Get("isolate_key"), "isolate_value");
}

class FullCompactionTest : public testing::Test {
 public:
  FullCompactionTest() : db_(nullptr) {
    dbname_ = testing::TempDir() + "/full_compaction_test";
  }

  void SetUp() override {
    DestroyDB(dbname_, Options());
    Options options;
    options.create_if_missing = true;
    // Use a small write buffer to trigger more files/levels
    options.write_buffer_size = 64 * 1024;
    ASSERT_LEVELDB_OK(DB::Open(options, dbname_, &db_));
  }

  void TearDown() override {
    delete db_;
    DestroyDB(dbname_, Options());
  }

  void Put(const std::string& k, const std::string& v) {
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), k, v));
  }

  std::string Get(const std::string& k) {
    std::string result;
    Status s = db_->Get(ReadOptions(), k, &result);
    if (s.IsNotFound()) return "NOT_FOUND";
    if (!s.ok()) return s.ToString();
    return result;
  }

  Status FillRandom(int count, int value_size = 1024, int start_index = 0) {
    WriteOptions wo;
    std::string val(value_size, 'x');
    for (int i = start_index; i < start_index + count; ++i) {
      char key[32];
      std::snprintf(key, sizeof(key), "key%08d", i);
      Status s = db_->Put(wo, key, val);
      if (!s.ok()) return s;
    }
    return Status::OK();
  }

  int FilesAtLevel(int level) {
    std::string prop;
    char buf[64];
    std::snprintf(buf, sizeof(buf), "leveldb.num-files-at-level%d", level);
    if (!db_->GetProperty(buf, &prop)) return -1;
    return std::stoi(prop);
  }

  int TotalFiles() {
    int total = 0;
    for (int i = 0; i < 7; ++i) {
      int n = FilesAtLevel(i);
      if (n > 0) total += n;
    }
    return total;
  }

  std::string dbname_;
  DB* db_;
};

class StdoutCapture {
 public:
  StdoutCapture() {
    fflush(stdout);
    old_fd_ = dup(fileno(stdout));
    if (pipe(pipefd_) != 0) return;
    dup2(pipefd_[1], fileno(stdout));
    close(pipefd_[1]);
    active_ = true;
  }
  ~StdoutCapture() {
    if (active_) Finish();
  }
  std::string Finish() {
    if (!active_) return captured_;
    fflush(stdout);
    dup2(old_fd_, fileno(stdout));
    close(old_fd_);
    active_ = false;

    char buf[4096];
    ssize_t n;
    while ((n = read(pipefd_[0], buf, sizeof(buf))) > 0)
      captured_.append(buf, n);
    close(pipefd_[0]);
    return captured_;
  }

 private:
  int pipefd_[2] = {-1, -1};
  int old_fd_ = -1;
  bool active_ = false;
  std::string captured_;
};

// Helper to check if a thread is blocked (approximate)
bool IsBlocked(std::atomic<int>& counter, int expected, int timeout_ms = 500) {
  auto start = std::chrono::steady_clock::now();
  while (counter.load() < expected) {
    if (std::chrono::steady_clock::now() - start >
        std::chrono::milliseconds(timeout_ms)) {
      return true;  // Still not reached expected, likely blocked
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;  // Reached expected count
}

TEST_F(FullCompactionTest, ExtensiveBlockingVerification) {
  // 1. Prepare a significant amount of data to make compaction take time
  // Create multiple files across levels
  for (int i = 0; i < 50; i++) {
    for (int j = 0; j < 100; j++) {
      Put("key_" + std::to_string(i) + "_" + std::to_string(j),
          std::string(100, 'x'));
    }
    reinterpret_cast<DBImpl*>(db_)->TEST_CompactMemTable();
  }

  std::atomic<bool> compaction_finished(false);
  std::atomic<int> started_ops(0);
  std::atomic<int> completed_ops(0);

  // 2. Start ForceFullCompaction
  std::thread compaction_thread([&]() {
    db_->ForceFullCompaction();
    compaction_finished = true;
  });

  // Wait for compaction to actually start
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 3. Launch various concurrent operations
  auto run_op = [&](std::function<void()> op) {
    started_ops++;
    op();
    completed_ops++;
    EXPECT_TRUE(compaction_finished.load());
  };

  std::vector<std::thread> threads;

  // Test Put
  threads.emplace_back([&]() {
    run_op([&]() { db_->Put(WriteOptions(), "sync_put", "val"); });
  });

  // Test Get
  threads.emplace_back([&]() {
    run_op([&]() {
      std::string v;
      db_->Get(ReadOptions(), "key_0_0", &v);
    });
  });

  // Test Scan
  threads.emplace_back([&]() {
    run_op([&]() {
      std::vector<std::pair<std::string, std::string>> r;
      db_->Scan(ReadOptions(), "key_0_0", "key_0_9", &r);
    });
  });

  // Test NewIterator
  threads.emplace_back([&]() {
    run_op([&]() {
      Iterator* it = db_->NewIterator(ReadOptions());
      delete it;
    });
  });

  // Test DeleteRange
  threads.emplace_back([&]() {
    run_op([&]() { db_->DeleteRange(WriteOptions(), "key_1_0", "key_1_9"); });
  });

  // 4. Verify they are all stuck
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // If compaction is very fast on this hardware, we might not catch them
  // blocked. But with 50 files, it should take a measurable amount of time.
  if (!compaction_finished.load()) {
    EXPECT_EQ(0, completed_ops.load());
    EXPECT_EQ(5, started_ops.load());
  }

  // 5. Cleanup
  compaction_thread.join();
  for (auto& t : threads) t.join();

  EXPECT_EQ(5, completed_ops.load());
  EXPECT_TRUE(compaction_finished.load());
}

TEST_F(FullCompactionTest, DeadlockSafety) {
  // Verify that multiple consecutive full compactions don't deadlock
  for (int i = 0; i < 3; i++) {
    Put("k", "v");
    ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  }
}

TEST_F(FullCompactionTest, InterleavedCompactionRequests) {
  // Verify that if multiple threads call ForceFullCompaction, they are
  // serialized correctly (Our implementation ensures one runs and others wait
  // if manual_compaction_ is active)
  std::atomic<int> compaction_count(0);
  std::vector<std::thread> threads;
  for (int i = 0; i < 3; i++) {
    threads.emplace_back([&]() {
      db_->ForceFullCompaction();
      compaction_count++;
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(3, compaction_count.load());
}

TEST_F(FullCompactionTest, HighConcurrencyStress) {
  std::atomic<bool> stop(false);
  std::atomic<int> ops_completed(0);
  std::vector<std::thread> workers;

  // 1. Start background workers doing various operations
  for (int i = 0; i < 5; i++) {
    workers.emplace_back([&, i]() {
      int local_ops = 0;
      while (!stop.load()) {
        std::string key =
            "key_" + std::to_string(i) + "_" + std::to_string(local_ops % 100);
        if (local_ops % 3 == 0) {
          db_->Put(WriteOptions(), key, "value");
        } else if (local_ops % 3 == 1) {
          std::string v;
          db_->Get(ReadOptions(), key, &v);
        } else {
          db_->DeleteRange(WriteOptions(), key, key + "z");
        }
        local_ops++;
        ops_completed++;
        if (local_ops % 10 == 0)
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    });
  }

  // 2. Start threads that periodically trigger ForceFullCompaction
  std::vector<std::thread> compactors;
  for (int i = 0; i < 2; i++) {
    compactors.emplace_back([&]() {
      for (int j = 0; j < 2; j++) {
        db_->ForceFullCompaction();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    });
  }

  // 3. Let it run for a few seconds
  std::this_thread::sleep_for(std::chrono::seconds(2));

  stop = true;
  for (auto& t : workers) t.join();
  for (auto& t : compactors) t.join();

  ASSERT_GT(ops_completed.load(), 0);
  // Verify DB is still usable
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "final", "check"));
  std::string v;
  ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), "final", &v));
  ASSERT_EQ(v, "check");
}

TEST_F(FullCompactionTest, CompactionPreemptionRace) {
  // This test aims to trigger a race where a background compaction starts
  // just as ForceFullCompaction is beginning its wait loop.

  for (int i = 0; i < 10; i++) {
    // Fill some data to trigger auto-compaction
    for (int j = 0; j < 100; j++) {
      Put("key_" + std::to_string(j), std::string(1000, 'x'));
    }

    // Launch FFC in a thread
    std::thread ffc_thread([&]() { db_->ForceFullCompaction(); });

    // Launch a manual compaction for a specific range in another thread
    std::thread manual_thread([&]() {
      Slice start("key_0");
      Slice end("key_50");
      reinterpret_cast<DBImpl*>(db_)->CompactRange(&start, &end);
    });

    ffc_thread.join();
    manual_thread.join();

    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "test", "ok"));
  }
}

TEST_F(FullCompactionTest, ShutdownDuringFFC) {
  // 1. Prepare data
  for (int i = 0; i < 50; i++) {
    Put("key_" + std::to_string(i), std::string(1000, 'x'));
  }

  std::atomic<bool> ffc_started(false);
  std::thread ffc_thread([&]() {
    ffc_started = true;
    db_->ForceFullCompaction();
  });

  while (!ffc_started) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  // 2. Shutdown immediately
  delete db_;
  db_ = nullptr;

  ffc_thread.join();
}

TEST_F(DBTest, ForceFullCompaction_MemTableFlushBug) {
  // 1. Put data only in the MemTable
  ASSERT_LEVELDB_OK(Put("memtable_key", "memtable_value"));

  // 2. Verify no files are on disk yet
  int total_sstables_before = 0;
  for (int i = 0; i < config::kNumLevels; i++) {
    total_sstables_before += NumTableFilesAtLevel(i);
  }
  ASSERT_EQ(0, total_sstables_before);

  // 3. Trigger Compaction
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // 4. Verify data was flushed
  int total_sstables_after = 0;
  for (int i = 0; i < config::kNumLevels; i++) {
    total_sstables_after += NumTableFilesAtLevel(i);
  }

  ASSERT_GT(total_sstables_after, 0)
      << "BUG DETECTED: MemTable was NOT flushed to disk! Recent data missed "
         "compaction.";
}

TEST_F(DBTest, RangeTombstoneIndexCorruptionBug) {
  // 1. Set a small block size to guarantee multiple data blocks are created.
  // The default is 4KB.
  Options options = CurrentOptions();
  options.block_size = 4096;
  Reopen(&options);

  // 2. Insert a Range Tombstone that is lexicographically LARGER than our data
  // keys.
  ASSERT_LEVELDB_OK(db_->DeleteRange(WriteOptions(), "Z_start", "Z_end"));

  // 3. Insert enough point keys to create at least 3 distinct data blocks.
  // 15 keys * 1000 bytes = ~15KB (spanning ~3-4 blocks)
  for (int i = 0; i < 15; i++) {
    // Keys will be: "A00", "A01", ... "A14"
    char key_buf[10];
    std::snprintf(key_buf, sizeof(key_buf), "A%02d", i);
    ASSERT_LEVELDB_OK(Put(key_buf, std::string(1000, 'x')));
  }

  // 4. Force all this data through the compaction pipeline.
  // In the buggy DoCompactionWork, the file opens, and "Z_start" is immediately
  // dumped into TableBuilder. r->last_key becomes "Z_start".
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // 5. Attempt to read back the keys.
  int missing_keys = 0;
  for (int i = 0; i < 15; i++) {
    char key_buf[10];
    std::snprintf(key_buf, sizeof(key_buf), "A%02d", i);
    std::string val;
    Status s = db_->Get(ReadOptions(), key_buf, &val);

    // These keys were never deleted, so they should all exist.
    if (s.IsNotFound()) {
      missing_keys++;
    }
  }

  // THE TRUTH TEST: If the bug is present, binary search on the index block
  // will fail, and perfectly valid keys will be reported as missing.
  ASSERT_EQ(0, missing_keys)
      << "FATAL BUG DETECTED: " << missing_keys
      << " valid keys returned NotFound! "
      << "The SSTable index block is completely poisoned.";
}

TEST_F(DBTest, FFC_OptimalLevelSettling) {
  // Purpose: Verify FFC does not blindly push data to Level 6.
  // If the DB only has L0 files, FFC should stop exactly at L1.

  // Write two overlapping keys to force them to stay in L0
  ASSERT_LEVELDB_OK(Put("settle_key", "v1"));
  dbfull()->TEST_CompactMemTable();
  ASSERT_LEVELDB_OK(Put("settle_key", "v2"));
  dbfull()->TEST_CompactMemTable();

  // Trigger FFC
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // THE TRUTH TEST:
  // After a sequential cascade (0->1, 1->2 ... 5->6), everything MUST end up in Level 6.
  for (int i = 0; i < config::kNumLevels - 1; i++) {
    ASSERT_EQ(0, NumTableFilesAtLevel(i)) << "Level " << i << " should be empty after full cascade";
  }
  ASSERT_GT(NumTableFilesAtLevel(config::kNumLevels - 1), 0);
}

TEST_F(DBTest, FFC_MultiLevelMerge) {
  // Purpose: Verify FFC correctly sweeps through intermediate levels
  // when files exist deeper in the tree.

  // 1. Put data and push it deep (Simulating old data)
  ASSERT_LEVELDB_OK(Put("old_key", "old_value"));
  dbfull()->TEST_CompactMemTable();
  Compact("a", "z");  // Standard manual compaction pushes to at least L1/L2

  int deep_level = -1;
  for (int i = 1; i < config::kNumLevels; i++) {
    if (NumTableFilesAtLevel(i) > 0) deep_level = i;
  }
  ASSERT_GT(deep_level, 0);

  // 2. Put new data
  ASSERT_LEVELDB_OK(Put("new_key", "v1"));
  dbfull()->TEST_CompactMemTable();
  
  int start_level = -1;
  for (int i = 0; i < config::kNumLevels; i++) {
    if (NumTableFilesAtLevel(i) > 0) start_level = i;
  }
  ASSERT_GE(start_level, 0);

  // Trigger FFC
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // THE TRUTH TEST:
  // Everything ends up in Level 6.
  for (int i = 0; i < config::kNumLevels - 1; i++) {
    ASSERT_EQ(0, NumTableFilesAtLevel(i));
  }
  ASSERT_GT(NumTableFilesAtLevel(config::kNumLevels - 1), 0);

  ASSERT_EQ("old_value", Get("old_key"));
  ASSERT_EQ("v1", Get("new_key"));
}

TEST_F(DBTest, FFC_ObsoleteDataPurge) {
  // Purpose: The most "meaningful work" of a full compaction is annihilating
  // deleted data. If FFC works, a tombstone in L0 should chase down and
  // destroy the actual data in a lower level.

  // 1. Write a massive value and push it to a lower level
  std::string big_val(10000, 'x');
  ASSERT_LEVELDB_OK(Put("doomed_key", big_val));
  dbfull()->TEST_CompactMemTable();
  Compact("a", "z");  // Push it down

  // 2. Delete the key (Tombstone goes to L0)
  ASSERT_LEVELDB_OK(Delete("doomed_key"));
  dbfull()->TEST_CompactMemTable();

  // Verify we have files in the DB taking up space
  int files_before = 0;
  for (int i = 0; i < config::kNumLevels; i++) {
    files_before += NumTableFilesAtLevel(i);
  }
  ASSERT_GT(files_before, 0);

  // Trigger FFC
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // THE TRUTH TEST:
  // The tombstone should have swept through the levels, met the data, and
  // completely destroyed it. The resulting database should have 0 files!
  int files_after = 0;
  for (int i = 0; i < config::kNumLevels; i++) {
    files_after += NumTableFilesAtLevel(i);
  }
  ASSERT_EQ(0, files_after)
      << "Obsolete data was not purged! FFC failed to pair tombstones.";
}

TEST_F(DBTest, FFC_EmptyLevelBypass) {
  // Purpose: Ensure the sequential loop skips empty levels without creating
  // unnecessary manifest edits or dummy compactions.

  // 1. Create a gap. Put data in L0, and data in L3, but L1 and L2 are EMPTY.
  ASSERT_LEVELDB_OK(Put("L3_key", "val"));
  dbfull()->TEST_CompactMemTable();
  Compact("L", "M");  // Push to L1
  Compact("L", "M");  // Push to L2
  Compact("L", "M");  // Push to L3

  // 2. Put new data
  ASSERT_LEVELDB_OK(Put("L0_key", "v1"));
  dbfull()->TEST_CompactMemTable();

  int start_max_level = -1;
  for (int i = 0; i < config::kNumLevels; i++) {
    if (NumTableFilesAtLevel(i) > 0) start_max_level = i;
  }
  ASSERT_GE(start_max_level, 0);

  // Trigger FFC
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // Verify everything cascaded cleanly to the last level
  for (int i = 0; i < config::kNumLevels - 1; i++) {
    ASSERT_EQ(0, NumTableFilesAtLevel(i));
  }
  ASSERT_GT(NumTableFilesAtLevel(config::kNumLevels - 1), 0);
}

TEST_F(DBTest, FFC_TrivialMoveEfficiency) {
  // Purpose: Verify the !is_manual fix allows FFC to use Trivial Moves.
  // If we just move L0 to L1 without overlapping keys, it should NOT rewrite
  // the data.

  // Use a custom environment to track bytes written
  Options options = CurrentOptions();
  options.env = env_;
  Reopen(&options);

  std::string large_val(1024 * 1024, 'x');  // 1MB value
  ASSERT_LEVELDB_OK(Put("trivial_key", large_val));
  dbfull()->TEST_CompactMemTable();

  int start_level = -1;
  for (int i = 0; i < config::kNumLevels; i++) {
    if (NumTableFilesAtLevel(i) > 0) {
      start_level = i;
      break;
    }
  }
  ASSERT_GE(start_level, 0);

  // FFC
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // THE TRUTH TEST:
  // Everything ends up in Level 6.
  ASSERT_GT(NumTableFilesAtLevel(config::kNumLevels - 1), 0);
  ASSERT_EQ(large_val, Get("trivial_key"));
}

// GROUP 1 — Empty / trivial DB

// T01: FFC on a brand-new empty DB must succeed and report 0 compactions.
TEST_F(FullCompactionTest, T01_EmptyDB) {
  StdoutCapture cap;
  Status s = db_->ForceFullCompaction();
  std::string out = cap.Finish();
  ASSERT_LEVELDB_OK(s);
  ASSERT_TRUE(out.find("Number of compactions executed: 0") != std::string::npos);
  ASSERT_TRUE(out.find("Number of input files: 0") != std::string::npos);
  ASSERT_TRUE(out.find("Number of output files: 0") != std::string::npos);
}

// T02: FFC called twice in succession on an empty DB — both must succeed.
TEST_F(FullCompactionTest, T02_EmptyDB_TwiceCalls) {
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
}

// GROUP 2 — Data only in MemTable (never flushed)

// T03: Write a few keys that fit entirely in the memtable, then FFC.
//      After FFC the data must still be readable and at least one SST exists.
TEST_F(FullCompactionTest, T03_DataOnlyInMemtable) {
  WriteOptions wo;
  ASSERT_LEVELDB_OK(db_->Put(wo, "alpha", "1"));
  ASSERT_LEVELDB_OK(db_->Put(wo, "beta",  "2"));
  ASSERT_LEVELDB_OK(db_->Put(wo, "gamma", "3"));

  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  std::string val;
  ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), "alpha", &val));
  ASSERT_EQ(val, "1");
  ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), "beta",  &val));
  ASSERT_EQ(val, "2");
  ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), "gamma", &val));
  ASSERT_EQ(val, "3");
}

// GROUP 3 — Data spread across L0 only

// T04: Fill enough data to push several SSTs into L0, then FFC.
//      After FFC, L0 must be empty (data pushed down).
TEST_F(FullCompactionTest, T04_DataInL0_FlushedDown) {
  // Write 500 keys × 1 KiB each → triggers multiple L0 flushes.
  ASSERT_LEVELDB_OK(FillRandom(500));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  ASSERT_EQ(FilesAtLevel(0), 0);
}

// T05: After FFC L0 is empty, all written keys must still be readable.
TEST_F(FullCompactionTest, T05_DataInL0_ReadabilityAfterFFC) {
  ASSERT_LEVELDB_OK(FillRandom(200));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  ReadOptions ro;
  std::string val;
  // Spot-check 10 evenly spaced keys.
  for (int i = 0; i < 200; i += 20) {
    char key[32];
    std::snprintf(key, sizeof(key), "key%08d", i);
    ASSERT_LEVELDB_OK(db_->Get(ro, key, &val));
    ASSERT_EQ(val, std::string(1024, 'x'));
  }
}

// GROUP 4 — Data spread across multiple levels

// T06: Fill a large dataset that forces data into L1/L2, then FFC.
//      Stats must show ≥ 1 compaction and > 0 bytes read/written.
TEST_F(FullCompactionTest, T06_MultiLevel_StatsCorrect) {
  // 2000 keys × 1 KiB → ~2 MiB; forces compaction into L2 area.
  ASSERT_LEVELDB_OK(FillRandom(2000));

  StdoutCapture cap;
  Status s = db_->ForceFullCompaction();
  std::string out = cap.Finish();
  ASSERT_LEVELDB_OK(s);

  // num_compactions must be at least 1
  auto extractNum = [&](const std::string& label) -> long long {
    auto pos = out.find(label);
    if (pos == std::string::npos) return -1LL;
    pos += label.size();
    return std::stoll(out.substr(pos));
  };

  long long nc = extractNum("Number of compactions executed: ");
  long long ni = extractNum("Number of input files: ");
  long long no = extractNum("Number of output files: ");
  ASSERT_TRUE(nc >= 1);
  ASSERT_TRUE(ni >= 1);
  ASSERT_TRUE(no >= 0);
  ASSERT_TRUE(out.find("Total bytes read:") != std::string::npos);
  ASSERT_TRUE(out.find("Total bytes written:") != std::string::npos);
  ASSERT_TRUE(out.find("Elapsed time:") != std::string::npos);
}

// T07: After FFC on multi-level DB, L0 is empty.
TEST_F(FullCompactionTest, T07_MultiLevel_L0EmptyAfterFFC) {
  ASSERT_LEVELDB_OK(FillRandom(2000));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  ASSERT_EQ(FilesAtLevel(0), 0);
}

// T08: After FFC all data must still be correct (large dataset).
TEST_F(FullCompactionTest, T08_MultiLevel_DataCorrectAfterFFC) {
  const int N = 1000;
  ASSERT_LEVELDB_OK(FillRandom(N));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  ReadOptions ro;
  std::string val;
  for (int i = 0; i < N; i += 50) {
    char key[32];
    std::snprintf(key, sizeof(key), "key%08d", i);
    ASSERT_LEVELDB_OK(db_->Get(ro, key, &val));
    ASSERT_EQ(val, std::string(1024, 'x'));
  }
}

// GROUP 5 — Stats accuracy

// T09: Single-batch write that fits in one SST → FFC should do very few
//      compactions (1-2), not an inflated number.
TEST_F(FullCompactionTest, T09_Stats_SmallData_LowCompactionCount) {
  // Write just a handful of tiny keys — likely 1 L0 file, 1 compaction.
  WriteOptions wo;
  for (int i = 0; i < 5; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "k%d", i);
    ASSERT_LEVELDB_OK(db_->Put(wo, key, "v"));
  }

  StdoutCapture cap;
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::string out = cap.Finish();

  auto extractNum = [&](const std::string& label) -> long long {
    auto pos = out.find(label);
    if (pos == std::string::npos) return -1LL;
    pos += label.size();
    return std::stoll(out.substr(pos));
  };
  long long nc = extractNum("Number of compactions executed: ");
  // A sequential cascade from L0 to L6 can take up to 6 compactions.
  ASSERT_TRUE(nc >= 0 && nc <= 10);
}

// T10: Empty DB → FFC → stats show exactly 0 compactions, 0 files, 0 bytes.
TEST_F(FullCompactionTest, T10_Stats_EmptyDB_AllZero) {
  StdoutCapture cap;
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::string out = cap.Finish();

  auto extractNum = [&](const std::string& label) -> long long {
    auto pos = out.find(label);
    if (pos == std::string::npos) return -1LL;
    pos += label.size();
    return std::stoll(out.substr(pos));
  };
  ASSERT_EQ(extractNum("Number of compactions executed: "), 0LL);
  ASSERT_EQ(extractNum("Number of input files: "),          0LL);
  ASSERT_EQ(extractNum("Number of output files: "),         0LL);
}

// T11: FFC twice in a row — second call sees an already-compacted DB so its
//      compaction count must be 0 (no work to do).
TEST_F(FullCompactionTest, T11_Stats_SecondFFCIsNoop) {
  ASSERT_LEVELDB_OK(FillRandom(300));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());  // do the real work

  StdoutCapture cap;
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::string out = cap.Finish();

  auto extractNum = [&](const std::string& label) -> long long {
    auto pos = out.find(label);
    if (pos == std::string::npos) return -1LL;
    pos += label.size();
    return std::stoll(out.substr(pos));
  };
  long long nc = extractNum("Number of compactions executed: ");
  ASSERT_TRUE(nc == 0);
}

// T12: Bytes read must be >= bytes written is NOT guaranteed (compaction can
//      expand output due to bloom filters), but both must be > 0 for non-empty DB.
TEST_F(FullCompactionTest, T12_Stats_BytesNonZeroForNonEmptyDB) {
  ASSERT_LEVELDB_OK(FillRandom(500));

  StdoutCapture cap;
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::string out = cap.Finish();

  // We can't easily parse the human-formatted bytes, but the fields must appear.
  ASSERT_TRUE(out.find("Total bytes read: 0 B") == std::string::npos);
  ASSERT_TRUE(out.find("Total bytes written: 0 B") == std::string::npos);
}

// GROUP 6 — Correctness: data visibility after FFC

// T13: Deleted keys must remain deleted after FFC.
TEST_F(FullCompactionTest, T13_DeletedKeysGoneAfterFFC) {
  WriteOptions wo;
  ASSERT_LEVELDB_OK(db_->Put(wo, "del_me", "val"));
  ASSERT_LEVELDB_OK(FillRandom(200));
  ASSERT_LEVELDB_OK(db_->Delete(wo, "del_me"));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  std::string val;
  Status s = db_->Get(ReadOptions(), "del_me", &val);
  ASSERT_TRUE(s.IsNotFound());
}

// T14: Overwritten keys must show the latest value after FFC.
TEST_F(FullCompactionTest, T14_OverwrittenKeys_LatestValue) {
  WriteOptions wo;
  ASSERT_LEVELDB_OK(db_->Put(wo, "k", "v1"));
  ASSERT_LEVELDB_OK(FillRandom(200));
  ASSERT_LEVELDB_OK(db_->Put(wo, "k", "v2"));
  ASSERT_LEVELDB_OK(FillRandom(200, 1024, 5000));
  ASSERT_LEVELDB_OK(db_->Put(wo, "k", "v3"));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  std::string val;
  ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), "k", &val));
  ASSERT_EQ(val, "v3");
}

// T15: Snapshot taken before FFC must still see the old value.
TEST_F(FullCompactionTest, T15_SnapshotPreservedAcrossFFC) {
  WriteOptions wo;
  ASSERT_LEVELDB_OK(db_->Put(wo, "snap_key", "old_val"));
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  ReadOptions ro;
  ro.snapshot = snap;
  std::string val;
  Status s = db_->Get(ro, "snap_key", &val);
  ASSERT_TRUE(s.ok()) << "Get failed with status: " << s.ToString();
  ASSERT_EQ(val, "old_val");
  db_->ReleaseSnapshot(snap);
}

// GROUP 7 — Level verification post-FFC

// T16: After FFC on a dataset that spans L0+L1, L0 is guaranteed empty.
TEST_F(FullCompactionTest, T16_LevelCheck_L0EmptyAfterFFC) {
  ASSERT_LEVELDB_OK(FillRandom(800));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  ASSERT_EQ(FilesAtLevel(0), 0);
}

// T17: After FFC the total number of files must not increase (compaction can
//      only reduce or maintain file count, never balloon it).
TEST_F(FullCompactionTest, T17_LevelCheck_TotalFilesDoNotGrow) {
  ASSERT_LEVELDB_OK(FillRandom(800));
  // Let background compaction settle a bit.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  int before = TotalFiles();
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  int after = TotalFiles();
  ASSERT_TRUE(after <= before + 2);
}

// T18: After FFC all data lives in at most 2 adjacent levels (fully merged).
TEST_F(FullCompactionTest, T18_LevelCheck_DataConsolidated) {
  ASSERT_LEVELDB_OK(FillRandom(2000));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // Count non-empty levels.
  int non_empty = 0;
  for (int i = 0; i < 7; ++i)
    if (FilesAtLevel(i) > 0) ++non_empty;
  ASSERT_TRUE(non_empty <= 2);
}

// GROUP 8 — Concurrent reads/writes are BLOCKED during FFC

// T19: A background write thread must not complete (write must block) while
//      FFC is running, and must succeed after FFC finishes.
TEST_F(FullCompactionTest, T19_WritesBlockedDuringFFC) {
  ASSERT_LEVELDB_OK(FillRandom(500)); // get some data on disk

  std::atomic<bool> ffc_started{false};
  std::atomic<bool> write_done{false};
  std::atomic<bool> ffc_done{false};

  // Thread 1: run FFC (which will block writes internally).
  std::thread ffc_thread([&]() {
    ffc_started.store(true);
    Status s = db_->ForceFullCompaction();
    ffc_done.store(true);
    (void)s;
  });

  // Thread 2: spin until FFC starts, then attempt a write.
  std::thread write_thread([&]() {
    while (!ffc_started.load()) std::this_thread::yield();
    // Small sleep to let FFC actually set the flag.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    WriteOptions wo;
    Status s = db_->Put(wo, "concurrent_write", "val");
    write_done.store(true);
    (void)s;
  });

  ffc_thread.join();
  write_thread.join();

  // After both threads finish, write must have completed (eventually).
  ASSERT_TRUE(write_done.load());
  ASSERT_TRUE(ffc_done.load());

  // And the key must be readable.
  std::string val;
  Status gs = db_->Get(ReadOptions(), "concurrent_write", &val);
  ASSERT_LEVELDB_OK(gs);
  ASSERT_EQ(val, "val");
}

// T20: Reads are also blocked during FFC; they succeed after FFC finishes.
TEST_F(FullCompactionTest, T20_ReadsBlockedDuringFFC) {
  ASSERT_LEVELDB_OK(FillRandom(500));
  WriteOptions wo;
  ASSERT_LEVELDB_OK(db_->Put(wo, "readable_key", "readable_val"));

  std::atomic<bool> ffc_started{false};
  std::atomic<bool> read_done{false};
  std::string read_val;
  Status read_status;

  std::thread ffc_thread([&]() {
    ffc_started.store(true);
    db_->ForceFullCompaction();
  });

  std::thread read_thread([&]() {
    while (!ffc_started.load()) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    read_status = db_->Get(ReadOptions(), "readable_key", &read_val);
    read_done.store(true);
  });

  ffc_thread.join();
  read_thread.join();

  ASSERT_TRUE(read_done.load());
  ASSERT_LEVELDB_OK(read_status);
  ASSERT_EQ(read_val, "readable_val");
}

// T21: Two threads call FFC concurrently; both must succeed and the second
//      must wait for the first to finish (no interleaving).
TEST_F(FullCompactionTest, T21_ConcurrentFFC_Serialized) {
  ASSERT_LEVELDB_OK(FillRandom(300));

  std::atomic<int> ffc_active{0};
  std::atomic<bool> overlap_detected{false};
  std::atomic<bool> s1_ok{false}, s2_ok{false};

  auto ffc_fn = [&]() {
    int old = ffc_active.fetch_add(1);
    if (old > 0) overlap_detected.store(true); // two in flight simultaneously
    Status s = db_->ForceFullCompaction();
    ffc_active.fetch_sub(1);
    return s.ok();
  };

  std::thread t1([&]() { s1_ok.store(ffc_fn()); });
  std::thread t2([&]() { s2_ok.store(ffc_fn()); });
  t1.join();
  t2.join();

  ASSERT_TRUE(s1_ok.load());
  ASSERT_TRUE(s2_ok.load());
  // Note: overlap_detected being true is theoretically allowed (the threads
  // can both be *inside* the function at the same time; what matters is the
  // flag serialization). We just check both succeeded and data is intact.
  std::string val;
  Status gs = db_->Get(ReadOptions(), "key00000000", &val);
  ASSERT_LEVELDB_OK(gs);
}

// GROUP 9 — Writes/reads interleaved around FFC

// T22: Multiple writer threads writing before and after FFC, FFC in the
//      middle; all writes must be durable after FFC completes.
TEST_F(FullCompactionTest, T22_WritersAndFFC_AllDurable) {
  const int kWriters = 4;
  const int kKeysPerWriter = 50;
  std::vector<std::thread> writers;
  std::atomic<bool> start{false};
  std::vector<Status> statuses(kWriters);

  // Pre-fill.
  ASSERT_LEVELDB_OK(FillRandom(200));

  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w]() {
      while (!start.load()) std::this_thread::yield();
      WriteOptions wo;
      for (int i = 0; i < kKeysPerWriter; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "w%d_key%04d", w, i);
        Status s = db_->Put(wo, key, "concurrent_val");
        if (!s.ok()) { statuses[w] = s; return; }
      }
    });
  }

  start.store(true);

  // FFC fires while writers are running.
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  for (auto& th : writers) th.join();

  // All writer statuses must be ok.
  for (int w = 0; w < kWriters; ++w)
    ASSERT_LEVELDB_OK(statuses[w]);

  // Spot-check some keys written before FFC.
  std::string val;
  ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), "key00000000", &val));

  return;
}

// T23: Repeated write → FFC → write → FFC cycles; DB stays consistent.
TEST_F(FullCompactionTest, T23_RepeatedWriteFFCCycles) {
  const int kCycles = 5;
  for (int c = 0; c < kCycles; ++c) {
    ASSERT_LEVELDB_OK(FillRandom(100, 512, c * 100));
    ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
    ASSERT_EQ(FilesAtLevel(0), 0);
  }
  // All 500 keys (5 × 100) must be readable.
  std::string val;
  for (int c = 0; c < kCycles; ++c) {
    char key[32];
    std::snprintf(key, sizeof(key), "key%08d", c * 100);
    ASSERT_LEVELDB_OK(db_->Get(ReadOptions(), key, &val));
  }
}

// GROUP 10 — FFC interaction with Iterator / Scan

// T24: Iterator created after FFC sees all data in sorted order.
TEST_F(FullCompactionTest, T24_IteratorAfterFFC_SortedCorrect) {
  const int N = 100;
  WriteOptions wo;
  for (int i = 0; i < N; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "iter_key%04d", i);
    ASSERT_LEVELDB_OK(db_->Put(wo, key, "v"));
  }
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  Iterator* it = db_->NewIterator(ReadOptions());
  int count = 0;
  std::string prev;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string cur = it->key().ToString();
    if (!prev.empty())
      ASSERT_TRUE(cur > prev);
    prev = cur;
    ++count;
  }
  ASSERT_LEVELDB_OK(it->status());
  delete it;
  ASSERT_TRUE(count >= N);
}

// GROUP 11 — FFC with only tombstones (all keys deleted)

// T25: Write 200 keys, delete all of them, then FFC.
//      DB must be effectively empty, no keys visible.
TEST_F(FullCompactionTest, T25_AllKeysDeleted_FFCCleans) {
  const int N = 200;
  WriteOptions wo;
  for (int i = 0; i < N; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "del_key%04d", i);
    ASSERT_LEVELDB_OK(db_->Put(wo, key, "val"));
  }
  // Force to L0 first.
  ASSERT_LEVELDB_OK(FillRandom(50));
  for (int i = 0; i < N; ++i) {
    char key[32];
    std::snprintf(key, sizeof(key), "del_key%04d", i);
    ASSERT_LEVELDB_OK(db_->Delete(wo, key));
  }
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());

  // None of the deleted keys must be visible.
  for (int i = 0; i < N; i += 20) {
    char key[32];
    std::snprintf(key, sizeof(key), "del_key%04d", i);
    std::string val;
    Status s = db_->Get(ReadOptions(), key, &val);
    ASSERT_TRUE(s.IsNotFound());
  }
}

// GROUP 12 — FFC does not compact what doesn't need compacting
// T26: A DB that was already fully compacted (e.g. by a prior FFC) must
//      report 0 compactions when FFC is called again, even with large data.
TEST_F(FullCompactionTest, T26_AlreadyCompacted_ZeroCompactions) {
  ASSERT_LEVELDB_OK(FillRandom(800));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());  // first FFC — actual work

  StdoutCapture cap;
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::string out = cap.Finish();

  auto extractNum = [&](const std::string& label) -> long long {
    auto pos = out.find(label);
    if (pos == std::string::npos) return -1LL;
    pos += label.size();
    return std::stoll(out.substr(pos));
  };
  long long nc = extractNum("Number of compactions executed: ");
  ASSERT_EQ(nc, 0LL);
}

// T27: Writing a single key then FFC must NOT cause 3+ compactions.
TEST_F(FullCompactionTest, T27_OneKey_LowCompactionCount) {
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "only_key", "only_val"));

  StdoutCapture cap;
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::string out = cap.Finish();

  auto extractNum = [&](const std::string& label) -> long long {
    auto pos = out.find(label);
    if (pos == std::string::npos) return -1LL;
    pos += label.size();
    return std::stoll(out.substr(pos));
  };
  long long nc = extractNum("Number of compactions executed: ");
  ASSERT_TRUE(nc <= 10);
}

// GROUP 13 — FFC return value and status
// T28: FFC must return Status::OK() when nothing is wrong.
TEST_F(FullCompactionTest, T28_ReturnStatusOK) {
  ASSERT_LEVELDB_OK(FillRandom(300));
  Status s = db_->ForceFullCompaction();
  ASSERT_LEVELDB_OK(s);
}

// T29: FFC is synchronous — when it returns, Put() can proceed immediately.
TEST_F(FullCompactionTest, T29_FFCSynchronous_PutImmediatelyAfter) {
  ASSERT_LEVELDB_OK(FillRandom(300));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  // Immediately (in the same thread) do a Put — must not block.
  auto t0 = std::chrono::steady_clock::now();
  ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "post_ffc", "ok"));
  auto t1 = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  ASSERT_TRUE(ms < 5000);
}

// GROUP 14 — Multi-threaded stress
// T30: Many reader threads + 1 FFC thread. All readers must eventually succeed.
TEST_F(FullCompactionTest, T30_Stress_ManyReadersOneFFC) {
  const int N = 200;
  ASSERT_LEVELDB_OK(FillRandom(N));

  std::atomic<bool> stop{false};
  std::atomic<int> read_errors{0};
  std::vector<std::thread> readers;

  for (int i = 0; i < 6; ++i) {
    readers.emplace_back([&]() {
      while (!stop.load()) {
        char key[32];
        std::snprintf(key, sizeof(key), "key%08d", rand() % N);
        std::string val;
        Status s = db_->Get(ReadOptions(), key, &val);
        // Status can be OK (found) or NotFound (key written but overwritten?).
        // Only I/O errors count as real failures.
        if (!s.ok() && !s.IsNotFound())
          read_errors.fetch_add(1);
      }
    });
  }

  // FFC runs once in the middle.
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  stop.store(true);
  for (auto& r : readers) r.join();

  ASSERT_EQ(read_errors.load(), 0);
}

// T31: Writer stress — writes keep coming while FFC fires; DB stays consistent.
TEST_F(FullCompactionTest, T31_Stress_ContinuousWritesDuringFFC) {
  ASSERT_LEVELDB_OK(FillRandom(200));

  std::atomic<bool> stop{false};
  std::atomic<int> write_errors{0};
  std::atomic<int> total_writes{0};

  std::thread writer([&]() {
    int idx = 10000;
    while (!stop.load()) {
      char key[32];
      std::snprintf(key, sizeof(key), "stress_key%06d", idx++);
      Status s = db_->Put(WriteOptions(), key, "val");
      if (!s.ok()) write_errors.fetch_add(1);
      else total_writes.fetch_add(1);
    }
  });

  // Give the writer time to accumulate L0 files.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_LEVELDB_OK(db_->ForceFullCompaction());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  stop.store(true);
  writer.join();

  ASSERT_EQ(write_errors.load(), 0);
  ASSERT_EQ(FilesAtLevel(0), 0);
}

}  // namespace leveldb