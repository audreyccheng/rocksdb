//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/cast_util.h"
#include "util/hash_map.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class OccLockBucketsImplBase : public OccLockBuckets {
 public:
  virtual port::Mutex& GetLockBucket(const Slice& key, uint64_t seed) = 0;
};

template <bool cache_aligned>
class OccLockBucketsImpl : public OccLockBucketsImplBase {
 public:
  explicit OccLockBucketsImpl(size_t bucket_count) : locks_(bucket_count) {}
  port::Mutex& GetLockBucket(const Slice& key, uint64_t seed) override {
    return locks_.Get(key, seed);
  }
  size_t ApproximateMemoryUsage() const override {
    return locks_.ApproximateMemoryUsage();
  }

 private:
  // TODO: investigate optionally using folly::MicroLock to majorly save space
  using M = std::conditional_t<cache_aligned, CacheAlignedWrapper<port::Mutex>,
                               port::Mutex>;
  Striped<M> locks_;
};

class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(
      DB* db, const OptimisticTransactionDBOptions& occ_options,
      bool take_ownership = true)
      : OptimisticTransactionDB(db),
        db_owner_(take_ownership),
        validate_policy_(occ_options.validate_policy) {
    if (validate_policy_ == OccValidationPolicy::kValidateParallel) {
      auto bucketed_locks = occ_options.shared_lock_buckets;
      if (!bucketed_locks) {
        uint32_t bucket_count = std::max(16u, occ_options.occ_lock_buckets);
        bucketed_locks = MakeSharedOccLockBuckets(bucket_count);
      }
      bucketed_locks_ = static_cast_with_check<OccLockBucketsImplBase>(
          std::move(bucketed_locks));
    }

    // TODO(accheng): init shared data structs here
    num_clusters_ = 2;

    cluster_sched_idx_ = 0;

    std::vector<int> arr {0, /* No-op */
      1,2 //,3,4,5,6,7,8//,9,10
      // 1,2,3,4,5,6,7,8,//9,10 //,11,12,13,14,15,16,17,18,19,20 //,21,22,23,24,25,26,27,28,29,30,31,32,
    };
    new (&cluster_sched_)(decltype(cluster_sched_))();
    cluster_sched_.resize(arr.size());
    for (uint32_t i = 0; i < cluster_sched_.size(); i++) {
      cluster_sched_[i] = arr[i];
    }

    new (&sched_counts_)(decltype(sched_counts_))();
    sched_counts_.resize(num_clusters_ + 1);
    for (auto& p : sched_counts_) {
      p = std::make_unique<std::atomic<int>>(0);
    }

    cluster_hash_mutexes_.resize(num_clusters_ + 1);
    for (int i = 0; i < num_clusters_ + 1; ++i) {
      cluster_hash_[i] = std::vector<WriteCallback*>();
    }
  }

  ~OptimisticTransactionDBImpl() {
    // Prevent this stackable from destroying
    // base db
    if (!db_owner_) {
      db_ = nullptr;
    }
  }

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const OptimisticTransactionOptions& txn_options,
                                Transaction* old_txn) override;

  // Transactional `DeleteRange()` is not yet supported.
  using StackableDB::DeleteRange;
  virtual Status DeleteRange(const WriteOptions&, ColumnFamilyHandle*,
                             const Slice&, const Slice&) override {
    return Status::NotSupported();
  }

  // Range deletions also must not be snuck into `WriteBatch`es as they are
  // incompatible with `OptimisticTransactionDB`.
  virtual Status Write(const WriteOptions& write_opts,
                       WriteBatch* batch) override {
    if (batch->HasDeleteRange()) {
      return Status::NotSupported();
    }
    return OptimisticTransactionDB::Write(write_opts, batch);
  }

  OccValidationPolicy GetValidatePolicy() const { return validate_policy_; }

  Status PartialScheduleImpl(uint16_t cluster, WriteCallback* callback);

  Status ScheduleImpl(uint16_t cluster, WriteCallback* callback);

  port::Mutex& GetLockBucket(const Slice& key, uint64_t seed) {
    return bucketed_locks_->GetLockBucket(key, seed);
  }

  void SubCount(uint16_t cluster);

 private:
  std::shared_ptr<OccLockBucketsImplBase> bucketed_locks_;

  bool db_owner_;

  const OccValidationPolicy validate_policy_;

  // TODO(accheng): shared data structs here
  std::mutex sys_mutex_;

  const uint16_t num_clusters_;

  uint16_t cluster_sched_idx_;
  std::vector<uint16_t> cluster_sched_;
  std::vector<std::unique_ptr<std::atomic_int>> sched_counts_;

  // Protected map of <cluster, callbacks>
  std::vector<std::mutex> cluster_hash_mutexes_;
  HashMap<uint16_t, std::vector<WriteCallback *>> cluster_hash_;

  // TODO(accheng)
  // locked hash map based on cluster

  void ReinitializeTransaction(Transaction* txn,
                               const WriteOptions& write_options,
                               const OptimisticTransactionOptions& txn_options =
                                   OptimisticTransactionOptions());
};

}  // namespace ROCKSDB_NAMESPACE
