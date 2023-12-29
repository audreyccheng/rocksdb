//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "utilities/transactions/optimistic_transaction_db_impl.h"

#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "utilities/transactions/optimistic_transaction.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<OccLockBuckets> MakeSharedOccLockBuckets(size_t bucket_count,
                                                         bool cache_aligned) {
  if (cache_aligned) {
    return std::make_shared<OccLockBucketsImpl<true>>(bucket_count);
  } else {
    return std::make_shared<OccLockBucketsImpl<false>>(bucket_count);
  }
}

Transaction* OptimisticTransactionDBImpl::BeginTransaction(
    const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options, Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new OptimisticTransaction(this, write_options, txn_options);
  }
}

Status OptimisticTransactionDB::Open(const Options& options,
                                     const std::string& dbname,
                                     OptimisticTransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status OptimisticTransactionDB::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    OptimisticTransactionDB** dbptr) {
  return OptimisticTransactionDB::Open(db_options,
                                       OptimisticTransactionDBOptions(), dbname,
                                       column_families, handles, dbptr);
}

Status OptimisticTransactionDB::Open(
    const DBOptions& db_options,
    const OptimisticTransactionDBOptions& occ_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    OptimisticTransactionDB** dbptr) {
  Status s;
  DB* db;

  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;

  // Enable MemTable History if not already enabled
  for (auto& column_family : column_families_copy) {
    ColumnFamilyOptions* options = &column_family.options;

    if (options->max_write_buffer_size_to_maintain == 0 &&
        options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to
      // max_write_buffer_number * write_buffer_size.
      options->max_write_buffer_size_to_maintain = -1;
    }
  }

  s = DB::Open(db_options, dbname, column_families_copy, handles, &db);

  if (s.ok()) {
    *dbptr = new OptimisticTransactionDBImpl(db, occ_options);
  }

  return s;
}

void OptimisticTransactionDBImpl::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  assert(dynamic_cast<OptimisticTransaction*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<OptimisticTransaction*>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

/** Add trx to appropriate cluster lock queue. */
static void queue_clust_trx(uint16_t cluster, WriteCallback* callback) {
  cluster_hash_mutexes_[cluster].lock();
  cluster_hash_[cluster].push_back(&callback);
  cluster_hash_mutexes_[cluster].unlock();
}

static uint32_t next_sched_idx() {
  uint32_t idx = cluster_sched_idx_ + 1;
  if (idx == cluster_sched_.size()) {
    idx = 1;
  }
  return idx;
}

static uint32_t prev_sched_idx(uint16_t cluster) {
  uint32_t idx = cluster_id - 1;
  if (idx == 0) {
    idx = cluster_sched_.size() - 1;
  }
  return idx;
}

/** Check if there are any ongoing transactions. */
static bool check_ongoing_trx() {
  bool ongoing = false;
  for (uint32_t i = 1; i < sched_counts_.size(); ++i) {
    if (sched_counts_[i]->load() != 0) {
      ongoing = true;
      break;
    }
  }

  return ongoing;
}

/** Update cluster_sched_idx past first instance of given cluster. */
static void update_sched_idx(uint16_t cluster) {
  cluster_sched_idx_ = cluster + 1;
  if (cluster_sched_idx_ == cluster_sched_.size()) {
    cluster_sched_idx_ = 1;
  }
}

static bool lock_clust_peek(uint16_t cluster) {
  bool found = false;
  cluster_hash_mutexes_[cluster].lock();
  if (cluster_hash_[cluster].size() != 0) {
    found = true;
  }
  cluster_hash_mutexes_[cluster].unlock();

  return found;
}

static bool check_ongoing_phase(uint16_t cluster) {
  if (cluster_sched_idx_ == 0) {
    return true;
  }

  if (sched_counts[1]_->load() == 0 && sched_counts_[2]->load() == 0 &&
      !lock_clust_peek(1) && !lock_clust_peek(2)) {
    return true;
  }

  // trx_sys->cluster_sched_idx represents next cluster
  if (cluster == 22) { // P-W1
    if (cluster_sched_idx_ == 1 && sched_counts_[2]->load() != 0) { // NO-W1
      // std::cout << "NO-GO check_ongoing_phase cluster: " << cluster << " trx_sys->cluster_sched_idx: " << trx_sys->cluster_sched_idx << std::endl;
      return false;
    }
  } else { // P-W2
    if (cluster_sched_idx_ == 2 && sched_counts_[1]->load() != 0) { // NO-W2
      // std::cout << "NO-GO check_ongoing_phase cluster: " << cluster << " trx_sys->cluster_sched_idx: " << trx_sys->cluster_sched_idx << std::endl;
      return false;
    }
  }
  // std::cout << "GO check_ongoing_phase cluster: " << cluster
  // << " trx_sys->cluster_sched_idx: " << trx_sys->cluster_sched_idx
  // << " ongoing count: " << trx_sys->sched_counts[trx_sys->cluster_sched_idx]->load()
  // << std::endl;
  return true;
}

static bool partial_release_next_lock(uint16_t cluster) {
  bool found = false;
  cluster_hash_mutexes_[cluster].lock();

  if (cluster_hash_[cluster].size() != 0) { // && check_ongoing_phase(cluster)) {
    found = true;
    // std::cout << "partial releasing cluster: " << cluster << std::endl;
    sched_counts_[cluster]->fetch_add(1);

    /* Initiate first callback in vector. */
    cluster_hash_[cluster][0]->Callback(this); // TODO(accheng): need to pass db?
    cluster_hash_[cluster].erase(cluster_hash_[cluster].begin());
  }
  cluster_hash_mutexes_[cluster].unlock();

  return found;
}

 /** Find the next available cluster and release only the next cluster lock
of that transaction. */
void partial_release_next_clust(uint16_t cluster) {
  if (check_ongoing_phase(cluster)) {
    partial_release_next_lock(cluster);
  }
 }

static void check_partial_release(uint16_t cluster) {
  if (cluster == 1 && lock_clust_peek(22) && sched_counts_[22]->load() == 0) { //
    partial_release_next_clust(22);
  } else if (cluster == 2 && lock_clust_peek(21) && sched_counts_[21]->load() == 0) { //
    partial_release_next_clust(21);
  }
}

/** Find the next available cluster and release the cluster lock of that
 transaction. */
static void release_next_clust() {
  // std::cout << "release_next_clust cluster_sched_idx:" << trx_sys->cluster_sched_idx << std::endl;

  uint32_t curr_idx = cluster_sched_idx_;
  uint16_t cluster = cluster_sched_[curr_idx];

  bool found = false;
  cluster_hash_mutexes_[cluster].lock();
  while (cluster_hash_[cluster].size() != 0) {
    found = true;

    sched_counts_[cluster]->fetch_add(1);

    /* Initiate first callback in vector. */
    cluster_hash_[cluster][0]->Callback(this); // TODO(accheng): need to pass db?
    cluster_hash_[cluster].erase(cluster_hash_[cluster].begin());
  }
  cluster_sched_idx_ = next_sched_idx();
  cluster_hash_mutexes_[cluster].unlock();

  // NEW CODE
  // check_partial_release(cluster);
  // // if (!found) {
  // //   std::cout << "no locks: " << cluster << std::endl;
  // //   print_all_ongoing_trx();
  // // }
}

Status OptimisticTransactionDB::PartialScheduleImpl(uint16_t cluster,
                                            WriteCallback* callback) {
  sys_mutex_.lock();
  // std::cout << "partial trx cluster: " << trx->cluster_id << std::endl;
  if (cluster > 30) {
    sys_mutex_.unlock();

    return Status::OK();
  }

  if (lock_clust_peek(cluster) || sched_counts_[cluster]->load() != 0 //) {
      || !check_ongoing_phase(cluster)) {
    // std::cout << "queuing cluster: " << trx->cluster_id << std::endl;
    queue_clust_trx(cluster, &callback);

    sys_mutex_.unlock();

    return Status::Busy(); // TODO(accheng): update?

  } else {
    // std::cout << "go cluster: " << trx->cluster_id << std::endl;
    sched_counts_[cluster]->fetch_add(1);

    sys_mutex_.unlock();

    return Status::OK();
  }
}

Status OptimisticTransactionDB::ScheduleImpl(uint16_t cluster,
                                            WriteCallback* callback) {
  // TODO(accheng): enqueue callback
  // std::cout << "trx cluster: " << cluster << std::endl;
  if (cluster_sched_idx_ == 0) {
    sys_mutex_.lock();
    // std::cout<< "1-queuing cluster-" << cluster << " count1-" << sched_counts_[prev_sched_idx(cluster)]->load()
    //       << " count2-" << sched_counts_[cluster]->load() << std::endl;
    if (cluster_sched_idx_ != 0) {
      if (!check_ongoing_trx()) {
        sched_counts_[cluster]->fetch_add(1);

        update_sched_idx(cluster);

        sys_mutex_.unlock();
        return Status::OK();
      }
      queue_clust_trx(cluster, &callback);
      sys_mutex_.unlock();
      return Status::Busy(); // TODO(accheng): update?
    }

    while (cluster_sched_[cluster_sched_idx_] != cluster) {
      cluster_sched_idx_ = next_sched_idx();
    }

    sched_counts_[cluster]>fetch_add(1);

    sys_mutex_.unlock();
    return Status::OK();
  } else {
    sys_mutex_.lock();
    // std::cout<< "2-queuing cluster-" << cluster << " count1-" << sched_counts_[prev_sched_idx(cluster)]->load()
    //     << " count2-" << sched_counts_[cluster]->load() << std::endl;
    if (!check_ongoing_trx()) {
      sched_counts_[cluster]>fetch_add(1);

      update_sched_idx(cluster);

      sys_mutex_.unlock();
      return Status::OK();
    }

    queue_clust_trx(cluster, &callback);
    sys_mutex_.unlock();
    return Status::Busy(); // TODO(accheng): update?
  }
}

void SubCount(uint16_t cluster) {
  sys_mutex_.lock();
  sched_counts_[cluster]->fetch_sub(1);

  // NEW CODE
  // if (cluster() > 20) {
  //   partial_release_next_clust();
  // } else {
  // // OLD CODE
  if (sched_counts_[cluster]->load() == 0) {
    release_next_clust();
  }
  // }
  sys_mutex_.unlock();
}


}  // namespace ROCKSDB_NAMESPACE
