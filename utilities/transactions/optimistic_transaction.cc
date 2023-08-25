//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).


#include "utilities/transactions/optimistic_transaction.h"

#include <cstdint>
#include <string>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/cast_util.h"
#include "util/defer.h"
#include "util/string_util.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include "utilities/transactions/optimistic_transaction.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

struct WriteOptions;

OptimisticTransaction::OptimisticTransaction(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options)
    : TransactionBaseImpl(txn_db->GetBaseDB(), write_options,
                          PointLockTrackerFactory::Get()),
      txn_db_(txn_db) {
  Initialize(txn_options);
}

void OptimisticTransaction::Initialize(
    const OptimisticTransactionOptions& txn_options) {
  if (txn_options.set_snapshot) {
    SetSnapshot();
  }
}

void OptimisticTransaction::Reinitialize(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  TransactionBaseImpl::Reinitialize(txn_db->GetBaseDB(), write_options);
  Initialize(txn_options);
  this->cluster_ = 0;
  this->ready_ = false;
}

OptimisticTransaction::~OptimisticTransaction() {}

void OptimisticTransaction::Clear() { TransactionBaseImpl::Clear(); }

// Status OptimisticTransaction::Schedule(int type) {
//   // Set up callback which will schedule this transaction.
//   OptimisticScheduleCallback callback(this);

//   auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
//                                             OptimisticTransactionDB>(txn_db_);
//   assert(txn_db_impl);

//   uint16_t cluster = (uint16_t) type;
//   this->SetCluster(cluster);

//   Status s;
//   // // NEW CODE
//   // // if (cluster > 20) {
//   //   s = txn_db_impl->PartialScheduleImpl(cluster, this, &callback);
//   // // } else {
//   // // // OLD CODE
//   // // s = txn_db_impl->ScheduleImpl(cluster, this, &callback);
//   // // }

//   s = txn_db_impl->NewScheduleImpl(cluster, this, &callback);

//   return s;
// }

Status OptimisticTransaction::Schedule(int type) {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);

  uint16_t cluster = (uint16_t) type;
  this->SetCluster(cluster);

  Status s = txn_db_impl->TScheduleImpl(cluster, this);

  return s;
}

Status OptimisticTransaction::GetKey(const ReadOptions& options, const Slice& key, std::string* value) {
  std::string key_str(key.data());
  // std::string key_byte(key.data());
  // std::cout << "Get key: " << key_byte << std::endl;
  // int key_val = stoi(key_byte.substr(0,15));
  // std::string key_str = std::to_string(key_val);
  std::cout << "Get key: " << key_str  << " tid: " << this->GetIndex() << std::endl;
  bool get_success = false;
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);

  // if (txn_db_impl->CheckHotKey(key_str)) {
    // std::cout << "Get HOT key: " << key_str << std::endl;
    // if (this->GetCluster() != 0) {
    //   txn_db_impl->ScheduleKey(this->GetCluster(), key_str, 0 /* rw */, this);
    // }

    if (txn_db_impl->CheckHotKey(key_str) && this->GetCluster() != 0) {
      txn_db_impl->ScheduleKey(this->GetCluster(), key_str, 0 /* rw */, this);
    }

    auto rp = txn_db_impl->AddReadVersion(key_str, this->index_); // if id == 0, fetch from DB
    if (rp.first != 0) {
      get_success = true;
      this->read_versions_[key_str] = rp.first;
      this->read_values_[key_str] = rp.second;
      value->assign(this->read_values_[key_str].c_str(), this->read_values_[key_str].length());
      // std::cout << "Getkey: " << key_str
      // << " rp.second size: " << rp.second.length()
      // << " rvalue size: " << this->read_values_[key_str].length()
      // << " value size: " << value->length()
      // << std::endl;
    }
    // else {
    //   std::string empty("");
    //   value->assign(empty.c_str(), empty.length());
    //   std::cout << "NO VALUE GetForUpdate key: " << key_str << std::endl;
    //   // return Status::Busy();
    // }

    // if (this->GetCluster() != 0) {
    //   txn_db_impl->KeySubCount(key_str, 0 /* rw */, this->GetIndex());
    // }
  // }
    if (txn_db_impl->CheckHotKey(key_str) && this->GetCluster() != 0) {
      txn_db_impl->KeySubCount(key_str, 0 /* rw */, this->GetIndex());
    }

  // // still call Get to get the right lock
  auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
  if (get_success) {
    std::string temp_value;
    return txn_impl->Get(options, key, &temp_value);
  }
  return txn_impl->Get(options, key, value);
}

Status OptimisticTransaction::GetForUpdateKey(const ReadOptions& options, const Slice& key,
                              std::string* value, bool exclusive,
                              const bool do_validate) {
  std::string key_str(key.data());
  // std::string key_byte(key.data());
  // std::cout << "GetForUpdate key: " << key_byte << std::endl;
  // int key_val = stoi(key_byte.substr(0,15));
  // std::string key_str = std::to_string(key_val);
  std::cout << "GetForUpdate key: " << key_str << " tid: " << this->GetIndex() << std::endl;
  bool get_success = false;
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  if (txn_db_impl->CheckHotKey(key_str) && this->GetCluster() != 0) {
    txn_db_impl->ScheduleKey(this->GetCluster(), key_str, 1 /* rw */, this);
    this->AddHK(key_str);
  }
  // std::cout << "DONE queueing GetForUpdate key: " << key_str << " tid: " << this->GetIndex() << std::endl;
  // if (txn_db_impl->CheckHotKey(key_str)) {
    // std::cout << "GetForUpdate HOT key: " << key_str << std::endl;
    // if (this->GetCluster() != 0) {
    //   txn_db_impl->ScheduleKey(this->GetCluster(), key_str, 1 /* rw */, this);
    // }

    auto rp = txn_db_impl->AddReadVersion(key_str, this->index_); // if id == 0, fetch from DB
    if (rp.first != 0) {
      get_success = true;
      this->read_versions_[key_str] = rp.first;
      this->read_values_[key_str] = rp.second;
      value->assign(this->read_values_[key_str].c_str(), this->read_values_[key_str].length());
      // std::cout << "GetForUpdate key: " << key_str
      // << " rp.second size: " << rp.second.length()
      // << " rvalue size: " << this->read_values_[key_str].length()
      // << " value size: " << value->length()
      // << std::endl;
    }
    // else {
    //   std::string empty("");
    //   value->assign(empty.c_str(), empty.length());
    //   std::cout << "NO VALUE GetForUpdate key: " << key_str << std::endl;
    //   // return Status::Busy();
    // }
  // }

  // still call Get to get the right lock
  auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
  if (get_success && value->length() > 0) {
    std::string temp_value;
    return txn_impl->GetForUpdate(options, key, &temp_value, exclusive, do_validate);
  }
  return txn_impl->GetForUpdate(options, key, value, exclusive, do_validate);
}

Status OptimisticTransaction::PutKey(const Slice& key, const Slice& value) {
  std::string key_str(key.data());
  // std::string key_byte(key.data());
  // std::cout << "Put key: " << key_byte << std::endl;
  // int key_val = stoi(key_byte.substr(0,15));
  // std::string key_str = std::to_string(key_val);
  std::cout << "Put key: " << key_str << " tid: " << this->GetIndex() << std::endl;
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  bool put_success = false;
  // if (txn_db_impl->CheckHotKey(key_str)) {
    // std::cout << "Put HOT key: " << key_str << " val_size:" << value.size() << std::endl;
    if (txn_db_impl->AddWriteVersion(key_str, value, this->index_)) {
      put_success = true;
      size_t len = value.size();
      char* val = new char[len];
      strcpy(val, value.data());
      std::string val_str(val, len);
      this->write_values_[key_str] = val_str;
      // std::cout << "SL Putkey: " << key_str << " val_size:" << this->write_values_[key_str].length() << std::endl;
    }

    if (txn_db_impl->CheckHotKey(key_str) && this->GetCluster() != 0) {
      txn_db_impl->KeySubCount(key_str, 1 /* rw */, this->GetIndex());
      this->RemoveHK(key_str);
    }

    if (!put_success) {
      // std::cout << "MVTSO Write fail! key: " << key_str << std::endl;
      return Status::Busy();
    }

    // if (this->GetCluster() != 0) {
    //   txn_db_impl->KeySubCount(key_str, 1 /* rw */, this->GetIndex());
    // }
  // }

  auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
  return txn_impl->Put(key, value);
}

Status OptimisticTransaction::DeleteKey(const Slice& key) {
  // std::string key_byte(key.data());
  // uint64_t key_val = (uint64_t) stoi(key_byte);
  // std::string key_str = std::to_string(key_val);
  // auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
  //                                           OptimisticTransactionDB>(txn_db_);
  // // std::cout << "Delete HOT key: " << key_str << std::endl;
  //   if (txn_db_impl->AddWriteVersion(key_str, Slice(), this->index_)) {
  //     this->write_values_[key_str] = "";
  //     // std::cout << "SL Deletekey: " << key_str << " val_size:" << this->write_values_[key_str].length() << std::endl;
  //   } else {
  //     // std::cout << "MVTSO Delete fail! key: " << key_str << std::endl;
  //     return Status::Busy();
  //   }

  auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
  return txn_impl->Delete(key);
}

Status OptimisticTransaction::LoadHotKey(const Slice& key, const Slice& value, bool isReadWrite) {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  std::string key_byte(key.data());
  std::cout << "LoadHotKey key: " << key_byte << " len: " << key_byte.length() << std::endl;
  std::string key_str = key_byte.substr(0,18);
  std::cout << "LoadHotKey key_substr: " << key_substr << " len: " << key_str.length() << std::endl;
  // int key_val = stoi(key_substr);
  // std::string key_str = std::to_string(key_val);
  std::string val_str(value.data());
  std::cout << "LoadHotKey value: " << val_str << " key: " << key_str << " rw: " << isReadWrite << std::endl;
  uint16_t val = (uint16_t) stoi(val_str);
  txn_db_impl->AddHotKey(key_str, val, (uint16_t) isReadWrite);

  return Status::OK();
}


// Status OptimisticTransaction::KeySchedule(int type, const std::vector<std::string>& keys) {
//   // Set up callback which will schedule this transaction
//   OptimisticScheduleCallback callback(this);

//   auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
//                                             OptimisticTransactionDB>(txn_db_);
//   assert(txn_db_impl);

//   uint16_t cluster = (uint16_t) type;
//   this->SetCluster(cluster);

//   // std::cout << "KeySchedule keys.size(): " << keys.size() << std::endl;
//   Status s;
//   std::vector<Slice> skeys;
//   for (std::string k : keys) {
//     skeys.push_back(Slice(k));
//     // std::cout << "KeySchedule key: " << k << std::endl;
//   }
//   // std::cout << "KeyScheduleImpl: " << cluster << std::endl;
//   s = txn_db_impl->KeyScheduleImpl(cluster, skeys, this, &callback);

//   // std::cout << "Status: " << s.ToString() << std::endl;
//   return s;
// }

// void OptimisticTransaction::TryScheduleKey(const Slice& key) {
//   if (this->GetCluster() != 0) {
//     auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
//                                             OptimisticTransactionDB>(txn_db_);
//     assert(txn_db_impl);

//     OptimisticScheduleCallback callback(this); // TODO(accheng): always create object?
//     txn_db_impl->ScheduleKeyImpl(key, this, &callback);
//   }
// }

// Status OptimisticTransaction::Get(const ReadOptions& options,
//              const Slice& key, std::string* value) {
//   TryScheduleKey(key);

//   auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
//   return txn_impl->Get(options, key, value);
// }

// Status OptimisticTransaction::GetForUpdate(const ReadOptions& options, const Slice& key,
//                       std::string* value, bool exclusive,
//                       const bool do_validate) {
//   TryScheduleKey(key);

//   auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
//   return txn_impl->GetForUpdate(options, key, value, exclusive, do_validate);
// }

// Status OptimisticTransaction::Put(const Slice& key, const Slice& value) {
//   // TryScheduleKey(key);

//   std::cout << "Put key: " << key.ToString() << std::endl;
//   auto txn_impl = reinterpret_cast<TransactionBaseImpl*>(this);
//   return txn_impl->Put(key, value);
// }

Status OptimisticTransaction::Prepare() {
  return Status::InvalidArgument(
      "Two phase commit not supported for optimistic transactions.");
}

void OptimisticTransaction::FreeLock() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);

  // TODO(accheng): update sched_counts_
  // std::cout << "FreeLock cluster: " << this->GetCluster() << std::endl;
  if (this->GetCluster() != 0) {
    // std::cout << "committing cluster: " << this->GetCluster() << std::endl; // <<  " count: " << sched_counts_[this->GetCluster()]->load()
    // // OLD CODE 222
    // txn_db_impl->SubCount(this->GetCluster());
    // this->SetCluster(0); // In case commit fails, mark that we have already freed this cluster

    txn_db_impl->NewSubCount(this->GetCluster());
    this->SetCluster(0);

    // txn_db_impl->SubDepCount(this);
    // this->SetCluster(0);
    // this->SetIndex(0);
  }
}

Status OptimisticTransaction::Commit() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);

  txn_db_impl->CheckCommitVersions(this);
  txn_db_impl->CleanVersions(this, false);

  if (this->GetAbort()) {
    return Status::Busy();
  }

  Status s = Status::OK();
  switch (txn_db_impl->GetValidatePolicy()) {
    case OccValidationPolicy::kValidateParallel:
      s = CommitWithParallelValidate();
      break;
    case OccValidationPolicy::kValidateSerial:
      s = CommitWithSerialValidate();
      break;
    default:
      s = CommitWithParallelValidate();
      break;
      // assert(0);
  }

  // unreachable, just void compiler complain
  return s;
}

Status OptimisticTransaction::CommitWithSerialValidate() {
  // Set up callback which will call CheckTransactionForConflicts() to
  // check whether this transaction is safe to be committed.
  OptimisticTransactionCallback callback(this);

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());

  Status s = db_impl->WriteWithCallback(
      write_options_, GetWriteBatch()->GetWriteBatch(), &callback);

  FreeLock();
  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::CommitWithParallelValidate() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  assert(db_impl);
  std::set<port::Mutex*> lk_ptrs;
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracked_locks_->GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();

    // To avoid the same key(s) contending across CFs or DBs, seed the
    // hash independently.
    uint64_t seed = reinterpret_cast<uintptr_t>(db_impl) +
                    uint64_t{0xb83c07fbc6ced699} /*random prime*/ * cf;

    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracked_locks_->GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      auto lock_bucket_ptr = &txn_db_impl->GetLockBucket(key_it->Next(), seed);
      TEST_SYNC_POINT_CALLBACK(
          "OptimisticTransaction::CommitWithParallelValidate::lock_bucket_ptr",
          lock_bucket_ptr);
      lk_ptrs.insert(lock_bucket_ptr);
    }
  }
  // NOTE: in a single txn, all bucket-locks are taken in ascending order.
  // In this way, txns from different threads all obey this rule so that
  // deadlock can be avoided.
  for (auto v : lk_ptrs) {
    // WART: if an exception is thrown during a Lock(), previously locked will
    // not be Unlock()ed. But a vector of MutexLock is likely inefficient.
    v->Lock();
  }
  Defer unlocks([&]() {
    for (auto v : lk_ptrs) {
      v->Unlock();
    }
  });

  Status s = Status::OK(); //
  // Status s = TransactionUtil::CheckKeysForConflicts(db_impl, txn_db_, *tracked_locks_,
  //                                                   true /* cache_only */);
  if (!s.ok()) {
    FreeLock();
    return s;
  }

  s = db_impl->Write(write_options_, GetWriteBatch()->GetWriteBatch());
  FreeLock();
  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::Rollback() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  txn_db_impl->CheckCommitVersions(this);
  txn_db_impl->CleanVersions(this, true);

  FreeLock();
  // // TODO(accheng): update sched_counts_
  // if (this->GetCluster() != 0) {
  //   std::cout << "rolling back cluster: " << this->GetCluster() << std::endl; // <<  " count: " << sched_counts_[this->GetCluster()]->load()
  //   txn_db_impl->SubCount(this->GetCluster());
  // }

  Clear();
  return Status::OK();
}

// Record this key so that we can check it for conflicts at commit time.
//
// 'exclusive' is unused for OptimisticTransaction.
Status OptimisticTransaction::TryLock(ColumnFamilyHandle* column_family,
                                      const Slice& key, bool read_only,
                                      bool exclusive, const bool do_validate,
                                      const bool assume_tracked) {
  assert(!assume_tracked);  // not supported
  (void)assume_tracked;
  if (!do_validate) {
    return Status::OK();
  }
  uint32_t cfh_id = GetColumnFamilyID(column_family);

  SetSnapshotIfNeeded();

  SequenceNumber seq;
  if (snapshot_) {
    seq = snapshot_->GetSequenceNumber();
  } else {
    seq = db_->GetLatestSequenceNumber();
  }

  std::string key_str = key.ToString();

  TrackKey(cfh_id, key_str, seq, read_only, exclusive);

  // Always return OK. Confilct checking will happen at commit time.
  return Status::OK();
}

// Returns OK if it is safe to commit this transaction.  Returns Status::Busy
// if there are read or write conflicts that would prevent us from committing OR
// if we can not determine whether there would be any such conflicts.
//
// Should only be called on writer thread in order to avoid any race conditions
// in detecting write conflicts.
Status OptimisticTransaction::CheckTransactionForConflicts(DB* db) {
  auto db_impl = static_cast_with_check<DBImpl>(db);
  if (db_impl == nullptr) {
    return Status::OK();
  }

  // Since we are on the write thread and do not want to block other writers,
  // we will do a cache-only conflict check.  This can result in TryAgain
  // getting returned if there is not sufficient memtable history to check
  // for conflicts.
  return Status::OK();
  // return TransactionUtil::CheckKeysForConflicts(db_impl, txn_db_, *tracked_locks_,
  //                                               true /* cache_only */);
}

Status OptimisticTransaction::SetName(const TransactionName& /* unused */) {
  return Status::InvalidArgument("Optimistic transactions cannot be named.");
}

}  // namespace ROCKSDB_NAMESPACE
