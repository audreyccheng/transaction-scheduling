// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once


#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

class OptimisticTransaction : public TransactionBaseImpl {
 public:
  OptimisticTransaction(OptimisticTransactionDB* db,
                        const WriteOptions& write_options,
                        const OptimisticTransactionOptions& txn_options);
  // No copying allowed
  OptimisticTransaction(const OptimisticTransaction&) = delete;
  void operator=(const OptimisticTransaction&) = delete;

  virtual ~OptimisticTransaction();

  void Reinitialize(OptimisticTransactionDB* txn_db,
                    const WriteOptions& write_options,
                    const OptimisticTransactionOptions& txn_options);

  Status Schedule(int type) override;

  using Transaction::GetKey;
  Status GetKey(const ReadOptions& options, const Slice& key, std::string* value) override;

  using Transaction::GetForUpdateKey;
  Status GetForUpdateKey(const ReadOptions& options, const Slice& key,
                              std::string* value, bool exclusive,
                              const bool do_validate) override;

  using Transaction::PutKey;
  Status PutKey(const Slice& key, const Slice& value) override;

  using Transaction::DeleteKey;
  Status DeleteKey(const Slice& key) override;

  using Transaction::LoadHotKey;
  Status LoadHotKey(const Slice& key, const Slice& value, bool isReadWrite) override;

  // Status KeySchedule(int type, const std::vector<std::string>& keys) override;

  // using Transaction::Get;
  // Status Get(const ReadOptions& options,
  //            const Slice& key, std::string* value) override;

  // using Transaction::GetForUpdate;
  // Status GetForUpdate(const ReadOptions& options, const Slice& key,
  //                     std::string* value, bool exclusive,
  //                     const bool do_validate) override;

  // Status Put(const Slice& key, const Slice& value) override;

  // TODO(accheng): scheduling for Delete() currently unimplemented

  Status Prepare() override;

  Status Commit() override;

  Status Rollback() override;

  Status SetName(const TransactionName& name) override;

 protected:
  Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                 bool read_only, bool exclusive, const bool do_validate = true,
                 const bool assume_tracked = false) override;

 private:
  ROCKSDB_FIELD_UNUSED OptimisticTransactionDB* const txn_db_;

  friend class OptimisticTransactionCallback;

  void Initialize(const OptimisticTransactionOptions& txn_options);

  // Returns OK if it is safe to commit this transaction.  Returns Status::Busy
  // if there are read or write conflicts that would prevent us from committing
  // OR if we can not determine whether there would be any such conflicts.
  //
  // Should only be called on writer thread.
  Status CheckTransactionForConflicts(DB* db);

  void TryScheduleKey(const Slice& key);

  void FreeLock();

  void Clear() override;

  void UnlockGetForUpdate(ColumnFamilyHandle* /* unused */,
                          const Slice& /* unused */) override {
    // Nothing to unlock.
  }

  Status CommitWithSerialValidate();

  Status CommitWithParallelValidate();
};

// Used at commit time to trigger transaction validation
class OptimisticTransactionCallback : public WriteCallback {
 public:
  explicit OptimisticTransactionCallback(OptimisticTransaction* txn)
      : txn_(txn) {}

  Status Callback(DB* db) override {
    return txn_->CheckTransactionForConflicts(db);
  }

  bool AllowWriteBatching() override { return false; }

 private:
  OptimisticTransaction* txn_;
};

class OptimisticScheduleCallback : public WriteCallback {
 public:
  explicit OptimisticScheduleCallback(OptimisticTransaction* txn)
      : txn_(txn) {((void)txn_); } // TODO(accheng): needed?

  Status Callback(DB* db) override {
    assert(db != nullptr);
    if (db == nullptr) {
      return Status::Busy();
    }

    // TODO(accheng): after schedule successful
    auto txn_impl = reinterpret_cast<Transaction*>(txn_);
    txn_impl->ReleaseCV();

    return Status::OK();
  }

  bool AllowWriteBatching() override { return false; }

  // uint16_t GetTxnCluster() {
  //   return txn_->GetCluster();
  // }

 private:
  OptimisticTransaction* txn_;
};

}  // namespace ROCKSDB_NAMESPACE
