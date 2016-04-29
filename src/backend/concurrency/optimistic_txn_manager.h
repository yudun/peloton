//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimistic_txn_manager.h
//
// Identification: src/backend/concurrency/optimistic_txn_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/tile_group.h"
#include <boost/container/flat_map.hpp>
#include <boost/smart_ptr/detail/spinlock.hpp>

namespace peloton {
namespace concurrency {

//===--------------------------------------------------------------------===//
// optimistic concurrency control
//===--------------------------------------------------------------------===//

class OptimisticTxnManager : public TransactionManager {
 public:
  OptimisticTxnManager() {}

  virtual ~OptimisticTxnManager() {}

  static OptimisticTxnManager &GetInstance();

  virtual bool IsVisible(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool IsOwner(const storage::TileGroupHeader *const tile_group_header,
                       const oid_t &tuple_id);

  virtual bool IsOwnable(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tuple_id);

  virtual bool AcquireOwnership(
      const storage::TileGroupHeader *const tile_group_header,
      const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void SetOwnership(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformInsert(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformRead(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual bool PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual bool PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id,
                             const ItemPointer &new_location);

  virtual void PerformUpdate(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual void PerformDelete(const oid_t &tile_group_id, const oid_t &tuple_id);

  virtual Result CommitTransaction();

  virtual Result AbortTransaction();

  virtual Transaction *BeginTransaction() {
    txn_id_t txn_id = GetNextTransactionId();
    cid_t begin_cid = INVALID_CID;
    {
      std::lock_guard<boost::detail::spinlock> guard(lock);
      begin_cid = GetNextCommitId();
      running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM][txn_id] = begin_cid;
    }

    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    current_epoch = new Epoch(begin_cid);
    current_epoch->Join();

    return txn;
  }

  virtual void EndTransaction() {
    txn_id_t txn_id = current_txn->GetTransactionId();
    AddEpochToMap(current_txn->GetBeginCommitId(), current_epoch);
    {
      std::lock_guard<boost::detail::spinlock> guard(lock);
      running_txn_buckets_[txn_id % RUNNING_TXN_BUCKET_NUM].erase(txn_id);
    }

    // order is important - first add to map, then call Leave();
    current_epoch->Leave();
    if(GetCurrentEpochId() > current_epoch->GetEpochId()) {
      // some thread has deleted performed GC on current epoch
      // so it is safe to delete the object
      delete current_epoch;
    }

    current_epoch = nullptr;
    delete current_txn;
    current_txn = nullptr;
  }

  virtual cid_t GetMaxCommittedCid() {
    cid_t min_running_cid = MAX_CID;
    for (size_t i = 0; i < RUNNING_TXN_BUCKET_NUM; ++i) {
      {
        std::lock_guard<boost::detail::spinlock> guard(lock);
        if(running_txn_buckets_[i].size()) {
          if(running_txn_buckets_[i].begin()->second < min_running_cid) {
            min_running_cid = running_txn_buckets_[i].begin()->second;
          }
        }
      }
    }
    return min_running_cid;
  }

 private:
  boost::container::flat_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
  boost::detail::spinlock lock;
};
}
}
