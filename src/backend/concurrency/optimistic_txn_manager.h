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
      running_txn_buckets_[begin_cid % RUNNING_TXN_BUCKET_NUM][begin_cid] = txn_id;
    }

    Transaction *txn = new Transaction(txn_id, begin_cid);
    current_txn = txn;

    current_epoch = new Epoch(begin_cid);

    // current thread joins epoch for executing transaction
    current_epoch->Join();

    return txn;
  }

  virtual void EndTransaction() {
    cid_t begin_cid = current_txn->GetBeginCommitId();
    // order is important - first add to map, then call Leave();
    AddEpochToMap(begin_cid, current_epoch);
    {
      std::lock_guard<boost::detail::spinlock> guard(lock);
      running_txn_buckets_[begin_cid % RUNNING_TXN_BUCKET_NUM].erase(begin_cid);
    }

    if(current_epoch->Leave()) {
      // if Leave() returns true, we can safely delete current_epoch
      delete current_epoch;
    }

    current_epoch = nullptr;
    delete current_txn;
    current_txn = nullptr;
  }

  // Returns the largest CID committed when this function was called
  virtual cid_t GetMaxCommittedCid() {
    cid_t min_running_cid = MAX_CID;
    for (size_t i = 0; i < RUNNING_TXN_BUCKET_NUM; ++i) {
      {
        std::lock_guard<boost::detail::spinlock> guard(lock);
        if(running_txn_buckets_[i].size()) {
          if(running_txn_buckets_[i].begin()->first < min_running_cid) {
            min_running_cid = running_txn_buckets_[i].begin()->first;
          }
        }
      }
    }
    if(min_running_cid == MAX_CID) {
      return MAX_CID;
    }
    return min_running_cid - 1;
  }

 private:
  boost::container::flat_map<txn_id_t, cid_t> running_txn_buckets_[RUNNING_TXN_BUCKET_NUM];
  boost::detail::spinlock lock;
};
}
}
