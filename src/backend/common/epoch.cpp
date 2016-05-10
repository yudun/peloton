//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch.cpp
//
// Identification: src/backend/common/epoch.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/types.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/gc/gc_manager.h"
#include "assert.h"

namespace peloton {

// Add the tuple to the epoch's possibly free list
void Epoch::AddToPossiblyFreeList(const TupleMetadata tm) {
  possibly_free_list_.TryPush(tm);
}

// Join the epoch, each thread which performs a transaction does this
void Epoch::Join() {
  // increment the count for number of threads in this epoch
  ref_count.fetch_add(1);
}

// Leave the epoch. Called from EndTransaction
bool Epoch::Leave() {
  // decrease the thread count
  ref_count.fetch_sub(1);
  bool retval = false;
  // if this is the last thread leaving this epoch
  if (ref_count == 0) {
    // make sure we are running in epoch mode
    auto &gc_manager = gc::GCManagerFactory::GetInstance();
    if (gc_manager.GetStatus() == false ||
        gc_manager.GetGCType() != GC_TYPE_EPOCH) {
      return false;
    }
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // order of fetching smallest_epoch, largest_epoch and max_cid is VERY
    // IMPORTANT
    auto smallest_epoch = txn_manager.GetSmallestEpochCleanedCid();
    // get the largest epoch that can be cleaned
    auto largest_epoch = txn_manager.GetCurrentEpochId();
    auto max_cid = txn_manager.GetMaxCommittedCid();
    if (max_cid == MAX_CID) {
      // no transaction is running, i.e. all epochs until largest epoch can be
      // cleaned
      // update the largest epoch according to the max_epochs_per_thread
      assert(largest_epoch >= smallest_epoch);
      if (largest_epoch - smallest_epoch > max_epochs_per_thread) {
        largest_epoch = smallest_epoch + max_epochs_per_thread;
      }
      return GCHelper(smallest_epoch, largest_epoch);
    } else if (smallest_epoch != INVALID_CID) {
      // There are some running threads, we can clean tuples upto max_cid
      max_cid++;
      // adjust max_cid according to max_epochs_per_thread
      if (max_cid - smallest_epoch > max_epochs_per_thread) {
        max_cid = smallest_epoch + max_epochs_per_thread;
      }
      return GCHelper(smallest_epoch, max_cid);
    }
  }
  return retval;
}

// Helper Function for cleaning from @param clean_from_epoch to @param
// clean_till_epoch
bool Epoch::GCHelper(oid_t clean_from_epoch, oid_t clean_till_epoch) {
  bool retval = false;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  // Perform CAS on the smallest epoch. This ensures that only one
  // thread (the one that wins the CAS) cleans one epoch
  if (txn_manager.PerformEpochCAS(clean_from_epoch, clean_till_epoch)) {
    // iterate through all the epochs from smallest to largest and clean
    // them
    for (; clean_from_epoch < clean_till_epoch; clean_from_epoch++) {
      Epoch *e = txn_manager.GetEpoch(clean_from_epoch);
      if (e == nullptr) {
        continue;
      }
      gc_manager.PerformGC(e);
      // if cleaned some epoch other than self, clean it
      // if cleaned self, signal the calling function (EndTransaction) using
      // retval
      // the calling function will delete the epoch in that case
      txn_manager.EraseEpoch(clean_from_epoch);
      if (e != this) {
        delete e;
      } else {
        retval = true;
      }
    }
    return retval;
  } else {
    // lost the CAS, simply return
    return retval;
  }
}

}  // namespace peloton
