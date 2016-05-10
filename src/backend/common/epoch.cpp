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
      // Perform CAS on the smallest epoch. This ensures that only one
      // thread (the one that wins the CAS) cleans one epoch
      if (txn_manager.PerformEpochCAS(smallest_epoch, largest_epoch)) {
        // iterate through all the epochs from smallest to largest and clean
        // them
        for (; smallest_epoch < largest_epoch; smallest_epoch++) {
          Epoch *e = txn_manager.GetEpoch(smallest_epoch);
          if (e == nullptr) {
            continue;
          }
          gc_manager.PerformGC(e);
          // if cleaned some epoch other than self, clean it
          // if cleaned self, signal the calling function (EndTransaction) using
          // retval
          // the calling function will delete the epoch in that case
          txn_manager.EraseEpoch(smallest_epoch);
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
    } else if (smallest_epoch != INVALID_CID) {
      // There are some running threads, we can clean tuples upto max_cid
      max_cid++;
      // adjust max_cid according to max_epochs_per_thread
      if (max_cid - smallest_epoch > max_epochs_per_thread) {
        max_cid = smallest_epoch + max_epochs_per_thread;
      }
      // perform CAS so that only one thread does this
      if (txn_manager.PerformEpochCAS(smallest_epoch, max_cid)) {
        assert(max_cid >= smallest_epoch);
        // clean the epochs from [smallest_epoch, max_cid), because we
        // incremented max_cid above
        for (; smallest_epoch < max_cid; smallest_epoch++) {
          Epoch *e = txn_manager.GetEpoch(smallest_epoch);
          if (e == nullptr) {
            continue;
          }
          // perform GC of this epoch's possibly free list
          gc_manager.PerformGC(e);
          txn_manager.EraseEpoch(smallest_epoch);
          if (e != this) {
            // delete self reference afterwards, handled by setting the retval
            delete e;
          } else {
            retval = true;
          }
        }
        return retval;
      } else {
        return retval;
      }
    }
  }
  return retval;
}

}  // namespace peloton
