
#include "backend/common/types.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/gc/gc_manager.h"
#include "assert.h"

namespace peloton {

void Epoch::AddToPossiblyFreeList(const TupleMetadata tm) {
  possibly_free_list_.Push(tm);
}

void Epoch::Join() {
  /*
   * We call join from every operation that needs to be performed on the tree.
   */
  ref_count.fetch_add(1);
}

bool Epoch::Leave() {
  ref_count.fetch_sub(1);
  if (ref_count == 0) {
    auto &gc_manager = gc::GCManagerFactory::GetInstance();
    if(gc_manager.GetStatus() == false || gc_manager.GetGCType() != GC_TYPE_EPOCH) {
      return false;
    }
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();
    auto smallest_epoch = txn_manager.GetSmallestEpochCleanedCid();
    auto largest_epoch = txn_manager.GetCurrentEpochId(); // to get the largest epoch that can be cleaned
    if(max_cid == MAX_CID) {
      // no transaction is running, i.e. all epochs until largest epoch can be cleaned
      if(txn_manager.PerformEpochCAS(smallest_epoch, largest_epoch)) {
        // iterate through all the epochs from smallest to largest and clean them
        //smallest_epoch++;
        for(; smallest_epoch < largest_epoch; smallest_epoch++) {
          Epoch *e = txn_manager.GetEpoch(smallest_epoch);
          //assert(e != nullptr);
          if(e == nullptr) {
            continue;
          }
          gc_manager.PerformGC(e);
          // perform GC of this epoch's possibly free list
          txn_manager.EraseEpoch(smallest_epoch);
          if(e != this) {
            delete e;
          }
        }
        return true;
      } else {
        return false;
      }
    } else if(smallest_epoch != INVALID_CID) {
      if(txn_manager.PerformEpochCAS(smallest_epoch, max_cid)) {
        //smallest_epoch++;
        for(; smallest_epoch < max_cid; smallest_epoch++) {
          Epoch *e = txn_manager.GetEpoch(smallest_epoch);
          //assert(e != nullptr);
          if(e == nullptr) {
            continue;
          }
          // perform GC of this epoch's possibly free list
          gc_manager.PerformGC(e);
          txn_manager.EraseEpoch(smallest_epoch);
          if(e != this) {
            // delete self reference afterwards
            delete e;
          }
        }
        return true;
      } else {
        return false;
      }
    }
  }
  return false;
}

}  // namespace peloton
