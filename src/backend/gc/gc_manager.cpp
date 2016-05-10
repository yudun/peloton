//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.cpp
//
// Identification: src/backend/gc/gc_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/types.h"
#include "backend/gc/gc_manager.h"
#include "backend/index/index.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"
namespace peloton {
namespace gc {

// Starts the GC based on the GC mode
void GCManager::StartGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  if (this->gc_type_ == GC_TYPE_VACUUM) {
    std::thread(&GCManager::Poll, this).detach();
  }
  this->is_running_ = true;
}

// Stops the GC
void GCManager::StopGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  this->is_running_ = false;
}

// Moves tuples from the possibly free list to the actually free list for the
// corresponding table and updates the mapping from the table id to the table's
// free list
void GCManager::RefurbishTuple(TupleMetadata tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header =
      manager.GetTileGroup(tuple_metadata.tile_group_id)->GetHeader();

  // Set the values for the tuple slot such that when this
  // can be returned by ReturnFreeSlow and used as a new tuple slot
  // by the calling function
  tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id,
                                      INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id, MAX_CID);
  tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id, MAX_CID);

  std::pair<size_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> free_list;

  // if the entry for table_id exists.
  if (free_map_.find(tuple_metadata.table_id, free_list) == true) {
    // if the entry for tuple_metadata.table_id exists.
    free_map_.erase(tuple_metadata.table_id);
    // update the free list and its size
    free_list.second->TryPush(tuple_metadata);
    free_list.first = free_list.first + 1;
  } else {
    // if the entry for tuple_metadata.table_id does not exist
    // create a new free list and initialize its size
    free_list.second.reset(new LockfreeQueue<TupleMetadata>(max_tuples_per_gc));
    free_list.second->TryPush(tuple_metadata);
    free_list.first = 1;
  }
  // Update the map corresponding to the table to now have
  // the updates list
  free_map_[tuple_metadata.table_id] = free_list;
}

// GC for epoch based scheme
void GCManager::PerformGC(Epoch *e) {
  TupleMetadata tuple_metadata;
  // Refurbish tuples from the epoch
  while (e->possibly_free_list_.TryPop(tuple_metadata)) {
    RefurbishTuple(tuple_metadata);
  }
}

// GC for vacuum and cooperative schemes
void GCManager::PerformGC() {
  // Check if we can move anything from the possibly free list to the free list.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto max_cid = txn_manager.GetMaxCommittedCid();
  LOG_INFO("MAX %lu", max_cid);
  // if GC is not running, return without doing anything
  if (is_running_ == false) {
	LOG_INFO("returning");
    return;
  }
  // every time we garbage collect at most max_tuples_per_gc tuples.
  for (size_t i = 0; i < max_tuples_per_gc; ++i) {
    TupleMetadata tuple_metadata;
    // if there's no more tuples in the queue, then break.
    if (possibly_free_list_.TryPop(tuple_metadata) == false) {
      break;
    }
    // if there are no transactions running or if this tuple is not visible to
    // any of the running transactions
    if (max_cid == MAX_CID || tuple_metadata.tuple_end_cid <= max_cid) {
      RefurbishTuple(tuple_metadata);
    } else {
      // if a tuple can't be refurbished, add it back to the list.
      possibly_free_list_.TryPush(tuple_metadata);
    }
  }  // end for
}

// Called by start GC as the thread function when mode is vacuum
void GCManager::Poll() {
  // Loop infinitely and perform GC
  while (1) {
    if (!this->is_running_) break;
    PerformGC();
    std::this_thread::sleep_for(
        std::chrono::seconds(vacuum_thread_sleep_time_));
  }
}

// called by transaction manager.
void GCManager::RecycleTupleSlot(const oid_t &table_id,
                                 const oid_t &tile_group_id,
                                 const oid_t &tuple_id,
                                 const cid_t &tuple_end_cid) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  // Populate the tuple metadata structure
  TupleMetadata tuple_metadata(table_id, tile_group_id, tuple_id,
                               tuple_end_cid);
  // if epoch scheme, then add to the epoch specific possibly free list
  // else add to the global possibly free list
  if (this->gc_type_ == GC_TYPE_EPOCH) {
    peloton::concurrency::current_epoch->AddToPossiblyFreeList(tuple_metadata);
  } else {
    possibly_free_list_.TryPush(tuple_metadata);
  }
}

// this function returns a free tuple slot, if one exists
ItemPointer GCManager::ReturnFreeSlot(const oid_t &table_id) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return ItemPointer();
  }

  std::pair<size_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> free_list;
  // if there exists free_list
  if (free_map_.find(table_id, free_list) == true) {
    TupleMetadata tuple_metadata;
    if (free_list.second->TryPop(tuple_metadata) == true) {
      free_list.first = free_list.first - 1;
      return ItemPointer(tuple_metadata.tile_group_id,
                         tuple_metadata.tuple_slot_id);
    }
  }
  return ItemPointer();
}

// delete a tuple from all its indexes it belongs to.
// TODO: we do not perform this function,
// as we do not have concurrent bw tree right now.
void GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata
                                       __attribute__((unused))) {}

// Get the number of tuples refurbished in a tile group in a table
size_t GCManager::GetRefurbishedTupleSlotCountPerTileGroup(
    const oid_t &table_id, const oid_t &tile_group_id) {
  // if GC mode is off, return 0
  if (this->gc_type_ == GC_TYPE_OFF) {
    return 0;
  }
  size_t count = 0;
  {
    std::lock_guard<std::mutex> lock(free_map_mutex);
    std::pair<size_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> free_list;
    // if there exists free_list for this table
    if (free_map_.find(table_id, free_list)) {
      size_t size = free_list.first;
      // iterate over the free list
      for (unsigned i = 0; i < size; ++i) {
        // pop the queue and then later push it back
        TupleMetadata tuple_metadata;
        if (free_list.second->TryPop(tuple_metadata)) {
          // check whether the tuple belongs to this tile group
          if (tuple_metadata.tile_group_id == tile_group_id) {
            count++;
          }
          free_list.second->TryPush(tuple_metadata);
        }
      }
    }
  }

  return count;
}

}  // namespace gc
}  // namespace peloton
