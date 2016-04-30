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

void GCManager::StartGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  if(this->gc_type_ == GC_TYPE_VACUUM) {
    //gc_thread_.reset(new std::thread(&GCManager::Poll, this));
	std::thread(&GCManager::Poll, this).detach();
  }

  this->is_running_ = true;
}

void GCManager::StopGC() {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  this->is_running_ = false;
  //this->gc_thread_->join();
}

void GCManager::RefurbishTuple(TupleMetadata tuple_metadata) {
  auto &manager = catalog::Manager::GetInstance();
  auto tile_group_header =
    manager.GetTileGroup(tuple_metadata.tile_group_id)->GetHeader();

  tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id,
      INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id,
      MAX_CID);
  tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id,
      MAX_CID);

  std::pair<size_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> free_list;

  // if the entry for table_id exists.
  if (free_map_.find(tuple_metadata.table_id, free_list) ==
      true) {
    // if the entry for tuple_metadata.table_id exists.
    free_list.second -> Push(tuple_metadata);
    free_list.first = free_list.first + 1;
  } else {
    // if the entry for tuple_metadata.table_id does not exist.
    free_list.second -> reset(new LockfreeQueue<TupleMetadata>(MAX_TUPLES_PER_GC));
    free_list.second ->Push(tuple_metadata);
    free_list.first = 1;
    free_map_[tuple_metadata.table_id] = free_list;
  }
}

// GC for epoch based co-operative scheme
void GCManager::PerformGC(Epoch *e) {
  TupleMetadata tuple_metadata;
  while(e->possibly_free_list_.Pop(tuple_metadata)) {
    RefurbishTuple(tuple_metadata);
  }
}

void GCManager::PerformGC() {
  // Check if we can move anything from the possibly free list to the free list.
  // auto &manager = catalog::Manager::GetInstance();

  while (true) {
    LOG_DEBUG("Polling GC thread...");

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto max_cid = txn_manager.GetMaxCommittedCid();

    // if max_cid == MAX_CID, then it means there's no running transaction.
    if (max_cid != MAX_CID) {

      // every time we garbage collect at most 1000 tuples.
      for (size_t i = 0; i < MAX_TUPLES_PER_GC; ++i) {

        TupleMetadata tuple_metadata;
        // if there's no more tuples in the queue, then break.
        if (possibly_free_list_.Pop(tuple_metadata) == false) {
          break;
        }

        if (tuple_metadata.tuple_end_cid < max_cid) {
          RefurbishTuple(tuple_metadata);
#if 0
          // Now that we know we need to recycle tuple, we need to delete all
          // tuples from the indexes to which it belongs as well.

          // TODO: currently, we do not delete tuple from indexes,
          // as we do not have a concurrent index yet. --Yingjun
          //DeleteTupleFromIndexes(tuple_metadata);

          auto tile_group_header =
              manager.GetTileGroup(tuple_metadata.tile_group_id)->GetHeader();

          tile_group_header->SetTransactionId(tuple_metadata.tuple_slot_id,
                                              INVALID_TXN_ID);
          tile_group_header->SetBeginCommitId(tuple_metadata.tuple_slot_id,
                                              MAX_CID);
          tile_group_header->SetEndCommitId(tuple_metadata.tuple_slot_id,
                                            MAX_CID);

          std::shared_ptr<LockfreeQueue<TupleMetadata>> free_list;

          // if the entry for table_id exists.
          if (free_map_.find(tuple_metadata.table_id, free_list) ==
              true) {
            // if the entry for tuple_metadata.table_id exists.
            free_list->Push(tuple_metadata);
          } else {
            // if the entry for tuple_metadata.table_id does not exist.
            free_list.reset(new LockfreeQueue<TupleMetadata>(MAX_TUPLES_PER_GC));
            free_list->Push(tuple_metadata);
            free_map_[tuple_metadata.table_id] = free_list;
          }
#endif
        } else {
          // if a tuple can't be reaped, add it back to the list.
          possibly_free_list_.Push(tuple_metadata);
        }
      }  // end for
    }    // end if
    if (is_running_ == false) {
      return;
    }
  }
}

void GCManager::Poll() {
  while (1) {
    LOG_DEBUG("Polling GC thread...");
    PerformGC();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

// called by transaction manager.
void GCManager::RecycleTupleSlot(const oid_t &table_id, const oid_t &tile_group_id, const oid_t &tuple_id, const cid_t &tuple_end_cid) {
  if (this->gc_type_ == GC_TYPE_OFF) {
    return;
  }
  TupleMetadata tuple_metadata;
  tuple_metadata.table_id = table_id;
  tuple_metadata.tile_group_id = tile_group_id;
  tuple_metadata.tuple_slot_id = tuple_id;
  tuple_metadata.tuple_end_cid = tuple_end_cid;

  if (this->gc_type_ == GC_TYPE_EPOCH) {
    peloton::concurrency::current_epoch->AddToPossiblyFreeList(tuple_metadata);
  } else {
    possibly_free_list_.Push(tuple_metadata);
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
    if (free_list.second->Pop(tuple_metadata) == true) {
      free_list.first = free_list.first - 1;
      return ItemPointer(tuple_metadata.tile_group_id, tuple_metadata.tuple_slot_id);
    }
  }
  return ItemPointer();
}

// delete a tuple from all its indexes it belongs to.
// TODO: we do not perform this function,
// as we do not have concurrent bw tree right now.
void GCManager::DeleteTupleFromIndexes(const TupleMetadata &tuple_metadata __attribute__((unused))) {
}

size_t GCManager::GetRecycledTupleSlotCountPerTileGroup(const oid_t& table_id, const oid_t& tile_group_id) {
  if (this -> gc_type == GC_TYPE_OFF)
  {
    return 0;
  }
  size_t count = 0;
  {
    std::lock_guard<std::mutex> lock(free_map_mutex);
    std::pair<size_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>> free_list;
    // if there exists free_list
    if (free_map_.find(table_id, free_list)) {
      size_t size = free_list.first;
      for(unsigned i = 0; i < size; ++i) {
        TupleMetadata tuple_metadata;
        if (free_list.second->Pop(tuple_metadata)) {
          if (tuple_metadata.tile_group_id == tile_group_id)
          {
            count++;
          }
          free_list.second -> Push(tuple_metadata);
        }
      }
    } else {
      assert(false);
    }
  }

  return count;
}

}  // namespace gc
}  // namespace peloton
