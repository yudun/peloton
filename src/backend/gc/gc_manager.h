//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/backend/gc/gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <thread>
#include <unordered_map>

#include "backend/common/types.h"
#include "backend/common/lockfree_queue.h"
#include "backend/common/logger.h"
#include "backend/common/epoch.h"
#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

#define MAX_TUPLES_PER_GC 1000
#define VACUUM_THREAD_SLEEP_TIME 5

class GCManager {
 public:
  GCManager(const GCManager &) = delete;
  GCManager &operator=(const GCManager &) = delete;
  GCManager(GCManager &&) = delete;
  GCManager &operator=(GCManager &&) = delete;

  GCManager(const GCType type, size_t max_tuples = MAX_TUPLES_PER_GC,
            unsigned int sleep_time_ = VACUUM_THREAD_SLEEP_TIME)
      : is_running_(true),
        gc_type_(type),
        possibly_free_list_(FREE_LIST_LENGTH),
        max_tuples_per_gc(max_tuples),
        vacuum_thread_sleep_time_(sleep_time_) {}

  ~GCManager() { StopGC(); }

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->is_running_; }

  // Get GCType
  GCType GetGCType() { return this->gc_type_; }
  void SetGCType(GCType gc_type) { this->gc_type_ = gc_type; }
  // Get and Set Sleep Time for vacuum thread
  unsigned int GetVacuumThreadSleepTime() {
    return this->vacuum_thread_sleep_time_;
  }
  void SetVacuumThreadSleepTime(unsigned int sleep_time) {
    this->vacuum_thread_sleep_time_ = sleep_time;
  }

  // PerformGC function used by vacuum and cooperative mode. Uses global
  // possibly and actual free list
  void PerformGC();
  // PerformGC function used by epoch mode. Uses epoch specific possibly and
  // actual free list
  void PerformGC(Epoch *e);
  // Start and Stop the GC
  void StartGC();
  void StopGC();

  // This adds a tuple to the possibly free list
  void RecycleTupleSlot(const oid_t &table_id, const oid_t &tile_group_id,
                        const oid_t &tuple_id, const cid_t &tuple_end_cid);
  // Helper function to get the number of tuples refurbished (present in the
  // actually free list)
  size_t GetRefurbishedTupleSlotCountPerTileGroup(const oid_t &table_id,
                                                  const oid_t &tile_group_id);

  // Gets the item pointer for a tuple slot from the actually free list
  ItemPointer ReturnFreeSlot(const oid_t &table_id);

 private:
  // Infinite poll used by the vacuum thread
  void Poll();
  // TODO Delete the refurbished tuples from the indexes as well
  void DeleteTupleFromIndexes(const TupleMetadata &);

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Is the GC running
  volatile bool is_running_;
  // GC mode
  GCType gc_type_;
  // The global possibly free list used by vacuum and cooperative modes
  LockfreeQueue<TupleMetadata> possibly_free_list_;
  // Maps table ids to the list of free tuples in the table
  // We are using the third-party cuckoohash_map, as our look-free concurrent
  // hashmap. Source code: https://github.com/efficient/libcuckoo
  cuckoohash_map<
      oid_t, std::pair<size_t, std::shared_ptr<LockfreeQueue<TupleMetadata>>>>
      free_map_;
  // mutex to access the free map
  std::mutex free_map_mutex;
  // Moves the tuple from possibly free list to the free map
  void RefurbishTuple(const TupleMetadata tuple_metadata);

  // Max tuples to clean in one GC invokation (for vacuum and cooperative modes)
  size_t max_tuples_per_gc;
  // Seconds to sleep for the vacuum thread
  unsigned int vacuum_thread_sleep_time_;
};

}  // namespace gc
}  // namespace peloton
