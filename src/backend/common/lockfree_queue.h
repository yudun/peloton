//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lockfree_queue.h
//
// Identification: src/backend/common/lockfree_queue.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrentqueue.h"
#include <xmmintrin.h>


namespace peloton {

//===--------------------------------------------------------------------===//
// Lockfree Queue
// this is a wrapper of boost lockfree queue.
// this data structure supports multiple consumers and multiple producers.
//===--------------------------------------------------------------------===//

template <typename T>
class LockfreeQueue {
 public:
  LockfreeQueue(const size_t &size) : queue_(size) {}

  LockfreeQueue(const LockfreeQueue&) = delete;             // disable copying
  LockfreeQueue& operator=(const LockfreeQueue&) = delete;  // disable assignment

  // return true if pop is successful.
  // if queue is empty, then return false.
  bool TryPop(T& item) {
    return queue_.try_dequeue(item);
  }

  // return true if push is successful.
  bool TryPush(const T& item) {
    return queue_.enqueue(item);
  }


  void BlockingPop(T& item) {
    while (queue_.try_dequeue(item) == false) {
      _mm_pause();
    }
  }

  void BlockingPush(const T& item) {
    while (queue_.enqueue(item) == false) {
      _mm_pause();
    }
  }

 private:
  moodycamel::ConcurrentQueue<T> queue_;
};

}  // namespace peloton
