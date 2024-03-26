//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize(num_buckets_);
  dir_[0] = std::make_shared<Bucket>(Bucket(bucket_size_, global_depth_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_.lock();
  auto idx = IndexOf(key);
  auto res = dir_[idx]->Find(key, value);
  latch_.unlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  auto idx = IndexOf(key);
  auto res = dir_[idx]->Remove(key);
  latch_.unlock();
  return res;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  while (true) {
    auto idx = IndexOf(key);
    if (dir_[idx]->Insert(key, value)) {
      latch_.unlock();
      return;
    }
    if (GetLocalDepthInternal(idx) == GetGlobalDepthInternal()) {
      global_depth_++;
      num_buckets_ <<= 1;
      dir_.resize(num_buckets_);
      std::copy(dir_.begin(), dir_.begin() + num_buckets_ / 2, dir_.begin() + num_buckets_ / 2);
    }
    auto localDepth = dir_[idx]->GetDepth();
    dir_[idx]->IncrementDepth();
    auto list_ = dir_[idx]->GetItems();
    auto lastBucket = std::make_shared<Bucket>(Bucket(bucket_size_, localDepth + 1));
    auto newBucket = std::make_shared<Bucket>(Bucket(bucket_size_, localDepth + 1));
    auto startIdx = idx & ((1 << localDepth) - 1);
    for (int i = startIdx; i < (1 << global_depth_); i += (1 << (localDepth + 1))) {
      dir_[i] = lastBucket;
      dir_[i + (1 << localDepth)] = newBucket;
    }\
    for (const auto& it : list_) {
      dir_[IndexOf(it.first)]->Insert(it.first, it.second);
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto& it : list_) {
    if (it.first == key) {
      value = it.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto& it : list_) {
    if (it.first == key) {
      it.second = value;
      return true;
    }
  }
  if (IsFull())
    return false;
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
