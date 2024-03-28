//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"
#include "common/logger.h"

namespace bustub {

    BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                         LogManager *log_manager)
            : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
        // we allocate a consecutive memory space for the buffer pool
        pages_ = new Page[pool_size_];
        page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
        replacer_ = new LRUKReplacer(pool_size, replacer_k);

        // Initially, every page is in the free list.
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<int>(i));
        }
    }

    BufferPoolManagerInstance::~BufferPoolManagerInstance() {
        delete[] pages_;
        delete page_table_;
        delete replacer_;
    }

    auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
        std::scoped_lock<std::mutex> lock(latch_);
        auto newPageId = AllocatePage();
        auto frameId = -1;
        if (!free_list_.empty()) {
            frameId = free_list_.back();
            free_list_.pop_back();
        } else if (replacer_->Evict(&frameId)) {
            auto lastPageId = -1;
            lastPageId = pages_[frameId].GetPageId();
            // std::cout << lastPageId << "被移出\n";
            page_table_->Remove(lastPageId);
            if (pages_[frameId].IsDirty()) {
                disk_manager_->WritePage(lastPageId, pages_[frameId].GetData());
                pages_[frameId].is_dirty_ = false;
            }
            pages_[frameId].ResetMemory();
        } else
            return nullptr;

        *page_id = newPageId;
        // std::cout << newPageId << " 已经被存入了 " << frameId << '\n';
        page_table_->Insert(newPageId, frameId);

        replacer_->RecordAccess(frameId);
        replacer_->SetEvictable(frameId, false);

        pages_[frameId].pin_count_++;
        pages_[frameId].page_id_ = newPageId;
        return &pages_[frameId];
    }

    auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
        std::scoped_lock<std::mutex> lock(latch_);
        auto frameId = -1;
        if (page_table_->Find(page_id, frameId))
            return &pages_[frameId];

        if (!free_list_.empty()) {
            frameId = free_list_.back();
            free_list_.pop_back();
        } else if (replacer_->Evict(&frameId)) {
            auto lastPageId = -1;
            lastPageId = pages_[frameId].GetPageId();
            // std::cout << lastPageId << "被移出\n";
            page_table_->Remove(lastPageId);
            if (pages_[frameId].IsDirty()) {
                disk_manager_->WritePage(lastPageId, pages_[frameId].GetData());
                pages_[frameId].is_dirty_ = false;
            }
            pages_[frameId].ResetMemory();
        } else
            return nullptr;

        // std::cout << page_id << " 已经被存入了 " << frameId << '\n';
        page_table_->Insert(page_id, frameId);

        replacer_->RecordAccess(frameId);
        replacer_->SetEvictable(frameId, false);

        disk_manager_->ReadPage(page_id, pages_[frameId].GetData());
        pages_[frameId].pin_count_++;
        pages_[frameId].page_id_ = page_id;
        return &pages_[frameId];
    }

    auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        auto frameId = -1;
        if (!page_table_->Find(page_id, frameId) || pages_[frameId].pin_count_ <= 0)
            return false;
        // std::cout << page_id << " 对应的 " << frameId << " 进行了unpin\n";
        if (--pages_[frameId].pin_count_ == 0) {
            // std::cout << frameId << " pin_count_==0\n";
            replacer_->SetEvictable(frameId, true);
        }
        pages_[frameId].is_dirty_ = is_dirty;
        return true;
    }

    auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        auto frameId = -1;
        if (!page_table_->Find(page_id, frameId))
            return false;
        disk_manager_->WritePage(page_id, pages_[frameId].GetData());
        pages_[frameId].is_dirty_ = false;
        return true;
    }

    void BufferPoolManagerInstance::FlushAllPgsImp() {
        std::scoped_lock<std::mutex> lock(latch_);
        for (size_t i = 0; i < pool_size_; i++) {
            FlushPgImp(pages_[i].GetPageId());
        }
    }

    auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
        std::scoped_lock<std::mutex> lock(latch_);
        auto frameId = -1;
        if (!page_table_->Find(page_id, frameId))
            return true;
        if (pages_[frameId].pin_count_ > 0)
            return false;
        if (!page_table_->Remove(page_id))
            return false;
        replacer_->Remove(frameId);
        free_list_.push_back(frameId);
        pages_[frameId].ResetMemory();
        DeallocatePage(page_id);
        return true;
    }

    auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
