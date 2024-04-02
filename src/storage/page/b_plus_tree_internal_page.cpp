//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
        SetPageType(IndexPageType::INTERNAL_PAGE);
        SetSize(0);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        SetMaxSize(max_size);
    }
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
        // replace with your own code
        return array_[index].first;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
        array_[index].first = key;
    }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
        return array_[index].second;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
        array_[index].second = value;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &k, const KeyComparator &keyComparator) -> int {
        auto pos = FindPos(k, keyComparator);
        if (pos != GetSize() && keyComparator(array_[pos].first, k) == 0)
            return ValueAt(pos);
        else
            return ValueAt(pos - 1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindPos(const KeyType &k, const KeyComparator &keyComparator) const -> int {
        return std::distance(array_, std::lower_bound(array_, array_ + GetSize(), k,
                                              [&keyComparator](const auto &x, auto k) {
                                                  return keyComparator(x.first, k) < 0;
                                              }));
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *siblingNode, BufferPoolManager *bufferPoolManager) {
        auto midPos = GetMinSize();
        siblingNode->CopyNFrom(array_ + midPos, GetSize() - midPos, bufferPoolManager);
        SetSize(midPos);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType * arr, int size, BufferPoolManager *bufferPoolManager) {
        std::copy(arr, arr + size, array_ + GetSize());
        for (int i = 0; i < size; i++) {
            auto page = bufferPoolManager->FetchPage(arr[i].second);
            auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
            node->SetParentPageId(GetPageId());
            bufferPoolManager->UnpinPage(page->GetPageId(), true);
        }
        IncreaseSize(size);
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &keyComparator) -> int {
        auto pos = FindPos(key, keyComparator);
        if (pos == GetSize()) {
            array_[GetSize()] = {key, value};
            IncreaseSize(1);
            return GetSize();
        }
        if (keyComparator(array_[pos].first, key) == 0)
            return GetSize();
        std::move_backward(array_ + pos, array_ + GetSize(), array_ + GetSize() + 1);
        array_[pos] = {key, value};
        IncreaseSize(1);
        return GetSize();
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
        auto it = std::find_if(array_, array_ + GetSize(),
                               [&value](const auto &pair) { return pair.second == value; });
        return std::distance(array_, it);
    }


    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                          BufferPoolManager *bufferPoolManager) {
        SetKeyAt(0, middle_key);
        auto first_item = array_[0];
        recipient->CopyLastFrom(first_item, bufferPoolManager);
        std::move(array_ + 1, array_ + GetSize(), array_);
        IncreaseSize(-1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *bufferPoolManager) {
        *(array_ + GetSize()) = pair;
        IncreaseSize(1);
        auto page = bufferPoolManager->FetchPage(pair.second);
        auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        node->SetParentPageId(GetPageId());
        bufferPoolManager->UnpinPage(page->GetPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                           BufferPoolManager *bufferPoolManager) {
        auto last_item = array_[GetSize() - 1];
        recipient->SetKeyAt(0, middle_key);
        recipient->CopyFirstFrom(last_item, bufferPoolManager);
        IncreaseSize(-1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *bufferPoolManager) {
        std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
        *array_ = pair;
        IncreaseSize(1);
        auto page = bufferPoolManager->FetchPage(pair.second);
        auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        node->SetParentPageId(GetPageId());
        bufferPoolManager->UnpinPage(page->GetPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                   BufferPoolManager *buffer_pool_manager) {
        SetKeyAt(0, middle_key);
        recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
        SetSize(0);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
        std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
        IncreaseSize(-1);
    }

// valuetype for internalNode should be page id_t
    template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
    template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
    template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
    template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
    template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
