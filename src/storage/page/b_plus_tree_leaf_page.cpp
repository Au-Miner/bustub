//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
        SetPageType(IndexPageType::LEAF_PAGE);
        SetSize(0);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        SetNextPageId(INVALID_PAGE_ID);
        SetMaxSize(max_size);
    }

/**
 * Helper methods to set/get next page id
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
        // replace with your own code
        return array_[index].first;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &k, const KeyComparator &keyComparator, ValueType &value) -> bool {
        auto pos = FindPos(k, keyComparator);
        if (pos != GetSize() && keyComparator(KeyAt(pos), k) == 0) {
            value = array_[pos].second;
            return true;
        } else
            return false;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindPos(const KeyType &k, const KeyComparator &keyComparator) const -> int {
        return std::distance(array_, std::lower_bound(array_, array_ + GetSize(), k,
                                              [&keyComparator](const auto &x, auto k) {
                                                  return keyComparator(x.first, k) < 0;
                                              }));
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &keyComparator) -> int {
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
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *siblingNode) {
        auto midPos = GetMinSize();
        siblingNode->CopyNFrom(array_ + midPos, GetSize() - midPos);
        SetSize(midPos);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType * arr, int size) {
        std::copy(arr, arr + size, array_ + GetSize());
        IncreaseSize(size);
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int {
        int targetInArray = FindPos(key, keyComparator);
        if (targetInArray == GetSize() || keyComparator(array_[targetInArray].first, key) != 0) {
            return GetSize();
        }
        std::move(array_ + targetInArray + 1, array_ + GetSize(), array_ + targetInArray);
        IncreaseSize(-1);
        return GetSize();
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
        std::move(array_ + 1, array_ + GetSize(), array_);
        IncreaseSize(-1);
        recipient->CopyLastFrom(array_[0]);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
        *(array_ + GetSize()) = item;
        IncreaseSize(1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
        IncreaseSize(-1);
        recipient->CopyFirstFrom(array_[GetSize() - 1]);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
        std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
        *array_ = item;
        IncreaseSize(1);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
        recipient->CopyNFrom(array_, GetSize());
        recipient->SetNextPageId(GetNextPageId());
        SetSize(0);
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & {
        return array_[index];
    }

    template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
    template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
    template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
    template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
    template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
