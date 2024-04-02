#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
    INDEX_TEMPLATE_ARGUMENTS
    BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                              int leaf_max_size, int internal_max_size)
            : index_name_(std::move(name)),
              root_page_id_(INVALID_PAGE_ID),
              buffer_pool_manager_(buffer_pool_manager),
              comparator_(comparator),
              leaf_max_size_(leaf_max_size),
              internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
        return root_page_id_ == INVALID_PAGE_ID;
    }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
        root_page_id_latch_.RLock();
        auto leafPage = FindLeaf(key, Operation::SEARCH, transaction);
        auto leafNode = reinterpret_cast<LeafPage *>(leafPage->GetData());
        ValueType value;
        auto exist = leafNode->LookUp(key, comparator_, value);
        leafPage->RUnlatch();
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
        if (!exist)
            return false;
        result->push_back(value);
        return true;
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, const Operation operation, Transaction *transaction,
                                  bool leftMost, bool rightMost) -> Page * {
        assert(root_page_id_ != INVALID_PAGE_ID);
        auto page = buffer_pool_manager_->FetchPage(root_page_id_);
        auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
        if (operation == Operation::SEARCH) {
            root_page_id_latch_.RUnlock();
            page->RLatch();
        } else {
            page->WLatch();
            if (operation == Operation::DELETE && node->GetSize() > 2)
                ReleaseLatchFromQueue(transaction);
            if (operation == Operation::INSERT && node->IsLeafPage() && node->GetSize() < node->GetMaxSize() - 1)
                ReleaseLatchFromQueue(transaction);
            if (operation == Operation::INSERT && !node->IsLeafPage() && node->GetSize() < node->GetMaxSize())
                ReleaseLatchFromQueue(transaction);
        }
        while (!node->IsLeafPage()) {
            auto interNode = reinterpret_cast<InternalPage *>(node);
            page_id_t childPageId;
            if (leftMost)
                childPageId = interNode->ValueAt(0);
            else if (rightMost)
                childPageId = interNode->ValueAt(interNode->GetSize() - 1);
            else
                childPageId = interNode->LookUp(key, comparator_);
            auto childPage = buffer_pool_manager_->FetchPage(childPageId);
            auto childNode = reinterpret_cast<BPlusTreePage *>(childPage->GetData());
            if (operation == Operation::SEARCH) {
                page->RUnlatch();
                childPage->RLatch();
                buffer_pool_manager_->UnpinPage(childPageId, false);
            } else if (operation == Operation::INSERT) {
                childPage->WLatch();
                transaction->AddIntoPageSet(page);
                if (childNode->IsLeafPage() && childNode->GetSize() < childNode->GetMaxSize() - 1)
                    ReleaseLatchFromQueue(transaction);
                if (!childNode->IsLeafPage() && childNode->GetSize() < childNode->GetMaxSize())
                    ReleaseLatchFromQueue(transaction);
            } else if (operation == Operation::DELETE) {
                childPage->WLatch();
                transaction->AddIntoPageSet(page);
                if (childNode->GetSize() > childNode->GetMinSize())
                    ReleaseLatchFromQueue(transaction);
            }
            page = childPage;
            node = childNode;
        }
        return page;
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
        root_page_id_latch_.WLock();
        transaction->AddIntoPageSet(nullptr);
        if (IsEmpty()) {
            StartNewTree(key, value);
            ReleaseLatchFromQueue(transaction);
            return true;
        }
        return InsertIntoLeaf(key, value, transaction);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
        auto page = buffer_pool_manager_->NewPage(&root_page_id_);
        if (page == nullptr)
            throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
        auto node = reinterpret_cast<LeafPage *>(page->GetData());
        node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
        node->Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
        auto leafPage = FindLeaf(key, Operation::INSERT, transaction);
        auto leafNode = reinterpret_cast<LeafPage *>(leafPage->GetData());
        auto lastSize = leafNode->GetSize();
        auto nowSize = leafNode->Insert(key, value, comparator_);
        if (nowSize == lastSize) {
            ReleaseLatchFromQueue(transaction);
            leafPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
            return false;
        }
        if (nowSize < leaf_max_size_) {
            ReleaseLatchFromQueue(transaction);
            leafPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
            return true;
        }
        auto siblingLeafNode = Split(leafNode);
        siblingLeafNode->SetNextPageId(leafNode->GetNextPageId());
        leafNode->SetNextPageId(siblingLeafNode->GetPageId());
        InsertIntoParent(leafNode, siblingLeafNode, siblingLeafNode->KeyAt(0), transaction);

        leafPage->WUnlatch();
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(siblingLeafNode->GetPageId(), true);
        return true;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *oldNode, BPlusTreePage *newNode, const KeyType &key, Transaction *transaction) {
        if (oldNode->IsRootPage()) {
            auto rootPage = buffer_pool_manager_->NewPage(&root_page_id_);
            auto rootNode = reinterpret_cast<InternalPage *>(rootPage->GetData());
            rootNode->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
            rootNode->SetKeyAt(1, key);
            rootNode->SetValueAt(0, oldNode->GetPageId());
            rootNode->SetValueAt(1, newNode->GetPageId());
            rootNode->SetSize(2);
            oldNode->SetParentPageId(rootNode->GetPageId());
            newNode->SetParentPageId(rootNode->GetPageId());
            buffer_pool_manager_->UnpinPage(root_page_id_, true);
            ReleaseLatchFromQueue(transaction);
            return;
        }
        auto parentPageId = oldNode->GetParentPageId();
        auto parentPage = buffer_pool_manager_->FetchPage(parentPageId);
        auto parentNode = reinterpret_cast<InternalPage *>(parentPage->GetData());
        if (parentNode->GetSize() < internal_max_size_) {
            parentNode->Insert(key, newNode->GetPageId(), comparator_);
            oldNode->SetParentPageId(parentNode->GetPageId());
            newNode->SetParentPageId(parentNode->GetPageId());
            ReleaseLatchFromQueue(transaction);
            buffer_pool_manager_->UnpinPage(parentPageId, true);
            return;
        }
        auto mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (internal_max_size_ + 1)];
        auto copyParentNode = reinterpret_cast<InternalPage *>(mem);
        std::memcpy(mem, parentNode, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * internal_max_size_);
        copyParentNode->Insert(key, newNode->GetPageId(), comparator_);
        auto siblingParentNode = Split(copyParentNode);
        std::memcpy(parentNode, copyParentNode, INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * copyParentNode->GetMinSize());
        InsertIntoParent(parentNode, siblingParentNode, siblingParentNode->KeyAt(0), transaction);
        buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(siblingParentNode->GetPageId(), true);
        delete[] mem;
    }

    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    auto BPLUSTREE_TYPE::Split(N *node) -> N * {
        auto siblingPageId = -1;
        auto siblingPage = buffer_pool_manager_->NewPage(&siblingPageId);
        auto siblingNode = reinterpret_cast<N *>(siblingPage->GetData());
        siblingNode->SetPageType(node->GetPageType());

        if (node->IsLeafPage()) {
            auto leaf = reinterpret_cast<LeafPage *>(node);
            auto siblingLeaf = reinterpret_cast<LeafPage *>(siblingNode);
            siblingLeaf->Init(siblingPageId, leaf->GetParentPageId(), leaf_max_size_);
            leaf->MoveHalfTo(siblingLeaf);
        } else {
            auto internal = reinterpret_cast<InternalPage *>(node);
            auto siblingInternal = reinterpret_cast<InternalPage *>(siblingNode);
            siblingInternal->Init(siblingPageId, internal->GetParentPageId(), internal_max_size_);
            internal->MoveHalfTo(siblingInternal, buffer_pool_manager_);
        }
        return siblingNode;
    }


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
        root_page_id_latch_.WLock();
        transaction->AddIntoPageSet(nullptr);
        if (IsEmpty()) {
            ReleaseLatchFromQueue(transaction);
            return;
        }
        auto leafPage = FindLeaf(key, Operation::DELETE, transaction);
        auto *node = reinterpret_cast<LeafPage *>(leafPage->GetData());
        if (node->GetSize() == node->RemoveAndDeleteRecord(key, comparator_)) {
            ReleaseLatchFromQueue(transaction);
            leafPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
            return;
        }
        auto nodeShouldDelete = CoalesceOrRedistribute(node, transaction);
        leafPage->WUnlatch();
        if (nodeShouldDelete) {
            transaction->AddIntoDeletedPageSet(node->GetPageId());
        }
        buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
        std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                      [&bpm = buffer_pool_manager_](const page_id_t page_id) { bpm->DeletePage(page_id); });
        transaction->GetDeletedPageSet()->clear();
    }

    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) -> bool {
        if (node->IsRootPage()) {
            auto rootShouldDelete = AdjustRoot(node);
            ReleaseLatchFromQueue(transaction);
            return rootShouldDelete;
        }
        if (node->GetSize() >= node->GetMinSize()) {
            ReleaseLatchFromQueue(transaction);
            return false;
        }
        auto parentPage = buffer_pool_manager_->FetchPage(node->GetParentPageId());
        auto parentNode = reinterpret_cast<InternalPage *>(parentPage->GetData());
        auto idx = parentNode->ValueIndex(node->GetPageId());
        if (idx > 0) {
            auto siblingPage = buffer_pool_manager_->FetchPage(parentNode->ValueAt(idx - 1));
            siblingPage->WLatch();
            N *siblingNode = reinterpret_cast<N *>(siblingPage->GetData());
            if (siblingNode->GetSize() > siblingNode->GetMinSize()) {
                Redistribute(siblingNode, node, parentNode, idx, true);
                ReleaseLatchFromQueue(transaction);
                buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
                siblingPage->WUnlatch();
                buffer_pool_manager_->UnpinPage(siblingPage->GetPageId(), true);
                return false;
            }
            // coalesce
            auto parentNodeShouldDelete = Coalesce(siblingNode, node, parentNode, idx, transaction);
            if (parentNodeShouldDelete)
                transaction->AddIntoDeletedPageSet(parentNode->GetPageId());
            buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
            siblingPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(siblingPage->GetPageId(), true);
            return true;
        }
        if (idx != parentNode->GetSize() - 1) {
            auto siblingPage = buffer_pool_manager_->FetchPage(parentNode->ValueAt(idx + 1));
            siblingPage->WLatch();
            N *siblingNode = reinterpret_cast<N *>(siblingPage->GetData());
            if (siblingNode->GetSize() > siblingNode->GetMinSize()) {
                Redistribute(siblingNode, node, parentNode, idx, false);
                ReleaseLatchFromQueue(transaction);
                buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
                siblingPage->WUnlatch();
                buffer_pool_manager_->UnpinPage(siblingPage->GetPageId(), true);
                return false;
            }
            // coalesce
            auto siblingIdx = parentNode->ValueIndex(siblingNode->GetPageId());
            auto parentNodeShouldDelete = Coalesce(node, siblingNode, parentNode, siblingIdx, transaction);  // NOLINT
            transaction->AddIntoDeletedPageSet(siblingNode->GetPageId());
            if (parentNodeShouldDelete)
                transaction->AddIntoDeletedPageSet(parentNode->GetPageId());
            buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
            siblingPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(siblingPage->GetPageId(), true);
            return false;
        }
        return false;
    }

    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                                  Transaction *transaction) -> bool {
        auto middleKey = parent->KeyAt(index);

        if (node->IsLeafPage()) {
            auto *leafNode = reinterpret_cast<LeafPage *>(node);
            auto *prevLeafNode = reinterpret_cast<LeafPage *>(neighbor_node);
            leafNode->MoveAllTo(prevLeafNode);
        } else {
            auto *internalNode = reinterpret_cast<InternalPage *>(node);
            auto *prevInternalNode = reinterpret_cast<InternalPage *>(neighbor_node);
            internalNode->MoveAllTo(prevInternalNode, middleKey, buffer_pool_manager_);
        }
        parent->Remove(index);
        return CoalesceOrRedistribute(parent, transaction);
    }

    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, InternalPage *parent, int index, bool from_prev) {
        if (node->IsLeafPage()) {
            auto *leafNode = reinterpret_cast<LeafPage *>(node);
            auto *neighborLeafNode = reinterpret_cast<LeafPage *>(neighbor_node);
            if (!from_prev) {
                neighborLeafNode->MoveFirstToEndOf(leafNode);
                parent->SetKeyAt(index + 1, neighborLeafNode->KeyAt(0));
            } else {
                neighborLeafNode->MoveLastToFrontOf(leafNode);
                parent->SetKeyAt(index, leafNode->KeyAt(0));
            }
        } else {
            auto *internalNode = reinterpret_cast<InternalPage *>(node);
            auto *neighborInternalNode = reinterpret_cast<InternalPage *>(neighbor_node);
            if (!from_prev) {
                neighborInternalNode->MoveFirstToEndOf(internalNode, parent->KeyAt(index + 1), buffer_pool_manager_);
                parent->SetKeyAt(index + 1, neighborInternalNode->KeyAt(0));
            } else {
                neighborInternalNode->MoveLastToFrontOf(internalNode, parent->KeyAt(index), buffer_pool_manager_);
                parent->SetKeyAt(index, internalNode->KeyAt(0));
            }
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *oldRootNode) -> bool {
        if (!oldRootNode->IsLeafPage() && oldRootNode->GetSize() == 1) {
            auto rootNode = reinterpret_cast<InternalPage *>(oldRootNode);
            auto onlyChildPage = buffer_pool_manager_->FetchPage(rootNode->ValueAt(0));
            auto onlyChildNode = reinterpret_cast<BPlusTreePage *>(onlyChildPage->GetData());
            onlyChildNode->SetParentPageId(INVALID_PAGE_ID);
            root_page_id_ = onlyChildNode->GetPageId();
            UpdateRootPageId(0);
            buffer_pool_manager_->UnpinPage(onlyChildPage->GetPageId(), true);
            return true;
        }
        if (oldRootNode->IsLeafPage() && oldRootNode->GetSize() == 0) {
            root_page_id_ = INVALID_PAGE_ID;
            return true;
        }
        return false;
    }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
        if (root_page_id_ == INVALID_PAGE_ID)
            return INDEXITERATOR_TYPE(nullptr, nullptr);
        root_page_id_latch_.RLock();
        auto leftMostPage = FindLeaf(KeyType(), Operation::SEARCH, nullptr, true);
        return INDEXITERATOR_TYPE(buffer_pool_manager_, leftMostPage, 0);
    }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
        if (root_page_id_ == INVALID_PAGE_ID)
            return INDEXITERATOR_TYPE(nullptr, nullptr);
        root_page_id_latch_.RLock();
        auto leafPage = FindLeaf(key, Operation::SEARCH);
        auto leafNode = reinterpret_cast<LeafPage *>(leafPage->GetData());
        auto idx = leafNode->FindPos(key, comparator_);
        return INDEXITERATOR_TYPE(buffer_pool_manager_, leafPage, idx);
    }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
        if (root_page_id_ == INVALID_PAGE_ID)
            return INDEXITERATOR_TYPE(nullptr, nullptr);
        root_page_id_latch_.RLock();
        auto rightMostPage = FindLeaf(KeyType(), Operation::SEARCH, nullptr, false, true);
        auto leafNode = reinterpret_cast<LeafPage *>(rightMostPage->GetData());
        return INDEXITERATOR_TYPE(buffer_pool_manager_, rightMostPage, leafNode->GetSize());
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::ReleaseLatchFromQueue(Transaction *transaction) {
        while (!transaction->GetPageSet()->empty()) {
            Page *page = transaction->GetPageSet()->front();
            transaction->GetPageSet()->pop_front();
            if (page == nullptr) {
                this->root_page_id_latch_.WUnlock();
            } else {
                page->WUnlatch();
                buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            }
        }
    }

/**
 * @return Page id of the root of this tree
 */
    INDEX_TEMPLATE_ARGUMENTS
    auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
        return root_page_id_;
    }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
        auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
        if (insert_record != 0) {
            // create a new record<index_name + root_page_id> in header_page
            header_page->InsertRecord(index_name_, root_page_id_);
        } else {
            // update root_page_id in header_page
            header_page->UpdateRecord(index_name_, root_page_id_);
        }
        buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
    }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
        int64_t key;
        std::ifstream input(file_name);
        while (input) {
            input >> key;

            KeyType index_key;
            index_key.SetFromInteger(key);
            RID rid(key);
            Insert(index_key, rid, transaction);
        }
    }
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
        int64_t key;
        std::ifstream input(file_name);
        while (input) {
            input >> key;
            KeyType index_key;
            index_key.SetFromInteger(key);
            Remove(index_key, transaction);
        }
    }

/**
 * This method is used for debug only, You don't need to modify
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
        if (IsEmpty()) {
            LOG_WARN("Draw an empty tree");
            return;
        }
        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
        out << "}" << std::endl;
        out.flush();
        out.close();
    }

/**
 * This method is used for debug only, You don't need to modify
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
        if (IsEmpty()) {
            LOG_WARN("Print an empty tree");
            return;
        }
        ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
    }

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        if (page->IsLeafPage()) {
            auto *leaf = reinterpret_cast<LeafPage *>(page);
            // Print node name
            out << leaf_prefix << leaf->GetPageId();
            // Print node properties
            out << "[shape=plain color=green ";
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
                << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
                << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->GetSize(); i++) {
                out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
                out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
                out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
            }

            // Print parent links if there is a parent
            if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
                out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
                    << leaf->GetPageId() << ";\n";
            }
        } else {
            auto *inner = reinterpret_cast<InternalPage *>(page);
            // Print node name
            out << internal_prefix << inner->GetPageId();
            // Print node properties
            out << "[shape=plain color=pink ";  // why not?
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
                << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
                << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->GetSize(); i++) {
                out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
                if (i > 0) {
                    out << inner->KeyAt(i);
                } else {
                    out << " ";
                }
                out << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Parent link
            if (inner->GetParentPageId() != INVALID_PAGE_ID) {
                out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
                    << inner->GetPageId() << ";\n";
            }
            // Print leaves
            for (int i = 0; i < inner->GetSize(); i++) {
                auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
                ToGraph(child_page, bpm, out);
                if (i > 0) {
                    auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
                    if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
                        out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
                            << child_page->GetPageId() << "};\n";
                    }
                    bpm->UnpinPage(sibling_page->GetPageId(), false);
                }
            }
        }
        bpm->UnpinPage(page->GetPageId(), false);
    }

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
        if (page->IsLeafPage()) {
            auto *leaf = reinterpret_cast<LeafPage *>(page);
            std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
                      << " next: " << leaf->GetNextPageId() << std::endl;
            for (int i = 0; i < leaf->GetSize(); i++) {
                std::cout << leaf->KeyAt(i) << ",";
            }
            std::cout << std::endl;
            std::cout << std::endl;
        } else {
            auto *internal = reinterpret_cast<InternalPage *>(page);
            std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
            for (int i = 0; i < internal->GetSize(); i++) {
                std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
            }
            std::cout << std::endl;
            std::cout << std::endl;
            for (int i = 0; i < internal->GetSize(); i++) {
                ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
            }
        }
        bpm->UnpinPage(page->GetPageId(), false);
    }

    template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
    template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
    template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
    template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
    template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
