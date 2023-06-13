//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
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
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  root_latch_.lock();
  if (IsEmpty()) {
    root_latch_.unlock();
    return false;
  }
  root_latch_.unlock();

  // 在叶节点中寻找 key
  auto leaf_page = FindLeafPage(key);
  LeafPage *leaf = ToLeafPage(leaf_page);

  ValueType value;
  auto success = leaf->Lookup(key, &value, comparator_);
  if (success) {
    result->push_back(value);
  }

  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  root_latch_.lock();

  auto success = true;
  if (!IsEmpty()) {
    success = InsertIntoLeaf(key, value, transaction);
  } else {
    StartNewTree(key, value);
  }

  return success;
}

/**
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // 创建一个叶节点作为根节点，并插入新数据
  LeafPage *root = ToLeafPage(NewPage(&root_page_id_));
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);

  UpdateRootPageId(1);
  root_latch_.unlock();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/**
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 定位到包含 key 的叶节点
  auto leaf_page = FindLeafPage(key, false, OperationType::INSERT, transaction);
  LeafPage *leaf = ToLeafPage(leaf_page);

  // 不能插入相同的键
  ValueType exist_value;
  if (leaf->Lookup(key, &exist_value, comparator_)) {
    UnlockAncestors(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }

  // 如果叶节点没有满，就直接插进去（array 最后留了一个空位），否则分裂叶节点并更新父节点
  auto size = leaf->Insert(key, value, comparator_);

  if (size == leaf_max_size_) {
    LeafPage *new_leaf = Split(leaf);
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }

  UnlockAncestors(transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return true;
}

/**
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 创建新节点
  page_id_t new_page_id;
  auto new_page = NewPage(&new_page_id);
  N *new_node = reinterpret_cast<N *>(new_page->GetData());

  // 将右半边的 item 移动到新节点中
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);

  return new_node;
}

/**
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 根节点发生分裂需要新建一个根节点，B+树的高度 +1
  if (old_node->IsRootPage()) {
    auto root_page = NewPage(&root_page_id_);

    // 创建新节点并更新子节点指针
    InternalPage *root = ToInternalPage(root_page);
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // 更新父节点指针
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UnlockAncestors(transaction, false);
    return;
  }

  // 找到父节点并将新节点的最左侧 key 插入其中
  auto parent_id = old_node->GetParentPageId();
  InternalPage *parent = ToInternalPage(buffer_pool_manager_->FetchPage(parent_id));
  auto size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  // 父节点溢出时需要再次分裂
  if (size == internal_max_size_) {
    InternalPage *new_page = Split(parent);
    InsertIntoParent(parent, new_page->KeyAt(0), new_page, transaction);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  } else {
    UnlockAncestors(transaction, false);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.lock();

  if (IsEmpty()) {
    root_latch_.unlock();
    return;
  }

  // 定位到叶节点并删除键值对
  auto leaf_page = FindLeafPage(key, false, OperationType::REMOVE, transaction);
  LeafPage *leaf = ToLeafPage(leaf_page);
  auto old_size = leaf->GetSize();
  auto size = leaf->RemoveAndDeleteRecord(key, comparator_);

  // 叶节点删除之后没有处于半满状态需要合并相邻节点或者重新分配
  if (size < leaf->GetMinSize() && CoalesceOrRedistribute(leaf, transaction)) {
    transaction->AddIntoDeletedPageSet(leaf->GetPageId());
  }

  UnlockAncestors(transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), old_size != size);

  // DeletePages(transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // 找到相邻的兄弟节点并加锁
  InternalPage *parent = ToInternalPage(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  auto index = parent->ValueIndex(node->GetPageId());
  auto sibling_index = index > 0 ? index - 1 : 1;  // idnex 为 0 时必定有右兄弟节点

  auto sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(sibling_index));
  N *sibling = reinterpret_cast<N *>(sibling_page->GetData());
  sibling_page->WLatch();

  // 如果两个节点的大小和大于 max_size-1，就直接重新分配，否则直接合并兄弟节点
  bool is_merge = sibling->GetSize() + node->GetSize() <= node->GetMaxSize() - 1;
  if (is_merge) {
    Coalesce(&sibling, &node, &parent, index, transaction);
  } else {
    Redistribute(sibling, node, index);
  }

  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);

  return is_merge;
}

/**
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  // 如果兄弟节点在右边，需要交换两个指针的值，这样就能确保数据移动方向是从右到左
  if (index == 0) {
    std::swap(node, neighbor_node);
  }

  N *child = *node, *neighbor_child = *neighbor_node;
  InternalPage *parent_node = *parent;

  // 内部节点要从父节点获取插到 node 中的键，右兄弟节点对应的是第一个有效键，左兄弟节点对应的就是 index 处的键
  KeyType middle_key;
  auto middle_index = index == 0 ? 1 : index;
  if (!child->IsLeafPage()) {
    middle_key = parent_node->KeyAt(middle_index);
  }

  // 将键值对移动到兄弟节点之后删除节点
  child->MoveAllTo(neighbor_child, middle_key, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  buffer_pool_manager_->DeletePage(child->GetPageId());

  // 删除父节点中的键值对，并递归调整父节点
  parent_node->Remove(middle_index);
  return CoalesceOrRedistribute(parent_node, transaction);
}

/**
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // 更新父节点
  InternalPage *parent = ToInternalPage(buffer_pool_manager_->FetchPage(node->GetParentPageId()));

  // 内部节点要从父节点获取插到 node 中的键，右兄弟节点对应的是第一个有效键，左兄弟节点对应的就是 index 处的键
  KeyType middle_key;
  auto middle_index = index == 0 ? 1 : index;
  if (!node->IsLeafPage()) {
    middle_key = parent->KeyAt(middle_index);
  }

  // 兄弟节点在右边，移动第一个键值对给 node，否则将兄弟节点的最后一个键值对移给 node 并更新父节点的键
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(middle_index, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent->SetKeyAt(middle_index, node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  bool is_deleted = false;

  // 根节点只包含一个无效键时需要删除根节点，将子节点变为根节点；根节点为叶节点且为没有键值对时，删除整棵树
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    InternalPage *old_root = ToInternalPage(old_root_node);
    root_page_id_ = old_root->RemoveAndReturnOnlyChild();

    // 更新子节点的元数据
    InternalPage *child = ToInternalPage(buffer_pool_manager_->FetchPage(root_page_id_));
    child->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    UpdateRootPageId();
    is_deleted = true;
  } else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    is_deleted = true;
  }

  return is_deleted;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key;
  return INDEXITERATOR_TYPE(buffer_pool_manager_, FindLeafPage(key, true), 0);
}

/**
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key);
  auto index = ToLeafPage(page)->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index);
}

/**
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/**
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, OperationType operation,
                                   Transaction *transaction) {
  if (operation == OperationType::READ) {
    root_latch_.lock();
  }

  auto page_id = root_page_id_;
  auto page = buffer_pool_manager_->FetchPage(page_id);
  auto node = ToTreePage(page);

  // 给根节点上锁
  if (operation == OperationType::READ) {
    page->RLatch();
    root_latch_.unlock();
  } else {
    page->WLatch();
    if (!IsPageSafe(node, operation)) {
      transaction->AddIntoPageSet(nullptr);  // 加一个空指针表示根节点 id 的锁
    } else {
      root_latch_.unlock();
    }
  }

  // 定位到包含 key 的叶节点
  while (!node->IsLeafPage()) {
    InternalPage *inode = ToInternalPage(node);

    // 寻找下一个包含 key 的节点
    if (!leftMost) {
      page_id = inode->Lookup(key, comparator_);
    } else {
      page_id = inode->ValueAt(0);
    }

    // 移动到子节点
    auto child_page = buffer_pool_manager_->FetchPage(page_id);

    // 给子节点上锁
    if (operation == OperationType::READ) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);

      // 如果子节点安全，就释放所有祖先节点上的写锁
      if (IsPageSafe(ToTreePage(child_page), operation)) {
        UnlockAncestors(transaction);
      }
    }

    page = child_page;
    node = ToTreePage(page);
  }

  return page;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *page, OperationType operation) {
  auto size = page->GetSize();

  switch (operation) {
    case OperationType::READ:
      return true;

    case OperationType::INSERT:
      return size < page->GetMaxSize() - 1;

    case OperationType::REMOVE:
      if (page->IsRootPage()) {
        return page->IsLeafPage() ? size > 1 : size > 2;
      }

      return size > page->GetMinSize();

    default:
      break;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAncestors(Transaction *transaction, bool unpin) {
  auto pages = transaction->GetPageSet().get();

  while (!pages->empty()) {
    auto page = pages->front();
    pages->pop_front();

    if (!page) {
      root_latch_.unlock();
    } else {
      page->WUnlatch();
      if (unpin) {
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction *transaction) {
  auto pages = transaction->GetDeletedPageSet();
  for (auto page_id : *pages) {
    buffer_pool_manager_->DeletePage(page_id);
  }

  transaction->GetDeletedPageSet()->clear();
}

/**
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/**
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
/**
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
 * This method is used for debug only, You don't  need to modify
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
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

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::NewPage(page_id_t *page_id) {
  auto page = buffer_pool_manager_->NewPage(page_id);
  if (!page) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Can't create new page");
  }

  return page;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
