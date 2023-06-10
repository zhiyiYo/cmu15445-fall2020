//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }
bool BPlusTreePage::IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const {
  // 数据库系统概念中文第六版 Page #486
  // 叶节点最少有一个键，而内部节点除了无效的第一个键外还需一个键
  if (IsRootPage()) {
    return IsLeafPage() ? 1 : 2;
  }

  // 内部节点的最小键值对数量为 ⌈(max_size_ - 1 - 1)/2⌉ + 1
  if (!IsLeafPage()) {
    return (max_size_ - 2 + 1) / 2 + 1;
  }

  // 叶节点最小键值对数量为 ⌈(max_size_ - 1)/2⌉
  return (max_size_ - 1 + 1) / 2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}  // namespace bustub
