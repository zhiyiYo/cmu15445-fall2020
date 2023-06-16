/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : IndexIterator(nullptr, nullptr, 0){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, Page *page, int index)
    : buffer_pool_manager_(buffer_pool_manager),
      page_(page),
      index_(index),
      page_id_(page ? page->GetPageId() : INVALID_PAGE_ID){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!isEnd()) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
};

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return itr.page_id_ == page_id_ && itr.index_ == index_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !operator==(itr); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page_);
  return leaf->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }

  LeafPage *leaf = reinterpret_cast<LeafPage *>(page_);
  if (index_ < leaf->GetSize() - 1) {
    index_++;
  } else {
    Page *old_page = page_;

    // 移动到下一页
    page_id_ = leaf->GetNextPageId();
    if (page_id_ != INVALID_PAGE_ID) {
      page_ = buffer_pool_manager_->FetchPage(page_id_);
      page_->RLatch();
    } else {
      page_ = nullptr;
    }

    index_ = 0;
    old_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_page->GetPageId(), false);
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
