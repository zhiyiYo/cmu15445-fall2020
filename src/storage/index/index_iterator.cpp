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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, B_PLUS_TREE_LEAF_PAGE_TYPE *page, int index)
    : buffer_pool_manager_(buffer_pool_manager),
      page_(page),
      index_(index),
      page_id_(page ? page->GetPageId() : INVALID_PAGE_ID){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!isEnd()) {
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
const MappingType &INDEXITERATOR_TYPE::operator*() { return page_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }

  if (index_ < page_->GetSize() - 1) {
    index_++;
  } else {
    index_ = 0;
    buffer_pool_manager_->UnpinPage(page_id_, false);

    // 移动到下一页
    page_id_ = page_->GetNextPageId();
    if (page_id_ != INVALID_PAGE_ID) {
      page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(page_id_)->GetData());
    } else {
      page_ = nullptr;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
