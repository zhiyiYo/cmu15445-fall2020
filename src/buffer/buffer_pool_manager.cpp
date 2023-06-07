//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "buffer/buffer_pool_manager.h"

using namespace std;

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

frame_id_t BufferPoolManager::GetVictimFrameId() {
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }

  return frame_id;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  lock_guard<mutex> lock(latch_);

  // 1.     Search the page table for the requested page (P).
  Page *page;
  auto it = page_table_.find(page_id);

  // 1.1    If P exists, pin it and return it immediately.
  if (it != page_table_.end()) {
    auto frame_id = it->second;
    page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++;
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  auto frame_id = GetVictimFrameId();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  // 2.     If R is dirty, write it back to the disk.
  page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->page_id_);
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, page->data_);
  page->update(page_id, 1, false);
  replacer_->Pin(frame_id);

  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  lock_guard<mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto frame_id = it->second;
  auto &page = pages_[frame_id];
  if (page.pin_count_ <= 0) {
    return false;
  }

  page.is_dirty_ |= is_dirty;
  if (--page.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  lock_guard<mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto &page = pages_[it->second];
  disk_manager_->WritePage(page_id, page.data_);
  page.is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  lock_guard<mutex> lock(latch_);

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  auto frame_id = GetVictimFrameId();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  auto page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  *page_id = disk_manager_->AllocatePage();
  page_table_.erase(page->page_id_);
  page_table_[*page_id] = frame_id;
  page->update(*page_id, 1, false, true);
  replacer_->Pin(frame_id);

  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  lock_guard<mutex> lock(latch_);

  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  auto frame_id = it->second;
  auto &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page.page_id_);
  free_list_.push_back(frame_id);
  page.update(INVALID_PAGE_ID, 0, false);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  lock_guard<mutex> lock(latch_);

  for (auto &[page_id, frame_id] : page_table_) {
    auto &page = pages_[frame_id];
    if (page.IsDirty()) {
      disk_manager_->WritePage(page_id, page.data_);
      page.is_dirty_ = false;
    }
  }
}

}  // namespace bustub
