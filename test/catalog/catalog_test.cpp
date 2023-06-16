//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it
  ASSERT_TRUE(table_metadata);
  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());
  auto table_oid = table_metadata->oid_;

  table_metadata = catalog->GetTable(table_oid);
  ASSERT_TRUE(table_metadata);
  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(table_oid, table_metadata->oid_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());

  table_metadata = catalog->GetTable(table_name);
  ASSERT_TRUE(table_metadata);
  EXPECT_EQ(table_name, table_metadata->name_);
  EXPECT_EQ(table_oid, table_metadata->oid_);
  EXPECT_EQ(schema.ToString(), table_metadata->schema_.ToString());

  delete catalog;
  delete bpm;
  delete disk_manager;
}

TEST(CatalogTest, CreateIndedxTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  std::string index_name = "index";
  auto index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(nullptr, index_name, table_name,
                                                                                   schema, schema, {0}, 8);

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it
  ASSERT_TRUE(index_info);
  EXPECT_EQ(table_name, index_info->table_name_);
  EXPECT_EQ(schema.ToString(), index_info->key_schema_.ToString());
  auto index_oid = index_info->index_oid_;

  index_info = catalog->GetIndex(index_oid);
  ASSERT_TRUE(index_info);
  EXPECT_EQ(index_name, index_info->name_);
  EXPECT_EQ(table_name, index_info->table_name_);
  EXPECT_EQ(index_oid, index_info->index_oid_);
  EXPECT_EQ(schema.ToString(), index_info->key_schema_.ToString());

  index_info = catalog->GetIndex(index_name, table_name);
  ASSERT_TRUE(index_info);
  EXPECT_EQ(index_name, index_info->name_);
  EXPECT_EQ(table_name, index_info->table_name_);
  EXPECT_EQ(index_oid, index_info->index_oid_);
  EXPECT_EQ(schema.ToString(), index_info->key_schema_.ToString());

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
