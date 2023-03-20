/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * ************************************************************************
 *
 * This file defines ObLogPartitionKey.
 *  Note: ObPartitionKey is a basic structure, a member of the structure ObReadParam
 *      Only three member variables uint64_t, int32_t, int32_t are used, all of which are currently set as public
 *      Use structure instead of class, use default assignment construction and copy construction
 */

#ifndef OCEANBASE_COMMON_OB_PARTITION_KEY_
#define OCEANBASE_COMMON_OB_PARTITION_KEY_

#include "lib/ob_define.h"
#include "lib/cbtree/btree.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/json/ob_yson.h"
#include "lib/ob_name_id_def.h"
namespace oceanbase
{
namespace common
{
struct ObPartitionKey
{
public:
  typedef cbtree::DummyCompHelper<ObPartitionKey> comp_helper_t;

  ObPartitionKey():table_id_(OB_INVALID_ID),
      partition_idx_(OB_INVALID_INDEX), partition_cnt_(0), hash_value_(0) {}

  ObPartitionKey(const uint64_t table_id, const int32_t partition_idx,
      const int32_t partition_cnt) : table_id_(table_id),
      partition_idx_(partition_idx), partition_cnt_(partition_cnt), hash_value_(0)
  {
    hash_value_ = inner_hash();
  }
  ~ObPartitionKey() {}

  int init(const uint64_t table_id, const int32_t partition_idx, const int32_t partition_cnt);
  int parse(const char* str);
  /* this function is defined for c driver client compile */
  void reset() {};
  /* this function is defined for c driver client compile */
  bool is_valid() const {return true;};
  uint64_t hash() const;
  uint64_t inner_hash() const;
  int compare(const ObPartitionKey &other) const;
  bool operator==(const ObPartitionKey &other) const { return 0 == compare(other); }
  bool operator!=(const ObPartitionKey &other) const { return !operator==(other); }
  bool operator<(const ObPartitionKey &other) const { return -1 == compare(other); }
  bool veq(const ObPartitionKey &key) const { return 0 == compare(key); }
  bool ideq(const ObPartitionKey &key) const { return 0 == compare(key); }
  uint64_t get_tenant_id() const;
  uint64_t get_table_id() const { return table_id_; }
  int32_t get_partition_idx() const { return partition_idx_; }
  int32_t get_partition_cnt() const { return partition_cnt_; }
  TO_STRING_AND_YSON(ID(tid), table_id_,
                     ID(part_idx), partition_idx_,
                     ID(part_cnt), partition_cnt_);
  NEED_SERIALIZE_AND_DESERIALIZE;

  uint64_t table_id_;     // Indicates the ID of the table
  int32_t partition_idx_; // Indicates the ID of the Partition
  int32_t partition_cnt_; // Indicates the count of the Partition
  uint64_t hash_value_;
};

typedef ObSEArray<ObPartitionKey, 16> ObPartitionArray;

class ObPartitionLeaderArray
{
public:
  ObPartitionLeaderArray() {}
  ~ObPartitionLeaderArray() {}
  void reset();
public:
  int push(const common::ObPartitionKey &partition, const common::ObAddr &leader);
  const common::ObPartitionArray &get_partitions() const { return partitions_; }
  const common::ObAddrArray &get_leaders() const { return leaders_; }
  int64_t count() const;
  bool is_single_leader() const;
  bool is_single_partition() const;

  TO_STRING_KV(K_(partitions), K_(leaders));
private:
  common::ObPartitionArray partitions_;
  common::ObAddrArray leaders_;
};

} // end namespace common
} // end namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_PARTITION_KEY_
