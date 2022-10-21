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
 */

#ifndef OCEANBASE_SHARE_PART_OB_PART_MGR_UTIL_
#define OCEANBASE_SHARE_PART_OB_PART_MGR_UTIL_

namespace oceanbase
{
namespace share
{
namespace schema {
//level of patition
enum ObPartitionLevel
{
  PARTITION_LEVEL_ZERO = 0,//means no patition
  PARTITION_LEVEL_ONE = 1,
  PARTITION_LEVEL_TWO = 2,
  PARTITION_LEVEL_MAX,
};

enum ObPartitionFuncType
{
  //TODO add other type
  PARTITION_FUNC_TYPE_HASH = 0,
  PARTITION_FUNC_TYPE_KEY,
  PARTITION_FUNC_TYPE_KEY_IMPLICIT,
  PARTITION_FUNC_TYPE_RANGE,
  PARTITION_FUNC_TYPE_RANGE_COLUMNS,
  PARTITION_FUNC_TYPE_LIST,
  PARTITION_FUNC_TYPE_KEY_V2,
  PARTITION_FUNC_TYPE_LIST_COLUMNS,
  PARTITION_FUNC_TYPE_HASH_V2,
  PARTITION_FUNC_TYPE_KEY_V3,
  PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2,
  PARTITION_FUNC_TYPE_MAX,
};

enum ObPartitionFuncTypeV4
{
  //TODO add other type
  PARTITION_FUNC_TYPE_V4_HASH = 0,
  PARTITION_FUNC_TYPE_V4_KEY,
  PARTITION_FUNC_TYPE_V4_KEY_IMPLICIT,
  PARTITION_FUNC_TYPE_V4_RANGE,
  PARTITION_FUNC_TYPE_V4_RANGE_COLUMNS,
  PARTITION_FUNC_TYPE_V4_LIST,
  PARTITION_FUNC_TYPE_V4_LIST_COLUMNS,
  PARTITION_FUNC_TYPE_V4_INTERVAL,
  PARTITION_FUNC_TYPE_V4_MAX,
};

const char *get_partition_func_type_str(const ObPartitionFuncType type)
{
  static const char *type_str_array[PARTITION_FUNC_TYPE_MAX] =
  {
    "HASH",
    "KEY",
    "KEY_IMPLICIT",
    "RANGE",
    "RANGE_COLUMNS",
    "LIST",
    "KEY_V2",
    "LIST_COLUMNS",
    "HASH_V2",
    "KEY_V3",
    "KEY_IMPLICIT_V2",
  };
  const char *str = "";
  if (OB_LIKELY(type >= PARTITION_FUNC_TYPE_HASH) && OB_LIKELY(type < PARTITION_FUNC_TYPE_MAX)) {
    str = type_str_array[type];
  }
  return str;
}

inline bool is_hash_part(const ObPartitionFuncType part_type, const int64_t cluster_version)
{
  bool bret = false;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    bret = PARTITION_FUNC_TYPE_HASH == part_type || PARTITION_FUNC_TYPE_HASH_V2 == part_type;
  } else {
    bret = PARTITION_FUNC_TYPE_V4_HASH == static_cast<ObPartitionFuncTypeV4>(part_type);
  }
  return bret;
}

inline bool is_range_part(const ObPartitionFuncType part_type, const int64_t cluster_version)
{
  bool bret = false;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    bret = PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type;
  } else {
    ObPartitionFuncTypeV4 v4_type = static_cast<ObPartitionFuncTypeV4>(part_type);
    bret = PARTITION_FUNC_TYPE_V4_RANGE == v4_type || PARTITION_FUNC_TYPE_V4_RANGE_COLUMNS == v4_type;
  }

  return bret;
}

inline bool is_key_part(const ObPartitionFuncType part_type, const int64_t cluster_version)
{
  bool bret = false;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    bret = PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type || PARTITION_FUNC_TYPE_KEY_V2 == part_type
         || PARTITION_FUNC_TYPE_KEY_V3 == part_type || PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_type;
  } else {
    ObPartitionFuncTypeV4 v4_type = static_cast<ObPartitionFuncTypeV4>(part_type);
    bret = PARTITION_FUNC_TYPE_V4_KEY_IMPLICIT == v4_type || PARTITION_FUNC_TYPE_V4_KEY == v4_type;
  }
  return bret;
}

inline bool is_list_part(const ObPartitionFuncType part_type, const int64_t cluster_version)
{
  bool bret = false;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    bret = PARTITION_FUNC_TYPE_LIST == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type;
  } else {
    ObPartitionFuncTypeV4 v4_type = static_cast<ObPartitionFuncTypeV4>(part_type);
    bret = PARTITION_FUNC_TYPE_V4_LIST == v4_type || PARTITION_FUNC_TYPE_V4_LIST_COLUMNS == v4_type;
  }

  return bret;
}

OB_INLINE int64_t generate_phy_part_id(int64_t part_idx, int64_t sub_part_idx)
{
  static const uint64_t OB_TWOPART_BEGIN_MASK = (((uint64_t)0x1 << OB_PART_IDS_BITNUM)
                                                | ((uint64_t)0x1 << (OB_PART_IDS_BITNUM + OB_PART_ID_SHIFT)));

  return (part_idx < 0 || sub_part_idx < 0) ? -1 :
          (((uint64_t)part_idx << OB_PART_ID_SHIFT) | sub_part_idx) | OB_TWOPART_BEGIN_MASK;
}

OB_INLINE int64_t generate_phy_part_id(int64_t part_idx,
                                       int64_t sub_part_idx,
                                       ObPartitionLevel part_level)
{ return (PARTITION_LEVEL_TWO == part_level) ? generate_phy_part_id(part_idx, sub_part_idx) : part_idx; }

}
}
}
#endif
