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

#define USING_LOG_PREFIX PROXY
#include "proxy/route/obproxy_part_mgr.h"
#include "proxy/route/obproxy_part_info.h"
#include "share/part/ob_part_desc_hash.h"
#include "share/part/ob_part_desc_key.h"
#include "share/part/ob_part_desc_range.h"
#include "share/part/ob_part_desc_list.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
int64_t ObPartNameIdPair::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(part_name), K_(part_id));
  J_OBJ_END();
  return pos;
}

ObProxyPartMgr::ObProxyPartMgr(ObIAllocator &allocator) : first_part_desc_(NULL)
                                                        , sub_part_desc_(NULL)
                                                        , first_part_num_(0)
                                                        , sub_part_num_(NULL)
                                                        , part_pair_array_(NULL)
                                                        , allocator_(allocator)
{
}

int ObProxyPartMgr::get_part_with_part_name(const ObString &part_name,
                                            int64_t &part_id)
{
  int ret = OB_SUCCESS;
  if (NULL != part_pair_array_&& first_part_num_ > 0) {
    bool found = false;
    for (int64_t i = 0; !found && i < first_part_num_; ++i) {
      if (part_pair_array_[i].get_part_name().case_compare(part_name) == 0) {
        part_id = part_pair_array_[i].get_part_id();
        found = true;
      }
    }
    if (found) {
      LOG_DEBUG("succ to get part id with part name", K(part_id), K(part_name));
    }
  }
  return ret;
}

int ObProxyPartMgr::get_first_part_id_by_idx(const int64_t idx, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= first_part_num_ || OB_ISNULL(part_pair_array_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get part id by idx", K(ret), K(idx), K(first_part_num_), K(part_pair_array_));
  } else {
    part_id = part_pair_array_[idx].get_part_id();
  }
  return ret;
}

int ObProxyPartMgr::get_first_part(ObNewRange &range,
                                   ObIAllocator &allocator,
                                   ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(first_part_desc_)) {
    ret = first_part_desc_->get_part(range, allocator, part_ids);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_FAIL(ret)) {
    LOG_DEBUG("fail to get part", K_(first_part_desc), K(ret));
  }
  return ret;
}

int ObProxyPartMgr::get_sub_part_desc_by_first_part_id(const bool is_template_table,
                                                       const int64_t first_part_id,
                                                       ObPartDesc *&sub_part_desc_ptr)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(is_sub_part_valid())) {
    ObPartDesc *sub_part_desc_tmp = NULL;
    
    if (is_template_table) {
      sub_part_desc_tmp = sub_part_desc_;
    } else {
      const ObPartitionFuncType sub_part_func_type = sub_part_desc_->get_part_func_type();
      for (int64_t i = 0; i < first_part_num_; ++i) {
        if (is_range_part(sub_part_func_type)) {
          ObPartDescRange *desc_range = (((ObPartDescRange*)sub_part_desc_) + i);
          RangePartition *part_array = desc_range->get_part_array();
          // use first sub partition of everty first partition to get firt partition id
          if (first_part_id == part_array[0].first_part_id_) {
            sub_part_desc_tmp = desc_range;
            break;
          }
        } else if (is_list_part(sub_part_func_type)) {
          ObPartDescList *desc_list = (((ObPartDescList*)sub_part_desc_) + i);
          ListPartition *part_array = desc_list->get_part_array();
          // use first sub partition of everty first partition to get firt partition id
          if (first_part_id == part_array[0].first_part_id_) {
            sub_part_desc_tmp = desc_list;
            break;
          }
        } else if (is_hash_part(sub_part_func_type)) {
          ObPartDescHash *desc_hash = (((ObPartDescHash*)sub_part_desc_) + i);
          if (first_part_id == desc_hash->get_first_part_id()) {
            sub_part_desc_tmp = desc_hash;
            break;
          }
        } else if (is_key_part(sub_part_func_type)) {
          ObPartDescKey *desc_key = (((ObPartDescKey*)sub_part_desc_) + i);
          if (first_part_id == desc_key->get_first_part_id()) {
            sub_part_desc_tmp = desc_key;
            break;
          }
        } else {
          // nothing.
        }
      }
    }

    if (OB_NOT_NULL(sub_part_desc_tmp)) {
      sub_part_desc_ptr = sub_part_desc_tmp;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get sub part desc ptr", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get sub part desc by first part id", K(ret));
  }
  
  return ret;
}                                   

int ObProxyPartMgr::get_sub_part(ObNewRange &range,
                                 ObIAllocator &allocator,
                                 ObPartDesc *sub_part_desc_ptr,
                                 ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(sub_part_desc_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get sub part, null ptr", K(ret));
  } else {
    if (OB_FAIL(sub_part_desc_ptr->get_part(range, allocator, part_ids))) {
      LOG_WARN("fail to get sub part", K(sub_part_desc_ptr), K(ret));
    }
  }
  
  return ret;
}

int ObProxyPartMgr::get_sub_part_by_random(const int64_t rand_num, 
                                           ObPartDesc *sub_part_desc_ptr,
                                           ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(sub_part_desc_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get sub part, null ptr", K(ret));
  } else {
    if (OB_FAIL(sub_part_desc_ptr->get_part_by_num(rand_num, part_ids))) {
      LOG_WARN("fail to get sub part by random", K(sub_part_desc_ptr), K(ret));
    }
  }
  
  return ret;
}

int ObProxyPartMgr::build_hash_part(const bool is_oracle_mode,
                                    const ObPartitionLevel part_level,
                                    const ObPartitionFuncType part_func_type,
                                    const int64_t part_num,
                                    const int64_t part_space,
                                    const bool is_template_table,
                                    const ObProxyPartKeyInfo &key_info,
                                    ObResultSetFetcher *rs_fetcher /*NULL*/)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescHash *desc_hash = NULL;
  int64_t part_id = -1;
  ObString part_name;
  int64_t sub_part_num = 0;

  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescHash)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_hash = new (tmp_buf) ObPartDescHash())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else {
    if (NULL != rs_fetcher && PARTITION_LEVEL_ONE == part_level) {
      first_part_num_ = part_num;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartNameIdPair) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      } else if (OB_ISNULL(part_pair_array_ = new (tmp_buf) ObPartNameIdPair[part_num])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to do placement new", K(tmp_buf), K(part_num), K(ret));
      } else if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      }
      for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
        if (OB_FAIL(rs_fetcher->next())) {
          LOG_DEBUG("failed to get next rs_fetcher", K(ret));
        } else {
          PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "part_id", part_id, int64_t);
          PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "part_name", part_name);
          part_pair_array_[i].set_part_id(part_id);
          if (OB_FAIL(part_pair_array_[i].set_part_name(part_name))) {
            LOG_WARN("fail to set part name", K(part_name), K(ret));
          } else if (!is_template_table) {
            PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "sub_part_num", sub_part_num, int64_t);
            sub_part_num_[i] = sub_part_num;
          }
        }
      } // end for
      if (OB_ITER_END == ret) {
        LOG_DEBUG("empty first hash part info, maybe ob version < 2.1", K(ret));
        ret = OB_SUCCESS;
      }
    } // end NULL != rs_fetcher && PARTITION_LEVEL_ONE == part_level
  }

  if (OB_SUCC(ret)) {
    desc_hash->set_oracle_mode(is_oracle_mode);
    desc_hash->set_part_space(part_space);
    desc_hash->set_part_num(part_num);
    desc_hash->set_part_level(part_level);
    desc_hash->set_part_func_type(part_func_type);
    // find the first match key, should consider whether the key is both part one and part two?
    for (int i = 0; i < key_info.key_num_; ++i) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == part_level) {
        desc_hash->set_part_key_type(static_cast<ObObjType>(key_info.part_keys_[i].obj_type_));
        desc_hash->set_part_key_cs_type(static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_));
        break;
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_hash;
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_hash;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }
  return ret;
}

int ObProxyPartMgr::build_sub_hash_part_with_non_template(const bool is_oracle_mode,
                                                          const ObPartitionFuncType part_func_type,
                                                          const int64_t part_space,
                                                          const ObProxyPartKeyInfo &key_info,
                                                          ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescHash *desc_hash = NULL;
  common::ObPartDesc *sub_part_desc = NULL;
  int64_t part_id = -1;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescHash) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescHash[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(ret));
  }

  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    desc_hash = (((ObPartDescHash *)sub_part_desc) + i);

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next())) {
        LOG_WARN("failed to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_id, int64_t);
      }
    }

    if (OB_SUCC(ret)) {
      desc_hash->set_oracle_mode(is_oracle_mode);
      desc_hash->set_first_part_id(part_id);
      desc_hash->set_part_space(part_space);
      desc_hash->set_part_num(sub_part_num_[i]);
      desc_hash->set_part_level(PARTITION_LEVEL_TWO);
      desc_hash->set_part_func_type(part_func_type);
      // find the first match key, should consider whether the key is both part one and part two?
      for (int i = 0; i < key_info.key_num_; ++i) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == PARTITION_LEVEL_TWO) {
          desc_hash->set_part_key_type(static_cast<ObObjType>(key_info.part_keys_[i].obj_type_));
          desc_hash->set_part_key_cs_type(static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_));
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
  }

  return ret;
}

int ObProxyPartMgr::build_key_part(const ObPartitionLevel part_level,
                                   const ObPartitionFuncType part_func_type,
                                   const int64_t part_num,
                                   const int64_t part_space,
                                   const bool is_template_table,
                                   const ObProxyPartKeyInfo &key_info,
                                   ObResultSetFetcher *rs_fetcher /*NULL*/)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescKey *desc_key = NULL;
  int64_t part_id = -1;
  ObString part_name;
  int64_t sub_part_num = 0;

  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescKey)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_key = new (tmp_buf) ObPartDescKey())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else {
    if (NULL != rs_fetcher && PARTITION_LEVEL_ONE == part_level) {
      first_part_num_ = part_num;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartNameIdPair) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      } else if (OB_ISNULL(part_pair_array_ = new (tmp_buf) ObPartNameIdPair[part_num])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to do placement new", K(tmp_buf), K(part_num), K(ret));
      } else if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      }
      for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
        if (OB_FAIL(rs_fetcher->next())) {
          LOG_DEBUG("failed to get next rs_fetcher", K(ret));
        } else {
          PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "part_id", part_id, int64_t);
          PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "part_name", part_name);
          part_pair_array_[i].set_part_id(part_id);
          if (OB_FAIL(part_pair_array_[i].set_part_name(part_name))) {
            LOG_WARN("fail to set part name", K(part_name), K(ret));
          } else if (!is_template_table) {
            PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "sub_part_num", sub_part_num, int64_t);
            sub_part_num_[i] = sub_part_num;
          }
        }
      } // end for
      if (OB_ITER_END == ret) {
        LOG_DEBUG("empty first key part info, maybe ob version < 2.1", K(ret));
        ret = OB_SUCCESS;
      }
    } // end NULL != rs_fetcher && PARTITION_LEVEL_ONE == part_level
  }
  if (OB_SUCC(ret)) {
    desc_key->set_part_space(part_space);
    desc_key->set_part_num(part_num);
    desc_key->set_part_level(part_level);
    desc_key->set_part_func_type(part_func_type);
    // find the first match key, should consider whether the key is both part one and part two?
    for (int i = 0; i < key_info.key_num_; ++i) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == part_level) {
        desc_key->set_part_key_type(static_cast<ObObjType>(key_info.part_keys_[i].obj_type_));
        desc_key->set_part_key_cs_type(static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_));
        break;
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_key;
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_key;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }
  return ret;
}

int ObProxyPartMgr::build_sub_key_part_with_non_template(const ObPartitionFuncType part_func_type,
                                                         const int64_t part_space,
                                                         const ObProxyPartKeyInfo &key_info,
                                                         ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescKey *desc_key = NULL;
  common::ObPartDesc *sub_part_desc = NULL;
  int64_t part_id = -1;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescKey) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescKey[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(ret));
  }

  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    desc_key = (((ObPartDescKey *)sub_part_desc) + i);

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next())) {
        LOG_WARN("failed to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_id, int64_t);
      }
    }

    if (OB_SUCC(ret)) {
      desc_key->set_first_part_id(part_id);
      desc_key->set_part_space(part_space);
      desc_key->set_part_num(sub_part_num_[i]);
      desc_key->set_part_level(PARTITION_LEVEL_TWO);
      desc_key->set_part_func_type(part_func_type);
      // find the first match key, should consider whether the key is both part one and part two?
      for (int i = 0; i < key_info.key_num_; ++i) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == PARTITION_LEVEL_TWO) {
          desc_key->set_part_key_type(static_cast<ObObjType>(key_info.part_keys_[i].obj_type_));
          desc_key->set_part_key_cs_type(static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_));
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
  }

  return ret;
}

int ObProxyPartMgr::build_range_part(const ObPartitionLevel part_level,
                                     const ObPartitionFuncType part_func_type,
                                     const int64_t part_num,
                                     const bool is_template_table,
                                     const ObProxyPartKeyInfo &key_info,
                                     ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescRange *desc_range = NULL;
  RangePartition *part_array = NULL;
  ObString part_name;
  int64_t sub_part_num = 0;

  // alloc desc_range and part_array
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescRange)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_range = new (tmp_buf) ObPartDescRange())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(ret));
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(RangePartition) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(part_array = new (tmp_buf) RangePartition[part_num])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(part_num), K(ret));
  } else {
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_num_ = part_num;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartNameIdPair) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      } else if (OB_ISNULL(part_pair_array_ = new (tmp_buf) ObPartNameIdPair[part_num])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to do placement new", K(tmp_buf), K(part_num), K(ret));
      } else if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      }
    }
  }

  // build desc_range
  ObString tmp_str;
  int64_t pos = 0;
  for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
    if (OB_FAIL(rs_fetcher.next())) {
      LOG_WARN("failed to get next rs_fetcher", K(ret));
    } else {
      if (PARTITION_LEVEL_ONE == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[i].part_id_, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", part_name);

        part_pair_array_[i].set_part_id(part_array[i].part_id_);
        if (OB_FAIL(part_pair_array_[i].set_part_name(part_name))) {
          LOG_WARN("fail to set part name", K(part_name), K(ret));
        } else if (!is_template_table) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_num", sub_part_num, int64_t);
          sub_part_num_[i] = sub_part_num;
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[i].part_id_, int64_t);
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);

      pos = 0;
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to fetch result", K(ret));
      } else if (OB_FAIL(part_array[i].high_bound_val_.deserialize(allocator_, tmp_str.ptr(),
                                                                   tmp_str.length(), pos))) {
        LOG_WARN("failed to deserialize", K(tmp_str), K(ret));
      } else {
        // do nothing
      }
    }
  } // end of for

  // TODO: maybe we need to sort part_array here

  if (OB_SUCC(ret)) {
    if (OB_FAIL(desc_range->set_part_array(part_array, part_num))) {
      LOG_WARN("failed to set_part_array, unexpected ", K(ret));
    }
  }

  // set desc_range
  if (OB_SUCC(ret)) {
    desc_range->set_part_level(part_level);
    desc_range->set_part_func_type(part_func_type);
    for (int k = 0; k < key_info.key_num_; ++k) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[k].level_) == part_level) {
        if (static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_ != CS_TYPE_INVALID)) {
          desc_range->set_collation_type(static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_));
          break;
        }
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_range;
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_range;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }
  return ret;
}

int ObProxyPartMgr::build_sub_range_part_with_non_template(const ObPartitionFuncType part_func_type,
                                                           const ObProxyPartKeyInfo &key_info,
                                                           ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  RangePartition *part_array = NULL;
  common::ObPartDesc *sub_part_desc = NULL;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescRange) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescRange[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(ret));
  }

  // build desc_range
  ObString tmp_str;
  int64_t pos = 0;
  ObPartDescRange *desc_range = NULL;
  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(RangePartition) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("part mgr reach memory limit", K(ret));
    } else if (OB_ISNULL(part_array = new (tmp_buf) RangePartition[sub_part_num_[i]])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to do placement new", K(tmp_buf), "sub_part_index", i,
               "sub_part_num", sub_part_num_[i], K(ret));
    } else {
      desc_range = (((ObPartDescRange *)sub_part_desc) + i);
    }

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next())) {
        LOG_WARN("failed to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[j].first_part_id_, int64_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[j].part_id_, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);

        pos = 0;
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to fetch result", K(ret));
        } else if (OB_FAIL(part_array[j].high_bound_val_.deserialize(allocator_, tmp_str.ptr(),
                                                                     tmp_str.length(), pos))) {
          LOG_WARN("failed to deserialize", K(tmp_str), K(ret));
        } else {
          // do nothing
        }
      }
    }

    if (OB_SUCC(ret)) {
      desc_range->set_part_level(PARTITION_LEVEL_TWO);
      desc_range->set_part_func_type(part_func_type);
      for (int k = 0; k < key_info.key_num_; ++k) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[k].level_) == PARTITION_LEVEL_TWO) {
          if (static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_ != CS_TYPE_INVALID)) {
            desc_range->set_collation_type(static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_));
            break;
          }
        }
      }
      if (OB_FAIL(desc_range->set_part_array(part_array, sub_part_num_[i]))) {
        LOG_WARN("failed to set_part_array, unexpected ", K(ret));
      }
    }
  } // end of for

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
  }

  return ret;
}

int ObProxyPartMgr::build_list_part(const ObPartitionLevel part_level,
                                    const ObPartitionFuncType part_func_type,
                                    const int64_t part_num,
                                    const bool is_template_table,
                                    const ObProxyPartKeyInfo &key_info,
                                    ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescList *desc_list = NULL;
  ListPartition *part_array = NULL;
  ObString part_name;
  int64_t sub_part_num = 0;

  // alloc desc_range and part_array
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescList)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_list = new (tmp_buf) ObPartDescList())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(ret));
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ListPartition) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(part_array = new (tmp_buf) ListPartition[part_num])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(part_num), K(ret));
  } else {
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_num_ = part_num;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartNameIdPair) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      } else if (OB_ISNULL(part_pair_array_ = new (tmp_buf) ObPartNameIdPair[part_num])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to do placement new", K(tmp_buf), K(part_num), K(ret));
      } else if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
        ret = OB_REACH_MEMORY_LIMIT;
        LOG_WARN("part mgr reach memory limit", K(ret));
      }
    }
  }

  // build desc_list
  ObString tmp_str;
  int64_t pos = 0;
  ObNewRow row;
  ObNewRow tmp_row;
  ObObj obj_array[OB_USER_ROW_MAX_COLUMNS_COUNT];
  row.assign(obj_array, OB_USER_ROW_MAX_COLUMNS_COUNT);
  for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
    if (OB_FAIL(rs_fetcher.next())) {
      LOG_WARN("failed to get next rs_fetcher", K(ret));
    } else {
      if (PARTITION_LEVEL_ONE == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[i].part_id_, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", part_name);
        part_pair_array_[i].set_part_id(part_array[i].part_id_);
        if (OB_FAIL(part_pair_array_[i].set_part_name(part_name))) {
          LOG_WARN("fail to set part name", K(part_name), K(ret));
        } else if (!is_template_table) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_num", sub_part_num, int64_t);
          sub_part_num_[i] = sub_part_num;
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[i].part_id_, int64_t);
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);
      pos = 0;
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to fetch result", K(ret));
      } else {
        // deserialize ob rows array
        while (OB_SUCC(ret) && pos < tmp_str.length()) {
          if (OB_FAIL(row.deserialize(tmp_str.ptr(), tmp_str.length(), pos))) {
            LOG_WARN("fail to deserialize ob row", K(tmp_str), K(ret));
          } else if (OB_FAIL(ob_write_row(allocator_, row, tmp_row))) {
            LOG_WARN("fail to write row to tmp row", K(row), K(tmp_row), K(ret));
          } else if (OB_FAIL(part_array[i].rows_.push_back(tmp_row))) {
            LOG_WARN("fail to add row array", K(ret));
          }
        } // end while
        if (part_array[i].rows_.count() == 1
            && part_array[i].rows_[0].get_count() == 1
            && part_array[i].rows_[0].get_cell(0).is_ext()) {
          desc_list->set_default_part_array_idx(i);
        }
      }
    }
  } // end of for

  if (OB_SUCC(ret)) {
    if (OB_FAIL(desc_list->set_part_array(part_array, part_num))) {
      LOG_WARN("failed to set_part_array, unexpected ", K(ret));
    }
  }

  // set desc_range
  if (OB_SUCC(ret)) {
    desc_list->set_part_level(part_level);
    desc_list->set_part_func_type(part_func_type);
    for (int k = 0; k < key_info.key_num_; ++k) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[k].level_) == part_level) {
        if (static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_ != CS_TYPE_INVALID)) {
          desc_list->set_collation_type(static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_));
          break;
        }
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_list;
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_list;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }
  return ret;
}

int ObProxyPartMgr::build_sub_list_part_with_non_template(const ObPartitionFuncType part_func_type,
                                                          const ObProxyPartKeyInfo &key_info,
                                                          ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ListPartition *part_array = NULL;
  common::ObPartDesc *sub_part_desc = NULL;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescList) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescList[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to do placement new", K(tmp_buf), K(ret));
  }

  // build desc_list
  ObString tmp_str;
  int64_t pos = 0;
  ObNewRow row;
  ObNewRow tmp_row;
  ObObj obj_array[OB_USER_ROW_MAX_COLUMNS_COUNT];
  row.assign(obj_array, OB_USER_ROW_MAX_COLUMNS_COUNT);
  ObPartDescList *desc_list = NULL;
  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ListPartition) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("part mgr reach memory limit", K(ret));
    } else if (OB_ISNULL(part_array = new (tmp_buf) ListPartition[sub_part_num_[i]])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to do placement new", K(tmp_buf), "sub_part_index", i,
               "sub_part_num", sub_part_num_[i], K(ret));
    } else {
      desc_list = (((ObPartDescList *)sub_part_desc) + i);
    }

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next())) {
        LOG_WARN("failed to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[j].first_part_id_, int64_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[j].part_id_, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);

        pos = 0;
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to fetch result", K(ret));
        } else {
          // deserialize ob rows array
          while (OB_SUCC(ret) && pos < tmp_str.length()) {
            if (OB_FAIL(row.deserialize(tmp_str.ptr(), tmp_str.length(), pos))) {
              LOG_WARN("fail to deserialize ob row", K(tmp_str), K(ret));
            } else if (OB_FAIL(ob_write_row(allocator_, row, tmp_row))) {
              LOG_WARN("fail to write row to tmp row", K(row), K(tmp_row), K(ret));
            } else if (OB_FAIL(part_array[j].rows_.push_back(tmp_row))) {
              LOG_WARN("fail to add row array", K(ret));
            }
          } // end while
          if (part_array[j].rows_.count() == 1
              && part_array[j].rows_[0].get_count() == 1
              && part_array[j].rows_[0].get_cell(0).is_ext()) {
            desc_list->set_default_part_array_idx(j);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      desc_list->set_part_level(PARTITION_LEVEL_TWO);
      desc_list->set_part_func_type(part_func_type);
      for (int k = 0; k < key_info.key_num_; ++k) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[k].level_) == PARTITION_LEVEL_TWO) {
          if (static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_ != CS_TYPE_INVALID)) {
            desc_list->set_collation_type(static_cast<ObCollationType>(key_info.part_keys_[k].cs_type_));
            break;
          }
        }
      }
      if (OB_FAIL(desc_list->set_part_array(part_array, sub_part_num_[i]))) {
        LOG_WARN("failed to set_part_array, unexpected ", K(ret));
      }
    }
  } // end of for

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
  }

  return ret;
}

int ObProxyPartMgr::get_first_part_num(int64_t &num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_first_part_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get first part num", K(ret));
  } else {
    num = first_part_num_;
  }
  return ret;
}

int ObProxyPartMgr::get_sub_part_num_by_first_part_id(ObProxyPartInfo &part_info,
                                                      const int64_t first_part_id,
                                                      int64_t &num)
{
  int ret = OB_SUCCESS;
  
  if (part_info.is_template_table()) {
    num = part_info.get_sub_part_option().part_num_;
  } else {
    if (OB_UNLIKELY(!is_sub_part_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get sub part num by first due to invalid sub part", K(ret));
    } else {
      int sub_ret = OB_INVALID_ARGUMENT;
      const ObPartitionFuncType sub_part_func_type = sub_part_desc_->get_part_func_type();
      
      for (int64_t i = 0; i < first_part_num_; ++i) {
        if (is_range_part(sub_part_func_type)) {
          ObPartDescRange *desc_range = (((ObPartDescRange*)sub_part_desc_) + i);
          RangePartition *part_array = desc_range->get_part_array();
          if (first_part_id == part_array[0].first_part_id_) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else if (is_list_part(sub_part_func_type)) {
          ObPartDescList *desc_list = (((ObPartDescList*)sub_part_desc_) + i);
          ListPartition *part_array = desc_list->get_part_array();
          if (first_part_id == part_array[0].first_part_id_) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else if (is_hash_part(sub_part_func_type)) {
          ObPartDescHash *desc_hash = (((ObPartDescHash*)sub_part_desc_) + i);
          if (first_part_id == desc_hash->get_first_part_id()) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else if (is_key_part(sub_part_func_type)) {
          ObPartDescKey *desc_key = (((ObPartDescKey*)sub_part_desc_) + i);
          if (first_part_id == desc_key->get_first_part_id()) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else {
          LOG_WARN("unsupported partition func type", K(sub_part_func_type));
          break;
        }
      }

      if (OB_FAIL(sub_ret)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail to get sub part num by first part id", K(ret));
      }
    }
  }
  
  return ret;
}

int64_t ObProxyPartMgr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  J_KV(KP(this), KPC_(first_part_desc));
  J_COMMA();

  // only sub_part_num_ != NULL, table is non-template partition table
  if (NULL != sub_part_desc_ && NULL != sub_part_num_) {
    const ObPartitionFuncType sub_part_func_type = sub_part_desc_->get_part_func_type();
    ObPartDesc *sub_part_desc = NULL;
    int i = 0;
    // If it is a non-template partition, only print the first secondary partition to avoid too many logs
    if (is_range_part(sub_part_func_type)) {
      sub_part_desc = (((ObPartDescRange*)sub_part_desc_) + i);
    } else if (is_list_part(sub_part_func_type)) {
      sub_part_desc = (((ObPartDescList*)sub_part_desc_) + i);
    } else if (is_hash_part(sub_part_func_type)) {
      sub_part_desc = (((ObPartDescHash*)sub_part_desc_) + i);
    } else if (is_key_part(sub_part_func_type)) {
      sub_part_desc = (((ObPartDescKey*)sub_part_desc_) + i);
    }
    J_KV("first_part_id", i, KPC(sub_part_desc));
    J_COMMA();
  } else {
    J_KV(KPC_(sub_part_desc));
  }

  J_OBJ_END();
  return pos;
}

} // namespace route
} // namespace obproxy
} // namespace oceanbase
