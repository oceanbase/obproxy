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
#include "share/part/ob_part_mgr_util.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "proxy/route/ob_server_route.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObProxyPartMgr::ObProxyPartMgr(ObIAllocator &allocator) : first_part_desc_(NULL)
                                                        , sub_part_desc_(NULL)
                                                        , first_part_num_(0)
                                                        , sub_part_num_(NULL)
                                                        , all_first_part_name_buf_(NULL)
                                                        , all_sub_part_name_buf_(NULL)
                                                        , all_first_part_name_length_(0)
                                                        , all_sub_part_name_length_(0)
                                                        , first_part_name_id_map_()
                                                        , sub_part_name_id_map_()
                                                        , allocator_(allocator)
                                                        , cluster_version_(0)
{
}

void ObProxyPartMgr::destroy()
{
  if(first_part_name_id_map_.created()) {
    first_part_name_id_map_.destroy();
  }
  if(sub_part_name_id_map_.created()) {
    sub_part_name_id_map_.destroy();
  }
}
int ObProxyPartMgr::get_part_with_part_name(const ObString &part_name,
                                            int64_t &part_id,
                                            ObProxyPartInfo &part_info,
                                            ObServerRoute &route,
                                            ObProxyExprCalculator &expr_calculator)
{
  int ret = OB_SUCCESS;
  if ( part_name.length() <= 0 || part_name.length() > MAX_PART_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error part name", K(part_name));
  } else {
    ObPartitionLevel part_level = part_info.get_part_level();
    ObString store_part_name;
    int64_t * part_id_ptr = NULL;
    PART_NAME_BUF tmp_buf;//64 byte stack buf
    store_part_name.assign_ptr((char *)&tmp_buf, part_name.length());
    MEMCPY(store_part_name.ptr(), part_name.ptr(), store_part_name.length());
    string_to_upper_case(store_part_name.ptr(), store_part_name.length());
    if ((sub_part_name_id_map_.created()) && OB_NOT_NULL(part_id_ptr = (int64_t *)sub_part_name_id_map_.get(store_part_name))) {
    //find sub part name, get ptr->(physical part id)
      part_id = *part_id_ptr;
      LOG_DEBUG("succ to get part id by sub part name", K(part_id), K(part_name));
    } else if ((first_part_name_id_map_.created()) && OB_NOT_NULL(part_id_ptr = (int64_t *)first_part_name_id_map_.get(store_part_name))) {
    //find first part name, get ptr->(first part id)
      LOG_DEBUG("succ to get part id by first part name", K(*part_id_ptr), K(part_name));
      if(OB_LIKELY(PARTITION_LEVEL_ONE == part_level)) {
        part_id = *part_id_ptr;
      } else if((PARTITION_LEVEL_TWO == part_level)
        && !obutils::get_global_proxy_config().enable_primary_zone
        && !obutils::get_global_proxy_config().enable_cached_server) {
        int64_t first_part_id = *part_id_ptr;
        int64_t sub_part_id = OB_INVALID_INDEX;
        if(OB_FAIL(expr_calculator.calc_part_id_by_random_choose_from_exist(part_info, first_part_id, sub_part_id, part_id))) {
          LOG_DEBUG("fail to get random part id by first part name", K(first_part_id), K(part_id), K(part_name));
        } else {
          // get part id by random, no need update pl
          route.no_need_pl_update_ = true;
          LOG_DEBUG("succ to get random part id by first part name", K(first_part_id), K(sub_part_id), K(part_id));
        }
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int ObProxyPartMgr::get_first_part_id_by_random(const int64_t rand_num, 
                                                int64_t &first_part_id,
                                                int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> part_ids;
  ObSEArray<int64_t, 1> tablet_ids;
  if (OB_ISNULL(first_part_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get first part, null ptr", K(ret));
  } else {
    if (OB_FAIL(first_part_desc_->get_part_by_num(rand_num, part_ids, tablet_ids))) {
      LOG_WARN("fail to get first part by random", K(first_part_desc_), K(ret));
    } else {
      if (part_ids.count() >= 1) {
        first_part_id = part_ids[0];
      }
      if (tablet_ids.count() >= 1) {
        tablet_id = tablet_ids[0];
      }
    }
  }

  return ret;
}

int ObProxyPartMgr::get_first_part(ObNewRange &range,
                                   ObIAllocator &allocator,
                                   ObIArray<int64_t> &part_ids,
                                   ObPartDescCtx &ctx,
                                   ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(first_part_desc_)) {
    ret = first_part_desc_->get_part(range, allocator, part_ids, ctx, tablet_ids);
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
                                                       ObPartDesc *&sub_part_desc_ptr,
                                                       const int64_t cluster_version)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(is_sub_part_valid())) {
    ObPartDesc *sub_part_desc_tmp = NULL;

    if (is_template_table && IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      sub_part_desc_tmp = sub_part_desc_;
    } else {
      const ObPartitionFuncType sub_part_func_type = sub_part_desc_->get_part_func_type();
      for (int64_t i = 0; i < first_part_num_; ++i) {
        if (is_range_part(sub_part_func_type, cluster_version)) {
          ObPartDescRange *desc_range = (((ObPartDescRange*)sub_part_desc_) + i);
          RangePartition *part_array = desc_range->get_part_array();
          // use first sub partition of everty first partition to get firt partition id
          if (first_part_id == part_array[0].first_part_id_) {
            sub_part_desc_tmp = desc_range;
            break;
          }
        } else if (is_list_part(sub_part_func_type, cluster_version)) {
          ObPartDescList *desc_list = (((ObPartDescList*)sub_part_desc_) + i);
          ListPartition *part_array = desc_list->get_part_array();
          // use first sub partition of everty first partition to get firt partition id
          if (first_part_id == part_array[0].first_part_id_) {
            sub_part_desc_tmp = desc_list;
            break;
          }
        } else if (is_hash_part(sub_part_func_type, cluster_version)) {
          ObPartDescHash *desc_hash = (((ObPartDescHash*)sub_part_desc_) + i);
          if (first_part_id == desc_hash->get_first_part_id()) {
            sub_part_desc_tmp = desc_hash;
            break;
          }
        } else if (is_key_part(sub_part_func_type, cluster_version)) {
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
                                 ObIArray<int64_t> &part_ids,
                                 ObPartDescCtx &ctx,
                                 ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_part_desc_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get sub part, null ptr", K(ret));
  } else {
    if (OB_FAIL(sub_part_desc_ptr->get_part(range, allocator, part_ids, ctx, tablet_ids))) {
      LOG_WARN("fail to get sub part", K(sub_part_desc_ptr), K(ret));
    }
  }

  return ret;
}

int ObProxyPartMgr::get_sub_part_by_random(const int64_t rand_num,
                                           ObPartDesc *sub_part_desc_ptr,
                                           ObIArray<int64_t> &part_ids,
                                           ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sub_part_desc_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get sub part, null ptr", K(ret));
  } else {
    if (OB_FAIL(sub_part_desc_ptr->get_part_by_num(rand_num, part_ids, tablet_ids))) {
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
                                    const int64_t part_col_num,
                                    const bool is_template_table,
                                    const ObProxyPartKeyInfo &key_info,
                                    ObResultSetFetcher *rs_fetcher /*NULL*/,
                                    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescHash *desc_hash = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  int64_t sub_part_num = 0;
  ObString part_name;
  int64_t *part_array = NULL;

  // After observer v4.0, there is no concept of template partition
  if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && PARTITION_LEVEL_TWO == part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster version bigger than 4 and level is two", K(ret), K(cluster_version));
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescHash)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_hash = new (tmp_buf) ObPartDescHash())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, part_num))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_hash->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit, alloc tablet id array failed", K(ret), K(part_num));
  } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(part_array = (int64_t*)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit, alloc part array failed", K(ret), K(part_num));
  } else {
    if (NULL != rs_fetcher) {
      if (PARTITION_LEVEL_ONE == part_level) {
        first_part_num_ = part_num;
        if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
          ret = OB_REACH_MEMORY_LIMIT;
          LOG_WARN("part mgr reach memory limit", K(ret));
        }
        for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
          if (OB_FAIL(rs_fetcher->next())) {
            LOG_DEBUG("fail to get next rs_fetcher", K(ret));
          } else {
            PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "part_id", part_id_buf[i], int64_t);
            PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "part_name", part_name);

            if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
              int64_t tablet_id = -1;
              part_array[i] = part_id_buf[i];
              PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "tablet_id", tablet_id, int64_t);
              desc_hash->tablet_id_array_[i] = tablet_id;
              if ((tablet_id != -1) && (tablet_id != 0)) {
                part_id_buf[i] = tablet_id;
              }
            }

            if (part_name.length() <= MAX_PART_NAME_LENGTH) {
              MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
              name_len_buf[i] = part_name.length();
              all_first_part_name_length_ += part_name.length();
            } else {//treat too long name string as empty string
              name_len_buf[i] = 0;
            }

            if (!is_template_table) {
              PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "sub_part_num", sub_part_num, int64_t);
              sub_part_num_[i] = sub_part_num;
            }
          }
        } // end for
        if (OB_ITER_END == ret) {
          LOG_DEBUG("empty first hash part info, maybe ob version < 2.1", K(ret));
          ret = OB_SUCCESS;
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {//part_num=sub part num
          if (OB_FAIL(rs_fetcher->next())) {
            LOG_DEBUG("fail to get next rs_fetcher", K(ret));
          } else {
            PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "sub_part_id", part_id_buf[i], int64_t);
            PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "part_name", part_name);
            if (part_name.length() <= MAX_PART_NAME_LENGTH) {
              MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
              name_len_buf[i] = part_name.length();
              all_sub_part_name_length_ += part_name.length();
            } else {//treat too long name string as empty string
              name_len_buf[i] = 0;
            }
          }
        } // end for
      } else {
        //nothing
      }
    } // end NULL != rs_fetcher 
  }

  if (OB_SUCC(ret)) {
    if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      desc_hash->set_part_array(part_array);
    }
    desc_hash->set_oracle_mode(is_oracle_mode);
    desc_hash->set_part_space(part_space);
    desc_hash->set_part_num(part_num);
    desc_hash->set_part_level(part_level);
    desc_hash->set_part_func_type(part_func_type);
    for (int i = 0; i < part_col_num; i++) {
      desc_hash->get_accuracies().push_back(ObAccuracy());
      desc_hash->get_obj_types().push_back(ObObjType());
      desc_hash->get_cs_types().push_back(ObCollationType());
    }
    // find all keys in this level 
    for (int i = 0; i < key_info.key_num_; ++i) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == part_level) {
        ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                            key_info.part_keys_[i].accuracy_.length_,
                            key_info.part_keys_[i].accuracy_.precision_,
                            key_info.part_keys_[i].accuracy_.scale_);
        desc_hash->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;        
        desc_hash->get_obj_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObObjType>(key_info.part_keys_[i].obj_type_);
        desc_hash->get_cs_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_);
      }
    }

    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_hash;
      if (OB_FAIL(init_first_part_map())) {
        LOG_DEBUG("fail to create first part name id map", K(ret));
      } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_first_part_name_buf_, all_first_part_name_length_, part_num, first_part_name_id_map_))) {
        LOG_DEBUG("fail to build first part name id map", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_hash;
      if (OB_FAIL(init_sub_part_map())) {
        LOG_DEBUG("fail to create temp sub part name id map", K(ret));
      } else if (OB_FAIL(build_temp_sub_part_name_id_map(name_buf, name_len_buf, part_id_buf, part_num))) {
        LOG_DEBUG("fail to build temp sub part name id map", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_sub_hash_part_with_non_template(const bool is_oracle_mode,
                                                          const ObPartitionFuncType part_func_type,
                                                          const int64_t part_space,
                                                          const int64_t part_col_num,
                                                          const ObProxyPartKeyInfo &key_info,
                                                          ObResultSetFetcher &rs_fetcher,
                                                          const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescHash *desc_hash = NULL;
  common::ObPartDesc *sub_part_desc = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  int64_t first_part_id = -1;
  int64_t sub_part_id = -1;
  ObString sub_part_name;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescHash) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescHash[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, all_sub_part_num_))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  }

  int64_t index=0;
  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    desc_hash = (((ObPartDescHash *)sub_part_desc) + i);
    if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_hash->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("allocator alloc tablet id array failed", K(ret), K(sub_part_num_[i]));
    }

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next()) || index >= all_sub_part_num_) {
        LOG_WARN("fail to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", first_part_id, int64_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", sub_part_id, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", sub_part_name);
        if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", desc_hash->tablet_id_array_[j], int64_t);
        }
        if (sub_part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[index], sub_part_name.ptr(), sub_part_name.length());
          name_len_buf[index] = sub_part_name.length();
          if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
            part_id_buf[index] = desc_hash->tablet_id_array_[j];
          } else {
            part_id_buf[index] = generate_phy_part_id(first_part_id, sub_part_id);
          }
          all_sub_part_name_length_ += sub_part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[index] = 0;
        }
        ++index;
      }
    }
    if (OB_SUCC(ret)) {
      desc_hash->set_oracle_mode(is_oracle_mode);
      desc_hash->set_first_part_id(first_part_id);
      desc_hash->set_part_space(part_space);
      desc_hash->set_part_num(sub_part_num_[i]);
      desc_hash->set_part_level(PARTITION_LEVEL_TWO);
      desc_hash->set_part_func_type(part_func_type);
      for (int i = 0; i < part_col_num; i++) {
        desc_hash->get_accuracies().push_back(ObAccuracy());
        desc_hash->get_obj_types().push_back(ObObjType());
        desc_hash->get_cs_types().push_back(ObCollationType());
      }
      // find all keys in this level 
      for (int i = 0; i < key_info.key_num_; ++i) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == PARTITION_LEVEL_TWO) {
          ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                              key_info.part_keys_[i].accuracy_.length_,
                              key_info.part_keys_[i].accuracy_.precision_,
                              key_info.part_keys_[i].accuracy_.scale_);
          desc_hash->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;
          desc_hash->get_obj_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObObjType>(key_info.part_keys_[i].obj_type_);
          desc_hash->get_cs_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
    if (OB_FAIL(init_sub_part_map())) {
      LOG_DEBUG("fail to create non temp sub part name id map", K(ret));
    } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_sub_part_name_buf_, all_sub_part_name_length_, all_sub_part_num_, sub_part_name_id_map_))) {
      LOG_DEBUG("fail to build non temp sub part name id map", K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_key_part(const ObPartitionLevel part_level,
                                   const ObPartitionFuncType part_func_type,
                                   const int64_t part_num,
                                   const int64_t part_space,
                                   const int64_t part_col_num,
                                   const bool is_template_table,
                                   const ObProxyPartKeyInfo &key_info,
                                   ObResultSetFetcher *rs_fetcher /*NULL*/,
                                   const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescKey *desc_key = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  int64_t sub_part_num = 0;
  ObString part_name;
  int64_t *part_array = NULL;

  if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && PARTITION_LEVEL_TWO == part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster version is bigger than 4 and level is two", K(ret), K(cluster_version));
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescKey)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_key = new (tmp_buf) ObPartDescKey())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, part_num))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_key->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret), K(part_num));
  } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(part_array = (int64_t*)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("fail to alloc part array", K(ret), K(part_num));
  } else {
    if (NULL != rs_fetcher) {
      if (PARTITION_LEVEL_ONE == part_level) {
        first_part_num_ = part_num;
        if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
          ret = OB_REACH_MEMORY_LIMIT;
          LOG_WARN("part mgr reach memory limit", K(ret));
        }
        for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
          if (OB_FAIL(rs_fetcher->next())) {
            LOG_DEBUG("fail to get next rs_fetcher", K(ret));
          } else {
            PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "part_id", part_id_buf[i], int64_t);
            PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "part_name", part_name);

            if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
              int64_t tablet_id = -1;
              part_array[i] = part_id_buf[i];
              PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "tablet_id", tablet_id, int64_t);
              desc_key->tablet_id_array_[i] = tablet_id;
              if ((tablet_id != -1) && (tablet_id != 0)) {
                part_id_buf[i] = tablet_id;
              }
            }

            if (part_name.length() <= MAX_PART_NAME_LENGTH) {
              MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
              name_len_buf[i] = part_name.length();
              all_first_part_name_length_ += part_name.length();
            } else {//treat too long name string as empty string
              name_len_buf[i] = 0;
            }

            if (!is_template_table) {
              PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "sub_part_num", sub_part_num, int64_t);
              sub_part_num_[i] = sub_part_num;
            }
          }
        } // end for
        if (OB_ITER_END == ret) {
          LOG_DEBUG("empty first key part info, maybe ob version < 2.1", K(ret));
          ret = OB_SUCCESS;
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {//part_num=sub part num
          if (OB_FAIL(rs_fetcher->next())) {
            LOG_DEBUG("fail to get next rs_fetcher", K(ret));
          } else {
            PROXY_EXTRACT_INT_FIELD_MYSQL(*rs_fetcher, "sub_part_id", part_id_buf[i], int64_t);
            PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "part_name", part_name);
            if (part_name.length() <= MAX_PART_NAME_LENGTH) {
              MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
              name_len_buf[i] = part_name.length();
              all_sub_part_name_length_ += part_name.length();
            } else {//treat too long name string as empty string
              name_len_buf[i] = 0;
            }
          }
        } // end for
      } else {
        //nothing
      }
    } // end NULL != rs_fetcher
  }

  if (OB_SUCC(ret)) {
    if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
      desc_key->set_part_array(part_array);
    }
    desc_key->set_part_space(part_space);
    desc_key->set_part_num(part_num);
    desc_key->set_part_level(part_level);
    desc_key->set_part_func_type(part_func_type);
    for (int i = 0; i < part_col_num; i++) {
      desc_key->get_accuracies().push_back(ObAccuracy());
      desc_key->get_obj_types().push_back(ObObjType());
      desc_key->get_cs_types().push_back(ObCollationType());
    }
    // find all keys in this level 
    for (int i = 0; i < key_info.key_num_; ++i) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == part_level) {
        ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                              key_info.part_keys_[i].accuracy_.length_,
                              key_info.part_keys_[i].accuracy_.precision_,
                              key_info.part_keys_[i].accuracy_.scale_);
        desc_key->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;
        desc_key->get_obj_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObObjType>(key_info.part_keys_[i].obj_type_);
        desc_key->get_cs_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_);
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_key;
      if (OB_FAIL(init_first_part_map())) {
        LOG_DEBUG("fail to create first part name id map", K(ret));
      } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_first_part_name_buf_, all_first_part_name_length_, part_num, first_part_name_id_map_))) {
        LOG_DEBUG("fail to build first part name id map", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_key;
      if (OB_FAIL(init_sub_part_map())) {
        LOG_DEBUG("fail to create temp sub part name id map", K(ret));
      } else if (OB_FAIL(build_temp_sub_part_name_id_map(name_buf, name_len_buf, part_id_buf, part_num))) {
        LOG_DEBUG("fail to build temp sub part name id map", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_sub_key_part_with_non_template(const ObPartitionFuncType part_func_type,
                                                         const int64_t part_space,
                                                         const int64_t part_col_num,
                                                         const ObProxyPartKeyInfo &key_info,
                                                         ObResultSetFetcher &rs_fetcher,
                                                         const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescKey *desc_key = NULL;
  common::ObPartDesc *sub_part_desc = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  int64_t first_part_id = -1;
  int64_t sub_part_id = -1;
  ObString sub_part_name;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescKey) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescKey[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, all_sub_part_num_))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  }

  int64_t index=0;
  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    desc_key = (((ObPartDescKey *)sub_part_desc) + i);
    if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_key->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("allocate tablet id array failed", K(ret), K(sub_part_num_[i]));
    }

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next()) || index >= all_sub_part_num_) {
        LOG_WARN("fail to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", first_part_id, int64_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", sub_part_id, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", sub_part_name);
        if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", desc_key->tablet_id_array_[j], int64_t);
        }
        if (sub_part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[index], sub_part_name.ptr(), sub_part_name.length());
          name_len_buf[index] = sub_part_name.length();
          if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
            part_id_buf[index] = desc_key->tablet_id_array_[j];
          } else {
            part_id_buf[index] = generate_phy_part_id(first_part_id, sub_part_id);
          }
          all_sub_part_name_length_ += sub_part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[index] = 0;
        }
        ++index;
      }
    }

    if (OB_SUCC(ret)) {
      desc_key->set_first_part_id(first_part_id);
      desc_key->set_part_space(part_space);
      desc_key->set_part_num(sub_part_num_[i]);
      desc_key->set_part_level(PARTITION_LEVEL_TWO);
      desc_key->set_part_func_type(part_func_type);
      for (int i = 0; i < part_col_num; i++) {
        desc_key->get_accuracies().push_back(ObAccuracy());
        desc_key->get_obj_types().push_back(ObObjType());
        desc_key->get_cs_types().push_back(ObCollationType());
      }
      // find all keys in this level 
      for (int i = 0; i < key_info.key_num_; ++i) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == PARTITION_LEVEL_TWO) {
          ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                              key_info.part_keys_[i].accuracy_.length_,
                              key_info.part_keys_[i].accuracy_.precision_,
                              key_info.part_keys_[i].accuracy_.scale_);
          desc_key->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;
          desc_key->get_obj_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObObjType>(key_info.part_keys_[i].obj_type_);
          desc_key->get_cs_types().at(key_info.part_keys_[i].idx_in_part_columns_) = static_cast<ObCollationType>(key_info.part_keys_[i].cs_type_);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
    if (OB_FAIL(init_sub_part_map())) {
      LOG_DEBUG("fail to create non temp sub part name id map", K(ret));
    } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_sub_part_name_buf_, all_sub_part_name_length_, all_sub_part_num_, sub_part_name_id_map_))) {
      LOG_DEBUG("fail to build non temp sub part name id map", K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_range_part(const ObPartitionLevel part_level,
                                     const ObPartitionFuncType part_func_type,
                                     const int64_t part_num,
                                     const int64_t part_col_num,
                                     const bool is_template_table,
                                     const ObProxyPartKeyInfo &key_info,
                                     ObResultSetFetcher &rs_fetcher,
                                     const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescRange *desc_range = NULL;
  RangePartition *part_array = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  int64_t sub_part_num = 0;
  ObString part_name;

  if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && PARTITION_LEVEL_TWO == part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster version bigger than 4, level two has no tempalte", K(ret));
  // alloc desc_range and part_array
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescRange)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_range = new (tmp_buf) ObPartDescRange())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(ret));
  } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_range->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(RangePartition) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(part_array = new (tmp_buf) RangePartition[part_num])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(part_num), K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, part_num))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  } else {
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_num_ = part_num;
      if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
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
      LOG_WARN("fail to get next rs_fetcher", K(ret));
    } else {
      if (PARTITION_LEVEL_ONE == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[i].part_id_, int64_t);
        part_id_buf[i] = part_array[i].part_id_;
        if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          int64_t tablet_id = -1;
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", tablet_id, int64_t);
          desc_range->tablet_id_array_[i] = tablet_id;
          if ((tablet_id != -1) && (tablet_id != 0)) {
            part_id_buf[i] = tablet_id;
          }
        }
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", part_name);
        if (part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
          name_len_buf[i] = part_name.length();
          all_first_part_name_length_ += part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[i] = 0;
        }
        if (!is_template_table) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_num", sub_part_num, int64_t);
          sub_part_num_[i] = sub_part_num;
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[i].part_id_, int64_t);//sub_part_id
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", part_name);
        part_id_buf[i] = part_array[i].part_id_;
        if (part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
          name_len_buf[i] = part_name.length();
          all_sub_part_name_length_ += part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[i] = 0;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);

      pos = 0;
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to fetch result", K(ret));
      } else if (OB_FAIL(part_array[i].high_bound_val_.deserialize(allocator_, tmp_str.ptr(),
                                                                   tmp_str.length(), pos))) {
        LOG_WARN("fail to deserialize", K(tmp_str), K(ret));
      } else {
        // do nothing
      }
    }
  } // end of for

  // TODO: maybe we need to sort part_array here

  if (OB_SUCC(ret)) {
    if (OB_FAIL(desc_range->set_part_array(part_array, part_num))) {
      LOG_WARN("fail to set_part_array, unexpected ", K(ret));
    }
  }

  // set desc_range
  if (OB_SUCC(ret)) {
    desc_range->set_part_level(part_level);
    desc_range->set_part_func_type(part_func_type);
    for (int i = 0; i < part_col_num; i++) {
      desc_range->get_accuracies().push_back(ObAccuracy());
    }
    // find all keys in this level 
    for (int i = 0; i < key_info.key_num_; ++i) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == part_level) {
        ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                              key_info.part_keys_[i].accuracy_.length_,
                              key_info.part_keys_[i].accuracy_.precision_,
                              key_info.part_keys_[i].accuracy_.scale_);
        desc_range->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_range;
      if (OB_FAIL(init_first_part_map())) {
        LOG_DEBUG("fail to create first part name id map", K(ret));
      } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_first_part_name_buf_, all_first_part_name_length_, part_num, first_part_name_id_map_))) {
        LOG_DEBUG("fail to build first part name id map", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_range;
      if (OB_FAIL(init_sub_part_map())) {
        LOG_DEBUG("fail to create temp sub part name id map", K(ret));
      } else if (OB_FAIL(build_temp_sub_part_name_id_map(name_buf, name_len_buf, part_id_buf, part_num))) {
        LOG_DEBUG("fail to build temp sub part name id map", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_sub_range_part_with_non_template(const ObPartitionFuncType part_func_type,
                                                           const int64_t part_col_num,
                                                           const ObProxyPartKeyInfo &key_info,
                                                           ObResultSetFetcher &rs_fetcher,
                                                           const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  RangePartition *part_array = NULL;
  common::ObPartDesc *sub_part_desc = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  ObString sub_part_name;

  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescRange) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescRange[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, all_sub_part_num_))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  }
  // build desc_range
  ObString tmp_str;
  int64_t index = 0;
  int64_t pos = 0;
  ObPartDescRange *desc_range = NULL;
  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    desc_range = (((ObPartDescRange *)sub_part_desc) + i);
    tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(RangePartition) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("part mgr reach memory limit", K(ret));
    } else if (OB_ISNULL(part_array = new (tmp_buf) RangePartition[sub_part_num_[i]])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to do placement new", K(tmp_buf), "sub_part_index", i,
               "sub_part_num", sub_part_num_[i], K(ret));
    } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_range->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("alloc tablet id array failed", K(ret), K(sub_part_num_[i]));
    }

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next()) || index >= all_sub_part_num_) {
        LOG_WARN("fail to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[j].first_part_id_, int64_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[j].part_id_, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);

        if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", desc_range->tablet_id_array_[j], int64_t);
        }

        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", sub_part_name);
        if (sub_part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[index], sub_part_name.ptr(), sub_part_name.length());
          name_len_buf[index] = sub_part_name.length();
          if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
            part_id_buf[index] = desc_range->tablet_id_array_[j];
          } else {
            part_id_buf[index] = generate_phy_part_id(part_array[j].first_part_id_, part_array[j].part_id_);
          }
          all_sub_part_name_length_ += sub_part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[index] = 0;
        }
        ++index;
        pos = 0;
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to fetch result", K(ret));
        } else if (OB_FAIL(part_array[j].high_bound_val_.deserialize(allocator_, tmp_str.ptr(),
                                                                     tmp_str.length(), pos))) {
          LOG_WARN("fail to deserialize", K(tmp_str), K(ret));
        } else {
          // do nothing
        }
      }
    }

    if (OB_SUCC(ret)) {
      desc_range->set_part_level(PARTITION_LEVEL_TWO);
      desc_range->set_part_func_type(part_func_type);
      for (int i = 0; i < part_col_num; i++) {
        desc_range->get_accuracies().push_back(ObAccuracy());
      }
      // find all keys in this level 
      for (int i = 0; i < key_info.key_num_; ++i) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == PARTITION_LEVEL_TWO) {
          ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                              key_info.part_keys_[i].accuracy_.length_,
                              key_info.part_keys_[i].accuracy_.precision_,
                              key_info.part_keys_[i].accuracy_.scale_);
          desc_range->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;
        }
      }
      if (OB_FAIL(desc_range->set_part_array(part_array, sub_part_num_[i]))) {
        LOG_WARN("fail to set_part_array, unexpected ", K(ret));
      }
    }
  } // end of for

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
    if (OB_FAIL(init_sub_part_map())) {
      LOG_DEBUG("fail to create non temp sub part name id map", K(ret));
    } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_sub_part_name_buf_, all_sub_part_name_length_, all_sub_part_num_, sub_part_name_id_map_))) {
      LOG_DEBUG("fail to build non temp sub part name id map", K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_list_part(const ObPartitionLevel part_level,
                                    const ObPartitionFuncType part_func_type,
                                    const int64_t part_num,
                                    const int64_t part_col_num,
                                    const bool is_template_table,
                                    const ObProxyPartKeyInfo &key_info,
                                    ObResultSetFetcher &rs_fetcher,
                                    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObPartDescList *desc_list = NULL;
  ListPartition *part_array = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  int64_t sub_part_num = 0;
  ObString part_name;

  if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && PARTITION_LEVEL_TWO == part_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster version bigger than 4, part level is two", K(ret), K(cluster_version));
  // alloc desc_range and part_array
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescList)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(desc_list = new (tmp_buf) ObPartDescList())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(ret));
  } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_list->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit, alloc tablet id array failed", K(ret));
  } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ListPartition) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(part_array = new (tmp_buf) ListPartition[part_num])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(part_num), K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, part_num))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  } else {
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_num_ = part_num;
      if (!is_template_table && OB_ISNULL(sub_part_num_ = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
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
  for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
    if (OB_FAIL(rs_fetcher.next())) {
      LOG_WARN("fail to get next rs_fetcher", K(ret));
    } else {
      if (PARTITION_LEVEL_ONE == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[i].part_id_, int64_t);
        part_id_buf[i] = part_array[i].part_id_;
        if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          int64_t tablet_id = -1;
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", tablet_id, int64_t);
          desc_list->tablet_id_array_[i] = tablet_id;
          if ((tablet_id != -1) && (tablet_id != 0)) {
            part_id_buf[i] = tablet_id;
          }
        }
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", part_name);
        if (part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
          name_len_buf[i] = part_name.length();
          all_first_part_name_length_ += part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[i] = 0;
        }
        if (!is_template_table) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_num", sub_part_num, int64_t);
          sub_part_num_[i] = sub_part_num;
        }
      } else if (PARTITION_LEVEL_TWO == part_level) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[i].part_id_, int64_t);//sub_part_id
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", part_name);
        part_id_buf[i] = part_array[i].part_id_;
        if (part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[i], part_name.ptr(), part_name.length());
          name_len_buf[i] = part_name.length();
          all_sub_part_name_length_ += part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[i] = 0;
        }
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
          row.assign(obj_array, OB_USER_ROW_MAX_COLUMNS_COUNT);
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
      LOG_WARN("fail to set_part_array, unexpected ", K(ret));
    }
  }

  // set desc_range
  if (OB_SUCC(ret)) {
    desc_list->set_part_level(part_level);
    desc_list->set_part_func_type(part_func_type);
    for (int i = 0; i < part_col_num; i++) {
      desc_list->get_accuracies().push_back(ObAccuracy());
    }
    // find all keys in this level 
    for (int i = 0; i < key_info.key_num_; ++i) {
      if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == part_level) {
        ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                            key_info.part_keys_[i].accuracy_.length_,
                            key_info.part_keys_[i].accuracy_.precision_,
                            key_info.part_keys_[i].accuracy_.scale_);
        desc_list->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;
      }
    }
    if (PARTITION_LEVEL_ONE == part_level) {
      first_part_desc_ = desc_list;
      if (OB_FAIL(init_first_part_map())) {
        LOG_DEBUG("fail to create first part name id map", K(ret));
      } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_first_part_name_buf_, all_first_part_name_length_, part_num, first_part_name_id_map_))) {
        LOG_DEBUG("fail to build first part name id map", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == part_level) {
      sub_part_desc_ = desc_list;
      if (OB_FAIL(init_sub_part_map())) {
        LOG_DEBUG("fail to create temp sub part name id map", K(ret));
      } else if (OB_FAIL(build_temp_sub_part_name_id_map(name_buf, name_len_buf, part_id_buf, part_num))) {
        LOG_DEBUG("fail to build temp sub part name id map", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("part level invalid", K(part_level), K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
  return ret;
}

int ObProxyPartMgr::build_sub_list_part_with_non_template(const ObPartitionFuncType part_func_type,
                                                          const int64_t part_col_num,
                                                          const ObProxyPartKeyInfo &key_info,
                                                          ObResultSetFetcher &rs_fetcher,
                                                          const int64_t cluster_version)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ListPartition *part_array = NULL;
  common::ObPartDesc *sub_part_desc = NULL;
  PART_NAME_BUF *name_buf = NULL;
  int64_t *name_len_buf = NULL;
  int64_t *part_id_buf = NULL;
  ObString sub_part_name;
  
  // alloc desc_range
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObPartDescList) * first_part_num_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit", K(ret));
  } else if (OB_ISNULL(sub_part_desc = new (tmp_buf) ObPartDescList[first_part_num_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do placement new", K(tmp_buf), K(ret));
  } else if (OB_FAIL(alloc_name_buf(name_buf, name_len_buf, part_id_buf, all_sub_part_num_))) {
    LOG_WARN("fail to alloc name buf", K(ret));
  }
  // build desc_list
  ObString tmp_str;
  int64_t index = 0;
  int64_t pos = 0;
  ObNewRow row;
  ObNewRow tmp_row;
  ObObj obj_array[OB_USER_ROW_MAX_COLUMNS_COUNT];
  ObPartDescList *desc_list = NULL;
  for (int64_t i = 0; i < first_part_num_ && OB_SUCC(ret); ++i) {
    tmp_buf = NULL;
    desc_list = (((ObPartDescList *)sub_part_desc) + i);
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ListPartition) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("part mgr reach memory limit", K(ret));
    } else if (OB_ISNULL(part_array = new (tmp_buf) ListPartition[sub_part_num_[i]])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to do placement new", K(tmp_buf), "sub_part_index", i,
               "sub_part_num", sub_part_num_[i], K(ret));
    } else if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version) && OB_ISNULL(desc_list->tablet_id_array_ = (int64_t*)allocator_.alloc(sizeof(int64_t) * sub_part_num_[i]))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WARN("alloc tablet id array failed", K(ret), K(sub_part_num_[i]));
    }

    for (int64_t j = 0; j < sub_part_num_[i] && OB_SUCC(ret); ++j) {
      if (OB_FAIL(rs_fetcher.next()) || index >= all_sub_part_num_) {
        LOG_WARN("fail to get next rs_fetcher", K(ret));
      } else {
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "part_id", part_array[j].first_part_id_, int64_t);
        PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "sub_part_id", part_array[j].part_id_, int64_t);
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "high_bound_val_bin", tmp_str);

        if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "tablet_id", desc_list->tablet_id_array_[j], int64_t);
        }

        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(rs_fetcher, "part_name", sub_part_name);
        if (sub_part_name.length() <= MAX_PART_NAME_LENGTH) {
          MEMCPY(name_buf[index], sub_part_name.ptr(), sub_part_name.length());
          name_len_buf[index] = sub_part_name.length();
          if (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
            part_id_buf[index] = desc_list->tablet_id_array_[j];
          } else {
            part_id_buf[index] = generate_phy_part_id(part_array[j].first_part_id_, part_array[j].part_id_);
          }
          all_sub_part_name_length_ += sub_part_name.length();
        } else {//treat too long name string as empty string
          name_len_buf[index] = 0;
        }
        ++index;
        pos = 0;
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to fetch result", K(ret));
        } else {
          // deserialize ob rows array
          while (OB_SUCC(ret) && pos < tmp_str.length()) {
            row.assign(obj_array, OB_USER_ROW_MAX_COLUMNS_COUNT);
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
      for (int i = 0; i < part_col_num; i++) {
        desc_list->get_accuracies().push_back(ObAccuracy());
      }
      // find all keys in this level 
      for (int i = 0; i < key_info.key_num_; ++i) {
        if (static_cast<ObPartitionLevel>(key_info.part_keys_[i].level_) == PARTITION_LEVEL_TWO) {
          ObAccuracy accuracy(key_info.part_keys_[i].accuracy_.valid_, 
                              key_info.part_keys_[i].accuracy_.length_,
                              key_info.part_keys_[i].accuracy_.precision_,
                              key_info.part_keys_[i].accuracy_.scale_);
          desc_list->get_accuracies().at(key_info.part_keys_[i].idx_in_part_columns_) = accuracy;       
        }
      }
      if (OB_FAIL(desc_list->set_part_array(part_array, sub_part_num_[i]))) {
        LOG_WARN("fail to set_part_array, unexpected ", K(ret));
      }
    }
  } // end of for

  if (OB_SUCC(ret)) {
    sub_part_desc_ = sub_part_desc;
    if (OB_FAIL(init_sub_part_map())) {
      LOG_DEBUG("fail to create non temp sub part name id map", K(ret));
    } else if (OB_FAIL(build_part_name_id_map(name_buf, name_len_buf, part_id_buf, all_sub_part_name_buf_, all_sub_part_name_length_, all_sub_part_num_, sub_part_name_id_map_))) {
      LOG_DEBUG("fail to build non temp sub part name id map", K(ret));
    }
  }

  free_name_buf(name_buf, name_len_buf, part_id_buf);
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
  const int64_t cluster_version = part_info.get_cluster_version();
  if (part_info.is_template_table() && IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    num = part_info.get_sub_part_option().part_num_;
  } else {
    if (OB_UNLIKELY(!is_sub_part_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get sub part num by first due to invalid sub part", K(ret));
    } else {
      int sub_ret = OB_INVALID_ARGUMENT;
      const ObPartitionFuncType sub_part_func_type = sub_part_desc_->get_part_func_type();

      for (int64_t i = 0; i < first_part_num_; ++i) {
        if (is_range_part(sub_part_func_type, cluster_version)) {
          ObPartDescRange *desc_range = (((ObPartDescRange*)sub_part_desc_) + i);
          RangePartition *part_array = desc_range->get_part_array();
          if (first_part_id == part_array[0].first_part_id_) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else if (is_list_part(sub_part_func_type, cluster_version)) {
          ObPartDescList *desc_list = (((ObPartDescList*)sub_part_desc_) + i);
          ListPartition *part_array = desc_list->get_part_array();
          if (first_part_id == part_array[0].first_part_id_) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else if (is_hash_part(sub_part_func_type, cluster_version)) {
          ObPartDescHash *desc_hash = (((ObPartDescHash*)sub_part_desc_) + i);
          if (first_part_id == desc_hash->get_first_part_id()) {
            num = sub_part_num_[i];
            sub_ret = OB_SUCCESS;
            break;
          }
        } else if (is_key_part(sub_part_func_type, cluster_version)) {
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

int ObProxyPartMgr::build_part_name_id_map(const PART_NAME_BUF * const name_buf, 
                                           const int64_t * const name_len_buf, 
                                           const int64_t * const part_id_buf, 
                                           char * target_part_name_buf,
                                           const int64_t total_part_name_len,
                                           const int64_t part_num, 
                                           PartNameIdMap& part_name_id_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_buf) || OB_ISNULL(name_len_buf) || OB_ISNULL(part_id_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get part name buf", K(ret));
  } else if (OB_NOT_NULL(target_part_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part name buf is alloc twice", K(ret));
  } else if (OB_ISNULL(target_part_name_buf = (char *)allocator_.alloc(total_part_name_len))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit when alloc all first part name buf", K(ret));
  } else {
    ObString part_name;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      if (OB_LIKELY( 0 != name_len_buf[i])) {
        part_name.assign_ptr(target_part_name_buf + pos, static_cast<ObString::obstr_size_t>(name_len_buf[i]));
        MEMCPY(target_part_name_buf + pos, name_buf[i], name_len_buf[i]);
        string_to_upper_case(part_name.ptr(), part_name.length());
        part_name_id_map.set_refactored(part_name, part_id_buf[i]);
        pos += name_len_buf[i];
      }
    }
  }
  return ret;
}

int ObProxyPartMgr::build_temp_sub_part_name_id_map(const PART_NAME_BUF * const name_buf, 
                                                    const int64_t * const name_len_buf, 
                                                    const int64_t * const part_id_buf, 
                                                    const int64_t part_num)
{
  int ret = OB_SUCCESS;
  all_sub_part_name_length_ = (all_first_part_name_length_ * part_num) //total length of first-part-name
                              + (all_sub_part_name_length_ * first_part_num_) //total length of sub-part-name
                              + (part_num * first_part_num_); //total length of 's' 
  if (OB_ISNULL(name_buf) || OB_ISNULL(name_len_buf) || OB_ISNULL(part_id_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get part name buf", K(ret));
  } else if (OB_ISNULL(all_sub_part_name_buf_ = (char *)allocator_.alloc(sizeof(char) * all_sub_part_name_length_))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit when alloc all first part name buf", K(ret));
  } else {
    ObString part_name;
    int64_t pos = 0;
    int64_t tmp_len = 0;
    PartNameIdMap::const_iterator iter = first_part_name_id_map_.begin();
    PartNameIdMap::const_iterator end = first_part_name_id_map_.end();
    for (; OB_SUCC(ret) && iter !=end; iter++) {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_LIKELY(0 != name_len_buf[i])) {
            tmp_len = iter->first.length() + 1 + name_len_buf[i];//first part name + 's' + sub part name
          if (OB_LIKELY(tmp_len <= MAX_PART_NAME_LENGTH)) {
            part_name.assign_ptr(all_sub_part_name_buf_ + pos, static_cast<ObString::obstr_size_t>(tmp_len));
            MEMCPY(all_sub_part_name_buf_ + pos, iter->first.ptr(), iter->first.length());
            pos += iter->first.length();
            all_sub_part_name_buf_[pos++] = 'S';
            MEMCPY(all_sub_part_name_buf_ + pos, name_buf[i], name_len_buf[i]);
            pos += name_len_buf[i];
            string_to_upper_case(part_name.ptr(), part_name.length());
            sub_part_name_id_map_.set_refactored(part_name, generate_phy_part_id(iter->second, part_id_buf[i]));
          }
        }  
      }
    }
  }
  return ret;
}

int ObProxyPartMgr::alloc_name_buf(PART_NAME_BUF *&name_buf, int64_t *&name_len_buf, int64_t *&part_id_buf, const int64_t part_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_buf = (PART_NAME_BUF *)allocator_.alloc(sizeof(PART_NAME_BUF) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit when alloc name buf", K(ret));
  } else if (OB_ISNULL(name_len_buf = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit when alloc name len buf", K(ret));
  } else if (OB_ISNULL(part_id_buf = (int64_t *)allocator_.alloc(sizeof(int64_t) * part_num))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WARN("part mgr reach memory limit when alloc part id buf", K(ret));
  }

  if (OB_NOT_NULL(name_len_buf)) {
    MEMSET(name_len_buf, 0, sizeof(int64_t) * part_num);
  }

  return ret;
}

void ObProxyPartMgr::free_name_buf(PART_NAME_BUF *name_buf, int64_t *name_len_buf, int64_t *part_id_buf)
{
  if (OB_NOT_NULL(part_id_buf)) {
    allocator_.free(part_id_buf);
  }
  if (OB_NOT_NULL(name_len_buf)) {
    allocator_.free(name_len_buf);
  }
  if (OB_NOT_NULL(name_buf)) {
    allocator_.free(name_buf);
  }
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
    if (is_range_part(sub_part_func_type, cluster_version_)) {
      sub_part_desc = (((ObPartDescRange*)sub_part_desc_) + i);
    } else if (is_list_part(sub_part_func_type, cluster_version_)) {
      sub_part_desc = (((ObPartDescList*)sub_part_desc_) + i);
    } else if (is_hash_part(sub_part_func_type, cluster_version_)) {
      sub_part_desc = (((ObPartDescHash*)sub_part_desc_) + i);
    } else if (is_key_part(sub_part_func_type, cluster_version_)) {
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
