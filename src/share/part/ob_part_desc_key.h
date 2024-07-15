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

#ifndef _OB_PART_DESC_KEY_H
#define _OB_PART_DESC_KEY_H 1

#include "share/part/ob_part_desc.h"
#include "share/part/ob_part_mgr_util.h"
#include "proxy/route/obproxy_part_mgr.h"
namespace oceanbase
{
namespace common
{
class ObPartDescKey : public ObPartDesc
{
public:
  ObPartDescKey();
  virtual ~ObPartDescKey() {}

  virtual int get_part(ObNewRange &range,
                       ObIAllocator &allocator,
                       ObIArray<int64_t> &part_ids,
                       ObPartDescCtx &ctx,
                       ObIArray<int64_t> &tablet_ids,
                       int64_t &part_idx) override;
  virtual int get_ls_id_by_num(const int64_t num, ObIArray<int64_t> &ls_ids) override;
  virtual int get_part_by_num(const int64_t num,
                              ObIArray<int64_t> &part_ids,
                              ObIArray<int64_t> &tablet_ids) override;
  virtual int get_part_for_obkv(ObNewRange &range,
                                ObIAllocator &allocator,
                                ObIArray<int64_t> &part_ids,
                                ObPartDescCtx &ctx,
                                ObIArray<int64_t> &tablet_ids,
                                ObIArray<int64_t> &ls_ids) override;
  virtual int get_all_part_id_for_obkv(ObIArray<int64_t> &part_ids,
                               ObIArray<int64_t> &tablet_ids,
                               ObIArray<int64_t> &ls_ids) override;
  void set_part_num(int64_t part_num) { part_num_ = part_num; }
  void set_part_space(int64_t part_space) { part_space_ = part_space; }
  void set_first_part_id(int64_t first_part_id) { first_part_id_ = first_part_id; }
  int64_t get_first_part_id() { return first_part_id_; }
  void set_part_array(int64_t *part_array) { part_array_ = part_array; }

  ObIArray<ObObjType> &get_obj_types() { return obj_types_; }
  int set_obj_types(int64_t pos, const ObObjType obj_type);
  ObIArray<ObCollationType> &get_cs_types() { return cs_types_; }
  int set_cs_types(int64_t pos, const ObCollationType cs_type);

  VIRTUAL_TO_STRING_KV("part_type", "key",
                       K_(part_num),
                       K_(part_space),
                       K_(first_part_id),
                       K_(part_level),
                       "part_func_type", share::schema::get_partition_func_type_str(part_func_type_),
                       K_(obj_types),
                       K_(cs_types));
  virtual int64_t to_plain_string(char* buf, const int64_t buf_len) const;
private:
  int calc_value_for_mysql(const ObObj *objs, int64_t objs_cnt, int64_t &result, ObPartDescCtx &ctx);
  uint64_t calc_hash_value_with_seed(const ObObj &obj, const int64_t cluster_version, int64_t seed);
  int calc_key_part_idx(const uint64_t val, const int64_t part_num, int64_t &partition_idx);
  int get_part_hash_idx(const int64_t part_idx, int64_t &part_id);

private:
  int64_t part_num_;
  int64_t part_space_;
  int64_t first_part_id_;
  // multiple key
  ObSEArray<ObObjType, 4> obj_types_;
  ObSEArray<ObCollationType, 4> cs_types_;
  int64_t *part_array_;
};

} // end of common
} // end of oceanbase

#endif /* _OB_PART_DESC_KEY_H */


