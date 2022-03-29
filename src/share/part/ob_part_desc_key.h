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
                       ObPartDescCtx &ctx);
  virtual int get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids);
  void set_part_num(int64_t part_num) { part_num_ = part_num; }
  void set_part_space(int64_t part_space) { part_space_ = part_space; }
  void set_first_part_id(int64_t first_part_id) { first_part_id_ = first_part_id; }
  int64_t get_first_part_id() { return first_part_id_; }
  void set_part_key_type(const ObObjType obj_type) { obj_type_ = obj_type; }
  void set_part_key_cs_type(const ObCollationType cs_type) { cs_type_ = cs_type; }
  void set_part_array(int64_t *part_array) { part_array_ = part_array; }

  VIRTUAL_TO_STRING_KV("part_type", "key",
                       K_(part_num),
                       K_(part_space),
                       K_(first_part_id),
                       K_(part_level),
                       K_(part_func_type));
private:
  int64_t part_num_;
  int64_t part_space_;
  int64_t first_part_id_;
  // TODO: add multiple key
  ObObjType obj_type_;
  ObCollationType cs_type_;
  int64_t *part_array_;
};

} // end of common
} // end of oceanbase

#endif /* _OB_PART_DESC_KEY_H */


