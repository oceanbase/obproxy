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

#ifndef OCEANBASE_COMMON_FIELD_
#define OCEANBASE_COMMON_FIELD_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/ob_object.h"
#include "common/ob_accuracy.h"

namespace oceanbase
{
namespace common
{
struct ObField
{
  ObString dname_; //database name for display
  ObString tname_; // table name for display
  ObString org_tname_; // original table name
  ObString cname_;     // column name for display
  ObString org_cname_; // original column name
  ObObj type_;      // value type
  ObObj default_value_; //default value, only effective when command was OB_MYSQL_COM_FIELD_LIST
  ObAccuracy accuracy_;
  uint16_t charsetnr_;    //collation of table, this is to correspond with charset.
  uint16_t flags_; // binary, not_null flag etc
  int32_t length_;//in bytes not characters. used for client

  ObField()
      : dname_(), tname_(), org_tname_(), cname_(), org_cname_(), type_(),
        default_value_(ObObj::make_nop_obj()), accuracy_(),
        charsetnr_(CS_TYPE_UTF8MB4_GENERAL_CI),
        flags_(0), length_(0)
  {
  }

  int64_t to_string(char *buffer, int64_t length) const;
  int deep_copy(const ObField &other, ObIAllocator *allocator);
  static int get_field_mb_length(const ObObjType type,
                                 const ObAccuracy &accuracy,
                                 const ObCollationType charsetnr,
                                 int32_t &length);

private:
  static int32_t my_decimal_precision_to_length_no_truncation(int16_t precision,
                                                              int16_t scale,
                                                              bool unsigned_flag)
  {
    /*
     * When precision is 0 it means that original length was also 0. Thus
     * unsigned_flag is ignored in this case.
     */
    return (int32_t)(precision + (scale > 0 ? 1 : 0) +
                     ((unsigned_flag || !precision) ? 0 : 1));
  }

};
}
}
#endif
