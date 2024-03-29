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

#ifndef _OB_MYSQL_FIELD_H_
#define _OB_MYSQL_FIELD_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "common/ob_accuracy.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace obmysql
{

struct TypeInfo {
  TypeInfo() : relation_name_(), type_name_(), version_(0), is_elem_type_(false) {} ;
  common::ObString relation_name_;
  common::ObString type_name_;
  uint64_t version_;
  ObObjType elem_type_;
  bool is_elem_type_;
  TO_STRING_KV(K_(relation_name), K_(type_name), K_(version), K_(elem_type), K_(is_elem_type));
};

class ObMySQLField
{
public:
  ObMySQLField();
  /**
   * serialize data to the format recognized by MySQL
   *
   * @param [in] buf data after serializing
   * @param [in] len buf size 
   * @param [in,out] pos input is valid offset of buf, output is valid offset of buf after serialization.
   *
   * @return return oceanbase error code.
   */
  int serialize(char *buf, const int64_t len, int64_t &pos) const
  {
    return serialize_pro41(buf, len, pos);
  }

  void set_charset_number(uint16_t number)
  {
    charsetnr_ = number;
  }
  int64_t to_string(char *buffer, int64_t len) const;


  static int my_decimal_precision_to_length_no_truncation(
      int32_t &ans,
      int16_t precision,
      int16_t scale,
      bool unsigned_flag)
  {
    int ret = OB_SUCCESS;
    /*
     * When precision is 0 it means that original length was also 0. Thus
     * unsigned_flag is ignored in this case.
     **/
    if (precision || !scale) {
      ans = (int32_t)(precision + (scale > 0 ? 1 : 0) +
                     (unsigned_flag || !precision ? 0 : 1));
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }
private:
  /**
   * serialize data to the format recognized by MySQL(version 4.1)
   *
   * @param [in] buf data after serializing
   * @param [in] len buf size
   * @param [in,out] pos input is valid offset of buf, output is valid offset of buf after serialization.
   *
   * @return return oceanbase error code.
   */
  int serialize_pro41(char *buf, const int64_t len, int64_t &pos) const;

private:
  const char *catalog_;     /* Catalog for table */
  // void *extension;

public:
  common::ObString dname_;
  common::ObString tname_; // table name for display
  common::ObString org_tname_; // original table name
  common::ObString cname_;     // column name for display
  common::ObString org_cname_; // original column name
  common::ObAccuracy accuracy_;
  EMySQLFieldType type_;      // value type
  TypeInfo type_info_;         // for complex type
  uint16_t flags_;            // unsigned and so on...
  EMySQLFieldType default_value_; //default value, only effective when command was OB_MYSQL_COM_FIELD_LIST
  uint16_t charsetnr_;    //character set of table
  int32_t length_;
}; // end class ObMySQLField

inline int64_t ObMySQLField::to_string(char *buffer, int64_t len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buffer)) {
  } else {
    common::databuff_printf(buffer, len, pos,
        "dname: %.*s, tname: %.*s, org_tname: %.*s, "
        "cname: %.*s, org_cname, %.*s, type: %d, charset: %hu, decimal_scale: %hhu, flag: %x",
        dname_.length(), dname_.ptr(), tname_.length(), tname_.ptr(), org_tname_.length(), org_tname_.ptr(),
        cname_.length(), cname_.ptr(), org_cname_.length(), org_cname_.ptr(),
        type_, charsetnr_, accuracy_.get_scale(), flags_);
  }
  return pos;
}

} // namespace obmysql
} // namespace oceanbase


#endif /* _OB_MYSQL_FIELD_H_ */
