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

#ifndef OCEANBASE_SCHEMA_COLUMN_SCHEMA_H_
#define OCEANBASE_SCHEMA_COLUMN_SCHEMA_H_
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_schema_struct.h"
#include "common/ob_object.h"
#include "common/ob_hint.h"
#include "common/ob_rowkey_info.h"
#include "common/ob_accuracy.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

const char *const STR_COLUMN_TYPE_INT = "int";
const char *const STR_COLUMN_TYPE_UINT64 = "uint64";
const char *const STR_COLUMN_TYPE_FLOAT = "float";
const char *const STR_COLUMN_TYPE_UFLOAT = "ufloat";
const char *const STR_COLUMN_TYPE_DOUBLE = "double";
const char *const STR_COLUMN_TYPE_UDOUBLE = "udouble";
const char *const STR_COLUMN_TYPE_VCHAR = "varchar";
const char *const STR_COLUMN_TYPE_DATETIME = "datetime";
const char *const STR_COLUMN_TYPE_TIMESTAMP = "timestamp";
const char *const STR_COLUMN_TYPE_PRECISE_DATETIME = "precise_datetime";
const char *const STR_COLUMN_TYPE_C_TIME = "create_time";
const char *const STR_COLUMN_TYPE_M_TIME = "modify_time";
const char *const STR_COLUMN_TYPE_BOOLEAN = "bool";
const char *const STR_COLUMN_TYPE_NUMBER = "number";
const char *const STR_COLUMN_TYPE_UNKNOWN = "unknown";

class ObTableSchema;
class ObColumnSchemaV2 : public ObSchema
{
    OB_UNIS_VERSION_V(1);

public:
  static const char *convert_column_type_to_str(common::ColumnType type);
  static common::ColumnType convert_str_to_column_type(const char *str);

  //constructor and destructor
  ObColumnSchemaV2();
  explicit ObColumnSchemaV2(common::ObIAllocator *allocator);
  ObColumnSchemaV2(const ObColumnSchemaV2 &src_schema);
  virtual ~ObColumnSchemaV2();

  //operators
  ObColumnSchemaV2 &operator=(const ObColumnSchemaV2 &src_schema);
  bool operator==(const ObColumnSchemaV2 &r) const;
  bool operator!=(const ObColumnSchemaV2 &r) const;

  //set methods
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_table_id(const uint64_t id) { table_id_ = id; }
  inline void set_column_id(const uint64_t id) { column_id_ = id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_rowkey_position(const int64_t rowkey_position) { rowkey_position_ = rowkey_position; }
  inline void set_index_position(const int64_t index_position) { index_position_ = index_position; }
  inline void set_order_in_rowkey(const ObOrderType order_in_rowkey) { order_in_rowkey_ = order_in_rowkey; }
  inline void set_partition_key_position(const int64_t partition_key_position) { partition_key_position_ = partition_key_position; }
  inline void set_data_type(const common::ColumnType type) { meta_type_.set_type(type); }
  inline void set_meta_type(const common::ObObjMeta type) { meta_type_ = type; }
  inline void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_ = accuracy; }
  inline void set_data_length(const int32_t length) { accuracy_.set_length(length); }
  inline void set_data_precision(const int16_t precision) { accuracy_.set_precision(precision); }
  inline void set_data_scale(const int16_t data_scale) { accuracy_.set_scale(data_scale); }
  inline void set_zero_fill(const bool is_zero_fill) { is_zero_fill_ = is_zero_fill; }
  inline void set_nullable(const bool is_nullable) { is_nullable_ = is_nullable; }
  inline void set_autoincrement(const bool is_autoincrement) { is_autoincrement_ = is_autoincrement; }
  inline void set_is_hidden(const bool is_hidden) { is_hidden_ = is_hidden; }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  // The following two functions is not used for juding if column's type is binary.
  inline void set_binary_collation(const bool is_binary) { is_binary_collation_ = is_binary; }
  inline bool is_binary_collation() const  { return is_binary_collation_; }
  inline void set_collation_type(const common::ObCollationType type) { meta_type_.set_collation_type(type); }
  inline int set_orig_default_value(const common::ObObj default_value) { return deep_copy_obj(default_value, orig_default_value_); }
  inline void set_ori_default_value_colloation(common::ObCollationType collation_type) { orig_default_value_.set_collation_type(collation_type); }
  inline int set_cur_default_value(const common::ObObj default_value) { return deep_copy_obj(default_value, cur_default_value_); }
  inline void set_cur_default_value_colloation(common::ObCollationType collation_type) { cur_default_value_.set_collation_type(collation_type); }
  inline int set_column_name(const char *col_name) { return deep_copy_str(col_name, column_name_); }
  inline int set_column_name(const common::ObString &col_name) { return deep_copy_str(col_name, column_name_); }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline void set_on_update_current_timestamp(const bool is_on_update_for_timestamp) { on_update_current_timestamp_ = is_on_update_for_timestamp; }

  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline uint64_t get_column_id() const { return column_id_; }
  inline uint64_t& get_column_id() { return column_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline int64_t get_rowkey_position() const { return rowkey_position_; }
  inline int64_t get_index_position() const { return index_position_; }
  inline ObOrderType get_order_in_rowkey() const { return order_in_rowkey_; }
  inline int64_t get_partition_key_position() const { return partition_key_position_; }
  inline common::ColumnType get_data_type() const { return meta_type_.get_type(); }
  inline common::ColumnTypeClass get_data_type_class() const { return meta_type_.get_type_class(); }
  inline common::ObObjMeta get_meta_type() const { return meta_type_; }
  inline const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  inline int32_t get_data_length() const;
  inline int16_t get_data_precision() const { return accuracy_.get_precision(); }
  inline int16_t get_data_scale() const { return accuracy_.get_scale(); }
  inline bool is_nullable() const { return is_nullable_; }
  inline bool is_zero_fill() const { return is_zero_fill_; }
  inline bool is_autoincrement() const { return is_autoincrement_; }
  inline bool is_hidden() const { return is_hidden_; }
  inline bool is_string_type() const { return meta_type_.is_string_type(); }
  inline common::ObCharsetType get_charset_type() const { return charset_type_;}
  inline common::ObCollationType get_collation_type() const { return meta_type_.get_collation_type();}
  inline const common::ObObj &get_orig_default_value()  const { return orig_default_value_; }
  inline const common::ObObj &get_cur_default_value()  const { return cur_default_value_; }
  inline const char *get_column_name() const { return extract_str(column_name_); }
  inline const common::ObString &get_column_name_str() const { return column_name_; }
  inline const char *get_comment() const {return extract_str(comment_);}
  inline const common::ObString &get_comment_str() const { return comment_; }

  inline bool is_rowkey_column() const { return rowkey_position_ > 0; }
  inline bool is_index_column() const { return index_position_ > 0; }
  inline bool is_partition_key_column() const { return partition_key_position_ > 0; }
  inline bool is_shadow_column() const { return column_id_ > common::OB_MIN_SHADOW_COLUMN_ID; }
  inline bool is_on_update_current_timestamp() const { return on_update_current_timestamp_; }

  inline static bool is_hidden_pk_column_id(const uint64_t column_id);

  //other methods
  int64_t get_convert_size(void) const;
  void reset();
  void print_info() const;
  void print(FILE *fd) const;
  int get_byte_length(int64_t &length) const;

  DECLARE_VIRTUAL_TO_STRING;

private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t schema_version_;
  int64_t rowkey_position_;  // greater than zero if this is rowkey column
  int64_t index_position_;  // greater than zero if this is index column
  ObOrderType order_in_rowkey_; // asc or desc; if this column is index column this means order in index
  int64_t partition_key_position_; //greater than zero if this column is used to calc part expr
  common::ObObjMeta meta_type_;
  common::ObAccuracy accuracy_;
  bool is_nullable_;
  bool is_zero_fill_;
  bool is_autoincrement_;
  bool is_hidden_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  bool is_binary_collation_;
  bool on_update_current_timestamp_;
  //default value
  common::ObObj orig_default_value_;//first default value, used for alter table add column; collation must be same with the column
  common::ObObj cur_default_value_; //collation must be same with the column
  common::ObString column_name_;
  common::ObString comment_;
};

inline int32_t ObColumnSchemaV2::get_data_length() const
{
  return (common::ObStringTC == meta_type_.get_type_class()?
    accuracy_.get_length() : ob_obj_type_size( meta_type_.get_type()));
}

inline bool ObColumnSchemaV2::is_hidden_pk_column_id(const uint64_t column_id)
{
  return common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id ||
      common::OB_HIDDEN_PK_PARTITION_COLUMN_ID == column_id ||
      common::OB_HIDDEN_PK_CLUSTER_COLUMN_ID == column_id;
}

inline static bool column_schema_compare(const ObColumnSchemaV2 &lhs, const ObColumnSchemaV2 &rhs)
{
  bool ret = false;
  if ((lhs.get_table_id() < rhs.get_table_id())
      || (lhs.get_table_id() == rhs.get_table_id() && lhs.get_column_id() < rhs.get_column_id())) {
    ret = true;
  }
  return ret;
}

struct ObColumnSchemaV2Compare
{
  inline bool operator()(const ObColumnSchemaV2 &lhs, const ObColumnSchemaV2 &rhs);
};

inline bool ObColumnSchemaV2Compare::operator()(const ObColumnSchemaV2 &lhs,
                                                const ObColumnSchemaV2 &rhs)
{
  return column_schema_compare(lhs, rhs);
}

struct ObColumnSchemaIndexCompare
{
  ObColumnSchemaIndexCompare(const ObColumnSchemaV2 *columns)
      : columns_(columns)
  {}
  inline bool operator()(const int64_t &lhs, const int64_t &rhs);
  const ObColumnSchemaV2 *columns_;
};

inline bool ObColumnSchemaIndexCompare::operator()(const int64_t &lhs, const int64_t &rhs)
{
  return column_schema_compare(columns_[lhs], columns_[rhs]);
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OCEANBASE_SCHEMA_COLUMN_SCHEMA_H_
