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

#ifndef OCEANBASE_SCHEMA_TABLE_SCHEMA
#define OCEANBASE_SCHEMA_TABLE_SCHEMA

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/objectpool/ob_pool.h"
#include "common/ob_row.h"
#include "common/ob_rowkey_info.h"
#include "common/ob_object.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_struct.h"
namespace oceanbase
{
namespace share
{
namespace schema
{

struct ObColDesc
{
  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    (void)common::databuff_printf(buffer, length, pos, "column_id=%lu ", col_id_);
    pos += col_type_.to_string(buffer + pos, length - pos);
    return pos;
  }
  void reset()
  {
    col_id_ = common::OB_INVALID_ID;
    col_type_.reset();
  }
  uint64_t col_id_;
  common::ObObjMeta col_type_;
};

struct ObTableStatus
{
public:
  bool is_exist_;
  bool is_version_correct_;
  bool is_range_complete_;
  bool is_copy_enough_;
  bool is_all_available_;
  bool is_row_count_equal_;
  bool is_table_available_;
public:
  ObTableStatus()
  : is_exist_(false),
  is_version_correct_(true),
  is_range_complete_(true),
  is_copy_enough_(true),
  is_all_available_(true),
  is_row_count_equal_(false),
  is_table_available_(false)
  {}
  virtual  ~ObTableStatus() {}
  void reset();
};

struct ObColumnIdKey
{
  uint64_t column_id_;

  explicit ObColumnIdKey() : column_id_(common::OB_INVALID_ID) {}
  explicit ObColumnIdKey(const uint64_t column_id) : column_id_(column_id) {}

  ObColumnIdKey &operator=(const uint64_t column_id)
  {
    column_id_ = column_id;
    return *this;
  }

  inline operator uint64_t() const { return column_id_; }

  inline uint64_t hash() const { return ((column_id_ * 29 + 7) & 0xFFFF); }
};

template<class K, class V>
struct ObGetColumnKey
{
  void operator()(const K &k, const V &v)
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template<>
struct ObGetColumnKey<ObColumnIdKey, ObColumnSchemaV2 *>
{
  ObColumnIdKey operator()(const ObColumnSchemaV2 *column_schema) const
  {
    return ObColumnIdKey(column_schema->get_column_id());
  }
};

template<>
struct ObGetColumnKey<ObColumnSchemaHashWrapper, ObColumnSchemaV2 *>
{
  ObColumnSchemaHashWrapper operator()(const ObColumnSchemaV2 *column_schema) const
  {
    return ObColumnSchemaHashWrapper(column_schema->get_column_name_str());
  }
};



typedef common::hash::ObPointerHashArray<ObColumnIdKey, ObColumnSchemaV2 *, ObGetColumnKey>
IdHashArray;
typedef common::hash::ObPointerHashArray<ObColumnSchemaHashWrapper, ObColumnSchemaV2 *, ObGetColumnKey>
NameHashArray;
typedef const common::ObObj& (ObColumnSchemaV2::*get_default_value)() const;

extern const uint64_t HIDDEN_PK_COLUMN_IDS[3];
extern const char* HIDDEN_PK_COLUMN_NAMES[3];

class ObTableSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  bool cmp_table_id(const ObTableSchema *a, const ObTableSchema *b)
  {
    return a->get_tenant_id() < b->get_tenant_id() ||
        a->get_database_id() < b->get_database_id() ||
        a->get_table_id() < b->get_table_id();
  }
public:
  typedef ObColumnSchemaV2* const *const_column_iterator;
  ObTableSchema();
  explicit ObTableSchema(common::ObIAllocator *allocator);
  ObTableSchema(const ObTableSchema &src_schema);
  virtual ~ObTableSchema();
  ObTableSchema &operator=(const ObTableSchema &src_schema);

  void init_as_sys_table();
  //set methods
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_tablegroup_id(const uint64_t id) { tablegroup_id_ = id; }
  inline void set_table_id(uint64_t id) { table_id_ = id; }
  inline void set_max_used_column_id(const uint64_t id)  { max_used_column_id_ = id; }
  inline void set_first_timestamp_index(const uint64_t id)  { first_timestamp_index_ = id; }
  inline void set_rowkey_column_num(const int64_t rowkey_column_num) { rowkey_column_num_ = rowkey_column_num; }
  inline void set_index_column_num(const int64_t index_column_num) { index_column_num_ = index_column_num; }
  inline void set_rowkey_split_pos(int64_t pos) { rowkey_split_pos_ = pos;}
  inline void set_partition_key_column_num(const int64_t partition_key_column_num) { partition_key_column_num_ = partition_key_column_num; }
  inline void set_progressive_merge_num(const int64_t progressive_merge_num) { progressive_merge_num_ = progressive_merge_num; }
  inline void set_replica_num(const int64_t replica_num) { replica_num_ = replica_num; }
  inline void set_autoinc_column_id(const int64_t autoinc_column_id) { autoinc_column_id_ = autoinc_column_id; }
  inline void set_auto_increment(const uint64_t auto_increment) { auto_increment_ = auto_increment; }
  inline void set_load_type(ObTableLoadType load_type) { load_type_ = load_type;}
  inline void set_def_type(ObTableDefType type) { def_type_ = type; }
  inline void set_part_level(ObPartitionLevel level) { part_level_ = level; }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_collation_type(const common::ObCollationType type) { collation_type_ = type; }
  inline void set_create_mem_version(const int64_t version) { create_mem_version_ = version; }
  inline void set_code_version(const int64_t code_version) {code_version_ = code_version; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_last_modified_frozen_version(const int64_t frozen_version) { last_modified_frozen_version_ = frozen_version; }
  inline void set_table_type(const ObTableType table_type) { table_type_ = table_type; }
  inline void set_index_type(const ObIndexType index_type) { index_type_ = index_type; }
  inline void set_index_using_type(const ObIndexUsingType index_using_type) { index_using_type_ = index_using_type; }
  inline void set_index_status(const ObIndexStatus index_status) { index_status_ = index_status; }
  inline void set_name_case_mode(const common::ObNameCaseMode cmp_mode) { name_case_mode_ = cmp_mode; }
  inline void set_data_table_id(const uint64_t data_table_id) { data_table_id_ = data_table_id; }
  inline void set_max_column_id(const uint64_t id) { max_used_column_id_ = id; }
  inline void set_is_use_bloomfilter(const bool is_use_bloomfilter) { is_use_bloomfilter_ = is_use_bloomfilter; }
  inline void set_block_size(const int64_t block_size) { block_size_ = block_size; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  int set_tablegroup_name(const char *tablegroup_name) { return deep_copy_str(tablegroup_name, tablegroup_name_); }
  int set_tablegroup_name(const common::ObString &tablegroup_name) { return deep_copy_str(tablegroup_name, tablegroup_name_); }
  int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  int set_table_name(const char *name) { return deep_copy_str(name, table_name_); }
  int set_table_name(const common::ObString &name) { return deep_copy_str(name, table_name_); }
  int set_expire_info(const common::ObString &expire_info) { return deep_copy_str(expire_info, expire_info_); }
  int set_compress_func_name(const char *compressor);
  int set_compress_func_name(const common::ObString &compressor);
  int add_column(const ObColumnSchemaV2 &column);
  int delete_column(const uint64_t column_id);
  int delete_column(const char *column_name);
  int delete_column(const common::ObString &column_name);
  int alter_column(const ObColumnSchemaV2 &column);
  int add_index_tid(const uint64_t index_tid);
  int add_partition_key(const common::ObString &column_name);
  int set_zone_list(const common::ObIArray<common::ObString> &zone_list);
  int set_primary_zone(const common::ObString &zone);
  int add_zone(const common::ObString &zone);
  int set_view_definition(const common::ObString &view_definition);

//get methods
  bool is_valid() const;
  int check_column_is_partition_key(const uint64_t column_id, bool &is_partition_key) const;
  int get_index_tid_array(uint64_t *index_tid_array, int64_t &size) const;
  const ObColumnSchemaV2 *get_column_schema(const uint64_t column_id) const;
  const ObColumnSchemaV2 *get_column_schema(const char *column_name) const;
  const ObColumnSchemaV2 *get_column_schema(const common::ObString &column_name) const;
  const ObColumnSchemaV2 *get_column_schema_by_idx(const int64_t idx) const;
  ObColumnSchemaV2 *get_column_schema(const uint64_t column_id);
  ObColumnSchemaV2 *get_column_schema(const char *column_name);
  ObColumnSchemaV2 *get_column_schema(const common::ObString &column_name);
  ObColumnSchemaV2 *get_column_schema_by_idx(const int64_t idx);
  int64_t get_column_idx(const uint64_t column_id, const bool ignore_hidden_column = false) const;
  inline ObTenantTableId get_tenant_table_id() const {return ObTenantTableId(tenant_id_, table_id_);}
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_tablegroup_id() const { return tablegroup_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline int64_t get_index_tid_count() const { return index_cnt_; }
  inline int64_t get_index_column_number() const { return index_column_num_; }
  inline uint64_t get_max_used_column_id() const { return max_used_column_id_; }
  inline int64_t get_rowkey_split_pos() const { return rowkey_split_pos_; }
  inline int64_t get_block_size() const { return block_size_;}
  inline bool is_use_bloomfilter()   const { return is_use_bloomfilter_; }
  inline int64_t get_progressive_merge_num() const { return progressive_merge_num_; }
  inline uint64_t get_autoinc_column_id() const { return autoinc_column_id_; }
  inline uint64_t get_auto_increment() const { return auto_increment_; }
  inline int64_t get_replica_num() const { return replica_num_; }
  inline int64_t get_rowkey_column_num() const { return rowkey_info_.get_size(); }
  inline int64_t get_shadow_rowkey_column_num() const { return shadow_rowkey_info_.get_size(); }
  inline int64_t get_index_column_num() const { return index_info_.get_size(); }
  inline int64_t get_partition_key_column_num() const { return partition_key_info_.get_size(); }
  inline int64_t is_partitioned_table() const { return 0 != get_partition_key_column_num(); }
  inline ObTableLoadType get_load_type() const { return load_type_; }
  inline ObTableType get_table_type() const { return table_type_; }
  inline ObIndexType get_index_type() const { return index_type_; }
  inline ObIndexUsingType get_index_using_type() const { return index_using_type_; }
  inline ObTableDefType get_def_type() const { return def_type_; }
  inline ObPartitionLevel get_part_level() const { return part_level_; }
  int64_t get_part_num() const;
  inline const char *get_table_name() const { return extract_str(table_name_); }
  inline const common::ObString &get_table_name_str() const { return table_name_; }
  inline const char *get_compress_func_name() const { return extract_str(compress_func_name_); }
  inline bool is_compressed() const { return !compress_func_name_.empty() && compress_func_name_ != "none"; }
  inline const char *get_tablegroup_name_str() const { return extract_str(tablegroup_name_); }
  inline const common::ObString &get_tablegroup_name() const { return tablegroup_name_; }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline const common::ObRowkeyInfo &get_rowkey_info() const { return rowkey_info_; }
  inline const common::ObRowkeyInfo &get_shadow_rowkey_info() const { return shadow_rowkey_info_; }
  inline const common::ObIndexInfo &get_index_info() const { return index_info_; }
  inline const common::ObPartitionKeyInfo &get_partition_key_info() const { return partition_key_info_; }
  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return collation_type_; }
  inline uint64_t get_data_table_id() const { return data_table_id_; }
  inline int64_t get_create_mem_version() const { return create_mem_version_; }
  inline ObIndexStatus get_index_status() const { return index_status_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline int64_t get_code_version() const { return code_version_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline int64_t get_last_modified_frozen_version() const { return last_modified_frozen_version_; }
  inline const common::ObString &get_expire_info() const { return expire_info_; }
  inline const ObPartitionOption &get_part_expr() const { return part_expr_; }
  inline ObPartitionOption &get_part_expr() { return part_expr_; }
  inline const ObPartitionOption &get_sub_part_expr() const { return sub_part_expr_; }
  inline ObPartitionOption &get_sub_part_expr() { return sub_part_expr_; }
  inline ObViewSchema &get_view_schema() { return view_schema_; }
  inline const ObViewSchema &get_view_schema() const { return view_schema_; }
  inline const common::ObIArray<common::ObString> &get_zone_list() const { return zone_list_; }
  inline const common::ObString &get_primary_zone() const { return primary_zone_; }
  inline bool is_read_only() const { return read_only_; }

  inline bool is_user_table() const { return USER_TABLE == table_type_; }
  inline bool is_user_view() const { return USER_VIEW == table_type_; }
  inline bool is_sys_table() const { return SYSTEM_TABLE == table_type_; }
  inline bool is_sys_view() const { return SYSTEM_VIEW == table_type_; }
  inline bool is_vir_table() const { return VIRTUAL_TABLE == table_type_; }
  inline bool is_view_table() const { return USER_VIEW == table_type_ || SYSTEM_VIEW == table_type_; }
  inline bool is_tmp_table() const { return TMP_TABLE == table_type_; }
  inline bool is_index_table()  const { return USER_INDEX == table_type_; }
  inline bool is_local_index_table() const;
  inline bool is_normal_index() const;
  inline bool is_unique_index() const;
  inline bool can_read_index() const { return INDEX_STATUS_AVAILABLE == index_status_; }
  //TODO: rename to has_partition_for_store
  inline bool has_partition() const;
  bool has_hidden_primary_key() const;
  int get_orig_default_row(common::ObNewRow &default_row) const;
  int get_cur_default_row(common::ObNewRow &default_row) const;
  inline int64_t get_column_count() const { return column_cnt_; }
  inline const_column_iterator column_begin() const { return column_array_; }
  inline const_column_iterator column_end() const { return NULL == column_array_ ? NULL : &(column_array_[column_cnt_]); }
  int fill_column_collation_info();
  int get_column_ids(common::ObIArray<ObColDesc> &column_ids) const;

  template <typename Allocator>
  static int build_index_table_name(Allocator &allocator,
                                    const uint64_t data_table_id,
                                    const common::ObString &index_name,
                                    common::ObString &index_table_name);

  template <typename Allocator>
  static int get_index_name(Allocator &allocator, uint64_t table_id,
      const common::ObString &src, common::ObString &dst);

  // only index table schema can invoke this function
  int get_index_name(common::ObString &index_name) const;
  //other methods
  int64_t get_convert_size() const;
  void reset_index_tid_array();
  void reset();
  //int64_t to_string(char *buf, const int64_t buf_len) const;
  //this is for test
  void print_info(bool verbose = false) const;
  void print(FILE *fd) const;
  //whether the primary key or index is ordered
  inline bool is_ordered() const { return USING_BTREE == index_using_type_; }
  virtual int serialize_columns(char *buf, const int64_t data_len, int64_t &pos) const;
  virtual int deserialize_columns(const char *buf, const int64_t data_len, int64_t &pos);
  int check_primary_key_cover_partition_column();
  DECLARE_VIRTUAL_TO_STRING;

protected:
  int add_col_to_id_hash_array(ObColumnSchemaV2 *column);
  int remove_col_from_id_hash_array(const ObColumnSchemaV2 *column);
  int add_col_to_name_hash_array(ObColumnSchemaV2 *column);
  int remove_col_from_name_hash_array(const ObColumnSchemaV2 *column);
  int add_col_to_column_array(ObColumnSchemaV2 *column);
  int remove_col_from_column_array(const ObColumnSchemaV2 *column);
  int64_t column_cnt_;

private:
  int get_default_row(common::ObNewRow &default_row, get_default_value func) const;
  inline int64_t get_id_hash_array_mem_size(const int64_t column_cnt) const;
  inline int64_t get_name_hash_array_mem_size(const int64_t column_cnt) const;
  int delete_column_internal(ObColumnSchemaV2 *column_schema);
  ObColumnSchemaV2 *get_column_schema_by_id_internal(const uint64_t column_id) const;
  ObColumnSchemaV2 *get_column_schema_by_name_internal(const common::ObString &column_name) const;
  int check_column_can_be_altered(const ObColumnSchemaV2 *src_schema,
                                  const ObColumnSchemaV2 *dst_schema);
  int check_rowkey_column_can_be_altered(const ObColumnSchemaV2 *src_schema,
                                         const ObColumnSchemaV2 *dst_schema);

private:
  static const int64_t DEFAULT_COLUMN_ARRAY_CAPACITY = 16;
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  uint64_t max_used_column_id_;
  int64_t rowkey_column_num_;
  int64_t index_column_num_;
  int64_t rowkey_split_pos_;//TODO: not used so far;reserved
  int64_t partition_key_column_num_;
  int64_t block_size_; //KB
  bool is_use_bloomfilter_; // not used. all table will use bloomfilter
  int64_t progressive_merge_num_;
  int64_t replica_num_;
  uint64_t autoinc_column_id_;
  uint64_t auto_increment_;
  bool read_only_;
  ObTableLoadType load_type_; // not used yet
  ObTableType table_type_;
  ObIndexType index_type_;
  ObIndexUsingType index_using_type_;
  ObTableDefType def_type_;
  ObIndexStatus index_status_;
  common::ObNameCaseMode name_case_mode_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;//default:utf8mb4_general_ci
  uint64_t data_table_id_;
  int64_t create_mem_version_;
  int64_t code_version_;//for compatible use, the version of the whole schema system
  int64_t schema_version_; //the last modify timestamp of this version
  int64_t last_modified_frozen_version_;  //the frozen version when last modified
  int64_t first_timestamp_index_;
  //partition info
  ObPartitionLevel part_level_;
  //first partition meta
  ObPartitionOption part_expr_;
  //secord partition meta
  ObPartitionOption sub_part_expr_;

  common::ObString tablegroup_name_;
  common::ObString comment_;
  common::ObString table_name_;
  common::ObString compress_func_name_;
  common::ObString expire_info_;

  //view schema
  ObViewSchema view_schema_;

  common::ObArrayHelper<common::ObString> zone_list_;
  common::ObString primary_zone_;

  int64_t index_cnt_;
  uint64_t index_tid_array_[common::OB_MAX_INDEX_PER_TABLE];

  ObColumnSchemaV2 **column_array_;

  int64_t column_array_capacity_;
  //generated data
  common::ObRowkeyInfo rowkey_info_;
  common::ObRowkeyInfo shadow_rowkey_info_;
  common::ObIndexInfo index_info_;
  common::ObPartitionKeyInfo partition_key_info_;
  IdHashArray *id_hash_array_;
  NameHashArray *name_hash_array_;
};

inline bool ObTableSchema::is_local_index_table() const
{
  return INDEX_TYPE_NORMAL_LOCAL == index_type_ || INDEX_TYPE_UNIQUE_LOCAL == index_type_;
}

inline bool ObTableSchema::is_normal_index() const
{
  return INDEX_TYPE_NORMAL_LOCAL == index_type_ || INDEX_TYPE_NORMAL_GLOBAL == index_type_;
}

inline bool ObTableSchema::is_unique_index() const
{
  return INDEX_TYPE_UNIQUE_LOCAL == index_type_ || INDEX_TYPE_UNIQUE_GLOBAL == index_type_;
}

// only effect storage when creating partition for 'real table'
inline bool ObTableSchema::has_partition() const
{
  return !(is_vir_table() || is_view_table() || is_index_table());
}

inline int64_t ObTableSchema::get_id_hash_array_mem_size(const int64_t column_cnt) const
{
  return common::max(IdHashArray::MIN_HASH_ARRAY_ITEM_COUNT,
    column_cnt * 2) * sizeof(void*) + sizeof(IdHashArray);
}

inline int64_t ObTableSchema::get_name_hash_array_mem_size(const int64_t column_cnt) const
{
  return common::max(NameHashArray::MIN_HASH_ARRAY_ITEM_COUNT,
    column_cnt * 2) * sizeof(void *) + sizeof(NameHashArray);
}

template <typename Allocator>
int ObTableSchema::build_index_table_name(Allocator &allocator,
                                          const uint64_t data_table_id,
                                          const common::ObString &index_name,
                                          common::ObString &index_table_name)
{
  int ret = common::OB_SUCCESS;
  int nwrite = 0;
  const int64_t buf_size = 64;
  char buf[buf_size];
  if ((nwrite = snprintf(buf, buf_size, "%lu", data_table_id)) >= buf_size || nwrite < 0) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SHARE_SCHEMA_LOG(WDIAG, "buf is not large enough", K(buf_size), K(data_table_id), K(ret));
  } else {
    common::ObString table_id_str = common::ObString::make_string(buf);
    int32_t src_len = table_id_str.length() + index_name.length()
        + static_cast<int32_t>(strlen(common::OB_INDEX_PREFIX)) + 1;
    char *ptr = NULL;
    //TODO: refactor following code, use snprintf instead
    if (OB_UNLIKELY(0 >= src_len)) {
      index_table_name.assign(NULL, 0);
    } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(src_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WDIAG, "alloc memory failed", K(ret), "size", src_len);
    } else {
      int64_t pos = 0;
      MEMCPY(ptr + pos, common::OB_INDEX_PREFIX, strlen(common::OB_INDEX_PREFIX));
      pos += strlen(common::OB_INDEX_PREFIX);
      MEMCPY(ptr + pos, table_id_str.ptr(), table_id_str.length());
      pos += table_id_str.length();
      MEMCPY(ptr + pos, "_", 1);
      pos += 1;
      MEMCPY(ptr + pos, index_name.ptr(), index_name.length());
      pos += index_name.length();
      if (pos == src_len) {
        index_table_name.assign_ptr(ptr, src_len);
      } else {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(EDIAG, "length mismatch", K(ret));
      }
    }
  }

  return ret;
}

template <typename Allocator>
int ObTableSchema::get_index_name(Allocator &allocator, uint64_t table_id,
    const common::ObString &src, common::ObString &dst)
{
  int ret = common::OB_SUCCESS;
  common::ObString::obstr_size_t dst_len = 0;
  char *ptr = NULL;
  common::ObString::obstr_size_t pos = 0;
  const int64_t BUF_SIZE = 64; //table_id max length
  char table_id_buf[BUF_SIZE] = {'\0'};
  if (common::OB_INVALID_ID == table_id || src.empty()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WDIAG, "invalid argument", K(ret), K(table_id), K(src));
  } else {
    int64_t n = snprintf(table_id_buf, BUF_SIZE, "%lu", table_id);
    if (n < 0 || n >= BUF_SIZE) {
      ret = common::OB_BUF_NOT_ENOUGH;
      SHARE_SCHEMA_LOG(WDIAG, "buffer not enough", K(ret), K(n), LITERAL_K(BUF_SIZE));
    } else {
      common::ObString table_id_str = common::ObString::make_string(table_id_buf);
      pos += static_cast<int32_t>(strlen(common::OB_INDEX_PREFIX));
      pos += table_id_str.length();
      pos += 1;
      dst_len = src.length() - pos;
      if (OB_UNLIKELY(0 >= dst_len)) {
        dst.assign(NULL, 0);
      } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(dst_len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SHARE_SCHEMA_LOG(WDIAG, "alloc memory failed", K(ret), "size", dst_len);
      } else {
        MEMCPY(ptr, src.ptr() + pos, dst_len);
        dst.assign_ptr(ptr, dst_len);
      }
    }
  }
  return ret;
}


}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase


#endif //OCEANBASE_SCHEMA_TABLE_SCHEMA
