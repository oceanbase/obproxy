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

#ifndef _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_array_helper.h"
#include "common/ob_object.h"
#include "common/ob_zone.h"
#include "share/schema/ob_priv_type.h"
#include "share/part/ob_part_mgr_util.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

#define ASSIGN_STRING(dst, src, field, buffer, skip)\
  (dst)->field.assign(buffer + offset, (src)->field.length());\
  offset += (src)->field.length() + (skip);

#define ASSIGN_CONST_STRING(dst, src, field, buffer, skip)\
  (const_cast<ObString &>((dst)->field)).assign(buffer + offset, (src)->field.length());\
  offset += (src)->field.length() + (skip);

//just match the format: DEFAULT NOW() or DEFAULT CURRENT_TIMESTAMP()
#define IS_DEFAULT_NOW_STR(data_type, def_str) \
  ({ \
    bool bret = false; \
    if (ObDateTimeType == data_type || ObTimestampType == data_type) \
    { \
      if (def_str == N_CURRENT_TIMESTAMP) \
      { \
        bret = true; \
      } \
    } \
    bret; \
  })

#define IS_DEFAULT_NOW_OBJ(def_obj) \
  ((ObExtendType == def_obj.get_type()) && (ObActionFlag::OP_DEFAULT_NOW_FLAG == def_obj.get_ext()))

static const uint64_t OB_MIN_ID  = 0;//used for lower_bound
//-------enum defenition
enum ObOrderType
{
  ASC = 0,
  DESC = 1,
};

enum ObTableLoadType
{
  TABLE_LOAD_TYPE_IN_DISK = 0,
  TABLE_LOAD_TYPE_IN_RAM = 1,
  TABLE_LOAD_TYPE_MAX = 2,
};
//the defination type of table
enum ObTableDefType
{
  TABLE_DEF_TYPE_INTERNAL = 0,
  TABLE_DEF_TYPE_USER = 1,
  TABLE_DEF_TYPE_MAX = 2,
};
enum ObTableType
{
  SYSTEM_TABLE = 0,
  SYSTEM_VIEW,
  VIRTUAL_TABLE,
  USER_TABLE,
  USER_VIEW,
  USER_INDEX, // urgly, compatible with uniform process in ddl_service
              // will add index for sys table???
  TMP_TABLE,
  MAX_TABLE_TYPE
};

//ObTableType=>const char* ; used for show tables
const char *ob_table_type_str(ObTableType type);
const char *ob_mysql_table_type_str(ObTableType type);

ObTableType get_inner_table_type_by_id(const uint64_t tid);

enum ObIndexType
{
  INDEX_TYPE_IS_NOT = 0,//is not index table
  INDEX_TYPE_NORMAL_LOCAL = 1,
  INDEX_TYPE_UNIQUE_LOCAL = 2,
  INDEX_TYPE_NORMAL_GLOBAL = 3,
  INDEX_TYPE_UNIQUE_GLOBAL = 4,
  INDEX_TYPE_PRIMARY = 5,
  INDEX_TYPE_MAX = 6,
};

// using type for index
enum ObIndexUsingType
{
  USING_BTREE = 0,
  USING_HASH,
  USING_TYPE_MAX,
};

enum ViewCheckOption
{
  VIEW_CHECK_OPTION_NONE = 0,
  VIEW_CHECK_OPTION_LOCAL = 1,
  VIEW_CHECK_OPTION_CASCADED = 2,
  VIEW_CHECK_OPTION_MAX = 3,
};

const char *ob_view_check_option_str(const ViewCheckOption option);
enum ObIndexStatus
{
  //this is used in index virtual table:__index_process_info:means the table may be deleted when you get it
  INDEX_STATUS_NOT_FOUND = 0,
  INDEX_STATUS_UNAVAILABLE = 1,
  INDEX_STATUS_AVAILABLE = 2,
  INDEX_STATUS_UNIQUE_CHECKING = 3,
  INDEX_STATUS_UNIQUE_INELIGIBLE = 4,
  INDEX_STATUS_INDEX_ERROR = 5,
  INDEX_STATUS_MAX = 6,
};

const char *ob_index_status_str(ObIndexStatus status);

struct ObTenantTableId
{
  ObTenantTableId() : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID)
  {}
  ObTenantTableId(const uint64_t tenant_id, const uint64_t table_id)
      : tenant_id_(tenant_id),
        table_id_(table_id)
  {}
  bool operator ==(const ObTenantTableId &rv) const
  {
    return (tenant_id_ == rv.tenant_id_) && (table_id_ == rv.table_id_);
  }
  ObTenantTableId &operator = (const ObTenantTableId &tenant_table_id);
  int64_t hash() const { return table_id_; }
  bool operator <(const ObTenantTableId &rv) const
  {
    bool res = tenant_id_ < rv.tenant_id_;
    if (tenant_id_ == rv.tenant_id_) {
      res = table_id_ < rv.table_id_;
    }
    return res;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != table_id_);
  }

  TO_STRING_KV(K_(tenant_id), K_(table_id));

  uint64_t tenant_id_;
  uint64_t table_id_;
};

struct ObTenantDatabaseId
{
  ObTenantDatabaseId() : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID)
  {}
  ObTenantDatabaseId(const uint64_t tenant_id, const uint64_t database_id)
      : tenant_id_(tenant_id),
        database_id_(database_id)
  {}
  bool operator ==(const ObTenantDatabaseId &rv) const
  {
    return ((tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_));
  }
  int64_t hash() const { return database_id_; }
  bool operator <(const ObTenantDatabaseId &rv) const
  {
    bool res = tenant_id_ < rv.tenant_id_;
    if (tenant_id_ == rv.tenant_id_) {
      res = database_id_ < rv.database_id_;
    }
    return res;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != database_id_);
  }

  TO_STRING_KV(K_(tenant_id), K_(database_id));

  uint64_t tenant_id_;
  uint64_t database_id_;
};

struct ObTenantTablegroupId
{
  ObTenantTablegroupId() : tenant_id_(common::OB_INVALID_ID), tablegroup_id_(common::OB_INVALID_ID)
  {}
  ObTenantTablegroupId(const uint64_t tenant_id, const uint64_t tablegroup_id)
      : tenant_id_(tenant_id),
        tablegroup_id_(tablegroup_id)
  {}
  bool operator ==(const ObTenantTablegroupId &rv) const
  {
    return (tenant_id_ == rv.tenant_id_) && (tablegroup_id_ == rv.tablegroup_id_);
  }
  int64_t hash() const { return tablegroup_id_; }
  bool operator <(const ObTenantTablegroupId &rv) const
  {
    bool res = tenant_id_ < rv.tenant_id_;
    if (tenant_id_ == rv.tenant_id_) {
      res = tablegroup_id_ < rv.tablegroup_id_;
    }
    return res;
  }
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != tablegroup_id_);
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    tablegroup_id_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(K_(tenant_id), K_(tablegroup_id));

  uint64_t tenant_id_;
  uint64_t tablegroup_id_;
};

typedef enum {
  TENANT_SCHEMA = 0,
  OUTLINE_SCHEMA = 1,
  USER_SCHEMA = 2,
  DATABASE_SCHEMA = 3,
  TABLEGROUP_SCHEMA = 4,
  TABLE_SCHEMA = 5,
  DATABASE_PRIV = 6,
  TABLE_PRIV = 7,
  OB_MAX_SCHEMA = 8,
} ObSchemaType;

const char *schema_type_str(const ObSchemaType schema_type);

bool is_normal_schema(const ObSchemaType schema_type);

struct ObSimpleTableSchema
{
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  uint64_t data_table_id_;
  common::ObString table_name_;
  int64_t schema_version_;
  ObTableType table_type_;
  ObSimpleTableSchema()
    : tenant_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      tablegroup_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      data_table_id_(common::OB_INVALID_ID),
      schema_version_(common::OB_INVALID_VERSION),
      table_type_(MAX_TABLE_TYPE)
  {}
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    tablegroup_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    data_table_id_ = common::OB_INVALID_ID;
    table_name_.reset();
    schema_version_ = common::OB_INVALID_VERSION;
    table_type_ = MAX_TABLE_TYPE;
  }
  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(tablegroup_id),
               K_(table_id),
               K_(data_table_id),
               K_(table_name),
               K_(schema_version),
               K_(table_type));
   bool is_valid() const
   {
     return (common::OB_INVALID_ID != tenant_id_ &&
             common::OB_INVALID_ID != database_id_ &&
             common::OB_INVALID_ID != tablegroup_id_ &&
             common::OB_INVALID_ID != table_id_ &&
             common::OB_INVALID_ID != data_table_id_ &&
             !table_name_.empty() &&
             schema_version_ >= 0 &&
             table_type_ != MAX_TABLE_TYPE);
   }
};

enum TableStatus {
  TABLE_NOT_CREATE,  //table create in future version
  TABLE_EXIST, //table exist in current version
  TABLE_DELETED, //table has been deleted
};

struct ObTenantResource
{
    OB_UNIS_VERSION(1);

public:
  ObTenantResource();
  ~ObTenantResource();
  void reset();
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  int64_t cpu_reserved_;
  int64_t cpu_max_;
  int64_t mem_reserved_;
  int64_t mem_max_;
  int64_t iops_reserved_;
  int64_t iops_max_;
  int64_t tps_reserved_;
  int64_t tps_max_;
  int64_t qps_reserved_;
  int64_t qps_max_;
};

struct ObUser
{
  ObUser();
  ~ObUser();

  void reset();
  inline bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  uint64_t tenant_id_;
  uint64_t user_id_;
  char user_name_[common::OB_MAX_USER_NAME_LENGTH];
  char host_[common::OB_MAX_HOST_NAME_LENGTH];
  char passwd_[common::OB_MAX_PASSWORD_LENGTH];
  char info_[common::OB_MAX_USER_INFO_LENGTH];
  bool priv_all_;
  bool priv_alter_;
  bool priv_create_;
  bool priv_create_user_;
  bool priv_delete_;
  bool priv_drop_;
  bool priv_grant_option_;
  bool priv_insert_;
  bool priv_update_;
  bool priv_select_;
  bool priv_replace_;
  bool is_locked_;
};

bool ObUser::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != user_id_;
}

struct ObDatabasePrivilege
{
  ObDatabasePrivilege();
  ~ObDatabasePrivilege();

  void reset();
  inline bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  uint64_t tenant_id_;
  uint64_t user_id_;
  char host_[common::OB_MAX_HOST_NAME_LENGTH];
  uint64_t database_id_;
  bool priv_all_;
  bool priv_alter_;
  bool priv_create_;
  bool priv_delete_;
  bool priv_drop_;
  bool priv_grant_option_;
  bool priv_insert_;
  bool priv_update_;
  bool priv_select_;
  bool priv_replace_;
};

bool ObDatabasePrivilege::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != user_id_
      && common::OB_INVALID_ID != database_id_;
}

struct ObSysParam
{
  ObSysParam();
  ~ObSysParam();

  int init(const uint64_t tenant_id,
           const common::ObZone &zone,
           const common::ObString &name,
           const int64_t data_type,
           const common::ObString &value,
           const common::ObString &min_val,
           const common::ObString &max_val,
           const common::ObString &info,
           const int64_t flags);
  void reset();
  inline bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  uint64_t tenant_id_;
  common::ObZone zone_;
  char name_[common::OB_MAX_SYS_PARAM_NAME_LENGTH];
  int64_t data_type_;
  char value_[common::OB_MAX_SYS_PARAM_VALUE_LENGTH];
  char min_val_[common::OB_MAX_SYS_PARAM_VALUE_LENGTH];
  char max_val_[common::OB_MAX_SYS_PARAM_VALUE_LENGTH];
  char info_[common::OB_MAX_SYS_PARAM_INFO_LENGTH];
  int64_t flags_;
};

bool ObSysParam::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_;
}

class ObSchema
{
public:
  ObSchema();
  explicit ObSchema(common::ObDataBuffer &buffer);
  explicit ObSchema(common::ObIAllocator *allocator);
  virtual ~ObSchema();
  virtual void reset();
  virtual inline bool is_valid() const { return common::OB_SUCCESS == error_ret_; }

  virtual int string_array2str(const common::ObIArray<common::ObString> &string_array,
                               char *buf, const int64_t buf_size) const;
  virtual int str2string_array(const char *str,
                               common::ObIArray<common::ObString> &string_array) const;

  virtual int64_t get_convert_size() const { return 0;};
  template<typename SRCSCHEMA, typename DSTSCHEMA>
  static int set_replica_options(const SRCSCHEMA &src, DSTSCHEMA &dst,
                                 const bool force_update = false);
  template<typename SRCSCHEMA, typename DSTSCHEMA>
  static int set_charset_and_collation_options(const SRCSCHEMA &src, DSTSCHEMA &dst);
  static common::ObCollationType get_cs_type_with_cmp_mode(const common::ObNameCaseMode mode);

  ObSchema *get_buffer() const { return buffer_; }
  void set_buffer(ObSchema *buffer) { buffer_ = buffer; }
protected:
  static const int64_t STRING_ARRAY_EXTEND_CNT = 7;
  void *alloc(const int64_t size);
  void free(void *ptr);
  int deep_copy_str(const char *src, common::ObString &dest);
  int deep_copy_str(const common::ObString &src, common::ObString &dest);
  int deep_copy_obj(const common::ObObj &src, common::ObObj &dest);
  int deep_copy_string_array(const common::ObIArray<common::ObString> &src,
                             common::ObArrayHelper<common::ObString> &dst);
  int add_string_to_array(const common::ObString &str,
                          common::ObArrayHelper<common::ObString> &str_array);
  int serialize_string_array(char *buf, const int64_t buf_len, int64_t &pos,
                             const common::ObArrayHelper<common::ObString> &str_array) const;
  int deserialize_string_array(const char *buf, const int64_t data_len, int64_t &pos,
                               common::ObArrayHelper<common::ObString> &str_array);
  int64_t get_string_array_serialize_size(
      const common::ObArrayHelper<common::ObString> &str_array) const;
  void reset_string(common::ObString &str);
  void reset_string_array(common::ObArrayHelper<common::ObString> &str_array);
  const char *extract_str(const common::ObString &str) const;
  // buffer is the memory used to store schema item, if not same with this pointer,
  // it means that this schema item has already been rewrote to buffer when rewriting
  // other schema manager, and when we want to rewrite this schema item in current schema
  // manager, we just set pointer of schema item in schema manager to buffer
  ObSchema *buffer_;
  int error_ret_;
  bool is_inner_allocator_;
  common::ObIAllocator *allocator_;
};

template<typename SRCSCHEMA, typename DSTSCHEMA>
int ObSchema::set_replica_options(const SRCSCHEMA &src, DSTSCHEMA &dst, const bool force_update)
{
  int ret = common::OB_SUCCESS;

  if (dst.get_replica_num() <= 0 || force_update) {
    dst.set_replica_num(src.get_replica_num());
  }

  // primary zone don't copy from upper layer schema
  /*
  if (dst.get_primary_zone().empty() || force_update) {
    if (OB_FAIL(dst.set_primary_zone(src.get_primary_zone()))) {
      SHARE_SCHEMA_LOG(WDIAG, "set_primary_zone failed", K(ret));
    }
  }
  */

  if (OB_SUCC(ret)) {
    if (dst.get_zone_list().count() <= 0 || force_update) {
      if (OB_FAIL(dst.set_zone_list(src.get_zone_list()))) {
        SHARE_SCHEMA_LOG(WDIAG, "set_zone_list failed", "zone_list", src.get_zone_list(), K(ret));
      }
    }
  }
  return ret;
}

template<typename SRCSCHEMA, typename DSTSCHEMA>
int ObSchema::set_charset_and_collation_options(const SRCSCHEMA &src, DSTSCHEMA &dst)
{
  int ret = common::OB_SUCCESS;
  if (dst.get_charset_type() == common::CHARSET_INVALID
      && dst.get_collation_type() == common::CS_TYPE_INVALID) {
    //use upper layer schema's charset and collation type
    if (src.get_charset_type() == common::CHARSET_INVALID
        || src.get_collation_type() == common::CS_TYPE_INVALID) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WDIAG, "charset type or collation type is invalid ", K(ret));
    } else {
      dst.set_charset_type(src.get_charset_type());
      dst.set_collation_type(src.get_collation_type());
    }
  } else {
    common::ObCharsetType charset_type = dst.get_charset_type();
    common::ObCollationType collation_type = dst.get_collation_type();
    if (OB_FAIL(common::ObCharset::check_and_fill_info(charset_type, collation_type))) {
      SHARE_SCHEMA_LOG(WDIAG, "fail to check charset collation",
                       K(charset_type), K(collation_type), K(ret));
    } else {
      dst.set_charset_type(charset_type);
      dst.set_collation_type(collation_type);
    }
  }
  if (common::OB_SUCCESS == ret &&
      !common::ObCharset::is_valid_collation(dst.get_charset_type(),
                                             dst.get_collation_type())) {
    ret = common::OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WDIAG, "invalid collation!", K(dst.get_charset_type()),
                     K(dst.get_collation_type()), K(ret));
  }
  return ret;
}

class ObTenantSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  //base methods
  ObTenantSchema();
  explicit ObTenantSchema(common::ObIAllocator *allocator);
  virtual ~ObTenantSchema();
  ObTenantSchema(const ObTenantSchema &src_schema);
  ObTenantSchema &operator=(const ObTenantSchema &src_schema);
  //for sorted vector
  static bool cmp(const ObTenantSchema *lhs, const ObTenantSchema *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_id() < rhs->get_tenant_id() : false; }
  static bool equal(const ObTenantSchema *lhs, const ObTenantSchema *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_id() == rhs->get_tenant_id() : false; }
  static bool cmp_tenant_id(const ObTenantSchema *lhs, const uint64_t tenant_id)
  { return NULL != lhs ? lhs->get_tenant_id() < tenant_id : false; }
  static bool equal_tenant_id(const ObTenantSchema *lhs, const uint64_t tenant_id)
  { return NULL != lhs ? lhs->get_tenant_id() == tenant_id : false; }
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id)  { tenant_id_ = tenant_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int set_tenant_name(const char *tenant_name) { return deep_copy_str(tenant_name, tenant_name_); }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_tenant_name(const common::ObString &tenant_name) { return deep_copy_str(tenant_name, tenant_name_); }
  inline void set_replica_num(const int64_t replica_num) { replica_num_ = replica_num; }
  inline int set_zone_list(const common::ObIArray<common::ObString> &zone_list);
  inline int set_primary_zone(const common::ObString &zone);
  inline int add_zone(const common::ObString &zone);
  inline void set_locked(const bool locked) { locked_ = locked; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  inline void set_rewrite_merge_version(const int64_t version) { rewrite_merge_version_ = version; }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_collation_type(const common::ObCollationType type) { collation_type_ = type; }
  inline void set_name_case_mode(const common::ObNameCaseMode mode) { name_case_mode_ = mode; }
  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_tenant_name() const { return extract_str(tenant_name_); }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const common::ObString &get_tenant_name_str() const { return tenant_name_; }
  inline int64_t get_replica_num() const { return replica_num_; }
  inline const common::ObIArray<common::ObString> &get_zone_list() const { return zone_list_; }
  inline const common::ObString &get_primary_zone() const { return primary_zone_; }
  inline bool get_locked() const { return locked_; }
  inline bool is_read_only() const { return read_only_; }
  inline int64_t get_rewrite_merge_version() const { return rewrite_merge_version_; }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return collation_type_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(schema_version), K_(tenant_name), K_(replica_num), K_(zone_list),
      K_(primary_zone), K_(charset_type), K_(locked), K_(comment), K_(name_case_mode), K_(read_only),
      K_(rewrite_merge_version));
private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  common::ObString tenant_name_;
  int64_t replica_num_;
  common::ObArrayHelper<common::ObString> zone_list_;
  common::ObString primary_zone_;
  bool locked_;
  bool read_only_;
  int64_t rewrite_merge_version_;
  common::ObCharsetType charset_type_;
  common::ObCollationType collation_type_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString comment_;
};

inline int ObTenantSchema::set_zone_list(const common::ObIArray<common::ObString> &zone_list)
{
  return deep_copy_string_array(zone_list, zone_list_);
}

inline int ObTenantSchema::set_primary_zone(const common::ObString &zone)
{
  return deep_copy_str(zone, primary_zone_);
}

inline int ObTenantSchema::add_zone(const common::ObString &zone)
{
  return add_string_to_array(zone, zone_list_);
}

class ObDatabaseSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  //base methods
  ObDatabaseSchema();
  explicit ObDatabaseSchema(common::ObIAllocator *allocator);
  virtual ~ObDatabaseSchema();
  ObDatabaseSchema(const ObDatabaseSchema &src_schema);
  ObDatabaseSchema &operator=(const ObDatabaseSchema &src_schema);
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  int set_database_name(const char *database_name) { return deep_copy_str(database_name, database_name_); }
  int set_database_name(const common::ObString &database_name) { return deep_copy_str(database_name, database_name_); }
  inline void set_replica_num(const int64_t replica_num) { replica_num_ = replica_num; }
  inline int set_zone_list(const common::ObIArray<common::ObString> &zone_list);
  inline int set_primary_zone(const common::ObString &zone);
  inline int add_zone(const common::ObString &zone);
  int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_collation_type(const common::ObCollationType type) {collation_type_ = type; }
  inline void set_name_case_mode(const common::ObNameCaseMode mode) {name_case_mode_ = mode; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  void set_default_tablegroup_id(const uint64_t tablegroup_id) { default_tablegroup_id_ = tablegroup_id; }
  int set_default_tablegroup_name(const common::ObString &tablegroup_name) { return deep_copy_str(tablegroup_name, default_tablegroup_name_); }

  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_database_name() const { return extract_str(database_name_); }
  inline const common::ObString &get_database_name_str() const { return database_name_; }
  inline int64_t get_replica_num() const { return replica_num_; }
  inline const common::ObIArray<common::ObString> &get_zone_list() const { return zone_list_; }
  inline const common::ObString &get_primary_zone() const { return primary_zone_; }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return collation_type_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline bool is_read_only() const { return read_only_; }
  inline uint64_t get_default_tablegroup_id() const { return default_tablegroup_id_; }
  inline const common::ObString &get_default_tablegroup_name() const { return default_tablegroup_name_; }
  ObTenantDatabaseId get_tenant_database_id() const;
  //other methods
  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual void reset();
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(schema_version), K_(database_name),
    K_(replica_num), K_(zone_list), K_(primary_zone),
    K_(charset_type), K_(collation_type), K_(name_case_mode), K_(comment), K_(read_only),
    K_(default_tablegroup_id), K_(default_tablegroup_name));
  void print_info() const;

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_version_;
  common::ObString database_name_;
  int64_t replica_num_;
  common::ObArrayHelper<common::ObString> zone_list_;
  common::ObString primary_zone_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;//default:utf8mb4_general_ci
  common::ObNameCaseMode name_case_mode_;//default:OB_NAME_CASE_INVALID
  common::ObString comment_;
  bool read_only_;
  uint64_t default_tablegroup_id_;
  common::ObString default_tablegroup_name_;
};

inline int ObDatabaseSchema::set_zone_list(const common::ObIArray<common::ObString> &zone_list)
{
  return deep_copy_string_array(zone_list, zone_list_);
}

inline int ObDatabaseSchema::set_primary_zone(const common::ObString &zone)
{
  return deep_copy_str(zone, primary_zone_);
}

inline int ObDatabaseSchema::add_zone(const common::ObString &zone)
{
  return add_string_to_array(zone, zone_list_);
}

class ObTablegroupSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  //base methods
  ObTablegroupSchema();
  explicit ObTablegroupSchema(common::ObIAllocator *allocator);
  virtual ~ObTablegroupSchema();
  ObTablegroupSchema(const ObTablegroupSchema &src_schema);
  ObTablegroupSchema &operator=(const ObTablegroupSchema &src_schema);
  //set methods
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_tablegroup_id(const uint64_t tablegroup_id) { tablegroup_id_ = tablegroup_id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int set_tablegroup_name(const char *name) { return deep_copy_str(name, tablegroup_name_); }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_tablegroup_name(const common::ObString &name) { return deep_copy_str(name, tablegroup_name_); }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_tablegroup_id() const { return tablegroup_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline const char *get_tablegroup_name() const { return extract_str(tablegroup_name_); }
  inline const char *get_comment() const { return  extract_str(comment_); }
  inline const common::ObString &get_tablegroup_name_str() const { return tablegroup_name_; }
  inline const common::ObString &get_comment_str() const { return comment_; }
  ObTenantTablegroupId get_tenant_tablegroup_id() const;
  //other methods
  virtual void reset();
  int64_t get_convert_size() const ;
  virtual bool is_valid() const;
  void print_info() const;
  TO_STRING_KV(K_(tenant_id),
               K_(tablegroup_id),
               K_(schema_version),
               K_(tablegroup_name),
               K_(comment));
private:
  uint64_t tenant_id_;
  uint64_t tablegroup_id_;
  int64_t schema_version_;
  common::ObString tablegroup_name_;
  common::ObString comment_;
};

class ObPartitionOption : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObPartitionOption();
  explicit ObPartitionOption(common::ObIAllocator *allocator);
  virtual ~ObPartitionOption();
  ObPartitionOption(const ObPartitionOption &expr);
  ObPartitionOption &operator=(const ObPartitionOption &expr);
  bool operator==(const ObPartitionOption &expr) const;
  bool operator!=(const ObPartitionOption &expr) const;

  //set methods
  int set_part_expr(const common::ObString &expr) { return deep_copy_str(expr, part_func_expr_); }
  inline void set_part_num(const int64_t part_num) { part_num_ = part_num; }
  inline void set_part_func_type(const ObPartitionFuncType func_type) { part_func_type_ = func_type; }

  //get methods
  inline const common::ObString &get_part_func_expr_str() const { return part_func_expr_; }
  inline const char *get_part_func_expr() const { return extract_str(part_func_expr_); }
  inline int64_t get_part_num() const { return part_num_; }
  inline ObPartitionFuncType get_part_func_type() const { return part_func_type_; }

  //other methods
  virtual void reset();
  int64_t get_convert_size() const ;
  virtual bool is_valid() const;
  TO_STRING_KV(K_(part_func_type), K_(part_func_expr), K_(part_num));
private:
  ObPartitionFuncType part_func_type_;
  common::ObString part_func_expr_;
  int64_t part_num_;
};

class ObViewSchema : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObViewSchema();
  explicit ObViewSchema(common::ObIAllocator *allocator);
  virtual ~ObViewSchema();
  ObViewSchema(const ObViewSchema &src_schema);
  ObViewSchema &operator=(const ObViewSchema &src_schema);
  bool operator==(const ObViewSchema &other) const;
  bool operator!=(const ObViewSchema &other) const;

  inline int set_view_definition(const char *view_definition) { return deep_copy_str(view_definition, view_definition_); }
  inline int set_view_definition(const common::ObString &view_definition) { return deep_copy_str(view_definition, view_definition_); }
  inline void set_view_check_option(const ViewCheckOption option) { view_check_option_ = option; }
  inline void set_view_is_updatable(const bool is_updatable) { view_is_updatable_ = is_updatable; }

  inline const common::ObString &get_view_definition_str() const { return view_definition_; }
  inline const char *get_view_definition() const { return extract_str(view_definition_); }
  inline ViewCheckOption get_view_check_option() const { return view_check_option_; }
  inline bool get_view_is_updatable() const { return view_is_updatable_; }
  int64_t get_convert_size() const;
  virtual bool is_valid() const;
  virtual  void reset();

  TO_STRING_KV(N_VIEW_DEFINITION, view_definition_,
               N_CHECK_OPTION, ob_view_check_option_str(view_check_option_),
               N_IS_UPDATABLE, STR_BOOL(view_is_updatable_));
private:
  common::ObString view_definition_;
  ViewCheckOption view_check_option_;
  bool view_is_updatable_;
};

class ObColumnSchemaHashWrapper
{
public:
  ObColumnSchemaHashWrapper() {}
  explicit ObColumnSchemaHashWrapper(const common::ObString &str) : column_name_(str) {}
  ~ObColumnSchemaHashWrapper(){}
  void set_name(const common::ObString &str) { column_name_ = str; }
  inline bool operator==(const ObColumnSchemaHashWrapper &other) const
  {
    return (common::ObCharset::case_insensitive_equal(column_name_, other.column_name_));
  }
  inline uint64_t hash() const;
  common::ObString column_name_;
};
typedef ObColumnSchemaHashWrapper ObColumnNameHashWrapper;
typedef ObColumnSchemaHashWrapper ObIndexNameHashWrapper;

inline uint64_t ObColumnSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  //case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, column_name_, hash_ret);
  return hash_ret;
}
class ObIndexSchemaHashWrapper
{
public :
  ObIndexSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID)
  {
  }
  ObIndexSchemaHashWrapper(uint64_t tenant_id, const uint64_t database_id,
                           const common::ObString &index_name)
      : tenant_id_(tenant_id), database_id_(database_id), index_name_(index_name)
  {
  }
  ~ObIndexSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObIndexSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_index_name() const { return index_name_; }
private :
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString index_name_;
};

inline uint64_t ObIndexSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  //case insensitive
  hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, index_name_, hash_ret);
  return hash_ret;
}

inline bool ObIndexSchemaHashWrapper::operator ==(const ObIndexSchemaHashWrapper &rv) const
{
  //case insensitive
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
         && (common::ObCharset::case_insensitive_equal(index_name_, rv.index_name_));
}

class ObTableSchemaHashWrapper
{
public :
  ObTableSchemaHashWrapper()
      : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID),
      name_case_mode_(common::OB_NAME_CASE_INVALID)
  {
  }
  ObTableSchemaHashWrapper(const uint64_t tenant_id, const uint64_t database_id, const common::ObNameCaseMode mode,
                           const common::ObString &table_name)
      : tenant_id_(tenant_id), database_id_(database_id), name_case_mode_(mode), table_name_(table_name)
  {
  }
  ~ObTableSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObTableSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_table_name() const { return table_name_; }
private :
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString table_name_;
};

inline uint64_t ObTableSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  hash_ret = common::ObCharset::hash(cs_type, table_name_, hash_ret);
  return hash_ret;
}

inline bool ObTableSchemaHashWrapper::operator ==(const ObTableSchemaHashWrapper &rv) const
{
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
      && (name_case_mode_ == rv.name_case_mode_)
      && (0 == common::ObCharset::strcmp(cs_type, table_name_ ,rv.table_name_));
}

class ObDatabaseSchemaHashWrapper
{
public :
  ObDatabaseSchemaHashWrapper() : tenant_id_(common::OB_INVALID_ID), name_case_mode_(common::OB_NAME_CASE_INVALID)
  {
  }
  ObDatabaseSchemaHashWrapper(const uint64_t tenant_id, const common::ObNameCaseMode mode,
                              const common::ObString &database_name)
      : tenant_id_(tenant_id), name_case_mode_(mode), database_name_(database_name)
  {
  }
  ~ObDatabaseSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObDatabaseSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline const common::ObString &get_database_name() const { return database_name_; }
private :
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString database_name_;
};

inline uint64_t ObDatabaseSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  hash_ret = common::ObCharset::hash(cs_type, database_name_, hash_ret);
  return hash_ret;
}

inline bool ObDatabaseSchemaHashWrapper::operator ==(const ObDatabaseSchemaHashWrapper &rv) const
{
  common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
  return (tenant_id_ == rv.tenant_id_)
      && (name_case_mode_ == rv.name_case_mode_)
      && (0 == common::ObCharset::strcmp(cs_type, database_name_ ,rv.database_name_));
}

class ObTablegroupSchemaHashWrapper
{
public :
  ObTablegroupSchemaHashWrapper() : tenant_id_(common::OB_INVALID_ID)
  {
  }
  ObTablegroupSchemaHashWrapper(uint64_t tenant_id, const common::ObString &tablegroup_name)
      : tenant_id_(tenant_id), tablegroup_name_(tablegroup_name)
  {
  }
  ~ObTablegroupSchemaHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObTablegroupSchemaHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_tablegroup_name() const { return tablegroup_name_; }
private :
  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
};

inline uint64_t ObTablegroupSchemaHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(tablegroup_name_.ptr(), tablegroup_name_.length(), hash_ret);
  return hash_ret;
}
inline bool ObTablegroupSchemaHashWrapper::operator ==(const ObTablegroupSchemaHashWrapper &rv)
const
{
  return (tenant_id_ == rv.tenant_id_) && (tablegroup_name_ == rv.tablegroup_name_);
}


struct ObTenantOutlineId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantOutlineId()
      : tenant_id_(common::OB_INVALID_ID), outline_id_(common::OB_INVALID_ID)
  {}
  ObTenantOutlineId(const uint64_t tenant_id, const uint64_t outline_id)
      : tenant_id_(tenant_id), outline_id_(outline_id)
  {}
  bool operator==(const ObTenantOutlineId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (outline_id_ == rhs.outline_id_);
  }
  bool operator!=(const ObTenantOutlineId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantOutlineId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = outline_id_ < rhs.outline_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&outline_id_, sizeof(outline_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (outline_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(outline_id));
  uint64_t tenant_id_;
  uint64_t outline_id_;
};


//For managing privilege
struct ObTenantUserId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantUserId()
      : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTenantUserId(const uint64_t tenant_id, const uint64_t user_id)
      : tenant_id_(tenant_id), user_id_(user_id)
  {}
  bool operator==(const ObTenantUserId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_);
  }
  bool operator!=(const ObTenantUserId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantUserId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id));
  uint64_t tenant_id_;
  uint64_t user_id_;
};

class ObPrintPrivSet
{
public:
  explicit ObPrintPrivSet(ObPrivSet priv_set) : priv_set_(priv_set)
  {}

  DECLARE_TO_STRING;
private:
  ObPrivSet priv_set_;
};

class ObPriv
{
  OB_UNIS_VERSION_V(1);

public:
  ObPriv()
      : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
        schema_version_(1), priv_set_(0)
  { }
  ObPriv(const uint64_t tenant_id, const uint64_t user_id,
         const int64_t schema_version, const ObPrivSet priv_set)
      : tenant_id_(tenant_id), user_id_(user_id),
        schema_version_(schema_version), priv_set_(priv_set)
  { }

  virtual ~ObPriv() { }
  ObPriv& operator=(const ObPriv &other);
  static bool cmp_tenant_user_id(const ObPriv *lhs, const ObTenantUserId &tenant_user_id)
  { return (lhs->get_tenant_user_id() < tenant_user_id); }
  static bool equal_tenant_user_id(const ObPriv *lhs, const ObTenantUserId &tenant_user_id)
  { return (lhs->get_tenant_user_id() == tenant_user_id); }
  static bool cmp_tenant_id(const ObPriv *lhs, const uint64_t tenant_id)
  { return (lhs->get_tenant_id() < tenant_id); }
  ObTenantUserId get_tenant_user_id() const
  { return ObTenantUserId(tenant_id_, user_id_); }

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_user_id(const uint64_t user_id) { user_id_ = user_id; }
  inline void set_schema_version(const uint64_t schema_version) { schema_version_ = schema_version;}
  inline void set_priv(const ObPrivType priv) { priv_set_ |= priv; }
  inline void set_priv_set(const ObPrivSet priv_set) { priv_set_ = priv_set; }
  inline void reset_priv_set() { priv_set_ = 0; }

  inline uint64_t get_tenant_id() const { return tenant_id_; };
  inline uint64_t get_user_id() const { return user_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline ObPrivSet get_priv_set() const { return priv_set_; }
  inline ObPrivType get_priv(const ObPrivType priv) const { return priv_set_ & priv; }
  virtual void reset();
  virtual bool is_valid() const
  { return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != user_id_
        && schema_version_ > 0; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(schema_version), "privileges", ObPrintPrivSet(priv_set_));
protected:
  uint64_t tenant_id_;
  uint64_t user_id_;
  int64_t schema_version_;
  ObPrivSet priv_set_;
};

// Not used now
class ObUserInfoHashWrapper
{
public :
  ObUserInfoHashWrapper()
      : tenant_id_(common::OB_INVALID_ID)
  {}
  ObUserInfoHashWrapper(uint64_t tenant_id, const common::ObString &user_name)
      : tenant_id_(tenant_id),user_name_(user_name)
  {
  }
  ~ObUserInfoHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObUserInfoHashWrapper &rv) const;

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_user_name() const { return user_name_; }
private :
  uint64_t tenant_id_;
  common::ObString user_name_;
};

inline uint64_t ObUserInfoHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(user_name_.ptr(), user_name_.length(), hash_ret);
  return hash_ret;
}

inline bool ObUserInfoHashWrapper::operator ==(const ObUserInfoHashWrapper &other) const
{
  return (tenant_id_ == other.tenant_id_) && (user_name_ == other.user_name_);
}

class ObUserInfo : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  ObUserInfo()
    :ObSchema(), ObPriv(),
     user_name_(), host_(), passwd_(), info_(), locked_(false)
  { }
  explicit ObUserInfo(common::ObIAllocator *allocator);
  virtual ~ObUserInfo();
  ObUserInfo(const ObUserInfo &other);
  ObUserInfo& operator=(const ObUserInfo &other);
  static bool cmp(const ObUserInfo *lhs, const ObUserInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_user_id() < rhs->get_tenant_user_id() : false; }
  static bool equal(const ObUserInfo *lhs, const ObUserInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_tenant_user_id() == rhs->get_tenant_user_id() : false; }

  //set methods
  inline int set_user_name(const char *user_name) { return deep_copy_str(user_name, user_name_); }
  inline int set_user_name(const common::ObString &user_name)
  { return deep_copy_str(user_name, user_name_); }
  inline int set_host(const char *host) { return deep_copy_str(host, host_); }
  inline int set_host(const common::ObString &host) { return deep_copy_str(host, host_); }
  inline int set_passwd(const char *passwd) { return deep_copy_str(passwd, passwd_); }
  inline int set_passwd(const common::ObString &passwd) { return deep_copy_str(passwd, passwd_); }
  inline int set_info(const char *info) { return deep_copy_str(info, info_); }
  inline int set_info(const common::ObString &info) { return deep_copy_str(info, info_); }
  inline void set_is_locked(const bool locked) { locked_ = locked; }

  //get methods
  inline const char* get_user_name() const { return extract_str(user_name_); }
  inline const common::ObString& get_user_name_str() const { return user_name_; }
  inline const char* get_host() const { return extract_str(host_); }
  inline const common::ObString& get_host_str() const { return host_; }
  inline const char* get_passwd() const { return extract_str(passwd_); }
  inline const common::ObString& get_passwd_str() const { return passwd_; }
  inline const char* get_info() const { return extract_str(info_); }
  inline const common::ObString& get_info_str() const { return info_; }
  inline bool get_is_locked() const { return locked_; }

  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(user_name), K_(host),
               "privileges", ObPrintPrivSet(priv_set_), K_(info), K_(locked));
private:
  common::ObString user_name_;
  common::ObString host_;
  common::ObString passwd_;
  common::ObString info_;
  bool locked_;
};

struct ObDBPrivSortKey
{
  ObDBPrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID), sort_(0)
  {}
  ObDBPrivSortKey(const uint64_t tenant_id, const uint64_t user_id, const uint64_t sort_value)
      : tenant_id_(tenant_id), user_id_(user_id), sort_(sort_value)
  {}
  bool operator==(const ObDBPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (sort_ == rhs.sort_);
  }
  bool operator!=(const ObDBPrivSortKey &rhs) const
  { return !(*this == rhs); }
  bool operator<(const ObDBPrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = sort_ > rhs.sort_;//sort values of 'sort_' from big to small
      }
    }
    return bret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K(sort_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t sort_;
};

struct ObOriginalDBKey
{
  ObOriginalDBKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObOriginalDBKey(const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db)
  {}
  bool operator==(const ObOriginalDBKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (db_ == rhs.db_);
  }
  bool operator!=(const ObOriginalDBKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObOriginalDBKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
    }
    return bret;
  }
  //Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(db_.ptr(), db_.length(), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
};

class ObDBPriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  ObDBPriv()
    : ObPriv(), db_(), sort_(0)
  {}
  explicit ObDBPriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(), db_(), sort_(0)
  {}
  virtual ~ObDBPriv()
  {}
  ObDBPriv(const ObDBPriv &other)
      : ObSchema(), ObPriv()
  { *this = other; }
  ObDBPriv& operator=(const ObDBPriv &other);

  static bool cmp(const ObDBPriv *lhs, const ObDBPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ?
      (lhs->get_sort_key() < rhs->get_sort_key()) : false;
  }
  static bool cmp_sort_key(const ObDBPriv *lhs, const ObDBPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  ObDBPrivSortKey get_sort_key() const
  { return ObDBPrivSortKey(tenant_id_, user_id_, sort_); }
  static bool equal(const ObDBPriv *lhs, const ObDBPriv *rhs)
  {
    return (NULL != lhs && NULL != rhs) ?
      lhs->get_sort_key() == rhs->get_sort_key(): false;
  } // point check
  ObOriginalDBKey get_original_key() const
  { return ObOriginalDBKey(tenant_id_, user_id_, db_); }

  //set methods
  inline int set_database_name(const char *db) { return deep_copy_str(db, db_); }
  inline int set_database_name(const common::ObString &db) { return deep_copy_str(db, db_); }
  inline void set_sort(const uint64_t sort) { sort_ = sort; }

  //get methods
  inline const char* get_database_name() const { return extract_str(db_); }
  inline const common::ObString& get_database_name_str() const { return db_; }
  inline uint64_t get_sort() const { return sort_; }
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), "privileges", ObPrintPrivSet(priv_set_));
private:
  common::ObString db_;
  uint64_t sort_;
};

// In order to find in table_privs_ whether a table is authorized under a certain db
struct ObTablePrivDBKey
{
  ObTablePrivDBKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTablePrivDBKey(const uint64_t tenant_id, const uint64_t user_id, const common::ObString &db)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db)
  {}
  bool operator==(const ObTablePrivDBKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (db_ == rhs.db_);
  }
  bool operator!=(const ObTablePrivDBKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTablePrivDBKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = db_ < rhs.db_;
      }
    }
    return bret;
  }
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
};

struct ObTablePrivSortKey
{
  ObTablePrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID)
  {}
  ObTablePrivSortKey(const uint64_t tenant_id, const uint64_t user_id,
                     const common::ObString &db, const common::ObString &table)
      : tenant_id_(tenant_id), user_id_(user_id), db_(db), table_(table)
  {}
  bool operator==(const ObTablePrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (db_ == rhs.db_) && (table_ == rhs.table_);
  }
  bool operator!=(const ObTablePrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObTablePrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = db_ < rhs.db_;
        if (false == bret && db_ == rhs.db_) {
          bret = table_ < rhs.table_;
        }
      }
    }
    return bret;
  }
  //Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(db_.ptr(), db_.length(), hash_ret);
    hash_ret = common::murmurhash(table_.ptr(), table_.length(), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString table_;
};

class ObTablePriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  //constructor and destructor
  ObTablePriv()
      : ObSchema(), ObPriv()
  { }
  explicit ObTablePriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv()
  { }
  ObTablePriv(const ObTablePriv &other)
      : ObSchema(), ObPriv()
  { *this = other; }
  virtual ~ObTablePriv() { }

  //operator=
  ObTablePriv& operator=(const ObTablePriv &other);

  //for sort
  ObTablePrivSortKey get_sort_key() const
  { return ObTablePrivSortKey(tenant_id_, user_id_, db_, table_); }
  static bool cmp(const ObTablePriv *lhs, const ObTablePriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false; }
  static bool cmp_sort_key(const ObTablePriv *lhs, const ObTablePrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  static bool equal(const ObTablePriv *lhs, const ObTablePriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false; }
  static bool equal_sort_key(const ObTablePriv *lhs, const ObTablePrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() == sort_key : false; }

  ObTablePrivDBKey get_db_key() const
  { return ObTablePrivDBKey(tenant_id_, user_id_, db_); }
  static bool cmp_db_key(const ObTablePriv *lhs, const ObTablePrivDBKey &db_key)
  { return lhs->get_db_key() < db_key; }

  //set methods
  inline int set_database_name(const char *db) { return deep_copy_str(db, db_); }
  inline int set_database_name(const common::ObString &db) { return deep_copy_str(db, db_); }
  inline int set_table_name(const char *table) { return deep_copy_str(table, table_); }
  inline int set_table_name(const common::ObString &table) { return deep_copy_str(table, table_); }

  //get methods
  inline const char* get_database_name() const { return extract_str(db_); }
  inline const common::ObString& get_database_name_str() const { return db_; }
  inline const char* get_table_name() const { return extract_str(table_); }
  inline const common::ObString& get_table_name_str() const { return table_; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table),
               "privileges", ObPrintPrivSet(priv_set_));
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;

private:
  common::ObString db_;
  common::ObString table_;
};

enum ObPrivLevel
{
  OB_PRIV_INVALID_LEVEL,
  OB_PRIV_USER_LEVEL,
  OB_PRIV_DB_LEVEL,
  OB_PRIV_TABLE_LEVEL,
  OB_PRIV_DB_ACCESS_LEVEL,
  OB_PRIV_MAX_LEVEL,
};

const char *ob_priv_level_str(const ObPrivLevel grant_level);

struct ObNeedPriv
{
  ObNeedPriv(const common::ObString &db,
             const common::ObString &table,
             ObPrivLevel priv_level,
             ObPrivSet priv_set,
             const bool is_sys_table)
      : db_(db), table_(table), priv_level_(priv_level),
        priv_set_(priv_set), is_sys_table_(is_sys_table)
  { }
  ObNeedPriv()
      : db_(), table_(),
        priv_level_(OB_PRIV_INVALID_LEVEL),
        priv_set_(0), is_sys_table_(false)
  { }
  int deep_copy(const ObNeedPriv &other, common::ObIAllocator &allocator);
  common::ObString db_;
  common::ObString table_;
  ObPrivLevel priv_level_;
  ObPrivSet priv_set_;
  bool is_sys_table_; // May be used to represent the table of schema metadata
  TO_STRING_KV(K_(db), K_(table), K_(priv_set), K_(priv_level), K_(is_sys_table));
};

struct ObStmtNeedPrivs
{
  typedef common::ObFixedArray<ObNeedPriv, common::ObIAllocator> NeedPrivs;
  explicit ObStmtNeedPrivs(common::ObIAllocator &alloc) : need_privs_(alloc)
  {}
  ObStmtNeedPrivs() : need_privs_()
  {}
  ~ObStmtNeedPrivs()
  { reset(); }
  void reset()
  { need_privs_.reset(); }
  int deep_copy(const ObStmtNeedPrivs &other, common::ObIAllocator &allocator);
  NeedPrivs need_privs_;
  TO_STRING_KV(K_(need_privs));
};

struct ObSessionPrivInfo
{
  ObSessionPrivInfo() :
      tenant_id_(common::OB_INVALID_ID),
      user_id_(common::OB_INVALID_ID),
      user_name_(),
      db_(),
      user_priv_set_(0),
      db_priv_set_(0)
  {}
  ObSessionPrivInfo(const uint64_t tenant_id, const uint64_t user_id,
                    const common::ObString &db, const ObPrivSet user_priv_set,
                    const ObPrivSet db_priv_set)
      : tenant_id_(tenant_id),
        user_id_(user_id),
        user_name_(),
        db_(db),
        user_priv_set_(user_priv_set),
        db_priv_set_(db_priv_set)
  {}

  virtual ~ObSessionPrivInfo() {}

  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    user_id_ = common::OB_INVALID_ID;
    user_name_.reset();
    db_.reset();
    user_priv_set_ = 0;
    db_priv_set_ = 0;
  }

  bool is_valid() const
  { return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID); }

  uint64_t tenant_id_; //for privilege.Current login tenant. if normal tenant access
                       //sys tenant's object should use other method for priv checking.
  uint64_t user_id_;
  common::ObString user_name_;
  common::ObString db_;              //db name in current session
  ObPrivSet user_priv_set_;
  ObPrivSet db_priv_set_;    //user's db_priv_set of db
  virtual TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(user_priv_set), K_(db_priv_set));
};

struct ObUserLoginInfo
{
  ObUserLoginInfo()
  {}
  ObUserLoginInfo(const common::ObString &tenant_name,
                  const common::ObString &user_name,
                  const common::ObString &passwd,
                  const common::ObString &db)
      : tenant_name_(tenant_name), user_name_(user_name), passwd_(passwd), db_(db)
  {}
  TO_STRING_KV(K_(tenant_name), K_(user_name), K_(db));
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString passwd_;
  common::ObString db_;
};


enum ObHintFormat
{
  HINT_NORMAL,
  HINT_LOCAL,
};

//used for outline manager
class ObOutlineInfo: public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObOutlineInfo();
  explicit ObOutlineInfo(common::ObIAllocator *allocator);
  virtual ~ObOutlineInfo();
  ObOutlineInfo(const ObOutlineInfo &src_schema);
  ObOutlineInfo &operator=(const ObOutlineInfo &src_schema);
  void reset();
  bool is_valid() const;
  bool is_valid_for_replace() const;
  int64_t get_convert_size() const;
  static bool cmp(const ObOutlineInfo *lhs, const ObOutlineInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_outline_id() < rhs->get_outline_id() : false; }
  static bool equal(const ObOutlineInfo *lhs, const ObOutlineInfo *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_outline_id() == rhs->get_outline_id() : false; }

  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_database_id(const uint64_t id) { database_id_ = id; }
  inline void set_outline_id(uint64_t id) { outline_id_ = id; }
  inline void set_schema_version(int64_t version) { schema_version_ = version; }
  int set_name(const char *name) { return deep_copy_str(name, name_); }
  int set_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  int set_signature(const char *sig) { return deep_copy_str(sig, signature_); }
  int set_signature(const common::ObString &sig) { return deep_copy_str(sig, signature_); }
  int set_outline_content(const char *content) { return deep_copy_str(content, outline_content_); }
  int set_outline_content(const common::ObString &content) { return deep_copy_str(content, outline_content_); }
  int set_sql_text(const char *sql) { return deep_copy_str(sql, sql_text_); }
  int set_sql_text(const common::ObString &sql) { return deep_copy_str(sql, sql_text_); }
  int set_owner(const char *owner) { return deep_copy_str(owner, owner_); }
  int set_owner(const common::ObString &owner) { return deep_copy_str(owner, owner_); }
  void set_used(const bool used) {used_ = used;}
  int set_version(const char *version) { return deep_copy_str(version, version_); }
  int set_version(const common::ObString &version) { return deep_copy_str(version, version_); }
  void set_compatible(const bool compatible) { compatible_ = compatible;}
  void set_enabled(const bool enabled) { enabled_ = enabled;}
  void set_format(const ObHintFormat hint_format) { format_ = hint_format;}

  inline ObTenantOutlineId get_tenant_outline_id() const {return ObTenantOutlineId(tenant_id_, outline_id_);}
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_outline_id() const { return outline_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline bool is_used() const { return used_; }
  inline bool is_enabled() const { return enabled_; }
  inline bool is_compatible() const { return compatible_; }
  inline ObHintFormat get_hint_format() const { return format_; }
  inline const char *get_name() const { return extract_str(name_); }
  inline const common::ObString &get_name_str() const { return name_; }
  inline const char *get_signature() const { return extract_str(signature_); }
  inline const common::ObString &get_signature_str() const { return signature_; }
  inline const char *get_outline_content() const { return extract_str(outline_content_); }
  inline const common::ObString &get_outline_content_str() const { return outline_content_; }
  inline const char *get_sql_text() const { return extract_str(sql_text_); }
  inline const common::ObString &get_sql_text_str() const { return sql_text_; }
  inline const char *get_owner() const { return extract_str(owner_); }
  inline const common::ObString &get_owner_str() const { return owner_; }
  inline const char *get_version() const { return extract_str(version_); }
  inline const common::ObString &get_version_str() const { return version_; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(database_id), K_(outline_id), K_(schema_version),
                       K_(name), K_(signature), K_(outline_content), K_(sql_text),
                       K_(owner), K_(used), K_(compatible),
                       K_(enabled), K_(format));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t outline_id_;
  int64_t schema_version_; //the last modify timestamp of this version
  common::ObString name_;//outline name
  common::ObString signature_;// SQL after constant parameterization
  common::ObString outline_content_;
  common::ObString sql_text_;
  common::ObString owner_;
  bool used_;
  common::ObString version_;
  bool compatible_;
  bool enabled_;
  ObHintFormat format_;
};

class ObOutlineNameHashWrapper
{
public:
  ObOutlineNameHashWrapper() : tenant_id_(common::OB_INVALID_ID),
                               database_id_(common::OB_INVALID_ID),
                               name_() {}
  ObOutlineNameHashWrapper(const uint64_t tenant_id, const uint64_t database_id,
                           const common::ObString &name_)
      : tenant_id_(tenant_id), database_id_(database_id), name_(name_)
  {}
  ~ObOutlineNameHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObOutlineNameHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_name(const common::ObString &name) { name_ = name;}

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_name() const { return name_; }
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString name_;
};

inline uint64_t ObOutlineNameHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(name_.ptr(), name_.length(), hash_ret);
  return hash_ret;
}

inline bool ObOutlineNameHashWrapper::operator ==(const ObOutlineNameHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_) && (name_ == rv.name_);
}

class ObOutlineSignatureHashWrapper
{
public:
  ObOutlineSignatureHashWrapper() : tenant_id_(common::OB_INVALID_ID),
                                    database_id_(common::OB_INVALID_ID),
                                    signature_() {}
  ObOutlineSignatureHashWrapper(const uint64_t tenant_id, const uint64_t database_id,
                                const common::ObString &signature)
      : tenant_id_(tenant_id), database_id_(database_id), signature_(signature)
  {}
  ~ObOutlineSignatureHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObOutlineSignatureHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_signature(const common::ObString &signature) { signature_ = signature;}

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_signature() const { return signature_; }
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString signature_;
};

inline uint64_t ObOutlineSignatureHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(signature_.ptr(), signature_.length(), hash_ret);
  return hash_ret;
}

inline bool ObOutlineSignatureHashWrapper::operator ==(const ObOutlineSignatureHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_) && (database_id_ == rv.database_id_)
      && (signature_ == rv.signature_);
}

}//namespace schema
}//namespace share
}//namespace oceanbase

#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H */
