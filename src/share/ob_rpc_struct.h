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

#ifndef OCEANBASE_RPC_OB_RPC_STRUCT_H_
#define OCEANBASE_RPC_OB_RPC_STRUCT_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_debug_sync.h"
#include "common/ob_member.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "common/ob_role_mgr.h"
#include "common/ob_zone.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/partition_table/ob_partition_location.h"
#include  "sql/session/ob_sys_var_class_type.h"

namespace oceanbase
{
namespace obrpc
{
typedef common::ObSArray<common::ObAddr> ObServerList;

struct Bool
{
  OB_UNIS_VERSION(1);

public:
  Bool(bool v = false)
      : v_(v) {}

  operator bool () { return v_; }
  operator bool () const { return v_; }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  bool v_;
};

struct Int64
{
  OB_UNIS_VERSION(1);

public:
  Int64(int64_t v = common::OB_INVALID_ID)
      : v_(v) {}

  inline void reset();
  operator int64_t () { return v_; }
  operator int64_t () const { return v_; }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  int64_t v_;
};

struct UInt64
{
  OB_UNIS_VERSION(1);

public:
  UInt64(uint64_t v = common::OB_INVALID_ID)
      : v_(v) {}

  operator uint64_t () { return v_; }
  operator uint64_t () const { return v_; }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  uint64_t v_;
};

struct ObGetRootserverRoleResult
{
  OB_UNIS_VERSION(1);

public:
  ObGetRootserverRoleResult():
    role_(0)
  {}
  DECLARE_TO_STRING;

  int32_t role_;
  common::ObZone zone_;
};

struct ObServerInfo
{
  OB_UNIS_VERSION(1);

public:
  common::ObZone zone_;
  common::ObAddr server_;

  bool operator <(const ObServerInfo &r) const { return zone_ < r.zone_; }
  DECLARE_TO_STRING;
};

struct ObPartitionId
{
  OB_UNIS_VERSION(1);

public:
  int64_t table_id_;
  int64_t partition_id_;

  ObPartitionId() : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_INDEX) {}

  DECLARE_TO_STRING;
};

struct ObPartitionStat
{
  OB_UNIS_VERSION(1);

public:
  enum PartitionStat
  {
    RECOVERING = 0,
    WRITABLE = 1,
  };
  ObPartitionStat() : partition_key_(), stat_(RECOVERING) {}
  void reset() { partition_key_.reset(); stat_ = RECOVERING; }
  common::ObPartitionKey partition_key_;
  PartitionStat stat_;

  DECLARE_TO_STRING;
};

typedef common::ObSArray<ObServerInfo> ObServerInfoList;
typedef common::ObSArray<common::ObPartitionKey> ObPartitionList;
typedef common::ObSArray<ObPartitionStat> ObPartitionStatList;
typedef common::ObArray<ObServerInfoList> ObPartitionServerList;

struct ObCreateResourceUnitArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateResourceUnitArg() :
      min_cpu_(0),
      min_iops_(0),
      min_memory_(0),
      max_cpu_(0),
      max_memory_(0),
      max_disk_size_(0),
      max_iops_(0),
      max_session_num_(0),
      if_not_exist_(false)
  { }
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  double min_cpu_;
  int64_t min_iops_;
  int64_t min_memory_;
  double max_cpu_;
  int64_t max_memory_;
  int64_t max_disk_size_;
  int64_t max_iops_;
  int64_t max_session_num_;
  bool if_not_exist_;
};

struct ObAlterResourceUnitArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterResourceUnitArg() :
      min_cpu_(0),
      min_iops_(0),
      min_memory_(0),
      max_cpu_(0),
      max_memory_(0),
      max_disk_size_(0),
      max_iops_(0),
      max_session_num_(0)
  { }
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  double min_cpu_;
  int64_t min_iops_;
  int64_t min_memory_;
  double max_cpu_;
  int64_t max_memory_;
  int64_t max_disk_size_;
  int64_t max_iops_;
  int64_t max_session_num_;
};

struct ObDropResourceUnitArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropResourceUnitArg():
    if_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  bool if_exist_;
};

struct ObCreateResourcePoolArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateResourcePoolArg():
    unit_num_(0),
    if_not_exist_(0)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  common::ObString unit_;
  int64_t unit_num_;
  common::ObSArray<common::ObZone> zone_list_;
  bool if_not_exist_;
};

struct ObAlterResourcePoolArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterResourcePoolArg()
    : pool_name_(), unit_(), unit_num_(0), zone_list_() {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  common::ObString unit_;
  int64_t unit_num_;
  common::ObSArray<common::ObZone> zone_list_;
};

struct ObDropResourcePoolArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropResourcePoolArg():
    if_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  bool if_exist_;
};

struct ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  common::ObString ddl_stmt_str_;
};

struct ObSysVarIdValue
{
  OB_UNIS_VERSION(1);
public:
  ObSysVarIdValue() : sys_id_(sql::SYS_VAR_INVALID), value_() {}
  ObSysVarIdValue(sql::ObSysVarClassType sys_id, common::ObString &value) : sys_id_(sys_id), value_(value) {}
  ~ObSysVarIdValue() {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  sql::ObSysVarClassType sys_id_;
  common::ObString value_;
};

struct ObCreateTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTenantArg() : ObDDLArg(), tenant_schema_(), pool_list_(), if_not_exist_(false), sys_var_list_() {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_schema_;
  common::ObSArray<common::ObString> pool_list_;
  bool if_not_exist_;
  common::ObSArray<ObSysVarIdValue> sys_var_list_;
};

struct ObModifyTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum ModifiableOptions {
       REPLICA_NUM = 1,
       CHARSET_TYPE,
       COLLATION_TYPE,
       PRIMARY_ZONE,
       ZONE_LIST,
       RESOURCE_POOL_LIST,
       READ_ONLY,
       COMMENT,
       REWRITE_MERGE_VERSION,
       MAX_OPTION
  };
  bool is_valid() const;
  int check_normal_tenant_can_do(bool &normal_can_do) const;

  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_schema_;
  common::ObSArray<common::ObString> pool_list_;
  //used to mark alter tenant options
  common::ObBitSet<> alter_option_bitset_;
};

struct ObLockTenantArg
{
  OB_UNIS_VERSION(1);

public:
  ObLockTenantArg():
    is_locked_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
  bool is_locked_;
};

struct ObDropTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropTenantArg():
      if_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
  bool if_exist_;
};

struct ObCreateDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateDatabaseArg():
    if_not_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  share::schema::ObDatabaseSchema database_schema_;
  //used to mark alter database options
  common::ObBitSet<> alter_option_bitset_;
  bool if_not_exist_;
};

struct ObAlterDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum ModifiableOptions {
       REPLICA_NUM = 1,
       CHARSET_TYPE,
       COLLATION_TYPE,
       PRIMARY_ZONE,
       READ_ONLY,
       DEFAULT_TABLEGROUP,
       MAX_OPTION
  };

public:
  bool is_valid() const;
  bool only_alter_primary_zone() const
  { return (1 == alter_option_bitset_.num_members()
            && alter_option_bitset_.has_member(PRIMARY_ZONE)); }
  DECLARE_TO_STRING;

  share::schema::ObDatabaseSchema database_schema_;
  //used to mark alter database options
  common::ObBitSet<> alter_option_bitset_;
};

struct ObDropDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropDatabaseArg():
    tenant_id_(common::OB_INVALID_ID),
    if_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString database_name_;
  bool if_exist_;
};

struct ObCreateTablegroupArg : ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTablegroupArg():
    if_not_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  share::schema::ObTablegroupSchema tablegroup_schema_;
  bool if_not_exist_;
};

struct ObDropTablegroupArg : ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropTablegroupArg():
    tenant_id_(common::OB_INVALID_ID),
    if_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
  bool if_exist_;
};

struct ObTableItem;
struct ObAlterTablegroupArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterTablegroupArg():
    tenant_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
  common::ObSArray<ObTableItem> table_items_;
};

struct ObCreateIndexArg;//Forward declaration
struct ObCreateTableArg : ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTableArg():
    if_not_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  bool if_not_exist_;
  share::schema::ObTableSchema schema_;
  common::ObSArray<ObCreateIndexArg> index_arg_list_;
  common::ObString db_name_;
};

struct ObCreateTableLikeArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateTableLikeArg():
      if_not_exist_(false),
      tenant_id_(common::OB_INVALID_ID),
      origin_db_name_(),
      origin_table_name_(),
      new_db_name_(),
      new_table_name_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  bool if_not_exist_;
  uint64_t tenant_id_;
  common::ObString origin_db_name_;
  common::ObString origin_table_name_;
  common::ObString new_db_name_;
  common::ObString new_table_name_;
};

struct ObIndexArg : public ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  enum IndexActionType
  {
    INVALID_ACTION = 1,
    ADD_INDEX,
    DROP_INDEX
  };

  uint64_t tenant_id_;
  common::ObString index_name_;
  common::ObString table_name_;
  common::ObString database_name_;
  IndexActionType index_action_type_;

  ObIndexArg():
      tenant_id_(common::OB_INVALID_ID),
      index_name_(),
      table_name_(),
      database_name_(),
      index_action_type_(INVALID_ACTION)
  {}
  virtual ~ObIndexArg() {}
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID,
    index_name_.reset();
    table_name_.reset();
    database_name_.reset();
    index_action_type_ = INVALID_ACTION;
  }
  bool is_valid() const;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObDropIndexArg: public ObIndexArg
{
  //if add new member,should add to_string and serialize function
public:
  ObDropIndexArg()
  {
    index_action_type_ = DROP_INDEX;
  }
  virtual ~ObDropIndexArg() {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = DROP_INDEX;
  }
  bool is_valid() const { return ObIndexArg::is_valid(); }
};

struct ObTruncateTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObTruncateTableArg():
    tenant_id_(common::OB_INVALID_ID),
    database_name_(),
    table_name_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString database_name_;
  common::ObString table_name_;
};

struct ObRenameTableItem
{
  OB_UNIS_VERSION(1);
public:
  ObRenameTableItem():
      origin_db_name_(),
      new_db_name_(),
      origin_table_name_(),
      new_table_name_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString origin_db_name_;
  common::ObString new_db_name_;
  common::ObString origin_table_name_;
  common::ObString new_table_name_;
};

struct ObRenameTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObRenameTableArg():
      tenant_id_(common::OB_INVALID_ID),
      rename_table_items_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObSArray<ObRenameTableItem> rename_table_items_;
};

struct ObAlterTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum ModifiableTableColumns {
       AUTO_INCREMENT = 1,
       BLOCK_SIZE,
       CHARSET_TYPE,
       COLLATION_TYPE,
       COMPRESS_METHOD,
       COMMENT,
       EXPIRE_INFO,
       PRIMARY_ZONE,
       REPLICA_NUM,
       PROGRESSIVE_MERGE_NUM,
       TABLE_NAME,
       TABLEGROUP_NAME,
       SEQUENCE_COLUMN_ID,
       USE_BLOOM_FILTER,
       READ_ONLY,
       PARTITION_FUNC,
       MAX_OPTION
  };
  ObAlterTableArg():
      is_alter_columns_(false),
      is_alter_indexs_(false),
      is_alter_options_(false),
      index_arg_list_(),
      alter_table_schema_()
  {}
  bool is_valid() const;
  bool has_rename_action() const
  { return alter_table_schema_.alter_option_bitset_.has_member(TABLE_NAME); }
  TO_STRING_KV(K_(is_alter_columns),
               K_(is_alter_indexs),
               K_(is_alter_options),
               K_(index_arg_list),
               K_(alter_table_schema));

  bool is_alter_columns_;
  bool is_alter_indexs_;
  bool is_alter_options_;
  common::ObSArray<ObIndexArg *> index_arg_list_;
  share::schema::AlterTableSchema alter_table_schema_;
  common::PageArena<> allocator_;
  common::ObTimeZoneInfo tz_info_;
  int serialize_index_args(char *buf, const int64_t data_len, int64_t &pos) const;
  int deserialize_index_args(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_index_args_serialize_size() const;
};

struct ObTableItem
{
  OB_UNIS_VERSION(1);
public:
  ObTableItem():
      mode_(common::OB_NAME_CASE_INVALID), //for compare
      database_name_(),
      table_name_()
  {}
  bool operator==(const ObTableItem &table_item) const;
  inline uint64_t hash(uint64_t seed = 0) const;
  void reset() {
    mode_ = common::OB_NAME_CASE_INVALID;
    database_name_.reset();
    table_name_.reset();
  }
  DECLARE_TO_STRING;

  common::ObNameCaseMode mode_;
  common::ObString database_name_;
  common::ObString table_name_;
};

inline uint64_t ObTableItem::hash(uint64_t seed) const
{
  uint64_t val = seed;
  if (!database_name_.empty() && !table_name_.empty()) {
    val = common::murmurhash(database_name_.ptr(), database_name_.length(), val);
    val = common::murmurhash(table_name_.ptr(), table_name_.length(), val);
  }
  return val;
}

struct ObDropTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropTableArg():
      tenant_id_(common::OB_INVALID_ID),
      table_type_(share::schema::MAX_TABLE_TYPE),
      tables_(),
      if_exist_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  share::schema::ObTableType table_type_;
  common::ObSArray<ObTableItem> tables_;
  bool if_exist_;
};

struct ObColumnSortItem
{
  OB_UNIS_VERSION(1);
public:
  ObColumnSortItem() : column_name_(),
                       prefix_len_(0),
                       order_type_(share::schema::ASC)
  {}
  void reset()
  {
    column_name_.reset();
    prefix_len_ = 0;
    order_type_ = share::schema::ASC;
  }
  DECLARE_TO_STRING;

  common::ObString column_name_;
  int32_t prefix_len_;
  share::schema::ObOrderType order_type_;
};

struct ObTableOption
{
  OB_UNIS_VERSION(1);
public:
  ObTableOption() :
    block_size_(-1),
    replica_num_(-1),
    index_status_(share::schema::INDEX_STATUS_UNAVAILABLE),
    use_bloom_filter_(false),
    compress_method_("none"),
    comment_(),
    progressive_merge_num_(common::OB_DEFAULT_PROGRESSIVE_MERGE_NUM),
    primary_zone_()
  {}
  void reset()
  {
    block_size_ = common::OB_DEFAULT_SSTABLE_BLOCK_SIZE;
    replica_num_ = -1;
    index_status_ = share::schema::INDEX_STATUS_UNAVAILABLE;
    use_bloom_filter_ = false;
    compress_method_ = common::ObString::make_string("none");
    comment_.reset();
    tablegroup_name_.reset();
    progressive_merge_num_ = common::OB_DEFAULT_PROGRESSIVE_MERGE_NUM;
    primary_zone_.reset();
  }
  bool is_valid() const;
  DECLARE_TO_STRING;

  int64_t block_size_;
  int64_t replica_num_;
  share::schema::ObIndexStatus index_status_;
  bool use_bloom_filter_;
  common::ObString compress_method_;
  common::ObString comment_;
  common::ObString tablegroup_name_;
  int64_t progressive_merge_num_;
  common::ObString primary_zone_;
};

struct ObCreateIndexArg : public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObCreateIndexArg(): index_type_(share::schema::INDEX_TYPE_IS_NOT),
                      index_columns_(),
                      store_columns_(),
                      index_option_()
  {
    index_action_type_ = ADD_INDEX;
    index_using_type_ = share::schema::USING_BTREE;
  }
  virtual ~ObCreateIndexArg() {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = ADD_INDEX;
    index_type_ = share::schema::INDEX_TYPE_IS_NOT;
    index_columns_.reset();
    store_columns_.reset();
    index_option_.reset();
    index_using_type_ = share::schema::USING_BTREE;
  }
  bool is_valid() const;

  DECLARE_VIRTUAL_TO_STRING;

  share::schema::ObIndexType index_type_;
  common::ObSEArray<ObColumnSortItem, common::OB_PREALLOCATED_NUM> index_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> store_columns_;
  ObTableOption index_option_;
  share::schema::ObIndexUsingType index_using_type_;
};

struct ObCreatePartitionArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionArg() { reset(); }
  ~ObCreatePartitionArg() {}
  inline void reset();
  bool is_valid() const;

  DECLARE_TO_STRING;

  common::ObZone zone_;
  common::ObPartitionKey partition_key_;
  int64_t memstore_version_;
  int64_t replica_num_;
  common::ObMemberList member_list_;
  common::ObAddr leader_;
  int64_t lease_start_;
};

struct ObCreatePartitionBatchArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionBatchArg() { reset(); }
  ~ObCreatePartitionBatchArg() {}
  bool is_valid() const;
  inline void reset();
  int assign(const ObCreatePartitionBatchArg &other);
  void reuse() { args_.reuse(); }

  DECLARE_TO_STRING;

  common::ObSArray<ObCreatePartitionArg> args_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePartitionBatchArg);
};

struct ObCreatePartitionBatchRes
{
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionBatchRes() : ret_list_() {}
  ~ObCreatePartitionBatchRes() {}
  inline void reset();
  int assign(const ObCreatePartitionBatchRes &other);
  inline void reuse();

  DECLARE_TO_STRING;
  // response includes all rets
  common::ObSArray<int> ret_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePartitionBatchRes);
};

//TODO:delete these two rpc
struct ObMigrateArg
{
  OB_UNIS_VERSION(1);

public:
  ObMigrateArg():
    keep_src_(false)
  {}
  bool is_valid() const
  {
    return partition_key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }
  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  common::ObMember src_;
  common::ObMember dst_;
  common::ObMember replace_;
  bool keep_src_;
};

struct ObMigrateOverArg
{
  OB_UNIS_VERSION(1);

public:
  ObMigrateOverArg():
    keep_src_(false),
    result_(0)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  common::ObMember src_;
  common::ObMember dst_;
  common::ObMember replace_;
  bool keep_src_;
  int64_t result_;
};

//----Structs for partition online/offline----
struct ObAddTemporaryReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAddTemporaryReplicaArg() : key_(), dst_() {}
  bool is_valid() const { return key_.is_valid() && dst_.is_valid(); }

  TO_STRING_KV(K_(key), K_(dst));

  common::ObPartitionKey key_;
  common::ObMember dst_;

};

struct ObAddTemporaryReplicaRes
{
  OB_UNIS_VERSION(1);

public:
  ObAddTemporaryReplicaRes():
    result_(0)
  {}
  bool is_valid() const { return key_.is_valid() && dst_.is_valid(); }
  TO_STRING_KV(K_(key), K_(dst), K_(result));

  common::ObPartitionKey key_;
  common::ObMember dst_;
  int64_t result_;
};

struct ObAddReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAddReplicaArg() : key_(), src_(), dst_(), is_rebuild_(false) {}
  bool is_valid() const { return key_.is_valid() && src_.is_valid() && dst_.is_valid(); }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(is_rebuild));

  common::ObPartitionKey key_;
  common::ObMember src_;
  common::ObMember dst_;
  bool is_rebuild_;
};

struct ObAddReplicaRes
{
  OB_UNIS_VERSION(1);

public:
  ObAddReplicaRes():
    result_(0)
  {}
  bool is_valid() const { return key_.is_valid() && src_.is_valid() && dst_.is_valid(); }
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(result));

  common::ObPartitionKey key_;
  common::ObMember src_;
  common::ObMember dst_;
  int64_t result_;
};

struct ObRemoveReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObRemoveReplicaArg() : key_(), dst_() {}
  bool is_valid() const { return key_.is_valid() && dst_.is_valid(); }

  TO_STRING_KV(K_(key), K_(dst));

  common::ObPartitionKey key_;
  common::ObMember dst_;
};

struct ObMigrateReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObMigrateReplicaArg() : key_(), src_(), dst_() {}
  bool is_valid() const { return key_.is_valid() && src_.is_valid() && dst_.is_valid(); }

  TO_STRING_KV(K_(key), K_(src), K_(dst));

  common::ObPartitionKey key_;
  common::ObMember src_;
  common::ObMember dst_;
};

struct ObMigrateReplicaRes
{
  OB_UNIS_VERSION(1);

public:
  ObMigrateReplicaRes():
    result_(0)
  {}
  bool is_valid() const { return key_.is_valid() && src_.is_valid() && dst_.is_valid(); }
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(result));

  common::ObPartitionKey key_;
  common::ObMember src_;
  common::ObMember dst_;
  int64_t result_;
};
//----End structs for partition online/offline----

//----Structs for managing privileges----

struct ObMajorFreezeArg
{
  OB_UNIS_VERSION(1);

public:
  ObMajorFreezeArg():
    frozen_version_(0),
    schema_version_(0),
    frozen_timestamp_(0)
  {}
  inline void reset();
  inline void reuse();
  bool is_valid() const;
  DECLARE_TO_STRING;

  int64_t frozen_version_;
  int64_t schema_version_;
  int64_t frozen_timestamp_;

private:
  //DISALLOW_COPY_AND_ASSIGN(ObMajorFreezeArg);
};

struct ObQueryMajorFreezeStatusArg
{
  OB_UNIS_VERSION(1);

public:
  ObQueryMajorFreezeStatusArg() : partitions_(), frozen_version_(0) {}
  inline void reset();
  inline void reuse();
  bool is_valid() const;
  int assign(const ObQueryMajorFreezeStatusArg &other);
  DECLARE_TO_STRING;

  ObPartitionList partitions_;
  int64_t frozen_version_;

private:
  //DISALLOW_COPY_AND_ASSIGN(ObQueryMajorFreezeStatusArg);
};

struct ObMajorFreezeStatus
{
  OB_UNIS_VERSION(1);

public:
  ObMajorFreezeStatus():
    status_(0),
    timestamp_(0)
  {}
  inline void reset();
  DECLARE_TO_STRING;

  int64_t status_;
  int64_t timestamp_;
};

typedef common::ObSArray<ObMajorFreezeStatus> ObFreezeStatusList;

struct ObQueryMajorFreezeStatusResult
{
  OB_UNIS_VERSION(1);

public:
  ObQueryMajorFreezeStatusResult() : statuses_() {}
  inline void reset();
  inline void reuse();
  DECLARE_TO_STRING;

  ObFreezeStatusList statuses_;
};

struct ObSwitchLeaderArg
{
  OB_UNIS_VERSION(1);

public:
  ObSwitchLeaderArg() : partition_key_(), leader_addr_() {}
  ~ObSwitchLeaderArg() {}
  inline void reset();
  bool is_valid() const { return partition_key_.is_valid() && leader_addr_.is_valid(); }

  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  common::ObAddr leader_addr_;
};

struct ObGetLeaderCandidatesArg
{
  OB_UNIS_VERSION(1);

public:
  ObGetLeaderCandidatesArg() : partitions_() {}
  void reuse() { partitions_.reuse(); }
  bool is_valid() const { return partitions_.count() > 0; }

  TO_STRING_KV(K_(partitions));

  ObPartitionList partitions_;
};

struct ObGetLeaderCandidatesResult
{
  OB_UNIS_VERSION(1);

public:
  ObGetLeaderCandidatesResult() : candidates_() {}
  void reuse() { candidates_.reuse(); }
  TO_STRING_KV(K_(candidates));

  common::ObSArray<ObServerList> candidates_;
};

//----Structs for managing privileges----
struct ObCreateUserArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateUserArg() : tenant_id_(common::OB_INVALID_ID)//, user_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_infos));

  uint64_t tenant_id_;
  common::ObSArray<share::schema::ObUserInfo> user_infos_;
};

struct ObDropUserArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropUserArg() : tenant_id_(common::OB_INVALID_ID)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(users));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> users_;
};

struct ObRenameUserArg
{
  OB_UNIS_VERSION(1);

public:
  ObRenameUserArg() : tenant_id_(common::OB_INVALID_ID)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(origin_names), K_(to_names));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> origin_names_;
  common::ObSArray<common::ObString> to_names_;
};

struct ObSetPasswdArg
{
  OB_UNIS_VERSION(1);

public:
  ObSetPasswdArg() : tenant_id_(common::OB_INVALID_ID)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_name), K_(passwd));

  uint64_t tenant_id_;
  common::ObString user_name_;
  common::ObString passwd_;
};

struct ObLockUserArg
{
  OB_UNIS_VERSION(1);

public:
  ObLockUserArg() : tenant_id_(common::OB_INVALID_ID), locked_(false)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_names), K_(locked));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> user_names_;
  bool locked_;
};

struct ObGrantArg
{
  OB_UNIS_VERSION(1);

public:
  ObGrantArg() : tenant_id_(common::OB_INVALID_ID), priv_level_(share::schema::OB_PRIV_INVALID_LEVEL),
                 priv_set_(0), need_create_user_(false), has_create_user_priv_(false)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(priv_level), K_(db), K_(table), K_(priv_set),
               K_(users), K_(need_create_user), K_(has_create_user_priv));

  uint64_t tenant_id_;
  share::schema::ObPrivLevel priv_level_;
  common::ObString db_;
  common::ObString table_;
  ObPrivSet priv_set_;
  common::ObSArray<common::ObString> users_;//user_name1, pwd1, user_name2, pwd2
  bool need_create_user_;
  bool has_create_user_priv_;
};


struct ObRevokeUserArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeUserArg() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                      priv_set_(0), revoke_all_(false)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_),
               K_(revoke_all));

  uint64_t tenant_id_;
  uint64_t user_id_;
  ObPrivSet priv_set_;
  bool revoke_all_;
};

struct ObRevokeDBArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeDBArg() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                         priv_set_(0)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(db),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  ObPrivSet priv_set_;
};

struct ObRevokeTableArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeTableArg() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                            priv_set_(0), grant_(true)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(db),
               K_(table),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString  db_;
  common::ObString table_;
  ObPrivSet priv_set_;
  bool grant_;
};
//----End of structs for managing privileges----

// system admin (alter system ...) rpc argument define
struct ObAdminServerArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminServerArg() : servers_(), zone_() {}
  ~ObAdminServerArg() {}
  // zone can be empty, so don't check it
  bool is_valid() const { return servers_.count() > 0; }
  TO_STRING_KV(K_(servers), K_(zone));

  ObServerList servers_;
  common::ObZone zone_;
};

struct ObAdminZoneArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminZoneArg() : zone_() {}
  ~ObAdminZoneArg() {}

  bool is_valid() const { return !zone_.is_empty(); }
  TO_STRING_KV(K_(zone));

  common::ObZone zone_;
};

struct ObAdminSwitchReplicaRoleArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminSwitchReplicaRoleArg()
      : role_(common::FOLLOWER), partition_key_(), server_(), zone_(), tenant_name_() {}
  ~ObAdminSwitchReplicaRoleArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(role), K_(partition_key), K_(server), K_(zone), K_(tenant_name));

  common::ObRole role_;
  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  common::ObZone zone_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSwitchRSRoleArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminSwitchRSRoleArg()
      : role_(common::FOLLOWER), server_(), zone_() {}
  ~ObAdminSwitchRSRoleArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(role), K_(server), K_(zone));

  common::ObRole role_;
  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminDropReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminDropReplicaArg()
      : partition_key_(), server_(), zone_(), create_timestamp_(0) {}
  ~ObAdminDropReplicaArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(partition_key), K_(server), K_(zone), K_(create_timestamp));

  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  common::ObZone zone_;
  int64_t create_timestamp_;
};

struct ObAdminMigrateReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminMigrateReplicaArg()
      : is_copy_(false), partition_key_(), src_(), dest_() {}
  ~ObAdminMigrateReplicaArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(is_copy), K_(partition_key), K_(src), K_(dest));

  bool is_copy_;
  common::ObPartitionKey partition_key_;
  common::ObAddr src_;
  common::ObAddr dest_;
};

struct ObServerZoneArg
{
  OB_UNIS_VERSION(1);

public:
  ObServerZoneArg() : server_(), zone_() {}

  // server can be invalid, zone can be empty
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(server), K_(zone));

  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminReportReplicaArg : public ObServerZoneArg
{
};

struct ObAdminRecycleReplicaArg : public ObServerZoneArg
{
};

struct ObAdminRefreshSchemaArg : public ObServerZoneArg
{
};

struct ObAdminClearLocationCacheArg : public ObServerZoneArg
{
};

struct ObRunJobArg : public ObServerZoneArg
{
  OB_UNIS_VERSION(1);

public:
  ObRunJobArg() : ObServerZoneArg(), job_() {}

  bool is_valid() const { return ObServerZoneArg::is_valid() && !job_.empty(); }
  TO_STRING_KV(K_(server), K_(zone), K_(job));

  common::ObString job_;
};

struct ObAdminMergeArg
{
  OB_UNIS_VERSION(1);

public:
  enum Type {
    START_MERGE = 1,
    SUSPEND_MERGE = 2,
    RESUME_MERGE = 3,
  };

  ObAdminMergeArg() : type_(START_MERGE), zone_() {}
  bool is_valid() const;
  TO_STRING_KV(K_(type), K_(zone));

  Type type_;
  common::ObZone zone_;
};

struct ObAdminClearRoottableArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminClearRoottableArg() : tenant_name_() {}

  // tenant_name be empty means all tenant
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(tenant_name));

  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSetConfigItem
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(name), K_(value), K_(comment), K_(zone), K_(server), K_(tenant_name));

  common::ObFixedLengthString<common::OB_MAX_CONFIG_NAME_LEN> name_;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> value_;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_INFO_LEN> comment_;
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSetConfigArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminSetConfigArg() : items_() {}
  ~ObAdminSetConfigArg() {}

  bool is_valid() const { return items_.count() > 0; }
  TO_STRING_KV(K_(items));

  common::ObSArray<ObAdminSetConfigItem> items_;
};

struct ObAdminMigrateUnitArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminMigrateUnitArg():
    unit_id_(0), destination_()
  {}
  ~ObAdminMigrateUnitArg() {}

  bool is_valid() const { return common::OB_INVALID_ID != unit_id_ && destination_.is_valid(); }
  TO_STRING_KV(K_(unit_id), K_(destination));

  uint64_t unit_id_;
  common::ObAddr destination_;
};

struct ObAutoincSyncArg
{
  OB_UNIS_VERSION(1);

public:
  ObAutoincSyncArg()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      column_id_(common::OB_INVALID_ID),
      table_part_num_(0),
      auto_increment_(0)
  {}
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(column_id), K_(table_part_num), K_(auto_increment));

  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  uint64_t table_part_num_;
  uint64_t auto_increment_;  // only for sync table option auto_increment
};

struct ObDropReplicaArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropReplicaArg() : partition_key_(), member_() {}
  bool is_valid() const { return partition_key_.is_valid() && member_.is_valid(); }

  TO_STRING_KV(K_(partition_key), K_(member));

  common::ObPartitionKey partition_key_;
  common::ObMember member_;
};

struct ObUpdateIndexStatusArg
{
  OB_UNIS_VERSION(1);
public:
  ObUpdateIndexStatusArg():
    index_table_id_(common::OB_INVALID_ID),
    status_(share::schema::INDEX_STATUS_MAX),
    create_mem_version_(0)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(index_table_id), K_(status), K_(create_mem_version));

  uint64_t index_table_id_;
  share::schema::ObIndexStatus status_;
  int64_t create_mem_version_;
};

struct ObMergeFinishArg
{
  OB_UNIS_VERSION(1);
public:
  ObMergeFinishArg():
    frozen_version_(0)
  {}

  bool is_valid() const { return server_.is_valid() && frozen_version_ > 0; }
  TO_STRING_KV(K_(server), K_(frozen_version));

  common::ObAddr server_;
  int64_t frozen_version_;
};

struct ObMergeErrorArg
{
  OB_UNIS_VERSION(1);
public:
  ObMergeErrorArg():
    error_code_(0)
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(partition_key), K_(server), K_(error_code));

  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  int error_code_;
};

struct ObRebuildReplicaArg
{
  OB_UNIS_VERSION(1);
public:
  ObRebuildReplicaArg() : key_(), server_() {}

  bool is_valid() const { return key_.is_valid() && server_.is_valid(); }
  TO_STRING_KV(K_(key), K_(server));

  common::ObPartitionKey key_;
  common::ObAddr server_;
};

struct ObDebugSyncActionArg
{
  OB_UNIS_VERSION(1);
public:
  ObDebugSyncActionArg():
    reset_(false),
    clear_(false)
  {}

  bool is_valid() const { return reset_ || clear_ || action_.is_valid(); }
  TO_STRING_KV(K_(reset), K_(clear), K_(action));

  bool reset_;
  bool clear_;
  common::ObDebugSyncAction action_;
};

struct ObRootMajorFreezeArg
{
  OB_UNIS_VERSION(1);
public:
  ObRootMajorFreezeArg() : try_frozen_version_(0), launch_new_round_(false) {}
  inline void reset();
  inline bool is_valid() const { return try_frozen_version_ >= 0; }
  TO_STRING_KV(K_(try_frozen_version), K_(launch_new_round));

  int64_t try_frozen_version_;
  bool launch_new_round_;
};

struct ObSyncPartitionTableFinishArg
{
  OB_UNIS_VERSION(1);
public:
  ObSyncPartitionTableFinishArg() : server_(), version_(0) {}

  inline bool is_valid() const { return server_.is_valid() && version_ > 0; }
  TO_STRING_KV(K_(server), K_(version));

  common::ObAddr server_;
  int64_t version_;
};

struct ObMemberListAndLeaderArg
{
  OB_UNIS_VERSION(1);
public:
  ObMemberListAndLeaderArg() : member_list_(), leader_(), self_() {}
  void reset();
  inline bool is_valid() const { return member_list_.count()  > 0 && self_.is_valid(); }
  TO_STRING_KV(K_(member_list), K_(leader), K_(self));

  common::ObSEArray<common::ObAddr, common::OB_MAX_MEMBER_NUMBER,
        common::ObNullAllocator, common::ObArrayDefaultCallBack<common::ObAddr>,
        common::DefaultItemEncode<common::ObAddr> > member_list_; // copy won't fail
  common::ObAddr leader_;
  common::ObAddr self_;
};

struct ObSwitchLeaderListArg
{
  OB_UNIS_VERSION(1);

public:
  ObSwitchLeaderListArg() : partition_key_list_(), leader_addr_() {}
  ~ObSwitchLeaderListArg() {}
  inline void reset() { partition_key_list_.reset(); leader_addr_.reset(); }
  bool is_valid() const { return partition_key_list_.count() > 0 && leader_addr_.is_valid(); }

  TO_STRING_KV(K_(leader_addr), K_(partition_key_list));;

  common::ObSArray<common::ObPartitionKey> partition_key_list_;
  common::ObAddr leader_addr_;
};

struct ObGetPartitionCountResult
{
  OB_UNIS_VERSION(1);

public:
  ObGetPartitionCountResult() : partition_count_(0) {}
  void reset() { partition_count_ = 0; }
  TO_STRING_KV(K_(partition_count));

  int64_t partition_count_;
};

inline void Int64::reset()
{
  v_ = common::OB_INVALID_ID;
}

inline void ObCreatePartitionArg::reset()
{
  zone_.reset();
  partition_key_.reset();
  memstore_version_ = 0;
  replica_num_ = 0;
  member_list_.reset();
  leader_.reset();
  lease_start_ = 0;
}

inline void ObCreatePartitionBatchArg::reset()
{
  args_.reset();
}

inline void ObCreatePartitionBatchRes::reset()
{
  ret_list_.reset();
}

inline void ObCreatePartitionBatchRes::reuse()
{
  ret_list_.reuse();
}

inline void ObMajorFreezeArg::reset()
{
  frozen_version_ = 0;
  schema_version_ = 0;
  frozen_timestamp_ = 0;
}

inline void ObMajorFreezeArg::reuse()
{
  frozen_version_ = 0;
  schema_version_ = 0;
  frozen_timestamp_ = 0;
}

inline void ObQueryMajorFreezeStatusArg::reset()
{
  partitions_.reset();
  frozen_version_ = 0;
}

inline void ObQueryMajorFreezeStatusArg::reuse()
{
  partitions_.reuse();
  frozen_version_ = 0;
}

inline void ObMajorFreezeStatus::reset()
{
  status_ = 0;
  timestamp_ = 0;
}

inline void ObQueryMajorFreezeStatusResult::reset()
{
  statuses_.reset();
}

inline void ObQueryMajorFreezeStatusResult::reuse()
{
  statuses_.reuse();
}

inline void ObSwitchLeaderArg::reset()
{
  partition_key_.reset();
  leader_addr_.reset();
}

inline void ObRootMajorFreezeArg::reset()
{
  try_frozen_version_ = 0;
  launch_new_round_ = false;
}

struct ObCreateOutlineArg : ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateOutlineArg(): or_replace_(false), outline_info_(), db_name_() {}
  virtual ~ObCreateOutlineArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(or_replace), K_(outline_info), K_(db_name));

  bool or_replace_;
  share::schema::ObOutlineInfo outline_info_;
  common::ObString db_name_;
};

struct ObDropOutlineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropOutlineArg(): tenant_id_(common::OB_INVALID_ID), db_name_(), outline_name_() {}
  virtual ~ObDropOutlineArg() {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString outline_name_;
};

struct ObFetchAliveServerArg
{
  OB_UNIS_VERSION(1);
public:
  ObFetchAliveServerArg() : cluster_id_(0) {}
  TO_STRING_KV(K_(cluster_id));
  bool is_valid() const { return cluster_id_ >= 0; }

  int64_t cluster_id_;
};

struct ObFetchAliveServerResult
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(server_list));
  bool is_valid() const { return !server_list_.empty(); }

  ObServerList server_list_;
};

}//end namespace obrpc
}//end namespace oceanbase
#endif
