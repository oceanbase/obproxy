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

#ifndef _OB_OCEANBAE_SCHEMA_SCHEMA_SERVICE_H
#define _OB_OCEANBAE_SCHEMA_SCHEMA_SERVICE_H

#include "lib/ob_define.h"
#include "lib/ob_name_def.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "common/ob_hint.h"
#include "common/ob_object.h"
#include "common/ob_obj_cast.h"
#include "common/mysql_proxy/ob_isql_client.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace common
{
class ObScanHelper;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObISQLClient;
}
namespace share
{
namespace schema
{
#define OP_TYPE_DEF(ACT)                                         \
  ACT(OB_INVALID_DDL_OP, = 0)                                    \
  ACT(OB_DDL_TABLE_OPERATION_BEGIN, = 1)                         \
  ACT(OB_DDL_DROP_TABLE, = 2)                                        \
  ACT(OB_DDL_ALTER_TABLE, = 3)                                       \
  ACT(OB_DDL_CREATE_TABLE, = 4)                                      \
  ACT(OB_DDL_ADD_COLUMN, = 5)                                        \
  ACT(OB_DDL_DROP_COLUMN, = 6)                                       \
  ACT(OB_DDL_CHANGE_COLUMN, = 7)                                     \
  ACT(OB_DDL_MODIFY_COLUMN, = 8)                                     \
  ACT(OB_DDL_ALTER_COLUMN, = 9)                                      \
  ACT(OB_DDL_MODIFY_META_TABLE_ID, = 10)                              \
  ACT(OB_DDL_KILL_INDEX,)                                        \
  ACT(OB_DDL_STOP_INDEX_WRITE,)                                  \
  ACT(OB_DDL_MODIFY_INDEX_STATUS,)                               \
  ACT(OB_DDL_MODIFY_TABLE_SCHEMA_VERSION, = 14)                        \
  ACT(OB_DDL_MODIFY_TABLE_OPTION,)                               \
  ACT(OB_DDL_TABLE_RENAME,)                                      \
  ACT(OB_DDL_MODIFY_DATA_TABLE_INDEX,)                           \
  ACT(OB_DDL_DROP_INDEX,)                                        \
  ACT(OB_DDL_DROP_VIEW,)                                         \
  ACT(OB_DDL_CREATE_INDEX,)                                      \
  ACT(OB_DDL_CREATE_VIEW,)                                       \
  ACT(OB_DDL_ALTER_TABLEGROUP_ADD_TABLE,)                        \
  ACT(OB_DDL_TRUNCATE_TABLE_CREATE,)                             \
  ACT(OB_DDL_TRUNCATE_TABLE_DROP,)                               \
  ACT(OB_DDL_TABLE_OPERATION_END,)                               \
  ACT(OB_DDL_TENANT_OPERATION_BEGIN, = 101)                      \
  ACT(OB_DDL_ADD_TENANT,)                                        \
  ACT(OB_DDL_ALTER_TENANT,)                                      \
  ACT(OB_DDL_DEL_TENANT,)                                        \
  ACT(OB_DDL_TENANT_OPERATION_END,)                              \
  ACT(OB_DDL_DATABASE_OPERATION_BEGIN, = 201)                    \
  ACT(OB_DDL_ADD_DATABASE,)                                      \
  ACT(OB_DDL_ALTER_DATABASE,)                                    \
  ACT(OB_DDL_DEL_DATABASE,)                                      \
  ACT(OB_DDL_RENAME_DATABASE,)                                   \
  ACT(OB_DDL_DATABASE_OPERATION_END,)                            \
  ACT(OB_DDL_TABLEGROUP_OPERATION_BEGIN, = 301)                  \
  ACT(OB_DDL_ADD_TABLEGROUP,)                                    \
  ACT(OB_DDL_DEL_TABLEGROUP,)                                    \
  ACT(OB_DDL_RENAME_TABLEGROUP,)                                 \
  ACT(OB_DDL_TABLEGROUP_OPERATION_END,)                          \
  ACT(OB_DDL_USER_OPERATION_BEGIN, = 401)                        \
  ACT(OB_DDL_CREATE_USER,)                                       \
  ACT(OB_DDL_DROP_USER,)                                         \
  ACT(OB_DDL_RENAME_USER,)                                       \
  ACT(OB_DDL_LOCK_USER,)                                         \
  ACT(OB_DDL_SET_PASSWD,)                                        \
  ACT(OB_DDL_GRANT_REVOKE_USER,)                                 \
  ACT(OB_DDL_USER_OPERATION_END,)                                \
  ACT(OB_DDL_DB_PRIV_OPERATION_BEGIN, = 501)                     \
  ACT(OB_DDL_GRANT_REVOKE_DB,)                                   \
  ACT(OB_DDL_DEL_DB_PRIV,)                                       \
  ACT(OB_DDL_DB_PRIV_OPERATION_END,)                             \
  ACT(OB_DDL_TABLE_PRIV_OPERATION_BEGIN, = 601)                  \
  ACT(OB_DDL_GRANT_REVOKE_TABLE,)                                \
  ACT(OB_DDL_DEL_TABLE_PRIV,)                                    \
  ACT(OB_DDL_TABLE_PRIV_OPERATION_END,)                          \
  ACT(OB_DDL_OUTLINE_OPERATION_BEGIN, = 701)                     \
  ACT(OB_DDL_CREATE_OUTLINE,)                                    \
  ACT(OB_DDL_UPDATE_OUTLINE,)                                    \
  ACT(OB_DDL_DROP_OUTLINE,)                                      \
  ACT(OB_DDL_OUTLINE_OPERATION_END,)                             \
  ACT(OB_DDL_MAX_OP,)

DECLARE_ENUM(ObSchemaOperationType, op_type, OP_TYPE_DEF);

struct ObSchemaOperation
{

  ObSchemaOperation();
  int64_t  schema_version_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  common::ObString database_name_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  common::ObString table_name_;
  ObSchemaOperationType op_type_;
  common::ObString ddl_stmt_str_;
  uint64_t outline_id_;

  bool operator <(const ObSchemaOperation &rv) const { return schema_version_ < rv.schema_version_; }
  void reset();
  bool is_valid() const;
  static const char *type_str(ObSchemaOperationType op_type);
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

struct AlterColumnSchema : public ObColumnSchemaV2
{
  OB_UNIS_VERSION_V(1);
public:
  AlterColumnSchema()
    : ObColumnSchemaV2(),
      alter_type_(OB_INVALID_DDL_OP),
      origin_column_name_(),
      is_primary_key_(false),
      is_autoincrement_(false),
      is_unique_key_(false),
      is_drop_default_(false),
      is_set_nullable_(false),
      is_set_default_(false),
      check_timestamp_column_order_(false),
      is_no_zero_date_(false)
  {}

  AlterColumnSchema(common::ObIAllocator *allocator)
    : ObColumnSchemaV2(allocator),
      alter_type_(OB_INVALID_DDL_OP),
      origin_column_name_(),
      is_primary_key_(false),
      is_autoincrement_(false),
      is_unique_key_(false),
      is_drop_default_(false),
      is_set_nullable_(false),
      is_set_default_(false),
      check_timestamp_column_order_(false),
      is_no_zero_date_(false)
  {}
  AlterColumnSchema &operator=(const AlterColumnSchema &alter_column_schema);
  const common::ObString& get_origin_column_name() const { return origin_column_name_;};
  int set_origin_column_name(const common::ObString& origin_column_name)
    { return deep_copy_str(origin_column_name, origin_column_name_); }
  void reset();

  ObSchemaOperationType alter_type_;
  common::ObString origin_column_name_;
  bool is_primary_key_;
  bool is_autoincrement_;
  bool is_unique_key_;
  bool is_drop_default_;
  bool is_set_nullable_;
  bool is_set_default_;
  bool check_timestamp_column_order_;
  bool is_no_zero_date_;
  DECLARE_VIRTUAL_TO_STRING;
};

struct AlterTableSchema : public ObTableSchema
{
  OB_UNIS_VERSION_V(1);
public:
  AlterTableSchema()
    : ObTableSchema(),
      alter_type_(OB_INVALID_DDL_OP),
      origin_table_name_(),
      new_database_name_(),
      origin_database_name_(),
      origin_tablegroup_id_(common::OB_INVALID_ID),
      alter_option_bitset_()
  {
  }
  inline const common::ObString &get_origin_table_name() const { return origin_table_name_; }
  inline int set_origin_table_name(const common::ObString &origin_table_name);
  inline const common::ObString &get_database_name() const { return new_database_name_; }
  inline int set_database_name(const common::ObString &db_name);
  inline const common::ObString &get_origin_database_name() const { return origin_database_name_;}
  inline int set_origin_database_name(const common::ObString &origin_db_name);
  inline void set_origin_tablegroup_id(const uint64_t origin_tablegroup_id);
  void reset();

  ObSchemaOperationType alter_type_;
  //original table name
  common::ObString origin_table_name_;
  common::ObString new_database_name_;
  common::ObString origin_database_name_;
  uint64_t origin_tablegroup_id_;
  common::ObBitSet<> alter_option_bitset_;

  int add_column(const AlterColumnSchema &column);
  int deserialize_columns(const char *buf, const int64_t data_len, int64_t &pos);

  DECLARE_VIRTUAL_TO_STRING;
};

int AlterTableSchema::set_origin_table_name(const common::ObString &origin_table_name)
{
  return deep_copy_str(origin_table_name, origin_table_name_);
}

int AlterTableSchema::set_database_name(const common::ObString &db_name)
{
  return deep_copy_str(db_name, new_database_name_);
}

int AlterTableSchema::set_origin_database_name(const common::ObString &origin_db_name)
{
  return deep_copy_str(origin_db_name, origin_database_name_);
}

void AlterTableSchema::set_origin_tablegroup_id(const uint64_t origin_tablegroup_id)
{
  origin_tablegroup_id_ = origin_tablegroup_id;
}


// new cache
class SchemaUpdateInfo;
typedef common::ObIArray<SchemaUpdateInfo> SchemaUpdateInfos;
class IdVersion;
typedef common::ObIArray<IdVersion> IdVersions;
//table schema service interface layer
class ObSchemaService
{
public:
  typedef common::ObArray<ObSchemaOperation>  ObSchemaOperationSet;
  class SchemaOperationSetWithAlloc: public ObSchemaOperationSet
  {
  public:
    SchemaOperationSetWithAlloc() : string_buf_(common::ObModIds::OB_SCHEMA_OPERATOR_SET_WITH_ALLOC) { }
    virtual ~SchemaOperationSetWithAlloc() { }
    virtual void reset() {
      ObSchemaOperationSet::reset();
      string_buf_.reset();
    }
    int write_string(const common::ObString &str, common::ObString *stroed_str)
    { return string_buf_.write_string(str, stroed_str);}
    virtual void *alloc(const int64_t sz) { return string_buf_.alloc(sz); }
    virtual void free(void *ptr) { string_buf_.free(ptr); ptr = NULL; }

  private:
    common::ObStringBuf string_buf_;//alloc varchar
  };
  virtual ~ObSchemaService() {}
  virtual int init(common::ObMySQLProxy *client_proxy) = 0;
  // when rootservice start, invoke this function
  virtual void set_schema_version(const int64_t schema_version) = 0;

  //get all core table schema
  virtual int get_all_core_table_schema(ObTableSchema &table_schema) = 0;

  //get core table schemas
  virtual int get_core_table_schemas(const common::ObArray<uint64_t> &table_ids,
                                     common::ObISQLClient *sql_client,
                                     common::ObArray<ObTableSchema> &core_schemas) = 0;

  virtual int get_sys_table_schemas(const common::ObIArray<uint64_t> &table_ids,
                                    common::ObISQLClient *sql_client,
                                    common::ObIAllocator &allocator,
                                    common::ObArray<ObTableSchema *> &sys_schemas) = 0;

  virtual int get_tenant_max_used_table_id(const uint64_t tenant_id,
                                           const int64_t schema_version,
                                           common::ObISQLClient &sql_client,
                                           uint64_t &max_used_table_id) = 0;

  virtual int get_tenant_max_used_table_ids(const common::ObIArray<uint64_t> &tenant_ids,
                                            const int64_t schema_version,
                                            common::ObISQLClient *sql_client,
                                            common::ObIArray<uint64_t> &max_used_tids) = 0;

  //get table schema of a table id list with schema_version
  virtual int get_batch_table_schema(const int64_t schema_version,
                                     common::ObArray<uint64_t> &table_ids,
                                     common::ObISQLClient *sql_client,
                                     common::ObIAllocator &allocator,
                                     common::ObArray<ObTableSchema *> &table_schema_array) = 0;
  virtual int get_batch_table_schema(common::ObISQLClient *sql_client,
                                     SchemaUpdateInfos &table_infos) = 0;

  //get all table schema with schema_version
  virtual int get_all_table_schema(const int64_t schema_version,
                                   const uint64_t tenant_id,
                                   common::ObISQLClient *sql_client,
                                   common::ObIAllocator &allocator,
                                   common::ObArray<ObTableSchema *> &table_schema_array) = 0;
  virtual int get_all_table_schema(const int64_t schema_version,
                                   const uint64_t tenant_id,
                                   common::ObISQLClient *sql_client,
                                   common::ObIAllocator &allocator,
                                   common::ObArray<ObSimpleTableSchema> &table_schema_array) = 0;
  virtual int get_all_table_schema(const int64_t schema_version,
                                   const uint64_t tenant_id,
                                   common::ObISQLClient *sql_client,
                                   SchemaUpdateInfos &table_info) = 0;

  //get increment schema operation between (base_version, new_schema_version]
  virtual int get_increment_schema_operations(const int64_t base_version,
                                              const int64_t new_schema_version,
                                              common::ObISQLClient *sql_client,
                                              SchemaOperationSetWithAlloc &schema_operations) = 0;

  virtual int check_sys_schema_change(common::ObISQLClient &sql_client,
                                      const common::ObIArray<uint64_t> &sys_table_ids,
                                      const int64_t schema_version,
                                      const int64_t new_schema_version,
                                      bool &sys_schema_change) = 0;
  virtual int get_table_schema_version(common::ObISQLClient &sql_client,
                                       const int64_t schema_version,
                                       const uint64_t table_id,
                                       int64_t &table_schema_version) = 0;
  //get database schema of a database id list with schema_version
  virtual int get_batch_database_schema(const int64_t schema_version,
                                        common::ObArray<uint64_t> &db_ids,
                                        common::ObISQLClient *sql_client,
                                        common::ObArray<ObDatabaseSchema> &db_schema_array) = 0;
  virtual int get_batch_database_schema(common::ObISQLClient *sql_client,
                                        SchemaUpdateInfos &db_infos) = 0;

  //get all database schema with schema_version
  virtual int get_all_database_schema(const int64_t schema_version,
                                      const uint64_t tenant_id,
                                      common::ObISQLClient *sql_client,
                                      common::ObArray<ObDatabaseSchema> &db_schema_array) = 0;
  virtual int get_all_database_schema(const int64_t schema_version,
                                      const uint64_t tenant_id,
                                      common::ObISQLClient *sql_client,
                                      SchemaUpdateInfos &database_info) = 0;

  //get tablegroup schema of a tablegroup id list with schema_version
  virtual int get_batch_tablegroup_schema(const int64_t schema_version,
                                          common::ObArray<uint64_t> &tg_ids,
                                          common::ObISQLClient *sql_client,
                                          common::ObArray<ObTablegroupSchema> &tg_schema_array) = 0;
  virtual int get_batch_tablegroup_schema(common::ObISQLClient *sql_client,
                                          SchemaUpdateInfos &tg_infos) = 0;

  //get all tablegroup schema with schema_version
  virtual int get_all_tablegroup_schema(const int64_t schema_version,
                                        const uint64_t tenant_id,
                                        common::ObISQLClient *sql_client,
                                        common::ObArray<ObTablegroupSchema> &tg_schema_array) = 0;
  virtual int get_all_tablegroup_schema(const int64_t schema_version,
                                        const uint64_t tenant_id,
                                        common::ObISQLClient *sql_client,
                                        SchemaUpdateInfos &tablegroup_info) = 0;

  virtual int get_all_tenant_ids(const int64_t schema_version,
                                 common::ObISQLClient &sql_client,
                                 common::ObIArray<uint64_t> &tenant_ids) = 0;

  virtual int get_tenant_id(const common::ObString &tenant_name,
                            uint64_t &tenant_id) = 0;

  virtual int get_all_tenant_schema(const int64_t schema_version,
                                    common::ObISQLClient &client,
                                    common::ObArray<ObTenantSchema> &tenant_schema_array) = 0;
  virtual int get_all_tenant_schema(const int64_t schema_version,
                                    common::ObISQLClient &client,
                                    SchemaUpdateInfos &tenant_infos) = 0;

  virtual int get_tenant_schema(const int64_t schema_version,
                                common::ObISQLClient &client,
                                const uint64_t tenant_id,
                                ObTenantSchema &tenant_schema) = 0;

  virtual int get_tenant_schema_by_ids(const int64_t schema_version,
                                       common::ObISQLClient &client,
                                       const common::ObIArray<uint64_t> &tenant_ids,
                                       common::ObArray<ObTenantSchema> &tenant_schema_array) = 0;

  virtual int get_schema_version(common::ObISQLClient &sql_client,
      const int64_t frozen_version, int64_t &schema_version) = 0;

  virtual int restore_table_schema(common::ObArray<ObTableSchema> &table_schema_array,
                                   const char *filename,
                                   int64_t &schema_version) = 0;

  //---------ddl related---------//
  virtual int create_table(const ObTableSchema &table_schema,
                           common::ObISQLClient *sql_client,
                           const common::ObString *ddl_stmt_str = NULL,
                           const bool need_sync_schema_version = true,
                           const bool is_truncate_table = false) = 0;
  virtual int drop_table(const ObTableSchema &table_schema,
                         common::ObISQLClient *sql_client,
                         const common::ObString *ddl_stmt_str = NULL,
                         const bool is_truncate_table = false) = 0;
  //update table option
  virtual int update_table_options(common::ObISQLClient &sql_client,
                                   const ObTableSchema &table_schema,
                                   ObTableSchema &new_table_schema,
                                   share::schema::ObSchemaOperationType operation_type,
                                   const common::ObString *ddl_stmt_str = NULL) = 0;
  //alter table add column
  virtual int insert_single_column(common::ObISQLClient &sql_client,
                                   const ObTableSchema &new_table_schema,
                                   const ObColumnSchemaV2 &new_column_schema) = 0;
  //alter table modify column change column alter column
  virtual int update_single_column(common::ObISQLClient &sql_client,
                                   const ObTableSchema &origin_table_schema,
                                   const ObTableSchema &new_table_schema,
                                   const ObColumnSchemaV2 &column_schema) = 0;
  //alter table drop column
  virtual int delete_single_column(common::ObISQLClient &sql_client,
                                   const ObTableSchema &table_schema,
                                   const ObColumnSchemaV2 &column_schema) = 0;
  //update schema version and max used_column_id
  virtual int update_table_attribute(common::ObISQLClient &sql_client,
                                     ObTableSchema &new_table_schema,
                                     const common::ObString *ddl_stmt_str = NULL) = 0;
  //decrease rowkey postion when delete rowkey column of index table
  virtual int decrease_index_rowkey_position(common::ObISQLClient &sql_client,
                                             const ObColumnSchemaV2 &column_schema) = 0;
  //decrease rowkey_count when delete rowkey column of index table
  virtual int decrease_index_rowkey_column_num(const uint64_t table_id,
                                         const uint64_t rowkey_column_num,
                                         const uint64_t index_column_num,
                                         common::ObISQLClient &sql_client) = 0;

  virtual int delete_table(const uint64_t &table_id,
                           common::ObISQLClient *sql_client) = 0;

  virtual int insert_tenant(const ObTenantSchema &tenant_schema,
                            common::ObISQLClient *sql_client,
                            const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int delete_tenant(const uint64_t tenant_id,
                            common::ObISQLClient *sql_client,
                            const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int delete_tenant_content(common::ObISQLClient &client,
                                    const uint64_t tenant_id,
                                    const char *table_name) = 0;
  virtual int alter_tenant(const ObTenantSchema &tenant_schema,
                           common::ObISQLClient *sql_client,
                           const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int insert_database(const ObDatabaseSchema &database_schema,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int update_database(const ObDatabaseSchema &database_schema,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int delete_database(const uint64_t tenant_id,
                              const uint64_t database_id,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int insert_tablegroup(const ObTablegroupSchema &tablegroup_schema,
                                common::ObISQLClient *sql_client,
                                const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int delete_tablegroup(const uint64_t tenant_id,
                                const uint64_t tablegroup_id,
                                common::ObISQLClient *sql_client,
                                const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int update_tablegroup(const uint64_t tenant_id,
                                const uint64_t tablegroup_id,
                                const ObTableSchema &table_schema,
                                common::ObISQLClient *sql_client,
                                const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int insert_sys_param(const ObSysParam &sys_param,
                               common::ObISQLClient *sql_client) = 0;
  virtual int delete_partition_table(
      const uint64_t partition_table_id, const uint64_t tenant_id,
      const int64_t partition_idx, common::ObISQLClient &sql_client) = 0;

  virtual int log_core_operation(common::ObISQLClient &sql_client) = 0;
  virtual int get_core_version(common::ObISQLClient &sql_client,
                               const int64_t frozen_version,
                               int64_t &core_schema_version) = 0;


  virtual int log_operation(ObSchemaOperation &ddl_operation,
                            common::ObISQLClient *sql_client) = 0;
  virtual int fetch_new_table_id(const uint64_t tenant_id, uint64_t &new_table_id) = 0;
  virtual int fetch_new_tenant_id(uint64_t &new_tenant_id) = 0;
  virtual int fetch_new_database_id(const uint64_t tenant_id, uint64_t &new_database_id) = 0;
  virtual int fetch_new_tablegroup_id(const uint64_t tenant_id, uint64_t &new_tablegroup_id) = 0;
  virtual int fetch_new_user_id(const uint64_t tenant_id, uint64_t &new_user_id) = 0;
  virtual int fetch_new_outline_id(const uint64_t tenant_id, uint64_t &new_outline_id) = 0;

  virtual int update_index_status(const uint64_t data_table_id, const uint64_t index_table_id,
      const ObIndexStatus status, const int64_t create_mem_version,
      common::ObISQLClient &sql_client) = 0;

//------------------For managing privileges-----------------------------//
  virtual int get_batch_tenants(const int64_t schema_version,
                                common::ObArray<uint64_t> &tenant_ids,
                                common::ObISQLClient *sql_client,
                                common::ObArray<ObTenantSchema> &tenant_info_array) = 0;
  virtual int get_batch_tenants(common::ObISQLClient *sql_client,
                                SchemaUpdateInfos &tenant_infos) = 0;

  virtual int get_batch_users(const int64_t schema_version,
                              common::ObArray<ObTenantUserId> &tn_user_ids,
                              common::ObISQLClient *sql_client,
                              common::ObArray<ObUserInfo> &user_info_array) = 0;
  virtual int get_batch_users(common::ObISQLClient *sql_client,
                              SchemaUpdateInfos &user_infos) = 0;

  virtual int get_all_users(const int64_t schema_version,
                            const uint64_t tenant_id,
                            common::ObISQLClient *sql_client,
                            common::ObArray<ObUserInfo> &user_info_array) = 0;
  virtual int get_all_users(const int64_t schema_version,
                            const uint64_t tenant_id,
                            common::ObISQLClient *sql_client,
                            SchemaUpdateInfos &user_infos) = 0;

  virtual int get_batch_db_privs(const int64_t schema_version,
                                 common::ObArray<ObOriginalDBKey> &db_priv_keys,
                                 common::ObISQLClient *sql_client,
                                 common::ObArray<ObDBPriv> &db_priv_array) = 0;
  virtual int get_batch_db_privs(common::ObISQLClient *sql_client,
                                 SchemaUpdateInfos &db_priv_infos) = 0;

  virtual int get_all_db_privs(const int64_t schema_version,
                               const uint64_t tenant_id,
                               common::ObISQLClient *sql_client,
                               common::ObArray<ObDBPriv> &db_priv_array) = 0;
  virtual int get_all_db_privs(common::ObIAllocator &allocator,
                               const int64_t schema_version,
                               const uint64_t tenant_id,
                               common::ObISQLClient *sql_client,
                               SchemaUpdateInfos &db_priv_infos) = 0;

  virtual int get_batch_table_privs(const int64_t schema_version,
                                    common::ObArray<ObTablePrivSortKey> &table_priv_keys,
                                    common::ObISQLClient *sql_client,
                                    common::ObArray<ObTablePriv> &table_priv_array) = 0;
  virtual int get_batch_table_privs(common::ObISQLClient *sql_client,
                                    SchemaUpdateInfos &table_priv_infos) = 0;

  virtual int get_all_table_privs(const int64_t schema_version,
                                  const uint64_t tenant_id,
                                  common::ObISQLClient *sql_client,
                                  common::ObArray<ObTablePriv> &table_priv_array) = 0;
  virtual int get_all_table_privs(common::ObIAllocator &allocator,
                               const int64_t schema_version,
                               const uint64_t tenant_id,
                               common::ObISQLClient *sql_client,
                               SchemaUpdateInfos &table_priv_infos) = 0;
  virtual int get_all_tenant_id_versions(common::ObISQLClient *sql_client,
                                         const int64_t schema_version,
                                         IdVersions &id_vesions) = 0;
  virtual int get_all_user_id_versions_in_tenant(common::ObISQLClient *sql_client,
                                                 const uint64_t tenant_id,
                                                 const int64_t schema_version,
                                                 IdVersions &id_vesions) = 0;

  virtual int get_all_outline_id_versions_in_tenant(
    common::ObISQLClient *sql_client,
    const uint64_t tenant_id,
    const int64_t schema_version,
    IdVersions &id_versions) = 0;

  virtual int get_all_outline_id_versions_in_database(
    common::ObISQLClient *sql_client,
    const uint64_t database_id,
    const int64_t schema_version,
    IdVersions &id_versions) = 0;


  virtual int get_all_database_id_versions_in_tenant(common::ObISQLClient *sql_client,
                                                     const uint64_t tenant_id,
                                                     const int64_t schema_version,
                                                     IdVersions &id_vesions) = 0;
  virtual int get_all_tablegroup_id_versions_in_tenant(common::ObISQLClient *sql_client,
                                                       const uint64_t tenant_id,
                                                       const int64_t schema_version,
                                                       IdVersions &id_versions) = 0;
  virtual int batch_get_table_id_versions(common::ObISQLClient *sql_client,
                                          const int64_t schema_version,
                                          const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          const int64_t get_size,
                                          IdVersions &id_versions) = 0;
  virtual int get_all_table_id_versions_in_tenant(common::ObISQLClient *sql_client,
                                                  const uint64_t tenant_id,
                                                  const int64_t schema_version,
                                                  IdVersions &id_vesions) = 0;
  virtual int get_all_table_id_versions_in_database(common::ObISQLClient *sql_client,
                                                    const int64_t schema_version,
                                                    const uint64_t database_id,
                                                    IdVersions &id_versions) = 0;
  virtual int get_all_table_id_versions_in_tablegroup(common::ObISQLClient *sql_client,
                                                      const int64_t schema_version,
                                                      const uint64_t tablegroup_id,
                                                      IdVersions &id_versions) = 0;
  virtual int check_database_exists_in_tablegroup(common::ObISQLClient *sql_client,
                                                  const int64_t schema_version,
                                                  const uint64_t tablegroup_id,
                                                  bool &not_empty) = 0;

  //----For manager all outline------//
  virtual int get_all_outline_info(const int64_t schema_version,
                                    const uint64_t tenant_id,
                                    common::ObISQLClient *sql_client,
                                    SchemaUpdateInfos &outline_info_array) = 0;

  virtual int drop_outline(const ObOutlineInfo &outline_info,
                         common::ObISQLClient *sql_client,
                         const common::ObString *ddl_stmt_str = NULL) = 0;
  //--------------For account management statements(For managing privileges)---//
  virtual int create_user(const ObUserInfo &user,
                          common::ObISQLClient *sql_client) = 0;
  virtual int drop_user(const uint64_t tenant_id,
                        const uint64_t user_id,
                        common::ObISQLClient *sql_client) = 0;
  virtual int rename_user(const ObUserInfo &user_info,
                          common::ObISQLClient *sql_client) = 0;
  virtual int set_passwd(const ObUserInfo &user_info,
                         common::ObISQLClient *sql_client) = 0;
  virtual int grant_revoke_user(const ObUserInfo &user_info,
                                common::ObISQLClient *sql_client) = 0;
  virtual int lock_user(const ObUserInfo &user_info,
                        common::ObISQLClient *sql_client) = 0;
  virtual int grant_database(const ObOriginalDBKey &db_priv_key,
                             const ObPrivSet priv_set,
                             common::ObISQLClient *sql_client) = 0;
  virtual int revoke_database(const ObOriginalDBKey &db_priv_key,
                              const ObPrivSet priv_set,
                              common::ObISQLClient *sql_client) = 0;
  virtual int delete_db_priv(const ObOriginalDBKey &org_db_key,
                             common::ObISQLClient *sql_client) = 0;
  virtual int grant_table(const ObTablePrivSortKey &table_priv_key,
                          const ObPrivSet priv_set,
                          common::ObISQLClient *sql_client) = 0;
  virtual int revoke_table(const ObTablePrivSortKey &table_priv_key,
                           const ObPrivSet priv_set,
                           common::ObISQLClient *sql_client) = 0;
  virtual int delete_table_priv(const ObTablePrivSortKey &table_priv_key,
                                common::ObISQLClient *sql_client) = 0;

  virtual int update_sys_param(const ObTenantSchema &tenant_schema,
                               common::ObISQLClient *sql_client,
                               const common::ObString &var_name,
                               const common::ObString &val_str) = 0;
#if 1
  //--------------For managing outlines---//
  virtual int insert_outline(const ObOutlineInfo &outline_info,
                             common::ObISQLClient *sql_client,
                             const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int replace_outline(const ObOutlineInfo &outline_info,
                             common::ObISQLClient *sql_client,
                             const common::ObString *ddl_stmt_str = NULL) = 0;
  virtual int delete_outline(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const uint64_t outline_id,
                             common::ObISQLClient *sql_client,
                             const common::ObString *ddl_stmt_str = NULL) = 0;
#endif
  virtual int get_batch_outlines(
      const int64_t schema_version,
      common::ObArray<uint64_t> &tenant_outline_ids,
      common::ObISQLClient *sql_client,
      common::ObArray<ObOutlineInfo> &outline_info_array) = 0;
virtual int get_batch_outlines(
    common::ObISQLClient *sql_client,
    SchemaUpdateInfos &outline_infos) = 0;

  //-------------------------- for new schema_cache ------------------------------

  virtual int get_table_schema(uint64_t table_id,
                               int64_t schema_version,
                               common::ObISQLClient *sql_client,
                               common::ObIAllocator &allocator,
                               ObTableSchema *&table_schema) = 0;

    //just for debug
  virtual void print_table_schema(const common::ObArray<ObTableSchema> &table_schema_array) = 0;
  virtual void print_database_schema(const common::ObArray<ObDatabaseSchema> &table_schema_array) = 0;
  virtual void print_tablegroup_schema(
      const common::ObArray<ObTablegroupSchema> &table_schema_array) = 0;
  virtual int update_data_table_schema_version(common::ObISQLClient &sql_client,
                                               const uint64_t data_table_id) = 0;

  virtual int sync_index_schema_version_for_history(
      common::ObISQLClient &sql_client,
      const ObTableSchema &index_schema1,
      const uint64_t new_schema_version) = 0;


  virtual int64_t gen_new_schema_version() = 0;

  // when refresh schema, if new ddl operations are as following:
  // (ALTER USER TABLE, v1), (ALTER SYS TABLE, v2),
  // if we replay new ddl operation one by one, when we execute sql to read sys table
  // to fetch user table schema, leader partition server find sys table version not match,
  // read new user table schema will fail, so we need first refresh sys table schema,
  // then publish, then refresh new user table schemas and publish,
  // but what version we used to publish sys table schema when we haven't refresh use table,
  // we use a temporary version which means it don't contain all schema item whose version
  // is small than temporary version. now we have temporary core versin for core table,
  // temporary system version for system table, we set SCHEMA_VERSION_INC_STEP to 8 so that
  // it is enough when we add more temporary version
  static const int64_t SCHEMA_VERSION_INC_STEP = 8;

  static bool is_formal_version(const int64_t schema_version);
  static bool is_core_temp_version(const int64_t schema_version);
  static bool is_sys_temp_version(const int64_t schema_version);
  static int gen_core_temp_version(const int64_t schema_version,
                                    int64_t &core_temp_version);
  static int gen_sys_temp_version(const int64_t schema_version,
                                   int64_t &sys_temp_version);
  static int alloc_table_schema(const ObTableSchema &table, common::ObIAllocator &allocator,
                                ObTableSchema *&allocated_table_schema);

};
}//namespace schema
}//namespace share
}//namespace oceanbase
#endif /* _OB_OCEANBAE_SCHEMA_SCHEMA_SERVICE_H */
