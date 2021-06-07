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

#ifndef OBPROXY_SESSION_FIELD_MGR_H
#define OBPROXY_SESSION_FIELD_MGR_H

#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/ptr/ob_ptr.h"
#include "common/ob_obj_cast.h"
#include "common/ob_string_buf.h"
#include "sql/session/ob_system_variable_init.h"
#include "sql/session/ob_system_variable_alias.h"
#include "proxy/mysqllib/ob_field_heap.h"
#include "lib/string/ob_sql_string.h"
#include <map>

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
class ObMysqlResultHandler;
}

enum ObVFieldType
{
  OB_V_FIELD_EMPTY = 0,
  OB_V_FIELD_CLUSTER_NAME,
  OB_V_FIELD_TENANT_NAME,
  OB_V_FIELD_USER_NAME,
  OB_V_FIELD_DATABASE_NAME,
  OB_V_FIELD_LOGIC_TENANT_NAME,
  OB_V_FIELD_LOGIC_DATABASE_NAME,
  OB_V_FIELD_SHARD_NAME,
  OB_V_FIELD_LDG_LOGICAL_CLUSTER_NAME,
  OB_V_FIELD_LDG_LOGICAL_TENANT_NAME,
  OB_V_FIELD_LDG_REAL_CLUSTER_NAME,
  OB_V_FIELD_LDG_REAL_TENANT_NAME,
  OB_V_FIELD_MAX,
};

struct ObSessionVField
{
  ObSessionVField() : type_(OB_V_FIELD_EMPTY), value_len_(0), value_(NULL) {}
  ~ObSessionVField() { reset(); }
  void reset();
  bool is_empty() const { return type_ == OB_V_FIELD_EMPTY; }
  int move_strings(ObFieldStrHeap &new_heap);
  int64_t strings_length() const;
  int format(common::ObSqlString &sql) const;
  DECLARE_TO_STRING;

  ObVFieldType type_;
  uint16_t value_len_;
  const char *value_;
};

enum ObKVFieldType
{
  OB_KV_FIELD_EMPTY = 0,
  OB_KV_FIELD_PS_CACHE = 1,
  OB_KV_FIELD_MAX = 2
};

//maybe useful for used for ps cache? not used so for
struct ObSessionKVField
{
  ObSessionKVField();
  ~ObSessionKVField() {}
  int move_strings(ObFieldStrHeap &new_heap);
  int64_t strings_length() const;
  int format(common::ObSqlString &sql) const;

  ObKVFieldType type_;
  uint16_t name_len_;
  uint16_t value_len_;
  const char *name_;
  const char *value_;
};

enum ObSessionFieldStat
{
  OB_FIELD_EMPTY = 0,
  OB_FIELD_USED = 1,
};

enum ObSessionVarType
{
  OB_SESSION_USER_VAR = 0,
  OB_SESSION_SYS_VAR,
  OB_SESSION_COMMON_SYS_VAR,
  OB_SESSION_MYSQL_SYS_VAR,
  OB_SESSION_VAR_MAX,
};

// distinguish system session variables modify frequency.
// some session variables are frequently modified, but others are rarely modified.
// all user-defined session variabes are treated as cold modify mod.
//
// last_insert_id may be modified by every insert or update statement, so we carry
// it out alone to reduce session var sync times.
enum ObSessionFieldModifyMod
{
  OB_FIELD_COLD_MODIFY_MOD = 0,
  OB_FIELD_HOT_MODIFY_MOD,
  OB_FIELD_LAST_INSERT_ID_MODIFY_MOD,
};

struct ObSessionBaseField
{
  ObSessionBaseField() : stat_(OB_FIELD_EMPTY), name_len_(0),
                         name_(NULL), value_(), modify_mod_(OB_FIELD_COLD_MODIFY_MOD) {}
  virtual ~ObSessionBaseField() {}
  virtual void reset();
  bool is_empty() const { return OB_FIELD_EMPTY == stat_; }
  int move_strings(ObFieldStrHeap &new_heap);
  int64_t strings_length() const;

  int format_util(common::ObSqlString &sql, bool is_sys_field) const;
  virtual int format(common::ObSqlString &sql,
                     const ObSessionFieldModifyMod modify_mod) const;
  DECLARE_TO_STRING;

  static const int64_t OB_SHORT_SESSION_VAR_LENGTH = 512;
  static const int64_t OB_MAX_SESSION_VAR_LENGTH   = 32 * 1024; // max sql length

  ObSessionFieldStat stat_;
  uint16_t name_len_;
  const char *name_;
  common::ObObj value_;
  ObSessionFieldModifyMod modify_mod_;
};

struct ObSessionSysField : public ObSessionBaseField
{
  ObSessionSysField() : type_(common::ObNullType), scope_(sql::ObSysVarFlag::NONE) {}
  virtual ~ObSessionSysField() {}
  virtual void reset();

  virtual int format(common::ObSqlString &sql,
                     const ObSessionFieldModifyMod modify_mod) const;
  virtual int format(common::ObSqlString &sql) const;

  common::ObString get_variable_name_str() const { return common::ObString(name_len_, name_); }
  bool is_readonly() const { return ((scope_ & sql::ObSysVarFlag::READONLY) || (scope_ & sql::ObSysVarFlag::SESSION_READONLY)); }
  bool is_session_scope() const { return scope_ & sql::ObSysVarFlag::SESSION_SCOPE; }
  bool is_global_scope() const { return scope_ & sql::ObSysVarFlag::GLOBAL_SCOPE; }
  bool is_invisible() const { return scope_ & sql::ObSysVarFlag::INVISIBLE; }
  bool is_nullable() const { return scope_ & sql::ObSysVarFlag::NULLABLE; }

  common::ObObjType type_;
  int64_t scope_;
};

typedef ObSessionBaseField ObSessionUserField;

template <class T, HeapObjType TYPE>
struct ObFieldBlock : public ObFieldHeader
{
  ObFieldBlock();
  ~ObFieldBlock() {}
  void inc_removed_count() { ++removed_count_; }
  void dec_removed_count() { --removed_count_; }

  int move_strings(ObFieldStrHeap *new_heap);
  int64_t strings_length();
  int format(common::ObSqlString &sql, const ObSessionFieldModifyMod modify_mod) const;
  DECLARE_TO_STRING;

  const static int64_t FIELD_BLOCK_SLOTS_NUM = 16; //maybe a better value
  int64_t free_idx_;
  int64_t removed_count_;
  ObFieldBlock *next_;
  T field_slots_[FIELD_BLOCK_SLOTS_NUM];
};

template <class T, HeapObjType TYPE>
ObFieldBlock<T, TYPE>::ObFieldBlock() : free_idx_(0), removed_count_(0), next_(NULL)
{
  ObFieldHeader::init(TYPE, sizeof(ObFieldBlock));
}

template <class T, HeapObjType TYPE>
int ObFieldBlock<T, TYPE>::move_strings(ObFieldStrHeap *new_heap)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(new_heap)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObFieldStrHeap &heap = *new_heap;
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < free_idx_; ++i) {
      T &field = field_slots_[i];
      if (OB_FAIL(field.move_strings(heap))) {
        PROXY_LOG(WARN, "fail to move strings", K(field), K(ret));
      }
    }
  }
  return ret;
}

template <class T, HeapObjType TYPE>
int64_t ObFieldBlock<T, TYPE>::strings_length()
{
  int64_t length = 0;
  for (int64_t i = 0; i < free_idx_; ++i) {
    length += field_slots_[i].strings_length();
  }
  return length;
}

template <class T, HeapObjType TYPE>
int ObFieldBlock<T, TYPE>::format(common::ObSqlString &sql,
                                  const ObSessionFieldModifyMod modify_mod) const
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; common::OB_SUCCESS == ret && i < free_idx_; ++i) {
    const T &field = field_slots_[i];
    if (OB_FAIL(field.format(sql, modify_mod))) {
      PROXY_LOG(WARN, "fail to construct reset sql", K(field), K(ret));
    }
  }
  return ret;
}

template <class T, HeapObjType TYPE>
int64_t ObFieldBlock<T, TYPE>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObFieldHeader::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(free_idx), K_(removed_count), K_(next));
  for (int64_t i = 0; i < FIELD_BLOCK_SLOTS_NUM; ++i) {
    J_COMMA();
    J_KV(K(field_slots_[i]));
  }
  J_OBJ_END();
  return pos;
}

typedef ObFieldBlock<ObSessionSysField, HEAP_OBJ_SYS_VAR_BLOCK>  ObSysVarFieldBlock;
typedef ObFieldBlock<ObSessionUserField, HEAP_OBJ_USER_VAR_BLOCK>  ObUserVarFieldBlock;
typedef ObFieldBlock<ObSessionVField, HEAP_OBJ_STR_BLOCK>  ObStrFieldBlock;

class ObFieldBaseMgr
{
public:
  ObFieldBaseMgr() : field_heap_(NULL), sys_block_list_tail_(NULL), sys_first_block_(NULL),
                     is_inited_(false) {}
  virtual ~ObFieldBaseMgr();
  virtual int init();
  virtual void destroy();
  int get_sys_first_block(const ObSysVarFieldBlock *&block_out);
  int64_t get_memory_size() const { return NULL == field_heap_ ? 0 : field_heap_->get_memory_size(); }
protected:
  int duplicate_field(const char *name, uint16_t name_len,
                      const common::ObObj &src_obj, ObSessionBaseField &field);
  template<typename T>
  int alloc_block(T *&out_block) const
  {
    int ret = common::OB_SUCCESS;
    void *new_space = NULL;
    // this function will be called in init(), so we just judge field_heap
    if (OB_UNLIKELY(NULL == field_heap_)) {
      ret = common::OB_NOT_INIT;
      PROXY_LOG(WARN, "not init", K(ret));
    } else if (OB_FAIL(field_heap_->allocate_block(sizeof(T), new_space))) {
      PROXY_LOG(WARN, "fail to allocate Block", K(ret));
    } else if (OB_ISNULL(out_block = new (new_space) T())) {
      ret = common::OB_ERR_SYS;
      PROXY_LOG(WARN, "sys error, fail to new Block", K(ret));
    }
    return ret;
  }

  template<typename T, typename F>
  int alloc_field(T *list_head, T *&list_tail, T *&block_out, F *&new_field, bool &is_reused) const
  {
    int ret = common::OB_SUCCESS;
    new_field = NULL;
    bool found = false;
    is_reused = false;
    T *block = list_head;
    F *field = NULL;

    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      PROXY_LOG(WARN, "not init", K(ret));
    }

    // first try to resue removed field
    for (; (common::OB_SUCCESS == ret) && !found && (NULL != block); block = block->next_) {
      if (block->removed_count_ > 0) {
        for (int64_t i = 0; common::OB_SUCCESS == ret && (i < block->free_idx_); ++i) {
          field = const_cast<F *>(block->field_slots_ + i);
          if (field->is_empty()) {
            new_field = field;
            found = true;
            is_reused = true;
            block_out = block;
            break;
          }
        }
        if (OB_UNLIKELY(!found)) {
          ret = common::OB_ERR_UNEXPECTED;
          PROXY_LOG(WARN, "can't find removed field pos", K(block->removed_count_), K(ret));
        }
      } else {
        // continue
      }
    }

    if (OB_SUCC(ret)) {
      if (!found) {
        if (list_tail->free_idx_ >= T::FIELD_BLOCK_SLOTS_NUM) {
          T *new_block = NULL;
          void *new_space =  NULL;
          if (common::OB_SUCCESS != (ret = field_heap_->allocate_block(sizeof(T), new_space))) {
            PROXY_LOG(WARN, "fail to allocate Block", K(ret));
          } else if (OB_ISNULL(new_block = new (new_space) T())) {
            ret = common::OB_ERR_SYS;
            PROXY_LOG(WARN, "sys error, fail to new Block", K(ret));
          } else {
            list_tail->next_ = new_block;
            list_tail = new_block;
          }
        }

        if (OB_SUCC(ret)) {
          int64_t free_idx = (list_tail->free_idx_)++;
          new_field = &(list_tail->field_slots_[free_idx]);
          block_out = list_tail;
        }
      }
    }
    return ret;
  }
protected:
  ObFieldHeap *field_heap_;
  ObSysVarFieldBlock *sys_block_list_tail_;//sys variables
  ObSysVarFieldBlock *sys_first_block_;
  bool is_inited_;
};

class ObDefaultSysVarSet;
class ObSessionFieldMgr : public ObFieldBaseMgr
{
public:
  ObSessionFieldMgr();
  virtual ~ObSessionFieldMgr() {}
  virtual int init();
  virtual void destroy();

public:
  typedef bool (*NeedFunc)(const common::ObString& var_name);
  // calc val hash
  int calc_common_hot_sys_var_hash(uint64_t &hash_val);
  int calc_common_cold_sys_var_hash(uint64_t &hash_val);
  int calc_mysql_hot_sys_var_hash(uint64_t &hash_val);
  int calc_mysql_cold_sys_var_hash(uint64_t &hash_val);
  int calc_hot_sys_var_hash(uint64_t &hash_val);
  int calc_cold_sys_var_hash(uint64_t &hash_val);
  int calc_user_var_hash(uint64_t &hash_val);
  //set and get methord
  int set_cluster_name(const common::ObString &cluster_name);
  int set_tenant_name(const common::ObString &tenant_name);
  int set_user_name(const common::ObString &user_name);
  int set_database_name(const common::ObString &database_name);
  int set_logic_tenant_name(const common::ObString &logic_tenant_name);
  int set_logic_database_name(const common::ObString &logic_database_name);
  int set_ldg_logical_cluster_name(const common::ObString &cluster_name);
  int set_ldg_logical_tenant_name(const common::ObString &tenant_name);
  int get_cluster_name(common::ObString &cluster_name) const;
  int get_tenant_name(common::ObString &tenant_name) const;
  int get_user_name(common::ObString &user_name) const;
  int get_database_name(common::ObString &database_name) const;
  int get_logic_tenant_name(common::ObString &logic_tenant_name) const;
  int get_logic_database_name(common::ObString &logic_database_name) const;
  int get_ldg_logical_cluster_name(common::ObString &cluster_name) const;
  int get_ldg_logical_tenant_name(common::ObString &tenant_name) const;
  int remove_database_name() { return remove_str_field(OB_V_FIELD_DATABASE_NAME); }

  //sys variables related methords
  //common sys variables related methords
  int insert_common_sys_variable(const common::ObString &name, const common::ObObj &value,
                                 ObSessionSysField *&field, ObDefaultSysVarSet *var_set);
  int update_common_sys_variable(const common::ObObj &value, ObSessionSysField *&field);
  int update_common_sys_variable(const common::ObString &name, const common::ObObj &value,
                                 ObSessionSysField *&field, const bool is_need_insert,
                                 const bool is_oceanbase);
  int insert_mysql_system_variable(const common::ObString &name, const common::ObObj &value, ObSessionSysField *&field);
  int update_mysql_system_variable(const common::ObString &name, const common::ObObj &value, ObSessionSysField *&field);

  //sys variables related methords
  int update_system_variable(const common::ObString &name, const common::ObObj &value, ObSessionSysField *&field);
  int get_sys_variable(const common::ObString &name, ObSessionSysField *&value) const;
  int get_sys_variable_value(const common::ObString &name, common::ObObj &value) const;
  int get_sys_variable_value(const char *name, common::ObObj &value) const;
  common::ObObj get_sys_variable_value(const char* name) const;
  int sys_variable_exists(const common::ObString &name, bool &is_exist);
  int get_changed_sys_var_names(common::ObIArray<common::ObString> &names);
  int get_all_sys_var_names(common::ObIArray<common::ObString> &names);
  int get_all_common_sys_var_names(common::ObIArray<common::ObString> &names);
  int get_all_mysql_sys_var_names(common::ObIArray<common::ObString> &names);

  int get_all_changed_sys_vars(common::ObIArray<ObSessionSysField> &fields);
  int get_all_sys_vars(common::ObIArray<ObSessionSysField> &fields);
  int get_all_user_vars(common::ObIArray<ObSessionBaseField> &fields);

  // is session system variable equal with snapshot
  int is_equal_with_snapshot(const common::ObString &sys_var_name,
                             const common::ObObj &value, bool &is_equal);
  // @synopsis get variable type by name
  int get_sys_variable_type(const common::ObString &var_name, common::ObObjType &type);

  //sys variables related methords
  int replace_variable_common(const common::ObString &name, const common::ObObj &value, ObSessionVarType var_type, bool is_oceanbase = true);
  int replace_user_variable(const common::ObString &name, const common::ObObj &val);
  int replace_mysql_sys_variable(const common::ObString &name, const common::ObObj &val);
  int replace_common_sys_variable(const common::ObString &name, const common::ObObj &val, bool is_oceanbase = true);

  int remove_variable_common(const common::ObString &name, ObSessionVarType var_type);
  int remove_user_variable(const common::ObString &name);
  int remove_mysql_sys_variable(const common::ObString &name);
  int remove_common_sys_variable(const common::ObString &name);

  int get_user_variable(const common::ObString &name, ObSessionUserField *&value) const;
  int get_variable_value_common(const common::ObString &name, common::ObObj &value, ObSessionVarType var_type) const;
  int get_user_variable_value(const common::ObString &name, common::ObObj &value) const;
  int get_common_sys_variable_value(const common::ObString &name, common::ObObj &value) const;
  int get_mysql_sys_variable_value(const common::ObString &name,  common::ObObj &value) const;
  int user_variable_exists(const common::ObString &name, bool &is_exist);
  int get_all_user_var_names(common::ObIArray<common::ObString> &names);

  int format_common_sys_var(common::ObSqlString &sql) const;
  int format_common_hot_sys_var(common::ObSqlString &sql) const;
  int format_mysql_sys_var(common::ObSqlString &sql) const;
  int format_mysql_hot_sys_var(common::ObSqlString &sql) const;
  int format_all_var(common::ObSqlString &sql) const;
  int format_sys_var(common::ObSqlString &sql) const;
  int format_hot_sys_var(common::ObSqlString &sql) const;
  int format_user_var(common::ObSqlString &sql) const;
  int format_last_insert_id(common::ObSqlString &sql) const;

  // add for session pool
  int remove_all_user_vars();
  int remove_all_sys_vars();
  int remove_all_mysql_sys_vars();
  int remove_all_common_sys_vars();

  int remove_sys_variable(const common::ObString &name);
  int get_sys_variable(const common::ObString &name, ObSessionSysField *&value,
                       ObSysVarFieldBlock *&out_block) const;
  int replace_last_insert_id_var(ObSessionFieldMgr& field_manager, bool is_oceanbase = true);
  int replace_all_sys_vars(ObSessionFieldMgr& field_manager, NeedFunc need_func = NULL);
  int replace_all_user_vars(ObSessionFieldMgr& field_manager);
  int replace_all_common_sys_vars(ObSessionFieldMgr& field_manager, bool is_oceanbase = true, NeedFunc need_func = NULL);
  int replace_all_mysql_sys_vars(ObSessionFieldMgr& field_manager, NeedFunc need_func = NULL);

  int replace_all_hot_sys_vars(ObSessionFieldMgr& field_manager);
  int replace_all_cold_sys_vars(ObSessionFieldMgr& field_manager);
  int replace_all_common_hot_sys_vars(ObSessionFieldMgr& field_manager, bool is_oceanbase = true);
  int replace_all_common_cold_sys_vars(ObSessionFieldMgr& field_manager, bool is_oceanbase = true);
  int replace_all_mysql_hot_sys_vars(ObSessionFieldMgr& field_manager);
  int replace_all_mysql_cold_sys_vars(ObSessionFieldMgr& field_manager);

  bool is_same_last_insert_id_var(const ObSessionFieldMgr& field_manager);
  bool is_same_cold_sys_vars_common(const ObSessionFieldMgr& field_manager, ObSessionVarType var_type);
  bool is_same_hot_sys_vars_common(const ObSessionFieldMgr& field_manager, ObSessionVarType var_type);
  bool is_same_hot_sys_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_cold_sys_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_user_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_mysql_cold_session_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_mysql_hot_session_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_common_cold_session_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_common_hot_session_vars(const ObSessionFieldMgr& field_manager);
  bool is_same_vars_in_array(common::ObIArray<common::ObString>& names1,
                             const common::ObIArray<common::ObString>& names2,
                             const ObSessionFieldMgr& field_manager,
                             int type);
  // end add for session pool
  static bool is_cold_modified_variable(const common::ObString &var_name);
  static bool is_hot_modified_variable(const common::ObString &var_name);
  static bool is_last_insert_id_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_LAST_INSERT_ID; }
  static bool is_user_privilege_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_PROXY_USER_PRIVILEGE; }
  static bool is_set_trx_executed_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_PROXY_SET_TRX_EXECUTED
                                                                                   || var_name == sql::OB_SV_PROXY_SESSION_TEMPORARY_TABLE_USED; }
  static bool is_partition_hit_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_PROXY_PARTITION_HIT; }
  static bool is_global_version_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_PROXY_GLOBAL_VARIABLES_VERSION; }
  static bool is_capability_flag_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_CAPABILITY_FLAG; }
  static bool is_safe_read_snapshot_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_SAFE_WEAK_READ_SNAPSHOT; }
  static bool is_route_policy_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_ROUTE_POLICY; }
  static bool is_enable_transmission_checksum_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_ENABLE_TRANSMISSION_CHECKSUM; }
  static bool is_statement_trace_id_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_STATEMENT_TRACE_ID; }
  static bool is_read_consistency_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_READ_CONSISTENCY; }
  static bool is_client_reroute_info_variable(const common::ObString &var_name) { return var_name == sql::OB_SV_CLIENT_REROUTE_INFO; }

  static bool is_nls_date_timestamp_format_variable(const common::ObString &var_name)
  {
    return var_name == sql::OB_SV_NLS_DATE_FORMAT
        || var_name == sql::OB_SV_NLS_TIMESTAMP_FORMAT
        || var_name == sql::OB_SV_NLS_TIMESTAMP_TZ_FORMAT;
  }

  int set_sys_var_set(ObDefaultSysVarSet *set);

  ObDefaultSysVarSet *get_sys_var_set() { return default_sys_var_set_; }
  void set_allow_var_not_found(bool bret) {allow_var_not_found_ = bret;}

private:
  bool is_same_obj_val(const common::ObObj &obj_client, const common::ObObj &obj_server);
  int get_user_variable(const common::ObString &name, ObSessionUserField *&value,
                        ObUserVarFieldBlock *&out_block) const;
  int get_common_sys_variable(const common::ObString &name, ObSessionSysField *&value,
                                         ObSysVarFieldBlock *&out_block) const;
  int get_mysql_sys_variable(const common::ObString &name, ObSessionSysField *&value,
                                         ObSysVarFieldBlock *&out_block) const;
  int insert_user_variable(const common::ObString &name, const common::ObObj &val);
  int insert_mysql_system_variable(const common::ObString &name, const common::ObObj &value);
  int insert_common_sys_variable(const common::ObString &name, const common::ObObj &value, bool is_oceanbase);

  int update_field_value(const common::ObObj &src, common::ObObj &dest_obj);
  int replace_str_field(const ObVFieldType type, const common::ObString &value);
  int remove_str_field(const ObVFieldType type);
  int insert_str_field(const ObVFieldType type, const common::ObString &value);
  int get_str_field_value(const ObVFieldType type, common::ObString &value) const;
  bool str_field_exists(const ObVFieldType type) const;
  int get_sys_variable_from_block(const common::ObString &name, ObSessionSysField *&value,
                                  const ObSysVarFieldBlock *block) const;
  int get_common_sys_variable(const common::ObString &name, ObSessionSysField *&value) const;
  int get_mysql_sys_variable(const common::ObString &name, ObSessionSysField *&value) const;
  int get_sys_variable_local(const common::ObString &name, ObSessionSysField *&value) const;
  bool sys_variable_exists_local(const common::ObString &var);

  template<typename T>
  int format_var(T *head, common::ObSqlString &sql,
                 ObSessionFieldModifyMod modify_mod) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      PROXY_LOG(WARN, "not inited", K(ret));
    } else {
      const T *block = head;
      for (; common::OB_SUCCESS == ret && NULL != block; block = block->next_) {
        if (OB_FAIL(block->format(sql, modify_mod))) {
          PROXY_LOG(WARN, "construct reset sql failed", K(ret));
        }
      }
    }
    return ret;
  }

  template<typename T>
  int get_var_names_common(const T *block, common::ObIArray<common::ObString> &names)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      PROXY_LOG(WARN, "not inited", K(ret));
    } else {
      const ObSessionBaseField *field = NULL;
      common::ObString var_name;
      for (; common::OB_SUCCESS == ret && NULL != block; block = block->next_) {
        for (int64_t i = 0; common::OB_SUCCESS == ret && i < block->free_idx_; ++i) {
          field = block->field_slots_ + i;
          if (OB_FIELD_USED == field->stat_) {
            var_name.assign(const_cast<char *>(field->name_), field->name_len_);
            if (OB_FAIL(names.push_back(var_name))) {
              PROXY_LOG(WARN, "fail to push back var_name", K(var_name), K(ret));
            }
          }
        }
      }
    }
    return ret;
  }

  template<typename T>
  int calc_var_hash_common(const T *block, uint64_t& hash_val, NeedFunc need_func, const common::ObString& session_name)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      PROXY_LOG(WARN, "not inited", K(ret));
    } else {
      // using hash_map for sort
      std::map<std::string, uint64_t> hash_map;
      const ObSessionBaseField *field = NULL;
      for (; common::OB_SUCCESS == ret && NULL != block; block = block->next_) {
        for (int64_t i = 0; common::OB_SUCCESS == ret && i < block->free_idx_; ++i) {
          field = block->field_slots_ + i;
          if (OB_FIELD_USED == field->stat_) {
            std::string var_name(field->name_, field->name_len_);
            uint64_t val_hash_val = 0;
            if (need_func == NULL || need_func(common::ObString::make_string(var_name.c_str()))) {
              if (field->value_.is_varchar()) {
                val_hash_val = field->value_.varchar_hash(common::ObCharset::get_system_collation());
              } else {
                val_hash_val = field->value_.hash();
              }
              PROXY_LOG(DEBUG, "hash value", "name", var_name.c_str(), K(field->value_), K(val_hash_val));
              hash_map.insert(std::pair<std::string, uint64_t>(var_name, val_hash_val));
            }
          }
        }
      }
      common::ObSqlString all_val_hash_str;
      for (std::map<std::string, uint64_t>::iterator it = hash_map.begin(); it != hash_map.end(); ++it) {
        all_val_hash_str.append_fmt("%s:%ld,", it->first.c_str(), it->second);
      }
      hash_val = all_val_hash_str.string().hash();
      PROXY_LOG(DEBUG, "all_val_hash_str is ", K(session_name), K(hash_val));
    }
    return ret;
  }

  template<typename T, typename FILEDS, typename FILED>
  int get_vars_common(const T *block, FILEDS &fields, const FILED *field)
  {
    int ret = common::OB_SUCCESS;
    common::ObString var_name;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      PROXY_LOG(WARN, "not inited", K(ret));
    }
    for (; common::OB_SUCCESS == ret && NULL != block; block = block->next_) {
      for (int64_t i = 0; common::OB_SUCCESS == ret && i < block->free_idx_; ++i) {
        field = block->field_slots_ + i;
        if (OB_FIELD_USED == field->stat_) {
          if (OB_FAIL(fields.push_back(*field))) {
            PROXY_LOG(WARN, "fail to push back var_name", K(var_name), K(ret));
          }
        }
      }
    }
    return ret;
  }

private:
  ObUserVarFieldBlock *user_block_list_tail_;//user variables
  ObUserVarFieldBlock *user_first_block_;
  ObStrFieldBlock *str_block_list_tail_; //string field can only be set once
  ObStrFieldBlock *str_first_block_;
  ObSysVarFieldBlock *common_sys_block_list_tail_; //common sys var
  ObSysVarFieldBlock *common_sys_first_block_;
  ObSysVarFieldBlock *mysql_sys_block_list_tail_; // mysql sys var
  ObSysVarFieldBlock *mysql_sys_first_block_;
  ObDefaultSysVarSet *default_sys_var_set_;
  bool allow_var_not_found_; //control log level when not find var
};

template<class K, class V>
struct GetFieldKey
{
  common::ObString operator() (const ObSessionSysField *sys_field) const
  {
    return sys_field->get_variable_name_str();
  }
};

// cast a obj to the same type with the given field
class ObFieldObjCaster
{
public:
  ObFieldObjCaster(): casted_cell_(),
                      allocator_(common::ObModIds::OB_SQL_EXPR) {}
  int obj_cast(const common::ObObj &value, const ObSessionSysField &field,
               const common::ObObj *&res_cell);
private:
  common::ObObj casted_cell_;
  common::ObArenaAllocator allocator_;
};

class ObDefaultSysVarSet : public ObFieldBaseMgr, public common::ObSharedRefCount
{
public:
  ObDefaultSysVarSet();
  virtual ~ObDefaultSysVarSet() {}

  virtual void free() { destroy(); }

  void destroy()
  {
    var_name_map_.destroy();
    is_inited_ = false;
    delete this;
  }

  int init();

  int get_sys_variable(const common::ObString &var, ObSessionSysField *&value, bool allow_var_not_found = false);
  int sys_variable_exists(const common::ObString &var_name, bool &is_exist);

  int load_system_variable_snapshot(proxy::ObMysqlResultHandler &result_handler);

  int load_default_system_variable();

  int load_system_variable(const common::ObString &name, const common::ObObj &type,
                           const common::ObObj &value, const int64_t flags);
  int load_system_variable(const common::ObString &name, const int64_t dtype,
                           const common::ObString &value, const int64_t flags);

  bool sys_variable_exists_local(const common::ObString &var_name);
  int64_t get_last_modified_time() const { return last_modified_time_; }
  int64_t get_sys_var_count() const { return var_name_map_.count(); }

  TO_STRING_KV(KP(this), K_(ref_count), K_(is_inited), K_(last_modified_time));

private:
  int load_sysvar_int(const common::ObString &var_name, const int64_t var_value,
                      const int64_t flags, const bool print_info_log);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDefaultSysVarSet);

  static const int64_t HASH_MAP_SIZE = 16 * 1024;//16k
  bool is_inited_;
  int64_t last_modified_time_;
  common::hash::ObPointerHashMap<common::ObString, ObSessionSysField *,
      GetFieldKey, HASH_MAP_SIZE> var_name_map_;
};

}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_SESSION_FIELD_MGR_H */
