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

#define USING_LOG_PREFIX PROXY
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashset.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

namespace oceanbase
{
namespace obproxy
{

static const ObString hot_var_names[] = { ObString(OB_SV_AUTOCOMMIT),
                                          ObString(OB_SV_LOG_LEVEL),
                                          ObString(OB_SV_QUERY_TIMEOUT),
                                          ObString(OB_SV_LAST_SCHEMA_VERSION)
                                        };
static const int64_t MAX_HOT_MODIFY_VARIABLES_NUM = ARRAYSIZEOF(hot_var_names);

inline int ObDefaultSysVarSet::load_sysvar_int(const ObString &var_name,
                                               const int64_t var_value,
                                               const int64_t flags,
                                               const bool print_info_log)
{
  int ret = OB_SUCCESS;
  ObObj type;
  ObObj value;
  if (sys_variable_exists_local(var_name)) {
    ret = OB_ERR_ALREADY_EXISTS;
    LOG_INFO("system variable already exist", K(var_name), K(var_value), K(ret));
  } else {
    type.set_type(ObIntType);
    value.set_int(var_value);
    if (OB_FAIL(load_system_variable(var_name, type, value, flags))) {
      LOG_WDIAG("fail to load default system variable", K(var_name), K(var_value), K(ret));
    } else if (print_info_log) {
      LOG_INFO("load default system variable", K(var_name), K(var_value));
    }
  }
  return ret;
}

DEF_TO_STRING(ObSessionVField)
{
  int64_t pos = 0;
  ObString value(value_len_, value_);
  J_OBJ_START();
  J_KV(K_(type), K(value), K_(value_len));
  J_OBJ_END();
  return pos;
}

void ObSessionVField::reset()
{
  type_ = OB_V_FIELD_EMPTY;
  value_len_ = 0;
  value_ = NULL;
}

int ObSessionVField::move_strings(ObFieldStrHeap &new_heap)
{
  int ret = OB_SUCCESS;
  if (OB_V_FIELD_EMPTY == type_) {
    // do nothing
  } else if (OB_UNLIKELY(type_ < OB_V_FIELD_EMPTY || type_ >= OB_V_FIELD_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid type", K(type_), K(ret));
  } else if (OB_FAIL(ObFieldHeapUtils::str_heap_move_str(new_heap, value_, value_len_))) {
    LOG_WDIAG("failed to move str", K(value_), K(value_len_), K(ret));
  } else {}
  return ret;
}

int64_t ObSessionVField::strings_length() const
{
  int64_t length = 0;
  if (OB_V_FIELD_EMPTY < type_ && type_ < OB_V_FIELD_MAX) {
    length += value_len_;
  }
  return length;
}

int ObSessionVField::format(ObSqlString &sql) const
{
  UNUSED(sql);
  return OB_NOT_IMPLEMENT;
}

//-----ObSessionKVField------//
ObSessionKVField::ObSessionKVField() : type_(OB_KV_FIELD_EMPTY), name_len_(0),
                                       value_len_(0), name_(NULL), value_(NULL)
{
}

int ObSessionKVField::move_strings(ObFieldStrHeap &new_heap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type_ <= OB_KV_FIELD_EMPTY || type_ >= OB_KV_FIELD_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid type", K(type_), K(ret));
  } else if (OB_FAIL(ObFieldHeapUtils::str_heap_move_str(new_heap, name_, name_len_))) {
    LOG_WDIAG("failed to move name str", K(name_), K(name_len_), K(ret));
  } else if (OB_FAIL(ObFieldHeapUtils::str_heap_move_str(new_heap, value_, value_len_))) {
    LOG_WDIAG("failed to move value str", K(value_), K(value_len_), K(ret));
  } else {}
  return ret;
}

int64_t ObSessionKVField::strings_length() const
{
  int64_t length = 0;
  if (OB_KV_FIELD_EMPTY < type_ && type_ < OB_KV_FIELD_MAX) {
    length += name_len_;
    length += value_len_;
  }
  return length;
}

int ObSessionKVField::format(ObSqlString &sql) const
{
  UNUSED(sql);
  return OB_NOT_IMPLEMENT;
}

void ObSessionBaseField::reset()
{
  name_ = NULL;
  name_len_ = 0;
  stat_ = OB_FIELD_EMPTY;
  value_.reset();
}

int ObSessionBaseField::move_strings(ObFieldStrHeap &new_heap)
{
  int ret = OB_SUCCESS;
  if (OB_FIELD_USED == stat_) {
    if (OB_FAIL(ObFieldHeapUtils::str_heap_move_str(new_heap, name_, name_len_))) {
      LOG_WDIAG("failed to move name str", K(name_), K(name_len_), K(ret));
    } else if (OB_FAIL(ObFieldHeapUtils::str_heap_move_obj(new_heap, value_))) {
      LOG_WDIAG("failed to move value obj", K(value_), K(ret));
    } else {}
  }
  return ret;
}

int64_t ObSessionBaseField::strings_length() const
{
  int64_t length = 0;
  if (OB_FIELD_USED == stat_) {
    length += name_len_;
    length += value_.get_deep_copy_size();
  }
  return length;
}

int ObSessionBaseField::format_util(ObSqlString &sql, bool is_sys_field) const
{
  int ret = OB_SUCCESS;
  if (OB_FIELD_USED != stat_) {
    LOG_DEBUG("not a used field");
  } else {
    // set print func
    int (ObObj::*print_func)(char *, int64_t, int64_t &, const ObTimeZoneInfo * tz_info) const;
    if (is_sys_field) {
      // sys field will use print_sql to add '
      print_func = &ObObj::print_sql_literal;
    } else {
      // user field has store ' into value, no need to print ' again
      print_func = &ObObj::print_plain_str_literal;
    }

    // use two kinds of buffer:
    // 1. for small session variable use buffer in stack;
    // 2. for large session variable use buffer in heap(by tc_malloc);
    char buf[OB_SHORT_SESSION_VAR_LENGTH] = {0};
    char *pbuf = buf;
    int64_t pos = 0;
    if (OB_FAIL((value_.*print_func)(pbuf, OB_SHORT_SESSION_VAR_LENGTH, pos, NULL))) {
      // large session variable
      if (OB_SIZE_OVERFLOW == ret || pos >= OB_SHORT_SESSION_VAR_LENGTH - 1) {
        LOG_DEBUG("long session variable comming");
        pos = 0; // reset pos
        // aa will print as 'aa'\0, so we add 3 if it is string type
        int64_t buf_length = value_.is_string_type() ? value_.get_string_len() + 3 :
                                                       OB_MAX_SESSION_VAR_LENGTH;
        if (OB_ISNULL(pbuf = static_cast<char *>(
                ob_malloc(buf_length, ObModIds::OB_PROXY_SESSION)))) {
          LOG_WDIAG("malloc failed", K(*this), K(ret));
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL((value_.*print_func)(pbuf, buf_length, pos, NULL))) {
          LOG_WDIAG("fail to print sql literal", K(is_sys_field), K(buf_length), K(*this), K(ret));
        } else {}
      } else {
        LOG_WDIAG("fail to print sql literal", K(is_sys_field), K(*this), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" %s%.*s = %.*s,",
                                 is_sys_field ? "@@" : "@",
                                 name_len_, name_, static_cast<int>(pos), pbuf))) {
        LOG_WDIAG("fail to format field", K(*this), K(ret));
      } else {}
    }
    // free memory if need
    if (buf != pbuf && NULL != pbuf) {
      ob_free(pbuf);
      pbuf = NULL;
    }
  }
  return ret;
}

int ObSessionBaseField::format(ObSqlString &sql,
                               const ObSessionFieldModifyMod modify_mod) const
{
  int ret = OB_SUCCESS;
  if (OB_FIELD_USED == stat_ && modify_mod_ == modify_mod) {
    bool is_sys_field = false;
    ret = format_util(sql, is_sys_field);
  }
  return ret;
}

DEF_TO_STRING(ObSessionBaseField)
{
  int64_t pos = 0;
  ObString name(name_len_, name_);
  J_OBJ_START();
  J_KV(K_(stat), K(name), K_(name_len), K_(value), K_(modify_mod));
  J_OBJ_END();
  return pos;
}

void ObSessionSysField::reset()
{
  ObSessionBaseField::reset();
  type_ = ObNullType;
  scope_ = ObSysVarFlag::NONE;
}

int ObSessionSysField::format(ObSqlString &sql,
                              const ObSessionFieldModifyMod modify_mod) const
{
  int ret = OB_SUCCESS;
  if (OB_FIELD_USED == stat_ && modify_mod_ == modify_mod && !is_readonly() && is_session_scope()) {
    bool is_sys_field = true;
    ret = format_util(sql, is_sys_field);
  }
  return ret;
}

int ObSessionSysField::format(ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  if (OB_FIELD_USED == stat_ && !is_readonly() && is_session_scope()) {
    bool is_sys_field = true;
    ret = format_util(sql, is_sys_field);
  }
  return ret;
}

//--------ObFieldBaseMgr-----------//
ObFieldBaseMgr::~ObFieldBaseMgr()
{
  destroy();
}

int ObFieldBaseMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(ret));
  } else if (OB_FAIL(ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE,
                                                      field_heap_))) {
    LOG_WDIAG("fail to init heap", K(ret));
  } else {
    if (OB_FAIL(alloc_block(sys_first_block_))) {
      LOG_WDIAG("fail to allocate Block", K(ret));
      field_heap_->destroy();
      field_heap_ = NULL;
    } else {
      sys_block_list_tail_ = sys_first_block_;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObFieldBaseMgr::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    ObFieldHeap *cur = field_heap_;
    ObFieldHeap *next = field_heap_;
    while (NULL != cur) {
      next = cur->get_next();
      cur->destroy();
      cur = next;
    };
    field_heap_ = NULL;
    is_inited_ = false;
  } else {
    // if not inited, do nothing
  }
}

int ObFieldBaseMgr::get_sys_first_block(const ObSysVarFieldBlock *&block_out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    block_out = sys_first_block_;
  }
  return ret;
}


int ObFieldBaseMgr::duplicate_field(const char *name, uint16_t name_len, const ObObj &src_obj,
                                    ObSessionBaseField &field)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(field_heap_->duplicate_str_and_obj(name, name_len, field.name_, field.name_len_,
                                                        src_obj, field.value_))) {
    LOG_WDIAG("fail to duplicate_str", K(name), K(name_len), K(src_obj), K(field), K(ret));
  }
  return ret;
}

//--------ObSessionFieldMgr------------//
ObSessionFieldMgr::ObSessionFieldMgr() : user_block_list_tail_(NULL), user_first_block_(NULL),
                                         str_block_list_tail_(NULL), str_first_block_(NULL),
                                         common_sys_block_list_tail_(NULL), common_sys_first_block_(NULL),
                                         mysql_sys_block_list_tail_(NULL), mysql_sys_first_block_(NULL),
                                         default_sys_var_set_(NULL), allow_var_not_found_(true)
{}

int ObSessionFieldMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(ret));
  } else if (OB_FAIL(ObFieldBaseMgr::init())) {
    LOG_WDIAG("fail to init base class", K(ret));
  } else if (OB_FAIL(alloc_block(user_first_block_))) {
    ObFieldBaseMgr::destroy();
    LOG_WDIAG("fail to allocate user block", K(ret));
  } else if (OB_FAIL(alloc_block(str_first_block_))) {
    ObFieldBaseMgr::destroy();
    LOG_WDIAG("fail to allocate str block", K(ret));
  } else if (OB_FAIL(alloc_block(common_sys_first_block_))) {
    ObFieldBaseMgr::destroy();
    LOG_WDIAG("fail to allocate str block", K(ret));
  } else if (OB_FAIL(alloc_block(mysql_sys_first_block_))) {
    ObFieldBaseMgr::destroy();
    LOG_WDIAG("fail to allocate str block", K(ret));
  } else {
    user_block_list_tail_ = user_first_block_;
    str_block_list_tail_ = str_first_block_;
    common_sys_block_list_tail_ = common_sys_first_block_;
    mysql_sys_block_list_tail_ = mysql_sys_first_block_;
    is_inited_ = true;
  }
  return ret;
}

void ObSessionFieldMgr::destroy()
{
  user_block_list_tail_ = NULL;
  user_first_block_ = NULL;
  str_block_list_tail_ = NULL;
  str_first_block_ = NULL;
  common_sys_block_list_tail_ = NULL;
  common_sys_first_block_ = NULL;
  mysql_sys_block_list_tail_ = NULL;
  mysql_sys_first_block_ = NULL;
  if (NULL != default_sys_var_set_) {
    default_sys_var_set_->dec_ref();
    default_sys_var_set_ = NULL;
  }
  ObFieldBaseMgr::destroy();
}

int ObSessionFieldMgr::calc_common_cold_sys_var_hash(uint64_t &hash_val)
{
  NeedFunc need_func = is_cold_modified_variable;
  return calc_var_hash_common(common_sys_first_block_, hash_val, need_func, ObString::make_string("common_cold_sys_var"));
}

int ObSessionFieldMgr::calc_common_hot_sys_var_hash(uint64_t &hash_val)
{
  NeedFunc need_func = is_hot_modified_variable;
  return calc_var_hash_common(common_sys_first_block_, hash_val, need_func, ObString::make_string("common_hot_sys_var"));
}

int ObSessionFieldMgr::calc_mysql_hot_sys_var_hash(uint64_t &hash_val)
{
  NeedFunc need_func = is_hot_modified_variable;
  return calc_var_hash_common(mysql_sys_first_block_, hash_val, need_func, ObString::make_string("mysql_hot_sys_var"));
}

int ObSessionFieldMgr::calc_mysql_cold_sys_var_hash(uint64_t &hash_val)
{
  NeedFunc need_func = is_cold_modified_variable;
  return calc_var_hash_common(mysql_sys_first_block_, hash_val, need_func, ObString::make_string("mysql_cold_sys_var"));
}

int ObSessionFieldMgr::calc_hot_sys_var_hash(uint64_t &hash_val)
{
  int ret = OB_SUCCESS;
  std::map<std::string, uint64_t> hash_map;
  uint64_t val_hash_val = 0;
  for (int i = 0; i < MAX_HOT_MODIFY_VARIABLES_NUM; i++) {
    const ObString& hot_var_name = hot_var_names[i];
    std::string var_name(hot_var_name.ptr(), hot_var_name.length());
    ObObj obj;
    if (OB_FAIL(get_sys_variable_value(hot_var_name, obj))) {
     //maybe not support, use 0 to hack it
      val_hash_val = 0;
    } else if (obj.is_varchar()) {
      val_hash_val = obj.varchar_hash(common::ObCharset::get_system_collation());
    } else {
      val_hash_val = obj.hash();
    }
    hash_map.insert(std::pair<std::string, uint64_t>(var_name, val_hash_val));
  }
  common::ObSqlString all_val_hash_str;
  for (std::map<std::string, uint64_t>::iterator it = hash_map.begin(); it != hash_map.end(); ++it) {
    all_val_hash_str.append_fmt("%s:%ld,", it->first.c_str(), it->second);
  }
  hash_val = all_val_hash_str.string().hash();
  LOG_DEBUG("host_sys_var all_hot_val_hash_str is ", K(all_val_hash_str), K(hash_val));
  return ret;
}

int ObSessionFieldMgr::calc_cold_sys_var_hash(uint64_t &hash_val)
{
  NeedFunc need_func = is_cold_modified_variable;
  return calc_var_hash_common(sys_first_block_, hash_val, need_func, ObString::make_string("cold_sys_var"));
}

int ObSessionFieldMgr::calc_user_var_hash(uint64_t &hash_val)
{
  return calc_var_hash_common(user_first_block_, hash_val, NULL, ObString::make_string("user_var"));
}

int ObSessionFieldMgr::set_sys_var_set(ObDefaultSysVarSet *set)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL != default_sys_var_set_)) {
    default_sys_var_set_->dec_ref();
    default_sys_var_set_ = NULL;
  }
  // NULL == set is also legal;
  if (NULL != set) {
    set->inc_ref();
    default_sys_var_set_ = set;
    set = NULL;
  }
  return ret;
}

int ObSessionFieldMgr::set_cluster_name(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_CLUSTER_NAME, cluster_name))) {
    LOG_WDIAG("fail to set cluster_name", K(cluster_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_tenant_name(const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_TENANT_NAME, tenant_name))) {
    LOG_WDIAG("fail to set tenant_name", K(tenant_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_vip_addr_name(const ObString &vip_addr_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_VIP_ADDR_NAME, vip_addr_name))) {
    LOG_WDIAG("fail to set vip addr name", K(vip_addr_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_user_name(const ObString &user_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_USER_NAME, user_name))) {
    LOG_WDIAG("fail to set user name", K(user_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_logic_tenant_name(const ObString &logic_tenant_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_UNLIKELY(logic_tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("logic_tenant_name is empty", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_LOGIC_TENANT_NAME, logic_tenant_name))) {
    LOG_WDIAG("fail to set logic tenant name", K(logic_tenant_name), K(ret));
  } else {
    LOG_DEBUG("succ to set logic tenant name", K(logic_tenant_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_logic_database_name(const ObString &logic_database_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_UNLIKELY(logic_database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("logic_database_name is empty", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_LOGIC_DATABASE_NAME, logic_database_name))) {
    LOG_WDIAG("fail to set logic database name", K(logic_database_name), K(ret));
  } else {
    LOG_DEBUG("succ to set logic database name", K(logic_database_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_database_name(const ObString &database_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("database_name is empty", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_DATABASE_NAME, database_name))) {
    LOG_WDIAG("fail to set database name", K(database_name), K(ret));
  } else {
    LOG_DEBUG("succ to set database name", K(database_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_service_name(const ObString &service_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_SERVICE_NAME, service_name))) {
    LOG_WDIAG("fail to set tenant_name", K(service_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_ldg_logical_cluster_name(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_LDG_LOGICAL_CLUSTER_NAME, cluster_name))) {
    LOG_WDIAG("fail to set tenant_name", K(cluster_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::set_ldg_logical_tenant_name(const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(replace_str_field(OB_V_FIELD_LDG_LOGICAL_TENANT_NAME, tenant_name))) {
    LOG_WDIAG("fail to set tenant_name", K(tenant_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_cluster_name(ObString &cluster_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_str_field_value(OB_V_FIELD_CLUSTER_NAME, cluster_name))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      LOG_DEBUG("cluster name not exists", K(ret));
    } else {
      LOG_WDIAG("fail to get cluster name", K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_tenant_name(ObString &tenant_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_str_field_value(OB_V_FIELD_TENANT_NAME, tenant_name))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      LOG_DEBUG("tenant name not exists", K(ret));
    } else {
      LOG_WDIAG("fail to get tenant name", K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_vip_addr_name(ObString &vip_addr_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_str_field_value(OB_V_FIELD_VIP_ADDR_NAME, vip_addr_name))) {
    LOG_DEBUG("fail to get vip addr name", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_user_name(ObString &user_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_str_field_value(OB_V_FIELD_USER_NAME, user_name))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      LOG_DEBUG("user name not exists", K(ret));
    } else {
      LOG_WDIAG("fail to get user name", K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_logic_tenant_name(ObString &logic_tenant_name) const
{
  return get_str_field_value(OB_V_FIELD_LOGIC_TENANT_NAME, logic_tenant_name);
}

int ObSessionFieldMgr::get_service_name(ObString &service_name) const
{
  return get_str_field_value(OB_V_FIELD_SERVICE_NAME, service_name);
}

int ObSessionFieldMgr::get_logic_database_name(ObString &logic_database_name) const
{
  return get_str_field_value(OB_V_FIELD_LOGIC_DATABASE_NAME, logic_database_name);
}

int ObSessionFieldMgr::get_ldg_logical_cluster_name(ObString &cluster_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_str_field_value(OB_V_FIELD_LDG_LOGICAL_CLUSTER_NAME, cluster_name))) {
    LOG_WDIAG("fail to get cluster name", K(cluster_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_ldg_logical_tenant_name(ObString &tenant_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_str_field_value(OB_V_FIELD_LDG_LOGICAL_TENANT_NAME, tenant_name))) {
    LOG_WDIAG("fail to get cluster name", K(tenant_name), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_database_name(ObString &database_name) const
{
  return get_str_field_value(OB_V_FIELD_DATABASE_NAME, database_name);
}

//common sys variables related methords
int ObSessionFieldMgr::insert_common_sys_variable(const ObString &name, const ObObj &value,
                                                  ObSessionSysField *&field, ObDefaultSysVarSet *var_set)
{
  int ret = OB_SUCCESS;
  if (NULL != var_set && OB_FAIL(var_set->get_sys_variable(name, field))) {
    LOG_WDIAG("fail to get sys var from global set", K(name), K(ret));
  } else {
    ObSysVarFieldBlock *block_out = NULL;
    ObSessionSysField *new_field = NULL;
    bool is_reused = false;
    if (OB_FAIL(alloc_field(common_sys_first_block_, common_sys_block_list_tail_, block_out, new_field, is_reused))) {
      LOG_WDIAG("fail to alloc sys var field", K(ret));
    } else if (OB_ISNULL(block_out) || OB_ISNULL(new_field)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected null pointer", K(ret));
    } else {
      if (NULL != field) {
        new_field->scope_ = field->scope_;
        new_field->type_ = field->type_;
        new_field->modify_mod_ = field->modify_mod_;
      } else {
        if (0 == name.case_compare("character_set_results")) {
          new_field->scope_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NULLABLE;
        } else {
          new_field->scope_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
        }
        new_field->type_ = value.get_type();
        if (ObSessionFieldMgr::is_hot_modified_variable(name)) {
          new_field->modify_mod_ = OB_FIELD_HOT_MODIFY_MOD;
        } else {
          new_field->modify_mod_ = OB_FIELD_COLD_MODIFY_MOD;
        }
      }
      uint16_t len = static_cast<uint16_t>(name.length());

      ObFieldObjCaster caster;
      const ObObj *res_cell = NULL;
      if (OB_FAIL(caster.obj_cast(value, *new_field, res_cell))) {
        LOG_WDIAG("fail to cast obj", K(value), K(*new_field), K(ret));
      } else if (OB_ISNULL(res_cell)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("res_cell is null , which is unexpected", K(ret));
      } else if (OB_FAIL(duplicate_field(name.ptr(), len, *res_cell, *new_field))) {
        LOG_WDIAG("fail to duplicate_field", K(name), K(value), K(ret));
      } else {
        new_field->stat_ = OB_FIELD_USED;
        field = new_field;
        if (is_reused) {
          block_out->dec_removed_count();
        }
      }
    }
  }

  return ret;
}

int ObSessionFieldMgr::update_common_sys_variable(const ObObj &value, ObSessionSysField *&field)
{
  int ret = OB_SUCCESS;
  ObFieldObjCaster caster;
  const ObObj *res_cell = NULL;
  if (OB_ISNULL(field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else if (OB_FAIL(caster.obj_cast(value, *field, res_cell))) {
    LOG_WDIAG("fail to cast obj", K(value), K(*field), K(ret));
  } else if (OB_ISNULL(res_cell)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("res_cell is null , which is unexpected", K(ret));
  } else if (OB_FAIL(update_field_value(*res_cell, field->value_))) {
    LOG_WDIAG("fail to update field value", K(*res_cell), K(field->value_), K(ret));
  }

  return ret;
}

int ObSessionFieldMgr::update_common_sys_variable(const ObString &name, const ObObj &value,
                                                  ObSessionSysField *&field, const bool is_need_insert,
                                                  const bool is_oceanbase)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_common_sys_variable(name, field))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_need_insert) {
      if (is_oceanbase && OB_ISNULL(default_sys_var_set_)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WDIAG("sys var set has not been added", K(ret));
      } else {
        if (OB_FAIL(insert_common_sys_variable(name, value, field,
                                               is_oceanbase ? default_sys_var_set_ : NULL)))  {
          LOG_WDIAG("fail to insert common sys var", K(name), K(value), K(ret));
        }
      }
    }
  } else if (OB_FAIL(update_common_sys_variable(value, field))) {
    LOG_WDIAG("fail to update common sys variable", K(name), K(value), K(ret));
  }

  return ret;
}

int ObSessionFieldMgr::insert_mysql_system_variable(const ObString &name, const ObObj &value)
{
  ObSessionSysField *field = NULL;
  return insert_mysql_system_variable(name, value, field);
}

int ObSessionFieldMgr::insert_common_sys_variable(const ObString &name, const ObObj &value, bool is_oceanbase)
{
  int ret = OB_SUCCESS;
  ObSessionSysField *field = NULL;
  if (is_oceanbase && OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K(ret));
  } else {
    insert_common_sys_variable(name, value, field, is_oceanbase ? default_sys_var_set_ : NULL);
  }
  return ret;
}

int ObSessionFieldMgr::insert_mysql_system_variable(const ObString &name, const ObObj &value, ObSessionSysField *&field)
{
  int ret = OB_SUCCESS;

  ObSysVarFieldBlock *block_out = NULL;
  ObSessionSysField *new_field = NULL;
  bool is_reused = false;
  uint16_t len = static_cast<uint16_t>(name.length());
  if (OB_FAIL(alloc_field(mysql_sys_first_block_, mysql_sys_block_list_tail_, block_out, new_field, is_reused))) {
    LOG_WDIAG("fail to alloc user var field", K(name), K(ret));
  } else if (OB_ISNULL(new_field) || OB_ISNULL(block_out)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else {
    new_field->scope_ = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE;
    new_field->type_ = ObVarcharType;
    if (ObSessionFieldMgr::is_hot_modified_variable(name)) {
      new_field->modify_mod_ = OB_FIELD_HOT_MODIFY_MOD;
    } else {
      new_field->modify_mod_ = OB_FIELD_COLD_MODIFY_MOD;
    }

    if (OB_FAIL(duplicate_field(name.ptr(), len, value, *new_field))) {
      LOG_WDIAG("fail to duplicate_field", K(name), K(value), K(ret));
    } else {
      new_field->stat_ = OB_FIELD_USED;
      field = new_field;
      if (is_reused) {
        block_out->dec_removed_count();
      }
    }
  }
  return ret;
}

int ObSessionFieldMgr::update_mysql_system_variable(const ObString &name, const ObObj &value, ObSessionSysField *&field)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_mysql_sys_variable(name, field))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(insert_mysql_system_variable(name, value, field))) {
        LOG_WDIAG("fail to insert mysql system variable", K(name), K(value), K(ret));
      }
    }
  } else if (OB_FAIL(update_field_value(value, field->value_))) {
    LOG_WDIAG("fail to update field value", K(name), K(value), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::update_system_variable(const ObString &name, const ObObj &value, ObSessionSysField *&field)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else {
    ObSysVarFieldBlock *block_out = NULL;
    bool is_reused = false;
    if (OB_ENTRY_NOT_EXIST == (ret = get_sys_variable_local(name, field))) {
      if (OB_FAIL(default_sys_var_set_->get_sys_variable(name, field))) {
        LOG_WDIAG("fail to get sys var from global set", K(name), K(ret));
      } else {
        ObSessionSysField *new_field = NULL;
        if (OB_FAIL(alloc_field(sys_first_block_, sys_block_list_tail_, block_out, new_field, is_reused))) {
          LOG_WDIAG("fail to alloc sys var field", K(ret));
        } else if (OB_ISNULL(block_out) || OB_ISNULL(new_field)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected null pointer", K(ret));
        } else {
          new_field->scope_ = field->scope_;
          new_field->type_ = field->type_;
          new_field->modify_mod_ = field->modify_mod_;
          uint16_t len = static_cast<uint16_t>(name.length());

          ObFieldObjCaster caster;
          const ObObj *res_cell = NULL;
          if (OB_FAIL(caster.obj_cast(value, *new_field, res_cell))) {
            LOG_WDIAG("fail to cast obj", K(value), K(*new_field), K(ret));
          } else if (OB_ISNULL(res_cell)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("res_call is null , which is unexpected", K(ret));
          } else if (OB_FAIL(duplicate_field(name.ptr(), len, *res_cell, *new_field))) {
            LOG_WDIAG("fail to duplicate_field", K(name), K(value), K(ret));
          } else {
            new_field->stat_ = OB_FIELD_USED;
            field = new_field;
            if (is_reused) {
              block_out->dec_removed_count();
            }
          }
        }
      }
    } else if (OB_SUCC(ret)) {
      ObFieldObjCaster caster;
      const ObObj *res_cell = NULL;
      if (OB_ISNULL(field)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected null pointer", K(ret));
      } else if (OB_FAIL(caster.obj_cast(value, *field, res_cell))) {
        LOG_WDIAG("fail to cast obj", K(value), K(*field), K(ret));
      } else if (OB_ISNULL(res_cell)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("res_call is null , which is unexpected", K(ret));
      } else if (OB_FAIL(update_field_value(*res_cell, field->value_))) {
        LOG_WDIAG("fail to update field value", K(*res_cell), K(field->value_), K(ret));
      }
    } else if (OB_FAIL(ret)) {
      LOG_WDIAG("fail to get sys var from local set", K(name), K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_sys_variable(const ObString &name, ObSessionSysField *&value) const
{
  int ret = OB_SUCCESS;
  value = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else if (OB_ENTRY_NOT_EXIST == (ret = get_sys_variable_local(name, value))) {
    if (OB_FAIL(default_sys_var_set_->get_sys_variable(name, value, allow_var_not_found_))) {
      if (allow_var_not_found_) {
        LOG_DEBUG("fail to get sys variable from default sys var set", K(name), K(ret));
      } else {
        LOG_WDIAG("fail to get sys variable from default sys var set", K(name), K(ret));
      }
    }
  } else if (OB_FAIL(ret)) {
    LOG_WDIAG("fail to get sys variable from local var set", K(name), K(ret));
  } else {
    // get variable from local
  }
  return ret;
}

int ObSessionFieldMgr::get_sys_variable_value(const ObString &name, ObObj &value) const
{
  int ret = OB_SUCCESS;
  ObSessionSysField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_sys_variable(name, field))) {
    if (allow_var_not_found_) {
      LOG_DEBUG("fail to get sys variable", K(name), K(ret));
    } else {
      LOG_WDIAG("fail to get sys variable", K(name), K(ret));
    }
  } else if (OB_ISNULL(field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else {
    value = field->value_;
  }
  return ret;
}

common::ObObj ObSessionFieldMgr::get_sys_variable_value(const char* name) const
{
  int ret = OB_SUCCESS;
  common::ObObj obj;
  if (OB_FAIL(get_sys_variable_value(name, obj))) {
  }
  return obj;
}

int ObSessionFieldMgr::get_sys_variable_value(const char *name, ObObj &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid var name :NULL", K(ret));
  } else {
    ret = get_sys_variable_value(ObString::make_string(name), value);
  }
  return ret;
}

int ObSessionFieldMgr::sys_variable_exists(const ObString &name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else {
    if (false == (is_exist = sys_variable_exists_local(name))) {
      if (OB_FAIL(default_sys_var_set_->sys_variable_exists(name, is_exist))) {
        LOG_WDIAG("fail to check whether var is exist in global sys var set", K(name), K(ret));
      }
    }
  }
  return ret;
}

int ObSessionFieldMgr::is_equal_with_snapshot(const ObString &sys_var_name,
                                              const ObObj &value, bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  ObSessionSysField *field = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else if (OB_UNLIKELY(sys_var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(sys_var_name), K(ret));
  } else if (OB_FAIL(default_sys_var_set_->get_sys_variable(sys_var_name, field))) {
    LOG_WDIAG("fail to get sys var from global set", K(sys_var_name), K(ret));
  } else if (OB_ISNULL(field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else {
    ObObj &default_obj = field->value_;

    //FIXME readonly and only global scope var no need to reset session variable
    if (field->is_readonly() || (!field->is_session_scope())) {
      is_equal = true;

    } else {
      ObFieldObjCaster caster;
      const ObObj *res_cell = NULL;

      if (OB_FAIL(caster.obj_cast(value, *field, res_cell))) {
        LOG_WDIAG("fail to cast obj", K(value), K(field), K(ret));
      } else if (OB_ISNULL(res_cell)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("res_call is null , which is unexpected", K(ret));
      } else if (0 == res_cell->compare(default_obj, CS_TYPE_UTF8MB4_GENERAL_CI)) {
        //FIXME consider the character set no need to reset session variable
        is_equal = true;
      } else {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_sys_variable_type(const ObString &name, ObObjType &type)
{
  int ret = OB_SUCCESS;
  ObSessionSysField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_sys_variable(name, field))) {
    LOG_WDIAG("fail to get sys variable", K(name), K(ret));
  } else if (OB_ISNULL(field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else {
    type = field->type_;
  }
  return ret;
}

//sys variables related methords
int ObSessionFieldMgr::replace_variable_common(const ObString &name, const ObObj &value,
    ObSessionVarType var_type, bool is_oceanbase /*true*/)
{
  int ret = OB_SUCCESS;
  bool succ_get = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObSessionUserField *field = NULL;
    ObSessionSysField *sys_field = NULL;
    switch(var_type) {
      case OB_SESSION_USER_VAR:
        if (OB_ENTRY_NOT_EXIST == (ret = get_user_variable(name, field))) {
          if (OB_FAIL(insert_user_variable(name, value))) {
            LOG_WDIAG("fail to insert user variable", K(name), K(value), K(ret));
          }
        } else if (OB_SUCC(ret)) {
          succ_get = true;
        }
        break;
      case OB_SESSION_COMMON_SYS_VAR:
        if (OB_ENTRY_NOT_EXIST == (ret = get_common_sys_variable(name, sys_field))) {
          if (OB_FAIL(insert_common_sys_variable(name, value, is_oceanbase))) {
            LOG_WDIAG("fail to insert user variable", K(name), K(value), K(ret));
          }
        } else if (OB_SUCC(ret)) {
          field = sys_field;
          succ_get = true;
        }
        break;
      case OB_SESSION_MYSQL_SYS_VAR:
        if (OB_ENTRY_NOT_EXIST == (ret = get_mysql_sys_variable(name, sys_field))) {
          if (OB_FAIL(insert_mysql_system_variable(name, value))) {
            LOG_WDIAG("fail to insert user variable", K(name), K(value), K(ret));
          }
        } else if (OB_SUCC(ret)) {
          field = sys_field;
          succ_get = true;
        }
        break;
      default:
        LOG_WDIAG("invalid type", K(var_type));
        ret = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCC(ret)) {
      if (succ_get) {
        if (OB_ISNULL(field)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected null pointer", K(ret));
        } else if (OB_FAIL(update_field_value(value, field->value_))) {
          LOG_WDIAG("fail to update field value", K(name), K(value), K(ret));
        } else {
          LOG_DEBUG("replace succ", K(name), K(value), K(var_type));
        }
      }
    } else {
      LOG_WDIAG("fail to get variable", K(name), K(var_type), K(is_oceanbase), K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::replace_user_variable(const ObString &name, const ObObj &value)
{
  return replace_variable_common(name, value, OB_SESSION_USER_VAR);
}

int ObSessionFieldMgr::replace_mysql_sys_variable(const ObString &name, const ObObj &value)
{
  return replace_variable_common(name, value, OB_SESSION_MYSQL_SYS_VAR, false);
}

int ObSessionFieldMgr::replace_common_sys_variable(const ObString &name, const ObObj &value, bool is_oceanbase/*true*/)
{
  return replace_variable_common(name, value, OB_SESSION_COMMON_SYS_VAR, is_oceanbase);
}

int ObSessionFieldMgr::remove_variable_common(const common::ObString &name, ObSessionVarType var_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObSessionUserField *field_ptr = NULL;
    ObSessionSysField *sys_field_ptr = NULL;
    ObUserVarFieldBlock *block = NULL;
    ObSysVarFieldBlock *sys_block = NULL;
    switch(var_type) {
      case OB_SESSION_USER_VAR:
        ret = get_user_variable(name, field_ptr, block);
        break;
      case OB_SESSION_MYSQL_SYS_VAR:
        ret = get_mysql_sys_variable(name, sys_field_ptr, sys_block);
        break;
      case OB_SESSION_COMMON_SYS_VAR:
        ret = get_common_sys_variable(name, sys_field_ptr, sys_block);
        break;
      default:
        LOG_WDIAG("invalid type", K(name), K(var_type));
        ret = OB_INVALID_ARGUMENT;
    }

    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WDIAG("variable is not exist", K(name), K(var_type), K(ret));
    } else if (OB_SUCC(ret) && NULL != field_ptr) {
      if (OB_SESSION_USER_VAR == var_type) {
        field_heap_->free_obj(field_ptr->value_);
        field_heap_->free_string(field_ptr->name_, field_ptr->name_len_);
        field_ptr->reset();
        block->inc_removed_count();
      } else {
        field_heap_->free_obj(sys_field_ptr->value_);
        field_heap_->free_string(sys_field_ptr->name_, sys_field_ptr->name_len_);
        sys_field_ptr->reset();
        sys_block->inc_removed_count();
      }
    } else {
      LOG_WDIAG("fail to get_variable", K(name), K(var_type), K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::remove_user_variable(const ObString &name)
{
  return remove_variable_common(name, OB_SESSION_USER_VAR);
}

int ObSessionFieldMgr::remove_mysql_sys_variable(const ObString &name)
{
  return remove_variable_common(name, OB_SESSION_MYSQL_SYS_VAR);
}

int ObSessionFieldMgr::remove_common_sys_variable(const ObString &name)
{
  return remove_variable_common(name, OB_SESSION_COMMON_SYS_VAR);
}

int ObSessionFieldMgr::get_user_variable(const ObString &name, ObSessionUserField *&value) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObUserVarFieldBlock *block = NULL;
    if (OB_ENTRY_NOT_EXIST == (ret = get_user_variable(name, value, block))) {
      LOG_INFO("user variable not exist", K(name), K(ret));
    } else if (OB_FAIL(ret)) {
      LOG_WDIAG("fail to get user variable", K(name), K(ret));
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_variable_value_common(const ObString &name, ObObj &value, ObSessionVarType var_type) const
{
  int ret = OB_SUCCESS;
  ObSessionUserField *field = NULL;
  ObSessionSysField *sys_field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    switch(var_type) {
      case OB_SESSION_USER_VAR:
        ret = get_user_variable(name, field);
        break;
      case OB_SESSION_MYSQL_SYS_VAR:
        ret = get_mysql_sys_variable(name, sys_field);
        field = sys_field;
        break;
      case OB_SESSION_COMMON_SYS_VAR:
        ret = get_common_sys_variable(name, sys_field);
        field = sys_field;
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid type", K(name), K(var_type));
    }

    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("fail to get variable", K(name), K(var_type), K(ret));
      } else {
        LOG_WDIAG("fail to get variable", K(name), K(var_type), K(ret));
      }
    } else if (OB_ISNULL(field)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected null pointer", K(ret));
    } else {
      value = field->value_;
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_user_variable_value(const ObString &name, ObObj &value) const
{
  return get_variable_value_common(name, value, OB_SESSION_USER_VAR);
}

int ObSessionFieldMgr::get_common_sys_variable_value(const common::ObString &name, ObObj &value) const
{
  return get_variable_value_common(name, value, OB_SESSION_COMMON_SYS_VAR);
}

int ObSessionFieldMgr::get_mysql_sys_variable_value(const common::ObString &name,  ObObj &value) const
{
  return get_variable_value_common(name, value, OB_SESSION_MYSQL_SYS_VAR);
}

int ObSessionFieldMgr::user_variable_exists(const ObString &name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    is_exist = false;
    const ObUserVarFieldBlock *block = user_first_block_;
    ObSessionUserField *field = NULL;
    ObString var_name;
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionUserField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == name) {
            is_exist = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObSessionFieldMgr::format_all_var(ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_ERR_UNEXPECTED;
  static const int BUCKET_SIZE = 64;
  ObHashSet<ObString, NoPthreadDefendMode> local_var_set;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("session field not inited", K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else if (OB_FAIL(local_var_set.create(BUCKET_SIZE))) {
    LOG_WDIAG("hash table init failed", K(ret));
  } else {
    // 0. set common system session variable (hot,cold)
    const ObSysVarFieldBlock *block = common_sys_first_block_;
    ObSessionSysField *field = NULL;
    ObString var_name;
    for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          if (OB_FAIL(field->format(sql))) {
            LOG_WDIAG("construct sql failed", K(ret));
          } else {
            var_name.assign(const_cast<char *>(field->name_), field->name_len_);
            if (OB_FAIL(local_var_set.set_refactored(var_name))) {
              LOG_WDIAG("local_var_set set failed", K(ret));
            }
          }
        }
      }
    }

    // 1. set system session variable (hot,cold,last_insert_id)
    //    by local field mgr
    block = sys_first_block_;
    field = NULL;
    for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          hash_ret = local_var_set.exist_refactored(var_name);
          if (OB_HASH_EXIST == hash_ret) {
            // do nothing
          } else if (OB_HASH_NOT_EXIST != hash_ret) {
            ret = hash_ret;
            LOG_WDIAG("local_var_set set failed", K(ret));
          } else {
            if (OB_FAIL(field->format(sql))) {
              LOG_WDIAG("construct sql failed", K(ret));
            } else if (OB_FAIL(local_var_set.set_refactored(var_name))) {
              LOG_WDIAG("local_var_set set failed", K(ret));
            }
          }
        }
      }
    }

    // 2. set system session variable by global snapshot
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(default_sys_var_set_->get_sys_first_block(block))) {
      LOG_WDIAG("global sys var set not inited", K(ret));
    } else {
      field = NULL;
      ObString var_name;
      for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < block->free_idx_; ++i) {
          field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
          if (OB_FIELD_USED == field->stat_) {
            var_name.assign(const_cast<char *>(field->name_), field->name_len_);
            hash_ret = local_var_set.exist_refactored(var_name);
            if (OB_HASH_EXIST == hash_ret) {
              // do nothing
            } else if (OB_HASH_NOT_EXIST != hash_ret) {
              ret = hash_ret;
              LOG_WDIAG("local_var_set set failed", K(ret));
            } else if (OB_FAIL(field->format(sql))) {
              LOG_WDIAG("construct sql failed", K(ret));
            } else {
              // do nothing
            }
          }
        }
      }
    }
    // 3. set user-defined session
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_user_var(sql))) {
      LOG_WDIAG("construct user var failed", K(ret));
    } else {
      // do nothing
    }
  }
  local_var_set.destroy();
  return ret;
}

int ObSessionFieldMgr::format_common_sys_var(ObSqlString &sql) const
{
  return format_var(common_sys_first_block_, sql, OB_FIELD_COLD_MODIFY_MOD);
}

int ObSessionFieldMgr::format_common_hot_sys_var(ObSqlString &sql) const
{
  return format_var(common_sys_first_block_, sql, OB_FIELD_HOT_MODIFY_MOD);
}

int ObSessionFieldMgr::format_mysql_sys_var(ObSqlString &sql) const
{
  return format_var(mysql_sys_first_block_, sql, OB_FIELD_COLD_MODIFY_MOD);
}

int ObSessionFieldMgr::format_mysql_hot_sys_var(ObSqlString &sql) const
{
  return format_var(mysql_sys_first_block_, sql, OB_FIELD_HOT_MODIFY_MOD);
}

int ObSessionFieldMgr::format_sys_var(ObSqlString &sql) const
{
  return format_var(sys_first_block_, sql, OB_FIELD_COLD_MODIFY_MOD);
}

int ObSessionFieldMgr::format_hot_sys_var(ObSqlString &sql) const
{
  return format_var(sys_first_block_, sql, OB_FIELD_HOT_MODIFY_MOD);
}

int ObSessionFieldMgr::format_last_insert_id(ObSqlString &sql) const
{
  int ret = OB_SUCCESS;
  ObSessionSysField *field;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_sys_variable(ObString::make_string(OB_SV_LAST_INSERT_ID), field))) {
    LOG_WDIAG("get last_insert_id field error", K(ret));
  } else if (OB_ISNULL(field)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WDIAG("unexpect null value in get_sys_variable", K(ret));
  } else if (OB_FAIL(field->format_util(sql, true))) {
    LOG_WDIAG("fail to construct last_insert_id", K(ret));
  } else {}
  return ret;
}

int ObSessionFieldMgr::format_user_var(ObSqlString &sql) const
{
  return format_var(user_first_block_, sql, OB_FIELD_COLD_MODIFY_MOD);
}

int ObSessionFieldMgr::get_user_variable(const ObString &name, ObSessionUserField *&value,
                                         ObUserVarFieldBlock *&out_block) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObUserVarFieldBlock *block = user_first_block_;
  ObSessionUserField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObString var_name;
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionUserField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == name) {
            value = field;
            out_block = const_cast<ObUserVarFieldBlock *>(block);
            is_exist = true;
          }
        }
      }
    }
  }
  // set ret value to OB_ENTRY_NOT_EXIST if not exist
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {} // keep the ret value
  } else {} // keep the ret value
  return ret;
}

int ObSessionFieldMgr::get_common_sys_variable(const ObString &name, ObSessionSysField *&value,
                                         ObSysVarFieldBlock *&out_block) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObSysVarFieldBlock *block = common_sys_first_block_;
  ObSessionSysField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObString var_name;
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == name) {
            value = field;
            out_block = const_cast<ObSysVarFieldBlock *>(block);
            is_exist = true;
          }
        }
      }
    }
  }
  // set ret value to OB_ENTRY_NOT_EXIST if not exist
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {} // keep the ret value
  } else {} // keep the ret value
  return ret;
}

int ObSessionFieldMgr::get_mysql_sys_variable(const ObString &name, ObSessionSysField *&value,
                                         ObSysVarFieldBlock *&out_block) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObSysVarFieldBlock *block = mysql_sys_first_block_;
  ObSessionSysField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObString var_name;
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == name) {
            value = field;
            out_block = const_cast<ObSysVarFieldBlock *>(block);
            is_exist = true;
          }
        }
      }
    }
  }
  // set ret value to OB_ENTRY_NOT_EXIST if not exist
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {} // keep the ret value
  } else {} // keep the ret value
  return ret;
}

int ObSessionFieldMgr::insert_user_variable(const ObString &name, const ObObj &value)
{
  int ret = OB_SUCCESS;
  ObSessionUserField *new_field = NULL;
  ObUserVarFieldBlock *block_out = NULL;
  bool is_reused = false;
  uint16_t len = static_cast<uint16_t>(name.length());
  if (OB_FAIL(alloc_field(user_first_block_, user_block_list_tail_, block_out, new_field, is_reused))) {
    LOG_WDIAG("fail to alloc user var field", K(name), K(ret));
  } else if (OB_ISNULL(new_field) || OB_ISNULL(block_out)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else if (OB_FAIL(duplicate_field(name.ptr(), len, value, *new_field))) {
    LOG_WDIAG("fail to duplicate_field", K(name), K(value), K(ret));
  } else {
    new_field->stat_ = OB_FIELD_USED;
    if (is_reused) {
      block_out->dec_removed_count();
    }
  }
  return ret;
}

//use old value if condition allows;make sure dest_obj is not modified if failed
int ObSessionFieldMgr::update_field_value(const ObObj &src_obj, ObObj &dest_obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (dest_obj.get_deep_copy_size() >= src_obj.get_deep_copy_size()) {
    int64_t pos = 0;
    void *buf = const_cast<void *>(dest_obj.get_data_ptr());
    if (OB_FAIL(dest_obj.deep_copy(src_obj, static_cast<char *>(buf),
                                   dest_obj.get_deep_copy_size(), pos))) {
      LOG_WDIAG("fail to deep copy", K(src_obj), K(dest_obj), K(ret));
    } else {
      const int64_t free_len = dest_obj.get_deep_copy_size() - src_obj.get_deep_copy_size();
      field_heap_->free_string(static_cast<char *>(buf) + src_obj.get_deep_copy_size(), free_len);
    }
  } else {
    ObObj old_obj = dest_obj;
    if (OB_FAIL(field_heap_->duplicate_obj(src_obj, dest_obj))) {
    } else {
      field_heap_->free_obj(old_obj);
    }
  }
  return ret;
}

int ObSessionFieldMgr::remove_str_field(const ObVFieldType type)
{
  int ret = OB_SUCCESS;
  ObStrFieldBlock *block = str_first_block_;
  ObSessionVField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  }

  for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObStrFieldBlock::FIELD_BLOCK_SLOTS_NUM; ++i) {
      if (type == block->field_slots_[i].type_) {
        field = const_cast<ObSessionVField *>(&(block->field_slots_[i]));
        break;
      }
    }
    if (NULL != field) {
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL != field) {
      char *value = const_cast<char *>(field->value_);
      uint16_t len = field->value_len_;
      field_heap_->free_string(value, len);
      field->reset();
      block->inc_removed_count();
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WDIAG("fail to get str field", "str_field_type", type, K(ret));
    }
  }

  return ret;
}

int ObSessionFieldMgr::replace_str_field(const ObVFieldType type, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("value is empty", K(type), K(value), K(ret));
  } else {
    const ObStrFieldBlock *block = str_first_block_;
    ObSessionVField *field = NULL;
    for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ObStrFieldBlock::FIELD_BLOCK_SLOTS_NUM; ++i) {
        if (type == block->field_slots_[i].type_) {
          field = const_cast<ObSessionVField *>(&(block->field_slots_[i]));
          break;
        }
      }
      if (NULL != field) {
        break;
      }
    }

    if (NULL != field) {
      //already exist ,just update
      char *old_value = const_cast<char *>(field->value_);
      uint16_t old_len = field->value_len_;
      uint16_t new_len = static_cast<uint16_t>(value.length());
      if (old_len >= new_len) {
        //reuse old memory
        MEMCPY(old_value, value.ptr(), value.length());
        field->value_len_ = new_len;
        field_heap_->free_string(field->value_ + value.length(), old_len - new_len);
      } else {
        if (OB_FAIL(field_heap_->duplicate_str(value.ptr(), new_len, field->value_, field->value_len_))) {
          LOG_WDIAG("fail to duplicate_str", K(type), K(value), K(field), K(ret));
        } else {
          field_heap_->free_string(old_value, static_cast<int64_t>(old_len));
        }
      }
    } else {
      //insert this field
      if (OB_FAIL(insert_str_field(type, value))) {
        LOG_WDIAG("fail to insert_str_field", K(value), K(field), K(ret));
      }
    }
  }
  return ret;
}

int ObSessionFieldMgr::insert_str_field(const ObVFieldType type, const ObString &value)
{
  int ret = OB_SUCCESS;
  ObSessionVField *new_field = NULL;
  ObStrFieldBlock *block_out = NULL;
  bool is_reused = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(alloc_field(str_first_block_, str_block_list_tail_, block_out, new_field,
                                 is_reused))) {
    LOG_WDIAG("fail to alloc v field", K(ret));
  } else if (OB_ISNULL(new_field) || OB_ISNULL(block_out)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else if (OB_FAIL(field_heap_->duplicate_str(value.ptr(), static_cast<uint16_t>(value.length()),
                                                new_field->value_, new_field->value_len_))) {
    LOG_WDIAG("fail to duplicate str", K(ret));
  } else {
    new_field->type_ = type;
    if (is_reused) {
      block_out->dec_removed_count();
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_str_field_value(const ObVFieldType type, ObString &value) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObStrFieldBlock *block = str_first_block_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0;
           OB_SUCCESS == ret && !is_exist && i < ObStrFieldBlock::FIELD_BLOCK_SLOTS_NUM; ++i) {
        if (type == block->field_slots_[i].type_) {
          value.assign(const_cast<char *>(block->field_slots_[i].value_),
                       block->field_slots_[i].value_len_);
          is_exist = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {} // keep the ret value
  } else {} // keep the ret value
  return ret;
}

bool ObSessionFieldMgr::str_field_exists(const ObVFieldType type) const
{
  bool is_exist = false;
  const ObStrFieldBlock *block = str_first_block_;
  if (OB_UNLIKELY(!is_inited_)) {
    is_exist = false;
  } else {
    for (; !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; !is_exist && i < ObStrFieldBlock::FIELD_BLOCK_SLOTS_NUM; ++i) {
        if (type == block->field_slots_[i].type_) {
          is_exist = true;
        }
      }
    }
  }
  return is_exist;
}

int ObSessionFieldMgr::get_changed_sys_var_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_var_names_common(sys_first_block_, names))) {
    LOG_WDIAG("fail to get changed sys var_names", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_all_sys_var_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  const ObSysVarFieldBlock *block = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else if (OB_FAIL(default_sys_var_set_->get_sys_first_block(block))) {
    LOG_WDIAG("global sys var set not inited", K(ret));
  } else if (OB_FAIL(get_var_names_common(block, names))) {
    LOG_WDIAG("fail to get all sys var_names", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_all_common_sys_var_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_var_names_common(common_sys_first_block_, names))) {
    LOG_WDIAG("fail to get all sys var_names", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_all_mysql_sys_var_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_var_names_common(mysql_sys_first_block_, names))) {
    LOG_WDIAG("fail to get all sys var_names", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_all_user_var_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_var_names_common(user_first_block_, names))) {
    LOG_WDIAG("fail to get all user var_names", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_all_changed_sys_vars(ObIArray<ObSessionSysField> &fields)
{
  int ret = OB_SUCCESS;
  ObSessionSysField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_vars_common(sys_first_block_, fields, field))) {
    LOG_WDIAG("fail to get all changed sys vars", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_all_sys_vars(ObIArray<ObSessionSysField> &fields)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_ERR_UNEXPECTED;
  const ObSysVarFieldBlock *block = NULL;
  ObSessionSysField *field = NULL;
  ObString var_name;
  static const int BUCKET_SIZE = 64;
  ObHashSet<ObString, NoPthreadDefendMode> local_var_set;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("session field not inited", K(ret));
  } else if (OB_ISNULL(default_sys_var_set_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("sys var set has not been added", K_(default_sys_var_set), K(ret));
  } else if (OB_FAIL(local_var_set.create(BUCKET_SIZE))) {
    LOG_WDIAG("hash table init failed", K(ret));
  } else {
    // 1. get changed sys vars
    block = sys_first_block_;
    for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (OB_FAIL(fields.push_back(*field))) {
            LOG_WDIAG("fail to push back var_name", K(var_name), K(ret));
          } else if (OB_FAIL(local_var_set.set_refactored(var_name))) {
            LOG_WDIAG("local_var_set set failed", K(ret));
          } else {
            // do nothing
          }
        }
      }
    }
  }

  // 2.get all default_sys vars except local changed sys vars
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(default_sys_var_set_->get_sys_first_block(block))) {
    LOG_WDIAG("global sys var set not inited", K(ret));
  } else {
    field = NULL;
    for (; OB_SUCC(ret) && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          hash_ret = local_var_set.exist_refactored(var_name);
          if (OB_HASH_EXIST == hash_ret) {
            // do nothing
          } else if (OB_HASH_NOT_EXIST != hash_ret) {
            ret = hash_ret;
            LOG_WDIAG("local_var_set set failed", K(ret));
          } else if (OB_FAIL(fields.push_back(*field))) {
            LOG_WDIAG("fail to push back var_name", K(var_name), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObSessionFieldMgr::get_all_user_vars(ObIArray<ObSessionBaseField> &fields)
{
  int ret = OB_SUCCESS;
  ObSessionBaseField *field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_vars_common(user_first_block_, fields, field))) {
    LOG_WDIAG("fail to get all user vars", K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::get_sys_variable_from_block(const ObString &name, ObSessionSysField *&value,
                                                   const ObSysVarFieldBlock *block) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObSessionSysField *field = NULL;
  ObString var_name;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == name) {
            value = field;
            is_exist = true;
          }
        }
      }
    }
  }
  // set ret value to OB_ENTRY_NOT_EXIST if not exist
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {} // keep the ret value
  } else {} // keep the ret value

  return ret;
}

int ObSessionFieldMgr::get_common_sys_variable(const ObString &name, ObSessionSysField *&value) const
{
  return get_sys_variable_from_block(name, value, common_sys_first_block_);
}

int ObSessionFieldMgr::get_mysql_sys_variable(const ObString &name, ObSessionSysField *&value) const
{
  return get_sys_variable_from_block(name, value, mysql_sys_first_block_);
}

int ObSessionFieldMgr::get_sys_variable_local(const ObString &name, ObSessionSysField *&value) const
{
  return get_sys_variable_from_block(name, value, sys_first_block_);
}

bool ObSessionFieldMgr::sys_variable_exists_local(const ObString &var)
{
  bool is_exist = false;
  ObSessionSysField *field = NULL;
  ObString var_name;
  const ObSysVarFieldBlock *block = sys_first_block_;
  if (OB_UNLIKELY(!is_inited_)) {
    is_exist = false;
  } else {
    for (; !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == var) {
            is_exist = true;
          }
        }
      }
    }
  }
  return is_exist;
}

bool ObSessionFieldMgr::is_hot_modified_variable(const ObString &var_name)
{
  int bret = false;
  for (int64_t i = 0; !bret && i < MAX_HOT_MODIFY_VARIABLES_NUM; ++i) {
    if (hot_var_names[i] == var_name) {
      bret = true;
    }
  }
  return bret;
}

bool ObSessionFieldMgr::is_cold_modified_variable(const common::ObString &var_name)
{
  if (is_hot_modified_variable(var_name)
          || is_last_insert_id_variable(var_name)
          || is_partition_hit_variable(var_name)) {
    return false;
  } else {
    return true;
  }
}

int ObSessionFieldMgr::remove_all_user_vars()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(get_all_user_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
  } else {
    int64_t count = names.count();
    for (int i = 0; i < count; i++) {
      remove_user_variable(names.at(i)); // ignore ret
    }
  }
  return ret;
}

int ObSessionFieldMgr::remove_all_mysql_sys_vars()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(get_all_mysql_sys_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
  } else {
    int64_t count = names.count();
    for (int i = 0; i < count; i++) {
      remove_user_variable(names.at(i)); // ignore ret
    }
  }
  return ret;
}

int ObSessionFieldMgr::remove_all_common_sys_vars()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(get_all_common_sys_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
  } else {
    int64_t count = names.count();
    for (int i = 0; i < count; i++) {
      remove_user_variable(names.at(i)); // ignore ret
    }
  }
  return ret;
}

int ObSessionFieldMgr::get_sys_variable(const common::ObString &name, ObSessionSysField *&value,
                        ObSysVarFieldBlock *&out_block) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObSysVarFieldBlock *block = sys_first_block_;
  ObSessionSysField *field = NULL;
  ObString var_name;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    for (; OB_SUCC(ret) && !is_exist && NULL != block; block = block->next_) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < block->free_idx_; ++i) {
        field = const_cast<ObSessionSysField *>(block->field_slots_ + i);
        if (OB_FIELD_USED == field->stat_) {
          var_name.assign(const_cast<char *>(field->name_), field->name_len_);
          if (var_name == name) {
            value = field;
            out_block = const_cast<ObSysVarFieldBlock*>(block);
            is_exist = true;
          }
        }
      }
    }
  }
  // set ret value to OB_ENTRY_NOT_EXIST if not exist
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {} // keep the ret value
  } else {} // keep the ret value
  return ret;
}

int ObSessionFieldMgr::remove_sys_variable(const common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObSessionSysField *field_ptr = NULL;
    ObSysVarFieldBlock *block = NULL;
    if (OB_ENTRY_NOT_EXIST == (ret = get_sys_variable(name, field_ptr, block))) {
      LOG_DEBUG("sys_variable is not exist", K(name), K(ret));
    } else if (OB_SUCCESS == ret && NULL != field_ptr) {
      field_heap_->free_obj(field_ptr->value_);
      field_heap_->free_string(field_ptr->name_, field_ptr->name_len_);
      field_ptr->reset();
      block->inc_removed_count();
    } else {
      LOG_WDIAG("fail to get_user_variable", K(name), K(ret));
    }
  }
  return ret;
}

// remove all sys except last_insert_id
int ObSessionFieldMgr::remove_all_sys_vars()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(get_changed_sys_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
  } else {
    int64_t count = names.count();
    for (int i = 0; i < count; i++) {
      if (!is_last_insert_id_variable(names.at(i))){
        remove_sys_variable(names.at(i));// ignore ret
      }
    }
  }
  return ret;
}

int ObSessionFieldMgr::replace_last_insert_id_var(ObSessionFieldMgr& field_manager, bool is_oceanbase)
{
  int ret = OB_SUCCESS;
  common::ObObj obj;
  ObString var_name = ObString::make_string(sql::OB_SV_LAST_INSERT_ID);
  ObSessionSysField *field = NULL;
  if (OB_FAIL(field_manager.get_sys_variable_value(var_name, obj))) {
    LOG_DEBUG("get value fail", K(ret));
    if (OB_INNER_STAT_ERROR == ret && !is_oceanbase) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(update_system_variable(var_name, obj, field))){
    LOG_WDIAG("fail to update_system_variable", K(obj), K(ret));
  } else {
    LOG_DEBUG("succ update_system_variable", K(var_name), K(obj), K(ret));
  }
  return ret;
}

int ObSessionFieldMgr::replace_all_sys_vars(ObSessionFieldMgr& field_manager, NeedFunc need_func)
{
  int ret = OB_SUCCESS;
  // remove_all_sys_vars();
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(field_manager.get_changed_sys_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
    return false;
  }
  // hot_var_name if not changed should be set as default
  // here maybe duplicate with changed, igonre it
  for (int64_t i = 0; i < MAX_HOT_MODIFY_VARIABLES_NUM; ++i) {
    names.push_back(hot_var_names[i]);
  }
  ObDefaultSysVarSet * default_sys_var_set = field_manager.get_sys_var_set();
  if (NULL != default_sys_var_set) {
    set_sys_var_set(default_sys_var_set);
  } else {
    LOG_DEBUG("default_sys_var_set is null");
  }
  int64_t count = names.count();
  common::ObObj obj;
  ObSessionSysField *field = NULL;
  for (int64_t i = 0; i < count; i++) {
    obj.reset();
    field = NULL;
    if (need_func != NULL && !need_func(names.at(i))) {
      // not need skip it
    } else if (OB_FAIL(field_manager.get_sys_variable_value(names.at(i), obj))) {
      LOG_WDIAG("fail to get sys obj", K(names.at(i)), K(i), K(ret));
    } else if (OB_SUCC(get_common_sys_variable(names.at(i), field))) {
      // if common have update common and sys
      if (OB_FAIL(update_common_sys_variable(obj, field))) {
        LOG_WDIAG("fail to update common sys", K(names.at(i)), K(ret));
      } else if (OB_FAIL(update_system_variable(names.at(i), obj, field))) {
        LOG_WDIAG("fail to update_system_variable", K(names.at(i)), K(i), K(ret));
      }
    } else if (OB_FAIL(update_system_variable(names.at(i), obj, field))) {
      LOG_WDIAG("fail to update_system_variable", K(names.at(i)), K(i), K(ret));
    } else {
      // LOG_DEBUG("succ replace", K(names.at(i)), K(i), K(obj));
    }
  }
  return ret;
}

int ObSessionFieldMgr::replace_all_user_vars(ObSessionFieldMgr& field_manager)
{
  int ret = OB_SUCCESS;
  // remove_all_user_vars();
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(field_manager.get_all_user_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
    return false;
  }
  int64_t count = names.count();
  common::ObObj obj;
  for (int64_t i = 0; i < count; ++i) {
    obj.reset();
    if (OB_FAIL(field_manager.get_user_variable_value(names.at(i), obj))) {
      LOG_WDIAG("fail to get user obj", K(names.at(i)), K(i), K(ret));
    } else if (OB_FAIL(replace_user_variable(names.at(i), obj))){
      LOG_WDIAG("fail to replace_user_variable", K(names.at(i)), K(i), K(ret));
    } else {
      // LOG_DEBUG("succ replace", K(names.at(i)), K(i));
    }
  }
  return ret;
}

int ObSessionFieldMgr::replace_all_common_sys_vars(ObSessionFieldMgr& field_manager, bool is_oceanbase /*true*/, NeedFunc need_func)
{
  int ret = OB_SUCCESS;
  // remove_all_common_sys_vars();
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(field_manager.get_all_common_sys_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
  }
  int64_t count = names.count();
  common::ObObj obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    obj.reset();
    if (need_func != NULL && !need_func(names.at(i))) {
      // not need skip
    } else if (OB_FAIL(field_manager.get_common_sys_variable_value(names.at(i), obj))) {
      LOG_WDIAG("fail to get common sys obj", K(names.at(i)), K(i), K(ret));
    } else if (OB_FAIL(replace_common_sys_variable(names.at(i), obj, is_oceanbase))){
      LOG_WDIAG("fail to replace_common_sys_variable", K(names.at(i)), K(i), K(ret));
    } else {
      // LOG_DEBUG("succ replace", K(names.at(i)), K(i), K(obj));
    }
  }
  return ret;
}

int ObSessionFieldMgr::replace_all_mysql_sys_vars(ObSessionFieldMgr& field_manager, NeedFunc need_func)
{
  int ret = OB_SUCCESS;
  // remove_all_mysql_sys_vars();
  common::ObSEArray<common::ObString, 32> names;
  if (OB_FAIL(field_manager.get_all_mysql_sys_var_names(names))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
  }
  ObSessionSysField *field = NULL;
  int64_t count = names.count();
  common::ObObj obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    obj.reset();
    // ignore error?
    if (need_func != NULL && need_func(names.at(i))) {
      // not need ,skip it
    } else if (OB_FAIL(field_manager.get_mysql_sys_variable_value(names.at(i), obj))) {
      LOG_WDIAG("fail to get mysql sys obj", K(names.at(i)), K(i), K(ret));
    } else if (OB_SUCC(get_common_sys_variable(names.at(i), field))) {
      // if common have update common and sys
      if (OB_FAIL(update_common_sys_variable(obj, field))) {
        LOG_WDIAG("fail to update common sys", K(names.at(i)), K(ret));
      } else if (OB_FAIL(update_mysql_system_variable(names.at(i), obj, field))) {
        LOG_WDIAG("fail to update_system_variable", K(names.at(i)), K(i), K(ret));
      }
    } else if (OB_FAIL(update_mysql_system_variable(names.at(i), obj, field))){
      LOG_WDIAG("fail to replace_mysql_sys_variable", K(names.at(i)), K(i), K(ret));
    } else {
      // LOG_DEBUG("succ replace", K(names.at(i)), K(i));
    }
  }
  return ret;
}

int ObSessionFieldMgr::replace_all_hot_sys_vars(ObSessionFieldMgr& field_manager)
{
  NeedFunc need_func = is_hot_modified_variable;
  return replace_all_sys_vars(field_manager,need_func);
}

int ObSessionFieldMgr::replace_all_cold_sys_vars(ObSessionFieldMgr& field_manager)
{
  NeedFunc need_func = is_cold_modified_variable;
  return replace_all_sys_vars(field_manager,need_func);
}

int ObSessionFieldMgr::replace_all_common_hot_sys_vars(ObSessionFieldMgr& field_manager, bool is_oceanbase)
{
  NeedFunc need_func = is_hot_modified_variable;
  return replace_all_common_sys_vars(field_manager, is_oceanbase, need_func);
}

int ObSessionFieldMgr::replace_all_common_cold_sys_vars(ObSessionFieldMgr& field_manager, bool is_oceanbase)
{
  NeedFunc need_func = is_cold_modified_variable;
  return replace_all_common_sys_vars(field_manager, is_oceanbase, need_func);
}

int ObSessionFieldMgr::replace_all_mysql_hot_sys_vars(ObSessionFieldMgr& field_manager)
{
  NeedFunc need_func = is_hot_modified_variable;
  return replace_all_mysql_sys_vars(field_manager, need_func);
}

int ObSessionFieldMgr::replace_all_mysql_cold_sys_vars(ObSessionFieldMgr& field_manager)
{
  NeedFunc need_func = is_cold_modified_variable;
  return replace_all_mysql_sys_vars(field_manager, need_func);
}

bool ObSessionFieldMgr::is_same_cold_sys_vars(const ObSessionFieldMgr& field_manager)
{
  return is_same_cold_sys_vars_common(field_manager, OB_SESSION_SYS_VAR);
}

bool ObSessionFieldMgr::is_same_mysql_cold_session_vars(const ObSessionFieldMgr& field_manager)
{
  return is_same_cold_sys_vars_common(field_manager, OB_SESSION_MYSQL_SYS_VAR);
}

bool ObSessionFieldMgr::is_same_mysql_hot_session_vars(const ObSessionFieldMgr& field_manager)
{
  return is_same_hot_sys_vars_common(field_manager, OB_SESSION_MYSQL_SYS_VAR);
}

bool ObSessionFieldMgr::is_same_common_cold_session_vars(const ObSessionFieldMgr& field_manager)
{
  return is_same_cold_sys_vars_common(field_manager, OB_SESSION_COMMON_SYS_VAR);
}

bool ObSessionFieldMgr::is_same_common_hot_session_vars(const ObSessionFieldMgr& field_manager)
{
  return is_same_hot_sys_vars_common(field_manager, OB_SESSION_COMMON_SYS_VAR);
}

bool ObSessionFieldMgr::is_same_hot_sys_vars(const ObSessionFieldMgr& field_manager)
{
  return is_same_hot_sys_vars_common(field_manager, OB_SESSION_SYS_VAR);
}

bool ObSessionFieldMgr::is_same_hot_sys_vars_common(const ObSessionFieldMgr& field_manager, ObSessionVarType var_type)
{
  common::ObSEArray<common::ObString, 8> names_hot_1;
  common::ObSEArray<common::ObString, 8> names_hot_2;
  common::ObSEArray<common::ObString, 32> names1;
  common::ObSEArray<common::ObString, 32> names2;
  const ObString* hot_var_array = NULL;
  int64_t hot_var_array_len = 0;
  int ret = OB_SUCCESS;
  bool bret = true;
  ObSessionFieldMgr* manager_ptr = const_cast<ObSessionFieldMgr*>(&field_manager);
  switch (var_type) {
    case OB_SESSION_SYS_VAR:
      // if OB comoare all hot vars
      hot_var_array = hot_var_names;
      hot_var_array_len = MAX_HOT_MODIFY_VARIABLES_NUM;
      for (int64_t i = 0; i < hot_var_array_len; ++i) {
        names_hot_1.push_back(hot_var_array[i]);
        names_hot_2.push_back(hot_var_array[i]);
      }
      break;
    case OB_SESSION_MYSQL_SYS_VAR:
      if (OB_FAIL(get_all_mysql_sys_var_names(names1))) {
        LOG_WDIAG("fail get all var name for source", K(ret));
        bret = false;
      } else if (OB_FAIL(manager_ptr->get_all_mysql_sys_var_names(names2))) {
        LOG_WDIAG("fail get all var name for dest", K(ret));
        bret = false;
      }
      break;
    case OB_SESSION_COMMON_SYS_VAR:
      if (OB_FAIL(get_all_common_sys_var_names(names1))) {
        LOG_WDIAG("fail get all var name for source", K(ret));
        bret = false;
      } else if (OB_FAIL(manager_ptr->get_all_common_sys_var_names(names2))) {
        LOG_WDIAG("fail get all var name for dest", K(ret));
        bret = false;
      }
      break;
    default:
      bret = false;
      LOG_WDIAG("invalid type", K(var_type));
  }
  if (bret) {
    for (int i = 0; i < names1.count(); i++) {
      if (is_hot_modified_variable(names1.at(i))) {
        names_hot_1.push_back(names1.at(i));
      }
    }
    for (int i = 0; i < names2.count(); i++) {
      if (is_hot_modified_variable(names2.at(i))) {
        names_hot_2.push_back(names2.at(i));
      }
    }
    bret = is_same_vars_in_array(names_hot_1, names_hot_2, field_manager, var_type);
  }
  LOG_DEBUG("is_same_hot_sys_vars_common", K(bret), K(var_type), K(names_hot_1.count()), K(names_hot_2.count()));
  return bret;
}

bool ObSessionFieldMgr::is_same_cold_sys_vars_common(const ObSessionFieldMgr& field_manager, ObSessionVarType var_type)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObString, 32> names1;
  common::ObSEArray<common::ObString, 32> names2;
  common::ObSEArray<common::ObString, 32> names_cold_1;
  common::ObSEArray<common::ObString, 32> names_cold_2;
  bool bret = true;
  ObSessionFieldMgr* manager_ptr = const_cast<ObSessionFieldMgr*>(&field_manager);
  switch(var_type) {
    case OB_SESSION_SYS_VAR:
      if (OB_FAIL(get_changed_sys_var_names(names1))) {
        LOG_WDIAG("fail get all var name for source", K(ret));
        bret = false;
      } else if (OB_FAIL(manager_ptr->get_changed_sys_var_names(names2))) {
        LOG_WDIAG("fail get all var name for dest", K(ret));
        bret = false;
      }
      break;
    case OB_SESSION_COMMON_SYS_VAR:
      if (OB_FAIL(get_all_common_sys_var_names(names1))) {
        LOG_WDIAG("fail get all var name for source", K(ret));
        bret = false;
      } else if (OB_FAIL(manager_ptr->get_all_common_sys_var_names(names2))) {
        LOG_WDIAG("fail get all var name for dest", K(ret));
        bret = false;
      }
      break;
    case OB_SESSION_MYSQL_SYS_VAR:
      if (OB_FAIL(get_all_mysql_sys_var_names(names1))) {
        LOG_WDIAG("fail get all var name for source", K(ret));
        bret = false;
      } else if (OB_FAIL(manager_ptr->get_all_mysql_sys_var_names(names2))) {
        LOG_WDIAG("fail get all var name for dest", K(ret));
        bret = false;
      }
      break;
    default:
      LOG_WDIAG("invalid type", K(var_type));
      bret = false;
  }
  if (bret) {
    int64_t count1 = names1.count();
    int64_t count2 = names2.count();
    for (int64_t i = 0; bret && i < count1; i++) {
      if (is_cold_modified_variable(names1.at(i))){
        names_cold_1.push_back(names1.at(i));
      }
    }
    for (int64_t i = 0; i < count2; i++) {
      if (is_cold_modified_variable(names2.at(i))) {
        names_cold_2.push_back(names2.at(i));
      }
    }
    int64_t count_cold1 = names_cold_1.count();
    int64_t count_cold2 = names_cold_2.count();
    bret = is_same_vars_in_array(names_cold_1, names_cold_2, field_manager, var_type);
    LOG_DEBUG("is_same_cold_sys_vars_common", K(bret), K(var_type), K(count1), K(count2), K(count_cold1),
      K(count_cold2), K(names_cold_1), K(names_cold_2));
  }
  return bret;
}

bool ObSessionFieldMgr::is_same_obj_val(const common::ObObj &obj_client, const common::ObObj &obj_server)
{
  bool bret = false;
  // as obj_is_equal may crash when different type
  // treat different type as not same
  if (obj_client.get_type() != obj_server.get_type()) {
    bret = false;
  } else if (obj_client.is_equal(obj_server, ObCharset::get_system_collation())) {
    bret = true;
  }
  return bret;
}

/*
 * 1: client is empty treat as same
 * 2: client not have treat as same, no matter server have or not
 * 3: client and server both have , if same value is same, else is false
 */
bool ObSessionFieldMgr::is_same_vars_in_array(common::ObIArray<common::ObString>& names1,
  const common::ObIArray<common::ObString>& names2,
  const ObSessionFieldMgr& field_manager,
  int type)
{
  int ret = OB_SUCCESS;
  int64_t count1 = names1.count();
  int64_t count2 = names2.count();
  if (count1 == 0) {
    // LOG_DEBUG("count 1 is 0, treat as same");
    return true;
  }
  if (count1 > count2) {
    LOG_DEBUG(" count not same", K(count1), K(count2), K(names1), K(names2));
    return false;
  }
  common::ObObj obj_client, obj_server;
  bool found1;
  bool found2;
  bool bret = true;
  ObString var_name;
  for (int i = 0; i < count1 && bret; i++) {
    obj_client.reset();
    obj_server.reset();
    found1 = true;
    found2 = true;
    var_name = names1.at(i);
    switch(type) {
    case OB_SESSION_USER_VAR:
      if (OB_FAIL(get_user_variable_value(var_name, obj_client))) {
        LOG_DEBUG("fail get obj_client", K(var_name), K(ret));
        found1 = false;
      }
      if (OB_FAIL(field_manager.get_user_variable_value(var_name, obj_server))) {
        LOG_DEBUG("fail get obj2", K(var_name), K(ret));
        found2 = false;
      }
      break;
    case OB_SESSION_SYS_VAR:
      if (OB_FAIL(get_sys_variable_value(var_name, obj_client))) {
        LOG_DEBUG("fail get obj_client", K(var_name), K(ret));
        found1 = false;
      }
      if (OB_FAIL(field_manager.get_sys_variable_value(var_name, obj_server))) {
        LOG_DEBUG("fail get obj_server", K(var_name), K(ret));
        found2 = false;
      }
      break;
    case OB_SESSION_COMMON_SYS_VAR:
      if (OB_FAIL(get_common_sys_variable_value(var_name, obj_client))) {
        LOG_DEBUG("fail get obj_client", K(var_name), K(ret));
        found1 = false;
      }
      if (OB_FAIL(field_manager.get_common_sys_variable_value(var_name, obj_server))) {
        LOG_DEBUG("fail get obj_server", K(var_name), K(ret));
        found2 = false;
      }
      break;
    case OB_SESSION_MYSQL_SYS_VAR:
      if (OB_FAIL(get_mysql_sys_variable_value(var_name, obj_client))) {
        LOG_DEBUG("fail get obj_client", K(var_name), K(ret));
        found1 = false;
      }
      if (OB_FAIL(field_manager.get_mysql_sys_variable_value(var_name, obj_server))) {
        LOG_DEBUG("fail get obj_server", K(var_name), K(ret));
        found2 = false;
      }
      break;
    default:
      LOG_WDIAG("invalid type", K(type));
      bret = false;
    }
    if (found1 == found2) {
      if (!found1) {
        // both not found, treat as same continue next one
        continue;
      } else if (!is_same_obj_val(obj_client, obj_server)) {
        bret = false;
      }
    } else if (found1) {
      // client have server not have, if is session_sys_var treat as server not support will skip
      if (type != OB_SESSION_SYS_VAR) {
        bret = false;
      }
    } else {
      // client not have, server is set by others, ignore
    }
    if (var_name.case_compare("autocommit") == 0) {
      LOG_DEBUG("autocommit is ", K(found1), K(found2), K(var_name), K(obj_client), K(obj_server), K(type));
    }
    if (!bret) {
      LOG_DEBUG("not same", K(found1), K(found2), K(var_name), K(obj_client), K(obj_server), K(type));
    }
  }
  return bret;
}

bool ObSessionFieldMgr::is_same_user_vars(const ObSessionFieldMgr& field_manager)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObString, 32> names1;
  common::ObSEArray<common::ObString, 32> names2;
  if (OB_FAIL(get_all_user_var_names(names1))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
    return false;
  }
  ObSessionFieldMgr* manager_ptr = const_cast<ObSessionFieldMgr*>(&field_manager);
  if (OB_FAIL(manager_ptr->get_all_user_var_names(names2))) {
    LOG_WDIAG("fail get all var name for source", K(ret));
    return false;
  }
  int64_t count1 = names1.count();
  int64_t count2 = names2.count();
  bool result =  is_same_vars_in_array(names1, names2, field_manager, OB_SESSION_USER_VAR);
  LOG_DEBUG("is_same_user_vars", K(result), K(count1), K(count2));
  return result;
}

bool ObSessionFieldMgr::is_same_last_insert_id_var(const ObSessionFieldMgr& field_manager)
{
  int ret = OB_SUCCESS;
  common::ObObj obj1, obj2;
  bool found1 = true;
  bool found2 = true;
  ObString var_name = ObString::make_string(sql::OB_SV_LAST_INSERT_ID);
  if (OB_FAIL(get_sys_variable_value(var_name, obj1))) {
    LOG_DEBUG("get value fail", K(ret));
    found1 = false;
  }
  if (OB_FAIL(field_manager.get_sys_variable_value(var_name, obj2))) {
    LOG_DEBUG("get value fail", K(ret));
    found1 = false;
  }
  bool result = false;
  if ((found1 == found2) && (obj1 == obj2)) {
    result = true;
  }
  LOG_DEBUG("is_same_last_insert_id_var", K(result), K(found1), K(found2), K(obj1), K(obj2));
  return result;
}

int ObDefaultSysVarSet::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFieldBaseMgr::init())) {
    LOG_WDIAG("fail to init base class", K(ret));
  } else {
    var_name_map_.init();
    is_inited_ = true;
  }
  return ret;
}

ObDefaultSysVarSet::ObDefaultSysVarSet()
    : is_inited_(false), last_modified_time_(0), var_name_map_(ObModIds::OB_PROXY_DEFAULT_SYS_VARIABLE)
{
}

int ObDefaultSysVarSet::get_sys_variable(const ObString &var, ObSessionSysField *&value, bool allow_var_not_found)
{
  int ret = OB_SUCCESS;
  ObSessionSysField *local_field = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("DefalutSysVarSet is not inited", K(var), K(ret));
  } else if (OB_FAIL(var_name_map_.get_refactored(var, local_field))) {
    ret = OB_ERR_SYS_VARIABLE_UNKNOWN;
    if (allow_var_not_found) {
      LOG_DEBUG("unknown sys variable", K(var), K(ret));
    } else {
      LOG_WDIAG("unknown sys variable", K(var), K(ret));
    }
  } else if (OB_ISNULL(local_field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("got field should not be NULL", K(local_field), K(ret));
  } else {
    value = local_field;
  }
  return ret;
}

int ObDefaultSysVarSet::sys_variable_exists(const ObString &var_name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else {
    is_exist = sys_variable_exists_local(var_name);
  }
  return ret;
}

bool ObDefaultSysVarSet::sys_variable_exists_local(const ObString &var_name)
{
  ObSessionSysField *field = NULL;
  return (OB_SUCCESS == var_name_map_.get_refactored(var_name, field));
}

int ObDefaultSysVarSet::load_default_system_variable()
{
  int ret = OB_SUCCESS;
  const int64_t both_scope = ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::READONLY;
  bool print_info_log = true;
  if (OB_FAIL(load_sysvar_int(ObString::make_string("autocommit"), 1, ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar autocommit", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_AUTO_INCREMENT_INCREMENT), 1, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar auto_increment_increment", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_AUTO_INCREMENT_OFFSET), 1, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar auto_increment_offset", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_LAST_INSERT_ID), 0, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar last_insert_id", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_INTERACTIVE_TIMEOUT), 0, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar interactive_timeout", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_QUERY_TIMEOUT), 10000000, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar ob_query_timeout", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_TRX_TIMEOUT), 100000000, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar ob_trx_timeout", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_NET_READ_TIMEOUT), 30, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar net_read_timeout", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_NET_WRITE_TIMEOUT), 60, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar net_write_timeout", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_WAIT_TIMEOUT), 28800, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar wait_timeout", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_LOWER_CASE_TABLE_NAMES), 2, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar lower_case_table_names", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_TX_READ_ONLY), 0, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar tx_read_only", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_READ_CONSISTENCY), 3, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar ob_read_consistency", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_COLLATION_CONNECTION), 45, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar collation_connection", K(ret));
  } else if (OB_FAIL(load_sysvar_int(ObString::make_string(OB_SV_ENABLE_TRANSMISSION_CHECKSUM), 1, both_scope, print_info_log))) {
    LOG_WDIAG("fail to load default sysvar ob_enable_transmission_checksum", K(ret));
  }
  return ret;
}

//load system variable when not exist
int ObDefaultSysVarSet::load_system_variable(const ObString &name, const ObObj &type,
                                             const ObObj &value, const int64_t flags)
{
  int ret = OB_SUCCESS;
  ObSessionSysField *new_field = NULL;
  ObSysVarFieldBlock *block_out = NULL;
  bool is_reused = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("variable name is empty", K(name), K(ret));
  } else if (OB_UNLIKELY(sys_variable_exists_local(name))) {
    ret = OB_ERR_ALREADY_EXISTS;
    LOG_WDIAG("variable already exists", K(name), K(ret));
  } else if (OB_FAIL(alloc_field(sys_first_block_, sys_block_list_tail_, block_out, new_field, is_reused))) {
    LOG_WDIAG("fail to alloc sys var field", K(ret));
  } else if (OB_ISNULL(new_field) || OB_ISNULL(block_out)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer", K(ret));
  } else {
    new_field->type_ = type.get_type();
    new_field->scope_ = flags;
    if (ObSessionFieldMgr::is_last_insert_id_variable(name)) {
      new_field->modify_mod_ = OB_FIELD_LAST_INSERT_ID_MODIFY_MOD;
    } else if (ObSessionFieldMgr::is_hot_modified_variable(name)) {
      new_field->modify_mod_ = OB_FIELD_HOT_MODIFY_MOD;
    } else {
      new_field->modify_mod_ = OB_FIELD_COLD_MODIFY_MOD;
    }
    uint16_t len = static_cast<uint16_t>(name.length());

    ObFieldObjCaster caster;
    const ObObj *res_cell = NULL;
    ObArenaAllocator calc_buf(ObModIds::OB_PROXY_DEFAULT_SYS_VARIABLE);
    if (OB_FAIL(caster.obj_cast(value, *new_field, res_cell))) {
      LOG_WDIAG("fail to cast obj", K(value), K(*new_field), K(ret));
    } else if (OB_ISNULL(res_cell)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("res_call is null, which is unexpected", K(ret));
    } else if (OB_FAIL(duplicate_field(name.ptr(), len, *res_cell, *new_field))) {
      LOG_WDIAG("fail to duplicate field", K(value), K(type), K(ret));
    } else {
      if (OB_FAIL(var_name_map_.set_refactored(name, new_field))) {
        LOG_WDIAG("fail to set var_name_map", K(name), K(type), K(value), K(flags), K(ret));
      } else {
        new_field->stat_ = OB_FIELD_USED;
        if (is_reused) {
          block_out->dec_removed_count();
        }
      }
    }
  }
  return ret;
}

int ObDefaultSysVarSet::load_system_variable(const ObString &name, const int64_t dtype,
                                             const ObString &value, const int64_t flags)
{
  ObObj ovalue;
  ovalue.set_varchar(value);
  ObObj otype;
  otype.set_type(static_cast<ObObjType>(dtype));
  return load_system_variable(name, otype, ovalue, flags);
}

int ObDefaultSysVarSet::load_system_variable_snapshot(proxy::ObMysqlResultHandler &result_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K(ret));
  } else {
    ObArenaAllocator allocator;
    char name_buf[OB_MAX_COLUMN_NAME_LENGTH + 1] = "";
    int64_t name_len = 0;
    char *value_buf = NULL;
    int64_t value_len = 0;
    int64_t vtype = 0;
    int64_t flag = 0;
    int64_t tmp_modified_time = 0;
    int64_t max_modified_time = 0;
    while ((OB_SUCC(ret)) && OB_SUCC(result_handler.next())) {
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "name", name_buf,
                                       static_cast<int32_t>(sizeof(name_buf)), name_len);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "data_type", vtype, int64_t);
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL_UNLIMIT_LENGTH(result_handler, "value", value_buf, value_len, allocator);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "modified_time", tmp_modified_time, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "flags", flag, int64_t);
      if (OB_SUCC(ret)) {
        ObString name(name_len, name_buf);
        ObString value(value_len, value_buf);
        if (OB_FAIL(load_system_variable(name, vtype, value, flag))) {
          LOG_EDIAG("load sys var failed", K(ret), K(name), K(vtype), K(value), K(flag));
        } else {
          max_modified_time = (max_modified_time < tmp_modified_time ? tmp_modified_time : max_modified_time);
          LOG_DEBUG("load sys var success", K(ret), K(name), K(vtype), K(value), K(flag), K(value_len),
                    "modified_time", tmp_modified_time);
        }
      }
      if (value_buf != NULL) {
        allocator.free(value_buf);
        value_buf = NULL;
      }
    }

    if (OB_ITER_END == ret) {
      // max(modify_time) is assigned to last_modified_time, which we treat as global variables version
      last_modified_time_ = max_modified_time;
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("get result failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("load all session system variables succ", K(last_modified_time_));
  }
  return ret;
}

int ObFieldObjCaster::obj_cast(const ObObj &value, const ObSessionSysField &field,
                               const ObObj *&res_cell)
{
  int ret = OB_SUCCESS;
  // if a session variable is nullable(only 'character_set_results' in this version)
  // and the value length is 0,
  // we treat this value as null value
  if (field.is_nullable() && 0 == value.get_val_len()) {
    casted_cell_.set_null();
    res_cell = &casted_cell_;
  } else {
    ObCastCtx cast_ctx(&allocator_, NULL, CM_NONE, ObCharset::get_system_collation());
    ret = ObObjCasterV2::to_type(field.type_, cast_ctx, value, casted_cell_, res_cell);
    // some sys var is 1 or ON
    if (OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == ret && value.is_varchar() && ObIntType == field.type_) {
      ObString str;
      value.get_varchar(str);
      if (0 == str.case_compare("ON") || 0 == str.case_compare("OFF")) {
        if (0 == str.case_compare("ON")) {
          casted_cell_.set_int(1);
        } else if (0 == str.case_compare("OFF")) {
          casted_cell_.set_int(0);
        }
        res_cell = &casted_cell_;
        ret = OB_SUCCESS;
      } else if (0 == str.case_compare("utf8") || 0 == str.case_compare("utf8mb4")) {
        casted_cell_.set_int(45);
        res_cell = &casted_cell_;
        ret = OB_SUCCESS;
      } else if (0 == str.case_compare("gbk")) {
        casted_cell_.set_int(28);
        res_cell = &casted_cell_;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}
}//end of namespace obproxy
}//end of namespace oceanbase
