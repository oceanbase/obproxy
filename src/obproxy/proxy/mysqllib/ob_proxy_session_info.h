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

#ifndef OBPROXY_SESSION_INFO_H
#define OBPROXY_SESSION_INFO_H

#include "lib/container/ob_vector.h"
#include "lib/net/ob_addr.h"
#include "lib/random/ob_mysql_random.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_cached_variables.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "proxy/route/ob_ldc_struct.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "proxy/mysql/ob_cursor_struct.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "rpc/obmysql/packet/ompk_ssl_request.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "obutils/ob_proxy_json_config_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObProxyConfigString;
}
class ObDefaultSysVarSet;
namespace proxy
{
enum ObProxyChecksumSwitch
{
  CHECKSUM_UNUSED = -1,
  CHECKSUM_OFF = 0,
  CHECKSUM_ON,
};

enum ObProxyProtocol
{
  PROTOCOL_NORMAL = 0,
  PROTOCOL_CHECKSUM,
  PROTOCOL_OB20,
};

class ObSessionVarVersion
{
public:
  ObSessionVarVersion() { reset(); }
  ~ObSessionVarVersion() { reset(); }
  void reset() { memset(this, 0, sizeof(ObSessionVarVersion)); }

  void inc_common_hot_sys_var_version() { common_hot_sys_var_version_++; }
  void inc_common_sys_var_version() { common_sys_var_version_++; }
  void inc_mysql_hot_sys_var_version() { mysql_hot_sys_var_version_++; }
  void inc_mysql_sys_var_version() { mysql_sys_var_version_++; }
  void inc_hot_sys_var_version() { hot_sys_var_version_++; }
  void inc_sys_var_version() { sys_var_version_++; }
  void inc_user_var_version() { user_var_version_++; }
  void inc_db_name_version() { db_name_version_++; }
  void inc_last_insert_id_version() { last_insert_id_version_++; }
  TO_STRING_KV(K_(common_hot_sys_var_version), K_(common_sys_var_version),
               K_(mysql_hot_sys_var_version), K_(mysql_sys_var_version),
               K_(hot_sys_var_version), K_(sys_var_version), K_(user_var_version),
               K_(db_name_version), K_(last_insert_id_version));
public:
  int64_t common_hot_sys_var_version_;
  int64_t common_sys_var_version_;
  int64_t mysql_hot_sys_var_version_;
  int64_t mysql_sys_var_version_;
  int64_t hot_sys_var_version_;
  int64_t sys_var_version_;
  int64_t user_var_version_;
  int64_t db_name_version_;
  int64_t last_insert_id_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSessionVarVersion);
};

class ObSessionVarValHash
{
public:
  ObSessionVarValHash() { reset(); }
  ~ObSessionVarValHash() { reset(); }
  void reset() { memset(this, 0, sizeof(ObSessionVarValHash)); }
  TO_STRING_KV(K_(common_hot_sys_var_hash), K_(common_cold_sys_var_hash),
               K_(mysql_hot_sys_var_hash), K_(mysql_cold_sys_var_hash),
               K_(hot_sys_var_hash), K_(cold_sys_var_hash), K_(user_var_hash));
public:
  uint64_t common_hot_sys_var_hash_;
  uint64_t common_cold_sys_var_hash_;
  uint64_t mysql_hot_sys_var_hash_;
  uint64_t mysql_cold_sys_var_hash_;
  uint64_t hot_sys_var_hash_;
  uint64_t cold_sys_var_hash_;
  uint64_t user_var_hash_;

  DISALLOW_COPY_AND_ASSIGN(ObSessionVarValHash);
};

class ObServerSessionInfo
{
public:
  ObServerSessionInfo();
  ~ObServerSessionInfo();

  // client_ps_id ----> ObPsIdPair
  struct ObPsIdPairHashing
  {
    typedef const uint32_t &Key;
    typedef ObPsIdPair Value;
    typedef ObDLList(ObPsIdPair, ps_id_pair_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
    static Key key(Value const *value) { return value->client_ps_id_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };

  typedef common::hash::ObBuildInHashMap<ObPsIdPairHashing> ObPsIdPairMap;

public:
  int init();
  void reset();

  int update_common_sys_variable(const ObString &var_name, const ObObj &value,
                                 const bool is_need_insert, const bool is_oceanbase);
  void set_ob_server(const common::ObAddr &ob_server) { ob_server_ = ob_server; }
  uint64_t get_ob_capability() const { return cap_; }
  void set_ob_capability(const uint64_t cap) { cap_ = cap; }
  const obmysql::ObMySQLCapabilityFlags get_compatible_capability_flags() const {
    // for compatible, OBServer 1479 handshake return SESSION_TRACK = 0, but still return session state info in ok packet
    if (is_oceanbase_server()) {
      obmysql::ObMySQLCapabilityFlags capability(compatible_capability_.capability_);
      capability.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
      return capability;
    } else {
      return compatible_capability_;
    }
  };
  void save_compatible_capability_flags(const obmysql::ObMySQLCapabilityFlags &capability) { compatible_capability_ = capability; };
  bool is_partition_table_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_PARTITION_TABLE); }
  bool is_change_user_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_CHANGE_USER); }
  bool is_checksum_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_CHECKSUM); }
  bool is_safe_read_weak_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_SAFE_WEAK_READ); }
  bool is_new_partition_hit_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_PRIORITY_HIT); }
  bool is_checksum_switch_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_CHECKSUM_SWITCH); }
  bool is_ob_protocol_v2_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_OB_PROTOCOL_V2); }
  bool is_extra_ok_packet_for_stats_enabled() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_EXTRA_OK_PACKET_FOR_STATISTICS); }
  bool is_pl_route_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_PL_ROUTE); }

  const common::ObAddr &get_ob_server() const { return ob_server_;}
  const ObSessionVarVersion &get_session_var_version() const { return version_; }

  int64_t get_common_hot_sys_var_version() const { return version_.common_hot_sys_var_version_; }
  int64_t get_common_sys_var_version() const { return version_.common_sys_var_version_; }
  int64_t get_mysql_hot_sys_var_version() const { return version_.mysql_hot_sys_var_version_; }
  int64_t get_mysql_sys_var_version() const { return version_.mysql_sys_var_version_; }
  int64_t get_hot_sys_var_version() const { return version_.hot_sys_var_version_; }
  int64_t get_sys_var_version() const { return version_.sys_var_version_; }
  int64_t get_user_var_version() const { return version_.user_var_version_; }
  int64_t get_db_name_version() const { return version_.db_name_version_; }
  int64_t get_last_insert_id_version() const { return version_.last_insert_id_version_; }

  void set_common_hot_sys_var_version(const int64_t version) { version_.common_hot_sys_var_version_ = version; }
  void set_common_sys_var_version(const int64_t version) { version_.common_sys_var_version_ = version; }
  void set_mysql_hot_sys_var_version(const int64_t version) { version_.mysql_hot_sys_var_version_ = version; }
  void set_mysql_sys_var_version(const int64_t version) { version_.mysql_sys_var_version_ = version; }
  void set_hot_sys_var_version(const int64_t version) { version_.hot_sys_var_version_ = version; }
  void set_sys_var_version(const int64_t version) { version_.sys_var_version_ = version; }
  void set_user_var_version(const int64_t version)  { version_.user_var_version_ = version; }
  void set_db_name_version(const int64_t version) { version_.db_name_version_ = version; }
  void set_last_insert_id_version(const int64_t version) { version_.last_insert_id_version_ = version; }

  ObProxyChecksumSwitch get_checksum_switch() const { return checksum_switch_; }
  void set_checksum_switch(const ObProxyChecksumSwitch checksum_switch) { checksum_switch_ = checksum_switch; }
  bool is_checksum_on() const { return CHECKSUM_ON == checksum_switch_;}
  void set_checksum_switch(const bool switch_on) { checksum_switch_ = (switch_on ? CHECKSUM_ON : CHECKSUM_OFF); }

  ObPsIdPair *get_ps_id_pair(uint32_t client_ps_id) const;
  uint32_t get_server_ps_id() const { return ps_id_; }
  void reset_server_ps_id() { ps_id_ = 0; }
  void set_server_ps_id(uint32_t ps_id) { ps_id_ = ps_id; }
  uint32_t get_server_ps_id(uint32_t client_ps_id);
  int add_ps_id_pair(ObPsIdPair *ps_id_pair)
  {
    set_server_ps_id(ps_id_pair->server_ps_id_);
    return ps_id_pair_map_.unique_set(ps_id_pair);
  }
  bool is_ps_id_pair_exist(uint32_t client_ps_id);
  void remove_ps_id_pair(uint32_t client_ps_id)
  {
    ObPsIdPair *ps_id_pair = ps_id_pair_map_.remove(client_ps_id);
    if (NULL != ps_id_pair) {
      if (ps_id_pair->server_ps_id_ == ps_id_) {
        reset_server_ps_id();
      }
      ps_id_pair->destroy();
      ps_id_pair = NULL;
    }
  }
  void destroy_ps_id_pair_map();

  bool is_server_text_ps_name_exist(const uint32_t text_ps_name_id);

  int get_server_cursor_id(uint32_t client_cursor_id, uint32_t &server_cursor_id);
  int add_cursor_id_pair(ObCursorIdPair *cursor_id_pair)
  {
    return cursor_id_pair_map_.unique_set(cursor_id_pair);
  }
  ObCursorIdPair *get_curosr_id_pair(uint32_t client_cursor_id) const;
  void remove_cursor_id_pair(uint32_t client_cursor_id)
  {
    ObCursorIdPair *cursor_id_pair = cursor_id_pair_map_.remove(client_cursor_id);
    if (NULL != cursor_id_pair) {
      cursor_id_pair->destroy();
      cursor_id_pair = NULL;
    }
  }
  void destroy_cursor_id_pair_map();
  int add_text_ps_name(const uint32_t text_ps_name_id);

  int get_database_name(ObString &database_name) const;
  int set_database_name(const common::ObString &database_name, const bool is_string_to_lower_case);
  int remove_database_name() { return field_mgr_.remove_database_name(); }
  ObString get_database_name() const;
  void set_server_type(DBServerType server_type) { server_type_ = server_type; }
  DBServerType get_server_type() const { return server_type_; }
  bool is_oceanbase_server() const { return DB_OB_MYSQL == server_type_ || DB_OB_ORACLE == server_type_; }
  TO_STRING_KV(K_(cap), K_(ob_server), K_(version), K_(val_hash), K_(checksum_switch), K_(server_type));

  dbconfig::ObShardConnector *get_shard_connector() { return shard_conn_; }
  void set_shard_connector(dbconfig::ObShardConnector *shard_conn) {
    if (NULL != shard_conn_) {
      shard_conn_->dec_ref();
      shard_conn_ = NULL;
    }

    if (NULL != shard_conn) {
      shard_conn->inc_ref();
    }
    shard_conn_ = shard_conn;
  }
public:
  ObSessionFieldMgr field_mgr_;
  ObSessionVarValHash val_hash_;
private:
  uint64_t cap_;
  obmysql::ObMySQLCapabilityFlags compatible_capability_;

  common::ObAddr ob_server_;
  ObSessionVarVersion version_;
  ObProxyChecksumSwitch checksum_switch_;
  bool is_inited_;
  common::DBServerType server_type_;
  dbconfig::ObShardConnector *shard_conn_;

  uint32_t ps_id_;
  ObPsIdPairMap ps_id_pair_map_;

  ObCursorIdPairMap cursor_id_pair_map_;
  common::ObArenaAllocator allocator_;
  common::hash::ObHashSet<uint32_t> text_ps_name_set_;

  DISALLOW_COPY_AND_ASSIGN(ObServerSessionInfo);
};

inline bool ObServerSessionInfo::is_ps_id_pair_exist(uint32_t client_ps_id)
{
  int ret = OB_SUCCESS;
  bool bret = false;
  ObPsIdPair *ps_id_pair = NULL;
  if (OB_SUCC(ps_id_pair_map_.get_refactored(client_ps_id, ps_id_pair)) && NULL != ps_id_pair) {
    bret = true;
  }
  return bret;
}

inline uint32_t ObServerSessionInfo::get_server_ps_id(uint32_t client_ps_id)
{
  int ret = OB_SUCCESS;
  uint32_t server_ps_id = 0;
  ObPsIdPair *ps_id_pair = NULL;
  if (OB_SUCC(ps_id_pair_map_.get_refactored(client_ps_id, ps_id_pair)) && NULL != ps_id_pair) {
    server_ps_id = ps_id_pair->server_ps_id_;
    ps_id_ = server_ps_id;
  }
  return server_ps_id;
}

bool ObServerSessionInfo::is_server_text_ps_name_exist(const uint32_t text_ps_name_id)
{
  bool bret = false;
  if (OB_HASH_EXIST == text_ps_name_set_.exist_refactored(text_ps_name_id)) {
    bret = true;
  }

  return bret;
}

inline ObPsIdPair *ObServerSessionInfo::get_ps_id_pair(uint32_t client_ps_id) const
{
  ObPsIdPair *ps_id_pair = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ps_id_pair_map_.get_refactored(client_ps_id, ps_id_pair))) {
    if (OB_HASH_NOT_EXIST != ret) {
      _PROXY_LOG(WARN, "fail to get ps_id_pair with client ps id, ret=%d, client_ps_id=%d", ret, client_ps_id);
    }
  } else if (OB_ISNULL(ps_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    _PROXY_LOG(WARN, "ps_id_pair is null, ret=%d, client_ps_id=%d", ret, client_ps_id);
  }
  return ps_id_pair;
}

inline int ObServerSessionInfo::get_server_cursor_id(uint32_t client_cursor_id, uint32_t &server_cursor_id)
{
  int ret = OB_SUCCESS;

  server_cursor_id = 0;
  ObCursorIdPair *cursor_id_pair = NULL;
  if (OB_FAIL(cursor_id_pair_map_.get_refactored(client_cursor_id, cursor_id_pair))) {
    PROXY_LOG(WARN, "fail to get cursor id pair", K(client_cursor_id), K(ret));
  } else if (OB_UNLIKELY(NULL == cursor_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WARN, "unexpected cursor_id_pair is null", K(client_cursor_id), K(ret));
  } else {
    server_cursor_id = cursor_id_pair->server_cursor_id_;
  }

  return ret;
}

inline ObCursorIdPair *ObServerSessionInfo::get_curosr_id_pair(uint32_t client_cursor_id) const
{
  ObCursorIdPair *cursor_id_pair = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(cursor_id_pair_map_.get_refactored(client_cursor_id, cursor_id_pair))) {
    if (OB_HASH_NOT_EXIST != ret) {
      _PROXY_LOG(WARN, "fail to get cursor_id_pair with client cursor id, ret=%d, client_cursor_id=%d", ret, client_cursor_id);
    }
  } else if (OB_ISNULL(cursor_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    _PROXY_LOG(WARN, "cursor_id_pair is null, ret=%d, client_cursor_id=%d", ret, client_cursor_id);
  }
  return cursor_id_pair;
}

class ObSysVarSetProcessor;

class ObClientSessionInfo
{
public:
  ObClientSessionInfo();
  ~ObClientSessionInfo();

  // ps_id ----> ObPsIdEntry
  struct ObPsIdEntryHashing
  {
    typedef const uint32_t &Key;
    typedef ObPsIdEntry Value;
    typedef ObDLList(ObPsIdEntry, ps_id_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
    static Key key(Value const *value) { return value->ps_id_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };

  typedef common::hash::ObBuildInHashMap<ObPsIdEntryHashing> ObPsIdEntryMap;

  // text_ps_name ----> ObTextPsNameEntry
  struct ObTextPsNameEntryHashing
  {
    typedef const ObString Key;
    typedef ObTextPsNameEntry Value;
    typedef ObDLList(ObTextPsNameEntry, text_ps_name_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value const *value) { return value->text_ps_name_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };

  typedef common::hash::ObBuildInHashMap<ObTextPsNameEntryHashing> ObTextPsNameEntryMap;

public:
  int init();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t get_cluster_id() const { return cluster_id_; }
  const common::ObString &get_meta_cluster_name() const { return real_meta_cluster_name_; }
  void set_cluster_id(const int64_t cluster_id) { cluster_id_ = cluster_id; }
  int set_cluster_info(const bool enable_cluster_checkout,
                       const common::ObString &cluster_name,
                       const obutils::ObProxyConfigString &real_meta_cluster_name,
                       const int64_t cluster_id,
                       bool &need_delete_cluster);
  void free_real_meta_cluster_name();

  uint64_t get_ob_capability() const { return cap_; }
  void set_ob_capability(const uint64_t cap) { cap_ = cap; }
  bool is_partition_table_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_PARTITION_TABLE); }
  bool is_change_user_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_CHANGE_USER); }
  bool is_checksum_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_CHECKSUM); }
  bool is_read_weak_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_READ_WEAK); }
  bool is_safe_read_weak_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_SAFE_WEAK_READ); }
  bool is_ob_protocol_v2_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_OB_PROTOCOL_V2); }
  bool is_pl_route_supported() const { return OB_TEST_CAPABILITY(cap_, OB_CAP_PL_ROUTE); }
  bool is_oracle_mode() const {
    bool is_oracle_mode = false;
    if (is_sharding_user()) {
      if (OB_NOT_NULL(shard_conn_)) {
        is_oracle_mode = DB_OB_ORACLE == shard_conn_->server_type_;
      }
    } else {
      is_oracle_mode = is_oracle_mode_;
    }

    return is_oracle_mode;
  }
  void set_oracle_mode(const bool is_oracle_mode) {
    is_oracle_mode_ = is_oracle_mode;
    if (is_oracle_mode_) {
      server_type_ = DB_OB_ORACLE;
    }
  }

  //get and set methords
  const ObSessionVarVersion &get_session_version() { return version_; }
  int64_t get_common_hot_sys_var_version() const { return version_.common_hot_sys_var_version_; }
  int64_t get_common_sys_var_version() const { return version_.common_sys_var_version_; }
  int64_t get_mysql_hot_sys_var_version() const { return version_.mysql_hot_sys_var_version_; }
  int64_t get_mysql_sys_var_version() const { return version_.mysql_sys_var_version_; }
  int64_t get_hot_sys_var_version() const { return version_.hot_sys_var_version_; }
  int64_t get_sys_var_version() const { return version_.sys_var_version_; }
  int64_t get_user_var_version() const { return version_.user_var_version_; }
  int64_t get_db_name_version() const { return version_.db_name_version_; }
  int64_t get_last_insert_id_version() const { return version_.last_insert_id_version_; }

  void set_db_name_version(const int64_t version) { version_.db_name_version_ = version; }

  //set and get methord
  int set_cluster_name(const common::ObString &cluster_name);
  int set_tenant_name(const common::ObString &tenant_name);
  int set_database_name(const common::ObString &database_name, const bool inc_db_version = true);
  int set_user_name(const common::ObString &user_name);
  int set_ldg_logical_cluster_name(const common::ObString &cluster_name);
  int set_ldg_logical_tenant_name(const common::ObString &tenant_name);
  int set_logic_tenant_name(const common::ObString &logic_tenant_name);
  int set_logic_database_name(const common::ObString &logic_database_name);
  int get_cluster_name(common::ObString &cluster_name) const;
  int get_tenant_name(common::ObString &tenant_name) const;
  int get_database_name(common::ObString &database_name) const;
  common::ObString get_database_name() const;
  common::ObString get_full_username();
  int get_user_name(common::ObString &user_name) const;
  int get_ldg_logical_cluster_name(common::ObString &cluster_name) const;
  int get_ldg_logical_tenant_name(common::ObString &tenant_name) const;
  int get_logic_tenant_name(common::ObString &logic_tenant_name) const;
  int get_logic_database_name(common::ObString &logic_database_name) const;

  int remove_database_name() { return field_mgr_.remove_database_name(); }

  // judge whether need reset session variables
  bool need_reset_database(const ObServerSessionInfo &server_info) const;
  bool need_reset_last_insert_id(const ObServerSessionInfo &server_info) const;
  bool need_reset_common_hot_session_vars(const ObServerSessionInfo &server_info) const;
  bool need_reset_common_cold_session_vars(const ObServerSessionInfo &server_info) const;
  bool need_reset_mysql_hot_session_vars(const ObServerSessionInfo &server_info) const;
  bool need_reset_mysql_cold_session_vars(const ObServerSessionInfo &server_info) const;
  bool need_reset_hot_session_vars(const ObServerSessionInfo &server_info) const;
  bool need_reset_cold_session_vars(const ObServerSessionInfo &server_info) const;
  bool need_reset_safe_read_snapshot(const ObServerSessionInfo &server_info) const;
  bool need_reset_user_session_vars(const ObServerSessionInfo &server_info) const;
  // not include last_insert_id system variable
  bool need_reset_session_vars(const ObServerSessionInfo &server_info) const;
  // include all
  bool need_reset_all_session_vars() const { return is_global_vars_changed_; }
  bool is_user_idc_name_set() const { return is_user_idc_name_set_; }

  //sys variables related methords
  int update_cached_variable(const common::ObString &var_name, ObSessionSysField *field);
  int update_common_sys_variable(const common::ObString &var_name, const common::ObObj &value,
                                 const bool is_need_insert = true, const bool is_oceanbase = true);
  int update_common_sys_variable(const common::ObString &var_name, const common::ObString &value,
                                 const bool is_need_insert = true, const bool is_oceanbase = true);
  int update_mysql_sys_variable(const common::ObString &var_name, const common::ObObj &value);
  int update_mysql_sys_variable(const common::ObString &var_name, const common::ObString &value);
  int update_sys_variable(const common::ObString &name, const common::ObObj &value);
  int update_sys_variable(const common::ObString &name, const common::ObString &value);
  int get_sys_variable(const common::ObString &name, ObSessionSysField *&value) const;
  int get_sys_variable_value(const common::ObString &name, common::ObObj &value) const;
  int get_sys_variable_value(const char *name, common::ObObj &value) const;
  int sys_variable_exists(const common::ObString &name, bool &is_exist);
  int is_equal_with_snapshot(const common::ObString &name,
                             const common::ObString &value, bool &is_equal);
  int get_changed_sys_var_names(common::ObIArray<common::ObString> &names);
  int get_all_sys_var_names(common::ObIArray<common::ObString> &names);

  // @synopsis get variable type by name
  int get_sys_variable_type(const common::ObString &var_name, common::ObObjType &type);

  //user variables related methords
  int replace_user_variable(const common::ObString &name, const common::ObObj &val);
  int replace_user_variable(const common::ObString &name, const common::ObString &val);
  int remove_user_variable(const common::ObString &name);
  int get_user_variable(const common::ObString &name, ObSessionUserField *&value) const;
  int get_user_variable_value(const common::ObString &name, common::ObObj &value) const;
  int user_variable_exists(const common::ObString &name, bool &is_exist);
  int get_all_user_var_names(common::ObIArray<common::ObString> &names);

  int get_changed_sys_vars(common::ObIArray<ObSessionSysField> &fileds);
  int get_all_sys_vars(common::ObIArray<ObSessionSysField> &fileds);
  int get_all_user_vars(common::ObIArray<ObSessionBaseField> &fileds);

  int extract_all_variable_reset_sql(common::ObSqlString &sql);
  int extract_variable_reset_sql(ObServerSessionInfo &server_info, common::ObSqlString &sql);
  int extract_oceanbase_variable_reset_sql(ObServerSessionInfo &server_info,
                                           common::ObSqlString &sql, bool &need_reset);
  int extract_mysql_variable_reset_sql(ObServerSessionInfo &server_info,
                                       common::ObSqlString &sql, bool &need_reset);
  int extract_changed_schema(ObServerSessionInfo &server_info, common::ObString &db_name);
  int extract_last_insert_id_reset_sql(ObServerSessionInfo &server_info, common::ObSqlString &sql);

  // return the raw timeout value in session
  int get_session_timeout(const char *timeout_name, int64_t &timeout) const;

  proxy::ObMysqlAuthRequest &get_login_req() { return login_req_; }
  obmysql::OMPKSSLRequest &get_ssl_req() { return ssl_req_; }
  ObProxySessionPrivInfo &get_priv_info() { return priv_info_; }
  const ObProxySessionPrivInfo &get_priv_info() const { return priv_info_; }

  dbconfig::ObShardUserPrivInfo &get_shard_user_priv() { return up_info_; }

  // get capability saved by proxy
  // now we get it from first_login_req_ as in some case the saved_login_req_ is not valid
  const obmysql::ObMySQLCapabilityFlags &get_orig_capability_flags() const { return orig_capability_; };
  void save_orig_capability_flags(const obmysql::ObMySQLCapabilityFlags &capability) { orig_capability_ = capability; };

  // safe weak read sanpshot (the value is set when we receive by ok packet)
  int64_t get_safe_read_snapshot() { return safe_read_snapshot_; }
  void set_safe_read_snapshot(int64_t snapshot) { safe_read_snapshot_ = snapshot; }

  // safe_read_snapshot need sync (the value is set before we sync system variables)
  int64_t get_syncing_safe_read_snapshot() { return syncing_safe_read_snapshot_; }
  void set_syncing_safe_read_snapshot(int64_t snapshot) { syncing_safe_read_snapshot_ = snapshot; }

  // route policy
  int64_t get_route_policy() const { return route_policy_; }
  void set_route_policy(int64_t route_policy) { route_policy_ = route_policy; }

  //proxy route policy
  ObProxyRoutePolicyEnum get_proxy_route_policy() const { return proxy_route_policy_; }
  void set_proxy_route_policy(ObProxyRoutePolicyEnum policy) { proxy_route_policy_ = policy; }

  // the initial value of this flag is false,
  // when we specifies next transaction characteristic(set transaction xxx), this flag will be set;
  // we will clear this flag when the next comming transaction commit successfully;
  void set_trans_specified_flag() { is_trans_specified_ = true; }
  void clear_trans_specified_flag() { is_trans_specified_ = false; }
  bool is_trans_specified() const { return is_trans_specified_; }

  void set_user_identity(const ObProxyLoginUserType identity) { user_identity_ = identity; }
  ObProxyLoginUserType get_user_identity() const { return user_identity_; }
  bool enable_analyze_internal_cmd() const;
  bool is_sharding_user() const { return USER_TYPE_SHARDING == user_identity_; }
  bool is_metadb_user() const { return USER_TYPE_METADB == user_identity_; }
  bool is_proxysys_user() const { return USER_TYPE_PROXYSYS == user_identity_; }
  bool is_rootsys_user() const { return USER_TYPE_ROOTSYS == user_identity_; }
  bool is_proxyro_user() const { return USER_TYPE_PROXYRO == user_identity_; }
  bool is_inspector_user() const { return USER_TYPE_INSPECTOR == user_identity_; }
  bool is_proxysys_tenant() const { return (is_proxysys_user() || is_inspector_user()); }

  int create_scramble(common::ObMysqlRandom &random);
  common::ObString &get_scramble_string() { return scramble_string_; }
  common::ObString &get_idc_name() { return idc_name_; }
  common::ObString get_idc_name() const { return idc_name_; }
  void set_idc_name(const ObString &name);
  common::ObString &get_client_host() { return client_host_; }
  void set_client_host(const common::ObAddr &host);
  common::ObString &get_origin_username() { return origin_username_; }
  int set_origin_username(const common::ObString &username);

  void set_enable_reset_db(bool enable) { enable_reset_db_ = enable; }

  void set_user_priv_set(const int64_t user_priv_set) { priv_info_.user_priv_set_ = user_priv_set; }

  // the global_vars_version, set when we receive the ok packet of first login packet
  void set_global_vars_version(int64_t version) { global_vars_version_ = version; }
  int64_t get_global_vars_version() const { return global_vars_version_; }
  bool is_first_login_succ() const { return common::OB_INVALID_VERSION != global_vars_version_; }

  void set_obproxy_route_addr(int64_t ip_port) { obproxy_route_addr_ = ip_port; }
  int64_t get_obproxy_route_addr() const { return obproxy_route_addr_; }

  // mark the global vars has changed
  bool is_global_vars_changed() const { return is_global_vars_changed_; }
  void set_global_vars_changed_flag() { is_global_vars_changed_ = true; }


  // mark the ob_route_policy_set has set
  bool is_read_consistency_set() const { return is_read_consistency_set_; }
  void set_read_consistency_set_flag(const bool flag) { is_read_consistency_set_ = flag; }

  bool enable_shard_authority() const { return enable_shard_authority_; }
  void set_enable_shard_authority() { enable_shard_authority_ = true; }

  // deep copy, this function will alloc buf
  int set_start_trans_sql(const common::ObString &sql);
  // reset start trans sql, free buf
  void reset_start_trans_sql();
  // get start trans sql
  common::ObString &get_start_trans_sql() { return saved_start_trans_sql_; }

  obproxy::ObDefaultSysVarSet *get_sys_var_set() { return field_mgr_.get_sys_var_set(); }
  void set_sysvar_set_processor(ObSysVarSetProcessor &var_set_processor) { var_set_processor_ = &var_set_processor; }
  int revalidate_sys_var_set(ObSysVarSetProcessor &var_set_processor);
  int add_sys_var_set(obproxy::ObDefaultSysVarSet &set);

  // get cached variables
  obutils::ObCachedVariables &get_cached_variables() { return cached_variables_; }
  int64_t get_query_timeout() const { return cached_variables_.get_query_timeout(); }
  int64_t get_trx_timeout() const { return cached_variables_.get_trx_timeout(); }
  int64_t get_wait_timeout() const { return cached_variables_.get_wait_timeout(); }
  int64_t get_net_read_timeout() const { return cached_variables_.get_net_read_timeout(); }
  int64_t get_net_write_timeout() const{ return cached_variables_.get_net_write_timeout(); }
  bool need_use_lower_case_names() const { return cached_variables_.need_use_lower_case_names(); }
  int64_t get_read_consistency() const { return cached_variables_.get_read_consistency(); }

  ObConsistencyLevel get_consistency_level_prop() const {return consistency_level_prop_;}
  void set_consistency_level_prop(ObConsistencyLevel level) {consistency_level_prop_ = level;}

  // get memory size(stat field_mgr_ only, add more later)
  int64_t get_memory_size() const { return field_mgr_.get_memory_size(); }
  void destroy();

  dbconfig::ObShardConnector *get_shard_connector() { return shard_conn_; }
  void set_shard_connector(dbconfig::ObShardConnector *shard_conn) {
    if (NULL != shard_conn_) {
      shard_conn_->dec_ref();
      shard_conn_ = NULL;
    }

    if (NULL != shard_conn) {
      shard_conn->inc_ref();
      set_server_type(shard_conn->server_type_);
    }
    shard_conn_ = shard_conn;
  }

  DBServerType get_server_type() const { return server_type_; }
  void set_server_type(DBServerType server_type) { server_type_ = server_type; }

  bool is_oceanbase_server() const { return DB_OB_MYSQL == server_type_ || DB_OB_ORACLE == server_type_; }
  void set_allow_use_last_session(const bool is_allow_use_last_session) { is_allow_use_last_session_ = is_allow_use_last_session; }
  bool is_allow_use_last_session() { return is_allow_use_last_session_; }

  void set_group_id(const int64_t group_id) { group_id_ = group_id; }
  int64_t get_group_id() { return group_id_; }

  void set_need_sync_session_vars(bool need_sync) { need_sync_session_vars_ = need_sync; }
  bool is_sys_hot_version_changed() const {
    return hash_version_.hot_sys_var_version_ != version_.hot_sys_var_version_;
  }
  bool is_sys_cold_version_changed() const {
    return hash_version_.sys_var_version_ != version_.sys_var_version_;
  }
  bool is_common_hot_sys_version_changed() const {
    return hash_version_.common_hot_sys_var_version_ != version_.common_hot_sys_var_version_;
  }
  bool is_common_cold_sys_version_changed() const {
    return hash_version_.common_sys_var_version_ != version_.common_sys_var_version_;
  }
  bool is_mysql_hot_sys_version_changed() const {
    return hash_version_.mysql_hot_sys_var_version_ != version_.mysql_hot_sys_var_version_;
  }
  bool is_mysql_cold_sys_version_changed() const {
    return hash_version_.mysql_sys_var_version_ != version_.mysql_sys_var_version_;
  }
  bool is_user_var_version_changed() const {
    return hash_version_.user_var_version_ != version_.user_var_version_;
  }

  // prepare statement
  void reset_recv_client_ps_id() { recv_client_ps_id_ = 0; }
  uint32_t get_recv_client_ps_id() const { return recv_client_ps_id_; }
  void set_recv_client_ps_id(uint32_t id) { recv_client_ps_id_ = id; }
  uint32_t get_client_ps_id() { return ps_id_; }
  void set_client_ps_id(uint32_t ps_id) { ps_id_ = ps_id; }
  void reset_client_ps_id() { ps_id_ = 0; }
  ObPsEntry *get_ps_entry() { return ps_entry_; }
  ObPsEntry *get_ps_entry(uint32_t ps_id);
  void reset_ps_entry() { ps_entry_ = NULL; }
  int add_ps_id_entry(ObPsIdEntry *ps_id_entry) {
      set_ps_entry(ps_id_entry->ps_entry_);
      set_client_ps_id(ps_id_entry->ps_id_);
      return ps_id_entry_map_.unique_set(ps_id_entry);
  }
  void remove_ps_id_entry(uint32_t client_ps_id) {
    ObPsIdEntry *ps_id_entry = ps_id_entry_map_.remove(client_ps_id);
    if (NULL != ps_id_entry) {
      ps_id_entry->destroy();
      ps_id_entry = NULL;
    }
  }
  void destroy_ps_id_entry_map();
  void set_ps_entry(ObPsEntry *entry) { ps_entry_ = entry; }
  int get_ps_sql(common::ObString &ps_sql);
  bool need_do_prepare(ObServerSessionInfo &server_info) const;

  int add_text_ps_name_entry(ObTextPsNameEntry *text_ps_name_entry);
  int delete_text_ps_name_entry(ObTextPsNameEntry *text_ps_name_entry);

  void set_text_ps_entry(ObTextPsEntry *entry) { text_ps_entry_ = entry; }
  void set_client_text_ps_name(const common::ObString &text_ps_name)
  {
    MEMCPY(text_ps_name_buf_, text_ps_name.ptr(), text_ps_name.length());
    text_ps_name_.assign(text_ps_name_buf_, text_ps_name.length());
  }
  ObTextPsEntry *get_text_ps_entry() { return text_ps_entry_; }
  ObTextPsEntry *get_text_ps_entry(const common::ObString &text_ps_name);
  int get_text_ps_sql(common::ObString &sql);
  const common::ObString& get_text_ps_name() const { return text_ps_name_; }
  bool need_do_text_ps_prepare(ObServerSessionInfo &server_info) const;

  ObTextPsNameEntry *get_text_ps_name_entry(const common::ObString &text_ps_name) const;
  void reset_text_ps_state() { is_text_ps_execute_ = false; }
  void set_text_ps_execute(const bool is_execute) { is_text_ps_execute_ = is_execute; }
  bool is_text_ps_execute() const { return is_text_ps_execute_; }

  uint32_t get_client_cursor_id() { return cursor_id_; }
  void set_client_cursor_id(uint32_t cursor_id) { cursor_id_ = cursor_id; }
  void reset_client_cursor_id() { cursor_id_ = 0; }
  int add_cursor_id_addr(ObCursorIdAddr *cursor_id_addr) {
    set_client_cursor_id(cursor_id_addr->cursor_id_);
    return cursor_id_addr_map_.unique_set(cursor_id_addr);
  }
  int get_cursor_id_addr(ObCursorIdAddr *&addr) {
    return cursor_id_addr_map_.get_refactored(cursor_id_, addr);
  }
  ObCursorIdAddr *get_cursor_id_addr(uint32_t client_cursor_id) const;
  void remove_cursor_id_addr(uint32_t client_cursor_id) {
    ObCursorIdAddr *cursor_id_addr = cursor_id_addr_map_.remove(client_cursor_id);
    if (NULL != cursor_id_addr) {
      cursor_id_addr->destroy();
      cursor_id_addr = NULL;
    }
  }
  void destroy_cursor_id_addr_map();

  int add_ps_id_addrs(ObPsIdAddrs *ps_id_addrs) {
    return ps_id_addrs_map_.unique_set(ps_id_addrs);
  }

  ObPsIdAddrs *get_ps_id_addrs(uint32_t client_ps_id) const;
  void remove_ps_id_addrs(uint32_t client_ps_id) {
    ObPsIdAddrs *ps_id_addrs = ps_id_addrs_map_.remove(client_ps_id);
    if (NULL != ps_id_addrs) {
      ps_id_addrs->destroy();
      ps_id_addrs = NULL;
    }
  }
  void destroy_ps_id_addrs_map();

  void set_is_read_only_user(bool is_read_only_user)
  {
    is_read_only_user_ = is_read_only_user;
  }
  void set_is_request_follower_user(bool is_request_follower_user)
  {
    is_request_follower_user_ = is_request_follower_user;
  }

  bool is_read_only_user() const { return is_read_only_user_; }
  bool is_request_follower_user() const { return is_request_follower_user_; }

public:
  ObSessionFieldMgr field_mgr_;
  bool is_session_pool_client_; // used for ObMysqlClient
  ObSessionVarVersion hash_version_;
  ObSessionVarValHash val_hash_;

private:
  int load_all_cached_variable();

  bool is_inited_;
  // when exec set transaction xxx, it is true until the next transaction commit
  bool is_trans_specified_;
  // the default value is false, we will check it when we receive the ok packet of saved login
  bool is_global_vars_changed_;
  //when user set proxy_idc_name, set it true;
  bool is_user_idc_name_set_;
  //when user set ob_route_policy, set it true;
  bool is_read_consistency_set_;
  // is oracle mode
  bool is_oracle_mode_;

  bool enable_shard_authority_;
  bool enable_reset_db_;

  // original login capability
  obmysql::ObMySQLCapabilityFlags orig_capability_;

  // proxy capability
  uint64_t cap_;

  // safe snapshot version
  int64_t safe_read_snapshot_;
  // sync safe snapshot version
  int64_t syncing_safe_read_snapshot_;
  // ob route policy
  int64_t route_policy_;

  ObProxyRoutePolicyEnum proxy_route_policy_;

  // login packet will be used to next time(include raw packet data and analyzed result)
  proxy::ObMysqlAuthRequest login_req_;

  // ssl request packet
  obmysql::OMPKSSLRequest ssl_req_;

  // privilege info of current client session
  ObProxySessionPrivInfo priv_info_;

  // privilege info of current logic db user
  dbconfig::ObShardUserPrivInfo up_info_;

  // saved start transaction sql
  common::ObString saved_start_trans_sql_;

  ObProxyLoginUserType user_identity_;
  ObSessionVarVersion version_;
  // cached variables
  obutils::ObCachedVariables cached_variables_;
  // global variables version, will be set at the first time get login responce(OK packet)
  int64_t global_vars_version_;
  // obproxy_route_addr
  int64_t obproxy_route_addr_;

  ObSysVarSetProcessor *var_set_processor_;
  int64_t cluster_id_;
  common::ObString real_meta_cluster_name_;
  char *real_meta_cluster_name_str_;

  common::ObString scramble_string_;
  char scramble_buf_[obmysql::OMPKHandshake::SCRAMBLE_TOTAL_SIZE + 1];

  common::ObString idc_name_;
  char idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];

  DBServerType server_type_;
  dbconfig::ObShardConnector *shard_conn_;
  int64_t group_id_;
  bool is_allow_use_last_session_;

  common::ObString client_host_;
  char client_host_buf_[common::MAX_IP_ADDR_LENGTH];

  common::ObString origin_username_;
  char username_buf_[common::OB_MAX_USER_NAME_LENGTH];

  // consistency_level_prop in shard_conn_prop
  ObConsistencyLevel consistency_level_prop_;
  // if has synced will not need sync to avoid loop sync when compare failed
  bool need_sync_session_vars_;

  uint32_t recv_client_ps_id_;
  uint32_t ps_id_;
  ObPsEntry *ps_entry_;
  ObPsIdEntryMap ps_id_entry_map_;
  common::ObString text_ps_name_;
  ObTextPsEntry *text_ps_entry_;
  ObTextPsNameEntryMap text_ps_name_entry_map_;
  bool is_text_ps_execute_;

  uint32_t cursor_id_;
  ObCursorIdAddrMap cursor_id_addr_map_;

  ObPsIdAddrsMap ps_id_addrs_map_;

  bool is_read_only_user_;
  bool is_request_follower_user_;

  char text_ps_name_buf_[common::OB_MAX_TEXT_PS_NAME_LENGTH];

  DISALLOW_COPY_AND_ASSIGN(ObClientSessionInfo);
};

inline int ObClientSessionInfo::set_origin_username(const common::ObString &username)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(username.length() > common::OB_MAX_USER_NAME_LENGTH)) {
    ret = common::OB_SIZE_OVERFLOW;
    PROXY_LOG(WARN, "username buf is not enough", K(username), K(ret));
  } else {
    MEMCPY(username_buf_, username.ptr(), username.length());
    origin_username_.assign_ptr(username_buf_, username.length());
  }
  return ret;
}

inline void ObClientSessionInfo::set_client_host(const common::ObAddr &host)
{
  if (host.ip_to_string(client_host_buf_, common::MAX_IP_ADDR_LENGTH)) {
    client_host_.assign(client_host_buf_, static_cast<int32_t>(strlen(client_host_buf_)));
  }
}

inline ObPsEntry *ObClientSessionInfo::get_ps_entry(uint32_t ps_id)
{
  int ret = OB_SUCCESS;
  ObPsEntry *ps_entry = NULL;
  ObPsIdEntry *ps_id_entry = NULL;
  if (OB_FAIL(ps_id_entry_map_.get_refactored(ps_id, ps_id_entry))) {
    if (OB_HASH_NOT_EXIST != ret) {
      _PROXY_LOG(WARN, "fail to get ps_id_entry with client ps id, ret=%d, client_ps_id=%d", ret, ps_id);
    }
  } else if (OB_ISNULL(ps_id_entry)) {
    ret = OB_ERR_UNEXPECTED;
    _PROXY_LOG(WARN, "ps_id_entry is null, ret=%d, client_ps_id=%d", ret, ps_id);
  } else {
    ps_entry = ps_id_entry->ps_entry_;
  }
  return ps_entry;
}

inline void ObClientSessionInfo::set_idc_name(const ObString &name)
{
  const int64_t len = min(name.length(), OB_PROXY_MAX_IDC_NAME_LENGTH);
  MEMCPY(idc_name_buf_, name.ptr(), len);
  idc_name_.assign(idc_name_buf_, static_cast<int32_t>(len));
  is_user_idc_name_set_ = true;
}

inline bool ObClientSessionInfo::need_reset_database(const ObServerSessionInfo &server_info) const
{
  if (!is_session_pool_client_) {
    return get_db_name_version() > server_info.get_db_name_version() && enable_reset_db_;
  }
  if (get_database_name().empty()) return false;
  // need reset when database_name not equal
  PROXY_LOG(DEBUG, "need_reset_database", K(get_database_name()), K(server_info.get_database_name()));
  if (is_oracle_mode()) {
    return get_database_name().compare(server_info.get_database_name()) != 0;
  } else {
    return get_database_name().case_compare(server_info.get_database_name()) != 0;
  }
}

inline bool ObClientSessionInfo::need_reset_common_hot_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  bool bret_hash = false;
  if (!is_session_pool_client_) {
    bret =  get_common_hot_sys_var_version() > server_info.get_common_hot_sys_var_version();
  } else {
    if (is_common_hot_sys_version_changed()) {
      bret_hash = true;
    } else if (val_hash_.common_hot_sys_var_hash_ != server_info.val_hash_.common_hot_sys_var_hash_) {
      bret_hash = true;
    }
    bret =  !(const_cast<ObClientSessionInfo*>(this))->field_mgr_.is_same_common_hot_session_vars(server_info.field_mgr_);
    PROXY_LOG(DEBUG, "need_reset_common_hot_session_vars", K(bret), K(bret_hash));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_common_cold_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  if (!is_session_pool_client_) {
    bret =  get_common_sys_var_version() > server_info.get_common_sys_var_version();
  } else {
    bool is_changed = is_common_cold_sys_version_changed();
    if (is_changed) {
      bret = true;
    } else if (0 == version_.common_sys_var_version_) {
      bret = false;
    } else if (val_hash_.common_cold_sys_var_hash_ != server_info.val_hash_.common_cold_sys_var_hash_) {
      bret = true;
    }
    PROXY_LOG(DEBUG, "need_reset_common_cold_session_vars", K(bret), K(is_changed));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_mysql_hot_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  bool bret_hash = false;
  if (!is_session_pool_client_) {
    bret = get_mysql_hot_sys_var_version() > server_info.get_mysql_hot_sys_var_version();
  } else {
    bool is_changed = is_mysql_hot_sys_version_changed();
    if (is_changed) {
      bret_hash = true;
    } else if (val_hash_.mysql_hot_sys_var_hash_ != server_info.val_hash_.mysql_hot_sys_var_hash_) {
      bret_hash = true;
    }
    bret = !(const_cast<ObClientSessionInfo*>(this))->field_mgr_.is_same_mysql_hot_session_vars(server_info.field_mgr_);
    PROXY_LOG(DEBUG, "need_reset_mysql_hot_session_vars", K(bret), K(bret_hash), K(is_changed));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_mysql_cold_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  if (!is_session_pool_client_) {
    bret =  get_mysql_sys_var_version() > server_info.get_mysql_sys_var_version();
  } else {
    bool is_changed = is_mysql_cold_sys_version_changed();
    if (is_changed) {
      bret = true;
    } else if (val_hash_.mysql_cold_sys_var_hash_ != server_info.val_hash_.mysql_cold_sys_var_hash_) {
      bret = true;
    }
    PROXY_LOG(DEBUG, "need_reset_mysql_cold_session_vars", K(bret), K(is_changed));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_hot_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  bool bret_hash_diff = false;
  if (!is_session_pool_client_) {
    bret =  get_hot_sys_var_version() > server_info.get_hot_sys_var_version();
  } else {
    bool is_changed = is_sys_hot_version_changed();
    if (is_changed) {
      bret = true;
    } else if (val_hash_.hot_sys_var_hash_ != server_info.val_hash_.hot_sys_var_hash_) {
      bret_hash_diff = true;
      bret = true;
    }
    PROXY_LOG(DEBUG, "need_reset_hot_session_vars", K(bret), K(bret_hash_diff), K(is_changed));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_cold_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  if (!is_session_pool_client_) {
    bret = get_sys_var_version() > server_info.get_sys_var_version();
  } else {
    bool is_changed= is_sys_cold_version_changed();
    if (is_changed) {
      bret = true;
    } else if (0 == version_.sys_var_version_) {
      bret = false;
    } else if (val_hash_.cold_sys_var_hash_ != server_info.val_hash_.cold_sys_var_hash_) {
      bret = true;
    }
    PROXY_LOG(DEBUG, "need_reset_cold_session_vars", K(bret), K(is_changed));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_user_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = false;
  if (!is_session_pool_client_) {
    bret = get_user_var_version() > server_info.get_user_var_version();
  } else {
    bool is_changed = is_user_var_version_changed();
    if (is_changed) {
      bret = true;
    } else if (0 == version_.user_var_version_) {
      bret = false;
    } else if (val_hash_.user_var_hash_ != server_info.val_hash_.user_var_hash_) {
      bret = true;
    }
    PROXY_LOG(DEBUG, "need_reset_user_session_vars", K(bret), K(is_changed));
  }
  return bret;
}

inline bool ObClientSessionInfo::need_reset_last_insert_id(const ObServerSessionInfo &server_info) const
{
  if (is_oceanbase_server()) {
    if (!is_session_pool_client_) {
      return get_last_insert_id_version() > server_info.get_last_insert_id_version();
    }
    return !(const_cast<ObClientSessionInfo*>(this))->field_mgr_.is_same_last_insert_id_var(server_info.field_mgr_);
  } else {
    return false;
  }
}

inline bool ObClientSessionInfo::need_reset_safe_read_snapshot(const ObServerSessionInfo &server_info) const
{
  return syncing_safe_read_snapshot_ > 0 && server_info.is_safe_read_weak_supported();
}

inline bool ObClientSessionInfo::need_reset_session_vars(const ObServerSessionInfo &server_info) const
{
  bool bret = true;
  bool bret2 = false;
  if (is_oceanbase_server()) {
    bret = need_reset_common_hot_session_vars(server_info)
           || need_reset_common_cold_session_vars(server_info)
           || need_reset_hot_session_vars(server_info)
           || need_reset_cold_session_vars(server_info)
           || need_reset_user_session_vars(server_info);
    bret2 = need_reset_safe_read_snapshot(server_info);
  } else {
    bret =  need_reset_common_hot_session_vars(server_info)
           || need_reset_common_cold_session_vars(server_info)
           || need_reset_mysql_hot_session_vars(server_info)
           || need_reset_mysql_cold_session_vars(server_info)
           || need_reset_user_session_vars(server_info);
  }
  // for defence
  if (bret && !need_sync_session_vars_) {
    bret = bret2;
    PROXY_LOG(WARN, "session not same but no need sync", K(bret2));
  } else if (!bret) {
    bret = bret2;
  }
  return bret;
}

inline int ObClientSessionInfo::create_scramble(common::ObMysqlRandom &random)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(random.create_random_string(scramble_buf_, sizeof(scramble_buf_)))) {
    scramble_string_.assign_ptr(scramble_buf_, static_cast<int32_t>(STRLEN(scramble_buf_)));
  }
  return ret;
}

inline bool ObClientSessionInfo::enable_analyze_internal_cmd() const
{
  return (USER_TYPE_METADB == user_identity_
          || USER_TYPE_ROOTSYS == user_identity_
          || USER_TYPE_PROXYSYS == user_identity_);
}

inline bool ObClientSessionInfo::need_do_prepare(ObServerSessionInfo &server_info) const
{
  return 0 != ps_id_ && !server_info.is_ps_id_pair_exist(ps_id_);
}

inline bool ObClientSessionInfo::need_do_text_ps_prepare(ObServerSessionInfo &server_info) const
{
  return NULL != text_ps_entry_ && !server_info.is_server_text_ps_name_exist(text_ps_entry_->get_version());
}

int ObClientSessionInfo::get_ps_sql(common::ObString &ps_sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    _PROXY_LOG(WARN, "ps entry is null, ret=%d", ret);
  } else {
    const common::ObString &sql = ps_entry_->get_base_ps_sql();
    ps_sql.assign(const_cast<char *>(sql.ptr()), sql.length());
  }
  return ret;
}

int ObClientSessionInfo::get_text_ps_sql(common::ObString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(text_ps_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    _PROXY_LOG(WARN, "text ps entry is null, ret = %d", ret);
  } else {
    const common::ObString &tmp_sql = text_ps_entry_->get_base_ps_sql();
    sql.assign(const_cast<char*>(tmp_sql.ptr()), tmp_sql.length());
  }
  return ret;
}

}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase
#endif /* OBPROXY_SESSION_INFO_H */
