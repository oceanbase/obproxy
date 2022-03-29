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

#ifndef OBPROXY_CONFIG_PROCESSOR_H
#define OBPROXY_CONFIG_PROCESSOR_H

#include "lib/utility/ob_print_utils.h"
#include "lib/json/ob_json.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/string/ob_string.h"
#include "utils/ob_proxy_lib.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/config/ob_config.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "qos/ob_proxy_qos_action.h"
#include "qos/ob_proxy_qos_condition.h"
#include "proxy/mysql/ob_mysql_transact.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

enum ObProxyConfigType
{
  INDEX_CONFIG = 0,
  INIT_CONFIG,
  DYNAMIC_CONFIG,
  LIMIT_CONTROL_CONFIG,
  FUSE_CONTROL_CONFIG,
  SECURITY_CONFIG,
  // TODO CLUSTER_CHANGE_CONFIG,
  INVALID_CONFIG
};

class ObProxyAppConfig;
class ObProxyBaseConfig : public common::ObInitConfigContainer
{
public:
  ObProxyBaseConfig()
    : type_(INVALID_CONFIG), need_sync_(false), is_complete_(false),
      app_name_(), version_(), api_version_()
  {
  }
  ObProxyBaseConfig(ObProxyConfigType type, bool need_sync=true)
    : type_(type), need_sync_(need_sync), is_complete_(false), app_name_(), version_(), api_version_()
  {
  }
  virtual ~ObProxyBaseConfig() { destroy_config_container(); }

  void destroy_config_container();
  int parse_from_json(json::Value &json_value, const bool is_from_local);
  int to_json_str(common::ObSqlString &buf) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int parse_config_meta(json::Value &json_value, const bool is_from_local);
  int meta_to_json(common::ObSqlString &buf) const;

  virtual int parse_config_spec(json::Value &json_value);
  virtual int spec_to_json(common::ObSqlString &buf) const;
  int assign(const ObProxyBaseConfig &other);
  int add_config_item(const common::ObString &config_name, const common::ObString &config_value);

  int get_file_name();
  int dump_to_local(const char *file_path);
  int load_from_local(const char *file_path, ObProxyAppConfig &app_config);

public:
  ObProxyConfigType type_;
  bool need_sync_;
  bool is_complete_;
  ObProxyConfigString app_name_;
  ObProxyConfigString version_;
  ObProxyConfigString api_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyBaseConfig);
};

class ObProxyConfigReference
{
public:
  ObProxyConfigReference() : type_(INVALID_CONFIG), version_(), data_id_() {}
  explicit ObProxyConfigReference(ObProxyConfigType type)
    : type_(type), version_(), data_id_() {}
  ~ObProxyConfigReference() {}

  int parse_config_reference(json::Value &json_value);
  int reference_to_json(common::ObSqlString &buf) const;
  bool is_valid() const
  {
    return INVALID_CONFIG != type_ && !version_.empty() && !data_id_.empty();
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int assign(const ObProxyConfigReference &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      type_ = other.type_;
      version_.assign(other.version_);
      data_id_.assign(other.data_id_);
    }
    return ret;
  }

public:
  ObProxyConfigType type_;
  ObProxyConfigString version_;
  ObProxyConfigString data_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyConfigReference);
};

class ObProxyIndexConfig : public ObProxyBaseConfig
{
public:
  ObProxyIndexConfig() : ObProxyBaseConfig(INDEX_CONFIG),
                         init_ref_(INIT_CONFIG), dynamic_ref_(DYNAMIC_CONFIG),
                         limit_control_ref_(LIMIT_CONTROL_CONFIG),
                         fuse_control_ref_(FUSE_CONTROL_CONFIG) {}
  virtual ~ObProxyIndexConfig() {}

  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int parse_config_spec(json::Value &json_value);
  virtual int spec_to_json(common::ObSqlString &buf) const;

  int assign(const ObProxyIndexConfig &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      if (OB_SUCC(ObProxyBaseConfig::assign(other))) {
        init_ref_.assign(other.init_ref_);
        dynamic_ref_.assign(other.dynamic_ref_);
        limit_control_ref_.assign(other.limit_control_ref_);
        fuse_control_ref_.assign(other.fuse_control_ref_);
      }
    }
    return ret;
  }

public:
  ObProxyConfigReference init_ref_;
  ObProxyConfigReference dynamic_ref_;
  ObProxyConfigReference limit_control_ref_;
  ObProxyConfigReference fuse_control_ref_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyIndexConfig);
};

class ObProxyInitConfig : public ObProxyBaseConfig
{
public:
  ObProxyInitConfig() : ObProxyBaseConfig(INIT_CONFIG) {}
  virtual ~ObProxyInitConfig() {}

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int assign(const ObProxyInitConfig &other);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyInitConfig);
};

class ObProxyDynamicConfig : public ObProxyBaseConfig
{
public:
  ObProxyDynamicConfig() : ObProxyBaseConfig(DYNAMIC_CONFIG) {}
  virtual ~ObProxyDynamicConfig() {}

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int assign(const ObProxyDynamicConfig &other);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyDynamicConfig);
};

enum ObProxyLimitStatus {
  LIMIT_STATUS_INVALID = -1,
  LIMIT_STATUS_OBSERVE,
  LIMIT_STATUS_RUNNING,
  LIMIT_STATUS_SHUTDOWN
};

enum ObProxyLimitMode {
  LIMIT_MODE_INVALID = -1,
  LIMIT_MODE_DB_USER_MATCH,
  LIMIT_MODE_KEY_WORD_MATCH,
  LIMIT_MODE_GRADE_MATCH,
  LIMIT_MODE_TESTLOAD_LIMIT,
  LIMIT_MODE_TESTLOAD_FUSE,
  LIMIT_MODE_HIGH_RISK_FUSE
};

#define LIMIT_RULE_MAP_BUCKET 8

class ObProxyLimitConfig
{
public:
  ObProxyLimitConfig() : action_(NULL),
    cond_array_(ObModIds::OB_PROXY_QOS, OB_MALLOC_NORMAL_BLOCK_SIZE),
    limit_mode_(LIMIT_MODE_INVALID), limit_priority_(-1), limit_qps_(-1),
    limit_status_(LIMIT_STATUS_INVALID), limit_time_window_(-1),
    limit_conn_(0), limit_fuse_time_(0), allocator_(NULL) {};
  ~ObProxyLimitConfig();

  int init(common::ObArenaAllocator &allocator);

  void set_action(qos::ObProxyQosAction *action) { action_ = action; };
  qos::ObProxyQosAction *get_action() { return action_; }

  ObIArray<qos::ObProxyQosCond*> &get_cond_array() { return cond_array_; }

  int set_cluster_name(const ObString &cluster_name) { return ob_write_string(*allocator_, cluster_name, cluster_name_); }
  const ObString &get_cluster_name() const { return cluster_name_; }

  int set_tenant_name(const ObString &tenant_name) { return copy_param(tenant_name_, tenant_name); }
  const ObString &get_tenant_name() const { return tenant_name_; }

  int set_database_name(const ObString &database_name) { return copy_param(database_name_, database_name); }
  const ObString &get_database_name() const { return database_name_; }

  int set_user_name(const ObString &user_name) { return copy_param(user_name_, user_name); }
  const ObString &get_user_name() const { return user_name_; }

  int set_limit_name(const ObString &limit_name) { return copy_param(limit_name_, limit_name); }
  const ObString &get_limit_name() const { return limit_name_; }

  hash::ObHashMap<ObString, ObString>& get_limit_rule() { return limit_rule_map_; }

  void set_limit_mode(const ObProxyLimitMode limit_mode) { limit_mode_ = limit_mode; }
  ObProxyLimitMode get_limit_mode() const { return limit_mode_; }

  void set_limit_priority(const int64_t limit_priority) { limit_priority_ = limit_priority; }
  int64_t get_limit_priority() const { return limit_priority_; }

  void set_limit_qps(const int64_t limit_qps) { limit_qps_ = limit_qps; }
  int64_t get_limit_qps() const { return limit_qps_; }

  void set_limit_status(const ObProxyLimitStatus limit_status) { limit_status_ = limit_status; }
  ObProxyLimitStatus get_limit_status() const { return limit_status_; }

  int assign(ObProxyLimitConfig &other);
  int parse_limit_rule(const ObString &key, const ObString &value);
  int parse_limit_action(const ObProxyLimitMode limit_mode);
  int handle_action();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int calc(proxy::ObMysqlTransact::ObTransState &trans_state, const proxy::ObClientSessionInfo &cs_info,
           common::ObIAllocator *calc_allocator, bool &is_pass, common::ObString &limit_name);

  template<typename T, typename TA>
  int create_action_or_cond(TA *&action_or_cond)
  {
    int ret = common::OB_SUCCESS;

    action_or_cond = NULL;
    void *ptr = allocator_->alloc(sizeof(T));

    if (OB_UNLIKELY(NULL == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      action_or_cond = new(ptr) T();
    }

    return ret;
  }

private:
  int copy_param(ObString &param, const ObString &value);
  int push_limit_rule(const ObString &key, const ObString &value);

private:
  qos::ObProxyQosAction *action_;
  common::ObSEArray<qos::ObProxyQosCond*, 4> cond_array_;

  ObString cluster_name_;
  ObString tenant_name_;
  ObString database_name_;
  ObString user_name_;
  ObString limit_name_;
  ObProxyLimitMode limit_mode_;
  hash::ObHashMap<ObString, ObString> limit_rule_map_;
  int64_t limit_priority_;
  int64_t limit_qps_;  // unit r/s

  // this is break rule's qps and rt
  int64_t limit_rule_qps_; // unit r/s
  int64_t limit_rule_rt_;  // unit ms
  ObProxyLimitStatus limit_status_;
  int64_t limit_time_window_;
  double limit_conn_;
  int64_t limit_fuse_time_;

  common::ObArenaAllocator *allocator_;
};

class ObProxyLimitControlConfig : public ObProxyBaseConfig
{
public:
  ObProxyLimitControlConfig() : ObProxyBaseConfig(LIMIT_CONTROL_CONFIG), allocator_(ObModIds::OB_PROXY_QOS),
    limit_config_array_(ObModIds::OB_PROXY_QOS, OB_MALLOC_NORMAL_BLOCK_SIZE) {}
  ObProxyLimitControlConfig(ObProxyConfigType type) : ObProxyBaseConfig(type), allocator_(ObModIds::OB_PROXY_QOS),
    limit_config_array_(ObModIds::OB_PROXY_QOS, OB_MALLOC_NORMAL_BLOCK_SIZE) {}
  virtual ~ObProxyLimitControlConfig();

  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int parse_config_spec(json::Value &json_value);
  virtual int spec_to_json(common::ObSqlString &buf) const;
  int assign(const ObProxyLimitControlConfig &other);
  int calc(proxy::ObMysqlTransact::ObTransState &trans_state, const proxy::ObClientSessionInfo &cs_info,
           common::ObIAllocator *calc_allocator, bool &is_pass, common::ObString &limit_name);

  const ObIArray<ObProxyLimitConfig*> &get_limit_config_array() const { return limit_config_array_; }
  ObIArray<ObProxyLimitConfig*> &get_limit_config_array() { return limit_config_array_; }

private:
  int parse_limit_action(json::Value &json_value, ObProxyLimitConfig *limit_config);
  int parse_limit_rule(json::Value &json_value, ObProxyLimitConfig *limit_config);
  int parse_limit_conf(json::Value &json_value);

private:
  common::ObArenaAllocator allocator_;
  common::ObSEArray<ObProxyLimitConfig*, 4> limit_config_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyLimitControlConfig);
};

class ObProxyFuseControlConfig : public ObProxyLimitControlConfig
{
  public:
  ObProxyFuseControlConfig() : ObProxyLimitControlConfig(FUSE_CONTROL_CONFIG) {}
  virtual ~ObProxyFuseControlConfig() {}
};

enum ObProxyAppConfigState
{
  AC_BUILDING = 0,
  AC_AVAIL,
  AC_DELETING,
  AC_INVALID
};

class ObProxyAppConfig : public common::ObSharedRefCount
{
public:
  ObProxyAppConfig()
    : common::ObSharedRefCount(), ac_state_(AC_INVALID),
      app_name_(), version_(),
      index_config_(), init_config_(),
      dynamic_config_(), limit_control_config_(),
      fuse_control_config_()
  {
  }
  virtual ~ObProxyAppConfig() {}

  virtual void free() { destroy(); }
  void destroy();

  void set_building_state() { ac_state_ = AC_BUILDING; }
  bool is_building_state() const { return AC_BUILDING == ac_state_; }
  void set_avail_state() { ac_state_ = AC_AVAIL; }
  bool is_avail_state() const { return AC_AVAIL == ac_state_; }
  void set_deleting_state() { ac_state_ = AC_DELETING; }
  bool is_deleting_state() const { return AC_DELETING == ac_state_; }

  bool is_all_config_complete() const
  {
    return index_config_.is_complete_
           && (!index_config_.init_ref_.is_valid() || init_config_.is_complete_)
           && (!index_config_.dynamic_ref_.is_valid() || dynamic_config_.is_complete_)
           && (!index_config_.limit_control_ref_.is_valid() || limit_control_config_.is_complete_)
           && (!index_config_.fuse_control_ref_.is_valid() || fuse_control_config_.is_complete_);
  }

  static int alloc_building_app_config(const common::ObString &app_name,
                                       const common::ObString &version,
                                       ObProxyAppConfig *&app_config);

  int update_config(const common::ObString &json_str, const ObProxyConfigType type, const bool is_from_local=false);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int calc_limit(proxy::ObMysqlTransact::ObTransState &trans_state, const proxy::ObClientSessionInfo &cs_info,
                 common::ObIAllocator *calc_allocator, bool &is_pass, common::ObString &limit_name);

  int dump_to_local();
  int load_from_local(const char *file_path);
private:
  int handle_index_config(const bool is_from_local);

public:
  ObProxyAppConfigState ac_state_;
  ObProxyConfigString app_name_;
  ObProxyConfigString version_;
  ObProxyIndexConfig index_config_;
  ObProxyInitConfig init_config_;
  ObProxyDynamicConfig dynamic_config_;
  ObProxyLimitControlConfig limit_control_config_;
  ObProxyFuseControlConfig fuse_control_config_;
  ObProxyConfigString security_version_;

public:
  LINK(ObProxyAppConfig, app_config_link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyAppConfig);
};

enum AppConfigVersionIndex
{
  INDEX_CURRENT = 0,
  INDEX_BUILDING,
  MAX_VERSION_SIZE
};

class ObProxyAppSecurityConfig
{
public:
  ObProxyAppSecurityConfig() : source_type_(), ca_(), public_key_(), private_key_() {}
  ~ObProxyAppSecurityConfig() {}

  int update_config();

public:
  static const int64_t SECURITY_CONFIG_STRING_LEN = 4096;
  ObProxySizeConfigString<SECURITY_CONFIG_STRING_LEN> source_type_;
  ObProxySizeConfigString<SECURITY_CONFIG_STRING_LEN> ca_;
  ObProxySizeConfigString<SECURITY_CONFIG_STRING_LEN> public_key_;
  ObProxySizeConfigString<SECURITY_CONFIG_STRING_LEN> private_key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyAppSecurityConfig);
};

class ObProxyConfigProcessor
{
public:
  ObProxyConfigProcessor() {}
  ~ObProxyConfigProcessor() {}

  static const int64_t OB_APP_HASH_BUCKET_SIZE = 16;

  // app name ---> app config
  struct ObProxyAppConfigHashing
  {
    typedef const common::ObString &Key;
    typedef ObProxyAppConfig Value;
    typedef ObDLList(ObProxyAppConfig, app_config_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->app_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObProxyAppConfigHashing, OB_APP_HASH_BUCKET_SIZE> ACHashMap;


  int init();
  ObProxyAppConfig *get_app_config(const common::ObString &appname, const int64_t version_index=INDEX_CURRENT);
  int get_app_config_string(const common::ObString &appname,
                            const ObProxyConfigType type,
                            common::ObSqlString &buf);
  int add_app_config(ObProxyAppConfig &app_config, const int64_t version_index);
  int remove_app_config(const common::ObString &appname, const int64_t version_index);
  int update_app_config(const common::ObString &appname,
                        const common::ObString &version,
                        const common::ObString &config_value,
                        const ObProxyConfigType type);
  int load_local_config();
  void move_local_dir(const common::ObString &app_name, const bool is_rollback=false);

int update_app_security_config(const common::ObString &appname,
                               const common::ObString &version,
                               const common::ObString &config_value,
                               const ObProxyConfigType type);

  ObProxyAppSecurityConfig& get_security_config() { return security_config_; }

private:
  int load_local_app_config(const common::ObString &app_name);
  int handle_app_config_complete(ObProxyAppConfig &new_app_config, const bool is_from_local=false);
  int replace_app_config(const common::ObString &app_name,
                         ObProxyAppConfig &new_app_config,
                         ObProxyAppConfig *&cur_app_config,
                         const bool is_rollback=false);
  void clean_tmp_dir(const common::ObString &app_name);
  int update_global_proxy_config(const ObProxyAppConfig &new_app_config, const ObProxyAppConfig *cur_app_config);
  int rollback_global_proxy_config(const common::ObIArray<common::ObConfigItem *> &old_config_items);
  int do_update_global_proxy_config(const ObProxyBaseConfig &new_app_config, common::ObIArray<common::ObConfigItem *> &old_config_items);

public:
  common::DRWLock rw_locks_[MAX_VERSION_SIZE];
  ACHashMap ac_maps_[MAX_VERSION_SIZE];
  ObProxyAppSecurityConfig security_config_;
  common::DRWLock security_rw_lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyConfigProcessor);
};

ObProxyConfigProcessor &get_global_proxy_config_processor();

ObProxyLimitStatus get_limit_status_by_str(const ObString &limit_status_str);
const char *get_limit_status_str(const ObProxyLimitStatus limit_status);
ObProxyLimitMode get_limit_mode_by_str(const ObString &limit_mode_str);
const char *get_limit_mode_str(const ObProxyLimitMode limit_mode);

const char *get_config_state_str(const ObProxyAppConfigState state);
const char *get_config_type_str(const ObProxyConfigType type);
const char *get_config_file_name(const ObProxyConfigType type);
ObProxyConfigType get_config_type_by_str(const ObString& str);

ObProxyBasicStmtType get_stmt_type_by_name(const ObString &stmt_name);

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_CONFIG_PROCESSOR_H */
