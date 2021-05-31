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

#ifndef OBPROXY_DB_CONFIG_INFO_H
#define OBPROXY_DB_CONFIG_INFO_H

#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/json/ob_json.h"
#include "share/schema/ob_priv_type.h"
#include "dbconfig/ob_proxy_json_shard_config_info.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include <map>

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace obproxy
{
namespace dbconfig
{

static const int64_t SEQUENCE_DEFAULT_MAX_VALUE = 99999999;
static const int64_t SEQUENCE_DEFAULT_MIN_VALUE = 1;
static const int64_t SEQUENCE_DEFAULT_STEP = 1000;
static const int64_t SEQUENCE_DEFAULT_RETRY_COUNT = 10;

enum ObDbConfigState
{
    DC_BORN = 0,
    DC_BUILDING,
    DC_BUILD_FAILED,
    DC_AVAIL,
    DC_UPDATING,
    DC_DELETING,
    DC_DEAD
};

enum ObResourceFetchOp
{
  FETCH_MIN_OP = 0,
  FETCH_WITH_BUILDING_OP,
  FETCH_WITH_UPDATING_OP
};

enum ObTestLoadType
{
  TESTLOAD_NON = -1,
  TESTLOAD_ALIPAY, /* testload = 1 */
  TESTLOAD_MIRROR, /* testload = 2 */
  TESTLOAD_ALIPAY_COMPATIBLE, /* testload = 8 */
  TESTLOAD_MIRROR_COMPATIBLE  /* testload = 9 */
};

class ObDataBaseKey
{
public:
  ObDataBaseKey()
    : tenant_name_(),
      database_name_() {}
  ~ObDataBaseKey() {}

  uint64_t hash(const uint64_t seed = 0) const;

  int assign(const ObDataBaseKey &other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(this != &other)) {
      tenant_name_.assign(other.tenant_name_);
      database_name_.assign(other.database_name_);
    }
    return ret;
  }

  bool operator==(const ObDataBaseKey &other) const
  {
    return tenant_name_ == other.tenant_name_
           && database_name_ == other.database_name_;
  }

  bool operator!=(const ObDataBaseKey &other) const { return !(*this == other); }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  obutils::ObProxyConfigString tenant_name_;
  obutils::ObProxyConfigString database_name_;
};

inline uint64_t ObDataBaseKey::hash(const uint64_t seed) const
{
  uint64_t hash_ret = tenant_name_.config_string_.hash(seed);
  hash_ret = database_name_.config_string_.hash(hash_ret);
  return hash_ret;
}

struct ObReferenceInfo
{
  obutils::ObProxyConfigString parent_;
  obutils::ObProxyConfigString namespace_;
  obutils::ObProxyConfigString kind_;
};

struct StringCIComparator {
  bool operator()(const std::string &key1, const std::string &key2) const
  {
    bool bret = true;
    int ret = strncasecmp(key1.c_str(), key2.c_str(), std::min(key1.length(), key2.length()));
    if (ret > 0) {
      bret = false;
    } else if (ret == 0) {
      bret = key1.length() < key2.length();
    }
    return bret;
  }
};

class ObDbConfigChild : public common::ObSharedRefCount
{
public:
  explicit ObDbConfigChild(const ObDDSCrdType type) : type_(type), dc_state_(DC_BORN),
                                                      need_dump_config_(false),
                                                      name_(), version_(),
                                                      db_info_key_(), kv_map_(), k_obj_map_() {}
  virtual ~ObDbConfigChild() {}

  virtual int init(const std::string &meta, const ObDataBaseKey &key, const bool with_db_info_key = true);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int get_file_name(char *buf, const int64_t buf_len) const;
  virtual int parse_from_json(const json::Value &json_value);
  static bool is_necessary_config(ObDDSCrdType type)
  {
    return TYPE_DATABASE == type
           || TYPE_SHARDS_TPO == type || TYPE_SHARDS_ROUTER == type
           || TYPE_SHARDS_CONNECTOR == type;
  }
  virtual int assign(const ObDbConfigChild &other);

  virtual void destroy() = 0;
  virtual void free() { destroy(); }
  static int alloc_child_info(const ObDataBaseKey &key,
                              ObDDSCrdType type,
                              const common::ObString &name,
                              const common::ObString &version,
                              ObDbConfigChild *&child_info);

  bool is_version_changed(const common::ObString &version) const { return version_.config_string_ != version; }
  common::ObString get_version() { return version_.config_string_; }
  void set_version(const common::ObString &version) { version_.set_value(version); }

  virtual bool is_avail() const { return DC_AVAIL == dc_state_ || DC_UPDATING == dc_state_; }
  void set_building_state() { dc_state_ = DC_BUILDING; }
  void set_build_fail_state() { dc_state_ = DC_BUILD_FAILED; }
  void set_avail_state() { dc_state_ = DC_AVAIL; }
  void set_updating_state() { dc_state_ = DC_UPDATING; }
  void set_deleting_state() { dc_state_ = DC_DELETING; }
  bool is_building_state() const { return dc_state_ == DC_BUILDING; }
  bool is_updating_state() const { return dc_state_ == DC_UPDATING; }
  ObDbConfigState get_dbconfig_state() const { return dc_state_; }
  void set_need_dump_config(bool need_dump) { need_dump_config_ = need_dump; }
  bool need_dump_config() const { return need_dump_config_; }

  common::ObString get_variable_value(const common::ObString &key_str) const;
  int init_test_load_table_map(std::map<std::string, std::string> &test_load_table_map);

public:
  static const int64_t DB_HASH_BUCKET_SIZE = 16;

public:
  ObDDSCrdType type_;
  ObDbConfigState dc_state_;
  bool need_dump_config_;
  obutils::ObProxyConfigString name_;
  obutils::ObProxyConfigString version_;
  ObDataBaseKey db_info_key_;
  std::map<std::string, std::string> kv_map_;
  std::map<std::string, std::string> k_obj_map_; // use to store object or array string
};

class ObShardUserPrivInfo
{
public:
  ObShardUserPrivInfo() : priv_set_(OB_PRIV_SET_EMPTY), username_(),
                          host_(), password_stage2_(), password_() {}

  ~ObShardUserPrivInfo() {}

  void reset()
  {
    priv_set_ = OB_PRIV_SET_EMPTY;
    username_.reset();
    host_.reset();
    password_stage2_.reset();
    password_.reset();
  }
  bool is_valid() const
  {
    return !username_.empty() && !host_.empty()
           && !password_stage2_.empty() && !password_.empty();
  }

  int to_json_str(common::ObSqlString &buf) const;
  int parse_from_json(const json::Value &json_value);
  TO_STRING_KV(K_(priv_set), K_(username), K_(host), K_(password), K_(password_stage2));

  void set_username(const char *username, int64_t len) { username_.set_value(len, username); }
  void set_host(const char* host, int64_t len) { host_.set_value(len, host); }
  int set_password(const char *password, int64_t len);
  void set_user_priv(const char *priv_str, int64_t len, OB_PRIV_SHIFT type);
  bool has_alter_priv() const { return priv_set_ & OB_PRIV_ALTER; }
  bool has_create_priv() const { return priv_set_ & OB_PRIV_CREATE; }
  bool has_delete_priv() const { return priv_set_ & OB_PRIV_DELETE; }
  bool has_drop_priv() const { return priv_set_ & OB_PRIV_DROP; }
  bool has_insert_priv() const { return priv_set_ & OB_PRIV_INSERT; }
  bool has_update_priv() const { return priv_set_ & OB_PRIV_UPDATE; }
  bool has_select_priv() const { return priv_set_ & OB_PRIV_SELECT; }
  bool has_index_priv() const { return priv_set_ & OB_PRIV_INDEX; }

  int assign(const ObShardUserPrivInfo &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      priv_set_ = other.priv_set_;
      username_.assign(other.username_);
      host_.assign(other.host_);
      password_stage2_.assign(other.password_stage2_);
      password_.assign(other.password_);
    }
    return ret;
  }

public:
  ObPrivSet priv_set_;
  obutils::ObProxyConfigString username_;
  obutils::ObProxyConfigString host_;
  obutils::ObProxyConfigString password_stage2_; // server stored stage2
  obutils::ObProxyConfigString password_; // blow fish encrypted password

private:
  DISALLOW_COPY_AND_ASSIGN(ObShardUserPrivInfo);
};

class ObDataBaseAuth : public ObDbConfigChild
{
public:
  ObDataBaseAuth() : ObDbConfigChild(TYPE_DATABASE_AUTH) {}
  virtual ~ObDataBaseAuth() {}

  virtual void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  virtual int assign(const ObDbConfigChild &other);

  int get_user_priv_info(const common::ObString &username,
                         const common::ObString &host,
                         ObShardUserPrivInfo &up_info);

public:
  common::ObSEArray<ObShardUserPrivInfo, 3> up_array_;
  LINK(ObDataBaseAuth, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDataBaseAuth);
};


enum DatabasePropType {
  PROP_TYPE_INVALID = 0,
  PROP_TYPE_TESTLOAD,
  PROP_TYPE_MIRROR,
  PROP_TYPE_SELFADJUST,
  PROP_TYPE_WHITELIST
};

typedef std::map<std::string, int64_t, StringCIComparator> RuleMap;
class ObDataBaseProp : public ObDbConfigChild
{
public:
  ObDataBaseProp() : ObDbConfigChild(TYPE_DATABASE_PROP),
                     prop_type_(PROP_TYPE_INVALID),
                     prop_name_(), prop_rule_(), rule_map_() {}
  virtual ~ObDataBaseProp() {}

  virtual void destroy()
  {
    dc_state_ = DC_DEAD;
    _PROXY_LOG(INFO, "ObDataBaseProp will be destroyed, this=%p", this);
    op_free(this);
  }
  virtual int assign(const ObDbConfigChild &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  int parse_db_prop_rule(json::Value &prop_value);
  int set_db_prop_type(const common::ObString &prop_type);
  int get_disaster_eid(const common::ObString &disaster_status, int64_t &eid);

public:
  DatabasePropType prop_type_;
  obutils::ObProxyConfigString prop_name_;
  obutils::ObProxyConfigString prop_rule_;
  RuleMap rule_map_;
  LINK(ObDataBaseProp, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDataBaseProp);
};


class ObDataBaseVar : public ObDbConfigChild
{
public:
  ObDataBaseVar() : ObDbConfigChild(TYPE_DATABASE_VAR) {}
  virtual ~ObDataBaseVar() {}

  virtual void destroy()
  {
    dc_state_ = DC_DEAD;
    _PROXY_LOG(INFO, "ObDataBaseVar will be destroyed, this=%p", this);
    op_free(this);
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool enable_remote_connection() const;
  int init_test_load_table_map(std::map<std::string, std::string> &test_load_table_map);
public:
  LINK(ObDataBaseVar, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDataBaseVar);
};

class ObElasticInfo
{
public:
  ObElasticInfo() : eid_(0), read_weight_(0), write_weight_(0), shard_name_() {}
  ~ObElasticInfo() {}

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int to_json_str(common::ObSqlString &buf) const;
  int parse_from_json(const json::Value &json_value);

  int assign(const ObElasticInfo &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      shard_name_.assign(other.shard_name_);
      eid_ = other.eid_;
      read_weight_ = other.read_weight_;
      write_weight_ = other.write_weight_;
    }
    return ret;
  }

public:
  int64_t eid_;
  int64_t read_weight_;
  int64_t write_weight_;
  obutils::ObProxyConfigString shard_name_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObElasticInfo);
};

typedef common::ObSEArray<ObElasticInfo, 3> ObEsInfoArray;

class ObGroupCluster
{
public:
  ObGroupCluster() : gc_name_(), total_read_weight_(0), total_write_weight_(0), es_array_() {}
  ~ObGroupCluster() {}

  int assign(const ObGroupCluster &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int to_json_str(common::ObSqlString &buf) const;
  int parse_from_json(const json::Value &json_value);
  common::ObString get_shard_name_by_weight() const;
  common::ObString get_shard_name_by_eid(const int64_t eid) const;
  int64_t get_es_size() const;
  int get_random_elastic_id(int64_t &eid) const;
  int get_elastic_id_by_read_weight(int64_t &eid) const;
  int get_elastic_id_by_write_weight(int64_t &eid) const;
  int get_elastic_id_by_weight(int64_t &eid, const bool is_read_stmt) const;
  const ObElasticInfo *get_random_es_info() const;
  void destroy()
  {
    es_array_.reset();
    op_free(this);
  }
  inline void calc_total_read_weight()
  {
    total_read_weight_ = 0;
    for (int64_t i = 0; i < es_array_.count(); ++i) {
      total_read_weight_ += es_array_.at(i).read_weight_;
    }
    total_write_weight_ = 0;
    for (int64_t i = 0; i < es_array_.count(); ++i) {
      total_write_weight_ += es_array_.at(i).write_weight_;
    }
  }
public:
  obutils::ObProxyConfigString gc_name_;
  int64_t total_read_weight_;
  int64_t total_write_weight_;
  ObEsInfoArray es_array_;

public:
  LINK(ObGroupCluster, group_cluster_link_);
};

class ObShardTpo : public ObDbConfigChild
{
public:
  ObShardTpo() : ObDbConfigChild(TYPE_SHARDS_TPO),
                 is_zone_shard_tpo_(false), tpo_name_(), arch_(),
                 specification_(), gc_map_() {}
  virtual ~ObShardTpo() {}

  virtual void destroy();
  virtual int assign(const ObDbConfigChild &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  int get_group_cluster(const common::ObString &gc_name, ObGroupCluster *&gc_info);
  void parse_tpo_specification(const common::ObString &specification);

public:
  struct ObGroupClusterHashing
  {
    typedef const common::ObString &Key;
    typedef ObGroupCluster Value;
    typedef ObDLList(ObGroupCluster, group_cluster_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->gc_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObGroupClusterHashing, ObDbConfigChild::DB_HASH_BUCKET_SIZE> GCHashMap;

public:
  bool is_zone_shard_tpo_;
  obutils::ObProxyConfigString tpo_name_;
  obutils::ObProxyConfigString arch_;
  obutils::ObProxyConfigString specification_;
  GCHashMap gc_map_;
  LINK(ObShardTpo, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObShardTpo);
};

class ObShardRule
{
public:
  ObShardRule() : is_sequence_(false), tb_size_(0), db_size_(0),
                  tb_suffix_len_(0), db_suffix_len_(0),
                  table_name_(), tb_name_pattern_(), db_name_pattern_(),
                  tb_prefix_(), db_prefix_(), tb_suffix_(), tb_tail_(), db_tail_(),
                  tb_rules_(), db_rules_(), es_rules_(),
                  allocator_() {}
  ~ObShardRule() {}

  int init(const std::string &table_name);
  void set_is_sequence() { is_sequence_ = true; }

  int assign(const ObShardRule &other);
  void destroy() { op_free(this); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int to_json_str(common::ObSqlString &buf) const;
  int parse_from_json(const json::Value &json_value);
  int parse_rules_from_json(const json::Value &json_value, ObProxyShardRuleList &rules);
  int get_index_by_real_name(const common::ObString &name, const common::ObString &name_prefix, int64_t index_len, int64_t &index);
  int get_real_name_by_index(const int64_t size, const int64_t suffix_len,
                             int64_t index, const common::ObString &name_prefix,
                             const common::ObString &name_tail,
                             char *buf, const int64_t buf_len);
  static int get_physic_index_random(const int64_t physic_size,int64_t &index);
  static int get_physic_index(const obutils::SqlFieldResult &sql_result,
                       ObProxyShardRuleList &rules,
                       const int64_t physic_size,
                       const ObTestLoadType type,
                       int64_t &index,
                       const bool is_elastic_index = false);
  static void handle_special_char(char *value, int64_t len);

  static int get_physic_index_array(const obutils::SqlFieldResult &sql_result,
                                    ObProxyShardRuleList &rules,
                                    const int64_t physic_size,
                                    const ObTestLoadType type,
                                    common::ObIArray<int64_t> &index_array,
                                    const bool is_elastic_index = false);

private:
  static int64_t get_index_from_route_info();
  int assign_rule_list(const ObProxyShardRuleList &src_rule_list,
                       ObProxyShardRuleList &target_rule_list);
public:
  bool is_sequence_;
  int64_t tb_size_;
  int64_t db_size_;
  int64_t tb_suffix_len_;
  int64_t db_suffix_len_;
  obutils::ObProxyConfigString table_name_;
  obutils::ObProxyConfigString tb_name_pattern_;
  obutils::ObProxyConfigString db_name_pattern_;
  obutils::ObProxyConfigString tb_prefix_;
  obutils::ObProxyConfigString db_prefix_;
  obutils::ObProxyConfigString tb_suffix_;
  obutils::ObProxyConfigString tb_tail_;
  obutils::ObProxyConfigString db_tail_;
  ObProxyShardRuleList tb_rules_;
  ObProxyShardRuleList db_rules_;
  ObProxyShardRuleList es_rules_;
  common::ObArenaAllocator allocator_;

  LINK(ObShardRule, shard_rule_link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObShardRule);
};

class ObShardRouter : public ObDbConfigChild
{
public:
  ObShardRouter() : ObDbConfigChild(TYPE_SHARDS_ROUTER), seq_table_name_(), mr_map_() {}
  virtual ~ObShardRouter() {}

  virtual int assign(const ObDbConfigChild &other);
  virtual void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int get_shard_rule(const common::ObString &tb_name, ObShardRule *&shard_rule);
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  void set_sequence_table(const std::string &table_name) { seq_table_name_.set_value(table_name.length(), table_name.c_str()); }
  const common::ObString get_sequence_table() const { return seq_table_name_.config_string_; }
public:
  struct ObShardsRuleHashing
  {
    typedef const common::ObString &Key;
    typedef ObShardRule Value;
    typedef ObDLList(ObShardRule, shard_rule_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->table_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObShardsRuleHashing, ObDbConfigChild::DB_HASH_BUCKET_SIZE> MarkedRuleHashMap;

public:
  obutils::ObProxyConfigString seq_table_name_;
  MarkedRuleHashMap mr_map_;

  LINK(ObShardRouter, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObShardRouter);
};

class ObMarkedDist
{
public:
  ObMarkedDist() {}
  ~ObMarkedDist() {}

  int init(const std::string &name, const std::string &dist);

  void destroy() { op_free(this); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int assign(const ObMarkedDist &other)
  {
    if (this != &other) {
      shard_name_.assign(other.shard_name_);
      dist_.assign(other.dist_);
    }
    return common::OB_SUCCESS;
  }
public:
  obutils::ObProxyConfigString shard_name_;
  obutils::ObProxyConfigString dist_;
  LINK(ObMarkedDist, marked_dist_link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMarkedDist);
};

class ObShardDist : public ObDbConfigChild
{
public:
  ObShardDist() : ObDbConfigChild(TYPE_SHARDS_DIST), md_map_() {}
  virtual ~ObShardDist() {}

  virtual int assign(const ObDbConfigChild &other);
  virtual void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
public:
  struct ObMarkedDistHashing
  {
    typedef const common::ObString &Key;
    typedef ObMarkedDist Value;
    typedef ObDLList(ObMarkedDist, marked_dist_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->shard_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObMarkedDistHashing, ObDbConfigChild::DB_HASH_BUCKET_SIZE> MarkedDistHashMap;

public:
  MarkedDistHashMap md_map_;
  LINK(ObShardDist, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObShardDist);
};

enum ObConnectorEncType {
  ENC_DEFAULT = 0,
  ENC_BEYOND_TRUST,
};

class ObShardConnector : public ObDbConfigChild
{
public:
  ObShardConnector() : ObDbConfigChild(TYPE_SHARDS_CONNECTOR),
                       shard_name_(), shard_type_(), shard_url_(), enc_type_(ENC_DEFAULT),
                       full_username_(), password_(), org_password_(), database_name_(),
                       username_(), tenant_name_(), cluster_name_(),
                       physic_addr_(), is_physic_ip_(false), physic_port_(), read_consistency_(), server_type_(common::DB_MAX) {}
  virtual ~ObShardConnector() {}

  virtual void destroy()
  {
    dc_state_ = DC_DEAD;
    _PROXY_LOG(DEBUG, "ObShardConnector will be destroyed, this=%p", this);
    op_free(this);
  }
  virtual int assign(const ObDbConfigChild &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  int get_dec_password(char *buf, int64_t len);
  int set_shard_type(const std::string &shard_type);
  int set_shard_type(const common::ObString &shard_type);
  int get_physic_ip(sockaddr &addr);

  bool is_enc_beyond_trust() const { return ENC_BEYOND_TRUST == enc_type_; }

  inline bool is_same_connection(const ObShardConnector *other) const {
    bool bret = true;
    if (server_type_ != other->server_type_ || full_username_ != other->full_username_) {
      bret = false;
    } else if (common::DB_OB_MYSQL != server_type_ && common::DB_OB_ORACLE != server_type_) {
      bret = (physic_addr_ == other->physic_addr_ && physic_port_ == other->physic_port_);
    }

    return bret;
  }
  void set_full_username(const common::ObString& str);
  void set_shard_name(const common::ObString& str) {
    shard_name_.set_value(str);
  }
  bool operator !=(const ObShardConnector &other) const
  {
    return shard_name_ != other.shard_name_
           || shard_type_ != other.shard_type_
           || shard_url_ != other.shard_url_
           || full_username_ != other.full_username_;
  }

public:
  static const int64_t OB_MAX_CONNECTOR_NAME_LENGTH = 128;

public:
  obutils::ObProxyConfigString shard_name_;
  obutils::ObProxyConfigString shard_type_;
  obutils::ObProxyConfigString shard_url_;
  ObConnectorEncType enc_type_;
  obutils::ObProxyConfigString full_username_;
  obutils::ObProxyConfigString password_;
  obutils::ObProxyConfigString org_password_;
  obutils::ObProxyConfigString database_name_;
  common::ObString username_;
  common::ObString tenant_name_;
  common::ObString cluster_name_;
  obutils::ObProxyConfigString physic_addr_;
  bool is_physic_ip_;
  obutils::ObProxyConfigString physic_port_;
  obutils::ObProxyConfigString read_consistency_;
  common::DBServerType server_type_;
  LINK(ObShardConnector, link_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObShardConnector);
};

enum ConnectionPropType {
  TYPE_SHARD_PROP = 0,
  TYPE_SHARD_DIST_OTHRES,
  TYPE_SHARD_DIST_CURRENT,
  TYPE_ZONE_PROP_BY_PREFIX,
  TYPE_ZONE_PROP_BY_NAME,
  TYPE_CONN_PROP_MAX
};

const char *get_type_conn_prop_name(const ConnectionPropType type)
{
  static const char *type_name_array[TYPE_CONN_PROP_MAX] =
  {
    "TYPE_SHARD_PROP",
    "TYPE_SHARD_DIST_OTHRES",
    "TYPE_SHARD_DIST_CURRENT",
    "TYPE_ZONE_PROP_BY_PREFIX",
    "TYPE_ZONE_PROP_BY_NAME"
  };
  const char *str_ret = "TYPE_UNKNOW";
  if (type >= TYPE_SHARD_PROP && type < TYPE_CONN_PROP_MAX) {
    str_ret = type_name_array[type];
  }
  return str_ret;
}

struct ConnectionProperties {
  ConnectionProperties() : max_conn_(0), min_conn_(0) {}
  ConnectionProperties(int64_t max_conn, int64_t min_conn) : max_conn_(max_conn), min_conn_(min_conn) {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t max_conn_;
  int64_t min_conn_;
};

class ObShardProp : public ObDbConfigChild
{
public:
  ObShardProp() : ObDbConfigChild(TYPE_SHARDS_PROP), shard_name_(), connect_timeout_(0),
    socket_timeout_(0), idle_timeout_ms_(0), blocking_timeout_ms_(0), need_prefill_(false), read_consistency_(),
    kv_kv_map_(), kv_vec_map_(), cur_conn_prop_(),
    conn_prop_type_(TYPE_SHARD_PROP), current_zone_(), conn_prop_map_() {}
  virtual ~ObShardProp() {}

  virtual void destroy()
  {
    dc_state_ = DC_DEAD;
    _PROXY_LOG(INFO, "ObShardProp will be destroyed, this=%p", this);
    op_free(this);
  }
  virtual int assign(const ObDbConfigChild &other);
  int set_prop_values_from_conf();
  int set_conn_props_from_conf();

  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  int do_handle_json_value(const json::Value *json_root, const std::string& key);
  int do_handle_connection_prop(const json::Value& json_root, std::string& key);
  int64_t get_max_conn() { return cur_conn_prop_.max_conn_; }
  int64_t get_min_conn() { return cur_conn_prop_.min_conn_; }
  int64_t get_connect_timeout() { return connect_timeout_; }
  int64_t get_socket_timeout() { return socket_timeout_; }
  int64_t get_idle_timeout() { return idle_timeout_ms_; }
  int64_t get_blocking_timeout() { return blocking_timeout_ms_; }
  bool get_need_prefill() { return need_prefill_; }

public:
  obutils::ObProxyConfigString shard_name_;
  int64_t connect_timeout_;
  int64_t socket_timeout_;
  int64_t idle_timeout_ms_;
  int64_t blocking_timeout_ms_;
  bool need_prefill_;
  obutils::ObProxyConfigString read_consistency_;
  std::map<std::string, std::map<std::string, std::string> > kv_kv_map_;
  std::map<std::string, std::vector<std::string> > kv_vec_map_;
  ConnectionProperties cur_conn_prop_;
  ConnectionPropType conn_prop_type_;
  obutils::ObProxyConfigString current_zone_;
  typedef std::map<std::string, ConnectionProperties>  ConnectionPropertiesMap;
  ConnectionPropertiesMap conn_prop_map_;
  LINK(ObShardProp, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObShardProp);
};

template <typename T>
class ObDbConfigChildArrayInfo
{
public:
  ObDbConfigChildArrayInfo() {}
  ~ObDbConfigChildArrayInfo() {}

  int64_t to_string(char *buf, const int64_t buf_len) const;
  void destroy();

public:
  struct ObChildCRHashing
  {
    typedef const common::ObString &Key;
    typedef T Value;
    typedef common::DLL<T, typename T::Link_link_> ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObChildCRHashing, ObDbConfigChild::DB_HASH_BUCKET_SIZE> CCRHashMap;

public:
  ObReferenceInfo ref_info_;
  CCRHashMap ccr_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigChildArrayInfo);
};

struct ObSequenceParam
{
public:
  ObSequenceParam() : min_value_(SEQUENCE_DEFAULT_MIN_VALUE),
                      max_value_(SEQUENCE_DEFAULT_MAX_VALUE),
                      step_(SEQUENCE_DEFAULT_STEP),
                      retry_count_(SEQUENCE_DEFAULT_RETRY_COUNT),
                      is_sequence_enable_(true),
                      tnt_col_() {}
  ~ObSequenceParam() {}
public:
  int64_t min_value_;
  int64_t max_value_;
  int64_t step_;
  int64_t retry_count_;
  // is_sequence_enable_ default is true
  bool  is_sequence_enable_;
  obutils::ObProxyConfigString tnt_col_;
};

class ObDbConfigLogicDb : public ObDbConfigChild
{
public:
  ObDbConfigLogicDb() : ObDbConfigChild(TYPE_DATABASE),
                        dc_state_(DC_BORN),
                        is_strict_spec_mode_(false), need_update_bt_(false), db_name_(),
                        db_cluster_(), db_mode_(), db_type_(),
                        testload_prefix_(), da_array_(),
                        dp_array_(), dv_array_(),
                        st_array_(), sr_array_(),
                        sd_array_(), sc_array_(), sp_array_(), test_load_table_map_() {}
  virtual ~ObDbConfigLogicDb() {}

  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  virtual int get_file_name(char *buf, const int64_t buf_len) const;

  int update_conn_prop_with_shard_dist();
  template<typename T>
  static int child_reference_to_json(const ObDbConfigChildArrayInfo<T> &cr_array, const ObDDSCrdType type,
                                     common::ObSqlString &buf,
                                     const char *testload_prefix_str=NULL,
                                     const bool is_strict_spec_mode = false);
  template<typename T>
  int load_child_config(
       const json::Value &json_value,
       ObDbConfigChildArrayInfo<T> &cr_array,
       const ObDDSCrdType type);
  template<typename T>
  int load_child_array(
      const json::Value &json_value,
      ObDbConfigChildArrayInfo<T> &cr_array,
      const ObDDSCrdType type);

  int get_sequence_param(ObSequenceParam& param);
  int get_database_prop(const common::ObString &prop_name, ObDataBaseProp *&db_prop);
  int get_database_var(ObDataBaseVar *&db_var);
  int64_t get_shard_tpo_count() const;
  int get_shard_tpo_by_zone(ObShardTpo *&shard_tpo, bool use_zone_tpo);
  int get_shard_tpo(ObShardTpo *&shard_tpo);
  int get_all_shard_table(common::ObIArray<common::ObString> &all_table);
  int get_shard_router(const common::ObString &tb_name, ObShardRouter *&shard_router);
  bool is_shard_rule_empty();
  int get_shard_connector(const common::ObString &shard_name, ObShardConnector *&shard_conn);
  int get_testload_shard_connector(const common::ObString &shard_name,
                                   const common::ObString &testload_prefix,
                                   ObShardConnector *&shard_conn);

  virtual void destroy();

  bool enable_remote_connection();

  bool is_avail() const { return !(DC_DELETING == dc_state_ || DC_DEAD == dc_state_); }

  void set_db_name(const common::ObString &db_name) { db_name_.set_value(db_name); }

  void set_testload_prefix(const std::string &testload_prefix)
  {
    testload_prefix_.set_value(testload_prefix.length(), testload_prefix.c_str());
  }

  bool is_strict_spec_mode() const { return is_strict_spec_mode_; }
  void set_is_strict_spec_mode() { is_strict_spec_mode_ = true; }

  bool need_update_bt() const { return need_update_bt_; }
  void set_need_update_bt(const bool need_update=true) { need_update_bt_ = need_update; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int acquire_random_shard_connector(ObShardConnector *&shard_conn);
  int acquire_random_shard_connector(ObShardConnector *&shard_conn, common::ObString &gc_name);

  bool is_single_shard_db_table();

  int get_real_info(const common::ObString &table_name,
                    obutils::ObSqlParseResult &parse_result,
                    ObShardConnector *&shard_conn,
                    char *real_database_name, int64_t db_name_len,
                    char *real_table_name, int64_t tb_name_len,
                    int64_t &group_id, int64_t &table_id, int64_t &es_id,
                    const common::ObString &hint_table, ObTestLoadType testload_type, bool is_read_stmt);
  int get_single_real_table_info(const common::ObString &table_name,
                                 const ObShardConnector &shard_conn,
                                 char *real_database_name, int64_t db_name_len,
                                 char *real_table_name, int64_t tb_name_len,
                                 const common::ObString &hint_table);
  int get_single_table_info(const common::ObString &table_name,
                            ObShardConnector *&shard_conn,
                            char *real_database_name, int64_t db_name_len,
                            char *real_table_name, int64_t tb_name_len,
                            int64_t &group_id, int64_t &table_id, int64_t &es_id,
                            const common::ObString &hint_table,
                            ObTestLoadType testload_type, const bool is_read_stmt);
  int get_shard_table_info(const common::ObString &table_name,
                           obutils::SqlFieldResult &sql_result,
                           ObShardConnector *&shard_conn,
                           char *real_database_name, int64_t db_name_len,
                           char *real_table_name, int64_t tb_name_len,
                           int64_t &group_index, int64_t &tb_index, int64_t &es_index,
                           const common::ObString &hint_table,
                           ObTestLoadType testload_type, const bool is_read_stmt);
  int get_shard_prop(const common::ObString & shard_name,
                      ObShardProp* &shard_prop);

  const common::ObString get_sequence_table_name();
  const common::ObString get_sequence_table_name_from_router();
  const common::ObString get_sequence_table_name_from_variables();

  int get_user_priv_info(const common::ObString &username,
                         const common::ObString &host,
                         ObShardUserPrivInfo &up_info);

  int get_disaster_eid(const common::ObString &disaster_status, int64_t &eid);

  int init_connector_password_for_bt();

  int get_sharding_select_info(const common::ObString &table_name,
                               obutils::ObSqlParseResult &parse_result,
                               ObTestLoadType testload_type,
                               bool is_read_stmt,
                               int64_t es_index,
                               common::ObIAllocator &allocator,
                               common::ObIArray<ObShardConnector*> &shard_connector_array,
                               common::ObIArray<common::ObString> &physical_table_name_array);
  int init_test_load_table_map();
  int check_and_get_testload_table(std::string origin_name, std::string &real_name);

public:
  LINK(ObDbConfigLogicDb, db_link_);

public:
  ObDbConfigState dc_state_;
  bool is_strict_spec_mode_;
  bool need_update_bt_;
  obutils::ObProxyConfigString db_name_;
  obutils::ObProxyConfigString db_cluster_;
  obutils::ObProxyConfigString db_mode_;
  obutils::ObProxyConfigString db_type_;
  obutils::ObProxyConfigString testload_prefix_;

  ObDbConfigChildArrayInfo<ObDataBaseAuth> da_array_;
  ObDbConfigChildArrayInfo<ObDataBaseProp> dp_array_;
  ObDbConfigChildArrayInfo<ObDataBaseVar> dv_array_;
  ObDbConfigChildArrayInfo<ObShardTpo> st_array_;
  ObDbConfigChildArrayInfo<ObShardRouter> sr_array_;
  ObDbConfigChildArrayInfo<ObShardDist> sd_array_;
  ObDbConfigChildArrayInfo<ObShardConnector> sc_array_;
  ObDbConfigChildArrayInfo<ObShardProp> sp_array_;

  //testload table_map
  std::map<std::string, std::string> test_load_table_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigLogicDb);
};

class ObDbConfigLogicTenant : public ObDbConfigChild
{
public:
  ObDbConfigLogicTenant() : ObDbConfigChild(TYPE_TENANT), tenant_name_(), ld_map_() {}
  virtual ~ObDbConfigLogicTenant() {}

  virtual void destroy();

  virtual int get_file_name(char *buf, const int64_t buf_len) const;
  virtual int to_json_str(common::ObSqlString &buf) const;
  virtual int parse_from_json(const json::Value &json_value);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  ObDbConfigLogicDb *get_exist_db(const common::ObString &db_name);
  int acquire_random_logic_db(ObDbConfigLogicDb *&db_info);
  int acquire_random_logic_db_with_auth(const common::ObString &username,
                              const common::ObString &host,
                              ObShardUserPrivInfo &up_info,
                              ObDbConfigLogicDb *&db_info);

  void set_tenant_name(const common::ObString &tenant_name) { tenant_name_.set_value(tenant_name); }
  int load_local_db(const json::Value &json_value);

public:
  struct ObDbConfigLogicDbHashing
  {
    typedef const common::ObString &Key;
    typedef ObDbConfigLogicDb Value;
    typedef ObDLList(ObDbConfigLogicDb, db_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->db_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObDbConfigLogicDbHashing, ObDbConfigChild::DB_HASH_BUCKET_SIZE> LDHashMap;

public:
  LINK(ObDbConfigLogicTenant, tenant_link_);

public:
  obutils::ObProxyConfigString tenant_name_;
  LDHashMap ld_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigLogicTenant);
};

inline ObDbConfigLogicDb *ObDbConfigLogicTenant::get_exist_db(const common::ObString &db_name)
{
  ObDbConfigLogicDb *db_info = NULL;
  obutils::ObProxyConfigString db;
  db.set_value(db_name);
  if (common::OB_SUCCESS == ld_map_.get_refactored(db, db_info)) {
    if (NULL != db_info) {
      db_info->inc_ref();
    }
  }
  return db_info;
}

class ObDbConfigCache
{
public:
  ObDbConfigCache() : lt_map_(), rwlock_() {}
  ~ObDbConfigCache() {}

public:
  struct ObDbConfigLogicTenantHashing
  {
    typedef const common::ObString &Key;
    typedef ObDbConfigLogicTenant Value;
    typedef ObDLList(ObDbConfigLogicTenant, tenant_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->tenant_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObDbConfigLogicTenantHashing, ObDbConfigChild::DB_HASH_BUCKET_SIZE> LTHashMap;

  bool is_db_exist(const ObDataBaseKey &key) const;
  bool is_tenant_exist(const common::ObString &tenant_name) const;

  bool need_fetch_resource(const ObDataBaseKey &key, const std::string &reference, const ObDDSCrdType type, ObResourceFetchOp &op);

  ObDbConfigLogicTenant *get_exist_tenant(const common::ObString &tenant_name);
  ObDbConfigLogicDb *get_exist_db_info(const ObDataBaseKey &key);
  ObDbConfigLogicDb *get_exist_db_info(const common::ObString &tenant_name,
                                       const common::ObString &db_name);

  int get_all_logic_tenant(common::ObIArray<common::ObString> &all_tenant);
  ObDbConfigChild *get_child_info(const ObDataBaseKey &key, const common::ObString &name, const ObDDSCrdType type);
  ObDbConfigChild *get_avail_child_info(const ObDataBaseKey &key,
                                        const common::ObString &name, const ObDDSCrdType type);
  int get_shard_prop(const common::ObString &tenant_name,
                         const common::ObString &db_name,
                         const common::ObString &shard_name,
                         ObShardProp* &conn_prop);

  int load_local_dbconfig();
  int load_logic_tenant_config(const common::ObString &tenant_name);

  int handle_new_db_info(ObDbConfigLogicDb &db_info, bool is_from_local=false);


public:
  LTHashMap lt_map_;
  obsys::CRWLock rwlock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigCache);
};

inline bool ObDbConfigCache::is_db_exist(const ObDataBaseKey &key) const
{
  bool bret = false;
  ObDbConfigLogicTenant *tenant_info = NULL;
  ObDbConfigLogicDb *db_info = NULL;
  obsys::CRLockGuard guard(rwlock_);
  if (common::OB_SUCCESS == lt_map_.get_refactored(key.tenant_name_.config_string_, tenant_info)) {
    if (NULL != tenant_info
       && common::OB_SUCCESS == tenant_info->ld_map_.get_refactored(key.database_name_.config_string_, db_info)) {
      bret = true;
    }
  }
  return bret;
}

inline bool ObDbConfigCache::is_tenant_exist(const common::ObString &tenant_name) const
{
  ObDbConfigLogicTenant *tenant_info = NULL;
  obsys::CRLockGuard guard(rwlock_);
  return common::OB_SUCCESS == lt_map_.get_refactored(tenant_name, tenant_info);
}

inline ObDbConfigLogicTenant *ObDbConfigCache::get_exist_tenant(const common::ObString &tenant_name)
{
  ObDbConfigLogicTenant *tenant_info = NULL;
  obsys::CRLockGuard guard(rwlock_);
  if (common::OB_SUCCESS == lt_map_.get_refactored(tenant_name, tenant_info)) {
    if (NULL != tenant_info) {
      tenant_info->inc_ref();
    }
  }
  return tenant_info;
}

inline ObDbConfigLogicDb *ObDbConfigCache::get_exist_db_info(const ObDataBaseKey &key)
{
  ObDbConfigLogicDb *db_info = NULL;
  ObDbConfigLogicTenant *tenant_info = NULL;
  obsys::CRLockGuard guard(rwlock_);
  if (common::OB_SUCCESS == lt_map_.get_refactored(key.tenant_name_.config_string_, tenant_info)) {
    if (NULL != tenant_info
       && common::OB_SUCCESS == tenant_info->ld_map_.get_refactored(key.database_name_.config_string_, db_info)) {
      if (NULL != db_info) {
        db_info->inc_ref();
      }
    }
  }
  return db_info;
}

inline ObDbConfigLogicDb *ObDbConfigCache::get_exist_db_info(const common::ObString &tenant_name,
                                                             const common::ObString &db_name)
{
  ObDataBaseKey key;
  key.tenant_name_.set_value(tenant_name);
  key.database_name_.set_value(db_name);
  return get_exist_db_info(key);
}

inline ObDbConfigChild *ObDbConfigCache::get_avail_child_info(const ObDataBaseKey &key,
       const common::ObString &name, const ObDDSCrdType type)
{
  ObDbConfigChild *child_info = NULL;
  child_info = get_child_info(key, name, type);
  if (NULL != child_info && !child_info->is_avail()) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return child_info;
}


const char *get_dbconfig_state_str(ObDbConfigState state);
ObTestLoadType get_testload_type(int64_t type);
const char *get_testload_type_str(ObTestLoadType type);

inline bool testload_need_handle_special_char(ObTestLoadType type)
{
  return (TESTLOAD_ALIPAY == type) || (TESTLOAD_ALIPAY_COMPATIBLE == type);
}

ObConnectorEncType get_enc_type(int64_t type);
const char* get_enc_type_str(ObConnectorEncType type);
ObConnectorEncType get_enc_type(const char *enc_str, int64_t str_len);

ObDbConfigCache &get_global_dbconfig_cache();

template<typename T>
void ObDbConfigChildArrayInfo<T>::destroy()
{
  typename CCRHashMap::iterator end = ccr_map_.end();
  typename CCRHashMap::iterator tmp_it;
  for (typename CCRHashMap::iterator it = ccr_map_.begin(); it != end; ++it) {
    tmp_it = it;
    ++it;
    tmp_it->dec_ref();
  }
  ccr_map_.reset();
}

template<typename T>
int64_t ObDbConfigChildArrayInfo<T>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  CCRHashMap &map = const_cast<CCRHashMap &>(ccr_map_);
  for (typename CCRHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "child cr", *it);
  }
  J_OBJ_END();
  return pos;
}

} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_DB_CONFIG_INFO_H */
