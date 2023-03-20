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
#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>
#include <dirent.h>
#include "dbconfig/ob_proxy_db_config_info.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "utils/ob_proxy_utils.h"
#include "utils/ob_proxy_blowfish.h"
#include "lib/hash/ob_hashset.h"
#include "lib/number/ob_number_v2.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/container/ob_se_array_iterator.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "obutils/ob_proxy_sequence_utils.h"
#include "obutils/ob_proxy_stmt.h"
#include "proxy/shard/obproxy_shard_utils.h"

using namespace obsys;
using namespace oceanbase::json;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

static const ObString LOCAL_DIR            = ObString::make_string(".");
static const ObString PARENT_DIR           = ObString::make_string("..");

// ObDbConfigChild
static const char *CONFIG_VERSION          = "version";
static const char *CONFIG_VALUES           = "variables";

// LogicTenantInfo
static const char *TENANT_NAME             = "tenant_name";
static const char *TENANT_DATABASES        = "databases";

// CR Reference
static const char *REF_KIND                = "kind";
static const char *REF_NAMESPACE           = "namespace";
static const char *REF_PARENT              = "parent";
static const char *REF_VALUE               = "reference";
static const char *TESTLOAD_REF_VALUE      = "test_load_reference";

// DatabaseVariables
static const ObString DATABASE_VARS        = ObString::make_string(CONFIG_VALUES);
static const ObString REMOTE_ACCESS        = ObString::make_string("remoteAccess");
static const ObString SEQUENCE_MIN_VALUE   = ObString::make_string("sequenceInitMinValue");
static const ObString SEQUENCE_MAX_VALUE   = ObString::make_string("sequenceInitMaxValue");
static const ObString SEQUENCE_STEP        = ObString::make_string("sequenceInitStep");
static const ObString SEQUENCE_RETRY_COUNT = ObString::make_string("sequenceRetryCount");
static const ObString SEQUENCE_TABLE       = ObString::make_string("sequenceTable");
static const ObString SEQUENCE_ENABLE      = ObString::make_string("sequenceEnable");
static const ObString SEQUENCE_TNT_ID_COL  = ObString::make_string("sequenceTntId");
static const ObString TESTLOAD_TABLE_MAP   = ObString::make_string("testLoadTableMap");//testLoadTableMap


// LogicDbInfo
static const char *LOGIC_DB_NAME                    = "database_name";
static const char *LOGIC_DB_CLUSTER                 = "cluster_name";
static const char *LOGIC_DB_MODE                    = "database_mode";
static const char *LOGIC_DB_TYPE                    = "database_type";
static const char *TESTLOAD_PREFIX                  = "test_load_prefix";

// DatabaseAuth
static const char *AUTH_HAS_PRIV                    = "Y";
static const char *AUTH_NO_PRIV                     = "N";

static const char *AUTH_USERS                       = "users";
static const char *AUTH_ALTER_PRIV                  = "alter_priv";
static const char *AUTH_CREATE_PRIV                 = "create_priv";
static const char *AUTH_DELETE_PRIV                 = "delete_priv";
static const char *AUTH_DROP_PRIV                   = "drop_priv";
static const char *AUTH_INSERT_PRIV                 = "insert_priv";
static const char *AUTH_UPDATE_PRIV                 = "update_priv";
static const char *AUTH_SELECT_PRIV                 = "select_priv";
static const char *AUTH_INDEX_PRIV                  = "index_priv";
static const char *AUTH_HOST                        = "host";

// ObDataBaseProp
static const char *DB_PROP_NAME                     = "properties_name";
static const char *DB_PROP_RULE                     = "properties_rule";
static const char *DB_PROP_TYPE                     = "propsType";
static const char *TESTLOAD_DBKEY_TYPE              = "testLoadDbKeyType";
static const char *MIRROR_DBKEY_TYPE                = "mirrorDbKeyType";

static const char *DB_PROP_TESTLOAD                 = "testload";
static const char *DB_PROP_MIRROR                   = "mirror";
static const char *DB_PROP_SELFADJUST               = "selfadjust";
static const char *DB_PROP_WHITELIST                = "whitelist";

// ObShardTpo
static const char *SHARDS_TPO_NAME                  = "topology_name";
static const char *SHARDS_TPO_ARCH                  = "architecture";
static const char *SHARDS_TPO_GROUPS                = "specific_layer";
static const char *SHARDS_TPO_SPEC_MODE             = "specific_mode";
static const char *SHARDS_TPO_SPEC                  = "specification";
static const char *STRICT_SPEC_MODE                 = "strict";
static const char *NORMAL_SPEC_MODE                 = "normal";

// ObShardRule
static const char *RULE_TB_NAME                     = "mark";
static const char *ROUTER_SEQUENCE                  = "sequence";
static const char *RULE_ROUTER                      = "router";
static const char *RULE_RULES                       = "rules";
static const char *RULE_TB_NAME_PATTERN             = "tbNamePattern";
static const char *RULE_DB_NAME_PATTERN             = "dbNamePattern";
static const char *RULE_TB_SUFFIX_PADDING           = "tbSuffixPadding";
static const char *RULE_TB_RULES                    = "tbRules";
static const char *RULE_DB_RULES                    = "dbRules";
static const char *RULE_ES_RULES                    = "elasticRules";

// ObShardRouter
static const char *ROUTER_RULES                     = "routers";

// ObShardConnector
static const char *SHARDS_NAME                      = "shards_name";
static const char *SHARDS_TYPE                      = "shards_type";
static const char *SHARDS_AUTH                      = "shards_authority";
static const char *SHARDS_ENC_TYPE                  = "encType";
static const char *SHARDS_URL                       = "shards_connector";
static const char *SHARDS_USERNAME                  = "user";
static const char *SHARDS_PASSWORD                  = "password";

// ObShardProp
static const char *SHARDS_CONN_PROP                 = "connectionProperties";
static const char *SHARDS_CONN_TIMEOUT              = "connectTimeout";
static const char *SHARDS_SOCK_TIMEOUT              = "socketTimeout";
static const char *SHARDS_IDLE_TIMEOUT              = "idleTimeoutMinutes";
static const char *SHARDS_NEED_PREFILL              = "prefill";
static const char *SHARDS_BLOCKING_TIMEOUT          = "blockingTimeoutMillis";

static const char *SHARDS_READ_CONSISTENCY          = "obReadConsistency";
static const char* SHARDS_ZONE_PROPERTIES           = "zoneProperties";
static const char* SHARDS_ZONE_MIN_CONN_PROP        = "minConn";
static const char* SHARDS_ZONE_MAX_CONN_PROP        = "maxConn";
static const ObString SHARDS_ZONE_CURRENT_CONN_PROP = ObString::make_string("current");
static const ObString SHARDS_ZONE_OTHERS_CONN_PROP  = ObString::make_string("others");
// OBShardDist
static const char *SHARD_DISTS                      = "distributions";
static const char *SHARD_DISTS_DIST                 = "distribution";
static const char *SHARD_DISTS_MARK                 = "mark";

static const int BUCKET_SIZE = 8;


ObDbConfigCache &get_global_dbconfig_cache()
{
  static ObDbConfigCache dbconfig_cache_;
  return dbconfig_cache_;
}

//------ ObDataBaseKey------
int64_t ObDataBaseKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_name),
       K_(database_name));
  J_OBJ_END();
  return pos;
}

//------ ObDbConfigChild------
int ObDbConfigChild::alloc_child_info(
                     const ObDataBaseKey &db_info_key,
                     ObDDSCrdType type,
                     const common::ObString &name,
                     const common::ObString &version,
                     ObDbConfigChild *&child_info)
{
  int ret = OB_SUCCESS;
  switch(type) {
    case TYPE_TENANT:
      child_info = op_alloc(ObDbConfigLogicTenant);
      break;
    case TYPE_DATABASE:
      child_info = op_alloc(ObDbConfigLogicDb);
      break;
    case TYPE_DATABASE_AUTH:
      child_info = op_alloc(ObDataBaseAuth);
      break;
    case TYPE_DATABASE_VAR:
      child_info = op_alloc(ObDataBaseVar);
      break;
    case TYPE_DATABASE_PROP:
      child_info = op_alloc(ObDataBaseProp);
      break;
    case TYPE_SHARDS_TPO:
      child_info = op_alloc(ObShardTpo);
      break;
    case TYPE_SHARDS_ROUTER:
      child_info = op_alloc(ObShardRouter);
      break;
    case TYPE_SHARDS_DIST:
      child_info = op_alloc(ObShardDist);
      break;
    case TYPE_SHARDS_CONNECTOR:
      child_info = op_alloc(ObShardConnector);
      break;
    case TYPE_SHARDS_PROP:
      child_info = op_alloc(ObShardProp);
      break;
    default:
      break;
  }
  if (OB_ISNULL(child_info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObDbConfig", "type", get_type_task_name(type), K(name), K(ret));
  } else {
    child_info->inc_ref();
    child_info->db_info_key_.assign(db_info_key);
    child_info->name_.set_value(name);
    child_info->version_.set_value(version);
    child_info->set_building_state();
  }
  return ret;
}

int64_t ObDbConfigChild::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(name),
       K_(version),
       K_(db_info_key),
       "type", get_type_task_name(type_),
       "state", get_dbconfig_state_str(dc_state_));
  std::map<std::string, std::string> &tmp_map = const_cast<std::map<std::string, std::string> &>(kv_map_);
  for (std::map<std::string, std::string>::iterator iter = tmp_map.begin();
       iter != tmp_map.end(); ++iter) {
    J_KV(", var_name", iter->first.c_str(),
         "var_value", iter->second.c_str());
  }
  J_OBJ_END();
  return pos;
}

int ObDbConfigChild::assign(const ObDbConfigChild &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    type_ = other.type_;
    dc_state_ = other.dc_state_;
    name_.assign(other.name_);
    version_.assign(other.version_);
    db_info_key_.assign(other.db_info_key_);
    std::map<std::string, std::string> &other_kv_map = const_cast<std::map<std::string, std::string> &>(other.kv_map_);
    for (std::map<std::string, std::string>::iterator iter = other_kv_map.begin();
         iter != other_kv_map.end(); ++iter) {
      kv_map_.insert(std::pair<std::string, std::string>(iter->first, iter->second));
    }
    std::map<std::string, std::string> &other_k_obj_map = const_cast<std::map<std::string, std::string> &>(other.k_obj_map_);
    for (std::map<std::string, std::string>::iterator iter = other_k_obj_map.begin();
         iter != other_k_obj_map.end(); ++iter) {
      k_obj_map_.insert(std::pair<std::string, std::string>(iter->first, iter->second));
    }
  }
  return ret;
}

int ObDbConfigChild::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%s\", \"%s\":{",
                       CONFIG_VERSION, version_.ptr(),
                       CONFIG_VALUES);
  std::map<std::string, std::string> &tmp_map = const_cast<std::map<std::string, std::string> &>(kv_map_);
  int i = 0;
  for (std::map<std::string, std::string>::iterator iter = tmp_map.begin();
       OB_SUCC(ret) && iter != tmp_map.end(); ++iter) {
    if (OB_FAIL(buf.append_fmt("\"%s\":\"%s\"", iter->first.c_str(),
                               iter->second.c_str()))) {
      LOG_WARN("fail to append config", K(ret));
    } else if (i < tmp_map.size() - 1) {
      ret = buf.append(",");
    }
    ++i;
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("}}");
  }
  return ret;
}

int ObDbConfigChild::parse_from_json(const Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *config_value = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == CONFIG_VALUES) {
        config_value = it->value_;
      }
    }
  }
  if (NULL != config_value && JT_OBJECT == config_value->get_type()) {
    DLIST_FOREACH(it, config_value->get_object()) {
      std::string name_str(it->name_.ptr(), static_cast<size_t>(it->name_.length()));
      std::string value_str(it->value_->get_string().ptr(), static_cast<size_t>(it->value_->get_string().length()));
      kv_map_.insert(std::pair<std::string, std::string>(name_str, value_str));
    }
  }
  return ret;
}

int ObDbConfigChild::get_file_name(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t len = snprintf(buf, buf_len, "%s.%s.json", get_type_task_name(type_), name_.ptr());
  if (OB_UNLIKELY(len <=0) || OB_UNLIKELY(len > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get file name for child config", KPC(this), K(ret));
  }
  return ret;
}

// for all resource
// with db info key: meta = cluster.tenant.database.name.version
// with db info key for parent cr: meta = cluster.tenant.database.version
// non with db info key: meta = name.version
int ObDbConfigChild::init(const std::string &meta, const ObDataBaseKey &key, const bool with_db_info_key/*true*/)
{
  int ret = OB_SUCCESS;
  size_t tail_pos = std::string::npos;
  size_t head_pos = std::string::npos;
  if (OB_FAIL(db_info_key_.assign(key))) {
    LOG_WARN("fail to copy db info key", K(key));
  } else if (OB_UNLIKELY(std::string::npos == (tail_pos = meta.rfind(".")))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config", "meta", meta.c_str(), K(ret));
  } else {
    version_.set_value(meta.length() - tail_pos - 1, meta.c_str() + tail_pos + 1);
    if (with_db_info_key) {
      if (OB_UNLIKELY(std::string::npos == (head_pos = meta.find(".")))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid config", "meta", meta.c_str(), K(ret));
      } else if (OB_UNLIKELY(std::string::npos == (head_pos = meta.find(".", head_pos + 1)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid config", "meta", meta.c_str(), K(ret));
      } else if (TYPE_DATABASE != type_ && OB_UNLIKELY(std::string::npos == (head_pos = meta.find(".", head_pos + 1)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid config", "meta", meta.c_str(), K(ret));
      } else if (OB_UNLIKELY(head_pos >= tail_pos)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid config", "meta", meta.c_str(), K(ret));
      } else {
        name_.set_value(tail_pos - head_pos - 1, meta.c_str() + head_pos + 1);
      }
    } else {
      name_.set_value(tail_pos, meta.c_str());
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to init resource meta", K_(name), K_(version), K_(db_info_key));
  }
  return ret;
}

//------ ObShardUserPrivInfo--------
int ObShardUserPrivInfo::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\",\
                               \"%s\":\"%s\",  \"%s\":\"%s\", \"%s\":\"%s\",\
                               \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\",\
                               \"%s\":\"%s\", \"%s\":\"%s\"}",
                             SHARDS_USERNAME, username_.ptr(),
                             AUTH_HOST, host_.ptr(), SHARDS_PASSWORD, password_.ptr(),
                             AUTH_ALTER_PRIV, has_alter_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_CREATE_PRIV, has_create_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_DELETE_PRIV, has_delete_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_DROP_PRIV, has_drop_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_INSERT_PRIV, has_insert_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_UPDATE_PRIV, has_update_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_SELECT_PRIV, has_select_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV,
                             AUTH_INDEX_PRIV, has_index_priv() ? AUTH_HAS_PRIV : AUTH_NO_PRIV))) {
    LOG_WARN("fail to append json str for ObShardUserPrivInfo", KPC(this), K(ret));
  }
  return ret;
}

int ObShardUserPrivInfo::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == SHARDS_USERNAME) {
        username_.set_value(it->value_->get_string());
      } else if (it->name_ == AUTH_HOST) {
        host_.set_value(it->value_->get_string());
      } else if (it->name_ == SHARDS_PASSWORD) {
        ret = set_password(it->value_->get_string().ptr(), it->value_->get_string().length());
      } else if (it->name_ == AUTH_ALTER_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_ALTER_SHIFT);
      } else if (it->name_ == AUTH_CREATE_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_CREATE_SHIFT);
      } else if (it->name_ == AUTH_DELETE_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_DELETE_SHIFT);
      } else if (it->name_ == AUTH_DROP_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_DROP_SHIFT);
      } else if (it->name_ == AUTH_INSERT_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_INSERT_SHIFT);
      } else if (it->name_ == AUTH_UPDATE_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_UPDATE_SHIFT);
      } else if (it->name_ == AUTH_SELECT_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_SELECT_SHIFT);
      } else if (it->name_ == AUTH_INDEX_PRIV) {
        set_user_priv(it->value_->get_string().ptr(), it->value_->get_string().length(), ::OB_PRIV_INDEX_SHIFT);
      }
    }
  } // end JT_OBJECT
  return ret;
}

int ObShardUserPrivInfo::set_password(const char *password, int64_t len)
{
  int ret = OB_SUCCESS;
  char pwd_buf[OB_MAX_PASSWORD_LENGTH];
  memset(pwd_buf, 0, sizeof(pwd_buf));
  if (OB_FAIL(ObBlowFish::decode(password, len, pwd_buf, OB_MAX_PASSWORD_LENGTH))) {
    LOG_WARN("fail to decode encrypted password", K(password), K(len), K(ret));
  } else {
    ObString password_str(static_cast<int32_t>(strlen(pwd_buf)), pwd_buf);
    char stage2_buf[SCRAMBLE_LENGTH * 2 + 1];
    char stage2_hex_buf[SCRAMBLE_LENGTH];
    ObString pwd_stage2;
    ObString pwd_stage2_hex;
    pwd_stage2.assign_ptr(stage2_buf, SCRAMBLE_LENGTH * 2 + 1);
    pwd_stage2_hex.assign_ptr(stage2_hex_buf, SCRAMBLE_LENGTH);
    if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(password_str, pwd_stage2))) {
      LOG_WARN("fail to encrypt passwd to stage2", K(ret));
    } else {
      ++pwd_stage2; // trim the first const character '*'
      ObEncryptedHelper::displayable_to_hex(pwd_stage2, pwd_stage2_hex);
      password_stage2_.set_value(pwd_stage2_hex);
      password_.set_value(len, password);
    }
  }
  return ret;
}

void ObShardUserPrivInfo::set_user_priv(const char *priv_str, int64_t len, ::OB_PRIV_SHIFT type)
{
  ObString priv_value(len, priv_str);
  bool has_priv = priv_value.length() > 0
                  && priv_value.case_compare(AUTH_HAS_PRIV) == 0;
  if (has_priv) {
    switch(type) {
      case ::OB_PRIV_ALTER_SHIFT:
        priv_set_ |= OB_PRIV_ALTER;
        break;
      case ::OB_PRIV_CREATE_SHIFT:
        priv_set_ |= OB_PRIV_CREATE;
        break;
      case ::OB_PRIV_DELETE_SHIFT:
        priv_set_ |= OB_PRIV_DELETE;
        break;
      case ::OB_PRIV_DROP_SHIFT:
        priv_set_ |= OB_PRIV_DROP;
        break;
      case ::OB_PRIV_INSERT_SHIFT:
        priv_set_ |= OB_PRIV_INSERT;
        break;
      case ::OB_PRIV_UPDATE_SHIFT:
        priv_set_ |= OB_PRIV_UPDATE;
        break;
      case ::OB_PRIV_SELECT_SHIFT:
        priv_set_ |= OB_PRIV_SELECT;
        break;
      case ::OB_PRIV_INDEX_SHIFT:
        priv_set_ |= OB_PRIV_INDEX;
        break;
      default:
        LOG_INFO("unknown priv type", K(type));
        break;
    }
  }
}

ObString ObDbConfigChild::get_variable_value(const ObString &key_str) const
{
  ObString value;
  std::string key(key_str.ptr(), static_cast<size_t>(key_str.length()));
  std::map<std::string, std::string>::const_iterator iter = kv_map_.find(key);
  if (iter != kv_map_.end()) {
    value.assign_ptr(iter->second.c_str(), static_cast<int32_t>(iter->second.length()));
  }
  return value;
}

//------ ObDataBaseAuth------
void ObDataBaseAuth::destroy()
{
  dc_state_ = DC_DEAD;
  LOG_INFO("ObDataBaseAuth will be destroyed", KP(this));
  up_array_.reset();
  op_free(this);
}

int64_t ObDataBaseAuth::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos= 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  J_KV("user priv map count", up_array_.count());
  for (int64_t i = 0; i < up_array_.count(); ++i) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "user_priv_info", up_array_.at(i));
  }
  J_OBJ_END();
  return pos;
}

int ObDataBaseAuth::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObDataBaseAuth &other = static_cast<const ObDataBaseAuth &>(child_info);
    ObShardUserPrivInfo up_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.up_array_.count(); ++i) {
      up_info.reset();
      up_info.assign(other.up_array_.at(i));
      if (OB_FAIL(up_array_.push_back(up_info))) {
        LOG_WARN("fail to put ObShardUserPrivInfo", K(up_info), K(ret));
      }
    }
  }
  return ret;
}

int ObDataBaseAuth::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":[",
              CONFIG_VERSION, version_.ptr(), AUTH_USERS))) {
    LOG_WARN("fail to append database auth to json", KPC(this), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < up_array_.count(); ++i) {
    const ObShardUserPrivInfo &up_info = up_array_.at(i);
    if (OB_FAIL(up_info.to_json_str(buf))) {
      LOG_WARN("fail to get json str for shard user auth info", K(up_info), K(ret));
    } else if (i < up_array_.count() - 1) {
      ret = buf.append(",");
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("]}");
  }
  return ret;
}

int ObDataBaseAuth::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *db_users = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == AUTH_USERS) {
        db_users = it->value_;
      }
    }
  } // end JT_OBJECT
  if (OB_SUCC(ret) && NULL != db_users && JT_ARRAY == db_users->get_type()) {
    ObShardUserPrivInfo up_info;
    DLIST_FOREACH(it, db_users->get_array()) {
      up_info.reset();
      if (OB_FAIL(up_info.parse_from_json(*it))) {
        LOG_WARN("fail to parse ObShardUserPrivInfo", K(up_info), K(ret));
      } else if (OB_FAIL(up_array_.push_back(up_info))) {
        LOG_WARN("fail to put ObShardUserPrivInfo", K(up_info), K(ret));
      }
    } // end loop db_users
  } // end JT_ARRAY
  return ret;
}

int ObDataBaseAuth::get_user_priv_info(const ObString &username,
                    const ObString &host, ObShardUserPrivInfo &up_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  up_info.reset();
  for (int64_t i = 0; !found && i < up_array_.count(); ++i) {
    const ObShardUserPrivInfo &tmp_up_info = up_array_.at(i);
    if (username.case_compare(tmp_up_info.username_.config_string_) == 0
        && ObProxyPbUtils::match_like(host, tmp_up_info.host_.config_string_)) {
      found = true;
      up_info.assign(tmp_up_info);
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    LOG_DEBUG("succ to get user priv info", K(up_info));
  }
  return ret;
}

//------ ObDataBaseProp------
int64_t ObDataBaseProp::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(prop_name), K_(prop_rule), K_(prop_type));
  if (PROP_TYPE_SELFADJUST == prop_type_) {
    RuleMap &tmp_map = const_cast<RuleMap &>(rule_map_);
    for (RuleMap::iterator iter = tmp_map.begin();
         iter != tmp_map.end(); ++iter) {
      J_KV(", disaster_status", iter->first.c_str(),
           ", elastic_id", iter->second);
    }
  }
  J_OBJ_END();
  return pos;
}

int ObDataBaseProp::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObDataBaseProp &other = static_cast<const ObDataBaseProp &>(child_info);
    prop_type_ = other.prop_type_;
    prop_name_.assign(other.prop_name_);
    prop_rule_.assign(other.prop_rule_);
    RuleMap &tmp_map = const_cast<RuleMap &>(other.rule_map_);
    for (RuleMap::iterator iter = tmp_map.begin();
         iter != tmp_map.end(); ++iter) {
      rule_map_.insert(std::pair<std::string, int64_t>(iter->first, iter->second));
    }
  }
  return ret;
}

int ObDataBaseProp::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":%s}",
                             CONFIG_VERSION, version_.ptr(),
                             DB_PROP_NAME, prop_name_.ptr(),
                             DB_PROP_RULE, prop_rule_.ptr()))) {
    LOG_WARN("fail to append json str for ObDataBaseProp", KPC(this), K(ret));
  }
  return ret;
}

int ObDataBaseProp::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *prop_value = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == DB_PROP_NAME) {
        prop_name_.set_value(it->value_->get_string());
      } else if (it->name_ == DB_PROP_RULE) {
        prop_value = it->value_;
        // parse from local config file, no need to set prop_rule_ string
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(prop_name_.empty()) || OB_ISNULL(prop_value)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid database prop config", K(prop_name_), K(ret));
    } else if (OB_FAIL(parse_db_prop_rule(*prop_value))) {
      LOG_WARN("fail to parse database prop value", K(prop_name_), K(ret));
    }
  }
  return ret;
}

int ObDataBaseProp::parse_db_prop_rule(Value &prop_value)
{
  int ret = OB_SUCCESS;
  Value *rule_value = NULL;
  if (JT_OBJECT == prop_value.get_type()) {
    DLIST_FOREACH(it, prop_value.get_object()) {
      if (it->name_ == DB_PROP_TYPE) {
        ret = set_db_prop_type(it->value_->get_string());
      } else if (it->name_ == RULE_RULES) {
        rule_value = it->value_;
      } else if (it->name_ == TESTLOAD_DBKEY_TYPE || it->name_ == MIRROR_DBKEY_TYPE) {
        // for testload, it does not matter which type it is
        // for mirror, we do dot support yet
      }
    }
  }
  if (OB_SUCC(ret) && NULL != rule_value && PROP_TYPE_INVALID != prop_type_) {
    if (PROP_TYPE_SELFADJUST == prop_type_) {
      if (JT_OBJECT == rule_value->get_type()) {
        DLIST_FOREACH(it, rule_value->get_object()) {
          int64_t eid = OBPROXY_MAX_DBMESH_ID;
          if (OB_FAIL(get_int_value(it->name_, eid))) {
            LOG_WARN("fail to get int value for eid value", K(it->name_), K(ret));
          } else {
            std::string status(it->value_->get_string().ptr(), static_cast<size_t>(it->value_->get_string().length()));
            rule_map_.insert(std::pair<std::string, int64_t>(status, eid));
          }
        }
      }
    }
  }
  return ret;
}

int ObDataBaseProp::set_db_prop_type(const ObString &prop_type)
{
  int ret = OB_SUCCESS;
  if (prop_type.case_compare(DB_PROP_TESTLOAD) == 0) {
    prop_type_ = PROP_TYPE_TESTLOAD;
  } else if (prop_type.case_compare(DB_PROP_MIRROR) == 0) {
    prop_type_ = PROP_TYPE_MIRROR;
  } else if (prop_type.case_compare(DB_PROP_SELFADJUST) == 0) {
    prop_type_ = PROP_TYPE_SELFADJUST;
  } else if (prop_type.case_compare(DB_PROP_WHITELIST) == 0) {
    prop_type_ = PROP_TYPE_WHITELIST;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid database prop type", K(prop_type), K(ret));
  }
  return ret;
}

int ObDataBaseProp::get_disaster_eid(const ObString &disaster_status, int64_t &eid)
{
  int ret = OB_SUCCESS;
  std::string disaster_status_str(disaster_status.ptr(), static_cast<size_t>(disaster_status.length()));
  RuleMap::iterator iter = rule_map_.find(disaster_status_str);
  if (OB_UNLIKELY(iter == rule_map_.end())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("disaster status does not exist", "disaster_status", disaster_status_str.c_str(), KPC(this), K(ret));
  } else {
    eid = iter->second;
    LOG_DEBUG("succ to find disaster eid", K(disaster_status), K(eid));
  }
  return ret;
}

int ObDbConfigLogicDb::get_database_prop(const ObString &prop_name, ObDataBaseProp *&db_prop)
{
  int ret = OB_SUCCESS;
  db_prop = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  if (OB_FAIL(dp_array_.ccr_map_.get_refactored(prop_name, db_prop))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_ISNULL(db_prop) || !db_prop->is_avail()) {
    LOG_INFO("database prop is not avail", KPC(db_prop));
    db_prop = NULL;
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    db_prop->inc_ref();
  }
  return ret;
}

//------ ObDataBaseVar------
int64_t ObDataBaseVar::to_string(char *buf, const int64_t buf_len) const
{
  return ObDbConfigChild::to_string(buf, buf_len);
}

bool ObDataBaseVar::enable_remote_connection() const
{
  bool bret = true;
  ObString value = get_variable_value(REMOTE_ACCESS);
  if (!value.empty()) {
    bret = 0 == value.case_compare("true");
  }
  return bret;
}

int ObDataBaseVar::init_test_load_table_map(std::map<std::string, std::string> &test_load_table_map)
{
  int ret = OB_SUCCESS;

  ObString value = get_variable_value(TESTLOAD_TABLE_MAP);
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  if (!value.empty()) {
    json::Parser parser;
    json::Value *json_root = NULL;
    json::Value * config_value = NULL;
    std::string inner_key;
    std::string inner_val;

    for (int i=0; i < value.length(); i++) {
      if (value[i] == '\'') {
        value.ptr()[i] = '\"';
      }
    }

    /* init testloadTableMap to JSON parser, failed when error format */
    if (OB_FAIL(parser.init(&allocator))) {
      LOG_WARN("json parser init failed", K(ret));
    } else if (OB_FAIL(parser.parse(value.ptr(), value.length(), json_root))) {
      ret = OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP;
      LOG_WARN("parse json failed", K(ret), K(value));
    } else if (NULL == json_root) {
      ret = OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP;
      LOG_WARN("no root value", K(ret), K(value));
    } else {
      char buf[OB_MAX_TABLE_NAME_LENGTH];
      test_load_table_map.clear();
      DLIST_FOREACH(it1, json_root->get_object()) {
        config_value = it1->value_;
        if (OB_MAX_TABLE_NAME_LENGTH <= it1->name_.length()) {
          ret = OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP;
          LOG_WARN("test load map error for size", K(OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP), K(it1->name_.length()));
        } else {
          //change TABLE_NAME to upper
          memcpy(buf, it1->name_.ptr(), it1->name_.length());
          string_to_upper_case(buf, it1->name_.length());
          inner_key = std::string(buf, it1->name_.length());
          if (NULL != config_value && JT_STRING == config_value->get_type()) {
            inner_val = std::string(config_value->get_string().ptr(), config_value->get_string().length());
            test_load_table_map.insert(std::pair<std::string, std::string>(inner_key, inner_val));
            LOG_DEBUG("get value ", K(it1->name_), K(config_value->get_string()));
          } else {
            ret = OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP;
            LOG_WARN("init test load map unexpected", K(ret), K(config_value));
          }
        }
      }
    }
  } else {
    LOG_DEBUG("testloadTableMap is null, not have any testload table configed.");
  }

  return ret;
}


int ObDbConfigLogicDb::get_database_var(ObDataBaseVar *&db_var)
{
  int ret = OB_SUCCESS;
  db_var = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  if (OB_FAIL(dv_array_.ccr_map_.get_refactored(DATABASE_VARS, db_var))) {
    LOG_DEBUG("database variables does not exist", K(ret));
  } else if (OB_ISNULL(db_var) || !db_var->is_avail()) {
    db_var = NULL;
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    db_var->inc_ref();
  }
  return ret;
}

//------ ObElasticInfo------
int64_t ObElasticInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(eid), K_(shard_name), K_(read_weight), K_(write_weight));
  J_OBJ_END();
  return pos;
}

int ObElasticInfo::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("%ld:%s:r%ldw%ld",
                             eid_, shard_name_.ptr(), read_weight_, write_weight_))) {
    LOG_WARN("fail to append json str for ObElasticInfo", K(ret), KPC(this));
  }
  return ret;
}

//------ ObGroupCluster------
int64_t ObGroupCluster::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(gc_name), K_(total_read_weight), "es_array_count", es_array_.count());
  for (int64_t i = 0; i < es_array_.count(); ++i) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "group es info", es_array_.at(i));
  }
  J_OBJ_END();
  return pos;
}

int ObGroupCluster::assign(const ObGroupCluster &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    gc_name_.assign(other.gc_name_);
    total_read_weight_ = other.total_read_weight_;
    total_write_weight_ = other.total_write_weight_;
    ObElasticInfo es_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.es_array_.count(); ++i) {
      es_info.assign(other.es_array_.at(i));
      if (OB_FAIL(es_array_.push_back(es_info))) {
        LOG_WARN("fail to push back es_info", K(es_info), K(ret));
      }
    }
  }
  return ret;
}

const ObElasticInfo *ObGroupCluster::get_random_es_info() const
{
  const ObElasticInfo *es_info = NULL;
  bool found = false;
  for (int64_t i = 0; !found && i < es_array_.count(); ++i) {
    const ObElasticInfo &tmp_es_info = es_array_.at(i);
    if (10 == tmp_es_info.read_weight_ && 10 == tmp_es_info.write_weight_) {
      found = true;
      es_info = &tmp_es_info;
    }
  }
  return es_info;
}

int ObGroupCluster::get_random_elastic_id(int64_t &eid) const
{
  int ret = OB_SUCCESS;
  eid = OBPROXY_MAX_DBMESH_ID;
  const ObElasticInfo *es_info = NULL;
  if (OB_ISNULL(es_info = get_random_es_info())) {
    LOG_WARN("fail to get random es info", KPC(this), K(ret));
  } else {
    eid = es_info->eid_;
  }
  return ret;
}

int ObGroupCluster::get_elastic_id_by_read_weight(int64_t &eid) const
{
  int ret = OB_SUCCESS;
  eid = OBPROXY_MAX_DBMESH_ID;
  bool found = false;
  int64_t cur_num = ObRandom::rand(0, total_read_weight_);
  int64_t sum_num = 0;
  for (int64_t i = 0; !found && i < es_array_.count(); ++i) {
    const ObElasticInfo &tmp_es_info = es_array_.at(i);
    if (tmp_es_info.read_weight_ <= 0) {
      continue;
    } else if (sum_num + tmp_es_info.read_weight_ >= cur_num) {
      found = true;
      eid = tmp_es_info.eid_;
    } else {
      sum_num += tmp_es_info.read_weight_;
    }
  }
  LOG_DEBUG("get_elastic_id_by_read_weight", K(total_read_weight_), K(cur_num), K(eid));
  return ret;
}

int ObGroupCluster::get_elastic_id_by_write_weight(int64_t &eid) const
{
  int ret = OB_SUCCESS;
  eid = OBPROXY_MAX_DBMESH_ID;
  bool found = false;
  int64_t cur_num = ObRandom::rand(0, total_write_weight_);
  int64_t sum_num = 0;
  for (int64_t i = 0; !found && i < es_array_.count(); ++i) {
    const ObElasticInfo &tmp_es_info = es_array_.at(i);
    if (tmp_es_info.write_weight_ <= 0) {
      continue;
    } else if (sum_num + tmp_es_info.write_weight_ >= cur_num) {
      found = true;
      eid = tmp_es_info.eid_;
    } else {
      sum_num += tmp_es_info.write_weight_;
    }
  }
  LOG_DEBUG("get_elastic_id_by_write_weight", K(total_write_weight_), K(cur_num), K(eid));
  return ret;
}

int ObGroupCluster::get_elastic_id_by_weight(int64_t &eid, const bool is_read_stmt) const
{
  if (is_read_stmt) {
    return get_elastic_id_by_read_weight(eid);
  } else {
    return get_elastic_id_by_write_weight(eid);
  }
}

int64_t ObGroupCluster::get_es_size() const
{
  int64_t max_eid = 0;
  for (int64_t i = 0; i < es_array_.count(); ++i) {
    const ObElasticInfo &es_info = es_array_.at(i);
    if (es_info.eid_ > max_eid) {
      max_eid = es_info.eid_;
    }
  }
  return max_eid + 1;
}

ObString ObGroupCluster::get_shard_name_by_eid(const int64_t eid) const
{
  ObString shard_name;
  for (int64_t i = 0; i < es_array_.count(); ++i) {
    const ObElasticInfo &es_info = es_array_.at(i);
    if (eid == es_info.eid_) {
      shard_name = es_info.shard_name_.config_string_;
      break;
    }
  }
  return shard_name;
}

ObString ObGroupCluster::get_shard_name_by_weight() const
{
  ObString shard_name;
  int64_t index = -1;
  for (int64_t i = 0; index < 0 && i < es_array_.count(); ++i) {
    const ObElasticInfo &es_info = es_array_.at(i);
    if (10 == es_info.read_weight_ && 10 == es_info.write_weight_) {
      index = i;
    }
  }
  if (index < 0 && es_array_.count() > 0) {
    index = 0;
  }
  if (index >=0) {
    shard_name = es_array_[index].shard_name_.config_string_;
  }
  return shard_name;
}

int ObGroupCluster::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("\"%s\":\"", gc_name_.ptr()))) {
    LOG_WARN("fail to append GroupCluster", K(ret), KPC(this));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < es_array_.count(); ++i) {
    const ObElasticInfo &es_info = es_array_.at(i);
    if (OB_FAIL(es_info.to_json_str(buf))) {
      LOG_WARN("fail to get json str for ObElasticInfo", K(ret), K(es_info));
    } else if (i < es_array_.count() - 1) {
      ret = buf.append(",");
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("\"");
  }
  return ret;
}

//------ ObShardTpo------
void ObShardTpo::destroy()
{
  dc_state_ = DC_DEAD;
  LOG_INFO("shard topology will be destroyed", KPC(this));
  GCHashMap::iterator end = gc_map_.end();
  GCHashMap::iterator tmp_iter;
  for (GCHashMap::iterator it = gc_map_.begin(); it != end;) {
    tmp_iter = it;
    ++it;
    tmp_iter->destroy();
  }
  gc_map_.reset();
  op_free(this);
}

int64_t ObShardTpo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  J_KV(K_(tpo_name), K_(arch), K_(specification), K_(is_zone_shard_tpo));
  GCHashMap &map = const_cast<GCHashMap &>(gc_map_);
  J_KV("gc map count", map.count());
  for (GCHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "group cluster", *it);
  }
  J_OBJ_END();
  return pos;
}

int ObShardTpo::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObShardTpo &other = static_cast<const ObShardTpo &>(child_info);
    is_zone_shard_tpo_ = other.is_zone_shard_tpo_;
    tpo_name_.assign(other.tpo_name_);
    arch_.assign(other.arch_);
    specification_.assign(other.specification_);
    GCHashMap &map = const_cast<GCHashMap &>(other.gc_map_);
    ObGroupCluster *gc_info = NULL;
    for (GCHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
      if (OB_ISNULL(gc_info = op_alloc(ObGroupCluster))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObGroupCluster", K(ret), K_(db_info_key));
      } else if (OB_FAIL(gc_info->assign(*it))) {
        LOG_WARN("fail to copy group cluster info", K(ret), K_(db_info_key));
      } else if (OB_FAIL(gc_map_.unique_set(gc_info))) {
        LOG_WARN("fail to unique_set gc_info", KPC(gc_info), K(ret));
      }
      if (OB_FAIL(ret) && NULL != gc_info) {
        op_free(gc_info);
        gc_info = NULL;
      }
    }
  }
  return ret;
}

int ObShardTpo::get_group_cluster(const ObString &gc_name, ObGroupCluster *&gc_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gc_map_.get_refactored(gc_name, gc_info))) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObShardTpo::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\",\
                               \"%s\":\"%s\", \"%s\":{",
              SHARDS_TPO_NAME, tpo_name_.ptr(),
              CONFIG_VERSION, version_.ptr(),
              SHARDS_TPO_ARCH, arch_.ptr(),
              SHARDS_TPO_SPEC, specification_.ptr(),
              SHARDS_TPO_GROUPS))) {
    LOG_WARN("fail append shard connector to json str", K(ret), KPC(this));
  } else {
    GCHashMap &map = const_cast<GCHashMap &>(gc_map_);
    int64_t i = 0;
    for (GCHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
      if (OB_FAIL(it->to_json_str(buf))) {
        LOG_WARN("fail to get json str for ObGroupCluster", K(ret), K(*it));
      } else if (i < map.count() - 1) {
        ret = buf.append(",");
      }
      ++i;
    } // end for gc map
    if (OB_SUCC(ret)) {
      ret = buf.append("}}");
    }
  }
  return ret;
}

int ObShardTpo::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *cluster_config = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == SHARDS_TPO_NAME) {
        tpo_name_.set_value(it->value_->get_string());
      } else if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == SHARDS_TPO_ARCH) {
        arch_.set_value(it->value_->get_string());
      } else if (it->name_ == SHARDS_TPO_GROUPS) {
        cluster_config = it->value_;
      } else if (it->name_ == SHARDS_TPO_SPEC) {
        specification_.set_value(it->value_->get_string());
        parse_tpo_specification(it->value_->get_string());
      }
    }
  }
  if (NULL != cluster_config && JT_OBJECT == cluster_config->get_type()) {
    ObGroupCluster *gc_info = NULL;
    std::string group_name;
    std::string group_value;
    DLIST_FOREACH(it, cluster_config->get_object()) {
      if (OB_ISNULL(gc_info = op_alloc(ObGroupCluster))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObGroupCluster", K(ret));
      } else {
        group_name.assign(it->name_.ptr(), static_cast<size_t>(it->name_.length()));
        group_value.assign(it->value_->get_string().ptr(), static_cast<size_t>(it->value_->get_string().length()));
        if (OB_FAIL(ObProxyPbUtils::parse_group_cluster(group_name, group_value, *gc_info))) {
          LOG_WARN("fail to parse group cluster", "group_name", group_name.c_str(),
                   "group_value", group_value.c_str(), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        gc_info->calc_total_read_weight();
        gc_map_.unique_set(gc_info);
      }
      if (OB_FAIL(ret) && NULL != gc_info) {
        op_free(gc_info);
        gc_info = NULL;
      }
    } // end loop
  }
  return ret;
}

void ObShardTpo::parse_tpo_specification(const ObString &specification)
{
  const char *zone = get_global_proxy_config().server_zone;
  if (NULL != zone && STRLEN(zone) > 0) {
    ObString tmp_spec = specification;
    ObString zone_name;
    const char *sep_pos = NULL;
    bool found = false;
    while (!found && NULL != (sep_pos = tmp_spec.find(','))) {
      zone_name = tmp_spec.split_on(sep_pos);
      zone_name.trim();
      if (zone_name.case_compare(zone) == 0) {
        found = true;
      }
    }
    if (!found) {
      zone_name = tmp_spec;
      zone_name.trim();
      if (zone_name.case_compare(zone) == 0) {
        found = true;
      }
    }
    if (found) {
      is_zone_shard_tpo_ = true;
    }
  }
}

int ObDbConfigLogicDb::get_shard_rule(ObShardRule *&shard_rule, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObShardRouter *shard_router = NULL;
  shard_rule = NULL;

  // table_name_len must be less than OB_MAX_TABLE_NAME_LENGTH
  char table_name_str[OB_MAX_TABLE_NAME_LENGTH];
  memcpy(table_name_str, table_name.ptr(), table_name.length());
  table_name_str[table_name.length()] = '\0';
  string_to_upper_case(table_name_str, table_name.length());
  ObString upper_table_name(table_name.length(), table_name_str);

  if (OB_FAIL(get_shard_router(upper_table_name, shard_router))) {
    LOG_WARN("fail to get shard router", K_(sr_array), K(upper_table_name), K(ret));
  } else if (OB_FAIL(shard_router->get_shard_rule(upper_table_name, shard_rule))) {
    LOG_WARN("fail to get logic tb info", KPC(shard_router), K(upper_table_name), K(ret));
  } else if (OB_ISNULL(shard_rule)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logic tb info is null", K(ret));
  }

  if (NULL != shard_router) {
    shard_router->dec_ref();
    shard_router = NULL;
  }

  return ret;
}

int ObDbConfigLogicDb::get_shard_tpo(ObShardTpo *&shard_tpo)
{
  int ret = OB_SUCCESS;
  bool use_zone_tpo = true;
  const char *zone = get_global_proxy_config().server_zone;
  bool is_server_zone_exist = OB_NOT_NULL(zone) && STRLEN(zone) > 0;
  int64_t shard_tpo_count = get_shard_tpo_count();
  if (is_server_zone_exist) {
    ret = get_shard_tpo_by_zone(shard_tpo, use_zone_tpo);
  }
  if (!is_server_zone_exist || OB_FAIL(ret)) {
    if (is_strict_spec_mode_ && shard_tpo_count > 1) {
      ret = OB_ERR_NO_ZONE_SHARD_TPO;
      LOG_WARN("strict mode and has zone shard tpo, but local zone shard tpo does not exist",
               "local_zone", zone, K_(db_info_key), K(shard_tpo_count), K_(is_strict_spec_mode), K(ret));
    } else {
      // three case to use default zone:
      // 1. do not set server zone
      // 2. non-strict mode and do not have same zone config
      // 3. stric mode, only have default zone config. this case equals case 2
      use_zone_tpo = false;
      if (OB_FAIL(get_shard_tpo_by_zone(shard_tpo, use_zone_tpo))) {
        ret = OB_ERR_NO_DEFAULT_SHARD_TPO;
        LOG_WARN("fail to get default shard tpo", K(ret));
      }
    }
  }
  return ret;
}

inline int64_t ObDbConfigLogicDb::get_shard_tpo_count() const
{
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  return st_array_.ccr_map_.count();
}

int ObDbConfigLogicDb::get_shard_tpo_by_zone(ObShardTpo *&shard_tpo, bool use_zone_tpo)
{
  int ret = OB_SUCCESS;
  shard_tpo = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  ObDbConfigChildArrayInfo<ObShardTpo>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObShardTpo>::CCRHashMap &>(st_array_.ccr_map_);
  bool found = false;
  for (ObDbConfigChildArrayInfo<ObShardTpo>::CCRHashMap::iterator it = map.begin(); !found && it != map.end(); ++it) {
    if (use_zone_tpo) {
      if (it->is_zone_shard_tpo_) {
        shard_tpo = &(*it);
        found = true;
      }
    } else if (it->specification_.config_string_.case_compare("default") == 0) {
      shard_tpo = &(*it);
      found = true;
    }
  }
  if (!found) {
    LOG_DEBUG("no zone shard topology", K(use_zone_tpo), KPC(this));
    shard_tpo = NULL;
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_ISNULL(shard_tpo) || !shard_tpo->is_avail()) {
    LOG_DEBUG("shard tpo is not avail", KPC(shard_tpo));
    shard_tpo = NULL;
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    shard_tpo->inc_ref();
  }
  return ret;
}

//------ ObShardRule------
int64_t ObShardRule::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_sequence), K_(tb_size), K_(db_size), K_(table_name),
       K_(tb_prefix), K_(db_prefix), K_(tb_suffix), K(tb_tail_), K(db_tail_),
       K_(tb_name_pattern), K_(db_name_pattern),
       K_(tb_rules), K_(db_rules), K_(es_rules));
  J_OBJ_END();
  return pos;
}

int ObShardRule::assign(const ObShardRule &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    is_sequence_ = other.is_sequence_;
    tb_size_ = other.tb_size_;
    db_size_ = other.db_size_;
    tb_suffix_len_ = other.tb_suffix_len_;
    db_suffix_len_ = other.db_suffix_len_;
    table_name_.assign(other.table_name_);
    tb_name_pattern_.assign(other.tb_name_pattern_);
    db_name_pattern_.assign(other.db_name_pattern_);
    tb_prefix_.assign(other.tb_prefix_);
    db_prefix_.assign(other.db_prefix_);
    tb_suffix_.assign(other.tb_suffix_);
    tb_tail_.assign(other.tb_tail_);
    db_tail_.assign(other.db_tail_);
    if (OB_FAIL(assign_rule_list(other.tb_rules_, tb_rules_))) {
      LOG_WARN("fail to copy tb rules", K(ret));
    } else if (OB_FAIL(assign_rule_list(other.db_rules_, db_rules_))) {
      LOG_WARN("fail to copy db rules", K(ret));
    } else if (OB_FAIL(assign_rule_list(other.es_rules_, es_rules_))) {
      LOG_WARN("fail to copy es rules", K(ret));
    }
  }
  return ret;
}

int ObShardRule::assign_rule_list(const ObProxyShardRuleList &src_rule_list,
                                  ObProxyShardRuleList &target_rule_list)
{
  int ret = OB_SUCCESS;
  ObProxyShardRuleInfo new_rule_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < src_rule_list.count(); ++i) {
    new_rule_info.reset();
    const ObProxyShardRuleInfo &rule_info = src_rule_list.at(i);
    if (OB_FAIL(ObProxyPbUtils::force_parse_groovy(rule_info.shard_rule_str_.config_string_,
                new_rule_info, allocator_))) {
      LOG_WARN("fail to parse shard rule str", K(rule_info), K(ret));
    } else if (!new_rule_info.is_valid()) {
      // do nothing
      LOG_WARN("rule str is not support", K(rule_info.shard_rule_str_));
    } else if (OB_FAIL(target_rule_list.push_back(new_rule_info))) {
      LOG_WARN("fail to push back rule info", K(new_rule_info), K(ret));
    }
  }
  return ret;
}

int ObShardRule::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%s\", \"%s\":%s, \"%s\": {\
                       \"%s\": {\"%s\":\"%s\",\
                       \"%s\":\"%s\", \"%s\":\"%s\", \"%s\": [",
                       RULE_TB_NAME, table_name_.ptr(),
                       ROUTER_SEQUENCE, is_sequence_ ? "true" : "false",
                       RULE_ROUTER,RULE_RULES,
                       RULE_TB_NAME_PATTERN, tb_name_pattern_.ptr(),
                       RULE_DB_NAME_PATTERN, db_name_pattern_.ptr(),
                       RULE_TB_SUFFIX_PADDING, tb_suffix_.ptr(),
                       RULE_TB_RULES);

  for (int64_t i = 0; OB_SUCC(ret) && i < tb_rules_.count(); ++i) {
    const ObProxyShardRuleInfo &rule_info = tb_rules_.at(i);
    if (OB_FAIL(buf.append_fmt("\"%.*s\"", rule_info.shard_rule_str_.length(), rule_info.shard_rule_str_.ptr()))) {
      LOG_WARN("fail to append shard tb rule", K(ret));
    } else if (i < tb_rules_.count() - 1) {
      ret = buf.append(",");
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append_fmt("], \"%s\":[", RULE_DB_RULES);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < db_rules_.count(); ++i) {
    const ObProxyShardRuleInfo &rule_info = db_rules_.at(i);
    if (OB_FAIL(buf.append_fmt("\"%.*s\"", rule_info.shard_rule_str_.length(), rule_info.shard_rule_str_.ptr()))) {
      LOG_WARN("fail to append shard db rule", K(ret));
    } else if (i < db_rules_.count() - 1) {
      ret = buf.append(",");
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append_fmt("], \"%s\":[", RULE_ES_RULES);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < es_rules_.count(); ++i) {
    const ObProxyShardRuleInfo &rule_info = es_rules_.at(i);
    if (OB_FAIL(buf.append_fmt("\"%.*s\"", rule_info.shard_rule_str_.length(), rule_info.shard_rule_str_.ptr()))) {
      LOG_WARN("fail to append shard es rule", K(ret));
    } else if (i < es_rules_.count() - 1) {
      ret = buf.append(",");
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("]}}}");
  }

  return ret;
}

int ObShardRule::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *router_config = NULL;
  Value *rules_config = NULL;
  Value *tb_rules_config = NULL;
  Value *db_rules_config = NULL;
  Value *es_rules_config = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == RULE_TB_NAME) {
        table_name_.set_value(it->value_->get_string());
        string_to_upper_case(table_name_.ptr(), table_name_.length());
      } else if (it->name_ == ROUTER_SEQUENCE) {
        is_sequence_ = JT_TRUE == it->value_->get_type();
      } else if (it->name_ == RULE_ROUTER) {
        router_config = it->value_;
      }
    }
  }
  if (OB_SUCC(ret) && NULL != router_config) {
    if (JT_OBJECT == router_config->get_type()) {
      DLIST_FOREACH(it, router_config->get_object()) {
        if (it->name_ == RULE_RULES) {
          rules_config = it->value_;
        }
      }
    }
  }
  if (OB_SUCC(ret) && NULL != rules_config) {
    if (JT_OBJECT == rules_config->get_type()) {
      DLIST_FOREACH(it, rules_config->get_object()) {
        std::string rule_pattern;
        if (it->name_ == RULE_TB_SUFFIX_PADDING) {
          tb_suffix_.set_value(it->value_->get_string());
        } else if (it->name_ == RULE_TB_NAME_PATTERN) {
          tb_name_pattern_.set_value(it->value_->get_string());
          rule_pattern.assign(tb_name_pattern_.ptr(), static_cast<size_t>(tb_name_pattern_.length()));
          if (OB_FAIL(ObProxyPbUtils::parse_rule_pattern(rule_pattern, tb_prefix_, tb_size_, tb_suffix_len_, tb_tail_))) {
            LOG_WARN("fail to parse tb name pattern", K_(tb_name_pattern), K(ret));
          }
        } else if (it->name_ == RULE_DB_NAME_PATTERN) {
          db_name_pattern_.set_value(it->value_->get_string());
          rule_pattern.assign(db_name_pattern_.ptr(), static_cast<size_t>(db_name_pattern_.length()));
          if (OB_FAIL(ObProxyPbUtils::parse_rule_pattern(rule_pattern, db_prefix_, db_size_, db_suffix_len_, db_tail_))) {
            LOG_WARN("fail to parse db name pattern", K_(db_name_pattern), K(ret));
          }
        } else if (it->name_ == RULE_TB_RULES) {
          tb_rules_config = it->value_;
        } else if (it->name_ == RULE_DB_RULES) {
          db_rules_config = it->value_;
        } else if (it->name_ == RULE_ES_RULES) {
          es_rules_config = it->value_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL != tb_rules_config && OB_FAIL(ObProxyPbUtils::do_parse_json_rules(tb_rules_config, tb_rules_, allocator_))) {
      LOG_WARN("fail to parse tb rule list", K(ret));
    } else if (NULL != db_rules_config && OB_FAIL(ObProxyPbUtils::do_parse_json_rules(db_rules_config, db_rules_, allocator_))) {
      LOG_WARN("fail to parse db rule list", K(ret));
    } else if (NULL != es_rules_config && OB_FAIL(ObProxyPbUtils::do_parse_json_rules(es_rules_config, es_rules_, allocator_))) {
      LOG_WARN("fail to parse elastic rule list", K(ret));
    }
  }

  return ret;
}

int ObShardRule::init(const std::string &table_name)
{
  int ret = OB_SUCCESS;
  table_name_.set_value(table_name.length(), table_name.c_str());
  return ret;
}

int ObShardRule::get_index_by_real_name(const ObString &name, const ObString &name_prefix, int64_t index_len, int64_t &index)
{
  int ret = OB_SUCCESS;

  // skip prefix and _
  const ObString name_suffix(index_len, name.ptr() + name_prefix.length() + 1);
  if (OB_FAIL(get_int_value(name_suffix, index))) {
    LOG_WARN("fail to get int value for name suffix", K(name), K(name_prefix), K(name_suffix), K(ret));
  }

  return ret;
}

int ObShardRule::get_real_name_by_index(const int64_t size, const int64_t suffix_len,
                                         int64_t index, const ObString &name_prefix,
                                         const ObString &name_tail,
                                         char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > 1 && index >= size)) {
    ret = OB_EXPR_CALC_ERROR;
    LOG_WARN("shard index is larger than shard count", K(size), K(index), K(name_prefix), K(ret));
  } else {
    snprintf(buf, buf_len, "%.*s", name_prefix.length(), name_prefix.ptr());
    if (size > 1 && suffix_len > 0) {
      char format[20] = "\0";
      snprintf(format, 20, "_%%0%ldld", suffix_len);

      char suffix[20] = {0};
      snprintf(suffix, 20, format, index);
      snprintf(buf + name_prefix.length(),
               buf_len - name_prefix.length(), suffix);
    }
    if (!name_tail.empty()) {
      int64_t now_len = strlen(buf);
      snprintf(buf + now_len, buf_len - now_len, "%.*s", name_tail.length(), name_tail.ptr());
    }
  }
  return ret;
}

int ObShardRule::get_physic_index_random(const int64_t physic_size,int64_t &index)
{
  int ret = OB_SUCCESS;
  if (OBPROXY_MAX_DBMESH_ID != index) {
    // do nothing, index comes from hint
  } else if (physic_size == 1) {
    // for elastic id, physic_size = max elastic id + 1
    index = 0;
  } else if (OB_FAIL(ObRandomNumUtils::get_random_num(0, physic_size - 1, index))) {
    LOG_DEBUG("fail to get random num", K(index), K(physic_size), K(ret));
  }

  return ret;
}

int ObShardRule::get_physic_index(const SqlFieldResult &sql_result,
                                  ObProxyShardRuleList &rules,
                                  const int64_t physic_size,
                                  const ObTestLoadType type,
                                  int64_t &index,
                                  const bool is_elastic_index /*false*/)
{
  int ret = OB_SUCCESS;
  int64_t last_index = OBPROXY_MAX_DBMESH_ID;
  if (OBPROXY_MAX_DBMESH_ID != index) {
    // do nothing, index comes from hint
  } else if (physic_size == 1) {
    // for elastic id, physic_size = max elastic id + 1
    index = 0;
  } else {
    ObArenaAllocator allocator;
    LOG_DEBUG("get physic index", K(type), K(physic_size), K(is_elastic_index));

    for (int i = 0; i < sql_result.field_num_; i++) {
      LOG_DEBUG("SqlField is ", K(i), K(sql_result.fields_[i]));
    }

    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && i < rules.count(); i++) {
      const ObProxyShardRuleInfo &rule = rules.at(i);
      ObProxyExprCtx expr_ctx(physic_size, type, is_elastic_index, &allocator);
      ObProxyExpr *proxy_expr = rule.expr_;
      ObSEArray<ObObj, 4> result_array;
      int64_t tmp_index = OBPROXY_MAX_DBMESH_ID;
      int64_t tmp_index_for_one_obj = OBPROXY_MAX_DBMESH_ID;
      LOG_DEBUG("begin to calc rule", K(rule));

      ObProxyExprCalcItem calc_item(const_cast<SqlFieldResult*>(&sql_result));
      if (OB_ISNULL(proxy_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("proxy expr is null unexpected", K(ret));
      } else if (OB_FAIL(proxy_expr->calc(expr_ctx, calc_item, result_array)) || result_array.empty()) {
        if (OB_EXPR_COLUMN_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          continue;
        } else {
          LOG_WARN("calc proxy expr failed", K(ret));
        }
      } else {
        for(int64_t j = 0; OB_SUCC(ret) && j < result_array.count(); j++) {
          ObObj& result_obj = result_array.at(j);
          ObObj tmp_obj;
          if (OB_FAIL(ObProxyFuncExpr::get_int_obj(result_obj, tmp_obj, allocator))) {
            LOG_WARN("get int obj failed", K(result_obj), K(ret));
          } else if (OB_FAIL(tmp_obj.get_int(tmp_index_for_one_obj))) {
            LOG_WARN("get int failed", K(ret));
          } else if (tmp_index_for_one_obj < 0 || tmp_index_for_one_obj >= physic_size) {
              ret = OB_EXPR_CALC_ERROR;
              LOG_WARN("invalid index", K(tmp_index_for_one_obj), K(physic_size), K(ret));
          } else if (OBPROXY_MAX_DBMESH_ID == tmp_index) {
            tmp_index = tmp_index_for_one_obj;
          } else if (tmp_index != tmp_index_for_one_obj) {
            ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
            LOG_WARN("different physic index for one sharding key is not supported", K(tmp_index), K(last_index), K(ret));
          } else {
            //nothing
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OBPROXY_MAX_DBMESH_ID != last_index) {
          if (tmp_index != last_index) {
            ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
            LOG_WARN("different physic index is not supported", K(tmp_index), K(last_index), K(ret));
          }
        } else {
          last_index = tmp_index;
        }
      }
    } // end rules

    if (OB_SUCC(ret)) {
      if (OBPROXY_MAX_DBMESH_ID == last_index) {
        ret = OB_EXPR_CALC_ERROR;
        LOG_DEBUG("not match any shard rule", K(ret));
      } else {
        index = last_index;
      }
    } else if (ret == OB_INVALID_ARGUMENT
                  || ret == OB_INVALID_DATA
                  || ret == OB_EXPR_CALC_ERROR) {
       LOG_WARN("error to get physic index use rules", K(ret), "rule_index", i);
       ret = OB_ERR_GET_PHYSIC_INDEX_BY_RULE;
    }
  }
  return ret;
}

int ObShardRule::get_physic_index_array(const SqlFieldResult &sql_result,
                                        ObProxyShardRuleList &rules,
                                        const int64_t physic_size,
                                        const ObTestLoadType type,
                                        ObIArray<int64_t> &index_array,
                                        const bool is_elastic_index)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSEArray<int64_t, 4> last_index_array;
  ObSEArray<int64_t, 4> sort_index_array;
  ObHashSet<int64_t> index_set;
  LOG_DEBUG("get physic index", K(type), K(physic_size), K(is_elastic_index));

  for (int i = 0; i < sql_result.field_num_; i++) {
    LOG_DEBUG("SqlField is ", K(i), K(sql_result.fields_[i]));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_set.create(BUCKET_SIZE, ObModIds::OB_PROXY_SHARDING_CONFIG,
                                 ObModIds::OB_PROXY_SHARDING_CONFIG))) {
      LOG_WARN("fail to create index hashset", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rules.count(); i++) {
    const ObProxyShardRuleInfo &rule = rules.at(i);
    ObProxyExprCtx expr_ctx(physic_size, type, is_elastic_index, &allocator);
    ObProxyExpr *proxy_expr = rule.expr_;
    ObSEArray<ObObj, 4> result_obj_array;

    LOG_DEBUG("begin to calc rule", K(rule));

    ObProxyExprCalcItem calc_item(const_cast<SqlFieldResult*>(&sql_result));
    if (OB_ISNULL(proxy_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("proxy expr is null unexpected", K(ret));
    } else if (OB_FAIL(proxy_expr->calc(expr_ctx, calc_item, result_obj_array))) {
      // Ignore the return value and scan the table directly
      if (OB_EXPR_COLUMN_NOT_EXIST != ret) {
        LOG_WARN("calc proxy expr failed", K(ret));
      }
      ret = OB_SUCCESS;
      continue;
    } else {
      sort_index_array.reset();
      index_set.reuse();
      // Because the obj in result_obj_array may not be an integer,
      // it is stored in index_array after conversion.
      for (int64_t i = 0; OB_SUCC(ret) && i < result_obj_array.count(); i++) {
        ObObj tmp_obj;
        int64_t tmp_index;
        if (OB_FAIL(ObProxyFuncExpr::get_int_obj(result_obj_array.at(i), tmp_obj, allocator))) {
          LOG_WARN("get int obj failed", K(ret), K(result_obj_array.at(i)));
        } else if (OB_FAIL(tmp_obj.get_int(tmp_index))) {
          LOG_WARN("get int failed", K(ret));
        } else if (tmp_index < 0 || tmp_index >= physic_size) {
          ret = OB_EXPR_CALC_ERROR;
          LOG_WARN("invalid index", K(tmp_index), K(physic_size), K(ret));
        } else if (OB_FAIL(index_set.set_refactored(tmp_index))) {
          LOG_WARN("fail to add index to hash set", K(tmp_index), K(ret));
        }
      }

      ObHashSet<int64_t>::iterator iter = index_set.begin();
      ObHashSet<int64_t>::iterator end = index_set.end();
      for (; OB_SUCC(ret) && iter != end; iter++) {
        if (OB_FAIL(sort_index_array.push_back(iter->first))) {
          LOG_WARN("push back index failed", "index", iter->first, K(ret));
        }
      }

      // sort
      if (OB_SUCC(ret)) {
        std::sort(sort_index_array.begin(), sort_index_array.end());
      }

      if (OB_SUCC(ret)) {
        if (!last_index_array.empty()) {
          if (last_index_array.count() != sort_index_array.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("count not equal", "last count", last_index_array.count(),
                     "sort count", sort_index_array.count(), K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < last_index_array.count(); i++) {
            if (last_index_array.at(i) != sort_index_array.at(i)) {
              ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
              LOG_WARN("different physcic index is not supported", K(i), "last index", last_index_array.at(i),
                       "sort index", sort_index_array.at(i), K(ret));
            }
          }
        } else if (rules.count() > 1) {
          if (OB_FAIL(last_index_array.assign(sort_index_array))) {
            LOG_WARN("fail assign to last index array", K(sort_index_array), K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_array.assign(sort_index_array))) {
      LOG_WARN("fail assgin to index array", K(sort_index_array), K(ret));
    }
  }

  return ret;
}

// a~j ==> 0~9
void ObShardRule::handle_special_char(char *value, int64_t len)
{
  if (NULL != value) {
    for (int64_t i = 0; i < len; ++i) {
      if (value[i] == 'a' || value[i] == 'A') {
        value[i] = '0';
      } else if (value[i] == 'b' || value[i] == 'B') {
        value[i] = '1';
      } else if (value[i] == 'c' || value[i] == 'C') {
        value[i] = '2';
      } else if (value[i] == 'd' || value[i] == 'D') {
        value[i] = '3';
      } else if (value[i] == 'e' || value[i] == 'E') {
        value[i] = '4';
      } else if (value[i] == 'f' || value[i] == 'F') {
        value[i] = '5';
      } else if (value[i] == 'g' || value[i] == 'G') {
        value[i] = '6';
      } else if (value[i] == 'h' || value[i] == 'H') {
        value[i] = '7';
      } else if (value[i] == 'i' || value[i] == 'I') {
        value[i] = '8';
      } else if (value[i] == 'j' || value[i] == 'J') {
        value[i] = '9';
      }
    }
  }
}

//------ ObShardRouter------
void ObShardRouter::destroy()
{
  dc_state_ = DC_DEAD;
  LOG_INFO("shard router will be destroyed", KPC(this));
  MarkedRuleHashMap::iterator end = mr_map_.end();
  MarkedRuleHashMap::iterator tmp_iter;
  for (MarkedRuleHashMap::iterator it = mr_map_.begin(); it != end;) {
    tmp_iter = it;
    ++it;
    tmp_iter->destroy();
  }
  mr_map_.reset();
  op_free(this);
}

int64_t ObShardRouter::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  MarkedRuleHashMap &map = const_cast<MarkedRuleHashMap &>(mr_map_);
  J_KV(K_(seq_table_name));
  for (MarkedRuleHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "shard_rule", *it);
  }
  J_OBJ_END();
  return pos;
}

int ObShardRouter::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObShardRouter &other = static_cast<const ObShardRouter &>(child_info);
    seq_table_name_.assign(other.seq_table_name_);
    MarkedRuleHashMap &map = const_cast<MarkedRuleHashMap &>(other.mr_map_);
    ObShardRule *rule_info = NULL;
    for (MarkedRuleHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
      if (OB_ISNULL(rule_info = op_alloc(ObShardRule))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObShardRule", K(ret), K_(db_info_key));
      } else if (OB_FAIL(rule_info->assign(*it))) {
        LOG_WARN("fail to copy rule_info", K(*it), K_(db_info_key), K(ret));
      } else if (OB_FAIL(mr_map_.unique_set(rule_info))) {
        LOG_WARN("fail to unique_set rule_info", KPC(rule_info), K_(db_info_key), K(ret));
      }
      if (OB_FAIL(ret) && NULL != rule_info) {
        op_free(rule_info);
        rule_info = NULL;
      }
    }
  }
  return ret;
}

int ObShardRouter::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  MarkedRuleHashMap &map = const_cast<MarkedRuleHashMap &>(mr_map_);
  ret = buf.append_fmt("{\"%s\":\"%s\", \"%s\":[",
                       CONFIG_VERSION, version_.ptr(), ROUTER_RULES);
  int64_t i = 0;
  for (MarkedRuleHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
    if (OB_FAIL(it->to_json_str(buf))) {
      LOG_WARN("fail to get json str for shard table rule", K(ret), K(*it));
    } else if (i < map.count() - 1) {
      ret = buf.append(",");
    }
    ++i;
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("]}");
  }
  return ret;
}

int ObShardRouter::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *rules_config = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == ROUTER_RULES) {
        rules_config = it->value_;
      }
    }
  }
  if (NULL != rules_config && JT_ARRAY == rules_config->get_type()) {
    ObShardRule *rule_info = NULL;
    DLIST_FOREACH(it, rules_config->get_array()) {
      if (JT_OBJECT == it->get_type()) {
        if (OB_ISNULL(rule_info = op_alloc(ObShardRule))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for ObShardRule", K(ret));
        } else if (OB_FAIL(rule_info->parse_from_json(*it))) {
          LOG_WARN("fail to parse ObShardRule", K(ret), KPC(rule_info));
        } else {
          if (rule_info->is_sequence_) {
            seq_table_name_.set_value(rule_info->table_name_);
          }
          mr_map_.unique_set(rule_info);
        }
        if (OB_FAIL(ret) && NULL != rule_info) {
          op_free(rule_info);
          rule_info = NULL;
        }
      } // end JT_OBJ
    } // end loop
  }
  return ret;
}

int ObShardRouter::get_shard_rule(const ObString &tb_name, ObShardRule *&shard_rule)
{
  int ret = OB_SUCCESS;
  shard_rule = NULL;
  if (OB_FAIL(mr_map_.get_refactored(tb_name, shard_rule))) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

bool ObDbConfigLogicDb::is_shard_rule_empty()
{
  bool bret = true;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &>(sr_array_.ccr_map_);
  for (ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap::iterator it = map.begin(); bret && it != map.end(); ++it) {
    if (it->mr_map_.count() != 0) {
      bret = false;
    }
  }
  return bret;
}

int ObDbConfigLogicDb::get_shard_router(const ObString &tb_name, ObShardRouter *&shard_router)
{
  int ret = OB_SUCCESS;
  shard_router = NULL;
  ObShardRule *shard_rule = NULL;
  bool found = false;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &>(sr_array_.ccr_map_);
  for (ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap::iterator it = map.begin(); !found && it != map.end(); ++it) {
    if (OB_FAIL(it->mr_map_.get_refactored(tb_name, shard_rule))) {
      LOG_DEBUG("shard router does not exist", K(tb_name), K(ret));
    } else {
      found = true;
      shard_router = &(*it);
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("shard router does not exist", K(ret), K(tb_name));
  } else if (OB_ISNULL(shard_router) || !shard_router->is_avail()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("shard router is not avail", K(ret), K(tb_name));
    shard_router = NULL;
  } else {
    shard_router->inc_ref();
  }
  return ret;
}

int ObDbConfigLogicDb::get_first_group_shard_connector(ObShardConnector *&shard_conn, int64_t es_id,
                                                       bool is_read_stmt, ObTestLoadType testload_type)
{
  int ret = OB_SUCCESS;

  ObShardTpo *shard_tpo = NULL;
  ObGroupCluster *gc_info = NULL;

  if (OB_FAIL(get_shard_tpo(shard_tpo))) {
    LOG_WARN("fail to get shard tpo info", K(ret));
  } else if (OB_ISNULL(shard_tpo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard tpo info is null", K(ret));
  } else if (is_single_shard_db_table()) {
    ObShardTpo::GCHashMap::iterator it = (const_cast<ObShardTpo::GCHashMap &>(shard_tpo->gc_map_)).begin();
    gc_info = &(*it);
  } else {
    ObShardRule *logic_tb_info = NULL;
    {
      ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
      obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
      ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &>(sr_array_.ccr_map_);
      for (ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap::iterator it = map.begin(); it != map.end(); ++it) {
        ObShardRouter::MarkedRuleHashMap &mr_map = it->mr_map_;
        for (ObShardRouter::MarkedRuleHashMap::iterator sub_it = mr_map.begin();
             sub_it != mr_map.end(); ++sub_it) {
          logic_tb_info = &(*sub_it);
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];
      if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->db_size_, logic_tb_info->db_suffix_len_, 0,
                                                        logic_tb_info->db_prefix_.config_string_,
                                                        logic_tb_info->db_tail_.config_string_,
                                                        real_database_name, OB_MAX_DATABASE_NAME_LENGTH))) {
        LOG_WARN("fail to get real group name", KPC(logic_tb_info), K(ret));
      } else if (OB_FAIL(shard_tpo->get_group_cluster(ObString::make_string(real_database_name), gc_info))) {
        LOG_DEBUG("group does not exist", "phy_db_name", real_database_name, K(ret));
      } else if (OB_ISNULL(gc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group cluster info is null", "phy_db_name", real_database_name, K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // Calculate es_id
    int64_t es_size = gc_info->get_es_size();
    ObString shard_name;
    if (-1 == es_id || OBPROXY_MAX_DBMESH_ID == es_id) {
      if (OB_FAIL(gc_info->get_elastic_id_by_weight(es_id, is_read_stmt))) {
        LOG_WARN("fail to get eid by read weight", KPC(gc_info));
      } else {
        LOG_DEBUG("succ to get eid by weight", K(es_id));
      }
    } else if (OB_UNLIKELY(es_id >= es_size)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("es index is larger than elastic array", K(es_id), K(es_size), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString shard_name;
    if (FALSE_IT(shard_name = gc_info->get_shard_name_by_eid(es_id))) {
    } else if (TESTLOAD_NON != testload_type) {
      if (OB_FAIL(get_testload_shard_connector(shard_name, testload_prefix_.config_string_, shard_conn))) {
        LOG_WARN("testload shard connector not exist", "testload_type", get_testload_type_str(testload_type),
                 K(shard_name), "testload_prefix", testload_prefix_, K(ret));
      }
    } else if (OB_FAIL(get_shard_connector(shard_name, shard_conn))) {
      LOG_WARN("shard connector does not exist", K(shard_name), K(ret));
    }
  }

  if (NULL != shard_tpo) {
    shard_tpo->dec_ref();
    shard_tpo = NULL;
  }

  return ret;
}

int ObDbConfigLogicDb::get_all_shard_table(ObIArray<ObString> &all_table)
{
  int ret = OB_SUCCESS;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &>(sr_array_.ccr_map_);
  for (ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    ObShardRouter::MarkedRuleHashMap &mr_map = it->mr_map_;
    for (ObShardRouter::MarkedRuleHashMap::iterator sub_it = mr_map.begin();
         sub_it != mr_map.end(); ++sub_it) {
      all_table.push_back(sub_it->table_name_.config_string_);
    }
  }
  return ret;
}

//------ ObMarkedDist------
int64_t ObMarkedDist::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(shard_name), K_(dist));
  J_OBJ_END();
  return pos;
}

int ObMarkedDist::init(const std::string &name, const std::string &dist)
{
  int ret = OB_SUCCESS;
  shard_name_.set_value(name.length(), name.c_str());
  dist_.set_value(dist.length(), dist.c_str());
  return ret;
}

//------ ObShardDist------
int64_t ObShardDist::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  MarkedDistHashMap &map = const_cast<MarkedDistHashMap &>(md_map_);
  for (MarkedDistHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "shard_dist", *it);
  }
  J_OBJ_END();
  return pos;
}
int ObShardDist::to_json_str(common::ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":[",
                       CONFIG_VERSION, version_.ptr(),
                       SHARD_DISTS))) {
    LOG_WARN("fail append shard dist to json str", K(ret), KPC(this));
  }
  int64_t i = 0;
  MarkedDistHashMap& tmp_map = const_cast<MarkedDistHashMap &>(md_map_);
  for (MarkedDistHashMap::iterator it = tmp_map.begin(); OB_SUCC(ret) && it != tmp_map.end(); ++it) {
    ObMarkedDist& mark_dist = *it;
    if (OB_FAIL(buf.append_fmt("{\"%s\":\"%.*s\", \"%s\":\"%.*s\"}",
      SHARD_DISTS_DIST, mark_dist.dist_.config_string_.length(), mark_dist.dist_.config_string_.ptr(),
      SHARD_DISTS_MARK, mark_dist.shard_name_.config_string_.length(), mark_dist.shard_name_.config_string_.ptr()))) {
      LOG_WARN("fail append shard dist to json str", K(ret), KPC(this));
    } else if (i < tmp_map.count() - 1) {
      ret = buf.append(",");
    }
    ++i;
  }
  if (OB_SUCC(ret) && OB_FAIL(buf.append("]}"))) {
    LOG_WARN("fail append shard dist to json str", K(ret), KPC(this));
  }
  return ret;
}
int ObShardDist::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value * dist_config = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_.case_compare(CONFIG_VERSION) == 0) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_.case_compare(SHARD_DISTS) == 0) {
        dist_config = it->value_;
      }
    }
    if (dist_config != NULL && JT_ARRAY == dist_config->get_type()) {
      obutils::ObProxyConfigString shard_name;
      obutils::ObProxyConfigString dist;
      ObMarkedDist* mark_dist = NULL;
      DLIST_FOREACH(it, dist_config->get_array()) {
        if (JT_OBJECT == it->get_type()) {
          DLIST_FOREACH(it2, it->get_object()) {
            if (JT_STRING == it2->value_->get_type()) {
              if (it2->name_.case_compare(SHARD_DISTS_DIST) == 0) {
                dist.set_value(it2->value_->get_string());
              } else if (it2->name_.case_compare(SHARD_DISTS_MARK) == 0) {
                shard_name.set_value(it2->value_->get_string());
              }
            } else {
              LOG_WARN("invalid type", K(it2->value_->get_type()));
            }
          }
          if (!shard_name.empty() && !dist.empty()) {
            if (OB_ISNULL(mark_dist = op_alloc(ObMarkedDist))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory for ObMarkedDist", K(shard_name), K(dist), K(ret));
            } else {
              mark_dist->shard_name_.set_value(shard_name.config_string_);
              mark_dist->dist_.set_value(dist.config_string_);
              if (OB_FAIL(md_map_.unique_set(mark_dist))) {
                mark_dist->destroy();
                mark_dist = NULL;
                LOG_WARN("add to map failed", K(ret));
              }
            }
          } else {
            LOG_WARN("invalid shard_name or dist", K(shard_name), K(dist));
          }
        } else {
          LOG_WARN("unexpected json type", K(it->get_type()));
        }
      }
    } else {
      LOG_WARN("unexpected dist_config", K(dist_config));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("ObShardDist::parse_from_json succ", KPC(this));
  }
  return ret;
}


int ObShardDist::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObShardDist &other = static_cast<const ObShardDist &>(child_info);
    MarkedDistHashMap &map = const_cast<MarkedDistHashMap &>(other.md_map_);
    ObMarkedDist *dist_info = NULL;
    for (MarkedDistHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
      if (OB_ISNULL(dist_info = op_alloc(ObMarkedDist))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObMarkedDist", K(ret), K_(db_info_key));
      } else if (OB_FAIL(dist_info->assign(*it))) {
        LOG_WARN("fail to copy shard dist", K(*it), K_(db_info_key), K(ret));
      } else if (OB_FAIL(md_map_.unique_set(dist_info))) {
        LOG_WARN("fail to unique_set dist_info", KPC(dist_info), K_(db_info_key), K(ret));
      }
      if (OB_FAIL(ret) && NULL != dist_info) {
        op_free(dist_info);
        dist_info = NULL;
      }
    }
  }
  return ret;
}

void ObShardDist::destroy()
{
  dc_state_ = DC_DEAD;
  LOG_INFO("shard distribute will be destroyed", KPC(this));
  MarkedDistHashMap::iterator end = md_map_.end();
  MarkedDistHashMap::iterator tmp_iter;
  for (MarkedDistHashMap::iterator it = md_map_.begin(); it != end;) {
    tmp_iter = it;
    ++it;
    tmp_iter->destroy();
  }
  md_map_.reset();
  op_free(this);
}

//------ ObShardConnector------
int64_t ObShardConnector::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(shard_name), K_(shard_type), K_(shard_url), K_(username),
       K_(database_name), K_(tenant_name), K_(cluster_name), K_(server_type),
       K_(physic_addr), K_(is_physic_ip), K_(physic_port), K_(read_consistency),
      "enc_type", get_enc_type_str(enc_type_));
  J_OBJ_END();
  return pos;
}

int ObShardConnector::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObShardConnector &other = static_cast<const ObShardConnector &>(child_info);
    shard_name_.assign(other.shard_name_);
    shard_type_.assign(other.shard_type_);
    shard_url_.assign(other.shard_url_);
    enc_type_ = other.enc_type_;
    full_username_.assign(other.full_username_);
    ObProxyPbUtils::parse_shard_auth_user(*this);
    password_.assign(other.password_);
    org_password_.assign(other.org_password_);
    database_name_.assign(other.database_name_);
    physic_addr_.assign(other.physic_addr_);
    physic_port_.assign(other.physic_port_);
    read_consistency_.assign(other.read_consistency_);
    server_type_ = other.server_type_;
  }
  return ret;
}

int ObShardConnector::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\",\
                               \"%s\":\"%s\", \"%s\": {\"%s\":\"%s\",\
                               \"%s\":\"%s\", \"%s\": \"%s\"}}",
              CONFIG_VERSION, version_.ptr(),
              SHARDS_NAME, shard_name_.ptr(),
              SHARDS_TYPE, shard_type_.ptr(),
              SHARDS_URL, shard_url_.ptr(),
              SHARDS_AUTH,
              SHARDS_ENC_TYPE, get_enc_type_str(enc_type_),
              SHARDS_USERNAME, full_username_.ptr(),
              SHARDS_PASSWORD, is_enc_beyond_trust() ? "" : org_password_.ptr()))) {
    LOG_WARN("fail append shard connector to json str", K(ret), KPC(this));
  }
  return ret;
}
void ObShardConnector::set_full_username(const common::ObString& str)
{
  full_username_.set_value(str);
  ObProxyPbUtils::parse_shard_auth_user(*this);
}
int ObShardConnector::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *shard_auth = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == SHARDS_NAME) {
        shard_name_.set_value(it->value_->get_string());
      } else if (it->name_ == SHARDS_TYPE) {
        set_shard_type(it->value_->get_string());
      } else if (it->name_ == SHARDS_URL) {
        shard_url_.set_value(it->value_->get_string());
        std::string url_str(shard_url_.ptr(), static_cast<size_t>(shard_url_.length()));
        ObProxyPbUtils::parse_shard_url(url_str, *this);
      } else if (it->name_ == SHARDS_AUTH) {
        shard_auth = it->value_;
      }
    }
    if (OB_SUCC(ret) && NULL != shard_auth
        && JT_OBJECT == shard_auth->get_type()) {
      DLIST_FOREACH(it, shard_auth->get_object()) {
        if (it->name_ == SHARDS_ENC_TYPE) {
          enc_type_ = get_enc_type(it->value_->get_string().ptr(), it->value_->get_string().length());
        } else if (it->name_ == SHARDS_USERNAME) {
          set_full_username(it->value_->get_string());
        } else if (it->name_ == SHARDS_PASSWORD) {
          org_password_.set_value(it->value_->get_string());
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_enc_beyond_trust()) {
    // get plain password
    char pwd_buf[OB_MAX_PASSWORD_LENGTH];
    memset(pwd_buf, 0, sizeof(pwd_buf));
    if (OB_FAIL(ObBlowFish::decode(org_password_.ptr(), org_password_.length(), pwd_buf, OB_MAX_PASSWORD_LENGTH))) {
      LOG_WARN("fail to decode encrypted password", K_(org_password), K(ret));
      ret = OB_SUCCESS; // ignore ret
    } else {
      password_.set_value(strlen(pwd_buf), pwd_buf);
    }
  }
  return ret;
}

int ObShardConnector::get_dec_password(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= password_.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf", K(buf), K(buf_len), K(password_.length()), K(ret));
  } else {
    memcpy(buf, password_.ptr(), password_.length());
    buf[password_.length()] = '\0';
  }
  return ret;
}

int ObShardConnector::set_shard_type(const std::string &shard_type)
{
  ObString ob_shard_type(shard_type.length(), shard_type.c_str());
  return set_shard_type(ob_shard_type);
}

int ObShardConnector::set_shard_type(const ObString &shard_type)
{
  int ret = OB_SUCCESS;

  shard_type_.set_value(shard_type);

  if (0 == shard_type.case_compare("MYSQL")) {
    server_type_ = DB_MYSQL;
  } else if (0 == shard_type.case_compare("OCEANBASE")
             || 0 == shard_type.case_compare("OCEANBASE20")
             || 0 == shard_type.case_compare("OB_CLOUD")
             || 0 == shard_type.case_compare("OB_CLOUD20")) {
    server_type_ = DB_OB_MYSQL;
  } else if (0 == shard_type.case_compare("OB_ORACLE")) {
    server_type_ = DB_OB_ORACLE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shard_type", K(shard_type), K(ret));
  }

  return ret;
}

int ObShardConnector::get_physic_ip(sockaddr &addr)
{
  return ObProxyPbUtils::get_physic_ip(physic_addr_.config_string_, is_physic_ip_, addr);
}

int ObDbConfigLogicDb::get_testload_shard_connector(const ObString &shard_name,
                                                    const ObString &testload_prefix,
                                                    ObShardConnector *&shard_conn)
{
  int ret = OB_SUCCESS;
  char real_shard_name[ObShardConnector::OB_MAX_CONNECTOR_NAME_LENGTH];
  int64_t w_len = snprintf(real_shard_name, ObShardConnector::OB_MAX_CONNECTOR_NAME_LENGTH, "%.*s%.*s",
                           static_cast<int32_t>(testload_prefix.length()), testload_prefix.ptr(),
                           static_cast<int32_t>(shard_name.length()), shard_name.ptr());
  ObString testload_shard_name(w_len, real_shard_name);
  if (OB_FAIL(get_shard_connector(testload_shard_name, shard_conn))) {
    LOG_DEBUG("fail to get testload shard connector, we can still try reuse normal db",
             "connector", real_shard_name, K(ret));
    if (OB_FAIL(get_shard_connector(shard_name, shard_conn))) {
      LOG_WARN("fail to get shard connecor", K(shard_name), K(ret));
    }
  } else {
    LOG_DEBUG("succ to get testload shard connector", KPC(shard_conn));
  }
  return ret;
}

int ObDbConfigLogicDb::get_shard_connector(const ObString &shard_name, ObShardConnector *&shard_conn)
{
  int ret = OB_SUCCESS;
  shard_conn = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  if (OB_SUCC(sc_array_.ccr_map_.get_refactored(shard_name, shard_conn))) {
    if (NULL != shard_conn && shard_conn->is_avail()) {
      shard_conn->inc_ref();
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("shard connector is not avail", KPC(shard_conn));
    }
  }
  return ret;
}
int64_t ConnectionProperties::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(max_conn), K_(min_conn));
  J_OBJ_END();
  return pos;
}

//------ ObShardProp------
int64_t ObShardProp::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObDbConfigChild::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(shard_name), K_(connect_timeout), K_(socket_timeout), K_(idle_timeout_ms),
    K_(blocking_timeout_ms), K_(need_prefill), K_(read_consistency), K_(cur_conn_prop),
    K_(current_zone), "conn_prop_type", get_type_conn_prop_name(conn_prop_type_));
  ConnectionPropertiesMap &tmp_map = const_cast<ConnectionPropertiesMap &>(conn_prop_map_);
  if (tmp_map.size() > 0) {
    J_COMMA();
    J_NAME(SHARDS_ZONE_PROPERTIES);
    J_COLON();
    J_OBJ_START();
    for (ConnectionPropertiesMap::iterator it = tmp_map.begin();
      it != tmp_map.end(); ++it) {
      if (it != tmp_map.begin()) {
        J_COMMA();
      }
      J_NAME(it->first.c_str());
      J_COLON();
      J_OBJ_START();
      databuff_print_kv(buf, buf_len, pos, SHARDS_ZONE_MAX_CONN_PROP, it->second.max_conn_,
        SHARDS_ZONE_MIN_CONN_PROP, it->second.min_conn_);
      J_OBJ_END();
    }
    J_OBJ_END();
  }
  J_OBJ_END();
  return pos;
}

int ObShardProp::assign(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObDbConfigChild::assign(child_info))) {
    const ObShardProp &other = static_cast<const ObShardProp &>(child_info);
    shard_name_.assign(other.shard_name_);
    connect_timeout_ = other.connect_timeout_;
    socket_timeout_ = other.socket_timeout_;
    read_consistency_.assign(other.read_consistency_);
    // no need copy kv_kv_map_ and kv_vec_map_ which are use only in parse and save in k_obj_map
  }
  return ret;
}

int ObShardProp::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":{",
                       CONFIG_VERSION, version_.ptr(),
                       SHARDS_NAME, shard_name_.ptr(),
                       CONFIG_VALUES);
  std::map<std::string, std::string> &tmp_map = const_cast<std::map<std::string, std::string> &>(kv_map_);
  int i = 0;
  for (std::map<std::string, std::string>::iterator iter = tmp_map.begin();
       OB_SUCC(ret) && iter != tmp_map.end(); ++iter) {
    if (OB_FAIL(buf.append_fmt("\"%s\":\"%s\"", iter->first.c_str(),
                               iter->second.c_str()))) {
      LOG_WARN("fail to append config", K(ret));
    } else if (i < tmp_map.size() - 1) {
      ret = buf.append(",");
    }
    ++i;
  }
  if (OB_SUCC(ret) && k_obj_map_.size() > 0) {
    ret = buf.append(",");
    i = 0;
    std::map<std::string, std::string> &tmp_obj_map = const_cast<std::map<std::string, std::string> &>(k_obj_map_);
    for (std::map<std::string, std::string>::iterator iter = tmp_obj_map.begin();
       OB_SUCC(ret) && iter != tmp_obj_map.end(); ++iter) {
      if (OB_FAIL(buf.append_fmt("\"%s\":%s", iter->first.c_str(),
                                 iter->second.c_str()))) {
        LOG_WARN("fail to append config", K(ret));
      } else if (i < tmp_obj_map.size() - 1) {
        ret = buf.append(",");
      }
      ++i;
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("}}");
  }
  return ret;
}

int ObShardProp::parse_from_json(const json::Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *config_value = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == SHARDS_NAME) {
        shard_name_.set_value(it->value_->get_string());
      } else if (it->name_ == CONFIG_VALUES) {
        config_value = it->value_;
      }
    }
    if (OB_SUCC(ret) && NULL != config_value && JT_OBJECT == config_value->get_type()) {
      DLIST_FOREACH(it, config_value->get_object()) {
        std::string name_str(it->name_.ptr(), static_cast<size_t>(it->name_.length()));
        if (OB_FAIL(do_handle_json_value(it->value_, name_str))) {
          LOG_WARN("fail to handle json value", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = set_prop_values_from_conf();
  }
  return ret;
}
int ObShardProp::set_conn_props_from_conf()
{
  int ret = OB_SUCCESS;
  bool find = false;
  bool is_current = false;
  ConnectionProperties* conn_prop_by_name = NULL;
  ConnectionProperties* conn_prop_by_prefix = NULL;
  ConnectionProperties* conn_prop_by_current = NULL;
  ConnectionProperties* conn_prop_by_other = NULL;
  ObString zone(get_global_proxy_config().server_zone);
  if (zone.empty()) {
    LOG_WARN("fail to get server_zone from env");
  } else if (conn_prop_map_.size() > 0) {
    if (!current_zone_.empty()) {
      // current_zone format is "RZ41,RZ35"
      ObString tmp_current_zone = current_zone_.config_string_;
      ObString tmp_zone = tmp_current_zone.split_on(',');
      while (!is_current && !tmp_zone.empty()) {
        if (zone.prefix_case_match(tmp_zone)) {
          is_current = true;
        }
        tmp_zone = tmp_current_zone.split_on(',');
      }
      if (!is_current && !tmp_current_zone.empty()) {
        if (zone.prefix_case_match(tmp_current_zone)) {
          is_current = true;
        }
      }
    }
    ConnectionPropertiesMap::iterator iter;
    for (iter = conn_prop_map_.begin(); !find && iter != conn_prop_map_.end(); ++iter) {
      if (zone.case_compare(iter->first.c_str()) == 0) {
        conn_prop_by_name = &(iter->second);
        find = true;
        // cur_conn_prop_ = iter->second;
        // conn_prop_type_ = TYPE_ZONE_PROP_BY_NAME;
        // find = true;
      } else if (zone.prefix_case_match(iter->first.c_str())) {
        conn_prop_by_prefix = &(iter->second);
      } else if (is_current && (SHARDS_ZONE_CURRENT_CONN_PROP.case_compare(iter->first.c_str()) == 0)) {
        conn_prop_by_current = &iter->second;
      } else if (!is_current && (SHARDS_ZONE_OTHERS_CONN_PROP.case_compare(iter->first.c_str()) == 0)) {
        conn_prop_by_other = &iter->second;
      }
    }
  }
  if (NULL != conn_prop_by_name) {
    cur_conn_prop_ = *conn_prop_by_name;
    conn_prop_type_ = TYPE_ZONE_PROP_BY_NAME;
  } else if (NULL != conn_prop_by_prefix) {
    cur_conn_prop_ = *conn_prop_by_prefix;
    conn_prop_type_ = TYPE_ZONE_PROP_BY_PREFIX;
  } else if (NULL != conn_prop_by_current) {
    cur_conn_prop_ = *conn_prop_by_current;
    conn_prop_type_ = TYPE_SHARD_DIST_CURRENT;
  } else if (NULL != conn_prop_by_other) {
    cur_conn_prop_ = *conn_prop_by_other;
    conn_prop_type_ = TYPE_SHARD_DIST_OTHRES;
  } else {
    //3: use variables value
    ObString tmp_str;
    int64_t min_conn = 0;
    int64_t max_conn = 0;
    bool find_min = false;
    bool find_max = false;
    tmp_str = get_variable_value(SHARDS_ZONE_MAX_CONN_PROP);
    if (OB_SUCC(get_int_value(tmp_str, max_conn))) {
      find_max = true;
    }
    tmp_str = get_variable_value(SHARDS_ZONE_MIN_CONN_PROP);
    if (OB_SUCC(get_int_value(tmp_str, min_conn))) {
      find_min = true;
    }
    if (find_min && find_max) {
      cur_conn_prop_.max_conn_ = max_conn;
      cur_conn_prop_.min_conn_ = min_conn;
      conn_prop_type_ = TYPE_SHARD_PROP;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObShardProp::set_prop_values_from_conf()
{
  int ret = OB_SUCCESS;
  int64_t idle_timeout = 0;
  int64_t blocking_timeout = 0;
  ObString tmp_str = get_variable_value(SHARDS_IDLE_TIMEOUT);
  if (!tmp_str.empty() && OB_FAIL(get_int_value(tmp_str, idle_timeout))) {
    LOG_WARN("fail to get idle_timeout", K(tmp_str), K(ret));
  } else {
    idle_timeout_ms_ = idle_timeout * 60 * 1000;
  }
  if (OB_SUCC(ret)) {
    tmp_str = get_variable_value(SHARDS_BLOCKING_TIMEOUT);
    if (!tmp_str.empty() && OB_FAIL(get_int_value(tmp_str, blocking_timeout))) {
      LOG_WARN("fail to get blocking_timeout", K(tmp_str), K(ret));
    } else {
      blocking_timeout_ms_ = blocking_timeout;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str = get_variable_value(SHARDS_NEED_PREFILL);
    if (tmp_str.case_compare("true") == 0) {
      need_prefill_ = true;
    }
  }
  std::map<std::string, std::map<std::string, std::string> > &tmp_kv_kv_map =
      const_cast<std::map<std::string, std::map<std::string, std::string> > &>(kv_kv_map_);
  for (std::map<std::string, std::map<std::string, std::string> >::iterator iter1 = tmp_kv_kv_map.begin();
    OB_SUCC(ret) && iter1 != tmp_kv_kv_map.end(); ++iter1) {
    std::map<std::string, std::string> &inner_map = iter1->second;
    ObString name(iter1->first.length(), iter1->first.c_str());
    if ((name.case_compare(SHARDS_CONN_PROP) != 0)) {
      continue;
    }
    for(std::map<std::string, std::string>::iterator iter2 = inner_map.begin();
      OB_SUCC(ret) && iter2 != inner_map.end(); ++iter2) {
      ObString name2(iter2->first.length(), iter2->first.c_str());
      if (name2.case_compare(SHARDS_CONN_TIMEOUT) == 0) {
        int64_t connect_timeout = 0;
        ObString tmp_value(iter2->second.length(), iter2->second.c_str());
        if (OB_FAIL(get_int_value(tmp_value, connect_timeout))) {
          LOG_WARN("fail to get connect_timeout", K(tmp_value), K(ret));
          ret = OB_INVALID_ARGUMENT;
        } else {
          connect_timeout_ = connect_timeout;
        }
      } else if (name2.case_compare(SHARDS_SOCK_TIMEOUT) == 0) {
        int64_t socket_timeout = 0;
        ObString tmp_value(iter2->second.length(), iter2->second.c_str());
        if (OB_FAIL(get_int_value(tmp_value, socket_timeout))) {
          LOG_WARN("fail to get socket_timeout", K(tmp_value), K(ret));
          ret = OB_INVALID_ARGUMENT;
        } else {
          socket_timeout_ = socket_timeout;
        }
      } else if (name2.case_compare(SHARDS_READ_CONSISTENCY) == 0) {
        ObString tmp_value(iter2->second.length(), iter2->second.c_str());
        read_consistency_.set_value(tmp_value);
      }
    }
  }
  return ret;
}

int ObShardProp::do_handle_connection_prop(const json::Value& json_root, std::string& key) {
  int ret = OB_SUCCESS;
  json::Value * config_value = NULL;
  int64_t min_conn = 0;
  int64_t max_conn = 0;
  bool has_min = false;
  bool has_max = false;
  DLIST_FOREACH(it1, json_root.get_object()) {
    config_value = it1->value_;
    if (NULL != config_value &&  JT_NUMBER == config_value->get_type()) {
      if (it1->name_.case_compare(SHARDS_ZONE_MAX_CONN_PROP) == 0) {
        max_conn = it1->value_->get_number();
        has_max = true;
      } else if (it1->name_.case_compare(SHARDS_ZONE_MIN_CONN_PROP) == 0) {
        min_conn = it1->value_->get_number();
        has_min = true;
      }
    } else if (NULL != config_value) {
      LOG_WARN("unexpected type", K(config_value->get_type()));
    } else {
      LOG_WARN("unexpected null config_value");
    }
  }
  if (has_min && has_max) {
    ConnectionProperties conn_prop(max_conn, min_conn);
    conn_prop_map_.insert(std::pair<std::string, ConnectionProperties>(key, conn_prop));
  }
  return ret;
}

int ObShardProp::do_handle_json_value(const json::Value *json_root, const std::string& key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_root)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json_root is null");
  } else if (JT_STRING == json_root->get_type()) {
    std::string value_str(json_root->get_string().ptr(), static_cast<size_t>(json_root->get_string().length()));
    kv_map_.insert(std::pair<std::string, std::string>(key, value_str));
  } else if (JT_OBJECT == json_root->get_type()) {
    std::map<std::string, std::string> tmp_map;
    std::string inner_key;
    std::string inner_val;
    json::Value * config_value = NULL;
    ObString tmp_key( key.length(), key.c_str());
    DLIST_FOREACH(it1, json_root->get_object()) {
      config_value = it1->value_;
      inner_key = std::string(it1->name_.ptr(), it1->name_.length());
      if (NULL != config_value && JT_STRING == config_value->get_type()) {
        inner_val = std::string(config_value->get_string().ptr(), config_value->get_string().length());
        LOG_INFO("get value ", K(it1->name_), K(config_value->get_string()));
        tmp_map.insert(std::pair<std::string, std::string>(inner_key, inner_val));
      } else if (NULL != config_value && JT_OBJECT == config_value->get_type()) {
        if (tmp_key.case_compare(SHARDS_ZONE_PROPERTIES) == 0) {
          if (OB_FAIL(do_handle_connection_prop(*config_value, inner_key))) {
            LOG_WARN("handle zone prop failed", K(ret));
          }
        } else {
          LOG_DEBUG("no need handle prop", K(it1->name_));
        }
      } else {
        LOG_WARN("null value");
      }
    }
    kv_kv_map_.insert(std::pair<std::string, std::map<std::string, std::string> >(key, tmp_map));
  } else if (JT_ARRAY == json_root->get_type()) {
    std::vector<std::string> tmp_vec;
    std::string inner_val;
    const json::Value * config_value = NULL;
    DLIST_FOREACH(it1, json_root->get_array()) {
      config_value = it1;
      if (NULL != config_value && JT_STRING == config_value->get_type()) {
        inner_val = std::string(config_value->get_string().ptr(), config_value->get_string().length());
        tmp_vec.push_back(inner_val);
        LOG_INFO("get value ", K(config_value->get_string()));
      } else if (NULL != config_value) {
        LOG_WARN("type not supported", K(config_value->get_type()));
      } else {
        LOG_WARN("null value");
      }
    }
    kv_vec_map_.insert(std::pair<std::string, std::vector<std::string> >(key, tmp_vec));
  } else {
    LOG_WARN("type not supported", K(json_root->get_type()));
  }
  return ret;
}

//------ ObDbConfigLogicTenant----
void ObDbConfigLogicTenant::destroy()
{
  LOG_INFO("logic tenant will be destroyed", KPC(this));
  LDHashMap::iterator end = ld_map_.end();
  LDHashMap::iterator tmp_iter;
  for (LDHashMap::iterator iter = ld_map_.begin(); iter != end;) {
    tmp_iter = iter;
    ++iter;
    tmp_iter->dec_ref();
  }
  ld_map_.reset();
  op_free(this);
}

int64_t ObDbConfigLogicTenant::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(tenant_name));
  LDHashMap &map = const_cast<LDHashMap &>(ld_map_);
  for (LDHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "logic_db", *it);
  }
  J_OBJ_END();
  return pos;
}

int ObDbConfigLogicTenant::get_file_name(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t len = snprintf(buf, buf_len, "%s.json", get_type_task_name(type_));
  if (OB_UNLIKELY(len <=0) || OB_UNLIKELY(len > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get file name for tenant config", KPC(this), K(ret));
  }
  return ret;
}

int ObDbConfigLogicTenant::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":[",
                       CONFIG_VERSION, version_.ptr(),
                       TENANT_NAME, tenant_name_.ptr(),
                       TENANT_DATABASES);
  LDHashMap &map = const_cast<LDHashMap &>(ld_map_);
  int64_t i = 0;
  for (LDHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
    if (OB_FAIL(buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\"}",
                               LOGIC_DB_NAME, it->db_name_.ptr(),
                               CONFIG_VERSION, it->version_.ptr()))) {
      LOG_WARN("fail to get json str for db version", K(ret), K(*it));
    } else if (i < map.count() - 1) {
      ret = buf.append(",");
    }
    ++i;
  }
  if (OB_SUCC(ret)) {
    ret = buf.append("]}");
  }
  return ret;
}

int ObDbConfigLogicTenant::parse_from_json(const Value &json_value)
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicTenant *cur_tenant_info = NULL;
  bool is_new_tenant = false;
  bool is_new_tenant_version = false;
  Value *db_array = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == TENANT_NAME) {
        if (0 != tenant_name_.config_string_.case_compare(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant name is mismatched", K(tenant_name_),
                   K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == CONFIG_VERSION) {
        version_.set_value(it->value_->get_string());
      } else if (it->name_ == TENANT_DATABASES) {
        db_array = it->value_;
      }
    }
  }
  if (OB_SUCC(ret) && !version_.empty()) {
    if (OB_ISNULL(cur_tenant_info = dbconfig_cache.get_exist_tenant(tenant_name_.config_string_))) {
      CWLockGuard guard(dbconfig_cache.rwlock_);
      if (OB_FAIL(dbconfig_cache.lt_map_.unique_set(this))) {
        LOG_WARN("fail to add new tenant", K(tenant_name_), K(version_), K(ret));
      } else {
        inc_ref();
        is_new_tenant_version = true;
        is_new_tenant = true;
      }
    } else if (cur_tenant_info->is_version_changed(version_.config_string_)) {
      is_new_tenant_version = true;
    }
  }
  if (OB_SUCC(ret) && is_new_tenant_version && NULL != db_array) {
    if (OB_FAIL(load_local_db(*db_array))) {
      LOG_WARN("fail to load local db config", K(tenant_name_), K(version_), K(ret));
      if (is_new_tenant) {
        ObDbConfigLogicTenant *tmp_tenant = NULL;
        CWLockGuard guard(dbconfig_cache.rwlock_);
        tmp_tenant = dbconfig_cache.lt_map_.remove(tenant_name_.config_string_);
        if (NULL != tmp_tenant) {
          tmp_tenant->dec_ref(); // remember to remove new add tenant info
          tmp_tenant = NULL;
        }
      }
    } else {
      if (!is_new_tenant) {
        CWLockGuard guard(dbconfig_cache.rwlock_);
        cur_tenant_info->set_version(version_.config_string_);
      }
      LOG_INFO("succ to load db from local", K(tenant_name_), K(version_));
    }
  } else {
    LOG_DEBUG("the same tenant version, no need parse", K(version_));
  }
  if (NULL != cur_tenant_info) {
    cur_tenant_info->dec_ref();
    cur_tenant_info = NULL;
  }
  return ret;
}

int ObDbConfigLogicTenant::load_local_db(const Value &json_value)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = tenant_name_.config_string_;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  if (JT_ARRAY == json_value.get_type()) {
    ObDbConfigLogicDb *db_info = NULL;
    ObDbConfigLogicDb *new_db_info = NULL;
    ObString db_name;
    ObString db_version;
    bool is_new_db = false;
    bool is_new_db_version = false;

    hash::ObHashSet<ObString> new_db_names;
    bool is_need_delete_db = false;
    const ObString runtime_env = get_global_proxy_config().runtime_env.str();

    if (0 == runtime_env.case_compare(OB_PROXY_DBP_RUNTIME_ENV)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = new_db_names.create(BUCKET_SIZE, ObModIds::OB_PROXY_SHARDING_CONFIG,
                                                                   ObModIds::OB_PROXY_SHARDING_CONFIG)))) {
        LOG_WARN("hash set init failed", K(tmp_ret));
        is_need_delete_db = false;
      } else {
        is_need_delete_db = true;
      }
    }

    DLIST_FOREACH(it, json_value.get_array()) {
      new_db_info = NULL;
      db_info = NULL;
      db_name.reset();
      db_version.reset();
      is_new_db = false;
      is_new_db_version = false;
      if (JT_OBJECT == it->get_type()) {
        DLIST_FOREACH(p, it->get_object()) {
          if (p->name_ == LOGIC_DB_NAME) {
            db_name = p->value_->get_string();
          } else if (p->name_ == CONFIG_VERSION) {
            db_version = p->value_->get_string();
          }
        } // end loop JT_OBJECT
      } // end JT_OBJ
      if (!db_name.empty() && !db_version.empty()) {
        if (OB_ISNULL(db_info = dbconfig_cache.get_exist_db_info(tenant_name, db_name))) {
          is_new_db_version = true;
          is_new_db = true;
        } else if (db_info->is_version_changed(db_version)) {
          is_new_db_version = true;
        }

        if (is_need_delete_db) {
          int tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = new_db_names.set_refactored(db_name)))) {
            if (OB_UNLIKELY(OB_HASH_EXIST != tmp_ret)) {
              LOG_WARN("fail to add db name", K(db_name), K(tmp_ret));
              is_need_delete_db = false;
            }
          }
        }
      }
      if (is_new_db_version) {
        ObDataBaseKey db_info_key;
        db_info_key.tenant_name_.set_value(tenant_name);
        db_info_key.database_name_.set_value(db_name);
        bool is_from_local = true;
        if (OB_FAIL(ObDbConfigChild::alloc_child_info(db_info_key, TYPE_DATABASE, db_name,
                                                      db_version, reinterpret_cast<ObDbConfigChild *&>(new_db_info)))) {
          LOG_WARN("fail to alloc  ObDbConfigLogicDb", K(db_name), K(db_version), K(ret));
        } else {
          new_db_info->set_db_name(db_name);
          if (is_new_db) {
            new_db_info->set_building_state();
          } else {
            new_db_info->set_updating_state();
          }
          if (OB_FAIL(ObProxyPbUtils::parse_local_child_config(*new_db_info))) {
            LOG_WARN("fail to load local db config", K(db_name), K(db_version), K(ret));
          } else if (OB_FAIL(get_global_dbconfig_cache().handle_new_db_info(*new_db_info, is_from_local))) {
            // monitor local config file, allow some logic db success, some logic db fail
            // if some logic db failed, will not load to memory and go on to pasre next logic db
            const bool start_mode = !get_global_db_config_processor().is_config_inited();
            LOG_WARN("fail to handle new db info", K(start_mode), K(db_name), K(db_version), K(ret));
            if (!start_mode) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
      if (NULL != db_info) {
        db_info->dec_ref();
        db_info = NULL;
      }
      if (NULL != new_db_info) {
        new_db_info->dec_ref();
        new_db_info = NULL;
      }
    } // end loop array

    if (OB_SUCC(ret) && is_need_delete_db) {
      ObDbConfigLogicTenant *cur_tenant_info = NULL;
      if (OB_ISNULL(cur_tenant_info = dbconfig_cache.get_exist_tenant(tenant_name))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("logic tenant does not exist", K(tenant_name), K(ret));
      } else {
        obsys::CWLockGuard guard(dbconfig_cache.rwlock_);
        ObDbConfigLogicTenant::LDHashMap &ld_map = const_cast<ObDbConfigLogicTenant::LDHashMap &>(cur_tenant_info->ld_map_);
        ObDbConfigLogicTenant::LDHashMap::iterator end = ld_map.end();
        ObDbConfigLogicTenant::LDHashMap::iterator tmp_iter;
        for (ObDbConfigLogicTenant::LDHashMap::iterator iter = ld_map.begin(); iter != end;) {
          if (OB_HASH_NOT_EXIST == new_db_names.exist_refactored(iter->db_name_.config_string_)) {
            tmp_iter = iter;
            ++iter;
            ld_map.remove(&(*tmp_iter));
            tmp_iter->dec_ref();
          } else {
            ++iter;
          }
        }
      }

      if (NULL != cur_tenant_info) {
        cur_tenant_info->dec_ref();
        cur_tenant_info = NULL;
      }
    }
  }
  return ret;
}

int ObDbConfigLogicTenant::acquire_random_logic_db(ObDbConfigLogicDb *&db_info)
{
  db_info = NULL;
  int64_t idx = 0;
  int ret = OB_SUCCESS;
  const int64_t max_idx = ld_map_.count() - 1;
  if (OB_FAIL(ObRandomNumUtils::get_random_num(0, max_idx, idx))) {
    LOG_DEBUG("fail to get random num", K(idx), K(max_idx), K(ret));
  } else {
    ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
    obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
    LDHashMap::iterator begin = ld_map_.begin();
    LDHashMap::iterator end = ld_map_.end();
    int64_t i = 0;
    while (i < idx && begin != end) {
      ++begin;
      ++i;
    }
    if (begin == end) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it arrived the end, it should not happened", K(idx), K(i), K(ret));
    } else {
      db_info = &(*begin);
      db_info->inc_ref();
    }
  }
  return ret;
}

int ObDbConfigLogicTenant::acquire_random_logic_db_with_auth(
                           const ObString &username,
                           const ObString &host,
                           ObShardUserPrivInfo &up_info,
                           ObDbConfigLogicDb *&db_info)
{
  int ret = OB_SUCCESS;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  LDHashMap::iterator end = ld_map_.end();
  bool found = false;
  for (LDHashMap::iterator it = ld_map_.begin(); !found && it != end; ++it) {
    if (OB_FAIL(it->get_user_priv_info(username, host, up_info))) {
      LOG_DEBUG("user priv info does not exist", K(username), K(host));
    } else {
      found = true;
      db_info = &(*it);
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to get random logic db", K(username), K(host), K(ret));
  } else if (NULL != db_info) {
    db_info->inc_ref();
  }
  return ret;
}

//------ ObDbConfigLogicDb------
void ObDbConfigLogicDb::destroy()
{
  dc_state_ = DC_DEAD;
  LOG_INFO("logic db info will be destroyed", KPC(this));
  da_array_.destroy();
  dp_array_.destroy();
  dv_array_.destroy();
  st_array_.destroy();
  sd_array_.destroy();
  sr_array_.destroy();
  sc_array_.destroy();
  sp_array_.destroy();
  op_free(this);
}

int64_t ObDbConfigLogicDb::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(db_name),
       K_(version),
       K_(testload_prefix),
       K_(da_array),
       K_(dp_array),
       K_(dv_array),
       K_(st_array),
       K_(sr_array),
       K_(sd_array),
       K_(sc_array),
       K_(sp_array));
  J_OBJ_END();
  return pos;
}

int ObDbConfigLogicDb::get_file_name(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t len = snprintf(buf, buf_len, "%s.json", get_type_task_name(type_));
  if (OB_UNLIKELY(len <=0) || OB_UNLIKELY(len > buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get file name for database config", KPC(this), K(ret));
  }
  return ret;
}

int ObDbConfigLogicDb::update_conn_prop_with_shard_dist()
{
  int ret = OB_SUCCESS;
  ObDbConfigChildArrayInfo<ObShardProp>::CCRHashMap::iterator it = sp_array_.ccr_map_.begin();
  ObDbConfigChildArrayInfo<ObShardDist>::CCRHashMap::iterator it2 = sd_array_.ccr_map_.begin();
  ObShardDist* shard_dist = NULL;
  ObMarkedDist* mark_dist = NULL;
  for (;OB_SUCC(ret) && it != sp_array_.ccr_map_.end(); ++it) {
    ObShardProp& shard_prop = (ObShardProp&)(*it);
    ObString& shard_name = shard_prop.shard_name_.config_string_;
    for (; OB_SUCC(ret) && it2 != sd_array_.ccr_map_.end(); ++it2) {
      shard_dist = &(*it2);
      if (OB_ISNULL(mark_dist = shard_dist->md_map_.get(shard_name))) {
        LOG_DEBUG("get from map failed", K(shard_name));
      } else {
        shard_prop.current_zone_.set_value(mark_dist->dist_.config_string_);
      }
    }
    if (OB_FAIL(shard_prop.set_conn_props_from_conf())) {
      LOG_WARN("fail to set_conn_props_from_conf", K(shard_prop));
    } else {
      LOG_INFO("succ set values from conf", K(shard_prop));
    }
  }
  return ret;
}

int ObDbConfigLogicDb::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%s\", \"%s\":\"%s\", \
                       \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", ",
                       CONFIG_VERSION, version_.ptr(),
                       LOGIC_DB_CLUSTER, db_cluster_.ptr(),
                       LOGIC_DB_MODE, db_mode_.ptr(),
                       LOGIC_DB_NAME, db_name_.ptr(),
                       LOGIC_DB_TYPE, db_type_.ptr());
  if (OB_SUCC(ret)) {
    if (OB_FAIL(child_reference_to_json(da_array_, TYPE_DATABASE_AUTH, buf))) {
      LOG_WARN("fail to convert database auth reference to json");
    } else if (OB_FAIL(child_reference_to_json(dv_array_, TYPE_DATABASE_VAR, buf))) {
      LOG_WARN("fail to convert database variable reference to json");
    } else if (OB_FAIL(child_reference_to_json(dp_array_, TYPE_DATABASE_PROP, buf))) {
      LOG_WARN("fail to convert database prop reference to json");
    } else if (OB_FAIL(child_reference_to_json(st_array_, TYPE_SHARDS_TPO, buf, NULL, is_strict_spec_mode_))) {
      LOG_WARN("fail to convert shards topology to json");
    } else if (OB_FAIL(child_reference_to_json(sr_array_, TYPE_SHARDS_ROUTER, buf))) {
      LOG_WARN("fail to convert shards router reference to json");
    } else if (OB_FAIL(child_reference_to_json(sd_array_, TYPE_SHARDS_DIST, buf))) {
      LOG_WARN("fail to convert shards distribute reference to json");
    } else if (OB_FAIL(child_reference_to_json(sc_array_, TYPE_SHARDS_CONNECTOR, buf, testload_prefix_.ptr()))) {
      LOG_WARN("fail to convert shards connector reference to json");
    } else if (OB_FAIL(child_reference_to_json(sp_array_, TYPE_SHARDS_PROP, buf))) {
      LOG_WARN("fail to convert shards prop reference to json");
    } else if (OB_FAIL(buf.append_fmt("\"tenant_name\":\"%s\"}", db_info_key_.tenant_name_.ptr()))) {
      LOG_WARN("fail to append tenant name", K(db_info_key_.tenant_name_), K(ret));
    }
  }
  return ret;
}

template<typename T>
int ObDbConfigLogicDb::child_reference_to_json(const ObDbConfigChildArrayInfo<T> &cr_array,
                                            const ObDDSCrdType type,
                                            ObSqlString &buf,
                                            const char *testload_prefix_str/*NULL*/,
                                            const bool is_strict_spec_mode/*false*/)
{
  int ret = OB_SUCCESS;
  if (cr_array.ccr_map_.count() == 0) {
    // do nothing
  } else {
    ret = buf.append_fmt("\"%s\":{\"%s\":\"%s\", \"%s\":\"%s\", \"%s\":\"%s\", \"%s\":[",
                         get_child_cr_name(type),
                         REF_KIND, cr_array.ref_info_.kind_.ptr(),
                         REF_NAMESPACE, cr_array.ref_info_.namespace_.ptr(),
                         REF_PARENT, cr_array.ref_info_.parent_.ptr(), REF_VALUE);
    typename ObDbConfigChildArrayInfo<T>::CCRHashMap &map = const_cast<typename ObDbConfigChildArrayInfo<T>::CCRHashMap &>(cr_array.ccr_map_);
    typename ObDbConfigChildArrayInfo<T>::CCRHashMap::iterator it = map.begin();

    ObString testload_prefix(testload_prefix_str);
    // reference format: name.version
    for (; OB_SUCC(ret) && it != map.end(); ++it) {
      if (it->name_.config_string_.prefix_match(testload_prefix)
          && testload_prefix.length() > 0) {
      } else if (OB_FAIL(buf.append_fmt("\"%s.%s\",", it->name_.ptr(), it->version_.ptr()))) {
        LOG_WARN("fail to append child reference",
                 "name", it->name_.ptr(),
                 "version", it->version_.ptr(), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // trim last ','
      char *buf_start = buf.ptr();
      if (',' == buf_start[buf.length() - 1]) {
        buf_start[buf.length() - 1] = ']';
      } else {
        ret = buf.append("]");
      }
    }

    if (OB_SUCC(ret) && TYPE_SHARDS_TPO == type) {
      // append strict mode
      ret = buf.append_fmt(", \"%s\":\"%s\"", SHARDS_TPO_SPEC_MODE, is_strict_spec_mode ? STRICT_SPEC_MODE : NORMAL_SPEC_MODE);
    }

    if (OB_SUCC(ret) && TYPE_SHARDS_CONNECTOR == type
        && testload_prefix.length() > 0) {
      if (OB_FAIL(buf.append_fmt(", \"%s\":\"%s\", \"%s\":[",
                                 TESTLOAD_PREFIX, testload_prefix.ptr(), TESTLOAD_REF_VALUE))) {
        LOG_WARN("fail to append testload_prefix", K(testload_prefix), K(ret));
      } else {
        it = map.begin();
        for (; OB_SUCC(ret) && it != map.end(); ++it) {
          if (!it->name_.config_string_.prefix_match(testload_prefix)) {
          } else if (OB_FAIL(buf.append_fmt("\"%s.%s\",", it->name_.ptr(), it->version_.ptr()))) {
            LOG_WARN("fail to append child reference",
                     "name", it->name_.ptr(),
                     "version", it->version_.ptr(), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          // buf.ptr() may change, so need get buf.ptr() again
          char *buf_start = buf.ptr();
          if (',' == buf_start[buf.length() - 1]) {
            buf_start[buf.length() - 1] = ']';
          } else {
            ret = buf.append("]");
          }
        }
      }
    } // end TYPE_SHARDS_CONNECTOR == type
    if (OB_SUCC(ret)) {
      ret = buf.append_fmt("}, ");
    }
  }
  return ret;
}

int ObDbConfigLogicDb::parse_from_json(const Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *da_ref = NULL;
  Value *dv_ref = NULL;
  Value *dp_ref = NULL;
  Value *st_ref = NULL;
  Value *sr_ref = NULL;
  Value *sd_ref = NULL;
  Value *sc_ref = NULL;
  Value *sp_ref = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_VERSION) {
        if (is_version_changed(it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("db version is different between Database.json and  Tenant.json",
                   K(version_), K(it->value_->get_string()), K(ret));
        }
      } else if (it->name_ == LOGIC_DB_NAME) {
        db_name_.set_value(it->value_->get_string());
      } else if (it->name_ == LOGIC_DB_CLUSTER) {
        db_cluster_.set_value(it->value_->get_string());
      } else if (it->name_ == LOGIC_DB_MODE) {
        db_mode_.set_value(it->value_->get_string());
      } else if (it->name_ == LOGIC_DB_TYPE) {
        db_type_.set_value(it->value_->get_string());
      } else if (it->name_ == get_child_cr_name(TYPE_DATABASE_AUTH)) {
        da_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_DATABASE_VAR)) {
        dv_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_DATABASE_PROP)) {
        dp_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_SHARDS_TPO)) {
        st_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_SHARDS_ROUTER)) {
        sr_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_SHARDS_DIST)) {
        sd_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_SHARDS_CONNECTOR)) {
        sc_ref = it->value_;
      } else if (it->name_ == get_child_cr_name(TYPE_SHARDS_PROP)) {
        sp_ref = it->value_;
      }
    } //
  } // end JT_OBJECT
  if (OB_SUCC(ret) && NULL != da_ref) {
    ret = load_child_config(*da_ref, da_array_, TYPE_DATABASE_AUTH);
  }
  if (OB_SUCC(ret) && NULL != dv_ref) {
    ret = load_child_config(*dv_ref, dv_array_, TYPE_DATABASE_VAR);
  }
  if (OB_SUCC(ret) && NULL != dp_ref) {
    ret = load_child_config(*dp_ref, dp_array_, TYPE_DATABASE_PROP);
  }
  if (OB_SUCC(ret) && NULL != st_ref) {
    ret = load_child_config(*st_ref, st_array_, TYPE_SHARDS_TPO);
  }
  if (OB_SUCC(ret) && NULL != sr_ref) {
    ret = load_child_config(*sr_ref, sr_array_, TYPE_SHARDS_ROUTER);
  }
  if (OB_SUCC(ret) && NULL != sd_ref) {
    ret = load_child_config(*sd_ref, sd_array_, TYPE_SHARDS_DIST);
  }
  if (OB_SUCC(ret) && NULL != sc_ref) {
    ret = load_child_config(*sc_ref, sc_array_, TYPE_SHARDS_CONNECTOR);
  }
  if (OB_SUCC(ret) && NULL != sp_ref) {
    ret = load_child_config(*sp_ref, sp_array_, TYPE_SHARDS_PROP);
  }
  return ret;
}

template<typename T>
int ObDbConfigLogicDb::load_child_config(
                       const Value &json_value,
                       ObDbConfigChildArrayInfo<T> &cr_array,
                       const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  Value *cr_ref = NULL;
  Value *testload_ref = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == REF_KIND) {
        cr_array.ref_info_.kind_.set_value(it->value_->get_string());
      } else if (it->name_ == REF_NAMESPACE) {
        cr_array.ref_info_.namespace_.set_value(it->value_->get_string());
      } else if (it->name_ == REF_PARENT) {
        cr_array.ref_info_.parent_.set_value(it->value_->get_string());
      } else if (it->name_ == REF_VALUE) {
        cr_ref = it->value_;
      } else if (it->name_ == TESTLOAD_REF_VALUE) {
        testload_ref = it->value_;
      } else if (it->name_ == TESTLOAD_PREFIX) {
        testload_prefix_.set_value(it->value_->get_string());
      } else if (it->name_ == SHARDS_TPO_SPEC_MODE) {
        is_strict_spec_mode_ = 0 == it->value_->get_string().case_compare(STRICT_SPEC_MODE);
      }
    } // end loop object
  } // end JT_OBJECT

  if (OB_SUCC(ret) && NULL != cr_ref) {
    if (OB_FAIL(load_child_array(*cr_ref, cr_array, type))) {
      LOG_WARN("fail to load child config", "type", get_type_task_name(type), K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != testload_ref) {
    if (OB_FAIL(load_child_array(*testload_ref, cr_array, type))) {
      LOG_WARN("fail to load testload child config", "type", get_type_task_name(type), K(ret));
    }
  }
  return ret;
}

template<typename T>
int ObDbConfigLogicDb::load_child_array(
                       const Value &json_value,
                       ObDbConfigChildArrayInfo<T> &cr_array,
                       const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  ObString reference;
  ObString cr_name;
  ObString cr_version;
  ObDbConfigChild *child_info = NULL;
  ObDbConfigChild *new_child_info = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  const char *start_pos = NULL;
  const char *sep_pos = NULL;
  const char *last_sep_pos = NULL;
  bool is_new_child_version = false;
  if (JT_ARRAY == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_array()) {
      child_info = NULL;
      new_child_info = NULL;
      cr_name.reset();
      cr_version.reset();
      reference.reset();
      start_pos = NULL;
      sep_pos = NULL;
      last_sep_pos = NULL;
      if (JT_STRING == it->get_type()) {
        // format: name.version, name.region.version
        reference = it->get_string();
        start_pos = reference.ptr();
        while (NULL != (sep_pos = reference.find('.'))) {
          last_sep_pos = sep_pos;
          reference.split_on(sep_pos);
        }
        if (OB_ISNULL(start_pos) || OB_ISNULL(last_sep_pos))  {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid reference", K(reference), K(ret));
        } else {
          cr_name.assign_ptr(start_pos, static_cast<ObString::obstr_size_t>(last_sep_pos - start_pos));
          cr_version = reference;
        }
      }
      if (OB_SUCC(ret) && (OB_UNLIKELY(cr_name.empty()) || OB_UNLIKELY(cr_version.empty()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid reference", K(reference), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObDbConfigChild::alloc_child_info(db_info_key_, type, cr_name, cr_version, new_child_info))) {
          LOG_WARN("fail to alloc child info", "type", get_type_task_name(type), K(cr_name), K(cr_version), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        child_info = dbconfig_cache.get_child_info(db_info_key_, cr_name, type);
        if (NULL == child_info) {
          is_new_child_version = true;
        } else if (child_info->is_version_changed(cr_version)) {
          is_new_child_version = true;
        }
        if (is_new_child_version) {
          if (OB_FAIL(ObProxyPbUtils::parse_local_child_config(*new_child_info))) {
            LOG_WARN("fail to load local child info", K(db_info_key_), K(cr_name), K(cr_version), K(ret));
          } else if (TYPE_SHARDS_CONNECTOR == type) {
            ObShardConnector *shard_conn = reinterpret_cast<ObShardConnector *>(new_child_info);
            if (shard_conn->is_enc_beyond_trust()) {
              set_need_update_bt();
            }
          }
        } else if (OB_FAIL(new_child_info->assign(*child_info))) {
          LOG_WARN("fail to copy child info", K(db_info_key_), K(cr_name), K(cr_version), K(ret));
        }
        if (OB_SUCC(ret)) {
          new_child_info->set_avail_state();
          if (OB_FAIL(cr_array.ccr_map_.unique_set(reinterpret_cast<T *>(new_child_info)))) {
            LOG_WARN("fail to add child info into cr array", K(db_info_key_), K(cr_name), K(cr_version), K(ret));
          }
        }
      }
      if (NULL != child_info) {
        child_info->dec_ref();
        child_info = NULL;
      }
      if (OB_FAIL(ret) && NULL != new_child_info) {
        new_child_info->dec_ref();
        new_child_info = NULL;
      }
    } // end loop array
  } // end JT_ARRAY
  return ret;
}

int ObDbConfigLogicDb::acquire_random_shard_connector(ObShardConnector *&shard_conn)
{
  ObString gc_name;
  return acquire_random_shard_connector(shard_conn, gc_name);
}

int ObDbConfigLogicDb::acquire_random_shard_connector(ObShardConnector *&shard_conn, ObString &gc_name)
{
  int ret = OB_SUCCESS;
  shard_conn = NULL;
  int64_t idx = 0;
  int64_t max_idx = 0;
  ObShardTpo *shard_tpo = NULL;

  if (OB_FAIL(get_shard_tpo(shard_tpo))) {
    LOG_WARN("fail to get shard tpo info", K(ret));
  } else if (OB_ISNULL(shard_tpo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard tpo info is null", K(ret));
  } else if (FALSE_IT(max_idx = shard_tpo->gc_map_.count() - 1)) {
    //nerver here
  } else if (OB_FAIL(ObRandomNumUtils::get_random_num(0, max_idx, idx))) {
    LOG_WARN("fail to get random num", K(idx), K(max_idx), K_(sc_array), K(ret));
  } else {
    ObShardTpo::GCHashMap::iterator begin = shard_tpo->gc_map_.begin();
    ObShardTpo::GCHashMap::iterator end = shard_tpo->gc_map_.end();
    int64_t i = 0;
    while (i < idx && begin != end) {
      ++begin;
      ++i;
    }
    if (begin == end) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it arrived the end, it should not happened", K(idx), K(i), K(ret));
    } else {
      int64_t eid = 0;
      ObString shard_name;
      ObGroupCluster *gc_info = &(*begin);
      if (OB_FAIL(gc_info->get_elastic_id_by_read_weight(eid))) {
        LOG_WARN("fail to get eid by read weight", KPC(gc_info));
      } else if (FALSE_IT(shard_name = gc_info->get_shard_name_by_eid(eid))) {
        //nerver here
      } else if (OB_FAIL(get_shard_connector(shard_name, shard_conn))) {
        LOG_WARN("shard connector does not exist", K(shard_name), K(ret));
      } else {
        gc_name = gc_info->gc_name_.config_string_;
        LOG_INFO("succ to get random shard connector", KPC(shard_conn));
      }
    }
  }

  if (NULL != shard_tpo) {
    shard_tpo->dec_ref();
    shard_tpo = NULL;
  }

  return ret;
}

bool ObDbConfigLogicDb::is_single_shard_db_table()
{
  bool bret = false;
  ObShardTpo *shard_tpo = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_shard_tpo(shard_tpo))) {
    LOG_WARN("fail to get shard tpo info", K(ret));
  } else if (OB_ISNULL(shard_tpo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard tpo info is null", K(ret));
  } else if (shard_tpo->gc_map_.count() == 1) {
    bret = is_shard_rule_empty();
  }
  if (NULL != shard_tpo) {
    shard_tpo->dec_ref();
    shard_tpo = NULL;
  }
  return bret;
}

bool ObDbConfigLogicDb::enable_remote_connection()
{
  bool bret = true;
  int ret = OB_SUCCESS;
  ObDataBaseVar *db_var = NULL;
  if (OB_FAIL(get_database_var(db_var))) {
    LOG_DEBUG("database var info not exist", K(ret));
  } else if (OB_ISNULL(db_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database variable info is null", K(ret));
  } else {
    bret = db_var->enable_remote_connection();
  }
  if (NULL != db_var) {
    db_var->dec_ref();
    db_var = NULL;
  }
  return bret;
}

int ObDbConfigLogicDb::init_test_load_table_map()
{
  int ret = OB_SUCCESS;
  ObDataBaseVar *db_var = NULL;
  if (OB_FAIL(get_database_var(db_var))) {
    LOG_DEBUG("database var info not exist", K(ret));
  } else if (OB_ISNULL(db_var)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("database variable info is null", K(ret));
  } else if (OB_FAIL(db_var->init_test_load_table_map(test_load_table_map_))) {
    LOG_WARN("database variable of testloadTableMap is unexpected", K(ret));
  }
  if (NULL != db_var) {
    db_var->dec_ref();
    db_var = NULL;
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_ERR_NULL_DB_VAL_TESTLOAD_TABLE_MAP;
  }
  return ret;
}

int ObDbConfigLogicDb::check_and_get_testload_table(std::string origin_name, std::string &real_name)
{
  int ret = OB_SUCCESS;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  std::string find_key;
  if (test_load_table_map_.size() == 0) {
    obsys::CWLockGuard guard(dbconfig_cache.rwlock_);
    if (test_load_table_map_.size() == 0 && OB_FAIL(init_test_load_table_map())) {
      LOG_WARN("init testload table map error", K(ret), K(db_name_));
    }
  }

  if (OB_MAX_TABLE_NAME_LENGTH <= origin_name.length()) {
    ret = OB_ERR_UNEXPECTED; //not to be here
    LOG_WARN("size need to check is big than OB_MAX_TABLE_NAME_LENGTH", K(origin_name.length()),
      K(OB_MAX_TABLE_NAME_LENGTH));
  }

  if (OB_SUCC(ret)) {
    // convert all origin_name to upper str to compare
    char buf[OB_MAX_TABLE_NAME_LENGTH];
    int len = static_cast<int>(origin_name.length());
    memcpy(buf, origin_name.c_str(), len);
    string_to_upper_case(buf, len);
    buf[len] = '\0';
    find_key = std::string(buf, len);

    obsys::CRLockGuard guardr(dbconfig_cache.rwlock_); //change to r-lock
    std::map<std::string, std::string>::iterator iter;
    iter = test_load_table_map_.find(find_key);
    if (iter != test_load_table_map_.end()) {
      real_name = iter->second;
      LOG_DEBUG("check_and_get_testload_table find table", K(ret),
                "origin_name", ObString::make_string(origin_name.c_str()),
                "real_name", ObString::make_string(real_name.c_str()));
    } else {
      real_name = origin_name; //use origin name
      LOG_DEBUG("check_and_get_testload_table not find table", K(ret),
                "origin_name", ObString::make_string(origin_name.c_str()));
    }
  }
  return ret;
}

int ObDbConfigLogicDb::get_sequence_param(ObSequenceParam& param)
{
  int ret = OB_SUCCESS;
  ObDataBaseVar *db_var = NULL;
  if (OB_FAIL(get_database_var(db_var))) {
    LOG_DEBUG("database var info not exist", K(ret));
  } else if (OB_ISNULL(db_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database variable info is null", K(ret));
  } else {
    ObString tmp_str;
    int64_t tmp_value;
    tmp_str = db_var->get_variable_value(SEQUENCE_MIN_VALUE);
    if (OB_SUCC(get_int_value(tmp_str, tmp_value))) {
      param.min_value_ = tmp_value;
    }
    tmp_str = db_var->get_variable_value(SEQUENCE_MAX_VALUE);
    if (OB_SUCC(get_int_value(tmp_str, tmp_value))) {
      param.max_value_ = tmp_value;
    }
    tmp_str = db_var->get_variable_value(SEQUENCE_STEP);
    if (OB_SUCC(get_int_value(tmp_str, tmp_value))) {
      param.step_ = tmp_value;
    }
    tmp_str = db_var->get_variable_value(SEQUENCE_RETRY_COUNT);
    if (OB_SUCC(get_int_value(tmp_str, tmp_value))) {
      param.retry_count_ = tmp_value;
    }
    tmp_str = db_var->get_variable_value(SEQUENCE_TNT_ID_COL);
    param.tnt_col_.set_value(tmp_str);
    tmp_str = db_var->get_variable_value(SEQUENCE_ENABLE);
    if (!tmp_str.empty() && (0 != tmp_str.case_compare("true"))) {
      param.is_sequence_enable_ = false;
    }
  }
  if (NULL != db_var) {
    db_var->dec_ref();
    db_var = NULL;
  }
  //ignore not exist param,will always return succ
  return OB_SUCCESS;
}

int ObDbConfigLogicDb::get_real_info(const common::ObString &table_name,
                                     ObSqlParseResult &parse_result,
                                     ObShardConnector *&shard_conn,
                                     char *real_database_name, int64_t db_name_len,
                                     char *real_table_name, int64_t tb_name_len,
                                     int64_t &group_index, int64_t &tb_index, int64_t &es_index,
                                     const ObString &hint_table, ObTestLoadType testload_type, bool is_read_stmt)
{
  int ret = OB_SUCCESS;

  if (is_single_shard_db_table()) {
    if (OB_FAIL(get_single_table_info(table_name, shard_conn,
                                      real_database_name, db_name_len, real_table_name,
                                      tb_name_len, group_index, tb_index, es_index,
                                      hint_table, testload_type, is_read_stmt))) {
      LOG_WARN("fail to get single table info", K(table_name), K(ret));
    }
  } else {
    SqlFieldResult &sql_result = parse_result.get_sql_filed_result();
    if (OB_FAIL(get_shard_table_info(table_name, sql_result, shard_conn,
                                     real_database_name, db_name_len,
                                     real_table_name, tb_name_len,
                                     group_index, tb_index, es_index,
                                     hint_table, testload_type, is_read_stmt))) {
      LOG_WARN("fail to get shard table info", K(table_name), K(ret));
    }
  }

  return ret;
}

int ObDbConfigLogicDb::get_real_table_name(const ObString &table_name, SqlFieldResult &sql_result,
                                           char *real_table_name, int64_t tb_name_len, int64_t &tb_index,
                                           const ObString &hint_table, ObTestLoadType testload_type)
{
  int ret = OB_SUCCESS;

  ObShardRule *logic_tb_info = NULL;

  if (!hint_table.empty()) {
    snprintf(real_table_name, tb_name_len, "%.*s", static_cast<int>(hint_table.length()), hint_table.ptr());
  } else if (OB_FAIL(get_shard_rule(logic_tb_info, table_name))) {
    LOG_WARN("fail to get shard rule", K(table_name), K(ret));
  } else if (OB_FAIL(ObShardRule::get_physic_index(sql_result, logic_tb_info->tb_rules_,
                                                   logic_tb_info->tb_size_, testload_type, tb_index))) {
    LOG_WARN("fail to get physic tb index", K(table_name), KPC(logic_tb_info), K(ret));
  } else if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->tb_size_, logic_tb_info->tb_suffix_len_,
                                                           tb_index, logic_tb_info->tb_prefix_.config_string_,
                                                           logic_tb_info->tb_tail_.config_string_, real_table_name, tb_name_len))) {
    LOG_WARN("fail to get real table name", K(tb_index), KPC(logic_tb_info), K(ret));
  } else if (TESTLOAD_NON != testload_type) {
    snprintf(real_table_name + strlen(real_table_name), tb_name_len - strlen(real_table_name), "_T");
  }

  return ret;
}

int ObDbConfigLogicDb::get_shard_table_info(const ObString &table_name,
                                            SqlFieldResult &sql_result,
                                            ObShardConnector *&shard_conn,
                                            char *real_database_name, int64_t db_name_len,
                                            char *real_table_name, int64_t tb_name_len,
                                            int64_t &group_index, int64_t &tb_index, int64_t &es_index,
                                            const ObString &hint_table, ObTestLoadType testload_type,
                                            const bool is_read_stmt,
                                            const int64_t last_es_index /*OBPROXY_MAX_DBMESH_ID*/)
{
  int ret = OB_SUCCESS;

  ObShardRule *logic_tb_info = NULL;
  ObShardTpo *shard_tpo = NULL;
  ObGroupCluster *gc_info = NULL;

  if (OB_FAIL(get_shard_tpo(shard_tpo))) {
    LOG_WARN("fail to get shard tpo info", K(ret));
  } else if (OB_ISNULL(shard_tpo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard tpo info is null", K(ret));
  } else if (OB_FAIL(get_shard_rule(logic_tb_info, table_name))) {
    LOG_WARN("fail to get shard rule", K(table_name), K(ret));
  // get group_id
  } else if (OB_FAIL(ObShardRule::get_physic_index(sql_result, logic_tb_info->db_rules_,
                                                   logic_tb_info->db_size_, testload_type, group_index))) {
    LOG_WARN("fail to get physic db index", K(table_name), KPC(logic_tb_info), K(ret));
  // reuse real_database_name to store group name instead of using a new buffer
  // real_database_name will be set after shard connector is chosen
  // get group_name
  } else  if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->db_size_, logic_tb_info->db_suffix_len_, group_index,
                                                            logic_tb_info->db_prefix_.config_string_,
                                                            logic_tb_info->db_tail_.config_string_, real_database_name, db_name_len))) {
    LOG_WARN("fail to get real group name", K(group_index), KPC(logic_tb_info), K(ret));
  } else if (OB_FAIL(shard_tpo->get_group_cluster(ObString::make_string(real_database_name), gc_info))) {
    LOG_DEBUG("group does not exist", "phy_db_name", real_database_name, K(ret));
  } else if (OB_ISNULL(gc_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group cluster info is null", "phy_db_name", real_database_name, K(ret));
  } else {
    // calculate es_id
    int64_t es_size = gc_info->get_es_size();
    bool is_elastic_index = true;
    ObString shard_name;
    if (-1 == es_index || (OBPROXY_MAX_DBMESH_ID == es_index && logic_tb_info->es_rules_.empty())) {
      if (is_read_stmt
          && (last_es_index >= 0)
          && (last_es_index < es_size)) {
        //In multiple statements, the read request can use the effective es id of the previous statement
        es_index = last_es_index;
      } else if (OB_FAIL(gc_info->get_elastic_id_by_weight(es_index, is_read_stmt))) {
        LOG_WARN("fail to get eid by read weight", KPC(gc_info));
      } else {
        LOG_DEBUG("succ to get eid by weight", K(es_index));
      }
    } else if (OB_FAIL(ObShardRule::get_physic_index(sql_result, logic_tb_info->es_rules_, es_size,
                    testload_type, es_index, is_elastic_index))) {
            LOG_WARN("fail to calculate elastic index", K(table_name), KPC(logic_tb_info), K(ret));
    } else if (OB_UNLIKELY(es_index >= es_size)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("es index is larger than elastic array", K(es_index), K(es_size), K(ret));
    }

    if (OB_SUCC(ret)) {
      shard_name = gc_info->get_shard_name_by_eid(es_index);
      if (TESTLOAD_NON != testload_type) {
        if (OB_FAIL(get_testload_shard_connector(shard_name, testload_prefix_.config_string_, shard_conn))) {
          LOG_WARN("testload shard connector not exist", "testload_type", get_testload_type_str(testload_type),
                   K(shard_name), K(testload_prefix_), K(ret));
        }
      } else if (OB_FAIL(get_shard_connector(shard_name, shard_conn))) {
        LOG_WARN("shard connector does not exist", K(shard_name), K(ret));
      }
      // get physical schema name
      if (OB_SUCC(ret) && NULL != shard_conn) {
        snprintf(real_database_name, db_name_len, "%.*s", shard_conn->database_name_.length(), shard_conn->database_name_.ptr());
      }
    }

    if (OB_SUCC(ret)) {
      // calculate table_name
      if (!hint_table.empty()) {
        snprintf(real_table_name, tb_name_len, "%.*s", static_cast<int>(hint_table.length()), hint_table.ptr());
      } else {
        if (OB_FAIL(ObShardRule::get_physic_index(sql_result, logic_tb_info->tb_rules_,
                        logic_tb_info->tb_size_, testload_type, tb_index))) {
          LOG_WARN("fail to get physic tb index", K(table_name), KPC(logic_tb_info), K(ret));
        } else if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->tb_size_, logic_tb_info->tb_suffix_len_,
                      tb_index, logic_tb_info->tb_prefix_.config_string_,
                      logic_tb_info->tb_tail_.config_string_, real_table_name, tb_name_len))) {
          LOG_WARN("fail to get real table name", K(tb_index), KPC(logic_tb_info), K(ret));
        }

        if (OB_SUCC(ret) && TESTLOAD_NON != testload_type) {
          snprintf(real_table_name + strlen(real_table_name), tb_name_len - strlen(real_table_name), "_T");
        }
      }
    }
  }

  if (NULL != shard_tpo) {
    shard_tpo->dec_ref();
    shard_tpo = NULL;
  }
  return ret;
}

int ObDbConfigLogicDb::get_single_real_table_info(const common::ObString &table_name,
                                                  const ObShardConnector &shard_conn,
                                                  char *real_database_name, int64_t db_name_len,
                                                  char *real_table_name, int64_t tb_name_len,
                                                  const common::ObString &hint_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(real_database_name) || OB_ISNULL(real_table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error argument to init real db and table info", K(ret));
  } else {
    if (!hint_table.empty()) {
      snprintf(real_table_name, tb_name_len, "%.*s", hint_table.length(), hint_table.ptr());
    } else {
      snprintf(real_table_name, tb_name_len, "%.*s", table_name.length(), table_name.ptr());
    }
    snprintf(real_database_name, db_name_len, "%.*s", shard_conn.database_name_.length(), shard_conn.database_name_.ptr());
  }
  return ret;
}

int ObDbConfigLogicDb::get_single_table_info(const ObString &table_name,
                                             ObShardConnector *&shard_conn,
                                             char *real_database_name, int64_t db_name_len,
                                             char *real_table_name, int64_t tb_name_len,
                                             int64_t &group_id, int64_t &table_id, int64_t &es_id,
                                             const ObString &hint_table, ObTestLoadType testload_type,
                                             const bool is_read_stmt, const int64_t last_es_id/*OBPROXY_MAX_DBMESH_ID*/)
{
  int ret = OB_SUCCESS;

  ObShardTpo *shard_tpo = NULL;

  group_id = 0;
  table_id = 0;
  if (OB_FAIL(get_shard_tpo(shard_tpo))) {
    LOG_WARN("fail to get shard tpo info", K(ret));
  } else if (OB_ISNULL(shard_tpo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard tpo info is null", K(ret));
  } else if (OB_UNLIKELY(shard_tpo->gc_map_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("shard tpo has more than 1 group", K(shard_tpo), K(ret));
  } else {
    ObShardTpo::GCHashMap::iterator it = (const_cast<ObShardTpo::GCHashMap &>(shard_tpo->gc_map_)).begin();
    const ObGroupCluster &gc_info = *it;
    int64_t es_size = gc_info.get_es_size();
    if (es_id < 0 || OBPROXY_MAX_DBMESH_ID == es_id) {
      if (is_read_stmt
          && (last_es_id >= 0)
          && (last_es_id < es_size)) {
        //In multiple statements, the read request can use the effective es id of the previous statement
        es_id = last_es_id;
      } if (OB_FAIL(gc_info.get_elastic_id_by_weight(es_id, is_read_stmt))) {
        LOG_WARN("fail to get random eid", K(gc_info));
      }
    } else if (OB_UNLIKELY(es_id >= es_size)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("es index is larger than elastic array", K(es_id), K(es_size), K(ret));
    }
    if (OB_SUCC(ret)) {
      ObString shard_name = gc_info.get_shard_name_by_eid(es_id);
      if (TESTLOAD_NON != testload_type) {
        if (OB_FAIL(get_testload_shard_connector(shard_name, testload_prefix_.config_string_, shard_conn))) {
          LOG_WARN("testload shard connector not exist", "testload_type", get_testload_type_str(testload_type),
                   K(shard_name), K(testload_prefix_), K(ret));
        }
      } else if (OB_FAIL(get_shard_connector(shard_name, shard_conn))) {
        LOG_WARN("shard connector does not exist", K(shard_name), K(ret));
      }
      if (OB_SUCC(ret) && NULL != shard_conn && OB_FAIL(get_single_real_table_info(table_name, *shard_conn,
                                                                     real_database_name, db_name_len,
                                                                     real_table_name, tb_name_len,
                                                                     hint_table))) {
        LOG_WARN("fail to get real table info", K(table_name), K(ret));
      }
    }
  }

  if (NULL != shard_tpo) {
    shard_tpo->dec_ref();
    shard_tpo = NULL;
  }

  return ret;
}

const ObString ObDbConfigLogicDb::get_sequence_table_name()
{
  ObString ret_str;
  ret_str = get_sequence_table_name_from_variables();
  if (ret_str.empty()) {
    ret_str = get_sequence_table_name_from_router();
  }
  if (ret_str.empty()) {
   ret_str = ObProxySequenceUtils::get_default_sequence_table_name();
  }
  return ret_str;
}

const ObString ObDbConfigLogicDb::get_sequence_table_name_from_router()
{
  ObString ret_str;
  bool found = false;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap &>(sr_array_.ccr_map_);
  for (ObDbConfigChildArrayInfo<ObShardRouter>::CCRHashMap::iterator it = map.begin(); !found && it != map.end(); ++it) {
    if (!it->get_sequence_table().empty()) {
      found = true;
      ret_str = it->get_sequence_table();
    }
  }
  return ret_str;
}

const ObString ObDbConfigLogicDb::get_sequence_table_name_from_variables()
{
  ObString ret_str;
  int ret = OB_SUCCESS;
  ObDataBaseVar *db_var = NULL;
  if (OB_FAIL(get_database_var(db_var))) {
    LOG_DEBUG("database var info not exist", K(ret));
  } else if (OB_ISNULL(db_var)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database variable info is null", K(ret));
  } else {
    ret_str = db_var->get_variable_value(SEQUENCE_TABLE);
  }
  if (NULL != db_var) {
    db_var->dec_ref();
    db_var = NULL;
  }
  return ret_str;
}

int ObDbConfigCache::get_all_logic_tenant(ObIArray<common::ObString> &all_tenant)
{
  int ret = OB_SUCCESS;
  obsys::CRLockGuard guard(rwlock_);
  LTHashMap &map = const_cast<LTHashMap &>(lt_map_);
  for (LTHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
    if (OB_FAIL(all_tenant.push_back(it->tenant_name_.config_string_))) {
      LOG_WARN("fail to push back tenant name", K(it->tenant_name_), K(ret));
    }
  }
  return ret;
}

int ObDbConfigLogicDb::get_shard_prop(const common::ObString& shard_name,
                      ObShardProp* &shard_prop)
{
  int ret = OB_SUCCESS;
  ObShardProp *tmp_shard_prop = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  if (OB_FAIL(sp_array_.ccr_map_.get_refactored(shard_name, tmp_shard_prop))) {
    LOG_WARN("fail to get shard prop", K(shard_name), K(ret));
  } else if (OB_ISNULL(tmp_shard_prop)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard prop is null", K(shard_name));
  } else {
    shard_prop = tmp_shard_prop;
    shard_prop->inc_ref();
  }
  return ret;
}

int ObDbConfigLogicDb::get_user_priv_info(
                       const ObString &username,
                       const ObString &host,
                       ObShardUserPrivInfo &up_info)
{
  int ret = OB_SUCCESS;
  up_info.reset();
  bool found = false;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
  ObDbConfigChildArrayInfo<ObDataBaseAuth>::CCRHashMap &map = const_cast<ObDbConfigChildArrayInfo<ObDataBaseAuth>::CCRHashMap &>(da_array_.ccr_map_);
  for (ObDbConfigChildArrayInfo<ObDataBaseAuth>::CCRHashMap::iterator it = map.begin(); !found && it != map.end(); ++it) {
    if (OB_FAIL(it->get_user_priv_info(username, host, up_info))) {
      LOG_DEBUG("shard user priv info does not exist", K(username), K(host), K(ret));
    } else {
      found = true;
    }
  }
  if (!found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("shard user priv info does not exist", K(username), K(host), K(ret));
  }
  return ret;
}

int ObDbConfigLogicDb::get_disaster_eid(const common::ObString &disaster_status, int64_t &eid)
{
  int ret = OB_SUCCESS;
  ObDataBaseProp *db_prop = NULL;
  if (OB_FAIL(get_database_prop(ObString::make_string(DB_PROP_SELFADJUST), db_prop))) {
    LOG_DEBUG("self adjust prop is not exist", K(ret));
  } else if (OB_ISNULL(db_prop)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("self adjust prop is null", K(ret));
  } else if (OB_FAIL(db_prop->get_disaster_eid(disaster_status, eid))) {
    LOG_WARN("fail to get disaster eid", K(disaster_status), KPC(db_prop), K(ret));
  }
  if (NULL != db_prop) {
    db_prop->dec_ref();
    db_prop = NULL;
  }
  return ret;
}

int ObDbConfigLogicDb::init_connector_password_for_bt()
{
  int ret = OB_SUCCESS;
  if (need_update_bt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not support beyond trust password", K(ret));
  }
  return ret;
}

ObDbConfigChild *ObDbConfigCache::get_child_info(const ObDataBaseKey &key,
                                                 const ObString &name,
                                                 const ObDDSCrdType type)
{
  ObDbConfigChild *child_info = NULL;
  ObDbConfigLogicDb *db_info = NULL;
  if (NULL != (db_info = get_exist_db_info(key))) {
    obsys::CRLockGuard guard(rwlock_);
    switch(type) {
      case TYPE_DATABASE_AUTH:
        db_info->da_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObDataBaseAuth *&>(child_info));
        break;
      case TYPE_DATABASE_VAR:
        db_info->dv_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObDataBaseVar *&>(child_info));
        break;
      case TYPE_DATABASE_PROP:
        db_info->dp_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObDataBaseProp *&>(child_info));
        break;
      case TYPE_SHARDS_TPO:
        db_info->st_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObShardTpo *&>(child_info));
        break;
      case TYPE_SHARDS_ROUTER:
        db_info->sr_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObShardRouter *&>(child_info));
        break;
      case TYPE_SHARDS_DIST:
        db_info->sd_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObShardDist *&>(child_info));
        break;
      case TYPE_SHARDS_CONNECTOR:
        db_info->sc_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObShardConnector *&>(child_info));
        break;
      case TYPE_SHARDS_PROP:
        db_info->sp_array_.ccr_map_.get_refactored(name, reinterpret_cast<ObShardProp *&>(child_info));
        break;
      default:
        LOG_WARN("invalid type", K(type));
        break;
    } // end switch
  }
  if (NULL != child_info) {
    child_info->inc_ref();
  }
  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  return child_info;
}

const char *get_dbconfig_state_str(ObDbConfigState state)
{
  const char *state_str = NULL;
  switch (state) {
    case DC_BORN:
      state_str = "DC_BORN";
      break;
    case DC_BUILDING:
      state_str = "DC_BUILDING";
      break;
    case DC_BUILD_FAILED:
      state_str = "DC_BUILD_FAILED";
      break;
    case DC_AVAIL:
      state_str = "DC_AVAIL";
      break;
    case DC_UPDATING:
      state_str = "DC_UPDATING";
      break;
    case DC_DEAD:
      state_str = "DC_DEAD";
      break;
    case DC_DELETING:
      state_str = "DC_DELETING";
      break;
    default:
      state_str = "DC_UNKNOWN";
      break;
  }
  return state_str;
}

int ObDbConfigCache::load_local_dbconfig()
{
  int ret = OB_SUCCESS;
  struct dirent *ent = NULL;
  const char *layout_dbconfig_dir = get_global_layout().get_dbconfig_dir();
  DIR *dbconfig_dir = NULL;
  if (OB_ISNULL(layout_dbconfig_dir)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dbconfig layout is null", K(ret));
  } else if (OB_ISNULL(dbconfig_dir = opendir(layout_dbconfig_dir))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to open dir", K(layout_dbconfig_dir), KERRMSGS, K(ret));
  }
  event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  while (OB_SUCC(ret) && NULL != (ent = readdir(dbconfig_dir))) {
    bool is_need_load = false;
    if (LOCAL_DIR.compare(ent->d_name) == 0 || PARENT_DIR.compare(ent->d_name) == 0) {
      // do nothing
    } else if (ent->d_type == DT_DIR) {
      is_need_load = true;
    } else if (ent->d_type == DT_UNKNOWN) {
      struct stat st;
      char *full_path = NULL;
      allocator.reuse();
      if (OB_FAIL(ObLayout::merge_file_path(layout_dbconfig_dir, ent->d_name, allocator, full_path))) {
        LOG_WARN("fail to merge file", K(layout_dbconfig_dir), "name", ent->d_name, K(ret));
      } else if (0 != (stat(full_path, &st))) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to stat dir", K(full_path), KERRMSGS, K(ret));
      } else if (S_ISDIR(st.st_mode)) {
        is_need_load = true;
      }
    }

    if (OB_SUCC(ret) && is_need_load) {
      if (OB_FAIL(load_logic_tenant_config(ObString::make_string(ent->d_name)))) {
        LOG_WARN("fail to load tenant config", "tenant_dir", ent->d_name, K(ret));
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != dbconfig_dir) && OB_UNLIKELY(0 != closedir(dbconfig_dir))) {
    tmp_ret = OB_IO_ERROR;
    LOG_WARN("fail to close dir", "dir", layout_dbconfig_dir, KERRMSGS, K(tmp_ret));
  }
  return ret;
}

int ObDbConfigCache::load_logic_tenant_config(const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  get_global_db_config_processor().reset_bt_update_flag();
  ObDbConfigLogicTenant *tenant_info = NULL;
  if (OB_ISNULL(tenant_info = op_alloc(ObDbConfigLogicTenant))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObDbConfigLogicTenant", K(tenant_name), K(ret));
  } else {
    tenant_info->inc_ref();
    tenant_info->tenant_name_.set_value(tenant_name);
    tenant_info->name_.set_value(tenant_name);
    tenant_info->db_info_key_.tenant_name_.set_value(tenant_name);
    if (OB_FAIL(ObProxyPbUtils::parse_local_child_config(*tenant_info))) {
      LOG_WARN("fail to parse local tenant config", K(tenant_name), K(ret));
    }
  }

  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

int ObDbConfigCache::get_shard_prop(const ObString &tenant_name,
                                        const ObString &db_name,
                                        const ObString &shard_name,
                                        ObShardProp* &shard_prop)
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicDb *db_info = NULL;
  if (OB_ISNULL(db_info = get_exist_db_info(tenant_name, db_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("logic db does not exist", K(tenant_name), K(db_name), K(shard_name), K(ret));
  } else if (OB_FAIL(db_info->get_shard_prop(shard_name, shard_prop))) {
    LOG_WARN("fail to get shard prop", K(tenant_name), K(db_name), K(shard_name), K(ret));
  }
  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  return ret;
}


ObTestLoadType get_testload_type(int64_t type)
{
  ObTestLoadType ret = TESTLOAD_NON;
  switch(type) {
    case 1:
      ret = TESTLOAD_ALIPAY;
      break;
    case 8:
      ret = TESTLOAD_ALIPAY_COMPATIBLE;
      break;
    case 2:
    case 9:
      /* testload = 2 and testload = 9, will support in next */
    case -1:
    case 0:
    default:
      ret = TESTLOAD_NON;
      break;
  }
  return ret;
}

const char *get_testload_type_str(ObTestLoadType type)
{
  const char *str_ret = NULL;
  switch(type) {
    case TESTLOAD_NON:
      str_ret = "TESTLOAD_NON";
      break;
    case TESTLOAD_ALIPAY:
      str_ret = "TESTLOAD_ALIPAY";
      break;
    case TESTLOAD_MIRROR:
      str_ret = "TESTLOAD_MIRROR";
      break;
    case TESTLOAD_ALIPAY_COMPATIBLE:
      str_ret = "TESTLOAD_ALIPAY_COMPATIBLE";
      break;
    case TESTLOAD_MIRROR_COMPATIBLE:
      str_ret = "TESTLOAD_MIRROR_COMPATIBLE";
      break;
    default:
      break;
  }
  return str_ret;
}

ObConnectorEncType get_enc_type(int64_t type)
{
  ObConnectorEncType ret_type = ENC_DEFAULT;
  switch(type) {
    case 1:
      ret_type = ENC_BEYOND_TRUST;
      break;
    case 0:
    default:
      break;
  }
  return ret_type;
}

const char* get_enc_type_str(ObConnectorEncType type)
{
  const char *str_ret = "";
  switch (type) {
    case ENC_BEYOND_TRUST:
      str_ret = "BEYONDTRUST";
      break;
    case ENC_DEFAULT:
    default:
      break;
  }
  return str_ret;
}

ObConnectorEncType get_enc_type(const char *enc_str, int64_t str_len)
{
  ObString tmp_str(str_len, enc_str);
  ObConnectorEncType type = ENC_DEFAULT;
  if (tmp_str.case_compare("BEYONDTRUST") == 0) {
    type = ENC_BEYOND_TRUST;
  }
  return type;
}

int ObDbConfigCache::handle_new_db_info(ObDbConfigLogicDb &db_info, bool is_from_local/*false*/)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = db_info.db_info_key_.tenant_name_.config_string_;
  const ObString &db_name = db_info.db_info_key_.database_name_.config_string_;
  ObDbConfigLogicTenant *tenant_info = NULL;
  const bool need_update_bt = db_info.need_update_bt();
  // 1. init or update bt cache, if failed ,no need to update db info
  // 2. update db info
  if (need_update_bt) {
    if (OB_FAIL(get_global_db_config_processor().handle_bt_sdk())) {
      LOG_WARN("fail to handle bt sdk", K(db_info.db_info_key_),  K(ret));
    } else if (OB_FAIL(db_info.init_connector_password_for_bt())) {
      LOG_WARN("fail to get connector password for bt", K(ret));
    } else {
      db_info.set_need_update_bt(false);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(db_info.update_conn_prop_with_shard_dist())) {
    LOG_WARN("fail to update conn prop with shard dist", K(tenant_name), K(db_name));
  } else if (OB_ISNULL(tenant_info = get_exist_tenant(tenant_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logic tenant does not exist", K(tenant_name), K(ret));
  } else {
    // directly add into ld map
    db_info.set_avail_state();
    CWLockGuard guard(rwlock_);
    ObDbConfigLogicDb *cur_db_info = tenant_info->ld_map_.remove(db_name);
    if (NULL != cur_db_info) {
      cur_db_info->set_deleting_state();
      cur_db_info->dec_ref();
      cur_db_info = NULL;
    }
    if (OB_FAIL(tenant_info->ld_map_.unique_set(&db_info))) {
      LOG_WARN("fail to add new db info", K(db_name), K(tenant_name), K(ret));
    } else {
      db_info.inc_ref();
      if (!is_from_local) {
        db_info.set_need_dump_config(true);
      }
    }
  }

  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

int ObDbConfigLogicDb::get_es_index_by_gc(ObGroupCluster *gc_info, ObShardRule *shard_rule,
                                          ObTestLoadType testload_type, bool is_read_stmt,
                                          SqlFieldResult &sql_result, int64_t &es_index)
{
  int ret = OB_SUCCESS;

  // Each elastic bit will only be calculated once, and for subsequent reuse,
  // it is necessary to ensure that the following elastic bits are valid.
  if (-1 == es_index || OBPROXY_MAX_DBMESH_ID == es_index) {
    int64_t es_size = gc_info->get_es_size();
    ObSEArray<int64_t, 4> es_index_array;
    if (-1 == es_index || (OBPROXY_MAX_DBMESH_ID == es_index && shard_rule->es_rules_.empty())) {
      if (OB_FAIL(gc_info->get_elastic_id_by_weight(es_index, is_read_stmt))) {
        LOG_WARN("fail to get eid by read weight", K(is_read_stmt), KPC(gc_info));
      } else {
        LOG_DEBUG("succ to get eid by weight", K(es_index));
      }
    } else if (sql_result.field_num_ > 0
               && OB_FAIL(ObShardRule::get_physic_index_array(sql_result, shard_rule->es_rules_, es_size,
                                                              testload_type, es_index_array, true))) {
      LOG_WARN("fail to calculate elastic index", KPC(shard_rule), K(ret));
    } else if (es_index_array.empty()) {
      if (OB_FAIL(gc_info->get_elastic_id_by_weight(es_index, is_read_stmt))) {
        LOG_WARN("fail to get eid by read weight", K(is_read_stmt), KPC(gc_info));
      } else {
        LOG_DEBUG("succ to get eid by weight", K(es_index));
      }
    } else {
      es_index = es_index_array.at(0);
      if (es_index >= es_size) {
        ret = OB_EXPR_CALC_ERROR;
        LOG_WARN("es index is larger than elastic array", K(es_index), K(es_size), K(ret));
      }

      for (int64_t j = 1; OB_SUCC(ret) && j < es_index_array.count(); j++) {
        if (es_index != es_index_array.at(j)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get too many es index", K(es_index), K(j), K(es_index_array.at(j)), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDbConfigLogicDb::get_db_and_table_index(ObShardRule *shard_rule,
                                              ObSqlParseResult &parse_result,
                                              ObTestLoadType testload_type,
                                              ObIArray<int64_t> &group_index_array,
                                              ObIArray<int64_t> &table_index_array)
{
  int ret = OB_SUCCESS;

  SqlFieldResult &sql_result = parse_result.get_sql_filed_result();

  if (shard_rule->tb_size_ == 1) {
    // Sub-libraries are not divided into tables, calculated according to the library
    // Note that the sub-library single table will not go here
    if (sql_result.field_num_ > 0
        && OB_FAIL(ObShardRule::get_physic_index_array(sql_result, shard_rule->db_rules_,
                                                       shard_rule->db_size_, testload_type,
                                                       group_index_array))) {
      LOG_WARN("fail to get physic db index", KPC(shard_rule), K(ret));
    }

    // if do not get sharding info, scall all table
    if (OB_SUCC(ret) && group_index_array.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < shard_rule->db_size_; i++) {
        if (OB_FAIL(group_index_array.push_back(i))) {
          LOG_WARN("push back group index failed", K(i), K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < group_index_array.count(); i++) {
      if (OB_FAIL(table_index_array.push_back(group_index_array.at(i)))) {
        LOG_WARN("push back group index failed", "group index", group_index_array.at(i), K(ret));
      }
    }
  } else {
    // Sub-library and sub-table, according to table calculation
    // We only support shard-rules that satisfied 'db_index = tb_index / (tb_size / db_size)'
    int64_t route_hint = shard_rule->tb_size_ / shard_rule->db_size_;
    // If you need to scan the database, then the table should not be able to calculate
    if (sql_result.field_num_ > 0
        && OB_FAIL(ObShardRule::get_physic_index_array(sql_result, shard_rule->tb_rules_,
                                                       shard_rule->tb_size_, testload_type,
                                                       table_index_array))) {
      LOG_WARN("fail to get physic tb index", KPC(shard_rule), K(ret));
    }

    // if do not get sharding info, scall all table
    if (OB_SUCC(ret) && table_index_array.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < shard_rule->tb_size_; i++) {
        if (OB_FAIL(table_index_array.push_back(i))) {
          LOG_WARN("table index array push back failed", K(i), K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < table_index_array.count(); i++) {
      if (OB_FAIL(group_index_array.push_back(table_index_array.at(i) / route_hint))) {
        LOG_WARN("push back group index failed", "table index", table_index_array.at(i),
                 K(route_hint), K(ret));
      }
    }
  }

  return ret;
}

int ObDbConfigLogicDb::get_shard_prop_by_connector(ObIArray<ObShardConnector*> &shard_connector_array,
                                                   ObIArray<ObShardProp*> &shard_prop_array)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < shard_connector_array.count(); i++) {
    ObShardConnector *shard_connctor = shard_connector_array.at(i);
    ObShardProp *shard_prop = NULL;
    if (OB_FAIL(get_shard_prop(shard_connctor->shard_name_, shard_prop))) {
      LOG_DEBUG("fail to get shard prop", "shard name", shard_connctor->shard_name_, K(ret));
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(shard_prop_array.push_back(shard_prop))) {
      LOG_WARN("push back shard prop failed", KP(shard_prop), K(ret));
    }
  }

  return ret;
}

int ObDbConfigLogicDb::get_shard_connector_by_index(ObShardRule *shard_rule,
                                                    ObSqlParseResult &parse_result,
                                                    ObTestLoadType testload_type,
                                                    bool is_read_stmt,
                                                    int64_t es_index,
                                                    ObIArray<int64_t> &group_index_array,
                                                    ObIArray<ObShardConnector*> &shard_connector_array)
{
  int ret = OB_SUCCESS;

  SqlFieldResult &sql_result = parse_result.get_sql_filed_result();
  ObShardTpo *shard_tpo = NULL;

  if (OB_FAIL(get_shard_tpo(shard_tpo))) {
    LOG_WARN("fail to get shard tpo info", K(ret));
  } else if (OB_ISNULL(shard_tpo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard tpo info is null", K(ret));
  }

  // acquire shard_connector
  for (int64_t i = 0; OB_SUCC(ret) && i < group_index_array.count(); i++) {
    int64_t group_index = group_index_array.at(i);
    char group_name[OB_MAX_DATABASE_NAME_LENGTH];
    ObGroupCluster *gc_info = NULL;
    if (OB_FAIL(shard_rule->get_real_name_by_index(shard_rule->db_size_,
                                                   shard_rule->db_suffix_len_, group_index,
                                                   shard_rule->db_prefix_.config_string_,
                                                   shard_rule->db_tail_.config_string_,
                                                   group_name, OB_MAX_DATABASE_NAME_LENGTH))) {
      LOG_WARN("fail to get real group name", K(group_index), KPC(shard_rule), K(ret));
    } else if (OB_FAIL(shard_tpo->get_group_cluster(ObString::make_string(static_cast<char*>(group_name)), gc_info))) {
      LOG_WARN("group does not exist", K(group_name), K(ret));
    } else if (OB_ISNULL(gc_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group cluster info is null", K(ret));
    } else if (-1 == es_index || OBPROXY_MAX_DBMESH_ID == es_index) {
      if (OB_FAIL(get_es_index_by_gc(gc_info, shard_rule, testload_type, is_read_stmt, sql_result, es_index))) {
        LOG_WARN("fail to get es index", K(gc_info), K(shard_rule), K(testload_type), K(is_read_stmt), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObString shard_name = gc_info->get_shard_name_by_eid(es_index);
      ObShardConnector *shard_conn = NULL;
      if (TESTLOAD_NON != testload_type) {
        if (OB_FAIL(get_testload_shard_connector(shard_name, testload_prefix_.config_string_, shard_conn))) {
          LOG_WARN("testload shard connector not exist", K(ret), K(shard_name), K(testload_prefix_));
        }
      } else if (OB_FAIL(get_shard_connector(shard_name, shard_conn))) {
        LOG_WARN("shard connector does not exist", K(shard_name), K(ret));
      } else if (NULL != shard_conn && OB_FAIL(shard_connector_array.push_back(shard_conn))) {
        LOG_WARN("push back shard conn failed", K(shard_conn), K(ret));
      }
    }
  } // end group_index_array

  if (NULL != shard_tpo) {
    shard_tpo->dec_ref();
    shard_tpo = NULL;
  }

  return ret;
}

int ObDbConfigLogicDb::get_table_name_by_index(ObSqlParseResult &parse_result,
                                               ObTestLoadType testload_type,
                                               ObIAllocator &allocator,
                                               ObIArray<int64_t> &table_index_array,
                                               ObIArray<ObHashMapWrapper<ObString, ObString> > &table_name_map_array)
{
  int ret = OB_SUCCESS;

  ObProxyDMLStmt *dml_stmt = static_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());
  ObProxyDMLStmt::ExprMap &table_exprs_map = dml_stmt->get_table_exprs_map();
  SqlFieldResult &sql_result = parse_result.get_sql_filed_result();
  char real_table_name[OB_MAX_TABLE_NAME_LENGTH];

  ObHashMapWrapper<ObString, ObString> table_name_map_wrapper;
  if (OB_FAIL(table_name_map_wrapper.init(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                          ObModIds::OB_HASH_ALIAS_TABLE_MAP))) {
    LOG_WARN("fail to init table name map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_index_array.count(); i++) {
    int64_t tb_index = table_index_array.at(i);
    ObProxyDMLStmt::ExprMap::iterator iter = table_exprs_map.begin();
    ObProxyDMLStmt::ExprMap::iterator end = table_exprs_map.end();
    table_name_map_wrapper.reuse();

    for (; OB_SUCC(ret) && iter != end; iter++) {
      ObProxyExpr *expr = iter->second;
      ObProxyExprTable *table_expr = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null, unexpected", K(ret));
      } else if (OB_ISNULL(table_expr = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to cast to table expr", K(expr), K(ret));
      } else {
        ObString &sql_table_name = table_expr->get_table_name();
        ObString hint_table;

        if (OB_FAIL(get_real_table_name(sql_table_name, sql_result,
                                        real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                        tb_index, hint_table, testload_type))) {
          LOG_WARN("fail to get real table name", K(sql_table_name), K(tb_index), K(testload_type), K(ret));
        } else if (OB_FAIL(ObProxyShardUtils::add_table_name_to_map(allocator, table_name_map_wrapper.get_hash_map(),
                                                                    sql_table_name, real_table_name))) {
          LOG_WARN("fail to add table name to map", K(sql_table_name), K(real_table_name), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_name_map_array.push_back(table_name_map_wrapper))) {
        LOG_WARN("fail to push back table name map", K(ret));
      }
    }
  }

  return ret;
}

int ObDbConfigLogicDb::get_sharding_select_info(const common::ObString &table_name,
                                                ObSqlParseResult &parse_result,
                                                ObTestLoadType testload_type,
                                                bool is_read_stmt,
                                                int64_t es_index,
                                                ObIAllocator &allocator,
                                                ObIArray<ObShardConnector*> &shard_connector_array,
                                                ObIArray<ObShardProp*> &shard_prop_array,
                                                ObIArray<ObHashMapWrapper<ObString, ObString> > &table_name_map_array)
{
  int ret = OB_SUCCESS;

  ObShardRule *shard_rule = NULL;
  ObSEArray<int64_t, 4> group_index_array;
  ObSEArray<int64_t, 4> table_index_array;

  if (OB_FAIL(get_shard_rule(shard_rule, table_name))) {
    LOG_WARN("fail to get shard rule", K(table_name), K(ret));
  } else if (OB_FAIL(get_db_and_table_index(shard_rule, parse_result, testload_type,
                                            group_index_array, table_index_array))) {
    LOG_WARN("fail to get db and table index", KPC(shard_rule), K(testload_type), K(ret));
  } else if (OB_FAIL(get_shard_connector_by_index(shard_rule, parse_result, testload_type, is_read_stmt,
                                                  es_index, group_index_array, shard_connector_array))) {
    LOG_WARN("fail to get shard connector", KPC(shard_rule), K(testload_type), K(is_read_stmt), K(es_index), K(ret));
  } else if (OB_FAIL(get_shard_prop_by_connector(shard_connector_array, shard_prop_array))) {
    LOG_WARN("fail to get shard prop", K(ret));
  } else if (OB_FAIL(get_table_name_by_index(parse_result, testload_type,
                                             allocator, table_index_array, table_name_map_array))) {
    LOG_WARN("fail to get real table name", KPC(shard_rule), K(testload_type), K(ret));
  }

  return ret;
}

} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
