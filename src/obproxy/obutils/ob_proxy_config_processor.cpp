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

#include "obutils/ob_proxy_config_processor.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "utils/ob_layout.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_config_utils.h"
#include "cmd/ob_internal_cmd_processor.h"
#include "obutils/ob_proxy_reload_config.h"
#include "utils/ob_proxy_utils.h"
#include "utils/ob_proxy_monitor_utils.h"
#include "iocore/net/ob_ssl_processor.h"
#include "obutils/ob_config_processor.h"
#include "omt/ob_ssl_config_table_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::qos;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::omt;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

const static char *CONFIG_META              = "meta";
const static char *CONFIG_SPEC              = "spec";

const static char *CONFIG_API_VERSION       = "api_version";
const static char *CONFIG_VERSION           = "version";
const static char *CONFIG_APP_NAME          = "app_name";
const static char *CONFIG_TYPE              = "config_type";
const static char *CONFIG_SYNC              = "need_sync";

// index config
const static char *CONFIG_REFERENCE         = "reference";
const static char *CONFIG_DATAID            = "data_id";

// config spec
const static char *CONFIG_VALUE             = "value";
const static char *CONFIG_ATTR              = "category";
const static char *CONFIG_PERSISTENT        = "persistent";

// limit config
const static char *LIMITERS                 = "limiters";
const static char *CLUSTER_NAME             = "cluster";
const static char *TENANT_NAME              = "tenant";
const static char *DATABASE_NAME            = "database";
const static char *USER_NAME                = "username";
const static char *LIMIT_NAME               = "limitName";
const static char *LIMIT_MODE               = "mode";
const static char *LIMIT_RULE               = "rule";
const static char *LIMIT_PRIORITY           = "priority";
const static char *LIMIT_STATUS             = "status";

// limit rule
const static char *LIMIT_SQL_TYPE           = "sqlType";
const static char *LIMIT_KEY_WORDS          = "keyWords";
const static char *LIMIT_TABLE_NAME         = "tableName";
const static char *LIMIT_QPS                = "qps";
const static char *LIMIT_RT                 = "averageRt";
const static char *LIMIT_CONDITION          = "scene";
const static char *LIMIT_CONDITION_USE_LIKE = "Uselike";
const static char *LIMIT_CONDITION_NO_WHERE = "Nowhere";
const static char *LIMIT_TIME_WINDOW        = "timeWindow";
const static char *LIMIT_CONN               = "limitConn";
const static char *LIMIT_FUSE_TIME          = "fuseTime";

// security
const static char *SECURITY_SOURCE_TYPE     = "sourceType";
const static char *SECURITY_CA              = "CA";
const static char *SECURITY_PUBLIC_KEY      = "publicKey";
const static char *SECURITY_PRIVATE_KEY     = "privateKey";

#define MONITOR_LIMIT_LOG_FORMAT "%s,%s,%s," \
                                 "%s,%.*s:%.*s:%.*s,%s,"  \
                                 "%s,%.*s,%s,%s,%s,%.*s,%.*s"

#define MONITOR_LIMIT_LOG_PARAM \
          get_global_proxy_config().app_name_str_,       \
          "",                                            \
          "",                                            \
                                                         \
          "",                                            \
          cluster_name_.length(), cluster_name_.ptr(),   \
          tenant_name_.length(), tenant_name_.ptr(),     \
          database_name_.length(), database_name_.ptr(), \
          database_type_str,                             \
                                                         \
          "",                                            \
          table_name.length(), table_name.ptr(),         \
          sql_cmd,                                       \
          stmt_type_str,                                 \
          get_limit_status_str(limit_status_),           \
          new_sql.length(), new_sql.ptr(),               \
          limit_name_.length(), limit_name_.ptr()

ObProxyConfigProcessor &get_global_proxy_config_processor()
{
  static ObProxyConfigProcessor g_proxy_config_processor;
  return g_proxy_config_processor;
}

//---------------ObProxyBaseConfig--------------
int64_t ObProxyBaseConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(app_name),
       K_(version),
       K_(api_version),
       K_(need_sync),
       "congfig_type:", get_config_type_str(type_));
  J_OBJ_END();
  return pos;
}

void ObProxyBaseConfig::destroy_config_container()
{
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (OB_NOT_NULL(it->second)) {
      delete it->second;
    }
  }
}

int ObProxyBaseConfig::parse_from_json(Value &json_value, const bool is_from_local)
{
  int ret = OB_SUCCESS;
  Value *json_meta = NULL;
  Value *json_spec = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_META) {
        json_meta = it->value_;
      } else if (it->name_ == CONFIG_SPEC) {
        json_spec = it->value_;
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(json_meta) || OB_ISNULL(json_spec)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid json config, meta or spec is null", K_(app_name), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_config_meta(*json_meta, is_from_local))) {
      LOG_WARN("fail to parse config meta", K_(app_name), K(is_from_local), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_config_spec(*json_spec))) {
      LOG_WARN("fail to parse config spec", K_(app_name), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_complete_ = true;
    LOG_INFO("succ to parse sub config completely", KPC(this));
  }
  return ret;
}

int ObProxyBaseConfig::parse_config_meta(Value &json_value, const bool is_from_local)
{
  int ret = OB_SUCCESS;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_API_VERSION) {
        api_version_.set_value(it->value_->get_string());
      } else if (it->name_ == CONFIG_VERSION) {
        if (INDEX_CONFIG == type_ && is_from_local) {
          version_.set_value(it->value_->get_string());
        } else if (OB_UNLIKELY(version_.config_string_ != it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config version is mismatched", "expected version", version_.ptr(),
                   "actual version", it->value_->get_string().ptr(), K(ret));
        }
      } else if (it->name_ == CONFIG_APP_NAME) {
        if (OB_UNLIKELY(app_name_.config_string_ != it->value_->get_string())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("appname is mismatched", "expected name", app_name_.ptr(),
                   "actual name", it->value_->get_string().ptr(), K(ret));
        }
      } else if (it->name_ == CONFIG_TYPE) {
        if (OB_UNLIKELY(it->value_->get_string() != get_config_type_str(type_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("config type is mismatched", "expected type", get_config_type_str(type_),
                   "actual type", it->value_->get_string().ptr(), K(ret));
        }
      } else if (it->name_ == CONFIG_SYNC) {
        need_sync_ = it->value_->get_string() == "true";
      }
    } // end obj
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }
  return ret;
}

int ObProxyBaseConfig::to_json_str(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{");
  if (OB_SUCC(ret)) {
    if (OB_FAIL(meta_to_json(buf))) {
      LOG_WARN("fail to append config meta", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append_fmt(", ");
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec_to_json(buf))) {
      LOG_WARN("fail to append config spec", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = buf.append_fmt("}");
  }
  return ret;
}

int ObProxyBaseConfig::meta_to_json(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("\"%s\": {\"%s\": \"%s\", \"%s\": \"%s\", \
                       \"%s\": \"%s\", \"%s\": \"%s\", \"%s\": \"%s\"}",
                       CONFIG_META, CONFIG_API_VERSION, api_version_.ptr(),
                       CONFIG_VERSION, version_.ptr(), CONFIG_APP_NAME, app_name_.ptr(),
                       CONFIG_TYPE, get_config_type_str(type_),
                       CONFIG_SYNC, need_sync_ ? "true" : "false");
  return ret;
}

int ObProxyBaseConfig::parse_config_spec(Value &json_value)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObString value;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      name = it->name_;
      value.reset();
      if (JT_OBJECT == it->value_->get_type()) {
        DLIST_FOREACH(p, it->value_->get_object()) {
          if (p->name_ == CONFIG_VALUE) {
            value = p->value_->get_string();
          }
        }
      } else {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
                 "actual type", it->value_->get_type(), K(name), K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(add_config_item(name, value))) {
        LOG_WARN("fail to add config item", K_(app_name), K(name), K(value), K(ret));
      }
    } // end spec obj
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }
  return ret;
}

int ObProxyBaseConfig::spec_to_json(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("\"%s\": { ", CONFIG_SPEC);
  ObConfigContainer::const_iterator it = container_.begin();
  const ObString memory_level(OB_CONFIG_VISIBLE_LEVEL_MEMORY);
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config item is null", "name", it->first.str(), K(ret));
    } else if (memory_level == it->second->visible_level()) {
      //no need serialize
    } else {
      ret = buf.append_fmt("\"%s\": {\"%s\": \"%s\", \"%s\": \"%s\"},", it->first.str(),
                           CONFIG_VALUE, it->second->str(),
                           CONFIG_ATTR, CONFIG_PERSISTENT);
    }
  }
  if (OB_SUCC(ret)) {
    // trim last ','
    char *buf_start = buf.ptr();
    if (',' == buf_start[buf.length() - 1]) {
      buf_start[buf.length() - 1] = '}';
    } else {
      ret = buf.append("}");
    }
  }
  return ret;
}

int ObProxyBaseConfig::add_config_item(const ObString &name, const ObString &value)
{
  int ret = OB_SUCCESS;
  ObConfigItem *const *pp_global_item = NULL;
  ObConfigItem *new_item = NULL;
  if (OB_ISNULL(pp_global_item = get_global_proxy_config().get_container().get(ObConfigStringKey(name)))) {
    /* make compatible with previous configuration */
    LOG_WARN("Invalid config string, no such config item", K(name), K(value));
  } else if (OB_ISNULL(new_item = (*pp_global_item)->clone())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to clone global app config item to new app item", K(name), K(ret));
  } else if (!new_item->set_value(value)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("Invalid config value", K(name), K(value), K(ret));
  } else if (!new_item->check()) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("Invalid config, value out of range", K(name), K(value), K(ret));
  } else if (OB_FAIL(container_.set_refactored(ObConfigStringKey(name), new_item, 1))) {
    LOG_WARN("fail to add new config item", K(name), K(value), K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(new_item)) {
    delete new_item; // new item was allocate by new in clone
    new_item = NULL;
  }
  return ret;
}

int ObProxyBaseConfig::assign(const ObProxyBaseConfig &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    type_ = other.type_;
    need_sync_ = other.need_sync_;
    is_complete_ = other.is_complete_;
    app_name_.assign(other.app_name_);
    version_.assign(other.version_);
    api_version_.assign(other.api_version_);
    const ObConfigContainer &other_container = const_cast<ObProxyBaseConfig &>(other).get_container();
    ObConfigContainer::const_iterator it = other_container.begin();
    for (; OB_SUCC(ret) && it != other_container.end(); ++it) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config item is null", "name", it->first.str(), K(ret));
      } else if (OB_FAIL(add_config_item(ObString::make_string(it->first.str()), ObString::make_string(it->second->str())))) {
        LOG_WARN("fail to copy config item",
                 "name", it->first.str(),
                 "value", it->second->str(), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyBaseConfig::dump_to_local(const char *file_path)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  bool need_backup = false;
  const char *file_name = get_config_file_name(type_);
  if (is_complete_) {
    if (OB_FAIL(to_json_str(buf))) {
      LOG_WARN("fail to get json str from config", KPC(this), K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write_to_file(file_path, file_name, buf.ptr(), buf.length(), need_backup))) {
      LOG_WARN("fail to write config to file", K(file_path),
               K(file_name), KPC(this), K(ret));
    }
  }
  return ret;
}

int ObProxyBaseConfig::load_from_local(const char *file_path, ObProxyAppConfig &app_config)
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  const char *file_name = get_config_file_name(type_);
  char *buf = NULL;
  int64_t buf_size = 0;
  ObString json_str;
  bool is_from_local = true;
  if (OB_FAIL(ObProxyFileUtils::calc_file_size(file_path, file_name, buf_size))) {
    LOG_WARN("fail to get file size", K(file_path), K(file_name), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read_from_file(file_path, file_name, buf, buf_size, read_len))) {
    LOG_WARN("fail to read config from file", K(ret), K(file_path), K(file_name), K(buf_size));
  } else if (FALSE_IT(json_str.assign_ptr(buf, static_cast<int32_t>(read_len)))) {
    // impossible
  } else if (OB_FAIL(app_config.update_config(json_str, type_, is_from_local))) {
    LOG_WARN("fail to load local config", K(json_str), K(file_path), K(ret));
  }

  if (NULL != buf) {
    ob_free(buf);
  }

  return ret;
}

//---------------ObProxyConfigReference--------------
int64_t ObProxyConfigReference::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(version), K_(data_id), "config_type", get_config_type_str(type_));
  J_OBJ_END();
  return pos;
}

int ObProxyConfigReference::parse_config_reference(Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *ref_value = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == CONFIG_REFERENCE) {
        ref_value = it->value_;
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }
  // TODO: support multi version config
  // now, only save one
  if (OB_SUCC(ret) && OB_NOT_NULL(ref_value)) {
    if (JT_ARRAY == ref_value->get_type()) {
      DLIST_FOREACH(it, ref_value->get_array()) {
        if (JT_OBJECT == it->get_type()) {
          DLIST_FOREACH(p, it->get_object()) {
            if (p->name_ == CONFIG_DATAID) {
              data_id_.set_value(p->value_->get_string());
            } else if (p->name_ == CONFIG_VERSION) {
              version_.set_value(p->value_->get_string());
            }
          }
        } else {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
                   "actual type", it->get_type(), K(ret));
        }
      }
    } else {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid json config type", "expected type", JT_ARRAY,
               "actual type", ref_value->get_type(), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_valid()) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid config reference", K(ret));
    }
  }
  return ret;
}

int ObProxyConfigReference::reference_to_json(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = buf.append_fmt("\"%s\" : {\"%s\": [{\"%s\": \"%s\", \"%s\": \"%s\"}]},",
                         get_config_type_str(type_), CONFIG_REFERENCE, CONFIG_DATAID, data_id_.ptr(),
                         CONFIG_VERSION, version_.ptr());
  }
  return ret;
}

//---------------ObProxyIndexConfig--------------
int64_t ObProxyIndexConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObProxyBaseConfig::to_string(buf + pos, buf_len - pos);
  J_KV(K_(init_ref), K_(dynamic_ref), K_(limit_control_ref));
  J_OBJ_END();
  return pos;
}

int ObProxyIndexConfig::parse_config_spec(Value &json_value)
{
  int ret = OB_SUCCESS;
  Value *init_ref_value = NULL;
  Value *dynamic_ref_value = NULL;
  Value *limit_control_ref_value = NULL;
  Value *fuse_control_ref_value = NULL;
  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_.case_compare(get_config_type_str(INIT_CONFIG)) ==0) {
        init_ref_value = it->value_;
      } else if (it->name_.case_compare(get_config_type_str(DYNAMIC_CONFIG)) == 0) {
        dynamic_ref_value = it->value_;
      } else if (it->name_.case_compare(get_config_type_str(LIMIT_CONTROL_CONFIG)) == 0) {
        limit_control_ref_value = it->value_;
      } else if (it->name_.case_compare(get_config_type_str(FUSE_CONTROL_CONFIG)) == 0) {
        fuse_control_ref_value = it->value_;
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }
  if (OB_SUCC(ret) && NULL != init_ref_value) {
    if (OB_FAIL(init_ref_.parse_config_reference(*init_ref_value))) {
      LOG_WARN("fail to parse init config reference", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != dynamic_ref_value) {
    if (OB_FAIL(dynamic_ref_.parse_config_reference(*dynamic_ref_value))) {
      LOG_WARN("fail to parse dynamic config reference", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != limit_control_ref_value) {
    if (OB_FAIL(limit_control_ref_.parse_config_reference(*limit_control_ref_value))) {
      LOG_WARN("fail to parse limit control config reference", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != fuse_control_ref_value) {
    if (OB_FAIL(fuse_control_ref_.parse_config_reference(*fuse_control_ref_value))) {
      LOG_WARN("fail to parse limit control config reference", K(ret));
    }
  }
  return ret;
}

int ObProxyIndexConfig::spec_to_json(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buf.append_fmt("\"%s\": {", CONFIG_SPEC))) {
    LOG_WARN("fail to append index config spec key", KPC(this), K(ret));
  } else if (OB_FAIL(init_ref_.reference_to_json(buf))) {
    LOG_WARN("fail to append init_config reference", KPC(this), K(ret));
  } else if (OB_FAIL(dynamic_ref_.reference_to_json(buf))) {
    LOG_WARN("fail to append dynamic_config reference", KPC(this), K(ret));
  } else if (OB_FAIL(limit_control_ref_.reference_to_json(buf))) {
    LOG_WARN("fail to append limit_control_config reference", KPC(this), K(ret));
  } else if (OB_FAIL(fuse_control_ref_.reference_to_json(buf))) {
    LOG_WARN("fail to append fuse_control_config reference", KPC(this), K(ret));
  } else {
    // trim last ','
    char *buf_start = buf.ptr();
    if (',' == buf_start[buf.length() - 1]) {
      buf_start[buf.length() - 1] = '}';
    } else {
      ret = buf.append("}");
    }
  }
  return ret;
}

//---------------ObProxyInitConfig--------------
int64_t ObProxyInitConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObProxyBaseConfig::to_string(buf + pos, buf_len - pos);
  J_OBJ_END();
  return pos;
}

int ObProxyInitConfig::assign(const ObProxyInitConfig &other)
{
  int ret = OB_SUCCESS;
  ret = ObProxyBaseConfig::assign(other);
  return ret;
}

//---------------ObProxyDynamicConfig--------------
int64_t ObProxyDynamicConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObProxyBaseConfig::to_string(buf + pos, buf_len - pos);
  J_OBJ_END();
  return pos;
}

int ObProxyDynamicConfig::assign(const ObProxyDynamicConfig &other)
{
  int ret = OB_SUCCESS;
  ret = ObProxyBaseConfig::assign(other);
  return ret;
}

//---------------ObProxyLimitControlConfig--------------
ObProxyLimitConfig::~ObProxyLimitConfig()
{
  int64_t count = cond_array_.count();
  for (int64_t i = 0; i < count; i++) {
    cond_array_.at(i)->~ObProxyQosCond();
  }
}

int64_t ObProxyLimitConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_name), K_(tenant_name), K_(database_name), K_(user_name),
       K_(limit_name), K_(limit_mode), K_(limit_priority), K_(limit_qps),
       K_(limit_status), K_(limit_time_window), K_(limit_conn));
  J_OBJ_END();
  return pos;
}

int ObProxyLimitConfig::init(ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(limit_rule_map_.create(LIMIT_RULE_MAP_BUCKET, ObModIds::OB_PROXY_QOS, ObModIds::OB_PROXY_QOS))) {
    LOG_WARN("fail to init limit rule map", K(ret));
  } else {
    allocator_ = &allocator;
  }

  return ret;
}

int ObProxyLimitConfig::copy_param(ObString &param, const ObString &value)
{
  int ret = OB_SUCCESS;

  if (!param.empty()) {
    allocator_->free(param.ptr());
  }

  if (OB_FAIL(ob_write_string(*allocator_, value, param))) {
    LOG_WARN("fail to write string", K(value), K(ret));
  }

  return ret;
}

int ObProxyLimitConfig::push_limit_rule(const ObString &key, const ObString &value)
{
  int ret = OB_SUCCESS;

  ObString new_key;
  ObString new_value;
  if (OB_FAIL(ob_write_string(*allocator_, key, new_key))) {
    LOG_WARN("fail to write string", K(key), K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, value, new_value))) {
    LOG_WARN("fail to write string", K(value), K(ret));
  } else if (OB_FAIL(limit_rule_map_.set_refactored(new_key, new_value))) {
    LOG_WARN("fail to set limit rule map", K(new_key), K(new_value), K(ret));
  }

  return ret;
}

int ObProxyLimitConfig::parse_limit_action(const ObProxyLimitMode limit_mode)
{
  int ret = OB_SUCCESS;

  ObProxyQosAction *action = NULL;

  if (LIMIT_MODE_DB_USER_MATCH == limit_mode
      || LIMIT_MODE_KEY_WORD_MATCH == limit_mode
      || LIMIT_MODE_GRADE_MATCH == limit_mode
      || LIMIT_MODE_TESTLOAD_LIMIT == limit_mode) {
    ret = create_action_or_cond<ObProxyQosActionLimit>(action);
  } else if (LIMIT_MODE_TESTLOAD_FUSE == limit_mode) {
    ret = create_action_or_cond<ObProxyQosActionCircuitBreaker>(action);
  } else if (LIMIT_MODE_HIGH_RISK_FUSE == limit_mode) {
    ret = create_action_or_cond<ObProxyQosActionBreaker>(action);
  }

  if (OB_SUCC(ret)) {
    limit_mode_ = limit_mode;
    action_ = action;
  } else {
    LOG_WARN("fail to alloc parse limit action", K(limit_mode), K(ret));
  }

  return ret;
}

int ObProxyLimitConfig::parse_limit_rule(const ObString &key, const ObString &value)
{
  int ret = OB_SUCCESS;

  ObProxyQosCond *cond = NULL;

  if (key == LIMIT_SQL_TYPE) {
    ret = create_action_or_cond<ObProxyQosCondStmtType>(cond);
    if (0 == value.case_compare("ALL")) {
      ((ObProxyQosCondStmtType*)cond)->set_stmt_kind(OB_PROXY_QOS_COND_STMT_KIND_ALL);
    } else {
      ((ObProxyQosCondStmtType*)cond)->set_stmt_type(get_stmt_type_by_name(value));
    }
  } else if (key == LIMIT_TABLE_NAME) {
    if (OB_FAIL(create_action_or_cond<ObProxyQosCondTableName>(cond))) {
      LOG_WARN("fail to create cond", K(value), K(ret));
    } else if (OB_FAIL(((ObProxyQosCondTableName*)cond)->init(value, allocator_))){
      LOG_WARN("fail to init table name cond", K(value), K(ret));
    }

    // qps, rt in rule must use with TESTLOAD_FUSE mode
  } else if (key == LIMIT_QPS) {
    if (OB_FAIL(get_int_value(value, limit_rule_qps_))) {
      LOG_WARN("fail to get int", K(key), K(value), K(ret));
    }
  } else if (key == LIMIT_RT) {
    if (OB_FAIL(get_int_value(value, limit_rule_rt_))) {
      LOG_WARN("fail to get int", K(key), K(value), K(ret));
    }
  } else if (key == LIMIT_TIME_WINDOW) {
    if (OB_FAIL(get_int_value(value, limit_time_window_))) {
      LOG_WARN("fail to get int", K(key), K(value), K(ret));
    } else if (limit_time_window_ <= 0 || limit_time_window_ > 10) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid limit_time_window value", K(limit_time_window_));
    }
  } else if (key == LIMIT_CONN) {
    if (OB_FAIL(get_double_value(value, limit_conn_))) {
      LOG_WARN("fail to get double", K(key), K(value), K(ret));
    } else if (limit_conn_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid limit conn", K(limit_conn_));
    }
  } else if (key == LIMIT_FUSE_TIME) {
    if (OB_FAIL(get_int_value(value, limit_fuse_time_))) {
      LOG_WARN("fail to get int", K(key), K(value), K(ret));
    } else if (limit_fuse_time_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid limit_fuse_time", K(ret));
    }
  } else if (key == LIMIT_KEY_WORDS) {
    ObString key_words = value;
    ObString key_word = key_words.split_on('~');
    while (OB_SUCC(ret) && !key_word.empty()) {
      if (OB_FAIL(create_action_or_cond<ObProxyQosCondSQLMatch>(cond))) {
        LOG_WARN("fail to create cond", K(value), K(ret));
      } else if (OB_FAIL(((ObProxyQosCondSQLMatch*)cond)->init(key_word, allocator_))){
        LOG_WARN("fail to init sql match cond", K(value), K(key_word), K(ret));
      } else {
        cond_array_.push_back(cond);
        cond = NULL;
        key_word = key_words.split_on('~');
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_action_or_cond<ObProxyQosCondSQLMatch>(cond))) {
        LOG_WARN("fail to create cond", K(value), K(ret));
      } else if (OB_FAIL(((ObProxyQosCondSQLMatch*)cond)->init(key_words, allocator_))){
        LOG_WARN("fail to init sql match cond", K(value), K(key_words), K(ret));
      }
    }

  } else if (key == LIMIT_CONDITION) {
    if (0 == value.case_compare(LIMIT_CONDITION_NO_WHERE)) {
      if (OB_FAIL(create_action_or_cond<ObProxyQosCondNoWhere>(cond))) {
        LOG_WARN("fail to create cond", K(value), K(ret));
      }
    } else if (0 == value.case_compare(LIMIT_CONDITION_USE_LIKE)) {
      if (OB_FAIL(create_action_or_cond<ObProxyQosCondUseLike>(cond))) {
        LOG_WARN("fail to create cond", K(value), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(push_limit_rule(key, value))) {
      LOG_WARN("fail to set limit rule map", K(key), K(value), K(ret));
    } else if(OB_NOT_NULL(cond)) {
      cond_array_.push_back(cond);
    }
  } else {
    LOG_WARN("fail to parse limit rule", K(key), K(value), K(ret));
  }

  return ret;
}

int ObProxyLimitConfig::handle_action()
{
  int ret = OB_SUCCESS;

  if (NULL != action_) {
    if (OB_PROXY_QOS_ACTION_TYPE_CIRCUIT_BREAKER == action_->get_action_type()) {
      ObProxyQosActionCircuitBreaker *cb_action = reinterpret_cast<ObProxyQosActionCircuitBreaker*>(action_);
      cb_action->set_cluster_name(cluster_name_);
      cb_action->set_tenant_name(tenant_name_);
      cb_action->set_database_name(database_name_);
      cb_action->set_user_name(user_name_);
      cb_action->set_qps(limit_rule_qps_);
      cb_action->set_rt(msec_to_usec(limit_rule_rt_));
      cb_action->set_time_window(limit_time_window_);
      cb_action->set_limit_conn(limit_conn_);
      cb_action->set_limit_fuse_time(limit_fuse_time_);

      if (limit_time_window_ > 0) {
        ObProxyQosCond *cond = NULL;
        if (OB_FAIL(create_action_or_cond<ObProxyQosCondTestLoadTableName>(cond))) {
          LOG_WARN("fail to create cond", K(ret));
        } else if(OB_NOT_NULL(cond)) {
          cond_array_.push_back(cond);
        }
      }
    } else if (OB_PROXY_QOS_ACTION_TYPE_LIMIT == action_->get_action_type()) {
      ObProxyQosActionLimit *limit_action = reinterpret_cast<ObProxyQosActionLimit*>(action_);
      limit_action->set_limit_qps(limit_qps_);
    }
  }

  return ret;
}

int ObProxyLimitConfig::assign(ObProxyLimitConfig &other)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(set_cluster_name(other.get_cluster_name()))) {
    LOG_WARN("fail to set cluster name", K(ret));
  } else if (OB_FAIL(set_tenant_name(other.get_tenant_name()))) {
    LOG_WARN("fail to set tenant name", K(ret));
  } else if (OB_FAIL(set_database_name(other.get_database_name()))) {
    LOG_WARN("fail to set database name", K(ret));
  } else if (OB_FAIL(set_user_name(other.get_user_name()))) {
    LOG_WARN("fail to set user name", K(ret));
  } else if (OB_FAIL(set_limit_name(other.get_limit_name()))) {
    LOG_WARN("fail to set limit name", K(ret));
  } else if (OB_FAIL(parse_limit_action(other.get_limit_mode()))) {
    LOG_WARN("fail to parse limit action", K(ret));
  } else {
    set_limit_priority(other.get_limit_priority());
    set_limit_qps(other.get_limit_qps());
    set_limit_status(other.get_limit_status());

    hash::ObHashMap<ObString, ObString>::iterator it = other.get_limit_rule().begin();
    hash::ObHashMap<ObString, ObString>::iterator end = other.get_limit_rule().end();
    for (; OB_SUCC(ret) && it != end; it++) {
      if (OB_FAIL(parse_limit_rule(it->first, it->second))) {
        LOG_WARN("fail to set limit rule map", "key: ", it->first, ", value: ", it->second, K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (handle_action()) {
        LOG_WARN("fail to handle action", K(ret));
      }
    }
  }

  return ret;
}

int ObProxyLimitConfig::calc(ObMysqlTransact::ObTransState &trans_state, const ObClientSessionInfo &cs_info,
                             ObIAllocator *calc_allocator, bool &is_pass, ObString &limit_name)
{
  int ret = OB_SUCCESS;
  bool is_match = true;
  is_pass = true;

  if (LIMIT_STATUS_OBSERVE == limit_status_ || LIMIT_STATUS_RUNNING == limit_status_) {
    ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;

    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < cond_array_.count(); i++) {
      is_match = false;
      ObProxyQosCond *cond = cond_array_.at(i);
      if (OB_FAIL(cond->calc(client_request, calc_allocator, is_match))) {
        LOG_WARN("fail to calc cond", KPC(cond), K(ret));
      }
    }

    if (OB_SUCC(ret) && is_match) {
      if (OB_FAIL(action_->calc(is_pass))) {
        LOG_WARN("fail to calc action", KPC_(action), K(ret));
      }
    }

    if (OB_SUCC(ret) && !is_pass) {
      limit_name = limit_name_;
      if (LIMIT_STATUS_OBSERVE == limit_status_) {
        is_pass = true;
      }

      const DBServerType database_type = cs_info.get_server_type();
      const char *database_type_str = ObProxyMonitorUtils::get_database_type_name(database_type);

      const ObSqlParseResult &parse_result = trans_state.trans_info_.client_request_.get_parse_result();
      const ObString &table_name = parse_result.get_table_name();
      const char *sql_cmd = ObProxyParserUtils::get_sql_cmd_name(trans_state.trans_info_.sql_cmd_);

      ObProxyBasicStmtType stmt_type = parse_result.get_stmt_type();

      const char *stmt_type_str = "";
      ObString new_sql;
      char new_sql_buf[PRINT_SQL_LEN] = "\0";
      if (OB_MYSQL_COM_QUERY == trans_state.trans_info_.sql_cmd_
          || OB_MYSQL_COM_STMT_PREPARE == trans_state.trans_info_.sql_cmd_
          || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == trans_state.trans_info_.sql_cmd_) {
        stmt_type_str = get_print_stmt_name(stmt_type);

        const ObString &origin_sql = trans_state.trans_info_.get_print_sql();
        int32_t new_sql_len = 0;
        ObProxyMonitorUtils::sql_escape(origin_sql.ptr(), origin_sql.length(),
                                        new_sql_buf, PRINT_SQL_LEN, new_sql_len);
        new_sql.assign_ptr(new_sql_buf, new_sql_len);
      }

      _OBPROXY_LIMIT_LOG(INFO, MONITOR_LIMIT_LOG_FORMAT, MONITOR_LIMIT_LOG_PARAM);
    }
  }

  return ret;
}

ObProxyLimitControlConfig::~ObProxyLimitControlConfig()
{
  int64_t count = limit_config_array_.count();
  for (int64_t i = 0; i < count; i++) {
    limit_config_array_.at(i)->~ObProxyLimitConfig();
  }
}

int64_t ObProxyLimitControlConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObProxyBaseConfig::to_string(buf + pos, buf_len - pos);
  J_KV(K_(limit_config_array));
  J_OBJ_END();
  return pos;
}

int ObProxyLimitControlConfig::calc(ObMysqlTransact::ObTransState &trans_state, const ObClientSessionInfo &cs_info,
                                    ObIAllocator *calc_allocator, bool &is_pass, ObString &limit_name)
{
  int ret = OB_SUCCESS;
  is_pass = true;

  ObString cluster_name;
  ObString tenant_name;
  ObString database_name;
  ObString user_name;
  cs_info.get_cluster_name(cluster_name);
  cs_info.get_tenant_name(tenant_name);
  cs_info.get_user_name(user_name);

  const ObSqlParseResult &parse_result = trans_state.trans_info_.client_request_.get_parse_result();
  database_name = parse_result.get_database_name();
  if (OB_UNLIKELY(database_name.empty())) {
    cs_info.get_database_name(database_name);
  }

  for (int64_t i = 0; OB_SUCC(ret) && is_pass && i < limit_config_array_.count(); i++) {
    ObProxyLimitConfig *limit_conf = limit_config_array_.at(i);
    if ((LIMIT_STATUS_OBSERVE == limit_conf->get_limit_status()
         || LIMIT_STATUS_RUNNING == limit_conf->get_limit_status())
        && 0 == limit_conf->get_cluster_name().case_compare(cluster_name)
        && 0 == limit_conf->get_tenant_name().case_compare(tenant_name)
        && 0 == limit_conf->get_database_name().case_compare(database_name)
        && 0 == limit_conf->get_user_name().case_compare(user_name)) {
      if (OB_FAIL(limit_conf->calc(trans_state, cs_info, calc_allocator, is_pass, limit_name))) {
        LOG_WARN("fail to calc limit conf", KPC(limit_conf), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyLimitControlConfig::parse_limit_rule(Value &json_value, ObProxyLimitConfig *limit_config)
{
  int ret = OB_SUCCESS;

  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (OB_FAIL(limit_config->parse_limit_rule(it->name_, it->value_->get_string()))) {
        LOG_WARN("fail to parse limit rule", "key: ", it->name_, ", value: ", it->value_->get_string(), K(ret));
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }

  return ret;
}

int ObProxyLimitControlConfig::parse_limit_conf(Value &json_value)
{
  int ret = OB_SUCCESS;

  if (JT_ARRAY == json_value.get_type()) {
    ObProxyLimitConfig *limit_config = NULL;
    DLIST_FOREACH(it, json_value.get_array()) {
      if (JT_OBJECT == it->get_type()) {
        limit_config = NULL;

        void *ptr = allocator_.alloc(sizeof(ObProxyLimitConfig));
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc ObProxyLimitConfig", K(ret));
        } else if (FALSE_IT(limit_config = new(ptr) ObProxyLimitConfig())) {
        } else if (OB_FAIL(limit_config->init(allocator_))) {
          LOG_WARN("fail to init ObProxyLimitConfig", K(ret));
        }

        DLIST_FOREACH(p, it->get_object()) {
          if (p->name_ == CLUSTER_NAME) {
            ret = limit_config->set_cluster_name(p->value_->get_string());

          } else if (p->name_ == TENANT_NAME) {
            ret = limit_config->set_tenant_name(p->value_->get_string());

          } else if (p->name_ == DATABASE_NAME) {
            ret = limit_config->set_database_name(p->value_->get_string());

          } else if (p->name_ == USER_NAME) {
            ret = limit_config->set_user_name(p->value_->get_string());

          } else if (p->name_ == LIMIT_NAME) {
            ret = limit_config->set_limit_name(p->value_->get_string());

          } else if (p->name_ == LIMIT_PRIORITY) {
            int64_t limit_priority = -1;
            if (OB_FAIL(get_int_value(p->value_->get_string(), limit_priority))) {
              LOG_WARN("fail to get int", "key:", p->name_, "value:", p->value_->get_string(), K(ret));
            } else {
              limit_config->set_limit_priority(limit_priority);
            }

          } else if (p->name_ == LIMIT_QPS) {
            int64_t limit_qps = -1;
            if (OB_FAIL(get_int_value(p->value_->get_string(), limit_qps))) {
              LOG_WARN("fail to get int", "key:", p->name_, "value:", p->value_->get_string(), K(ret));
            } else {
              limit_config->set_limit_qps(limit_qps);
            }

          } else if (p->name_ == LIMIT_STATUS) {
            limit_config->set_limit_status(get_limit_status_by_str(p->value_->get_string()));

            // just handle first limit_mode
          } else if (p->name_ == LIMIT_MODE && NULL == limit_config->get_action()) {
            if (OB_FAIL(limit_config->parse_limit_action(get_limit_mode_by_str(p->value_->get_string())))) {
              LOG_WARN("fail to parse limit action", K(ret));
            }

          } else if (p->name_ == LIMIT_RULE) {
            if (OB_FAIL(parse_limit_rule(*(p->value_), limit_config))) {
              LOG_WARN("fail to parse limit rule", K(ret));
            }
          }

          if (OB_FAIL(ret)) {
            LOG_WARN("fail to handle limit conf", "name:", p->name_, "value:", p->value_, K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(limit_config->handle_action())) {
            LOG_WARN("fail to handle action", K(ret));
          } else {
            limit_config_array_.push_back(limit_config);
          }
        }
      } else {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
                 "actual type", it->get_type(), K(ret));
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_ARRAY,
             "actual type", json_value.get_type(), K(ret));
  }

  return ret;
}

int ObProxyLimitControlConfig::parse_config_spec(Value &json_value)
{
  int ret = OB_SUCCESS;

  Value *json_limiter = NULL;

  if (JT_OBJECT == json_value.get_type()) {
    DLIST_FOREACH(it, json_value.get_object()) {
      if (it->name_ == LIMITERS) {
        json_limiter = it->value_;
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json_value.get_type(), K(ret));
  }

  if (OB_SUCC(ret) && NULL != json_limiter) {
    if (OB_FAIL(parse_limit_conf(*json_limiter))) {
      LOG_WARN("fail to parse limit conf", K(ret));
    }
  }
  return ret;
}

int ObProxyLimitControlConfig::spec_to_json(ObSqlString &buf) const
{
  int ret = OB_SUCCESS;

  ret = buf.append_fmt("\"%s\": {\"%s\": [", CONFIG_SPEC, LIMITERS);
  for (int64_t i = 0; OB_SUCC(ret) && i < limit_config_array_.count();) {
    ObProxyLimitConfig *limit_conf = limit_config_array_.at(i);
    if (OB_FAIL(buf.append_fmt("{ \"%s\": \"%.*s\", \"%s\": \"%.*s\", \"%s\": \"%.*s\", \"%s\": \"%.*s\","
                               "\"%s\": \"%.*s\", \"%s\": \"%s\","
                               "\"%s\": \"%ld\", \"%s\": \"%ld\", \"%s\": \"%s\","
                               "\"%s\": {",
                                  CLUSTER_NAME, limit_conf->get_cluster_name().length(), limit_conf->get_cluster_name().ptr(),
                                  TENANT_NAME, limit_conf->get_tenant_name().length(), limit_conf->get_tenant_name().ptr(),
                                  DATABASE_NAME, limit_conf->get_database_name().length(), limit_conf->get_database_name().ptr(),
                                  USER_NAME, limit_conf->get_user_name().length(), limit_conf->get_user_name().ptr(),

                                  LIMIT_NAME, limit_conf->get_limit_name().length(), limit_conf->get_limit_name().ptr(),
                                  LIMIT_MODE, get_limit_mode_str(limit_conf->get_limit_mode()),

                                  LIMIT_PRIORITY, limit_conf->get_limit_priority(),
                                  LIMIT_QPS, limit_conf->get_limit_qps(),
                                  LIMIT_STATUS, get_limit_status_str(limit_conf->get_limit_status()),
                                  LIMIT_RULE))) {
      LOG_WARN("fail to append config", K(ret));
    }

    // save limit_rule
    if (OB_SUCC(ret)) {
      int64_t size = limit_conf->get_limit_rule().size();
      hash::ObHashMap<ObString, ObString>::iterator it = limit_conf->get_limit_rule().begin();
      hash::ObHashMap<ObString, ObString>::iterator end = limit_conf->get_limit_rule().end();
      for (int64_t j = 0; OB_SUCC(ret) && it != end; it++) {
        if (OB_FAIL(buf.append_fmt("\"%.*s\": \"%.*s\"",
                                   it->first.length(), it->first.ptr(),
                                   it->second.length(), it->second.ptr()))) {
          LOG_WARN("fail to append config", K(ret));
        } else if (j < size - 1) {
          ret = buf.append(",");
        }
        j++;
      }

      if (OB_SUCC(ret)) {
        ret = buf.append("}}");
      }
    }

    if (OB_SUCC(ret)) {
      if (i < limit_config_array_.count() - 1) {
        ret = buf.append(",");
      }
    }
    ++i;
  }

  if (OB_SUCC(ret)) {
    ret = buf.append("]}");
  }

  return ret;
}

int ObProxyLimitControlConfig::assign(const ObProxyLimitControlConfig &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyBaseConfig::assign(other))) {
    LOG_WARN("fail to assign ObProxyBaseConfig", K(ret));
  } else {
    const ObIArray<ObProxyLimitConfig*> &other_limit_config_array = other.get_limit_config_array();
    ObProxyLimitConfig *limit_config = NULL;

    for (int64_t i = 0; OB_SUCC(ret) && i < other_limit_config_array.count(); i++) {
      ObProxyLimitConfig *other_limit_config = other_limit_config_array.at(i);

      void *ptr = allocator_.alloc(sizeof(ObProxyLimitConfig));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObProxyLimitConfig", K(ret));
      } else if (FALSE_IT(limit_config = new(ptr) ObProxyLimitConfig())) {
      } else if (OB_FAIL(limit_config->init(allocator_))) {
        LOG_WARN("fail to init ObProxyLimitConfig", K(ret));
      } else if (OB_FAIL(limit_config->assign(*other_limit_config))) {
        LOG_WARN("fail to assign ObProxyLimitConfig", K(ret));
      } else {
        limit_config_array_.push_back(limit_config);
      }
    }
  }

  return ret;
}

//---------------ObProxyAppConfig--------------
int64_t ObProxyAppConfig::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(app_name),
       K_(version),
       K_(index_config),
       K_(init_config),
       K_(dynamic_config),
       K_(limit_control_config),
       "congfig_state:", get_config_state_str(ac_state_));
  J_OBJ_END();
  return pos;
}

void ObProxyAppConfig::destroy()
{
  LOG_INFO("app config will be destroyed", KPC(this));
  op_free(this);
}

int ObProxyAppConfig::alloc_building_app_config(const ObString &appname, const ObString &version, ObProxyAppConfig *&app_config)
{
  int ret = OB_SUCCESS;
  app_config = NULL;
  if (OB_ISNULL(app_config = op_alloc(ObProxyAppConfig))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for proxy app config", K(appname), K(ret));
  } else {
    // version is empty string when load from local
    app_config->app_name_.set_value(appname);
    app_config->version_.set_value(version);
    app_config->index_config_.app_name_.set_value(appname);
    app_config->index_config_.version_.set_value(version);
    app_config->inc_ref();
    app_config->set_building_state();
    if (OB_FAIL(get_global_proxy_config_processor().add_app_config(*app_config, INDEX_BUILDING))) {
      LOG_WARN("fail to add building app config", K(appname), K(ret));
    }
  }
  if (OB_FAIL(ret) && NULL != app_config) {
    app_config->dec_ref();
    app_config = NULL;
  }
  return ret;
}

int ObProxyAppConfig::update_config(const ObString &json_str, const ObProxyConfigType type, const bool is_from_local/*false*/)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
  Value *json_root = NULL;
  Parser parser;
  if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json_str.ptr(), json_str.length(), json_root))) {
    LOG_WARN("parse json failed", K(ret), "json_str", get_print_json(json_str));
  } else if (OB_ISNULL(json_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json root is null", K(ret));
  } else {
    switch (type) {
      case INDEX_CONFIG:
        ret = index_config_.parse_from_json(*json_root, is_from_local);
        if (OB_SUCC(ret) && OB_FAIL(handle_index_config(is_from_local))) {
          LOG_WARN("fail to handle index config", K(ret));
        }
        break;
      case INIT_CONFIG:
        ret = init_config_.parse_from_json(*json_root, is_from_local);
        break;
      case DYNAMIC_CONFIG:
        ret = dynamic_config_.parse_from_json(*json_root, is_from_local);
        break;
      case LIMIT_CONTROL_CONFIG:
        ret = limit_control_config_.parse_from_json(*json_root, is_from_local);
        break;
      case FUSE_CONTROL_CONFIG:
        ret = fuse_control_config_.parse_from_json(*json_root, is_from_local);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown config type", K(type), K(ret));
    }
  }
  return ret;
}

int ObProxyAppConfig::dump_to_local()
{
  int ret = OB_SUCCESS;
  char tmp_path[FileDirectoryUtils::MAX_PATH];
  snprintf(tmp_path, FileDirectoryUtils::MAX_PATH, "%s/%s.tmp",
           get_global_layout().get_control_config_dir(), app_name_.ptr());
  // 1. write to tmp dir
  if (OB_FAIL(index_config_.dump_to_local(tmp_path))) {
    LOG_WARN("fail to dump index config into file", K(tmp_path), K(ret));
  } else if (OB_FAIL(init_config_.dump_to_local(tmp_path))) {
    LOG_WARN("fail to dump init config into file", K(tmp_path), K(ret));
  } else if (OB_FAIL(dynamic_config_.dump_to_local(tmp_path))) {
    LOG_WARN("fail to dump dynamic config into file", K(tmp_path), K(ret));
  } else if (OB_FAIL(limit_control_config_.dump_to_local(tmp_path))) {
    LOG_WARN("fail to dump limit control config into file", K(tmp_path), K(ret));
  } else if (OB_FAIL(fuse_control_config_.dump_to_local(tmp_path))) {
    LOG_WARN("fail to dump limit control config into file", K(tmp_path), K(ret));
  }

  // 2. move dir
  if (OB_SUCC(ret)) {
    get_global_proxy_config_processor().move_local_dir(app_name_.config_string_);
  }
  return ret;
}

int ObProxyAppConfig::load_from_local(const char *file_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_config_.load_from_local(file_path, *this))) {
    LOG_WARN("fail to load index config from local", K(file_path), K(ret));
  }
  if (OB_SUCC(ret) && index_config_.init_ref_.is_valid()) {
    if (OB_FAIL(init_config_.load_from_local(file_path, *this))) {
      LOG_WARN("fail to load init config from local", K(file_path), K(ret));
    }
  }
  if (OB_SUCC(ret) && index_config_.dynamic_ref_.is_valid()) {
    if (OB_FAIL(dynamic_config_.load_from_local(file_path, *this))) {
      LOG_WARN("fail to load dynamic config from local", K(file_path), K(ret));
    }
  }
  if (OB_SUCC(ret) && index_config_.limit_control_ref_.is_valid()) {
    if (OB_FAIL(limit_control_config_.load_from_local(file_path, *this))) {
      LOG_WARN("fail to load limit control config from local", K(file_path), K(ret));
    }
  }
  if (OB_SUCC(ret) && index_config_.fuse_control_ref_.is_valid()) {
    if (OB_FAIL(fuse_control_config_.load_from_local(file_path, *this))) {
      LOG_WARN("fail to load limit control config from local", K(file_path), K(ret));
    }
  }
  return ret;
}

int ObProxyAppConfig::handle_index_config(const bool is_from_local)
{
  int ret = OB_SUCCESS;
  if (is_from_local) {
    version_.set_value(index_config_.version_.config_string_);
  }
  ObProxyAppConfig *cur_config = get_global_proxy_config_processor().get_app_config(app_name_.config_string_);
  if (index_config_.init_ref_.is_valid()) {
    init_config_.version_.set_value(index_config_.init_ref_.version_.config_string_);
    init_config_.app_name_.set_value(app_name_.config_string_);
    if (NULL != cur_config
        && cur_config->index_config_.init_ref_.is_valid()
        && init_config_.version_ == cur_config->init_config_.version_) {
      if (OB_FAIL(init_config_.assign(cur_config->init_config_))) {
        LOG_WARN("fail to copy init config", K_(init_config), K_(cur_config->init_config), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && index_config_.dynamic_ref_.is_valid()) {
    dynamic_config_.version_.set_value(index_config_.dynamic_ref_.version_.config_string_);
    dynamic_config_.app_name_.set_value(app_name_.config_string_);
    if (NULL != cur_config
        && cur_config->index_config_.dynamic_ref_.is_valid()
        && dynamic_config_.version_ == cur_config->dynamic_config_.version_) {
      if (OB_FAIL(dynamic_config_.assign(cur_config->dynamic_config_))) {
        LOG_WARN("fail to copy dynamic config", K_(dynamic_config), K_(cur_config->dynamic_config), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && index_config_.limit_control_ref_.is_valid()) {
    limit_control_config_.version_.set_value(index_config_.limit_control_ref_.version_.config_string_);
    limit_control_config_.app_name_.set_value(app_name_.config_string_);
    if (NULL != cur_config
        && cur_config->index_config_.limit_control_ref_.is_valid()
        && limit_control_config_.version_ == cur_config->limit_control_config_.version_) {
      if (OB_FAIL(limit_control_config_.assign(cur_config->limit_control_config_))) {
        LOG_WARN("fail to copy limit control config", K_(limit_control_config), K_(cur_config->limit_control_config), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && index_config_.fuse_control_ref_.is_valid()) {
    fuse_control_config_.version_.set_value(index_config_.fuse_control_ref_.version_.config_string_);
    fuse_control_config_.app_name_.set_value(app_name_.config_string_);
    if (NULL != cur_config
        && cur_config->index_config_.fuse_control_ref_.is_valid()
        && fuse_control_config_.version_ == cur_config->fuse_control_config_.version_) {
      if (OB_FAIL(fuse_control_config_.assign(cur_config->fuse_control_config_))) {
        LOG_WARN("fail to copy limit control config", K_(fuse_control_config), K_(cur_config->fuse_control_config), K(ret));
      }
    }
  }
  if (NULL != cur_config) {
    cur_config->dec_ref();
    cur_config = NULL;
  }
  return ret;
}

int ObProxyAppConfig::calc_limit(ObMysqlTransact::ObTransState &trans_state, const ObClientSessionInfo &cs_info,
                                 ObIAllocator *calc_allocator, bool &is_pass, ObString &limit_name)
{
  int ret = OB_SUCCESS;
  is_pass = true;

  if (OB_FAIL(fuse_control_config_.calc(trans_state, cs_info,
                                        calc_allocator, is_pass, limit_name))) {
    LOG_WARN("fail to calc fuse control", K(ret));
  } else if (is_pass && OB_FAIL(limit_control_config_.calc(trans_state, cs_info,
                                        calc_allocator, is_pass, limit_name))) {
    LOG_WARN("fail to calc limit control", K(ret));
  }

  return ret;
}

//---------------ObProxyConfigProcessor--------------
int ObProxyConfigProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(load_local_config())) {
    LOG_WARN("fail to load local app config", K(ret));
  }
  return ret;
}

ObProxyAppConfig *ObProxyConfigProcessor::get_app_config(const ObString &app_name, const int64_t version_index /*INDEX_CURRENT*/)
{
  int ret = OB_SUCCESS;
  ObProxyAppConfig *app_config = NULL;
  if (OB_LIKELY(version_index < MAX_VERSION_SIZE)) {
    DRWLock::RDLockGuard lock(rw_locks_[version_index]);
    if (OB_FAIL(ac_maps_[version_index].get_refactored(app_name, app_config))) {
      LOG_DEBUG("app config does not exist", K(version_index), K(app_name), K(ret));
    } else if (INDEX_CURRENT == version_index
               && OB_UNLIKELY(!app_config->is_avail_state())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("app config is not avail", K(version_index), KPC(app_config), K(app_name), K(ret));
    } else if (INDEX_BUILDING == version_index
               && OB_UNLIKELY(!app_config->is_building_state())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("building app config is not in building state", K(version_index), KPC(app_config), K(app_name), K(ret));
    }
    if (OB_SUCC(ret)) {
      app_config->inc_ref();
    } else {
      app_config = NULL;
    }
  }
  return app_config;
}

int ObProxyConfigProcessor::get_app_config_string(const ObString &app_name,
                                                  const ObProxyConfigType type,
                                                  ObSqlString &buf)
{
  int ret = OB_SUCCESS;
  ObProxyAppConfig *app_config = NULL;
  if (OB_ISNULL(app_config = get_app_config(app_name)) && type != SECURITY_CONFIG) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("app config does not exist", K(app_name), K(ret));
  } else {
    switch (type) {
      case INDEX_CONFIG:
        ret = app_config->index_config_.to_json_str(buf);
        break;
      case INIT_CONFIG:
        ret = app_config->init_config_.to_json_str(buf);
        break;
      case DYNAMIC_CONFIG:
        ret = app_config->dynamic_config_.to_json_str(buf);
        break;
      case LIMIT_CONTROL_CONFIG:
        ret = app_config->limit_control_config_.to_json_str(buf);
        break;
      case FUSE_CONTROL_CONFIG:
        ret = app_config->fuse_control_config_.to_json_str(buf);
        break;
      case SECURITY_CONFIG:
        if (get_global_ssl_config_table_processor().is_ssl_key_info_valid("*", "*")) {
          ret = buf.append_fmt("SSL INFO VALID");
        } else {
          ret = buf.append_fmt("SSL INFO INVALID");
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown config type", K(type), K(ret));
    }
  }
  if (NULL != app_config) {
    app_config->dec_ref();
    app_config = NULL;
  }
  return ret;
}

int ObProxyConfigProcessor::add_app_config(ObProxyAppConfig &app_config, const int64_t version_index)
{
  int ret = OB_SUCCESS;
  ObProxyAppConfig *old_config = NULL;
  if (OB_UNLIKELY(version_index >= MAX_VERSION_SIZE) || OB_UNLIKELY(version_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version index", K(app_config.app_name_), K(version_index), K(ret));
  } else {
    DRWLock::WRLockGuard lock(rw_locks_[version_index]);
    if (INDEX_CURRENT == version_index) {
      // remember to set avail before add into current map
      app_config.set_avail_state();
    }
    old_config = ac_maps_[version_index].remove(app_config.app_name_.config_string_);
    if (OB_FAIL(ac_maps_[version_index].unique_set(&app_config))) {
      LOG_WARN("fail to add app config", K(app_config), K(version_index));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ac_maps_[version_index].unique_set(old_config))) {
        LOG_WARN("fail to rollback old app config into map", KPC(old_config), K(tmp_ret));
      } else {
        old_config = NULL;
      }
    } else {
      app_config.inc_ref();
    }
  }
  if (NULL != old_config) {
    old_config->dec_ref();
    old_config = NULL;
  }
  return ret;
}

int ObProxyConfigProcessor::remove_app_config(const ObString &appname, const int64_t version_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(version_index >= MAX_VERSION_SIZE) || OB_UNLIKELY(version_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid version index", K(appname), K(version_index), K(ret));
  } else {
    DRWLock::WRLockGuard lock(rw_locks_[version_index]);
    ObProxyAppConfig *app_config = ac_maps_[version_index].remove(appname);
    if (NULL != app_config) {
      app_config->set_deleting_state();
      app_config->dec_ref();
      app_config = NULL;
    }
  }
  return ret;
}

int ObProxyConfigProcessor::update_app_config(const ObString &appname,
                                              const ObString &version,
                                              const ObString &config_value,
                                              const ObProxyConfigType type)
{
  int ret = OB_SUCCESS;
  ObProxyAppConfig *cur_app_config = get_app_config(appname);
  ObProxyAppConfig *building_app_config = NULL;
  if (NULL != cur_app_config
      && version == cur_app_config->version_.config_string_) {
      // the same version as current config, no need update
      LOG_DEBUG("the same app config version, no need update", K(appname), K(version));
  } else {
    building_app_config = get_app_config(appname, INDEX_BUILDING);
    if (NULL != building_app_config) {
      if (version != building_app_config->version_.config_string_) {
        if (INDEX_CONFIG == type) {
          LOG_INFO("will delete old building app config and build a new one", "old building version", building_app_config->version_.ptr(),
                   "new version", version.ptr());
          building_app_config->dec_ref();
          building_app_config = NULL;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("building app config version and updating version is mismatched", K(appname), K(version),
                   K(building_app_config->version_), K(type), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && NULL == building_app_config) {
      // NULL == building_app_config, alloc new config
      if (INDEX_CONFIG != type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("new building app config only support INDEX_CONFIG", K(appname), K(version), K(type), K(ret));
      } else if (OB_FAIL(ObProxyAppConfig::alloc_building_app_config(appname, version, building_app_config))) {
        LOG_WARN("fail to alloc building app config", K(appname), K(version), K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != building_app_config) {
      if (OB_FAIL(building_app_config->update_config(config_value, type))) {
        LOG_WARN("fail to update app config", K(appname), K(version), K(type), K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != building_app_config
        && building_app_config->is_all_config_complete()) {
      if (OB_FAIL(handle_app_config_complete(*building_app_config))) {
        LOG_WARN("fail to handle app config complete", KPC(building_app_config), K(ret));
      }
    }
    if (OB_FAIL(ret) && NULL != building_app_config) {
      int tmp_ret = OB_SUCCESS;
      // remove building app config from map
      // maybe it has already been deleted in handle_app_config_complete
      if (OB_SUCCESS != (tmp_ret = remove_app_config(appname, INDEX_BUILDING))) {
        LOG_WARN("fail to remove building app config", K(appname), K(tmp_ret));
      }
    }
  }

  if (NULL != cur_app_config) {
    cur_app_config->dec_ref();
    cur_app_config = NULL;
  }
  if (NULL != building_app_config) {
    building_app_config->dec_ref();
    building_app_config = NULL;
  }
  return ret;
}

int ObProxyConfigProcessor::load_local_config()
{
  int ret = OB_SUCCESS;
  struct dirent *ent = NULL;
  const char *layout_etc_dir = get_global_layout().get_control_config_dir();
  DIR *etc_dir = NULL;
  if (OB_ISNULL(layout_etc_dir)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("etc layout is null", K(ret));
  } else if (OB_ISNULL(etc_dir = opendir(layout_etc_dir))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to open dir", K(layout_etc_dir), KERRMSGS, K(ret));
  }
  event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  while (OB_SUCC(ret) && NULL != (ent = readdir(etc_dir))) {
    allocator.reuse();
    // we only load current config dir
    if (ent->d_type == DT_DIR
        && NULL == memchr(ent->d_name, '.', ent->d_reclen)) {
      if (OB_FAIL(load_local_app_config(ObString::make_string(ent->d_name)))) {
        LOG_WARN("fail to load app config", "app name", ent->d_name, K(ret));
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != etc_dir) && OB_UNLIKELY(0 != closedir(etc_dir))) {
    tmp_ret = OB_IO_ERROR;
    LOG_WARN("fail to close dir", "dir", layout_etc_dir, KERRMSGS, K(tmp_ret));
  }
  return ret;
}

int ObProxyConfigProcessor::load_local_app_config(const ObString &app_name)
{
  int ret = OB_SUCCESS;
  ObProxyAppConfig *app_config = NULL;
  if (OB_FAIL(ObProxyAppConfig::alloc_building_app_config(app_name, ObString::make_empty_string(), app_config))) {
    LOG_WARN("fail to alloc building app config", K(app_name), K(ret));
  } else {
    char file_path[FileDirectoryUtils::MAX_PATH];
    snprintf(file_path, FileDirectoryUtils::MAX_PATH, "%s/%s",
             get_global_layout().get_control_config_dir(), app_name.ptr());
    if (OB_FAIL(app_config->load_from_local(file_path))) {
      LOG_WARN("fail to load config from current dir, now try to load from old dir", K(file_path), K(app_name), K(ret));
      file_path[0] = '\0';
      snprintf(file_path, FileDirectoryUtils::MAX_PATH, "%s/%s.old",
               get_global_layout().get_control_config_dir(), app_name.ptr());
      if (OB_FAIL(app_config->load_from_local(file_path))) {
        LOG_WARN("fail to load config from old dir", K(file_path), K(app_name), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && NULL != app_config
      && app_config->is_all_config_complete()) {
    bool is_from_local = true;
    if (OB_FAIL(handle_app_config_complete(*app_config, is_from_local))) {
      LOG_WARN("fail to handle app config complete for local config", K(is_from_local), KPC(app_config), K(ret));
    } else {
      LOG_INFO("succ to load app config from local", KPC(app_config));
    }
  }
  if (OB_FAIL(ret) && NULL != app_config) {
    int tmp_ret = OB_SUCCESS;
    // remove building app config from map,
    // maybe it has already been deleted in handle_app_config_complete
    if (OB_SUCCESS != (tmp_ret = remove_app_config(app_name, INDEX_BUILDING))) {
      LOG_WARN("fail to remove building app config", K(app_name), K(tmp_ret));
    }
  }
  if (NULL != app_config) {
    app_config->dec_ref();
    app_config = NULL;
  }
  return ret;
}

// if config from odp_agent:
//   1. dump to local file
//   2. replace current app config
//   3. update ObProxyConfig using init config and dynamic config
// if config from local file:
//   1. replace current app config
//   2. update ObProxyConfig using init config and dynamic config
int ObProxyConfigProcessor::handle_app_config_complete(ObProxyAppConfig &new_app_config, const bool is_from_local/*false*/)
{
  int ret = OB_SUCCESS;
  const ObString &app_name = new_app_config.app_name_.config_string_;
  ObProxyAppConfig *cur_app_config = get_app_config(app_name);
  bool need_rollback_dir = false;
  bool need_rollback_cur_config = false;
  if (is_from_local) {
    // from local: update cur app config
    if (OB_FAIL(replace_app_config(app_name, new_app_config, cur_app_config))) {
      LOG_WARN("fail to replace current config with new config", K(app_name), K(new_app_config), K(ret));
    }
  } else {
    // from remote: dump app config to local
    if (OB_FAIL(new_app_config.dump_to_local())) {
      LOG_WARN("fail to dump app config to local file", K(new_app_config), K(ret));
    } else if (OB_FAIL(replace_app_config(app_name, new_app_config, cur_app_config))) {
      // from remote: update cur app config
      LOG_WARN("fail to replace current config with new config", K(app_name), K(new_app_config), K(ret));
      need_rollback_dir = true;
    }
  }
  if (OB_SUCC(ret)) {
    // update global proxy config
    if (OB_FAIL(update_global_proxy_config(new_app_config, cur_app_config))) {
      LOG_WARN("fail to update global proxy config", K(new_app_config), K(ret));
      need_rollback_dir = !is_from_local;
      need_rollback_cur_config = !is_from_local;
    }
  }
  if (OB_FAIL(ret)) {
    bool is_rollback = true;
    if (need_rollback_dir) {
      move_local_dir(app_name, is_rollback);
    }
    if (need_rollback_cur_config) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = replace_app_config(app_name, new_app_config, cur_app_config, is_rollback))) {
        LOG_WARN("fail to rollback current config", K(app_name), K(tmp_ret));
      }
    }
  }

  clean_tmp_dir(app_name);

  if (NULL != cur_app_config) {
    cur_app_config->dec_ref();
    cur_app_config = NULL;
  }
  return ret;
}

int ObProxyConfigProcessor::replace_app_config(const ObString &app_name,
                                               ObProxyAppConfig &new_app_config,
                                               ObProxyAppConfig *&cur_app_config,
                                               const bool is_rollback/*false*/)
{
  int ret = OB_SUCCESS;
  if (!is_rollback) {
    // delete new app config from building map and add into current config map
    if (OB_FAIL(remove_app_config(app_name, INDEX_BUILDING))) {
      LOG_WARN("fail to remove building app config from building map", K(app_name), K(ret));
    } else if (OB_FAIL(add_app_config(new_app_config, INDEX_CURRENT))) {
      LOG_WARN("fail to add new app config into current map", K(new_app_config), K(ret));
    }
  } else {
    if (NULL == cur_app_config) {
      if (OB_FAIL(remove_app_config(app_name, INDEX_CURRENT))) {
        LOG_WARN("fail to remove new app config from current map", K(app_name), K(ret));
      }
    } else  {
      if (OB_FAIL(add_app_config(*cur_app_config, INDEX_CURRENT))) {
        LOG_WARN("fail to rollback app config into current map", KPC(cur_app_config), K(ret));
      }
    }
  }
  return ret;
}

// dump order:
//   app.old  ---> app.old.bak
//   app ---> app.old
//   app.tmp ---> app
// rollbak order:
//   app ---> app.tmp
//   app.old ---> app
//   app.old.bak ---> app.old
void ObProxyConfigProcessor::move_local_dir(const ObString &app_name, const bool is_rollback/*false*/)
{
  char tmp_path[FileDirectoryUtils::MAX_PATH];
  char cur_path[FileDirectoryUtils::MAX_PATH];
  char old_path[FileDirectoryUtils::MAX_PATH];
  char old_bak_path[FileDirectoryUtils::MAX_PATH];
  snprintf(tmp_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s.tmp",
           get_global_layout().get_control_config_dir(), app_name.length(), app_name.ptr());
  snprintf(cur_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s",
           get_global_layout().get_control_config_dir(), app_name.length(), app_name.ptr());
  snprintf(old_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s.old",
           get_global_layout().get_control_config_dir(), app_name.length(), app_name.ptr());
  snprintf(old_bak_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s.old.bak",
           get_global_layout().get_control_config_dir(), app_name.length(), app_name.ptr());
  if (!is_rollback) {
    ObProxyFileUtils::clear_dir(old_bak_path); // need delete old.bak, otherwise move will fail
    ObProxyFileUtils::move_file_dir(old_path, old_bak_path);
    ObProxyFileUtils::move_file_dir(cur_path, old_path);
    ObProxyFileUtils::move_file_dir(tmp_path, cur_path);
  } else {
    ObProxyFileUtils::clear_dir(tmp_path); // need delete tmp_path, otherwise move will fail
    ObProxyFileUtils::move_file_dir(cur_path, tmp_path);
    ObProxyFileUtils::move_file_dir(old_path, cur_path);
    ObProxyFileUtils::move_file_dir(old_bak_path, old_path);
  }
}

void ObProxyConfigProcessor::clean_tmp_dir(const ObString &app_name)
{
  char tmp_path[FileDirectoryUtils::MAX_PATH];
  char old_bak_path[FileDirectoryUtils::MAX_PATH];
  snprintf(tmp_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s.tmp",
           get_global_layout().get_control_config_dir(), app_name.length(), app_name.ptr());
  snprintf(old_bak_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s.old.bak",
           get_global_layout().get_control_config_dir(), app_name.length(), app_name.ptr());
  ObProxyFileUtils::clear_dir(tmp_path);
  ObProxyFileUtils::clear_dir(old_bak_path);
}

int ObProxyConfigProcessor::update_global_proxy_config(const ObProxyAppConfig &new_app_config, const ObProxyAppConfig *cur_app_config)
{
  int ret = OB_SUCCESS;
  ObProxyReloadConfig *reload_config = NULL;
  ObSEArray<ObConfigItem *, 16> old_global_config_items; // store old global config item value
  if (OB_ISNULL(reload_config = get_global_internal_cmd_processor().get_reload_config())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get reload config", K(ret));
  }
  // Update the configuration of dynamic_config, the init configuration passes in the parameters at startup
  if (OB_SUCC(ret)) {
    if (NULL == cur_app_config || new_app_config.dynamic_config_.version_ != cur_app_config->dynamic_config_.version_) {
      if (OB_FAIL(do_update_global_proxy_config(new_app_config.dynamic_config_, old_global_config_items))) {
        LOG_WARN("fail to update dynamic config to global proxy config", K_(new_app_config.dynamic_config), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL((*reload_config)(get_global_proxy_config()))) {
      LOG_WARN("fail to reload global config", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rollback_global_proxy_config(old_global_config_items))) {
      LOG_WARN("fail to rollback global proxy config", K(tmp_ret));
    }
  }

  for (int64_t i = 0; i < old_global_config_items.count(); ++i) {
    ObConfigItem *&old_config_item = old_global_config_items.at(i);
    if (NULL != old_config_item) {
     delete old_config_item;
    }
  }
  return ret;
}

int ObProxyConfigProcessor::rollback_global_proxy_config(const ObIArray<ObConfigItem *> &old_config_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_config_items.count(); ++i) {
    const ObConfigItem *item = old_config_items.at(i);
    if (NULL != item) {
      if (OB_FAIL(get_global_proxy_config().update_config_item(ObString::make_string(item->name()), ObString::make_string(item->str())))) {
        LOG_WARN("fail to update config", "name", item->name(), "value", item->str(), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyConfigProcessor::do_update_global_proxy_config(const ObProxyBaseConfig &base_config, ObIArray<ObConfigItem *> &old_config_items)
{
  int ret = OB_SUCCESS;
  ObConfigItem *const *pp_global_item = NULL;
  ObConfigItem *old_config_item = NULL;
  ObString key_string;
  ObString value_string;
  const ObConfigContainer &other_container = const_cast<ObProxyBaseConfig &>(base_config).get_container();
  ObConfigContainer::const_iterator it = other_container.begin();
  for (; OB_SUCC(ret) && it != other_container.end(); ++it) {
    pp_global_item = NULL;
    old_config_item = NULL;
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config item is null", "name", it->first.str(), K(ret));
    } else {
      key_string.assign_ptr(it->first.str(), static_cast<int32_t>(strlen(it->first.str())));
      value_string.assign_ptr(it->second->str(), static_cast<int32_t>(strlen(it->second->str())));
      if (key_string == get_global_proxy_config().app_name.name()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("app_name can only modified when restart", K(ret));
      } else if (OB_ISNULL(pp_global_item = get_global_proxy_config().get_container().get(ObConfigStringKey(key_string)))) {
        /* make compatible with previous configuration */
        LOG_WARN("Invalid config string, no such config item", K(key_string));
      } else if (OB_ISNULL(old_config_item = (*pp_global_item)->clone())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to clone global app config item to new app item", K(key_string), K(ret));
      } else if (OB_FAIL(old_config_items.push_back(old_config_item))) {
        LOG_WARN("fail to push back old config item", K(key_string), K(ret));
        delete old_config_item;
        old_config_item = NULL;
      } else if (OB_FAIL(get_global_proxy_config().update_config_item(key_string, value_string))) {
        LOG_WARN("fail to update config", K(key_string), K(value_string), K(ret));
      } else {
        LOG_DEBUG("succ to update config", K(key_string), K(value_string));
      }
    }
  } // end for

  return ret;
}

ObProxyLimitStatus get_limit_status_by_str(const ObString &limit_status_str)
{
  ObProxyLimitStatus limit_status = LIMIT_STATUS_INVALID;

  if (0 == limit_status_str.case_compare("OBSERVER")) {
    limit_status = LIMIT_STATUS_OBSERVE;
  } else if (0 == limit_status_str.case_compare("RUNNING")) {
    limit_status = LIMIT_STATUS_RUNNING;
  } else if (0 == limit_status_str.case_compare("SHUTDOWN")) {
    limit_status = LIMIT_STATUS_SHUTDOWN;
  }

  return limit_status;
}

const char *get_limit_status_str(const ObProxyLimitStatus limit_status)
{
  const char *ret_str = "LIMIT_STATUS_INVALID";
  switch (limit_status) {
    case LIMIT_STATUS_OBSERVE:
      ret_str = "OBSERVER";
      break;
    case LIMIT_STATUS_RUNNING:
      ret_str = "RUNNING";
      break;
    case LIMIT_STATUS_SHUTDOWN:
      ret_str = "SHUTDOWN";
      break;
    default:
     break;
  }
  return ret_str;
}

ObProxyLimitMode get_limit_mode_by_str(const ObString &limit_mode_str)
{
  ObProxyLimitMode limit_mode = LIMIT_MODE_INVALID;

  if (0 == limit_mode_str.case_compare("DB_USER_MATCH")) {
    limit_mode = LIMIT_MODE_DB_USER_MATCH;
  } else if (0 == limit_mode_str.case_compare("KEY_WORD_MATCH")) {
    limit_mode = LIMIT_MODE_KEY_WORD_MATCH;
  } else if (0 == limit_mode_str.case_compare("GRADE_MATCH")) {
    limit_mode = LIMIT_MODE_GRADE_MATCH;
  } else if (0 == limit_mode_str.case_compare("TESTLOAD_FUSE")) {
    limit_mode = LIMIT_MODE_TESTLOAD_FUSE;
  } else if (0 == limit_mode_str.case_compare("HIGH_RISK_FUSE")) {
    limit_mode = LIMIT_MODE_HIGH_RISK_FUSE;
  } else if (0 == limit_mode_str.case_compare("TESTLOAD_LIMIT")) {
    limit_mode = LIMIT_MODE_TESTLOAD_LIMIT;
  }

  return limit_mode;
}

const char *get_limit_mode_str(const ObProxyLimitMode limit_mode)
{
  const char *ret_str = "LIMIT_MODE_INVALID";
  switch (limit_mode) {
    case LIMIT_MODE_DB_USER_MATCH:
      ret_str = "DB_USER_MATCH";
      break;
    case LIMIT_MODE_KEY_WORD_MATCH:
      ret_str = "KEY_WORD_MATCH";
      break;
    case LIMIT_MODE_GRADE_MATCH:
      ret_str = "GRADE_MATCH";
      break;
    case LIMIT_MODE_TESTLOAD_FUSE:
      ret_str = "TESTLOAD_FUSE";
      break;
    case LIMIT_MODE_HIGH_RISK_FUSE:
      ret_str = "HIGH_RISK_FUSE";
      break;
    case LIMIT_MODE_TESTLOAD_LIMIT:
      ret_str = "TESTLOAD_LIMIT";
      break;
    default:
     break;
  }
  return ret_str;
}

const char *get_config_state_str(const ObProxyAppConfigState state)
{
  const char *ret_str = "AC_INVALID";
  switch (state) {
    case AC_BUILDING:
      ret_str = "AC_BUILDING";
      break;
    case AC_AVAIL:
      ret_str = "AC_AVAIL";
      break;
    case AC_DELETING:
      ret_str = "AC_DELETING";
      break;
    default:
     break;
  }
  return ret_str;
}

const char *get_config_type_str(const ObProxyConfigType type)
{
  const char *ret_str = "INVALID_CONFIG";
  switch (type) {
    case INDEX_CONFIG:
      ret_str = "metadata";
      break;
    case INIT_CONFIG:
      ret_str = "launch";
      break;
    case DYNAMIC_CONFIG:
      ret_str = "dynamic";
      break;
    case LIMIT_CONTROL_CONFIG:
      ret_str = "current_limit";
      break;
    case FUSE_CONTROL_CONFIG:
      ret_str = "current_fuse";
      break;
    default:
      break;
  }
  return ret_str;
}

const char *get_config_file_name(const ObProxyConfigType type)
{
  const char *ret_str = NULL;
  switch (type) {
    case INDEX_CONFIG:
      ret_str = "globalConfig.json";
      break;
    case INIT_CONFIG:
      ret_str = "initConfig.json";
      break;
    case DYNAMIC_CONFIG:
      ret_str = "dynamicConfig.json";
      break;
    case LIMIT_CONTROL_CONFIG:
      ret_str = "limitControlConfig.json";
      break;
    case FUSE_CONTROL_CONFIG:
      ret_str = "fuseControlConfig.json";
      break;
    default:
      break;
  }
  return ret_str;
}

ObProxyConfigType get_config_type_by_str(const ObString& config_type)
{
  static const char *config_type_strs[] = { "index_config", "init_config", "dynamic_config",
                                            "limit_config", "fuse_config", "security_config"};
  ObProxyConfigType ret = INVALID_CONFIG;
  for (int32_t i = 0; i < ARRAYSIZEOF(config_type_strs); ++i) {
    if (static_cast<int32_t>(strlen(config_type_strs[i])) == config_type.length()
        && 0 == strncasecmp(config_type_strs[i], config_type.ptr(), config_type.length())) {
      ret = static_cast<ObProxyConfigType>(i);
      break;
    }
  }
  return ret;
}

ObProxyBasicStmtType get_stmt_type_by_name(const ObString &stmt_name)
{
  ObProxyBasicStmtType stmt_type = OBPROXY_T_INVALID;

  if (0 == stmt_name.case_compare("SELECT")) {
    stmt_type = OBPROXY_T_SELECT;
  } else if (0 == stmt_name.case_compare("UPDATE")) {
    stmt_type = OBPROXY_T_UPDATE;
  } else if (0 == stmt_name.case_compare("INSERT")) {
    stmt_type = OBPROXY_T_INSERT;
  } else if (0 == stmt_name.case_compare("REPLACE")) {
    stmt_type = OBPROXY_T_REPLACE;
  } else if (0 == stmt_name.case_compare("DELETE")) {
    stmt_type = OBPROXY_T_DELETE;
  } else if (0 == stmt_name.case_compare("MERGE")) {
    stmt_type = OBPROXY_T_MERGE;
  }

  return stmt_type;
}

int ObProxyConfigProcessor::update_app_security_config(const ObString &appname,
                                                       const ObString &version,
                                                       const ObString &config_value,
                                                       const ObProxyConfigType type)
{
  int ret = OB_SUCCESS;
  UNUSED(appname);
  UNUSED(version);
  UNUSED(type);

  DRWLock::WRLockGuard lock(security_rw_lock_);
  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
  Value *json_root = NULL;
  Parser parser;
  if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(config_value.ptr(), config_value.length(), json_root))) {
    LOG_WARN("parse json failed", K(ret), "json_str", get_print_json(config_value), K(config_value.length()));
  } else if (OB_ISNULL(json_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json root is null", K(ret));
  } else {
    if (JT_OBJECT == json_root->get_type()) {
      DLIST_FOREACH(it, json_root->get_object()) {
        if (it->name_ == SECURITY_SOURCE_TYPE) {
          if (it->value_->get_string() == "INVALID")  {
            security_config_.source_type_.reset();
            security_config_.ca_.reset();
            security_config_.public_key_.reset();
            security_config_.private_key_.reset();
          } else {
            security_config_.source_type_.set_value(it->value_->get_string());
          }
        } else if (it->name_ == SECURITY_CA) {
          security_config_.ca_.set_value(it->value_->get_string());
        } else if (it->name_ == SECURITY_PUBLIC_KEY) {
          security_config_.public_key_.set_value(it->value_->get_string());
        } else if (it->name_ == SECURITY_PRIVATE_KEY) {
          security_config_.private_key_.set_value(it->value_->get_string());
        }
      }

      if (!security_config_.source_type_.get_string().empty() && !security_config_.ca_.get_string().empty()
          && !security_config_.public_key_.get_string().empty() && !security_config_.private_key_.get_string().empty()) {
        const int64_t json_len = security_config_.source_type_.get_string().length()
                          + security_config_.ca_.get_string().length()
                          + security_config_.public_key_.get_string().length()
                          + security_config_.private_key_.get_string().length() + 256;
        char *json_buf = (char*)ob_malloc(json_len + 1, ObModIds::OB_PROXY_CONFIG_TABLE);
        if (OB_ISNULL(json_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate json_buf failed", K(ret), K(json_len));
        } else {
          const char *json_string = "{\"sourceType\": \"%.*s\", \"CA\": \"%.*s\", \"publicKey\": \"%.*s\", \"privateKey\": \"%.*s\"}";
          int64_t len = static_cast<int64_t>(snprintf(json_buf, json_len, json_string,
                security_config_.source_type_.get_string().length(), security_config_.source_type_.get_string().ptr(),
                security_config_.ca_.get_string().length(), security_config_.ca_.get_string().ptr(),
                security_config_.public_key_.get_string().length(), security_config_.public_key_.get_string().ptr(),
                security_config_.private_key_.get_string().length(), security_config_.private_key_.get_string().ptr()));
          if (OB_UNLIKELY(len <= 0 || len > json_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fill sql failed", K(len), K(json_len), K(ret));
          } else if (OB_FAIL(get_global_config_processor().store_cloud_config("ssl_config", "*", "*", "key_info", json_buf))) {
            LOG_WARN("execute sql failed", K(ret));
          }
        }

        if (OB_NOT_NULL(json_buf)) {
          ob_free(json_buf);
        }
      }
    }
  }

  return ret;
}

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase
