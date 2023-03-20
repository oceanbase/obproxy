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

#include "obutils/ob_proxy_json_config_info.h"
#include "common/ob_record_header.h"
#include "lib/string/ob_sql_string.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_route_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

static const char *JSON_HTTP_MESSAGE                      = "Message";
static const char *HTTP_SUCC_MESSAGE                      = "successful";
static const char *JSON_HTTP_STATUS                       = "Success";
static const char *JSON_HTTP_CODE                         = "Code";
static const char *JSON_CONFIG_DATA                       = "Data";
static const char *JSON_CONFIG_VERSION                    = "Version";
static const char *JSON_CLUSTER_LIST                      = "ObRootServiceInfoUrlList";
static const char *JSON_OB_REGION                         = "ObRegion";
static const char *JSON_OB_CLUSTER                        = "ObCluster";
static const char *JSON_OB_REGION_ID                      = "ObRegionId";
static const char *JSON_OB_CLUSTER_ID                     = "ObClusterId";
static const char *JSON_OB_CLUSTER_TYPE                   = "Type";
static const char *JSON_REAL_META_REGION                  = "ObRealMetaRegion";
static const char *JSON_RS_URL                            = "ObRootServiceInfoUrl";
static const char *JSON_RS_LIST                           = "RsList";
static const char *JSON_ADDRESS                           = "address";
static const char *JSON_SQL_PORT                          = "sql_port";
static const char *JSON_ROLE                              = "role";
static const char *JSON_META_TABLE_INFO                   = "ObProxyDatabaseInfo";
static const char *JSON_META_DATABASE                     = "DataBase";
static const char *JSON_META_USER                         = "User";
static const char *JSON_META_PASSWORD                     = "Password";
static const char *JSON_BIN_URL                           = "ObProxyBinUrl";
static const char *JSON_IDC_LIST                          = "IDCList";
static const char *JSON_REGION                            = "region";
static const char *JSON_IDC                               = "idc";
static const char *JSON_READONLY_RS_LIST                  = "ReadonlyRsList";
static const char *LEADER_ROLE                            = "LEADER";
static const char *FOLLOWER_ROLE                          = "FOLLOWER";
static const char *PRIMARY_ROLE                           = "PRIMARY";
static const char *STANDBY_ROLE                           = "STANDBY";
static const char *JSON_ROOT_SERVICE_INFO_URL_TEMPLATE    = "ObRootServiceInfoUrlTemplate";
static const char *JSON_ROOT_SERVICE_INFO_URL_TEMPLATE_V2 = "ObRootServiceInfoUrlTemplateV2";
static const char *JSON_DATA_CLUSTER_LIST                 = "ObClusterList";

static const char *REGION_TEMPLATE                        = "${ObRegion}";
static const char *CLUSTER_TEMPLATE                       = "${ObCluster}";

const char *cluster_role_to_str(ObClusterRole role)
{
  const char *str_ret = "";
  switch(role) {
    case PRIMARY:
      str_ret = "PRIMARY";
      break;
    case STANDBY:
      str_ret = "STANDBY";
      break;
    case INVALID_CLUSTER_ROLE:
    default:
      LOG_WARN("invalid role", K(role));
      break;
  }
  return str_ret;
}

ObClusterRole str_to_cluster_role(const ObString &role_str)
{
  ObClusterRole role = INVALID_CLUSTER_ROLE;
  if (role_str.case_compare(PRIMARY_ROLE) == 0) {
    role = PRIMARY;
  } else if (role_str.case_compare(STANDBY_ROLE) == 0) {
    role = STANDBY;
  } else {
    LOG_WARN("invalid role_str", K(role_str));
  }
  return role;
}


//------ ObProxyConfigString------
DEF_TO_STRING(ObProxyConfigString)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(config_string));
  J_OBJ_END();
  return pos;
}

int ObProxyConfigString::parse(const Value *json_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_STRING))) {
    LOG_WARN("fail to check json string type", K(json_value), K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_string(json_value->get_string(), size_limit_))) {
    LOG_WARN("fail to check config string", K(json_value->get_string()), K(ret));
  } else {
    set_value(json_value->get_string());
  }
  return ret;
}

//------ ObProxyConfigUrl------
DEF_TO_STRING(ObProxyConfigUrl)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(url));
  J_OBJ_END();
  return pos;
}

int ObProxyConfigUrl::parse(const Value *value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(value, JT_STRING))) {
    LOG_WARN("fail to check rs url typr", K(value), K(ret));
  } else if (OB_UNLIKELY(NULL != url_str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cluster info, url should be null", K(url_str_), K(ret));
  } else if (OB_ISNULL(url_str_ = static_cast<char *>(allocator.alloc(value->get_string().length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem for rs url", K(ret));
  } else {
    MEMCPY(url_str_, value->get_string().ptr(), value->get_string().length());
    url_str_[value->get_string().length()] = '\0';
    url_.assign_ptr(url_str_, value->get_string().length());
  }
  return ret;
}

int ObProxyConfigUrl::set_url(char *buffer, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid url for set url", K(ret));
  } else {
    url_str_ = buffer;
    url_.assign_ptr(url_str_, static_cast<int32_t>(len));
  }

  return ret;
}

//------ ObProxyClusterInfo------
int64_t ObProxyClusterInfo::get_rs_list_count() const
{
  int64_t count = 0;
  SubCIHashMap &map = const_cast<SubCIHashMap &>(sub_ci_map_);
  for (SubCIHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    count += it->web_rs_list_.count();
  }
  return count;
}

inline int64_t ObProxyClusterInfo::get_idc_list_count() const
{
  int64_t count = 0;
  SubCIHashMap &map = const_cast<SubCIHashMap &>(sub_ci_map_);
  for (SubCIHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    count += it->idc_list_.count();
  }
  return count;
}

int ObProxyClusterInfo::get_rs_list_hash(const int64_t cluster_id, uint64_t &hash)
{
  int ret = OB_SUCCESS;
  int64_t real_cluster_id = cluster_id;
  if (OB_DEFAULT_CLUSTER_ID == cluster_id) {
    real_cluster_id = master_cluster_id_;
  }
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(get_sub_cluster_info(real_cluster_id, sub_cluster_info))) {
    LOG_WARN("cluster not exist, ignore", K_(cluster_name), K(real_cluster_id), K(ret));
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub cluster info is null", K_(cluster_name), K(cluster_id), K(ret));
  } else {
    hash = sub_cluster_info->rs_list_hash_;
  }
  return ret;
}

//------ ObProxySubClusterInfo------
DEF_TO_STRING(ObProxySubClusterInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_id), K_(is_used), K_(role), K_(web_rs_list), K_(rs_list_hash),
       K_(create_failure_count), K_(idc_list));
  J_OBJ_END();
  return pos;
}

int ObProxySubClusterInfo::update_rslist(const LocationList &web_rs_list, const uint64_t cur_rs_list_hash)
{
  int ret = OB_SUCCESS;
  if (!is_web_rs_list_changed(web_rs_list)) {
    ret = OB_EAGAIN;
  } else {
    LOG_INFO("will update rslist",
             "old rslist", web_rs_list_,
             "new rslist", web_rs_list, K_(cluster_id));
    if (OB_FAIL(web_rs_list_.assign(web_rs_list))) {
      LOG_WARN("fail to set cluster web_rs_list", K_(cluster_id), K(web_rs_list), K(ret));
    } else {
      if (0 == cur_rs_list_hash) {
        rs_list_hash_ = ObProxyClusterInfo::get_server_list_hash(web_rs_list);
      } else {
        rs_list_hash_ =  cur_rs_list_hash;
      }
    }
  }
  return ret;
}

int ObProxySubClusterInfo::get_idc_region(const common::ObString &idc_name,
    ObProxyNameString &region_name) const
{
  int ret = OB_SUCCESS;
  if (!idc_list_.empty() && !idc_name.empty() && idc_name.length() < common::MAX_PROXY_IDC_LENGTH) {
    ObProxyNameString lower_case_idc_name;
    lower_case_idc_name.set_value(idc_name);
    lower_case_idc_name.to_lower_case();
    bool found = false;
    const uint64_t hash = lower_case_idc_name.hash();
    for (int64_t i = 0; !found && i < idc_list_.count(); i++) {
      if (hash == idc_list_[i].idc_hash_ && lower_case_idc_name == idc_list_[i].idc_name_) {
        found = true;
        region_name.set_value(idc_list_[i].region_name_);
        LOG_DEBUG("succ to find idc region", K(lower_case_idc_name), K(region_name), K(ret));
      }
    }
  }
  return ret;
}

//------ ObProxyClusterInfo------
DEF_TO_STRING(ObProxyClusterInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(rs_url), K_(cluster_name), K_(master_cluster_id));
  SubCIHashMap &map = const_cast<SubCIHashMap &>(sub_ci_map_);
  for (SubCIHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "sub_cluster_info", *it);
  }
  J_OBJ_END();
  return pos;
}

ObProxyClusterInfo::ObProxyClusterInfo()
  : rs_url_(), cluster_name_(OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1),
    master_cluster_id_(OB_DEFAULT_CLUSTER_ID)
{
  reset();
}

void ObProxyClusterInfo::destroy()
{
  SubCIHashMap::iterator last = sub_ci_map_.end();
  for (SubCIHashMap::iterator it = sub_ci_map_.begin(); it != last; ++it) {
    LOG_DEBUG("destroy sub cluster info", K_(cluster_name), K_(it->cluster_id));
    it->destroy();
  }
  sub_ci_map_.reset();
  op_free(this);
}

void ObProxyClusterInfo::reset()
{
  rs_url_.reset();
  cluster_name_.reset();
  master_cluster_id_ = OB_DEFAULT_CLUSTER_ID;
}

int ObProxyClusterInfo::get_sub_cluster_info(const int64_t cluster_id, ObProxySubClusterInfo *&sub_cluster_info) const
{
  int ret = OB_SUCCESS;
  sub_cluster_info = NULL;
  if (OB_SUCCESS != sub_ci_map_.get_refactored(OB_DEFAULT_CLUSTER_ID == cluster_id ? master_cluster_id_ : cluster_id,
                                               sub_cluster_info)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("cluster not exist", K_(cluster_name), K_(master_cluster_id), K(cluster_id), K(ret));
  }
  return ret;
}

int ObProxyClusterInfo::parse(const Value *json_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check cluster info", K(json_value), K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == JSON_OB_REGION) {
        if(OB_FAIL(cluster_name_.parse(it->value_))) {
          LOG_WARN("fail to parse cluster name", K(it->name_), K(ret));
        }
      } else if (it->name_ == JSON_RS_URL) {
        if (OB_FAIL(rs_url_.parse(it->value_, allocator))) {
          LOG_WARN("fail to parse rs url", K(it->name_), K(ret));
        }
      } else {
        // ignore other json key name, for later compatibile with new config items
      }
    }//end of DLIST_FOREACH
  }
  return ret;
}

uint64_t ObProxyClusterInfo::get_server_list_hash(const LocationList &rs_list)
{
  uint64_t hash = 0;
  const void *data = NULL;
  int32_t len = 0;
  for (int64_t i = 0; i < rs_list.count(); ++i) {
    data = &(rs_list.at(i));
    len = static_cast<int32_t>(sizeof(proxy::ObProxyReplicaLocation));
    hash += murmurhash(data, len, 0);
  }
  return hash;
}

//------ ObProxyMetaTableInfo------
DEF_TO_STRING(ObProxyMetaTableInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(db), K_(username), K_(password), K_(real_cluster_name), K_(cluster_info));
  J_OBJ_END();
  return pos;
}

ObProxyMetaTableInfo::ObProxyMetaTableInfo()
  : db_(OB_MAX_DATABASE_NAME_LENGTH),
    username_(OB_PROXY_FULL_USER_NAME_MAX_LEN),
    password_(OB_PROXY_MAX_PASSWORD_LENGTH),
    real_cluster_name_(OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1)
{
  reset();
}

void ObProxyMetaTableInfo::reset()
{
  db_.reset();
  username_.reset();
  password_.reset();
  real_cluster_name_.reset();
  cluster_info_.reset();
}

int ObProxyMetaTableInfo::parse(const Value *json_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check meta table info", K(json_value), K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == JSON_META_DATABASE) {
        if (OB_FAIL(db_.parse(it->value_))) {
          LOG_WARN("fail to parse meta db", K(it->name_), K(ret));
        }
      } else if (it->name_ == JSON_META_USER) {
        if (OB_FAIL(username_.parse(it->value_))) {
          LOG_WARN("fail to parse meta username", K(it->name_), K(ret));
        } else if (OB_FAIL(check_and_trim_username())) {
          LOG_WARN("fail to check and trim username", K(ret));
        }
      } else if (it->name_ == JSON_META_PASSWORD) {
        if (OB_FAIL(password_.parse(it->value_))) {
          LOG_WARN("fail to parse meta password", K(it->name_), K(ret));
        }
      } else if (it->name_ == OB_META_DB_CLUSTER_NAME) {
        cluster_info_.cluster_name_.set_value(it->name_);
        if (OB_FAIL(cluster_info_.rs_url_.parse(it->value_, allocator))) {
          LOG_WARN("fail to parse meta db rs url", K(OB_META_DB_CLUSTER_NAME), K(it->name_), K(ret));
        }
      } else {
        // ignore other json key name, for later compatibile with new config items
      }
    }//end of DLIST_FOREACH
  }
  return ret;
}

int ObProxyMetaTableInfo::check_and_trim_username()
{
  int ret = OB_SUCCESS;
  ObString cluster = username_.after('#');
  if (OB_UNLIKELY(!cluster.empty())) {
    if (OB_UNLIKELY(cluster != OB_META_DB_CLUSTER_NAME)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("username parsed from config server contains an unexpected cluster name", K_(username), K(ret));
    } else {
      // trim cluster name
      ObString tmp_str(username_.length() - cluster.length() - 1, username_.ptr());
      username_.set_value(tmp_str);
    }
  }
  return ret;
}

//------ ObProxyClusterArrayInfo------
DEF_TO_STRING(ObProxyClusterArrayInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(default_cluster_name));
  CIHashMap &map = const_cast<CIHashMap &>(ci_map_);
  for (CIHashMap::iterator it = map.begin(); it != map.end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "cluster_info", *it);
  }
  J_OBJ_END();
  return pos;
}

ObProxyClusterArrayInfo::ObProxyClusterArrayInfo()
{
}

ObProxyClusterArrayInfo::~ObProxyClusterArrayInfo()
{
  destroy();
}

void ObProxyClusterArrayInfo::destroy()
{
  CIHashMap::iterator last = ci_map_.end();
  for (CIHashMap::iterator it = ci_map_.begin(); it != last; ++it) {
    LOG_DEBUG("destroy cluster info", K(it->cluster_name_));
    it->destroy();
  }
  ci_map_.reset();
}

bool ObProxyClusterArrayInfo::is_valid() const
{
  bool bret = true;
  CIHashMap &map = const_cast<CIHashMap &>(ci_map_);
  for (CIHashMap::iterator it = map.begin(); bret && it != map.end(); ++it) {
    if (!it->is_valid()) {
      bret = false;
    }
  }
  return 0 != count() && bret;
}

int ObProxyClusterArrayInfo::parse(const Value *json_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_ARRAY))) {
    LOG_WARN("fail to check cluster array info", K(json_value), K(ret));
  } else {
    ObProxyClusterInfo *cluster_info = NULL;
    DLIST_FOREACH(it, json_value->get_array()) {
      cluster_info = NULL;
      cluster_info = op_alloc(ObProxyClusterInfo);
      if (OB_LIKELY(NULL != cluster_info)) {
        if (OB_FAIL(cluster_info->parse(it, allocator))) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("fail to parse cluster info", K(it), K(ret));
        } else {
          ret = ci_map_.unique_set(cluster_info);
          if (OB_SUCCESS == ret) {
          } else if (OB_HASH_EXIST == ret) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("cluster info already exist", K(ret));
          } else { }
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for cluster info", K(cluster_info), K(ret));
      }
      if (OB_FAIL(ret) && OB_LIKELY(NULL != cluster_info)) {
        op_free(cluster_info);
        cluster_info = NULL;
      }
      if (1 == ci_map_.count()) {
        default_cluster_name_ = cluster_info->cluster_name_;
      }
    }
  }
  return ret;
}

int ObProxyClusterArrayInfo::parse_ob_region(const Value *json_value,
                const ObProxyConfigString &root_service_url_template,
                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (root_service_url_template.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not for version v2, wrong resposne", K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_ARRAY))) {
    LOG_WARN("fail to check obregion array info", K(json_value), K(ret));
  } else {
    ObProxyClusterInfo *cluster_info = NULL;
    DLIST_FOREACH(it, json_value->get_array()) {
      cluster_info = NULL;
      cluster_info = op_alloc(ObProxyClusterInfo);
      if (OB_LIKELY(NULL != cluster_info)) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it, JT_STRING))) {
          LOG_WARN("fail to check config info type", K(it), K(ret));
        } else if (OB_FAIL(cluster_info->cluster_name_.parse(it))) {
          LOG_WARN("fail to parse region name", K(ret));
        } else if (OB_FAIL(generate_cluster_url(cluster_info->cluster_name_.get_string(),
                root_service_url_template.get_string(), cluster_info->rs_url_, allocator))) {
          LOG_WARN("generate cluster url failed", K(cluster_info->cluster_name_),
              K(root_service_url_template));
        } else {
          ret = ci_map_.unique_set(cluster_info);
          if (OB_SUCCESS == ret) {
          } else if (OB_HASH_EXIST == ret) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("cluster info already exist", K(ret));
          } else {}
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for cluster info", K(ret));
      }

      if (OB_FAIL(ret) && OB_LIKELY(NULL != cluster_info)) {
        op_free(cluster_info);
        cluster_info = NULL;
      }

      if (1 == ci_map_.count()) {
        default_cluster_name_ = cluster_info->cluster_name_;
      }
    }
  }

  return ret;
}

int ObProxyClusterArrayInfo::generate_cluster_url(const ObString &cluster_name,
                                     const ObString &root_service_url_template,
                                     ObProxyConfigUrl &url, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (cluster_name.empty() || root_service_url_template.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(cluster_name), K(root_service_url_template));
  } else {
    int64_t url_len = root_service_url_template.length() + cluster_name.length() + 1;
    char *buffer = NULL;
    int64_t pos = 0;
    if (OB_ISNULL(buffer = static_cast<char*>(allocator.alloc(url_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(url_len), K(ret));
    } else {
      int64_t index = root_service_url_template.find(REGION_TEMPLATE);
      if (-1 == index) {
        index = root_service_url_template.find(CLUSTER_TEMPLATE);
      }
      if (OB_UNLIKELY(-1 == index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not find ${ObRegion}", K(ret), K(root_service_url_template));
      } else {
        MEMCPY(buffer + pos, root_service_url_template.ptr(), index);
        pos += index;
        MEMCPY(buffer + pos, cluster_name.ptr(), cluster_name.length());
        pos += cluster_name.length();
        buffer[pos] = '\0';
        if (OB_FAIL(url.set_url(buffer, pos))) {
          LOG_WARN("set url failed", K(ret), K(buffer));
        }
      }
    }
    if (OB_FAIL(ret) && NULL != buffer) {
      allocator.free(buffer);
      buffer = NULL;
    }
  }

  return ret;
}

bool ObProxyClusterArrayInfo::is_cluster_exists(const ObString &name) const
{
  bool bret = false;
  ObProxyClusterInfo *cluster_info = NULL;
  if (name == OB_META_DB_CLUSTER_NAME) {
    bret = true;
  } else if (OB_SUCCESS == ci_map_.get_refactored(name, cluster_info)) {
    bret = true;
  }
  return bret;
}

bool ObProxyClusterArrayInfo::is_idc_list_exists(const ObString &name) const
{
  bool bret = false;
  ObProxyClusterInfo *cluster_info = NULL;
  if (name == OB_META_DB_CLUSTER_NAME) {
    bret = true;
  } else if (OB_SUCCESS == ci_map_.get_refactored(name, cluster_info)) {
    bret = true;
  }
  return bret;
}
int ObProxyClusterArrayInfo::get(const ObString &name, ObProxyClusterInfo *&cluster_info) const
{
  int ret = OB_SUCCESS;
  cluster_info = NULL;
  if (OB_SUCCESS != ci_map_.get_refactored(name, cluster_info)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("cluster not exist", K(name), K(ret));
  }
  return ret;
}

//------ ObProxyDataInfo------
DEF_TO_STRING(ObProxyDataInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(bin_url), K_(version), K_(meta_table_info), K_(cluster_array),
      K_(root_service_url_template), K_(root_service_url_template_v2));
  J_OBJ_END();
  return pos;
}

ObProxyDataInfo::ObProxyDataInfo()
  : version_(OB_PROXY_MAX_VERSION_LENGTH + 1),
    bin_url_(),
    root_service_url_template_(OB_PROXY_MAX_CONFIG_STRING_LENGTH),
    root_service_url_template_v2_(OB_PROXY_MAX_CONFIG_STRING_LENGTH)
{
  reset();
}

void ObProxyDataInfo::reset()
{
  bin_url_.reset();
  meta_table_info_.reset();
  version_.reset();
  cluster_array_.destroy();
  root_service_url_template_.reset();
  root_service_url_template_v2_.reset();
}

int ObProxyDataInfo::parse(const Value *json_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check Data info type", K(json_value), K(ret));
  } else {
    Value *region_v2 = NULL;
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == JSON_CONFIG_VERSION) {
        if (OB_FAIL(version_.parse(it->value_))) {
          LOG_WARN("fail to parse md5 version", K(it->name_), K(ret));
        }
      } else if (it->name_ == JSON_CLUSTER_LIST) {
        if (OB_FAIL(cluster_array_.parse(it->value_, allocator))) {
          LOG_WARN("fail to parse rs info list", K(it->name_), K(ret));
        }
      } else if (it->name_ == JSON_META_TABLE_INFO) {
        if (OB_FAIL(meta_table_info_.parse(it->value_, allocator))) {
          LOG_WARN("fail to parse metadb table info", K(it->name_), K(ret));
        }
      } else if (it->name_ == JSON_BIN_URL) {
        if (OB_FAIL(bin_url_.parse(it->value_, allocator))) {
          LOG_WARN("fail to parse bin info", K(it->name_), K(ret));
        }
      } else if (it->name_ == JSON_ROOT_SERVICE_INFO_URL_TEMPLATE) {
        if (OB_FAIL(root_service_url_template_.parse(it->value_))) {
          LOG_WARN("fail to parse rootserviceinfourltemplate", K(ret));
        }
      } else if (it->name_ == JSON_ROOT_SERVICE_INFO_URL_TEMPLATE_V2) {
        if (OB_FAIL(root_service_url_template_v2_.parse(it->value_))) {
          LOG_WARN("fail to parse rootserviceinfourltemplate v2", K(ret));
        }
      } else if (it->name_ == JSON_OB_REGION || it->name_ == JSON_DATA_CLUSTER_LIST) {
        region_v2 = it->value_;
      } else {
        // ignore other json key name, for later compatibile with new config items
      }
    }//end of DLIST_FOREACH

    if (OB_SUCC(ret) && NULL != region_v2) {
      if (OB_FAIL(cluster_array_.parse_ob_region(region_v2,
              root_service_url_template_, allocator))) {
        LOG_WARN("fail to parse obregion for v2", K(ret));
      }
    }
  }
  return ret;
}

int ObProxyDataInfo::parse_version(const Value *value, const ObString &version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(value, JT_OBJECT))) {
    LOG_WARN("fail to check json config info type", K(ret));
  } else {
    Value *data_info = NULL;
    DLIST_FOREACH(it, value->get_object()) {
      if (it->name_ == JSON_CONFIG_DATA) {
        data_info = it->value_;
        break;
      }
    }

    if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(data_info, JT_OBJECT))) {
      LOG_WARN("fail to check Data info type", K(ret));
    } else {
      DLIST_FOREACH(it, data_info->get_object()) {
        if (it->name_ == JSON_CONFIG_VERSION) {
          if (version == it->value_->get_string()) {
            ret = OB_EAGAIN;
            LOG_DEBUG("the same version, no need to parse any more", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

//------ ObProxyJsonConfigInfo------
DEF_TO_STRING(ObProxyJsonConfigInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(refcount), K(gmt_modified_), K_(data_info));
  J_OBJ_END();
  return pos;
}

ObProxyJsonConfigInfo::ObProxyJsonConfigInfo()
    : gmt_modified_(0)
{
  reset();
}

void ObProxyJsonConfigInfo::reset()
{
  data_info_.reset();
}

int ObProxyJsonConfigInfo::parse(const Value *json_value)
{
  int ret = OB_SUCCESS;
  Value *value = NULL;
  if (OB_FAIL(ObProxyJsonUtils::parse_header(json_value, value))) {
    LOG_WARN("fail to parse proxy json", K(json_value), K(ret));
  } else if (OB_FAIL(data_info_.parse(value, allocator_))) {
    LOG_WARN("fail to parse data info", K(value), K(ret));
  }
  return ret;
}

// local rslist json format:
//  [
//    {   "ObRegion" : "MetaDataBase",
//        "RsList" :
//        [ {
//            "address" : "ip:port",
//            "role" : "leader",
//            "sql_port" : 3306
//          }
//        ]
//    },
//    {   "ObRegion" : "obcluster1"
//        "RsList" :
//        [ {
//            "address" : "ip:port",
//            "role" : "leader",
//            "sql_port" : 3306
//          },
//          ......
//        ]
//    },
//    ......
//  ]

int ObProxyJsonConfigInfo::parse_local_rslist(const Value *root)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(root, JT_ARRAY))) {
    LOG_WARN("fail to check local rs list", K(ret));
  } else {
    bool is_from_local = true;
    ObString app_name = ObString::make_empty_string();

    int64_t json_cluster_id = OB_DEFAULT_CLUSTER_ID;
    LocationList web_rslist;
    bool is_primary = false;
    ObString cluster_name;

    DLIST_FOREACH(it, root->get_array()) {
      json_cluster_id = OB_DEFAULT_CLUSTER_ID;
      web_rslist.reuse();
      is_primary = false;
      cluster_name.reset();
      if (OB_FAIL(parse_rslist_data(it, app_name, json_cluster_id, web_rslist, is_primary, cluster_name, is_from_local))) {
        if (OB_EAGAIN == ret) {
          // here e_again means local rslist for this cluster is empty, do nothing and go on parsing
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to parse rslist data", K(ret));
        }
      }

      if (OB_SUCC(ret) && is_primary) {
        int64_t master_cluster_id = OB_DEFAULT_CLUSTER_ID;
        if (OB_FAIL(get_master_cluster_id(cluster_name, master_cluster_id))) {
          LOG_WARN("fail to get master cluster id",  K(cluster_name), K(ret));
        } else if (OB_DEFAULT_CLUSTER_ID == master_cluster_id) {
          if (OB_FAIL(set_master_cluster_id(cluster_name, json_cluster_id))) {
            LOG_WARN("fail to set master cluster id",  K(json_cluster_id), K(ret));
          } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(cluster_name, OB_DEFAULT_CLUSTER_ID, web_rslist, !is_from_local))) {
            LOG_WARN("fail to build and add dummy entry", K(cluster_name), K(web_rslist), K(ret));
          }
        }
      }
    }  //end traverse rs list array
  }
  return ret;
}

// primary-slave, no cluster id Case:
//  1. if only one primary
//    1.1 on building, update sys dummy
//      PS: if sys dummy exist, description cluster is rebuild after deletng, need update too
//    1.2 after building, if Timed Task failed count exceed threshold, need update sys dummy
//  2. if no primary, no need update sys dummy and not set primary
//    2.1 on building, random select one and update sys dummy
//    2.2 after building, if Timed Task failed count exceed threshold, will delete cluter and building on next
//  3. if multi-primary, no need update sys dummy and not set primary
//    3.1 on building, random select one and update sys dummy
//    3.2 after building, if Timed Task failed count exceed threshold, will delete cluter and building on next
int ObProxyJsonConfigInfo::parse_rslist_array_data(const Value *root, const ObString &appname,
                                                   const bool is_from_local/*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(root, JT_ARRAY))) {
    LOG_WARN("fail to check local rs list", K(ret));
  } else {
    int64_t json_cluster_id = OB_DEFAULT_CLUSTER_ID;
    LocationList web_rslist;
    bool is_primary = false;
    ObString cluster_name;

    int64_t primary_cluster_id = OB_DEFAULT_CLUSTER_ID;
    LocationList primary_web_rslist;
    bool is_multi_primary = false;
    ObString primary_cluster_name;

    DLIST_FOREACH(it, root->get_array()) {
      json_cluster_id = OB_DEFAULT_CLUSTER_ID;
      web_rslist.reuse();
      is_primary = false;
      cluster_name.reset();
      if (OB_FAIL(parse_rslist_data(it, appname, json_cluster_id, web_rslist, is_primary, cluster_name, is_from_local))) {
        if (OB_EAGAIN == ret) {
          // here e_again means local rslist for this cluster is empty, do nothing and go on parsing
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to parse rslist data", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (is_primary && OB_DEFAULT_CLUSTER_ID != primary_cluster_id) {
          is_multi_primary = true;
        } else if (is_primary) {
          primary_cluster_id = json_cluster_id;
          primary_web_rslist.assign(web_rslist);
          primary_cluster_name = cluster_name;
        }
      }
    }  //end traverse rs list array

    if (OB_SUCC(ret)) {
      if (!is_multi_primary && OB_DEFAULT_CLUSTER_ID != primary_cluster_id) {
        if (OB_FAIL(set_master_cluster_id(is_from_local ? primary_cluster_name : appname, primary_cluster_id))) {
          LOG_WARN("fail to set master cluster id",  K(json_cluster_id), K(ret));
        } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(
                   is_from_local ? primary_cluster_name : appname, OB_DEFAULT_CLUSTER_ID, primary_web_rslist, !is_from_local))) {
          LOG_WARN("fail to build and add dummy entry", K(cluster_name), K(web_rslist), K(ret));
        }
      }
    }
  }
  return ret;
}

// local idc list json format:
//  [
//    {   "ObRegion" : "MetaDataBase",
//        "IDCList":[
//            {
//                "idc":"test1",
//                "region":"default_region"
//            },
//            {
//                "idc":"test2",
//                "region":"default_region"
//            }
//        ]
//    },
//    {   "ObRegion" : "obcluster1"
//        "IDCList":[
//            {
//                "idc":"test1",
//                "region":"default_region"
//            },
//            {
//                "idc":"test2",
//                "region":"default_region"
//            }
//        ]
//    },
//    ......
//  ]

int ObProxyJsonConfigInfo::parse_local_idc_list(const Value *root)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(root, JT_ARRAY))) {
    LOG_WARN("fail to check local rs list", K(ret));
  } else {
    ObString tmp_cluster_name;
    ObProxyIDCList idc_list;
    int64_t cluster_id = OB_DEFAULT_CLUSTER_ID;
    DLIST_FOREACH(it, root->get_array()) {
      idc_list.reuse();
      if (OB_FAIL(parse_idc_list_data(it, tmp_cluster_name, cluster_id, idc_list))) {
        LOG_WARN("fail to parse idc list data", K(ret));
      } else if (OB_FAIL(set_idc_list(tmp_cluster_name, cluster_id, idc_list))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          LOG_DEBUG("idc_list is not changed, no need to update", K(tmp_cluster_name), K(ret));
        } else {
          LOG_WARN("fail to update_idc_list", K(tmp_cluster_name), K(cluster_id), K(idc_list), K(ret));
        }
      }
    }  //end traverse local idc list array
  }
  return ret;
}


// remote rslist json format
// {
//   Message:"successful",
//   Success:true,
//   Code:200,
//   Data:
//     {  "ObRegion" : "obcluster1",
//        "ObRegionId": 1,
//        "type": "leader",
//        "RsList" :
//        [ {
//            "address" : "ip:port",
//            "role" : "leader",
//            "sql_port" : 3306
//          },
//          ......
//        ]
//     }
// }

int ObProxyJsonConfigInfo::parse_remote_rslist(const Value *root, const ObString &appname, const int64_t cluster_id,
    LocationList &web_rslist, const bool need_update_dummy_entry /*true*/)
{
  int ret = OB_SUCCESS;
  Value *value = NULL;
  bool is_from_local = false;
  if (OB_FAIL(ObProxyJsonUtils::parse_header(root, value))) {
    LOG_WARN("fail to parse remote rslist header", K(root), K(ret));
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs info data is null", K(appname), K(ret));
    // primary-slave and no cluster id case
  } else if (value->get_type() == JT_ARRAY) {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    // parse all cluster rs list for the cluster name, including master and all followers
    if (OB_FAIL(parse_rslist_array_data(value, appname))) {
      LOG_WARN("fail to parse remote rslist array", K(appname), K(cluster_id), K(ret));
    } else if (OB_FAIL(reset_is_used_flag(appname))) {
      // need reset IsUse Flag after fetch newest rs list
      LOG_WARN("fail to reset is_used flag", K(appname), K(ret));
    } else if (OB_FAIL(get_sub_cluster_info(appname, cluster_id, sub_cluster_info))) {
      if (OB_DEFAULT_CLUSTER_ID == cluster_id) {
        LOG_INFO("no primary cluster id exist, will try random cluster", K(cluster_id), K(appname));
        // mayby have no primary on config server
        // if appname == OB_META_DB_CLUSTER_NAME
        //   random select one  cluster rs list, the fun ObProxy::get_meta_table_server will use
        // if appname != OB_META_DB_CLUSTER_NAME, no need set, leave it to caller
        if (appname == OB_META_DB_CLUSTER_NAME) {
          bool is_master_changed = false;
          if (OB_FAIL(get_next_master_cluster_info(appname, sub_cluster_info, is_master_changed))) {
            LOG_WARN("fail to get next masrer cluster info", K(appname), K(ret));
          }
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("fail to get sub cluster info", K(appname), K(cluster_id), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sub_cluster_info)) {
        if (OB_DEFAULT_CLUSTER_ID != cluster_id || appname == OB_META_DB_CLUSTER_NAME) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sub cluster info is null", K(appname), K(cluster_id), K(ret));
        }
      } else if (OB_FAIL(web_rslist.assign(sub_cluster_info->web_rs_list_))){
        LOG_WARN("fail to assign web rs list", KPC(sub_cluster_info), K(web_rslist), K(ret));
      }
    }
    // 1. not primary-slave and no cluster id case
    //   1.1 on building:
    //    1.1.1 sys dummy not exist, update sys dummy
    //    1.1.2 sys dummy exist, update sys dummy
    //   1.2 after building, if timed task failed exceed threshold, update sys dummy
    // 2. have cluster id case
    //  2.1 do not care whether primary or not, no need update sys dummy
  } else if (value->get_type() == JT_OBJECT) {
    int64_t json_cluster_id = cluster_id;
    bool is_primary = false;
    ObString cluster_name;
    if (OB_FAIL(parse_rslist_data(value, appname, json_cluster_id, web_rslist, is_primary,
                                  cluster_name, is_from_local, need_update_dummy_entry)) && OB_EAGAIN != ret) {
      LOG_WARN("fail to parse remote rslist data", K(ret));
    // If there is no cluster id, there must be no active/standby case.
    // It must be set, otherwise the cluster resource of the main library cannot be obtained when get_cluster_resource
    // If you bring the cluster id, because you want to access a specific library, the main library information is not updated
    } else if (OB_DEFAULT_CLUSTER_ID == cluster_id) {
      if (OB_FAIL(set_master_cluster_id(is_from_local ? cluster_name : appname, json_cluster_id))) {
        LOG_WARN("fail to set master cluster id",  K(json_cluster_id), K(ret));
      } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(
                      is_from_local ? cluster_name : appname, OB_DEFAULT_CLUSTER_ID, web_rslist, !is_from_local))) {
        LOG_WARN("fail to build and add dummy entry", K(cluster_name), K(cluster_id), K(web_rslist), K(ret));
      }
    }
  }
  return ret;
}

// if parse local rslist data, appname is empty string
// if parse remote rslist data, appname is cluster name for normal cluster
// and if it is metadb cluster, appname is hard coded 'MetaDataBase'
int ObProxyJsonConfigInfo::parse_rslist_data(const Value *json_value, const ObString &appname, int64_t &cluster_id,
    LocationList &web_rslist, bool &is_primary, ObString &cluster_name, bool is_from_local /*false*/,
    const bool need_update_dummy_entry /*true*/)
{
  int ret = OB_SUCCESS;
  Value *rslist = NULL;
  Value *readonly_rslist = NULL;
  ObString real_meta_cluster_name;
  ObString role_str;
  int64_t json_cluster_id = cluster_id;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check rslist data", K(json_value), K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == JSON_OB_REGION || it->name_ == JSON_OB_CLUSTER) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, JT_STRING))) {
          LOG_WARN("fail to check cluster name", K(ret));
        } else {
          cluster_name = it->value_->get_string();
        }
      } else if (it->name_ == JSON_OB_REGION_ID || it->name_ == JSON_OB_CLUSTER_ID) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, JT_NUMBER))) {
          LOG_WARN("fail to check cluster id", K(ret));
        } else {
          json_cluster_id = it->value_->get_number();
        }
      } else if (it->name_ == JSON_OB_CLUSTER_TYPE) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, JT_STRING))) {
          LOG_WARN("fail to check region role", K(cluster_name), K(ret));
        } else {
          role_str = it->value_->get_string();
        }
      } else if (is_from_local && it->name_ == JSON_REAL_META_REGION) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, JT_STRING))) {
          LOG_WARN("fail to check real meta cluster name", K(ret));
        } else {
          real_meta_cluster_name = it->value_->get_string();
        }
      } else if (it->name_ == JSON_RS_LIST) {
        rslist = it->value_;
      } else if (it->name_ == JSON_READONLY_RS_LIST) {
        readonly_rslist = it->value_;
      } else {
        // ignore other json key name, for later compatibile with new config items
      }
    } // end dlist_foreach
  }
  if (OB_SUCC(ret)) {
    if (is_from_local) {
      if (cluster_id == OB_DEFAULT_CLUSTER_ID) {
        cluster_id = json_cluster_id;
      }
      if (cluster_name == OB_META_DB_CLUSTER_NAME) {
        if (!real_meta_cluster_name.empty()) {
          data_info_.meta_table_info_.real_cluster_name_.set_value(real_meta_cluster_name);
        } else {
          //do nothing when parse local rs config
        }
      }
    } else {
      if (cluster_id == OB_DEFAULT_CLUSTER_ID) {
        // return real cluster id which used to update primary cluster info on cluster info
        cluster_id = json_cluster_id;
      }
      if(appname == OB_META_DB_CLUSTER_NAME) {
        data_info_.meta_table_info_.real_cluster_name_.set_value(cluster_name);
      } else if (OB_UNLIKELY(appname != cluster_name)) {
        ret = OB_OBCONFIG_APPNAME_MISMATCH;
        LOG_WARN("obconfig appname mismatch", K(cluster_name), K(appname), K(ret));
      } else if (OB_UNLIKELY(cluster_id != json_cluster_id)) {
        ret = OB_OBCONFIG_APPNAME_MISMATCH;
        LOG_WARN("obconfig cluster_id mismatch", K(cluster_id), K(json_cluster_id), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      web_rslist.reuse();
      if (OB_FAIL(parse_rslist_item(rslist, is_from_local ? cluster_name : appname, web_rslist, false))) {
        LOG_WARN("fail to parse rslist item", K(rslist), K(ret));
      } else if (NULL != readonly_rslist
          && OB_FAIL(parse_rslist_item(readonly_rslist, is_from_local ? cluster_name : appname, web_rslist, true))) {
        LOG_WARN("fail to parse readonly_rslist item", K(readonly_rslist), K(ret));
      } else if (OB_FAIL(reset_create_failure_count(is_from_local ? cluster_name : appname, json_cluster_id))) {
        LOG_WARN("fail to reset_create_failure_count", K(json_cluster_id), K(ret));
      } else if (web_rslist.empty()) {
        LOG_INFO("rslist is empty", K(web_rslist), K(json_cluster_id), K(cluster_name));
      } else if (OB_FAIL(set_cluster_web_rs_list(is_from_local ? cluster_name : appname, json_cluster_id, web_rslist,
                                                 web_rslist, role_str.empty() ? ObString::make_string(PRIMARY_ROLE) : role_str))) {
        if (OB_EAGAIN == ret) {
          LOG_DEBUG("web rslist is not changed, no need to update", K(role_str), K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to set cluster web rs list", K(web_rslist), K(role_str), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (role_str.empty() || role_str.case_compare(PRIMARY_ROLE) == 0) {
        is_primary = true;
      } else if (role_str.case_compare(STANDBY_ROLE) == 0) {
        is_primary = false;
        // if current primary cluster has been switched to standby,
        // reset master_cluster_id to OB_DEFAULT_CLUSTER_ID
        int64_t cur_master_cluster_id = OB_DEFAULT_CLUSTER_ID;
        if (OB_FAIL(get_master_cluster_id(is_from_local ? cluster_name : appname, cur_master_cluster_id))) {
          LOG_WARN("cur_master_cluster_id does not exist", K(cluster_name), K(appname), K(ret));
        } else if (json_cluster_id == cur_master_cluster_id
                   && OB_FAIL(set_master_cluster_id(is_from_local ? cluster_name : appname,
                                                    OB_DEFAULT_CLUSTER_ID))) {
          LOG_WARN("fail to reset master cluster id", K(cluster_name), K(appname), K(ret));
        }
      }
      if (OB_SUCC(ret) && need_update_dummy_entry) {
        const bool is_rslist = !is_from_local;
        if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(
            is_from_local ? cluster_name : appname, json_cluster_id, web_rslist, is_rslist))) {
          LOG_WARN("fail to build and add dummy entry", K(cluster_name), K(cluster_id), K(web_rslist), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::swap_origin_web_rslist_and_build_sys(const ObString &cluster_name, const int64_t cluster_id, const bool need_save_rslist_hash)
{
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(reset_create_failure_count(cluster_name, cluster_id))) {
    LOG_WARN("fail to reset_create_failure_count", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_FAIL(get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
    LOG_WARN("cluster not exist", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_info is NULL", K(cluster_name), K(cluster_id), K(ret));
  } else {
    LOG_INFO("will update rslist", K(cluster_name), K(cluster_id),
             "real cluster_id", sub_cluster_info->cluster_id_,
             "old rslist", sub_cluster_info->web_rs_list_,
             "new rslist", sub_cluster_info->origin_web_rs_list_);
    if (OB_FAIL(sub_cluster_info->web_rs_list_.assign(sub_cluster_info->origin_web_rs_list_))) {
      LOG_WARN("fail to set cluster web_rs_list", K(cluster_name), K(cluster_id),
               "origin_web_rs_list", sub_cluster_info->origin_web_rs_list_, K(ret));
    } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(cluster_name, cluster_id, sub_cluster_info->origin_web_rs_list_, true))) {
      LOG_WARN("fail to build and add dummy entry", K(cluster_name), K(cluster_id),
               "origin_web_rs_list", sub_cluster_info->origin_web_rs_list_, K(ret));
    } else if (need_save_rslist_hash) {
      sub_cluster_info->rs_list_hash_ = ObProxyClusterInfo::get_server_list_hash(sub_cluster_info->origin_web_rs_list_);
    }
  }

  return ret;
}

//{
//    "Message":"successful",
//    "Success":true,
//    "Code":200,
//    "Data":{
//        "ObRegion":"ob1.xxx.xxx",
//        "ObRegionId":1,
//        "type": "leader",
//        "IDCList":[
//            {
//                "idc":"test1",
//                "region":"default_region"
//            },
//            {
//                "idc":"test2",
//                "region":"default_region"
//            }
//        ]
//    }
//}
int ObProxyJsonConfigInfo::parse_remote_idc_list(const Value *root,
    ObString &cluster_name, int64_t &cluster_id, ObProxyIDCList &idc_list)
{
  int ret = OB_SUCCESS;
  Value *value = NULL;
  if (OB_FAIL(ObProxyJsonUtils::parse_header(root, value))) {
    LOG_WARN("fail to parse remote idc list header", K(root), K(ret));
  } else if (OB_FAIL(parse_idc_list_data(value, cluster_name, cluster_id, idc_list))) {
    LOG_WARN("fail to parse remote idc list data", K(ret));
  }
  return ret;
}

int ObProxyJsonConfigInfo::parse_idc_list_data(const Value *json_value,
    ObString &cluster_name, int64_t &cluster_id, ObProxyIDCList &idc_list)
{
  int ret = OB_SUCCESS;
  Value *json_idc_list = NULL;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check idc list data", K(json_value), K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == JSON_OB_REGION || it->name_ == JSON_OB_CLUSTER) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, JT_STRING))) {
          LOG_WARN("fail to check cluster name", K(ret));
        } else {
          cluster_name = it->value_->get_string();
        }
      } else if (it->name_ == JSON_OB_REGION_ID || it->name_ == JSON_OB_CLUSTER_ID) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, JT_NUMBER))) {
          LOG_WARN("fail to check cluster id", K(ret));
        } else {
          cluster_id = it->value_->get_number();
        }
      } else if (it->name_ == JSON_IDC_LIST) {
        json_idc_list = it->value_;
      } else {
        // ignore other json key name, for later compatibile with new config items
      }
    } // end dlist_foreach
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_idc_list_item(json_idc_list, idc_list))) {
      LOG_WARN("fail to parse idc list item", K(ret));
    } else {
      LOG_DEBUG("succ to parse idc list item", K(cluster_name), K(idc_list), K(ret));
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::parse_idc_list_item(const Value *json_value, ObProxyIDCList &idc_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_ARRAY))) {
    LOG_WARN("fail to check cluster array info", K(json_value), K(ret));
  } else {
    idc_list.reset();
    ObProxyIDCInfo idc_info;
    DLIST_FOREACH(it, json_value->get_array()) {
      if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it, JT_OBJECT))) {
        LOG_WARN("fail to check idc list", K(ret));
      } else {
        idc_info.reset();
        DLIST_FOREACH(p, it->get_object()) {
          if (p->name_ == JSON_REGION) {
            if (OB_FAIL(idc_info.region_name_.parse(p->value_))) {
              LOG_WARN("fail to parse region name", K(p->value_), K(ret));
            }
          } else if (p->name_ == JSON_IDC) {
            if (OB_FAIL(idc_info.idc_name_.parse(p->value_))) {
              LOG_WARN("fail to parse idc name", K(p->value_), K(ret));
            } else {
              idc_info.idc_name_.to_lower_case();
              idc_info.idc_hash_ = idc_info.idc_name_.hash();
            }
          } else {
            // ignore other json key name, for later compatibile with new config items
          }
        }
        if (OB_FAIL(ret) || OB_UNLIKELY(!idc_info.is_valid())) {
          LOG_WARN("ignore invalid info, continue", K(idc_info), K(ret));
          ret = OB_SUCCESS;
        } else if (OB_FAIL(idc_list.push_back(idc_info))) {
          LOG_WARN("fail to push back idc_info", K(idc_info), K(ret));
        } else {
          LOG_DEBUG("succ to push back idc_info", K(idc_info), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::parse_rslist_item(const Value *rs_list, const ObString &cluster_name,
    LocationList &web_rslist, const bool is_readonly_zone)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(rs_list, JT_ARRAY))) {
    LOG_WARN("fail to check rs list item", K(cluster_name), K(ret));
  } else {
    int64_t sql_port = OB_INVALID_INDEX;
    ObString address_str;
    ObString role_str;
    const ObReplicaType replica_type = (is_readonly_zone ? REPLICA_TYPE_READONLY : REPLICA_TYPE_FULL);
    DLIST_FOREACH(it, rs_list->get_array()) {
      if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it, JT_OBJECT))) {
        LOG_WARN("fail to check rs list", K(cluster_name), K(ret));
      } else {
        DLIST_FOREACH(p, it->get_object()) {
          if (p->name_ == JSON_ADDRESS) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, JT_STRING))) {
              LOG_WARN("fail to check address", K(cluster_name), K(ret));
            } else {
              address_str = p->value_->get_string();
            }
          } else if (p->name_ == JSON_ROLE) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, JT_STRING))) {
              LOG_WARN("fail to check rs role", K(cluster_name), K(ret));
            } else {
              role_str = p->value_->get_string();
            }
          } else if (p->name_ == JSON_SQL_PORT) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, JT_NUMBER))) {
              LOG_WARN("fail to check port", K(cluster_name), K(ret));
            } else {
              sql_port = p->value_->get_number();
            }
          } else {
            // ignore other json key name, for later compatibile with new config items
          }
        } // end traverse one root addr item
        if (OB_SUCC(ret) && OB_FAIL(add_to_list(address_str, role_str, cluster_name, sql_port, replica_type, web_rslist))) {
          LOG_WARN("fail to add rs address into web rslist", K(cluster_name), K(ret));
        }
      }
    } // end traverse root addr array
  }

  return ret;
}

int ObProxyJsonConfigInfo::add_to_list(const ObString &ip, const ObString &role,
                                       const ObString &cluster_name, const int64_t sql_port,
                                       const common::ObReplicaType replica_type, LocationList &web_rslist)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_PORT_LENGTH];
  buf[0] = '\0';
  ObProxyReplicaLocation replica;
  int64_t w_len = snprintf(buf, MAX_IP_PORT_LENGTH, "%.*s", ip.length(), ip.ptr());
  if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len >= MAX_IP_PORT_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill ip_port buf error", K(w_len), K(ip), K(cluster_name), K(ret));
  } else if (OB_FAIL(replica.server_.parse_from_cstring(buf))) {
    LOG_WARN("fail to parse addr from ip string", K(buf), K(cluster_name), K(ret));
    ret = OB_SUCCESS; // ignore ret
  } else {
    replica.server_.set_port(static_cast<int32_t>(sql_port));
    replica.replica_type_ = replica_type;
    if (role.case_compare(LEADER_ROLE) == 0) {
      replica.role_ = LEADER;
    } else if (role.case_compare(FOLLOWER_ROLE) == 0) {
      replica.role_ = FOLLOWER;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected role", K(role), K(ip), K(cluster_name), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!replica.is_valid())) {
        //if addr is invalid, just print warn log and ignore it
        LOG_WARN("addr is invalid", K(replica), K(cluster_name));
      } else if (OB_FAIL(web_rslist.push_back(replica))) {
        LOG_WARN("fail to add addr to web rslist", K(replica), K(cluster_name), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::get_rslist_file_max_size(int64_t &max_size)
{
  int ret = OB_SUCCESS;
  max_size = 0;
  int64_t cluster_name_size = 0;
  int64_t cluster_count = 0;
  int64_t rslist_count = 0;
  //1. meta_cluster_info_'s  web_rs_list_ count
  cluster_name_size += 90;//MetaDataBase,"ObRealMetaRegion","max_length"
  ++cluster_count;
  rslist_count = data_info_.meta_table_info_.cluster_info_.get_rs_list_count();

  //2. others's  web_rs_list_ count
  for (ObProxyClusterArrayInfo::CIHashMap::iterator it = data_info_.cluster_array_.ci_map_.begin();
       OB_SUCC(ret) && it != data_info_.cluster_array_.ci_map_.end(); ++it) {
    if (it->get_rs_list_count() > 0) {
      cluster_name_size += (it->cluster_name_.length() * it->sub_ci_map_.count());
      cluster_count += it->sub_ci_map_.count();
      rslist_count += it->get_rs_list_count();
    }
  }

  if (OB_SUCC(ret)) {
    //{"address":"100.100.100.100:65535","role":"FOLLOWER","sql_port":65535},
    const int64_t location_size = 80;
    //{"ObRegion":"","ObRegionId":int64,"type":"FOLLOWER","RsList":[]}]
    const int64_t cluster_header_size = 85;
    max_size = location_size * rslist_count + cluster_header_size * cluster_count + cluster_name_size;
    max_size += OB_RECORD_HEADER_LENGTH;
  }
  return ret;
}

//{"ObRegion":"MetaDataBase","ObRegionId":int64,"ObRealMetaRegion","ob1.xxx.xxx","RsList":[]}]
//{"ObRegion":"ob1.xxx.xxx","ObRegionId":int64,"IDCList":[]}
//{"idc":"test1","region":"default_region"},
//{"idc":"test2","region":"default_region"}
int ObProxyJsonConfigInfo::get_idc_list_file_max_size(int64_t &max_size)
{
  int ret = OB_SUCCESS;
  max_size = 0;
  int64_t cluster_name_size = 0;
  int64_t cluster_count = 0;
  int64_t idc_list_count = 0;
  //1. meta_cluster_info_'s  web_rs_list_ count
  cluster_name_size += 90;//MetaDataBase,"ObRealMetaRegion","max_length"
  ++cluster_count;
  idc_list_count = data_info_.meta_table_info_.cluster_info_.get_idc_list_count();

  //2. others's  web_rs_list_ count
  for (ObProxyClusterArrayInfo::CIHashMap::iterator it = data_info_.cluster_array_.ci_map_.begin();
       OB_SUCC(ret) && it != data_info_.cluster_array_.ci_map_.end(); ++it) {
    if (it->get_idc_list_count() > 0) {
      cluster_name_size += (it->cluster_name_.length() + it->sub_ci_map_.count());
      cluster_count += it->sub_ci_map_.count();
      idc_list_count += it->get_idc_list_count();
    }
  }

  if (OB_SUCC(ret)) {
    //{"idc":"test2","region":"default_region"}
    const int64_t idc_info_size = 256 + 22;
    //{"ObRegion":"","ObRegionId":int64,"RsList":[]}]
    const int64_t cluster_header_size = 67;
    max_size = idc_info_size * idc_list_count + cluster_header_size * cluster_count + cluster_name_size;
    max_size += OB_RECORD_HEADER_LENGTH;
  }
  return ret;
}

int ObProxyJsonConfigInfo::rslist_to_json(char *buf, const int64_t buf_len, int64_t &data_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  data_len = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
     ret = OB_INVALID_ARGUMENT;
     LOG_WARN("invalid rs list buffer", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "["))) {
    LOG_WARN("fail to append string", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::cluster_rslist_to_json(data_info_.meta_table_info_.cluster_info_,
                                                              buf, buf_len, pos,
                                                              data_info_.meta_table_info_.real_cluster_name_.ptr()))) {
    LOG_WARN("fail to convert meta db rs list to json", K(buf_len), K(pos),  K(ret));
  } else {
    for (ObProxyClusterArrayInfo::CIHashMap::iterator it = data_info_.cluster_array_.ci_map_.begin();
         OB_SUCC(ret) && it != data_info_.cluster_array_.ci_map_.end(); ++it) {
      if (OB_FAIL(ObProxyJsonUtils::cluster_rslist_to_json(*it, buf, buf_len, pos))) {
        LOG_WARN("fail to convert rs list to json", K(*it), K(buf_len), K(pos), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // cover the last ',' with ]
    if (OB_LIKELY(buf[pos - 1] == ',')) {
      --pos;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "]"))) {
        LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
      }
    }
    data_len = pos;
  }
  return ret;
}

int ObProxyJsonConfigInfo::idc_list_to_json(char *buf, const int64_t buf_len, int64_t &data_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  data_len = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
     ret = OB_INVALID_ARGUMENT;
     LOG_WARN("invalid idc list buffer", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "["))) {
    LOG_WARN("fail to append string", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::cluster_idc_list_to_json(data_info_.meta_table_info_.cluster_info_,
                                                                buf, buf_len, pos,
                                                                data_info_.meta_table_info_.real_cluster_name_.ptr()))) {
    LOG_WARN("fail to convert meta db idc list to json", K(buf_len), K(pos),  K(ret));
  } else {
    for (ObProxyClusterArrayInfo::CIHashMap::iterator it = data_info_.cluster_array_.ci_map_.begin();
         OB_SUCC(ret) && it != data_info_.cluster_array_.ci_map_.end(); ++it) {
      if (OB_FAIL(ObProxyJsonUtils::cluster_idc_list_to_json(*it, buf, buf_len, pos))) {
        LOG_WARN("fail to convert idc list to json", K(*it), K(buf_len), K(pos), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // cover the last ',' with ]
    if (OB_LIKELY(buf[pos - 1] == ',')) {
      --pos;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "]"))) {
        LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
      }
    }
    data_len = pos;
  }
  return ret;
}

int ObProxyJsonConfigInfo::get_master_cluster_id(const ObString &cluster_name, int64_t &cluster_id) const
{
  int ret = OB_SUCCESS;
  cluster_id = OB_DEFAULT_CLUSTER_ID;
  if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    cluster_id = data_info_.meta_table_info_.cluster_info_.master_cluster_id_;
  } else {
    ObProxyClusterInfo *cluster_info = NULL;
    if (OB_FAIL(data_info_.cluster_array_.get(cluster_name, cluster_info))) {
      LOG_WARN("cluster not exist", K(cluster_name), K(ret));
    } else {
      cluster_id = cluster_info->master_cluster_id_;
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::get_next_master_cluster_info(
      const ObString &cluster_name,
      ObProxySubClusterInfo *&sub_cluster_info,
      bool &is_master_changed)
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo *cluster_info = NULL;
  sub_cluster_info = NULL;
  if (OB_FAIL(get_cluster_info(cluster_name, cluster_info))) {
    LOG_WARN("fail to get cluster info", K(cluster_name), K(ret));
  } else if (OB_ISNULL(cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster info is null", K(cluster_name), K(ret));
  } else {
    bool found = false;
    ObProxyClusterInfo::SubCIHashMap &map = const_cast<ObProxyClusterInfo::SubCIHashMap &>(cluster_info->sub_ci_map_);
    for (ObProxyClusterInfo::SubCIHashMap::iterator it = map.begin(); !found && it != map.end(); ++it) {
      if (it->is_used_ || it->web_rs_list_.empty()) {
        // do nothing
      } else {
        found = true;
        it->is_used_ = true;
        sub_cluster_info = &(*it);
      }
    }
    if (!found || NULL == sub_cluster_info) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("no avaiable sub cluster info", KPC(cluster_info), K(ret));
    } else {
      is_master_changed = cluster_info->master_cluster_id_ == sub_cluster_info->cluster_id_;
      cluster_info->master_cluster_id_ = sub_cluster_info->cluster_id_;
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::reset_is_used_flag(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo *cluster_info = NULL;
  if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    cluster_info = &data_info_.meta_table_info_.cluster_info_;
  } else if (OB_FAIL(data_info_.cluster_array_.get(cluster_name, cluster_info))) {
    LOG_WARN("cluster not exist, ignore", K(cluster_name), K(ret));
  } else if (OB_ISNULL(cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster info is null", K(cluster_name), K(ret));
  }
  if (OB_SUCC(ret)) {
    ObProxyClusterInfo::SubCIHashMap &map = cluster_info->sub_ci_map_;
    for (ObProxyClusterInfo::SubCIHashMap::iterator it = map.begin(); it != map.end(); ++it) {
      it->is_used_ = false;
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::get_rs_list_hash(const ObString &cluster_name,
    const int64_t cluster_id,
    uint64_t &rs_list_hash) const
{
  int ret = OB_SUCCESS;
  rs_list_hash = 0;
  ObProxyClusterInfo *cluster_info = NULL;
  if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    cluster_info = const_cast<ObProxyClusterInfo *>(&data_info_.meta_table_info_.cluster_info_);
  } else if (OB_FAIL(data_info_.cluster_array_.get(cluster_name, cluster_info))) {
    LOG_WARN("cluster not exist, ignore", K(cluster_name), K(ret));
  } else if (OB_ISNULL(cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster info is null", K(cluster_name), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cluster_info->get_rs_list_hash(cluster_id, rs_list_hash))) {
      LOG_WARN("fail to get rs list hash", K(cluster_name), K(cluster_id), K(ret));
    }
  }
  return ret;
}

bool ObProxyJsonConfigInfo::is_cluster_idc_list_exists(const common::ObString &cluster_name, const int64_t cluster_id) const
{
  bool ret_bool = false;
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
    LOG_WARN("cluster not exist", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_info is NULL", K(cluster_name), K(cluster_id), K(ret));
  } else {
    ret_bool = !sub_cluster_info->idc_list_.empty();
  }
  return ret_bool;
}

int ObProxyJsonConfigInfo::get_cluster_info(const ObString &cluster_name, ObProxyClusterInfo *&cluster_info) const
{
  int ret = OB_SUCCESS;
  cluster_info = NULL;
  if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    cluster_info = const_cast<ObProxyClusterInfo *>(&data_info_.meta_table_info_.cluster_info_);
  } else if (OB_FAIL(data_info_.cluster_array_.get(cluster_name, cluster_info))) {
    LOG_INFO("cluster not exist", K(cluster_name), K(ret));
  }
  return ret;
}

int ObProxyJsonConfigInfo::get_sub_cluster_info(const ObString &cluster_name, const int64_t cluster_id,
                                                ObProxySubClusterInfo *&sub_cluster_info) const
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo *cluster_info = NULL;
  sub_cluster_info = NULL;
  if (OB_FAIL(get_cluster_info(cluster_name, cluster_info))) {
    LOG_WARN("fail to get cluster info", K(cluster_name), K(ret));
  } else if (OB_ISNULL(cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster info is null", K(cluster_name), K(ret));
  } else {
    ret = cluster_info->get_sub_cluster_info(cluster_id, sub_cluster_info);
  }
  return ret;
}

int64_t ObProxyJsonConfigInfo::inc_create_failure_count(const common::ObString &cluster_name, int64_t cluster_id)
{
  int64_t ret_count = OB_INVALID_COUNT;
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
    LOG_WARN("cluster not exist", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_info is NULL", K(cluster_name), K(cluster_id), K(ret));
  } else {
    ret_count = ++(sub_cluster_info->create_failure_count_);
  }
  return ret_count;
}

int ObProxyJsonConfigInfo::get_default_cluster_name(char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  const ObString &name = data_info_.cluster_array_.default_cluster_name_;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cluster name buf", K(buf), K(ret));
  } else if (OB_UNLIKELY(len <= name.length())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf len is not enough", "default cluster name size",
             name.length(), K(len));
  } else {
    MEMCPY(buf, name.ptr(), name.length());
    buf[name.length()] = '\0';
  }
  return ret;
}

int ObProxyJsonConfigInfo::copy_bin_url(char *bin_url, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bin_url) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer", K(bin_url), K(len), K(ret));
  } else if (OB_UNLIKELY(len <= data_info_.bin_url_.length())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("bin url buffer is not enough", K(data_info_.bin_url_), K(len), K(ret));
  } else {
    MEMCPY(bin_url, data_info_.bin_url_.ptr(), data_info_.bin_url_.length());
    bin_url[data_info_.bin_url_.length()] = '\0';
  }
  return ret;
}

int ObProxyJsonConfigInfo::set_cluster_web_rs_list(const ObString &cluster_name, const int64_t cluster_id,
    const LocationList &web_rs_list, const LocationList &origin_web_rs_list, const ObString &role, const uint64_t cur_rs_list_hash/*0*/)
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo *cluster_info = NULL;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  bool new_sub_cluster_info = false;
  if (web_rs_list.empty() && origin_web_rs_list.empty()) {
    LOG_INFO("rslist is empty", K(web_rs_list), K(origin_web_rs_list), K(cluster_name));
  } else {
    if (cluster_name == OB_META_DB_CLUSTER_NAME) {
      cluster_info = const_cast<ObProxyClusterInfo *>(&data_info_.meta_table_info_.cluster_info_);
    } else if (OB_FAIL(data_info_.cluster_array_.get(cluster_name, cluster_info))) {
      LOG_DEBUG("cluster not exist", K(cluster_name), K(ret));
    } else if (OB_ISNULL(cluster_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cluster info is null", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cluster_info->get_sub_cluster_info(cluster_id, sub_cluster_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // add new sub cluster info
          if (OB_ISNULL(sub_cluster_info = op_alloc(ObProxySubClusterInfo))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory for sub cluster info", K(ret), K(cluster_id), K(cluster_name));
          } else {
            new_sub_cluster_info = true;
            ret = OB_SUCCESS;
            sub_cluster_info->cluster_id_ = cluster_id;
          }
        } else {
          LOG_WARN("fail to get sub_cluster_info", K(cluster_name), K(cluster_id), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(sub_cluster_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub cluster info is null", K(ret));
      } else if (!origin_web_rs_list.empty() && OB_FAIL(sub_cluster_info->origin_web_rs_list_.assign(origin_web_rs_list))) {
        LOG_WARN("fail to set cluster origin_web_rs_list", K(cluster_name), K(cluster_id), K(origin_web_rs_list), K(ret));
      } else if (!web_rs_list.empty()) {
        if (!sub_cluster_info->is_web_rs_list_changed(web_rs_list)
                   && !sub_cluster_info->is_cluster_role_changed(role)) {
          ret = OB_EAGAIN;
        } else {
          LOG_INFO("will update rslist",
                   "old rslist", sub_cluster_info->web_rs_list_,
                   "new rslist", web_rs_list,
                   "old cluster role", cluster_role_to_str(sub_cluster_info->role_),
                   K(role), K(cluster_name), K(cluster_id));
          if (OB_FAIL(sub_cluster_info->web_rs_list_.assign(web_rs_list))) {
            LOG_WARN("fail to set cluster web_rs_list", K(cluster_name), K(cluster_id), K(web_rs_list), K(ret));
          } else {
            if (0 == cur_rs_list_hash) {
              sub_cluster_info->rs_list_hash_ = ObProxyClusterInfo::get_server_list_hash(web_rs_list);
            } else {
              sub_cluster_info->rs_list_hash_ =  cur_rs_list_hash;
            }
            if (role.case_compare(PRIMARY_ROLE) == 0) {
              sub_cluster_info->role_ = PRIMARY;
            } else if (role.case_compare(STANDBY_ROLE) == 0) {
              sub_cluster_info->role_ = STANDBY;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected role", K(role), K(cluster_name), K(cluster_id), K(ret));
            }
          }
        }
      }
    }
    if (new_sub_cluster_info && OB_LIKELY(NULL != sub_cluster_info)) {
      if (OB_SUCC(ret)) {
        ret = cluster_info->sub_ci_map_.unique_set(sub_cluster_info);
        if (OB_HASH_EXIST == ret) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("cluster info already exist", K(cluster_name), K(cluster_id), K(ret));
        } else {
          LOG_INFO("succ to add sub cluster info", K(cluster_name), K(cluster_id));
        }
      }
      if (OB_FAIL(ret) && OB_EAGAIN != ret) {
        op_free(sub_cluster_info);
        sub_cluster_info = NULL;
      }
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::set_master_cluster_id(const ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo *cluster_info = NULL;
  if (OB_UNLIKELY(cluster_id < 0)) {
    LOG_DEBUG("master cluster id is invalid, no need set", K(cluster_name), K(cluster_id));
  } else if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    cluster_info = const_cast<ObProxyClusterInfo *>(&data_info_.meta_table_info_.cluster_info_);
  } else if (OB_FAIL(data_info_.cluster_array_.get(cluster_name, cluster_info))) {
    LOG_DEBUG("cluster not exist", K(cluster_name), K(ret));
  }
  if (OB_SUCC(ret) && NULL != cluster_info) {
    if (cluster_id != cluster_info->master_cluster_id_) {
      LOG_INFO("master cluster id will be updated", "last_master_cluster_id", cluster_info->master_cluster_id_,
               "new_master_cluster_id", cluster_id, K(cluster_name));
      cluster_info->master_cluster_id_ = cluster_id;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to set master cluster id", K(cluster_name), K(cluster_id));
  }
  return ret;
}

int ObProxyJsonConfigInfo::reset_create_failure_count(const common::ObString &cluster_name, int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("cluster not exist", K(cluster_name), K(cluster_id), K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_info is NULL", K(cluster_name), K(cluster_id), K(ret));
  } else {
    sub_cluster_info->create_failure_count_ = 0;
  }
  return ret;
}

int ObProxyJsonConfigInfo::set_real_meta_cluster_name(const common::ObString &real_meta_cluster_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(real_meta_cluster_name.empty())) {
    LOG_DEBUG("real meta cluster name empty, no need set");
    data_info_.meta_table_info_.real_cluster_name_.reset();
  } else {
    data_info_.meta_table_info_.real_cluster_name_.set_value(real_meta_cluster_name);
  }
  return ret;
}

int ObProxyJsonConfigInfo::set_idc_list(const common::ObString &cluster_name, const int64_t cluster_id,
    const ObProxyIDCList &idc_list)
{
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
    LOG_WARN("cluster not exist", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_info is NULL", K(cluster_name), K(cluster_id), K(ret));
  } else {
    if (!sub_cluster_info->is_idc_list_changed(idc_list)) {
      ret = OB_EAGAIN;
    } else {
      LOG_INFO("succ to update idc list",
               "old idc list", sub_cluster_info->idc_list_,
               "new idc list", idc_list, K(cluster_name), K(cluster_id));
      if (OB_FAIL(sub_cluster_info->idc_list_.assign(idc_list))) {
        LOG_WARN("fail to set cluster idc_list", K(cluster_name), K(cluster_id), K(idc_list), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyJsonConfigInfo::delete_cluster_rslist(const common::ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
    LOG_WARN("cluster not exist", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster_info is NULL", K(cluster_name), K(cluster_id), K(ret));
  } else {
    LOG_INFO("this cluster rslist will be deleted", K(cluster_name), KPC(sub_cluster_info));
    sub_cluster_info->reuse_rslist();
  }
  return ret;
}

int ObProxyJsonConfigInfo::add_default_cluster_info(ObProxyClusterInfo *cluster_info, const LocationList &web_rs_list)
{
  int ret = OB_SUCCESS;
  int64_t default_cluster_id = OB_DEFAULT_CLUSTER_ID;
  bool is_rslist = false;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_ISNULL(cluster_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null cluster info", K(ret));
  } else if (OB_ISNULL(sub_cluster_info = op_alloc(ObProxySubClusterInfo))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem for sub_cluster_info", K(ret));
  } else {
    cluster_info->master_cluster_id_ = default_cluster_id;
    sub_cluster_info->cluster_id_ = default_cluster_id;
    sub_cluster_info->role_ = PRIMARY;
    if (OB_FAIL(sub_cluster_info->origin_web_rs_list_.assign(web_rs_list))) {
      LOG_WARN("fail to set default cluster origin_web_rs_list", K(cluster_info), K(web_rs_list), K(ret));
    } else if (OB_FAIL(sub_cluster_info->web_rs_list_.assign(web_rs_list))) {
      LOG_WARN("fail to set default cluster web_rs_list", K(cluster_info), K(web_rs_list), K(ret));
    } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(
            cluster_info->cluster_name_, default_cluster_id, web_rs_list, is_rslist))) {
      LOG_WARN("fail to update user specified cluster dummy table entry", K(ret));
    } else if (OB_FAIL(data_info_.cluster_array_.ci_map_.unique_set(cluster_info))) {
      LOG_WARN("fail to add user specified cluster info", K(cluster_info), K(ret));
    } else if (OB_FAIL(cluster_info->sub_ci_map_.unique_set(sub_cluster_info))) {
      LOG_WARN("fail to add user specified sub cluster info", K(sub_cluster_info), K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != sub_cluster_info)) {
    op_free(sub_cluster_info);
    sub_cluster_info = NULL;
  }
  return ret;
}

//------ ObProxyJsonUtils------
int ObProxyJsonUtils::check_config_info_type(const Value *json_value, const Type type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_value) || OB_UNLIKELY(type != json_value->get_type())) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid json value", K(json_value), K(type), K(ret));
  }
  return ret;
}

int ObProxyJsonUtils::check_config_string(const ObString &value, int64_t size_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("empty value", K(value), K(ret));
  } else if (OB_UNLIKELY(value.length() >= size_limit)) {
    //config_string_ ends with '\0', so value length must be < size_limit
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("value is over size, buffer is not enough", K(value), K(size_limit), K(ret));
  } else { }
  return ret;
}
int ObProxyJsonUtils::parse_header(const Value *json_value, Value *&value)
{
  int ret = OB_SUCCESS;
  int64_t http_code = -1;
  Type http_status = JT_FALSE;
  ObString http_message;
  value = NULL;
  Value *tmp_value = NULL;
  if (OB_FAIL(check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check json info type", K(value), K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == JSON_HTTP_MESSAGE) {
        if (OB_FAIL(check_config_info_type(it->value_, JT_STRING))) {
          LOG_WARN("fail to check http message", K(it->name_), K(ret));
        } else {
          http_message = it->value_->get_string();
        }
      } else if (it->name_ == JSON_HTTP_CODE) {
        if (OB_FAIL(check_config_info_type(it->value_, JT_NUMBER))) {
          LOG_WARN("fail to check http code", K(it->name_), K(ret));
        } else {
          http_code = it->value_->get_number();
        }
      } else if (it->name_ == JSON_HTTP_STATUS) {
        if (OB_FAIL(check_config_info_type(it->value_, JT_TRUE))) {
          LOG_WARN("fail to check http status", K(it->name_), K(ret));
        } else {
          http_status = it->value_->get_type();
        }
      } else if (it->name_ == JSON_CONFIG_DATA) {
        tmp_value = it->value_;
      } else {
        // ignore other json key name, for later compatibile with new config items
      }
    } // end dlist_foreach
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(http_message != HTTP_SUCC_MESSAGE)
        || OB_UNLIKELY(JT_TRUE != http_status) || OB_UNLIKELY(2 != http_code/100)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid json config http header", K(http_message), K(http_status), K(http_code), K(ret));
    } else {
      value = tmp_value;
    }
  }
  return ret;
}

int ObProxyJsonUtils::cluster_rslist_to_json(const ObProxyClusterInfo &cluster_info,
                                             char *buf, const int64_t buf_len, int64_t &pos,
                                             const char *real_meta_cluster/*NULL*/)
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo::SubCIHashMap &map = const_cast<ObProxyClusterInfo::SubCIHashMap &>(cluster_info.sub_ci_map_);
  for (ObProxyClusterInfo::SubCIHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
    if (it->web_rs_list_.empty()) {
      // do nothing
    } else if (OB_FAIL(rslist_to_json(it->web_rs_list_, cluster_info.cluster_name_.ptr(),
                               it->cluster_id_, cluster_info.master_cluster_id_ == it->cluster_id_ ? PRIMARY : STANDBY, buf, buf_len, pos, real_meta_cluster))) {
      LOG_WARN("fail to dump sub cluster rslist", K_(cluster_info.cluster_name), K(*it), K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ","))) {
      LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

int ObProxyJsonUtils::rslist_to_json(const LocationList &addr_list, const char *appname,
    const int64_t cluster_id, const ObClusterRole role, char *buf, const int64_t buf_len, int64_t &pos,
    const char *real_meta_cluster/*NULL*/)
{
  int ret = OB_SUCCESS;
  if (NULL != real_meta_cluster) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{\"%s\":\"%s\",\"%s\":%ld,\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":[",
        JSON_OB_REGION, appname, JSON_OB_REGION_ID, cluster_id, JSON_OB_CLUSTER_TYPE, PRIMARY == role ? "PRIMARY" : "STANDBY",
        JSON_REAL_META_REGION, real_meta_cluster, JSON_RS_LIST))) {
      LOG_WARN("assign string failed", K(buf_len), K(pos), K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{\"%s\":\"%s\",\"%s\":%ld,\"%s\":\"%s\",\"%s\":[",
                                JSON_OB_REGION, appname, JSON_OB_REGION_ID, cluster_id,
                                JSON_OB_CLUSTER_TYPE, PRIMARY == role ? "PRIMARY" : "STANDBY", JSON_RS_LIST))) {
      LOG_WARN("assign string failed", K(buf_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char ip_buf[MAX_IP_ADDR_LENGTH];
    for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); ++i) {
      ip_buf[0] = '\0';
      if (i > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ","))) {
          LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!addr_list.at(i).server_.ip_to_string(ip_buf, sizeof(ip_buf))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert ip to string failed", K(ret), "server", addr_list.at(i).server_);
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
          "{\"%s\":\"%s:%d\",\"%s\":\"%s\",\"%s\":%d}",
          JSON_ADDRESS, ip_buf, addr_list.at(i).server_.get_port(),
          JSON_ROLE, LEADER == addr_list.at(i).role_ ? "LEADER" : "FOLLOWER",
          JSON_SQL_PORT, addr_list.at(i).server_.get_port()))) {
        LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "]}"))) {
        LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyJsonUtils::cluster_idc_list_to_json(const ObProxyClusterInfo &cluster_info,
                                               char *buf, const int64_t buf_len, int64_t &pos,
                                               const char *real_meta_cluster/*NULL*/)
{
  int ret = OB_SUCCESS;
  ObProxyClusterInfo::SubCIHashMap &map = const_cast<ObProxyClusterInfo::SubCIHashMap &>(cluster_info.sub_ci_map_);
  for (ObProxyClusterInfo::SubCIHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
    if (it->idc_list_.empty()) {
      // do nothing
    } else if (OB_FAIL(idc_list_to_json(it->idc_list_, cluster_info.cluster_name_.ptr(),
                                        it->cluster_id_, buf, buf_len, pos, real_meta_cluster))) {
      LOG_WARN("fail to dump sub cluster idc rslist", K_(cluster_info.cluster_name), K(*it), K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ","))) {
      LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

int ObProxyJsonUtils::idc_list_to_json(const ObProxyIDCList &idc_list, const char *appname,
    const int64_t cluster_id, char *buf, const int64_t buf_len, int64_t &pos,
    const char *real_meta_cluster/*NULL*/)
{
  int ret = OB_SUCCESS;
  if (NULL != real_meta_cluster) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{\"%s\":\"%s\",\"%s\":%ld,\"%s\":\"%s\",\"%s\":[",
        JSON_OB_REGION, appname, JSON_OB_REGION_ID, cluster_id,
        JSON_REAL_META_REGION, real_meta_cluster, JSON_IDC_LIST))) {
      LOG_WARN("assign string failed", K(buf_len), K(pos), K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{\"%s\":\"%s\",\"%s\":%ld,\"%s\":[",
                                JSON_OB_REGION, appname, JSON_OB_REGION_ID, cluster_id,
                                JSON_IDC_LIST))) {
      LOG_WARN("assign string failed", K(buf_len), K(pos), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < idc_list.count(); ++i) {
      if (i > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ","))) {
          LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
        }
      }
      const ObProxyIDCInfo &idc_info = idc_list.at(i);
      if (FAILEDx(databuff_printf(buf, buf_len, pos,
                                  "{\"%s\":\"%.*s\",\"%s\":\"%.*s\"}",
                                  JSON_IDC,
                                  idc_info.idc_name_.name_string_.length(),
                                  idc_info.idc_name_.name_string_.ptr(),
                                  JSON_REGION,
                                  idc_info.region_name_.name_string_.length(),
                                  idc_info.region_name_.name_string_.ptr()))) {
        LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "]}"))) {
        LOG_WARN("append string failed", K(buf_len), K(pos), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyNameString::parse(const Value *json_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_STRING))) {
    LOG_WARN("fail to check json string type", K(json_value), K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_string(json_value->get_string(), size_limit_))) {
    LOG_WARN("fail to check config string", K(json_value->get_string()), K(ret));
  } else {
    set_value(json_value->get_string());
  }
  return ret;
}

int ObProxyObInstance::parse(const Value *json_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check config info type", K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == "obCluster") {
        if (OB_FAIL(ob_cluster_.parse(it->value_))) {
          LOG_WARN("ob cluster parse failed", K(ret));
        }
      } else if (it->name_ == "obClusterId") {
        ob_cluster_id_ = it->value_->get_number();
      } else if (it->name_ == "obTenant") {
        if (OB_FAIL(ob_tenant_.parse(it->value_))) {
          LOG_WARN("ob tenant parse failed", K(ret));
        }
      } else if (it->name_ == "obTenantId") {
        ob_tenant_id_ = it->value_->get_number();
      } else if (it->name_ == "role") {
        if (OB_FAIL(role_.parse(it->value_))) {
          LOG_WARN("role parse failed", K(ret));
        }
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObProxyObInstance)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ob_cluster), K_(ob_cluster_id), K_(ob_tenant), K_(ob_tenant_id), K_(role));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObProxyLdgObInstacne)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ldg_cluster), K_(cluster_id), K_(ldg_tenant), K_(tenant_id));
  for (int64_t i = 0; i < instance_array_.count(); i++) {
    ObProxyObInstance *ob_instance = instance_array_.at(i);
    J_COMMA();
    J_KV(K(i), KPC(ob_instance));
  }
  J_OBJ_END();
  return pos;
}

ObString& ObProxyLdgObInstacne::get_hash_key()
{
  if (hash_key_.empty()) {
    int32_t len = ldg_tenant_.length();
    char ch = '#';
    MEMCPY(hash_key_buf_, ldg_tenant_.ptr(), len);
    MEMCPY(hash_key_buf_ + len, &ch, 1);
    len += 1;
    MEMCPY(hash_key_buf_ + len, ldg_cluster_.ptr(), ldg_cluster_.length());
    len += ldg_cluster_.length();
    hash_key_.assign_ptr(hash_key_buf_, len);
  }
  return hash_key_;
}

int ObProxyLdgObInstacne::parse(const Value *json_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, JT_OBJECT))) {
    LOG_WARN("fail to check ldg info", K(json_value), K(ret));
  } else {
    DLIST_FOREACH(it, json_value->get_object()) {
      if (it->name_ == "ldgCluster") {
        if (OB_FAIL(ldg_cluster_.parse(it->value_))) {
          LOG_WARN("ldg cluster parse failed", K(ret));
        }
      } else if (it->name_ == "ldgDatabaseId") {
        cluster_id_ = it->value_->get_number();
      } else if (it->name_ == "ldgTenant") {
        if (OB_FAIL(ldg_tenant_.parse(it->value_))) {
          LOG_WARN("ldg tenant parse failed", K(ret));
        }
      } else if (it->name_ == "ldgTenantId") {
        tenant_id_ = it->value_->get_number();
      } else if (it->name_ == "obInstanceList") {
        DLIST_FOREACH(ita, it->value_->get_array()) {
          ObProxyObInstance *ob_instance = NULL;
          if (NULL == (ob_instance = op_alloc(ObProxyObInstance))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory for ob instacne failed", K(ret));
          } else {
            ob_instance->inc_ref();
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(ob_instance->parse(ita))) {
              ob_instance->dec_ref();
              ob_instance = NULL;
              LOG_WARN("instance info parse failed", K(ret));
            } else if (OB_FAIL(instance_array_.push_back(ob_instance))) {
              ob_instance->dec_ref();
              ob_instance = NULL;
              LOG_WARN("instance array push back failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObProxyLdgInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(version));
  for (LdgInstanceMap::iterator it = const_cast<LdgInstanceMap&>(ldg_instance_map_).begin();
          it != const_cast<LdgInstanceMap&>(ldg_instance_map_).end(); ++it) {
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "ldg_instance_info", *it);
  }
  J_OBJ_END();
  return pos;
}

int ObProxyLdgInfo::update_ldg_instance(ObProxyLdgObInstacne *ldg_instance)
{
  int ret = OB_SUCCESS;
  ObProxyLdgObInstacne *tmp_instance = NULL;
  if (NULL != (tmp_instance = ldg_instance_map_.remove(ldg_instance->get_hash_key()))) {
    op_free(tmp_instance);
    tmp_instance = NULL;
  }
  if (OB_SUCC(ret) && OB_FAIL(ldg_instance_map_.unique_set(ldg_instance))) {
    LOG_WARN("ldg instance map add instance failed", K(ret));
  }
  return ret;
}

int ObProxyLdgInfo::get_ldg_instance(const common::ObString &key, ObProxyLdgObInstacne *&ldg_instance)
{
  int ret = OB_SUCCESS;
  ObProxyLdgObInstacne *tmp_instance = NULL;
  ldg_instance = NULL;
  if (OB_FAIL(ldg_instance_map_.get_refactored(key, tmp_instance))) {
    // LOG_WARN("ldg not exist", K(ret), K(key), K(*this));
  } else {
    ldg_instance = tmp_instance;
  }
  return ret;
}

int ObProxyLdgInfo::parse(const Value *json_value)
{
  int ret = OB_SUCCESS;
  Value *value = NULL;
  if (OB_FAIL(ObProxyJsonUtils::parse_header(json_value, value))) {
    LOG_WARN("fail to parse ldg info", K(json_value), K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(value, JT_OBJECT))) {
    LOG_WARN("fail to check ldg info", K(value), K(ret));
  } else {
    DLIST_FOREACH(it, value->get_object()) {
      if (it->name_ == JSON_CONFIG_VERSION) {
        if (OB_FAIL(version_.parse(it->value_))) {
          LOG_WARN("fail to parse md5 version", K(it->name_), K(ret));
        }
      } else if (it->name_ == "LdgObInstanceList") {
        DLIST_FOREACH(ita, it->value_->get_array()) {
          ObProxyLdgObInstacne *ldg_instance_info = NULL;
          if (OB_ISNULL(ldg_instance_info = op_alloc(ObProxyLdgObInstacne))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc ldg instance failed", K(ret));
          } else if (OB_FAIL(ldg_instance_info->parse(ita))) {
            op_free(ldg_instance_info);
            ldg_instance_info = NULL;
            LOG_WARN("ldg instacne info parse failed", K(ret));
          } else if (OB_FAIL(update_ldg_instance(ldg_instance_info))) {
            op_free(ldg_instance_info);
            ldg_instance_info = NULL;
            LOG_WARN("ldg instance array push back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObProxyLdgInfo::get_primary_role_instance(const ObString &tenant_name,
                                              const ObString &cluster_name,
                                              ObProxyObInstance* &instance)
{
  int ret = OB_SUCCESS;
  instance = NULL;
  if (tenant_name.empty() || cluster_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get primary role instance failed", K(ret), K(tenant_name), K(cluster_name));
  } else {
    char buf[OB_PROXY_FULL_USER_NAME_MAX_LEN];
    int32_t len = 0;
    MEMCPY(buf, tenant_name.ptr(), tenant_name.length());
    len += tenant_name.length();
    char ch = '#';
    MEMCPY(buf + len, &ch, 1);
    len += 1;
    MEMCPY(buf + len, cluster_name.ptr(), cluster_name.length());
    len += cluster_name.length();
    ObString key;
    key.assign_ptr(buf, len);
    ObProxyLdgObInstacne *tmp_ldg_instacne = NULL;
    if (OB_FAIL(get_ldg_instance(key, tmp_ldg_instacne))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("get ldg instance failed", K(ret), K(key));
      }
    } else if (OB_ISNULL(tmp_ldg_instacne)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ldg instance is null unexpected", K(ret), K(key));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_ldg_instacne->instance_array_.count(); i++) {
        ObProxyObInstance *ob_instance = tmp_ldg_instacne->instance_array_.at(i);
        if (ob_instance->role_.get_string() == "LDG PRIMARY") {
          instance = ob_instance;
          break;
        }
      }
    }
  }
  return ret;
}

void ObProxyLdgInfo::destroy()
{
  LdgInstanceMap::iterator tmp_iter;
  for (LdgInstanceMap::iterator it = ldg_instance_map_.begin();
          it != const_cast<LdgInstanceMap&>(ldg_instance_map_).end();) {
    tmp_iter = it;
    ++it;
    op_free(&(*tmp_iter));
  }
}

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase
