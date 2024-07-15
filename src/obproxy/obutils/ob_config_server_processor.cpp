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

#include <curl/curl.h>
#include <sys/utsname.h>

#include "obutils/ob_config_server_processor.h"
#include "lib/file/ob_file.h"
#include "common/ob_record_header.h"
#include "utils/ob_proxy_utils.h"
#include "utils/ob_zlib_stream_compressor.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_config_utils.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "obproxy/obutils/ob_hostname_ip_processor.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

static const char * const RS_URL_KEY_STRING = "ObRootServiceInfo";
static const ObString IDC_URL_KEY_STRING = ObString::make_string("ObIDCRegionInfo");
static const ObString IDC_URL_TAILER_STRING = ObString::make_string("_idc_list");
static const ObString RS_URL_V2("version=2");
static const ObString RS_URL_V2_REGION_ID("&ObRegionId=");
static const char JOIN_SEPARATOR = '?';
static const char PARAM_SEPARATOR = '&';
static const char * const CONFIG_URL_KEY_STRING_1 = "GetObProxyConfig";
static const char * const CONFIG_URL_KEY_STRING_2 = "GetObRootServiceInfoUrlTemplate";
static const ObString LDG_INSTANCE_INFO_BATCH_STRING = ObString::make_string("LdgInstanceInfoBatchQuery");
static const char *JSON_SERVICE_NAME_URL = "&ServiceNames=";
static const int64_t LDG_BATCH_SIZE = 10;
static const int64_t SERVICE_NAME_BATCH_SIZE = 10;
static const int64_t LDG_MAX_URL_LENGTH = 256 + OB_PROXY_MAX_CLUSTER_NAME_LENGTH * LDG_BATCH_SIZE;

static const int64_t MAX_CLUSTER_ID_LENGTH = 10; // see ob_max_cluster_id = 4294901759 in ob_define.h

ObConfigServerProcessor &get_global_config_server_processor()
{
  static ObConfigServerProcessor config_server_processor;
  return config_server_processor;
}

//------ ObConfigServerProcessor------
ObConfigServerProcessor::ObConfigServerProcessor()
  : is_inited_(false), dump_config_res_(false), need_dump_rslist_res_(false),
    need_dump_idc_list_res_(false), refresh_cont_(NULL),
    refresh_ldg_cont_(NULL),refresh_service_name_cont_(NULL),
    json_config_info_(NULL), json_info_lock_(WRITE_PRIORITY),
    proxy_config_(get_global_proxy_config()), kernel_release_(RELEASE_MAX),
    ldg_info_(NULL), service_name_info_(NULL), service_name_info_version_(0),
    disk_server_name_version_(0), ldg_info_lock_(), service_name_info_lock_()
{
}

ObConfigServerProcessor::~ObConfigServerProcessor()
{
  if (is_inited_) {
    curl_global_cleanup();
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(refresh_cont_))) {
      LOG_WDIAG("fail to destroy refresh config server task", K(ret));
    }

    if (NULL != refresh_ldg_cont_ && OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(refresh_ldg_cont_))) {
      LOG_WDIAG("fail to destroy refresh ldg cont task", K(ret));
    }
    if (NULL != refresh_service_name_cont_ && OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(refresh_service_name_cont_))) {
      LOG_WDIAG("fail to destroy refresh ldg cont task", K(ret));
    }

    {
      CWLockGuard lock(json_info_lock_);
      release(json_config_info_);
    }
    {
      DRWLock::WRLockGuard lock1(ldg_info_lock_);
      release(ldg_info_);
    }
    {
      DRWLock::WRLockGuard lock2(service_name_info_lock_);
      if (OB_NOT_NULL(service_name_info_)) {
        op_free(service_name_info_);
      }
    }
  }
  is_inited_ = false;
}

DEF_TO_STRING(ObConfigServerProcessor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(dump_config_res), K_(need_dump_rslist_res),
      K_(need_dump_idc_list_res), K_(kernel_release), KPC_(refresh_cont),
      KPC_(refresh_ldg_cont));
  J_OBJ_END();
  return pos;
}

int ObConfigServerProcessor::init(const bool load_local_config_succ)
{
  int ret = OB_SUCCESS;
  CURLcode curl_ret = CURLE_OK;
  ObProxyJsonConfigInfo *json_info = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("the fetch config processor has already been inited", K(ret));
  } else if (CURLE_OK != (curl_ret = curl_global_init(CURL_GLOBAL_ALL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to do curl_global_init", K(curl_ret), K(ret));
  } else if (OB_FAIL(init_proxy_kernel_release())) {
    LOG_WDIAG("fail to init proxy kernel release", K(ret));
  } else if (OB_UNLIKELY(NULL != json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ should be null before inited", K(ret));
  } else if (OB_ISNULL(json_info = op_alloc(ObProxyJsonConfigInfo))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc mem for json_config_info_", K(ret));
  } else if (OB_FAIL(swap(json_info))) {
    LOG_WDIAG("fail to init json_config_info_", K(ret));
  } else {
    if (proxy_config_.with_config_server_) {
      // if load_local_config_succ is true, we try to load json from local at first
      // else we will not care if  local json exist and try to fetch config server info from remote
      if (load_local_config_succ && !proxy_config_.ignore_local_config) {
        // 1 load json info from local at first
        // 2 if fail, try to get from remote config server, ignore ret because we have a timer task to do it
        if (OB_FAIL(load_config_from_local())) {
          LOG_WDIAG("fail to load json config info from local, now try to get from remote", K(ret));
          if (OB_FAIL(refresh_json_config_info())) {
            LOG_WDIAG("fail to get json config info from remote config server,"
            "ignore ret and proxy will schedule a timer task to get later", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          //load rs list info after get json config info, ignore ret
          if (OB_FAIL(load_rslist_info_from_local())) {
            LOG_INFO("fail to init rs list from local, we can get from config server later", K(ret));
          }

          //load idc list info after get json config info, ignore ret
          if (OB_FAIL(load_idc_list_info_from_local())) {
            LOG_INFO("fail to init idc list from local, we can get from config server later", K(ret));
          }
        }
      } else if (OB_FAIL(refresh_json_config_info())) {
        LOG_WDIAG("fail to get json config info from remote config server, try load local file and "
                 "schedule a timer task to update later", K(ret));
        if (OB_FAIL(load_config_from_local())) {
          LOG_INFO("fail to load json config info from local, we can get from config server later", K(ret));
        } else {
          if (OB_FAIL(load_rslist_info_from_local())) {
            LOG_INFO("fail to init rs list from local, we can get from config server later", K(ret));
          }

          //load idc list info after get json config info, ignore ret
          if (OB_FAIL(load_idc_list_info_from_local())) {
            LOG_INFO("fail to init idc list from local, we can get from config server later", K(ret));
          }
        }
      }

      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && proxy_config_.enable_ldg) {
      // init Hash bucket_num for cluster_name
      // HashMap插入的是集群名-版本号，预计集群的数量一般最多10+
      LdgHashMap& ldg_cluster_hash = get_global_config_server_processor().ldg_cluster_hash_;
      if (OB_FAIL(ldg_cluster_hash.create(4, ObModIds::OB_HASH_BUCKET_CONF_CONTAINER, ObModIds::OB_HASH_NODE_CONF_CONTAINER))) {
        LOG_WDIAG("ldg cluster name HashSet init failed", K(ret));
      } else {
        LOG_INFO("success to init ldg info hash");
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(load_service_name_info_from_local())) {
      LOG_WDIAG("fail to load service name info from local, maybe file not exist", K(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_INFO("success to load service name from local");
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObConfigServerProcessor::fetch_newest_cluster_rslist(const ObString &cluster_name, const int64_t cluster_id,
    LocationList &web_rslist, const bool need_update_dummy_entry /*true*/)
{
  int ret = OB_SUCCESS;
  char *url = NULL;
  ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;

  //1、get cluster url
  if (OB_FAIL(get_cluster_url(cluster_name, allocator, url, cluster_id))) {
    LOG_WDIAG("fail to get cluster url", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_FAIL(fetch_rs_list_from_url(url, cluster_name, cluster_id, web_rslist, need_update_dummy_entry))) {
    //2、fetch web rs list with cluster name and url
    if (OB_EAGAIN != ret) {
      LOG_WDIAG("fail to fetch web rs list from url", K(url), K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    need_dump_rslist_res_ = true;
  }

  return ret;
}

int ObConfigServerProcessor::get_newest_cluster_rs_list(const ObString &cluster_name, const int64_t cluster_id,
    ObIArray<ObAddr> &rs_list, ObIArray<ObAddr> &rpc_rs_list, const bool need_update_dummy_entry /*true*/)
{
  int ret = OB_SUCCESS;
  #ifdef ERRSIM
  if (OB_FAIL(OB_E(EventTable::EN_FETCH_RSLIST_FAIL) OB_SUCCESS)) {
    return ret;
  }
  #endif

  LocationList web_rslist;
  int64_t last_master_cluster_id = OB_DEFAULT_CLUSTER_ID;
  int64_t cur_master_cluster_id = OB_DEFAULT_CLUSTER_ID;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_FAIL(get_master_cluster_id(cluster_name, last_master_cluster_id))) {
    LOG_DEBUG("last master cluster id does not exist", K(cluster_name), K(ret));
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && OB_FAIL(fetch_newest_cluster_rslist(cluster_name, cluster_id, web_rslist, need_update_dummy_entry))) {
    // 1、fetch newest cluster rslist
    LOG_WDIAG("fail to fetch newest cluster rslist", K(cluster_name), K(cluster_id), K(ret));
  }

  //2、convert web_rslist to obaddr list
  // 当前允许为空, 由上层使用方检测
  if (OB_SUCC(ret) && OB_LIKELY(!web_rslist.empty())
      && OB_FAIL(convert_root_addr_to_addr(web_rslist, rs_list, rpc_rs_list))) {
    LOG_WDIAG("fail to convert web rslist to rslist", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(get_master_cluster_id(cluster_name, cur_master_cluster_id))) {
    LOG_WDIAG("master cluster id does not exist", K(cluster_name), K(ret));
  }
  // 拉主集群rslist时，如果主集群master cluster id发生变化，需要删除当前主集群cluster resource
  if (OB_SUCC(ret) && last_master_cluster_id != cur_master_cluster_id
      && OB_DEFAULT_CLUSTER_ID != last_master_cluster_id
      && OB_DEFAULT_CLUSTER_ID == cluster_id) {
    LOG_INFO("primary cluster id has been changed, will delete current primary cluster resource",
             K(cluster_name), K(last_master_cluster_id), K(cur_master_cluster_id));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = get_global_resource_pool_processor().delete_cluster_resource(
                       cluster_name, OB_DEFAULT_CLUSTER_ID))) {
      LOG_WDIAG("fail to delete primary cluster resource", K(cluster_name), K(tmp_ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_cluster_rs_list(const ObString &cluster_name,
    const int64_t cluster_id,
    ObIArray<ObAddr> &rs_list,
    ObIArray<ObAddr> &rpc_rs_list) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
      LOG_WDIAG("fail to get cluster info", K(cluster_name), K(cluster_id), K(ret));
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("null cluster info pointer", K(ret));
    } else if (OB_FAIL(convert_root_addr_to_addr(sub_cluster_info->web_rs_list_, rs_list, rpc_rs_list))) {
      LOG_INFO("fail to get cluster rs list, maybe we have not fetch rslist yet", KPC(sub_cluster_info), K(ret));
    } else { }
  }
  return ret;
}

bool ObConfigServerProcessor::has_slave_clusters(const ObString &cluster_name)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  ObProxyClusterInfo *cluster_info = NULL;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    LOG_WDIAG("json config info is null");
  } else if (OB_FAIL(json_config_info_->get_cluster_info(cluster_name, cluster_info))) {
    LOG_WDIAG("fail to get cluster info", K(cluster_name), K(ret));
  } else if (OB_ISNULL(cluster_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("null cluster info pointer", K(ret));
  } else {
    bret = cluster_info->get_sub_cluster_count() > 1;
  }
  return bret;
}

int ObConfigServerProcessor::get_next_master_cluster_rslist(const ObString &cluster_name,
                                                            ObIArray<ObAddr> &rs_list,
                                                            ObIArray<ObAddr> &rpc_rs_list)
{
  int ret = OB_SUCCESS;
  bool is_master_changed = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    // will change master cluster id, so we need write lock here
    CWLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_next_master_cluster_info(cluster_name, sub_cluster_info, is_master_changed))) {
      LOG_WDIAG("fail to get next master cluster info", K(cluster_name), K(ret));
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("null cluster info pointer", K(ret));
    } else if (OB_FAIL(convert_root_addr_to_addr(sub_cluster_info->web_rs_list_, rs_list, rpc_rs_list))) {
      LOG_INFO("fail to get cluster rs list, maybe we have not fetch rslist yet", KPC(sub_cluster_info), K(ret));
    } else if (is_master_changed) {
      need_dump_rslist_res_ = true;
      LOG_DEBUG("succ to get next master cluster", K(cluster_name), K(sub_cluster_info->cluster_id_));
    }
  }
  return ret;
}

int ObConfigServerProcessor::refresh_idc_list(const ObString &cluster_name, const int64_t cluster_id, ObProxyIDCList &idc_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    char *url = NULL;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    if (OB_FAIL(get_idc_url(cluster_name, allocator, url, cluster_id))) {
      LOG_INFO("fail to get cluster url", K(cluster_name), K(cluster_id), K(ret));
    } else if (OB_FAIL(refresh_idc_list_from_url(url, cluster_name, cluster_id, idc_list))) {
      LOG_INFO("fail to refresh idc list from url", K(url), K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_cluster_idc_region(const common::ObString &cluster_name,
    const int64_t cluster_id,
    const common::ObString &idc_name, ObProxyNameString &region_name) const
{
  int ret = OB_SUCCESS;
  region_name.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (!idc_name.empty()) {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("fail to get cluster info", K(cluster_name), K(cluster_id), K(ret));
      }
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("null cluster info pointer", K(ret));
    } else if (OB_FAIL(sub_cluster_info->get_idc_region(idc_name, region_name))) {
      LOG_WDIAG("fail to get idc region", K(sub_cluster_info->idc_list_), K(idc_name), K(ret));
    } else if (region_name.empty()) {
      LOG_INFO("can not find region from id list, ignore", KPC(sub_cluster_info), K(idc_name));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_master_cluster_id(const common::ObString &cluster_name, int64_t &cluster_id) const
{
  int ret = OB_SUCCESS;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(ret));
  } else if (OB_FAIL(json_config_info_->get_master_cluster_id(cluster_name, cluster_id))) {
    LOG_WDIAG("fail to get cluster id", K(cluster_name), K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::set_master_cluster_id(const ObString &cluster_name, int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  CWLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(ret));
  } else if (OB_FAIL(json_config_info_->set_master_cluster_id(cluster_name, cluster_id))) {
    LOG_WDIAG("fail to set master cluster id", K(cluster_name), K(cluster_id), K(ret));
  } else {
    need_dump_rslist_res_ = true;
  }
  return ret;
}

int ObConfigServerProcessor::get_rs_list_hash(const common::ObString &cluster_name,
    const int64_t cluster_id,
    uint64_t &rs_list_hash) const
{
  int ret = OB_SUCCESS;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(ret));
  } else if (OB_FAIL(json_config_info_->get_rs_list_hash(cluster_name, cluster_id, rs_list_hash))) {
    LOG_WDIAG("fail to get cluster rs list hash", K(cluster_name), K(cluster_id), K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::get_default_cluster_name(char *buffer, const int64_t len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_default_cluster_name(buffer, len))) {
      LOG_WDIAG("fail to get default cluster info", K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_cluster_url(
                             const ObString &cluster_name,
                             ObIAllocator &allocator,
                             char *&buffer,
                             const int64_t cluster_id/*0*/) const
{
  int ret = OB_SUCCESS;
  buffer = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObProxyClusterInfo *cluster_info = NULL;
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_cluster_info(cluster_name, cluster_info))) {
      LOG_WDIAG("fail to get cluster info", K(cluster_name), K(ret));
    } else if (OB_ISNULL(cluster_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("null cluster info pointer", K(ret));
    } else {
      // origin_url + separator + "version=2" + "&cluster_id=" + cluster_id + '\0'
      int64_t url_len = cluster_info->rs_url_.length() + 1 + RS_URL_V2.length() + RS_URL_V2_REGION_ID.length() + MAX_CLUSTER_ID_LENGTH + 1;
      if (OB_ISNULL(buffer = static_cast<char *>(allocator.alloc(url_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc mem for rs url", K(cluster_info), K(url_len), K(cluster_id), K(ret));
      } else {
        int64_t pos = 0;
        const char join_char = NULL == cluster_info->rs_url_.url_.find(JOIN_SEPARATOR) ? JOIN_SEPARATOR : PARAM_SEPARATOR;
        MEMCPY(buffer + pos, cluster_info->rs_url_.ptr(), cluster_info->rs_url_.length());
        pos += cluster_info->rs_url_.length();
        buffer[pos++] = join_char;
        MEMCPY(buffer + pos, RS_URL_V2.ptr(), RS_URL_V2.length());
        pos += RS_URL_V2.length();
        buffer[pos] = '\0';
        // OB_DEFAULT_CLUSTER_ID means master cluster, no need to add cluster id
        if (OB_DEFAULT_CLUSTER_ID != cluster_id) {
          int64_t w_len = snprintf(buffer + pos, static_cast<size_t>(url_len - pos),
                                   "%.*s%ld", RS_URL_V2_REGION_ID.length(),
                                   RS_URL_V2_REGION_ID.ptr(), cluster_id);
          if (OB_UNLIKELY(w_len < 0) || OB_UNLIKELY(w_len > url_len - pos)) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_EDIAG("fail to snprintf for cluster url", K(pos), K(url_len), K(cluster_id), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_idc_url(
                             const ObString &cluster_name,
                             ObIAllocator &allocator,
                             char *&buffer,
                             const int64_t cluster_id /*0*/) const
{
  int ret = OB_SUCCESS;
  buffer = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObProxyClusterInfo *cluster_info = NULL;
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_cluster_info(cluster_name, cluster_info))) {
      LOG_WDIAG("fail to get cluster info", K(cluster_name), K(ret));
    } else if (OB_ISNULL(cluster_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("null cluster info pointer", KPC(cluster_info), K(ret));
    } else if (OB_UNLIKELY(!cluster_info->rs_url_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_INFO("rs_url_ is invalid", KPC(cluster_info), K(ret));
    } else {
      const int64_t idc_url_len = cluster_info->rs_url_.length() + IDC_URL_TAILER_STRING.length()
        + 1 + RS_URL_V2.length() + RS_URL_V2_REGION_ID.length() + MAX_CLUSTER_ID_LENGTH + 1;
      if (OB_ISNULL(buffer = static_cast<char *>(allocator.alloc(idc_url_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc mem for rs url", K(idc_url_len), K(ret));
      } else if (OB_FAIL(get_idc_url(cluster_info->rs_url_.buf_ptr(), cluster_info->rs_url_.length(),
              buffer, idc_url_len, cluster_id))) {
        LOG_WDIAG("fail to get_idc_url", K(ret));
      }
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_idc_url(const char *rs_url_buf, const int64_t rs_url_buf_len,
    char *&idc_url_buf, const int64_t idc_url_buf_len, const int64_t cluster_id /*0*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rs_url_buf)
      || OB_ISNULL(idc_url_buf)
      || OB_UNLIKELY(idc_url_buf_len <= rs_url_buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("argument is invalid", KP(rs_url_buf), KP(idc_url_buf), K(rs_url_buf_len),
             K(idc_url_buf_len), K(ret));
  } else {
    const char *pos = strcasestr(rs_url_buf, RS_URL_KEY_STRING);
    if (NULL != pos) {
      const int64_t url_head_len = static_cast<int64_t>(pos - rs_url_buf);
      const int64_t url_key_len = static_cast<int64_t>(strlen(RS_URL_KEY_STRING));
      const int64_t url_tailer_len = rs_url_buf_len - url_head_len - url_key_len;
      MEMCPY(idc_url_buf, rs_url_buf, url_head_len);
      MEMCPY(idc_url_buf + url_head_len, IDC_URL_KEY_STRING.ptr(), IDC_URL_KEY_STRING.length());
      MEMCPY(idc_url_buf + url_head_len + IDC_URL_KEY_STRING.length(),
             rs_url_buf + url_head_len + url_key_len,
             url_tailer_len);
      idc_url_buf[url_head_len + IDC_URL_KEY_STRING.length() + url_tailer_len] = '\0';
    } else {
      MEMCPY(idc_url_buf, rs_url_buf, rs_url_buf_len);
      MEMCPY(idc_url_buf + rs_url_buf_len,
             IDC_URL_TAILER_STRING.ptr(),
             IDC_URL_TAILER_STRING.length());
      idc_url_buf[rs_url_buf_len + IDC_URL_TAILER_STRING.length()] = '\0';
    }
    if (OB_DEFAULT_CLUSTER_ID != cluster_id) {
      ObString tmp_str = ObString::make_string(idc_url_buf);
      const char join_char = NULL == tmp_str.find(JOIN_SEPARATOR) ? JOIN_SEPARATOR : PARAM_SEPARATOR;
      int64_t offset = tmp_str.length();
      int64_t w_len = snprintf(idc_url_buf + offset, static_cast<size_t>(idc_url_buf_len - offset),
                               "%c%.*s%.*s%ld", join_char,
                               RS_URL_V2.length(), RS_URL_V2.ptr(),
                               RS_URL_V2_REGION_ID.length(), RS_URL_V2_REGION_ID.ptr(),
                               cluster_id);
      if (OB_UNLIKELY(w_len < 0) || OB_UNLIKELY(w_len > idc_url_buf_len - offset)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_EDIAG("fail to snprintf for cluster idc url", K(offset), K(idc_url_buf_len), K(cluster_id), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("succ to get idc url", "rs_url_buf", rs_url_buf, "idc_url_buf", idc_url_buf, K(ret));
    }
  }
  return ret;
}

int64_t ObConfigServerProcessor::get_cluster_count() const
{
  int64_t count = 0;
  CRLockGuard lock(json_info_lock_);
  if (OB_LIKELY(NULL!= json_config_info_)) {
    count = json_config_info_->get_cluster_count();
  }
  return count;
}

int ObConfigServerProcessor::inc_and_get_create_failure_count(const ObString &cluster_name,
    const int64_t cluster_id,
    int64_t &new_failure_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    CWLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else {
      new_failure_count = json_config_info_->inc_create_failure_count(cluster_name, cluster_id);
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_create_failure_count(const ObString &cluster_name,
    const int64_t cluster_id,
    int64_t &new_failure_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json config info is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
      LOG_WDIAG("fail to get cluster info", K(cluster_name), K(cluster_id), K(ret));
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("null cluster info pointer", K(ret));
    } else {
      new_failure_count = sub_cluster_info->create_failure_count_;
      LOG_DEBUG("succ to get create_failure_count", K(cluster_name), K(cluster_id), K(new_failure_count));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_real_cluster_name(ObString &real_cluster_name, const ObString &cluster_name) const
{
  int ret = OB_SUCCESS;
  real_cluster_name = ObString::make_empty_string();
  ObProxyClusterInfo *cluster_info;
  CRLockGuard lock(json_info_lock_);
  if (OB_FAIL(json_config_info_->get_cluster_info(cluster_name, cluster_info))) {
    LOG_WDIAG("fail to get cluster info", K(cluster_name), K(ret));
  } else {
    real_cluster_name = cluster_info->real_cluster_name_;
  }
  return ret;
}

bool ObConfigServerProcessor::is_real_meta_cluster_exist() const
{
  bool bret = false;
  CRLockGuard lock(json_info_lock_);
  if (OB_LIKELY(NULL != json_config_info_)) {
    bret = json_config_info_->is_real_meta_cluster_exist();
  }
  return bret;
}

bool ObConfigServerProcessor::is_cluster_name_exists(const ObString &cluster_name) const
{
  bool bret = false;
  CRLockGuard lock(json_info_lock_);
  if (OB_LIKELY(NULL != json_config_info_)) {
    bret = json_config_info_->is_cluster_exists(cluster_name);
  }
  return bret;
}

bool ObConfigServerProcessor::is_cluster_name_alias(const ObString &cluster_name) const
{
  bool bret =false;
  CRLockGuard lock(json_info_lock_);
  if (OB_LIKELY(NULL != json_config_info_)) {
    bret = json_config_info_->is_cluster_name_alias(cluster_name);
  }
  return bret;
}

int ObConfigServerProcessor::get_cluster_info(const common::ObString &cluster_name,
    const bool is_from_default, ObProxyConfigString &real_meta_cluster_name) const
{
  int ret = OB_SUCCESS;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(cluster_name), K(ret));
  } else if (!json_config_info_->is_cluster_exists(cluster_name)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WDIAG("cluster_name do not exists in current proxy", K(cluster_name), K(ret));
  } else if (is_from_default && (json_config_info_->get_cluster_count() > 1)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WDIAG("cluster_name do not exists in login info", K(cluster_name), K(ret));
  } else {
    if (cluster_name == OB_META_DB_CLUSTER_NAME) {
      const ObString &name = json_config_info_->get_real_meta_cluster_name();
      if (OB_LIKELY(!name.empty())) {
        real_meta_cluster_name.set_value(name);
      }
      LOG_DEBUG("succ to set real meta cluster name", K(real_meta_cluster_name));
    }
  }

  return ret;
}

int ObConfigServerProcessor::get_proxy_meta_table_info(ObProxyMetaTableInfo &table_info) const
{
  int ret = OB_SUCCESS;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(ret));
  } else if (OB_FAIL(json_config_info_->get_meta_table_info(table_info))) {
    LOG_WDIAG("fail to get meta table info", K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::get_proxy_meta_table_username(ObProxyConfigString &username) const
{
  int ret = OB_SUCCESS;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(ret));
  } else if (OB_FAIL(json_config_info_->get_meta_table_username(username))) {
    LOG_WDIAG("fail to get meta table username", K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::get_proxy_meta_table_login_info(ObProxyLoginInfo &info) const
{
  int ret = OB_SUCCESS;
  CRLockGuard lock(json_info_lock_);
  if (OB_ISNULL(json_config_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json_config_info_ is null", K(ret));
  } else if (OB_FAIL(json_config_info_->get_meta_table_login_info(info))) {
    LOG_WDIAG("fail to get meta table login info", K(ret));
  }
  return ret;
}

ObProxyJsonConfigInfo *ObConfigServerProcessor::acquire()
{
  ObProxyJsonConfigInfo *json_info = NULL;
  if (OB_LIKELY(is_inited_)) {
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_info = json_config_info_)) {
      LOG_WDIAG("json_config_info_ is null");
    } else if (OB_UNLIKELY(json_info->refcount_inc() <= 1)) {
      LOG_WDIAG("json info refcount must be at least 2 now", "refcount", json_info->refcount());
      json_info = NULL;
    }
  }
  return json_info;
}

void ObConfigServerProcessor::release(ObProxyJsonConfigInfo *json_info)
{
  if (OB_LIKELY(NULL != json_info) && OB_LIKELY(0 == json_info->refcount_dec())) {
    json_info->destroy_cluster_info();
    op_free(json_info);
    json_info = NULL;
  }
}

void ObConfigServerProcessor::release(ObProxyLdgInfo *ldg_info)
{
  if (OB_LIKELY(NULL != ldg_info) && OB_LIKELY(0 == ldg_info->refcount_dec())) {
    op_free(ldg_info);
    ldg_info = NULL;
  }
}

int ObConfigServerProcessor::swap(ObProxyJsonConfigInfo *json_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(json_info), K(ret));
  } else {
    json_info->refcount_inc();
    ObProxyJsonConfigInfo *tmp_json = NULL;
    {
      CWLockGuard lock(json_info_lock_);
      tmp_json = json_config_info_;
      json_config_info_ = json_info;
      json_config_info_->gmt_modified_ = ObTimeUtility::current_time();
    }
    if (NULL != tmp_json) {
      release(tmp_json);
    }
  }
  return ret;
}

int ObConfigServerProcessor::swap_with_rslist(ObProxyJsonConfigInfo *new_json_info, const bool is_metadb_changed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_json_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(new_json_info), K(ret));
  } else {
    new_json_info->refcount_inc();
    {
      CRLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json config info is null", K(ret));
      } else {
        //1. deep copy meta_cluster_ rslist,
        //if metadb changed, no need to copy, will fetch newest rslist in rebuild metadb cluster resource
        ObProxySubClusterInfo *sub_cluster_info = NULL;
        const ObProxyClusterInfo &old_cluster_info = json_config_info_->get_meta_table_info().cluster_info_;
        if (!is_metadb_changed) {
          if (OB_FAIL(old_cluster_info.get_sub_cluster_info(old_cluster_info.master_cluster_id_, sub_cluster_info))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              LOG_WDIAG("fail to get meta db cluster info", K(old_cluster_info), K(ret));
            } else {
              LOG_DEBUG("old meta db cluster info has no sub cluster info, ignore it", K(old_cluster_info));
              ret = OB_SUCCESS;
            }
          } else if (OB_ISNULL(sub_cluster_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("sub_cluster_info of meta db cluster is null", K(ret));
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(new_json_info->set_real_meta_cluster_name(json_config_info_->get_real_meta_cluster_name()))) {
            LOG_WDIAG("fail to set real meta cluster name", "name", json_config_info_->get_real_meta_cluster_name(), K(ret));
          } else if (OB_DEFAULT_CLUSTER_ID != old_cluster_info.master_cluster_id_
                     && OB_FAIL(new_json_info->set_master_cluster_id(old_cluster_info.cluster_name_,
                     old_cluster_info.master_cluster_id_))) {
            LOG_WDIAG("fail to set cluster id", K(old_cluster_info), K(ret));
          } else if (NULL != sub_cluster_info
                  && OB_FAIL(new_json_info->set_cluster_web_rs_list(old_cluster_info.cluster_name_, old_cluster_info.master_cluster_id_,
                                                                    sub_cluster_info->web_rs_list_, sub_cluster_info->origin_web_rs_list_,
                                                                    ObString::make_string(cluster_role_to_str(sub_cluster_info->role_)),
                                                                    old_cluster_info.real_cluster_name_,
                                                                    old_cluster_info.is_cluster_name_alias()))) {
            if (OB_ENTRY_NOT_EXIST != ret && OB_EAGAIN != ret) {
              LOG_WDIAG("fail to set cluster web rs_list", K(old_cluster_info), K(ret));
            } else {
              //Here we no need copy cluster_create_failure_
              LOG_DEBUG("new json config info has no this cluster to set cluster web rs_list, ignore it",
                        K(old_cluster_info), K(ret));
              ret = OB_SUCCESS;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (NULL != sub_cluster_info && OB_FAIL(new_json_info->set_idc_list(old_cluster_info.cluster_name_, old_cluster_info.master_cluster_id_, sub_cluster_info->idc_list_))) {
            if (OB_ENTRY_NOT_EXIST != ret && OB_EAGAIN != ret) {
              LOG_WDIAG("fail to set idc list", K(old_cluster_info), K(ret));
            } else {
              LOG_DEBUG("new json config info has no this cluster to set idc list, ignore it",
                        K(old_cluster_info), K(ret));
              ret = OB_SUCCESS;
            }
          }
        } // end if (!is_meta_db_changed)

        //2. deep copy other cluster rslist
        // if old cluster does not exist, will delete its cluster resource
        if (OB_SUCC(ret)) {
          ObProxyClusterArrayInfo &old_cluster_array = const_cast<ObProxyClusterArrayInfo &>(json_config_info_->get_cluster_array());
          for (ObProxyClusterArrayInfo::CIHashMap::iterator old_it = old_cluster_array.ci_map_.begin();
               OB_SUCC(ret) && old_it != old_cluster_array.ci_map_.end(); ++old_it) {
            for (ObProxyClusterInfo::SubCIHashMap::iterator sub_it = old_it->sub_ci_map_.begin();
                 OB_SUCC(ret) && sub_it != old_it->sub_ci_map_.end(); ++sub_it) {
              if (sub_it == old_it->sub_ci_map_.begin()) {
                if (OB_DEFAULT_CLUSTER_ID != old_it->master_cluster_id_
                    && OB_FAIL(new_json_info->set_master_cluster_id(old_it->cluster_name_,
                    old_it->master_cluster_id_))) {
                  if (OB_ENTRY_NOT_EXIST != ret) {
                    LOG_WDIAG("fail to set cluster id", KPC(old_it.value_), K(ret));
                  }
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(new_json_info->set_cluster_web_rs_list(old_it->cluster_name_, sub_it->cluster_id_,
                                                                        sub_it->web_rs_list_, sub_it->origin_web_rs_list_,
                                                                        ObString::make_string(cluster_role_to_str(sub_it->role_)),
                                                                        old_it->real_cluster_name_,
                                                                        old_it->is_cluster_name_alias()))) {
                if (OB_ENTRY_NOT_EXIST != ret && OB_EAGAIN != ret) {
                  LOG_WDIAG("fail to set cluster web rs_list", KPC(old_it.value_), K(ret));
                } else if (OB_EAGAIN == ret) {
                  LOG_DEBUG("rslist does not changed, no need set", KPC(old_it.value_), K(ret));
                  ret = OB_SUCCESS;
                }
              } // end fail set_cluster_web_rs_list
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(new_json_info->set_idc_list(old_it->cluster_name_, sub_it->cluster_id_, sub_it->idc_list_))) {
                if (OB_ENTRY_NOT_EXIST != ret && OB_EAGAIN != ret) {
                  LOG_WDIAG("fail to set idc list", KPC(old_it.value_), K(ret));
                } else if (OB_EAGAIN == ret) {
                  LOG_DEBUG("idc list does not changed, no need set", KPC(old_it.value_), K(ret));
                  ret = OB_SUCCESS;
                }
              } else {
                //Here we no need copy cluster_create_failure_
                LOG_DEBUG("succ to set cluster web rs_list and idc list", KPC(old_it.value_));
              }
              if (OB_ENTRY_NOT_EXIST == ret) {
                LOG_INFO("this cluster has been deleted in config server,"
                         " we will delete its cluster resource", "cluster name", old_it->cluster_name_, "cluster_id", sub_it->cluster_id_);
                if (OB_FAIL(get_global_resource_pool_processor().add_cluster_delete_task(old_it->cluster_name_, sub_it->cluster_id_))) {
                  LOG_WDIAG("fail to add delete cluster resource task",
                           "cluster name", old_it->cluster_name_, "cluster_id", sub_it->cluster_id_, K(ret));
                }
                ret = OB_SUCCESS; // continue
              } // end OB_ENTRY_NOT_EXIST
            } // end loop sub cluster info
          }//end of for cluster info array
        }//end of cluster_array
      }//end of web_rs_list copy
    }

    if (OB_SUCC(ret)) {
      ObProxyJsonConfigInfo *tmp_json = NULL;
      {
        CWLockGuard lock(json_info_lock_);
        tmp_json = json_config_info_;
        json_config_info_ = new_json_info;
        json_config_info_->gmt_modified_ = ObTimeUtility::current_time();
      }
      if (NULL != tmp_json) {
        release(tmp_json);
      }
    }
  }
  return ret;
}

int ObConfigServerProcessor::do_fetch_proxy_bin(const char *bin_save_path, const char *binary_name)
{
  int ret = OB_SUCCESS;
  char *bin_url = NULL;

  if (OB_ISNULL(bin_save_path) || OB_ISNULL(binary_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("argument is null", K(bin_save_path), K(binary_name));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("ObConfigServerProcessor is not inited", K(ret));
  } else {
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    if (OB_FAIL(build_proxy_bin_url(binary_name, allocator, bin_url))) {
      LOG_WDIAG("fail to get proxy_bin_path", K(ret));
    } else {
      int fd = -1;
      if ((fd = ::open(bin_save_path, O_WRONLY | O_CREAT | O_TRUNC,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)) < 0) {
        ret = OB_IO_ERROR;
        LOG_WDIAG("fail to open file", K(bin_save_path), K(ret));
      } else if(OB_FAIL(fetch_by_curl(bin_url, usec_to_sec(proxy_config_.fetch_proxy_bin_timeout),
          reinterpret_cast<void *>((int64_t)fd), write_proxy_bin))) {
        LOG_WDIAG("fail to fetch new proxy bin", K(bin_url), K(bin_save_path), K(ret));
      } else { }

      if (fd > 0) {
        ::close(fd);
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succ to fetch proxy bin", K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::set_default_rs_list(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  ObProxyConfig &config = get_global_proxy_config();
  int64_t total_size = config.rootservice_list.size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(ret));
  } else if (OB_UNLIKELY(total_size <= 0)) {
    ret = OB_INVALID_CONFIG;
    LOG_WDIAG("default rslist is empty", K(ret));
  } else if (!json_config_info_->cluster_info_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("cluster_info has already been set", K(json_config_info_), K(ret));
  } else if (cluster_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("empty cluster name", K(cluster_name), K(ret));
  } else if (cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("cluster name buffer is not enough", K(cluster_name), K(ret));
  } else {
    //parse rs list and push back into cluster_info_
    ObProxyClusterInfo *cluster_info = op_alloc(ObProxyClusterInfo);
    if (NULL != cluster_info) {
      LocationList web_rs_list;
      cluster_info->cluster_name_.set_value(cluster_name);
      char addr_buf[MAX_IP_PORT_LENGTH];
      ObProxyReplicaLocation addr;
      for (int64_t i = 0; OB_SUCC(ret) && i < total_size; ++i) {
        addr_buf[0] = '\0';
        addr.reset();
        if (0 == i) {
          addr.role_ = LEADER;
        }
        if (OB_FAIL(config.rootservice_list.get(i, addr_buf, static_cast<int64_t>(sizeof(addr_buf))))) {
          LOG_WDIAG("get root server ip failed", K(ret));
        } else if (OB_FAIL(addr.server_.parse_from_cstring(addr_buf))) {
          LOG_WDIAG("parse_rs_addr failed", K(addr_buf), K(ret));
        } else if (OB_FAIL(web_rs_list.push_back(addr))) {
          LOG_WDIAG("fail to push web rs list", K(addr), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(json_config_info_->add_default_cluster_info(cluster_info, web_rs_list))) {
          LOG_WDIAG("fail to add cluster info", K(cluster_info), K(ret));
        }
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory for cluster info", K(cluster_info), K(ret));
    }
    if (OB_FAIL(ret) && NULL != cluster_info) {
      op_free(cluster_info);
      cluster_info = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to set web rs list", K(json_config_info_));
  }
  return ret;
}

int ObConfigServerProcessor::get_kernel_release_by_uname(ObProxyKernelRelease &release) const
{
  int ret = OB_SUCCESS;
  struct utsname u_info;
  if (0 != uname(&u_info)) {
    ret = OB_ERROR;
    LOG_WDIAG("fail to get Linux server info", K(ret));
  } else if (OB_ISNULL(u_info.release)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("release name is null", K(ret));
  } else if (NULL != strstr(u_info.release, ".el5")) {
    release = RELEASE_5U;
  } else if (NULL != strstr(u_info.release, ".alios5")) {
    release = RELEASE_5U;
  } else if (NULL != strstr(u_info.release, ".el6")) {
    release = RELEASE_6U;
  } else if (NULL != strstr(u_info.release, ".alios6")) {
    release = RELEASE_6U;
  } else if (NULL != strstr(u_info.release, ".el7")) {
    release = RELEASE_7U;
  } else if (NULL != strstr(u_info.release, ".alios7")) {
    release = RELEASE_7U;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unknown uname release", K(u_info.release), K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to get_kernel_release_by_uname", K(u_info.release),
             "release", get_kernel_release_string(release));
  }
  return ret;
}

int ObConfigServerProcessor::get_kernel_release_by_redhat(ObProxyKernelRelease &release) const
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int64_t read_count = -1;

  char result[OB_MAX_UNAME_INFO_LEN];
  result[0] = '\0';

  if (OB_UNLIKELY((fd = ::open("/etc/redhat-release", O_RDONLY)) < 0)) {
    ret = OB_IO_ERROR;
    LOG_WDIAG("fail to open /etc/redhat-release info", K(ret));
  } else if (OB_UNLIKELY((read_count = unintr_pread(fd, result, OB_MAX_UNAME_INFO_LEN, 0)) <= 0)) {
    ret = OB_IO_ERROR;
    LOG_WDIAG("fail to read /etc/redhat-release", K(read_count), K(ret));
  } else {
    char a = result[read_count -1];
    if ('\n' == a || '\t' == a || OB_MAX_UNAME_INFO_LEN == read_count) {
      result[read_count -1] = '\0';
    } else {
      result[read_count] = '\0';
    }

    if (NULL != strstr(result, "release 5.")) {
      release = RELEASE_5U;
    } else if (NULL != strstr(result, "release 6.")) {
      release = RELEASE_6U;
    } else if (NULL != strstr(result, "release 7.")) {
      release = RELEASE_7U;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown redhat release", K(result), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succ to get_kernel_release_by_redhat", K(result),
             "release", get_kernel_release_string(release));
  }

  if (fd > 0) {
    ::close(fd);
  }
  return ret;
}

int ObConfigServerProcessor::get_kernel_release_by_glibc(ObProxyKernelRelease &release) const
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  char result[OB_MAX_UNAME_INFO_LEN];
  result[0] = '\0';

  if (OB_ISNULL(fp = popen("rpm -q glibc | grep x86_64", "r"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to get glibc info", K(ret));
  } else {
    if (OB_ISNULL(fgets(result, OB_MAX_UNAME_INFO_LEN, fp))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to get glibc release", K(ret));
    } else if (NULL != strstr(result, ".el5")) {
      release = RELEASE_5U;
    } else if (NULL != strstr(result, ".alios5")) {
      release = RELEASE_5U;
    } else if (NULL != strstr(result, ".el6")) {
      release = RELEASE_6U;
    } else if (NULL != strstr(result, ".alios6")) {
      release = RELEASE_6U;
    } else if (NULL != strstr(result, ".el7")) {
      release = RELEASE_7U;
    } else if (NULL != strstr(result, ".alios7")) {
      release = RELEASE_7U;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown release from glibc", K(result), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succ to get_kernel_release_by_glibc", K(result),
             "release", get_kernel_release_string(release));
  }

  if (NULL != fp) {
    pclose(fp);
    fp = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::init_proxy_kernel_release()
{
  int ret = OB_SUCCESS;
  ObProxyKernelRelease uname_release = RELEASE_MAX;
  ObProxyKernelRelease redhat_release = RELEASE_MAX;
  ObProxyKernelRelease glibc_release = RELEASE_MAX;

  if (!proxy_config_.enable_strict_kernel_release) {
    kernel_release_ = RELEASE_UNKNOWN;
    LOG_INFO("succ to init_proxy_kernel_release by unknown kernel",
             "kernel_release_", get_kernel_release_string(kernel_release_),
             "enable_strict_kernel_release", proxy_config_.enable_strict_kernel_release.str());
  } else {
    if (OB_FAIL(get_kernel_release_by_uname(uname_release))) {
      LOG_WDIAG("fail to get_kernel_release_by_uname", K(ret));
    } else {
      kernel_release_ = uname_release;

      if (OB_FAIL(get_kernel_release_by_redhat(redhat_release))) {
        LOG_WDIAG("fail to get_kernel_release_by_redhat", K(ret));
      } else if ((kernel_release_ != redhat_release)
                  && OB_SUCC(get_kernel_release_by_glibc(glibc_release))
                  && (redhat_release == glibc_release)) {
        kernel_release_ = redhat_release;
      }
      LOG_INFO("succ to init_proxy_kernel_release", K(kernel_release_),
               "release", get_kernel_release_string(kernel_release_),
               K(uname_release), K(redhat_release), K(glibc_release), K(ret));
      //ignore failed
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObConfigServerProcessor::check_kernel_release(const char *release_str) const
{
  int ret = OB_SUCCESS;
  ObProxyKernelRelease provided_release = RELEASE_MAX;
  if (OB_ISNULL(release_str) || 0 == strlen(release_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("argument is null", K(release_str), K(ret));
  } else if (NULL != strstr(release_str, ".el5")) {
    provided_release = RELEASE_5U;
  } else if (NULL != strstr(release_str, ".alios5")) {
    provided_release = RELEASE_5U;
  } else if (NULL != strstr(release_str, ".el6")) {
    provided_release = RELEASE_6U;
  } else if (NULL != strstr(release_str, ".alios6")) {
    provided_release = RELEASE_6U;
  } else if (NULL != strstr(release_str, ".el7")) {
    provided_release = RELEASE_7U;
  } else if (NULL != strstr(release_str, ".alios7")) {
    provided_release = RELEASE_7U;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("failed to parser kernel release", K(release_str), K(provided_release), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (provided_release != kernel_release_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("the kernel release is not the same with current proxy kernel_release",
               K(provided_release), K(kernel_release_), K(release_str), K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::check_kernel_release(const common::ObString &release_string) const
{
  int ret = OB_SUCCESS;
  if (release_string.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("release_string is null", K(release_string), K(ret));
  } else {
    char *release_str = static_cast<char *>(op_fixed_mem_alloc(release_string.length() + 1));
    if (OB_ISNULL(release_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", "len", release_string.length() + 1, K(ret));
    } else {
      MEMCPY(release_str, release_string.ptr(), release_string.length());
      release_str[release_string.length()] = '\0';
      ret = check_kernel_release(release_str);

      op_fixed_mem_free(release_str, release_string.length() + 1);
      release_str = NULL;
    }
  }
  return ret;
}

int ObConfigServerProcessor::load_config_from_local()
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObString json;

  ObRecordHeader record_header;
  const char *payload_ptr = NULL;
  int64_t payload_size = -1;

  //used to store the decompressed JSON content
  char *json_buf = NULL;

  if (OB_FAIL(ObProxyFileUtils::get_file_size(CFG_SERVER_INFO_DUMP_NAME, buf_size))) {
    LOG_INFO("fail to get config server info file size, maybe file does not exist", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("buf size is invalid", K(buf_size), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read(CFG_SERVER_INFO_DUMP_NAME, buf, buf_size, read_len))) {
    LOG_WDIAG("fail to read config server info from file", K(ret));
  } else if (OB_FAIL(ObRecordHeader::check_record(buf, read_len, OB_PROXY_CONFIG_MAGIC))) {
    LOG_WDIAG("fail to check file header", K(ret));
  } else if (OB_FAIL(ObRecordHeader::get_record_header(buf, OB_RECORD_HEADER_LENGTH,
                                                       record_header, payload_ptr, payload_size))) {
    LOG_WDIAG("fail to get record header", K(ret));
  } else {
    LOG_DEBUG("record header", K(record_header.version_),
                               K(record_header.data_zlength_),
                               K(record_header.data_length_));

    if (HEADER_VERSION_ORIGINAL == record_header.version_) {
      json.assign_ptr(buf + OB_RECORD_HEADER_LENGTH, static_cast<int32_t>(read_len - OB_RECORD_HEADER_LENGTH));
    } else if (HEADER_VERSION_ZLIB_COMPRESS == record_header.version_) {
      ObZlibStreamCompressor compressor(6);
      int64_t filled_len = -1;

      // more alloc one byte,
      // just to decide whether decompress is end
      int64_t json_buf_length = record_header.data_length_ + 1;

      if (OB_UNLIKELY(record_header.data_zlength_ <= 0) || OB_UNLIKELY(record_header.data_length_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("the length of data or the length of the compressed data is error",
                 K(record_header.data_zlength_),
                 K(record_header.data_length_));
      } else if ((read_len - OB_RECORD_HEADER_LENGTH) != record_header.data_zlength_) {
        // defense
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("record_header.data_zlength must equal read_len - OB_RECORD_HEADER_LENGTH",
                 K(read_len), K(OB_RECORD_HEADER_LENGTH), K(record_header.data_zlength_), K(ret));
      } else if (OB_FAIL(compressor.add_decompress_data(buf + OB_RECORD_HEADER_LENGTH, record_header.data_zlength_))) {
        LOG_WDIAG("fail to add decompress data", K(record_header.data_zlength_), K(record_header.data_length_), K(ret));
      } else if (OB_ISNULL(json_buf = static_cast<char *>(ob_malloc(json_buf_length, ObModIds::OB_PROXY_FILE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc buffer for json data", K(ret));
      } else if (OB_FAIL(compressor.decompress(json_buf, json_buf_length, filled_len))) {
        LOG_WDIAG("fail to decompress", K(filled_len), K(record_header.data_zlength_),
                                       K(record_header.data_length_), K(ret));
      } else if (OB_UNLIKELY(json_buf_length <= filled_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("we have allocated sufficient memory to store the decompressed data", K(json_buf_length), K(ret));
      } else {
        json.assign_ptr(json_buf, record_header.data_length_);
      }

    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("not supported record header", K(record_header.version_), K(ret));
    }

    if (OB_SUCC(ret) && OB_FAIL(parse_json_config_info(json))) {
      LOG_WDIAG("fail to parse local json config info", K(ret));
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }

  if (NULL != json_buf) {
    ob_free(json_buf);
    json_buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::dump_json_config_to_local(char *json_info_buf, const int64_t buf_len, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObZlibStreamCompressor compressor(6);

  char *buffer = NULL;
  int64_t max_overflow_size = -1;
  int64_t compress_buf_len = -1;

  int64_t filled_len = -1;

  if (buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid buf_len", K(buf_len), K(ret));
  } else if (OB_FAIL(ObZlibStreamCompressor::get_max_overflow_size(buf_len, max_overflow_size))) {
    LOG_WDIAG("fail to get max overflow size", K(buf_len), K(ret));
  } else {
    // more alloc one byte,
    // just to decide whether compress is end
    compress_buf_len = buf_len + max_overflow_size + 1;
  }


  if (OB_SUCC(ret)) {
    if (OB_ISNULL(json_info_buf) || OB_UNLIKELY(data_len <= 0) || max_overflow_size <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid json config info buffer", K(json_info_buf), K(data_len), K(max_overflow_size), K(ret));
    } else if (OB_FAIL(compressor.add_compress_data(json_info_buf + OB_RECORD_HEADER_LENGTH, data_len, true))) {
      LOG_WDIAG("fail to add compress data", K(buf_len), K(data_len), K(ret));
    } else if (OB_ISNULL(buffer = static_cast<char *>(ob_malloc(compress_buf_len, ObModIds::OB_PROXY_FILE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc buffer for compressed data", K(ret));
    } else if (OB_FAIL(compressor.compress(buffer + OB_RECORD_HEADER_LENGTH,
                                           compress_buf_len - OB_RECORD_HEADER_LENGTH,
                                           filled_len))) {
      LOG_WDIAG("fail to compress", K(buf_len), K(compress_buf_len), K(filled_len));
    } else if (OB_UNLIKELY((compress_buf_len - OB_RECORD_HEADER_LENGTH) <= filled_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("compressed data cannot be larger than before", K(buf_len), K(data_len), K(filled_len), K(ret));
    } else if (OB_FAIL(add_serialized_file_header(buffer, compress_buf_len, filled_len, data_len))) {
      LOG_WDIAG("fail to serialize json config info", K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write(CFG_SERVER_INFO_DUMP_NAME, buffer, filled_len + OB_RECORD_HEADER_LENGTH))) {
      dump_config_res_ = false;
      LOG_WDIAG("fail to dump config server info to file", K(ret));
    } else {
      dump_config_res_ = true;
    }
  }

  if (OB_LIKELY(NULL != buffer)) {
    ob_free(buffer);
    buffer = NULL;
  }

  return ret;
}

int ObConfigServerProcessor::load_rslist_info_from_local()
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  char *buf = NULL;
  Value *rs_list = NULL;
  int64_t buf_size = 0;
  ObString json;

  if (OB_FAIL(ObProxyFileUtils::get_file_size(CFG_RSLIST_INFO_DUMP_NAME, buf_size))) {
    LOG_INFO("fail to get rslist info buf size, maybe file does not exist", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("buf size is invalid", K(buf_size), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read(CFG_RSLIST_INFO_DUMP_NAME, buf, buf_size, read_len))) {
    // no need to print log, the caller will do it
  } else if (OB_FAIL(ObRecordHeader::check_record(buf, read_len, OB_PROXY_CONFIG_MAGIC))) {
    LOG_WDIAG("fail to check file header", K(ret));
  } else if (FALSE_IT(json.assign_ptr(buf + OB_RECORD_HEADER_LENGTH, static_cast<int32_t>(read_len - OB_RECORD_HEADER_LENGTH)))) {
    // impossible
  } else {
    ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
    if (OB_FAIL(init_json(json, rs_list, json_allocator))) {
      LOG_WDIAG("fail to init json rslist", K(ret));
    } else {
      CWLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json_config_info_ is null", K(ret));
      } else if (OB_FAIL(json_config_info_->parse_local_rslist(rs_list))) {
        LOG_WDIAG("fail to parse local rs list", K(ret));
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::load_idc_list_info_from_local()
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  char *buf = NULL;
  Value *idc_list = NULL;
  int64_t buf_size = 0;
  ObString json;

  if (OB_FAIL(ObProxyFileUtils::get_file_size(CFG_IDC_LIST_INFO_DUMP_NAME, buf_size))) {
    LOG_INFO("fail to get idc list info buf size, maybe file does not exist", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("buf size is invalid", K(buf_size), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read(CFG_IDC_LIST_INFO_DUMP_NAME, buf, buf_size, read_len))) {
    // no need to print log, the caller will do it
  } else if (OB_FAIL(ObRecordHeader::check_record(buf, read_len, OB_PROXY_CONFIG_MAGIC))) {
    LOG_WDIAG("fail to check file header", K(ret));
  } else if (FALSE_IT(json.assign_ptr(buf + OB_RECORD_HEADER_LENGTH, static_cast<int32_t>(read_len - OB_RECORD_HEADER_LENGTH)))) {
    // impossible
  } else {
    ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
    if (OB_FAIL(init_json(json, idc_list, json_allocator))) {
      LOG_INFO("fail to init json idc list", K(ret));
    } else {
      CWLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json_config_info_ is null", K(ret));
      } else if (OB_FAIL(json_config_info_->parse_local_idc_list(idc_list))) {
        LOG_WDIAG("fail to parse local rs list", K(ret));
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}


int ObConfigServerProcessor::dump_rslist_info_to_local()
{
  int ret = OB_SUCCESS;
  char *rslist_buf = NULL;
  int64_t data_len = 0;
  int64_t buf_size = 0;

  {
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json_config_info_ is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_rslist_file_max_size(buf_size))) {
      LOG_WDIAG("fail to get rslist file max size", K(ret));
    } else if (OB_UNLIKELY(buf_size < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("buf_size is unexpected", K(buf_size), K(ret));
    } else {
      LOG_INFO("succ to get rslist file max size", K(buf_size));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(rslist_buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory", K(ret));
    } else {
      CRLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json_config_info_ is null", K(ret));
      } else if (OB_FAIL(json_config_info_->rslist_to_json(rslist_buf + OB_RECORD_HEADER_LENGTH,
                                                   buf_size - OB_RECORD_HEADER_LENGTH, data_len))) {
        LOG_WDIAG("fail to format json_config_info_ to string",
                 KPC(json_config_info_), K(buf_size), K(OB_RECORD_HEADER_LENGTH), K(data_len), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_serialized_file_header(rslist_buf, buf_size, data_len, data_len))) {
      LOG_WDIAG("fail to serialize rslist info", K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write(CFG_RSLIST_INFO_DUMP_NAME,
        rslist_buf, data_len + OB_RECORD_HEADER_LENGTH))) {
      LOG_WDIAG("fail to dump rslist info to file", K(ret));
      need_dump_rslist_res_ = true;
    } else {
      // Only at this place we set it false
      // As there is not lock for it, maybe can not dump other's modify this time
      need_dump_rslist_res_ = false;
    }
  }

  if (OB_LIKELY(NULL != rslist_buf)) {
    ob_free(rslist_buf);
    rslist_buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::dump_idc_list_info_to_local()
{
  int ret = OB_SUCCESS;
  char *idc_list_buf = NULL;
  int64_t data_len = 0;
  int64_t buf_size = 0;

  {
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json_config_info_ is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_idc_list_file_max_size(buf_size))) {
      LOG_WDIAG("fail to get idc list file max size", K(ret));
    } else if (OB_UNLIKELY(buf_size < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("buf_size is unexpected", K(buf_size), K(ret));
    } else {
      LOG_INFO("succ to get idc list file max size", K(buf_size));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(idc_list_buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory", K(ret));
    } else {
      CRLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json_config_info_ is null", K(ret));
      } else if (OB_FAIL(json_config_info_->idc_list_to_json(idc_list_buf + OB_RECORD_HEADER_LENGTH,
                                                   buf_size - OB_RECORD_HEADER_LENGTH, data_len))) {
        LOG_WDIAG("fail to format json_config_info_ to string",
                 KPC(json_config_info_), K(buf_size), K(OB_RECORD_HEADER_LENGTH), K(data_len), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_serialized_file_header(idc_list_buf, buf_size, data_len, data_len))) {
      LOG_WDIAG("fail to serialize idc list info", K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write(CFG_IDC_LIST_INFO_DUMP_NAME,
        idc_list_buf, data_len + OB_RECORD_HEADER_LENGTH))) {
      LOG_WDIAG("fail to dump idc list info to file", K(ret));
      need_dump_idc_list_res_ = true;
    } else {
      // Only at this place we set it false
      // As there is not lock for it, maybe can not dump other's modify this time
      need_dump_idc_list_res_ = false;
    }
  }

  if (OB_LIKELY(NULL != idc_list_buf)) {
    ob_free(idc_list_buf);
    idc_list_buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::delete_rslist(const common::ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(cluster_name), K(ret));
  } else {
    CWLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json_config_info_ is null", K(ret));
    } else if (OB_FAIL(json_config_info_->delete_cluster_rslist(cluster_name, cluster_id))) {
      LOG_WDIAG("fail to delete cluster rslist", K(cluster_name), K(cluster_id), K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::update_rslist(const common::ObString &cluster_name,
    const int64_t cluster_id,
    const LocationList &rs_list, const uint64_t cur_rs_list_hash)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty() || OB_UNLIKELY(rs_list.count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(cluster_name), K(rs_list), K(ret));
  } else {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    CWLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json_config_info_ is null", K(ret));
    } else if (OB_FAIL(json_config_info_->get_sub_cluster_info(cluster_name, cluster_id, sub_cluster_info))) {
      LOG_WDIAG("sub_cluster_info does not exist", K(cluster_name), K(cluster_id), K(ret));
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sub_cluster_info is null", K(cluster_name), K(cluster_id), K(ret));
    } else if (OB_FAIL(sub_cluster_info->update_rslist(rs_list, cur_rs_list_hash))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("rs_list is not changed, no need to update", K(ret));
      } else {
        LOG_WDIAG("fail to set cluster web rs list", K(ret));
      }
    } else {
      // It is not real time, let timer thread dump rslist
      // As there is not lock for it, maybe can not dump it this time
      need_dump_rslist_res_ = true;
    }
  }
  return ret;
}

int ObConfigServerProcessor::refresh_all_rslist()
{
  int ret = OB_SUCCESS;
  ObProxyJsonConfigInfo *json_info = NULL;
  if (OB_ISNULL(json_info = acquire())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("proxy_json_config_info is null", K(ret));
  } else {
    LocationList web_rslist;
    ObProxyClusterArrayInfo &cluster_array = const_cast<ObProxyClusterArrayInfo &>(json_info->get_cluster_array());
    ObProxyClusterArrayInfo::CIHashMap::iterator last = cluster_array.ci_map_.end();
    for (ObProxyClusterArrayInfo::CIHashMap::iterator it = cluster_array.ci_map_.begin();
         OB_SUCC(ret) && it != last; ++it) {
      for (ObProxyClusterInfo::SubCIHashMap::iterator sub_it = it->sub_ci_map_.begin();
           OB_SUCC(ret) && sub_it != it->sub_ci_map_.end(); ++sub_it) {
        web_rslist.reuse();
        if (!sub_it->web_rs_list_.empty()) {
          if (OB_FAIL(fetch_newest_cluster_rslist(it->cluster_name_, sub_it->cluster_id_, web_rslist))) {
            LOG_WDIAG("fail to fetch newest cluster rslist", K_(it->cluster_name), K_(sub_it->cluster_id), K(ret));
            ret = OB_SUCCESS;  // continue
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      web_rslist.reuse();
      if (OB_FAIL(fetch_newest_cluster_rslist(ObString::make_string(OB_META_DB_CLUSTER_NAME), OB_DEFAULT_CLUSTER_ID, web_rslist))) {
        LOG_WDIAG("fail to fetch newest meta dabase rslist", K(ret));
      }
    }
  }
  release(json_info);

  return ret;
}

int ObConfigServerProcessor::refresh_all_idc_list()
{
  int ret = OB_SUCCESS;
  ObProxyJsonConfigInfo *json_info = NULL;
  if (OB_ISNULL(json_info = acquire())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("proxy_json_config_info is null", K(ret));
  } else {
    ObProxyIDCList idc_list;
    ObProxyClusterArrayInfo &cluster_array = const_cast<ObProxyClusterArrayInfo &>(json_info->get_cluster_array());
    ObProxyClusterArrayInfo::CIHashMap::iterator last = cluster_array.ci_map_.end();
    for (ObProxyClusterArrayInfo::CIHashMap::iterator it = cluster_array.ci_map_.begin();
         OB_SUCC(ret) && it != last; ++it) {
      for (ObProxyClusterInfo::SubCIHashMap::iterator sub_it = it->sub_ci_map_.begin();
           OB_SUCC(ret) && sub_it != it->sub_ci_map_.end(); ++sub_it) {
        idc_list.reuse();
        if (!sub_it->web_rs_list_.empty()) {
          if (OB_FAIL(refresh_idc_list(it->cluster_name_, sub_it->cluster_id_, idc_list))) {
            LOG_INFO("fail to refresh idc list, continue",
                     "cluster_name", it->cluster_name_, "cluster_id", sub_it->cluster_id_, K(ret));
            ret = OB_SUCCESS;  // continue
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      idc_list.reuse();
      if (OB_FAIL(refresh_idc_list(ObString::make_string(OB_META_DB_CLUSTER_NAME), OB_DEFAULT_CLUSTER_ID, idc_list))) {
        LOG_INFO("fail to fetch newest meta dabase idc list", K(ret));
      }
    }
  }
  release(json_info);

  if (need_dump_idc_list_res_) {
    if (FALSE_IT(dump_idc_list_info_to_local())) {
      //impossible
    }
  }
  return ret;
}
int ObConfigServerProcessor::refresh_json_config_info(const bool force_refresh /*false*/)
{
  int ret = OB_SUCCESS;
  char *url = NULL;
  int64_t url_len = 0;
  int64_t pos = 0;
  static const ObString APPEND_VERSION("&VersionOnly=true");

  {
    CRLockGuard guard(get_global_proxy_config().rwlock_);
    url_len = STRLEN(proxy_config_.obproxy_config_server_url.str()) + APPEND_VERSION.length() + 1; // make sure end with '\0'
  }
  if (OB_UNLIKELY(url_len <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("obproxy_config_server_url is invalid", K(url_len), K(ret));
  } else if (OB_ISNULL(url = static_cast<char *>(op_fixed_mem_alloc(url_len)))) {
    ret =  OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc mem for config server url", K(ret));
  } else {
    CRLockGuard guard(get_global_proxy_config().rwlock_);
    if (OB_FAIL(databuff_printf(url, url_len, pos, "%s%.*s",
                                proxy_config_.obproxy_config_server_url.str(),
                                APPEND_VERSION.length(), APPEND_VERSION.ptr()))) {
      LOG_WDIAG("fail to build config server url", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const bool version_only = true;
    if (OB_FAIL(get_json_config_info(url, version_only))) {
      if (OB_EAGAIN != ret) {
        LOG_WDIAG("fail to get json config version", K(ret));
      }
    }
    if (OB_SUCC(ret) || (force_refresh && OB_EAGAIN == ret)) {
      // cut APPEND_VERSION
      url[pos - APPEND_VERSION.length()] = '\0';
      if (OB_FAIL(get_json_config_info(url))) {
        LOG_WDIAG("fail to get_json_config_info", K(ret));
      }
    }
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_LIKELY(NULL != url)) {
    op_fixed_mem_free(url, url_len);
    url = NULL;
  }

  if (need_dump_rslist_res_) {
    if (FALSE_IT(dump_rslist_info_to_local())) {
      //impossible
    }
  }
  if (need_dump_idc_list_res_) {
    if (FALSE_IT(dump_idc_list_info_to_local())) {
      //impossible
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_json_config_info(const char *url, const bool version_only /*false*/)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObString json;
  int64_t buf_size = 0;
  static int64_t buf_count = 1; // one buf is 64K, max count is 16

  if (OB_ISNULL(url)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid url", K(url), K(ret));
  } else {
    // we should reserve some bytes space for the next step to add a record header
    // for json config info file, so we should set a offset for the buf before fetching origin json config info
    // and that offset should be a record header length OB_RECORD_HEADER_LENGTH
    int64_t try_attempts = 0;
    do {
      buf_size = buf_count * OBPROXY_MAX_JSON_INFO_SIZE;
      if (OB_UNLIKELY(NULL != buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("buf should be null before memory allocated", K(buf), K(ret));
      } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
        ret =  OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc memory", K(ret));
      } else if (FALSE_IT(json.assign_buffer(buf + OB_RECORD_HEADER_LENGTH,
                 static_cast<int32_t>(buf_size - OB_RECORD_HEADER_LENGTH)))) {
        // impossible
      } else if (OB_FAIL(do_fetch_json_info(url, json))) {
        if (OB_SIZE_OVERFLOW == ret) {
          // double buf size
          buf_count = buf_count << 1L;
          if (OB_LIKELY(NULL != buf)) {
            ob_free(buf);
            buf = NULL;
          }
        }
        LOG_WDIAG("fail to fetch json info", "try_attempts", try_attempts, K(buf_count), K(ret));
      }
      ++try_attempts;
    } while (OB_SIZE_OVERFLOW == ret && try_attempts < 4);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(parse_json_config_info(json, version_only))) {
        if (OB_EAGAIN != ret) {
          LOG_WDIAG("fail to parse json config info", K(version_only), K(json), K(ret));
        }
      } else if (!version_only) {
        if (FALSE_IT(dump_json_config_to_local(buf, buf_size, json.length()))) {
          //impossible
        }
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::parse_json_config_info(const ObString &json, const bool version_only /*false*/)
{
  int ret = OB_SUCCESS;
  Value *root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);

  if (OB_FAIL(init_json(json, root, allocator))) {
    LOG_WDIAG("fail to convert config string to json format", K(ret));
  } else if (version_only) {
    CRLockGuard lock(json_info_lock_);
    if (OB_FAIL(ObProxyDataInfo::parse_version(root, json_config_info_->get_data_version()))) {
      if (OB_EAGAIN != ret) {
        LOG_WDIAG("fail to parse json config version", K(ret));
      }
    }
  } else {
    ObProxyJsonConfigInfo *json_info = NULL;
    bool is_metadb_changed = false;
    if (OB_ISNULL(json_info = op_alloc(ObProxyJsonConfigInfo))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc mem fot json config info", K(ret));
    } else if (OB_FAIL(json_info->parse(root))) {
      LOG_WDIAG("fail to parse json info", K(ret));
    } else if (!json_info->is_valid()) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("new json info is invalid", K(*json_info), K(ret));
    } else {
      CRLockGuard lock(json_info_lock_);
      if (json_config_info_->is_meta_db_changed(*json_info)) {
        is_metadb_changed = true;
      }
      // 新增获取service name的GetTenantInfoUrl值
      // 这里不用将json_info中新的信息，再添加到json_config_info_中。下面的swap_with_rslist会销毁旧指针
    }
    if (OB_SUCC(ret)) {
      // update first cluster name in resource pool, ignore ret
      if (OB_FAIL(get_global_resource_pool_processor().set_first_cluster_name(
              json_info->get_cluster_array().default_cluster_name_))) {
        LOG_WDIAG("fail to update resource pool default cluster name", K(ret));
      }
      if (OB_FAIL(swap_with_rslist(json_info, is_metadb_changed))) {
        LOG_WDIAG("fail to update json config info", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_LIKELY(NULL != json_info)) {
      op_free(json_info);
      json_info = NULL;
    }
    if (OB_SUCC(ret) && is_metadb_changed) {
      if (OB_FAIL(get_global_resource_pool_processor().add_cluster_delete_task(
                  ObString::make_string(OB_META_DB_CLUSTER_NAME), OB_DEFAULT_CLUSTER_ID))) {
        LOG_WDIAG("fail to add rebuild metabd task", K(ret));
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObConfigServerProcessor::fetch_rs_list_from_url(const char *url, const ObString &appname, const int64_t cluster_id,
    LocationList &web_rslist, const bool need_update_dummy_entry /*true*/)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObString json;
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(OB_PROXY_CONFIG_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc memory for rs list json info", K(ret));
  } else if (FALSE_IT(json.assign_buffer(buf, static_cast<int32_t>(OB_PROXY_CONFIG_BUFFER_SIZE)))) {
    // impossible
  } else if (OB_FAIL(do_fetch_json_info(url, json))) {
    LOG_WDIAG("fail to fetch rslist json info", K(json), K(ret));
  } else {
    Value *root = NULL;
    ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
    if (OB_FAIL(init_json(json, root, allocator))) {
      LOG_WDIAG("fail to init json root for rslist", K(ret));
    } else {
      CWLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json_config_info_ is null", K(ret));
      } else if (OB_FAIL(json_config_info_->parse_remote_rslist(root, appname, cluster_id, web_rslist, need_update_dummy_entry)) && OB_EAGAIN != ret) {
        LOG_WDIAG("fail to parse remote rslist", K(root), K(appname), K(cluster_id), K(ret));
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    op_fixed_mem_free(buf, OB_PROXY_CONFIG_BUFFER_SIZE);
    buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::swap_origin_web_rslist_and_build_sys(const ObString &cluster, const int64_t cluster_id, const bool need_save_rslist_hash)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(json_config_info_->swap_origin_web_rslist_and_build_sys(cluster, cluster_id, need_save_rslist_hash))) {
    LOG_WDIAG("fail to parse remote rslist", K(cluster), K(cluster_id), K(ret));
  }

  return ret;
}

int ObConfigServerProcessor::refresh_idc_list_from_url(const char *url,
    const ObString &cluster_name, const int64_t cluster_id, ObProxyIDCList &idc_list)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObString json;
  ObString cluster_name_from_url;
  int64_t cluster_id_from_url = OB_DEFAULT_CLUSTER_ID;
  bool is_cluster_alias = is_cluster_name_alias(cluster_name);
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(OB_PROXY_CONFIG_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc memory for region idc json info", K(ret));
  } else if (FALSE_IT(json.assign_buffer(buf, static_cast<int32_t>(OB_PROXY_CONFIG_BUFFER_SIZE)))) {
    // impossible
  } else if (OB_FAIL(do_fetch_json_info(url, json, CURL_IDC_TRANSFER_TIMEOUT))) {
    LOG_INFO("fail to fetch region idc json info", K(json), K(ret));
  } else {
    Value *root = NULL;
    ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
    if (OB_FAIL(init_json(json, root, allocator))) {
      LOG_INFO("fail to init json root for idc list", K(ret));
    } else if (OB_FAIL(ObProxyJsonConfigInfo::parse_remote_idc_list(root, cluster_name_from_url, cluster_id_from_url, idc_list))) {
      LOG_WDIAG("fail to parse remote idc list", K(root), K(ret));
    } 
  }
  if (OB_SUCC(ret)){
    if (!is_cluster_alias) {
      if (cluster_name != OB_META_DB_CLUSTER_NAME && cluster_name_from_url != cluster_name) {
        ret = OB_OBCONFIG_APPNAME_MISMATCH;
        LOG_WDIAG("obconfig cluster name mismatch", K(cluster_name), K(cluster_name_from_url), K(ret));
      }
    } else {
      ObString real_cluster_name;
      if (OB_FAIL(get_real_cluster_name(real_cluster_name, cluster_name))) {
        LOG_WDIAG("fail to get real cluster name", K(cluster_name), K(ret));
      } else if (real_cluster_name != cluster_name) {
        ret = OB_OBCONFIG_APPNAME_MISMATCH;
        LOG_WDIAG("obconfig cluster name mismatch", K(cluster_name), K(real_cluster_name), K(cluster_name_from_url), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (cluster_id != OB_DEFAULT_CLUSTER_ID && cluster_id != cluster_id_from_url) {
      ret = OB_OBCONFIG_APPNAME_MISMATCH;
      LOG_WDIAG("obconfig cluster id mismatch", K(cluster_id), K(cluster_id_from_url), K(ret));
    } else {
      LOG_DEBUG("succ to get idc list", K(cluster_name), K(cluster_name_from_url),
                K(cluster_id), K(cluster_id_from_url), K(idc_list), K(ret));
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    op_fixed_mem_free(buf, OB_PROXY_CONFIG_BUFFER_SIZE);
    buf = NULL;
  }

  if (OB_SUCC(ret)) {
    CWLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json_config_info_ is null", K(ret));
    } else if (OB_FAIL(json_config_info_->set_idc_list(cluster_name, cluster_id, idc_list))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("idc_list is not changed, no need to update", K(cluster_name), K(cluster_id), K(ret));
      } else {
        LOG_WDIAG("fail to update_idc_list", K(cluster_name), K(cluster_id), K(idc_list), K(ret));
      }
    } else {
      need_dump_idc_list_res_ = true;
    }
  }
  return ret;
}

int ObConfigServerProcessor::do_fetch_json_info(const char *url, ObString &json, int64_t timeout/*CURL_TRANSFER_TIMEOUT*/, const char* data/*NULL*/)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(json.size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid buffer", "json", get_print_json(json), K(ret));
  } else {
    int64_t fetch_attempts = 0;
    do {
      if (OB_FAIL(fetch_by_curl(url, timeout, static_cast<void*>(&json), write_data, data))) {
        LOG_WDIAG("fail to fetch json info", "try attempts:", fetch_attempts, K(url), K(is_inited_), K(data), K(ret));
      }
      ++fetch_attempts;
    } while (OB_FAIL(ret) && OB_SIZE_OVERFLOW != ret && !is_inited_ && fetch_attempts < 3);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_content_string(json.ptr(), json.length()))) {
        LOG_WDIAG("fail to handle content", "json", get_print_json(json), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to fetch json info", K(url), "json", get_print_json(json));
  }
  return ret;
}

int ObConfigServerProcessor::handle_content_string(char *content, const int64_t content_length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(content) || OB_UNLIKELY(content_length <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("empty json content", K(content), K(ret));
  } else {
    for (int64_t i = 0; i < content_length; ++i) {
      if (isspace(content[i])) {
        content[i] = ' ';
      }
    }
  }
  return ret;
}

int ObConfigServerProcessor::init_json(const ObString &json_str, Value *&json_root, ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  json_root = NULL;
  Parser parser;
  if (OB_FAIL(parser.init(&allocator))) {
    LOG_WDIAG("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json_str.ptr(), json_str.length(), json_root))) {
    LOG_INFO("parse json failed", K(ret), "json_str", get_print_json(json_str));
  } else { }
  return ret;
}

int ObConfigServerProcessor::convert_root_addr_to_addr(const LocationList &web_rs_list, ObIArray<ObAddr> &rs_list, ObIArray<ObAddr> &rpc_rs_list) const
{
  int ret = OB_SUCCESS;
  if (web_rs_list.empty()) {
    ret = OB_INVALID_CONFIG;
    LOG_INFO("empty web rs list", K(web_rs_list), K(ret));
  } else {
    //convert ObRootAddrList to rs_list
    ObAddr addr;
    ObAddr rpc_addr;
    for (int64_t i = 0; OB_SUCC(ret) && i < web_rs_list.count(); ++i) {
      addr.reset();
      rpc_addr.reset();
      addr = web_rs_list[i].server_;
      rpc_addr = web_rs_list[i].rpc_server_;
      if (OB_FAIL(rs_list.push_back(addr))) {
        LOG_WDIAG("fail to push addr to rs list", K(addr), K(ret));
      } else if (OB_FAIL(rpc_rs_list.push_back(rpc_addr))) {
        LOG_WDIAG("fail to push rpc addr to rs list", K(rpc_addr), K(ret));
      }
    }
  }
  return ret;
}

int ObConfigServerProcessor::build_proxy_bin_url(const char *binary_name, ObIAllocator &allocator, char *&bin_url)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  static const ObString APPEND_HEADER("&Version=");
  bin_url = NULL;

  if (OB_ISNULL(binary_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid bin url buf", K(binary_name), K(ret));
  } else {
    //1、get the bin root catalogur length from json_config_info_
    CRLockGuard lock(json_info_lock_);
    if (OB_ISNULL(json_config_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json_config_info_ is null", K(ret));
    } else {
      len = json_config_info_->get_bin_url().length() + 1 + STRLEN(binary_name) + APPEND_HEADER.length();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(bin_url = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc mem for bin url", K(ret));
    } else {
      CRLockGuard lock(json_info_lock_);
      if (OB_ISNULL(json_config_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json_config_info_ is null", K(ret));
      } else if (OB_FAIL(json_config_info_->copy_bin_url(bin_url, len))) {
        LOG_WDIAG("fail to get bin url root catalogue", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t pos = STRLEN(bin_url);
    if (OB_FAIL(check_kernel_release(binary_name))) {
      LOG_WDIAG("failed to parser linux kernel release in new_proxy_bin_version, maybe not supported",
               K(binary_name), K_(kernel_release), K(ret));
    } else if (OB_FAIL(databuff_printf(bin_url, len, pos, "%.*s%s",
                                       APPEND_HEADER.length(), APPEND_HEADER.ptr(), binary_name))) {
      LOG_WDIAG("fail to fill full proxy bin url", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to get proxy bin url", K(bin_url), K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::concat_cluster_name_array(char *data, const char *cluster_name_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data) || OB_ISNULL(cluster_name_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("NULL pointer, invalid curl or url", K(ret));
  } else {
    char tail[] = "\"}";
    int64_t len_data = strlen(data);
    int64_t len_cluster_name = strlen(cluster_name_array);
    int64_t len_tail = strlen(tail);
    int64_t len_append = std::max<int64_t>(std::min<int64_t>(LDG_MAX_URL_LENGTH - len_data - 1, len_cluster_name), 0);
    MEMCPY(data + len_data, cluster_name_array, len_append);
    len_data += len_append;
    len_append = std::max<int64_t>(std::min<int64_t>(LDG_MAX_URL_LENGTH - len_data - 1, len_tail), 0);
    MEMCPY(data + len_data, tail, len_append);
    len_data += len_append;
    data[len_data++] = '\0';
    LOG_DEBUG("succ to concate ldg url data", K(data), K(cluster_name_array));
  }
  return ret;
}

int ObConfigServerProcessor::fetch_by_curl(const char *url, int64_t timeout, void *content, write_func write_func_callback /*NULL*/, const char *data/*NULL*/)
{
  int ret = OB_SUCCESS;
  CURL *curl = NULL;
  if (OB_ISNULL(url) || OB_ISNULL(content) || OB_UNLIKELY(timeout <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("NULL pointer, invalid fetch by curl", K(url), K(content), K(ret));
  } else if (OB_ISNULL(curl = curl_easy_init())) {
    ret = OB_CURL_ERROR;
    LOG_WDIAG("init curl failed", K(ret));
  } else {
    CURLcode cc = CURLE_OK;
    int64_t http_code = 0;
    //set curl options
    if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_URL, url))) {
      LOG_WDIAG("set url failed", K(cc), "url", url);
    } else {
      struct curl_slist* headerList = NULL;
      if (OB_NOT_NULL(data)) {
        // set header
        LOG_DEBUG("ldg url data, ", K(data));
        headerList = curl_slist_append(headerList, "Content-Type: application/json");
        // 添加--request GET参数
        if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET"))) {
          LOG_WDIAG("set --request GET failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerList))) { // 添加--header 'Content-Type: application/json' 参数
          LOG_WDIAG("set --header Content failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data))) {       // 添加--data '{ 
          LOG_WDIAG("set --data Content failed", K(cc));
        }
      }
      if (CURLE_OK == cc) {
        if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L))) {
          LOG_WDIAG("set no signal failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L))) {
          LOG_WDIAG("set tcp_nodelay failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 3))) {//set max redirect
          LOG_WDIAG("set max redirect failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1))) {//for http redirect 301 302
          LOG_WDIAG("set follow location failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, CURL_CONNECTION_TIMEOUT))) {
          LOG_WDIAG("set connect timeout failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout))) {
          LOG_WDIAG("set transfer timeout failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_func_callback))) {
          LOG_WDIAG("set write callback failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, content))) {
          LOG_WDIAG("set write data failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_perform(curl))) {
          LOG_WDIAG("curl easy perform failed", K(cc));
        } else if (CURLE_OK != (cc = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code))) {
          LOG_WDIAG("curl getinfo failed", K(cc));
        } else {
          // http status code 2xx means success
          if (http_code / 100 != 2) {
            ret = OB_CURL_ERROR;
            LOG_WDIAG("unexpected http status code", K(http_code), K(content), K(url), K(ret));
          }
        }

      }
      if (OB_NOT_NULL(headerList)) {
        curl_slist_free_all(headerList);    // 清理header，防止内存泄漏
      }
    }

    if (CURLE_OK != cc) {
      if (CURLE_WRITE_ERROR == cc) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        ret = OB_CURL_ERROR;
      }
      LOG_WDIAG("curl error", "curl_error_code", cc, "curl_error_message",
          curl_easy_strerror(cc), K(ret), K(url));
    }
    curl_easy_cleanup(curl);
  }
  return ret;
}

int64_t ObConfigServerProcessor::write_proxy_bin(void *ptr, int64_t size,
                                                 int64_t nmemb, void *stream)
{
  int ret = OB_SUCCESS;
  int64_t real_size = 0;

  if (OB_ISNULL(stream) || OB_ISNULL(ptr) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(stream), K(ptr), K(size), K(ret));
  } else {
    real_size = size * nmemb;
    if (real_size > 0) {
      int fd = static_cast<int>(reinterpret_cast<int64_t>(stream));
      if (real_size != unintr_write(fd, static_cast<const char *>(ptr), real_size)) {
        ret = OB_IO_ERROR;
        LOG_WDIAG("write proxy bin error", K(ret));
      }
    }
  }
  return OB_SUCCESS == ret ? real_size : 0;
}

int64_t ObConfigServerProcessor::write_data(void *ptr, int64_t size,
                                            int64_t nmemb, void *stream)
{
  int ret = OB_SUCCESS;
  int64_t real_size = 0;
  ObString *content = NULL;

  if (OB_ISNULL(stream) || OB_ISNULL(ptr) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(stream), K(ptr), K(size), K(ret));
  } else {
    real_size = size * nmemb;
    if (real_size > 0) {
      content = static_cast<ObString *>(stream);
      if (real_size + content->length() > content->size()) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WDIAG("unexpected long content",
            "new_byte", real_size,
            "recved_byte", content->length(),
            "content_size", content->size(),
            K(ret));
      } else if (content->write(static_cast<const char *>(ptr), static_cast<int32_t>(real_size)) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("append data failed", K(ret));
      }
    }
  }
  return OB_SUCCESS == ret ? real_size : 0;
}

int ObConfigServerProcessor::add_serialized_file_header(char *buf, const int64_t buf_len,
                                                        const int64_t compressed_data_len, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  int64_t start = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len + OB_RECORD_HEADER_LENGTH > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WDIAG("invalid buffer", K(buf), K(buf_len), K(data_len), K(ret));
  } else {
    ObRecordHeader header;
    header.magic_ = OB_PROXY_CONFIG_MAGIC;
    header.header_length_ = static_cast<int16_t>(OB_RECORD_HEADER_LENGTH);
    header.version_ = HEADER_VERSION;
    header.data_length_ = static_cast<int32_t>(data_len);
    header.data_zlength_ = static_cast<int32_t>(compressed_data_len);
    header.data_checksum_ = ob_crc64(buf + OB_RECORD_HEADER_LENGTH, header.data_zlength_);
    header.set_header_checksum();

    if (OB_FAIL(header.serialize(buf, buf_len, start))) {
      LOG_WDIAG("fail to serialize record header", K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::set_refresh_interval()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("config server processor is not inited", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(refresh_cont_))) {
    LOG_WDIAG("fail to set config server refresh interval", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(refresh_ldg_cont_))) {
    LOG_WDIAG("fail to set ldg refresh interval", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(refresh_service_name_cont_))) {
    LOG_WDIAG("fail to set service name refresh interval", K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::start_refresh_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("config server is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL != refresh_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("refresh cont should be null here", K_(refresh_cont), K(ret));
  } else if (OB_UNLIKELY(NULL != refresh_ldg_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ldg refresh cont should be null here", K_(refresh_ldg_cont), K(ret));
  } else if (OB_UNLIKELY(NULL != refresh_service_name_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("service name refresh cont should be null here", K_(refresh_service_name_cont), K(ret));
  } else {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().config_server_refresh_interval);
    if (OB_ISNULL(refresh_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                  "config_server_refresh_task",
                                  ObConfigServerProcessor::do_repeat_task,
                                  ObConfigServerProcessor::update_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to create and start config server refresh task", K(interval_us));
    } else {
      LOG_INFO("succ to create and start config server refresh task", K(interval_us));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(refresh_ldg_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(
              get_global_proxy_config().ldg_info_refresh_interval.get(),
              "ldg_refresh_task",
              ObConfigServerProcessor::do_ldg_repeat_task,
              ObConfigServerProcessor::update_ldg_interval))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to create and start ldg refresh task");
      } else {
        LOG_INFO("succ to create and start ldg refresh task");
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(refresh_service_name_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(
              get_global_proxy_config().config_server_refresh_interval.get(),
              "service_name_refresh_task",
              ObConfigServerProcessor::do_service_name_repeat_task,
              ObConfigServerProcessor::update_service_name_interval))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to create and start service name refresh task");
      } else {
        LOG_INFO("succ to create and start service name refresh task");
      }
    }

  }
  return ret;
}

int ObConfigServerProcessor::do_repeat_task()
{
  return get_global_config_server_processor().refresh_config_server();
}

int ObConfigServerProcessor::do_ldg_repeat_task()
{
  int ret = OB_SUCCESS;
  if (get_global_proxy_config().enable_ldg && OB_FAIL(get_global_config_server_processor().refresh_ldg_config_info())) {
    LOG_WDIAG("ldg refresh ldg config info failed", K(ret));
  }
  return ret;
}

int ObConfigServerProcessor::do_service_name_repeat_task()
{
  int ret = OB_SUCCESS;
  if (get_global_proxy_config().enable_standby && OB_FAIL(get_global_config_server_processor().refresh_service_name_info())) {
    LOG_WDIAG("refresh service name info failed", K(ret));
  }
  return ret;
}

void ObConfigServerProcessor::update_interval()
{
  ObAsyncCommonTask *cont = get_global_config_server_processor().get_refresh_cont();
  if (OB_LIKELY(NULL != cont)) {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().config_server_refresh_interval);
    cont->set_interval(interval_us);
  }
}

void ObConfigServerProcessor::update_ldg_interval()
{
  ObAsyncCommonTask *ldg_cont = get_global_config_server_processor().get_refresh_ldg_cont();
  if (OB_LIKELY(NULL != ldg_cont)) {
    ldg_cont->set_interval(get_global_proxy_config().ldg_info_refresh_interval.get());
  }
}

void ObConfigServerProcessor::update_service_name_interval()
{
  ObAsyncCommonTask *service_name_cont = get_global_config_server_processor().get_refresh_service_name_cont();
  if (OB_LIKELY(NULL != service_name_cont)) {
    service_name_cont->set_interval(get_global_proxy_config().config_server_refresh_interval.get());
  }
}

int ObConfigServerProcessor::refresh_config_server()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("config server is not inited", K(ret));
  } else {
    bool force_refresh = proxy_config_.refresh_json_config;
    if (OB_FAIL(refresh_json_config_info(force_refresh))) {
      LOG_WDIAG("fail to do refresh json config info", K(force_refresh), K(ret));
    } else if (force_refresh) {
      proxy_config_.refresh_json_config = false;
    }

    if (proxy_config_.refresh_rslist) {
      if (OB_FAIL(refresh_all_rslist())) {
        LOG_WDIAG("fail to refresh all cluster rslist", K(ret));
      } else {
        proxy_config_.refresh_rslist = false;
      }
    }

    if (proxy_config_.refresh_idc_list) {
      if (OB_FAIL(refresh_all_idc_list())) {
        LOG_WDIAG("fail to refresh all cluster idc list", K(ret));
      } else {
        proxy_config_.refresh_idc_list = false;
      }
    }

    if (OB_FAIL(refresh_binlog_server_host_ip_map())) {
      LOG_WDIAG("fail to refresh binlog server host id map");
    } else {
      LOG_DEBUG("succ to refresh binlog server host id map");
    }
  }
  return ret;
}

int ObConfigServerProcessor::request_ldg_cluster_info(ObProxyLdgInfo *ldg_info, const char *ldg_url, const char* cluster_name_array, ObIArray<LdgClusterVersionPair> &cluster_info_array, ObIArray<LdgClusterVersionPair>& change_cluster_info_array) {
  int ret = OB_SUCCESS;
  ObString json;
  char *buf = NULL;

  // 把json解析结果，读到buffer，buffer最大=8*64K=512K
  int64_t try_attempts = 0;
  int64_t buf_count = 1;
  int64_t buf_size = 0;
  char data[LDG_MAX_URL_LENGTH] = "{\"LdgClusters\": \"";
  if (OB_FAIL(concat_cluster_name_array(data, cluster_name_array))) {
    LOG_WDIAG("fail to concat cluster name", K(data), K(cluster_name_array), K(ret));
  } else {
    do {
      buf_size = buf_count * OBPROXY_MAX_JSON_INFO_SIZE;
      if (OB_UNLIKELY(NULL != buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("buf should be null before memory allocated", K(buf), K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(op_fixed_mem_alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc memory for ldg json info", K(ret));
      } else if (FALSE_IT(json.assign_buffer(buf, static_cast<int32_t>(buf_size)))) {
        // impossible
      } else if (OB_FAIL(do_fetch_json_info(ldg_url, json, CURL_TRANSFER_TIMEOUT, data))) {
        if (OB_SIZE_OVERFLOW == ret) {
          // double buf size
          buf_count = buf_count << 1L;
          if (OB_LIKELY(NULL != buf)) {
            op_fixed_mem_free(buf, buf_size);
            buf = NULL;
          }
        }
        LOG_WDIAG("fail to fetch ldg info", "try_attempts", try_attempts,
          K(buf_count), K(ret));
      }
      ++try_attempts;
    } while (OB_SIZE_OVERFLOW == ret && try_attempts < 4);
  }

  // parse json to ldg_info
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  Value *root = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_json(json, root, allocator))) {
      LOG_WDIAG("fail to init json root for ldg", K(ret));
    } else if (OB_FAIL(ldg_info->parse(root, cluster_info_array, change_cluster_info_array))) {
      LOG_WDIAG("fail to parse ldg info", K(ret));
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    op_fixed_mem_free(buf, buf_size);
    buf = NULL;
  }
  return ret;
}

int ObConfigServerProcessor::copy_cluster_info(ObIArray<LdgClusterVersionPair>& cluster_info_array, const ObString &login_cluster_name)
{
  int ret = OB_SUCCESS;
  // 如果login_cluster_name!=null，则是首次登录，只拉取其登录的集群名
  // 否则login_cluster_name==null，拉取全局Hashset存储的集群名
  if (login_cluster_name.empty()) {
    DRWLock::RDLockGuard lock(ldg_info_lock_);
    LdgHashMap& ldg_cluster_hash = get_global_config_server_processor().ldg_cluster_hash_;
    LdgHashMap::iterator last = ldg_cluster_hash.end();
    for (LdgHashMap::iterator iter = ldg_cluster_hash.begin(); iter != last; ++iter) {
      LdgClusterVersionPair tmp_cluster_info(iter->first, iter->second);
      if (OB_FAIL(cluster_info_array.push_back(tmp_cluster_info))) {
        LOG_WDIAG("fail to push back cluster name and version", K(login_cluster_name), K(iter->second), K(ret));
      }
    }
  } else {
    ObProxyConfigString cluster_name;
    ObProxyConfigString cluster_version;
    cluster_name.set_value(login_cluster_name);
    LdgClusterVersionPair tmp_cluster_info(cluster_name, cluster_version);
    if (OB_FAIL(cluster_info_array.push_back(tmp_cluster_info))) {
      LOG_WDIAG("fail to insert cluster name and version to HashMap when login", K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::update_cluster_version(ObIArray<LdgClusterVersionPair>& change_cluster_info_array)
{
  int ret = OB_SUCCESS;

  LdgHashMap& ldg_cluster_hash = get_global_config_server_processor().ldg_cluster_hash_;
  // 外面需要加写锁，会修改全局对象
  // change_cluster_info_array::parse中申请，覆盖对应全局变量的version
  // 全局变量被替换的旧version，内存需要释放
  for (int64_t i = 0; OB_SUCC(ret) && i < change_cluster_info_array.count(); ++i) {
    LdgClusterVersionPair& cluster_info = change_cluster_info_array.at(i);
    if (OB_FAIL(ldg_cluster_hash.set_refactored(cluster_info.first, cluster_info.second, 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to insert cluster name and version to HashMap", K(cluster_info.first), K(cluster_info.second), K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::refresh_ldg_config_info(const ObString &login_cluster_name/*NULL*/)
{
  int ret = OB_SUCCESS;
  char ldg_url[256];
  char *url = NULL;
  int64_t url_len = 0;
  const char *pos = NULL;
  int64_t url_key_len = static_cast<int64_t>(strlen(CONFIG_URL_KEY_STRING_2));
  {
    CRLockGuard guard(get_global_proxy_config().rwlock_);
    url_len = STRLEN(proxy_config_.obproxy_config_server_url.str());
    if (OB_UNLIKELY(url_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("obproxy config server url is invalid", K(url_len), K(ret));
    } else if (OB_ISNULL(url = static_cast<char *>(op_fixed_mem_alloc(url_len + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc mem for config server url", K(ret));
    } else {
      if (0 >= snprintf(url, url_len + 1, "%s", proxy_config_.obproxy_config_server_url.str())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to build config server url", K(ret));
      }
    }
  }
  ObSEArray<LdgClusterVersionPair, 10> cluster_info_array;           // (cluster_name, cluster_version)
  ObSEArray<LdgClusterVersionPair, 10> change_cluster_info_array;    // 版本号改变的cluster
  if (OB_FAIL(copy_cluster_info(cluster_info_array, login_cluster_name))) {
    LOG_WDIAG("fail to copy cluster name and version for HashMap", K(ret));
  }

  if (NULL == url || OB_FAIL(ret)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("argument is invalid", K(ret), K(url));
  } else if ((pos = strcasestr(url, CONFIG_URL_KEY_STRING_2)) == NULL) {
    if ((pos = strcasestr(url, CONFIG_URL_KEY_STRING_1)) == NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("cannot find GetObProxyConfig in url", K(ret), K(url));
    } else {
      url_key_len = static_cast<int64_t>(strlen(CONFIG_URL_KEY_STRING_1));
    }
  }
  // 向ldg_url添加新接口参数
  if (OB_SUCC(ret)) {
    int64_t len_ldg_url = 0;
    const int64_t url_head_len = static_cast<int64_t>(pos - url);
    const int64_t url_tailer_len = url_len - url_head_len - url_key_len;
    MEMCPY(ldg_url + len_ldg_url, url, url_head_len);
    len_ldg_url += url_head_len;
    MEMCPY(ldg_url + len_ldg_url, LDG_INSTANCE_INFO_BATCH_STRING.ptr(), LDG_INSTANCE_INFO_BATCH_STRING.length());
    len_ldg_url += LDG_INSTANCE_INFO_BATCH_STRING.length();
    MEMCPY(ldg_url + len_ldg_url, url + url_head_len + url_key_len, url_tailer_len);
    len_ldg_url += url_tailer_len;
    ldg_url[len_ldg_url] = '\0';
    ObProxyLdgInfo *ldg_info = NULL;
    if (OB_ISNULL(ldg_info = op_alloc(ObProxyLdgInfo))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory for ldg info", K(ret));
    } else {
      char cluster_name_url[LDG_MAX_URL_LENGTH] = {};    // 应该设置为LDG_BATCH_SIZE个集群的最大长度
      int64_t len_cluster_name_url = 0;
      int64_t len_array = cluster_info_array.count();
      // 每组LDG_BATCH_SIZE（默认10）个集群，分批发送请求
      for (int64_t i = 1; i <= len_array; i++) {
        if ((i - 1) % LDG_BATCH_SIZE != 0) {
          cluster_name_url[len_cluster_name_url++] = ',';
        }
        ObString& cur_cluster_name_url = const_cast<ObString&>(cluster_info_array[i - 1].first.config_string_);
        int64_t len_cur_cluster_name = std::max<int64_t>(std::min<int64_t>(LDG_MAX_URL_LENGTH - len_cluster_name_url - 1, cur_cluster_name_url.length()), 0);
        MEMCPY(cluster_name_url + len_cluster_name_url, cur_cluster_name_url.ptr(), len_cur_cluster_name);
        len_cluster_name_url += len_cur_cluster_name;
        if (i % LDG_BATCH_SIZE == 0 || (i == len_array)) {
          // 拼接集群名，发送请求
          cluster_name_url[len_cluster_name_url++] = '\0';
          LOG_DEBUG("concat cluster = ", K(cluster_name_url));
          if (OB_FAIL(request_ldg_cluster_info(ldg_info, ldg_url, cluster_name_url, cluster_info_array, change_cluster_info_array))) {
            LOG_WDIAG("fail to mutil-batch request ldg cluster info", K(ret));
          }
          len_cluster_name_url = 0;
        }
      }
    }
    // 更新LDG_INFO
    if (OB_SUCC(ret)) {
      DRWLock::WRLockGuard lock(ldg_info_lock_);
      if (OB_ISNULL(ldg_info_)) {   // 是第一次创建ldg_info，直接交换指针即可
        ldg_info_ = ldg_info;
        ldg_info = NULL;
        LOG_DEBUG("first to create ldg info", K(login_cluster_name));
      } else {
        if (OB_FAIL(ldg_info_->update_ldg_info(ldg_info, change_cluster_info_array))) {
          LOG_WDIAG("fail to update ldg info", K(ret));
        } else if (OB_FAIL(update_cluster_version(change_cluster_info_array))) {
          LOG_WDIAG("fail to update cluster version", K(ret));
        } else {
          op_free(ldg_info);
          ldg_info = NULL;
          LOG_DEBUG("succ to get ldg info from configserver", K(ret));
        }
      }
    }
    
    if (OB_FAIL(ret) && NULL != ldg_info) {
      op_free(ldg_info);
      ldg_info = NULL;
    }
  }

  // 应该释放url_len + 1
  if (OB_LIKELY(NULL != url)) {
    op_fixed_mem_free(url, url_len + 1);
    url = NULL;
  }
  if (OB_FAIL(ret)) {
    LOG_WDIAG("fail to get ldg info from ocp");
  }

  return ret;
}

int ObConfigServerProcessor::get_ldg_primary_role_instance(
                              const ObString &tenant_name,
                              const ObString &cluster_name,
                              ObProxyObInstance* &instance)
{
  int ret = OB_SUCCESS;
  instance = NULL;
  // 加锁原因：refresh_ldg_config_info调用copy_cluster_name，会读取ldg_cluster_hash；如果此时ldg_cluster_hash.set_refactored()，可能会影响迭代器遍历Hashset;
  // 全局的ldg_cluster_hash_只会增加/被覆盖，不会减少;
  ObProxyConfigString insert_cluster_name;
  insert_cluster_name.set_value(cluster_name);
  LdgHashMap& ldg_cluster_hash = get_global_config_server_processor().ldg_cluster_hash_;
  bool is_exist_cluster_name = (NULL != ldg_cluster_hash.get(insert_cluster_name));
  // char *copy_ptr = NULL;
  if (false == is_exist_cluster_name) {
    DRWLock::WRLockGuard lock(ldg_info_lock_);
    is_exist_cluster_name = (NULL != ldg_cluster_hash.get(insert_cluster_name));
    if (!is_exist_cluster_name) {
      ObProxyConfigString insert_cluster_version;
      if (OB_FAIL(ldg_cluster_hash.set_refactored(insert_cluster_name, insert_cluster_version))) {
        LOG_WDIAG("fail to insert cluster name and version to ldg hashmap", K(insert_cluster_name), K(insert_cluster_version), K(ret));
      } else {
        LOG_DEBUG("success to insert cluster name and version to ldg hashmap", K(insert_cluster_name));
      }
    }
  }
  // 已释放写锁，更新LDG信息
  if (OB_SUCC(ret) && !is_exist_cluster_name) {
    if (OB_FAIL(refresh_ldg_config_info(cluster_name))) {
      LOG_WDIAG("refresh ldg info failed", K(ret));
    } else {
      LOG_DEBUG("succ to first login, refresh ldg info", K(cluster_name));
    }
  }

  if (OB_ISNULL(ldg_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ldg_info is null", K(ret));
  } else if (OB_FAIL(ldg_info_->get_primary_role_instance(tenant_name, cluster_name, instance))) {
    LOG_WDIAG("ldg info get primary role instance failed", K(ret), K(cluster_name), K(tenant_name));
  } else if (NULL != instance) {
    instance->inc_ref();
  }

  return ret;
}

bool ObConfigServerProcessor::is_cluster_array_empty()
{
  int bret = false;
  if (json_config_info_ == NULL) {
    bret = true;
  } else if (json_config_info_->get_cluster_count() <= 0) {
    bret = true;
  }
  return bret;
}

int ObConfigServerProcessor::refresh_binlog_server_host_ip_map()
{
  int ret = OB_SUCCESS;

  // empty hostname mean refresh all
  if (OB_FAIL(get_binlog_service_hostname_ip_processor().refresh_hostname_ip_map_all())) {
    LOG_WDIAG("fail to refresh binlog hostname list", K(ret));
  }

  return ret;
}

int ObConfigServerProcessor::dump_service_name_info_to_local()
{
  int ret = OB_SUCCESS;
  int64_t buf_size = 0;
  int64_t data_len = 0;
  char *service_name_info_buf = NULL;
  // 限制落盘大小为：64K * 16
  const int64_t max_buf_size = OBPROXY_MAX_JSON_INFO_SIZE * 16;
  {
    DRWLock::RDLockGuard guard(service_name_info_lock_);
    if (OB_ISNULL(service_name_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected service_name_info_ is null", K(ret));
    } else if (OB_FAIL(service_name_info_->get_file_max_size(buf_size))) {
      LOG_WDIAG("fail to get idc list file max size", K(ret));
    } else if (OB_UNLIKELY(buf_size < 0) || buf_size > max_buf_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("buf_size is unexpected", K(buf_size), K(max_buf_size), K(ret));
    } else {
      if (OB_ISNULL(service_name_info_buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("dump_service_name_info_to_local to alloc memory failed", K(ret));
      } else if (OB_FAIL(service_name_info_->info_to_json(
                       service_name_info_buf + OB_RECORD_HEADER_LENGTH,
                       buf_size - OB_RECORD_HEADER_LENGTH, data_len))) {
        LOG_WDIAG("fail to format service_name_info to string",
                  KPC(service_name_info_), K(buf_size),
                  K(OB_RECORD_HEADER_LENGTH), K(data_len), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_serialized_file_header(service_name_info_buf, buf_size, data_len, data_len))) {
      LOG_WDIAG("fail to serialize service name info", K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write(CFG_SERVICE_NAME_INFO_DUMP_NAME, service_name_info_buf, 
                                               data_len + OB_RECORD_HEADER_LENGTH))) {
      LOG_WDIAG("fail to dump service name info info to file", K(ret));
    } else {
      LOG_DEBUG("succ to dump service name to local",
                K_(service_name_info_version), K_(disk_server_name_version));
    }
  }

  if (OB_LIKELY(NULL != service_name_info_buf)) {
    ob_free(service_name_info_buf);
    service_name_info_buf = NULL;
  }

  return ret;
}

int ObConfigServerProcessor::load_service_name_info_from_local()
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  char *buf = NULL;
  Value *json_service_name_info = NULL;
  int64_t buf_size = 0;
  ObString json;

  if (OB_FAIL(ObProxyFileUtils::get_file_size(CFG_SERVICE_NAME_INFO_DUMP_NAME, buf_size))) {
    LOG_INFO("fail to get service name info buf size, maybe file does not exist", K(ret));
  } else if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("buf size is invalid", K(buf_size), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory",  K(buf_size), K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read(CFG_SERVICE_NAME_INFO_DUMP_NAME, buf, buf_size, read_len))) {
    LOG_WDIAG("fail to read service name info", K(buf_size), K(ret));
  // 测试下不用校验和，可以手写service name缓存启动proxy
  } else if (!get_global_proxy_config().enable_qa_mode
             && OB_FAIL(ObRecordHeader::check_record(buf, read_len, OB_PROXY_CONFIG_MAGIC))) {
    LOG_WDIAG("fail to check file header", K(ret));
  } else if (FALSE_IT(json.assign_ptr(buf + OB_RECORD_HEADER_LENGTH, static_cast<int32_t>(read_len - OB_RECORD_HEADER_LENGTH)))) {
    // impossible
  } else {
    ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
    if (OB_FAIL(init_json(json, json_service_name_info, json_allocator))) {
      LOG_INFO("fail to init json service name list", K(ret));
    } else {
      DRWLock::WRLockGuard guard(service_name_info_lock_);
      if (OB_UNLIKELY(NULL != service_name_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("service_name_info_ is not null when init from local", K(ret));
      } else if (OB_ISNULL(service_name_info_ = op_alloc(ObServiceNameInfo))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc memory for service name info when init from local", K(ret));
      } else if (OB_FAIL(service_name_info_->parse_local_service_name_info(json_service_name_info))) {
        op_free(service_name_info_);
        service_name_info_ = NULL;
        LOG_WDIAG("fail to parse service name info", K(ret));
      } else {
        // 载入文件后，推高版本号: 强制获取service name关联的cluster resource，
        ATOMIC_FAA(&service_name_info_version_, 1);
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }

  return ret;
}

int ObConfigServerProcessor::get_service_name(const ObString& service_name,
                                              ObIArray<ObServiceNameInstance::ServiceNameVersionPair> &service_name_array) const
{
  int ret = OB_SUCCESS;
  ObProxyConfigString service_name_str;
  ObProxyConfigString service_name_version;
  if (service_name.empty()) {
    DRWLock::RDLockGuard guard(service_name_info_lock_);
    if (OB_NOT_NULL(service_name_info_)) {
      ObServiceNameInfo::ServiceNameMap::iterator iter =
          service_name_info_->get_service_name_instance_map().begin();
      const ObServiceNameInfo::ServiceNameMap::iterator end_iter =
          service_name_info_->get_service_name_instance_map().end();
      for (; OB_SUCC(ret) && iter != end_iter; ++iter) {
        service_name_str.set_value(iter->service_name_);
        service_name_version.set_value(iter->version_md5_);
        ObServiceNameInstance::ServiceNameVersionPair tmp_service_name(service_name_str, service_name_version);
        if (OB_FAIL(service_name_array.push_back(tmp_service_name))) {
          LOG_WDIAG("fail to push back service name to array", K(service_name), K(ret));
        }
      }
    }
  } else {
    service_name_str.set_value(service_name);
    ObServiceNameInstance::ServiceNameVersionPair tmp_service_name(service_name_str, service_name_version);
    if (OB_FAIL(service_name_array.push_back(tmp_service_name))) {
      LOG_WDIAG("fail to push back service name to array", K(service_name), K(ret));
    }
  }
  if (IS_DEBUG_ENABLED()) {
    for (int64_t i = 0; i < service_name_array.count(); ++i) {
      ObServiceNameInstance::ServiceNameVersionPair &tmp_service_name = service_name_array.at(i);
      LOG_DEBUG("all service name array = ", K(i), K(tmp_service_name.first), K(tmp_service_name.second));
    }
  }
  return ret;
}

int ObConfigServerProcessor::request_service_name_info(ObServiceNameInfo &service_name_info,
                                                       const char *service_name_url,
                                                       const ObIArray<ObServiceNameInstance::ServiceNameVersionPair> &service_name_version_array,
                                                       const int64_t start_index, const int64_t end_index,
                                                       ObIArray<ObProxyConfigString> &delete_service_name_array)
{
  int ret = OB_SUCCESS;

  ObString json;
  char *buf = NULL;

  // 解析json结果到buffer，buffer最大=8*64K=512K
  int64_t try_attempts = 0;
  int64_t buf_count = 1;
  int64_t buf_size = 0;
  do {
    buf_size = buf_count * OBPROXY_MAX_JSON_INFO_SIZE;
    if (OB_UNLIKELY(NULL != buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("buf should be null before memory allocated", K(buf), K(ret));
    } else if (OB_ISNULL(buf = static_cast<char*>(op_fixed_mem_alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc memory for ldg json info", K(ret));
    } else if (FALSE_IT(json.assign_buffer(buf, static_cast<int32_t>(buf_size)))) {
      // impossible
    } else if (OB_FAIL(do_fetch_json_info(service_name_url, json, CURL_TRANSFER_TIMEOUT))) {
      if (OB_SIZE_OVERFLOW == ret) {
        // double buf size
        buf_count = buf_count << 1L;
        if (OB_LIKELY(NULL != buf)) {
          op_fixed_mem_free(buf, buf_size);
          buf = NULL;
        }
      }
      LOG_WDIAG("fail to fetch service name info", K(try_attempts), K(buf_count), K(ret));
    }
    ++try_attempts;
  } while (OB_SIZE_OVERFLOW == ret && try_attempts < 4);
  
  // parse json to service_name_info
  if (OB_SUCC(ret)) {
    ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
    Value *root = NULL;
    if (OB_FAIL(init_json(json, root, allocator))) {
      LOG_WDIAG("fail to init json root for service name", K(ret));
    } else if (OB_FAIL(service_name_info.parse(root, service_name_version_array, start_index, end_index, delete_service_name_array))) {
      LOG_WDIAG("fail to parse ldg info", K(ret));
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    op_fixed_mem_free(buf, buf_size);
    buf = NULL;
  }

  return ret;
}

int ObConfigServerProcessor::fetch_service_name_info(const char *tenant_info_url,
                                                     ObServiceNameInfo &service_name_info,
                                                     const ObIArray<ObServiceNameInstance::ServiceNameVersionPair> &service_name_version_array,
                                                     ObIArray<ObProxyConfigString> &delete_service_name_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_info_url)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexcepted tenant_info_url is null pointer", K(ret));
  } else {
    // max_len: tenant_info_url(512) + service_name(64) * batch_size;
    char tenant_info_url_with_service_name[512 + (64 + 1) * SERVICE_NAME_BATCH_SIZE + 1];
    const int64_t origin_len = strlen(tenant_info_url);
    int64_t start_index = 0;
    if (origin_len >= sizeof(tenant_info_url_with_service_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("url len can't greater than service name url", K(origin_len), K(sizeof(tenant_info_url_with_service_name)), K(ret));
    } else {
      MEMCPY(tenant_info_url_with_service_name, tenant_info_url, origin_len);
      const int64_t service_name_array_len = service_name_version_array.count();
      int64_t tail_len = origin_len;
      for (int64_t i = 1; i <= service_name_array_len; ++i) {
        // 每个batch的第一个不加','
        if (i % SERVICE_NAME_BATCH_SIZE != 1) {
          tenant_info_url_with_service_name[tail_len++] = ',';
        } else {
          start_index = i - 1;
        }
        // 先拼接service_name到URL，每batch_size个发送一次
        const ObProxyConfigString &service_name = service_name_version_array.at(i - 1).first;
        if (0 >= snprintf(tenant_info_url_with_service_name + tail_len, sizeof(tenant_info_url_with_service_name) - tail_len,
                          "%s", service_name.ptr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to snprintf service name to url", K(service_name), K(tenant_info_url_with_service_name), K(ret));
        } else {
          tail_len += service_name.length();
          LOG_DEBUG("after concate service name url", K(tenant_info_url_with_service_name));
          if (i % SERVICE_NAME_BATCH_SIZE == 0 || i == service_name_array_len) {
            if (OB_FAIL(request_service_name_info(service_name_info, tenant_info_url_with_service_name, service_name_version_array, start_index, i - 1, delete_service_name_array))) {
              LOG_WDIAG("fail to get service name info from url", K(i), K(tenant_info_url_with_service_name), K(ret));
            } else {
              tail_len = origin_len;
              tenant_info_url_with_service_name[tail_len] = '\0';
            }
          }
        }//end of concate url
      }//end of for
    }//end of batch

  }
  return ret;
}

int ObConfigServerProcessor::get_cluster_resource_for_service_name()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObServiceNameInstance*, 10> service_name_instance_array;
  // 获取所有首次更新的service name instance，并依次注册其更新任务
  {
    DRWLock::RDLockGuard guard(service_name_info_lock_);
    if (OB_ISNULL(service_name_info_)) {
      LOG_WDIAG("unexcepted service_name_info is nullptr", K(service_name_info_), K(ret));
    } else {
      ObServiceNameInfo::ServiceNameMap::iterator iter =
          service_name_info_->get_service_name_instance_map().begin();
      const ObServiceNameInfo::ServiceNameMap::iterator end_iter =
          service_name_info_->get_service_name_instance_map().end();
      for (; OB_SUCC(ret) && iter != end_iter; ++iter) {
        if (iter->has_get_cluster_resource_ || !iter->cas_set_has_get_cr()) {
          LOG_DEBUG("service name had already get cluster resource", K_(iter->service_name));
        } else if (OB_FAIL(service_name_instance_array.push_back(iter.value_))) {
          LOG_WDIAG("fail to push service name instance to array", K(ret));
        } else {
          iter->inc_ref();
        }
      }
    }
  }
  const int64_t instance_array_count = service_name_instance_array.count();
  // 注册没有获取过cr的异步任务
  for (int64_t i = 0; OB_SUCC(ret) && i < instance_array_count; ++i) {
    ObServiceNameInstance* instance = service_name_instance_array.at(i);
    if (OB_NOT_NULL(instance)) {
      ObServiceNameRoleRefreshCont* refresh_cont = NULL;
      if (OB_ISNULL(refresh_cont = op_alloc_args(ObServiceNameRoleRefreshCont, true))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("failed to alloc memory for ObServiceNameRoleRefreshCont", K(ret));
      } else if (refresh_cont->init(instance)) {
        LOG_WDIAG("failed to init for ObServiceNameRoleRefreshCont", K(ret));
      } else {
        if (OB_ISNULL(g_event_processor.schedule_imm(refresh_cont, ET_CALL, REFRESH_TENANT_ROLE_EVENT))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("failed to schedule ObServiceNameRoleRefreshCont", K(ret));
        } else {
          LOG_DEBUG("succ to schedule get cluster resource", K_(instance->service_name));
        }
      }
      instance->dec_ref();
    }
  }
  return ret;
}

int ObConfigServerProcessor::refresh_service_name_info(const ObString service_name/*空字符串""*/,
                                                       const bool force_update/*false*/)
{
  // 调用时：get_global_proxy_config().enable_standby
  // service_name为空：定时任务触发，批量拉取所有的service name
  // 否则只拉取传入的service_name
  int ret = OB_SUCCESS;
  char tenant_info_url[512]{};
  // 1. GetTenantInfoUrl
  {
    // json读锁
    CRLockGuard lock(json_info_lock_);
    const ObProxyConfigString &service_name_url = json_config_info_->get_data_info().get_service_name_tenant_;
    if (service_name_url.empty()) {
      // do nothing
    } else if (0 >= snprintf(tenant_info_url, sizeof(tenant_info_url),
                             "%s%s", service_name_url.ptr(),
                             JSON_SERVICE_NAME_URL)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }

  ObSEArray<ObServiceNameInstance::ServiceNameVersionPair, 10> service_name_version_array;
  ObSEArray<ObProxyConfigString, 10> delete_service_name_array;
  ObServiceNameInfo *service_name_info = NULL;
  bool need_update = false;
  // 2. url有效，且拉取service_name_version_array不为空时，才fetch url
  if (OB_FAIL(ret) || '\0' == tenant_info_url[0]) {
    LOG_DEBUG("don't refresh service name", K(tenant_info_url), K(ret));
    // do nothing
  } else if (OB_FAIL(get_service_name(service_name, service_name_version_array))) {
    LOG_WDIAG("fail to get all service name info", K(service_name), K(tenant_info_url), K(ret));
  } else if (service_name_version_array.empty()) {
    LOG_DEBUG("service name array is empty, will not fetch url");
  } else if (OB_ISNULL(service_name_info = op_alloc_args(ObServiceNameInfo, force_update))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for service name info", K(ret));
  } else if (OB_FAIL(fetch_service_name_info(tenant_info_url, *service_name_info, service_name_version_array, delete_service_name_array))) {
    LOG_WDIAG("fail to fetch service name info", K(service_name), K(tenant_info_url), K(ret));
  } else {
    // 写锁更新service_name：对OCP没有返回的service_name，需要删除
    DRWLock::WRLockGuard guard(service_name_info_lock_);
    if (OB_ISNULL(service_name_info_)) {
      service_name_info_ = service_name_info;
      service_name_info = NULL;
      need_update = true;
    } else {
      need_update = service_name_info->get_service_name_instance_map().count();
      if (OB_FAIL(service_name_info_->update(service_name_info->get_service_name_instance_map()))) {
        LOG_WDIAG("fail to update service name info", K(service_name), K(tenant_info_url), K(ret));
      } else if (OB_FAIL(service_name_info_->delete_service_name(delete_service_name_array))) {
        LOG_WDIAG("fail to delete invalid service name info", K(service_name), K(tenant_info_url), K(ret));
      } else {
        LOG_DEBUG("succ to refresh service name info", KPC(service_name_info_), K(service_name), K(tenant_info_url), K(ret));
      }
    }
  }
  
  // 判断是否需要增加版本号，并对没有获过集群资源的service name注册获取集群资源
  if (OB_SUCC(ret) && (!delete_service_name_array.empty() || need_update)) {
    ATOMIC_FAA(&service_name_info_version_, 1);
  }
  if (OB_NOT_NULL(service_name_info)) {
    op_free(service_name_info);
    service_name_info = NULL;
  }
  LOG_DEBUG("finish refresh service name", K(service_name), K_(service_name_info_version), K_(disk_server_name_version));
  // 仅定时任务触发时，会写入落盘，修改磁盘版本号；
  // 版本号修改时，可能存在并发问题，但这里无需强一致，没有锁保护
  if (OB_SUCC(ret) && service_name.empty() && disk_server_name_version_ != service_name_info_version_) {
    if (OB_FAIL(get_cluster_resource_for_service_name())) {
      LOG_WDIAG("fail to schedule get cluster resource for service name", K(service_name), K(tenant_info_url), K(ret));
    } else if (OB_FAIL(dump_service_name_info_to_local())) {
      LOG_WDIAG("fail to dump service name info to local", K(service_name), K(tenant_info_url), K(ret));
    } else {
      disk_server_name_version_ = service_name_info_version_;
      LOG_DEBUG("succ to dump service name", K(service_name), K_(disk_server_name_version));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_service_name_with_fetch(const ObString &service_name, ObServiceNameInstance* &instance)
{
  int ret = OB_SUCCESS;
  instance = NULL;
  // 登录使用
  if (OB_FAIL(get_service_name_without_fetch(service_name, instance))) {
    LOG_WDIAG("fail to get service name instance when first fetch", K(service_name), K(ret));
  }
  // 如果找不到service name，先拉取对应的ocp接口，再获取一次
  // 但缓存文件有错误数据，可能导致租户列表为空，此时需要强制更新对应service name
  if (OB_SUCC(ret)
      && (OB_ISNULL(instance) || instance->instance_array_.empty())) {
    if (OB_FAIL(refresh_service_name_info(service_name, OB_NOT_NULL(instance)))) {
      LOG_WDIAG("fail to refresh service name info", K(service_name), K(ret));
    } else if (OB_FAIL(get_service_name_without_fetch(service_name, instance))) {
      LOG_WDIAG("fail to get service name instance when second fetch", K(service_name), K(ret));
    }
  }
  return ret;
}

int ObConfigServerProcessor::get_service_name_without_fetch(const ObString &service_name, ObServiceNameInstance* &instance)
{
  int ret = OB_SUCCESS;
  instance = NULL;
  {
    DRWLock::RDLockGuard guard(service_name_info_lock_);
    if (OB_NOT_NULL(service_name_info_)
        && OB_FAIL(service_name_info_->get_service_name_instance(service_name, instance))) {
      LOG_WDIAG("fail to get service name instance", K(service_name), K(ret));
    }
  }
  return ret;
}

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase
