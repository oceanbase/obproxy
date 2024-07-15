#define USING_LOG_PREFIX PROXY

#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx.h"
#include "obutils/ob_resource_pool_processor.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

inline int ObRpcReqCtx::init()
{
  int ret = OB_SUCCESS;

  rc_state_ = ObRpcCtxState::RC_AVAIL;

  return ret;
}

int ObRpcReqCtx::alloc_and_init_rpc_ctx(ObRpcReqCtx *&ob_rpc_req_ctx)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(ObRpcReqCtx);
  void *buf = op_fixed_mem_alloc(alloc_size);

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "fail to alloc mem", K(alloc_size), K(ret));
  } else {
    ob_rpc_req_ctx = new (buf) ObRpcReqCtx();

    if (OB_FAIL(ob_rpc_req_ctx->init())) {
      PROXY_LOG(WDIAG, "fail to init rpc_ctx", KPC(ob_rpc_req_ctx), K(ret));
    } else {
      ob_rpc_req_ctx->inc_ref();
      LOG_DEBUG("ObRpcReqCtx alloc and init succ", KPC(ob_rpc_req_ctx));
    }
  }
  if (OB_FAIL(ret) && (NULL != buf)) {
    op_fixed_mem_free(buf, alloc_size);
    ob_rpc_req_ctx = NULL;
    alloc_size = 0;
  }

  return ret;
}

inline void ObRpcReqCtx::reset()
{
  rc_state_ = ObRpcCtxState::RC_DELETING;
  cluster_name_.reset();
  tenant_name_.reset();
  user_name_.reset();
  database_name_.reset();
  full_name_.reset();

  is_clustername_from_default_ = false;

  if (NULL != name_buf_ && 0 != name_len_) {
    op_fixed_mem_free(name_buf_, name_len_);
    name_buf_ = NULL;
    name_len_ = 0;
  }

  last_access_time_ns_ = 0;

  if (OB_NOT_NULL(cluster_resource_)) {
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }

  if (OB_NOT_NULL(dummy_entry_)) {
    dummy_entry_->dec_ref();
    dummy_entry_ = NULL;
  }
  dummy_ldc_.reset();
}

inline void ObRpcReqCtx::free()
{
  LOG_DEBUG("ObRpcReqCtx will be free", K(*this));
  reset();
  op_fixed_mem_free(this, sizeof(ObRpcReqCtx));
}

int ObRpcReqCtx::set_full_name(const ObString &full_name)
{
  int ret = OB_SUCCESS;

  if (full_name.empty() || full_name.length() > OB_PROXY_FULL_USER_NAME_MAX_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("set_full_name get wrong full_name", K(full_name));
  } else {
    MEMCPY(full_name_buf_, full_name.ptr(), full_name.length());
  }

  return ret;
}

int ObRpcReqCtx::get_analyze_name_buf(char *&buf_start, int64_t len)
{
  int ret = OB_SUCCESS;

  if (len <= OB_PROXY_FULL_USER_NAME_MAX_LEN) {
    buf_start = full_name_buf_;
  } else {
    if (NULL != name_buf_ && name_len_ < len) {
      op_fixed_mem_free(name_buf_, name_len_);
      name_buf_ = NULL;
      name_len_ = 0;
    }

    if (NULL == name_buf_) {
      if (OB_ISNULL(name_buf_ = static_cast<char *>(op_fixed_mem_alloc(len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc memory for login name", K(ret));
      } else {
        name_buf_[0] = '\0';
        name_len_ = len;
      }
    }
    if (OB_SUCC(ret)) {
      buf_start = name_buf_;
    }
  }

  return ret;
}

int ObRpcReqCtx::update_cache_entry(ObTableEntry *dummy_entry, ObLDCLocation &dummy_ldc, const int64_t tenant_location_valid_time)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dummy_entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(dummy_entry));
  } else if (OB_FAIL(ObLDCLocation::copy_dummy_ldc(dummy_ldc, dummy_ldc_))) {
    LOG_WARN("fail to copy_dummy_ldc", K(ret), K(dummy_ldc));
  } else {
    // store dummy entry into rpc_ctx
    if (OB_NOT_NULL(dummy_entry_)) {
      dummy_entry_->dec_ref();
      dummy_entry_ = NULL;
    }
    dummy_entry_ = dummy_entry;
    dummy_entry_->inc_ref();
    dummy_entry_valid_time_ns_ = ObRandomNumUtils::get_random_half_to_full(tenant_location_valid_time);
  }

  return ret;
}

int ObRpcReqCtx::check_update_ldc(ObTableEntry *dummy_entry, ObLDCLocation &dummy_ldc, int64_t server_state_version,
                                    obutils::ObClusterResource *cluster_resource)
{
  int ret = OB_SUCCESS;
  common::ModulePageAllocator *allocator = NULL;
  LOG_DEBUG("ObRpcReqCtx::check_update_ldc");

  if (OB_ISNULL(cluster_resource)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "cluster_resource is not avail", K(ret));
  } else if (OB_ISNULL(dummy_entry) || OB_UNLIKELY(!dummy_entry->is_tenant_servers_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CS_LOG(WDIAG, "dummy_entry is not avail", KPC(dummy_entry), K(ret));
  } else if (OB_FAIL(ObLDCLocation::get_thread_allocator(allocator))) {
    PROXY_CS_LOG(WDIAG, "fail to get_thread_allocator", K(ret));
  } else {
    bool is_base_servers_added = cluster_resource->is_base_servers_added();
    // TODO idc name
    ObString new_idc_name(get_global_proxy_config().proxy_idc_name);
    //we need update ldc when the follow happened:
    //1. servers_state_version has changed
    //or
    //2. dummuy_ldc is invalid
    //or
    //3. idc_name has changed
    //or
    //4. base_servers has not added
    if (cluster_resource->server_state_version_ != server_state_version
        || dummy_ldc.is_empty()
        || 0 != new_idc_name.case_compare(dummy_ldc.get_idc_name())
        || !is_base_servers_added) {
      PROXY_CS_LOG(DEBUG, "need update dummy_ldc",
                   "old_idc_name", dummy_ldc.get_idc_name(),
                   K(new_idc_name),
                   "cluster_name", cluster_resource->get_cluster_name(),
                   "old_ss_version", server_state_version,
                   "new_ss_version", cluster_resource->server_state_version_,
                   "dummy_ldc_is_empty", dummy_ldc.is_empty(),
                   K(is_base_servers_added));
      ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
          ObServerStateRefreshCont::DEFAULT_SERVER_COUNT, *allocator);
      bool need_ignore = false;
      if (!is_base_servers_added && 0 == cluster_resource->server_state_version_) {
        PROXY_CS_LOG(INFO, "base servers has not added, treat all tenant server as ok",
                    "tenant_server", *(dummy_entry->get_tenant_servers()), K(ret));
      } else {
        const uint64_t new_ss_version = cluster_resource->server_state_version_;
        common::ObIArray<ObServerStateSimpleInfo> &server_state_info = cluster_resource->get_server_state_info(new_ss_version);
        common::DRWLock &server_state_lock = cluster_resource->get_server_state_lock(new_ss_version);
        int err_no = 0;
        if (0 != (err_no = server_state_lock.try_rdlock())) {
          if (dummy_ldc.is_empty()) {
            //treate it as is_base_servers_added is false
            is_base_servers_added = false;
          } else {
            need_ignore = true;
          }
          PROXY_CS_LOG(EDIAG, "fail to tryrdlock server_state_lock, ignore this update",
                       K(err_no),
                       "old_idc_name", dummy_ldc.get_idc_name(),
                       K(new_idc_name),
                       "old_ss_version", server_state_version,
                       "new_ss_version", new_ss_version,
                       "dummy_ldc_is_empty", dummy_ldc.is_empty(),
                       K(cluster_resource->is_base_servers_added()),
                       K(is_base_servers_added), K(need_ignore), K(dummy_ldc));
        } else {
          if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
            PROXY_CS_LOG(WDIAG, "fail to assign servers_info_", K(ret));
          } else {
            server_state_version = new_ss_version;
          }
          server_state_lock.rdunlock();
        }
      }
      if (OB_SUCC(ret) && !need_ignore) {
        if (OB_FAIL(dummy_ldc.assign(dummy_entry->get_tenant_servers(), simple_servers_info,
            new_idc_name, is_base_servers_added, cluster_resource->get_cluster_name(),
            cluster_resource->get_cluster_id()))) {
          if (OB_EMPTY_RESULT == ret) {
            if (dummy_entry->is_entry_from_rslist()) {
              // set_need_delete_cluster();
              PROXY_CS_LOG(WDIAG, "tenant server from rslist is not match the server list, "
                           "need delete this cluster", KPC(dummy_entry), K(ret));
            } else {
              //sys dummy entry can not set dirty
              if (!dummy_entry->is_sys_dummy_entry() && dummy_entry->cas_set_dirty_state()) {
                PROXY_CS_LOG(WDIAG, "tenant server is invalid, set it dirty", KPC(dummy_entry), K(ret));
              }
            }
          } else {
            PROXY_CS_LOG(WDIAG, "fail to assign dummy_ldc", K(ret));
          }
        }
      }
      if (cluster_resource->is_base_servers_added()) {
        dummy_ldc.set_safe_snapshot_manager(&cluster_resource->safe_snapshot_mgr_);
      }
      if (OB_SUCC(ret)) {
        PROXY_CS_LOG(DEBUG, "succ to update dummy_ldc", K(dummy_ldc));
      }
    } else {
      PROXY_CS_LOG(DEBUG, "no need update dummy_ldc",
                   "old_idc_name", dummy_ldc.get_idc_name(),
                   K(new_idc_name),
                   "old_ss_version", server_state_version,
                   "new_ss_version", cluster_resource->server_state_version_,
                   "dummy_ldc_is_empty", dummy_ldc.is_empty(),
                   K(is_base_servers_added),
                   K(dummy_ldc));
    }
    allocator = NULL;
  }

  return ret;
}

void ObRpcReqCtx::set_cluster_resource(obutils::ObClusterResource *cluster_resource)
{
  if (OB_NOT_NULL(cluster_resource_)) {
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }
  cluster_resource_ = cluster_resource;
  cluster_resource_->inc_ref();
}

bool ObRpcReqCtx::is_cached_dummy_entry_expired()
{
  bool need_update = false;
  ObTableEntry *cached_dummy_entry = dummy_entry_;
  int64_t valid_ns = dummy_entry_valid_time_ns_;

  if (NULL != cached_dummy_entry) {
    if (cached_dummy_entry->is_deleted_state()) {
      need_update = true;
    } else if (cached_dummy_entry->is_need_update()) { // dirty, but not in punish time
      need_update = true;
    } else if (cached_dummy_entry->is_avail_state()) {
      if (valid_ns > 0) {
        bool expired = ((get_hrtime_internal() - hrtime_from_usec(cached_dummy_entry->get_create_time_us())) > valid_ns);
        if (expired
            && !cached_dummy_entry->is_sys_dummy_entry()
            && cached_dummy_entry->cas_set_dirty_state()) {
          LOG_INFO("this cached dummy entry is expired, set to dirty", KPC(cached_dummy_entry), K(valid_ns));
          need_update = true;
        }
      }
    }
  }

  if (need_update) {
    LOG_INFO("this cached dummy entry need force renew", KPC(cached_dummy_entry));
  }

  return need_update;
}

}
}
}