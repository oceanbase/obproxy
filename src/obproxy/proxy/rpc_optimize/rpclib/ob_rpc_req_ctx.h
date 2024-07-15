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

#ifndef OB_RPC_REQ_CTX_H
#define OB_RPC_REQ_CTX_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/time/ob_hrtime.h"
#include "proxy/route/ob_ldc_location.h"
#include "proxy/route/ob_table_entry.h"
#include "obkv/table/ob_table.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define RPC_REQ_CTX_LOOKUP_CACHE_DONE     (RPC_REQ_CTX_EVENT_EVENTS_START + 1)
#define RPC_REQ_CTX_LOOKUP_START_EVENT    (RPC_REQ_CTX_EVENT_EVENTS_START + 2)
#define RPC_REQ_CTX_LOOKUP_CACHE_EVENT    (RPC_REQ_CTX_EVENT_EVENTS_START + 3)

class ObRpcReqCtx : public common::ObSharedRefCount
{
friend class ObRpcReq;
friend class ObRpcOBKVInfo;
friend class ObRpcRequestSM;
public:
  static const int SCHEMA_LENGTH = 100;
public:
  enum ObRpcCtxState
  {
    RC_BORN = 0,
    RC_INITING,
    RC_AVAIL,
    RC_DELETING,
  };
  enum ObRpcCtxCacheEntryState
  {
    CACHE_ENTRY_AVAIL,
    CACHE_ENTRY_UPDATING
  };

  ObRpcReqCtx()
    : common::ObSharedRefCount(), rc_state_(RC_BORN), credential_(),
      cluster_name_(), tenant_name_(), user_name_(), database_name_(), full_name_(),
      is_clustername_from_default_(false), name_len_(0), name_buf_(NULL),
      last_access_time_ns_(0), lock_(), cluster_resource_(NULL), cache_cluster_resource_state_(CACHE_ENTRY_UPDATING),
      dummy_entry_(NULL), dummy_ldc_(), dummy_entry_valid_time_ns_(0), server_state_version_(0), cache_dummy_entry_state_(CACHE_ENTRY_UPDATING),
      config_info_(), cache_config_info_state_(CACHE_ENTRY_UPDATING) {}
  ~ObRpcReqCtx() {}

  static int alloc_and_init_rpc_ctx(ObRpcReqCtx *&ob_rpc_req_ctx);
  void reset();
  int init();
  void free();

  obkv::ObTableApiCredential get_credential() const { return credential_; }

  int set_full_name(const ObString &full_name);
  void set_clustername_from_default(bool flag) { is_clustername_from_default_ = flag; }
  bool is_clustername_from_default() const { return is_clustername_from_default_; }

  ObString get_tenant_name() const { return tenant_name_; }
  ObString get_cluster_name() const { return cluster_name_; }
  ObString get_user_name() const { return user_name_; }

  int  get_analyze_name_buf(char *&buf_start, int64_t len);
  void assign_user_name(char *buf, int64_t length) { user_name_.assign_ptr(buf, length); }
  void assign_cluster_name(char *buf, int64_t length) { cluster_name_.assign_ptr(buf, length); }
  void assign_tenant_name(char *buf, int64_t length) { tenant_name_.assign_ptr(buf, length); }
  void assign_database_name(char *buf, int64_t length) { database_name_.assign_ptr(buf, length); }
  void assign_full_name(char *buf, int64_t length) { full_name_.assign_ptr(buf, length); }

  void set_cluster_id(int64_t cluster_id) { credential_.cluster_id_ = cluster_id; }

  int update_cache_entry(ObTableEntry *dummy_entry, ObLDCLocation &dummy_ldc, const int64_t tenant_location_valid_time);
  bool cas_compare_and_swap_state(ObRpcCtxCacheEntryState &cache_entry_state, const ObRpcCtxCacheEntryState old_state, const ObRpcCtxCacheEntryState new_state);
  bool cas_set_cluster_resource_updating_state();
  bool cas_set_cluster_resource_avail_state();
  bool cas_set_dummy_entry_updating_state();
  bool cas_set_dummy_entry_avail_state();
  bool cas_set_config_info_updating_state();
  bool cas_set_config_info_avail_state();

  void set_cluster_resource(obutils::ObClusterResource *cluster_resource);

  ObRpcRequestConfigInfo &get_config_info() { return config_info_; }
  // check_update_ldc不应该依赖rpc_ctx内部变量
  static int check_update_ldc(ObTableEntry *dummy_entry, ObLDCLocation &dummy_ldc, int64_t server_state_version, obutils::ObClusterResource *cluster_resource);

  common::DRWLock &get_rpc_ctx_lock() { return lock_; }

  ObRpcCtxCacheEntryState get_cache_dummy_entry_state() const { return cache_dummy_entry_state_; }
  ObRpcCtxCacheEntryState get_cache_cluster_resource_state() const { return cache_cluster_resource_state_; }
  ObRpcCtxCacheEntryState get_cache_config_info_state() const { return cache_config_info_state_; }
  bool is_cached_entry_avail_state(const ObRpcCtxCacheEntryState &state) const { return ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL == state; }
  bool is_cached_entry_updating_state(const ObRpcCtxCacheEntryState &state) const { return ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING == state; }
  bool is_cached_dummy_entry_avail_state() const { return is_cached_entry_avail_state(cache_dummy_entry_state_); }
  bool is_cached_dummy_entry_updating_state() const { return is_cached_entry_updating_state(cache_dummy_entry_state_); }
  bool is_cached_cluster_resource_avail_state() const { return is_cached_entry_avail_state(cache_cluster_resource_state_); }
  bool is_cached_cluster_resource_updating_state() const { return is_cached_entry_updating_state(cache_cluster_resource_state_); }
  bool is_cached_config_info_avail_state() const { return is_cached_entry_avail_state(cache_config_info_state_); }
  bool is_cached_config_info_updating_state() const { return is_cached_entry_updating_state(cache_config_info_state_); }

  bool is_cached_dummy_entry_expired();
  bool is_cached_dummy_ldc_empty() const;

  void set_avail_state() { rc_state_ = RC_AVAIL; }
  void set_initing_state() { rc_state_ = RC_INITING; }
  void set_deleting_state() { rc_state_ = RC_DELETING; }
  bool is_avail() const { return (RC_AVAIL == rc_state_); }
  bool is_initing() const { return (RC_INITING == rc_state_); }
  bool is_deleting() const { return (RC_DELETING == rc_state_); }

  bool is_expired(const int64_t expired_time_ns) const { return last_access_time_ns_ > 0 && event::get_hrtime() - last_access_time_ns_ > expired_time_ns; }
  void renew_last_access_time() { last_access_time_ns_ = event::get_hrtime(); }

  TO_STRING_KV(KP(this),
               K_(credential),
               K_(cluster_name),
               K_(tenant_name),
               K_(user_name),
               K_(database_name),
               K_(full_name),
               KP_(cluster_resource),
               KP_(dummy_entry),
               "dummy_ldc_item_count", dummy_ldc_.count(),
               "config_version", config_info_.config_version_);

private:
  ObRpcCtxState rc_state_;
  obkv::ObTableApiCredential credential_;

  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString database_name_;
  common::ObString full_name_;

  bool is_clustername_from_default_;
  int64_t name_len_;
  char *name_buf_;
  char full_name_buf_[OB_PROXY_FULL_USER_NAME_MAX_LEN];
  char schema_name_buf_[SCHEMA_LENGTH];

  int64_t last_access_time_ns_;
  //  下面的数据访问都需要获取lock, TODO：考虑是否多个lock
  common::DRWLock lock_;
  obutils::ObClusterResource *cluster_resource_;
  ObRpcCtxCacheEntryState cache_cluster_resource_state_;

  ObTableEntry *dummy_entry_; // __all_dummy's table location entry
  ObLDCLocation dummy_ldc_;
  int64_t dummy_entry_valid_time_ns_;
  int64_t server_state_version_;
  ObRpcCtxCacheEntryState cache_dummy_entry_state_;

  ObRpcRequestConfigInfo config_info_;
  ObRpcCtxCacheEntryState cache_config_info_state_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtx);
};

inline bool ObRpcReqCtx::cas_compare_and_swap_state(
    ObRpcCtxCacheEntryState &cache_entry_state,
    const ObRpcCtxCacheEntryState old_state,
    const ObRpcCtxCacheEntryState new_state)
{
  return __sync_bool_compare_and_swap(&cache_entry_state, old_state, new_state);
}

inline bool ObRpcReqCtx::cas_set_cluster_resource_updating_state()
{
  return cas_compare_and_swap_state(cache_cluster_resource_state_, ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL, ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING);
}

inline bool ObRpcReqCtx::cas_set_cluster_resource_avail_state()
{
  return cas_compare_and_swap_state(cache_cluster_resource_state_, ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING, ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL);
}

inline bool ObRpcReqCtx::cas_set_dummy_entry_updating_state()
{
  return cas_compare_and_swap_state(cache_dummy_entry_state_, ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL, ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING);
}

inline bool ObRpcReqCtx::cas_set_dummy_entry_avail_state()
{
  return cas_compare_and_swap_state(cache_dummy_entry_state_, ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING, ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL);
}

inline bool ObRpcReqCtx::cas_set_config_info_updating_state()
{
  return cas_compare_and_swap_state(cache_config_info_state_, ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL, ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING);
}

inline bool ObRpcReqCtx::cas_set_config_info_avail_state()
{
  return cas_compare_and_swap_state(cache_config_info_state_, ObRpcCtxCacheEntryState::CACHE_ENTRY_UPDATING, ObRpcCtxCacheEntryState::CACHE_ENTRY_AVAIL);
}

inline bool ObRpcReqCtx::is_cached_dummy_ldc_empty() const
{
  return dummy_ldc_.is_empty();
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_RPC_REQ_CTX_H