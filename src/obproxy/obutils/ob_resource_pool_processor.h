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

#ifndef OBPROXY_RESOURCES_POOL_PROCESSOR_H
#define OBPROXY_RESOURCES_POOL_PROCESSOR_H
#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "proxy/mysqllib/ob_sys_var_set_processor.h"
#include "proxy/client/ob_mysql_client_pool.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "obutils/ob_congestion_manager.h"
#include "obutils/ob_server_state_processor.h"
#include "obutils/ob_async_common_task.h"
#include "obutils/ob_safe_snapshot_manager.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/hash/ob_hashset.h"
#include "obutils/ob_connection_diagnosis_trace.h"

namespace oceanbase
{
namespace obproxy
{
class ObDefaultSysVarSet;
namespace proxy
{
class ObTableEntry;
}
namespace obutils
{
const char *const OB_RANDOM_PRIMARY_ZONE = "RANDOM";

#define CLUSTER_RESOURCE_CREATE_EVENT (CLUSTER_RESOURCE_EVENT_EVENTS_START + 1)
#define CLUSTER_RESOURCE_INFORM_OUT_EVENT (CLUSTER_RESOURCE_EVENT_EVENTS_START + 2)
#define CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT (CLUSTER_RESOURCE_EVENT_EVENTS_START + 3)

class ObAsyncCommonTask;
class ObClusterResource;
class ObRslistFetchCont : public ObAsyncCommonTask
{
public:
  ObRslistFetchCont(ObClusterResource *cr, event::ObProxyMutex *m,
                    const bool need_update_dummy_entry,
                    ObConnectionDiagnosisTrace *connection_diagnosis_trace,
                    event::ObContinuation *cb_cont = NULL,
                    event::ObEThread *submit_thread = NULL)
    : ObAsyncCommonTask(m, "rslist_fetch_task", cb_cont, submit_thread),
      fetch_result_(false), need_update_dummy_entry_(need_update_dummy_entry), cr_(cr), connection_diagnosis_trace_(connection_diagnosis_trace)
      {
        if (connection_diagnosis_trace_ != NULL){
          connection_diagnosis_trace_->inc_ref();
        }
      }
  virtual ~ObRslistFetchCont() {}

  virtual void destroy();

  virtual int init_task();
  virtual int finish_task(void *data)
  {
    UNUSED(data);
    return OB_SUCCESS;
  }
  virtual void *get_callback_data() { return static_cast<void *>(&fetch_result_); }

private:
  bool fetch_result_;
  bool need_update_dummy_entry_;
  ObClusterResource *cr_;
  ObConnectionDiagnosisTrace *connection_diagnosis_trace_;
  DISALLOW_COPY_AND_ASSIGN(ObRslistFetchCont);
};


class ObIDCListFetchCont : public ObAsyncCommonTask
{
public:
  ObIDCListFetchCont(ObClusterResource *cr, event::ObProxyMutex *m,
                     ObConnectionDiagnosisTrace *connection_diagnosis_trace,
                     event::ObContinuation *cb_cont = NULL,
                     event::ObEThread *submit_thread = NULL,
                     const bool fetch_result = false)
    : ObAsyncCommonTask(m, "idc_list_fetch_task", cb_cont, submit_thread),
      fetch_result_(fetch_result), cr_(cr), connection_diagnosis_trace_(connection_diagnosis_trace) {
        if (connection_diagnosis_trace_ != NULL) {
          connection_diagnosis_trace_->inc_ref();
        }
      }
  virtual ~ObIDCListFetchCont() {}

  virtual void destroy();

  virtual int init_task();
  virtual int finish_task(void *data)
  {
    UNUSED(data);
    return OB_SUCCESS;
  }
  virtual void *get_callback_data() { return static_cast<void *>(&fetch_result_); }

private:
  bool fetch_result_;
  ObClusterResource *cr_;
  ObConnectionDiagnosisTrace *connection_diagnosis_trace_;
  DISALLOW_COPY_AND_ASSIGN(ObIDCListFetchCont);
};

enum ObClusterResourceInitState
{
 INIT_BORN = 0,
 INIT_RS,
 CHECK_VERSION,
 CHECK_CLUSTER_INFO,
 INIT_SS_INFO,
 INIT_IDC_LIST,
 INIT_SYSVAR,
 INIT_SERVER_STATE_PROCESSOR
};

struct ObClusterInfoKey
{
  ObClusterInfoKey() : cluster_id_(OB_DEFAULT_CLUSTER_ID), cluster_name_(OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1) {}
  ObClusterInfoKey(const ObProxyConfigString& name, int64_t id) : cluster_id_(id)
  {
    cluster_name_.set_value(name.config_string_);
  }
  ObClusterInfoKey(const common::ObString &name, const int64_t id) : cluster_id_(id)
  {
    cluster_name_.set_value(name);
  }

  ~ObClusterInfoKey() {}

  uint64_t hash(uint64_t seed = 0) const
  {
    uint64_t hashs = cluster_name_.hash(seed);
    hashs = common::murmurhash(&cluster_id_, sizeof(cluster_id_), hashs);
    return hashs;
  }

  void set_cluster_id(const int64_t id) { cluster_id_ = id; }
  void set_cluster_name(const common::ObString &name) { cluster_name_.set_value(name); }
  bool is_valid() const { return cluster_name_.is_valid(); } // cluster_id = invalid means master cluster

  void reset()
  {
    cluster_name_.reset();
    cluster_id_ = OB_INVALID_CLUSTER_ID;
  }

  int assign(const ObClusterInfoKey &other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_LIKELY(this != &other)) {
      if (OB_FAIL(cluster_name_.assign(other.cluster_name_))) {
        _PROXY_LOG(WDIAG, "cluster_name assign error, ret = [%d]", ret);
      } else {
        cluster_id_ = other.cluster_id_;
      }
    }
    return ret;
  }

  bool operator==(const ObClusterInfoKey &other) const
  {
    return cluster_id_ == other.cluster_id_ && cluster_name_ == other.cluster_name_;
  }
  bool operator!=(const ObClusterInfoKey &other) const { return !(*this == other); }

  DECLARE_TO_STRING;

public:
  int64_t cluster_id_;
  ObProxyConfigString cluster_name_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObClusterInfoKey);
};

typedef int (event::ObContinuation::*process_cr_pfn) (void *data);


typedef common::ObSEArray<common::ObString, common::MAX_ZONE_NUM> PrimaryZonePrioArrayType;
typedef common::ObSEArray<int8_t, common::MAX_ZONE_NUM> PrimaryZonePrioWeightArrayType;

class ObLocationTenantInfo : public common::ObSharedRefCount
{
public:
  ObLocationTenantInfo() : tenant_name_(), version_(0), primary_zone_(),
    primary_zone_prio_array_(), primary_zone_prio_weight_array_()
  {
    memset(tenant_name_str_, 0x0, sizeof(tenant_name_str_));
    memset(primary_zone_str_, 0x0, sizeof(primary_zone_str_));
  }
  virtual ~ObLocationTenantInfo() {}
  virtual void free() { destroy(); };
  void destroy();
  
  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(tenant_name), K_(version), K_(primary_zone),
            K_(primary_zone_prio_array), K_(primary_zone_prio_weight_array));
    J_OBJ_END();
    return pos;
  }

  int resolve_location_tenant_info_primary_zone(common::ObString &primary_zone);
  
public:
  common::ObString tenant_name_;
  char tenant_name_str_[common::OB_MAX_TENANT_NAME_LENGTH];
  uint64_t version_;
  
  common::ObString primary_zone_;
  char primary_zone_str_[common::MAX_ZONE_LIST_LENGTH];    //real:MAX_ZONE_LENGTH*MAX_ZONE_NUM, this is mem optimization
  PrimaryZonePrioArrayType primary_zone_prio_array_;  //pz priority zone name array, related to char buf
  PrimaryZonePrioWeightArrayType primary_zone_prio_weight_array_;  //pz priority weight array

  LINK(ObLocationTenantInfo, cr_link_);
};

class ObProxyConfig;
class ObResourcePoolConfig;
class ObClusterResource : public common::ObSharedRefCount
{
public:
  ObClusterResource()
    : sys_var_set_processor_(), congestion_manager_(), ss_refresh_cont_(NULL),
      detect_server_state_cont_(NULL), mysql_proxy_(), server_state_version_(0), dummy_entry_(NULL),
      cluster_info_key_(), last_access_time_ns_(0), deleting_completed_thread_num_(0),
      version_(0), fetch_rslist_task_count_(0), fetch_idc_list_task_count_(0),
      last_idc_list_refresh_time_ns_(0), last_rslist_refresh_time_ns_(0),
      pending_list_(), cluster_version_(0), alive_addr_set_(), is_inited_(false), cr_state_(CR_BORN), sys_ldg_info_map_(),
      sys_ldg_info_lock_(), location_tenant_info_lock_(), location_tenant_info_map_() {}
  virtual ~ObClusterResource() {}
  virtual void free();
  bool is_congestion_avail() { return congestion_manager_.is_congestion_avail(); }
  bool is_base_servers_added() const { return congestion_manager_.is_base_servers_added(); }

  int init(const common::ObString &cluster_name, const int64_t cluster_id, const int64_t version);
  int init_local_config(const ObResourcePoolConfig &config);
  int init_server_state_processor(const ObResourcePoolConfig &config);

  // if client pool is null, create it;
  // otherwise, we will destroy current client pool and create a new one
  int rebuild_mysql_client_pool(ObClusterResource *cr);

  bool is_expired(const int64_t expired_time_ns) const { return last_access_time_ns_ > 0 && event::get_hrtime() - last_access_time_ns_ > expired_time_ns; }
  void renew_last_access_time() { last_access_time_ns_ = event::get_hrtime(); }

  void set_avail_state() { cr_state_ = CR_AVAIL; }
  void set_initing_state() { cr_state_ = CR_INITING; }
  void set_init_failed_state() { cr_state_ = CR_INIT_FAILED; }
  void set_deleting_state() { cr_state_ = CR_DELETING; }
  bool is_avail() const { return (CR_AVAIL == cr_state_); }
  bool is_initing() const { return (CR_INITING == cr_state_); }
  bool is_init_failed() const { return (CR_INIT_FAILED == cr_state_); }
  bool is_deleting() const { return (CR_DELETING == cr_state_); }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool inc_and_test_deleting_complete();
  int stop_refresh_server_state();
  int stop_detect_server_state();
  bool is_default_cluster_resource() const { return (ObString::make_string(OB_PROXY_DEFAULT_CLUSTER_NAME) == get_cluster_name()); }
  bool is_metadb_cluster_resource() const { return (ObString::make_string(OB_META_DB_CLUSTER_NAME) == get_cluster_name()); }

  common::ObIArray<ObServerStateSimpleInfo> &get_server_state_info(const uint64_t version)
  {
    return server_state_info_[version % MAX_SERVER_STATE_INFO_ARRAY_SIZE];
  }

  common::DRWLock &get_server_state_lock(const uint64_t version)
  {
    return server_state_lock_[version % MAX_SERVER_STATE_INFO_ARRAY_SIZE];
  }

  proxy::ObSysVarSetProcessor &get_sysvar_set_processor() { return sys_var_set_processor_; }

  enum ObCRState
  {
    CR_BORN = 0,
    CR_INITING,
    CR_INIT_FAILED,
    CR_AVAIL,
    CR_DELETING,
    CR_DEAD
  };

  static const char *get_cr_state_str(const ObCRState state);
  const char *get_cr_state_str() const { return get_cr_state_str(cr_state_); }
  const common::ObString &get_cluster_name() const { return cluster_info_key_.cluster_name_.config_string_; }
  int64_t get_cluster_id() const { return cluster_info_key_.cluster_id_; }

  void destroy();

  int update_sys_ldg_info(ObSysLdgInfo *sys_ldg_info);
  bool check_tenant_valid(const common::ObString &tenant_name, const common::ObString &cluster_name);

  struct ObSysLdgInfoHashing
  {
    typedef const common::ObString &Key;
    typedef ObSysLdgInfo Value;
    typedef ObDLList(ObSysLdgInfo, sys_ldg_info_link_) ListHead;
    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->get_hash_key(); }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObSysLdgInfoHashing> ObSysLdgInfoMap;

  int update_location_tenant_info(common::ObSEArray<ObString, 4> &tenant_array,
                                  common::ObSEArray<ObString, 4> &locality_array,
                                  common::ObSEArray<ObString, 4> &primary_zone_array);
  uint64_t get_location_tenant_version(const ObString& tenant_name);
  int get_location_tenant_info(const ObString &tenant_name, ObLocationTenantInfo *&info_out);
  void destroy_location_tenant_info();

private:
  int set_cluster_info(const common::ObString &cluster_name, const int64_t cluster_id);

private:
  struct ObLocationTenantInfoHashing
  {
    typedef const ObString &Key;
    typedef ObLocationTenantInfo Value;
    typedef ObDLList(ObLocationTenantInfo, cr_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->tenant_name_; }
    static bool equal(Key lhs, Key rhs) { return (lhs == rhs); }
  };
  static const int64_t LTI_HASH_BUCKET_SIZE = 10;
  typedef common::hash::ObBuildInHashMap<ObLocationTenantInfoHashing, LTI_HASH_BUCKET_SIZE> ObLocationTenantInfoHashMap;

public:
  static const int64_t OP_LOCAL_NUM = 16;
  static const uint64_t MAX_SERVER_STATE_INFO_ARRAY_SIZE = 2;

  proxy::ObSysVarSetProcessor sys_var_set_processor_;
  ObCongestionManager congestion_manager_;
  ObServerStateRefreshCont *ss_refresh_cont_;
  ObDetectServerStateCont *detect_server_state_cont_;
  proxy::ObMysqlProxy mysql_proxy_;

  volatile uint64_t server_state_version_;
  common::ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> server_state_info_[MAX_SERVER_STATE_INFO_ARRAY_SIZE];
  mutable common::DRWLock server_state_lock_[MAX_SERVER_STATE_INFO_ARRAY_SIZE];

  mutable obsys::CRWLock dummy_entry_rwlock_;
  proxy::ObTableEntry *dummy_entry_;

  ObSafeSnapshotManager safe_snapshot_mgr_;

  ObClusterInfoKey cluster_info_key_;
  int64_t last_access_time_ns_;
  volatile int64_t deleting_completed_thread_num_;
  int64_t version_;
  volatile int64_t fetch_rslist_task_count_;
  volatile int64_t fetch_idc_list_task_count_;
  int64_t last_idc_list_refresh_time_ns_;
  int64_t last_rslist_refresh_time_ns_;
  common::ObAtomicList pending_list_;
  int64_t cluster_version_;
  common::hash::ObHashSet<net::ObIpEndpoint> alive_addr_set_;
  LINK(ObClusterResource, cr_link_);

private:
  bool is_inited_;
  ObCRState cr_state_;
  ObSysLdgInfoMap sys_ldg_info_map_;
  mutable common::DRWLock sys_ldg_info_lock_;
  common::DRWLock location_tenant_info_lock_;
  ObLocationTenantInfoHashMap location_tenant_info_map_;

  DISALLOW_COPY_AND_ASSIGN(ObClusterResource);
};

// used to delete cluster resource
struct ObResourceDeleteActor
{
public:
  ObResourceDeleteActor() : cr_(NULL), retry_count_(0) {}
  ~ObResourceDeleteActor() {}
  static ObResourceDeleteActor *alloc(ObClusterResource *cr);
  void free();
  bool is_valid() const { return (NULL != cr_); }
  ObClusterResource *cr_;
  int64_t retry_count_;
  LINK(ObResourceDeleteActor, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObResourceDeleteActor);
};

struct ObResourcePoolConfig
{
  ObResourcePoolConfig() { reset(); }
  ~ObResourcePoolConfig() { reset(); }
  void reset() { MEMSET(this, 0, sizeof(ObResourcePoolConfig)); }
  bool update(const ObProxyConfig &config);
  TO_STRING_KV(K_(long_async_task_timeout), K_(short_async_task_timeout),
               K_(server_state_refresh_interval), K_(metadb_server_state_refresh_interval),
               K_(min_keep_congestion_interval_us), K_(congestion_retry_interval_us),
               K_(congestion_fail_window_us), K_(congestion_failure_threshold),
               K_(server_detect_refresh_interval));

  int64_t long_async_task_timeout_;
  int64_t short_async_task_timeout_;
  int64_t server_state_refresh_interval_;
  int64_t metadb_server_state_refresh_interval_;
  int64_t min_keep_congestion_interval_us_;
  int64_t congestion_retry_interval_us_;
  int64_t congestion_fail_window_us_;
  int64_t congestion_failure_threshold_;
  int64_t server_detect_refresh_interval_;
};

class ObResourcePoolProcessor
{
public:
  ObResourcePoolProcessor()
    : is_inited_(false), default_sysvar_set_(NULL), default_cr_(NULL),
      meta_client_proxy_(NULL), first_cluster_name_str_(), newest_cluster_resource_version_(0),
      config_(), cr_map_rwlock_(), cr_map_() {}
  ~ObResourcePoolProcessor() { destroy(); }
  void destroy();

  int init(ObProxyConfig &config, proxy::ObMysqlProxy &meta_client_proxy);

  int get_cluster_resource(event::ObContinuation &cont,
                           process_cr_pfn process_resource_pool,
                           const bool is_proxy_mysql_client,
                           const common::ObString &cluster_name,
                           const int64_t cluster_id,
                           ObConnectionDiagnosisTrace *diagnosis_trace,
                           event::ObAction *&action);
  // Attention!! if succ, will inc_ref;
  ObClusterResource *acquire_avail_cluster_resource(const common::ObString &cluster_name, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID);
  ObClusterResource *acquire_cluster_resource(const common::ObString &cluster_name, const int64_t cluster_id);
  int acquire_all_avail_cluster_resource(common::ObIArray<ObClusterResource *> &cr_array);
  int acquire_all_cluster_resource(common::ObIArray<ObClusterResource *> &cr_array);
  void release_cluster_resource(ObClusterResource *cr);
  bool is_cluster_resource_avail(const common::ObString &cluster_name, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID);

  int create_cluster_resource(const common::ObString &cluster_name,
                              const int64_t cluster_id,
                              ObClusterResource *&created_cr,
                              event::ObContinuation &cont);
  int update_config_param();
  int get_recently_accessed_cluster_info(char *info_buf, const int64_t buf_len, int64_t &count);
  int set_first_cluster_name(const ObString &cluster_name);
  int get_first_cluster_name(ObString &cluster_name) const;
  int64_t acquire_cluster_resource_version() { return ATOMIC_AAF(&newest_cluster_resource_version_, 1); }
  ObDefaultSysVarSet *get_default_sysvar_set() { return default_sysvar_set_; }
  ObClusterResource *get_default_cluster_resource() { return default_cr_; }

  // Attention!! if delele metadb, must not use this fun directly, use rebuild_metadb instand
  int delete_cluster_resource(const ObString &cluster_name, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID);
  int rebuild_metadb(const bool ignore_cluster_not_exist = false);
  int add_cluster_delete_task(const common::ObString &cluster_name, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID);
  // when cluster resource count reach the high water mark, we will begin to expire;
  int expire_cluster_resource();

  DECLARE_TO_STRING;

  struct ObCRHashing
  {
    typedef const ObClusterInfoKey &Key;
    typedef ObClusterResource Value;
    typedef ObDLList(ObClusterResource, cr_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->cluster_info_key_; }
    static bool equal(Key lhs, Key rhs) { return (lhs == rhs); }
  };
  static const int64_t RP_HASH_BUCKET_SIZE = 32;
  typedef common::hash::ObBuildInHashMap<ObCRHashing, RP_HASH_BUCKET_SIZE> ObCRHashMap;

private:
  int init_cluster_resource_cont(event::ObContinuation &cont,
                                 const common::ObString &cluster_name,
                                 const int64_t cluster_id,
                                 ObConnectionDiagnosisTrace *diagnosis_trace,
                                 event::ObAction *&action);
  ObClusterResource *acquire_last_used_cluster_resource();

private:
  bool is_inited_;
  ObDefaultSysVarSet *default_sysvar_set_;
  ObClusterResource *default_cr_;
  proxy::ObMysqlProxy *meta_client_proxy_;
  common::ObString first_cluster_name_str_;
  char first_cluster_name_[OB_MAX_USER_NAME_LENGTH_STORE];
  volatile int64_t newest_cluster_resource_version_;

public:
  ObResourcePoolConfig config_;
  obsys::CRWLock cr_map_rwlock_; // used to protect cluster resource map
  ObCRHashMap cr_map_;
  common::hash::ObHashSet<net::ObIpEndpoint> ip_set_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObResourcePoolProcessor);
};

extern ObResourcePoolProcessor g_rp_processor;
inline ObResourcePoolProcessor &get_global_resource_pool_processor()
{
  return g_rp_processor;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RESOURCES_POOL_PROCESSOR_H
