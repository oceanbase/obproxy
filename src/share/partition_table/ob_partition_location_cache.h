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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_CACHE_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_CACHE_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/queue/ob_dedup_queue.h"
#include "common/cache/ob_kv_storecache.h"
#include "common/ob_srv_rpc_proxy.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_root_addr_agent.h"

#include "ob_partition_location.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObPartitionKey;
class ObTimeoutCtx;
}//end namespace common

namespace obrpc
{
class ObCommonRpcProxy;
}

namespace share
{
class ObRsMgr;
class ObPartitionTableOperator;
class ObIAliveServerTracer;

struct ObLocationCacheKey : public common::ObIKVCacheKey
{
  uint64_t table_id_;
  int64_t partition_id_;

  ObLocationCacheKey();
  ObLocationCacheKey(const uint64_t table_id, const int64_t partition_id);
  ~ObLocationCacheKey() { }
  virtual bool operator ==(const ObIKVCacheKey &other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  TO_STRING_KV(KT_(table_id), K_(partition_id));
};

struct ObLocationCacheValue : public common::ObIKVCacheValue
{
  int64_t size_;
  char *buffer_;

  ObLocationCacheValue();
  ~ObLocationCacheValue() { }
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  TO_STRING_KV(K_(size), "buffer", reinterpret_cast<int64_t>(buffer_));
};


class ObIPartitionLocationCache
{
public:
  enum PartitionLocationCacheType
  {
    PART_LOC_CACHE_TYPE_INVALID = 0,
    PART_LOC_CACHE_TYPE_NORMAL,
    PART_LOC_CACHE_TYPE_SQL,
  };
public:
  virtual ~ObIPartitionLocationCache() {}

  virtual ObIPartitionLocationCache::PartitionLocationCacheType get_type() const = 0;

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int get(const uint64_t table_id,
                  const int64_t partition_id,
                  ObPartitionLocation &location,
                  const int64_t expire_renew_time,
                  bool &is_cache_hit) = 0;

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int get(const common::ObPartitionKey &partition,
                  ObPartitionLocation &location,
                  const int64_t expire_renew_time,
                  bool &is_cache_hit) = 0;

  // get all partition locations of a table, virtual table should set partition_num to 1
  // return OB_LOCATION_NOT_EXIST if some partition doesn't have record in partition table
  virtual int get(const uint64_t table_id,
                  const int64_t partition_num,
                  common::ObIArray<ObPartitionLocation> &locations,
                  const int64_t expire_renew_time,
                  bool &is_cache_hit) = 0;

  // get leader addr of a partition, return OB_LOCATION_NOT_EXIST if not replica in partition
  // table, return OB_LOCATION_LEADER_NOT_EXIST if leader not exist
  virtual int get_leader(const common::ObPartitionKey &partition,
                         common::ObAddr &leader,
                         const bool force_renew = false) = 0;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const uint64_t table_id,
                           const int64_t partition_id,
                           ObPartitionLocation &location) = 0;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const common::ObPartitionKey &partition,
                           ObPartitionLocation &location) = 0;


  // return OB_LOCATION_NOT_EXIST if not all partition location of a table is in cache
  virtual int nonblock_get(const uint64_t table_id,
                           const int64_t partition_num,
                           common::ObIArray<ObPartitionLocation> &locations) = 0;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache,
  // return OB_LOCATION_LEADER_NOT_EXIST if partition's location in cache, but leader not exist
  virtual int nonblock_get_leader(const common::ObPartitionKey &partition,
                                  common::ObAddr &leader) = 0;

  // trigger a location update task and clear location in cache
  virtual int nonblock_renew(const common::ObPartitionKey &partition,
                             const int64_t expire_renew_time) = 0;
};

class ObILocationFetcher
{
public:
  virtual ~ObILocationFetcher() {}
  virtual int fetch_location(const uint64_t table_id,
                             const int64_t partiton_id,
                             ObPartitionLocation &location) = 0;
  // fetch virtual table locations
  virtual int fetch_vtable_location(const uint64_t table_id,
                                    common::ObSArray<ObPartitionLocation> &locations) = 0;
  virtual int renew_location_with_rpc(const ObPartitionLocation &cached_location,
                                   ObPartitionLocation &new_location,
                                   bool &is_new_location_valid) = 0;
  virtual bool treat_sql_as_timeout(int error_code);
protected:
  virtual int partition_table_fetch_location(ObPartitionTableOperator *pt,
                                             const uint64_t table_id,
                                             const int64_t partition_id,
                                             const bool update_timestamp,
                                             ObPartitionLocation &location);
};

// used by observer
class ObLocationFetcher : public ObILocationFetcher
{
public:
  static const int64_t OB_FETCH_LOCATION_TIMEOUT = 2 * 1000 * 1000;    //2s
  static const int64_t OB_FETCH_MEMBER_LIST_ADN_LEADER_TIMEOUT = 2 * 1000 * 1000;    //2s
  ObLocationFetcher();
  virtual ~ObLocationFetcher();
  int init(common::ObServerConfig &config,
           share::ObPartitionTableOperator &pt,
           ObRsMgr &rs_mgr,
           obrpc::ObCommonRpcProxy &rpc_proxy,
           obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  virtual int fetch_location(const uint64_t table_id,
                             const int64_t partition_id,
                             ObPartitionLocation &location);
  virtual int fetch_vtable_location(const uint64_t table_id,
                                    common::ObSArray<ObPartitionLocation> &locations);
  virtual int renew_location_with_rpc(const ObPartitionLocation &cached_location,
                                   ObPartitionLocation &new_location,
                                   bool &is_new_location_valid);
private:
  static int check_member_list(const common::ObIArray<ObReplicaLocation> &cached_member_list,
                             const common::ObIArray<common::ObAddr> &server_list,
                             bool &is_same);
  int check_is_prev_leader_change(
      const ObPartitionLocation &cached_location,
      common::ObIArray<obrpc::ObMemberListAndLeaderArg> &member_info_array,
      ObPartitionLocation &new_location,
      bool &is_new_location_valid,
      bool &is_member_list_same);
  int check_is_leader_in_prev_member(
      const ObPartitionLocation &cached_location,
      common::ObIArray<obrpc::ObMemberListAndLeaderArg> &member_info_array,
      ObPartitionLocation &new_location,
      bool &is_new_location_valid,
      bool &is_member_list_same);
  static uint64_t get_rpc_tenant_id(const uint64_t table_id);
private:
  bool inited_;
  common::ObServerConfig *config_;
  share::ObPartitionTableOperator *pt_;
  ObRsMgr *rs_mgr_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
};

class ObLocationUpdateTask;
class ObPartitionLocationCache : public ObIPartitionLocationCache
{
public:
  friend class ObLocationUpdateTask;

  class LocationSem
  {
  public:
    LocationSem();
    ~LocationSem();
    void set_max_count(const int64_t max_count);
    int acquire(const int64_t abs_timeout_us);
    int release();
  private:
    int64_t cur_count_;
    int64_t max_count_;
    common::ObThreadCond cond_;
  };

  explicit ObPartitionLocationCache(ObILocationFetcher &location_fetcher);
  virtual ~ObPartitionLocationCache();

  int init(common::ObServerConfig &config,
           ObIAliveServerTracer &server_tracer,
           const char *cache_name,
           const int64_t priority,
           bool auto_update);
  int destroy();

  int reload_config();

  virtual ObIPartitionLocationCache::PartitionLocationCacheType get_type() const
  {
    return ObIPartitionLocationCache::PART_LOC_CACHE_TYPE_NORMAL;
  }

  // get partition location of a partition
  virtual int get(const uint64_t table_id,
                  const int64_t partition_id,
                  ObPartitionLocation &location,
                  const int64_t expire_renew_time,
                  bool &is_cache_hit);

  // get partition location of a partition
  virtual int get(const common::ObPartitionKey &partition,
                  ObPartitionLocation &location,
                  const int64_t expire_renew_time,
                  bool &is_cache_hit);

  // get all partition locations of a table
  virtual int get(const uint64_t table_id,
                  const int64_t partition_num,
                  common::ObIArray<ObPartitionLocation> &locations,
                  const int64_t expire_renew_time,
                  bool &is_cache_hit);

  // get leader addr of a partition
  virtual int get_leader(const common::ObPartitionKey &partition,
                         common::ObAddr &leader,
                         const bool force_renew = false);

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const uint64_t table_id,
                           const int64_t partition_id,
                           ObPartitionLocation &location);

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const common::ObPartitionKey &partition,
                           ObPartitionLocation &location);

  // return OB_LOCATION_NOT_EXIST if not all partition location of a table is in cache
  virtual int nonblock_get(const uint64_t table_id,
                           const int64_t partition_num,
                           common::ObIArray<ObPartitionLocation> &locations);

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache,
  // return OB_LOCATION_LEADER_NOT_EXIST if partition's location in cache, but leader not exist
  virtual int nonblock_get_leader(const common::ObPartitionKey &partition,
                                  common::ObAddr &leader);

  // trigger a location update task and clear location in cache
  virtual int nonblock_renew(const common::ObPartitionKey &partition,
                             const int64_t expire_renew_time);

private:
  const static int64_t DEFAULT_FETCH_LOCATION_TIMEOUT_US =
      ObLocationFetcher::OB_FETCH_LOCATION_TIMEOUT + ObLocationFetcher::OB_FETCH_MEMBER_LIST_ADN_LEADER_TIMEOUT;// 4s
  static const int64_t OB_SYS_LOCATION_CACHE_BUCKET_NUM = 512;
  static const int64_t OB_MAX_LOCATION_SERIALIZATION_SIZE = 1024;
  typedef common::hash::ObHashMap<ObLocationCacheKey, ObPartitionLocation> NoSwapCache;
  typedef common::ObKVCache<ObLocationCacheKey, ObLocationCacheValue> KVCache;

  static int set_timeout_ctx(common::ObTimeoutCtx &ctx);

  int get_from_cache(const uint64_t table_id,
                     const int64_t partition_id,
                     const bool is_nonblock,
                     ObPartitionLocation &location);

  // renew location sync
  // if force_renew is false, we will try to get from cache again, because other thread
  // may has already fetched location
  // if ignore_redundant is true, if location's update timestamp till now is less than
  // expire_renew_time means need the location renew time larger than expire_renew_time.
  //    if exist location renew_time is not larger than expire_renew_time, renew is excuted; else renew is ignored
  int renew_location(const uint64_t table_id,
                     const int64_t partition_id,
                     ObPartitionLocation &location,
                     const int64_t expire_renew_time);

  int check_skip_rpc_renew(const ObPartitionLocation &location, bool &skip);

  // update location in cache
  int update_location(const uint64_t table_id,
                      const int64_t partition_id,
                      const ObPartitionLocation &location);
  // clear location in cache
  int clear_location(const uint64_t table_id,
                     const int64_t partiton_id,
                     const int64_t expire_renew_time);

  // virtual table related
  int vtable_get(const uint64_t table_id,
                 common::ObIArray<ObPartitionLocation> &locations,
                 const int64_t expire_renew_time,
                 bool &is_cache_hit);
  int get_from_vtable_cache(const uint64_t table_id,
                            common::ObSArray<ObPartitionLocation> &locations);
  int renew_vtable_location(const uint64_t table_id,
                            common::ObSArray<ObPartitionLocation> &locations);
  int update_vtable_location(const uint64_t table_id,
                             const common::ObSArray<ObPartitionLocation> &locations);

  int cache_value2location(const ObLocationCacheValue &cache_value,
                                                     ObPartitionLocation &location);
  int location2cache_value(const ObPartitionLocation &location,
                                                     char *buf, const int64_t buf_size,
                                                     ObLocationCacheValue &cache_value);
  template<typename LOCATION>
  static int cache_value2location(const ObLocationCacheValue &cache_value,
                           LOCATION &location);
  template<typename LOCATION>
  static int location2cache_value(const LOCATION &location,
                           char *buf, const int64_t buf_size,
                           ObLocationCacheValue &cache_value);

  // add location update task to suitable queue
  int add_update_task(const ObLocationUpdateTask &task);

private:
  static const int32_t ALL_ROOT_THREAD_CNT = 1;
  static const int32_t SYS_THREAD_CNT = 1;
  static const int64_t PLC_TASK_QUEUE_SIZE = 1024;
  static const int64_t PLC_TASK_MAP_SIZE = 1024;
  bool is_inited_;
  bool is_stopped_;
  ObILocationFetcher &location_fetcher_;
  common::ObServerConfig *config_;
  bool auto_update_;
  NoSwapCache sys_cache_;  //core or system table location cache, will not be swap out
  KVCache user_cache_;     //user table location cache
  // all core table's location don't need a queue because it fetch location through rpc
  common::ObDedupQueue all_root_update_queue_;
  common::ObDedupQueue sys_update_queue_;
  common::ObDedupQueue user_update_queue_;
  ObIAliveServerTracer *server_tracer_;
  LocationSem sem_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionLocationCache);
};

}//end namespace share
}//end namespace oceanbase

#endif
