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

#ifndef OBPROXY_TENANT_SERVER_H
#define OBPROXY_TENANT_SERVER_H
#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "proxy/route/ob_route_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObTenantServer
{
public:
  ObTenantServer() :
    is_inited_(false), server_count_(0), replica_count_(0), partition_count_(0),
    next_partition_idx_(0), server_array_(NULL)
  {}
  ~ObTenantServer() { destory(); }

  //locations must be ObSEArray
  int init(const common::ObIArray<ObProxyReplicaLocation> &locations);

  void destory();
  bool is_valid() const { return is_inited_; }
  uint64_t hash() const;
  int64_t count() const { return server_count_; }
  int64_t is_empty() const { return (NULL == server_array_ || 0 == server_count_); }
  int64_t replica_count(const int64_t partition_idx = 0) const;
  int64_t partition_count() const { return partition_count_; }
  uint64_t get_next_partition_index();
  int get_random_servers(ObProxyPartitionLocation &pl);
  //shuffle all replicas in each partition, partitions will not shuffle
  int shuffle();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  const ObProxyReplicaLocation *get_replica_location(const int64_t index) const;
  const ObProxyReplicaLocation *get_replica_location(const int64_t chosen_par_idx,
                                                     const int64_t init_replica_idx,
                                                     const int64_t idx) const;

  bool is_inited_;
  int64_t server_count_;        //server_list size
  int64_t replica_count_;       //replica_count_ of each partition
  uint64_t partition_count_;    //partition_count of this server list
  uint64_t next_partition_idx_; //use round robin way
  ObProxyReplicaLocation *server_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantServer);
};

inline int64_t ObTenantServer::replica_count(const int64_t partition_idx/* = 0*/) const
{
  return ((partition_idx == static_cast<int64_t>(partition_count_ - 1))
          ? server_count_ - replica_count_ * partition_idx
          : replica_count_);
}

inline const ObProxyReplicaLocation *ObTenantServer::get_replica_location(const int64_t index) const
{
  const ObProxyReplicaLocation *ret_replica = NULL;
  if (OB_LIKELY(is_inited_) && OB_LIKELY(index >= 0) && OB_LIKELY(index < server_count_)) {
    ret_replica = &(server_array_[index]);
  }
  return ret_replica;
}

//do no care the order
inline uint64_t ObTenantServer::hash() const
{
  uint64_t hash = 0;
  for (int64_t i = 0; i < server_count_; ++i) {
    hash += server_array_[i].server_.hash();
  }
  return hash;
}

inline const ObProxyReplicaLocation *ObTenantServer::get_replica_location(
    const int64_t chosen_par_idx, const int64_t init_replica_idx, const int64_t idx) const
{
  const ObProxyReplicaLocation *ret_replica = NULL;
  if (OB_LIKELY(is_inited_) && OB_LIKELY(replica_count_ > 0) && OB_LIKELY(server_count_ > 0)) {
    const int64_t tmp_replica_count = replica_count(chosen_par_idx);
    const int64_t server_idx = (((chosen_par_idx * replica_count_) + (idx + init_replica_idx) % tmp_replica_count) % server_count_);
    if (OB_LIKELY(server_idx >= 0)) {
      ret_replica = &(server_array_[server_idx]);
    }
  }
  return ret_replica;
}

inline uint64_t ObTenantServer::get_next_partition_index()
{
  uint64_t ret = 0;
  if (partition_count_ > 1) {
    ret = ATOMIC_FAA((&next_partition_idx_), 1) % partition_count_;
  }
  return ret;
}

DEF_TO_STRING(ObTenantServer)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(is_inited),
       K_(server_count),
       K_(replica_count),
       K_(partition_count),
       K_(next_partition_idx),
       KP_(server_array));
  J_COMMA();
  if (is_empty()) {
    J_KV("server_array_", "NULL");
  } else {
    J_KV("server_array_", common::ObArrayWrap<ObProxyReplicaLocation>(server_array_, server_count_));
  }
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_TENANT_SERVER_H */
