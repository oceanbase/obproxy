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

#include "proxy/route/ob_tenant_server.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

//locations must has formatted, like: partition=P, replica=R, Leader=L, Follower=F
//P0(RL, RF, RF,...), P1(RL, RF, RF,...), P2(RL, RF, RF,...),...Px(RL, RF)
//
//1. each partition has only one leader only at the first site
//2. each partition has same count of replica, except the last one
int ObTenantServer::init(const ObIArray<ObProxyReplicaLocation> &locations)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(locations.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica location", K(locations), K(ret));
  } else if (OB_UNLIKELY(NULL != server_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_list_ should be null here", K(server_array_), K(ret));
  } else {
    const int64_t alloc_size = static_cast<int64_t>(sizeof(ObProxyReplicaLocation)) * locations.count();
    char *server_list_buf = NULL;
    if (OB_ISNULL(server_list_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      server_count_ = locations.count();
      server_array_ = new (server_list_buf) ObProxyReplicaLocation[server_count_];
      // Attention: Assuming that the object memory is stored continuously
      memcpy(server_array_, &(locations.at(0)), alloc_size);
      replica_count_ = 0;
      for (int64_t i = 0; i < server_count_ && OB_SUCC(ret); ++i) {
        if (LEADER == server_array_[i].role_) {
          server_array_[i].role_ = FOLLOWER;
          if (0 == replica_count_ && 0 != i) {
            replica_count_ = i;
          }
        }
      }

      if (replica_count_ <= 0) {
        replica_count_ = server_count_;
        partition_count_ = 1;
        next_partition_idx_ = 0;
      } else {
        partition_count_ = static_cast<uint64_t>((server_count_ / replica_count_ + (0 == server_count_ % replica_count_ ? 0 : 1)));
        int64_t idx = 0;
        if (OB_FAIL(ObRandomNumUtils::get_random_num(0, static_cast<int64_t>(partition_count_ - 1), idx))) {
          LOG_WARN("fail to get random num", "max", partition_count_ - 1, K(ret));
        } else {
          next_partition_idx_ = static_cast<uint64_t>(idx);
        }
      }
    }

    if (FAILEDx(shuffle())) {
      LOG_WARN("fail to shuffle server list", K(ret));
    } else {
      is_inited_ = true;
      LOG_DEBUG("succ to init ObTenantServer", K(*this), K(locations));
    }
  }
  return ret;
}

int ObTenantServer::shuffle()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(server_array_) || OB_UNLIKELY(replica_count_ <= 0) || OB_UNLIKELY(0 == partition_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(*this), K(ret));
  } else {
    int64_t iterator_begin = 0;
    int64_t iterator_end = 0;
    for (uint64_t i = 0; i < partition_count_ && OB_SUCC(ret); ++i) {
      iterator_begin = i * replica_count_;
      iterator_end = iterator_begin + replica_count(i);
      if (OB_ISNULL(server_array_ + iterator_begin)
          || OB_UNLIKELY(!server_array_[iterator_begin].is_valid())
          || OB_ISNULL(server_array_ + iterator_end - 1)
          || OB_UNLIKELY(!server_array_[iterator_end - 1].is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is unexpected", K(iterator_begin), K(iterator_end - 1),
                 KPC(server_array_ + iterator_begin), KPC(server_array_ + iterator_end - 1), K(ret));
      } else {
        std::random_shuffle(server_array_ + iterator_begin, server_array_ + iterator_end);
      }
    }
  }
  return ret;
}

int ObTenantServer::get_random_servers(ObProxyPartitionLocation &location)
{
  int ret = OB_SUCCESS;
  int64_t init_replica_idx = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("it has not inited", K_(is_inited), K(ret));
  } else if (OB_FAIL(ObRandomNumUtils::get_random_num(0, replica_count_ - 1, init_replica_idx))) {
    LOG_WARN("fail to get random num", "max", replica_count_ - 1, K(ret));
  } else {
    const int64_t MAX_REPLICA_COUNT = ObProxyPartitionLocation::OB_PROXY_REPLICA_COUNT;
    const uint64_t chosen_partition_idx = get_next_partition_index();
    const int64_t replica_size = replica_count(chosen_partition_idx);
    const int64_t avail_count = std::min(replica_size, MAX_REPLICA_COUNT);
    ObSEArray<ObProxyReplicaLocation, MAX_REPLICA_COUNT> replicas;
    const ObProxyReplicaLocation *replica = NULL;
    for (int64_t i = 0; i < avail_count && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(replica = get_replica_location(chosen_partition_idx, init_replica_idx, i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("it should not happened", K(chosen_partition_idx), K(init_replica_idx), K(i), K(*this), K(ret));
      } else if (OB_FAIL(replicas.push_back(*replica))) {
        PROXY_LOG(WARN, "fail to push_back replica", K(*replica), K(ret));
      } else {
        PROXY_LOG(DEBUG, "succ to add replica location", K(*replica));
      }
    }

    if (FAILEDx(location.set_replicas(replicas))) {
      LOG_WARN("fail to set replicas", K(replicas), K(ret));
    }
  }
  return ret;
}


void ObTenantServer::destory()
{
  if (NULL != server_array_ && server_count_ > 0) {
    op_fixed_mem_free(server_array_, static_cast<int64_t>(sizeof(ObProxyReplicaLocation)) * server_count_);
  }
  server_array_ = NULL;
  server_count_ = 0;
  replica_count_ = 0;
  partition_count_ = 0;
  next_partition_idx_ = 0;
  is_inited_ = false;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
