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

#include "iocore/eventsystem/ob_buf_allocator.h"
#include "proxy/route/ob_partition_entry.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int64_t ObPartitionEntryKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cr_version),
       K_(cr_id),
       K_(table_id),
       K_(partition_id));
  J_OBJ_END();
  return pos;
}

void ObPartitionEntry::free()
{
  LOG_DEBUG("ObPartitionEntry will be free", K(*this));
  op_reclaim_free(this);
}

int ObPartitionEntry::alloc_and_init_partition_entry(
    const uint64_t table_id, const uint64_t partition_id, const int64_t cr_version, const int64_t cr_id,
    const common::ObIArray<ObProxyReplicaLocation> &replicas,
    ObPartitionEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id
                  || cr_version <= 0
                  || cr_id < 0
                  || OB_INVALID_ID == partition_id)
                  || replicas.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(table_id), K(partition_id), K(cr_version), K(cr_id), K(replicas), K(ret));
  } else if (OB_ISNULL(entry = op_reclaim_alloc(ObPartitionEntry))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc parttition entry", K(ret));
  } else if (OB_FAIL(entry->set_pl(replicas))) {
    LOG_WDIAG("fail to set pl", K(replicas), K(ret));
    entry->free();
    entry = NULL;
  } else {
    entry->inc_ref();
    entry->set_table_id(table_id);
    entry->set_cr_version(cr_version);
    entry->set_cr_id(cr_id);
    entry->set_partition_id(partition_id);

    entry->renew_last_access_time();
    entry->renew_last_valid_time();
    entry->set_create_time();
    entry->set_avail_state();
  }
  return ret;
}

int ObPartitionEntry::alloc_and_init_partition_entry(
    const ObPartitionEntryKey &key,
    const common::ObIArray<ObProxyReplicaLocation> &replicas,
    ObPartitionEntry *&entry)
{
  return alloc_and_init_partition_entry(key.table_id_, key.partition_id_, key.cr_version_,
      key.cr_id_, replicas, entry);
}

int ObPartitionEntry::alloc_and_init_partition_entry(
    const ObPartitionEntryKey &key,
    const ObProxyReplicaLocation &replica,
    ObPartitionEntry *&entry)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObProxyReplicaLocation, 1> replicas;
  if (OB_FAIL(replicas.push_back(replica))) {
    LOG_WDIAG("fail to add replica location", K(replica), K(ret));
  } else if (OB_FAIL(alloc_and_init_partition_entry(key.table_id_, key.partition_id_,
      key.cr_version_, key.cr_id_, replicas, entry))) {
    LOG_WDIAG("fail to alloc and init partition entry", K(key), K(replicas), K(ret));
  } else {}
  return ret;
}


int64_t ObPartitionEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObRouteEntry::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(KP(this),
       K_(pl),
       K_(has_dup_replica),
       K_(table_id),
       K_(partition_id));
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
