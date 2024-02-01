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

#include "proxy/route/ob_route_struct.h"
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

ObString ObProxyReplicaLocation::get_role_type_string(const ObRole role)
{
  static const ObString role_string_array[] =
  {
      ObString::make_string("INVALID_ROLE"),
      ObString::make_string("LEADER"),
      ObString::make_string("FOLLOWER"),
  };

  ObString string;
  if (OB_LIKELY(role >= INVALID_ROLE) && OB_LIKELY(role <= FOLLOWER)) {
    string = role_string_array[role];
  }
  return string;
}

ObString ObProxyReplicaLocation::get_replica_type_string(const ObReplicaType type)
{
  ObString string;
  switch (type) {
    case REPLICA_TYPE_FULL: {
      string = ObString::make_string("FULL");
      break;
    }
    case REPLICA_TYPE_BACKUP: {
      string = ObString::make_string("BACKUP");
      break;
    }
    case REPLICA_TYPE_LOGONLY: {
      string = ObString::make_string("LOGONLY");
      break;
    }
    case REPLICA_TYPE_READONLY: {
      string = ObString::make_string("READONLY");
      break;
    }
    case REPLICA_TYPE_MEMONLY: {
      string = ObString::make_string("MEMONLY");
      break;
    }
    case REPLICA_TYPE_ENCRYPTION_LOGONLY: {
      string = ObString::make_string("ENCRYPTION_LOGONLY");
      break;
    }
    case REPLICA_TYPE_MAX: {
      string = ObString::make_string("MAX");
      break;
    }
    default: {
      break;
    }
  }
  return string;
}

int64_t ObProxyReplicaLocation::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(server),
       K_(is_dup_replica),
       "role", get_role_type_string(role_),
       "type", get_replica_type_string(replica_type_));
  J_OBJ_END();
  return pos;
}

int ObProxyPartitionLocation::set_replicas(const common::ObIArray<ObProxyReplicaLocation> &replicas)
{
  int ret = OB_SUCCESS;
  //NOTE::leader must put into the first sit
  if (!replicas.empty()) {
    const int64_t alloc_size = static_cast<int64_t>(sizeof(ObProxyReplicaLocation)) * replicas.count();
    if (replica_count_ == replicas.count()) {
      MEMCPY(replicas_, &(replicas.at(0)), alloc_size);
    } else {
      destory();
      if (OB_ISNULL(replicas_ = static_cast<ObProxyReplicaLocation *>(op_fixed_mem_alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
      } else {
        MEMCPY(replicas_, &(replicas.at(0)), alloc_size);
        replica_count_ = replicas.count();
      }
    }
  } else {
    destory();
  }
  return ret;
}

ObProxyPartitionLocation &ObProxyPartitionLocation::operator=(const ObProxyPartitionLocation &other)
{
  if (this != &other) {
    if (other.is_valid()) {
      const int64_t alloc_size = static_cast<int64_t>(sizeof(ObProxyReplicaLocation)) * other.replica_count();
      if (replica_count_ == other.replica_count()) {
        MEMCPY(replicas_, other.replicas_, alloc_size);
      } else {
        destory();
        if (OB_ISNULL(replicas_ = static_cast<ObProxyReplicaLocation *>(op_fixed_mem_alloc(alloc_size)))) {
          LOG_WDIAG("fail to alloc mem", K(alloc_size));
        } else {
          MEMCPY(replicas_, other.replicas_ , alloc_size);
          replica_count_ = other.replica_count();
        }
      }
    } else {
      destory();
    }
  }
  return *this;
}

bool ObProxyPartitionLocation::check_and_update_server_changed(ObProxyPartitionLocation &other)
{
  bool is_server_changed = false;
  if (OB_UNLIKELY(!is_valid())
      || OB_UNLIKELY(!other.is_valid())
      || OB_UNLIKELY(is_server_changed_)
      || OB_UNLIKELY(other.is_server_changed_)) {
    is_server_changed = true;
  } else if (replica_count() != other.replica_count()) {
    is_server_changed = true;
  } else {
    bool is_exist = false;
    for (int i = 0; !is_server_changed && i < replica_count(); ++i) {
      is_exist = false;
      for (int j = 0; j < other.replica_count(); ++j) {
        if (replicas_[i].server_ == other.replicas_[j].server_) {
          is_exist = true;
          break;
        }
      }
      // if replica not found in other replica means server_changed
      if (!is_exist) {
        is_server_changed = true;
      }
    }
  }
  if (is_server_changed) {
    is_server_changed_ = true;
    other.is_server_changed_ = true;
  }
  return is_server_changed;
}

void ObProxyPartitionLocation::destory()
{
  if (NULL != replicas_ && replica_count_ > 0) {
    op_fixed_mem_free(replicas_, static_cast<int64_t>(sizeof(ObProxyReplicaLocation)) * replica_count_);
  }
  replicas_ = NULL;
  replica_count_ = 0;
}

int64_t ObProxyPartitionLocation::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(replica_count),
       "locations", ObArrayWrap<ObProxyReplicaLocation>(replicas_, replica_count_));
  J_OBJ_END();
  return pos;
}

const char *ObRouteEntry::get_route_entry_state(const ObRouteEntryState state)
{
  const char *name = NULL;
  switch (state) {
    case BORN:
      name = "BORN";
      break;
    case AVAIL:
      name = "AVAIL";
      break;
    case BUILDING:
      name = "BUILDING";
      break;
    case DIRTY:
      name = "DIRTY";
      break;
    case UPDATING:
      name = "UPDATING";
      break;
    case DELETED:
      name = "DELETED";
      break;
    default :
      name = "UNKNOWN";
      break;
  }
  return name;
}

int64_t ObRouteEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(ref_count),
       K_(cr_version),
       K_(cr_id),
       K_(create_time_us),
       K_(last_valid_time_us),
       K_(last_access_time_us),
       K_(last_update_time_us),
       K_(schema_version),
       K_(tenant_version),
       K_(time_for_expired),
       "state", get_route_entry_state(state_));
  J_OBJ_END();
  return pos;
}


int64_t ObTableEntryName::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_name),
       K_(tenant_name),
       K_(database_name),
       K_(package_name),
       K_(table_name));
  J_OBJ_END();
  return pos;
}

int ObTableEntryName::deep_copy(const ObTableEntryName &name,
                                char *buf_start,
                                const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_start) || (buf_len < name.get_total_str_len())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(buf_start), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("this table entry name is not valid", K(name), K(ret));
  } else {
    int64_t total_len = name.get_total_str_len();
    int64_t pos = 0;
    int64_t len = name.cluster_name_.length();
    MEMCPY(buf_start, name.cluster_name_.ptr(), len);
    cluster_name_.assign(buf_start, static_cast<int32_t>(len));
    pos += len;

    len = name.tenant_name_.length();
    MEMCPY(buf_start + pos, name.tenant_name_.ptr(), len);
    tenant_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    len = name.database_name_.length();
    MEMCPY(buf_start + pos, name.database_name_.ptr(), len);
    database_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    len = name.package_name_.length();
    MEMCPY(buf_start + pos, name.package_name_.ptr(), len);
    package_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    len = name.table_name_.length();
    MEMCPY(buf_start + pos, name.table_name_.ptr(), len);
    table_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    if (OB_UNLIKELY(pos != total_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to deep copy", K(pos), K(total_len), K(*this), K(ret));
    } else {
      LOG_DEBUG("succ deep copy ObTableEntryName", K(name), K(*this), K(total_len));
    }
  }

  return ret;
}

int64_t ObTableEntryKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(name), K_(cr_version), K_(cr_id));
  J_OBJ_END();
  return pos;
}

void ObRouteEntry::check_and_set_expire_time(const uint64_t tenant_version, const bool is_sys_dummy_entry)
{
  int64_t period_us = obutils::get_global_proxy_config().location_expire_period_time;
  const int64_t TENANT_LOCALITY_CHANGE_TIME = 60 * 1000 * 1000;
  const int64_t TENANT_LOCALITY_CHANGE_TIME_CONFIG = -1;
  if (AVAIL == state_ && !is_sys_dummy_entry) {
    if (tenant_version != tenant_version_) {
      tenant_version_ = tenant_version;
      // -1 means the change comes from a locality change
      current_expire_time_config_ = TENANT_LOCALITY_CHANGE_TIME_CONFIG;
      period_us = TENANT_LOCALITY_CHANGE_TIME;
      time_for_expired_ = ObRandomNumUtils::get_random_half_to_full(period_us) + common::ObTimeUtility::current_time();
    } else if (period_us != current_expire_time_config_ && TENANT_LOCALITY_CHANGE_TIME_CONFIG != current_expire_time_config_) {
      current_expire_time_config_ = period_us;
      if (period_us > 0) {
        time_for_expired_ = ObRandomNumUtils::get_random_half_to_full(period_us) + common::ObTimeUtility::current_time();
      } else if (period_us == 0) {
        time_for_expired_ = 0;
      } else {
        // period_us < 0, unexpected error
      }
    } else {
      // do nothing
    }
  }
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
