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

#ifndef OBPROXY_STATE_INFO_H
#define OBPROXY_STATE_INFO_H
#include "common/ob_zone_type.h"
#include "share/ob_server_status.h"
#include "share/ob_zone_info.h"
#include "obutils/ob_congestion_entry.h"
#include "proxy/route/ob_table_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
struct ObZoneStateInfo
{
  ObZoneStateInfo() { reset(); }
  ObZoneStateInfo(const ObZoneStateInfo &other);
  ~ObZoneStateInfo() {}
  bool is_valid() const;
  void reset();
  int set_zone_name(const common::ObString &zone_name);
  int set_region_name(const common::ObString &region_name);
  int set_idc_name(const common::ObString &idc_name);
  int64_t to_string(char *buffer, const int64_t size) const;
  bool is_readonly_zone() { return common::ZONE_TYPE_READONLY == zone_type_; }
  bool is_encryption_zone() { return common::ZONE_TYPE_ENCRYPTION == zone_type_; }
  ObZoneStateInfo &operator=(const ObZoneStateInfo &other);

  common::ObString zone_name_;
  common::ObString region_name_;
  common::ObString idc_name_;
  share::ObZoneStatus::Status zone_status_;
  common::ObZoneType zone_type_;
  obutils::ObCongestionZoneState::ObZoneState cgt_zone_state_;
  bool is_merging_;

private:
  char zone_name_buf_[common::MAX_ZONE_LENGTH];
  char region_name_buf_[common::MAX_REGION_LENGTH];
  char idc_name_buf_[common::MAX_PROXY_IDC_LENGTH];
};

struct ObServerStateInfo
{
  ObServerStateInfo();
  ~ObServerStateInfo() {}
  bool is_in_service() const { return (start_service_time_ > 0); }
  bool is_upgrading() const  { return (stop_time_ > 0); }
  bool is_treat_as_force_congested() const
  {
    //Attention:: when modify congestion about force_alive_congested and dead_congested
    //this should be update too.
    return ((ObCongestionEntry::INACTIVE == cgt_server_state_)//dead congested
            || (NULL != zone_state_ && ObCongestionZoneState::UPGRADE == zone_state_->cgt_zone_state_)//zone update
            || ObCongestionEntry::UPGRADE == cgt_server_state_);//server upgrade
  }
  bool is_valid() const;
  void reset();
  int add_addr(const char *ip, const int64_t port);
  int64_t to_string(char *buffer, const int64_t size) const;

  proxy::ObProxyReplicaLocation replica_;
  share::ObServerStatus::DisplayStatus server_status_;
  ObCongestionEntry::ObServerState cgt_server_state_;
  int64_t start_service_time_;
  int64_t stop_time_; // if stop_time_ > 0, always means this observer will upgrade
  ObZoneStateInfo *zone_state_; // must be at the end
};

struct ObServerStateSimpleInfo
{
  ObServerStateSimpleInfo() { reset(); }
  ObServerStateSimpleInfo(const ObServerStateSimpleInfo &other) { *this = other; }
  ~ObServerStateSimpleInfo() {}
  bool is_valid() const { return (addr_.is_valid()
                                  && !zone_name_.empty()
                                  && !region_name_.empty()
                                  && !idc_name_.empty()
                                  && common::ZONE_TYPE_INVALID != zone_type_); }
  void reset();
  int set_addr(const common::ObAddr &addr);
  int set_addr(const char *ip, const int64_t port);
  int set_zone_name(const common::ObString &zone_name);
  int set_region_name(const common::ObString &region_name);
  int set_idc_name(const common::ObString &idc_name);
  ObServerStateSimpleInfo &operator=(const ObServerStateSimpleInfo &other);
  int64_t to_string(char *buffer, const int64_t size) const;

public:
  common::ObAddr addr_;
  common::ObString zone_name_;
  common::ObString region_name_;
  common::ObString idc_name_;
  common::ObZoneType zone_type_;
  bool is_merging_;
  bool is_force_congested_;
  // The number of SQLs currently being executed by the server
  int64_t request_sql_cnt_;
  // The last time the request was sent
  int64_t last_response_time_;
  int64_t detect_fail_cnt_;
private:
  char zone_name_buf_[common::MAX_ZONE_LENGTH];
  char region_name_buf_[common::MAX_REGION_LENGTH];
  char idc_name_buf_[common::MAX_PROXY_IDC_LENGTH];
};

//-------------------------------ObZoneStateInfo-------------------------------------//
inline ObZoneStateInfo &ObZoneStateInfo::operator=(const ObZoneStateInfo &other)
{
  if (this != &other) {
    is_merging_ = other.is_merging_;
    zone_status_ = other.zone_status_;
    zone_type_ = other.zone_type_;
    cgt_zone_state_ = other.cgt_zone_state_;
    if (other.zone_name_.empty()) {
      zone_name_.reset();
    } else {
      MEMCPY(zone_name_buf_, other.zone_name_buf_, other.zone_name_.length());
      zone_name_.assign_ptr(zone_name_buf_, other.zone_name_.length());
    }

    if (other.region_name_.empty()) {
      region_name_.reset();
    } else {
      MEMCPY(region_name_buf_, other.region_name_buf_, other.region_name_.length());
      region_name_.assign_ptr(region_name_buf_, other.region_name_.length());
    }

    if (other.idc_name_.empty()) {
      idc_name_.reset();
    } else {
      MEMCPY(idc_name_buf_, other.idc_name_buf_, other.idc_name_.length());
      idc_name_.assign_ptr(idc_name_buf_, other.idc_name_.length());
    }
  }
  return *this;
}

inline ObZoneStateInfo::ObZoneStateInfo(const ObZoneStateInfo &other)
{
  *this = other;
}

inline void ObZoneStateInfo::reset()
{
  memset(this, 0, sizeof(ObZoneStateInfo));
  is_merging_ = false;
  zone_status_ = share::ObZoneStatus::UNKNOWN;
  cgt_zone_state_ = ObCongestionZoneState::ACTIVE;
  zone_type_ = ZONE_TYPE_INVALID;
};

inline int ObZoneStateInfo::set_zone_name(const ObString &zone_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(zone_name.empty()) || OB_UNLIKELY(zone_name.length() > MAX_ZONE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(zone_name), K(ret));
  } else {
    MEMCPY(zone_name_buf_, zone_name.ptr(), zone_name.length());
    zone_name_.assign_ptr(zone_name_buf_, zone_name.length());
  }
  return ret;
}

inline int ObZoneStateInfo::set_region_name(const ObString &region_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(region_name.empty()) || OB_UNLIKELY(region_name.length() > MAX_REGION_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(region_name), K(ret));
  } else {
    MEMCPY(region_name_buf_, region_name.ptr(), region_name.length());
    region_name_.assign_ptr(region_name_buf_, region_name.length());
  }
  return ret;
}

inline int ObZoneStateInfo::set_idc_name(const ObString &idc_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(idc_name.empty()) || OB_UNLIKELY(idc_name.length() > MAX_PROXY_IDC_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(idc_name), K(ret));
  } else {
    MEMCPY(idc_name_buf_, idc_name.ptr(), idc_name.length());
    idc_name_.assign_ptr(idc_name_buf_, idc_name.length());
  }
  return ret;
}


inline bool ObZoneStateInfo::is_valid() const
{
  bool bret = false;
  if ((!zone_name_.empty())
      && !region_name_.empty()
      && !idc_name_.empty()
      && (zone_status_ < share::ObZoneStatus::UNKNOWN)
      && (zone_status_ >= share::ObZoneStatus::INACTIVE)
      && (zone_type_ >= common::ZONE_TYPE_READWRITE)
      && (zone_type_ < common::ZONE_TYPE_INVALID)) {
    bret = true;
  }
  return bret;
}

inline int64_t ObZoneStateInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const char *zone_status_str = share::ObZoneStatus::get_status_str(zone_status_);
  const char *cgt_zone_state_str = ObCongestionZoneState::get_zone_state_name(cgt_zone_state_);
  const char *zone_type_str = common::zone_type_to_str(zone_type_);
  J_OBJ_START();
  J_KV(K_(zone_name), K_(region_name), K_(idc_name), K_(is_merging),
       K(zone_status_str), K(cgt_zone_state_str), K(zone_type_str));
  J_OBJ_END();
  return pos;
}

//-------------------------------ObServerStateInfo-------------------------------------//
inline ObServerStateInfo::ObServerStateInfo()
{
  reset();
}

inline void ObServerStateInfo::reset()
{
  memset(this, 0, sizeof(ObServerStateInfo));
  replica_.reset();
  server_status_ = share::ObServerStatus::OB_DISPLAY_MAX;
  cgt_server_state_ = ObCongestionEntry::ACTIVE;
  start_service_time_ = OB_INVALID_TIMESTAMP;
  stop_time_ = OB_INVALID_TIMESTAMP;
  zone_state_ = NULL;
}

inline bool ObServerStateInfo::is_valid() const
{
  return (replica_.is_valid())
         && (NULL != zone_state_)
         && (share::ObServerStatus::OB_DISPLAY_MAX != server_status_)
         && (OB_INVALID_TIMESTAMP != start_service_time_)
         && (OB_INVALID_TIMESTAMP != stop_time_);
}

inline int64_t ObServerStateInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const char *server_status_str = NULL;
  // for logging, ignore result and do not check NULL string.
  share::ObServerStatus::display_status_str(server_status_, server_status_str);

  const char *cgt_server_state_str = ObCongestionEntry::get_server_state_name(cgt_server_state_);
  J_OBJ_START();
  J_KV(K_(replica), KPC_(zone_state), K(server_status_str), K(cgt_server_state_str),
       K_(start_service_time), K_(stop_time));
  J_OBJ_END();
  return pos;
}

inline int ObServerStateInfo::add_addr(const char *ip, const int64_t port)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(replica_.add_addr(ip, static_cast<int32_t>(port)))) {
    PROXY_LOG(WDIAG, "fail to add addr", K(ip), K(port), K(ret));
  }
  return ret;
}

//-------------------------------ObServerStateSimpleInfo-------------------------------------//
inline ObServerStateSimpleInfo &ObServerStateSimpleInfo::operator=(const ObServerStateSimpleInfo &other)
{
  if (this != &other) {
    addr_ = other.addr_;
    is_merging_ = other.is_merging_;
    is_force_congested_ = other.is_force_congested_;
    zone_type_ = other.zone_type_;
    request_sql_cnt_ = other.request_sql_cnt_;
    last_response_time_ = other.last_response_time_;
    detect_fail_cnt_ = other.detect_fail_cnt_;
    if (other.zone_name_.empty()) {
      zone_name_.reset();
    } else {
      MEMCPY(zone_name_buf_, other.zone_name_buf_, other.zone_name_.length());
      zone_name_.assign_ptr(zone_name_buf_, other.zone_name_.length());
    }

    if (other.region_name_.empty()) {
      region_name_.reset();
    } else {
      MEMCPY(region_name_buf_, other.region_name_buf_, other.region_name_.length());
      region_name_.assign_ptr(region_name_buf_, other.region_name_.length());
    }

    if (other.idc_name_.empty()) {
      idc_name_.reset();
    } else {
      MEMCPY(idc_name_buf_, other.idc_name_buf_, other.idc_name_.length());
      idc_name_.assign_ptr(idc_name_buf_, other.idc_name_.length());
    }
  }
  return *this;
}

inline void ObServerStateSimpleInfo::reset()
{
  memset(this, 0, sizeof(ObServerStateSimpleInfo));
  zone_type_ = common::ZONE_TYPE_INVALID;
}

inline int ObServerStateSimpleInfo::set_addr(const common::ObAddr &addr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(addr), K(ret));
  } else {
    addr_ = addr;
  }
  return ret;
}

inline int ObServerStateSimpleInfo::set_addr(const char *ip, const int64_t port)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!addr_.set_ip_addr(ip, static_cast<int32_t>(port)))) {
    ret = common::OB_INVALID_ARGUMENT;
  }
  return ret;
}

inline int ObServerStateSimpleInfo::set_zone_name(const ObString &zone_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(zone_name.empty()) || OB_UNLIKELY(zone_name.length() > MAX_ZONE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(zone_name), K(ret));
  } else {
    MEMCPY(zone_name_buf_, zone_name.ptr(), zone_name.length());
    zone_name_.assign_ptr(zone_name_buf_, zone_name.length());
  }
  return ret;
}

inline int ObServerStateSimpleInfo::set_region_name(const ObString &region_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(region_name.empty()) || OB_UNLIKELY(region_name.length() > MAX_REGION_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(region_name), K(ret));
  } else {
    MEMCPY(region_name_buf_, region_name.ptr(), region_name.length());
    region_name_.assign_ptr(region_name_buf_, region_name.length());
  }
  return ret;
}

inline int ObServerStateSimpleInfo::set_idc_name(const ObString &idc_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(idc_name.empty()) || OB_UNLIKELY(idc_name.length() > MAX_PROXY_IDC_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(idc_name), K(ret));
  } else {
    MEMCPY(idc_name_buf_, idc_name.ptr(), idc_name.length());
    idc_name_.assign_ptr(idc_name_buf_, idc_name.length());
  }
  return ret;
}

inline int64_t ObServerStateSimpleInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(addr),
       K_(zone_name),
       K_(region_name),
       K_(idc_name),
       "zone_type", common::zone_type_to_str(zone_type_),
       K_(is_merging),
       K_(is_force_congested),
       K_(request_sql_cnt),
       K_(last_response_time),
       K_(detect_fail_cnt));
  J_OBJ_END();
  return pos;
}

//-------------------------------ObSysLdgInfo-------------------------------------//
struct ObSysLdgInfo
{
  ObSysLdgInfo() : tenant_id_(-1), tenant_name_(), cluster_id_(-1), cluster_name_(),
   ldg_role_(), ldg_status_(), ldg_hash_key_() { reset(); }
  ~ObSysLdgInfo() {}
  void reset();
  int set_tenant_name(const common::ObString &tenant_name);
  int set_cluster_name(const common::ObString &cluster_name);
  int set_ldg_role(const common::ObString &ldg_role);
  int set_ldg_status(const common::ObString &ldg_status);
  common::ObString& get_hash_key()
  {
    if (ldg_hash_key_.empty()) {
      int32_t len = 0;
      char ch = '#';
      MEMCPY(ldg_hash_key_buf_, tenant_name_.ptr(), tenant_name_.length());
      len += tenant_name_.length();
      MEMCPY(ldg_hash_key_buf_ + len, &ch, 1);
      len += 1;
      MEMCPY(ldg_hash_key_buf_ + len, cluster_name_.ptr(), cluster_name_.length());
      len += cluster_name_.length();
      ldg_hash_key_buf_[len] = '\0';
      ldg_hash_key_.assign_ptr(ldg_hash_key_buf_, len);
    }
    return ldg_hash_key_;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(tenant_name), K_(cluster_name), K_(ldg_role));
    J_OBJ_END();
    return pos;
  }
  int64_t tenant_id_;
  common::ObString tenant_name_;
  int64_t cluster_id_;
  common::ObString cluster_name_;
  common::ObString ldg_role_;
  common::ObString ldg_status_;
  common::ObString ldg_hash_key_;
  LINK(ObSysLdgInfo, sys_ldg_info_link_);
private:
  static const int64_t MAX_SYS_LDG_INFO_LENGTH = 128;
  char tenant_name_buf_[OB_MAX_TENANT_NAME_LENGTH];
  char cluster_name_buf_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];
  char ldg_role_buf_[MAX_SYS_LDG_INFO_LENGTH];
  char ldg_status_buf_[MAX_SYS_LDG_INFO_LENGTH];
  char ldg_hash_key_buf_[OB_PROXY_FULL_USER_NAME_MAX_LEN];
};

inline void ObSysLdgInfo::reset()
{
  memset(tenant_name_buf_, 0, sizeof(tenant_name_buf_));
  memset(cluster_name_buf_, 0, sizeof(cluster_name_buf_));
  memset(ldg_role_buf_, 0, sizeof(ldg_role_buf_));
  memset(ldg_status_buf_, 0, sizeof(ldg_status_buf_));
  memset(ldg_hash_key_buf_, 0, sizeof(ldg_hash_key_buf_));
}

int ObSysLdgInfo::set_tenant_name(const common::ObString &tenant_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(tenant_name.empty()) || OB_UNLIKELY(tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(tenant_name), K(ret));
  } else {
    MEMCPY(tenant_name_buf_, tenant_name.ptr(), tenant_name.length());
    tenant_name_.assign_ptr(tenant_name_buf_, tenant_name.length());
  }
  return ret;
}
int ObSysLdgInfo::set_cluster_name(const common::ObString &cluster_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(cluster_name), K(ret));
  } else {
    MEMCPY(cluster_name_buf_, cluster_name.ptr(), cluster_name.length());
    cluster_name_.assign_ptr(cluster_name_buf_, cluster_name.length());
  }
  return ret;
}
int ObSysLdgInfo::set_ldg_role(const common::ObString &ldg_role)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(ldg_role.empty()) || OB_UNLIKELY(ldg_role.length() > MAX_SYS_LDG_INFO_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(ldg_role), K(ret));
  } else {
    MEMCPY(ldg_role_buf_, ldg_role.ptr(), ldg_role.length());
    ldg_role_.assign_ptr(ldg_role_buf_, ldg_role.length());
  }
  return ret;
}
int ObSysLdgInfo::set_ldg_status(const common::ObString &ldg_status)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(ldg_status.empty()) || OB_UNLIKELY(ldg_status.length() > MAX_SYS_LDG_INFO_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(ldg_status), K(ret));
  } else {
    MEMCPY(ldg_status_buf_, ldg_status.ptr(), ldg_status.length());
    ldg_status_.assign_ptr(ldg_status_buf_, ldg_status.length());
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_SERVER_STATE_PROCESSOR_H
