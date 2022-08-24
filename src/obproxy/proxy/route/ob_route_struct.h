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

#ifndef OBPROXY_ROUTE_STRUCT_H
#define OBPROXY_ROUTE_STRUCT_H

#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/time/ob_hrtime.h"
#include "common/ob_role.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "iocore/eventsystem/ob_thread.h"
#include "obutils/ob_proxy_config.h"
#include "stat/ob_processor_stats.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define CHECK_SER_INPUT_VALUE_VALID \
do { \
  if (OB_SUCC(ret)) { \
    if (OB_ISNULL(buf) \
        || OB_UNLIKELY(len <= 0) \
        || OB_UNLIKELY(pos < 0)) { \
      ret = common::OB_INVALID_ARGUMENT; \
      PROXY_LOG(WARN, "invalid argument", KP(buf), K(len), K(pos), K(ret)); \
    } else if (OB_UNLIKELY(pos >= len)) { \
      ret = common::OB_SIZE_OVERFLOW; \
      PROXY_LOG(WARN, "buf is no enough", KP(buf), K(len), K(pos), K(ret)); \
    } \
  } \
} while(0)

#define OB_FB_SER_START \
  int ret = common::OB_SUCCESS; \
  { \
    CHECK_SER_INPUT_VALUE_VALID;

#define OB_FB_SER_END \
  } \
  return ret;

#define OB_FB_DESER_START OB_FB_SER_START
#define OB_FB_DESER_END OB_FB_SER_END

#define OB_FB_DECODE_INT(num, type) \
do { \
  if (OB_SUCC(ret) && (pos < len)) { \
    uint64_t tmp_num = 0; \
    const char *tmp_buf_start = buf + pos; \
    if (OB_FAIL(::oceanbase::obmysql::ObMySQLUtil::get_length(tmp_buf_start, tmp_num))) { \
      PROXY_LOG(ERROR, "fail to get length", K(pos), K(ret)); \
    } else if (FALSE_IT(pos += (tmp_buf_start - buf - pos))) { \
    } else if (OB_UNLIKELY(pos > len)) { \
      ret = common::OB_ERR_UNEXPECTED; \
      PROXY_LOG(ERROR, "invalid pos or len", K(pos), K(len), K(num), K(ret)); \
    } else { \
      num = static_cast<type>(tmp_num); \
    }\
  } \
} while (0)

struct ObProxyReplicaLocation
{
public:
  ObProxyReplicaLocation() : is_dup_replica_(false), server_(), role_(common::FOLLOWER), replica_type_(common::REPLICA_TYPE_FULL) {}
  ObProxyReplicaLocation(const common::ObAddr &server, const common::ObRole role, const common::ObReplicaType replica_type)
    : is_dup_replica_(false), server_(server), role_(role), replica_type_(replica_type) {}
  ~ObProxyReplicaLocation() { reset(); }

  void reset();
  bool is_valid() const;
  bool operator==(const ObProxyReplicaLocation &other) const;
  bool operator!=(const ObProxyReplicaLocation &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int add_addr(const char *ip, const int64_t port);
  int set_replica_type(const int32_t replica_type);
  common::ObReplicaType get_replica_type() const { return replica_type_; }
  bool is_leader() const { return common::LEADER == role_; }
  bool is_follower() const { return common::FOLLOWER == role_; }
  static common::ObString get_role_type_string(const common::ObRole role);
  static common::ObString get_replica_type_string(const common::ObReplicaType type);
  bool is_full_replica() const { return common::REPLICA_TYPE_FULL == replica_type_; }
  void set_dup_replica_type(const int32_t dup_replica_type);
  bool is_dup_replica() const { return is_dup_replica_; }
  bool is_weak_read_avail() const
  {
    return common::REPLICA_TYPE_FULL == replica_type_
           || common::REPLICA_TYPE_READONLY == replica_type_;
  }
  bool is_readonly_replica() const { return common::REPLICA_TYPE_READONLY == replica_type_; }

  bool is_dup_replica_;
  common::ObAddr server_;
  common::ObRole role_;
  common::ObReplicaType replica_type_;
};

struct ObProxyRerouteInfo
{
public:
  ObProxyRerouteInfo() : schema_version_(0), replica_()
  {
    table_name_buf_[0]  = '\0';
  }
  ~ObProxyRerouteInfo() { reset(); }

  void reset()
  {
    schema_version_ = 0;
    replica_.reset();
    table_name_buf_[0] = '\0';
  }
  TO_STRING_KV(K_(schema_version), K_(replica), K_(table_name_buf));

  int deserialize_struct(const char *buf, const int64_t len, int64_t &pos);
  int deserialize_struct_content(const char *buf, const int64_t len, int64_t &pos);

public:
  int64_t schema_version_;
  ObProxyReplicaLocation replica_;
  char table_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH + 1];
};


int ObProxyRerouteInfo::deserialize_struct(const char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  int64_t struct_len = 0;
  // read struct len
  OB_FB_DECODE_INT(struct_len, int64_t);

  // deseri struct content
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(struct_len <= 0)) {
      ret = common::OB_INVALID_DATA;
      PROXY_LOG(ERROR, "struct_len must > 0", K(pos), K(struct_len), K(len), K(ret));
    } else if ((pos + struct_len) > len) {
      ret = common::OB_INVALID_DATA;
      PROXY_LOG(ERROR, "invalid data buf", K(pos), K(struct_len), K(len), K(ret));
    } else {
      int64_t orig_pos = pos;
      int64_t curr_len = pos + struct_len;

      if (OB_FAIL(deserialize_struct_content(buf, curr_len, pos))) {
        PROXY_LOG(ERROR, "fail to deserialize_struct_content", K(buf), K(curr_len), K(pos), K(ret));
      } else {
        if (pos < orig_pos + struct_len) {
          // means current is old version, just skip
          pos = orig_pos + struct_len;
        } else if (OB_UNLIKELY(pos > orig_pos + struct_len)) {
          // impossible, just for defense
          ret = common::OB_ERR_UNEXPECTED;
          PROXY_LOG(ERROR, "unexpect error", K(pos), K(orig_pos), K(struct_len), K(len), K(ret));
        } else {
          // pos == orig_pos + struct_len, normal case
        }
      }
    }
  }
  OB_FB_DESER_END;
}

int ObProxyRerouteInfo::deserialize_struct_content(const char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  int32_t version = 0;
  int32_t ipv4 = 0;
  int32_t port = 0;
  uint64_t ipv6_high = 0;
  uint64_t ipv6_low = 0;
  OB_FB_DECODE_INT(version, int32_t);

  if (ObAddr::IPV4 == version) {
    OB_FB_DECODE_INT(ipv4, int32_t);
  } else if (ObAddr::IPV6 == version) {
    OB_FB_DECODE_INT(ipv6_high, uint64_t);
    OB_FB_DECODE_INT(ipv6_low, uint64_t);
  } else {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(ERROR, "invalid ip version", K(version), K(ret));
  }
  OB_FB_DECODE_INT(port, int32_t);
  OB_FB_DECODE_INT(replica_.role_, common::ObRole);
  OB_FB_DECODE_INT(replica_.replica_type_, common::ObReplicaType);
  if (OB_SUCC(ret)) {
    if (ObAddr::IPV4 == version) {
      replica_.server_.set_ipv4_addr(ipv4, port);
    } else {
      // must IPV6, not support yet, TODO open below later
      //server_.set_ipv6_addr(ipv6_high, ipv6_low, port);
      replica_.server_.reset();
    }
  }
  int64_t table_name_len = 0;
  OB_FB_DECODE_INT(table_name_len, int64_t);
  if (OB_SUCC(ret) && 0 != table_name_len) {
    if (OB_UNLIKELY(table_name_len > common::OB_MAX_TABLE_NAME_LENGTH)
        || OB_UNLIKELY(table_name_len < 0)) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(ERROR, "invalid table name len", K(pos), K(len), K(table_name_len), K(ret));
    } else if (pos + table_name_len > len) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(ERROR, "invalid pos or len", K(pos), K(len), K(table_name_len), K(ret));
    } else {
      MEMCPY(table_name_buf_, buf + pos, table_name_len);
      table_name_buf_[table_name_len] = '\0';
      pos += table_name_len;
    }
  }
  OB_FB_DECODE_INT(schema_version_, int64_t);
  OB_FB_DESER_END;
}

inline int ObProxyReplicaLocation::add_addr(const char *ip, const int64_t port)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == ip || port <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!server_.set_ipv4_addr(ip, static_cast<int32_t>(port)))) {
    ret = common::OB_INVALID_ARGUMENT;
  }
  return ret;
}

inline int ObProxyReplicaLocation::set_replica_type(const int32_t replica_type)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!common::ObReplicaTypeCheck::is_replica_type_valid(replica_type))) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    replica_type_ = static_cast<common::ObReplicaType>(replica_type);
  }
  return ret;
}

inline void ObProxyReplicaLocation::set_dup_replica_type(const int32_t dup_replica_type)
{
  if (dup_replica_type > 0) {
    is_dup_replica_ = true;
  } else {
    is_dup_replica_ = false;
  }
}

inline void ObProxyReplicaLocation::reset()
{
  is_dup_replica_ = false;
  server_.reset();
  role_ = common::FOLLOWER;
  replica_type_ = common::REPLICA_TYPE_FULL;
}

inline bool ObProxyReplicaLocation::is_valid() const
{
  return (server_.is_valid()
          && common::INVALID_ROLE != role_
          && common::ObReplicaTypeCheck::is_replica_type_valid(replica_type_));
}

inline bool ObProxyReplicaLocation::operator==(const ObProxyReplicaLocation &other) const
{
  return (server_ == other.server_)
          && (role_ == other.role_)
          && (replica_type_ == other.replica_type_)
          && (is_dup_replica_ == other.is_dup_replica_);
}

inline bool ObProxyReplicaLocation::operator!=(const ObProxyReplicaLocation &other) const
{
  return !(*this == other);
}

class ObProxyPartitionLocation
{
public:
  static const int64_t OB_PROXY_REPLICA_COUNT = common::OB_MAX_MEMBER_NUMBER;

  ObProxyPartitionLocation() : replica_count_(0), replicas_(NULL), is_server_changed_(false) {}
  ObProxyPartitionLocation(const ObProxyPartitionLocation &other);
  ~ObProxyPartitionLocation() { destory(); }
  void destory();

  bool is_valid() const { return NULL != replicas_ && replica_count_ > 0; }
  int64_t replica_count() const { return replica_count_; }
  bool exist_leader() const;
  ObProxyReplicaLocation *get_leader() const;
  ObProxyReplicaLocation *get_replica(const int64_t index) const;
  uint64_t get_all_server_hash() const;

  bool is_server_changed() const { return is_server_changed_; }
  void mark_server_unchanged() { is_server_changed_ = false; }
  bool check_and_update_server_changed(ObProxyPartitionLocation &other);

  int set_replicas(const common::ObIArray<ObProxyReplicaLocation> &replicas);
  ObProxyPartitionLocation &operator=(const ObProxyPartitionLocation &other);
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int64_t replica_count_;
  ObProxyReplicaLocation *replicas_;
  bool is_server_changed_;
};

inline bool ObProxyPartitionLocation::exist_leader() const
{
  bool found = false;
  //NOTE::leader must put into the first sit
  if (is_valid()
      && common::LEADER == replicas_[0].role_
      && replicas_[0].is_valid()) {
    found = true;
  }
  return found;
}

inline ObProxyReplicaLocation *ObProxyPartitionLocation::get_leader() const
{
  ObProxyReplicaLocation *replica = NULL;
  //NOTE::leader must put into the first sit
  if (is_valid()
      && common::LEADER == replicas_[0].role_
      && replicas_[0].is_valid()) {
    replica = (replicas_ + 0);
  }
  return replica;
}

inline ObProxyReplicaLocation *ObProxyPartitionLocation::get_replica(const int64_t index) const
{
  ObProxyReplicaLocation *replica = NULL;
  if (is_valid() && index >= 0 && index < replica_count_) {
    replica = (replicas_+index);
  }
  return replica;
}

//do no care the order
inline uint64_t ObProxyPartitionLocation::get_all_server_hash() const
{
  uint64_t hash = 0;
  if (is_valid()) {
    for (int64_t i = 0; i < replica_count_; ++i) {
      hash += replicas_[i].server_.hash();
    }
  }
  return hash;
}

inline ObProxyPartitionLocation::ObProxyPartitionLocation(const ObProxyPartitionLocation &other)
{
  *this = other;
}

class ObRouteEntry : public common::ObSharedRefCount
{
public:
  enum ObRouteEntryState
  {
    BORN = 0,
    BUILDING,
    AVAIL,
    DIRTY,
    UPDATING,
    DELETED
  };

  ObRouteEntry()
    : common::ObSharedRefCount(), cr_version_(-1), cr_id_(common::OB_INVALID_CLUSTER_ID), schema_version_(0), create_time_us_(0),
      last_valid_time_us_(0), last_access_time_us_(0), last_update_time_us_(0),
      state_(BORN), tenant_version_(0), time_for_expired_(0), current_expire_time_config_(0) {}
  virtual ~ObRouteEntry() {}
  virtual void free() = 0;

  void set_building_state() { state_ = BUILDING; }
  void set_avail_state() { state_ = AVAIL; }
  void set_dirty_state() { state_ = DIRTY; }
  void set_deleted_state() { state_ = DELETED; }
  void set_updating_state() { state_ = UPDATING; }
  bool cas_compare_and_swap_state(const ObRouteEntryState old_state,
                                  const ObRouteEntryState new_state);
  bool cas_set_dirty_state();
  bool cas_set_delay_update();
  bool is_building_state() const { return BUILDING == state_; }
  bool is_avail_state() const { return AVAIL == state_; }
  bool is_dirty_state() const { return DIRTY == state_; }
  bool is_updating_state() const { return UPDATING == state_; }
  bool is_deleted_state() const { return DELETED == state_; }

  void set_create_time() { create_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_last_valid_time() { last_valid_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_last_access_time() { last_access_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_last_update_time();
  int64_t get_last_access_time_us() const { return last_access_time_us_; }
  int64_t get_create_time_us() const { return create_time_us_; }
  int64_t get_last_valid_time_us() const { return last_valid_time_us_; }
  int64_t get_last_update_time_us() const { return last_update_time_us_; }

  void set_cr_version(const int64_t version) { cr_version_ = version; }
  int64_t get_cr_version() const { return cr_version_; }

  static const char *get_route_entry_state(const ObRouteEntryState state);
  const char *get_route_entry_state() const { return get_route_entry_state(state_); }

  void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version;  }
  int64_t get_schema_version() const { return schema_version_;  }

  void set_cr_id(int64_t cr_id) { cr_id_ = cr_id; }
  int64_t get_cr_id() const { return cr_id_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  // Atttention!! this func only use to avoid entry frequently updating
  bool is_need_update() const;
  void set_tenant_version(const uint64_t tenant_version) { tenant_version_ = tenant_version; }
  uint64_t get_tenant_version() const { return tenant_version_; }
  int64_t get_time_for_expired() const { return time_for_expired_; }
  void set_time_for_expired(int64_t expire_time) { time_for_expired_ = expire_time; }
  void check_and_set_expire_time(const uint64_t tenant_version, const bool is_dummy_entry);

protected:
  int64_t cr_version_; // one entry must belong to one cluster with the specfied version
  int64_t cr_id_;
  int64_t schema_version_;

  int64_t create_time_us_;
  int64_t last_valid_time_us_;//used for leader
  int64_t last_access_time_us_;
  int64_t last_update_time_us_;

  ObRouteEntryState state_;
  uint64_t tenant_version_;
  int64_t time_for_expired_;
  int64_t current_expire_time_config_;
};

inline bool ObRouteEntry::is_need_update() const
{
  // default is 5s
  const int64_t UPDATE_INTERVAL_US = obutils::get_global_proxy_config().delay_update_entry_interval;
  // 1. expired, need fetch from remote;
  // 2. avoid frequently updating;
  bool is_dirty = is_dirty_state();
  bool can_update = ((common::hrtime_to_usec(event::get_hrtime()) - last_update_time_us_) > UPDATE_INTERVAL_US)
                    || obutils::get_global_proxy_config().enable_qa_mode;

  if (is_dirty && !can_update) {
    PROXY_LOG(DEBUG, "this entry is dirty, but should delay update", KPC(this), K(UPDATE_INTERVAL_US));
    if ((NULL != obproxy::event::this_ethread()) && (NULL != obproxy::event::this_ethread()->mutex_)) {
      obproxy::event::ObProxyMutex *mutex_ = obproxy::event::this_ethread()->mutex_;
      PROCESSOR_INCREMENT_DYN_STAT(PL_DELAY_UPDATE_COUNT);
    }
  }

  return (is_dirty && can_update);
}

inline void  ObRouteEntry::renew_last_update_time() {
  last_update_time_us_ = common::hrtime_to_usec(event::get_hrtime());
  PROXY_LOG(DEBUG, "this entry will set delay update", KPC(this));
  if ((NULL != obproxy::event::this_ethread()) && (NULL != obproxy::event::this_ethread()->mutex_)) {
    obproxy::event::ObProxyMutex *mutex_ = obproxy::event::this_ethread()->mutex_;
    PROCESSOR_INCREMENT_DYN_STAT(SET_DELAY_UPDATE_COUNT);
  }
}

inline bool ObRouteEntry::cas_compare_and_swap_state(
    const ObRouteEntryState old_state,
    const ObRouteEntryState new_state)
{
  return __sync_bool_compare_and_swap(&state_, old_state, new_state);
}

inline bool ObRouteEntry::cas_set_dirty_state()
{
  return cas_compare_and_swap_state(ObRouteEntry::AVAIL, ObRouteEntry::DIRTY);
}

inline bool ObRouteEntry::cas_set_delay_update()
{
  bool bret = cas_set_dirty_state();
  renew_last_update_time();
  return bret;
}


struct ObTableEntryName
{
public:
  ObTableEntryName() : cluster_name_(), tenant_name_(), database_name_(),
                       package_name_(), table_name_() {}
  ~ObTableEntryName() {}
  bool is_valid() const;
  int64_t get_total_str_len() const;
  uint64_t hash(const uint64_t seed = 0) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool operator==(const ObTableEntryName &other) const;
  bool operator!=(const ObTableEntryName &other) const;
  void reset();
  bool is_ob_dummy() const;
  bool is_sys_dummy() const;

  bool is_sys_tenant() const;
  bool is_oceanbase_db() const;
  bool is_all_dummy_table() const;
  int deep_copy(const ObTableEntryName &name, char *buf, const int64_t buf_len);
  void shallow_copy(const ObTableEntryName &name);
  void shallow_copy(const common::ObString &cluster_name, const common::ObString &tenant_name,
                    const common::ObString &database_name, const common::ObString &table_name);
  void shallow_copy(const common::ObString &cluster_name, const common::ObString &tenant_name,
                    const common::ObString &database_name, const common::ObString &package_name,
                    const common::ObString &table_name);


  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::ObString package_name_;
  common::ObString table_name_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableEntryName);
};


inline void ObTableEntryName::shallow_copy(const ObTableEntryName &name)
{
  reset();
  cluster_name_ = name.cluster_name_;
  tenant_name_ = name.tenant_name_;
  database_name_ = name.database_name_;
  package_name_ = name.package_name_;
  table_name_ = name.table_name_;
}

inline void ObTableEntryName::shallow_copy(const common::ObString &cluster_name,
    const common::ObString &tenant_name, const common::ObString &database_name,
    const common::ObString &table_name)
{
  const common::ObString package_name;
  shallow_copy(cluster_name, tenant_name, database_name, package_name, table_name);
}

inline void ObTableEntryName::shallow_copy(const common::ObString &cluster_name,
    const common::ObString &tenant_name, const common::ObString &database_name,
    const common::ObString &package_name, const common::ObString &table_name)
{
  reset();
  cluster_name_ = cluster_name;
  tenant_name_ = tenant_name;
  database_name_ = database_name;
  package_name_ = package_name;
  table_name_ = table_name;
}

inline bool ObTableEntryName::is_sys_tenant() const
{
  static const common::ObString sys_tenant_str(common::OB_SYS_TENANT_NAME);
  return is_valid() && sys_tenant_str == tenant_name_;
}

inline bool ObTableEntryName::is_oceanbase_db() const
{
  static const common::ObString sys_database_str(common::OB_SYS_DATABASE_NAME);
  return is_valid() && sys_database_str == database_name_;
}

inline bool ObTableEntryName::is_all_dummy_table() const
{
  static const common::ObString all_dummy_tname_str(share::OB_ALL_DUMMY_TNAME);
  return is_valid() && table_name_[0] == '_' && all_dummy_tname_str == table_name_;
}

inline bool ObTableEntryName::is_ob_dummy() const
{
  return is_oceanbase_db() && is_all_dummy_table();
}

inline bool ObTableEntryName::is_sys_dummy() const
{
  return is_sys_tenant() && is_ob_dummy();
}

inline void ObTableEntryName::reset()
{
  cluster_name_.reset();
  tenant_name_.reset();
  database_name_.reset();
  package_name_.reset();
  table_name_.reset();
}

inline int64_t ObTableEntryName::get_total_str_len() const
{
  return (cluster_name_.length()
          + tenant_name_.length()
          + database_name_.length()
          + package_name_.length()
          + table_name_.length());
}

inline bool ObTableEntryName::operator==(const ObTableEntryName &other) const
{
  return ((cluster_name_ == other.cluster_name_)
          && (tenant_name_ == other.tenant_name_)
          && (database_name_ == other.database_name_)
          && (package_name_ == other.package_name_)
          && (table_name_ == other.table_name_));
}

inline bool ObTableEntryName::operator!=(const ObTableEntryName &other) const
{
  return !(*this == other);
}

inline bool ObTableEntryName::is_valid() const
{
  return ((!cluster_name_.empty())
          && (!tenant_name_.empty())
          && (!database_name_.empty())
          && (!table_name_.empty()));
}

inline uint64_t ObTableEntryName::hash(const uint64_t seed) const
{
  return ((cluster_name_.hash(seed)
          + tenant_name_.hash(seed)
          + database_name_.hash(seed)
          + package_name_.hash(seed)
          + table_name_.hash(seed)));
}

struct ObTableEntryKey
{
public:
  ObTableEntryKey() : name_(NULL), cr_version_(-1), cr_id_(common::OB_INVALID_CLUSTER_ID) {}
  ObTableEntryKey(const ObTableEntryName &name, int64_t cr_version, int64_t cr_id)
    : name_(&name), cr_version_(cr_version), cr_id_(cr_id) {}
  ~ObTableEntryKey() { name_ = NULL; cr_version_ = -1; }

  bool is_valid() const;
  uint64_t hash(const uint64_t seed = 0) const;
  bool operator==(const ObTableEntryKey &other) const;
  bool operator!=(const ObTableEntryKey &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  const ObTableEntryName *name_;
  int64_t cr_version_;
  int64_t cr_id_;
};


inline void ObTableEntryKey::reset()
{
  name_ = NULL;
  cr_version_ = -1;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
}

inline bool ObTableEntryKey::is_valid() const
{
  return ((NULL == name_) ? (false) : (name_->is_valid() && cr_version_ >= 0 && cr_id_ >= 0));
}

inline uint64_t ObTableEntryKey::hash(const uint64_t seed) const
{
  uint64_t hashs = NULL == name_ ? 0 : name_->hash(seed);
  hashs = common::murmurhash(&cr_version_, sizeof(cr_version_), hashs);
  hashs = common::murmurhash(&cr_id_, sizeof(cr_id_), hashs);
  return hashs;
}

inline bool ObTableEntryKey::operator==(const ObTableEntryKey &other) const
{
  return ((NULL == name_ || NULL == other.name_) ?
             (false) : (*name_ == *other.name_ && cr_version_ == other.cr_version_ && cr_id_ == other.cr_id_));
}

inline bool ObTableEntryKey::operator!=(const ObTableEntryKey &other) const
{
  return !(*this == other);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_ROUTE_STRUCT_H
