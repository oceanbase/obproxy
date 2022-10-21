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

#ifndef OBPROXY_CONGESTION_ENTRY_H
#define OBPROXY_CONGESTION_ENTRY_H

#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

#define CONGESTION_EVENT_ALIVE_CONGESTED (CONGESTION_EVENT_EVENTS_START + 1)
#define CONGESTION_EVENT_DEAD_CONGESTED (CONGESTION_EVENT_EVENTS_START + 2)
#define CONGESTION_EVENT_CONGESTED_LIST_DONE (CONGESTION_EVENT_EVENTS_START + 3)
#define CONGESTION_EVENT_CONTROL_LOOKUP_DONE (CONGESTION_EVENT_EVENTS_START + 4)

class ObCongestionRefCnt
{
public:
  ObCongestionRefCnt() : ref_count_(0) { }
  virtual ~ObCongestionRefCnt() { }
  virtual void free() = 0;

  void inc_ref()
  {
    (void)ATOMIC_FAA(&ref_count_, 1);
  }

  void dec_ref()
  {
    if (1 == ATOMIC_FAA(&ref_count_, -1)) {
      free();
    }
  }

  volatile int64_t ref_count_;
};

class ObCongestionControlConfig : public ObCongestionRefCnt
{
public:
  ObCongestionControlConfig()
      : conn_failure_threshold_(5), alive_failure_threshold_(5),
        fail_window_sec_(120), retry_interval_sec_(10),
        min_keep_congestion_interval_sec_(20)
  {
  }
  virtual ~ObCongestionControlConfig() { }
  virtual void free() { op_free(this); }

  bool operator ==(const ObCongestionControlConfig &other) const
  {
    return (conn_failure_threshold_ == other.conn_failure_threshold_
            && alive_failure_threshold_ == other.alive_failure_threshold_
            && fail_window_sec_ == other.fail_window_sec_
            && retry_interval_sec_ == other.retry_interval_sec_
            && min_keep_congestion_interval_sec_ == other.min_keep_congestion_interval_sec_);
  }

  bool operator !=(const ObCongestionControlConfig &other) const
  {
    return !(*this == other);
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int64_t ref_count = ref_count_;
    J_OBJ_START();
    J_KV(K(ref_count),
         KP(this),
         K_(conn_failure_threshold),
         K_(alive_failure_threshold),
         K_(fail_window_sec),
         K_(retry_interval_sec),
         K_(min_keep_congestion_interval_sec));
    J_OBJ_END();
    return pos;
  }

  int64_t conn_failure_threshold_;
  int64_t alive_failure_threshold_;
  int64_t fail_window_sec_; // unit second
  int64_t retry_interval_sec_; // unit second
  int64_t min_keep_congestion_interval_sec_; // unit second
};

class ObCongestionZoneState : public ObCongestionRefCnt
{
public:
  enum ObZoneState
  {
    INACTIVE,
    ACTIVE,
    MERGE,
    UPGRADE,
    DELETED
  };

  ObCongestionZoneState()
    : zone_name_(), region_name_(), state_(ACTIVE), last_revalidate_time_us_(0)
  {
    zone_name_str_[0] = '\0';
    region_name_str_[0] = '\0';
  }

  ObCongestionZoneState(const common::ObString &zone_name,
      const common::ObString &region_name, ObZoneState state)
  {
    set_zone_name(zone_name);
    set_region_name(region_name);
    state_ = state;
    last_revalidate_time_us_ = 0;
  }

  virtual ~ObCongestionZoneState() { }
  virtual void free()
  {
    PROXY_LOG(DEBUG, "this congestion zone state will be free", KPC(this));
    op_free(this);
  }

  void set_zone_name(const common::ObString &zone_name)
  {
    MEMCPY(zone_name_str_, zone_name.ptr(), zone_name.length());
    zone_name_.assign_ptr(zone_name_str_, zone_name.length());
  }

  void set_region_name(const common::ObString &region_name)
  {
    MEMCPY(region_name_str_, region_name.ptr(), region_name.length());
    region_name_.assign_ptr(region_name_str_, region_name.length());
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int64_t ref_count = ref_count_;
    J_OBJ_START();
    J_KV(K(ref_count),
         KP(this),
         K_(zone_name),
         K_(region_name),
         "state", get_zone_state_name(state_),
         K_(last_revalidate_time_us));
    J_OBJ_END();
    return pos;
  }

  static const char *get_zone_state_name(ObZoneState state)
  {
    const char *ret = NULL;
    switch(state) {
      case INACTIVE:
        ret = "INACTIVE";
        break;
      case ACTIVE:
        ret = "ACTIVE";
        break;
      case MERGE:
        ret = "MERGE";
        break;
      case UPGRADE:
        ret = "UPGRADE";
        break;
      case DELETED:
        ret = "DELETED";
        break;
      default:
        break;
    }
    return ret;
  }

  void renew_last_revalidate_time() { last_revalidate_time_us_ = common::ObTimeUtility::current_time(); }

  common::ObString zone_name_;
  common::ObString region_name_;
  ObZoneState state_;
  int64_t last_revalidate_time_us_; // us unit

  char zone_name_str_[common::MAX_ZONE_LENGTH];
  char region_name_str_[common::MAX_REGION_LENGTH];
};

struct ObFailHistory
{
  static const int64_t CONG_HIST_ENTRIES = 17;

  ObFailHistory()
      : start_(0), last_event_(0), bin_len_(0), length_(0), cur_index_(0), events_(0)
  {
    memset(bins_, 0, sizeof(bins_));
  }
  void init(const int64_t window);
  void init_event(const int64_t t, const int64_t n = 1);
  int64_t regist_event(const int64_t t, const int64_t n = 1);
  int64_t get_bin_events(const int64_t index) const
  {
    return bins_[(index + 1 + cur_index_) % CONG_HIST_ENTRIES];
  }

  int64_t start_; // seconds
  int64_t last_event_; // seconds
  int64_t bin_len_;
  int64_t length_;
  uint16_t bins_[CONG_HIST_ENTRIES];
  int64_t cur_index_;
  int64_t events_;
};

struct ObCongestionEntry : public ObCongestionRefCnt
{
  enum ObServerState
  {
    ACTIVE,
    INACTIVE,
    ACTIVE_CONGESTED,
    DELETING,
    DELETED,
    UPGRADE,
    REPLAY,
    DETECT_ALIVE,
    DETECT_DEAD,
  };

  enum ObEntryState
  {
    ENTRY_AVAIL,
    ENTRY_DELETED,
  };

  explicit ObCongestionEntry(const net::ObIpEndpoint &ip);
  virtual ~ObCongestionEntry() { }
  void destroy();

  virtual void free();

  int init(ObCongestionControlConfig *config, ObCongestionZoneState *zone_state);

  sockaddr const *get_ip() const
  {
    return &server_ip_.sa_;
  }

  // print the entry into the congested list output buffer
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_server_state_name(const ObServerState state);
  static const char *get_entry_state_name(const ObEntryState state);

  // congestion control functions
  // Is the server congested?
  bool is_dead_congested() const;
  bool is_force_alive_congested() const;
  bool is_alive_congested() const;
  bool is_detect_congested() const;
  bool is_congested() const;
  bool is_zone_merging() const;
  bool is_zone_upgrading() const;
  bool is_server_upgrading() const { return (UPGRADE == server_state_); }
  bool is_server_replaying() const { return (REPLAY == server_state_); }

  bool is_entry_avail() const { return (ENTRY_AVAIL == entry_state_); }
  bool is_entry_deleted() const { return (ENTRY_DELETED == entry_state_); }
  void set_entry_deleted_state() { entry_state_ = ENTRY_DELETED; }

  // Update state info
  void check_and_set_alive();
  void set_all_congested_free();
  void set_dead_free_alive_congested();
  void set_alive_congested();
  void set_alive_congested_free();
  void set_dead_congested();
  void set_dead_congested_free();
  void set_detect_congested();
  void set_detect_congested_free();
  void set_dead_failed_at(const ObHRTime t);
  void set_alive_failed_at(const ObHRTime t);

  // Connection controls
  bool alive_need_retry(const ObHRTime t);
  bool alive_need_update(const ObHRTime t);
  bool dead_need_update(const ObHRTime t);

  // fail history operations
  void reset_fail_history();
  bool check_dead_congested();
  bool check_alive_congested();

  // ObCongestionEntry and ObCongestionControl config interaction helper functions
  int validate_config(ObCongestionControlConfig *config);
  // retrun true if validate zone success, else return false
  int validate_zone(ObCongestionZoneState *zone_state);

  int apply_new_config(ObCongestionControlConfig *config);
  int apply_new_zone_state(ObCongestionZoneState *zone_state);

  int64_t get_last_revalidate_time_us() const { return last_revalidate_time_us_; }
  void renew_last_revalidate_time() { last_revalidate_time_us_ = common::ObTimeUtility::current_time(); }

  ObServerState server_state_;
  ObEntryState entry_state_;
  // host info
  net::ObIpEndpoint server_ip_;

  // Pointer to the global congestion control config
  // Remember to update the refcount of record
  ObCongestionControlConfig *control_config_;

  // Remember to update the refcount of zone state
  ObCongestionZoneState *zone_state_;

  // State -- connection failures
  ObFailHistory conn_fail_history_;
  ObFailHistory alive_fail_history_;
  common::ObPtr<event::ObProxyMutex> fail_hist_lock_;

  ObHRTime last_dead_congested_; // unit second
  volatile int64_t dead_congested_; // 0 | 1

  ObHRTime last_alive_congested_; // unit second
  volatile int64_t alive_congested_;

  ObHRTime last_detect_congested_;
  volatile int64_t detect_congested_;

  volatile int64_t stat_conn_failures_;
  volatile int64_t stat_alive_failures_;
  int64_t last_revalidate_time_us_; // unit us

  int64_t cr_version_;
};

inline bool ObCongestionEntry::alive_need_retry(const ObHRTime t)
{
  return ((common::hrtime_to_sec(t) - alive_fail_history_.last_event_) >= control_config_->retry_interval_sec_);
}

inline bool ObCongestionEntry::alive_need_update(const ObHRTime t)
{
  return ((common::hrtime_to_sec(t) - last_alive_congested_) >= control_config_->min_keep_congestion_interval_sec_);
}

inline bool ObCongestionEntry::dead_need_update(const ObHRTime t)
{
  return ((common::hrtime_to_sec(t) - last_dead_congested_) >= control_config_->min_keep_congestion_interval_sec_);
}

// force_alive_congestion means it has no min_keep_congestion_interval
inline bool ObCongestionEntry::is_force_alive_congested() const
{
  return (is_zone_upgrading()
          || is_server_upgrading());
}

inline bool ObCongestionEntry::is_alive_congested() const
{
  return (1 == alive_congested_);
}

inline bool ObCongestionEntry::is_dead_congested() const
{
  return (1 == dead_congested_);
}

inline bool ObCongestionEntry::is_detect_congested() const
{
  return (1 == detect_congested_);
}

inline bool ObCongestionEntry::is_zone_merging() const
{
  bool bret = false;
  if (OB_LIKELY(NULL != zone_state_)) {
    bret = (ObCongestionZoneState::MERGE == zone_state_->state_);
  }
  return bret;
}

inline bool ObCongestionEntry::is_zone_upgrading() const
{
  bool bret = false;
  if (OB_LIKELY(NULL != zone_state_)) {
    bret = (ObCongestionZoneState::UPGRADE == zone_state_->state_);
  }
  return bret;
}

inline bool ObCongestionEntry::is_congested() const
{
  // watch out the priority below:
  return (is_dead_congested()
          || is_force_alive_congested()
          || is_alive_congested()
          || is_detect_congested());
}

inline bool ObCongestionEntry::check_dead_congested()
{
  bool bret = false;
  if (dead_congested_) {
    bret = true;
  } else if (control_config_->conn_failure_threshold_ < 0) {
    bret = false;
  } else {
    bret = control_config_->conn_failure_threshold_ <= conn_fail_history_.events_;
  }

  return bret;
}

inline bool ObCongestionEntry::check_alive_congested()
{
  bool bret = false;
  if (is_alive_congested()) {
    bret = true;
  } else if (control_config_->alive_failure_threshold_ < 0) {
    bret = false;
  } else {
    bret = control_config_->alive_failure_threshold_ <= alive_fail_history_.events_;
  }

  return bret;
}

inline void ObCongestionEntry::reset_fail_history()
{
  conn_fail_history_.init(control_config_->fail_window_sec_);
  dead_congested_ = 0;
  alive_fail_history_.init(control_config_->fail_window_sec_);
  alive_congested_ = 0;
}

void ObCongestionEntry::free()
{
  PROXY_LOG(DEBUG, "this congestion entry will free", KPC(this));
  destroy();
  op_free(this);
}

inline void ObCongestionEntry::destroy()
{
  fail_hist_lock_.release();

  if (OB_LIKELY(NULL != control_config_)) {
    control_config_->dec_ref();
    control_config_ = NULL;
  }

  if (OB_LIKELY(NULL != zone_state_)) {
    zone_state_->dec_ref();
    zone_state_ = NULL;
  }
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CONGESTION_ENTRY_H
