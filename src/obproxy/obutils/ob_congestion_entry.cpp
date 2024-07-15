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
#include "obutils/ob_congestion_entry.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
void ObFailHistory::init(const int64_t window)
{
  bin_len_ = (window + CONG_HIST_ENTRIES) / CONG_HIST_ENTRIES;
  if (bin_len_ <= 0) {
    bin_len_ = 1;
  }

  length_ = bin_len_ * CONG_HIST_ENTRIES;
  memset(bins_, 0, sizeof(bins_));

  start_ = 0;
  last_event_ = 0;
  cur_index_ = 0;
  events_ = 0;
}

void ObFailHistory::init_event(const int64_t t, const int64_t n)
{
  last_event_ = t;
  cur_index_ = 0;
  events_ = n;

  memset(bins_, 0, sizeof(bins_));
  bins_[0] = static_cast<uint16_t>(n);

  start_ = (last_event_ + bin_len_) - last_event_ % bin_len_ - length_;
}

int64_t ObFailHistory::regist_event(const int64_t t, const int64_t n)
{
  if (t < start_) {
    // do nothing
  } else if (t > last_event_ + length_) {
    init_event(t, n);
  } else {
    if (t < start_ + length_) {
      int64_t index = ((t - start_) / bin_len_ + 1 + cur_index_) % CONG_HIST_ENTRIES;
      bins_[index] = static_cast<uint16_t>(bins_[index] + n);
    } else {
      do {
        start_ += bin_len_;
        ++cur_index_;
        if (CONG_HIST_ENTRIES == cur_index_) {
          cur_index_ = 0;
        }

        events_ -= bins_[cur_index_];
        bins_[cur_index_] = 0;
      } while (start_ + length_ < t);
      bins_[cur_index_] = static_cast<uint16_t>(n);
    }

    events_ += n;
    if (last_event_ < t) {
      last_event_ = t;
    }
  }

  return events_;
}

ObCongestionEntry::ObCongestionEntry(const ObIpEndpoint &ip)
    : server_state_(ACTIVE), entry_state_(ENTRY_AVAIL), control_config_(NULL),zone_state_(NULL),
      last_dead_congested_(0), dead_congested_(0),
      last_alive_congested_(0),alive_congested_(0),
      last_detect_congested_(0), detect_congested_(0),
      last_client_feedback_congested_(0), stat_client_feedback_failures_(0), client_feedback_congested_(0),
      stat_conn_failures_(0), stat_alive_failures_(0),
      last_revalidate_time_us_(0), cr_version_(-1)
{
  memset(&server_ip_, 0, sizeof(server_ip_));
  ops_ip_copy(server_ip_, ip);
}

int ObCongestionEntry::init(ObCongestionControlConfig *config, ObCongestionZoneState *zone_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config) || OB_ISNULL(zone_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(config), K(zone_state), K(ret));
  } else {
    if (NULL != control_config_) {
      control_config_->dec_ref();
    }
    config->inc_ref();
    control_config_ = config;

    if (NULL != zone_state_) {
      zone_state_->dec_ref();
    }
    zone_state->inc_ref();
    zone_state_ = zone_state;
    reset_fail_history();

    fail_hist_lock_ = new_proxy_mutex(CONGESTION_ENTRY_LOCK);
    if (OB_ISNULL(fail_hist_lock_.get_ptr())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("failed to allocate memory for fail history lock", K(ret));
    }
  }

  return ret;
}

int ObCongestionEntry::validate_config(ObCongestionControlConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, config is NULL", K(ret));
  } else if (OB_FAIL(apply_new_config(config))) {
    LOG_WDIAG("failed to apply new config", K(config), K(ret));
  }
  return ret;
}

int ObCongestionEntry::validate_zone(ObCongestionZoneState *zone_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(zone_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, zone_state is NULL", K(ret));
  } else if (OB_FAIL(apply_new_zone_state(zone_state))) {
    LOG_WDIAG("failed to apply new zone_state", K(zone_state), K(ret));
  }
  return ret;
}

int ObCongestionEntry::apply_new_config(ObCongestionControlConfig *config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, control config is NULL", K(ret));
  } else if (control_config_->fail_window_sec_ != config->fail_window_sec_) {
    control_config_->dec_ref();
    config->inc_ref();
    control_config_ = config;
    reset_fail_history();
  } else {
    int64_t cft = control_config_->conn_failure_threshold_;
    int64_t aft = control_config_->alive_failure_threshold_;
    control_config_->dec_ref();
    config->inc_ref();
    control_config_ = config;

    // TODO: This used to signal via SNMP
    if (control_config_->conn_failure_threshold_ < 0) {
      set_dead_congested_free();
    } else {
      // TODO: This used to signal via SNMP
      if (cft < control_config_->conn_failure_threshold_) {
        set_dead_congested_free();
      } else if (cft > control_config_->conn_failure_threshold_
                 && conn_fail_history_.events_ >= control_config_->conn_failure_threshold_) {
        set_dead_congested();
      }
    }

    if (control_config_->alive_failure_threshold_ < 0) {
      set_alive_congested_free();
    } else {
      // TODO: This used to signal via SNMP
      if (aft < control_config_->alive_failure_threshold_) {
        set_alive_congested_free();
      } else if (aft > control_config_->alive_failure_threshold_
                 && alive_fail_history_.events_ >= control_config_->alive_failure_threshold_) {
        set_alive_congested();
      }
    }

  }
  return ret;
}

int ObCongestionEntry::apply_new_zone_state(ObCongestionZoneState *zone_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(zone_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, zone_state is NULL", K(ret));
  } else if (NULL != zone_state_
             && zone_state_ != zone_state
             && zone_state_->zone_name_ == zone_state->zone_name_) {

    if (ObCongestionZoneState::DELETED != zone_state->state_) {
      zone_state_->dec_ref();
      zone_state->inc_ref();
      zone_state_ = zone_state;
      if (ObCongestionZoneState::INACTIVE == zone_state_->state_
          || ObCongestionZoneState::UPGRADE == zone_state_->state_) {
        // no need set aliveCongested = true
      } else {
        // no need set aliveCongested = false
      }
    } else {
      // zone is deleted, we don't need delete this entry,
      // when servers in this zone are deleted, the entries
      // will be deleted naturally;
    }
  }
  return ret;
}

int64_t ObCongestionEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  char str_time[100] = " ";

  int64_t ref_count = ref_count_;
  J_OBJ_START();
  J_KV(K(ref_count),
       KP(this),
       KPC_(zone_state),
       K_(server_ip),
       "server_state", get_server_state_name(server_state_),
       "entry_state", get_entry_state_name(entry_state_),
       K_(alive_congested),
       K_(last_alive_congested),
       K_(last_revalidate_time_us));
  J_COMMA();

  if (alive_congested_) {
    struct tm time;
    time_t seconds = last_alive_congested_;
    localtime_r(&seconds, &time);
    snprintf(str_time, sizeof(str_time), "%04d/%02d/%02d %02d:%02d:%02d",
             time.tm_year + 1900, time.tm_mon + 1, time.tm_mday,
             time.tm_hour, time.tm_min, time.tm_sec);
    databuff_printf(buf, buf_len, pos, "last_alive_congested=%s,", str_time);
  }

  J_KV(K_(dead_congested));
  J_COMMA();

  if (dead_congested_) {
    struct tm time;
    time_t seconds = last_dead_congested_;
    localtime_r(&seconds, &time);
    snprintf(str_time, sizeof(str_time), "%04d/%02d/%02d %02d:%02d:%02d",
             time.tm_year + 1900, time.tm_mon + 1, time.tm_mday,
             time.tm_hour, time.tm_min, time.tm_sec);
    databuff_printf(buf, buf_len, pos, "last_dead_congested=%s,", str_time);
  }

  J_KV(K_(detect_congested));
  J_COMMA();
  if (detect_congested_) {
    struct tm time;
    time_t seconds = last_detect_congested_;
    localtime_r(&seconds, &time);
    snprintf(str_time, sizeof(str_time), "%04d/%02d/%02d %02d:%02d:%02d",
             time.tm_year + 1900, time.tm_mon + 1, time.tm_mday,
             time.tm_hour, time.tm_min, time.tm_sec);
    databuff_printf(buf, buf_len, pos, "last_detect_congested=%s,", str_time);
  }

  J_KV(K_(stat_alive_failures), K_(stat_conn_failures), K_(cr_version));
  J_COMMA();

  databuff_printf(buf, buf_len, pos, "conn_last_fail_time=%ld, conn_failure_events=%ld, ",
                  conn_fail_history_.last_event_, conn_fail_history_.events_);

  databuff_printf(buf, buf_len, pos, "alive_last_fail_time=%ld, alive_failure_events=%ld",
                  alive_fail_history_.last_event_, alive_fail_history_.events_);
  J_OBJ_END();
  return pos;
}

const char *ObCongestionEntry::get_server_state_name(const ObServerState state)
{
  const char *state_ret = NULL;
  switch (state) {
    case INACTIVE:
      state_ret = "INACTIVE";
      break;
    case ACTIVE:
      state_ret = "ACTIVE";
      break;
    case ACTIVE_CONGESTED:
      state_ret = "ACTIVE_CONGESTED";
      break;
    case DELETING:
      state_ret = "DELETING";
      break;
    case DELETED:
      state_ret = "DELETED";
      break;
    case UPGRADE:
      state_ret = "UPGRADE";
      break;
    case REPLAY:
      state_ret = "REPLAY";
      break;
    case DETECT_ALIVE:
      state_ret = "DETECT_ALIVE";
      break;
    case DETECT_DEAD:
      state_ret = "DETECT_DEAD";
      break;
    default:
      break;
  }
  return state_ret;
}

const char *ObCongestionEntry::get_entry_state_name(const ObEntryState state)
{
  const char *state_ret = NULL;
  switch (state) {
    case ENTRY_AVAIL:
      state_ret = "ENTRY_AVAIL";
      break;
    case ENTRY_DELETED:
      state_ret = "ENTRY_DELETED";
      break;
    default:
      break;
  }
  return state_ret;
}

// When a connection failure or connection disconnect happened,
// try to get the lock first and change register the event, if
// we can not get the lock, discard the event
//UNUSED for proxy now
void ObCongestionEntry::set_dead_failed_at(const ObHRTime t)
{
  (void)ATOMIC_FAA(&stat_conn_failures_, 1);
  if (control_config_->conn_failure_threshold_ >= 0) {
    int64_t time = hrtime_to_sec(t);
    LOG_INFO("dead failed at", K(t));
    MUTEX_TRY_LOCK(lock, fail_hist_lock_, this_ethread());
    if (lock.is_locked()) {
      conn_fail_history_.regist_event(time);
      if (!dead_congested_) {
        bool new_congested = check_dead_congested();
        // TODO: This used to signal via SNMP
        if (new_congested && !ATOMIC_TAS(&dead_congested_, 1)) {
          last_dead_congested_ = conn_fail_history_.last_event_;
          // action congested ?
          LOG_INFO("set_dead_congested", KPC(this));
        }
      }
    } else {
      LOG_DEBUG("failure info lost due to lock contention",
                KPC(this), K(time));
    }
  }
}

void ObCongestionEntry::set_alive_failed_at(const ObHRTime t)
{
  (void)ATOMIC_FAA(&stat_alive_failures_, 1);
  if (control_config_->alive_failure_threshold_ >= 0) {
    int64_t time = hrtime_to_sec(t);
    LOG_INFO("alive failed at", K(t), KPC(this));
    MUTEX_TRY_LOCK(lock, fail_hist_lock_, this_ethread());
    if (lock.is_locked()) {
      alive_fail_history_.regist_event(time);
      if (!alive_congested_) {
        bool new_congested = check_alive_congested();
        // TODO: This used to signal via SNMP
        if (new_congested && !ATOMIC_TAS(&alive_congested_, 1)) {
          last_alive_congested_ = alive_fail_history_.last_event_;
          // action congested ?
          LOG_WARN("set alive congested", KPC(this));
        }
      }
    } else {
      LOG_DEBUG("failure info lost due to lock contention",
                KPC(this), K(time));
    }
  }
}

void ObCongestionEntry::set_client_feedback_failed_at(const ObHRTime t)
{
  (void)ATOMIC_FAA(&stat_client_feedback_failures_, 1);
  if (control_config_->alive_failure_threshold_ >= 0) {
    int64_t time = hrtime_to_sec(t);
    LOG_INFO("client feedback failed at", K(t));
    MUTEX_TRY_LOCK(lock, fail_hist_lock_, this_ethread());
    if (lock.is_locked()) {
      client_feedback_fail_history_.regist_event(time);
      if (!client_feedback_congested_) {
        bool new_congested = check_client_feedback_congested();
        // TODO: This used to signal via SNMP
        if (new_congested && !ATOMIC_TAS(&client_feedback_congested_, 1)) {
          last_client_feedback_congested_ = client_feedback_fail_history_.last_event_;
          // action congested ?
          LOG_INFO("set_client_feedback_congested", KPC(this));
        }
      }
    } else {
      LOG_DEBUG("failure info lost due to lock contention",
                KPC(this), K(time));
    }
  }
}

void ObCongestionEntry::check_and_set_alive()
{
  if (OB_LIKELY(NULL != control_config_)) {
    if (is_dead_congested()) {
      if (dead_need_update(get_hrtime())) {
        set_all_congested_free();
      } else {
        set_alive_failed_at(get_hrtime());
        set_dead_free_alive_congested();
      }
    } else if (is_alive_congested()
               && alive_need_update(get_hrtime())) {
      set_all_congested_free();
    } else { }
  }
}

void ObCongestionEntry::set_all_congested_free()
{
  set_dead_congested_free();
  set_alive_congested_free();
}

void ObCongestionEntry::set_dead_free_alive_congested()
{
  set_dead_congested_free();
  set_alive_congested();
}

void ObCongestionEntry::set_alive_congested()
{
  if (ATOMIC_TAS(&alive_congested_, 1)) {
    // action congested ?
  } else {
    last_alive_congested_ = ObTimeUtility::extract_second(ObTimeUtility::current_time());
    LOG_WARN("set alive congested", KPC(this));
  }
}

void ObCongestionEntry::set_alive_congested_free()
{
  if (ATOMIC_TAS(&alive_congested_, 0)) {
    // action not congested ?
    LOG_WARN("set alive congested free", KPC(this));
  }
}

void ObCongestionEntry::set_dead_congested()
{
  // TODO: This used to signal via SNMP
  if (ATOMIC_TAS(&dead_congested_, 1)) {
    // Action congested ?
  } else {
    last_dead_congested_ = ObTimeUtility::extract_second(ObTimeUtility::current_time());
    LOG_WARN("set dead congested", KPC(this));
  }
}

void ObCongestionEntry::set_dead_congested_free()
{
  if (ATOMIC_TAS(&dead_congested_, 0)) {
    // action not congested ?
    LOG_WARN("set dead congested free", KPC(this));
  }
}

void ObCongestionEntry::set_detect_congested()
{
  if (ATOMIC_TAS(&detect_congested_, 1)) {
    // Action congested ?
  } else {
    last_detect_congested_ = ObTimeUtility::extract_second(ObTimeUtility::current_time());
    LOG_WARN("set detect congested", KPC(this));
  }
}

void ObCongestionEntry::set_detect_congested_free()
{
  if (ATOMIC_TAS(&detect_congested_, 0)) {
    // action not congested ?
    LOG_WARN("set detect congested free", KPC(this));
  }
}


} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
