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
#include "obutils/ob_congestion_manager.h"
#include "iocore/net/ob_net_def.h"
#include "stat/ob_processor_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

#define SCHEDULE_CONGEST_CONT_INTERVAL HRTIME_MSECONDS(1)

ObRecRawStatBlock *congest_rsb = NULL;

int init_congestion_control()
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(register_congest_stats())) {
    LOG_WARN("failed to register congest stats variables", K(ret));
  }
  return ret;
}

/*
 * the ObCongestionManagerCont is the continuation to do the congestion manager related work
 * when the ObCongestionManager's corresponding function does not get the lock in the
 * first try
 */
class ObCongestionManagerCont : public ObContinuation
{
public:
  explicit ObCongestionManagerCont(ObCongestionManager &congestion_manager);
  virtual ~ObCongestionManagerCont() {}

  void destroy();

  int get_congest_list(int event, ObEvent *e);

  int get_congest_entry(int event, ObEvent *e);

  ObAction action_;
  ObCongestionManager *congestion_manager_;

  // congestion list info
  ObMIOBuffer *iobuf_;
  int64_t cur_partition_id_;

  // congestion entry info
  uint64_t hash_;
  ObIpEndpoint server_ip_;
  ObCongestionControlConfig *config_;
};

inline ObCongestionManagerCont::ObCongestionManagerCont(ObCongestionManager &congestion_manager)
    : ObContinuation(NULL), congestion_manager_(&congestion_manager), iobuf_(NULL),
      cur_partition_id_(-1), hash_(0), config_(NULL)
{
}

inline void ObCongestionManagerCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

ObCongestionManager::ObCongestionManager()
    : is_inited_(false), is_base_servers_added_(false),
      is_congestion_enabled_(true), zone_count_(0), config_(NULL)
{
  memset(zones_, 0, sizeof(zones_));
}

/**
 * There should be no entry in the manager, before you call the destructor
 */
ObCongestionManager::~ObCongestionManager()
{
  destroy();
}

void ObCongestionManager::destroy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObCongestionManager will destroy", KPC(this));
  ObCongestionEntry *entry = NULL;
  Iter it;
  for (int64_t i = 0; i < get_sub_part_count(); ++i) {
    ObProxyMutex *bucket_mutex = lock_for_part(i);
    if (NULL != bucket_mutex) {
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(i))) {
          LOG_WARN("failed to run todo list", K(ret));
        }
        ret = OB_SUCCESS; // ingore ret
        // remove all congestion entry
        entry = first_entry(i, it);
        while (NULL != entry) {
          remove_entry(i, it);
          entry->set_entry_deleted_state();
          LOG_DEBUG("this congestion entry will dec ref", KPC(entry));
          entry->dec_ref();
          entry = cur_entry(i, it);
        }
      } else {
        // fail to lock, never happen
        LOG_ERROR("fail to lock bucket mutex", K(i), K(bucket_mutex),
                  "thread_holding", bucket_mutex->thread_holding_,
                  "thread_holding_count",  bucket_mutex->thread_holding_count_);
      }
    }
  }

  if (NULL != config_) {
    config_->dec_ref();
    config_ = NULL;
  }

  for (int64_t i = 0; i < zone_count_; ++i) {
    if (NULL != zones_[i]) {
      zones_[i]->dec_ref();
      zones_[i] = NULL;
    }
  }
  zone_count_ = 0;

  is_inited_ = false;
  is_base_servers_added_ = false;
  is_congestion_enabled_ = true;
}

int ObCongestionManager::init(const int64_t table_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("congestion manager init twice", K(ret));
  } else if (OB_UNLIKELY(table_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_size), K(ret));
  } else if (OB_FAIL(CongestionTable::init(table_size, CONGESTION_TABLE_LOCK))) {
    LOG_WARN("fail to init hash table of congestion manager", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (OB_FAIL(todo_lists_[i].init("cong_todo_list",
          reinterpret_cast<int64_t>(&(reinterpret_cast<ObCongestRequestParam *> (0))->link_)))) {
        LOG_WARN("fail to init todo_lists_[]", K(i), K(ret));
      }
    }
    is_inited_ = true;
  }

  return ret;
}

DEF_TO_STRING(ObCongestionManager)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(is_inited), K_(is_base_servers_added), K_(is_congestion_enabled),
       K_(zone_count), KPC_(config));

  for (int32_t i = 0; i < MAX_ZONE_NUM; ++i) {
    if (NULL != zones_[i]) {
      BUF_PRINTF(", zones_[%d]=", i);
      BUF_PRINTO(*zones_[i]);
    }
  }
  J_OBJ_END();
  return pos;
}

int ObCongestionManager::update_congestion_config(
    ObCongestionControlConfig *config, const bool is_init)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("update config control", K(config), K(is_init));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("congestion manager not init", K(ret));
  } else if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(config), K(ret));
  } else {
    SpinWLockGuard guarlockd(rw_lock_);
    if (NULL == config_) {
      config->inc_ref();
      config_ = config;
    } else if (*config_ != *config) {
      config->inc_ref();
      config_->dec_ref();
      config_ = config;
      if (!is_init) {
        config_->inc_ref();
        // add asyn revalidate config task
        if (OB_FAIL(revalidate_config(config_))) {
          LOG_WARN("failed to revaildate config", K_(config), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObCongestionManager::update_zone(const ObString &zone_name,
                                     const common::ObString &region_name,
                                     const ObCongestionZoneState::ObZoneState state,
                                     const bool is_init)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("update zone", K(zone_name), "zone_state",
            ObCongestionZoneState::get_zone_state_name(state), K(is_init));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("congestion manager not init", K(ret));
  } else {
    ObCongestionZoneState **zone_state = NULL;
    ObCongestionZoneState *new_zone_state = NULL;
    bool found = false;

    SpinWLockGuard guard(rw_lock_);
    for (int64_t j = 0; !found && j < zone_count_; ++j) {
      if (zones_[j]->zone_name_ == zone_name) {
        found = true;
        zone_state = &zones_[j];
      }
    }

    bool handle_changed_zone = false;
    if (NULL == zone_state) { // zone doesn't exist, just add
      handle_changed_zone = true;
    } else if (region_name != (*zone_state)->region_name_) {
      handle_changed_zone = true;
      LOG_WARN("region name has changed",
               "old_region_name", (*zone_state)->region_name_, "new_region_name", region_name);
    } else if ((*zone_state)->state_ != state) { // zone already exist and state has changed
      if (ObCongestionZoneState::DELETED == state) {
        // now, if observer delete a zone, it will not delete the severs
        // in this zone; so in obproxy, we just set the zone's state to
        // DELETED and do not delete the servers;
        (*zone_state)->state_ = state;
        (*zone_state)->renew_last_revalidate_time();
      } else {
        handle_changed_zone = true;
      }
    }

    if (handle_changed_zone) {
      new_zone_state = op_alloc_args(ObCongestionZoneState, zone_name, region_name, state);
      if (OB_ISNULL(new_zone_state)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for zone state", K(ret));
      } else {
        if (NULL != zone_state) {
          (*zone_state)->dec_ref();
          new_zone_state->inc_ref();
          new_zone_state->renew_last_revalidate_time();
          *zone_state = new_zone_state;
          if (!is_init) {
            (*zone_state)->inc_ref();
            if (OB_FAIL(revalidate_zone(*zone_state))) {
              LOG_WARN("failed to revalidate zone", K(zone_state), K(ret));
            }
          }
        } else if (zone_count_ < MAX_ZONE_NUM) {
          new_zone_state->inc_ref();
          new_zone_state->renew_last_revalidate_time();
          zones_[zone_count_] = new_zone_state;
          ++zone_count_;
        } else {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("too many zones", K_(zone_count), K(MAX_ZONE_NUM), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObCongestionManager::update_server(const ObIpEndpoint &ip,
                                       const int64_t cr_version,
                                       const ObCongestionEntry::ObServerState state,
                                       const ObString &zone_name,
                                       const ObString &region_name,
                                       const bool is_init)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("update server state", K(ip), "server_state",
            ObCongestionEntry::get_server_state_name(state),
            K(zone_name), K(region_name), K(is_init));

  if (is_init) {
    if (OB_FAIL(add_new_server(ip, cr_version, state, zone_name, region_name, is_init))) {
      LOG_WARN("failed to handle new server", K(ip), K(cr_version), "server_state",
                ObCongestionEntry::get_server_state_name(state), K(zone_name), K(region_name),
                K(is_init), K(ret));
    }
  } else {
    if (OB_FAIL(revalidate_server(ip, cr_version, state, zone_name, region_name))) {
      LOG_WARN("failed to handle revalidate server", K(ip), K(cr_version), "server_state",
                ObCongestionEntry::get_server_state_name(state), K(zone_name), K(region_name), K(ret));
    }
  }

  return ret;
}

int ObCongestionManager::add_new_server(const net::ObIpEndpoint &ip,
                                        const int64_t cr_version,
                                        const ObCongestionEntry::ObServerState state,
                                        const ObString &zone_name,
                                        const ObString &region_name,
                                        const bool is_init /*= false*/)
{
  int ret = OB_SUCCESS;
  ObCongestionControlConfig *config = NULL;
  ObCongestionZoneState *zone_state = NULL;

  if (ObCongestionEntry::DELETED == state) {
    // need not add deleted server
  } else if (OB_ISNULL(config = get_congestion_control_config())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get congestion control config", K(ret));
  } else {
    if (NULL == (zone_state = get_zone_state(zone_name))) {
      LOG_INFO("failed to get congestion zone state,"
               "maybe has already been deleted, we will add this zone "
               "with DELTED state", K(zone_name));
      bool direct_add = true;
      if (OB_FAIL(update_zone(zone_name, region_name, ObCongestionZoneState::DELETED, direct_add))) {
        PROXY_LOG(WARN,"failed to update zone", K(zone_name), K(ret));
      } else if (OB_ISNULL(zone_state = get_zone_state(zone_name))) { // try to get zone state again
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get congestion zone state", K(zone_name), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObCongestionEntry *entry = op_alloc_args(ObCongestionEntry, ip);
      if (OB_ISNULL(entry)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for congestion server entry", K(ret));
      } else if (OB_FAIL(entry->init(config, zone_state))) {
        LOG_WARN("failed to init congestion entry", K(ret));
      } else {
        entry->cr_version_ = cr_version;
        entry->server_state_ = state;
        entry->renew_last_revalidate_time();
        switch (state) {
          case ObCongestionEntry::ACTIVE:
            entry->set_all_congested_free();
            break;
          case ObCongestionEntry::INACTIVE:
          case ObCongestionEntry::REPLAY:
            entry->set_dead_congested();
            break;
          case ObCongestionEntry::ACTIVE_CONGESTED:
            entry->set_dead_free_alive_congested();
            break;
          case ObCongestionEntry::DELETED:
          case ObCongestionEntry::DELETING:
          case ObCongestionEntry::UPGRADE:
          default:
            break;
        }
        if (OB_FAIL(add_record(ip, entry, is_init))) {
          LOG_WARN("failed to add server congestion entry", K(ret));
        }
      }
      zone_state->dec_ref();
    }
    config->dec_ref();
  }
  return ret;
}

int ObCongestionManager::add_record(const ObIpEndpoint &ip, ObCongestionEntry *entry,
                                    const bool skip_try_lock)
{
  int ret = OB_SUCCESS;
  // just defence
  if ((NULL != entry) && (entry->cr_version_ <= 0)) {
    LOG_ERROR("cr_version_ can not <= 0 ", KPC(entry));
  }

  uint64_t hash = do_hash(ip, 0);
  ObProxyMutex *bucket_mutex = lock_for_key(hash);
  if (!skip_try_lock) {
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WARN("failed to run todo list", K(ret));
      } else {
        entry->inc_ref();
        ObCongestionEntry *tmp = insert_entry(hash, ip, entry);
        if (NULL != tmp) {
          tmp->set_entry_deleted_state();
          tmp->dec_ref();
          tmp = NULL;
        }
      }
    } else if (OB_FAIL(add_record_todo_list(hash, ip, entry))) {
      LOG_WARN("failed to add record todo list", K(ret));
    }
  } else if (OB_FAIL(add_record_todo_list(hash, ip, entry))) {
    LOG_WARN("failed to add record todo list", K(ret));
  }
  return ret;
}

int ObCongestionManager::add_record_todo_list(const uint64_t hash,
                                              const ObIpEndpoint &ip,
                                              ObCongestionEntry *entry)
{
  int ret = OB_SUCCESS;
  ObCongestRequestParam *param = op_alloc(ObCongestRequestParam);
  if (OB_ISNULL(param)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for congest request parameter", K(ret));
  } else {
    param->op_ = ObCongestRequestParam::ADD_RECORD;
    param->hash_ = hash;
    param->key_ = ip;
    entry->inc_ref();
    param->entry_ = entry;
    todo_lists_[part_num(hash)].push(param);
  }
  return ret;
}

int ObCongestionManager::remove_all_records()
{
  int ret = OB_SUCCESS;
  ObCongestionEntry *entry = NULL;
  ObProxyMutex *bucket_mutex = NULL;
  ObCongestRequestParam *param = NULL;
  Iter it;

  for (int64_t part = 0; OB_SUCC(ret) && part < MT_HASHTABLE_PARTITIONS; ++part) {
    bucket_mutex = lock_for_key(part);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part))) {
        LOG_WARN("failed to run todo list", K(ret));
      } else {
        entry = first_entry(part, it);
        while (NULL != entry) {
          remove_entry(part, it);
          entry->set_entry_deleted_state();
          entry->dec_ref();
          entry = cur_entry(part, it);
        }
      }
    } else {
      param = op_alloc(ObCongestRequestParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory for congest request parameter", K(ret));
      } else {
        param->op_ = ObCongestRequestParam::REMOVE_ALL_RECORDS;
        param->hash_ = part;
        todo_lists_[part].push(param);
      }
    }
  }
  return ret;
}

int ObCongestionManager::remove_record(const ObIpEndpoint &ip)
{
  int ret = OB_SUCCESS;
  ObCongestionEntry *entry = NULL;

  uint64_t hash = do_hash(ip, 0);
  ObProxyMutex *bucket_mutex = lock_for_key(hash);
  MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
  if (lock.is_locked()) {
    if (OB_FAIL(run_todo_list(part_num(hash)))) {
      LOG_WARN("failed to run todo list", K(ret));
    } else {
      entry = remove_entry(hash, ip);
      if (NULL != entry) {
        entry->set_entry_deleted_state();
        entry->dec_ref();
        entry = NULL;
      }
    }
  } else {
    ObCongestRequestParam *param = op_alloc(ObCongestRequestParam);
    if (OB_ISNULL(param)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory for congest request parameter", K(ret));
    } else {
      param->op_ = ObCongestRequestParam::REMOVE_RECORD;
      param->key_ = ip;
      todo_lists_[part_num(hash)].push(param);
    }
  }
  return ret;
}

// process one item in the to do list
int ObCongestionManager::process(const int64_t buck_id, ObCongestRequestParam *param)
{
  int ret = OB_SUCCESS;
  ObCongestionEntry *entry = NULL;

  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, param is NULL", K(ret));
  } else {
    switch (param->op_) {
      case ObCongestRequestParam::ADD_RECORD:
        entry = insert_entry(param->hash_, param->key_, param->entry_);
        if (NULL != entry) {
          entry->set_entry_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
        break;

      case ObCongestRequestParam::REMOVE_ALL_RECORDS:
      {
        Iter it;
        entry = first_entry(param->hash_, it);
        while (NULL != entry) {
          remove_entry(param->hash_, it);
          entry->set_entry_deleted_state();
          entry->dec_ref();
          entry = cur_entry(param->hash_, it);
        }
        break;
      }

      case ObCongestRequestParam::REMOVE_RECORD:
        entry = remove_entry(param->hash_, param->key_);
        if (NULL != entry) {
          entry->set_entry_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
        break;

      case ObCongestRequestParam::REVALIDATE_BUCKET_CONFIG:
        if (OB_FAIL(revalidate_bucket_config(buck_id, param->config_))) {
          LOG_WARN("failed to revalidate_bucket_config", K(ret));
        }
        break;

      case ObCongestRequestParam::REVALIDATE_BUCKET_ZONE:
        if (OB_FAIL(revalidate_bucket_zone(buck_id, param->zone_state_))) {
          LOG_WARN("failed to revalidate_bucket_zone", K(ret));
        }
        break;

      case ObCongestRequestParam::REVALIDATE_SERVER: {
        ObCongestionEntry *entry = lookup_entry(param->hash_, param->key_);
        if (NULL != entry) {
          switch (param->server_state_) {
            entry->server_state_ = param->server_state_;
            case ObCongestionEntry::ACTIVE:
              entry->check_and_set_alive();
              break;
            case ObCongestionEntry::INACTIVE:
            case ObCongestionEntry::REPLAY:
              entry->set_dead_congested();
              break;
            case ObCongestionEntry::ACTIVE_CONGESTED:
              entry->set_dead_free_alive_congested();
              break;
            case ObCongestionEntry::DELETED:
              entry = remove_entry(param->hash_, param->key_);
              if (NULL != entry) {
                entry->set_entry_deleted_state();
                entry->dec_ref();
                entry = NULL;
              }
              break;
            case ObCongestionEntry::DELETING:
            case ObCongestionEntry::UPGRADE:
            default:
              break;
          }
        } else {
          if (OB_ISNULL(param->zone_state_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("zone state can not be NULL here", K(ret));
          } else {
            // server not existent, just add it
            bool is_init = false;
            ObString &zone_name = param->zone_state_->zone_name_;
            ObString &region_name = param->zone_state_->region_name_;
            const int64_t cr_version = param->cr_version_;
            LOG_DEBUG("will add new server", K(zone_name), K(region_name), K(cr_version), "ip", param->key_);
            if (OB_FAIL(add_new_server(param->key_, cr_version, param->server_state_,
                    zone_name, region_name, is_init))) {
              LOG_WARN("fail to handle new server", "ip", param->key_, K(cr_version),
                       "server_state", ObCongestionEntry::get_server_state_name(param->server_state_),
                       K(zone_name), K(region_name), K(is_init), K(ret));
            }
          }
        }
        // free zone_state
        if (NULL != param->zone_state_) {
          param->zone_state_->dec_ref();
          param->zone_state_ = NULL;
        }
        break;
      }

      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObCongestionManager::process unrecognized op", K(ret));
    }
  }
  return ret;
}

int ObCongestionManager::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buck_id < 0 || buck_id >= MT_HASHTABLE_PARTITIONS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buck_id), K(ret));
  } else {
    ObCongestRequestParam *param = NULL;
    ObCongestRequestParam *cur = NULL;
    if (NULL != (param = reinterpret_cast<ObCongestRequestParam *>(todo_lists_[buck_id].popall()))) {
      // start the work at the end of the list
      param->link_.prev_ = NULL;
      while (NULL != param->link_.next_) {
        param->link_.next_->link_.prev_ = param;
        param = param->link_.next_;
      };
      while (NULL != param) {
        // ignore process return, finish todo list
        if (OB_SUCCESS != process(buck_id, param)) {
          LOG_WARN("failed to process buck_id", K(buck_id), K(ret));
        }
        cur = param;
        param = param->link_.prev_;
        op_free(cur);
        cur = NULL;
      }
    }
  }
  return ret;
}

int ObCongestionManager::revalidate_bucket_config(
    const int64_t buck_id, ObCongestionControlConfig *config)
{
  int ret = OB_SUCCESS;
  Iter it;
  ObCongestionEntry *cur = NULL;

  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, config is NULL", K(ret));
  } else {
    cur = first_entry(buck_id, it);
    while (OB_SUCC(ret) && NULL != cur) {
      if (OB_FAIL(cur->validate_config(config))) {
        LOG_WARN("failed to validate_config", K(config), K(ret));
        if (remove_entry(buck_id, it) != cur) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to remove entry", K(buck_id));
        } else {
          cur->set_entry_deleted_state();
          cur->dec_ref();
          // the next entry has been moved to the current pos
          // because of the remove_entry
          cur = cur_entry(buck_id, it);
        }
      } else {
        cur = next_entry(buck_id, it);
      }
    }
    config->dec_ref();
  }
  return ret;
}

int ObCongestionManager::revalidate_bucket_zone(
    const int64_t buck_id, ObCongestionZoneState *zone_state)
{
  int ret = OB_SUCCESS;
  Iter it;
  ObCongestionEntry *cur = NULL;

  if (OB_ISNULL(zone_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, zone_state is NULL", K(ret));
  } else {
    cur = first_entry(buck_id, it);
    while (OB_SUCC(ret) && NULL != cur) {
      if (OB_FAIL(cur->validate_zone(zone_state))) {
        LOG_WARN("failed to validate_zone", K(zone_state), K(ret));
        if (remove_entry(buck_id, it) != cur) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to remove entry", K(buck_id));
        } else {
          cur->set_entry_deleted_state();
          cur->dec_ref();
          // the next entry has been moved to the current pos
          // because of the remove_entry
          cur = cur_entry(buck_id, it);
        }
      } else {
        cur = next_entry(buck_id, it);
      }
    }
    zone_state->dec_ref();
  }
  return ret;
}

// ObCongestionManagerCont implementation
int ObCongestionManagerCont::get_congest_list(int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  LOG_DEBUG("cont::get_congest_list started");

  if (action_.cancelled_) {
    LOG_INFO("ObCongestionManagerCont::get_congest_list, action cancelled", K(this));
    destroy();
  } else {
    ObProxyMutex *bucket_mutex = NULL;
    ObCongestionEntry *entry = NULL;
    char buf[1024];
    Iter it;
    int64_t len = 0;
    int64_t written_len = 0;
    for (;OB_SUCC(ret) && cur_partition_id_ < congestion_manager_->get_sub_part_count(); ++cur_partition_id_) {
      bucket_mutex = congestion_manager_->lock_for_key(cur_partition_id_);
      {
        MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
        if (!lock_bucket.is_locked()) {
          e->schedule_in(SCHEDULE_CONGEST_CONT_INTERVAL);
          event_ret = EVENT_CONT;
          ret = OB_ERR_UNEXPECTED;
        } else {
          if (OB_FAIL(congestion_manager_->run_todo_list(cur_partition_id_))) {
            LOG_WARN("failed to run todo list", K(ret));
          } else {
            entry = congestion_manager_->first_entry(cur_partition_id_, it);
            while (NULL != entry && OB_SUCC(ret)) {
              if (entry->is_congested()) {
                len = entry->to_string(buf, 1024);
                if (OB_FAIL(iobuf_->write(buf, len, written_len))) {
                  LOG_WARN("failed to write to iobuffer", K(len), K(written_len), K(ret));
                } else if (OB_UNLIKELY(len != written_len)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("failed to write to iobuffer", K(len), K(written_len), K(ret));
                }
              }
              entry = congestion_manager_->next_entry(cur_partition_id_, it);
            }
          }
        }
      }
    }

    // handle event done
    action_.continuation_->handle_event(CONGESTION_EVENT_CONGESTED_LIST_DONE, NULL);
    destroy();
  }
  return event_ret;
}

int ObCongestionManagerCont::get_congest_entry(int event, ObEvent *e)
{
  UNUSED(event);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  LOG_DEBUG("cont::get_congest_entry started", K_(server_ip));

  if (action_.cancelled_) {
    LOG_INFO("ObCongestionManagerCont::get_congest_entry, action cancelled", K(this));
    destroy();
    LOG_DEBUG("cont::get_congest_entry state machine canceled");
  } else {
    ObCongestionEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = congestion_manager_->lock_for_key(hash_);
    MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
    if (lock_bucket.is_locked()) {
      if (OB_FAIL(congestion_manager_->run_todo_list(congestion_manager_->part_num(hash_)))) {
        LOG_WARN("failed to run todo list", K(ret));
      } else {
        entry = congestion_manager_->lookup_entry(hash_, server_ip_);
        if (NULL != entry) {
          if (OB_FAIL(congestion_manager_->update_tc_congestion_map(*entry))) {
            LOG_WARN("fail to update tc congetsion map", KPC(entry), K(ret));
            ret = OB_SUCCESS; // ignore ret
          }
          PROCESSOR_INCREMENT_DYN_STAT(GET_CONGESTION_FROM_GLOBAL_CACHE_HIT);
          entry->inc_ref();
          LOG_DEBUG("cont::get_congest_entry entry found", KPC(entry));
        } else {
          PROCESSOR_INCREMENT_DYN_STAT(GET_CONGESTION_FROM_GLOBAL_CACHE_MISS);
          // non-existent, return NULL
          LOG_DEBUG("cont::get_congest_entry entry not found");
        }
      }
      config_->dec_ref();
      lock_bucket.release();
      action_.continuation_->handle_event(CONGESTION_EVENT_CONTROL_LOOKUP_DONE, entry);
      destroy();
    } else {
      LOG_INFO("cont::get_congest_entry MUTEX_TRY_LOCK failed, will reschedule");
      if (OB_ISNULL(e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("event is null", K(ret));
      } else if (OB_FAIL(e->schedule_in(SCHEDULE_CONGEST_CONT_INTERVAL))) {
        LOG_ERROR("fail to schedule ObCongestionManagerCont", K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    }
  }

  return event_ret;
}

int ObCongestionManager::revalidate_config(ObCongestionControlConfig *config)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *bucket_mutex = NULL;
  ObCongestRequestParam *param = NULL;

  if (OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, config is NULL", K(ret));
  } else {
    LOG_DEBUG("congestion control revalidating ObCongestionManager");
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_part_count(); ++i) {
      bucket_mutex = lock_for_key(i);
      {
        MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
        if (lock_bucket.is_locked()) {
          if (OB_FAIL(run_todo_list(i))) {
            LOG_WARN("failed to run todo list", K(ret));
          } else {
            config->inc_ref();
            if (OB_FAIL(revalidate_bucket_config(i, config))) {
              LOG_WARN("failed to revalidate bucket config", "bucket_id", i, K(config));
            }
          }
        } else {
          param = op_alloc(ObCongestRequestParam);
          if (OB_ISNULL(param)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for congest request parameter", K(ret));
          } else {
            param->op_ = ObCongestRequestParam::REVALIDATE_BUCKET_CONFIG;
            config->inc_ref();
            param->config_ = config;
            todo_lists_[i].push(param);
          }
        }
      }
    }
    config->dec_ref();

    LOG_DEBUG("congestion control revalidating ObCongestionManager Done");
  }
  return ret;
}

int ObCongestionManager::revalidate_zone(ObCongestionZoneState *zone_state)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *bucket_mutex = NULL;
  ObCongestRequestParam *param = NULL;
  if (OB_ISNULL(zone_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, zone_state is NULL", K(ret));
  } else {
    LOG_DEBUG("revalidating zone state");
    // asyn task, do not handle FAIL
    for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_part_count(); ++i) {
      bucket_mutex = lock_for_key(i);
      {
        MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
        if (lock_bucket.is_locked()) {
          if (OB_FAIL(run_todo_list(i))) {
            LOG_WARN("failed to run todo list", K(ret));
          } else {
            zone_state->inc_ref();
            if (OB_FAIL(revalidate_bucket_zone(i, zone_state))) {
               LOG_WARN("failed to revalidate bucket zone", "bucket_id", i, K(zone_state));
            }
          }
        } else {
          param = op_alloc(ObCongestRequestParam);
          if (OB_ISNULL(param)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for congest request parameter", K(ret));
          } else {
            param->op_ = ObCongestRequestParam::REVALIDATE_BUCKET_ZONE;
            zone_state->inc_ref();
            param->zone_state_ = zone_state;
            todo_lists_[i].push(param);
          }
        }
      }
    }
    zone_state->dec_ref();

    LOG_DEBUG("revalidating zone state Done");
  }
  return ret;
}

int ObCongestionManager::revalidate_server(const ObIpEndpoint &ip,
                                           const int64_t cr_version,
                                           const ObCongestionEntry::ObServerState server_state,
                                           const ObString &zone_name,
                                           const ObString &region_name)
{
  int ret = OB_SUCCESS;
  ObCongestionEntry *entry = NULL;

  uint64_t hash = do_hash(ip, 0);
  ObProxyMutex *bucket_mutex = lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    if (OB_FAIL(run_todo_list(part_num(hash)))) {
      LOG_WARN("failed to run todo list", K(ret));
    } else {
      entry = lookup_entry(hash, ip);
      LOG_DEBUG("lock bucket succ, find the entry", "server_state",
                ObCongestionEntry::get_server_state_name(server_state), K(*entry));
      if (NULL != entry) {
        entry->server_state_ = server_state;
        entry->renew_last_revalidate_time();
        switch (server_state) {
          case ObCongestionEntry::ACTIVE:
            entry->check_and_set_alive();
            break;
          case ObCongestionEntry::INACTIVE:
          case ObCongestionEntry::REPLAY:
            entry->set_dead_congested();
            break;
          case ObCongestionEntry::ACTIVE_CONGESTED:
            entry->set_dead_free_alive_congested();
            break;
          case ObCongestionEntry::DELETED:
            entry = remove_entry(hash, ip);
            if (NULL != entry) {
              entry->set_entry_deleted_state();
              entry->dec_ref();
              entry = NULL;
            }
            break;
          case ObCongestionEntry::DELETING:
          case ObCongestionEntry::UPGRADE:
          default:
            break;
        }
      } else {
        // if server not existent add it
        bool is_init = false;
        if (OB_FAIL(add_new_server(ip, cr_version, server_state, zone_name, region_name, is_init))) {
          LOG_WARN("failed to handle new server", K(ip), K(cr_version), "server_state",
                    ObCongestionEntry::get_server_state_name(server_state),
                    K(zone_name), K(is_init), K(ret));
        }
      }
    }
  } else {
    LOG_DEBUG("failed to lock_bucket, add to todo_list", "server_state",
              ObCongestionEntry::get_server_state_name(server_state), K(*entry));
    ObCongestRequestParam *param = op_alloc(ObCongestRequestParam);
    if (OB_ISNULL(param)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory for congest request parameter", K(ret));
    } else {
      param->op_ = ObCongestRequestParam::REVALIDATE_SERVER;
      param->hash_ = hash;
      param->key_ = ip;
      param->cr_version_ = cr_version;
      param->server_state_ = server_state;
      // only to store zone_name and region_name, will free in process()
      param->zone_state_ = op_alloc_args(ObCongestionZoneState, zone_name, region_name, ObCongestionZoneState::ACTIVE);
      if (OB_ISNULL(param->zone_state_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for zone state", K(ret));
        op_free(param);
        param = NULL;
      } else {
        param->zone_state_->inc_ref();
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

ObCongestionControlConfig *ObCongestionManager::get_congestion_control_config()
{
  SpinRLockGuard guard(rw_lock_);
  config_->inc_ref();
  return config_;
}

ObCongestionZoneState *ObCongestionManager::get_zone_state(const ObString &zone_name)
{
  ObCongestionZoneState *state_ret = NULL;
  bool found = false;
  SpinRLockGuard guard(rw_lock_);
  for (int64_t i = 0; !found && i < zone_count_; ++i) {
    if (zones_[i]->zone_name_ == zone_name) {
      state_ret = zones_[i];
      state_ret->inc_ref();
      found = true;
    }
  }
  return state_ret;
}

int ObCongestionManager::get_congest_entry(
    ObContinuation *cont, const ObIpEndpoint &ip, const int64_t cr_version,
    ObCongestionEntry **ppentry, ObAction *&action)
{
  int ret = OB_SUCCESS;
  *ppentry = NULL;
  action = NULL;

  ObCongestionControlConfig *config = get_congestion_control_config();
  if (OB_ISNULL(config) || OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(config), K(cont), K(ret));
  } else if (config->conn_failure_threshold_ <= 0 && config->alive_failure_threshold_ <= 0) {
    is_congestion_enabled_ = false;
    // no congestion control
    config->dec_ref();
  } else {
    ObProxyMutex *mutex_ = cont->mutex_;
    PROCESSOR_INCREMENT_DYN_STAT(GET_CONGESTION_TOTAL);

    is_congestion_enabled_ = true;

    // 1. find from thread cache
    ObCongestionRefHashMap &cgt_map = self_ethread().get_cgt_map();
    ObTcCongestionEntryKey tc_key(cr_version, ip);
    ObCongestionEntry *tmp_entry = cgt_map.get(tc_key);
    if (NULL != tmp_entry) {
     if (tmp_entry->is_entry_avail()) {
       // nothing
      } else {
        if (tmp_entry->is_entry_deleted()) {
          LOG_DEBUG("this cgt entry is deleted", KPC(tmp_entry));
        } else {
          LOG_ERROR("unexpected branch", KPC(tmp_entry));
        }
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
    }

    if (NULL != tmp_entry) {
      LOG_DEBUG("get cgt entry from thread cache", KPC(tmp_entry));
      PROCESSOR_INCREMENT_DYN_STAT(GET_CONGESTION_FROM_THREAD_CACHE_HIT);
      *ppentry = tmp_entry;
    } else {
      // 2. find from global cache
      uint64_t hash = do_hash(ip, 0);
      LOG_DEBUG("do hash", K(hash), K(ip), K(cr_version));

      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
      if (lock_bucket.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WARN("failed to run todo list", K(ret));
        } else {
          *ppentry = lookup_entry(hash, ip);
          if (NULL != *ppentry) {
            if (OB_FAIL(update_tc_congestion_map(**ppentry))) {
              LOG_WARN("fail to update congetsion map", KPC(*ppentry), K(ret));
              ret = OB_SUCCESS; // ignore ret
            }
            PROCESSOR_INCREMENT_DYN_STAT(GET_CONGESTION_FROM_GLOBAL_CACHE_HIT);
            (*ppentry)->inc_ref();
            LOG_DEBUG("get_congest_entry, found entry done", K(*ppentry), K(**ppentry));
          } else {
            PROCESSOR_INCREMENT_DYN_STAT(GET_CONGESTION_FROM_GLOBAL_CACHE_MISS);
            // non-existent, return NULL
            LOG_DEBUG("get_congest_entry, entry not found", K(ip));
          }
        }
      } else {
        LOG_INFO("get_congest_entry, trylock failed, schedule cont", K(ip), K(cr_version));
        ObCongestionManagerCont *congest_cont = op_alloc_args(ObCongestionManagerCont, *this);
        if (OB_ISNULL(congest_cont)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to allocate memory for congestion manager continuation", K(ret));
        } else {
          congest_cont->action_.set_continuation(cont);
          congest_cont->mutex_ = cont->mutex_;
          congest_cont->hash_ = hash;
          congest_cont->server_ip_ = ip;
          ops_ip_copy(congest_cont->server_ip_.sa_, ip);
          config->inc_ref();
          congest_cont->config_ = config;

          SET_CONTINUATION_HANDLER(congest_cont, &ObCongestionManagerCont::get_congest_entry);
          if(OB_ISNULL(cont->mutex_->thread_holding_->schedule_in(congest_cont, SCHEDULE_CONGEST_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to schedule get_congest_entry");
          } else {
            action = &congest_cont->action_;
          }
        }
      }
    }
    config->dec_ref();
  }

  return ret;
}

int ObCongestionManager::get_congest_list(ObContinuation *cont, ObMIOBuffer *buffer, ObAction *&action)
{
  int ret = OB_SUCCESS;
  char buf[1024];
  Iter it;
  int64_t len = 0;
  int64_t written_len = 0;
  ObProxyMutex *bucket_mutex = NULL;
  ObCongestionEntry *entry = NULL;
  ObCongestionManagerCont *congest_cont = NULL;
  action = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < get_sub_part_count() && NULL == action; ++i) {
    bucket_mutex = lock_for_key(i);
    {
      MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
      if (lock_bucket.is_locked()) {
        if (OB_FAIL(run_todo_list(i))) {
          LOG_WARN("failed to run todo list", K(ret));
        } else {
          entry = first_entry(i, it);
          while (NULL != entry && OB_SUCC(ret)) {
            if (entry->is_congested()) {
              len = entry->to_string(buf, 1024);
              if (OB_FAIL(buffer->write(buf, len, written_len))) {
                LOG_WARN("failed to write to iobuffer", K(len), K(written_len), K(ret));
              } else if (OB_UNLIKELY(len != written_len)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to write to iobuffer", K(len), K(written_len), K(ret));
              }
            }
            entry = next_entry(i, it);
          }
        }
      } else {
        // we did not get the lock, schedule it
        congest_cont = op_alloc_args(ObCongestionManagerCont, *this);
        if (OB_ISNULL(congest_cont)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to allocate memory for congestion manager continuation", K(ret));
        } else {
          congest_cont->cur_partition_id_ = i;
          congest_cont->iobuf_ = buffer;
          congest_cont->action_.set_continuation(cont);
          congest_cont->mutex_ = cont->mutex_;
          SET_CONTINUATION_HANDLER(congest_cont, &ObCongestionManagerCont::get_congest_list);
          if (OB_ISNULL(g_event_processor.schedule_in(congest_cont, SCHEDULE_CONGEST_CONT_INTERVAL, ET_NET))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to schedule get_congest_list", K(ret));
          } else {
            action = &congest_cont->action_;
          }
          // break
        }
      }
    }
  }

  return ret;
}

int64_t ObTcCongestionEntryKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cr_version),
       K_(ip));
  J_OBJ_END();
  return pos;
}

int ObCongestionManager::update_tc_congestion_map(ObCongestionEntry &entry)
{
  int ret = OB_SUCCESS;
  ObCongestionRefHashMap &cgt_map = self_ethread().get_cgt_map();
  if (entry.is_entry_avail()) {
    LOG_DEBUG("the cgt entry will add to tc congestion map", K(entry));
    ObCongestionEntry *tmp_entry = &entry;
    if (OB_FAIL(cgt_map.set(tmp_entry))) {
      LOG_WARN("fail to set table map", K(entry), K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unavail state, can not add to cgt map", K(entry), K(ret));
  }
  return ret;
}

int init_congestion_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  ObEThread **ethreads = NULL;
  if (OB_ISNULL(ethreads = g_event_processor.event_thread_[ET_CALL])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(ERROR, "fail to get ET_NET thread", K(ret));
  } else {
    for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(ethreads[i]->congestion_map_ = new (std::nothrow) ObCongestionRefHashMap(ObModIds::OB_PROXY_CONGESTION_ENTRY_MAP))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObCongestionRefHashMap", K(i), K(ethreads[i]), K(ret));
      } else if (OB_FAIL(ethreads[i]->congestion_map_->init())) {
        LOG_WARN("fail to init cgt_map", K(ret));
      }
    }
  }
  return ret;
}

int ObCongestionRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();

  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_entry_deleted()) {
        LOG_INFO("this congestion enty will erase from tc map", K(*it), KPC(*it));
        if (OB_FAIL(erase(it, i))) {
          LOG_WARN("fail to erase cgt entry", K(i), K(ret));
        }
      }
    }
  }

  return ret;

}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
