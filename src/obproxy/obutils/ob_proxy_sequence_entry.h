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

#ifndef OB_PROXY_SEQUENCE_ENTRY_H_
#define OB_PROXY_SEQUENCE_ENTRY_H_
#include "lib/hash_func/murmur_hash.h"
#include "lib/ob_define.h"
#include "lib/time/ob_hrtime.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_config_server_processor.h"
#include "dbconfig/ob_proxy_db_config_info.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObSequenceInfo {
public:
  ObSequenceInfo();
  ~ObSequenceInfo() {}
  bool is_valid() {
    if (!is_over_ && local_value_ < value_ + step_ &&
        local_value_ < max_value_ &&
        local_value_ >= min_value_ &&
        !is_expired()) {
      return true;
    }
    return false;
  }

  void reset() {
    seq_id_.reset();
    seq_name_.reset();
    tnt_id_.reset();
    tnt_col_.reset();
    gmt_create_.reset();
    gmt_modified_.reset();
    db_timestamp_.reset();
    timestamp_.reset();
    err_msg_.reset();
    value_ = 0;
    step_ = 0;
    min_value_ = 0;
    max_value_ = 0;
    local_value_ = 0;
    is_over_ = true;
    last_renew_time_ = 0;
    errno_ = 0;
  }

  ObSequenceInfo& operator=(const ObSequenceInfo& info) {
    seq_id_.set_value(info.seq_id_.config_string_);
    seq_name_.set_value(info.seq_name_.config_string_);
    tnt_id_.set_value(info.tnt_id_.config_string_);
    tnt_col_.set_value(info.tnt_col_.config_string_);
    gmt_create_.set_value(info.gmt_create_.config_string_);
    gmt_modified_.set_value(info.gmt_modified_.config_string_);
    db_timestamp_.set_value(info.db_timestamp_.config_string_);
    timestamp_.set_value(info.timestamp_.config_string_);
    err_msg_.set_value(info.err_msg_.config_string_);
    value_ = info.value_;
    step_ = info.step_;
    min_value_ = info.min_value_;
    max_value_ = info.max_value_;
    local_value_ = info.local_value_;
    is_over_ = info.is_over_;
    last_renew_time_ = info.last_renew_time_;
    errno_ = info.errno_;
    return *this;
  }
  bool is_expired();
  oceanbase::obproxy::obutils::ObProxyConfigString seq_id_;
  oceanbase::obproxy::obutils::ObProxyConfigString seq_name_;
  oceanbase::obproxy::obutils::ObProxyConfigString tnt_id_;
  oceanbase::obproxy::obutils::ObProxyConfigString tnt_col_;
  oceanbase::obproxy::obutils::ObProxyConfigString gmt_create_;
  oceanbase::obproxy::obutils::ObProxyConfigString gmt_modified_;
  oceanbase::obproxy::obutils::ObProxyConfigString db_timestamp_;
  oceanbase::obproxy::obutils::ObProxyConfigString timestamp_;
  oceanbase::obproxy::obutils::ObProxyConfigString err_msg_;
  int64_t value_;
  int64_t step_;
  int64_t min_value_;
  int64_t max_value_;
  int64_t local_value_;
  bool is_over_;
  int64_t last_renew_time_;
  int64_t errno_;
  DECLARE_TO_STRING;
};
class ObSequenceRouteParam
{
public:
  ObSequenceRouteParam() { reset(); }
  ~ObSequenceRouteParam() {
    if (NULL != shard_conn_) {
      shard_conn_->dec_ref();
      shard_conn_ = NULL;
    }
  }
  void reset();

  event::ObContinuation *cont_;
  dbconfig::ObShardConnector* shard_conn_;
  common::DBServerType server_type_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::ObString seq_name_;
  common::ObString table_name_;
  common::ObString seq_id_;
  common::ObString tnt_id_;
  obutils::ObProxyConfigString tnt_col_;
  common::ObString username_;
  common::ObString password_;
  int64_t min_value_;
  int64_t max_value_;
  int64_t step_;
  int64_t retry_count_;
  bool need_db_timestamp_;
  bool need_value_;
  bool only_need_db_timestamp_;
  DECLARE_TO_STRING;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSequenceRouteParam);
};

class ObSequenceEntry : public common::ObSharedRefCount
{
public:
  ObSequenceEntry();
  virtual ~ObSequenceEntry() {};

  void destroy() {op_free(this);}
  virtual void free() {destroy();}

  enum ObSEState {
    SE_BORN = 0,
    SE_INITING,
    SE_INIT_FAILED,
    SE_REMOTE_FATCHING,
    SE_AVAIL,
    SE_DEAD
  };

  DECLARE_TO_STRING;

  bool is_valid() {
    if (sequence_info_.is_valid() || sequence_info2_.is_valid()) {
      return true;
    }
    return false;
  }
  bool need_prefetch(int64_t ratio);
  void set_timestamp_fetching(bool is_fetch) {is_timestamp_fetching_ = is_fetch;}
  bool is_timestamp_fetching() { return is_timestamp_fetching_;}
  void change_use_info2_if_need();

  int init();
  bool is_remote_fetching_state() {return se_state_ == SE_REMOTE_FATCHING;}
  bool is_avail_state() {return se_state_ == SE_AVAIL;}

  void set_initing_state() {se_state_ = SE_INITING;}
  void set_init_failed_state() {se_state_ = SE_INIT_FAILED;}
  void set_remote_fetching_state() {se_state_ = SE_REMOTE_FATCHING;}
  void set_avail_state() {se_state_ = SE_AVAIL;}
  void set_dead_state() {se_state_ = SE_DEAD;}
public:
  obsys::CRWLock rwlock_;
  common::ObAtomicList pending_list_;
  common::ObAtomicList timestamp_pending_list_;
  bool is_timestamp_fetching_;
  ObSequenceInfo sequence_info_;
  ObSequenceInfo sequence_info2_;
  ObSEState se_state_;
  bool allow_insert_;
  LINK(ObSequenceEntry, sequence_entry_link_);
  DISALLOW_COPY_AND_ASSIGN(ObSequenceEntry);
};
typedef int (event::ObContinuation::*process_seq_pfn) (void *data);
class ObSequenceEntryCache
{
public:
  ObSequenceEntryCache() {}
  ~ObSequenceEntryCache() { destroy();}
  void destroy();
  static const int64_t MAX_SEQUENCE_ENTRY_COUNT = 1024;
  struct ObSequenceEntryHashing
  {
    typedef const common::ObString &Key;
    typedef ObSequenceEntry Value;
    typedef ObDLList(ObSequenceEntry, sequence_entry_link_) ListHead;

    static uint64_t hash(Key key) {return key.hash();}
    static Key key(Value* value) {return value->sequence_info_.seq_id_.config_string_;}
    static bool equal(Key lhs, Key rhs) {return lhs == rhs;}
  };

  int get_next_sequence(ObSequenceRouteParam& param,
                        process_seq_pfn process_sequence_info,
                        event::ObAction *&action);
  typedef common::hash::ObBuildInHashMap<ObSequenceEntryHashing, MAX_SEQUENCE_ENTRY_COUNT> SequenceEntryHashMap;
private:
  int create_entry_cont_task(ObSequenceEntry* sequence_entry,
                             const ObSequenceRouteParam& param,
                             event::ObAction *&action,
                             bool is_timestamp_task);
  int start_async_cont_if_need(ObSequenceRouteParam& param,
                               ObSequenceEntry* sequence_entry);
public:
  SequenceEntryHashMap sequence_entry_map_;
  obsys::CRWLock rwlock_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSequenceEntryCache);

};

class ObMysqlProxy;
class ObMysqlProxyEntry : public common::ObSharedRefCount
{
public:
  ObMysqlProxyEntry();
  ~ObMysqlProxyEntry();
  void destroy();
  virtual void free() {destroy();}

  void reset() {
    proxy_id_.reset();
  }

  oceanbase::obproxy::obutils::ObProxyConfigString proxy_id_;
  ObMysqlProxy* mysql_proxy_;
  LINK(ObMysqlProxyEntry, mysql_proxy_link_);
};

class ObMysqlProxyCache
{
public:
  ObMysqlProxyCache() {}
  ~ObMysqlProxyCache() {destroy();}
  void destroy();

  static const int64_t MAX_SEQUENCE_PROXY_COUNT = 100;
  struct ObMysqlProxyHashing
  {
    typedef const common::ObString &Key;
    typedef ObMysqlProxyEntry Value;
    typedef ObDLList(ObMysqlProxyEntry, mysql_proxy_link_) ListHead;

    static uint64_t hash(Key key) {return key.hash();}
    static Key key(Value* value) {return value->proxy_id_.config_string_;}
    static bool equal(Key lhs, Key rhs) {return lhs == rhs;}
  };
  typedef common::hash::ObBuildInHashMap<ObMysqlProxyHashing, MAX_SEQUENCE_PROXY_COUNT> MysqlProxyHashMap;

  ObMysqlProxyEntry* accquire_mysql_proxy_entry(const common::ObString& proxy_id);

  int add_mysql_proxy_entry(ObMysqlProxyEntry* entry);
  int remove_mysql_proxy_entry(ObMysqlProxyEntry* entry);
private:
  MysqlProxyHashMap proxy_entry_map_;
  obsys::CRWLock rwlock_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlProxyCache);
};

extern ObSequenceEntryCache &get_global_sequence_cache();
extern ObMysqlProxyCache &get_global_mysql_proxy_cache();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_PROXY_SEQUENCE_ENTRY_H_
