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

#ifndef OBPROXY_STAT_PROCESSOR_H
#define OBPROXY_STAT_PROCESSOR_H

#include "lib/allocator/page_arena.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_mutex.h"
#include "lib/string/ob_sql_string.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
}
namespace event
{
class ObEThread;
}
namespace obutils
{
class ObAsyncCommonTask;
}
enum ObRecType
{
  RECT_NULL = 0x00,
  RECT_PROCESS = 0x01,
  RECT_PLUGIN = 0x02,
  RECT_ALL = 0x04
};

enum ObRecPersistType
{
  RECP_NULL = 0,
  RECP_PERSISTENT
};

enum ObRecDataType
{
  RECD_NULL = 0,
  RECD_INT,
  RECD_FLOAT,
  RECD_COUNTER,
  RECD_MAX
};

enum ObRawStatSyncType
{
  SYNC_SUM = 0,
  SYNC_COUNT,
  SYNC_AVG,
  SYNC_HRTIME_AVG,
  SYNC_MHRTIME_AVG,
  SYNC_MSEC_AVG,
  SYNC_MAX
};

// Data Union
union ObRecData
{
  ObRecData() { ob_zero(*this); }
  ~ObRecData() { }

  int64_t rec_int_;
  int64_t rec_counter_;
  float rec_float_;
};

// RawStat Structures
struct ObRecRawStat
{
  ObRecRawStat() { ob_zero(*this); }
  ~ObRecRawStat() { }

  volatile int64_t sum_;
  volatile int64_t count_;

  // XXX - these will waist some space because they are only needed for the globals
  volatile int64_t last_sum_;   // value from the last global sync
  volatile int64_t last_count_; // value from the last global sync
};

// WARNING!  It's advised that developers do not modify the contents of
// the ObRecRawStatBlock.  ^_^
struct ObRecRecord;
struct ObRecRawStatBlock
{
  ObRecRawStatBlock()
  {
    ob_zero(*this);
    if (OB_UNLIKELY(common::OB_SUCCESS != common::mutex_init(&mutex_))) {
      PROXY_LOG(EDIAG, "fail to init mutex");
    }
  }
  ~ObRecRawStatBlock() { common::mutex_destroy(&mutex_); }

  int64_t ethr_stat_offset_;     // thread local raw-stat storage
  ObRecRawStat **global_;        // global raw-stat storage (ptr to ObRecRawStat)
  ObRecRecord **global_record_;  // global record storage (ptr to ObRecRecord)
  int64_t num_stats_;            // number of stats in this block
  int64_t max_stats_;            // maximum number of stats for this block
  const char *xflush_log_head_;  // for xflush log
  ObMutex mutex_;
  LINK(ObRecRawStatBlock, link_);
};

// Stat Callback Types
typedef int (*RecRawStatSyncCb)(ObRecDataType data_type, ObRecData &data,
                                ObRecRawStatBlock *rsb, const int64_t id);

struct ObRecStatMeta
{
  ObRecStatMeta()
      : sync_cb_(NULL), persist_type_(RECP_PERSISTENT) ,
        sync_rsb_(NULL), sync_id_(-1) {}
  ~ObRecStatMeta() { }

  ObRecRawStat data_raw_;
  RecRawStatSyncCb sync_cb_;
  ObRecPersistType persist_type_;
  ObRecRawStatBlock *sync_rsb_;
  int64_t sync_id_;
};

struct ObRecRecord
{
  ObRecRecord()
      : name_(NULL), rec_type_(RECT_NULL), data_type_(RECD_NULL)
  {
    if (OB_UNLIKELY(common::OB_SUCCESS != common::mutex_init(&mutex_))) {
      PROXY_LOG(WDIAG, "fail to int mutex");
    }
  }
  ~ObRecRecord() { common::mutex_destroy(&mutex_); }

  const char *name_;
  ObRecType rec_type_;
  ObRecDataType data_type_;
  ObRecData data_;
  ObRecData data_default_;
  ObMutex mutex_;
  ObRecStatMeta stat_meta_;
  LINK(ObRecRecord, link_);
};

class ObStatProcessor
{
public:
  ObStatProcessor();
  ~ObStatProcessor();

  int init(proxy::ObMysqlProxy &mysql_proxy_);
  int start_stat_task();

  ObRecRawStatBlock *allocate_raw_stat_block(const int64_t num_stats, const char *xflush_log_head);
  int register_raw_stat(ObRecRawStatBlock *rsb, ObRecType rec_type,
                        const char *name, ObRecDataType data_type,
                        const int64_t id, const ObRawStatSyncType sync_type,
                        const ObRecPersistType persist_type);

  // Setters for manipulating internal sleep intervals,
  //add an immiediate task for stat continuation
  int set_stat_table_sync_interval();
  int set_stat_dump_interval();

  int exec_raw_stat_sync_cbs();

  static int do_sync_stat_table();
  static int do_dump_stat();

  static void update_stat_table_sync_interval();
  static void update_stat_dump_interval();

  int sync_all_proxy_stat();
  int sync_stat_table();
  int dump();
  obutils::ObAsyncCommonTask *get_stat_table_cont() { return stat_table_cont_; }
  obutils::ObAsyncCommonTask *get_stat_dump_cont() {return stat_dump_cont_; }

  bool is_started() const { return NULL != stat_table_cont_ && NULL != stat_dump_cont_; };
  bool is_mysql_proxy_inited() const;

  static inline int atomic_incr_raw_stat_sum(ObRecRawStatBlock *rsb, event::ObEThread *ethread,
                                             const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, ethread))) {
      ret = common::OB_ERR_UNEXPECTED;
      _PROXY_LOG(WDIAG, "fail to get tlp_raw_stat, ret=%d", ret);
    } else {
      (void)ATOMIC_FAA(&(tlp->sum_), incr);
    }
    return ret;
  }

  static inline int get_thread_raw_stat_sum(ObRecRawStatBlock *rsb, event::ObEThread *ethread,
                                            const int64_t id, int64_t &data)
  {
    int ret = common::OB_SUCCESS;
    data = 0;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, ethread))) {
      ret = common::OB_ERR_UNEXPECTED;
      _PROXY_LOG(WDIAG, "fail to get tlp_raw_stat, ret=%d", ret);
    } else {
      data = tlp->sum_;
    }
    return ret;
  }

  static inline int64_t get_thread_raw_stat_sum(ObRecRawStatBlock *rsb, const event::ObEThread *ethread,
                                                const int64_t id)
  {
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, const_cast<event::ObEThread *>(ethread)))) {
      PROXY_LOG(WDIAG, "fail to get tlp_raw_stat");
    }
    return NULL == tlp ? 0 : tlp->sum_;
  }

  static inline int incr_raw_stat(ObRecRawStatBlock *rsb, event::ObEThread *ethread,
                                  const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, ethread))) {
      ret = common::OB_ERR_UNEXPECTED;
      _PROXY_LOG(WDIAG, "fail to get tlp_raw_stat, ret=%d", ret);
    } else {
      tlp->sum_ += incr;
      tlp->count_ += 1;
    }
    return ret;
  }

  static inline int decr_raw_stat(ObRecRawStatBlock *rsb, event::ObEThread *ethread,
                                  const int64_t id, int64_t decr)
  {
    int ret = common::OB_SUCCESS;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, ethread))) {
      ret = common::OB_ERR_UNEXPECTED;
      _PROXY_LOG(WDIAG, "fail to get tlp_raw_stat, ret=%d", ret);
    } else {
      tlp->sum_ -= decr;
      tlp->count_ += 1;
    }
    return ret;
  }

  static inline int incr_raw_stat_sum(ObRecRawStatBlock *rsb, event::ObEThread *ethread,
                                      const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, ethread))) {
      ret = common::OB_ERR_UNEXPECTED;
      _PROXY_LOG(WDIAG, "fail to get tlp_raw_stat, ret=%d", ret);
    } else {
      tlp->sum_ += incr;
    }
    return ret;
  }

  static inline int incr_raw_stat_count(ObRecRawStatBlock *rsb, event::ObEThread *ethread,
                                        const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat(rsb, id, ethread))) {
      ret = common::OB_ERR_UNEXPECTED;
      _PROXY_LOG(WDIAG, "fail to get tlp_raw_stat, ret=%d", ret);
    } else {
      tlp->count_ += incr;
    }
    return ret;
  }

  static inline int set_raw_stat_sum(ObRecRawStatBlock *rsb,
                                     const int64_t id, const int64_t data)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(clear_raw_stat_sum(rsb, id))) {
      _PROXY_LOG(WDIAG, "fail to clear raw stat num, ret=%d", ret);
    } else {
      (void)ATOMIC_SET(&(rsb->global_[id]->sum_), data);
    }
    return ret;
  }

  static inline int set_raw_stat_count(ObRecRawStatBlock *rsb,
                                       const int64_t id, const int64_t data)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(clear_raw_stat_count(rsb, id))) {
      _PROXY_LOG(WDIAG, "fail to clear raw stat num, ret=%d", ret);
    } else {
      (void)ATOMIC_SET(&(rsb->global_[id]->count_), data);
    }
    return ret;
  }

  static inline int get_raw_stat_sum(ObRecRawStatBlock *rsb,
                                     const int64_t id, int64_t &data)
  {
    int ret = common::OB_SUCCESS;
    data = 0;
    ObRecRawStat total;
    if (OB_FAIL(get_raw_stat_total(rsb, id, total))) {
      _PROXY_LOG(WDIAG, "fail to get raw stat total, ret=%d", ret);
    } else {
      data = total.sum_;
    }
    return ret;
  }

  static inline int get_raw_stat_count(ObRecRawStatBlock *rsb,
                                       const int64_t id, int64_t &data)
  {
    int ret = common::OB_SUCCESS;
    data = 0;
    ObRecRawStat total;
    if (OB_FAIL(get_raw_stat_total(rsb, id, total))) {
      _PROXY_LOG(WDIAG, "fail to get raw stat total, ret=%d", ret);
    } else {
      data = total.count_;
    }
    return ret;
  }

  // Global RawStat Items (e.g. same as above, but no thread-local behavior)
  static inline int incr_global_raw_stat(ObRecRawStatBlock *rsb,
                                         const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      (void)ATOMIC_FAA(&(rsb->global_[id]->sum_), incr);
      (void)ATOMIC_FAA(&(rsb->global_[id]->count_), 1);
    }
    return ret;
  }

  static inline int incr_global_raw_stat_sum(ObRecRawStatBlock *rsb,
                                             const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      (void)ATOMIC_FAA(&(rsb->global_[id]->sum_), incr);
    }
    return ret;
  }

  static inline int incr_global_raw_stat_count(ObRecRawStatBlock *rsb,
                                               const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      (void)ATOMIC_FAA(&(rsb->global_[id]->count_), incr);
    }
    return ret;
  }

  static inline int set_global_raw_stat_sum(ObRecRawStatBlock *rsb,
                                            const int64_t id, const int64_t data)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      (void)ATOMIC_SET(&(rsb->global_[id]->sum_), data);
    }
    return ret;
  }

  static inline int set_global_raw_stat_count(ObRecRawStatBlock *rsb,
                                              const int64_t id, const int64_t data)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      (void)ATOMIC_SET(&(rsb->global_[id]->count_), data);
    }
    return ret;
  }

  static inline int get_global_raw_stat_sum(ObRecRawStatBlock *rsb,
                                            const int64_t id, int64_t &data)
  {
    int ret = common::OB_SUCCESS;
    data = 0;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      data = ATOMIC_LOAD(&rsb->global_[id]->sum_);
    }
    return ret;
  }

  static inline int get_global_raw_stat_count(ObRecRawStatBlock *rsb,
                                              const int64_t id, int64_t &data)
  {
    int ret = common::OB_SUCCESS;
    data = 0;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else {
      data = ATOMIC_LOAD(&rsb->global_[id]->count_);
    }
    return ret;
  }

  static inline int64_t get_global_raw_stat_sum(ObRecRawStatBlock *rsb, const int64_t id)
  {
    return check_argument(rsb, id) ? rsb->global_[id]->sum_ : 0;
  }

  static inline int64_t get_global_raw_stat_count(ObRecRawStatBlock *rsb, const int64_t id)
  {
    return check_argument(rsb, id) ? rsb->global_[id]->count_ : 0;
  }

  // log module func
  // only for log module for recording warn and error log,
  // so it can't print log

  static inline ObRecRawStat *get_tlp_raw_stat_no_log(ObRecRawStatBlock *rsb, const int64_t id)
  {
    event::ObEThread *ethread = NULL;
    ObRecRawStat *raw_stat = NULL;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      //do nothing
    } else if (OB_ISNULL(ethread = get_this_ethread())) {
      //do nothing
    } else {
      raw_stat = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(ethread) + rsb->ethr_stat_offset_) + id;
    }
    return raw_stat;
  }

  static inline int incr_raw_stat_sum_no_log(ObRecRawStatBlock *rsb, const int64_t id, const int64_t incr)
  {
    int ret = common::OB_SUCCESS;
    ObRecRawStat *tlp = NULL;
    if (OB_ISNULL(tlp = get_tlp_raw_stat_no_log(rsb, id))) {
      ret = common::OB_ERR_UNEXPECTED;
    } else {
      tlp->sum_ += incr;
    }
    return ret;
  }

  static inline int set_global_raw_stat_sum_no_log(ObRecRawStatBlock *rsb,
                                                   const int64_t id, const int64_t data)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      (void)ATOMIC_SET(&(rsb->global_[id]->sum_), data);
    }
    return ret;
  }
  // end log module func

  inline const ObRecRecord *get_all_records_head()
  {
    return all_records_.head();
  }

  inline const ObRecRecord *get_all_records_next(ObRecRecord *record)
  {
    return NULL == record ? NULL : all_records_.next(record);
  }

  int report_session_stats(const char *cluster_name, const uint64_t session_id,
                           const int64_t *stats, const bool is_first_register,
                           const char **stat_names, const int64_t stats_size);

public:
  static const int64_t MAX_RUNNING_SESSION_STAT_REPROT_TASK_COUNT = 8;

  static const char *INSERT_PROXY_STAT_SQL_HEAD;
  static const char *INSERT_PROXY_STAT_SQL_VALUES_AND_INFO;
  static const char *INSERT_PROXY_STAT_SQL_VALUES_AND_INFO_END;
  static const char *INSERT_PROXY_STAT_SQL_VALUES;
  static const char *INSERT_PROXY_STAT_SQL_VALUES_END;

private:
  void print_xflush_log();

  static bool set_rec_data(ObRecDataType data_type, ObRecData &data_dst, ObRecData &data_src);
  static bool set_rec_data_from_int64(ObRecDataType data_type, ObRecData &data_dst, int64_t data_int64);
  static bool set_rec_data_from_float(ObRecDataType data_type, ObRecData &data_dst, float data_float);

  static int get_raw_stat_total(ObRecRawStatBlock *rsb, const int64_t id, ObRecRawStat &total);
  static int sync_raw_stat_to_global(ObRecRawStatBlock *rsb, const int64_t id);
  static int clear_raw_stat(ObRecRawStatBlock *rsb, const int64_t id);
  static int clear_raw_stat_sum(ObRecRawStatBlock *rsb, const int64_t id);
  static int clear_raw_stat_count(ObRecRawStatBlock *rsb, const int64_t id);

  // Predefined ObRawStat Callbacks
  static int sync_raw_stat_sum(ObRecDataType data_type, ObRecData &data,
                               ObRecRawStatBlock *rsb, const int64_t id);
  static int sync_raw_stat_count(ObRecDataType data_type, ObRecData &data,
                                 ObRecRawStatBlock *rsb, const int64_t id);
  static int sync_raw_stat_avg(ObRecDataType data_type, ObRecData &data,
                               ObRecRawStatBlock *rsb, const int64_t id);
  static int sync_raw_stat_hrtime_avg(ObRecDataType data_type, ObRecData &data,
                                      ObRecRawStatBlock *rsb, const int64_t id);
  static int sync_raw_stat_int_msecs_to_float_seconds(ObRecDataType data_type, ObRecData &data,
                                                      ObRecRawStatBlock *rsb, const int64_t id);
  static int sync_raw_stat_mhrtime_avg(ObRecDataType data_type, ObRecData &data,
                                       ObRecRawStatBlock *rsb, const int64_t id);

  static event::ObEThread *get_this_ethread();

  // ObRawStat Setting/Getting
  // Note: The following incr_raw_stat_XXX calls are fast and don't
  // require any atomic_xxx()'s to be executed. Use these RawStat
  // functions over other ObRawStat functions whenever possible.
  static inline ObRecRawStat *get_tlp_raw_stat(ObRecRawStatBlock *rsb, const int64_t id,
                                               event::ObEThread *ethread)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!check_argument(rsb, id))) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "invalid rsb or id, rsb=%p, id=%ld, ret=%d", rsb, id, ret);
    } else if (OB_ISNULL(ethread) && OB_ISNULL(ethread = get_this_ethread())) {
      ret = common::OB_INVALID_ARGUMENT;
      _PROXY_LOG(WDIAG, "ethread is null, ret=%d", ret);
    }
    return common::OB_SUCCESS == ret
           ? reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(ethread) + rsb->ethr_stat_offset_) + id
           : NULL;
  }

  static inline bool check_argument(ObRecRawStatBlock *rsb, const int64_t id)
  {
    bool bret = true;
    if (OB_ISNULL(rsb) || OB_UNLIKELY(id < 0) || OB_UNLIKELY(id >= rsb->max_stats_)) {
      bret = false;
    }
    return bret;
  }

private:
  bool is_inited_;
  bool is_first_register_;//we need sync all stats when is_first_register_ is false
  proxy::ObMysqlProxy *mysql_proxy_; //mysql_proxy_ is used to update stat columns in PROXY_INFO_TABLE_NAME;
  obutils::ObAsyncCommonTask *stat_table_cont_;
  obutils::ObAsyncCommonTask *stat_dump_cont_;
  common::PageArena<char> allocator_;
  ObMutex allocator_lock_;

  ASLL(ObRecRecord, link_) all_records_;
  volatile int64_t record_count_;

  ASLL(ObRecRawStatBlock, link_) all_blocks_;

  char proxy_ip_[common::MAX_IP_ADDR_LENGTH]; // ip primary key
  int32_t proxy_port_;                    // port primary key
  DISALLOW_COPY_AND_ASSIGN(ObStatProcessor);
};

extern ObStatProcessor g_stat_processor;
extern volatile int64_t g_current_report_count;

} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_STAT_PROCESSOR_H
