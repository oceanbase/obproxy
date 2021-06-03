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

#include "stat/ob_stat_processor.h"
#include "iocore/eventsystem/ob_event.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_resource_pool_processor.h"
#include "utils/ob_proxy_table_define.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "stat/ob_proxy_warning_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{

//-------ObSessionStatTableSync------
struct ObSessionStatTableSync : public event::ObContinuation
{
  explicit ObSessionStatTableSync(event::ObProxyMutex *m)
      : ObContinuation(m), mysql_proxy_(NULL), row_count_(0)
  {
    SET_HANDLER(&ObSessionStatTableSync::sync);
  }

  virtual ~ObSessionStatTableSync() { }

  int init(proxy::ObMysqlProxy &mysql_proxy, const char *cluster_name,
           const uint64_t session_id, const int64_t *stats,
           const bool is_first_register, const char **stat_names, const int64_t stats_size);

  void destroy()
  {
    mutex_.release();
    sql_.~ObSqlString();
    op_reclaim_free(this);
  }

  int sync(int event, event::ObEvent *e);

  proxy::ObMysqlProxy *mysql_proxy_;
  common::ObSqlString sql_;
  int64_t row_count_;
};

ObStatProcessor g_stat_processor;
volatile int64_t g_current_report_count                                             = 0;

const char *ObStatProcessor::INSERT_PROXY_STAT_SQL_HEAD                             =
    "INSERT INTO %s "
    "(cluster_name, proxy_ip, proxy_port, session_id, stat_name, value, info) VALUES ";

const char *ObStatProcessor::ObStatProcessor::INSERT_PROXY_STAT_SQL_VALUES_AND_INFO =
    "('%s', '%s', %d, %ld, '%s', %ld, '%s'),";

const char *ObStatProcessor::INSERT_PROXY_STAT_SQL_VALUES_AND_INFO_END              =
    "('%s', '%s', %d, %ld, '%s', %ld, '%s') "
    "ON DUPLICATE KEY UPDATE value = VALUES(value), info=VALUES(info);\n";

const char *ObStatProcessor::ObStatProcessor::INSERT_PROXY_STAT_SQL_VALUES          =
    "('%s', '%s', %d, %lu, '%s', %ld, ''),";

const char *ObStatProcessor::INSERT_PROXY_STAT_SQL_VALUES_END                       =
    "('%s', '%s', %d, %lu, '%s', %ld, '') "
    "ON DUPLICATE KEY UPDATE value = VALUES(value);\n";


int ObSessionStatTableSync::init(ObMysqlProxy &mysql_proxy, const char *cluster_name,
                                 const uint64_t session_id, const int64_t *stats,
                                 const bool is_first_register, const char **stat_names,
                                 const int64_t stats_size)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  char ip_str[OB_IP_STR_BUFF];
  ip_str[0] = '\0';

  if (OB_ISNULL(cluster_name)
      || OB_ISNULL(stats) || OB_ISNULL(stat_names)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter", K(cluster_name), K(stats), K(stat_names), K(ret));
  } else if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_local_addr(addr))) {
    LOG_WARN("fail to get proxy local addr", K(addr), K(ret));
  } else if (!addr.ip_to_string(ip_str, OB_IP_STR_BUFF)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to covert ip to string", K(addr), K(ret));
  } else if (OB_FAIL(sql_.append_fmt(ObStatProcessor::INSERT_PROXY_STAT_SQL_HEAD,
                                    ObProxyTableInfo::PROXY_STAT_TABLE_NAME))) {
    LOG_WARN("fail to append stmt", K(ret));
  } else {
    mysql_proxy_ = &mysql_proxy;
    const int64_t first_report_size = stats_size - 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < first_report_size; ++i) {
      //only first register, or value is not zero, we report it
      if (is_first_register || 0 != stats[i]) {
        if (OB_FAIL(sql_.append_fmt(ObStatProcessor::INSERT_PROXY_STAT_SQL_VALUES, cluster_name, ip_str,
                                    addr.get_port(), session_id, stat_names[i],
                                    stats[i]))) {
          const ObString simplified_sql(std::min(sql_.length(), PRINT_SQL_LEN), sql_.ptr());
          LOG_WARN("fail to append stmt", K(simplified_sql), "size", sql_.length(), K(ret));
        } else {
          ++row_count_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql_.append_fmt(ObStatProcessor::INSERT_PROXY_STAT_SQL_VALUES_END, cluster_name, ip_str,
                                  addr.get_port(), session_id, stat_names[stats_size - 1],
                                  stats[stats_size - 1]))) {
        const ObString simplified_sql(std::min(sql_.length(), PRINT_SQL_LEN), sql_.ptr());
        LOG_WARN("fail to append stmt", K(simplified_sql), "size", sql_.length(), K(ret));
      } else {
        ++row_count_;
        const ObString simplified_sql(std::min(sql_.length(), PRINT_SQL_LEN), sql_.ptr());
        LOG_DEBUG("succ to append sync stats sql", "size", sql_.length(), K(simplified_sql), K_(row_count));
      }
    }
  }

  return ret;
}

int ObSessionStatTableSync::sync(int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);

  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;

  LOG_DEBUG("ObSessionStatTableSync() processed");

  if (OB_FAIL(mysql_proxy_->write(sql_.ptr(), affected_rows))) {
    LOG_WARN("fail to execute", K_(sql), K(ret));
  } else if (OB_UNLIKELY(affected_rows > row_count_ * 2) || OB_UNLIKELY(affected_rows < 0)) {
    LOG_WARN("affected_rows is not expected", "size", sql_.length(),
             K_(sql), K_(row_count), K(affected_rows), K(ret));
  } else {/*do nothing*/}

  (void)ATOMIC_FAA(&g_current_report_count, -1);
  destroy();

  return EVENT_DONE;
}

ObStatProcessor::ObStatProcessor()
    : is_inited_(false), is_first_register_(true), mysql_proxy_(NULL),
      stat_table_cont_(NULL), stat_dump_cont_(NULL),
      record_count_(0), proxy_port_(0)
{
  MEMSET(proxy_ip_, 0, sizeof(proxy_ip_));
}

ObStatProcessor::~ObStatProcessor()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(stat_table_cont_))) {
    LOG_WARN("fail to destroy stat table sync task", K(ret));
  }
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(stat_dump_cont_))) {
    LOG_WARN("fail to destroy stat dump task", K(ret));
  }

  if(OB_FAIL(mutex_destroy(&allocator_lock_))) {
    LOG_WARN("failed to destroy allocator mutex", K(ret));
  }
  is_inited_ = false;
}

int ObStatProcessor::init(ObMysqlProxy &mysql_proxy)
{
  int ret = OB_SUCCESS;
  ObAddr local_addr;
  mysql_proxy_ = &mysql_proxy;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(mutex_init(&allocator_lock_))) {
    LOG_WARN("fail to init mutex", K(ret));
  } else if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_local_addr(local_addr))) {
    LOG_WARN("fail to get proxy local addr", K(local_addr), K(ret));
  } else if (OB_UNLIKELY(!local_addr.ip_to_string(proxy_ip_, static_cast<int32_t>(sizeof(proxy_ip_))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to covert ip to string", K(local_addr), K(ret));
  } else {
    proxy_port_ = local_addr.get_port();
    is_inited_ = true;
  }

  return ret;
}

int ObStatProcessor::start_stat_task()
{
  int ret = OB_SUCCESS;
  int64_t interval_us = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL != stat_table_cont_)
             || OB_UNLIKELY(NULL != stat_dump_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat_table_cont should be null here", K_(stat_table_cont), K_(stat_dump_cont), K(ret));
  }

  if (OB_SUCC(ret)) {
    interval_us = ObRandomNumUtils::get_random_half_to_full(
                  get_global_proxy_config().stat_table_sync_interval);
    if (OB_ISNULL(stat_table_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                     "stat_table_sync_task",
                                     ObStatProcessor::do_sync_stat_table,
                                     ObStatProcessor::update_stat_table_sync_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start stat_table_sync task", K(ret));
    } else {
      LOG_INFO("succ to start stat table sync task", K(interval_us));
    }
  }

  if (OB_SUCC(ret)) {
    interval_us = get_global_proxy_config().stat_dump_interval;
    if (OB_ISNULL(stat_dump_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                    "stat_dump_task",
                                    ObStatProcessor::do_dump_stat,
                                    ObStatProcessor::update_stat_dump_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start stat_dump task", K(ret));
    } else {
      LOG_INFO("succ to start stat dump task", K(interval_us));
    }
  }
  return ret;
}

void ObStatProcessor::print_xflush_log()
{
  int ret = OB_SUCCESS;
  int64_t len = OB_MALLOC_NORMAL_BLOCK_SIZE;
  char *buf = NULL;

  if (OB_ISNULL(buf = reinterpret_cast<char *>(ob_malloc(len, ObModIds::OB_PROXY_STAT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    int64_t pos = 0;
    int64_t i = 0;
    int64_t value = 0;
    ObRecRecord *record = NULL;
    ObRecRawStatBlock *block = all_blocks_.head();

    while (OB_SUCC(ret) && (NULL != block)) {
      i = 0;
      pos = 0;
      record = NULL;
      for (i = 0; OB_SUCC(ret) && (i < block->max_stats_); ++i) {
        if (OB_ISNULL(record = block->global_record_[i])) {
          // do nothing
        } else {
          value = (RECD_FLOAT != record->data_type_) ?
            record->data_.rec_int_ : static_cast<int64_t>(record->data_.rec_float_);
          if(OB_FAIL(databuff_printf(buf, len, pos, "%s=%ld,", record->name_, value))) {
            LOG_WARN("fail to fill stat dump buf", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        _OBPROXY_XF_LOG(INFO, "%s(%s)", block->xflush_log_head_, buf);
        buf[0] = '\0';
        block = all_blocks_.next(block);
      }
    }

    if (OB_LIKELY(NULL != buf)) {
      ob_free(buf);
      buf = NULL;
    }
  }
}

ObRecRawStatBlock *ObStatProcessor::allocate_raw_stat_block(const int64_t num_stats, const char *xflush_log_head)
{
  int ret = OB_SUCCESS;
  int64_t ethr_stat_offset = 0;
  ObRecRawStatBlock *rsb = NULL;
  int64_t stat_size = num_stats * sizeof(ObRecRawStat);
  // allocate thread-local raw-stat memory
  if (OB_UNLIKELY(stat_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid num_stats", K(stat_size), K(ret));
  } else if (OB_LIKELY(-1 != (ethr_stat_offset = g_event_processor.allocate(stat_size)))) {
    // create the raw-stat-block structure
    if (OB_SUCC(mutex_acquire(&allocator_lock_))) {
      rsb = allocator_.new_object<ObRecRawStatBlock>();
      if (OB_LIKELY(NULL != rsb)) {
        rsb->ethr_stat_offset_ = ethr_stat_offset;
        rsb->num_stats_ = 0;
        rsb->max_stats_ = num_stats;
        rsb->xflush_log_head_ = xflush_log_head;
        rsb->global_ = reinterpret_cast<ObRecRawStat **>(allocator_.alloc_aligned(num_stats * sizeof(ObRecRawStat *)));
        rsb->global_record_ = reinterpret_cast<ObRecRecord **>(allocator_.alloc_aligned(num_stats * sizeof(ObRecRecord *)));
        if (NULL != rsb->global_ && NULL != rsb->global_record_) {
          memset(rsb->global_, 0, num_stats * sizeof(ObRecRawStat *));
          memset(rsb->global_record_, 0, num_stats * sizeof(ObRecRecord *));
          all_blocks_.push(rsb);
        } else {
          rsb = NULL;
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = mutex_release(&allocator_lock_))) {
        LOG_WARN("fail to release mutex", K(tmp_ret));
      }
    }
  } else {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory from thread local data buffer", K(ret));
  }

  return rsb;
}

int ObStatProcessor::register_raw_stat(
    ObRecRawStatBlock *rsb, ObRecType rec_type,
    const char *name, ObRecDataType data_type, const int64_t id,
    const ObRawStatSyncType sync_type, const ObRecPersistType persist_type)
{
  int ret = OB_SUCCESS;
  static RecRawStatSyncCb sync_cb[SYNC_MAX] = {
    ObStatProcessor::sync_raw_stat_sum,
    ObStatProcessor::sync_raw_stat_count,
    ObStatProcessor::sync_raw_stat_avg,
    ObStatProcessor::sync_raw_stat_mhrtime_avg,
    ObStatProcessor::sync_raw_stat_int_msecs_to_float_seconds
  };
  ObRecRecord *r = NULL;
  ObRecData data_default;
  memset(&data_default, 0, sizeof(ObRecData));

  if (OB_UNLIKELY(!check_argument(rsb, id)) || OB_UNLIKELY(sync_type > SYNC_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rsb or id", K(rsb), K(id), K(sync_type), K(ret));
  } else {
    {
      if (OB_SUCC(mutex_acquire(&allocator_lock_))) {
        r = allocator_.new_object<ObRecRecord>();
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = mutex_release(&allocator_lock_))) {
          LOG_WARN("fail to release mutex", K(tmp_ret));
        }
      }
    }

    if (NULL != r) {
      r->rec_type_ = rec_type;
      r->name_ = name;
      r->data_type_ = data_type;
      set_rec_data(data_type, r->data_, data_default);
      set_rec_data(data_type, r->data_default_, data_default);

      // store a pointer to our record->stat_meta.data_raw in our rsb
      rsb->global_[id] = &(r->stat_meta_.data_raw_);
      rsb->global_[id]->last_sum_ = 0;
      rsb->global_[id]->last_count_ = 0;
      rsb->global_record_[id] = r;

      if (OB_FAIL(mutex_acquire(&r->mutex_))) {
        LOG_ERROR("fail to acquire mutex", K(ret));
      } else {
        r->stat_meta_.persist_type_ = persist_type;
        r->stat_meta_.sync_rsb_ = rsb;
        r->stat_meta_.sync_id_ = id;
        r->stat_meta_.sync_cb_ = sync_cb[sync_type];
        if (OB_FAIL(mutex_release(&r->mutex_))) {
          LOG_ERROR("fail to release mutex", K(ret));
        } else {
          all_records_.push(r);
          (void)ATOMIC_FAA(&record_count_, 1);
        }
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory error", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_raw_stat_sum(rsb, id, 0))) {
      LOG_WARN("fail to set raw stat num to 0", K(ret));
    } else if (OB_FAIL(set_raw_stat_count(rsb, id, 0))) {
      LOG_WARN("fail to set raw stat count to 0", K(ret));
    }
  }
  return ret;
}

int ObStatProcessor::set_stat_table_sync_interval()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat processor is not inited", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(stat_table_cont_))) {
    LOG_WARN("fail to set stat_table_sync interval", K(ret));
  }
  return ret;
}

int ObStatProcessor::set_stat_dump_interval()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat processor is not inited", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(stat_dump_cont_))) {
    LOG_WARN("fail to set stat_dump interval", K(ret));
  }
  return ret;
}

inline bool ObStatProcessor::is_mysql_proxy_inited() const
{
  return OB_LIKELY(NULL != mysql_proxy_) && mysql_proxy_->is_inited();
}

int ObStatProcessor::exec_raw_stat_sync_cbs()
{
  int ret = OB_SUCCESS;
  PROXY_SET_GLOBAL_DYN_STAT(ALLOCED_TINY_LOG_ITEM_COUNT, ObLogger::get_logger().get_alloced_log_item_count(LOG_ITEM_TINY));
  PROXY_SET_GLOBAL_DYN_STAT(ACTIVE_TINY_LOG_ITEM_COUNT, ObLogger::get_logger().get_active_log_item_count(LOG_ITEM_TINY));
  PROXY_SET_GLOBAL_DYN_STAT(RELEASED_TINY_LOG_ITEM_COUNT, ObLogger::get_logger().get_released_log_item_count(LOG_ITEM_TINY));
  PROXY_SET_GLOBAL_DYN_STAT(ALLOCED_NORMAL_LOG_ITEM_COUNT, ObLogger::get_logger().get_alloced_log_item_count(LOG_ITEM_NORMAL));
  PROXY_SET_GLOBAL_DYN_STAT(ACTIVE_NORMAL_LOG_ITEM_COUNT, ObLogger::get_logger().get_active_log_item_count(LOG_ITEM_NORMAL));
  PROXY_SET_GLOBAL_DYN_STAT(RELEASED_NORMAL_LOG_ITEM_COUNT, ObLogger::get_logger().get_released_log_item_count(LOG_ITEM_NORMAL));
  PROXY_SET_GLOBAL_DYN_STAT(ALLOCED_LARGE_LOG_ITEM_COUNT, ObLogger::get_logger().get_alloced_log_item_count(LOG_ITEM_LARGE));
  PROXY_SET_GLOBAL_DYN_STAT(ACTIVE_LARGE_LOG_ITEM_COUNT, ObLogger::get_logger().get_active_log_item_count(LOG_ITEM_LARGE));
  PROXY_SET_GLOBAL_DYN_STAT(RELEASED_LARGE_LOG_ITEM_COUNT, ObLogger::get_logger().get_released_log_item_count(LOG_ITEM_LARGE));
  PROXY_SET_GLOBAL_DYN_STAT(DEFAULT_LOG_WRITE_SIZE, ObLogger::get_logger().get_write_size());
  PROXY_SET_GLOBAL_DYN_STAT(DEFAULT_LOG_WRITE_COUNT, ObLogger::get_logger().get_write_count());
  PROXY_SET_GLOBAL_DYN_STAT(DEFAULT_LARGE_LOG_WRITE_COUNT, ObLogger::get_logger().get_large_write_count());
  PROXY_SET_GLOBAL_DYN_STAT(XFLUSH_LOG_WRITE_SIZE, ObLogger::get_logger().get_xflush_write_size());
  PROXY_SET_GLOBAL_DYN_STAT(XFLUSH_LOG_WRITE_COUNT, ObLogger::get_logger().get_xflush_write_count());
  PROXY_SET_GLOBAL_DYN_STAT(XFLUSH_LARGE_LOG_WRITE_COUNT, ObLogger::get_logger().get_xflush_large_write_count());

  PROXY_SET_GLOBAL_DYN_STAT(DROPPED_ERROR_LOG_COUNT, ObLogger::get_logger().get_dropped_error_log_count());
  PROXY_SET_GLOBAL_DYN_STAT(DROPPED_WARN_LOG_COUNT, ObLogger::get_logger().get_dropped_warn_log_count());
  PROXY_SET_GLOBAL_DYN_STAT(DROPPED_INFO_LOG_COUNT, ObLogger::get_logger().get_dropped_info_log_count());
  PROXY_SET_GLOBAL_DYN_STAT(DROPPED_TRACE_LOG_COUNT, ObLogger::get_logger().get_dropped_trace_log_count());
  PROXY_SET_GLOBAL_DYN_STAT(DROPPED_DEBUG_LOG_COUNT, ObLogger::get_logger().get_dropped_debug_log_count());
  PROXY_SET_GLOBAL_DYN_STAT(ASYNC_FLUSH_LOG_SPEED, ObLogger::get_logger().get_async_flush_log_speed());

  ObRecRecord *r = all_records_.head();
  while (OB_SUCC(ret) && NULL != r) {
    if (OB_FAIL(mutex_acquire(&r->mutex_))) {
      LOG_ERROR("fail to acquire mutex", K(ret));
    } else {
      if (OB_ISNULL(r->stat_meta_.sync_cb_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sync_cb is null", K(ret));
      } else if (OB_FAIL((*(r->stat_meta_.sync_cb_))(r->data_type_, r->data_,
                         r->stat_meta_.sync_rsb_, r->stat_meta_.sync_id_))) {
        LOG_WARN("fail to exex raw stat sync cb", K(ret));
      } else {/*do nothing*/}

      if (OB_FAIL(mutex_release(&r->mutex_))){
        LOG_ERROR("fail to release mutex", K(ret));
      } else {
        r = all_records_.next(r);
      }
    }
  }

  return ret;
}

int ObStatProcessor::sync_all_proxy_stat()
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  ObSqlString sql;

  print_xflush_log();

  if (OB_FAIL(sql.append_fmt(INSERT_PROXY_STAT_SQL_HEAD, ObProxyTableInfo::PROXY_STAT_TABLE_NAME))) {
    LOG_WARN("fail to append stmt", K(ret));
  } else {
    int64_t value = 0;
    const char *COMMON_INFO = "";
    ObRecRecord *r = all_records_.head();
    bool enable_sync_all_stats = get_global_proxy_config().enable_sync_all_stats;

    while (OB_SUCC(ret) && NULL != r) {
      value = (RECD_FLOAT != r->data_type_) ? r->data_.rec_int_
                                            : static_cast<int64_t>(r->data_.rec_float_);
      //we will report stat only the follow all happened
      //1. enable_sync_all_stats or PERSISTENT type
      //2. first register or value is not zero,
      if ((enable_sync_all_stats || RECP_PERSISTENT == r->stat_meta_.persist_type_)
          && (is_first_register_ || 0 != value)) {
        if (OB_FAIL(sql.append_fmt(INSERT_PROXY_STAT_SQL_VALUES_AND_INFO,
                                   get_global_proxy_config().app_name_str_,
                                   proxy_ip_, proxy_port_, 0L, r->name_, value, COMMON_INFO))) {
          LOG_WARN("fail to append stmt", "stat_name", r->name_, K(ret));
        } else {
          ++row_count;
        }
      }
      r = all_records_.next(r);
    }

    //FIXME: Here we have to depend on ObResourcePoolProcessor, it is not a good coding style
    //we need FIX it when ob_all_proxy_cluster_table is finish!!
    if (OB_SUCC(ret)) {
      static const char *CLUSTER_STAT_NAME = "cluster_recently_accessed_count";
      int64_t value = 0;
      char *info_buf = NULL;
      if (OB_ISNULL(info_buf = static_cast<char *>(op_fixed_mem_alloc(OB_MAX_CONFIG_INFO_LEN)))) {// 4k
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else if (OB_FAIL(get_global_resource_pool_processor().get_recently_accessed_cluster_info(info_buf,
          OB_MAX_CONFIG_INFO_LEN, value))) {
        LOG_WARN("fail to get all recently accessed cluster info from resource_pool", K(ret));
      } else if (OB_FAIL(sql.append_fmt(INSERT_PROXY_STAT_SQL_VALUES_AND_INFO_END,
                                        get_global_proxy_config().app_name_str_,
                                        proxy_ip_, proxy_port_, 0L, CLUSTER_STAT_NAME,
                                        value, info_buf, info_buf))) {
        LOG_WARN("fail to append stmt", K(CLUSTER_STAT_NAME), K(ret));
      } else {
        ++row_count;
      }
      if (OB_LIKELY(NULL != info_buf)) {
        op_fixed_mem_free(info_buf, OB_MAX_CONFIG_INFO_LEN);
        info_buf = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = -1;
      const ObString simplified_sql(std::min(sql.length(), PRINT_SQL_LEN), sql.ptr());
      if (OB_FAIL(mysql_proxy_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(simplified_sql), "size", sql.length(), K(ret));
      } else if (OB_UNLIKELY(affected_rows > row_count * 2) || OB_UNLIKELY(affected_rows < 0)) {
        LOG_WARN("affected_rows is not expected", K(simplified_sql), "size", sql.length(), K(row_count), K(affected_rows), K(ret));
      } else {
        is_first_register_ = false;
        LOG_DEBUG("succ to sync stats", K(simplified_sql), "size", sql.length(), K(row_count), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObStatProcessor::do_sync_stat_table()
{
  return g_stat_processor.sync_stat_table();
}

void ObStatProcessor::update_stat_table_sync_interval()
{
  ObAsyncCommonTask *cont = g_stat_processor.get_stat_table_cont();
  if (OB_LIKELY(NULL != cont)) {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().stat_table_sync_interval);
    cont->set_interval(interval_us);
  }
}

int ObStatProcessor::sync_stat_table()
{
  int ret = OB_SUCCESS;
  if (!is_mysql_proxy_inited()) {
    LOG_INFO("mysql proxy is not inited, can not sync_stat_table");
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("proxy need exit now", K(ret));
  } else if (OB_FAIL(exec_raw_stat_sync_cbs())) {
    LOG_WARN("fail to exec raw stat sync cbs", K(ret));
  } else if (OB_FAIL(sync_all_proxy_stat())) {
    LOG_WARN("fail to sync all proxy stat", K(ret));
  }
  return ret;
}

int ObStatProcessor::do_dump_stat()
{
  return g_stat_processor.dump();
}

void ObStatProcessor::update_stat_dump_interval()
{
  ObAsyncCommonTask *cont = g_stat_processor.get_stat_dump_cont();
  if (OB_LIKELY(cont)) {
    int64_t interval_us = get_global_proxy_config().stat_dump_interval;
    cont->set_interval(interval_us);
  }
}

int ObStatProcessor::dump()
{
  int ret = OB_SUCCESS;
  int64_t len = OB_MALLOC_MIDDLE_BLOCK_SIZE;
  char *buf = NULL;

  if (OB_ISNULL(buf = reinterpret_cast<char *>(ob_malloc(len, ObModIds::OB_PROXY_STAT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    int64_t pos = 0;
    if(OB_FAIL(databuff_printf(buf, len, pos, "\n      stat value      |       stat name\n"))) {
      LOG_WARN("fail to fill stat dump buf", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, len, pos, "----------------------|----------------------------------\n"))) {
      LOG_WARN("fail to fill stat dump buf", K(ret));
    } else {
      ObRecRecord *r = all_records_.head();
      while (OB_SUCC(ret) && NULL != r) {
        if (RECD_FLOAT != r->data_type_) {
          if (OB_FAIL(databuff_printf(buf, len, pos, " %20ld | %s\n",
                                      r->data_.rec_int_, r->name_))) {
            LOG_WARN("fail to fill stat dump buf", K(ret));
          }
        } else {
          if(OB_FAIL(databuff_printf(buf, len, pos, " %20.2f | %s\n",
                                     r->data_.rec_float_, r->name_))) {
            LOG_WARN("fail to fill stat dump buf", K(ret));
          }
        }
        r = all_records_.next(r);
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    LOG_INFO("dump statistic", K(buf));
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObStatProcessor::report_session_stats(
    const char *cluster_name, const uint64_t session_id, const int64_t *stats,
    const bool is_first_register, const char **stat_names, const int64_t stats_size)
{
  int ret = OB_SUCCESS;
  ObSessionStatTableSync *stat_sync = NULL;
  ObProxyMutex *mutex = NULL;
  if (OB_UNLIKELY(!is_mysql_proxy_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("mysql proxy is not inited, can not report_session_stats");
  } else if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for proxy mutex error", K(ret));
  } else if (OB_ISNULL(stat_sync =
    op_reclaim_alloc_args(ObSessionStatTableSync, mutex))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory for ObSessionStatTableSync", K(ret));
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  } else if (OB_FAIL(stat_sync->init(*mysql_proxy_, cluster_name, session_id, stats,
                                     is_first_register, stat_names, stats_size))) {
    LOG_WARN("failed to init ObSessionStatTableSync", K(ret));
  } else if (OB_ISNULL(g_event_processor.schedule_imm(stat_sync, ET_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schedule imm report session stats error", K(ret));
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != stat_sync)) {
    stat_sync->destroy();
    stat_sync = NULL;
  }
  return ret;
}

bool ObStatProcessor::set_rec_data(ObRecDataType data_type, ObRecData &data_dst, ObRecData &data_src)
{
  bool bret = true;

  switch (data_type) {
    case RECD_INT:
      if (data_dst.rec_int_ != data_src.rec_int_) {
        data_dst.rec_int_ = data_src.rec_int_;
      }
      break;

    case RECD_COUNTER:
      if (data_dst.rec_counter_ != data_src.rec_counter_) {
        data_dst.rec_counter_ = data_src.rec_counter_;
      }
      break;

    case RECD_FLOAT:
      if (data_dst.rec_float_ != data_src.rec_float_) {
        data_dst.rec_float_ = data_src.rec_float_;
      }
      break;

    default:
      LOG_WARN("Wrong RECD type!");
      bret = false;
  }

  return bret;

}

bool ObStatProcessor::set_rec_data_from_int64(
    ObRecDataType data_type, ObRecData &data_dst, int64_t data_int64)
{
  bool bret = true;
  switch (data_type) {
    case RECD_INT:
      data_dst.rec_int_ = data_int64;
      break;

    case RECD_COUNTER:
      data_dst.rec_counter_ = data_int64;
      break;

    case RECD_FLOAT:
      data_dst.rec_float_ = static_cast<float>(data_int64);
      break;

    default:
      LOG_WARN("Unexpected RecD type");
      bret = false;
  }

  return bret;
}

bool ObStatProcessor::set_rec_data_from_float(
    ObRecDataType data_type, ObRecData &data_dst, float data_float)
{
  bool bret = true;
  switch (data_type) {
    case RECD_INT:
      data_dst.rec_int_ = static_cast<int64_t>(data_float);
      break;

    case RECD_COUNTER:
      data_dst.rec_counter_ = static_cast<int64_t>(data_float);
      break;

    case RECD_FLOAT:
      data_dst.rec_float_ = static_cast<float>(data_float);
      break;

    default:
      LOG_WARN("Unexpected RecD type");
      bret = false;
  }

  return bret;
}

int ObStatProcessor::get_raw_stat_total(ObRecRawStatBlock *rsb, const int64_t id, ObRecRawStat &total)
{
  int ret = OB_SUCCESS;
  ObRecRawStat *tlp = NULL;
  total.sum_ = 0;
  total.count_ = 0;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rsb or id", K(rsb), K(id), K(ret));
  } else {
    // get global values
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;

    // get thread local values
    for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
      tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_event_threads_[i]) + rsb->ethr_stat_offset_) + id;
      total.sum_ += tlp->sum_;
      total.count_ += tlp->count_;
    }

    for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_; ++i) {
      tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_dedicate_threads_[i]) + rsb->ethr_stat_offset_) + id;
      total.sum_ += tlp->sum_;
      total.count_ += tlp->count_;
    }

    if (total.sum_ < 0) { // Assure that we stay positive
      total.sum_ = 0;
    }
  }

  return ret;
}

int ObStatProcessor::sync_raw_stat_to_global(ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat *tlp = NULL;
  ObRecRawStat total;
  total.sum_ = 0;
  total.count_ = 0;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rsb or id", K(rsb), K(id), K(ret));
  } else {
    // sum_ the thread local values
    for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
      tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_event_threads_[i]) + rsb->ethr_stat_offset_) + id;
      total.sum_ += tlp->sum_;
      total.count_ += tlp->count_;
    }

    for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_; ++i) {
      tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_dedicate_threads_[i]) + rsb->ethr_stat_offset_) + id;
      total.sum_ += tlp->sum_;
      total.count_ += tlp->count_;
    }

    if (total.sum_ < 0) { // Assure that we stay positive
      total.sum_ = 0;
    }

    // lock so the setting of the globals and last values are atomic
    if (OB_FAIL(mutex_acquire(&(rsb->mutex_)))) {
      LOG_ERROR("fail to acquire mutex", K(ret));
    } else {
      // get the delta from the last sync
      ObRecRawStat delta;
      delta.sum_ = total.sum_ - rsb->global_[id]->last_sum_;
      delta.count_ = total.count_ - rsb->global_[id]->last_count_;

      // increment the global values by the delta
      (void)ATOMIC_FAA(&(rsb->global_[id]->sum_), delta.sum_);
      (void)ATOMIC_FAA(&(rsb->global_[id]->count_), delta.count_);

      // set the new totals as the last values seen
      (void)ATOMIC_SET(&(rsb->global_[id]->last_sum_), total.sum_);
      (void)ATOMIC_SET(&(rsb->global_[id]->last_count_), total.count_);
      if (OB_FAIL(mutex_release(&(rsb->mutex_)))) {
        LOG_ERROR("fail to release mutex", K(ret));
      }
    }
  }

  return ret;
}

int ObStatProcessor::clear_raw_stat(ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rsb or id", K(rsb), K(id), K(ret));

    // the globals need to be reset too
    // lock so the setting of the globals and last values are atomic
  } else if (OB_FAIL(mutex_acquire(&(rsb->mutex_)))){
    LOG_ERROR("fail to acquire mutex", K(ret));
  } else {
    (void)ATOMIC_SET(&(rsb->global_[id]->sum_), 0L);
    (void)ATOMIC_SET(&(rsb->global_[id]->last_sum_), 0L);
    (void)ATOMIC_SET(&(rsb->global_[id]->count_), 0L);
    (void)ATOMIC_SET(&(rsb->global_[id]->last_count_), 0L);
    if (OB_FAIL(mutex_release(&(rsb->mutex_)))) {
      LOG_ERROR("fail to release mutex", K(ret));
    } else {
      // reset the local stats
      ObRecRawStat *tlp = NULL;
      for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
        tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_event_threads_[i]) + rsb->ethr_stat_offset_) + id;
        (void)ATOMIC_SET(&(tlp->sum_), 0L);
        (void)ATOMIC_SET(&(tlp->count_), 0L);
      }

      for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_; ++i) {
        tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_dedicate_threads_[i]) + rsb->ethr_stat_offset_) + id;
        (void)ATOMIC_SET(&(tlp->sum_), 0L);
        (void)ATOMIC_SET(&(tlp->count_), 0L);
      }
    }
  }

  return ret;
}

int ObStatProcessor::clear_raw_stat_sum(ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rsb or id", K(rsb), K(id), K(ret));

    // the globals need to be reset too
    // lock so the setting of the globals and last values are atomic
  } else if (OB_FAIL(mutex_acquire(&(rsb->mutex_)))){
    LOG_ERROR("fail to acquire mutex", K(ret));
  } else {
    (void)ATOMIC_SET(&(rsb->global_[id]->count_), 0L);
    (void)ATOMIC_SET(&(rsb->global_[id]->last_count_), 0L);
    if (OB_FAIL(mutex_release(&(rsb->mutex_)))) {
      LOG_ERROR("fail to release mutex", K(ret));
    } else {
      // reset the local stats
      ObRecRawStat *tlp = NULL;
      for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
        tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_event_threads_[i]) + rsb->ethr_stat_offset_) + id;
        (void)ATOMIC_SET(&(tlp->sum_), 0L);
      }

      for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_; ++i) {
        tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_dedicate_threads_[i]) + rsb->ethr_stat_offset_) + id;
        (void)ATOMIC_SET(&(tlp->sum_), 0L);
      }
    }
  }

  return ret;
}

int ObStatProcessor::clear_raw_stat_count(ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rsb or id", K(rsb), K(id), K(ret));

    // the globals need to be reset too
    // lock so the setting of the globals and last values are atomic
  } else if (OB_FAIL(mutex_acquire(&(rsb->mutex_)))){
    LOG_ERROR("fail to acquire mutex", K(ret));
  } else {
    (void)ATOMIC_SET(&(rsb->global_[id]->count_), 0L);
    (void)ATOMIC_SET(&(rsb->global_[id]->last_count_), 0L);
    if (OB_FAIL(mutex_release(&(rsb->mutex_)))) {
      LOG_ERROR("fail to release mutex", K(ret));
    } else {
      // reset the local stats
      ObRecRawStat *tlp = NULL;
      for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
        tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_event_threads_[i]) + rsb->ethr_stat_offset_) + id;
        (void)ATOMIC_SET(&(tlp->count_), 0L);
      }

      for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_; ++i) {
        tlp = reinterpret_cast<ObRecRawStat *>(reinterpret_cast<char *>(g_event_processor.all_dedicate_threads_[i]) + rsb->ethr_stat_offset_) + id;
        (void)ATOMIC_SET(&(tlp->count_), 0L);
      }
    }
  }

  return ret;
}

// Note: On these RecRawStatSync callbacks, our 'data' is protected
// under its lock by the caller, so no need to worry!
int ObStatProcessor::sync_raw_stat_sum(
    ObRecDataType data_type, ObRecData &data,
    ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat total;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rsb), K(id), K(ret));
  } else if (OB_FAIL(sync_raw_stat_to_global(rsb, id))) {
    LOG_WARN("fail to sync_raw_stat_to_global", K(ret));
  } else {
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!set_rec_data_from_int64(data_type, data, total.sum_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rec data type", K(ret));
  }
  return ret;
}

int ObStatProcessor::sync_raw_stat_count(
    ObRecDataType data_type, ObRecData &data,
    ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat total;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rsb), K(id), K(ret));
  } else if (OB_FAIL(sync_raw_stat_to_global(rsb, id))) {
    LOG_WARN("fail to sync_raw_stat_to_global", K(ret));
  } else {
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!set_rec_data_from_int64(data_type, data, total.count_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rec data type", K(ret));
  }
  return ret;
}

int ObStatProcessor::sync_raw_stat_avg(
    ObRecDataType data_type, ObRecData &data,
    ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat total;
  float avg = 0.0f;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rsb), K(id), K(ret));
  } else if (OB_FAIL(sync_raw_stat_to_global(rsb, id))) {
    LOG_WARN("fail to sync_raw_stat_to_global", K(ret));
  } else {
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;
    if (0 != total.count_) {
      avg = static_cast<float>(static_cast<double>(total.sum_) / static_cast<double>(total.count_));
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!set_rec_data_from_float(data_type, data, avg))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rec data type", K(ret));
  }
  return ret;
}

int ObStatProcessor::sync_raw_stat_hrtime_avg(
    ObRecDataType data_type, ObRecData &data,
    ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat total;
  float r = 0.0f;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rsb), K(id), K(ret));
  } else if (OB_FAIL(sync_raw_stat_to_global(rsb, id))) {
    LOG_WARN("fail to sync_raw_stat_to_global", K(ret));
  } else {
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;
    if (0 == total.count_) {
      r = 0.0f;
    } else {
      r = static_cast<float>(static_cast<double>(total.sum_) / static_cast<double>(total.count_));
      r = r / static_cast<float>(HRTIME_SECOND);
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!set_rec_data_from_float(data_type, data, r))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rec data type", K(ret));
  }
  return ret;
}

int ObStatProcessor::sync_raw_stat_int_msecs_to_float_seconds(
    ObRecDataType data_type, ObRecData &data,
    ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat total;
  float r = 0.0f;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rsb), K(id), K(ret));
  } else if (OB_FAIL(sync_raw_stat_to_global(rsb, id))) {
    LOG_WARN("fail to sync_raw_stat_to_global", K(ret));
  } else {
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;
    if (0 == total.count_) {
      r = 0.0f;
    } else {
      r = static_cast<float>(static_cast<double>(total.sum_) / 1000);
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!set_rec_data_from_float(data_type, data, r))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rec data type", K(ret));
  }
  return ret;
}

int ObStatProcessor::sync_raw_stat_mhrtime_avg(
    ObRecDataType data_type, ObRecData &data,
    ObRecRawStatBlock *rsb, const int64_t id)
{
  int ret = OB_SUCCESS;
  ObRecRawStat total;
  float r = 0.0f;

  if (OB_UNLIKELY(!check_argument(rsb, id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(rsb), K(id), K(ret));
  } else if (OB_FAIL(sync_raw_stat_to_global(rsb, id))) {
    LOG_WARN("fail to sync_raw_stat_to_global", K(ret));
  } else {
    total.sum_ = rsb->global_[id]->sum_;
    total.count_ = rsb->global_[id]->count_;
    if (total.count_ == 0) {
      r = 0.0f;
    } else {
      r = static_cast<float>(static_cast<double>(total.sum_) / static_cast<double>(total.count_));
      r = r / static_cast<float>(HRTIME_MSECOND);
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!set_rec_data_from_float(data_type, data, r))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rec data type", K(ret));
  }
  return ret;
}

ObEThread *ObStatProcessor::get_this_ethread()
{
  return this_ethread();
}

} // end of namespace obproxy
} // end of namespace oceanbase
