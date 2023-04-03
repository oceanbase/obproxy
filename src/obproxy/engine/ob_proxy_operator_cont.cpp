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

#include "ob_proxy_operator_cont.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

int ObProxyOperatorCont::init(ObProxyOperator* operator_root, uint8_t seq, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected buf is not null", K(ret));
  } else if (OB_ISNULL(buf_ = new_empty_miobuffer())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new_empty_miobuffer for buf", K(ret));
  } else if (OB_ISNULL(buf_reader_ = buf_->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to allocate buffer reader", K(ret));
  } else {
    operator_root_ = operator_root;
    timeout_ms_ = timeout_ms;
    seq_ = seq;
  }

  return ret;
}

int ObProxyOperatorCont::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;

  if (NULL == execute_thread_) {
    execute_thread_ = &self_ethread();
  }

  switch (event) {
    case EVENT_INTERVAL: {
      timeout_action_ = NULL;
      // If it has been canceled, then the underlying operator has been released,
      // and here we need to set pending_action_ to NULL
      if (action_.cancelled_) {
        pending_action_ = NULL;
      }

      if (OB_FAIL(handle_timeout())) {
        LOG_WARN("fail to handle timeout");
      }
      break;
    }
    case EVENT_IMMEDIATE: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (OB_FAIL(handle_event_start())) {
        LOG_INFO("fail to handle event start", K(ret));
      } else if (OB_FAIL(schedule_timeout())) {
        LOG_WARN("fail to schedule timeout action", K(ret));
      }
      break;
    }
    case VC_EVENT_READ_COMPLETE: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (OB_FAIL(handle_event_complete(data))) {
        LOG_WARN("fail to handle event complete", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_INFORM_OUT_EVENT: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (OB_FAIL(handle_event_inform_out())) {
        LOG_WARN("fail to handle inform out event", K(ret));
      }
      break;
    }
    case VC_EVENT_ACTIVE_TIMEOUT:
    case EVENT_ERROR:
    default: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (action_.cancelled_) {
        terminate_ = true;
        LOG_INFO("async task has been cancelled, will kill itself", K(ret));
      } else if (NULL != cb_cont_) {
        need_callback_ = true;
      } else {
        terminate_ = true;
      }

      if (EVENT_ERROR != event || VC_EVENT_ACTIVE_TIMEOUT != event) {
        LOG_WARN("error state, nerver run here", K(event), K(ret));
      } else {
        LOG_INFO("error state", K(event));
      }
      break;
    }
  }

  if (!terminate_ && (need_callback_ || OB_FAIL(ret))) {
    if (execute_thread_ == &self_ethread()) {
      execute_thread_->is_need_thread_pool_event_ = true;
    }
    if (OB_FAIL(handle_callback())) {
      LOG_WARN("fail to handle callback", K(ret));
    }
  }

  if (terminate_) {
    if (execute_thread_ == &self_ethread()) {
      execute_thread_->is_need_thread_pool_event_ = true;
    }
    destroy();
  }

  return EVENT_DONE;
}

int ObProxyOperatorCont::init_task()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(operator_root_->open(this, pending_action_, timeout_ms_))) {
    LOG_WARN("fail to open operator", K_(timeout_ms), K(ret));
  } else if (OB_FAIL(operator_root_->get_next_row())) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    LOG_WARN("pending action should not be null", K(ret));
  }

  return ret;
}

int ObProxyOperatorCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *result_resp = reinterpret_cast<ObProxyResultResp*>(data);
  if (result_resp->is_resultset_resp()) {
    if (OB_FAIL(build_executor_resp(buf_, seq_, result_resp))) {
      LOG_WARN("fail to build shard scan resp", K(ret));
    }
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*buf_, seq_, result_resp->get_err_code(), result_resp->get_err_msg()))) {
    LOG_WARN("fail to encode err pacekt buf", K_(seq), "errmsg", result_resp->get_err_msg(),
             "errcode", result_resp->get_err_code(), K(ret));
  }

  return ret;
}

int ObProxyOperatorCont::schedule_timeout()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout action must be NULL", K_(timeout_action), K(ret));
  } else if (OB_ISNULL(timeout_action_ = execute_thread_->schedule_in(this, HRTIME_MSECONDS(timeout_ms_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule timeout", K_(timeout_action), K_(timeout_ms), K(ret));
  }

  return ret;
}

int ObProxyOperatorCont::build_executor_resp(ObMIOBuffer *write_buf, uint8_t &seq, ObProxyResultResp *result_resp)
{
  int ret = OB_SUCCESS;
  int64_t column_count = result_resp->get_column_count();
  ObSEArray<obmysql::ObMySQLField, 1, common::ObIAllocator&> *fields = NULL;
  ObSEArray<ObField, 4> ob_fields;

  // header , cols , first eof
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result_resp->get_fields(fields))) {
      LOG_WARN("fail to push field", K(fields), K(ret));
    } else if (OB_ISNULL(fields)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fields should not be null", K(ret));
    } else if (OB_UNLIKELY(fields->count() != column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fields count should equal column_count", K(column_count), "fileds count", fields->count(), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::encode_header(*write_buf, seq, *fields))) {
      LOG_WARN("fail to encode header", K(fields), K(seq), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
        obmysql::ObMySQLField &mysql_field = fields->at(i);
        ObField ob_field;
        if (OB_FAIL(ObSMUtils::to_ob_field(mysql_field, ob_field))) {
          LOG_WARN("fail to covert to ob field", K(mysql_field), K(ret));
        } else if (OB_FAIL(ob_fields.push_back(ob_field))) {
          LOG_WARN("fail to push ob field", K(ob_field), K(ret));
        }
      }
    }
  }

  // rows
  if (OB_SUCC(ret)) {
    ObObj *objs = NULL;
    int64_t buf_len = sizeof(ObObj) * column_count;

    if (OB_ISNULL(objs = static_cast<ObObj *>(op_fixed_mem_alloc(buf_len)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to alloc obj array", K(column_count), K(ret));
    } else {
      ObNewRow row;
      //ObSEArray<common::ObObj, 4> *row_array;
      ObSEArray<common::ObObj*, 4, common::ObIAllocator&> *row_array;
      while ((OB_SUCC(ret)) && (OB_SUCC(result_resp->next(row_array)))) {
        if (OB_ISNULL(row_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row_array should not be null", K(ret));
        } else if (OB_UNLIKELY(row_array->count() != column_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row_array count should equal column_count", K(column_count), "row_array count", row_array->count(), K(ret));
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
          objs[i] = *(row_array->at(i));
        }

        if (OB_SUCC(ret)) {
          row.cells_ = objs;
          row.count_ = column_count;
          if (OB_FAIL(ObMysqlPacketUtil::encode_row_packet(*write_buf, seq, row, &ob_fields))) {
            LOG_WARN("fail to encode row", K(seq), K(row), K(ret));
          } else {
            row.reset();
            row_array = NULL;
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    if (OB_NOT_NULL(objs)) {
      op_fixed_mem_free(objs, buf_len);
      objs = NULL;
    }
  }

  // second eof
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*write_buf, seq))) {
      LOG_WARN("fail to encode row", K(seq), K(ret));
    }
  }

  return ret;
}

void ObProxyOperatorCont::destroy()
{
  LOG_DEBUG("ObProxyOperatorCont will be destroyed", KP(this));

  if (OB_LIKELY(NULL != buf_reader_)) {
    buf_reader_->dealloc();
    buf_reader_ = NULL;
  }

  if (OB_LIKELY(NULL != buf_)) {
    free_miobuffer(buf_);
    buf_ = NULL;
  }

  ObAsyncCommonTask::destroy();
}

} // end of namespace engine
} // end of namespace obproxy
} // end of namespace oceanbase
