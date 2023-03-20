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

#ifndef OBPROXY_PARALLEL_EXECUTE_CONT_H
#define OBPROXY_PARALLEL_EXECUTE_CONT_H

#include "obutils/ob_async_common_task.h"
#include "ob_proxy_parallel_processor.h"
#include "proxy/client/ob_mysql_proxy.h"

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

class ObProxyParallelResp
{
public:
  ObProxyParallelResp(int64_t cont_index)
    : resp_(NULL), rs_fetcher_(NULL),
      column_count_(0), cont_index_(cont_index), allocator_() {}
  ~ObProxyParallelResp();

  int init(proxy::ObClientMysqlResp *resp, common::ObIAllocator *allocator);
  int next(common::ObObj *&rows);

  bool is_error_resp() const { return resp_->is_error_resp(); }
  bool is_ok_resp() const { return resp_->is_ok_resp(); }
  bool is_resultset_resp() const { return resp_->is_resultset_resp(); }
  uint16_t get_err_code() const { return resp_->get_err_code(); }
  common::ObString get_err_msg() const {return resp_->get_err_msg(); }

  ObMysqlField *get_field() const { return rs_fetcher_->get_field(); }
  int64_t get_column_count() { return column_count_; }
  int64_t get_cont_index() { return cont_index_; }
  int64_t to_string(char *buf, int64_t buf_len) const;

private:
  proxy::ObClientMysqlResp *resp_;
  ObResultSetFetcher *rs_fetcher_;
  int64_t column_count_;
  int64_t cont_index_;
  common::ObIAllocator *allocator_;
};

class ObProxyParallelExecuteCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyParallelExecuteCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *submit_thread)
      : ObAsyncCommonTask(m, "parallel execute cont", cb_cont, submit_thread),
        shard_conn_(NULL), is_deep_copy_(false), request_sql_(), mysql_proxy_(NULL),
        result_set_(NULL), cont_index_(-1) {}
  ~ObProxyParallelExecuteCont() {}

  int init(const ObProxyParallelParam &parallel_param, const int64_t cont_index,
           ObIAllocator *allocator, const int64_t timeout_ms);
  void destroy();
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data() {
    ObProxyParallelResp *result_set = result_set_;
    result_set_ = NULL;
    return static_cast<void *>(result_set);
  };

private:
  int deep_copy_sql(const common::ObString &sql);
  void reset_request_sql();

private:
  dbconfig::ObShardConnector* shard_conn_;
  bool is_deep_copy_;
  common::ObString request_sql_;
  proxy::ObMysqlProxy* mysql_proxy_;
  ObProxyParallelResp *result_set_;
  int64_t cont_index_;
  common::ObIAllocator *allocator_;
};

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_PARALLEL_EXECUTE_CONT_H
