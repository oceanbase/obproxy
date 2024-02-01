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

#define USING_LOG_PREFIX PROXY_CS

#include "ob_proxy_trace_stats.h"
#include "proxy/mysql/ob_mysql_transact.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int64_t ObTraceRecord::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(attempts), K_(pl_attempts), K_(addr),
      "server_state", ObMysqlTransact::get_server_state_name(static_cast<ObMysqlTransact::ObServerStateType>(server_state_)),
      "send_action", ObMysqlTransact::get_send_action_name(static_cast<ObMysqlTransact::ObServerSendActionType>(send_action_)),
      "resp_error", ObMysqlTransact::get_server_resp_error_name(static_cast<ObMysqlTransact::ObServerRespErrorType>(resp_error_)),
      K_(cost_time_us));
  J_OBJ_END();
  return pos;
}

int64_t ObTraceBlock::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(next_idx), K_(block_idx), KP_(next));
  for (int32_t i = 0; i < TRACE_BLOCK_ENTRIES && i < next_idx_; ++i) {
    J_COMMA();
    J_KV("stat", stats_[i]);
  }
  J_OBJ_END();
  return pos;
}

void ObTraceStats::destory()
{
  ObTraceBlock *b = first_block_.next_;
  ObTraceBlock *tmp = NULL;
  while (NULL != b) {
    tmp = b->next_;
    op_fixed_mem_free(b, sizeof(ObTraceBlock));
    b = tmp;
  }
  last_trace_end_time_ = 0;
  current_block_ = NULL;
  op_fixed_mem_free(this, sizeof(ObTraceStats));
}

int ObTraceStats::append_new_trace_block()
{
  int ret = OB_SUCCESS;
  ObTraceBlock *new_block = NULL;
  char *buf = NULL;
  if (OB_ISNULL(current_block_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("current_block_ is null", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(sizeof(ObTraceBlock))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc mem for ObTraceBlock", "alloc_size", sizeof(ObTraceBlock), K(ret));
  } else if (OB_ISNULL(new_block = new (buf) ObTraceBlock())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("failed to placement new for ObTraceBlock", K(ret));
    op_fixed_mem_free(buf, sizeof(ObTraceBlock));
    buf = NULL;
  } else {
    new_block->block_idx_ = static_cast<int16_t>(current_block_->block_idx_ + 1);
    current_block_->next_ = new_block;
    current_block_ = new_block;
    LOG_DEBUG("Adding new large trace block", KPC(this));
  }
  return ret;
}

int ObTraceStats::get_current_record(ObTraceRecord *&record)
{
  int ret = common::OB_SUCCESS;
  record = NULL;
  if (OB_ISNULL(current_block_)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("current_block_ is null", K(ret));
  } else {
    if (need_reuse_trace_stats()) {
      //when last_trace_end_time_ is zero, we need reset stats
      reuse();
    } else {
      if (current_block_->next_idx_ >= ObTraceBlock::TRACE_BLOCK_ENTRIES) {
        if (current_block_->block_idx_ >= TRACE_STATS_ENTRIES) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WDIAG("there is too many blocks", LITERAL_K(TRACE_STATS_ENTRIES), KPC(current_block_), K(ret));
        } else if (OB_FAIL(append_new_trace_block())) {
          LOG_WDIAG("failed to append new trace block", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    record = &(current_block_->stats_[current_block_->next_idx_]);
    ++(current_block_->next_idx_);
  }
  return ret;
}

int64_t ObTraceStats::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(last_trace_end_time));
  const ObTraceBlock *b = &first_block_;
  while (NULL != b) {
    J_COMMA();
    J_KV("block", *b);
    b = b->next_;
  }
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
