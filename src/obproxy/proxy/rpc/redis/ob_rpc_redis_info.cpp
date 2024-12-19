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
#include "proxy/rpc/redis/ob_rpc_redis_info.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "proxy/rpc/redis/ob_rpc_redis_command_factory.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy;

void ObRpcRedisInfo::reset()
{
  free_request_buf();
  free_request_inner_buf();
  free_response_buf();
  free_response_inner_buf();
  if (OB_NOT_NULL(redis_request_)) {
    ObRpcRedisCommandFactory::free_redis_request(redis_request_);
    redis_request_ = NULL;
  }
  allocator_.reset();
  if (OB_NOT_NULL(redis_table_response_)) {
    ObRpcRedisCommandFactory::free_redis_internal_response(redis_table_response_);
    redis_table_response_ = NULL;
  }

  redis_cmd_type_ = REDIS_COMMAND_MAX;
  // TODO: free redis_request_
}

int ObRpcRedisInfo::alloc_request_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != request_buf_)) {
    if (OB_FAIL(free_request_buf())) {
      PROXY_LOG(EDIAG, "free request buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      MEMSET(buf, '\0', len);
      request_buf_ = buf;
      request_buf_len_ = len;
    }
  }
  return ret;
}

//not same as realloc() compare with alloc(), we not extend base on old buffer.
int ObRpcRedisInfo::realloc_request_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL == request_buf_)) {
    ret = alloc_request_buf(len);
  } else if (len == 0) {
    //to free
    ret = free_request_buf();
  } else {
    // keep
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      MEMSET(buf, '\0', len);
      uint64_t data_len = len > request_buf_len_ ? request_buf_len_ : len;
      MEMCPY(buf, request_buf_, data_len);
      //free old buffer
      op_fixed_mem_free(request_buf_, request_buf_len_);
      //update
      request_buf_ = buf;
      request_buf_len_ = len;
    }
  }
  return ret;
}

int ObRpcRedisInfo::free_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != request_buf_) {
    if (request_buf_len_ <= 0) {
      //do nothing
      // ret = common::OB_ERR_UNEXPECTED;
      // PROXY_LOG(EDIAG, "request_buf_len_ must > 0", K_(request_buf_len), K_(request_buf), K(ret));
    } else {
      op_fixed_mem_free(request_buf_, request_buf_len_);
      request_buf_ = NULL;
      request_buf_len_ = 0;
    }
  }
  return ret;
}

int ObRpcRedisInfo::alloc_request_inner_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != request_inner_buf_)) {
    if (OB_FAIL(free_request_inner_buf())) {
      PROXY_LOG(EDIAG, "free request inner buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      request_inner_buf_ = buf;
      request_inner_buf_len_ = len;
    }
  }
  return ret;
}

int ObRpcRedisInfo::alloc_response_inner_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != response_inner_buf_)) {
    if (OB_FAIL(free_response_inner_buf())) {
      PROXY_LOG(EDIAG, "free response inner buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      response_inner_buf_ = buf;
      response_inner_buf_len_ = len;
    }
  }
  return ret;
}

int ObRpcRedisInfo::free_request_inner_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != request_inner_buf_) {
    if (request_inner_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "request_buf_len_ must > 0", K_(request_inner_buf_len), K_(request_inner_buf), K(ret));
    } else {
      op_fixed_mem_free(request_inner_buf_, request_inner_buf_len_);
      request_inner_buf_ = NULL;
      request_inner_buf_len_ = 0;
    }
  }
  return ret;
}

int ObRpcRedisInfo::free_response_inner_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != response_inner_buf_) {
    if (response_inner_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "response_inner_buf_len_ must > 0", K_(response_inner_buf_len), K_(response_inner_buf), K(ret));
    } else {
      op_fixed_mem_free(response_inner_buf_, response_inner_buf_len_);
      response_inner_buf_ = NULL;
      response_inner_buf_len_ = 0;
    }
  }
  return ret;
}

int ObRpcRedisInfo::alloc_response_buf(uint64_t len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != response_buf_)) {
    if (OB_FAIL(free_response_buf())) {
      PROXY_LOG(EDIAG, "free response buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(len));

    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(len), K(ret));
    } else {
      response_buf_ = buf;
      response_buf_len_ = len;
    }
  }
  return ret;
}

int ObRpcRedisInfo::free_response_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != response_buf_) {
    if (response_buf_len_ <= 0) {
      //do nothing
      // ret = common::OB_ERR_UNEXPECTED;
      // PROXY_LOG(EDIAG, "response_buf_len_ must > 0", K_(response_buf_len), K_(response_buf), K(ret));
    } else {
      op_fixed_mem_free(response_buf_, response_buf_len_);
      response_buf_ = NULL;
      response_buf_len_ = 0;
    }
  }
  return ret;
}

void ObRpcRedisInfo::set_rpc_credential(const common::ObString &credential)
{
  if (credential.length() > 0 && credential.length() < 50) {
    MEMCPY(rpc_credential_, credential.ptr(), credential.length());
    credential_.assign(rpc_credential_, credential.length());
  }
}

int ObRpcRedisInfo::init_error_redis_msg_buf(uint64_t size)
{
  int ret = common::OB_SUCCESS;
  //not care about error_redis_msg_buf_ is NULL or not,
  if (OB_NOT_NULL(error_redis_msg_buf_)) {
    allocator_.free(error_redis_msg_buf_);
    error_redis_msg_buf_ = NULL;
  }
  if (OB_ISNULL(error_redis_msg_buf_ = (char *)allocator_.alloc(size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "alloc error msg redis buf failed", K(ret), K(size), K(this));
  } else {
    MEMSET(error_redis_msg_buf_, '\0', size);
  }
  return ret;
}

int ObRpcRedisInfo::init_redis_inner_msg_buf(uint64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(redis_inner_msg_buf_)) {
    allocator_.free(redis_inner_msg_buf_);
    redis_inner_msg_buf_ = NULL;
  }
  if (OB_ISNULL(redis_inner_msg_buf_ = (char *)allocator_.alloc(size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "alloc inner msg redis buf failed", K(ret), K(size), K(this));
  } else {
    MEMSET(redis_inner_msg_buf_, '\0', size);
  }
  return ret;
}
