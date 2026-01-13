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

#ifndef OBPROXY_SHARED_REQUEST_BUF_H
#define OBPROXY_SHARED_REQUEST_BUF_H

#include "lib/ptr/ob_ptr.h"
#include "lib/ob_define.h"
#include "proxy/rpc/rpclib/ob_rpc_throttle.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

/**
 * @brief 存储request_buf_，用于父子请求之间共享引用计数
 *
 * 使用场景：
 * 1. 父请求分配request_buf_并反序列化数据
 * 2. 子请求共享父请求的request_buf_，避免深拷贝
 * 3. 通过引用计数管理内存生命周期，确保安全释放
 */
class ObSharedRequestBuf : public common::ObSharedRefCount
{
public:
  ObSharedRequestBuf()
    : common::ObSharedRefCount(), buf_(nullptr), len_(0) {}

  virtual ~ObSharedRequestBuf() {}

  int init(int64_t len) {
    int ret = common::OB_SUCCESS;
    if (len <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_LOG(WDIAG, "invalid argument", K(len), K(ret));
    } else {
      // use global memory allocator, avoid cross-thread release problem
      char *buf = reinterpret_cast<char *>(common::ob_malloc(len, common::ObModIds::OB_RPC_SHARED_REQUEST_BUF));
      if (OB_UNLIKELY(NULL == buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(WDIAG, "fail to alloc mem", K(len), K(ret));
      } else {
        MEMSET(buf, '\0', len);
        obkv::get_global_rpc_throttle().update_holding_resource(len);
        buf_ = buf;
        len_ = len;
        PROXY_LOG(DEBUG, "init shared request buf", K(len), K(buf), K(ret));
      }
    }
    return ret;
  }

  virtual void free() override {
    if (buf_ != nullptr) {
      PROXY_LOG(DEBUG, "do free shared request buf", K(len_));
      common::ob_free(buf_);
      obkv::get_global_rpc_throttle().update_holding_resource(-len_);
      buf_ = nullptr;
      len_ = 0;
    }
  }

  void set_buf(char* buf) {
    buf_ = buf;
  }

  char* get_buf() const { return buf_; }

  void set_len(int64_t len) {
    len_ = len;
  }

  int64_t get_len() const { return len_; }

  bool is_valid() const { return buf_ != nullptr && len_ > 0; }

private:
  char* buf_;      // request_buf
  int64_t len_;    // request_buf_len
};

} // end namespace proxy
} // end namespace obproxy
} // end namespace oceanbase

#endif // OBPROXY_SHARED_REQUEST_BUF_H
