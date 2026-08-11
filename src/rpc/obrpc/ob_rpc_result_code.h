/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_RESULT_CODE_
#define OCEANBASE_RPC_OBRPC_OB_RPC_RESULT_CODE_

#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_warning_buffer.h"
namespace oceanbase
{
namespace obrpc
{

struct ObRpcResultCode
{
  OB_UNIS_VERSION(1);

public:
  ObRpcResultCode() : rcode_(0)
  {
    msg_[0] = '\0';
    warnings_.reset();
  }

  void reset()
  {
    rcode_ = 0;
    msg_[0] = '\0';
    warnings_.reset();
  }

  int set_err_msg(int32_t rcode)
  {
    int ret = common::OB_SUCCESS;
    msg_[0] = '\0';
    int32_t length = 0;
    const char *error_msg = common::ob_strerror(rcode);
    if (OB_ISNULL(error_msg)) {
      length = snprintf(msg_, sizeof(msg_), "Unknown user error");
    } else {
      length = snprintf(msg_, sizeof(msg_), error_msg);
    }

    if (OB_UNLIKELY(length < 0 || OB_UNLIKELY(length >= common::OB_MAX_ERROR_MSG_LEN))) {
      ret = common::OB_BUF_NOT_ENOUGH;
      PROXY_LOG(WDIAG, "rpc error msg buffer not enough", K(ret), K(length), K(error_msg));
    }
    return ret;
  }

  TO_STRING_KV("code", rcode_, "msg", msg_, K_(warnings));

  int32_t rcode_;
  char msg_[common::OB_MAX_ERROR_MSG_LEN];
  common::ObSEArray<common::ObWarningBuffer::WarningItem, 4> warnings_;
};

struct ObRpcResultCodeSimplified
{
  OB_UNIS_VERSION(1);
public:
  static const int SIMPLE_RESULT_CODE_MAX_LEN = 64; // 9 + 9 + 5 = 23, left buffer, so give 64

  ObRpcResultCodeSimplified() : rcode_(0)
  {
  }

  void reset()
  {
    rcode_ = 0;
  }

  TO_STRING_KV("code", rcode_);

  int32_t rcode_;
};

} // end of namespace obrpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_RESULT_CODE_
