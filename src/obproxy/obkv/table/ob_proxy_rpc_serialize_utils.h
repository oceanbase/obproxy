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
#ifndef OBPROXY_RPC_SERIALIZE_UTIL_H
#define OBPROXY_RPC_SERIALIZE_UTIL_H

#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

class ObRpcRequest;

class ObRpcFieldBuf
{
public:
  ObRpcFieldBuf() : buf_(NULL), buf_len_(0) {}
  ObRpcFieldBuf(char *buf, int64_t buf_len) : buf_(buf), buf_len_(buf_len) {}
  void reset()
  {
    buf_ = NULL;
    buf_len_ = 0;
  }
  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV("type", "ObRpcFieldBuf", KP(buf_), K(buf_len_));
public:
  char* buf_;
  int64_t buf_len_;
};

template<class T>
class IgnoreField
{
public:
  IgnoreField<T>() : buf_(NULL), buf_len_(0) {}
  IgnoreField<T>(char *buf, int64_t buf_len) : buf_(buf), buf_len_(buf_len) {}
  IgnoreField<T>(ObRpcFieldBuf buf) : buf_(buf.buf_), buf_len_(buf.buf_len_) {}
  void reset() { buf_ = NULL; buf_len_ = 0; }
  ODP_RPC_NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV("type", "IgnoreFIeld", KP(buf_), K(buf_len_));
public:
  char *buf_;
  int64_t buf_len_; 
};

template<class T>
class IgnoreFieldWithUnisSerialization
{
public:
  IgnoreFieldWithUnisSerialization<T>() : buf_(NULL), buf_len_(0) {}
  IgnoreFieldWithUnisSerialization<T>(char *buf, int64_t buf_len) : buf_(buf), buf_len_(buf_len) {}
  IgnoreFieldWithUnisSerialization<T>(ObRpcFieldBuf buf) : buf_(buf.buf_), buf_len_(buf.buf_len_) {}
  void reset() { buf_ = NULL; buf_len_ = 0; }
  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV("type", "IgnoreFieldWithUnisSerialization", KP(buf_), K(buf_len_));
public:
  char *buf_;
  int64_t buf_len_;
};

#define ODP_IGNORE_FIELD(type, field) oceanbase::obproxy::obkv::IgnoreField<type> field;
#define ODP_IGNORE_FIELD_TYPE(type) oceanbase::obproxy::obkv::IgnoreField<type>
#define ODP_IGNORE_UNIS_FIELD(type, field) oceanbase::obproxy::obkv::IgnoreFieldWithUnisSerialization<type> field
#define ODP_IGNORE_UNIS_FIELD_TYPE(type) oceanbase::obproxy::obkv::IgnoreFieldWithUnisSerialization<type>


// record the rest of buf as ObRpcFieldBuf
DEFINE_DESERIALIZE(ObRpcFieldBuf)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf) && (data_len - pos) > 0) {
    buf_ = const_cast<char *>(buf + pos);
    buf_len_ = data_len - pos;
    pos = data_len;
  } else if ((data_len - pos < 0) || (OB_ISNULL(buf) && (data_len - pos > 0))) {
    ret = OB_ERR_UNEXPECTED; 
    PROXY_RPC_SM_LOG(WDIAG, "received unexpected buf", KP(buf_), K(data_len), K(pos));
  } else {
    PROXY_RPC_SM_LOG(DEBUG, "received empty buf", KP(buf_), K(data_len), K(pos));
  }
  return ret;
}

DEFINE_SERIALIZE(ObRpcFieldBuf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || buf_len_ == 0) {
    PROXY_RPC_SM_LOG(DEBUG, "try to encode an empty buf", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, buf_, buf_len_))) {
    PROXY_RPC_SM_LOG(WDIAG, "fail to encode raw buf", K(ret)); 
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRpcFieldBuf)
{
  return buf_len_;
}

template<class T>
ODP_DEFINE_DESERIALIZE(IgnoreField<T>)
{
  int ret = OB_SUCCESS;
  int64_t origin_pos = pos;
  T obj;
  if (OB_FAIL(obj.deserialize(buf, data_len, pos, OB_RPC_REQUEST_VALUE))) {
    PROXY_RPC_SM_LOG(WDIAG, "fail to deserialize obj", K(ret));
  } else {
    buf_ = const_cast<char*>(buf + origin_pos);
    buf_len_ = pos - origin_pos;
  }
  return ret;
}

template<class T>
ODP_DEFINE_SERIALIZE(IgnoreField<T>)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || buf_len_ == 0) {
    PROXY_RPC_SM_LOG(DEBUG, "try to encode an empty buf", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, buf_, buf_len_))) {
    PROXY_RPC_SM_LOG(WDIAG, "fail to encode raw buf", K(ret)); 
  }
  return ret;
};

template<class T>
ODP_DEFINE_GET_SERIALIZE_SIZE(IgnoreField<T>)
{
  return buf_len_;
}

template<class T>
DEFINE_DESERIALIZE(IgnoreFieldWithUnisSerialization<T>)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t payload_len = 0;
  int64_t origin_pos = pos;
  const char *origin_buf = buf;
  OB_UNIS_DECODEx(version);
  OB_UNIS_DECODEx(payload_len);
  CHECK_VERSION_LENGTH_WITH_TARGET_VER(T, version, payload_len, T::UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t unis_header_len = (pos - origin_pos);
    buf_ = const_cast<char*>(origin_buf + origin_pos);
    buf_len_ = (unis_header_len + payload_len);
    pos += payload_len;
  }
  return ret;
}

template<class T>
DEFINE_SERIALIZE(IgnoreFieldWithUnisSerialization<T>)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) || buf_len_ == 0) {
    PROXY_RPC_SM_LOG(DEBUG, "try to encode an empty buf", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, buf_, buf_len_))) {
    PROXY_RPC_SM_LOG(WDIAG, "fail to encode raw buf", K(ret)); 
  }
  return ret;
};

template<class T>
DEFINE_GET_SERIALIZE_SIZE(IgnoreFieldWithUnisSerialization<T>)
{
  return buf_len_;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_OBRPC_REQUEST_H */
