/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "obkv/redis/ob_redis_rpc_request.h"
#include "lib/utility/ob_unify_serialize.h"
#include "obkv/redis/ob_redis_rpc_response.h"
#include "obkv/redis/ob_redis_rpc_request.h"


using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

OB_SERIALIZE_MEMBER(ObRedisOperationSimplifiedResult, resp_ret_, redis_str_);

void ObRpcRedisOperationSimplifiedResponse::reset()
{
  redis_operation_simplified_result_.reset();
  ObRpcResponse::reset();
}
int ObRpcRedisOperationSimplifiedResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return ret;
}

int64_t ObRpcRedisOperationSimplifiedResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += redis_operation_simplified_result_.get_serialize_size();
  return len;
}

int ObRpcRedisOperationSimplifiedResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(redis_operation_simplified_result_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }
  return ret;
}

//// ====== table operation response for redis ======
OB_DEF_SERIALIZE(ObRedisOperationResult)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObRedisOperationResult, ObTableResult));
  LST_DO_CODE(OB_UNIS_ENCODE, operation_type_);
  int64_t rowkey_size = rowkey_.count();
  OB_UNIS_ENCODE(rowkey_size);
  for (int i = 0; i < rowkey_size && OB_SUCC(ret); i++) {
    OB_UNIS_ENCODE(rowkey_.at(i));
  }
  if (properties_values_.count() != properties_names_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected properties count", K(ret));
  }
  int64_t properties_size = properties_values_.count();
  OB_UNIS_ENCODE(properties_size);
  for (int i = 0; i < properties_size && OB_SUCC(ret); i++) {
    OB_UNIS_ENCODE(properties_names_.at(i));
    OB_UNIS_ENCODE(properties_values_.at(i));
  }
  OB_UNIS_ENCODE(affected_rows_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObRedisOperationResult)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  BASE_ADD_LEN((ObRedisOperationResult, ObTableResult));
  LST_DO_CODE(OB_UNIS_ADD_LEN, operation_type_);
  int64_t rowkey_size = rowkey_.count();
  OB_UNIS_ADD_LEN(rowkey_size);
  for (int i = 0; i < rowkey_size && OB_SUCC(ret); i++) {
    OB_UNIS_ADD_LEN(rowkey_.at(i));
  }

  int64_t properties_size = properties_values_.count();
  if (properties_values_.count() != properties_names_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected properties count", K(ret));
  }
  OB_UNIS_ADD_LEN(properties_size);
  for (int i = 0; i < properties_size && OB_SUCC(ret); i++) {
    OB_UNIS_ADD_LEN(properties_names_.at(i));
    OB_UNIS_ADD_LEN(properties_values_.at(i));
  }
  OB_UNIS_ADD_LEN(affected_rows_);
  return ret;
}

OB_DEF_DESERIALIZE(ObRedisOperationResult)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_size = 0;
  int64_t properties_size = 0;
  BASE_DESER((ObRedisOperationResult, ObTableResult));
  LST_DO_CODE(OB_UNIS_DECODE,  operation_type_);
  //table entity info
  uint64_t version = 0;
  uint64_t  len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);

  OB_UNIS_DECODE(rowkey_size);
  if (OB_FAIL(rowkey_.prepare_allocate(rowkey_size))) {
    LOG_WDIAG("fail to prepare allcoate mem for rowkey", K(ret));
  }
  for (int i = 0; i < rowkey_size && OB_SUCC(ret); i++) {
    OB_UNIS_DECODE(rowkey_.at(i));
  }

  OB_UNIS_DECODE(properties_size);
  if (OB_FAIL(properties_names_.prepare_allocate(properties_size))) {
    LOG_WDIAG("fail to prepare allcoate mem for property names", K(ret));
  } else if (OB_FAIL(properties_values_.prepare_allocate(properties_size))) {
    LOG_WDIAG("fail to prepare allcoate mem for property values", K(ret));
  }
  // uint64_t strlen = 0;
  for (int i = 0; i < properties_size && OB_SUCC(ret); i++) {
    OB_UNIS_DECODE(properties_names_.at(i));
    {
      if (REDIS_PROPERTY_NAME.case_compare(properties_names_.at(i)) == 0) {
        uint64_t origin_pos = pos; //save pos
        uint64_t strlen = 0;
        pos += 4;
        OB_UNIS_DECODE(strlen);
        last_propertity_value_pos_ = (char *)(buf + pos);
        last_propertity_value_len_ = strlen;
        pos = origin_pos;
      }
    }
    OB_UNIS_DECODE(properties_values_.at(i));

    // OB_UNIS_DECODE(strlen);
    // properties_names_len_.push_back(pos);
    // properties_names_pos_.push_back(pos);
    // pos += (strlen + 1); //skip names
    /* 0x 01 0e 52 45 44 49 53 5f 43 4f 44 45 5f 53 54 52 00 16 02 00 ff 05 2b 4f 4b 0d 0a 00
     *    len       propertity names(REDIS_CODE_STR)       | obobj type| len response(+OK\r\n) */

  }
  OB_UNIS_DECODE(affected_rows_);
  return ret;
}

// DEF_TO_STRING(ObRedisOperationResult)
// {
//   int64_t pos = 0;
//   J_OBJ_START();
//   J_KV(K_(errno),
//        K_(operation_type),
//        K_(affected_rows),
//        K_(entity));
//   J_OBJ_END();
//   return pos;
// }

// OB_SERIALIZE_MEMBER((ObRedisOperationResult, ObTableResult),
//                     operation_type_, entity_, affected_rows_);
void ObRpcRedisOperationResponse::reset()
{
  redis_operation_result_.reset();
  ObRpcResponse::reset();
}
int ObRpcRedisOperationResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;    // 将pos设置为meta之后
  check_sum_pos = pos; // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcRedisOperationResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化redis_result
    OB_UNIS_ENCODE(redis_operation_result_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}

int64_t ObRpcRedisOperationResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += redis_operation_result_.get_serialize_size();
  return len;
}

int ObRpcRedisOperationResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(redis_operation_result_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }
  return ret;
}


// ====== redis response ======

int ObRpcRedisAuthResponse::analyze_response(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_login_result_.analyze_response(buf, len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(len), K(ret));
  }
  // build redis login result here
  return ret;
}

int ObRpcRedisAuthResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, redis_result_buf_, redis_result_len_))) {
    LOG_WDIAG("fail to encode redis common response", K(ret));
  }
  return ret;
}

int64_t ObRpcRedisAuthResponse::get_encode_size() const { return redis_result_len_; }


int ObRpcRedisCommonResponse::analyze_response(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObRedisOperationResult &table_redis_res = get_redis_operation_resp().get_table_operation_result();
  if (OB_FAIL(table_redis_result_.analyze_response(buf, len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(len), K(ret));
  } else if (OB_UNLIKELY(table_redis_res.properties_names_.count() != 1
                         || table_redis_res.properties_values_.count() != 1
                         || REDIS_PROPERTY_NAME.case_compare(table_redis_res.properties_names_.at(0)) == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis resutl", K(ret));
  } else {
    ObObj &redis_obj_result = table_redis_res.properties_values_.at(0);
    if (!redis_obj_result.is_string_type()) {
      LOG_WDIAG("unexpected redis result");
    } else {
      ObString result_str = redis_obj_result.get_string();
      redis_result_buf_ = result_str.ptr();
      redis_result_len_ = result_str.length();
    }
  }
  return ret;
}

int ObRpcRedisCommonResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, redis_result_buf_, redis_result_len_))) {
    LOG_WDIAG("fail to encode redis common response", K(ret));
  }
  return ret;
}

int64_t ObRpcRedisCommonResponse::get_encode_size() const { return redis_result_len_; }

int ObRpcRedisGlobalResponse::analyze_response(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(buf, len, pos);
  return ret;
}
int ObRpcRedisGlobalResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(buf, buf_len, pos);
  return ret;
}
int64_t ObRpcRedisGlobalResponse::get_encode_size() const { return 0; }


int ObRpcRedisInternalResponse::analyze_response(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(buf, len, pos);
  return ret;
}
int ObRpcRedisInternalResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // UNUSEDx(buf, buf_len, pos);
  if (OB_UNLIKELY(OB_ISNULL(ob_redis_result_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid internal response to encode", K(ret), K_(ob_redis_result));
  } else if (OB_FAIL(ob_redis_result_->encode(buf, buf_len, pos))) {
    LOG_WDIAG("failed to encode redis result internal", K(ret));
  } else {
    LOG_DEBUG("success to encode internal redis response", K(buf), K(buf_len), K(pos));
  }
  return ret;
}
int64_t ObRpcRedisInternalResponse::get_encode_size() const {
  int size = 0;
  if (OB_NOT_NULL(ob_redis_result_)) {
    size = ob_redis_result_->get_encode_size();
  }
  return size;
}


//ObRedis
int ObRedisResult::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, redis_result_buf_, redis_result_len_))) {
    LOG_WDIAG("fail to encode redis common response", K(ret));
  }
  return ret;
}

int ObRedisSingeLineResult::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_SL_FLAG, strlen(OB_REDIS_SL_FLAG)))) {
    LOG_WDIAG("fail to encode redis single line response flag", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, redis_result_buf_, redis_result_len_))) {
    LOG_WDIAG("fail to encode redis single line response", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_CL_CF, sizeof(OB_REDIS_CL_CF) / sizeof(char)))) {
    LOG_WDIAG("fail to encode redis single line response", K(ret));
  }
  return ret;
}

int64_t ObRedisSingeLineResult::get_encode_size()
{
  int value = -1;
  value = strlen(OB_REDIS_SL_FLAG) + redis_result_len_ + OB_REDIS_CL_CF_LEN;
  return value;
}

int ObRedisErrorResult::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_ERR_FLAG, strlen(OB_REDIS_ERR_FLAG)))) {
    LOG_WDIAG("fail to encode redis  error response flag", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, redis_result_buf_, redis_result_len_))) {
    LOG_WDIAG("fail to encode redis error response", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_CL_CF, sizeof(OB_REDIS_CL_CF) / sizeof(char)))) {
    LOG_WDIAG("fail to encode redis error response", K(ret));
  }
  return ret;
}

int64_t ObRedisErrorResult::get_encode_size()
{
  int value = -1;
  value = strlen(OB_REDIS_ERR_FLAG) + redis_result_len_ + OB_REDIS_CL_CF_LEN;
  return value;
}

int ObRedisIntegerResult::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char int_buf[20] { 0 };
  sprintf(int_buf, "%ld", return_value_);
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_INT_FLAG, strlen(OB_REDIS_INT_FLAG)))) {
    LOG_WDIAG("fail to encode redis integer response flag", K(ret));
  // } else if (OB_FAIL(serialization::encode_int(buf, buf_len, pos, return_value_))) {
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, int_buf, strlen(int_buf)))) {
    LOG_WDIAG("fail to encode redis integer response", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_CL_CF, sizeof(OB_REDIS_CL_CF) / sizeof(char)))) {
    LOG_WDIAG("fail to encode redis integer response", K(ret));
  }
  return ret;
}

int64_t ObRedisIntegerResult::get_encode_size()
{
  int value = -1;
  value = strlen(OB_REDIS_INT_FLAG) + serialization::encoded_length(return_value_) + OB_REDIS_CL_CF_LEN;
  return value;
}

int ObRedisBulkStringResult::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (is_null_) {
    redis_result_len_ = -1;
  }
  char int_buf[20] { 0 };
  sprintf(int_buf, "%ld", redis_result_len_);
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_BULK_STR_FLAG, strlen(OB_REDIS_BULK_STR_FLAG)))) {
    LOG_WDIAG("fail to encode redis bulk string response flag", K(ret));
  // } else if (OB_FAIL(serialization::encode_int(buf, buf_len, pos, redis_result_len_))) {
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, int_buf, strlen(int_buf)))) {
    LOG_WDIAG("fail to encode redis bulk string response", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_CL_CF, sizeof(OB_REDIS_CL_CF) / sizeof(char)))) {
    LOG_WDIAG("fail to encode redis bulk string response", K(ret));
  } else if (!is_null_ && OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, redis_result_buf_, redis_result_len_))) {
    LOG_WDIAG("fail to encode redis bulk string response", K(ret));
  } else if (!is_null_ && OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_CL_CF, sizeof(OB_REDIS_CL_CF) / sizeof(char)))) {
    LOG_WDIAG("fail to encode redis bulk string response", K(ret));
  }
  return ret;
}

int64_t ObRedisBulkStringResult::get_encode_size()
{
  int value = -1;
  if (is_null_) {
    redis_result_len_ = -1;
  }
  value = strlen(OB_REDIS_BULK_STR_FLAG) + serialization::encoded_length(redis_result_len_) + OB_REDIS_CL_CF_LEN;
  if (!is_null_) {
    value += redis_result_len_ + OB_REDIS_CL_CF_LEN;
  }
  return value;
}

int ObRedisArrayResult::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char int_buf[20] { 0 };
  sprintf(int_buf, "%ld", array_value_.count());
  if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_ARRAY_FLAG, strlen(OB_REDIS_ARRAY_FLAG)))) {
    LOG_WDIAG("fail to encode redis array response flag", K(ret));
  // } else if (OB_FAIL(serialization::encode_int(buf, buf_len, pos, array_value_.count()))) {
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, int_buf, strlen(int_buf)))) {
    LOG_WDIAG("fail to encode redis array response", K(ret));
  } else if (OB_FAIL(serialization::encode_raw_buf(buf, buf_len, pos, OB_REDIS_CL_CF, sizeof(OB_REDIS_CL_CF) / sizeof(char)))) {
    LOG_WDIAG("fail to encode redis array response", K(ret));
  } else {
    for (int64_t i = 0; i < array_value_.count() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(array_value_.at(i))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to encode array response due to invalid response to handle", K(i), "response", array_value_.at(i));
      } else if (OB_FAIL(array_value_.at(i)->encode(buf, buf_len, pos))) {
        LOG_WDIAG("fail to encode array response due to invalid response to handle", K(i), "response", array_value_.at(i));
      }
    }
  }
  return ret;
}

int64_t ObRedisArrayResult::get_encode_size()
{
  int value = -1;
  int ret = 0;
  value = strlen(OB_REDIS_ARRAY_FLAG) + serialization::encoded_length(array_value_.count()) + OB_REDIS_CL_CF_LEN;
  for (int64_t i = 0; OB_SUCC(ret) && i < array_value_.count(); i++) {
    if (OB_ISNULL(array_value_.at(i))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to encode array response due to invalid response to handle", K(i), "response", array_value_.at(i));
    } else {
      value += array_value_.at(i)->get_encode_size();
    }
  }

  if (OB_FAIL(ret)) {
    value = -1;
  }
  return value;
}

} // namespace obkv
} // namespace obproxy
} // namespace oceanbase
