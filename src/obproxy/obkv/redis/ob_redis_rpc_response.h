/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "obkv/table/ob_table_rpc_response.h"
#ifndef _OB_REDIS_RPC_RESPONSE_H
#define _OB_REDIS_RPC_RESPONSE_H 1

#include "lib/ob_define.h"
#include "obproxy/obkv/table/ob_table_rpc_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

class ObRedisResult;

// table operation response simplified for redis
class ObRedisOperationSimplifiedResult
{
  OB_UNIS_VERSION(1);
public:
  ObRedisOperationSimplifiedResult() : resp_ret_(common::OB_ERR_UNEXPECTED), redis_str_() {}
  ~ObRedisOperationSimplifiedResult() = default;
  void reset()
  {
    resp_ret_ = common::OB_ERR_UNEXPECTED;
    redis_str_.reset();
  }
  TO_STRING_KV(K_(resp_ret), K_(redis_str));
public:
  int32_t resp_ret_;
  ObString redis_str_;
};

class ObRpcRedisOperationSimplifiedResponse : public ObRpcResponse
{
public:
  ObRpcRedisOperationSimplifiedResponse() : redis_operation_simplified_result_() {}
  ~ObRpcRedisOperationSimplifiedResponse() {}
  // ObRpcRedisOperationResponse(const ObRpcRedisOperationResponse &response) : ObRpcResponse(response) {}
  TO_STRING_KV(K_(rpc_packet_meta), K_(redis_operation_simplified_result));
  virtual void reset();
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;
  ObRedisOperationSimplifiedResult &get_table_operation_result() { return redis_operation_simplified_result_; }
private:
  ObRedisOperationSimplifiedResult redis_operation_simplified_result_;
};
// table operation response for redis
class ObRedisOperationResult : public ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObRedisOperationResult() : operation_type_(), rowkey_(), properties_names_(), properties_values_(), affected_rows_(0), last_propertity_value_pos_(NULL), last_propertity_value_len_(0) {}
  ~ObRedisOperationResult() = default;

  void reset()
  {
    operation_type_ = ObTableOperationType::Type::REDIS;
    rowkey_.reuse();
    properties_names_.reuse();
    properties_values_.reuse();
    last_propertity_value_len_ = 0;
    last_propertity_value_pos_ = NULL;
  }
public:
   ObTableOperationType::Type operation_type_;
   ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> rowkey_;
   ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> properties_names_;
   ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> properties_values_;
//  ObSEArray<uint64_t, ROWKEY_COLUMNS_COUNT> properties_names_pos_;
//  ObSEArray<uint64_t, ROWKEY_COLUMNS_COUNT> properties_names_len_;
//  ObSEArray<uint64_t, ROWKEY_COLUMNS_COUNT> properties_values_pos_;
//  ObSEArray<uint64_t, ROWKEY_COLUMNS_COUNT> properties_values_len_;
  int64_t affected_rows_;
 //
  char* last_propertity_value_pos_;  //not the object to serialize or desiralize, just set the position of 'REDIS_CODE_STR'
  uint64_t last_propertity_value_len_;
};

// class ObRedisOperationResult : public ObTableResult
// {
//   OB_UNIS_VERSION(1);
// public:
//   ObRedisOperationResult() : operation_type_(ObTableOperationType::GET), entity_(), affected_rows_() {}
//   ~ObRedisOperationResult() = default;
//   DECLARE_TO_STRING;
// public:
//   ObTableOperationType::Type operation_type_;
//   ObTableEntity entity_;
//   int64_t affected_rows_;
// };

class ObRpcRedisOperationResponse : public ObRpcResponse
{
public:
  ObRpcRedisOperationResponse() : redis_operation_result_() {}
  ~ObRpcRedisOperationResponse() {}
  // ObRpcRedisOperationResponse(const ObRpcRedisOperationResponse &response) : ObRpcResponse(response) {}
  TO_STRING_KV(K_(rpc_packet_meta), K_(redis_operation_result));
  virtual void reset();
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;
  ObRedisOperationResult &get_table_operation_result() { return redis_operation_result_; }
private:
  ObRedisOperationResult redis_operation_result_;
};

using ObRpcRedisLoginOperationResponse = ObRpcTableLoginResponse;

// redis resposne
class ObRpcRedisAuthResponse : public ObRpcRedisResponse
{
public:
  ObRpcRedisAuthResponse() : table_login_result_() {}
  virtual ~ObRpcRedisAuthResponse() {}

  int analyze_response(const char *buf, const int64_t len, int64_t &pos) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  int64_t get_encode_size() const override;
  ObRpcRedisLoginOperationResponse &get_redis_operation_resp() { return table_login_result_; }
  INHERIT_TO_STRING_KV("ObRpcRedisResponse", ObRpcRedisResponse, K_(table_login_result));
private:
  ObRpcRedisLoginOperationResponse table_login_result_;
};

class ObRpcRedisCommonResponse : public ObRpcRedisResponse
{
public:
  ObRpcRedisCommonResponse() : table_redis_result_() {}
  virtual ~ObRpcRedisCommonResponse() {}

  int analyze_response(const char *buf, const int64_t len, int64_t &pos) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  int64_t get_encode_size() const override;
  ObRpcRedisOperationResponse &get_redis_operation_resp() { return table_redis_result_; }
  INHERIT_TO_STRING_KV("ObRpcRedisResponse", ObRpcRedisResponse, K_(table_redis_result));
private:
  ObRpcRedisOperationResponse table_redis_result_;
};

class ObRpcRedisInternalResponse : public ObRpcRedisResponse
{
public:
  ObRpcRedisInternalResponse() : ob_redis_result_(NULL) {}
  virtual ~ObRpcRedisInternalResponse() {}

  int analyze_response(const char *buf, const int64_t len, int64_t &pos) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  int64_t get_encode_size() const override;
  void set_redis_result(ObRedisResult *result) { ob_redis_result_ = result; }
  INHERIT_TO_STRING_KV("ObRpcRedisResponse", ObRpcRedisResponse, K(""));

private:
  ObRedisResult *ob_redis_result_;
};

class ObRpcRedisGlobalResponse : public ObRpcRedisResponse
{
public:
  ObRpcRedisGlobalResponse() {}
  virtual ~ObRpcRedisGlobalResponse() {}

  int analyze_response(const char *buf, const int64_t len, int64_t &pos) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  int64_t get_encode_size() const override;
  INHERIT_TO_STRING_KV("ObRpcRedisResponse", ObRpcRedisResponse, K(""));
};

class ObRedisResult
{
public:
  ObRedisResult() : redis_result_buf_(NULL), redis_result_len_(0) {}
  virtual ~ObRedisResult() {}
  const char OB_REDIS_CL_CF[2] = {'\r', '\n'};
  const int OB_REDIS_CL_CF_LEN = 2;

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size() const { return redis_result_len_; }
  // virtual int analyze_response(const char *buf, const int64_t len, int64_t &pos) = 0;
  void set_redis_result(const char *redis_result_buf, int64_t redis_result_len)
  {
    redis_result_buf_ = redis_result_buf;
    redis_result_len_ = redis_result_len;
  }
  const char *get_redis_result_ptr() { return redis_result_buf_; }
  int64_t get_redis_result_len() { return redis_result_len_; }

  TO_STRING_KV(KP_(redis_result_buf), K_(redis_result_len));

public:
  const char *redis_result_buf_;
  int64_t redis_result_len_;
};

class ObRedisSingeLineResult : public ObRedisResult
{
public:
  ObRedisSingeLineResult() : ObRedisResult() {}
  ~ObRedisSingeLineResult() {}
  const char *OB_REDIS_SL_FLAG = (const char *)"+";
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size();

public:
};

class ObRedisErrorResult : public ObRedisResult
{
public:
  ObRedisErrorResult() : ObRedisResult() {}
  ~ObRedisErrorResult() {}
  const char *OB_REDIS_ERR_FLAG = (const char *)"-";
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size();

public:
};

class ObRedisIntegerResult : public ObRedisResult
{
public:
  ObRedisIntegerResult() : ObRedisResult(), return_value_(0) {}
  ~ObRedisIntegerResult() {}
  const char *OB_REDIS_INT_FLAG = (const char *)":";
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size();

public:
  int64_t return_value_;
};

class ObRedisBulkStringResult : public ObRedisResult
{
public:
  ObRedisBulkStringResult() : ObRedisResult(), is_null_(false) {}
  ~ObRedisBulkStringResult() {}
  const char *OB_REDIS_BULK_STR_FLAG = (const char *)"$";
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size();

public:
  bool is_null_; /* null_value='$-1\r\n'  and '' = '$0\r\n\r\n' */
  // int64_t return_value_;
};

class ObRedisArrayResult : public ObRedisResult
{
public:
  ObRedisArrayResult() : ObRedisResult(), array_value_() {}
  ~ObRedisArrayResult() {}
  const char *OB_REDIS_ARRAY_FLAG = (const char *)"*";
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size();

public:
  ObSEArray<ObRedisResult *, 4> array_value_;
  // int64_t return_value_;
};

} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase
#endif
