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
#ifndef OBPROXY_RPC_REDIS_INFO_H
#define OBPROXY_RPC_REDIS_INFO_H
#include <stdint.h>
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{
class ObRpcRedisRequest;
class ObRpcRequest;
class ObRpcRedisResponse;
class ObRpcResponse;
}

namespace proxy
{
class ObRpcRedisCmdInfo;

#define COMMON_REDIS_ARGS_COUNT 4
#define OB_RPC_REDIS_DEFAULT_BUF_SIZE 1024
#define OB_RPC_REDIS_COMMAND_MAX_LEN 50
#define OB_RPC_REDIS_MONITOR_MAX_LEN 128
#define REDIS_CLIENT_NAME_LEN 512

//static const common::ObString REDIS_STRING_TABLE_NAME = "modis_string_table";
// static const common::ObString REDIS_LIST_TABLE_NAME = "modis_list_table";
// static const common::ObString REDIS_hash_TABLE_NAME = "modis_hash_table";
// static const common::ObString REDIS_SET_TABLE_NAME = "modis_set_table";
// static const common::ObString REDIS_ZSET_TABLE_NAME = "modis_zset_table";
static const common::ObString REDIS_STRING_TABLE_NAME = "obkv_redis_string_table";
static const common::ObString REDIS_LIST_TABLE_NAME = "obkv_redis_list_table";
static const common::ObString REDIS_HASH_TABLE_NAME = "obkv_redis_hash_table";
static const common::ObString REDIS_SET_TABLE_NAME = "obkv_redis_set_table";
static const common::ObString REDIS_ZSET_TABLE_NAME = "obkv_redis_zset_table";
static const common::ObString REDIS_EMPTY_TABLE_NAME = "";

static const common::ObString OB_REDIS_OB_PASSWORD_WRONG = "WRONGPASS invalid username-password pair or user is disabled.";
static const common::ObString OB_REDIS_OB_COMMAND_TIMEOUT = "Redis command timed out(odp).";

enum RedisCommandType {
  REDIS_COMMAND_INVALID = 0,
  // Auth
  REDIS_COMMAND_AUTH,
  REDIS_COMMAND_HELLO,
  // List
  REDIS_COMMAND_LINDEX,
  REDIS_COMMAND_LSET,
  REDIS_COMMAND_LRANGE,
  REDIS_COMMAND_LTRIM,
  REDIS_COMMAND_LPUSH,
  REDIS_COMMAND_LPUSHX,
  REDIS_COMMAND_RPUSH,
  REDIS_COMMAND_RPUSHX,
  REDIS_COMMAND_LPOP,
  REDIS_COMMAND_RPOP,
  REDIS_COMMAND_LREM,
  // REDIS_COMMAND_RPOPLPUSH,
  REDIS_COMMAND_LINSERT,
  REDIS_COMMAND_LLEN,
  REDIS_COMMAND_LDEL,

  // Set
  REDIS_COMMAND_SADD,
  REDIS_COMMAND_SCARD,
  REDIS_COMMAND_SDIFF,
  REDIS_COMMAND_SDIFFSTORE,
  REDIS_COMMAND_SINTER,
  REDIS_COMMAND_SINTERSTORE,
  REDIS_COMMAND_SISMEMBER,
  REDIS_COMMAND_SMEMBERS,
  REDIS_COMMAND_SMOVE,
  REDIS_COMMAND_SPOP,
  REDIS_COMMAND_SRANDMEMBER,
  REDIS_COMMAND_SREM,
  REDIS_COMMAND_SUNION,
  REDIS_COMMAND_SUNIONSTORE,

  // Zset
  REDIS_COMMAND_ZADD,
  REDIS_COMMAND_ZCARD,
  REDIS_COMMAND_ZCOUNT,
  REDIS_COMMAND_ZINCRBY,
  REDIS_COMMAND_ZINTERSTORE,
  REDIS_COMMAND_ZRANGE,
  REDIS_COMMAND_ZRANGEBYSCORE,
  REDIS_COMMAND_ZRANK,
  REDIS_COMMAND_ZREM,
  REDIS_COMMAND_ZREMRANGEBYRANK,
  REDIS_COMMAND_ZREMRANGEBYSCORE,
  REDIS_COMMAND_ZREVRANGE,
  REDIS_COMMAND_ZREVRANGEBYSCORE,
  REDIS_COMMAND_ZREVRANK,
  REDIS_COMMAND_ZSCORE,
  REDIS_COMMAND_ZUNIONSTORE,

  // Hash
  REDIS_COMMAND_HDEL,
  REDIS_COMMAND_HEXISTS,
  REDIS_COMMAND_HGET,
  REDIS_COMMAND_HGETALL,
  REDIS_COMMAND_HINCRBY,
  REDIS_COMMAND_HINCRBYFLOAT,
  REDIS_COMMAND_HKEYS,
  REDIS_COMMAND_HLEN,
  REDIS_COMMAND_HMGET,
  REDIS_COMMAND_HMSET,
  REDIS_COMMAND_HSET,
  REDIS_COMMAND_HSETNX,
  REDIS_COMMAND_HVALS,

  // String
  REDIS_COMMAND_APPEND,
  REDIS_COMMAND_BITCOUNT,
  REDIS_COMMAND_DECR,
  REDIS_COMMAND_DECRBY,
  REDIS_COMMAND_GET,
  REDIS_COMMAND_GETBIT,
  REDIS_COMMAND_GETRANGE,
  REDIS_COMMAND_GETSET,
  REDIS_COMMAND_INCR,
  REDIS_COMMAND_INCRBY,
  REDIS_COMMAND_INCRBYFLOAT,
  REDIS_COMMAND_MGET,
  REDIS_COMMAND_MSET,
  REDIS_COMMAND_PSETEX,
  REDIS_COMMAND_SET,
  REDIS_COMMAND_SETBIT,
  REDIS_COMMAND_SETEX,
  REDIS_COMMAND_SETNX,
  REDIS_COMMAND_SETRANGE,
  REDIS_COMMAND_STRLEN,

  // Common global command
  REDIS_COMMAND_TYPE,
  REDIS_COMMAND_DEL,
  REDIS_COMMAND_TTL,
  REDIS_COMMAND_EXISTS,
  REDIS_COMMAND_PTTL,
  REDIS_COMMAND_PEXPIRE,
  REDIS_COMMAND_PEXPIREAT,
  REDIS_COMMAND_PERSIST,
  REDIS_COMMAND_EXPIRE,
  REDIS_COMMAND_EXPIREAT,

  // Append new redis cmd_name type here
  REDIS_COMMAND_CLIENT,  //contains 'CLIENT LIST' / 'CLIENT SETNAME' / 'CLIENT GETNAME' / 'CLIENT PAUSE' / 'CLIENT KILL'
  REDIS_COMMAND_INFO,
  REDIS_COMMAND_MONITOR,

  REDIS_COMMAND_ECHO,
  REDIS_COMMAND_PING,
  REDIS_COMMAND_QUIT,
  REDIS_COMMAND_SELECT,
  REDIS_COMMAND_SWAPDB,
  REDIS_COMMAND_MAX
};

enum RedisResultType {
  OB_REDIS_SINGLE_LINE = 0,
  OB_REDIS_ERROR,
  OB_REDIS_INTEGER,
  OB_REDIS_BULK_STRING,
  OB_REDIS_ARRAY
};

class ObRpcRedisCmdInfo
{
public:
  ObRpcRedisCmdInfo() : redis_cmd_arr_len_(0), next_bulk_str_idx_(0), data_len_(0),
  valid_data_len_(0), data_pos_(0), redis_bulk_str_arr_(), redis_bulk_str_len_arr_()
  {}
  ~ObRpcRedisCmdInfo() {}
public:
  uint32_t redis_cmd_arr_len_;
  uint32_t next_bulk_str_idx_;
  uint64_t data_len_;
  uint64_t valid_data_len_;
  uint64_t data_pos_;

  common::ObSEArray<uint64_t, COMMON_REDIS_ARGS_COUNT> redis_bulk_str_arr_;
  common::ObSEArray<uint32_t, COMMON_REDIS_ARGS_COUNT> redis_bulk_str_len_arr_;
};

class ObRpcRedisInfo
{
public:
  ObRpcRedisInfo() : redis_cmd_info_(),
                     request_buf_(NULL), request_inner_buf_(NULL), response_buf_(NULL), response_inner_buf_(NULL), response_server_ptr_(NULL),
                     lower_redis_cmd_buf_(NULL), error_redis_msg_buf_(NULL), redis_inner_msg_buf_(NULL), request_buf_len_(0), request_inner_buf_len_(0), response_buf_len_(0), response_inner_buf_len_(0),
                     req_buf_repeat_times_(0), request_len_(0), response_len_(0),
                     redis_db_(0), tenant_id_(1), redis_args_(NULL), use_default_name_(false),
                     is_auth_request_(false), is_inner_request_(false), is_monitor_cmd_(false), is_error_response_(false), is_inner_response_(false),
                     is_use_response_inner_buf_(false), is_need_quit_(false), is_redis_msg_init_(false),
                     redis_cmd_type_(REDIS_COMMAND_MAX), redis_request_(NULL),
                    //  rewrited_request_(NULL), redis_response_(NULL), redis_table_response_(NULL),
                    //  rewrited_request_(NULL), redis_table_response_(NULL),
                     redis_table_response_(NULL),
                     rpc_credential_(), rpc_redis_msg_(), rpc_redis_monitor_msg_(), credential_(),
                     allocator_() {
                     }
  ~ObRpcRedisInfo() { reset(); };
  void reset();
  bool inner_redis_cmd() const;
  bool use_default_name() const { return use_default_name_; };
  void set_use_default_name(bool flag) { use_default_name_ = flag; }

  char    *get_request_buf() { return request_buf_; }
  char    *get_request_inner_buf() { return request_inner_buf_; }
  char    *get_response_inner_buf() { return response_inner_buf_; }
  char    *get_response_buf() { return response_buf_; }
  char    *get_response_server_ptr() { return response_server_ptr_; }
  int64_t  get_request_buf_len() const { return request_buf_len_; }
  int64_t  get_request_inner_buf_len() const { return request_inner_buf_len_; }
  int64_t  get_response_inner_buf_len() const { return response_inner_buf_len_; }
  int64_t  get_response_buf_len() const { return response_buf_len_; }
  int64_t  get_request_len() const { return request_len_; }
  int64_t  get_response_len() const { return response_len_; }

  int alloc_request_buf(uint64_t len);
  int realloc_request_buf(uint64_t len);
  int alloc_request_inner_buf(uint64_t len);
  int alloc_response_buf(uint64_t len);
  int alloc_response_inner_buf(uint64_t len);
  int free_request_buf();
  int free_request_inner_buf();
  int free_response_buf();
  int free_response_inner_buf();

  common::ObArenaAllocator &get_allocator() { return allocator_; }

  obkv::ObRpcRedisRequest *get_redis_request() const { return redis_request_; }
  // obkv::ObRpcRequest *get_rewrited_request() { return rewrited_request_; }

  void set_redis_request(obkv::ObRpcRedisRequest *redis_request) { redis_request_ = redis_request; }
  // void set_rewrited_request(obkv::ObRpcRequest *rewrited_request) { rewrited_request_ = rewrited_request; }
  void set_redis_response(obkv::ObRpcRedisResponse *redis_response) { redis_table_response_ = redis_response; }

  obkv::ObRpcRedisResponse* get_redis_response() { return redis_table_response_; }

  void set_redis_args(common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> *redis_arg) { redis_args_ = redis_arg; }
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> *get_redis_args() const { return redis_args_; }
  uint64_t get_redis_db() const { return redis_db_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  void set_request_len(uint64_t len) { request_len_ = len; }
  void set_auth_request(bool value) { is_auth_request_ = value; }
  void set_inner_request(bool value) { is_inner_request_ = value; }
  void set_monitor_cmd(bool value) { is_monitor_cmd_ = value; }
  void set_error_response(bool value) { is_error_response_ = value; }
  void set_inner_response(bool value) { is_inner_response_ = value; }
  void set_use_response_inner_buf(bool value) { is_use_response_inner_buf_ = value; }
  void set_need_quit(bool value) { is_need_quit_ = value; }
  void set_response_server_ptr(char *ptr) { response_server_ptr_ = ptr; }
  void set_response_len(uint64_t value) { response_len_ = value; }
  void set_redis_db(uint64_t db) { redis_db_ = db; }
  void set_tenant_id(uint64_t id) { tenant_id_ = id; }

  bool is_auth_request() { return is_auth_request_; }
  bool is_inner_request() { return is_inner_request_; }
  bool is_monitor_cmd() { return is_monitor_cmd_;}
  bool is_error_response() { return is_error_response_; }
  bool is_inner_response() { return is_inner_response_; }
  bool is_use_response_inner_buf() const { return is_use_response_inner_buf_; }
  bool is_need_quit() { return is_need_quit_; }

  void set_rpc_credential(const common::ObString &credential);
  common::ObString &get_rpc_credential() { return credential_; }
  char *get_lower_command_name();
  int init_error_redis_msg_buf(uint64_t size);
  char *get_error_redis_msg_buf() {return error_redis_msg_buf_; }
  int init_redis_inner_msg_buf(uint64_t size);
  char *get_redis_inner_msg_buf() {return redis_inner_msg_buf_; }
  char *get_redis_msg();
  char *get_redis_monitor_msg();
  void inc_req_buf_repeat_times() { req_buf_repeat_times_++; }

  TO_STRING_KV(KPC_(redis_args), KP_(redis_request));

public:
  ObRpcRedisCmdInfo redis_cmd_info_;

private:
  char *request_buf_;                   //byte data buffer received or to be send
  char *request_inner_buf_;             //byte data buffer which need re-serialize(only used in)
  char *response_buf_;                  //byte data buffer which is for response
  char *response_inner_buf_;            //byte data buffer which is for need re-serialize(only used in)

  char *response_server_ptr_;           //pointer to server response directly, not need to init RedisResponse again

  char *lower_redis_cmd_buf_;           //lower redis command info
  char *error_redis_msg_buf_;           //inited when need error msg in obproxy
  char *redis_inner_msg_buf_;           //inited when need inner msg in obproxy

  int64_t request_buf_len_;             //buffer length for request_buf_
  int64_t request_inner_buf_len_;       //buffer length for request_inner_buf_
  int64_t response_buf_len_;            //buffer lenght for response_buf_;
  int64_t response_inner_buf_len_;      //buffer lenght for response_buf_;
  int64_t req_buf_repeat_times_;        //req_buf used times, need to release more than MAX_REPEATE_TIMES
  int64_t request_len_;                 //request bytes' length
  int64_t response_len_;                //response bytes' length
  uint64_t redis_db_;                   // redis database
  uint64_t tenant_id_;                  //redis tenant_id
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> *redis_args_;

  bool use_default_name_;
  bool is_auth_request_;
  bool is_inner_request_;
  bool is_monitor_cmd_;
  bool is_error_response_;
  bool is_inner_response_;
  bool is_use_response_inner_buf_;
  bool is_need_quit_;
  bool is_redis_msg_init_;

  RedisCommandType redis_cmd_type_;
  obkv::ObRpcRedisRequest *redis_request_;
  // obkv::ObRpcRequest *rewrited_request_;
  // obkv::ObRpcResponse *redis_response_;
  obkv::ObRpcRedisResponse *redis_table_response_;

  char rpc_credential_[50];
  char rpc_redis_msg_[OB_RPC_REDIS_COMMAND_MAX_LEN];
  char rpc_redis_monitor_msg_[OB_RPC_REDIS_MONITOR_MAX_LEN];
  common::ObString credential_;
  // common::ObString client_name_;
  common::ObArenaAllocator allocator_; // clear for each request done
};

inline bool ObRpcRedisInfo::inner_redis_cmd() const
{
  return (redis_cmd_type_ == REDIS_COMMAND_INFO)
          || (redis_cmd_type_ == REDIS_COMMAND_QUIT)
          || (redis_cmd_type_ == REDIS_COMMAND_MONITOR)
          || (redis_cmd_type_ == REDIS_COMMAND_ECHO)
          || (redis_cmd_type_ == REDIS_COMMAND_PING);
          //TODO , select_db and swap_db
}

char *ObRpcRedisInfo::get_lower_command_name()
{
  uint64_t len = 0;
  if (OB_ISNULL(lower_redis_cmd_buf_)
        && OB_NOT_NULL(redis_args_)
        && redis_args_->count() > 0
        && (len = redis_args_->at(0).length()) > 0) { //not init lower redis info and has valid redis-command info
    char *buf = (char *)allocator_.alloc(len + 1);
    const char* cmd_ptr = redis_args_->at(0).ptr();
    for (int64_t i = 0; i < len; i++) {
      if (cmd_ptr[i] >= 'A' && cmd_ptr[i] <= 'Z') {
        buf[i] = static_cast<char>(cmd_ptr[i] + 32);
      } else {
        buf[i] = cmd_ptr[i];
      }
    }
    buf[len] = 0;
    lower_redis_cmd_buf_ = buf;
  }
  return lower_redis_cmd_buf_;
}

char *ObRpcRedisInfo::get_redis_msg()
{
  if (!is_redis_msg_init_ && OB_NOT_NULL(redis_args_)) {
    int64_t len = 0;
    int i = 0;
    while(len < OB_RPC_REDIS_COMMAND_MAX_LEN - 1 && i < redis_args_->count()) {
      common::ObString &info = redis_args_->at(i);
      int copy_len = info.length();
      const char *ptr = info.ptr();
      if (copy_len + len >= OB_RPC_REDIS_COMMAND_MAX_LEN) {
        copy_len = OB_RPC_REDIS_COMMAND_MAX_LEN - len - 1;
      }
      MEMCPY(rpc_redis_msg_ + len, ptr, copy_len);
      len += info.length();
      i++;
      if (i != redis_args_->count() && len < OB_RPC_REDIS_COMMAND_MAX_LEN - 1) {
        rpc_redis_msg_[len++] = ' ';
      }
    }
    if (len >= OB_RPC_REDIS_COMMAND_MAX_LEN) {
      len = OB_RPC_REDIS_COMMAND_MAX_LEN - 1;
    }
    rpc_redis_msg_[len] = 0;
    is_redis_msg_init_ = true;
  }
  return rpc_redis_msg_;
}

char *ObRpcRedisInfo::get_redis_monitor_msg()
{
  if (OB_NOT_NULL(redis_args_)) {
    int64_t len = 0;
    int i = 0;
    int args_count = is_auth_request() ? 1 : redis_args_->count();
    while (len < OB_RPC_REDIS_MONITOR_MAX_LEN - 1 && i < args_count) {
      common::ObString &info = redis_args_->at(i);
      int copy_len = info.length();
      const char *ptr = info.ptr();
      // just like "get"，for len+ "copy_len" + ""
      if (copy_len + len + 4 >= OB_RPC_REDIS_MONITOR_MAX_LEN) {
        copy_len = OB_RPC_REDIS_MONITOR_MAX_LEN - len - 5;
      }
      if(copy_len < 0) {
        copy_len = 0;
      }
      rpc_redis_monitor_msg_[len++] = '\"';
      MEMCPY(rpc_redis_monitor_msg_+ len, ptr, copy_len);
      len += copy_len;
      rpc_redis_monitor_msg_[len++] = '\"';
      i++;
      if (i != redis_args_->count() && len < OB_RPC_REDIS_MONITOR_MAX_LEN - 1) {
        rpc_redis_monitor_msg_[len++] = ' ';
      }
    }
    if (len >= OB_RPC_REDIS_MONITOR_MAX_LEN) {
      len = OB_RPC_REDIS_MONITOR_MAX_LEN - 1;
    }
    rpc_redis_monitor_msg_[len] = 0;
  }
  return rpc_redis_monitor_msg_;
}

}
}
}
#endif
