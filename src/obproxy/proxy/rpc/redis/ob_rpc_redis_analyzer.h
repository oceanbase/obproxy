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

#ifndef OBPROXY_RPC_REDIS_ANALYZER_H
#define OBPROXY_RPC_REDIS_ANALYZER_H

#include "obkv/redis/ob_redis_rpc_request.h"
#include "proxy/rpc/redis/ob_rpc_redis_info.h"
#include "obproxy/obkv/table/ob_table_rpc_request.h"
#include "lib/encrypt/ob_encrypted_helper.h"

namespace oceanbase
{
namespace obproxy
{

namespace obkv
{
class ObRedisOperationResult;
}

namespace proxy
{
class ObRpcReq;
class ObRpcRedisAnalyzer
{
public:
/**
 * @brief
 *  get redis info from rpc_req, analyze redis request and store analyze result into redis info
 * @param rpc_req
 * @return int
 */
static int analyze_redis_request(ObRpcReq &rpc_req);
/**
 * @brief
 *  get redis info from rpc_req, analyze redis response and store analyze result into redis info
 * @param rpc_req
 * @return int
 */
static int analyze_redis_response(ObRpcReq &rpc_req);

/**
 * @brief
 *  get redis info from rpc_req, rewrite resp into obkv request and store it in obkv info
 * @param rpc_req
 * @return int
 */
static int handle_redis_request_rewrite(ObRpcReq &rpc_req);

/**
 * @brief
 *  get redis info from rpc_req, rewrite obkv response into resp and store it in redis info
 * @param rpc_req
 * @return int
 */
static int handle_redis_response_rewrite(ObRpcReq &rpc_req);

/**
 * @brief
 *  get redis info from rpc_req, build internal resp response and store it in redis info
 * @param rpc_req
 * @return int
 */
static int build_redis_internal_response(ObRpcReq &rpc_req);


static int build_redis_rpc_login_request(proxy::ObRpcReq &rpc_req, obkv::ObRpcTableLoginRequest *&rpc_login_req);
static int build_redis_rpc_common_request(proxy::ObRpcReq &rpc_req, obkv::ObRpcRedisOperationRequest *&rpc_table_req);
static int build_redis_rpc_common_simplified_request(proxy::ObRpcReq &rpc_req, obkv::ObRpcRedisOperationSimplifiedRequest *&rpc_table_req);
static int build_packet_meta_for_table_request(proxy::ObRpcReq &rpc_req, obkv::ObRpcRequest *rpc_request, obrpc::ObRpcPacketCode pcode);

static int build_bulk_string_resp(common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> &bulk_stirng,
                                  const char *buf,
                                  const int64_t len,
                                  int64_t &pos)
{
  UNUSEDx(bulk_stirng, buf, len, pos);
  return common::OB_SUCCESS;
}

static int create_scramble(common::ObMysqlRandom &random, char *scramble_buf);

static int build_err_msg(proxy::ObRpcReq &rpc_req, common::ObString &err_content, bool is_error_from_server);

static int build_err_resp(proxy::ObRpcReq &rpc_req, common::ObString &err_content);

static int build_ok_resp(proxy::ObRpcReq &rpc_req);

static int build_pong_resp(proxy::ObRpcReq &rpc_req);
static int build_int_resp(proxy::ObRpcReq &rpc_req);

static int build_common_resp(proxy::ObRpcReq &rpc_req, const common::ObString &content);

static int handle_redis_serialize_response(proxy::ObRpcReq &rpc_req);

static int check_redis_response(obkv::ObRedisOperationResult &table_redis_response, uint64_t &value_pos);
static int get_real_redis_response(proxy::ObRpcReq &rpc_req, char *&buf_ptr, int64_t &len, obrpc::ObRpcPacketCode pcode);

static int build_simple_String_resp(proxy::ObRpcReq &rpc_req, const common::ObString &content);


static int build_err_resp(common::ObString err_content, const char *buf, const int64_t len, int64_t &pos)
{
  UNUSEDx(err_content, buf, len, pos);
  return common::OB_SUCCESS;
}

static int build_ok_resp(const char *buf, const int64_t len, int64_t &pos)
{
  UNUSEDx(buf, len, pos);
  return common::OB_SUCCESS;
}

static int build_simple_string(const char *buf, const int64_t len, int64_t &pos)
{
  UNUSEDx(buf, len, pos);
  return common::OB_SUCCESS;
}

static int build_integer(const char *buf, const int64_t len, int64_t &pos)
{
  UNUSEDx(buf, len, pos);
  return common::OB_SUCCESS;
}
};

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase
#endif
