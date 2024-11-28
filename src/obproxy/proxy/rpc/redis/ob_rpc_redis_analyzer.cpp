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

#include "ob_rpc_redis_analyzer.h"
#include "ob_rpc_redis_command_factory.h"
#include "obproxy/proxy/rpc/ob_rpc_req.h"
#include "obproxy/obkv/table/ob_rpc_struct.h"
#include "obproxy/obkv/table/ob_table_rpc_request.h"
#include "obproxy/obkv/redis/ob_redis_rpc_response.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/random/ob_mysql_random.h"
#include "obproxy/proxy/rpc/rpclib/ob_rpc_req_ctx.h"
#include "obproxy/obkv/redis/ob_redis_rpc_request.h"
#include "obproxy/obkv/redis/ob_redis_rpc_response.h"
#include "proxy/rpc/net/ob_rpc_client_net_handler.h"

using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obkv;
const char *OK_STRING = "OK";
const char *PONG_STRING = "PONG";

int ObRpcRedisAnalyzer::analyze_redis_request(ObRpcReq &rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis info", K(ret));
  } else {
    ObArenaAllocator &allocator = redis_info->get_allocator();
    ObRpcRedisRequest *redis_request = NULL;
    if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_request(redis_info, redis_request, allocator))) {
      LOG_WDIAG("fail to analyze redis request", K(ret));
    } else {
      redis_info->set_redis_request(redis_request);
      LOG_DEBUG("analyze redis request succ", KPC(redis_request));
    }
  }
  return ret;
}

int ObRpcRedisAnalyzer::analyze_redis_response(ObRpcReq &rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcRedisRequest *redis_request = NULL;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  ObRpcRedisResponse *redis_response = NULL;
  int64_t pos = 0;
  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis info", K(ret), KPC(redis_info));
  } else if (OB_FALSE_IT(redis_request = redis_info->get_redis_request())) {
  } else if (OB_ISNULL(redis_request) || OB_ISNULL(redis_request->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis request", K(ret), KPC(redis_request), KPC(redis_info));
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_response(redis_request, redis_response))){
    LOG_WDIAG("fail to allocate redis resposne for redis response", K(ret));
  } else if (OB_FAIL(redis_response->analyze_response(redis_info->get_response_buf(), redis_info->get_response_buf_len(), pos))) {
    LOG_WDIAG("fail to analyze redis response", K(ret));
  }
  return ret;
}

int ObRpcRedisAnalyzer::build_redis_internal_response(ObRpcReq &rpc_req)
{
  UNUSED(rpc_req);
  int ret = OB_SUCCESS;
  ObRpcRedisRequest *redis_request = NULL;
  // ObRpcRedisResponse *redis_response = NULL;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();

  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis info", K(ret), KPC(redis_info));
  } else if (OB_FALSE_IT(redis_request = redis_info->get_redis_request())) {
  } else if (OB_ISNULL(redis_request) || OB_ISNULL(redis_request->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis request", K(ret), KPC(redis_request), KPC(redis_info));
  }

  return ret;
}

int ObRpcRedisAnalyzer::build_err_msg(ObRpcReq &rpc_req, ObString &err_content, bool is_error_from_server)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  const ObRpcReqTraceId &rpc_trace_id = rpc_req.get_trace_id();
  ObConnectionAttributes &server_info = rpc_req.get_server_addr();
  ObConnectionAttributes &obproxy_info = obkv_info.client_info_;

  ObString trace_id = rpc_trace_id.get_rpc_trace_id_buf();
  uint16_t port = 0;
  char ip_buff[MAX_IP_ADDR_LENGTH];
  ip_buff[0]='\0';
  if (is_error_from_server) {
    ops_ip_ntop(server_info.addr_, ip_buff, sizeof(ip_buff));
    port = server_info.get_port();
  } else {
    ops_ip_ntop(obproxy_info.obproxy_addr_, ip_buff, sizeof(ip_buff));
    port = obproxy_info.get_obproxy_port();
  }
  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invliad ob redis info to build error message", K(ret), "origin_error_core", obkv_info.rpc_origin_error_code_);
  } else if (OB_SUCC(redis_info->init_error_redis_msg_buf(common::OB_MAX_ERROR_MSG_LEN))){
    char *buf = redis_info->get_error_redis_msg_buf();
    const char *error_msg = common::ob_strerror(obkv_info.rpc_origin_error_code_);
    const char *error_name = common::ob_strerrorname(obkv_info.rpc_origin_error_code_);
    int len = sprintf(buf, "ERR errCode:%d, errCodeName:%s, errMsg:%s, server:%s:%d, trace:%.*s",
          obkv_info.rpc_origin_error_code_, error_name, error_msg, ip_buff, port, trace_id.length(), trace_id.ptr());
    if (len > 0) {
      err_content.assign_ptr(buf, len);
    } else {
      err_content.assign_ptr(error_msg, strlen(error_msg));
    }
  } else {
    const char *error_msg = common::ob_strerror(obkv_info.rpc_origin_error_code_);
    err_content.assign_ptr(error_msg, strlen(error_msg));
  }
  return ret;
}

int ObRpcRedisAnalyzer::build_err_resp(ObRpcReq &rpc_req, ObString &err_content)
{
  int ret = OB_SUCCESS;

  ObRpcRedisRequest *redis_request = NULL;
  // ObRpcRedisInternalResponse *redis_response = NULL;
  ObRpcRedisResponse *redis_response = NULL;
  ObRedisResult *redis_result = NULL;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();

  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_result(redis_info, OB_REDIS_ERROR, redis_result))) {
    LOG_WDIAG("invalid to init error redis result", K(ret));
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_internal_response(redis_request, redis_response))) {
    LOG_WDIAG("invalid to init error redis result response", K(ret));
  } else {
    LOG_DEBUG("to init error redis result response", K(ret), K(redis_response), K(redis_result), K(err_content.length()), K(err_content));
    redis_result->set_redis_result(err_content.ptr(), err_content.length());
    ((ObRpcRedisInternalResponse *)redis_response)->set_redis_result(redis_result);
    redis_info->set_redis_response(redis_response);
  }
  return ret;
}

int ObRpcRedisAnalyzer::build_ok_resp(ObRpcReq &rpc_req)
{
  int ret = OB_SUCCESS;

  ObRpcRedisRequest *redis_request = NULL;
  ObRpcRedisResponse *redis_response = NULL;
  ObRedisResult *redis_result = NULL;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  // ObString OK_STRING("OK");

  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_result(redis_info, OB_REDIS_SINGLE_LINE, redis_result))) {
    LOG_WDIAG("invalid to init error redis result", K(ret));
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_internal_response(redis_request, redis_response))) {
    LOG_WDIAG("invalid to init error redis result response", K(ret));
  } else {
    LOG_DEBUG("to init ok redis result response", K(ret), K(redis_response), K(redis_result), K(OK_STRING));
    redis_result->set_redis_result(OK_STRING, strlen(OK_STRING));
    ((ObRpcRedisInternalResponse *)redis_response)->set_redis_result(redis_result);
    redis_info->set_redis_response(redis_response);
  }

  return ret;
}

int ObRpcRedisAnalyzer::build_common_resp(proxy::ObRpcReq &rpc_req, const common::ObString &content)
{
  int ret = OB_SUCCESS;

  ObRpcRedisRequest *redis_request = NULL;
  ObRpcRedisResponse *redis_response = NULL;
  ObRedisResult *redis_result = NULL;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();

  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_result(redis_info, OB_REDIS_BULK_STRING, redis_result))) {
    LOG_WDIAG("invalid to init error redis result", K(ret));
  } else if (OB_FAIL(ObRpcRedisCommandFactory::gen_redis_internal_response(redis_request, redis_response))) {
    LOG_WDIAG("invalid to init error redis result response", K(ret));
  } else {
    LOG_DEBUG("to init ok redis result response", K(ret), K(redis_response), K(redis_result), K(content));
    redis_result->set_redis_result(content.ptr(), content.length());
    ((ObRpcRedisInternalResponse *)redis_response)->set_redis_result(redis_result);
    redis_info->set_redis_response(redis_response);
  }

  return ret;
}


int ObRpcRedisAnalyzer::handle_redis_request_rewrite(ObRpcReq &rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcRedisRequest *redis_request = NULL;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis info", K(ret), KPC(redis_info));
  } else if (OB_FALSE_IT(redis_request = redis_info->get_redis_request())) {
  } else if (OB_ISNULL(redis_request) || OB_ISNULL(redis_request->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis request", K(ret), KPC(redis_request), KPC(redis_info));
  } else if (OB_NOT_NULL(rpc_req.get_rpc_request()) && OB_FAIL(rpc_req.free_rpc_request())) {
    LOG_WDIAG("fail to call free_rpc_request", K(ret), K(rpc_req));
  } else {
    const ObRpcRedisCommandMeta *meta_info = redis_request->get_redis_command_meta();
    if (meta_info->is_auth()) {
      ObRpcTableLoginRequest *rpc_login_req = NULL;
      if (OB_FAIL(build_redis_rpc_login_request(rpc_req, rpc_login_req))) {
        LOG_WDIAG("fail to build table login request", K(ret));
      } else if (OB_FAIL(build_packet_meta_for_table_request(rpc_req, rpc_login_req, obrpc::OB_TABLE_API_LOGIN))) {
        LOG_WDIAG("fail to build packet meta for table login request", K(ret));
      } else {
        // redis_info->set_rewrited_request(rpc_login_req);
        rpc_req.set_rpc_request(rpc_login_req);
        rpc_req.set_rpc_request_len(sizeof(ObRpcTableLoginRequest));
      }
      // todo : output login_request;
    } else {
      ObRpcRedisOperationRequest *rpc_table_req = NULL;
      if (OB_FAIL(build_redis_rpc_common_request(rpc_req, rpc_table_req))) {
        LOG_WDIAG("fail to build table request", K(ret));
      } else if (OB_FAIL(build_packet_meta_for_table_request(rpc_req, rpc_table_req, obrpc::OB_REDIS_EXECUTE))) {
        LOG_WDIAG("fail to build packet meta for redis table operation", K(ret));
      }else {
        // redis_info->set_rewrited_request(rpc_table_req);
        rpc_req.set_rpc_request(rpc_table_req); //init rpc request to send
        rpc_req.set_rpc_request_len(sizeof(ObRpcRedisOperationRequest));
      }
    }
  }
  return ret;
}

int ObRpcRedisAnalyzer::handle_redis_response_rewrite(ObRpcReq &rpc_req)
{
  int ret = OB_SUCCESS;
  UNUSED(rpc_req);
  // do nohting now
  return ret;
}

int ObRpcRedisAnalyzer::handle_redis_serialize_response(proxy::ObRpcReq &rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  const ObRpcReqTraceId &rpc_trace_id = rpc_req.get_trace_id();
  ObRpcRedisResponse *redis_response = NULL;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;

  if (OB_ISNULL(redis_info) || OB_ISNULL(redis_response = redis_info->get_redis_response())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid redis request to handle", K(ret), K(rpc_req), K(redis_response), K(rpc_trace_id));
  } else {
    int64_t response_len = redis_response->get_encode_size();
    LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_redis_serialize_response", K(response_len), "inner_response",
              redis_info->is_inner_response(), K(rpc_req), K(rpc_trace_id));
    if (OB_ISNULL(redis_info->get_response_buf())) {
      //alloc response buf
      if (OB_FAIL(redis_info->alloc_response_buf(response_len + 8))) {
        LOG_WDIAG("fail to allocate rpc response buf", K(ret), K(rpc_trace_id));
      } else {
        buf = redis_info->get_response_buf();
        buf_len = redis_info->get_response_buf_len();
      }
    } else {
      //need use inner buffer
      if (OB_FAIL(redis_info->alloc_response_inner_buf(response_len + 8))) {
        LOG_WDIAG("fail to allocate response inner buf", K(ret));
      } else {
        buf = redis_info->get_response_inner_buf();
        buf_len = redis_info->get_response_inner_buf_len();
        redis_info->set_use_response_inner_buf(true);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(redis_response->encode(buf, buf_len, pos))) {
        LOG_WDIAG("fail to encode the response to buf", K(ret), K(rpc_req), K(rpc_trace_id));
      } else {
        rpc_req.set_response_len(pos);
        LOG_DEBUG("succ to encode the response to buf", K(pos), K(buf), K(buf_len), K(rpc_trace_id));
      }
    }
  }
  return ret;
}

int ObRpcRedisAnalyzer::build_redis_rpc_login_request(proxy::ObRpcReq &rpc_req, obkv::ObRpcTableLoginRequest *&rpc_login_req)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = rpc_req.get_rpc_ctx();
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  char *table_req_buf = NULL;
  char *scramble_buf = NULL;
  char *pass_secret_buf = NULL;
  // const char *tmp_scr_mm = "uESQNji4eJSZgMNzFzmY";
  ObMysqlRandom &random = event::this_ethread()->get_random_seed();
  ObString raw_password;
  int64_t pos = 0;
  int64_t scramble_len = SCRAMBLE_LENGTH;
  if (OB_ISNULL(rpc_ctx) || OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected rpc context", K(ret));
  } else if (OB_ISNULL(table_req_buf
                       = ObRpcRedisCommandFactory::alloc_redis_table_request(sizeof(ObRpcTableLoginRequest)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allcoate mem for table request", K(ret));
  } else if (OB_ISNULL(scramble_buf = static_cast<char *>(redis_info->get_allocator().alloc(scramble_len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mem for scramble buf", K(ret));
  } else if (OB_ISNULL(pass_secret_buf = static_cast<char *>(redis_info->get_allocator().alloc(scramble_len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mem for pass secret buf", K(ret));
  } else if (OB_FAIL(create_scramble(random, scramble_buf))) {
    LOG_WDIAG("fail to create scramable buf", K(ret));
  } else if (FALSE_IT(raw_password = rpc_ctx->get_rpc_password_str())) {
    //skip it
  // } else if (FALSE_IT(MEMCPY(scramble_buf, tmp_scr_mm, scramble_len))) {
    //tmp to init
  } else if (OB_FAIL(ObEncryptedHelper::encrypt_password(raw_password, ObString(scramble_len, scramble_buf),
                                                         pass_secret_buf, scramble_len + 1, pos))) {
    LOG_WDIAG("fail to encrypt password for login request", K(ret));
  // } else if (OB_FAIL(build_packet_meta_for_table_request(rpc_req, rpc_login_req, OB_TABLE_API_LOGIN))) {
    // LOG_WDIAG("fail to build packet meta for login request", K(ret));
  } else {
    rpc_login_req = new (table_req_buf) ObRpcTableLoginRequest();
    ObTableLoginRequest &login_req = rpc_login_req->get_login_request();
    login_req.auth_method_ = 1;
    login_req.client_type_ = 2;
    login_req.client_version_ = 1;
    login_req.reserved1_ = 0;
    login_req.client_capabilities_ = 0;
    login_req.max_packet_size_ = 0;
    login_req.reserved2_ = 0;
    login_req.reserved3_ = 0;
    // todo : add user_name, tenant_name, database_name to ctx
    login_req.tenant_name_ = rpc_ctx->get_tenant_name();
    login_req.user_name_ = rpc_ctx->get_user_name();
    login_req.database_name_ = rpc_ctx->get_database_name();
    login_req.ttl_us_ = 0;
    // login_req.pass_scramble_.assign_ptr(scramble_buf, SCRAMBLE_LENGTH + 1);
    login_req.pass_scramble_.assign_ptr(scramble_buf, scramble_len);
    login_req.pass_secret_.assign_ptr(pass_secret_buf, pos);

    redis_info->set_tenant_id(1); //for login request(set tenant_id to 1 to auth)

    // if (OB_FAIL(build_packet_meta_for_table_request(rpc_req, rpc_login_req, OB_TABLE_API_LOGIN))) {
    //   LOG_WDIAG("fail to build packet meta for login request", K(ret));
    // } else {
    //   rpc_req.set_rpc_request(rpc_login_req);
    //   rpc_req.set_rpc_request_len(sizeof(ObRpcTableLoginRequest));
    //   //TODO need set flag as redis request
    // }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(table_req_buf)) {
    ObRpcRedisCommandFactory::free_redis_table_request(rpc_login_req, sizeof(ObRpcTableLoginRequest));
  }
  return ret;
}

// int ObRpcRedisAnalyzer::init_redis_rowkey(proxy::ObRpcReq &rpc_req, obkv::ObRpcRedisOperationRequest *&rpc_table_req)
// {
//   int ret = OB_SUCCESS;
//   // ObRpcReqCtx *rpc_ctx = rpc_req.get_rpc_ctx();
//   // ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
//   // ObRpcRedisRequest *redis_req = NULL;
//   // ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
//   // const ObRpcRedisCommandMeta *meta_info;
//   // char *table_req_buf = NULL;
//   // if (OB_ISNULL(rpc_ctx) || OB_ISNULL(redis_info)
//   //     || OB_ISNULL(redis_req = redis_info->get_redis_request())
//   //     || OB_ISNULL(meta_info = redis_req->get_redis_command_meta())) {
//   //   ret = OB_ERR_UNEXPECTED;
//   return ret;
// }

int ObRpcRedisAnalyzer::build_redis_rpc_common_request(proxy::ObRpcReq &rpc_req, obkv::ObRpcRedisOperationRequest *&rpc_table_req)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *rpc_ctx = rpc_req.get_rpc_ctx();
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  ObRpcRedisRequest *redis_req = NULL;
  ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
  const ObRpcRedisCommandMeta *meta_info;
  char *table_req_buf = NULL;
  if (OB_ISNULL(rpc_ctx) || OB_ISNULL(redis_info)
      || OB_ISNULL(redis_req = redis_info->get_redis_request())
      || OB_ISNULL(meta_info = redis_req->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected rpc context", K(ret));
  // } else if (OB_ISNULL(table_req_buf = ObRpcRedisCommandFactory::alloc_redis_table_request(sizeof(ObRpcTableOperationRequest)))) {
  } else if (OB_ISNULL(table_req_buf = ObRpcRedisCommandFactory::alloc_redis_table_request(sizeof(ObRpcRedisOperationRequest)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allcoate mem for table request", K(ret));
  } else {
    rpc_table_req = new (table_req_buf) ObRpcRedisOperationRequest();
    rpc_table_req->set_cluster_version(4);
    ObRedisOperationRequest &redis_table_req = rpc_table_req->get_redis_operation_request();
    // ObTableOperationRequest &redis_table_req = rpc_table_req->get_redis_operation_request();

    ObITableEntity *table_entity = NULL;
    if (OB_FAIL(redis_table_req.table_operation_.get_entity(table_entity))) {
      LOG_WDIAG("invalid to get table entity object", K(ret));
    } else {
      // ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> &rowkey = table_entity->get_rowkey_objs();
      // ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> &properties_names = table_entity->get_properties_names();
      // ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> &properties_values = table_entity->get_properties_values();
      ObIArray<ObObj> &rowkey = ((ObTableEntity*)table_entity)->get_rowkey_objs();
      ObIArray<ObString> &properties_names = ((ObTableEntity*)table_entity)->get_properties_names();
      ObIArray<ObObj>&properties_values = ((ObTableEntity*)table_entity)->get_properties_values();
      table_entity->set_lazy_mode(false);

      ObObj redis_db;
      redis_db.set_int(redis_info->get_redis_db());
      ObObj redis_req_text;
      // redis_req_text.set_varchar(redis_info->get_request_buf(), redis_info->get_request_buf_len());
      redis_req_text.set_varchar(redis_info->get_request_buf(), redis_info->get_request_len());

      if (OB_FAIL(rowkey.push_back(redis_db))) {
        LOG_WDIAG("fail to push back rowkey db info", K(ret));
      } else if (OB_FAIL(rowkey.push_back(redis_req->rowkey_.at(0)))) { //just put first rowkey info to rowkey of redis request in OBKV
        LOG_WDIAG("fail to push back rowkey user data", K(ret));
      } else if (OB_FAIL(properties_names.push_back(REDIS_PROPERTY_NAME))) {
        LOG_WDIAG("fail to push back property_names", K(ret));
      } else if (OB_FAIL(properties_values.push_back(redis_req_text))) {
        LOG_WDIAG("fail to push back property_values", K(ret));
      } else if (OB_FAIL(build_packet_meta_for_table_request(rpc_req, rpc_table_req, OB_REDIS_EXECUTE))) {
        LOG_WDIAG("fail to build packet meta for table request", K(ret));
      } else {

        redis_table_req.credential_ = redis_info->get_rpc_credential(); //init credential value
        redis_table_req.table_name_ = meta_info->get_table_name();
        redis_table_req.table_id_ = obkv_info.get_table_id();
        // redis_table_req.tablet_id_ = obkv_info.get_partition_id();
        redis_table_req.partition_id_ = obkv_info.get_partition_id();
        redis_table_req.entity_type_ = obkv::ObTableEntityType::ET_DYNAMIC;
        // redis_table_req.operation_type_ = obkv::ObTableOperationType::REDIS;
        redis_table_req.table_operation_.set_type(obkv::ObTableOperationType::REDIS);
        redis_table_req.consistency_level_ = obkv::ObTableConsistencyLevel::STRONG;
        redis_table_req.returning_rowkey_ = false;
        redis_table_req.returning_affected_entity_ = true;
        redis_table_req.returning_affected_rows_ = false;
        // redis_table_req.returning_affected_rows_ = true;
        LOG_DEBUG("redis to encode", K(&redis_table_req), K(rpc_table_req), K(redis_table_req), KPC(rpc_table_req), K(sizeof(ObRpcRedisOperationRequest)));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(table_req_buf)) {
    ObRpcRedisCommandFactory::free_redis_table_request(rpc_table_req, sizeof(ObRpcRedisOperationRequest));
  }
  return ret;
}

int ObRpcRedisAnalyzer::build_packet_meta_for_table_request(proxy::ObRpcReq &rpc_req,
                                                            obkv::ObRpcRequest *rpc_request,
                                                            ObRpcPacketCode pcode)
{
  int ret = OB_SUCCESS;
  ObRpcPacketMeta &packet_meta = rpc_request->get_packet_meta();
  ObRpcEzHeader &ez_header = packet_meta.ez_header_;
  ObRpcPacketHeader &packet_header = packet_meta.rpc_header_;
  ObRpcClientNetHandler *client_net_handler = rpc_req.get_cnet_sm();
  ObRpcReqCtx *rpc_ctx = rpc_req.get_rpc_ctx();
  ObRpcRedisInfo *redis_info = rpc_req.get_redis_info();
  if (OB_ISNULL(client_net_handler) || OB_ISNULL(rpc_ctx) || OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected client net handler", K(ret), K(client_net_handler), K(rpc_ctx));
  } else {
    const ObRpcReqTraceId &trace_id = rpc_req.get_trace_id();
    memcpy(&ez_header.magic_header_flag_, obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG,
           sizeof(obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG));
    ez_header.reserved_ = 0;
    ez_header.chid_ = ATOMIC_AAF(&client_net_handler->conn_channel_id_, 1);
    packet_header.timestamp_ = ObTimeUtility::current_time();
    packet_header.pcode_ = pcode;
    packet_header.priority_ = 5; // for redis
    // packet_header.tenant_id_ = 1;
    // packet_header.tenant_id_ = 1002; //TODO need update it
    packet_header.tenant_id_ = redis_info->get_tenant_id();
    packet_header.priv_tenant_id_ = 1;
    packet_header.flags_ = ObRpcPacketHeader::RPC_HEADER_DEFAULT_FLAG;
    packet_header.session_id_ = 0;
    packet_header.timeout_ = obutils::get_global_proxy_config().rpc_redis_operation_timeout * 1000; // unit ns
    // packet_header.trace_id_[0] = client_net_handler->conn_unique_id_;
    // packet_header.trace_id_[1] = ATOMIC_AAF(&client_net_handler->conn_seq_, 1);
    trace_id.get_rpc_trace_id(packet_header.trace_id_[1], packet_header.trace_id_[0]);
  }
  return ret;
}

int ObRpcRedisAnalyzer::create_scramble(common::ObMysqlRandom &random, char *scramble_buf)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(scramble_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected scramble buf", K(ret));
  } else if (OB_FAIL(random.create_random_string(scramble_buf, SCRAMBLE_LENGTH + 1))) {
    LOG_WDIAG("fail to create random string", K(ret));
  // } else {
  //   scramble_buf[SCRAMBLE_LENGTH] = '\0';
  }
  return ret;
}
int ObRpcRedisAnalyzer::check_redis_response(ObRedisOperationResult &table_redis_response, uint64_t &value_pos)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(table_redis_response.properties_names_.count() == 1
                  && table_redis_response.properties_values_.count()
                  && REDIS_PROPERTY_NAME.case_compare(table_redis_response.properties_names_.at(0)) == 0)) {
      value_pos = 0;
  } else if (table_redis_response.properties_names_.count() == table_redis_response.properties_values_.count()) {
    uint64_t i = 0;
    uint64_t propertities_count = table_redis_response.properties_names_.count();
    bool found = false;
    for (i = 0; i < propertities_count; i++) {
      if (REDIS_PROPERTY_NAME.case_compare(table_redis_response.properties_names_.at(i)) == 0) {
        if (found) {
          //TODO update it if need to set ret to 'OB_ERR_UNEXPECTED'
          // print WDIAG log for invalid format result, not to prevent this request's response
          LOG_WDIAG("invalid format redis result from server, has more than one redis responses, just get last", K(ret), "properties_names_count", table_redis_response.properties_names_.count(),
                    "properties_values_count", table_redis_response.properties_values_.count(), "properties_names", table_redis_response.properties_names_, K(REDIS_PROPERTY_NAME));
        }
        value_pos = i;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid redis result", K(ret), "properties_names_count", table_redis_response.properties_names_.count(),
                "properties_values_count", table_redis_response.properties_values_.count(), "properties_names", table_redis_response.properties_names_, K(REDIS_PROPERTY_NAME));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis result", K(ret), "properties_names_count", table_redis_response.properties_names_.count(),
                "properties_values_count", table_redis_response.properties_values_.count(), "properties_names", table_redis_response.properties_names_, K(REDIS_PROPERTY_NAME));
  }

  return ret;
}

int ObRpcRedisAnalyzer::get_real_redis_response(proxy::ObRpcReq &rpc_req, char *&buf_ptr, int64_t &len)
{
  int ret = OB_SUCCESS;
  ObRpcRedisOperationResponse *redis_response = dynamic_cast<ObRpcRedisOperationResponse *>(rpc_req.get_rpc_response());
  if (OB_ISNULL(redis_response)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("deserialize redis request wrong", K(ret));
  } else {
    ObRedisOperationResult &table_redis_res = redis_response->get_table_operation_result();
    uint64_t values_pos = 0;
    //if (OB_UNLIKELY(table_redis_res.properties_names_.count() != 1
    //                       || table_redis_res.properties_values_.count() != 1
    //                       || REDIS_PROPERTY_NAME.case_compare(table_redis_res.properties_names_.at(0)) != 0))
    if (OB_UNLIKELY(OB_FAIL(ObRpcRedisAnalyzer::check_redis_response(table_redis_res, values_pos)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("get an invalid redis response from server", K(ret));
    } else if (table_redis_res.last_propertity_value_len_ > 0 && OB_NOT_NULL(table_redis_res.last_propertity_value_pos_)) {
      buf_ptr = table_redis_res.last_propertity_value_pos_;
      len     = table_redis_res.last_propertity_value_len_;
    } else {
      // TODO this method need copy buffer
      ObObj &redis_obj_result = table_redis_res.properties_values_.at(values_pos);
      if (!redis_obj_result.is_string_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected redis result", K(ret));
      } else {
        ObString result_str = redis_obj_result.get_string();
        buf_ptr = result_str.ptr();
        len = result_str.length();
        LOG_DEBUG("decode redis response get", K(result_str.ptr()), K(result_str), K(ret), K(len));
      }
    }
  }
  return ret;
}