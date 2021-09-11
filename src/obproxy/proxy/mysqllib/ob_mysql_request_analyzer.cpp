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
#include "ob_mysql_request_analyzer.h"
#include "packet/ob_mysql_packet_reader.h"
#include "packet/ob_mysql_packet_util.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "obutils/ob_cached_variables.h"
#include "common/obsm_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include <ob_sql_parser.h>
#include <parse_malloc.h>

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace oceanbase::sql;
using namespace obutils;
using namespace obmysql;
using namespace event;
using namespace packet;

static const char OBPROXY_SIMPLE_PART_KEY_MARK = '*';

int ObRequestAnalyzeCtx::init_auth_request_analyze_ctx(ObRequestAnalyzeCtx &ctx,
                                                       ObIOBufferReader *buffer_reader,
                                                       const ObString &vip_tenant_name,
                                                       const ObString &vip_cluster_name)
{
  int ret = OB_SUCCESS;

  ctx.reader_ = buffer_reader;
  ctx.is_auth_ = true;
  ctx.vip_tenant_name_ = vip_tenant_name;
  ctx.vip_cluster_name_ = vip_cluster_name;
  /*
  useless in auth request analyze:
  ctx.tenant_name_;
  ctx.database_name_;
  ctx.parse_mode_
  ctx.large_request_threshold_len_ ;
  ctx.large_request_analyze_len_;
  */
  return ret;
}

ObMysqlRequestAnalyzer::ObMysqlRequestAnalyzer()
    : packet_length_(0),
      packet_seq_(0),
      nbytes_analyze_(0),
      is_last_request_packet_(true),
      request_count_(0),
      payload_offset_(0)
{
  MEMSET(payload_length_buffer_, 0, MYSQL_NET_HEADER_LENGTH);
}

void ObMysqlRequestAnalyzer::analyze_request(const ObRequestAnalyzeCtx &ctx,
                                             ObMysqlAuthRequest &auth_request,
                                             ObProxyMysqlRequest &client_request,
                                             ObMySQLCmd &sql_cmd,
                                             ObMysqlAnalyzeStatus &status,
                                             const bool is_oracle_mode /* false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.reader_)) {
    ret = OB_INVALID_ARGUMENT;
    status = ANALYZE_ERROR;
    LOG_WARN("invalid argument", "reader", ctx.reader_, K(ret), K(status));
  } else {
    // 1. determine whether mysql request packet is received complete
    ObMysqlAnalyzeResult result;
    if (ctx.is_auth_) {
      if (OB_FAIL(handle_auth_request(*ctx.reader_, result))) {
        LOG_WARN("fail to handle auth request", K(ret));
      }
    } else {
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*ctx.reader_, result))) {
        LOG_WARN("fail to analyze one packet", K(ret));
      }
    }

    // 2. verify request's legality, just print WARN
    int64_t avail_bytes = ctx.reader_->read_avail();
    if (avail_bytes >= MYSQL_NET_META_LENGTH) {
      if (avail_bytes > result.meta_.pkt_len_) {
        LOG_WARN("recevied more than one mysql packet at once, it is unexpected so far",
                 "first packet len(include packet header)", result.meta_.pkt_len_,
                 "total len received", avail_bytes, K(ctx.is_auth_));
      } else if (result.meta_.cmd_ < OB_MYSQL_COM_SLEEP || result.meta_.cmd_ >= OB_MYSQL_COM_MAX_NUM) {
        LOG_WARN("unknown mysql cmd", "cmd", result.meta_.cmd_, K(avail_bytes),
                 "packet len", result.meta_.pkt_len_);
      }
    }

    status = result.status_;
    if (ANALYZE_DONE == status) {
      LOG_DEBUG("mysql request packet is received complete", "is_auth", ctx.is_auth_,
               "analyze status", ObProxyParserUtils::get_analyze_status_name(status),
               K(result.meta_), "packet cmd type",
               ObProxyParserUtils::get_sql_cmd_name(result.meta_.cmd_));
    } else {
      LOG_DEBUG("mysql request packet has not yet been received complete", "is_auth", ctx.is_auth_,
                "analyze status", ObProxyParserUtils::get_analyze_status_name(status), K(avail_bytes),
                K(result.meta_), "packet cmd type",
                ObProxyParserUtils::get_sql_cmd_name(result.meta_.cmd_));
    }

    if (OB_SUCCESS == ret && (ANALYZE_DONE == status || ANALYZE_CONT == status)) {
      // 3. set mysql request packet meta
      sql_cmd = result.meta_.cmd_;
      if (OB_MYSQL_COM_LOGIN == result.meta_.cmd_ || OB_MYSQL_COM_HANDSHAKE == result.meta_.cmd_) {
        // add pkt meta to mysql auth request
        auth_request.set_packet_meta(result.meta_);
      } else {
        // add pkt meta to mysql common request
        // maybe we has not receive all data to get meta,
        // in this case, just set a empty packet meta, no bad affect.
        client_request.set_packet_meta(result.meta_);
      }

      // 4. dispatch mysql packet by cmd type, and parse mysql request
      if (ANALYZE_DONE == status) {
        if (OB_FAIL(do_analyze_request(ctx, sql_cmd, auth_request, client_request, is_oracle_mode))) {
          if (OB_ERR_PARSE_SQL == ret) {
            //ob parse fail will not disconnect
            status = ANALYZE_OBPARSE_ERROR;
          } else if (OB_ERROR_UNSUPPORT_EXPR_TYPE == ret) {
            status = ANALYZE_OBUNSUPPORT_ERROR;
          } else {
            status = ANALYZE_ERROR;
          }
          LOG_WARN("fail to dispatch mysql cmd", "analyze status",
                   ObProxyParserUtils::get_analyze_status_name(status), K(ret));
        }
      } else if (ANALYZE_CONT == status) {
        // we will analyze large request if we received enough packet(> request_buffer_len_)
        if (!ctx.is_auth_
            && ctx.large_request_threshold_len_ > 0
            && result.meta_.pkt_len_ > ctx.large_request_threshold_len_
            && avail_bytes + ObProxyMysqlRequest::PARSE_EXTRA_CHAR_NUM > ctx.request_buffer_length_
            && !client_request.is_sharding_user()
            && !ctx.using_ldg_) {
          if (OB_FAIL(do_analyze_request(ctx, sql_cmd, auth_request, client_request, is_oracle_mode))) {
            status = ANALYZE_ERROR;
            LOG_WARN("fail to dispatch mysql cmd", "analyze status",
                     ObProxyParserUtils::get_analyze_status_name(status), K(ret));
          } else {
            client_request.set_large_request(true);
          }
        } else {
          // continue to receive more packet, do nothing here
        }
      }
    }
  }
}

int ObMysqlRequestAnalyzer::get_payload_length(const char *buffer)
{
  int ret = OB_SUCCESS;
  int64_t payload_length = -1;

  if (OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer is NULL", K(ret));
  } else {
    payload_length = ob_uint3korr(buffer);
    packet_seq_ = ob_uint1korr(buffer + MYSQL_PAYLOAD_LENGTH_LENGTH);
    if (MYSQL_PACKET_MAX_LENGTH == payload_length) {
      is_last_request_packet_ = false;
    } else {
      ++request_count_;
      if (request_count_ > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("received two mysql packet, unexpected", K(packet_length_), K(payload_length),
                 K(packet_seq_), K(request_count_), K(ret));
      } else {
        is_last_request_packet_ = true;
      }
    }
    LOG_DEBUG("payload length is ", K(payload_length));
    packet_length_ += payload_length + MYSQL_NET_HEADER_LENGTH;
  }
  return ret;
}

int ObMysqlRequestAnalyzer::is_request_finished(const ObRequestBuffer &buff, bool &is_finish)
{
  int ret = OB_SUCCESS;
  int64_t length = buff.length();
  const char *data = buff.ptr();

  bool found_next_payload_length = true;
  int64_t i = 0;

  if (0 != payload_offset_) { // part of payload length found in previous buffer
    int64_t need_length = MYSQL_NET_HEADER_LENGTH - payload_offset_;
    if (length < need_length) {
      // this buffer isn't enough to hold all rest payload length,
      // the rest will be found in next buffer
      found_next_payload_length = false;
      for (i = 0; i < length; ++i, ++payload_offset_) {
        payload_length_buffer_[payload_offset_] = *(data + i);
      }
    } else {
      for (i = 0; i < need_length; ++i, ++payload_offset_) {
        payload_length_buffer_[payload_offset_] = *(data + i);
      }
      if (OB_FAIL(get_payload_length(payload_length_buffer_))) {
        LOG_WARN("fail to get_payload_length", K(ret));
      } else {
        MEMSET(payload_length_buffer_, 0, MYSQL_NET_HEADER_LENGTH);
        payload_offset_ = 0;
      }
    }
  }

  if (OB_SUCC(ret) && found_next_payload_length) {
    // it is possible that more than one mysql packet is in buff
    while (packet_length_ < length + nbytes_analyze_) {
      // found new payload length
      int64_t next_packet_start_offset = packet_length_ - nbytes_analyze_;
      int64_t left_length = length - next_packet_start_offset;
      if (left_length < MYSQL_NET_HEADER_LENGTH) {
        // this buffer isn't enough to hold all payload length,
        // the rest will be found in next buffer
        for (i = 0; i < left_length; ++i, ++payload_offset_) {
          payload_length_buffer_[payload_offset_] = *(data + i + next_packet_start_offset);
        }
        break;
      } else if (OB_FAIL(get_payload_length(data + next_packet_start_offset))) {
        mysql_hex_dump(data + next_packet_start_offset, left_length);
        LOG_WARN("fail to get_payload_length", K(ret));
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret)) {
    nbytes_analyze_ += length;
    if (packet_length_ == nbytes_analyze_ && is_last_request_packet_) {
      is_finish = true;
    } else {
      is_finish = false;
    }
  }
  LOG_DEBUG("analyze_request_is_finish", K(length), K(nbytes_analyze_), K(packet_length_),
                                         K(is_last_request_packet_), K(is_finish));
  return ret;
}

int ObMysqlRequestAnalyzer::is_request_finished(event::ObIOBufferReader &reader, bool &is_finish)
{
  int ret = OB_SUCCESS;

  ObIOBufferBlock *block = NULL;
  int64_t offset = 0;
  char *data = NULL;
  int64_t data_size = 0;
  if (NULL != reader.block_) {
    reader.skip_empty_blocks();
    block = reader.block_;
    offset = reader.start_offset_;
    data = block->start() + offset;
    data_size = block->read_avail() - offset;
  }

  if (data_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  }

  ObRequestBuffer buffer;
  while (OB_SUCC(ret) && NULL != block && data_size > 0 && !is_finish) {
    buffer.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(is_request_finished(buffer, is_finish))) {
      LOG_WARN("fail to analyze_request_is_finish", K(ret));
    } else {
      // on to the next block
      offset = 0;
      block = block->next_;
      if (NULL != block) {
        data = block->start();
        data_size = block->read_avail();
        if (is_finish && (data_size > 0)) {
          mysql_hex_dump(data, data_size);
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("receive two request, unexpected", K(is_finish), K(data_size), K(ret));
        }
      }
    }
  }

  LOG_DEBUG("analyze_request_is_finish", "data_size", reader.read_avail(), K(is_finish));

  return ret;
}

void ObMysqlRequestAnalyzer::reuse()
{
  packet_length_ = 0;
  packet_seq_ = 0;
  nbytes_analyze_ = 0;
  is_last_request_packet_ = true;
  MEMSET(payload_length_buffer_, 0, MYSQL_NET_HEADER_LENGTH);
  payload_offset_ = 0;
  request_count_ = 0;
}

inline int ObMysqlRequestAnalyzer::handle_auth_request(ObIOBufferReader &reader,
                                                        ObMysqlAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  int64_t len = reader.read_avail();
  // FIXME we assume that the auth process just includes three steps:
  //       1. hello packet(OB_MYSQL_COM_HANDSHAKE)     (observer---->mysql client);
  //       2. login packet                    (mysql client---->observer);
  //       3. login response (OB_MYSQL_COM_LOGIN)      (observer---->mysql client);
  //          <1> OK packet, login successful
  //          <2> ERR packet, login failed
  // other auth functions will be supported according to the realization of observer
  if (0 == len) {
    result.meta_.cmd_ = OB_MYSQL_COM_HANDSHAKE;
    result.status_ = ANALYZE_DONE;
    result.meta_.pkt_len_ = 0;
    result.meta_.pkt_seq_ = 0;
  } else if (len > 0) {
    if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(reader, result))) {
      LOG_WARN("fail to analyze one packet", K(ret));
    } else {
      result.meta_.cmd_ = OB_MYSQL_COM_LOGIN;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it shoulde not happened", K(ret));
  }
  return ret;
}

void ObMysqlRequestAnalyzer::extract_fileds(const ObExprParseResult& result, ObProxyMysqlRequest &client_request)
{
  SqlFieldResult &sql_result = client_request.get_parse_result().get_sql_filed_result();
  extract_fileds(result, sql_result);
}

void ObMysqlRequestAnalyzer::extract_fileds(const ObExprParseResult& result, SqlFieldResult &sql_result)
{
  int64_t total_num = result.all_relation_info_.relation_num_;
  ObProxyRelationExpr* relation_expr = NULL;
  ObString name_str;
  ObString value_str;
  for (int64_t i = 0; i < total_num; i ++) {
    relation_expr = result.all_relation_info_.relations_[i];
    if (OB_ISNULL(relation_expr)) {
      LOG_WARN("Got an empty relation_expr", K(i));
      continue;
    } else if (relation_expr->type_ != F_COMP_EQ) {
      LOG_DEBUG("only support eq now");
      continue;
    }
    if (relation_expr->left_value_ != NULL
        && relation_expr->left_value_->head_ != NULL
        && relation_expr->left_value_->head_->type_ == TOKEN_COLUMN) {
      if (relation_expr->left_value_->head_->column_name_.str_ != NULL) {
        name_str.assign_ptr(relation_expr->left_value_->head_->column_name_.str_,
                       relation_expr->left_value_->head_->column_name_.str_len_);
      } else {
        LOG_WARN("get an empty column_name_");
      }
    } else {
      LOG_WARN("left value is null");
    }
    if (relation_expr->right_value_ != NULL) {
      ObProxyTokenNode *token = relation_expr->right_value_->head_;
      SqlField field;
      field.reset();
      field.column_name_.set(name_str);
      SqlColumnValue column_value;
      while (NULL != token) {
        switch(token->type_) {
          case TOKEN_INT_VAL:
            column_value.value_type_ = TOKEN_INT_VAL;
            column_value.column_int_value_ = token->int_value_;
            column_value.column_value_.set_integer(token->int_value_);
            field.column_values_.push_back(column_value);
            break;
          case TOKEN_STR_VAL:
            column_value.value_type_ = TOKEN_STR_VAL;
            value_str.assign_ptr(token->str_value_.str_,
                                 token->str_value_.str_len_);
            column_value.column_value_.set(value_str);
            field.column_values_.push_back(column_value);
            break;
          default:
            LOG_DEBUG("invalid token type", "token type", get_obproxy_token_type(token->type_));
            break;
        }
        token = token->next_;
      }
      if (field.is_valid()) {
        sql_result.fields_.push_back(field);
        ++sql_result.field_num_;
      }
    } else {
      LOG_WARN("right value is null");
    }
  }
}

int ObMysqlRequestAnalyzer::parse_sql_fileds(ObProxyMysqlRequest &client_request,
                                             ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;
  bool need_parse_fields = true;
  ObExprParseMode parse_mode = INVLIAD_PARSE_MODE;
  ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
  if (sql_parse_result.is_select_stmt() && sql_parse_result.get_dbp_route_info().scan_all_) {
    //sharding parser extract by ob_parse
    LOG_DEBUG("sharding sql no need parse_sql_fileds");
    need_parse_fields = false;
  } else if (sql_parse_result.is_select_stmt() || sql_parse_result.is_delete_stmt()) {
    // we treat delete as select
    parse_mode = SELECT_STMT_PARSE_MODE;
  } else if (sql_parse_result.is_insert_stmt() || sql_parse_result.is_replace_stmt()
             || sql_parse_result.is_update_stmt()) {
    parse_mode = INSERT_STMT_PARSE_MODE;
  } else {
    LOG_DEBUG("no need parse sql fields", K(client_request.get_parse_sql()));
    need_parse_fields = false;
  }
  if (need_parse_fields) {
    ObArenaAllocator *allocator = NULL;
    ObExprParseResult expr_result;
    if (OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator))) {
      LOG_WARN("fail to get parse allocator", K(ret));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else {
      ObExprParser expr_parser(*allocator, parse_mode);
      ObString sql = client_request.get_sql();
      expr_result.part_key_info_.key_num_ = 0;
      ObString expr_sql = ObProxyMysqlRequest::get_expr_sql(sql, sql_parse_result.get_parsed_length());
      if (SELECT_STMT_PARSE_MODE == parse_mode) {
        const char *expr_sql_str = expr_sql.ptr();
        const char *pos = NULL;
        if (OB_LIKELY(NULL != expr_sql_str)
            && NULL != (pos = strcasestr(expr_sql_str, "WHERE"))
            && (pos - expr_sql_str) > 1
            && OB_LIKELY((pos - expr_sql_str) < expr_sql.length())) {
          const int32_t len_before_where = static_cast<int32_t>(pos - expr_sql_str);
          expr_sql += (len_before_where - 1);
        }
      }
      LOG_DEBUG("expr_sql is ", K(expr_sql));
      if (OB_FAIL(expr_parser.parse(expr_sql, expr_result, connection_collation))) {
        LOG_WARN("parse failed", K(expr_sql));
      } else if (INSERT_STMT_PARSE_MODE == parse_mode
                    && FALSE_IT(sql_parse_result.set_batch_insert_values_count(expr_result.multi_param_values_))) {
        // not to be here
      } else {
        LOG_DEBUG("parse success:", K(sql), K(expr_sql), K(expr_result.all_relation_info_.relation_num_));
      }
      extract_fileds(expr_result, client_request);
      if (NULL != allocator) {
        allocator->reuse();
      }
    }
  }
  SqlFieldResult &sql_result = sql_parse_result.get_sql_filed_result();
  LOG_DEBUG("finish extract sql fields", K(sql_result.fields_));
  return ret;
}

inline int ObMysqlRequestAnalyzer::do_analyze_request(
    const ObRequestAnalyzeCtx &ctx,
    const ObMySQLCmd sql_cmd,
    ObMysqlAuthRequest &auth_request,
    ObProxyMysqlRequest &client_request,
    const bool is_oracle_mode /*false*/)
{
  int ret = OB_SUCCESS;

  switch (sql_cmd) {
    case OB_MYSQL_COM_STMT_PREPARE_EXECUTE:
    case OB_MYSQL_COM_STMT_PREPARE:
    case OB_MYSQL_COM_QUERY: {
      // add packet's buffer to mysql common request, for parsing later
      if (OB_FAIL(client_request.add_request(ctx.reader_, ctx.request_buffer_length_))) {
        LOG_WARN("fail to add com request", K(ret));
      } else {
        LOG_DEBUG("the request sql", "sql", client_request.get_sql());

        ObString sql = client_request.get_parse_sql();
        if ((OB_SUCC(ret)) && !sql.empty()) {
          ObProxySqlParser sql_parser;
          ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
          // we will handle parser error in parse_sql
          if (OB_ISNULL(ctx.cached_variables_)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("cached_variables should not be null", K(ret));
          } else {
            bool use_lower_case_name = false;
            if (!ctx.is_sharding_mode_ && !is_oracle_mode)  {
              use_lower_case_name = ctx.cached_variables_->need_use_lower_case_names();
            }
            LOG_DEBUG("use_lower_case_name is ", K(use_lower_case_name), K(sql));

            if (OB_FAIL(sql_parser.parse_sql(sql, ctx.parse_mode_, sql_parse_result,
                                             use_lower_case_name,
                                             ctx.connection_collation_,
                                             ctx.drop_origin_db_table_name_,
                                             client_request.is_sharding_user()))) {
              LOG_WARN("fail to parse sql", K(sql), K(ret));
            } else if (client_request.is_sharding_user()
                       && OB_FAIL(parse_sql_fileds(client_request, ctx.connection_collation_))){
              LOG_WARN("fail to extract_fileds");
            } else if (OB_FAIL(handle_internal_cmd(client_request))) {
              LOG_WARN("fail to handle internal cmd", K(sql), K(ret));
            }
          }
        } else {
          LOG_DEBUG("skip to parse sql", K(ret), K(sql));
        }
      }
      break;
    }
    case OB_MYSQL_COM_LOGIN: {
      // add login packet's buffer to mysql auth request, for parsing later
      if (OB_FAIL(auth_request.add_auth_request(ctx.reader_, auth_request.get_packet_len()))) {
        LOG_WARN("fail to add auth request", K(ret));
      } else if (OB_FAIL(ObProxyAuthParser::parse_auth(
                  auth_request, ctx.vip_tenant_name_, ctx.vip_cluster_name_))) {
        LOG_WARN("fail to parse auth request", "vip cluster name", ctx.vip_tenant_name_,
                 "vip tenant name", ctx.vip_tenant_name_, K(ret));
      }
      break;
    }
    case OB_MYSQL_COM_STMT_FETCH:
    case OB_MYSQL_COM_STMT_CLOSE:
    case OB_MYSQL_COM_STMT_EXECUTE:
    case OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
    case OB_MYSQL_COM_STMT_GET_PIECE_DATA: {
      // add packet's buffer to mysql common request, for parsing later
      if (OB_FAIL(client_request.add_request(ctx.reader_, ctx.request_buffer_length_))) {
        LOG_WARN("fail to add com request", K(ret));
      }
      break;
    }
    case OB_MYSQL_COM_PING: {
      // proxy handle mysql_ping() by itself
      int64_t len = 0;
      if (NULL == ctx.reader_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid buffer reader", K_(ctx.reader), K(ret));
      } else if (MYSQL_NET_META_LENGTH != (len = ctx.reader_->read_avail())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("buffer reader length is not equal to MYSQL_NET_META_LENGTH", K(len));
        ObMysqlPacketReader reader;
        reader.print_reader(*ctx.reader_);
      }
      break;
    }
    case OB_MYSQL_COM_CHANGE_USER:
    case OB_MYSQL_COM_INIT_DB: {
      if (OB_FAIL(client_request.add_request(ctx.reader_, ctx.request_buffer_length_))) {
        LOG_WARN("fail to add com request", K(ret));
      }
      break;
    }
    case OB_MYSQL_COM_SHUTDOWN:
    case OB_MYSQL_COM_SLEEP:
    case OB_MYSQL_COM_FIELD_LIST:
    case OB_MYSQL_COM_CREATE_DB:
    case OB_MYSQL_COM_DROP_DB:
    case OB_MYSQL_COM_REFRESH:
    case OB_MYSQL_COM_STATISTICS:
    case OB_MYSQL_COM_PROCESS_INFO:
    case OB_MYSQL_COM_CONNECT:
    case OB_MYSQL_COM_PROCESS_KILL:
    case OB_MYSQL_COM_DEBUG:
    case OB_MYSQL_COM_TIME:
    case OB_MYSQL_COM_DELAYED_INSERT:
    case OB_MYSQL_COM_DAEMON: {
      //if it is proxysys tenant, we treat it as error cmd, and response error packet to client
      if (client_request.is_proxysys_tenant()) {
        if (OB_FAIL(handle_internal_cmd(client_request))) {
          LOG_WARN("fail to handle internal cmd", K(sql_cmd), K(ret));
        } else {
          LOG_INFO("this is proxysys user, current cmd was treated as error inter request cmd",
                   K(sql_cmd));
        }
        break;
      }
    }
    case OB_MYSQL_COM_HANDSHAKE:
    case OB_MYSQL_COM_QUIT: {
      break;
    }
    default: {
      break;
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::init_cmd_info(ObProxyMysqlRequest &client_request)
{
  int ret = OB_SUCCESS;

  client_request.set_internal_cmd(true);
  if (OB_LIKELY(NULL == client_request.cmd_info_)) {
    char *cmd_info_buf = NULL;
    if (OB_ISNULL(cmd_info_buf = static_cast<char *>(op_fixed_mem_alloc(static_cast<int64_t>(sizeof(ObInternalCmdInfo)))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for ObInternalCmdInfo", "size", sizeof(ObInternalCmdInfo), K(ret));
    } else if (OB_ISNULL(client_request.cmd_info_ = new (cmd_info_buf) ObInternalCmdInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for ObInternalCmdInfo", K(ret));
      op_fixed_mem_free(cmd_info_buf, static_cast<int64_t>(sizeof(ObInternalCmdInfo)));
      cmd_info_buf = NULL;
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(NULL != client_request.cmd_info_)) {
    ObSqlParseResult &parse_result = client_request.get_parse_result();
    client_request.cmd_info_->set_cmd_type(parse_result.get_stmt_type());
    client_request.cmd_info_->set_sub_cmd_type(parse_result.get_cmd_sub_type());
    client_request.cmd_info_->set_err_cmd_type(parse_result.get_cmd_err_type());
    if (!parse_result.is_internal_error_cmd()) {
      client_request.cmd_info_->set_integers(parse_result.cmd_info_.integer_[0],
                                             parse_result.cmd_info_.integer_[1],
                                             parse_result.cmd_info_.integer_[2]);
      client_request.cmd_info_->deep_copy_strings(parse_result.cmd_info_.string_[0].string_,
                                                  parse_result.cmd_info_.string_[1].string_);
    }
  }

  return ret;
}

int ObMysqlRequestAnalyzer::handle_internal_cmd(ObProxyMysqlRequest &client_request)
{
  int ret = OB_SUCCESS;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  bool is_internal_cmd = false;
  bool is_ping_proxy = false;

  //mysql_compatible_cmd: show processlist, kill [connection | query] id
  if (parse_result.is_mysql_compatible_cmd()) {
    if (parse_result.is_kill_query_cmd() && !parse_result.is_internal_error_cmd()) {
      //kill query need  forward to server, can not treat as internal request
      if (OB_FAIL(client_request.fill_query_info(parse_result.cmd_info_.integer_[0]))) {
        LOG_WARN("fail to fill query info", K(parse_result), K(ret));
      }
    } else {
      is_internal_cmd = true;
    }
  } else {
    //any one can call 'ping proxy' and show proxysession
    //but if parse failed, treat it as internal cmd
    if (parse_result.is_show_session_stmt()) {
      is_internal_cmd = true;
    } else if (parse_result.is_ping_proxy_cmd()) {
      if (parse_result.is_internal_error_cmd()) {
        is_internal_cmd = true;
      } else {
        is_ping_proxy = true;
      }
    } else if (client_request.enable_analyze_internal_cmd()) {
      //root@sys, root@proxysys, metadb user is enable analyze internal cmd
      if (parse_result.is_internal_cmd()) {
        is_internal_cmd =  true;
      }
    } else {
      if (parse_result.is_kill_session_cmd()) {
        //if disable analyze internal cmd, we need treat kill proxysession as error kill cmd
        parse_result.set_err_stmt_type(OBPROXY_T_ERR_PARSE);
        is_internal_cmd = true;
      }
    }
  }

  if (client_request.is_proxysys_user()) {
    // allow follow type to support java connect
    if (parse_result.is_show_stmt() || parse_result.is_set_stmt() || parse_result.is_select_stmt()
      || parse_result.is_set_names_stmt()
      || parse_result.is_update_stmt()) {
      is_internal_cmd = true;
    }
  }
  //proxysys MUST && ONLY handle internal request
  if ((client_request.is_proxysys_user() && !is_internal_cmd && !is_ping_proxy)
      || (client_request.is_inspector_user() && !is_ping_proxy)) {
    //proxy need resp ok for SET, BEGIN when this is proxysys user
    if (parse_result.is_need_resp_ok_cmd()) {
      // python mysql will send 'SET autocommit=0' after connected, proxysys need resp ok packet
      LOG_INFO("proxysys tenant do not support this cmd, return ok packet later",
               "sql", client_request.get_sql(), K(parse_result), K(ret));
      parse_result.set_err_stmt_type(OBPROXY_T_ERR_NEED_RESP_OK);
    } else {
      parse_result.set_err_stmt_type(OBPROXY_T_ERR_NOT_SUPPORTED);
    }
    is_internal_cmd = true;
  } else if (client_request.is_sharding_user() && parse_result.is_show_topology_stmt()) {
    is_internal_cmd = true;
  }


  if (OB_SUCC(ret) && is_internal_cmd) {
    ret = init_cmd_info(client_request);
  }
  return ret;
}

void  ObMysqlRequestAnalyzer::mysql_hex_dump(const void *data, const int64_t size)
{
  if ((NULL != data) && (size > 0)) {
    const int64_t MAX_DUMP_SIZE = 1024;
    int64_t dump_size = std::min(size, MAX_DUMP_SIZE);
    hex_dump(data, static_cast<int32_t>(dump_size), true, OB_LOG_LEVEL_WARN);
  }
}

int ObMysqlRequestAnalyzer::rewrite_part_key_comment(ObIOBufferReader *reader,
                                                     ObProxyMysqlRequest &client_request)
{
  int ret = OB_SUCCESS;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  if (parse_result.has_simple_route_info()
      && parse_result.is_simple_route_info_valid()) {
    const ObProxySimpleRouteInfo &info = parse_result.route_info_;
    int64_t route_info_len = std::max(info.table_len_ + info.table_offset_, info.part_key_len_ + info.part_key_offset_);
    // start pos and end pos is relative to packet payload, meta bytes are not included
    if (OB_ISNULL(reader)
       || OB_UNLIKELY(MYSQL_NET_META_LENGTH + route_info_len > reader->read_avail())) {
      // obproxy_simple_route_info_hint must be less than read_avail() - MYSQL_NET_META_LENGTH
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid buffer reader", K(route_info_len), K(reader), K(ret));
    } else {
      // rewrite obproxy_simple_route_info with '*'
      reader->replace_with_char(OBPROXY_SIMPLE_PART_KEY_MARK, info.table_len_, MYSQL_NET_META_LENGTH + info.table_offset_);
      reader->replace_with_char(OBPROXY_SIMPLE_PART_KEY_MARK, info.part_key_len_, MYSQL_NET_META_LENGTH + info.part_key_offset_);
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::do_analyze_execute_param(const char *buf,
                                                     int64_t data_len,
                                                     const int64_t param_num,
                                                     ObIArray<EMySQLFieldType> &param_types,
                                                     ObProxyMysqlRequest &client_request,
                                                     const int64_t target_index,
                                                     ObObj &target_obj)
{
  int ret = OB_SUCCESS;

  int8_t new_param_bound_flag = 0;
  ObIAllocator &allocator = client_request.get_param_allocator();

  int64_t bitmap_types = (param_num + 7) /8;
  const char *bitmap = buf;
  buf += bitmap_types;
  data_len -= bitmap_types;
  if (OB_FAIL(ObMysqlPacketUtil::get_int1(buf, data_len, new_param_bound_flag))) {
    LOG_WARN("fail to get int1", K(data_len), K(ret));
  } else if (1 == new_param_bound_flag) {
    param_types.reset();
  }
  uint8_t type = 0;
  int8_t flag = 0;
  ObObjType ob_type;
  bool is_null = false;
  ObCharsetType charset = ObCharset::get_default_charset();
  // decode all types
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    if (1 == new_param_bound_flag) {
      if (OB_FAIL(ObMysqlPacketUtil::get_uint1(buf, data_len, type))) {
        LOG_WARN("fail to get uint1", K(data_len), K(ret));
      } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(buf, data_len, flag))) {
        LOG_WARN("fail to get int1", K(data_len), K(ret));
      } else if (OB_FAIL(param_types.push_back(static_cast<EMySQLFieldType>(type)))) {
        LOG_WARN("fail to push back param type", K(type), K(ret));
      }
    } else {
      if (param_num != param_types.count()) {
        ret = OB_ERR_WRONG_DYNAMIC_PARAM;
        LOG_WARN("wrong param num and param_types num", K(param_num), K(param_types), K(ret));
      } else {
        type = static_cast<uint8_t>(param_types.at(i));
      }
    }
    if (OB_SUCC(ret) && OB_MYSQL_TYPE_COMPLEX == type) {
      TypeInfo type_name_info;
      uint8_t elem_type = 0;
      // skip all complex type bytes
      if (OB_FAIL(decode_type_info(buf, data_len, type_name_info))) {
        LOG_WARN("failed to decode type info", K(ret));
      } else if (type_name_info.type_name_.empty()) {
        type_name_info.is_elem_type_ = true;
        if (OB_FAIL(ObMysqlPacketUtil::get_uint1(buf, data_len, elem_type))) {
          LOG_WARN("fail to get uint1", K(data_len), K(ret));
        } else if (OB_FAIL(ObSMUtils::get_ob_type(type_name_info.elem_type_, static_cast<EMySQLFieldType>(elem_type)))) {
          LOG_WARN("cast ob type from mysql type failed", K(type_name_info.elem_type_), K(elem_type), K(ret));
        } else if (OB_MYSQL_TYPE_COMPLEX == elem_type
            && OB_FAIL(decode_type_info(buf, data_len, type_name_info))) {
          LOG_WARN("failed to decode type info", K(ret));
        }
      }
    } // end complex type
  }
  const char *param_buf = buf;
  // decode all values
  for (int64_t i = 0; OB_SUCC(ret) && i <= target_index; ++i) {
    target_obj.reset();
    type = param_types.at(i);
    if (OB_FAIL(ObSMUtils::get_ob_type(ob_type, static_cast<EMySQLFieldType>(type)))) {
      LOG_DEBUG("fail to cast mysql type to ob type, will add param with null", K(i), K(type), K(ret));
    } else {
      target_obj.set_type(ob_type);
      is_null = ObSMUtils::update_from_bitmap(target_obj, bitmap, i);
      if (is_null) {
        LOG_DEBUG("param is null", K(i), K(ob_type));
      } else if (OB_FAIL(parse_param_value(allocator, param_buf, data_len, type, charset, target_obj))) {
        LOG_DEBUG("fail to parse param value", K(i), K(ret));
      } else {
        LOG_DEBUG("succ to parse execute  param", K(ob_type), K(i));
      }
    }
  } // end for

  if (OB_FAIL(ret)) {
    LOG_DEBUG("fail to decode execute param or type", K(ret));
  }
  return ret;
}

int ObMysqlRequestAnalyzer::analyze_execute_param(const int64_t param_num,
                                                  ObIArray<EMySQLFieldType> &param_types,
                                                  ObProxyMysqlRequest &client_request,
                                                  const int64_t target_index,
                                                  ObObj &target_obj)
{
  int ret = OB_SUCCESS;
  ObString data = client_request.get_req_pkt();
  int64_t data_len = data.length();
  if (OB_UNLIKELY(param_num <= target_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param_num), K(target_index), K(ret));
  } else if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *buf = data.ptr() + MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH;
    data_len -= (MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH);
    if (param_num > 0) {
      if (OB_FAIL(do_analyze_execute_param(buf, data_len, param_num, param_types,
                                           client_request, target_index, target_obj))) {
        LOG_DEBUG("fail to do analyze execute param", K(ret));
      }
    } //  end if param_num > 0
  }
  return ret;
}

int ObMysqlRequestAnalyzer::analyze_prepare_execute_param(ObProxyMysqlRequest &client_request,
                                                          const int64_t target_index,
                                                          ObObj &target_obj)
{
  int ret = OB_SUCCESS;
  ObString data = client_request.get_req_pkt();
  int64_t data_len = data.length();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *buf = data.ptr() + MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH;
    data_len -= (MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH);
    uint64_t query_len = 0;
    int32_t param_num = 0;
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, data_len, query_len))) {
      LOG_WARN("failed to get length", K(data_len), K(ret));
    } else {
      buf += query_len;
      data_len -= query_len;
      if (OB_FAIL(ObMysqlPacketUtil::get_int4(buf, data_len, param_num))) {
        LOG_WARN("fail to get int4", K(data_len), K(ret));
      } else if (OB_UNLIKELY(param_num <= target_index)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(param_num), K(target_index), K(ret));
      }
    }

    if (OB_SUCC(ret) && param_num > 0) {
      common::ObArray<obmysql::EMySQLFieldType> param_types;
      if (OB_FAIL(do_analyze_execute_param(buf, data_len, param_num, param_types,
                                           client_request, target_index, target_obj))) {
        LOG_DEBUG("fail to do analyze execute param", K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlRequestAnalyzer::parse_param_value(ObIAllocator &allocator, 
                                              const char *&data, int64_t &buf_len, const uint8_t type,
                                              const ObCharsetType charset, ObObj &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(data), K(ret));
  } else {
    ObCollationType cs_type = ObCharset::get_default_collation(charset);
    switch (type) {
      case OB_MYSQL_TYPE_TINY: {
        int8_t value;
        ret = ObMysqlPacketUtil::get_int1(data, buf_len, value);
        param.set_int(value);
        break;
      }
      case OB_MYSQL_TYPE_SHORT: {
        int16_t value = 0;
        ret = ObMysqlPacketUtil::get_int2(data, buf_len, value);
        param.set_int(value);
        break;
      }
      case OB_MYSQL_TYPE_LONG: {
        int32_t value = 0;
        ret = ObMysqlPacketUtil::get_int4(data, buf_len, value);
        param.set_int(value);
        break;
      }
      case OB_MYSQL_TYPE_LONGLONG: {
        int64_t value = 0;
        ret = ObMysqlPacketUtil::get_int8(data, buf_len, value);
        param.set_int(value);
        break;
      }
      case OB_MYSQL_TYPE_FLOAT: {
        float value = 0;
        ret = ObMysqlPacketUtil::get_float(data, buf_len, value);
        param.set_float(value);
        break;
      }
      case OB_MYSQL_TYPE_DOUBLE: {
        double value = 0;
        ret = ObMysqlPacketUtil::get_double(data, buf_len, value);
        param.set_double(value);
        break;
      }
      case OB_MYSQL_TYPE_YEAR: {
        int16_t value = 0;
        ret = ObMysqlPacketUtil::get_int2(data, buf_len, value);
        param.set_year(static_cast<uint8_t>(value));
        break;
      }
      case OB_MYSQL_TYPE_DATE:
      case OB_MYSQL_TYPE_DATETIME:
      case OB_MYSQL_TYPE_TIMESTAMP: {
        if (OB_FAIL(parse_mysql_timestamp_value(static_cast<EMySQLFieldType>(type), data, buf_len, param))) {
          LOG_WARN("parse timestamp value from client failed", K(ret));
        }
        break;
      }
      case OB_MYSQL_TYPE_TIME:{
        if (OB_FAIL(parse_mysql_time_value(data, buf_len, param))) {
          LOG_WARN("parse timestamp value from client failed", K(ret));
        }
        break;
      }
      case OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      case OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case OB_MYSQL_TYPE_OB_TIMESTAMP_NANO: {
        // skip oracle timestamp length
        int8_t total_len = 0;
        ret = ObMysqlPacketUtil::get_int1(data, buf_len, total_len);
        data += total_len;
        buf_len -= total_len;
        break;
      }
      case OB_MYSQL_TYPE_OB_RAW:
      case OB_MYSQL_TYPE_STRING:
      case OB_MYSQL_TYPE_VARCHAR:
      case OB_MYSQL_TYPE_VAR_STRING:
      case OB_MYSQL_TYPE_NEWDECIMAL: {
        ObString str;
        ObString dst;
        uint64_t length = 0;
        if (OB_FAIL(ObMysqlPacketUtil::get_length(data, buf_len, length))) {
          LOG_ERROR("decode varchar param value failed", K(buf_len), K(ret));
        } else if (buf_len < length) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("data buf size is not enough", K(length), K(buf_len), K(ret));
        } else {
          str.assign_ptr(data, static_cast<ObString::obstr_size_t>(length));
          if (OB_FAIL(ob_write_string(allocator, str, dst))) {
            LOG_WARN("Failed to write str", K(ret));
          } else {
            if (OB_MYSQL_TYPE_NEWDECIMAL == type) {
              number::ObNumber nb;
              if (OB_FAIL(nb.from(str.ptr(), length, allocator))) {
                LOG_WARN("decode varchar param to number failed", K(ret));
              } else {
                param.set_number(nb);
              }
            } else {
              param.set_collation_type(cs_type);
              param.set_varchar(dst);
            }
            data += length;
            buf_len -= length;
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_ILLEGAL_TYPE;
        LOG_DEBUG("illegal mysql type, we will set param with null", K(type), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::parse_mysql_timestamp_value(const EMySQLFieldType field_type,
                                                        const char *&data, int64_t &buf_len,
                                                        ObObj &param)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  int16_t year = 0;
  int8_t month = 0;
  int8_t day = 0;
  int8_t hour = 0;
  int8_t min = 0;
  int8_t second = 0;
  int32_t microsecond = 0;
  ObPreciseDateTime value;
  if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, length))) {
    LOG_WARN("fail to get int1", K(ret));
  } else if (0 == length) {
    value = 0;
  } else if (4 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int2(data, buf_len, year))) {
      LOG_WARN("fail to get year", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, month))) {
      LOG_WARN("fail to get month", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, day))) {
      LOG_WARN("fail to get day", K(ret));
    }
  } else if (7 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int2(data, buf_len, year))) {
      LOG_WARN("fail to get year", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, month))) {
      LOG_WARN("fail to get month", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, day))) {
      LOG_WARN("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WARN("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WARN("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WARN("fail to get second", K(ret));
    }
  } else if (11 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int2(data, buf_len, year))) {
      LOG_WARN("fail to get year", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, month))) {
      LOG_WARN("fail to get month", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, day))) {
      LOG_WARN("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WARN("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WARN("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WARN("fail to get second", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, microsecond))) {
      LOG_WARN("fail to get microsecond", K(ret));
    }
  } else {
    ret = OB_ERROR;
    LOG_WARN("invalid mysql timestamp value length", K(length));
  }

  if (OB_SUCC(ret)) {
    ObTime ob_time;
    if (0 != length) {
      ob_time.parts_[DT_YEAR] = year;
      ob_time.parts_[DT_MON] = month;
      ob_time.parts_[DT_MDAY] = day;
      ob_time.parts_[DT_HOUR] = hour;
      ob_time.parts_[DT_MIN] = min;
      ob_time.parts_[DT_SEC] = second;
      ob_time.parts_[DT_USEC] = microsecond;
      if (!ObTimeUtility::is_valid_date(year, month, day)
          || !ObTimeUtility::is_valid_time(hour, min, second, microsecond)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
        if (field_type == OB_MYSQL_TYPE_DATE) {
          value = ob_time.parts_[DT_DATE];
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx.tz_info_, value))){
          LOG_WARN("convert obtime to datetime failed", K(value), K(year), K(month),
                   K(day), K(hour), K(min), K(second));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (field_type == OB_MYSQL_TYPE_TIMESTAMP) {
      param.set_timestamp(value);
    } else if (field_type == OB_MYSQL_TYPE_DATETIME) {
      param.set_datetime(value);
    } else if (field_type == OB_MYSQL_TYPE_DATE) {
      param.set_date(static_cast<int32_t>(value));
    }
  }
  LOG_DEBUG("get datetime", K(length), K(year), K(month), K(day), K(hour), K(min),K(second),  K(microsecond), K(value));
  return ret;
}

int ObMysqlRequestAnalyzer::parse_mysql_time_value(const char *&data, int64_t &buf_len, ObObj &param)
{
  int ret = OB_SUCCESS;
  int8_t length = 0;
  int8_t is_negative = 0;
  int16_t year = 0;
  int8_t month = 0;
  int32_t day = 0;
  int8_t hour = 0;
  int8_t min = 0;
  int8_t second = 0;
  int32_t microsecond = 0;
  struct tm tmval;
  MEMSET(&tmval, 0, sizeof(tmval));
  int64_t value;
  if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, length))) {
    LOG_WARN("fail to get int1", K(ret));
  } else if (0 == length) {
    value = 0;
  } else if (8 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, is_negative))) {
      LOG_WARN("fail to get is_negative", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, day))) {
      LOG_WARN("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WARN("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WARN("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WARN("fail to get second", K(ret));
    }
  } else if (12 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, is_negative))) {
      LOG_WARN("fail to get is_negative", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, day))) {
      LOG_WARN("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WARN("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WARN("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WARN("fail to get second", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, microsecond))) {
      LOG_WARN("fail to get microsecond", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected time length", K(length), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObTime ob_time;
    if (0 != length) {
      ob_time.parts_[DT_YEAR] = year;
      ob_time.parts_[DT_MON] = month;
      ob_time.parts_[DT_MDAY] = day;
      ob_time.parts_[DT_HOUR] = hour;
      ob_time.parts_[DT_MIN] = min;
      ob_time.parts_[DT_SEC] = second;
      ob_time.parts_[DT_USEC] = microsecond;
      if (!ObTimeUtility::is_valid_time(hour, min, second, microsecond)) {
        ret = OB_INVALID_DATE_FORMAT;
        LOG_WARN("invalid date format", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
        value = ObTimeConverter::ob_time_to_time(ob_time);
      }
    }
  }
  if (OB_SUCC(ret)) {
    param.set_time(value);
  }
  LOG_INFO("get time", K(length), K(year), K(month), K(day), K(hour), K(min),K(second),  K(microsecond), K(value));
  return ret;
}

int ObMysqlRequestAnalyzer::decode_type_info(const char*& buf, int64_t &buf_len, TypeInfo &type_info)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, buf_len, length))) {
      LOG_WARN("failed to get length", K(ret));
    } else if (buf_len < length) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("packet buf size is not enough", K(buf_len), K(length), K(ret));
    } else {
      type_info.relation_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
      buf += length;
      buf_len -= length;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, buf_len, length))) {
      LOG_WARN("failed to get length", K(ret));
    } else if (buf_len < length) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("packet buf size is not enough", K(buf_len), K(length), K(ret));
    } else {
      type_info.type_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
      buf += length;
      buf_len -= length;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t version = 0;
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, buf_len, version))) {
      LOG_WARN("failed to get version", K(ret));
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::analyze_sql_id(ObProxyMysqlRequest &client_request, ObString &sql_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator))) {
    LOG_WARN("fail to get parse allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    ParseResult parse_result;
    memset(&parse_result, 0, sizeof(parse_result));
    parse_result.is_fp_ = true;
    parse_result.is_multi_query_ = false;
    parse_result.malloc_pool_ = reinterpret_cast<void *>(allocator);
    parse_result.is_ignore_hint_ = false;
    parse_result.need_parameterize_ = true;
    parse_result.pl_parse_info_.is_pl_parse_ = false;
    parse_result.minus_ctx_.has_minus_ = false;
    parse_result.minus_ctx_.pos_ = -1;
    parse_result.minus_ctx_.raw_sql_offset_ = -1;
    parse_result.is_for_trigger_ = false;
    parse_result.is_dynamic_sql_ = false;
    parse_result.is_batched_multi_enabled_split_ = false;

    const ObString sql = client_request.get_sql();
    int64_t new_length = sql.length() + 1; // needed by sql parser, terminated with '\0'
    char *buf = (char *)parse_malloc(new_length, parse_result.malloc_pool_);

    parse_result.param_nodes_ = NULL;
    parse_result.tail_param_node_ = NULL;
    parse_result.no_param_sql_ = buf;
    parse_result.no_param_sql_buf_len_ = static_cast<int>(new_length);

    // sql_id is a fixed length string with 32 bytes
    // sql_id_buf length is fixed 33, terminated with '\0'
    char *sql_id_buf = client_request.get_sql_id_buf();
    int64_t sql_id_buf_len = client_request.get_sql_id_buf_len();
    ObSQLParser sql_parser(*(ObIAllocator *)(parse_result.malloc_pool_),
                           FP_MODE);
    if (OB_FAIL(sql_parser.parse_and_gen_sqlid(allocator, sql.ptr(), sql.length(), sql_id_buf_len, sql_id_buf))) {
      LOG_WARN("fail to do parse_and_gen_sqlid", K(sql), K(ret));
      sql_id_buf[0] = '\0';
    } else {
      sql_id_buf[sql_id_buf_len - 1] = '\0';
      sql_id.assign_ptr(sql_id_buf, static_cast<int32_t>(sql_id_buf_len - 1));
      LOG_DEBUG("succ to get sql id", K(sql_id), K(sql));
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
