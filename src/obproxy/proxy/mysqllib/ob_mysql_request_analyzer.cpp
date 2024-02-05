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
#include "rpc/obmysql/ob_mysql_util.h"
#include "obutils/ob_cached_variables.h"
#include "common/obsm_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/charset/ob_charset.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "obproxy/obutils/ob_proxy_sql_parser.h"
#include "proxy/shard/obproxy_shard_utils.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
#include "lib/ptr/ob_ptr.h"
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

ObMysqlRequestAnalyzer::ObMysqlRequestAnalyzer(const ObMysqlRequestAnalyzer& analyzer) {
  total_packet_length_ = analyzer.total_packet_length_;
  payload_len_ = analyzer.payload_len_;
  packet_seq_ = analyzer.packet_seq_;
  cmd_ = analyzer.cmd_;
  nbytes_analyze_ = analyzer.nbytes_analyze_;
  is_last_request_packet_ = analyzer.is_last_request_packet_;
  request_count_ = analyzer.request_count_;
  MEMCPY(header_length_buffer_, analyzer.header_length_buffer_, MYSQL_NET_META_LENGTH);
  header_content_offset_ = analyzer.header_content_offset_;
}

ObMysqlRequestAnalyzer &ObMysqlRequestAnalyzer::operator=(const ObMysqlRequestAnalyzer &analyzer) {
  if (this != &analyzer) {
    total_packet_length_ = analyzer.total_packet_length_;
    payload_len_ = analyzer.payload_len_;
    packet_seq_ = analyzer.packet_seq_;
    cmd_ = analyzer.cmd_;
    nbytes_analyze_ = analyzer.nbytes_analyze_;
    is_last_request_packet_ = analyzer.is_last_request_packet_;
    request_count_ = analyzer.request_count_;
    MEMCPY(header_length_buffer_, analyzer.header_length_buffer_, MYSQL_NET_META_LENGTH);
    header_content_offset_ = analyzer.header_content_offset_;
  }
  return *this;
}

void ObMysqlRequestAnalyzer::analyze_request(const ObRequestAnalyzeCtx &ctx,
                                             ObMysqlAuthRequest &auth_request,
                                             ObProxyMysqlRequest &client_request,
                                             ObMySQLCmd &sql_cmd,
                                             ObMysqlAnalyzeStatus &status,
                                             const bool is_oracle_mode /* false */,
                                             const bool is_client_support_ob20_protocol /* false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.reader_)) {
    ret = OB_INVALID_ARGUMENT;
    status = ANALYZE_ERROR;
    LOG_WDIAG("invalid argument", "reader", ctx.reader_, K(ret), K(status));
  } else {
    // 1. determine whether mysql request packet is received complete
    ObMysqlAnalyzeResult result;
    if (OB_UNLIKELY(ctx.is_auth_)) {
      if (OB_FAIL(handle_auth_request(*ctx.reader_, result))) {
        LOG_WDIAG("fail to handle auth request", K(ret));
      }
    // for content of file packet ignore the cmd/pkt type
    } else if (OB_UNLIKELY(OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == sql_cmd)) {
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet_only_header(*ctx.reader_, result))) {
        LOG_WDIAG("fail to analyze one packet only header", K(ret), "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(sql_cmd));
      }
    } else {
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*ctx.reader_, result))) {
        LOG_WDIAG("fail to analyze one packet", K(ret));
      }
    }

    // 2. verify request's legality, just print WARN
    int64_t avail_bytes = ctx.reader_->read_avail();
    if (avail_bytes >= MYSQL_NET_META_LENGTH) {
      if (is_client_support_ob20_protocol) {
        avail_bytes -= OB20_PROTOCOL_TAILER_LENGTH;   // avoid to print too much log in ob2.0 protocol with client
      }
      if (avail_bytes > result.meta_.pkt_len_) {
        LOG_DEBUG("recevied more than one mysql packet at once, it is unexpected so far",
                 "first packet len(include packet header)", result.meta_.pkt_len_,
                 "total len received", avail_bytes, K(ctx.is_auth_));
      } else if (result.meta_.cmd_ < OB_MYSQL_COM_SLEEP || result.meta_.cmd_ >= OB_MYSQL_COM_MAX_NUM) {
        LOG_WDIAG("unknown mysql cmd", "cmd", result.meta_.cmd_, K(avail_bytes),
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

    if (OB_SUCC(ret) && (ANALYZE_DONE == status || ANALYZE_CONT == status)) {
      // 3. set mysql request packet meta
      sql_cmd = result.meta_.cmd_;
      if (OB_UNLIKELY(OB_MYSQL_COM_LOGIN == result.meta_.cmd_ || OB_MYSQL_COM_HANDSHAKE == result.meta_.cmd_)) {
        // add pkt meta to mysql auth request
        auth_request.set_packet_meta(result.meta_);
      } else {
        // add pkt meta to mysql common request
        // maybe we has not receive all data to get meta,
        // in this case, just set a empty packet meta, no bad affect.
        client_request.set_packet_meta(result.meta_);
      }

      // 4. dispatch mysql packet by cmd type, and parse mysql request
      if (OB_LIKELY(ANALYZE_DONE == status)) {
        if (OB_FAIL(do_analyze_request(ctx, sql_cmd, auth_request, client_request, is_oracle_mode))) {
          if (OB_ERR_PARSE_SQL == ret) {
            //ob parse fail will not disconnect
            status = ANALYZE_OBPARSE_ERROR;
          } else {
            status = ANALYZE_ERROR;
          }
          LOG_WDIAG("fail to dispatch mysql cmd", "analyze status",
                   ObProxyParserUtils::get_analyze_status_name(status), K(ret));
        }
      } else if (ANALYZE_CONT == status) {
        // we will analyze large request if we received enough packet(> request_buffer_len_)
        if (!ctx.is_auth_
            && ctx.large_request_threshold_len_ > 0
            && result.meta_.pkt_len_ > ctx.large_request_threshold_len_
            && avail_bytes + ObProxyMysqlRequest::PARSE_EXTRA_CHAR_NUM > ctx.request_buffer_length_
            && !client_request.is_sharding_user()
            && !client_request.is_proxysys_user()
            && !ctx.using_ldg_
            && OB_MYSQL_COM_STMT_SEND_LONG_DATA != sql_cmd
            && OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT != sql_cmd
            && OB_MYSQL_COM_CHANGE_USER != sql_cmd) {
          if (OB_FAIL(do_analyze_request(ctx, sql_cmd, auth_request, client_request, is_oracle_mode))) {
            status = ANALYZE_ERROR;
            LOG_WDIAG("fail to dispatch mysql cmd", "analyze status",
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

// get len and seq from buffer
int ObMysqlRequestAnalyzer::get_payload_length(const char *buffer)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("buffer is NULL", K(ret));
  } else {
    payload_len_ = uint3korr(buffer);
    packet_seq_ = uint1korr(buffer + MYSQL_PAYLOAD_LENGTH_LENGTH);
    total_packet_length_ += payload_len_ + MYSQL_NET_HEADER_LENGTH;
  }
  return ret;
}

int ObMysqlRequestAnalyzer::check_is_last_request_packet(obmysql::ObMySQLCmd cmd)
{
  int ret = OB_SUCCESS;
  switch (cmd) {
    case obmysql::OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT:
      if (payload_len_ == 0) {
        is_last_request_packet_ = true;
        LOG_DEBUG("finish to check load data local infile request packet",
                  "last_payload_len", payload_len_, "last_packet_seq", packet_seq_);
      } else {
        is_last_request_packet_ = false;
        LOG_DEBUG("continue to check load data local infile request packet",
                  "payload_len", payload_len_, "packet_seq", packet_seq_);
      }
      break;
    default:
      if (MYSQL_PACKET_MAX_LENGTH == payload_len_) {
        is_last_request_packet_ = false;
      } else {
        ++request_count_;
        if (request_count_ > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("received two mysql packet, unexpected", K(total_packet_length_), K(payload_len_),
                   K(packet_seq_), K(request_count_), K(ret));
        } else {
          is_last_request_packet_ = true;
        }
      }
      LOG_DEBUG("payload length is ", K(payload_len_));
      break;
  }

  return ret;
}

int ObMysqlRequestAnalyzer::is_request_finished(
    const ObRequestBuffer &buff,
    bool &is_finish,
    obmysql::ObMySQLCmd cmd,
    ObProtocolDiagnosis *protocol_diagnosis)
{
  int ret = OB_SUCCESS;
  int64_t length = buff.length();
  const char *data = buff.ptr();

  bool found_next_payload_length = true;
  int64_t i = 0;

  int64_t header_len = 0;
  if (OB_LIKELY(OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT != cmd)) {
    header_len = MYSQL_NET_META_LENGTH;
  } else {
    header_len = MYSQL_NET_HEADER_LENGTH;
  }

  if (0 != header_content_offset_) { // part of header length found in previous buffer
    int64_t need_length = header_len - header_content_offset_;
    if (length < need_length) {
      // this buffer isn't enough to hold all rest payload length,
      // the rest will be found in next buffer
      found_next_payload_length = false;
      for (i = 0; i < length; ++i, ++header_content_offset_) {
        header_length_buffer_[header_content_offset_] = *(data + i);
      }
    } else {
      for (i = 0; i < need_length; ++i, ++header_content_offset_) {
        header_length_buffer_[header_content_offset_] = *(data + i);
      }
      if (OB_FAIL(get_payload_length(header_length_buffer_))) {
        LOG_WDIAG("fail to get_payload_length", K(ret));
      } else if (OB_LIKELY(MYSQL_NET_META_LENGTH == header_len)  &&
                 OB_FALSE_IT(cmd_ = uint1korr(header_length_buffer_ + MYSQL_NET_HEADER_LENGTH))) {
      } else if (OB_FAIL(check_is_last_request_packet(cmd))) {
        LOG_WDIAG("fail to check_is_last_request_packet", K(ret), K(cmd));
      } else {
        PROTOCOL_DIAGNOSIS(SINGLE_MYSQL, send, protocol_diagnosis, static_cast<int32_t>(payload_len_), packet_seq_, cmd_);
        MEMSET(header_length_buffer_, 0, sizeof(header_length_buffer_));
        payload_len_ = -1;
        cmd_ = 0;
        header_content_offset_ = 0;
      }
    }
  }

  if (OB_SUCC(ret) && found_next_payload_length) {
    // it is possible that more than one mysql packet is in buff
    while (total_packet_length_ < length + nbytes_analyze_) {
      // found new payload length
      int64_t next_packet_start_offset = total_packet_length_ - nbytes_analyze_;
      int64_t left_length = length - next_packet_start_offset;
      if (left_length < header_len) {
        // this buffer isn't enough to hold all payload length,
        // the rest will be found in next buffer
        for (i = 0; i < left_length; ++i, ++header_content_offset_) {
          header_length_buffer_[header_content_offset_] = *(data + i + next_packet_start_offset);
        }
        break;
      } else if (OB_FAIL(get_payload_length(data + next_packet_start_offset))) {
        mysql_hex_dump(data + next_packet_start_offset, left_length);
        LOG_WDIAG("fail to get_payload_length", K(ret));
      } else if (OB_LIKELY(MYSQL_NET_META_LENGTH == header_len) &&
                 OB_FALSE_IT(cmd_ = uint1korr(data + next_packet_start_offset + MYSQL_NET_HEADER_LENGTH))) {
      } else if (OB_FAIL(check_is_last_request_packet(cmd))) {
        LOG_WDIAG("fail to check_is_last_request_packet", K(ret), K(cmd));
      } else {
        PROTOCOL_DIAGNOSIS(SINGLE_MYSQL, send, protocol_diagnosis, static_cast<int32_t>(payload_len_), packet_seq_, cmd_);
        payload_len_ = -1;
        cmd_ = 0;
      }
    }
  }

  if (OB_SUCC(ret)) {
    nbytes_analyze_ += length;
    if (total_packet_length_ == nbytes_analyze_ && is_last_request_packet_) {
      is_finish = true;
    } else {
      is_finish = false;
    }
  }
  LOG_DEBUG("analyze_request_is_finish", K(length), K(nbytes_analyze_), K(total_packet_length_),
                                         K(is_last_request_packet_), K(is_finish));
  return ret;
}

int ObMysqlRequestAnalyzer::is_request_finished(
  event::ObIOBufferReader &reader,
  bool &is_finish,
  obmysql::ObMySQLCmd cmd,
  int64_t analyze_len,
  ObProtocolDiagnosis *protocol_diagnosis)
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
    data_size = std::min(block->read_avail() - offset, analyze_len);
  }

  if (data_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  }

  ObRequestBuffer buffer;
  while (OB_SUCC(ret) && NULL != block && data_size > 0 && !is_finish) {
    buffer.assign_ptr(data, static_cast<int32_t>(data_size));
    analyze_len -= data_size;
    if (OB_FAIL(is_request_finished(buffer, is_finish, cmd, protocol_diagnosis))) {
      LOG_WDIAG("fail to do is_request_finished", K(ret), K(cmd));
    } else {
      // on to the next block
      offset = 0;
      block = block->next_;
      if (NULL != block) {
        data = block->start();
        data_size = std::min(block->read_avail(), analyze_len);
        if (is_finish && (data_size > 0)) {
          mysql_hex_dump(data, data_size);
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("receive two request, unexpected", K(is_finish), K(data_size), K(ret));
        }
      }
    }
  }

  LOG_DEBUG("analyze_request_is_finish", "data_size", reader.read_avail(), K(is_finish), K(analyze_len));

  return ret;
}

void ObMysqlRequestAnalyzer::reset()
{
  reuse();
}
void ObMysqlRequestAnalyzer::reuse()
{
  total_packet_length_ = 0;
  payload_len_ = -1;
  packet_seq_ = 0;
  cmd_ = 0;
  nbytes_analyze_ = 0;
  is_last_request_packet_ = true;
  MEMSET(header_length_buffer_, 0, MYSQL_NET_HEADER_LENGTH);
  header_content_offset_ = 0;
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
      LOG_WDIAG("fail to analyze one packet", K(ret));
    } else {
      result.meta_.cmd_ = OB_MYSQL_COM_LOGIN;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("it shoulde not happened", K(ret));
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
  int ret = OB_SUCCESS;
  int64_t total_num = result.all_relation_info_.relation_num_;
  ObProxyRelationExpr* relation_expr = NULL;
  ObString name_str;
  ObString value_str;
  for (int64_t i = 0; i < total_num; i ++) {
    relation_expr = result.all_relation_info_.relations_[i];
    if (OB_ISNULL(relation_expr)) {
      LOG_WDIAG("Got an empty relation_expr", K(i));
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
        LOG_WDIAG("get an empty column_name_");
      }
    } else {
      LOG_WDIAG("left value is null");
    }
    if (relation_expr->right_value_ != NULL) {
      ObProxyTokenNode *token = relation_expr->right_value_->head_;
      SqlField* field = NULL;
      if (OB_FAIL(SqlField::alloc_sql_field(field))) {
        LOG_WDIAG("fail to alloc sql field", K(ret));
      } else {
        field->column_name_.set_value(name_str.length(), name_str.ptr());
        SqlColumnValue column_value;
        while (OB_SUCC(ret) && NULL != token) {
          switch(token->type_) {
            case TOKEN_INT_VAL:
              column_value.value_type_ = TOKEN_INT_VAL;
              column_value.column_int_value_ = token->int_value_;
              column_value.column_value_.set_integer(token->int_value_);
              if (OB_FAIL(field->column_values_.push_back(column_value))) {
                LOG_WDIAG("fail to push column_value", K(ret));
              }
              break;
            case TOKEN_STR_VAL:
              column_value.value_type_ = TOKEN_STR_VAL;
              value_str.assign_ptr(token->str_value_.str_,
                  token->str_value_.str_len_);
              column_value.column_value_.set_value(value_str);
              if (OB_FAIL(field->column_values_.push_back(column_value))) {
                LOG_WDIAG("fail to push back column_value", K(ret));
              }
              break;
            default:
              LOG_DEBUG("invalid token type", "token type", get_obproxy_token_type(token->type_));
              break;
          }
          token = token->next_;
        }
        if (OB_SUCC(ret) && field->is_valid() && OB_SUCCESS == sql_result.fields_.push_back(field)) {
          ++sql_result.field_num_;
        } else {
          field->reset();
          field = NULL;
        }
      }
    } else {
      LOG_WDIAG("right value is null");
    }
  }
}

int ObMysqlRequestAnalyzer::parse_sql_fileds(ObProxyMysqlRequest &client_request,
                                             ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;
  bool need_parse_fields = true;
  ObExprParseMode parse_mode = INVALID_PARSE_MODE;
  ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
  bool need_scan_all = ObProxyShardUtils::need_scan_all(sql_parse_result);
  if (need_scan_all) {
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
      LOG_WDIAG("fail to get parse allocator", K(ret));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("allocator is null", K(ret));
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
        LOG_WDIAG("parse failed", K(expr_sql));
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
    case OB_MYSQL_COM_QUERY:
    case OB_MYSQL_COM_STMT_PREPARE_EXECUTE:
    case OB_MYSQL_COM_STMT_PREPARE: {
      // add packet's buffer to mysql common request, for parsing later
      if (OB_FAIL(client_request.add_request(ctx.reader_, ctx.request_buffer_length_))) {
        LOG_WDIAG("fail to add com request", K(ret));
      } else {
        LOG_DEBUG("the request sql", "sql", client_request.get_sql());

        ObString sql = client_request.get_parse_sql();
        if ((OB_SUCC(ret)) && OB_LIKELY(!sql.empty())) {
          ObProxySqlParser sql_parser;
          ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
          // we will handle parser error in parse_sql
          if (OB_ISNULL(ctx.cached_variables_)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WDIAG("cached_variables should not be null", K(ret));
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
              LOG_WDIAG("fail to parse sql", K(sql), K(ret));
            } else if (OB_FAIL(handle_internal_cmd(client_request))) {
              LOG_WDIAG("fail to handle internal cmd", K(sql), K(ret));
            } else if (sql_parse_result.is_dual_request()
                       && OB_MYSQL_COM_STMT_PREPARE_EXECUTE == sql_cmd
                       && get_global_proxy_config().enable_xa_route) {
              // specically check if sql is xa start
              // from obci only one format: select DBMS_XA.XA_START(?, ?) from dual
              ObString tmp = sql;
              tmp.split_on(' ');
              ObString dbms_xa_start = tmp.split_on(' ').split_on('(').trim();
              if (0 == dbms_xa_start.case_compare("DBMS_XA.XA_START")) {
                sql_parse_result.set_xa_start_stmt(true);
                LOG_DEBUG("[ObMysqlRequestAnalyzer::do_analyze_request] receive xa start req in prepare execute packet");
              }
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
        LOG_WDIAG("fail to add auth request", K(ret));
      } else if (OB_FAIL(ObProxyAuthParser::parse_auth(
                  auth_request, ctx.vip_tenant_name_, ctx.vip_cluster_name_))) {
        LOG_WDIAG("fail to parse auth request", "vip cluster name", ctx.vip_tenant_name_,
                 "vip tenant name", ctx.vip_tenant_name_, K(ret));
      }
      break;
    }
    case OB_MYSQL_COM_STMT_FETCH:
    case OB_MYSQL_COM_STMT_CLOSE:
    case OB_MYSQL_COM_STMT_RESET:
    case OB_MYSQL_COM_STMT_EXECUTE:
    case OB_MYSQL_COM_STMT_SEND_LONG_DATA:
    case OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
    case OB_MYSQL_COM_STMT_GET_PIECE_DATA: {
      // add packet's buffer to mysql common request, for parsing later
      if (OB_FAIL(client_request.add_request(ctx.reader_, ctx.request_buffer_length_))) {
        LOG_WDIAG("fail to add com request", K(ret));
      }
      break;
    }
    case OB_MYSQL_COM_CHANGE_USER:
    case OB_MYSQL_COM_RESET_CONNECTION:
    case OB_MYSQL_COM_INIT_DB: {
      if (OB_FAIL(client_request.add_request(ctx.reader_, ctx.request_buffer_length_))) {
        LOG_WDIAG("fail to add com request", K(ret));
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
          LOG_WDIAG("fail to handle internal cmd", K(sql_cmd), K(ret));
        } else {
          LOG_INFO("this is proxysys user, current cmd was treated as error inter request cmd",
                   K(sql_cmd));
        }
        break;
      }
    }
    case OB_MYSQL_COM_PING:
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
      LOG_WDIAG("fail to alloc mem for ObInternalCmdInfo", "size", sizeof(ObInternalCmdInfo), K(ret));
    } else if (OB_ISNULL(client_request.cmd_info_ = new (cmd_info_buf) ObInternalCmdInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("alloc memory for ObInternalCmdInfo", K(ret));
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
        LOG_WDIAG("fail to fill query info", K(parse_result), K(ret));
      }
    } else if ((parse_result.is_kill_connection_cmd() || parse_result.is_show_processlist_cmd()) && client_request.is_enable_internal_kill_connection()) {
      // if use client session id v2, transfer kill cs_id/ show processlist to observer
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
      || parse_result.is_select_proxy_version()
      || parse_result.is_set_names_stmt()
      || parse_result.is_update_stmt()
      || parse_result.is_replace_stmt()
      || parse_result.is_delete_stmt()) {
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
  } else if (client_request.is_sharding_user() && (parse_result.is_show_topology_stmt()
                                                  || parse_result.is_show_elastic_id_stmt())) {
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
      LOG_WDIAG("invalid buffer reader", K(route_info_len), K(reader), K(ret));
    } else {
      // rewrite obproxy_simple_route_info with '*'
      reader->replace_with_char(OBPROXY_SIMPLE_PART_KEY_MARK, info.table_len_, MYSQL_NET_META_LENGTH + info.table_offset_);
      reader->replace_with_char(OBPROXY_SIMPLE_PART_KEY_MARK, info.part_key_len_, MYSQL_NET_META_LENGTH + info.part_key_offset_);
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::analyze_execute_header(const int64_t param_num,
                                                   const char *&bitmap,
                                                   int8_t &new_param_bound_flag,
                                                   const char *&buf, int64_t &data_len)
{
  int ret = OB_SUCCESS;

  int64_t bitmap_types = (param_num + 7) / 8;
  bitmap = buf;
  buf += bitmap_types;
  data_len -= bitmap_types;

  if (OB_FAIL(ObMysqlPacketUtil::get_int1(buf, data_len, new_param_bound_flag))) {
    LOG_WDIAG("fail to get int1", K(data_len), K(ret));
  }

  return ret;
}

// The default data of this method is complete and can only come in once
int ObMysqlRequestAnalyzer::parse_param_type(const int64_t param_num,
                                             ObIArray<EMySQLFieldType> &param_types,
                                             const char *&buf, int64_t &data_len)
{
  ObSEArray<TypeInfo, 4> type_infos;
  return parse_param_type(param_num, param_types, type_infos, buf, data_len);
}

// The default data of this method is complete and can only come in once
int ObMysqlRequestAnalyzer::parse_param_type(const int64_t param_num,
                                             ObIArray<EMySQLFieldType> &param_types,
                                             ObIArray<TypeInfo> &type_infos,
                                             const char *&buf, int64_t &data_len)
{
  int ret = OB_SUCCESS;

  uint8_t type = 0;
  int8_t flag = 0;
  // decode all types
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    if (OB_FAIL(ObMysqlPacketUtil::get_uint1(buf, data_len, type))) {
      LOG_WDIAG("fail to get uint1", K(data_len), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(buf, data_len, flag))) {
      LOG_WDIAG("fail to get int1", K(data_len), K(ret));
    } else if (OB_FAIL(param_types.push_back(static_cast<EMySQLFieldType>(type)))) {
      LOG_WDIAG("fail to push back param type", K(type), K(ret));
    } else if (OB_FAIL(type_infos.push_back(TypeInfo()))) {
      LOG_WDIAG("fail to push back empty type info", K(ret));
    } else if (OB_MYSQL_TYPE_COMPLEX == type) {
      TypeInfo &type_name_info = type_infos.at(i);
      uint8_t elem_type = 0;
      if (OB_FAIL(decode_type_info(buf, data_len, type_name_info))) {
        LOG_WDIAG("failed to decode type info", K(ret));
      } else if (type_name_info.type_name_.empty()) {
        type_name_info.is_elem_type_ = true;
        if (OB_FAIL(ObMysqlPacketUtil::get_uint1(buf, data_len, elem_type))) {
          LOG_WDIAG("fail to get uint1", K(data_len), K(ret));
        } else if (OB_FAIL(ObSMUtils::get_ob_type(type_name_info.elem_type_, static_cast<EMySQLFieldType>(elem_type)))) {
          LOG_WDIAG("cast ob type from mysql type failed", K(type_name_info.elem_type_), K(elem_type), K(ret));
        } else if (OB_MYSQL_TYPE_COMPLEX == elem_type
            && OB_FAIL(decode_type_info(buf, data_len, type_name_info))) {
          LOG_WDIAG("failed to decode type info", K(ret));
        }
      }
    } // end complex type
  }

  return ret;
}

// This method can be entered repeatedly, you need to pass the param offset and buf offset
int ObMysqlRequestAnalyzer::parse_param_type_from_reader(int64_t& param_offset,
                                                         const int64_t param_num,
                                                         ObIArray<EMySQLFieldType> &param_types,
                                                         event::ObIOBufferReader* reader,
                                                         int64_t& analyzed_len,
                                                         bool& is_finished)

{
  int ret = OB_SUCCESS;

  int64_t analyzed_len_tmp = analyzed_len;
  uint8_t type = 0;
  int8_t flag = 0;
  // decode all types
  for (int64_t i = param_offset; OB_SUCC(ret) && i < param_num; ++i) {
    if (OB_FAIL(get_uint1_from_reader(reader, analyzed_len_tmp, type))) {
      LOG_WDIAG("fail to get uint1", K(analyzed_len_tmp), K(ret));
    } else if (OB_FAIL(get_int1_from_reader(reader, analyzed_len_tmp, flag))) {
      LOG_WDIAG("fail to get int1", K(analyzed_len_tmp), K(ret));
    } else if (OB_FAIL(param_types.push_back(static_cast<EMySQLFieldType>(type)))) {
      LOG_WDIAG("fail to push back param type", K(type), K(ret));
    } else if (OB_MYSQL_TYPE_COMPLEX == type) {
      TypeInfo type_name_info;
      uint8_t elem_type = 0;
      // skip all complex type bytes
      if (OB_FAIL(decode_type_info_from_reader(reader, analyzed_len_tmp, type_name_info))) {
        LOG_WDIAG("failed to decode type info", K(ret));
      } else if (type_name_info.is_elem_type_) {
        if (OB_FAIL(get_uint1_from_reader(reader, analyzed_len_tmp, elem_type))) {
          LOG_WDIAG("fail to get uint1", K(analyzed_len_tmp), K(ret));
        } else if (OB_FAIL(ObSMUtils::get_ob_type(type_name_info.elem_type_, static_cast<EMySQLFieldType>(elem_type)))) {
          LOG_WDIAG("cast ob type from mysql type failed", K(type_name_info.elem_type_), K(elem_type), K(ret));
        } else if (OB_MYSQL_TYPE_COMPLEX == elem_type
                   && OB_FAIL(decode_type_info_from_reader(reader, analyzed_len_tmp, type_name_info))) {
            LOG_WDIAG("failed to decode type info", K(ret));
        }
      }
    } // end complex type

    if (OB_SUCC(ret)) {
      param_offset++;
      analyzed_len = analyzed_len_tmp;
    }
  }

  if (OB_SUCC(ret)) {
    // if type buff is complete
    if (param_num == param_offset) {
      is_finished = true;
    }
  }

  return ret;
}

int ObMysqlRequestAnalyzer::do_analyze_execute_param(const char *buf,
                                                     int64_t data_len,
                                                     const int64_t param_num,
                                                     ObIArray<EMySQLFieldType> *param_types,
                                                     ObProxyMysqlRequest &client_request,
                                                     const int64_t target_index,
                                                     ObObj &target_obj)
{
  int ret = OB_SUCCESS;

  int8_t new_param_bound_flag = 0;
  common::ObArray<obmysql::EMySQLFieldType> param_types_tmp;
  ObIAllocator &allocator = client_request.get_param_allocator();

  const char *bitmap = NULL;
  if (OB_FAIL(analyze_execute_header(param_num, bitmap, new_param_bound_flag, buf, data_len))) {
    LOG_WDIAG("fail to analyze execute header", K(param_num), K(ret));
  } else if (1 == new_param_bound_flag) {
    if (OB_FAIL(parse_param_type(param_num, param_types_tmp, buf, data_len))) {
      LOG_WDIAG("fail to parse param type", K(param_num), K(ret));
    } else {
      // If the flag is 1, anyway, the param type is also parsed here, so use the parsed directly here
      //   It is mainly the first Execute. If it is a large request, since the param type will not be parsed in the
      //   do_analyze_ps_execute_request function, when this function is reached, the passed param types array is empty
      // If the flag is 0, for example, the second Execute is a large request, then the passed param types parameter is used directly
      param_types = &param_types_tmp;
    }
  }

  if (OB_SUCC(ret)) {
    // Check the number in param_num and parm_type
    if (param_num != param_types->count()) {
      ret = OB_ERR_WRONG_DYNAMIC_PARAM;
      LOG_WDIAG("wrong param num and param_types num", K(param_num), KP(param_types), K(ret));
    }
  }

  bool is_null = false;
  ObObjType ob_type;
  const char *param_buf = buf;
  ObCharsetType charset = ObCharset::get_default_charset();
  // decode all values
  for (int64_t i = 0; OB_SUCC(ret) && i <= target_index; ++i) {
    target_obj.reset();
    uint8_t type = param_types->at(i);
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
        LOG_DEBUG("succ to parse execute param", K(ob_type), K(type), K(i));
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
    LOG_WDIAG("invalid argument", K(param_num), K(target_index), K(ret));
  } else if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *buf = data.ptr() + MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH;
    data_len -= (MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH);
    if (param_num > 0) {
      if (OB_FAIL(do_analyze_execute_param(buf, data_len, param_num, &param_types,
                                           client_request, target_index, target_obj))) {
        LOG_DEBUG("fail to do analyze execute param", K(ret));
      }
    } //  end if param_num > 0
  }
  return ret;
}

/*
 * try to calculate partition key for OB_MYSQL_COM_STMT_SEND_LONG_DATA
 * steps:
 * 1: get param id from send long data package, compare with param idx
 * 2: try to get obj type from execute parse result if exist
 * 3: get obj type from prepare parse result
 * 4: resolve send long data package, fill to obj, prepare to calculate partition key
 * 5: if the procedure failed, choose part key with random optimization
 */
int ObMysqlRequestAnalyzer::analyze_send_long_data_param(ObProxyMysqlRequest &client_request,
                                                         const int64_t execute_param_index,
                                                         ObProxyPartInfo *part_info,
                                                         ObPsIdEntry *ps_id_entry,
                                                         ObObj &target_obj)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ps_id_entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("ps id entry is NULL", K(ret));
  } else if (OB_UNLIKELY(execute_param_index >= ps_id_entry->get_param_count()
                         || execute_param_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid execute param idx", K(execute_param_index), K(ret));
  } else {
    ObString data = client_request.get_req_pkt();
    uint16_t origin_param_id = 0;
    const char *param_id_pos = data.ptr() + MYSQL_NET_META_LENGTH + MYSQL_PS_STMT_ID_LENGTH; // stmt_id 4
    obmysql::ObMySQLUtil::get_uint2(param_id_pos, origin_param_id);
    uint64_t param_id = static_cast<uint64_t>(origin_param_id);

    if (param_id != execute_param_index) {
      ret = OB_INVALID_ARGUMENT;
      LOG_DEBUG("param id is not part key.", K(ret));
    } else {
      // try to get type from execute, if not, then get from part key info
      bool get_param_type = false;
      common::ObIArray<EMySQLFieldType> &param_types = ps_id_entry->get_ps_sql_meta().get_param_types();
      if (!param_types.empty()) {
        if (param_id > param_types.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WDIAG("fail to get param type from execute parse result", K(ret), K(param_id), K(param_types.count()));
        } else {
          EMySQLFieldType param_type = param_types.at(param_id);
          ObObjType ob_type;
          if (OB_FAIL(ObSMUtils::get_ob_type(ob_type, param_type))) {
            LOG_WDIAG("fail to get ob type by mysql filed type", K(ret), K(param_type));
          } else {
            target_obj.set_type(ob_type);
            get_param_type = true;
          }
        }
      }

      if (OB_SUCC(ret) && !get_param_type) {
        ObString param_name;
        obutils::SqlFieldResult &sql_result = client_request.get_parse_result().get_sql_filed_result();
        if (static_cast<int>(param_id) > sql_result.field_num_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WDIAG("fail to get param name from prepare result", K(ret), K(param_id), K(sql_result.field_num_));
        } else {
          obutils::SqlField* field = sql_result.fields_.at(param_id);
          param_name = field->column_name_.config_string_;
          ObProxyPartKeyInfo &key_info = part_info->get_part_key_info();
          for (uint64_t i = 0; i < key_info.key_num_; ++i) {
            ObProxyPartKey &part_key = key_info.part_keys_[i];
            if (param_name == ObString::make_string(part_key.name_.str_)) {
              target_obj.set_type(static_cast<common::ObObjType>(part_key.obj_type_));
              get_param_type = true;
            }
          }
        }
      }

      if (OB_SUCC(ret) && get_param_type) {
        // common net meta + stmt_id + param_id
        uint16 value_inc = MYSQL_NET_META_LENGTH + MYSQL_PS_STMT_ID_LENGTH + MYSQL_PS_SEND_LONG_DATA_PARAM_ID_LENGTH;
        const char *buf = data.ptr() + value_inc;
        int64_t buf_len = data.length() - value_inc;
        ObCharsetType charset = ObCharset::get_default_charset();
        ObIAllocator &allocator = client_request.get_param_allocator();
        if (OB_FAIL(ObMysqlRequestAnalyzer::parse_param_value(allocator,
                                                              buf,
                                                              buf_len,
                                                              target_obj.get_type(),
                                                              charset,
                                                              target_obj))) {
          LOG_WDIAG("fail to parse param value for send long data", K(ret));
        }
      } else {
        LOG_WDIAG("fail to analyze obj for send long data", K(ret), K(get_param_type));
      }
    }
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
    LOG_WDIAG("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *buf = data.ptr() + MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH;
    data_len -= (MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH);
    uint64_t query_len = 0;
    int32_t param_num = 0;
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, data_len, query_len))) {
      LOG_WDIAG("failed to get length", K(data_len), K(ret));
    } else {
      buf += query_len;
      data_len -= query_len;
      if (OB_FAIL(ObMysqlPacketUtil::get_int4(buf, data_len, param_num))) {
        LOG_WDIAG("fail to get int4", K(data_len), K(ret));
      } else if (OB_UNLIKELY(param_num <= target_index)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid argument", K(param_num), K(target_index), K(ret));
      }
    }

    if (OB_SUCC(ret) && param_num > 0) {
      common::ObArray<obmysql::EMySQLFieldType> param_types;
      if (OB_FAIL(do_analyze_execute_param(buf, data_len, param_num, &param_types,
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
    LOG_WDIAG("invalid input value", K(data), K(ret));
  } else {
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
          LOG_WDIAG("parse timestamp value from client failed", K(ret));
        }
        break;
      }
      case OB_MYSQL_TYPE_TIME:{
        if (OB_FAIL(parse_mysql_time_value(data, buf_len, param))) {
          LOG_WDIAG("parse timestamp value from client failed", K(ret));
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
      case OB_MYSQL_TYPE_BLOB:
      case OB_MYSQL_TYPE_LONG_BLOB:
      case OB_MYSQL_TYPE_MEDIUM_BLOB:
      case OB_MYSQL_TYPE_TINY_BLOB:
      case OB_MYSQL_TYPE_STRING:
      case OB_MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_OB_NCHAR:
      case MYSQL_TYPE_OB_NVARCHAR2:
      case OB_MYSQL_TYPE_VAR_STRING:
      case OB_MYSQL_TYPE_NEWDECIMAL:
      case OB_MYSQL_TYPE_OB_UROWID:
      case OB_MYSQL_TYPE_JSON:
      case OB_MYSQL_TYPE_GEOMETRY: {
        ObString str;
        ObString dst;
        uint64_t length = 0;
        common::ObCollationType cs_type = common::ObCharset::get_default_collation(charset);
        if (OB_FAIL(ObMysqlPacketUtil::get_length(data, buf_len, length))) {
          LOG_EDIAG("decode varchar param value failed", K(buf_len), K(ret));
        } else if (buf_len < length) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WDIAG("data buf size is not enough", K(length), K(buf_len), K(ret));
        } else {
          str.assign_ptr(data, static_cast<ObString::obstr_size_t>(length));
          if (OB_FAIL(ob_write_string(allocator, str, dst))) {
            LOG_WDIAG("failed to ob write string", K(ret));
          } else {
            if (OB_MYSQL_TYPE_NEWDECIMAL == type) {
              number::ObNumber nb;
              if (OB_FAIL(nb.from(str.ptr(), length, allocator))) {
                LOG_WDIAG("decode varchar param to number failed", K(ret));
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
    LOG_WDIAG("fail to get int1", K(ret));
  } else if (0 == length) {
    value = 0;
  } else if (4 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int2(data, buf_len, year))) {
      LOG_WDIAG("fail to get year", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, month))) {
      LOG_WDIAG("fail to get month", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, day))) {
      LOG_WDIAG("fail to get day", K(ret));
    }
  } else if (7 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int2(data, buf_len, year))) {
      LOG_WDIAG("fail to get year", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, month))) {
      LOG_WDIAG("fail to get month", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, day))) {
      LOG_WDIAG("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WDIAG("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WDIAG("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WDIAG("fail to get second", K(ret));
    }
  } else if (11 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int2(data, buf_len, year))) {
      LOG_WDIAG("fail to get year", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, month))) {
      LOG_WDIAG("fail to get month", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, day))) {
      LOG_WDIAG("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WDIAG("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WDIAG("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WDIAG("fail to get second", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, microsecond))) {
      LOG_WDIAG("fail to get microsecond", K(ret));
    }
  } else {
    ret = OB_ERROR;
    LOG_WDIAG("invalid mysql timestamp value length", K(length));
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
        LOG_WDIAG("invalid date format", K(ret));
      } else {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
        if (field_type == OB_MYSQL_TYPE_DATE) {
          value = ob_time.parts_[DT_DATE];
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx.tz_info_, value))){
          LOG_WDIAG("convert obtime to datetime failed", K(value), K(year), K(month),
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
    LOG_WDIAG("fail to get int1", K(ret));
  } else if (0 == length) {
    value = 0;
  } else if (8 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, is_negative))) {
      LOG_WDIAG("fail to get is_negative", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, day))) {
      LOG_WDIAG("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WDIAG("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WDIAG("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WDIAG("fail to get second", K(ret));
    }
  } else if (12 == length) {
    if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, is_negative))) {
      LOG_WDIAG("fail to get is_negative", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, day))) {
      LOG_WDIAG("fail to get day", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, hour))) {
      LOG_WDIAG("fail to get hour", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, min))) {
      LOG_WDIAG("fail to get minute", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(data, buf_len, second))) {
      LOG_WDIAG("fail to get second", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(data, buf_len, microsecond))) {
      LOG_WDIAG("fail to get microsecond", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected time length", K(length), K(ret));
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
        LOG_WDIAG("invalid date format", K(ret));
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
      LOG_WDIAG("failed to get length", K(ret));
    } else if (buf_len < length) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("packet buf size is not enough", K(buf_len), K(length), K(ret));
    } else {
      type_info.relation_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
      buf += length;
      buf_len -= length;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, buf_len, length))) {
      LOG_WDIAG("failed to get length", K(ret));
    } else if (buf_len < length) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("packet buf size is not enough", K(buf_len), K(length), K(ret));
    } else {
      type_info.type_name_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length));
      buf += length;
      buf_len -= length;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMysqlPacketUtil::get_length(buf, buf_len, type_info.version_))) {
      LOG_WDIAG("failed to get version", K(ret));
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::decode_type_info_from_reader(event::ObIOBufferReader* reader,
                                                         int64_t &decoded_offset,
                                                         TypeInfo &type_info)
{
  int ret = OB_SUCCESS;
  uint64_t read_avail = reader->read_avail();
  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(get_length_from_reader(reader, decoded_offset, length))) {
      LOG_WDIAG("failed to get length", K(ret));
    } else if (read_avail < (decoded_offset + length)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("packet buf size is not enough", K(read_avail), K(decoded_offset), K(length), K(ret));
    } else {
      decoded_offset += length;
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t length = 0;
    if (OB_FAIL(get_length_from_reader(reader, decoded_offset, length))) {
      LOG_WDIAG("failed to get length", K(ret));
    } else if (read_avail < (decoded_offset + length)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("packet buf size is not enough", K(read_avail), K(decoded_offset), K(length), K(ret));
    } else {
      if (length == 0) {
        type_info.is_elem_type_ = true;
      }
      decoded_offset += length;
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t version = 0;
    if (OB_FAIL(get_length_from_reader(reader, decoded_offset, version))) {
      LOG_WDIAG("failed to get version", K(ret));
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::get_length_from_reader(event::ObIOBufferReader* reader,
                                                   int64_t &decoded_offset,
                                                   uint64_t &length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(reader->read_avail() - decoded_offset < 1)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    uint16_t s2 = 0;
    uint32_t s4 = 0;
    uint8_t sentinel = 0;
    get_uint1_from_reader(reader, decoded_offset, sentinel);
    if (sentinel < 251) {
      length = sentinel;
    } else if (sentinel == 251) {
      length = UINT64_MAX; // represents a NULL resultset
    } else if (sentinel == 252) {
      ret = get_uint2_from_reader(reader, decoded_offset, s2);
      length = s2;
    } else if (sentinel == 253) {
      ret = get_uint3_from_reader(reader, decoded_offset, s4);
      length = s4;
    } else if (sentinel == 254) {
      ret = get_uint8_from_reader(reader, decoded_offset, length);
    } else {
      // 255??? won't get here.
      decoded_offset++;           // roll back
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int ObMysqlRequestAnalyzer::get_uint1_from_reader(event::ObIOBufferReader* reader,
                                                  int64_t &decoded_offset,
                                                  uint8_t &v)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(reader->read_avail() - decoded_offset < 1)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    reader->copy((char*)&v, 1, decoded_offset);
    decoded_offset += 1;
  }

  return ret;
}

int ObMysqlRequestAnalyzer::get_uint2_from_reader(event::ObIOBufferReader* reader,
                                                  int64_t &decoded_offset,
                                                  uint16_t &v)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(reader->read_avail() - decoded_offset < 2)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    char buf[2] = "\0";
    char *buf_ptr = buf;
    reader->copy(buf, 2, decoded_offset);
    ObMySQLUtil::get_uint2(buf_ptr, v);
    decoded_offset += 2;
  }

  return ret;
}

int ObMysqlRequestAnalyzer::get_uint3_from_reader(event::ObIOBufferReader* reader,
                                                  int64_t &decoded_offset,
                                                  uint32_t &v)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(reader->read_avail() - decoded_offset < 3)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    char buf[3] = "\0";
    char *buf_ptr = buf;
    reader->copy(buf, 3, decoded_offset);
    ObMySQLUtil::get_uint3(buf_ptr, v);
    decoded_offset += 3;
  }

  return ret;
}

int ObMysqlRequestAnalyzer::get_uint8_from_reader(event::ObIOBufferReader* reader,
                                                  int64_t &decoded_offset,
                                                  uint64_t &v)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(reader->read_avail() - decoded_offset < 8)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    char buf[8] = "\0";
    char *buf_ptr = buf;
    reader->copy(buf, 8, decoded_offset);
    ObMySQLUtil::get_uint8(buf_ptr, v);
    decoded_offset += 8;
  }

  return ret;
}

int ObMysqlRequestAnalyzer::get_int1_from_reader(event::ObIOBufferReader* reader,
                                                 int64_t &decoded_offset,
                                                 int8_t &v)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(reader->read_avail() - decoded_offset < 1)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    reader->copy((char*)&v, 1, decoded_offset);
    decoded_offset += 1;
  }

  return ret;
}

int ObMysqlRequestAnalyzer::analyze_sql_id(const ObString &sql, ObProxyMysqlRequest &client_request, ObString &sql_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *allocator = NULL;
  if (OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator))) {
    LOG_WDIAG("fail to get parse allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("allocator is null", K(ret));
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
      LOG_WDIAG("fail to do parse_and_gen_sqlid", K(sql), K(ret));
      sql_id_buf[0] = '\0';
    } else {
      sql_id_buf[sql_id_buf_len - 1] = '\0';
      sql_id.assign_ptr(sql_id_buf, static_cast<int32_t>(sql_id_buf_len - 1));
      LOG_DEBUG("succ to get sql id", K(sql_id), K(sql));
    }

    if (OB_NOT_NULL(allocator)) {
      allocator->reuse();
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
