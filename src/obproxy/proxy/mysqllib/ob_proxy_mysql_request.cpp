/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "utils/ob_proxy_utils.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "obproxy/cmd/ob_internal_cmd_processor.h"
#include "obproxy/utils/ob_proxy_privilege_check.h"
#include "obproxy/proxy/mysqllib/ob_cdc_dump_packet.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
class ObProxyPrivilegeCheck;
namespace proxy
{
void ObProxyKillQueryInfo::reset()
{
  is_kill_query_ = false;
  cs_id_ = OB_INVALID_ID;
  group_id_ = OB_INVALID_ID;
  real_conn_id_ = OB_INVALID_FILE_ID;
  errcode_ = OB_MAX_ERROR_CODE;
  priv_name_ = NULL;
  memset(&server_addr_, 0, sizeof(ObIpEndpoint));
}

int ObProxyKillQueryInfo::do_privilege_check(const ObProxySessionPrivInfo &session_priv)
{
  int ret = OB_SUCCESS;
  share::schema::ObNeedPriv need_priv;
  ObProxyPrivilegeCheck::get_need_priv(sql::stmt::T_KILL, session_priv, need_priv);
  if (OB_FAIL(ObProxyPrivilegeCheck::check_privilege(session_priv, need_priv, priv_name_))) {
    LOG_WDIAG("user privilege is not match need privilege, permission denied", K(session_priv),
             K(need_priv), K_(priv_name), K(ret));
    errcode_ = ret;
  }
  return ret;
}

int64_t ObProxyKillQueryInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  databuff_print_kv(buf, buf_len, pos, K_(is_kill_query), K_(cs_id), K_(group_id),
                    K_(real_conn_id), K_(errcode), K_(priv_name), K_(server_addr));
  J_OBJ_END();
  return pos;
}

ObProxyMysqlRequest::ObProxyMysqlRequest()
  : cmd_info_(NULL), query_info_(NULL), meta_(), req_buf_(NULL), req_buf_len_(0),
    req_pkt_len_(0), req_buf_for_prepare_execute_(NULL),
    req_buf_for_prepare_execute_len_(0), result_(), ps_result_(NULL),
    user_identity_(USER_TYPE_NONE), is_internal_cmd_(false), is_kill_query_(false),
    is_large_request_(false), enable_analyze_internal_cmd_(false), is_mysql_req_in_ob20_payload_(false),
    expr_parse_second_sql_(), cdc_dump_pkt_(NULL)
{
  sql_id_buf_[0] = '\0';
  is_for_update_sql_.valid_ = false;
  is_for_update_sql_.value_ = false;
}

bool ObProxyMysqlRequest::is_real_dml_sql() const
{
  bool bret = false;
  switch (result_.get_stmt_type()) {
    case OBPROXY_T_SELECT: {
      //select without table name is not real dml
      if (!result_.get_table_name().empty()) {
        bret = true;
      }
      break;
    }
    case OBPROXY_T_UPDATE:
    case OBPROXY_T_DELETE:
    case OBPROXY_T_INSERT:
    case OBPROXY_T_MERGE:
    case OBPROXY_T_REPLACE: {
      bret = true;
      break;
    }
    default:
      break;
  }
  return bret;
}

void ObProxyMysqlRequest::reset(bool is_reset_origin_db_table /* true */)
{
  is_for_update_sql_.valid_ = false;
  is_for_update_sql_.value_ = false;
  reuse(is_reset_origin_db_table);
  allocator_.reset();
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(free_request_buf())) {
    PROXY_LOG(EDIAG, "free request buf error", K(ret));
  }

  if (OB_FAIL(free_prepare_execute_request_buf())) {
    PROXY_LOG(EDIAG, "free prepare execute request buf error", K(ret));
  }

  if (OB_NOT_NULL(cdc_dump_pkt_)) {
    op_free(cdc_dump_pkt_);
    cdc_dump_pkt_ = NULL;
  }
}

obutils::ObSqlParseResult& ObProxyMysqlRequest::get_parse_result()
{
  obutils::ObSqlParseResult *result = &result_;
  if ((obmysql::OB_MYSQL_COM_STMT_EXECUTE == meta_.cmd_ || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == meta_.cmd_)
      && NULL != ps_result_) {
    result = ps_result_;
  }
  return *result;
}

const obutils::ObSqlParseResult& ObProxyMysqlRequest::get_parse_result() const
{
  const obutils::ObSqlParseResult *result = &result_;
  if ((obmysql::OB_MYSQL_COM_STMT_EXECUTE == meta_.cmd_ || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == meta_.cmd_)
      && NULL != ps_result_) {
    result = ps_result_;
  }
  return *result;
}

int ObProxyMysqlRequest::alloc_request_buf(int64_t buf_len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != req_buf_)) {
    if (OB_FAIL(free_request_buf())) {
      PROXY_LOG(EDIAG, "free request buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(buf_len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(buf_len), K(ret));
    } else {
      req_buf_ = buf;
      req_buf_len_ = buf_len;
    }
  }
  return ret;
}

int ObProxyMysqlRequest::free_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != req_buf_) {
    if (req_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "req_buf_len_ must > 0", K_(req_buf_len), K_(req_buf), K(ret));
    } else {
      op_fixed_mem_free(req_buf_, req_buf_len_);
      req_buf_ = NULL;
      req_buf_len_ = 0;
    }
  }
  return ret;
}

int ObProxyMysqlRequest::alloc_prepare_execute_request_buf(const int64_t buf_len)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(buf_len < 0)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "buf_len must > 0", K(buf_len), K(ret));
  }

  // free buf if has alloc
  if (OB_SUCC(ret) && OB_UNLIKELY(NULL != req_buf_for_prepare_execute_)) {
    if (OB_FAIL(free_prepare_execute_request_buf())) {
      PROXY_LOG(EDIAG, "free prepare execute request buf error", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(buf_len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(EDIAG, "fail to alloc mem", K(buf_len), K(ret));
    } else {
      req_buf_for_prepare_execute_ = buf;
      req_buf_for_prepare_execute_len_ = buf_len;
    }
  }
  return ret;
}

void ObProxyMysqlRequest::borrow_req_buf(char *&req_buf, int64_t &req_buf_len) {
  if (OB_LIKELY(obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE != meta_.cmd_)) {
    req_buf = req_buf_;
    req_buf_len = req_buf_len_;
    req_buf_ = NULL;
    req_buf_len_ = 0;
  } else {
    req_buf = req_buf_for_prepare_execute_;
    req_buf_len = req_buf_for_prepare_execute_len_;
    req_buf_for_prepare_execute_ = NULL;
    req_buf_for_prepare_execute_len_ = 0;
  }
}

int ObProxyMysqlRequest::free_prepare_execute_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != req_buf_for_prepare_execute_) {
    if (req_buf_for_prepare_execute_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "req_buf_len_ must > 0", K_(req_buf_for_prepare_execute_len), K_(req_buf_for_prepare_execute), K(ret));
    } else {
      op_fixed_mem_free(req_buf_for_prepare_execute_, req_buf_for_prepare_execute_len_);
      req_buf_for_prepare_execute_ = NULL;
      req_buf_for_prepare_execute_len_ = 0;
    }
  }
  return ret;
}

common::ObString ObProxyMysqlRequest::get_sql()
{
  const char *sql = NULL;
  int64_t sql_len = 0;
  if (OB_LIKELY(NULL != req_buf_ && req_pkt_len_ > MYSQL_NET_META_LENGTH)) {
    if (OB_LIKELY(obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE != meta_.cmd_)) {
      sql = req_buf_ + MYSQL_NET_META_LENGTH; // skip pkt meta(5 bytes)
      sql_len = req_pkt_len_ - MYSQL_NET_META_LENGTH;
    } else {
      int ret = OB_SUCCESS;
      uint64_t query_len = 0;
      const char *pos = req_buf_ + MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH; // skip 9 bytes
      int64_t buf_len = req_pkt_len_ - MYSQL_NET_META_LENGTH - MYSQL_PS_EXECUTE_HEADER_LENGTH;
      if (OB_FAIL(ObMysqlPacketUtil::get_length(pos, buf_len, query_len))) {
        PROXY_LOG(EDIAG, "failed to get length", K(ret));
      } else if (query_len > 0) {
        // buf_len is less than (req_buf_len_ - PARSE_EXTRA_CHAR_NUM + other fields)
        // so mem of req_buf_len_ will never overflow
        int64_t copy_len = std::min(static_cast<int64_t>(query_len), buf_len);
        if (OB_ISNULL(req_buf_for_prepare_execute_)
            || req_buf_for_prepare_execute_len_ < req_buf_len_
            || req_buf_for_prepare_execute_len_ > req_buf_len_ * 2) {
          if (OB_FAIL(alloc_prepare_execute_request_buf(req_buf_len_))) {
            PROXY_LOG(EDIAG, "fail to alloc buf", K_(req_buf_len), K(ret));
          } else {
            PROXY_LOG(DEBUG, "alloc request buf ", K_(req_buf_len));
          }
        }

        if (OB_SUCC(ret)) {
          MEMCPY(req_buf_for_prepare_execute_, pos, copy_len);
          req_buf_for_prepare_execute_[copy_len + 1] = 0;
          req_buf_for_prepare_execute_[copy_len] = 0;

          sql = req_buf_for_prepare_execute_;
          sql_len = copy_len;
        }
      }
    }
  }
  common::ObString sql_str(sql_len, sql);
  return sql_str;
}

int ObProxyMysqlRequest::add_request(event::ObIOBufferReader *reader, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader) || OB_UNLIKELY(buf_len < MYSQL_NET_META_LENGTH + PARSE_EXTRA_CHAR_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid buffer reader", K(reader), K(buf_len), K(ret));
  } else {
    // we maybe get the OB_MYSQL_COM_QUERY packet like: {0x1, 0x0, 0x0, 0x0, 0x3},
    // which has no sql actual;
    int64_t total_len = reader->read_avail();
    int64_t req_buf_len = buf_len;
    if (OB_UNLIKELY(total_len < MYSQL_NET_META_LENGTH)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("buffer reader is empty", K(ret));
    } else {
      // OB_MYSQL_COM_STMT_CLOSE/OB_MYSQL_COM_STMT_SEND_LONG_DATA always followed other request
      // mysql req in ob20 payload, always followed by crc or other mysql req
      LOG_DEBUG("add request before", K(total_len), K(meta_), K(is_mysql_req_in_ob20_payload()));
      if (total_len > meta_.pkt_len_
          && (is_mysql_req_in_ob20_payload()
              || OB_UNLIKELY(OB_MYSQL_COM_STMT_CLOSE == meta_.cmd_ || OB_MYSQL_COM_STMT_SEND_LONG_DATA == meta_.cmd_))) {
        total_len = meta_.pkt_len_;
      }

      int64_t copy_len = 0;
      if (OB_UNLIKELY(is_sharding_user() || is_proxysys_user())) {
        copy_len = total_len;
        // add two '\0' at the tail for parser
        req_buf_len = req_buf_len > total_len + PARSE_EXTRA_CHAR_NUM ? req_buf_len : total_len + PARSE_EXTRA_CHAR_NUM;
      } else {
        copy_len = std::min(total_len, req_buf_len - PARSE_EXTRA_CHAR_NUM);
        req_buf_len = copy_len + PARSE_EXTRA_CHAR_NUM;
      }

      // if buf is not suitable we re-alloc it
      if (OB_ISNULL(req_buf_) || OB_UNLIKELY(req_buf_len_ < req_buf_len || req_buf_len_ > req_buf_len * 10)) {
        if (OB_FAIL(alloc_request_buf(req_buf_len))) {
          LOG_EDIAG("fail to alloc buf", K(req_buf_len), K(ret));
        } else {
          LOG_DEBUG("alloc request buf ", K(req_buf_len));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_LIKELY(NULL != req_buf_)
            && OB_LIKELY(copy_len + PARSE_EXTRA_CHAR_NUM <= req_buf_len_)) {
          char *written_pos = reader->copy(req_buf_, copy_len, 0);
          if (OB_UNLIKELY(written_pos != req_buf_ + copy_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("not copy completely", K(written_pos), K(req_buf_), K(copy_len), K(ret));
          } else {
            // add two '\0' at the tail for parser
            req_pkt_len_ = copy_len;
            req_buf_[copy_len + 1] = 0;
            req_buf_[copy_len] = 0;
            LOG_DEBUG("add mysql request succ", K(copy_len), K(req_buf_len), K(total_len), K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_EDIAG("unexpected null buf", K(req_buf_), K(copy_len), K(req_buf_len_));
        }
      }
    }
  }
  return ret;
}

int ObProxyMysqlRequest::fill_query_info(const int64_t cs_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL == query_info_)) {
    char *query_info_buf = NULL;
    int64_t alloc_size = static_cast<int64_t>(sizeof(ObProxyKillQueryInfo));
    if (OB_ISNULL(query_info_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem for ObProxyKillQueryInfo", K(alloc_size), K(ret));
    } else {
      query_info_ = new (query_info_buf) ObProxyKillQueryInfo();
    }
  }

  if (OB_LIKELY(NULL != query_info_)) {
    query_info_->cs_id_ = cs_id;
  } else {
    //kill query should not transmit to observer immediately
    //if fail to alloc query_info_, will disconnect later
  }
  is_kill_query_ = true;
  return ret;
}

bool ObProxyMysqlRequest::is_for_update_sql(common::ObString src_sql)
{
  bool bret = false;
  const char FOR_STRING_BUF[] = "for";
  const ObString FOR_STRING(FOR_STRING_BUF);
  const ObString UPDATE_STRING("update");
  //' for update'
  if (src_sql.length() > (FOR_STRING.length() + UPDATE_STRING.length() + 2)
      && '\0' == src_sql[src_sql.length()]) {
    char *ptr = src_sql.ptr();
    char *last_pos  = NULL;
    char *pos = ptr;
    const char *end = src_sql.ptr() + src_sql.length();
    while (!bret && NULL != (pos = strcasestr(pos, FOR_STRING_BUF))) {
      last_pos = pos;
      pos += 3;

      if (NULL != last_pos
          && last_pos > ptr
          && IS_SPACE(*(last_pos-1))
          && IS_SPACE(*(last_pos+3))) {
        last_pos = last_pos + 3;
        while (last_pos < end && IS_SPACE(*last_pos)) {
          last_pos++;
        }
        if (0 == strncasecmp(last_pos, UPDATE_STRING.ptr(), UPDATE_STRING.length())
            && ('\0' == last_pos[UPDATE_STRING.length()] || ';' == last_pos[UPDATE_STRING.length()])) {
          bret = true;
        }
      }
    }
  }
  return bret;
}


bool ObProxyMysqlRequest::is_for_update_sql()
{
  if (!is_for_update_sql_.valid_) {
    is_for_update_sql_.valid_ = true;
    is_for_update_sql_.value_ = ObProxyMysqlRequest::is_for_update_sql(get_sql());
  }

  return is_for_update_sql_.value_;
}

int ObProxyMysqlRequest::preprocess_multi_sql(ObIArray<common::ObString> &sql_array)
{
  int ret = OB_SUCCESS;
  const int64_t PARSE_EXTRA_CHAR_NUM = 2;
  for (int64_t i = 0; i < sql_array.count() && OB_SUCC(ret); ++i) {
    ObString& sql = sql_array.at(i);
    char *sql_buf = NULL;
    const int64_t total_sql_length = sql.length() + PARSE_EXTRA_CHAR_NUM;
    if (OB_ISNULL(sql_buf = static_cast<char*>(allocator_.alloc(total_sql_length)))) {
      ret = OB_REACH_MEMORY_LIMIT;
      LOG_WDIAG("fail to alloc memory for sql_buf", K(total_sql_length), K(ret));
    } else {
      MEMCPY(sql_buf ,sql.ptr(), sql.length());
      MEMSET(sql_buf + sql.length(), '\0', PARSE_EXTRA_CHAR_NUM);
      sql.assign_ptr(sql_buf, sql.length());
    }
  }
  return ret;
}

void ObProxyMysqlRequest::reuse(bool is_reset_origin_db_table /* true */)
{
  if (OB_UNLIKELY(NULL != cmd_info_)) {
    op_fixed_mem_free(cmd_info_, static_cast<int64_t>(sizeof(ObInternalCmdInfo)));
    cmd_info_ = NULL;
  }
  if (OB_UNLIKELY(NULL != query_info_)) {
    op_fixed_mem_free(query_info_, static_cast<int64_t>(sizeof(ObProxyKillQueryInfo)));
    query_info_ = NULL;
  }
  if (NULL != ps_result_) {
    ps_result_ = NULL;
  }
  meta_.reset();
  result_.reset(is_reset_origin_db_table);
  is_internal_cmd_ = false;
  is_kill_query_ = false;
  is_large_request_ = false;
  enable_analyze_internal_cmd_ = false;
  is_mysql_req_in_ob20_payload_ = false;
  user_identity_ = USER_TYPE_NONE;
  req_pkt_len_ = 0;
  enable_server_kill_connection_ = false;
  expr_parse_second_sql_.reset();
  if (OB_NOT_NULL(cdc_dump_pkt_)) {
    op_free(cdc_dump_pkt_);
    cdc_dump_pkt_ = NULL;
  }
  allocator_.reuse();
  sql_id_buf_[0] = '\0';
  is_for_update_sql_.valid_ = false;
  is_for_update_sql_.value_ = false;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
