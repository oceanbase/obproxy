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

#include "utils/ob_proxy_utils.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "obproxy/cmd/ob_internal_cmd_processor.h"
#include "obproxy/utils/ob_proxy_privilege_check.h"

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
  databuff_print_kv(buf, buf_len, pos, K_(is_kill_query), K_(cs_id),
                    K_(real_conn_id), K_(errcode), K_(priv_name), K_(server_addr));
  J_OBJ_END();
  return pos;
}

ObProxyMysqlRequest::ObProxyMysqlRequest()
  : cmd_info_(NULL), query_info_(NULL), meta_(), req_buf_(NULL), req_buf_len_(0),
    req_pkt_len_(0), req_buf_for_prepare_execute_(NULL),
    req_buf_for_prepare_execute_len_(0), result_(), ps_result_(NULL),
    user_identity_(USER_TYPE_NONE), is_internal_cmd_(false), is_kill_query_(false),
    is_large_request_(false), enable_analyze_internal_cmd_(false), is_mysql_req_in_ob20_payload_(false)
{
  sql_id_buf_[0] = '\0';
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
      }

      // if buf is not suitable we re-alloc it
      if (OB_ISNULL(req_buf_) || OB_UNLIKELY(req_buf_len_ != req_buf_len)) {
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
  enable_internal_kill_connection_ = false;
  allocator_.reuse();
  sql_id_buf_[0] = '\0';
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
