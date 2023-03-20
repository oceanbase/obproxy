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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_internal_cmd_handler.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "iocore/net/ob_net_def.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "obproxy/proxy/mysqllib/ob_2_0_protocol_utils.h"

using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
ObInternalCmdHandler::ObInternalCmdHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
    : ObContinuation(), cs_handler_(NULL), cs_id_(info.get_cs_id()), external_buf_(buf),
      internal_buf_(NULL), internal_reader_(NULL), internal_buf_limited_(info.get_memory_limit()),
      protocol_(info.get_protocol()), ob20_param_(info.get_ob20_head_param()), 
      is_inited_(false), header_encoded_(false),
      seq_(info.get_pkt_seq()), original_seq_(seq_), saved_event_(-1), like_name_()
{
  action_.set_continuation(cont);
  submit_thread_ =((OB_LIKELY(NULL != cont) && OB_LIKELY(NULL != cont->mutex_))
      ? cont->mutex_->thread_holding_
      : NULL);
  if (!info.get_like_string().empty()) {
    int32_t min_len =std::min(info.get_like_string().length(), static_cast<int32_t>(PROXY_LIKE_NAME_MAX_SIZE));
    MEMCPY(like_name_buf_, info.get_like_string().ptr(), min_len);
    like_name_.assign_ptr(like_name_buf_, min_len);
  }
}

ObInternalCmdHandler::~ObInternalCmdHandler()
{
  is_inited_ = false;
  action_.mutex_.release();
  mutex_.release();
  cs_handler_ = NULL;
  destroy_internal_buf();
  protocol_ = ObProxyProtocol::PROTOCOL_NORMAL;
  ob20_param_.reset();
}

int ObInternalCmdHandler::init(const bool is_query_cmd/*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    WARN_ICMD("fail to init twice", K(ret));
  } else if (OB_ISNULL(submit_thread_) || OB_ISNULL(external_buf_) || OB_ISNULL(action_.continuation_)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("internal argument is NULL", K(submit_thread_), K(external_buf_),
              K(action_.continuation_), K(ret));
  } else if (OB_UNLIKELY(NULL != mutex_) || OB_UNLIKELY(NULL != internal_buf_) || OB_UNLIKELY(NULL != internal_reader_)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("internal argument is not NULL", K(mutex_.ptr_), K(internal_buf_),
              K(internal_reader_), K(ret));
  } else if (OB_ISNULL(mutex_ = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WARN_ICMD("fail to new_proxy_mutex", K(ret));
  } else if (OB_ISNULL(internal_buf_ = new_empty_miobuffer())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WARN_ICMD("fail to new_empty_miobuffer for internal_buf_", K(ret));
  } else if (OB_ISNULL(internal_reader_ = internal_buf_->alloc_reader())) {
    ret = OB_ENTRY_NOT_EXIST;
    WARN_ICMD("fail to alloc_reader for internal_reader_", K(ret));
  } else {
    //WARN::The io buffer will alloced by thread allocator, and free by work thread. it will memory leak.
    //So we need alloc these buf in work thread before use it.
    //For non-query cmd, only one block is enough
    int64_t block_count = 1;
    if (is_query_cmd && internal_buf_limited_ > 0) {
      block_count = internal_buf_limited_ / DEFAULT_LARGE_BUFFER_SIZE;
      if (0 != internal_buf_limited_ % DEFAULT_LARGE_BUFFER_SIZE) {
        block_count += 1;
      }
    }

    if (OB_FAIL(internal_buf_->add_block(block_count))) {
      WARN_ICMD("fail to add_block for internal_buf_", K(block_count), K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObInternalCmdHandler::reset()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_FAIL(internal_reader_->consume(internal_reader_->read_avail()))) {
    WARN_ICMD("fail to consume internal buf", K(ret));
  } else {
    internal_buf_->reset();
    seq_ = original_seq_;
    // could not reset protocol and ob20_param in handler, cause we need filll external buf according to protocol
  }
  return ret;
}

void ObInternalCmdHandler::destroy_internal_buf()
{
  if (OB_LIKELY(NULL != internal_reader_)) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(internal_reader_->consume(internal_reader_->read_avail()))) {
      WARN_ICMD("fail to consume ", K(ret));
    }
    internal_reader_ = NULL;
  }
  
  if (OB_LIKELY(NULL != internal_buf_)) {
    free_miobuffer(internal_buf_);
    internal_buf_ = NULL;
  }
}

int ObInternalCmdHandler::encode_header(const ObString *cname, const EMySQLFieldType *ctype, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_ISNULL(cname) || OB_ISNULL(ctype) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObArray<ObMySQLField> fields;
    ObMySQLField field;
    //TODO: ref the observer to_mysql_field(), make the encode better
    field.charsetnr_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
    for (int64_t i = 0; (OB_SUCC(ret) && i < size); ++i) {
      field.cname_ = cname[i];
      field.org_cname_ = cname[i];
      field.type_ = ctype[i];
      if (OB_FAIL(fields.push_back(field))) {
        WARN_ICMD("fail to push field into array", K(field), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMysqlPacketUtil::encode_header(*internal_buf_, seq_, fields))) {
        WARN_ICMD("fail to encode header", K(ret));
      } else {
        DEBUG_ICMD("succ to encode header", K(fields));
      }
    }
  }
  return ret;
}

int ObInternalCmdHandler::encode_header(const ObProxyColumnSchema *column_schema, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_ISNULL(column_schema) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObArray<ObMySQLField> fields;
    ObMySQLField field;
    //TODO: ref the observer to_mysql_field(), make the encode better
    field.charsetnr_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
    for (int64_t i = 0; (OB_SUCC(ret) && i < size); ++i) {
      field.cname_ = column_schema[i].cname_;
      field.org_cname_ = column_schema[i].cname_;
      field.type_ = column_schema[i].ctype_;
      if (OB_FAIL(fields.push_back(field))) {
        WARN_ICMD("fail to push field into array", K(field), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMysqlPacketUtil::encode_header(*internal_buf_, seq_, fields))) {
        WARN_ICMD("fail to encode header", K(ret));
      } else {
        DEBUG_ICMD("succ to encode header", K(fields));
      }
    }
  }
  return ret;
}

int ObInternalCmdHandler::encode_row_packet(const ObNewRow &row, const bool need_limit_size/*true*/)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (need_limit_size
             && internal_buf_limited_ > 0
             && (data_size = internal_reader_->read_avail()) >= internal_buf_limited_) {
    ret = OB_BUF_NOT_ENOUGH;
    WARN_ICMD("internal cmd response size will out of limited size, break it",
              K(data_size), K(internal_buf_limited_), K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_row_packet(*internal_buf_, seq_, row))) {
    WARN_ICMD("fail to encode row packet", K(row), K(ret));
  } else {
    DEBUG_ICMD("succ to encode row packet", K(row));
  }
  return ret;
}

int ObInternalCmdHandler::encode_eof_packet()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*internal_buf_, seq_))) {
    WARN_ICMD("fail to encode eof packet", K(ret));
  } else {
    DEBUG_ICMD("succ to encode eof packet");
  }
  return ret;
}

// write packet in mysql format to internal buf, then trans it to external buf as ob20/mysql
int ObInternalCmdHandler::encode_err_packet(const int errcode)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_FAIL(reset())) {  // before encode err packet, we need clean buf
    WARN_ICMD("fail to do reset", K(errcode), K(ret));
  } else {
    char *err_msg = NULL;
    if (OB_FAIL(packet::ObProxyPacketWriter::get_err_buf(errcode, err_msg))) {
      WARN_ICMD("fail to get err buf", K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*internal_buf_, seq_, errcode, err_msg))) {
      WARN_ICMD("fail to encode err packet", K(errcode), K(err_msg), K(ret));
    } else {
      INFO_ICMD("succ to encode err packet", K(errcode), K(err_msg));
    }
  }
  
  return ret;
}

int ObInternalCmdHandler::encode_ok_packet(const int64_t affected_rows,
    const obmysql::ObMySQLCapabilityFlags &capability)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*internal_buf_, seq_, affected_rows, capability))) {
    WARN_ICMD("fail to encode ok packet", K(ret));
  } else {
    DEBUG_ICMD("succ to encode ok packet");
  }
  return ret;
}

bool ObInternalCmdHandler::match_like(const char *text, const char *pattern) const
{
  bool ret = false;
  if (OB_ISNULL(pattern) || '\0' == pattern[0]) {
    ret = true;
  } else if (OB_ISNULL(text)) {
    //return false if text config namme is NULL
  } else {
    ObString str_text(text);
    ObString str_pattern(pattern);
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

bool ObInternalCmdHandler::match_like(const ObString &str_text, const char *pattern) const
{
  bool ret = false;
  if (OB_ISNULL(pattern) || '\0' == pattern[0]) {
    ret = true;
  } else if (str_text.empty()) {
    //return false if text config namme is NULL
  } else {
    ObString str_pattern(pattern);
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

bool ObInternalCmdHandler::match_like(const ObString &str_text, const ObString &str_pattern) const
{
  bool ret = false;
  if (str_pattern.empty()) {
    ret = true;
  } else if (str_text.empty()) {
    //return false if text config namme is NULL
  } else {
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

int ObInternalCmdHandler::do_privilege_check(const ObProxySessionPrivInfo &session_priv,
    const ObNeedPriv &need_priv, int &errcode)
{
  int ret = OB_SUCCESS;
  char *priv_name = NULL;
  if (OB_FAIL(ObProxyPrivilegeCheck::check_privilege(session_priv, need_priv, priv_name))) {
    WARN_ICMD("user privilege is not match need privilege, permission denied", K(session_priv),
             K(need_priv), K(priv_name), K(ret));
    errcode = ret;
    if (OB_ISNULL(priv_name)) {
      ret = encode_err_packet(errcode);
    } else {
      ret = encode_err_packet(errcode, priv_name);
    }
    if (OB_FAIL(ret)) {
      WARN_ICMD("fail to encode err resp packet", K(errcode), K(priv_name), K(ret));
    }
  }
  return ret;
}

int ObInternalCmdHandler::do_cs_handler_with_proxy_conn_id(const ObEThread &ethread)
{
  int ret = OB_SUCCESS;
  const uint32_t cs_id = static_cast<uint32_t>(cs_id_);
  ObMysqlClientSessionMap &cs_map = get_client_session_map(ethread);
  ObMysqlClientSession *cs = NULL;

  if (OB_SUCC(cs_map.get(cs_id, cs)) && NULL != cs) {
    //found the specific session
    if (OB_ISNULL(cs_handler_)) {
      ret = OB_INVALID_ARGUMENT;
      WARN_ICMD("cs_handler_ is NULL", K(cs_id), K(ethread.id_), K(ret));
    } else {
      MUTEX_TRY_LOCK(lock, cs->mutex_, this_ethread());
      if (OB_UNLIKELY(!lock.is_locked())) {
        ret = OB_NEED_RETRY;
        WARN_ICMD("fail to try lock cs in cs_map, need retry", K(cs_id), K(ethread.id_), K(ret));
      } else if (OB_FAIL((this->*cs_handler_)(*cs))) {
        WARN_ICMD("fail to handle_cs", K(cs_id), K(ethread.id_), K(ret));
      } else {
        DEBUG_ICMD("succ to handle_cs with proxy conn id", K(cs_id), K(ethread.id_));
      }
    }
  } else {
    int errcode = OB_UNKNOWN_CONNECTION; //not found the specific session
    DEBUG_ICMD("not found the specific session", K(cs_id_), K(errcode));
    if (OB_FAIL(encode_err_packet(errcode, cs_id_))) {
      WARN_ICMD("fail to encode err resp packet", K(errcode), K_(cs_id), K(ret));
    }
  }
  return ret;
}

int ObInternalCmdHandler::do_cs_handler_with_server_conn_id(const ObEThread &ethread, bool &is_finished)
{
  int ret = OB_SUCCESS;
  const uint32_t cs_id = static_cast<uint32_t>(cs_id_);
  ObMysqlClientSessionMap &cs_map = get_client_session_map(ethread);
  CSIDHanders *cs_id_array = get_cs_id_array();
  is_finished = false;
  if (OB_ISNULL(cs_handler_) || OB_ISNULL(cs_id_array)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("cs_handler_ or cs_id_array is NULL", K(cs_id), K(ethread.id_), KP(cs_id_array), K(ret));
  } else if(cs_id_array->empty()) {
    // when we first traverse this thread, we cs_id_array_ must be empty, we need init it
    ObMysqlClientSessionMap::IDHashMap &id_map = cs_map.id_map_;
    cs_id_array->reuse();
    if (OB_FAIL(cs_id_array->reserve(id_map.count()))) {
      WARN_ICMD("fail to reserve cs_id_array", K(ethread.id_), "cs count", id_map.count(), K(ret));
    } else {
      ObMysqlClientSessionMap::IDHashMap::iterator spot = id_map.begin();
      ObMysqlClientSessionMap::IDHashMap::iterator end = id_map.end();
      ObCSIDHandler cs_id_handler;
      for (; OB_SUCC(ret) && spot != end; ++spot) {
        cs_id_handler.cs_id_ = spot->get_cs_id();
        if (OB_FAIL(cs_id_array->push_back(cs_id_handler))) {
          WARN_ICMD("fail to push_back cs_id_array", K(cs_id_handler), K(ethread.id_), K(ret));
        }
      }
    }
  }

  ObMysqlClientSession *cs = NULL;
  int64_t lock_fail_count = 0;
  uint32_t curr_cs_id = 0;
  // traverse all the cs from cs map, if lock failed, inc the lock_fail_count and try next.
  // when finish one traverse, check is_finished and lock_fail_count, do next traverse
  for (int64_t i = 0; OB_SUCC(ret) && !is_finished && i < cs_id_array->count(); ++i) {
    curr_cs_id = cs_id_array->at(i).cs_id_;
    if (cs_id_array->at(i).is_used_) {
      //do nothing
    } else if (OB_FAIL(cs_map.get(curr_cs_id, cs))) {
      if (OB_HASH_NOT_EXIST == ret) {
        //if cs has gone, just treat it as used
        cs_id_array->at(i).is_used_ = true;
        ret = OB_SUCCESS;
      } else {
        WARN_ICMD("fail to get cs from cs map ", K(curr_cs_id), K(ret));
      }
    } else if (OB_ISNULL(cs)) {
      ret = OB_ERR_NULL_VALUE;
      WARN_ICMD("cs is null", K(curr_cs_id), K(ret));
    } else {
      MUTEX_TRY_LOCK(lock, cs->mutex_, this_ethread());
      if (OB_UNLIKELY(!lock.is_locked())) {
        ++lock_fail_count;
        DEBUG_ICMD("fail to try lock cs in cs_map, need retry latter", K(curr_cs_id), K(ethread.id_), K(lock_fail_count));
      } else {
        cs_id_array->at(i).is_used_ = true;
        if (cs->is_hold_conn_id(cs_id)) {
          if (OB_FAIL((this->*cs_handler_)(*cs))) {
            WARN_ICMD("fail to handle_cs", K(cs_id), K(curr_cs_id), K(ethread.id_), K(ret));
          } else {
            is_finished = true;
            DEBUG_ICMD("succ to handle_cs with server conn id", K(cs_id), K(curr_cs_id), K(ethread.id_));
          }
        }//end of is_hold_conn_id
      }//end of locked
    }//end of else
  }//end of for

  if (OB_SUCC(ret) && !is_finished) {
    if (0 == lock_fail_count) {
      cs_id_array->reuse();
    } else {
      ret = OB_NEED_RETRY;
      DEBUG_ICMD("fail to try lock cs in cs_map, need retry", K(cs_id), K(ethread.id_), K(lock_fail_count), K(ret));
    }
  }
  return ret;
}

int ObInternalCmdHandler::handle_cs_with_proxy_conn_id(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread *ethread = NULL;
  bool need_callback = true;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("cur ethread is null, it should not happened", K(ret));
  } else if (OB_FAIL(do_cs_handler_with_proxy_conn_id(*ethread))) {
    if (OB_NEED_RETRY == ret) {
      if (OB_ISNULL(ethread->schedule_in(this, MYSQL_LIST_RETRY))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        ERROR_ICMD("fail to schedule self", K(ethread->id_), K(ret));
      } else {
        need_callback = false;
        DEBUG_ICMD("fail to do do_cs_handler_with_proxy_conn_id, need reschedule", K(ethread->id_), K(ret));
      }
    } else {
      WARN_ICMD("fail to do do_cs_handler_with_proxy_conn_id", K(ethread->id_), K(ret));
    }
  } else {
    DEBUG_ICMD("succ to handle_cs_with_proxy_conn_id", K(ethread->id_));
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

int ObInternalCmdHandler::handle_cs_with_server_conn_id(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  bool need_callback = true;
  bool is_finished = false;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("cur ethread is null, it should not happened", K(ret));
  } else if (OB_FAIL(do_cs_handler_with_server_conn_id(*ethread, is_finished))) {
    if (OB_NEED_RETRY == ret) {
      if (OB_ISNULL(ethread->schedule_in(this, MYSQL_LIST_RETRY))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        ERROR_ICMD("fail to schedule self", K(ethread->id_), K(ret));
      } else {
        need_callback = false;
        DEBUG_ICMD("fail to do do_cs_handler_with_proxy_conn_id, need reschedule", K(ethread->id_), K(ret));
      }
    } else {
      WARN_ICMD("fail to do do_cs_handler_with_server_conn_id", K(is_finished), K(ethread->id_), K(ret));
    }
  } else if (!is_finished) {
    const int64_t next_id = ((ethread->id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (OB_LIKELY(NULL != submit_thread_) && next_id != submit_thread_->id_) {
      DEBUG_ICMD("current thread do not found it", K(submit_thread_->id_), K(next_id));
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        ERROR_ICMD("fail to schedule self", K(next_id), K(ret));
      } else {
        need_callback = false;
      }
    } else {
      int errcode = OB_UNKNOWN_CONNECTION; //not found the specific session
      DEBUG_ICMD("not found the specific session", K(cs_id_), K(errcode));
      if (OB_FAIL(encode_err_packet(errcode, cs_id_))) {
        WARN_ICMD("fail to encode err resp packet", K(errcode), K_(cs_id), K(ret));
      } else { }
    }//end of else
  } else {
    DEBUG_ICMD("succ to handle_cs_with_server_conn_id", K(ethread->id_));
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

int ObInternalCmdHandler::internal_error_callback(int errcode)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_NONE;

  if (OB_FAIL(encode_err_packet(errcode))) {
    WARN_ICMD("fail to encode internal err packet, callback", K(errcode), K(ret));
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_FAILED, NULL);
  } else {
    INFO_ICMD("succ to encode internal err packet, callback", K(errcode));
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }
  return event_ret;
}

int ObInternalCmdHandler::fill_external_buf()
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(protocol_ == ObProxyProtocol::PROTOCOL_OB20)) {
    if (OB_FAIL(ObProto20Utils::consume_and_compress_data(internal_reader_, external_buf_,
                                                          internal_reader_->read_avail(), ob20_param_))) {
      WARN_ICMD("fail to consume and compress to ob20 packet", K(ret), K_(ob20_param));
    } else {
      DEBUG_ICMD("succ to write to client in ob20", K_(ob20_param));
    }
  } else if (protocol_ == ObProxyProtocol::PROTOCOL_NORMAL) {
    // mysql
    int64_t data_size = internal_reader_->read_avail();
    int64_t bytes_written = 0;
    if (OB_FAIL(external_buf_->remove_append(internal_reader_, bytes_written))) {
      ERROR_ICMD("Error while remove_append to external_buf_!", "Attempted size", data_size,
                 "wrote size", bytes_written, K(ret));
    } else if (OB_UNLIKELY(bytes_written != data_size)) {
      ret = OB_ERR_UNEXPECTED;
      WARN_ICMD("unexpected result", "Attempted size", data_size, "wrote size", bytes_written, K(ret));
    } else {
      internal_reader_ = NULL;
      DEBUG_ICMD("succ to write to client in mysql", "Attempted size", bytes_written);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("unexpected protocol type", K(ret), K_(protocol));
  }

  return ret;
}

int ObInternalCmdHandler::handle_callback(int event, void *data)
{
  UNUSED(data);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread *ethread = NULL;
  if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("cur ethread is null, it should not happened", K(ret));
  } else if (OB_ISNULL(submit_thread_)) {
    ret = OB_ERR_NULL_VALUE;
    ERROR_ICMD("submit_thread_ is null", K(ret));
  } else {
    if (saved_event_ < 0) {
      saved_event_ = event;
    }

    if (ethread != submit_thread_) {
      DEBUG_ICMD("Not the same thread, do scheduling", "this_thread", this_ethread()->id_,
                "submit_thread", submit_thread_->id_);
      SET_HANDLER(&ObInternalCmdHandler::handle_callback);
      if (OB_ISNULL(submit_thread_->schedule_imm(this))) {
        ret = OB_ERR_UNEXPECTED;
        ERROR_ICMD("fail to schedule self", "this_ethread", submit_thread_->id_, K(ret));
      } else {
        event_ret = EVENT_CONT;
      }
    } else {
      DEBUG_ICMD("The same thread, directly execute", "this_thread", this_ethread()->id_,
                "submit_thread", submit_thread_->id_, K(event));
      MUTEX_TRY_LOCK(lock, action_.mutex_, submit_thread_);
      if (lock.is_locked()) {
        if (!action_.cancelled_) {
          if (INTERNAL_CMD_EVENTS_SUCCESS == saved_event_
              && OB_SUCCESS != fill_external_buf()) {
            saved_event_ = INTERNAL_CMD_EVENTS_FAILED;
          }
          DEBUG_ICMD("do callback now", K_(saved_event));
          event_ret = action_.continuation_->handle_event(saved_event_, NULL);
        } else {
          INFO_ICMD("action is cancelled, no need callback");
        }
        delete this;
      } else {
        DEBUG_ICMD("lock failed, reschedule it", "this_thread", this_ethread()->id_,
                  "submit_thread", submit_thread_->id_);
        if (OB_ISNULL(submit_thread_->schedule_in(this, MYSQL_LIST_RETRY))) {
          ret = OB_ERR_UNEXPECTED;
          ERROR_ICMD("fail to schedule self", "this_ethread", submit_thread_->id_, K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    ERROR_ICMD("it should not happened, wait for human interference", K(ret));
  }
  return event_ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
