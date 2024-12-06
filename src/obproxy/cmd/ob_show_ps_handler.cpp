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

#define USING_LOG_PREFIX PROXY_CMD

#include "cmd/ob_show_ps_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "utils/ob_proxy_privilege_check.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//PSColumnID
enum
{
  OB_PC_CS_ID = 0,
  OB_PC_CLUSTER_NAME,
  OB_PC_TENANT_NAME,
  OB_PC_PS_ID,
  OB_PC_PS_NAME,
  OB_PC_PREPARE_SQL,
  OB_PC_PARSE_RESULT,
  OB_PC_USED_MEM,
  OB_PC_USED_SESSION,
  OB_PC_MAX_COLUMN_ID
};

const ObProxyColumnSchema SHOW_PS_ARRAY[OB_PC_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_PC_CS_ID,         "cs_id",          OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_PC_CLUSTER_NAME,  "cluster_name",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_TENANT_NAME,   "tenant_name",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_PS_ID,         "ps_id",          OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_PC_PS_NAME,       "ps_name",        OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_PREPARE_SQL,   "prepare_sql",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_PARSE_RESULT,  "parse_result",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_USED_MEM,      "used_mem",       OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_PC_USED_SESSION,  "used_session",   OB_MYSQL_TYPE_LONG),
};

ObShowPSHandler::ObShowPSHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), session_priv_(), need_dump_all_ps_cache_(false),
      sub_type_(info.get_sub_cmd_type()), row_buf_(NULL), tenant_name_(), tenant_str_()
{
  SET_HANDLER(&ObShowPSHandler::main_handler);
  need_output_detail_info_ = (sub_type_ == OBPROXY_T_SUB_PS_ALL);
  need_dump_all_ps_cache_ = (-1 == cs_id_)
                            && (info.get_value_string().empty());

  int32_t min_len = 0;
  if (!info.get_value_string().empty()) {
    min_len =std::min(info.get_value_string().length(), static_cast<int32_t>(OB_MAX_TENANT_NAME_LENGTH - 1));
    MEMCPY(tenant_str_, info.get_value_string().ptr(), static_cast<size_t>(min_len));
    tenant_name_.assign_ptr(tenant_str_, min_len);
  }
  tenant_str_[min_len] = '\0';
}

ObShowPSHandler::~ObShowPSHandler()
{
  if (OB_LIKELY(NULL != row_buf_)) {
    ob_free(row_buf_);
    row_buf_ = NULL;
  }
}

int ObShowPSHandler::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  bool need_callback = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(row_buf_ = static_cast<char *>(ob_malloc(BUF_LEN, ObModIds::OB_PROXY_PRINTF)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for show proxyps", K(ret));
  } else if (OB_FAIL(dump_header())) {
    LOG_WDIAG("fail to dump_list_header", K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("cur ethread is null, it should not happened", K(ret));
  } else {
    need_callback = false;
    if (OB_FAIL(handle_ps_cache(event, data))) {
      LOG_WDIAG("fail to handle ps cache" , K(ret));
    }
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      internal_error_callback(ret);
    } else {
      handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  return EVENT_NONE;
}

int ObShowPSHandler::dump_header()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(encode_header(SHOW_PS_ARRAY, OB_PC_MAX_COLUMN_ID))) {
    LOG_WDIAG("fail to encode header", K(ret));
  }

  return ret;
}

int ObShowPSHandler::handle_ps_cache(int event, void* data)
{
  int ret = OB_SUCCESS;

  bool need_callback = true;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (submit_thread_ != this_ethread()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid submit thread", K(event), K(data), K_(submit_thread), K(ret));
  } else if (-1 != cs_id_) {
    // show ps cache for client session
    need_callback = false;
    if (OB_FAIL(handle_ps_cache_for_cs())) {
      LOG_WDIAG("fail to handle_ps_cache_for_cs", K(ret));
    }
  } else if (!tenant_name_.empty()) {
    // ps cache for tenant
    need_callback = false;
    SET_HANDLER(&ObShowPSHandler::handle_ps_cache_for_tenant);
    if (OB_FAIL(handle_ps_cache_for_tenant(EVENT_IMMEDIATE, this /*useless*/))) {
      LOG_WDIAG("fail to handle_ps_cache_for_tenant", K(ret));
    }
  } else if (!enable_dump_all_ps_cache()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("fail to dump all ps cache for normal user", K(ret));
  } else if (get_global_proxy_config().enable_global_ps_cache) {
    // all ps cache for global
    if (OB_FAIL(handle_ps_cache_all_for_global())) {
      LOG_WDIAG("fail to dump_ps_cache_global", K(ret));
    }
  } else {
    // all ps cache for thread
    need_callback = false;
    SET_HANDLER(&ObShowPSHandler::handle_ps_cache_all_for_thread);
    if (OB_FAIL(handle_ps_cache_all_for_thread(EVENT_IMMEDIATE, this /*useless*/))) {
      LOG_WDIAG("fail to dump_ps_cache_thread", K(ret));
    }
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      internal_error_callback(ret);
    } else {
      handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  return ret;
}

int ObShowPSHandler::handle_ps_cache_for_cs()
{
  int ret = OB_SUCCESS;

  ObEThread *ethread = NULL;
  bool need_callback = true;
  bool is_proxy_conn_id = true;
  if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("cur ethread is null, it should not happened", K(ret));
  } else if (!is_conn_id_avail(cs_id_, is_proxy_conn_id)
             || !is_proxy_conn_id) {
    ret = OB_UNKNOWN_CONNECTION; //not found the specific session
    LOG_WDIAG("cs_id is not avail", K(cs_id_), K(ret));
  } else {
    SET_CS_HANDLER(&ObShowPSHandler::dump_ps_cache_for_cs);
    //connection id got from obproxy
    int64_t thread_id = -1;
    if (OB_FAIL(extract_thread_id(static_cast<uint32_t>(cs_id_), thread_id))) {
      LOG_WDIAG("fail to extract thread id, it should not happen", K(cs_id_), K(ret));
    } else if (thread_id == ethread->id_) {
      need_callback = false;
      handle_cs_with_proxy_conn_id(EVENT_NONE, this /*useless*/);
    } else {
      SET_HANDLER(&ObInternalCmdHandler::handle_cs_with_proxy_conn_id);
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][thread_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        EDIAG_ICMD("fail to schedule self", K(thread_id), K(ret));
      } else {
        need_callback = false;
      }
    }
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      internal_error_callback(ret);
    } else {
      handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  return ret;
}

int ObShowPSHandler::dump_ps_cache_for_cs(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dump_ps_cache_for_one_cs(cs))) {
    LOG_WDIAG("fail to dump_ps_cache_for_one_cs", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    LOG_WDIAG("fail to encode eof packet",  K(ret));
  } else {
    LOG_DEBUG("succ to dump ps cache for cs");
  }

  return ret;
}

int ObShowPSHandler::dump_ps_cache_for_one_cs(ObMysqlClientSession &cs, bool need_check_prvilige/*true*/)
{
  int ret = OB_SUCCESS;

  typedef ObClientSessionInfo::ObPsIdEntryMap ObPsIdEntryMap;
  typedef ObClientSessionInfo::ObTextPsNameEntryMap ObTextPsNameEntryMap;
  ObClientSessionInfo& session_info = cs.get_session_info();
  ObString cs_tenant_name = session_info.get_priv_info().tenant_name_;
  ObString cs_cluster_name = session_info.get_priv_info().cluster_name_;

  if (need_check_prvilige
      && !enable_dump_ps_cache(cs)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("not support to dump this client session", K(ret));
  } else if (!tenant_name_.empty()
             && !is_match_tenant(cs)) {
    LOG_INFO("client session tenant name not match cs id in SQL, ignore it",
              K(cs_tenant_name), K_(tenant_name), K(ret));
  } else if (OB_UNLIKELY(-1 != cs_id_
                         && cs.get_cs_id() != cs_id_)) {
    LOG_WDIAG("client session cs id not match cs id in SQL, ignore it",
              "client session cs_id", cs.get_cs_id(), K_(cs_id), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObPsIdEntryMap::iterator last = session_info.ps_id_entry_map_.end();
    ObPsIdEntryMap::iterator tmp_iter;
    for (ObPsIdEntryMap::iterator ps_iter = session_info.ps_id_entry_map_.begin();
         OB_SUCC(ret) && ps_iter != last;) {
      tmp_iter = ps_iter;
      ++ps_iter;
      const ObPsIdEntry& ps_id_entry = *tmp_iter;
      if (OB_ISNULL(ps_id_entry.ps_entry_)) {
        LOG_WDIAG("empty ps entry, ignore it", K(ps_id_entry));
        if (OB_FAIL(dump_empty_ps_entry())) {
          LOG_WDIAG("fail to dump_empty_ps_entry", K(ret));
        }
      } else if (OB_FAIL(dump_one_ps_entry(*ps_id_entry.ps_entry_, cs_cluster_name, cs_tenant_name,
                                           ps_id_entry.ps_id_, ""))) {
        LOG_WDIAG("fail to dump text ps entry", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTextPsNameEntryMap::iterator last = session_info.text_ps_name_entry_map_.end();
    ObTextPsNameEntryMap::iterator tmp_iter;
    for (ObTextPsNameEntryMap::iterator ps_iter = session_info.text_ps_name_entry_map_.begin();
         OB_SUCC(ret) && ps_iter != last;) {
      tmp_iter = ps_iter;
      ++ps_iter;
      const ObTextPsNameEntry& ps_name_entry = *tmp_iter;
      if (OB_ISNULL(ps_name_entry.text_ps_entry_)) {
        LOG_WDIAG("empty ps entry, ignore it", K(ps_name_entry));
        if (OB_FAIL(dump_empty_ps_entry())) {
          LOG_WDIAG("fail to dump_empty_ps_entry", K(ret));
        }
      } else if (OB_FAIL(dump_one_ps_entry(*ps_name_entry.text_ps_entry_, cs_cluster_name, cs_tenant_name,
                                           -1, ps_name_entry.text_ps_name_))) {
        LOG_WDIAG("fail to dump text ps entry", K(ret));
      }
    }
  }

  return ret;
}

int ObShowPSHandler::handle_ps_cache_for_tenant(int event, void* data)
{
  int ret = OB_SUCCESS;

  bool is_finished = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("cur ethread is null, it should not happened", K(ret));
  } else if (OB_FAIL(dump_ps_cache_for_tenant_in_thread(*ethread))) {
    LOG_WDIAG("fail to do show_cs_list_in_thread", K(ret));
  } else {
    const int64_t next_id = ((ethread->id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (OB_LIKELY(NULL != submit_thread_) && next_id != submit_thread_->id_) {
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        EDIAG_ICMD("fail to schedule self", K(next_id), K(ret));
      } else {
        is_finished = false;
        LOG_DEBUG("succ to reschedule", K(next_id));
      }
    } else {
      if (OB_FAIL(encode_eof_packet())) {
        LOG_WDIAG("fail to encode eof packet", K(ret));
      }
    }
  }

  if (is_finished) {
    if (OB_FAIL(ret)) {
      internal_error_callback(ret);
    } else {
      handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  return ret;
}

int ObShowPSHandler::dump_ps_cache_for_tenant_in_thread(const ObEThread& ethread)
{
  int ret = OB_SUCCESS;

  ObMysqlClientSessionMap::IDHashMap& id_map = get_client_session_map(ethread).id_map_;
  ObMysqlClientSessionMap::IDHashMap::iterator spot = id_map.begin();
  ObMysqlClientSessionMap::IDHashMap::iterator end = id_map.end();
  for (;OB_SUCC(ret) && spot != end; ++spot) {
    // here we only read cs, no need try lock it
    if (OB_UNLIKELY(!is_match_tenant(*spot))) {
      LOG_DEBUG("not match tenant, continue");
    } else if (enable_dump_ps_cache(*spot)) {
      if (OB_FAIL(dump_ps_cache_for_one_cs(*spot, false))) {
        LOG_WDIAG("fail to dump client session", K(spot->get_cs_id()));
      } else {
        LOG_DEBUG("succ to dump client_session", K(spot->get_cs_id()));
      }
    }
  }

  return ret;
}

int ObShowPSHandler::handle_ps_cache_all_for_global()
{
  int ret = OB_SUCCESS;

  typedef proxy::ObBasePsEntryGlobalCache::ObBasePsEntryGlobalMap PsEntryMap;
  const proxy::ObBasePsEntryGlobalCache& ps_entry_cache = get_global_ps_entry_cache();
  const proxy::ObBasePsEntryGlobalCache& text_ps_entry_cache = get_global_text_ps_entry_cache();
  const PsEntryMap& ps_entry_map = ps_entry_cache.get_ps_entry_map();
  const PsEntryMap& text_ps_entry_map = text_ps_entry_cache.get_ps_entry_map();

  if (OB_FAIL(ps_entry_map.traverse_map(ObShowPSHandler::dump_one_ps_entry, this))) {
    LOG_WDIAG("fail to traverse ps_entry_map", K(ret));
  } else if (OB_FAIL(text_ps_entry_map.traverse_map(ObShowPSHandler::dump_one_ps_entry, this))) {
    LOG_WDIAG("fail to traverse ps_entry_map", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    LOG_WDIAG("fail to encode eof packet",  K(ret));
  } else {
    LOG_DEBUG("succ to dump ps cache for cs");
  }

  return ret;
}

int ObShowPSHandler::handle_ps_cache_all_for_thread(int event, void* data)
{
  int ret = OB_SUCCESS;

  bool is_finished = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("cur ethread is null, it should not happened", K(ret));
  } else if (OB_FAIL(dump_ps_cache_all_in_thread(*ethread))) {
    LOG_WDIAG("fail to do show_cs_list_in_thread", K(ret));
  } else {
    const int64_t next_id = ((ethread->id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (OB_LIKELY(NULL != submit_thread_) && next_id != submit_thread_->id_) {
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        EDIAG_ICMD("fail to schedule self", K(next_id), K(ret));
      } else {
        is_finished = false;
      }
    } else {
      if (OB_FAIL(encode_eof_packet())) {
        LOG_WDIAG("fail to encode eof packet", K(ret));
      }
    }
  }

  if (is_finished) {
    if (OB_FAIL(ret)) {
      internal_error_callback(ret);
    } else {
      handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  return ret;
}

int ObShowPSHandler::dump_ps_cache_all_in_thread(const ObEThread& ethread)
{
  int ret = OB_SUCCESS;

  typedef proxy::ObBasePsEntryThreadCache::ObBasePsEntryMap PsEntryMap;
  proxy::ObBasePsEntryThreadCache& ps_entry_cache = ethread.get_ps_entry_cache();
  proxy::ObBasePsEntryThreadCache& text_ps_entry_cache = ethread.get_text_ps_entry_cache();
  PsEntryMap& ps_entry_map = ps_entry_cache.get_ps_entry_map();
  PsEntryMap& text_ps_entry_map = text_ps_entry_cache.get_ps_entry_map();

  {
    PsEntryMap::iterator last = ps_entry_map.end();
    PsEntryMap::iterator tmp_iter;
    for (PsEntryMap::iterator ps_iter = ps_entry_map.begin();
         OB_SUCC(ret) && ps_iter != last;) {
      tmp_iter = ps_iter;
      ++ps_iter;
      ObBasePsEntry& ps_entry = *tmp_iter;
      if (OB_FAIL(dump_one_ps_entry(ps_entry, "", "", -1, ""))) {
        LOG_WDIAG("fail to dump text ps entry", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    PsEntryMap::iterator last = text_ps_entry_map.end();
    PsEntryMap::iterator tmp_iter;
    for (PsEntryMap::iterator ps_iter = text_ps_entry_map.begin();
         OB_SUCC(ret) && ps_iter != last;) {
      tmp_iter = ps_iter;
      ++ps_iter;
      ObBasePsEntry& ps_entry = *tmp_iter;
      if (OB_FAIL(dump_one_ps_entry(ps_entry, "", "", -1, ""))) {
        LOG_WDIAG("fail to dump text ps entry", K(ret));
      }
    }
  }

  return ret;
}

int ObShowPSHandler::dump_one_ps_entry(ObBasePsEntry& ps_entry, const ObString& cluster_name,
                                       const ObString& tenant_name,
                                       int64_t ps_id, const ObString& ps_name)
{
  int ret = OB_SUCCESS;

  if (!like_name_.empty()
      && !common::match_like(ps_entry.get_base_ps_sql(), like_name_)) {
    LOG_DEBUG("not match sql, continue", "ps entry SQL", ps_entry.get_base_ps_sql(), K(like_name_));
  } else {
    ObNewRow row;
    ObObj cells[OB_PC_MAX_COLUMN_ID];
    cells[OB_PC_CS_ID].set_int(cs_id_);
    cells[OB_PC_CLUSTER_NAME].set_varchar(cluster_name);
    cells[OB_PC_TENANT_NAME].set_varchar(tenant_name);
    cells[OB_PC_PS_ID].set_int(ps_id);
    cells[OB_PC_PS_NAME].set_varchar(ps_name);
    cells[OB_PC_PREPARE_SQL].set_null();
    cells[OB_PC_PARSE_RESULT].set_null();
    cells[OB_PC_USED_MEM].set_int(ps_entry.get_mem_used());
    cells[OB_PC_USED_SESSION].set_int(ps_entry.ref_count_);

    if (need_output_detail_info_) {
      cells[OB_PC_PREPARE_SQL].set_varchar(ps_entry.base_ps_sql_);
      if (OB_ISNULL(row_buf_)) {
        LOG_WDIAG("null row_buf_", K_(cs_id), K(tenant_name));
      } else {
        int write_len = 0;
        write_len = ps_entry.get_base_ps_parse_result().to_string(row_buf_, BUF_LEN);
        cells[OB_PC_PARSE_RESULT].set_varchar(row_buf_, write_len);
      }
    }

    row.cells_ = cells;
    row.count_ = OB_PC_MAX_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      LOG_WDIAG("fail to encode row packet", K(row), K(ret));
    }
  }

  return ret;
}

int ObShowPSHandler::dump_one_ps_entry(ObBasePsEntry& ps_entry, va_list args)
{
  int ret = OB_SUCCESS;

  ObShowPSHandler* ps_handler = va_arg(args, ObShowPSHandler*);
  if (OB_ISNULL(ps_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected ps_handler", K(ps_handler), K(ret));
  } else if (!ps_handler->like_name_.empty()
             && !common::match_like(ps_entry.get_base_ps_sql(), ps_handler->like_name_)) {
    LOG_DEBUG("not match sql, continue", "ps entry SQL", ps_entry.get_base_ps_sql(), "like_name", ps_handler->like_name_);
  } else {
    ObNewRow row;
    ObObj cells[OB_PC_MAX_COLUMN_ID];
    cells[OB_PC_CS_ID].set_int(-1);
    cells[OB_PC_CLUSTER_NAME].set_varchar("");
    cells[OB_PC_TENANT_NAME].set_varchar("");
    cells[OB_PC_PS_ID].set_int(-1);
    cells[OB_PC_PS_NAME].set_varchar("");
    cells[OB_PC_PREPARE_SQL].set_null();
    cells[OB_PC_PARSE_RESULT].set_null();
    cells[OB_PC_USED_MEM].set_int(ps_entry.get_mem_used());
    cells[OB_PC_USED_SESSION].set_int(ps_entry.ref_count_);

    if (ps_handler->need_output_detail_info_) {
      // max print sql len is digest_sql_length
      cells[OB_PC_PREPARE_SQL].set_varchar(ps_entry.base_ps_sql_.ptr(),
            min(ps_entry.base_ps_sql_.length(), get_global_proxy_config().digest_sql_length));
      if (OB_ISNULL(ps_handler->row_buf_)) {
        LOG_WDIAG("null row_buf_", KP(ps_handler));
      } else {
        int write_len = 0;
        write_len = ps_entry.get_base_ps_parse_result().to_string(ps_handler->row_buf_, BUF_LEN);
        cells[OB_PC_PARSE_RESULT].set_varchar(ps_handler->row_buf_, write_len);
      }
    }

    row.cells_ = cells;
    row.count_ = OB_PC_MAX_COLUMN_ID;
    if (OB_FAIL(ps_handler->encode_row_packet(row))) {
      LOG_WDIAG("fail to encode row packet", K(row), K(ret));
    }
  }

  return ret;
}

int ObShowPSHandler::dump_empty_ps_entry()
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_PC_MAX_COLUMN_ID];
  cells[OB_PC_CS_ID].set_int(-1);
  cells[OB_PC_CLUSTER_NAME].set_varchar("");
  cells[OB_PC_TENANT_NAME].set_varchar("");
  cells[OB_PC_PS_ID].set_int(-1);
  cells[OB_PC_PS_NAME].set_varchar("");
  cells[OB_PC_PREPARE_SQL].set_null();
  cells[OB_PC_PARSE_RESULT].set_null();
  cells[OB_PC_USED_MEM].set_int(0);
  cells[OB_PC_USED_SESSION].set_int(0);

  row.cells_ = cells;
  row.count_ = OB_PC_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    LOG_WDIAG("fail to encode row packet", K(row), K(ret));
  }

  return ret;
}

bool ObShowPSHandler::is_match_tenant(const ObMysqlClientSession &cs) const
{
  bool b_ret = false;

  const ObProxySessionPrivInfo &other_priv_info = cs.get_session_info().get_priv_info();
  if (!cs.get_session_info().is_sharding_user()) {
    b_ret = common::match_like(other_priv_info.tenant_name_, tenant_name_);
  } else {
    b_ret = common::match_like(other_priv_info.logic_user_name_, tenant_name_);
  }

  return b_ret;
}

// user can see PS cache if in these condistions
// 1. proxysys user for PS cache of all client session
// 2. sys tenant for PS cache of all client session
// 3. same user can see PS cache of self
bool ObShowPSHandler::enable_dump_ps_cache(const ObMysqlClientSession &cs) const
{
  const ObProxySessionPrivInfo &other_priv_info = cs.get_session_info().get_priv_info();
  return  session_priv_.has_all_privilege_
          || session_priv_.tenant_name_ == OB_SYS_TENANT_NAME
          || ((!cs.get_session_info().is_sharding_user() && session_priv_.is_same_tenant(other_priv_info))
              && (session_priv_.cs_id_ == cs.get_cs_id())
              && (session_priv_.is_same_user(other_priv_info) || session_priv_.has_process_privilege()))
          || (cs.get_session_info().is_sharding_user() && session_priv_.is_same_logic_user(other_priv_info));
}

bool ObShowPSHandler::enable_dump_all_ps_cache() const
{
  return session_priv_.has_all_privilege_
         || session_priv_.tenant_name_ == OB_SYS_TENANT_NAME;
}

static int show_ps_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
                                ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowPSHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowPSHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowPSHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    LOG_WDIAG("fail to init for ObShowPSHandler");
  } else if (OB_FAIL(handler->session_priv_.deep_copy(info.session_priv_))) {
    LOG_WDIAG("fail to deep copy session priv");
  } else if (OB_ISNULL(this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected ethread", K(ret));
  } else {
    action = &handler->get_action();
    // must schedule start from submit thread
    if (OB_ISNULL(this_ethread()->schedule_imm(handler, EVENT_IMMEDIATE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObShowPSHandler", K(ret));
      action = NULL;
    } else {
      LOG_DEBUG("succ to schedule ObShowPSHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }

  return ret;
}

int show_ps_cmd_init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_PS,
                                                               &show_ps_cmd_callback))) {
    LOG_WDIAG("fail to register CMD_TYPE_SM", K(ret));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
