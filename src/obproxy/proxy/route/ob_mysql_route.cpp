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
#include "iocore/eventsystem/ob_event.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "proxy/route/ob_mysql_route.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/route/ob_routine_processor.h"
#include "proxy/route/ob_partition_processor.h"
#include "proxy/route/ob_table_processor.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "opsql/parser/ob_proxy_parser.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define MYSQL_ROUTE_SET_DEFAULT_HANDLER(h) { default_handler_ = h; }

int64_t ObRouteParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(cr_version),
       K_(cr_id),
       K_(name),
       K_(force_renew),
       K_(use_lower_case_name),
       KP_(mysql_proxy),
       KP_(client_request),
       KP_(client_info),
       K_(is_partition_table_route_supported),
       K_(need_pl_route),
       K_(is_oracle_mode),
       K_(is_need_force_flush),
       K_(tenant_version),
       K_(timeout_us),
       K_(current_idc_name));
  J_OBJ_END();
  return pos;
}

void ObRouteParam::set_cluster_resource(obutils::ObClusterResource *cr)
{
  if (NULL != cr_) {
    cr_->dec_ref();
    cr_ = NULL;
  }
  if (NULL != cr) {
    cr->inc_ref();
    cr_ = cr;
  }
}

ObMysqlRoute::ObMysqlRoute()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE), param_(), name_buf_(NULL),
    name_buf_len_(0), submit_thread_(NULL), action_(), pending_action_(NULL),
    timeout_action_(NULL), routine_entry_(NULL), table_entry_(NULL), part_entry_(NULL),
    next_action_(ROUTE_ACTION_UNDEFINED), is_routine_entry_from_remote_(false),
    is_table_entry_from_remote_(false), is_part_entry_from_remote_(false),
    is_routine_entry_lookup_succ_(true), is_route_sql_parse_succ_(true),
    is_table_entry_lookup_succ_(true), is_part_id_calc_succ_(true),
    is_part_entry_lookup_succ_(true), terminate_route_(false),
    part_id_(OB_INVALID_INDEX), route_sql_result_(), reentrancy_count_(0)
{
  SET_HANDLER(&ObMysqlRoute::main_handler);
  MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_route_start);
}

int ObMysqlRoute::main_handler(int event, void *data)
{
  int event_ret = EVENT_CONT;
  if (OB_UNLIKELY(OB_CONT_MAGIC_ALIVE != magic_ || reentrancy_count_ < 0)) {
    LOG_ERROR("invalid route magic or reentrancy_count", K_(magic), K_(reentrancy_count));
  }
  ++reentrancy_count_;

  LOG_DEBUG("[ObMysqlRoute::main_handler]", "next action", get_action_str(next_action_),
            "event", event, "ethread", this_ethread(), "tid", this_ethread()->tid_);


  if (action_.cancelled_) {
    terminate_route_ = true;
  } else if (EVENT_INTERVAL == event) {
    set_state_and_call_next(ROUTE_ACTION_TIMEOUT);
  } else {
    if (OB_ISNULL(default_handler_)) {
      LOG_ERROR("invalid internal state, default handler is NULL",
                "next action", get_action_str(next_action_), K(event),
                K(data), K_(reentrancy_count));
    } else {
      (this->*default_handler_)(event, data);
    }
  }

  if (terminate_route_ && (1 == reentrancy_count_)) {
    kill_this();
    event_ret = EVENT_DONE;
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count));
    }
  }

  return event_ret;
}

inline void ObMysqlRoute::kill_this()
{
  if (OB_UNLIKELY(1 != reentrancy_count_)) {
    LOG_ERROR("invalid internal state, reentrancy_count should be 1",
              K_(reentrancy_count));
  }

  LOG_DEBUG("ObMysqlRoute will be free", K_(param_.name), K(this));

  MYSQL_ROUTE_SET_DEFAULT_HANDLER(NULL);
  param_.reset();
  if (NULL != name_buf_) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }

  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WARN("fail to cancel timeout action", K(ret));
  }

  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  }

  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }

  if (NULL != routine_entry_) {
    routine_entry_->dec_ref();
    routine_entry_ = NULL;
  }

  if (NULL != part_entry_) {
    part_entry_->dec_ref();
    part_entry_ = NULL;
  }

  action_.set_continuation(NULL);
  submit_thread_ = NULL;
  mutex_.release();
  magic_ = OB_CONT_MAGIC_DEAD;

  free_route(this);
}

int ObMysqlRoute::state_route_start(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  pending_action_ = NULL;

  if (param_.need_pl_route_) {
    set_state_and_call_next(ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_START);
  } else {
    set_state_and_call_next(ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START);
  }
  return EVENT_CONT;
}

inline void ObMysqlRoute::setup_routine_entry_lookup()
{
  MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_routine_entry_lookup);
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObMysqlRoute::setup_routine_entry_lookup");
  ObRoutineParam routine_param;
  if (OB_FAIL(ObRouteUtils::convert_route_param_to_routine_param(param_, routine_param))) {
    LOG_WARN("fail to convert", K_(param), K(ret));
  } else {
    routine_param.cont_ = this;
    ret = ObRoutineProcessor::get_routine_entry(routine_param, pending_action_);
  }

  if (OB_SUCC(ret)) {
    if (NULL == pending_action_) {
      handle_event(ROUTINE_ENTRY_LOOKUP_CACHE_DONE, &routine_param.result_);
      routine_param.result_.reset();
    } else {
      // wait callback
    }
  } else {
    LOG_WARN("fail to get routine entry", K(routine_param), K(ret));
    is_table_entry_lookup_succ_ = false;
    set_state_and_call_next(ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_DONE);
  }
}

int ObMysqlRoute::state_routine_entry_lookup(int event, void *data)
{
  LOG_DEBUG("ObMysqlRoute::state_routine_entry_lookup");
  int ret = OB_SUCCESS;
  pending_action_ = NULL;

  if (OB_UNLIKELY(ROUTINE_ENTRY_LOOKUP_CACHE_DONE != event) || OB_ISNULL(data)) {
    is_routine_entry_lookup_succ_ = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected event type, it should not happen", K(event), K(data), K(ret));
  } else {
    ObRoutineResult *result = reinterpret_cast<ObRoutineResult *>(data);
    // hand over the ref
    if (NULL != routine_entry_) {
      routine_entry_->dec_ref();
      routine_entry_ = NULL;
    }
    routine_entry_ = result->target_entry_;
    result->target_entry_ = NULL;
    is_routine_entry_from_remote_ = result->is_from_remote_;

    LOG_DEBUG("ObMysqlRoute get routine entry succ", KPC_(routine_entry));
  }

  set_state_and_call_next(ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_DONE);

  return EVENT_DONE;
}

inline void ObMysqlRoute::handle_routine_entry_lookup_done()
{
  int ret = OB_SUCCESS;
  if (!is_routine_entry_lookup_succ_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlRoute::handle_routine_entry_lookup_done] fail to lookup routine entry",
             "routine name", param_.name_, K(ret));
  } else {
    // table entry lookup succ
    if (NULL != routine_entry_
        && !routine_entry_->get_route_sql().empty()) {
      set_state_and_call_next(ROUTE_ACTION_ROUTE_SQL_PARSE_START);
    } else {
      LOG_DEBUG("can not find avai route sql, use default route",
               "routine name", param_.name_, K(ret));
      //if empty route sql, use __all_dummy instead
      param_.name_.package_name_.reset();
      rewrite_route_names(OB_SYS_DATABASE_NAME, share::OB_ALL_DUMMY_TNAME);
      set_state_and_call_next(ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START);
    }
  }
  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

inline void ObMysqlRoute::rewrite_route_names(const common::ObString &db_name, const common::ObString &table_name)
{
  if (table_name.empty()
      ||table_name == ObString::make_string(share::OB_ALL_DUMMY_TNAME)) {
    param_.name_.database_name_ = ObString::make_string(OB_SYS_DATABASE_NAME);
    param_.name_.table_name_ = ObString::make_string(share::OB_ALL_DUMMY_TNAME);
  } else {
    param_.name_.table_name_ = table_name;
    if (!db_name.empty()) {
      param_.name_.database_name_ = db_name;
    }
  }

  const int64_t name_buf_len = param_.name_.get_total_str_len();
  char *name_buf = static_cast<char *>(op_fixed_mem_alloc(name_buf_len));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K_(name_buf_len), K(ret));
  } else if (OB_FAIL(param_.name_.deep_copy(param_.name_, name_buf, name_buf_len))) {
    LOG_WARN("fail to deep copy table entry name", K(ret));
  } else {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = name_buf;
    name_buf_len_ = name_buf_len;
  }
}

inline void ObMysqlRoute::setup_route_sql_parse()
{
  MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_route_sql_parse);
  int ret = OB_SUCCESS;
  if (NULL != routine_entry_ && !routine_entry_->get_route_sql().empty()) {
    ObArenaAllocator *allocator = NULL;
    if (OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator))) {
      LOG_WARN("fail to get parse allocator", K(ret));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else {
      ObProxyParser obproxy_parser(*allocator, NORMAL_PARSE_MODE);
      ObProxyParseResult obproxy_parse_result;
      int tmp_ret = OB_SUCCESS;
      const common::ObString &parse_sql = ObProxyMysqlRequest::get_parse_sql(routine_entry_->get_route_sql());
      // Because it is a sys tenant, the default UTF8 character set can be used
      if (OB_SUCCESS != (tmp_ret = obproxy_parser.parse(parse_sql,
                                                        obproxy_parse_result,
                                                        CS_TYPE_UTF8MB4_GENERAL_CI))) {
        LOG_INFO("fail to parse sql, will go on anyway", K(parse_sql), K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = route_sql_result_.load_result(obproxy_parse_result,
                                                                        param_.use_lower_case_name_))) {
        LOG_INFO("fail to load result, will go on anyway", K(parse_sql),
                 "use_lower_case_name", param_.use_lower_case_name_, K(tmp_ret));
      } else {
        LOG_DEBUG("success to do proxy parse", K(parse_sql), K(route_sql_result_));

        param_.name_.package_name_.reset();
        ObString table_name = route_sql_result_.get_table_name();
        ObString database_name = route_sql_result_.get_database_name();

        if (param_.is_oracle_mode_) {
          if (!table_name.empty() && OBPROXY_QUOTE_T_INVALID == route_sql_result_.get_table_name_quote()) {
            string_to_upper_case(table_name.ptr(), table_name.length());
          }

          if (!database_name.empty() && OBPROXY_QUOTE_T_INVALID == route_sql_result_.get_database_name_quote()) {
            string_to_upper_case(database_name.ptr(), database_name.length());
          }
        }

        if (database_name.empty() && routine_entry_->is_package_database()) {
          rewrite_route_names(routine_entry_->get_package_name(), table_name);
        } else {
          rewrite_route_names(database_name, table_name);
        }
      }
      allocator->reuse();
    }
    set_state_and_call_next(ROUTE_ACTION_ROUTE_SQL_PARSE_DONE);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this routine entry is available, should not arrive here", KPC_(routine_entry), K(ret));
  }

  if (OB_FAIL(ret)) {
    is_route_sql_parse_succ_ = false;
    set_state_and_call_next(ROUTE_ACTION_ROUTE_SQL_PARSE_DONE);
  }
}

int ObMysqlRoute::state_route_sql_parse(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  return EVENT_DONE;
}

inline void ObMysqlRoute::handle_route_sql_parse_done()
{
  int ret = OB_SUCCESS;
  if (!is_route_sql_parse_succ_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlRoute::handle_route_sql_parse_done] fail to parse route sql", K(ret));
  } else {
    set_state_and_call_next(ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START);
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

inline void ObMysqlRoute::setup_table_entry_lookup()
{
  MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_table_entry_lookup);

  LOG_DEBUG("ObMysqlRoute::setup_table_entry_lookup");
  ObTableProcessor &table_processor = get_global_table_processor();
  int ret = OB_SUCCESS;
  ObTableRouteParam table_param;
  ObAction *table_entry_action_handle = NULL;
  if (OB_FAIL(ObRouteUtils::convert_route_param_to_table_param(param_, table_param))) {
    LOG_WARN("fail to convert", K_(param), K(ret));
  } else {
    table_param.cont_ = this;
    ret = table_processor.get_table_entry(table_param, table_entry_action_handle);
  }

  if (OB_SUCC(ret)) {
    if (NULL != table_param.result_.target_entry_) {
      ObTableEntry* entry = table_param.result_.target_entry_;
      if (param_.tenant_version_ != entry->get_tenant_version()
           && entry->is_avail_state()
           && !entry->is_dummy_entry()
           && entry->get_time_for_expired() == 0) {
        LOG_INFO("tenant locality change, set table entry time expired",
            K_(param_.tenant_version), K(entry->get_tenant_version()), KPC(entry));
        entry->set_time_for_expired(ObRandomNumUtils::get_random_half_to_full(60*1000*1000)
            + common::ObTimeUtility::current_time());
      }
    }

    if (NULL == table_entry_action_handle) { // cache hit
      handle_event(TABLE_ENTRY_EVENT_LOOKUP_DONE, &table_param.result_);
      table_param.result_.reset();
    } else {
      pending_action_ = table_entry_action_handle;
    }
  } else {
    LOG_WARN("fail to get table entry", K(table_param), K(ret));
    is_table_entry_lookup_succ_ = false;
    set_state_and_call_next(ROUTE_ACTION_TABLE_ENTRY_LOOKUP_DONE);
  }
}

int ObMysqlRoute::state_table_entry_lookup(int event, void *data)
{
  LOG_DEBUG("ObMysqlRoute::state_table_entry_lookup");
  int ret = OB_SUCCESS;
  pending_action_ = NULL;

  if (OB_UNLIKELY(TABLE_ENTRY_EVENT_LOOKUP_DONE != event) || OB_ISNULL(data)) {
    is_table_entry_lookup_succ_ = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected event type, it should not happen", K(event), K(data), K(ret));
  } else {
    ObRouteResult *result = reinterpret_cast<ObRouteResult *>(data);
    // hand over the ref count
    table_entry_ = result->target_entry_;
    if (false == param_.is_need_force_flush_) {
      param_.is_need_force_flush_ = result->is_need_force_flush_;
    }
    result->target_entry_ = NULL;
    is_table_entry_from_remote_ = result->is_from_remote_;
    // use different log level only for debug
    if (NULL != table_entry_ && (table_entry_->is_avail_state())) {
      LOG_DEBUG("ObMysqlRoute get table entry succ", K(param_.name_),
          KPC_(table_entry), K_(is_table_entry_from_remote));
    } else {
      LOG_INFO("ObMysqlRoute get table entry succ", K(param_.name_),
          KPC_(table_entry), K_(is_table_entry_from_remote));
    }
  }
  set_state_and_call_next(ROUTE_ACTION_TABLE_ENTRY_LOOKUP_DONE);

  return EVENT_DONE;
}

inline void ObMysqlRoute::handle_table_entry_lookup_done()
{
  int ret = OB_SUCCESS;
  if (!is_table_entry_lookup_succ_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlRoute::handle_table_entry_lookup_done] fail to lookup table entry",
             "table entry name", param_.name_, K(ret));
  } else {
    // table entry lookup succ
    if (NULL != table_entry_
        && table_entry_->is_partition_table()
        && param_.is_partition_table_route_supported_) {
      set_state_and_call_next(ROUTE_ACTION_PARTITION_ID_CALC_START);
    } else {
      set_state_and_call_next(ROUTE_ACTION_NOTIFY_OUT);
    }
  }
  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

inline int ObMysqlRoute::check_and_rebuild_call_params()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_.client_request_) || OB_ISNULL(param_.client_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("client_request_ or client_info_ should not be null", K(param_), K(ret));
  } else if (OB_UNLIKELY(!param_.client_request_->get_parse_result().call_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("call_info is invaild, we'd better ignore accurate route", "call_info",
              param_.client_request_->get_parse_result().call_info_, K(ret));
  } else {
    ObProxyCallInfo &call_info = param_.client_request_->get_parse_result().call_info_;
    ObClientSessionInfo &client_info = *param_.client_info_;
    ObSessionSysField *sys_filed = NULL;
    ObSessionUserField *user_filed = NULL;
    for (int32_t i = 0; OB_SUCC(ret) && i < call_info.param_count_; ++i) {
      ObProxyCallParam &call_param = call_info.params_.at(i);
      if (CALL_TOKEN_SYS_VAR == call_param.type_) {
        sys_filed = NULL;
        if (OB_FAIL(client_info.get_sys_variable(call_param.str_value_.string_, sys_filed))) {
          LOG_INFO("fail to find sys variables", "name", call_param.str_value_.string_, K(ret));
        } else if (NULL != sys_filed) {
          char *buf = call_param.str_value_.buf_;
          const int64_t max_size = call_param.str_value_.get_max_size();
          int64_t pos = 0;
          if (OB_FAIL(sys_filed->value_.print_sql_literal(buf, max_size, pos))) {
            LOG_INFO("fail to print sql literal", K(pos), K(i), KPC(sys_filed), K(ret));
          } else {
            ObString new_value(pos, buf);
            call_param.str_value_.set(new_value);
          }
        }
      } else if (CALL_TOKEN_USER_VAR == call_param.type_) {
        user_filed = NULL;
        if (OB_FAIL(client_info.get_user_variable(call_param.str_value_.string_, user_filed))) {
          LOG_INFO("fail to find sys variables, ignore", "name", call_param.str_value_.string_, K(ret));
        } else if (NULL != user_filed) {
          char *buf = call_param.str_value_.buf_;
          const int64_t max_size = call_param.str_value_.get_max_size();
          int64_t pos = 0;
          if (OB_FAIL(user_filed->value_.print_plain_str_literal(buf, max_size, pos))) {
            LOG_INFO("fail to print sql literal, ignore", K(pos), K(i), KPC(user_filed), K(ret));
          } else {
            ObString new_value(pos, buf);
            call_param.str_value_.set(new_value);
          }
        }
      }
    }
  }
  return ret;
}

inline void ObMysqlRoute::setup_partition_id_calc()
{
  // 1. fast parse
  // 2. get calc expr
  // 3. calc the partition id

  MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_partition_id_calc);
  int ret = OB_SUCCESS;

  part_id_ = OB_INVALID_INDEX;
  if (NULL != table_entry_ && table_entry_->is_partition_table()) {
    ObProxyPartInfo *part_info = NULL;
    obutils::ObSqlParseResult *result = NULL;
    ObString user_sql;
    ObArenaAllocator *allocator = NULL;
    if (NULL == (part_info = table_entry_->get_part_info())) {
      LOG_DEBUG("fail to get part info, maybe not support partition table routing");
    } else if (part_info->has_unknown_part_key()) {
      LOG_DEBUG("some part key type is unsupported now", KPC(part_info), K(ret));
    } else if (OB_ISNULL(param_.client_request_)
               || OB_ISNULL(param_.client_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client request  and client_info_ should not be null here", K(ret));
    } else if (OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator))) {
      LOG_WARN("fail to get parse allocator", K(ret));
    } else if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (param_.need_pl_route_ && NULL != routine_entry_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = check_and_rebuild_call_params()))) {
        LOG_INFO("fail to check_and_rebuild_call_params, just use tenant server", K(tmp_ret));
      } else {
        // for call stmt, here result is parse_result for the first sql in the function,
        // parse_result in client_request is for the original "call xxx()"
        user_sql = routine_entry_->get_route_sql();
        result = &route_sql_result_;
      }
    } else {
      result = &param_.client_request_->get_parse_result();
      if (obmysql::OB_MYSQL_COM_STMT_EXECUTE == param_.client_request_->get_packet_meta().cmd_) {
        if (OB_FAIL(param_.client_info_->get_ps_sql(user_sql))) {
          LOG_WARN("fail to get ps sql", K(ret));
          ret = OB_SUCCESS;
        }
      } else if (result->is_text_ps_execute_stmt()) {
        if (OB_FAIL(param_.client_info_->get_text_ps_sql(user_sql))) {
          LOG_WARN("fail to get text ps sql", K(ret));
          ret = OB_SUCCESS;
        }
      } else {
        user_sql = param_.client_request_->get_sql();
      }
    }

    if (OB_SUCC(ret) && !user_sql.empty()) {
      //parse sql
      ObProxyExprCalculator expr_calculator;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = expr_calculator.calculate_partition_id(*allocator,
                                                                          user_sql,
                                                                          *result,
                                                                          *param_.client_request_,
                                                                          *param_.client_info_,
                                                                          *part_info,
                                                                          part_id_))) {
        LOG_INFO("fail to calculate partition id, just use tenant server", K(tmp_ret));
      } else {
        LOG_DEBUG("succ to calculate partition id", K(part_id_));
      }
    }

    if (NULL != allocator) {
      allocator->reuse();
    }

    set_state_and_call_next(ROUTE_ACTION_PARTITION_ID_CALC_DONE);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this table entry is non-partition table", KPC_(table_entry), K(ret));
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

int ObMysqlRoute::state_partition_id_calc(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  return EVENT_DONE;
}

inline void ObMysqlRoute::handle_partition_id_calc_done()
{
  int ret = OB_SUCCESS;
  if (!is_part_id_calc_succ_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlRoute::handle_partition_id_calc_done] fail to calc partition id", K(ret));
  } else {
    // we will come here in these cases:
    // 1. the function of expr we do not support
    // 2. not support part table routing
    // 3. the sql do not contains part key
    if (common::OB_INVALID_INDEX == part_id_) {
      // just inform out
      set_state_and_call_next(ROUTE_ACTION_NOTIFY_OUT);
    } else {
      // begin to get partition entry
      set_state_and_call_next(ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_START);
    }
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

inline void ObMysqlRoute::setup_partition_entry_lookup()
{
  LOG_DEBUG("ObMysqlRoute::setup_partition_entry_lookup");
  MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_partition_entry_lookup);
  int ret = OB_SUCCESS;
  // get partition entry from cache or remote
  ObPartitionParam part_param;
  part_param.cont_ = this;
  part_param.partition_id_ = part_id_;
  part_param.force_renew_ = param_.force_renew_;
  part_param.is_need_force_flush_ = param_.is_need_force_flush_;
  part_param.set_table_entry(table_entry_);
  part_param.mysql_proxy_ = param_.mysql_proxy_;
  part_param.tenant_version_ = param_.tenant_version_;
  if (!param_.current_idc_name_.empty()) {
    MEMCPY(part_param.current_idc_name_buf_, param_.current_idc_name_.ptr(), param_.current_idc_name_.length());
    part_param.current_idc_name_.assign_ptr(part_param.current_idc_name_buf_, param_.current_idc_name_.length());
  } else {
    part_param.current_idc_name_.reset();
  }

  ret = ObPartitionProcessor::get_partition_entry(part_param, pending_action_);
  if (OB_SUCC(ret)) {
    if (NULL == pending_action_) {
      handle_event(PARTITION_ENTRY_LOOKUP_CACHE_DONE, &part_param.result_);
      part_param.result_.reset();
    } else {
      // wait callback
    }
  } else {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

int ObMysqlRoute::state_partition_entry_lookup(int event, void *data)
{
  LOG_DEBUG("ObMysqlRoute::state_partition_entry_lookup");
  int ret = OB_SUCCESS;
  pending_action_ = NULL;

  if (OB_UNLIKELY(PARTITION_ENTRY_LOOKUP_CACHE_DONE != event) || OB_ISNULL(data)) {
    is_part_entry_lookup_succ_ = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected event type, it should not happen", K(event), K(data), K(ret));
  } else {
    ObPartitionResult *result = reinterpret_cast<ObPartitionResult *>(data);
    // hand over the ref
    if (NULL != part_entry_) {
      part_entry_->dec_ref();
      part_entry_ = NULL;
    }
    part_entry_ = result->target_entry_;
    result->target_entry_ = NULL;
    is_part_entry_from_remote_ = result->is_from_remote_;

    if (NULL != part_entry_ && part_entry_->is_avail_state()
         && param_.tenant_version_ != part_entry_->get_tenant_version()
         && part_entry_->get_time_for_expired() == 0) {
      LOG_DEBUG("tenant locality change, set partition entry time expired",
          K(param_.tenant_version_), K(part_entry_->get_tenant_version()), KPC_(part_entry));
      part_entry_->set_time_for_expired(ObRandomNumUtils::get_random_half_to_full(60*1000*1000)
            + common::ObTimeUtility::current_time());
    }

    if (NULL != part_entry_ && (part_entry_->is_avail_state())) {
      LOG_DEBUG("ObMysqlRoute get partition entry succ", K(param_.name_),
          KPC_(part_entry), K_(is_part_entry_from_remote));
    } else {
      LOG_INFO("ObMysqlRoute get partition entry succ", K(param_.name_),
          KPC_(part_entry), K_(is_part_entry_from_remote));
    }
  }

  set_state_and_call_next(ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_DONE);

  return EVENT_DONE;
}

inline void ObMysqlRoute::handle_partition_entry_lookup_done()
{
  int ret = OB_SUCCESS;
  if (!is_part_entry_lookup_succ_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlRoute::handle_partition_entry_lookup_done] fail to lookup part entry", K(ret));
  } else {
    set_state_and_call_next(ROUTE_ACTION_NOTIFY_OUT);
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
}

inline void ObMysqlRoute::notify_caller()
{
  int ret = OB_SUCCESS;
  if (this_ethread() == submit_thread_) {
    ObContinuation *cont = action_.continuation_;
    if (NULL != cont) {
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel active timeout", K(ret));
      } else {
        if (!action_.cancelled_) {
          if (NULL != param_.result_.table_entry_) {
            LOG_ERROR("table entry must be NULL here", KPC_(param_.result_.table_entry));
            param_.result_.table_entry_->dec_ref();
            param_.result_.table_entry_ = NULL;
          }
          if (NULL != param_.result_.part_entry_) {
            LOG_ERROR("part entry must be NULL here", KPC_(param_.result_.part_entry));
            param_.result_.part_entry_->dec_ref();
            param_.result_.part_entry_ = NULL;
          }

          if (NULL != table_entry_ && NULL != part_entry_) {
            param_.result_.has_dup_replica_ = part_entry_->has_dup_replica();
          } else if (NULL != table_entry_) {
            param_.result_.has_dup_replica_ = table_entry_->has_dup_replica();
          }
          param_.result_.table_entry_ = table_entry_;
          table_entry_ = NULL;
          param_.result_.is_table_entry_from_remote_ = is_table_entry_from_remote_;

          param_.result_.part_entry_ = part_entry_;
          part_entry_ = NULL;
          param_.result_.is_partition_entry_from_remote_ = is_part_entry_from_remote_;

          cont->handle_event(TABLE_ENTRY_EVENT_LOOKUP_DONE, &param_.result_);
          param_.result_.reset();
        } else {
          // cancelled
          LOG_INFO("mysql route has been cancelled, no need to notify caller");
        }
      }
    } else {
      LOG_INFO("no caller, no need to notify caller");
    }
    terminate_route_ = true;
  } else {
    MYSQL_ROUTE_SET_DEFAULT_HANDLER(&ObMysqlRoute::state_notify_caller);
    if (OB_ISNULL(pending_action_ = submit_thread_->schedule_imm(this))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to schedule imm", K(ret));
    }
  }
}

int ObMysqlRoute::state_notify_caller(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);

  pending_action_ = NULL;
  notify_caller();
  return EVENT_CONT;
}

inline void ObMysqlRoute::setup_error_route()
{
  LOG_INFO("ObMysqlRoute::setup_error_route");
  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WARN("fail to cancel timeout action", K(ret));
  }

  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  }
  notify_caller();
}


inline void ObMysqlRoute::handle_timeout()
{
  LOG_INFO("mysql route timeout", K_(param_.timeout_us));
  timeout_action_ = NULL;
  setup_error_route();
}

inline void ObMysqlRoute::set_state_and_call_next(const ObMysqlRouteAction next_action)
{
  next_action_ = next_action;
  call_next_action();
}

inline void ObMysqlRoute::call_next_action()
{
  switch (next_action_) {
    case ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_START: {
      setup_routine_entry_lookup();
      break;
    }
    case ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_DONE: {
      handle_routine_entry_lookup_done();
      break;
    }
    case ROUTE_ACTION_ROUTE_SQL_PARSE_START: {
      setup_route_sql_parse();
      break;
    }
    case ROUTE_ACTION_ROUTE_SQL_PARSE_DONE: {
      handle_route_sql_parse_done();
      break;
    }
    case ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START: {
      setup_table_entry_lookup();
      break;
    }
    case ROUTE_ACTION_TABLE_ENTRY_LOOKUP_DONE: {
      handle_table_entry_lookup_done();
      break;
    }
    case ROUTE_ACTION_PARTITION_ID_CALC_START: {
      setup_partition_id_calc();
      break;
    }
    case ROUTE_ACTION_PARTITION_ID_CALC_DONE: {
      handle_partition_id_calc_done();
      break;
    }
    case ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_START: {
      setup_partition_entry_lookup();
      break;
    }
    case ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_DONE: {
      handle_partition_entry_lookup_done();
      break;
    }
    case ROUTE_ACTION_NOTIFY_OUT: {
      notify_caller();
      break;
    }
    case ROUTE_ACTION_TIMEOUT: {
      handle_timeout();
      break;
    }
    case ROUTE_ACTION_UNEXPECTED_NOOP: {
      setup_error_route();
      break;
    }
    default: {
      LOG_ERROR("unknown route next action", K_(next_action));
      setup_error_route();
      break;
    }
  }
}

inline int ObMysqlRoute::init(ObRouteParam &route_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!route_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(route_param), K(ret));
  } else if (OB_FAIL(deep_copy_route_param(route_param))) {
    LOG_WARN("fail to shallow copy route param", K(ret));
  } else {
    action_.set_continuation(route_param.cont_);
    mutex_ = route_param.cont_->mutex_;
    submit_thread_ = route_param.cont_->mutex_->thread_holding_;
  }

  return ret;
}

inline int ObMysqlRoute::deep_copy_route_param(ObRouteParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(&param_ != &param)) {
    if (!param.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid input value", K(param), K(ret));
    } else {
      param_.cont_ = param.cont_;
      param_.force_renew_ = param.force_renew_;
      param_.mysql_proxy_ = param.mysql_proxy_;
      param_.cr_version_ = param.cr_version_;
      param_.cr_id_ = param.cr_id_;
      param_.tenant_version_ = param.tenant_version_;
      param_.timeout_us_ = param.timeout_us_;
      param_.is_partition_table_route_supported_ = param.is_partition_table_route_supported_;
      param_.need_pl_route_ = param.need_pl_route_;
      param_.is_oracle_mode_ = param.is_oracle_mode_;
      param_.is_need_force_flush_ = param.is_need_force_flush_;
      if (!param.current_idc_name_.empty()) {
        MEMCPY(param_.current_idc_name_buf_, param.current_idc_name_.ptr(), param.current_idc_name_.length());
        param_.current_idc_name_.assign_ptr(param_.current_idc_name_buf_, param.current_idc_name_.length());
      } else {
        param_.current_idc_name_.reset();
      }
      // !!Attention client_request should not be used in scheduled cont
      param_.client_request_ = param.client_request_;
      param_.client_info_ = param.client_info_;
      // no need assign result_ and cr
      if (NULL != name_buf_ && name_buf_len_ > 0) {
        op_fixed_mem_free(name_buf_, name_buf_len_);
        name_buf_ = NULL;
        name_buf_len_ = 0;
      }
      name_buf_len_ = param.name_.get_total_str_len();
      name_buf_ = static_cast<char *>(op_fixed_mem_alloc(name_buf_len_));
      if (OB_ISNULL(name_buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem", K_(name_buf_len), K(ret));
      } else if (OB_FAIL(param_.name_.deep_copy(param.name_, name_buf_, name_buf_len_))) {
        LOG_WARN("fail to deep copy table entry name", K(ret));
      }

      if (OB_FAIL(ret) && (NULL != name_buf_)) {
        op_fixed_mem_free(name_buf_, name_buf_len_);
        name_buf_ = NULL;
        name_buf_len_ = 0;
        param_.name_.reset();
      }
    }
  }

  return ret;
}

int ObMysqlRoute::get_route_entry(ObRouteParam &route_param,
                                  obutils::ObClusterResource *cr,
                                  ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObMysqlRoute *mysql_route = NULL;
  action = NULL;
  if (OB_UNLIKELY(!route_param.is_valid() || NULL == cr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(route_param), K(cr), K(ret));
  } else if (OB_ISNULL(mysql_route = allocate_route())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate mysql route", K(mysql_route), K(ret));
  } else if (OB_FAIL(mysql_route->init(route_param))) {
    LOG_WARN("fail to init mysql route", K(ret));
  } else {
    int event_ret = mysql_route->handle_event(EVENT_IMMEDIATE, NULL);
    // fast path, thread cache hit, mysql route was freed;
    if (EVENT_DONE == event_ret) {
      mysql_route = NULL;
      action = NULL;
    } else {
      // slow path
      mysql_route->set_cluster_resource(cr);
      if (OB_FAIL(mysql_route->schedule_timeout_action())) {
        LOG_WARN("fail to schedule timeout action", K(ret));
        // will handle error inner schedule_timeout_action
        ret = OB_SUCCESS;
      } else {
        action = &mysql_route->action_;
      }
    }
  }

  if (OB_FAIL(ret) && (NULL != mysql_route)) {
    free_route(mysql_route);
    mysql_route = NULL;
  }

  return ret;
}

inline ObMysqlRoute *ObMysqlRoute::allocate_route()
{
  return op_reclaim_alloc(ObMysqlRoute);
}

inline void ObMysqlRoute::free_route(ObMysqlRoute *mysql_route)
{
  if (NULL != mysql_route) {
    op_reclaim_free(mysql_route);
    mysql_route = NULL;
  }
}

inline int ObMysqlRoute::schedule_timeout_action()
{
  int ret = OB_SUCCESS;
  int64_t timeout_ns = HRTIME_USECONDS(param_.timeout_us_);
  if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout_action must be NULL here", K_(timeout_action), K(ret));
  } else if (OB_ISNULL(timeout_action_ = self_ethread().schedule_in(this, timeout_ns))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to schedule timeout", K(timeout_action_), K(ret));
  }

  if (OB_FAIL(ret)) {
    set_state_and_call_next(ROUTE_ACTION_UNEXPECTED_NOOP);
  }
  return ret;
}

inline int ObMysqlRoute::cancel_pending_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      PROXY_LOG(WARN, "fail to cancel pending action", K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }
  return ret;
}

inline int ObMysqlRoute::cancel_timeout_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != timeout_action_) {
    if (OB_FAIL(timeout_action_->cancel())) {
      PROXY_LOG(WARN, "fail to cancel timeout action", K_(timeout_action), K(ret));
    } else {
      timeout_action_ = NULL;
    }
  }
  return ret;
}

inline const char *ObMysqlRoute::get_action_str(const ObMysqlRouteAction action)
{
  const char *name = NULL;
  switch (action) {
    case ROUTE_ACTION_UNDEFINED:
      name = "ROUTE_ACTION_UNDEFINED";
      break;
    case ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_START:
      name = "ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_START";
      break;
    case ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_DONE:
      name = "ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_DONE";
      break;
    case ROUTE_ACTION_ROUTE_SQL_PARSE_START:
      name = "ROUTE_ACTION_ROUTE_SQL_PARSE_START";
      break;
    case ROUTE_ACTION_ROUTE_SQL_PARSE_DONE:
      name = "ROUTE_ACTION_ROUTE_SQL_PARSE_DONE";
      break;
    case ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START:
      name = "ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START";
      break;
    case ROUTE_ACTION_TABLE_ENTRY_LOOKUP_DONE:
      name = "ROUTE_ACTION_TABLE_ENTRY_LOOKUP_DONE";
      break;
    case ROUTE_ACTION_PARTITION_ID_CALC_START:
      name = "ROUTE_ACTION_PARTITION_ID_CALC_START";
      break;
    case ROUTE_ACTION_PARTITION_ID_CALC_DONE:
      name = "ROUTE_ACTION_PARTITION_ID_CALC_DONE";
      break;
    case ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_START:
      name = "ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_START";
      break;
    case ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_DONE:
      name = "ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_DONE";
      break;
    case ROUTE_ACTION_NOTIFY_OUT:
      name = "ROUTE_ACTION_NOTIFY_OUT";
      break;
    case ROUTE_ACTION_TIMEOUT:
      name = "ROUTE_ACTION_TIMEOUT";
      break;
    case ROUTE_ACTION_UNEXPECTED_NOOP:
      name = "ROUTE_ACTION_UNEXPECTED_NOOP";
      break;
    default:
      name = "unknown action";
      break;
  }
  return name;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
