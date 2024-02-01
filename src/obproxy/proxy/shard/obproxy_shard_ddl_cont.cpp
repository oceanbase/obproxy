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

#include <curl/curl.h>
#include "proxy/shard/obproxy_shard_utils.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::json;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//-------ObShardDDLStatus
int ObShardDDLStatus::parse_id_list_from_json(const Value *json)
{
  int ret = OB_SUCCESS;

  if (JT_ARRAY == json->get_type()) {
    DLIST_FOREACH(it, json->get_array()) {
      ddl_task_id_list_.push_back(it->get_number());
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WDIAG("invalid json config type", "expected type", JT_ARRAY,
             "actual type", json->get_type(), K(ret));
  }

  return ret;
}

int ObShardDDLStatus::parse_data_from_json(const Value *json, const bool is_first)
{
  int ret = OB_SUCCESS;

  if (JT_OBJECT == json->get_type()) {
    DLIST_FOREACH(it, json->get_object()) {
      if (it->name_ == SHARD_DDL_STATUS) {
        if (OB_FAIL(set_ddl_status(it->value_->get_string()))) {
          LOG_WDIAG("fail to set ddl status", K(ret));
        }
      } else if (it->name_ == SHARD_DDL_ERROR_CODE) {
        if (OB_FAIL(set_error_code(it->value_->get_string()))) {
          LOG_WDIAG("fail to set error code", K(ret));
        }
      } else if (it->name_ == SHARD_DDL_ERROR_MSG) {
        if (OB_FAIL(set_error_message(it->value_->get_string()))) {
          LOG_WDIAG("fail to set error msg", K(ret));
        }
      } else if (it->name_ == SHARD_DDL_TASK_ID_LIST) {
        if (is_first && OB_FAIL(parse_id_list_from_json(it->value_))) {
          LOG_WDIAG("fail to parse id list", K(ret));
        }
      }
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WDIAG("invalid json config type", "expected type", JT_OBJECT,
             "actual type", json->get_type(), K(ret));
  }

  return ret;
}

int ObShardDDLStatus::parse_from_json(ObString &json, const bool is_first)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
  Value *json_root = NULL;
  Parser parser;
  if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WDIAG("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(json.ptr(), json.length(), json_root))) {
    LOG_WDIAG("parse json failed", K(ret), "json", get_print_json(json));
  } else if (OB_ISNULL(json_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json root is null", K(ret));
  } else {
    if (JT_OBJECT == json_root->get_type()) {
      DLIST_FOREACH(it, json_root->get_object()) {
        if (it->name_ == SHARD_DDL_DATA) {
          if (OB_FAIL(parse_data_from_json(it->value_, is_first))) {
            LOG_WDIAG("fail to parse data", K(ret));
          }
        }
      }
    } else {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("invalid json config type", "expected type", JT_OBJECT,
               "actual type", json_root->get_type(), K(ret));
    }
  }

  return ret;
}

int ObShardDDLStatus::to_json(ObSqlString &buf)
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%.*s\", \"%s\":\"%.*s\", \"%s\":\"%.*s\", \"%s\":[",
                       SHARD_DDL_STATUS, ddl_status_.length(), ddl_status_.ptr(),
                       SHARD_DDL_ERROR_CODE, error_code_.length(), error_code_.ptr(),
                       SHARD_DDL_ERROR_MSG, error_message_.length(), error_message_.ptr(),
                       SHARD_DDL_TASK_ID_LIST);

  if (OB_SUCC(ret)) {
    int64_t count = ddl_task_id_list_.count();
    for (int64_t j = 0; OB_SUCC(ret) && j < count; j++) {
      if (OB_FAIL(buf.append_fmt("%ld", ddl_task_id_list_.at(j)))) {
        LOG_WDIAG("fail to append config", K(ret));
      } else if (j < count - 1) {
        ret = buf.append(",");
      }
      j++;
    }

    if (OB_SUCC(ret)) {
      ret = buf.append("]}");
    }
  }

  return ret;
}

//-------ObShardDDLTableSourceDefinition
int ObShardDDLTableSourceDefinition::init(const ObString &schema, const ObString &sql, const ObShardDDLOperation operation)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_options_.create(TABLE_OPTIONS_MAP_BUCKET, ObModIds::OB_PROXY_SHARDING_DDL, ObModIds::OB_PROXY_SHARDING_DDL))) {
    LOG_WDIAG("fail to init table options rule map", K(ret));
  } else {
    schema_ = schema;
    sql_ = sql;
    operation_ = operation;
  }

  return ret;
}

int ObShardDDLTableSourceDefinition::set_table_options(const ObString &key, const ObString value)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(table_options_.set_refactored(key, value))) {
    LOG_WDIAG("fail to set table options", K(key), K(value), K(ret));
  }

  return ret;
}

const char* ObShardDDLTableSourceDefinition::get_operation_name(ObShardDDLOperation operation)
{
  static const char *operation_name_array[SHARD_DDL_OPERATION_MAX] =
  {
    "CREATE_TABLE",
    "CREATE_INDEX",
    "CREATE_TABLEGROUP",
    "ALTER",
    "DROP",
    "DROP_TABLEGROUP",
    "RENAME",
    "TRUNCATE",
    "STOP",
    "RETRY"
  };
  const char *str_ret = "";
  if (operation >= SHARD_DDL_OPERATION_CREATE_TABLE && operation < SHARD_DDL_OPERATION_MAX) {
    str_ret = operation_name_array[operation];
  }
  return str_ret;
}

int ObShardDDLTableSourceDefinition::to_json(ObSqlString &buf)
{
  int ret = OB_SUCCESS;
  ret = buf.append_fmt("{\"%s\":\"%.*s\", \"%s\":\"%.*s\", \"%s\":\"%s\", \"%s\":{",
                       SHARD_DDL_SCHEMA, schema_.length(), schema_.ptr(),
                       SHARD_DDL_SQL, sql_.length(), sql_.ptr(),
                       SHARD_DDL_OPERATION, get_operation_name(operation_),
                       SHARD_DDL_TABLE_OPTIONS);

  if (OB_SUCC(ret)) {
    int64_t size = table_options_.size();
    hash::ObHashMap<ObString, ObString>::iterator it = table_options_.begin();
    hash::ObHashMap<ObString, ObString>::iterator end = table_options_.end();
    for (int64_t j = 0; OB_SUCC(ret) && it != end; it++) {
      if (OB_FAIL(buf.append_fmt("\"%.*s\": \"%.*s\"",
                                 it->first.length(), it->first.ptr(),
                                 it->second.length(), it->second.ptr()))) {
        LOG_WDIAG("fail to append config", K(ret));
      } else if (j < size - 1) {
        ret = buf.append(",");
      }
      j++;
    }

    if (OB_SUCC(ret)) {
      ret = buf.append("}}");
    }
  }

  return ret;
}

//-------ObShardDDLCont------
ObShardDDLCont::ObShardDDLCont(ObContinuation *cb_cont, ObEThread *cb_thread)
  : ObAsyncCommonTask(cb_cont->mutex_, "shardDDLCont", cb_cont, cb_thread),
    is_first_(true), allocator_(ObModIds::OB_PROXY_SHARDING_DDL),
    response_buf_(NULL), ddl_status_(allocator_)
{
}

void ObShardDDLCont::destroy()
{
  ObAsyncCommonTask::destroy();
}

int ObShardDDLCont::covert_stmt_type_to_operation(const ObProxyBasicStmtType stmt_type,
                                                  const ObProxyBasicStmtSubType sub_stmt_type,
                                                  ObShardDDLOperation &operation)
{
  int ret = OB_SUCCESS;

  if (stmt_type == OBPROXY_T_CREATE) {
    if (sub_stmt_type == OBPROXY_T_SUB_CREATE_TABLE) {
      operation = SHARD_DDL_OPERATION_CREATE_TABLE;
    } else if (sub_stmt_type == OBPROXY_T_SUB_CREATE_INDEX) {
      operation = SHARD_DDL_OPERATION_CREATE_INDEX;
    } else if (sub_stmt_type == OBPROXY_T_SUB_CREATE_TABLEGROUP) {
      operation = SHARD_DDL_OPERATION_CREATE_TABLEGROUP;
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  } else if (stmt_type == OBPROXY_T_ALTER) {
    operation = SHARD_DDL_OPERATION_ALTER;
  } else if (stmt_type == OBPROXY_T_DROP) {
    if (sub_stmt_type == OBPROXY_T_SUB_DROP_TABLEGROUP) {
      operation = SHARD_DDL_OPERATION_DROP_TABLEGROUP;
    } else {
      operation = SHARD_DDL_OPERATION_DROP;
    }
  } else if (stmt_type == OBPROXY_T_RENAME) {
    operation = SHARD_DDL_OPERATION_RENAME;
  } else if (stmt_type == OBPROXY_T_TRUNCATE) {
    operation = SHARD_DDL_OPERATION_TRUNCATE;
  } else if (stmt_type == OBPROXY_T_STOP_DDL_TASK) {
    operation = SHARD_DDL_OPERATION_STOP;
  } else if (stmt_type == OBPROXY_T_RETRY_DDL_TASK) {
    operation = SHARD_DDL_OPERATION_RETRY;
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  return ret;
}

int ObShardDDLCont::get_ddl_url(const char *url_template, char *&buffer)
{
  int ret = OB_SUCCESS;

  ObProxyConfig &proxy_config = get_global_proxy_config();
  int64_t mng_url_len = strlen(proxy_config.mng_url.str());
  int64_t cloud_instance_id_len = strlen(proxy_config.cloud_instance_id.str());
  int64_t url_len = strlen(url_template) + mng_url_len + cloud_instance_id_len;
  if (OB_ISNULL(buffer = static_cast<char *>(allocator_.alloc(url_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc mem for ddl url", K(url_len), K(ret));
  } else {
    // If the last character is /, remove it
    if ('/' == *(proxy_config.mng_url.str() + mng_url_len - 1)) {
      mng_url_len -= 1;
    }
    int64_t w_len = snprintf(buffer, static_cast<size_t>(url_len), url_template,
                             mng_url_len, proxy_config.mng_url.str(),
                             proxy_config.cloud_instance_id.str());
    if (OB_UNLIKELY(w_len < 0) || OB_UNLIKELY(w_len > url_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_EDIAG("fail to snprintf for post ddl url", K(url_len), K(ret));
    }
  }
  return ret;
}

int ObShardDDLCont::init(const ObString &instance_id, const ObString &schema, const ObString &sql, 
                         const int64_t group_id, const ObString& hint_table_name,
                         const ObSqlParseResult& parse_result)
{
  int ret = OB_SUCCESS;

  const int64_t task_id = parse_result.cmd_info_.integer_[0];
  const ObProxyBasicStmtType stmt_type = parse_result.get_stmt_type();
  const ObProxyBasicStmtSubType sub_stmt_type = parse_result.get_cmd_sub_type();

  if (OB_FAIL(ob_write_string(allocator_, instance_id, instance_id_))) {
    LOG_WDIAG("fail to write string instance_id", K(instance_id), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, schema, schema_))) {
    LOG_WDIAG("fail to write string schema", K(schema), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, sql, sql_))) {
    LOG_WDIAG("fail to write string sql", K(sql), K(ret));
  } else if (OB_FAIL(covert_stmt_type_to_operation(stmt_type, sub_stmt_type, operation_))) {
    LOG_WDIAG("fail to covert stmt type to operation", K(stmt_type), K(sub_stmt_type), K(ret));
  } else if (OB_UNLIKELY(NULL != response_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("buf should be null before memory allocated", K_(response_buf), K(ret));
  } else if (OB_ISNULL(response_buf_ = static_cast<char *>(allocator_.alloc(OBPROXY_MAX_JSON_INFO_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory", K(ret));
  } else if (OB_FAIL(int_to_string(allocator_, group_id, group_id_))) {
    LOG_WDIAG("fail to convert group_id to ObString", K(ret));
  } else {
    hint_table_name_ = hint_table_name;
    if (SHARD_DDL_OPERATION_STOP == operation_
        || SHARD_DDL_OPERATION_RETRY == operation_) {
      ddl_status_.ddl_task_id_list_.push_back(task_id);
    }
    ddl_status_.set_error_code(ObShardDDLStatus::SHARD_DDL_JOB_ERROR);
    ddl_status_.set_error_message(ObShardDDLStatus::SHARD_DDL_JOB_ERROR_MSG);
  }

  return ret;
}

int ObShardDDLCont::init_task()
{
  int ret = OB_SUCCESS;

  bool need_reschedule = false;
  char *async_ddl_url = NULL;
  char *check_ddl_url = NULL;
  ObSqlString buf;

  response_string_.assign_buffer(response_buf_, static_cast<int32_t>(OBPROXY_MAX_JSON_INFO_SIZE));
  if (is_first_) {
    if (OB_FAIL(get_ddl_url_by_type(async_ddl_url))) {
      LOG_WDIAG("fail to get async ddl url", K(ret));
    } else if (OB_FAIL(get_ddl_json_by_type(buf))) {
      LOG_WDIAG("fail to get ddl json", K(ret));
    } else if (OB_FAIL(post_ddl_request(async_ddl_url, buf.ptr(), static_cast<void *>(&response_string_), write_data))) {
      LOG_WDIAG("fail to post ddl request", K(async_ddl_url), K(ret));
    } else if (OB_FAIL(ddl_status_.parse_from_json(response_string_, is_first_))) {
      LOG_WDIAG("fail to parse ddl response", K_(response_string), K(ret));
    } else {
      is_first_ = false;
    }
  } else {
    if (OB_FAIL(get_ddl_url(CHECK_DDL_URL, check_ddl_url))) {
      LOG_WDIAG("fail to get check ddl url", K(ret));
    } else if (OB_FAIL(ddl_status_.to_json(buf))) {
      LOG_WDIAG("fail to get post json data", K(ret));
    } else if (OB_FAIL(post_ddl_request(check_ddl_url, buf.ptr(), static_cast<void *>(&response_string_), write_data))) {
      LOG_WDIAG("fail to post ddl request", K(check_ddl_url), K(ret));
    } else if (OB_FAIL(ddl_status_.parse_from_json(response_string_, is_first_))) {
      LOG_WDIAG("fail to parse ddl response", K_(response_string), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (ddl_status_.is_success()) {
      need_reschedule = false;
    } else if (ddl_status_.is_running()) {
      need_reschedule = true;
    } else {
      LOG_WDIAG("Execute ddl failed", K_(sql), "errcode", ddl_status_.get_error_code(),
               "errmsg", ddl_status_.get_error_message());
      ddl_status_.set_error_code(ObShardDDLStatus::SHARD_DDL_JOB_ERROR);
      need_reschedule = false;
    }

    if (need_reschedule) {
      // reschedule
      if (OB_ISNULL(self_ethread().schedule_in(this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), EVENT_IMMEDIATE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to reschedule shard ddl cont", K(ret));
      } else {
        ret = OB_SUCCESS;
        LOG_DEBUG("succ to reschedule shard ddl cont", K(ret));
      }
    } else {
      if (NULL != cb_cont_) {
        need_callback_ = true;
      } else {
        terminate_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
    ddl_status_.set_error_code(ObShardDDLStatus::SHARD_DDL_JOB_ERROR);
    ddl_status_.set_error_message(ObShardDDLStatus::SHARD_DDL_JOB_ERROR_MSG);
  }

  return ret;
}

int ObShardDDLCont::get_ddl_url_by_type(char *&buf)
{
  int ret = OB_SUCCESS;

  if (SHARD_DDL_OPERATION_STOP == operation_) {
    ret = get_ddl_url(STOP_DDL_URL, buf);
  } else if (SHARD_DDL_OPERATION_RETRY == operation_) {
    ret = get_ddl_url(RETRY_DDL_URL, buf);
  } else {
    ret = get_ddl_url(ASYNC_DDL_URL, buf);
  }

  return ret;
}

int ObShardDDLCont::get_ddl_json_by_type(ObSqlString &buf)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(SHARD_DDL_OPERATION_STOP == operation_) 
      || (SHARD_DDL_OPERATION_RETRY == operation_)) {
    if (OB_FAIL(ddl_status_.to_json(buf))) {
      LOG_WDIAG("fail to get post json data for stop/retry", K(ret));
    }
  } else {
    ObShardDDLTableSourceDefinition table_source_definition;
    if (OB_FAIL(table_source_definition.init(schema_, sql_, operation_))) {
      LOG_WDIAG("fail to init table source definition", K_(schema), K_(sql), K_(operation), K(ret));
    } else if (OB_FAIL(table_source_definition.set_table_options("InstanceId", instance_id_))) {
      LOG_WDIAG("fail to set table options", K_(instance_id), K(ret));
    } else if (OB_FAIL(table_source_definition.set_table_options("groupId", group_id_))) {
      LOG_WDIAG("fail to set table options", K_(group_id), K(ret));
    } else if (OB_FAIL(table_source_definition.set_table_options("hintTableName", hint_table_name_))) {
      LOG_WDIAG("fail to set table options", K_(hint_table_name), K(ret));
    } else if (OB_FAIL(table_source_definition.to_json(buf))) {
      LOG_WDIAG("fail to get post json data", K(ret));
    }
  }
  LOG_DEBUG("get json for table_source_definition", K(buf), K(ret));

  return ret;
}

int ObShardDDLCont::post_ddl_request(const char *url, void *postrequest, void *response, write_func write_func_callback /*NULL*/)
{
  int ret = OB_SUCCESS;
  CURL *curl = NULL;
  if (OB_ISNULL(url) || OB_ISNULL(postrequest) || OB_ISNULL(response)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("NULL pointer, invalid fetch by curl", KP(url), KP(postrequest), KP(response), K(ret));
  } else if (OB_ISNULL(curl = curl_easy_init())) {
    ret = OB_CURL_ERROR;
    LOG_WDIAG("init curl failed", K(ret));
  } else {
    CURLcode cc = CURLE_OK;
    curl_slist *plist = NULL;
    int64_t http_code = 0;
    //set curl options
    if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_URL, url))) {
      LOG_WDIAG("set url failed", K(cc), "url", url);
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L))) {
      LOG_WDIAG("set no signal failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L))) {
      LOG_WDIAG("set tcp_nodelay failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 3))) {//set max redirect
      LOG_WDIAG("set max redirect failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1))) {//for http redirect 301 302
      LOG_WDIAG("set follow location failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, CURL_CONNECTION_TIMEOUT))) {
      LOG_WDIAG("set connect timeout failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, CURL_TRANSFER_TIMEOUT))) {
      LOG_WDIAG("set transfer timeout failed", K(cc));
    } else if (FALSE_IT(plist = curl_slist_append(NULL, JSON_HTTP_HEADER))) {
      // impossiable
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, plist))) {
      LOG_WDIAG("set http header failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postrequest))) {
      LOG_WDIAG("set postfields failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_func_callback))) {
      LOG_WDIAG("set write callback failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, response))) {
      LOG_WDIAG("set write data failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_perform(curl))) {
      LOG_WDIAG("curl easy perform failed", K(cc));
    } else if (CURLE_OK != (cc = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code))) {
      LOG_WDIAG("curl getinfo failed", K(cc));
    } else {
      // http status code 2xx means success
      if (http_code / 100 != 2) {
        ret = OB_CURL_ERROR;
        LOG_WDIAG("unexpected http status code", K(http_code), K(postrequest), K(url), K(ret));
      }
    }

    if (CURLE_OK != cc) {
      if (CURLE_WRITE_ERROR == cc) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        ret = OB_CURL_ERROR;
      }
      LOG_WDIAG("curl error", "curl_error_code", cc, "curl_error_message",
          curl_easy_strerror(cc), K(ret), K(url));
    }
    curl_easy_cleanup(curl);
  }
  return ret;
}

int64_t ObShardDDLCont::write_data(void *ptr, int64_t size, int64_t nmemb, void *stream)
{
  int ret = OB_SUCCESS;
  int64_t real_size = 0;
  ObString *content = NULL;

  if (OB_ISNULL(stream) || OB_ISNULL(ptr) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(stream), K(ptr), K(size), K(ret));
  } else {
    real_size = size * nmemb;
    if (real_size > 0) {
      content = static_cast<ObString *>(stream);
      if (real_size + content->length() > content->size()) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WDIAG("unexpected long content",
                 "new_byte", real_size,
                 "recved_byte", content->length(),
                 "content_size", content->size(),
                 K(ret));
      } else if (content->write(static_cast<const char *>(ptr), static_cast<int32_t>(real_size)) <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("append data failed", K(ret));
      }
    }
  }
  return OB_SUCCESS == ret ? real_size : 0;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
