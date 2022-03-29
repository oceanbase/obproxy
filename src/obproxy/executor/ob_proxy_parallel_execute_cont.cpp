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

#include "ob_proxy_parallel_execute_cont.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "common/obsm_utils.h"
#include "common/ob_obj_cast.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

ObProxyParallelResp::~ObProxyParallelResp()
{
  if (OB_NOT_NULL(resp_)) {
    op_free(resp_); // free the resp come from ObMysqlProxy
    resp_ = NULL;
  }

  rs_fetcher_ = NULL;
  column_count_ = 0;
  allocator_ = NULL;
}

int ObProxyParallelResp::init(ObClientMysqlResp *resp, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(resp_ = resp) || OB_ISNULL(allocator_ = allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resp can not be NULL", KP(resp), KP(allocator), K(ret));
  }

  if (OB_SUCC(ret) && is_resultset_resp()) {
    if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher_))) {
      LOG_WARN("fail to get resultset fetcher", K(ret));
    } else if (OB_ISNULL(rs_fetcher_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rs_fetcher can not be NULL", K(ret));
    } else {
      column_count_ = rs_fetcher_->get_column_count();
    }
  }

  return ret;
}

int ObProxyParallelResp::next(ObObj *&rows)
{
  int ret = OB_SUCCESS;

  ObMysqlField *fields = rs_fetcher_->get_field();
  int64_t buf_len = (sizeof(ObObj) * column_count_);
  char *buf = NULL;

  if (OB_FAIL(rs_fetcher_->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(buf_len), K(ret));
  } else {
    rows = new (buf) ObObj[column_count_];
    ObObjType ob_type;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; i++) {
      if (OB_FAIL(rs_fetcher_->get_obj(i, rows[i]))) {
        LOG_WARN("fail to get varchar", K(i), K(ret));
      } else if (rows[i].need_deep_copy()) {
        int64_t copy_size = rows[i].get_deep_copy_size();
        int64_t pos = 0;
        buf = NULL;
        if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(copy_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", K(copy_size), K(ret));
        } else if (OB_FAIL(rows[i].deep_copy(rows[i], buf, copy_size, pos))) {
          LOG_WARN("fail to deep coy", "obj", rows[i], K(copy_size), K(ret));
        }
      }

      if (OB_SUCC(ret) && rows[i].is_varchar()) {
        ObCollationType cs_type = static_cast<ObCollationType>(fields[i].charsetnr_);
        // utf8_general_ci => CS_TYPE_UTF8MB4_GENERAL_CI
        if (33 == fields[i].charsetnr_) {
          cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
        // utf8_bin => CS_TYPE_UTF8MB4_BIN
        } else if (83 == fields[i].charsetnr_) {
          cs_type = CS_TYPE_UTF8MB4_BIN;
        }

        rows[i].set_collation_type(cs_type);

        if (0 != rows[i].get_string_len()) {
          // if can not convert, stay varchar type
          if (OB_FAIL(ObSMUtils::get_ob_type(ob_type, fields[i].type_))) {
            LOG_INFO("cast ob type from mysql type failed", K(ob_type), "elem_type", fields[i].type_, K(ret));
            ret = OB_SUCCESS;
          } else if (ObTimestampType == ob_type || ObTimeType == ob_type
                     || ObDateType == ob_type || ObDateTimeType == ob_type) {
            //do nothing
          } else {
            ObCastCtx cast_ctx(allocator_, NULL, CM_NULL_ON_WARN, cs_type);
            // use src_obj as buf_obj
            if (OB_FAIL(ObObjCasterV2::to_type(ob_type, cs_type, cast_ctx, rows[i], rows[i]))) {
              COMMON_LOG(WARN, "failed to cast obj", "idx", i, "row", rows[i], K(ob_type), K(cs_type), K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObProxyParallelExecuteCont::init(const ObProxyParallelParam &parallel_param, const int64_t cont_index, ObIAllocator *allocator, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  shard_conn_ = parallel_param.shard_conn_;
  shard_conn_->inc_ref();

  const ObString &username = shard_conn_->full_username_.config_string_;
  const ObString &passwd = shard_conn_->password_.config_string_;
  const ObString &database_name = shard_conn_->database_name_.config_string_;

  char passwd_staged1_buf[ENC_STRING_BUF_LEN]; // 1B '*' + 40B octal num
  ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
  if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(passwd, passwd_string))) {
    LOG_WARN("fail to encrypt_passwd_to_stage1", K(ret));
  } else {
    passwd_string += 1;//trim the head'*'
    if (OB_ISNULL(mysql_proxy_ = op_alloc(ObMysqlProxy))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObMysqlProxy");
    } else if (OB_FAIL(mysql_proxy_->init(timeout_ms, username, passwd_string, database_name))) {
      LOG_WARN("fail to init proxy", K(username), K(database_name));
    } else if (OB_FAIL(mysql_proxy_->rebuild_client_pool(shard_conn_, parallel_param.shard_prop_,
                                                         false, username, passwd_string, database_name))) {
      LOG_WARN("fail to create mysql client pool", K(username), K(database_name), K(ret));
    } else if (OB_FAIL(deep_copy_sql(parallel_param.request_sql_))) {
      LOG_WARN("fail to deep_copy_sql", K(parallel_param.request_sql_), K(ret));
    } else {
      cont_index_ = cont_index;
      allocator_ = allocator;
      LOG_DEBUG("rebuild_client_pool success", K(username), K(database_name));
    }
  }

  // if failed, no need clear mysql_proxy which will be cleared by caller
  return ret;
}

int ObProxyParallelExecuteCont::deep_copy_sql(const common::ObString &sql)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(sql), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(sql.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", "alloc_size", sql.length(), K(ret));
  } else {
    reset_request_sql();
    MEMCPY(buf, sql.ptr(), sql.length());
    request_sql_.assign_ptr(buf, sql.length());
    is_deep_copy_ = true;
  }
  return ret;
}

void ObProxyParallelExecuteCont::reset_request_sql()
{
  if (is_deep_copy_ && !request_sql_.empty()) {
    op_fixed_mem_free(request_sql_.ptr(), request_sql_.length());
  }
  request_sql_.reset();
  is_deep_copy_ = false;
} 

int ObProxyParallelExecuteCont::init_task()
{
  int ret = OB_SUCCESS;  

  ObMysqlRequestParam request_param;
  request_param.sql_ = request_sql_;
  if (OB_FAIL(mysql_proxy_->async_read(this, request_param, pending_action_))) {
    LOG_WARN("fail to async read", K_(request_sql), K(ret));
  }

  return ret;
}

int ObProxyParallelExecuteCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("finish_task", KP(this), KP(data), K_(cont_index),
            KP_(cb_cont), K_(request_sql), KPC_(shard_conn), K(ret));

  if (NULL != data) {
    ObClientMysqlResp *resp = NULL;
    if (OB_ISNULL(result_set_ = op_alloc_args(ObProxyParallelResp, cont_index_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate ObProxyParallelResp", K(ret));
    } else if (FALSE_IT(resp = reinterpret_cast<ObClientMysqlResp *>(data))) {
    } else if (OB_FAIL(result_set_->init(resp, allocator_))) {
      LOG_WARN("fail to init ObProxyParallelResp", K(ret));
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(result_set_)) {
        op_free(result_set_);
        result_set_ = NULL;
      } else {
        op_free(resp);
        resp = NULL;
      }
    }
  }

  return ret;
}


void ObProxyParallelExecuteCont::destroy()
{
  LOG_DEBUG("parallel execute cont will be destroyed", KP(this));

  cancel_pending_action();

  if (OB_NOT_NULL(shard_conn_)) {
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }

  reset_request_sql();

  if (OB_NOT_NULL(mysql_proxy_)) {
    op_free(mysql_proxy_);
    mysql_proxy_ = NULL;
  }

  if (OB_NOT_NULL(result_set_)) {
    op_free(result_set_);
    result_set_ = NULL;
  }

  // copy from destroy func of base class, because this Cont is alloced by op_alloc
  cb_cont_ = NULL;
  allocator_ = NULL;
  submit_thread_ = NULL;
  mutex_.release();
  action_.mutex_.release();

  op_free(this);
}

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase
