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

#include "proxy/client/ob_client_utils.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "rpc/obmysql/packet/ompk_handshake_response.h"
#include "packet/ob_mysql_packet_reader.h"
#include "packet/ob_mysql_packet_writer.h"


using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static const int64_t MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

//--------------------------ObMysqlRequestParam--------------------------------//
void ObMysqlRequestParam::reset()
{
  reset_sql();
  current_idc_name_.reset();
  is_user_idc_name_set_ = false;
  need_print_trace_stat_ = false;

}

void ObMysqlRequestParam::reset_sql()
{
  if (is_deep_copy_ && !sql_.empty()) {
    op_fixed_mem_free(sql_.ptr(), sql_.length());
  }
  sql_.reset();
  is_deep_copy_ = false;
}

int ObMysqlRequestParam::deep_copy_sql(const common::ObString &sql)
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
    reset();
    MEMCPY(buf, sql.ptr(), sql.length());
    sql_.assign_ptr(buf, sql.length());
    is_deep_copy_ = true;
  }
  return ret;
}

int ObMysqlRequestParam::deep_copy(const ObMysqlRequestParam &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_sql(other.sql_))) {
    LOG_WARN("fail to deep_copy_sql", K(other), K(ret));
  } else {
    is_user_idc_name_set_ = other.is_user_idc_name_set_;
    need_print_trace_stat_ = other.need_print_trace_stat_;
    if (other.is_user_idc_name_set_ && !other.current_idc_name_.empty()) {
      MEMCPY(current_idc_name_buf_, other.current_idc_name_.ptr(), other.current_idc_name_.length());
      current_idc_name_.assign_ptr(current_idc_name_buf_, other.current_idc_name_.length());
    }
  }
  return ret;
}

//--------------------------ObClientMysqlResp--------------------------------//
void ObClientMysqlResp::reset()
{
  analyzer_.reset();
  mysql_resp_.reset();
  if (NULL != rs_fetcher_) {
    op_free(rs_fetcher_);
    rs_fetcher_ = NULL;
  }
}

void ObClientMysqlResp::destroy()
{
  reset();
  consume_resp_buf();
  if (NULL != response_buf_) {
    free_miobuffer(response_buf_);
    response_buf_ = NULL;
  }
  response_reader_ = NULL;
  is_inited_ = false;
}

int ObClientMysqlResp::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(response_buf_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc response miobuffer", K(ret));
  } else if (OB_ISNULL(response_reader_ = response_buf_->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObClientMysqlResp::analyze_resp(const ObMySQLCmd cmd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else {
    reset();
    analyzer_.set_server_cmd(cmd, STANDARD_MYSQL_PROTOCOL_MODE, false, false);

    if (response_reader_->read_avail() > 0) {
      if (OB_FAIL(analyzer_.analyze_trans_response(*response_reader_, &mysql_resp_))) {
        LOG_WARN("fail to analyze_trans_response", K(ret));
      } else if (!analyzer_.is_resp_completed()) {
        ret = OB_EAGAIN;
        LOG_INFO("response has not received complete", K(ret));
      } else {
        if (is_resultset_resp()) {
          if (OB_ISNULL(rs_fetcher_ = op_alloc(ObResultSetFetcher))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate ObResultSetFetcher", K(ret));
          } else if (OB_FAIL(rs_fetcher_->init(response_reader_))) {
            LOG_WARN("fail to init rs fetcher", K(ret));
          }
          if (OB_FAIL(ret) && (NULL != rs_fetcher_)) {
            op_free(rs_fetcher_);
            rs_fetcher_ = NULL;
          }
        }
      }
    }
  }
  return ret;
}

int ObClientMysqlResp::get_affected_rows(int64_t &affected_row)
{
  int ret = OB_SUCCESS;
  if (is_ok_resp()) {
    if ((NULL != response_reader_) && (response_reader_->read_avail() > 0)) {
      OMPKOK src_ok;
      ObMySQLCapabilityFlags cap(ObClientUtils::CAPABILITY_FLAGS);
      ObMysqlPacketReader pkt_reader;
      if (OB_FAIL(pkt_reader.get_ok_packet(*response_reader_, 0, cap, src_ok))) {
        LOG_WARN("fail to get ok packet", K(ret));
      } else {
        affected_row = static_cast<int64_t>(src_ok.get_affected_rows());
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("response reader is not valid", K_(response_reader), K(ret));
    }
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("the resp is not ok packet", K(ret));
  }

  return ret;
}

int ObClientMysqlResp::get_resultset_fetcher(ObResultSetFetcher *&result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rs_fetcher_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("this response is not the resultset response", K_(mysql_resp), K(ret));
  } else {
    result = rs_fetcher_;
  }
  return ret;
}

//-------------------------ObMysqlResultHandler--------------------------------//
void ObMysqlResultHandler::destroy()
{
  if (NULL != resp_) {
    op_free(resp_);
    resp_= NULL;
  }
  rs_fetcher_ = NULL;
}

int ObMysqlResultHandler::next()
{
  int ret = OB_SUCCESS;
  if (NULL == rs_fetcher_) {
    if (OB_FAIL(get_resultset_fetcher(rs_fetcher_))) {
      LOG_WARN("fail to get rs_fetcher", K(ret));
    }
  }

  if (OB_SUCC(ret) && (NULL != rs_fetcher_)) {
    ret = rs_fetcher_->next();
  }

  return ret;
}

int ObMysqlResultHandler::get_resultset_fetcher(ObResultSetFetcher *&rs_fetcher)
{
  int ret = OB_SUCCESS;
  rs_fetcher = NULL;
  if (!is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("result handler is not valid", K_(resp), K(ret));
  } else {
    if (resp_->is_error_resp()) {
      ret = -resp_->get_err_code();
      LOG_WARN("fail to execute sql", K(ret));
    } else if (resp_->is_resultset_resp()) {
      if (OB_FAIL(resp_->get_resultset_fetcher(rs_fetcher))) {
        LOG_WARN("fail to get resultset fetcher", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected resp", K(ret));
    }
  }

  return ret;
}

int64_t ObMysqlResultHandler::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(resp), KP_(rs_fetcher));
  J_OBJ_END();
  return pos;
}

//------------------------- ObClientReuqestInfo--------------------------------//
void ObClientReuqestInfo::reset()
{
  reset_names();
  need_skip_stage2_ = false;
  request_param_.reset();
}

void ObClientReuqestInfo::reset_names()
{
  if ((NULL != name_) && (name_len_ > 0)) {
    op_fixed_mem_free(name_, name_len_);
  }
  name_ = NULL;
  name_len_ = 0;
  user_name_.reset();
  database_name_.reset();
  password_.reset();
  password0_.reset();
  password1_.reset();
  using_password_num_ = -1;
}

void ObClientReuqestInfo::reset_sql()
{
  request_param_.reset();
}

int64_t ObClientReuqestInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(user_name),
       K_(database_name),
       K_(request_param));
  J_OBJ_END();
  return pos;
}

int ObClientReuqestInfo::set_names(const ObString &user_name,
                                   const ObString &password,
                                   const ObString &database_name,
                                   const ObString &password1)
{
  int ret = OB_SUCCESS;
  if (user_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user_name can not be NULL", K(user_name), K(ret));
  } else {
    reset_names();
    int64_t total_len = user_name.length() + password.length() + password1.length() + database_name.length();
    if (OB_ISNULL(name_ = static_cast<char *>(op_fixed_mem_alloc(total_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate mem", "alloc size", total_len, K(ret));
    } else {
      int64_t pos = 0;
      name_len_ = total_len;
      MEMCPY(name_, user_name.ptr(), user_name.length());
      user_name_.assign_ptr(name_, user_name.length());
      pos += user_name.length();

      if (!password.empty()) {
        MEMCPY(name_ + pos, password.ptr(), password.length());
        password_.assign_ptr(name_ + pos, password.length());
        password0_ = password_;
        pos += password.length();
      }
      
      if (!password1.empty()) {
        MEMCPY(name_ + pos, password1.ptr(), password1.length());
        password1_.assign_ptr(name_ + pos, password1.length());
        pos += password1.length();
      }

      if (!password.empty() && !password1.empty()) {
        using_password_num_ = 0;
      }

      if (!database_name.empty()) {
        MEMCPY(name_ + pos, database_name.ptr(), database_name.length());
        database_name_.assign_ptr(name_ + pos, database_name.length());
        pos += database_name.length();
      }
      if (pos != total_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pos must be equal to total len", K(pos), K(total_len), K(ret));
      }
    }
  }

  return ret;
}

int ObClientReuqestInfo::set_request_param(const ObMysqlRequestParam &request_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!request_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reqeust sql can not be NULL", K(request_param), K(ret));
  } else if (OB_FAIL(request_param_.deep_copy(request_param))) {
    LOG_WARN("fail to deep_copy request_param_", K(ret));
  }

  return ret;
}

int ObClientUtils::get_auth_password(const ObString &raw_pwd, const ObString &scramble,
                                     char *pwd_buf, int64_t buf_len, int64_t &copy_len)
{
  return ObEncryptedHelper::encrypt_password(raw_pwd, scramble, pwd_buf, buf_len, copy_len);
}

int ObClientUtils::get_auth_password_from_stage1(const ObString &passwd_stage1,
    const common::ObString &scramble_string, char *pwd_buf, const int64_t pwd_len, int64_t &copy_len)
{
  int ret = OB_SUCCESS;
  char passwd_stage1_hex[SCRAMBLE_LENGTH] = {0};
  ObString passwd_stage1_hex_str(SCRAMBLE_LENGTH, passwd_stage1_hex);
  //1. we restore the stored, displayable stage1 hash to its hex form
  if (OB_FAIL(ObEncryptedHelper::displayable_to_hex(passwd_stage1, passwd_stage1_hex_str))) {
    LOG_WARN("fail to displayable_to_hex", K(passwd_stage1), K(ret));
  //2. we call the mysql validation logic.
  } else if (OB_FAIL(ObEncryptedHelper::encrypt_stage1_hex(passwd_stage1_hex_str,
      scramble_string, pwd_buf, pwd_len, copy_len))) {
    LOG_WARN("fail to encrypt_stage1", K(passwd_stage1), K(passwd_stage1_hex_str), K(ret));
  }
  return ret;
}

int ObClientUtils::get_scramble(ObIOBufferReader *response_reader, char *buf,
                                const int64_t buf_len, int64_t &copy_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(response_reader)
      || OB_UNLIKELY(response_reader->read_avail() <= MYSQL_NET_HEADER_LENGTH)
      || OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(response_reader), KP(buf), K(buf_len), K(ret));
  } else {
    OMPKHandshake handshake;
    ObMysqlPacketReader pkt_reader;
    if (OB_FAIL(pkt_reader.get_packet(*response_reader, handshake))) {
      LOG_WARN("fail to get handshake packet", K(ret));
    } else if (OB_FAIL(handshake.get_scramble(buf, buf_len, copy_len))) {
      LOG_WARN("fail to get scramble", K(ret));
    }
  }

  return ret;
}

int ObClientUtils::build_handshake_response_packet(
    ObClientMysqlResp *handshake,
    ObClientReuqestInfo *info,
    ObMIOBuffer *handshake_resp_buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handshake) || OB_ISNULL(info) || OB_ISNULL(handshake_resp_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(handshake), K(info),
             K(handshake_resp_buf), K(ret));
  } else {
    // prepare handshake response packet
    OMPKHandshakeResponse login_hsr;
    login_hsr.set_seq(1);
    ObMySQLCapabilityFlags flag(ObClientUtils::CAPABILITY_FLAGS);
    login_hsr.set_max_packet_size(16777216);
    login_hsr.set_character_set(33); // utf8 COLLATE utf8_general_ci
    login_hsr.set_username(info->get_user_name());
    if (!info->get_database_name().empty()) {
      login_hsr.set_database(info->get_database_name());
      flag.cap_flags_.OB_CLIENT_CONNECT_WITH_DB = 1;
    }
    // now raw client do not compress protocol
    flag.cap_flags_.OB_CLIENT_COMPRESS = 0;
    flag.cap_flags_.OB_CLIENT_SUPPORT_ORACLE_MODE = 1;
    login_hsr.set_capability_flags(flag);

    // encrypt user password
    const int64_t pwd_buf_len = SHA1_HASH_SIZE + 1;
    char pwd_buf[pwd_buf_len] = {0};
    int64_t actual_len = 0;
    char scramble_buf[SCRAMBLE_LENGTH + 1] = {0};

    //1. get challenge random number
    if (OB_FAIL(ObClientUtils::get_scramble(handshake->get_response_reader(),
        scramble_buf, obmysql::OMPKHandshake::SCRAMBLE_TOTAL_SIZE, actual_len))) {
      LOG_WARN("fail to get scramble", K(ret));
    } else {
      ObString scramble_string(actual_len, scramble_buf);
      const ObString &passwd_stage1 = info->get_password();
     if (info->is_need_skip_stage2()) {
        LOG_DEBUG("need_skip_stage2");
        login_hsr.set_auth_response(passwd_stage1);
      } else if (!passwd_stage1.empty()) {
        //2. get auth_password from stage1
        if (OB_FAIL(ObClientUtils::get_auth_password_from_stage1(passwd_stage1,
            scramble_string, pwd_buf, pwd_buf_len, actual_len))) {
          LOG_WARN("fail to get get_auth_password_from_stage1", K(ret));
        } else {
          ObString auth_str(actual_len, pwd_buf);
          login_hsr.set_auth_response(auth_str);
          LOG_DEBUG("succ to encrypt passwd", "user", info->get_user_name(),
                   K(scramble_string), K(passwd_stage1), K(auth_str.hash()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMysqlPacketWriter::write_packet(*handshake_resp_buf, login_hsr))) {
        LOG_WARN("fail to write hsr pkt", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("build handshake response packet succ", K(login_hsr));
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
