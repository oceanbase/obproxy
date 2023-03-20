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
#include "utils/ob_target_db_server.h"


namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;

int ObTargetDbServer::init(const char* target_db_server_str, uint64_t target_db_server_str_len)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(target_db_server_str)) {
    ret = OB_INIT_FAIL;
    LOG_WARN("fail to init target db server for NULL str", K(ret));
  } else if (target_db_server_str_len == 0) {
    LOG_DEBUG("succ to init target db server with empty str");
  } else if (OB_ISNULL(target_db_server_buf_ = static_cast<char*>(op_fixed_mem_alloc(target_db_server_str_len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate mem for target db server", K(ret), K(target_db_server_str_len));
  } else {
    target_db_server_buf_len_ = target_db_server_str_len + 1;
    MEMSET(target_db_server_buf_, '\0', target_db_server_buf_len_);
    MEMCPY(target_db_server_buf_, target_db_server_str, target_db_server_str_len);
  
    ObString target_db_server_str(target_db_server_str_len, target_db_server_buf_);
    if (OB_FAIL(split_weight_group(target_db_server_str, target_db_server_, target_db_server_weight_))) {
       LOG_WARN("fail to split target db server str", K(ret));
    } else { /* succ */ }
  }

  if (OB_SUCC(ret)) {
    is_init_ = true;
    LOG_DEBUG("succ to init all target db server", K_(target_db_server), K_(target_db_server_weight));
  } else {
    reset();
  }
  return ret;
}

int ObTargetDbServer::get(net::ObIpEndpoint &dest)
{
  int ret = OB_SUCCESS;

  // [weight_start, weight_end) 
  if (!is_init_) {
    ret = OB_INIT_FAIL;
    LOG_WARN("fail to get next, target db server not init", K(ret));
  } else if (last_failed_idx_ == target_db_server_.count() - 1) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("fail to get next, all target db server has failed", K(ret));
  } else if (!is_rand_) {
    ObTargetDbServerArray::iterator weight_begin = target_db_server_.begin();
    for (int64_t i = 0; i < target_db_server_.count(); i++) {
      if (i == target_db_server_.count() - 1) {
        std::random_shuffle(weight_begin, target_db_server_.end());
      } else if (target_db_server_weight_.at(i) != target_db_server_weight_.at(i + 1)) {
        ObTargetDbServerArray::iterator weight_end = target_db_server_.begin() + (i + 1);
        std::random_shuffle(weight_begin, weight_end);
        weight_begin = weight_end;
      }
    }
    is_rand_ = true;
  }
  if (OB_SUCC(ret)) {
    ObString addr;
    uint64_t idx = last_failed_idx_ + 1;
    if (OB_FAIL(target_db_server_.at(idx, addr))) {
      LOG_WARN("fail to access target db server array", K(ret), K(idx));
    } else if (OB_FAIL(ops_ip_pton(addr, dest))) {
      LOG_WARN("fail to do ops_ip_pton on target db server", K(ret), K(addr));
    } else {
      LOG_DEBUG("succ to get target db server", K(dest));
    } 
  }
  return ret;
}

int ObTargetDbServer::get_next(net::ObIpEndpoint &dest_addr)
{
  // mark previous one as failed
  last_failed_idx_++;
  return get(dest_addr);
}

void ObTargetDbServer::reset() 
{
  is_init_ = false;
  is_rand_ = false;
  last_failed_idx_ = -1;
  if (OB_NOT_NULL(target_db_server_buf_)) {
    op_fixed_mem_free(target_db_server_buf_, target_db_server_buf_len_);
    target_db_server_buf_len_ = 0;
    target_db_server_buf_ = NULL;
  }
  target_db_server_.reset();
  target_db_server_weight_.reset();
}

bool ObTargetDbServer::contains(net::ObIpEndpoint &addr) 
{
  bool ret = false;
  if (is_init_) {
    char addr_buf[MAX_IP_ADDR_LENGTH];
    MEMSET(addr_buf, '\0', MAX_IP_ADDR_LENGTH);
    // get like '{xxx.xxxx.xxx.xxx:xxxx}'
    addr.to_string(addr_buf, sizeof(addr_buf));
    // remove the last '}'
    addr_buf[strlen(addr_buf) - 1] = '\0';
    ObTargetDbServerArray::iterator it = target_db_server_.begin();
    while (it != target_db_server_.end()) {
      // skip the first '{'
      if (it->case_compare(addr_buf + 1) == 0) {
        ret = true;
        break;
      }
      it++;
    }
  } else {
    LOG_DEBUG("target db server not init!");
  }
  return ret;
}

int64_t ObTargetDbServer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_init), K_(is_rand), K_(last_failed_idx), K_(target_db_server));
  J_OBJ_END();
  return pos;
}
} // namespace obproxy
} // namespace oceanbase