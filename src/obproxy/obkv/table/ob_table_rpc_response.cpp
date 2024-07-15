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
#include "ob_table_rpc_response.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::common;

int ObRpcTableLoginResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(login_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login response wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}

int64_t ObRpcTableLoginResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += login_res_.get_serialize_size();
  return len;
}

int ObRpcTableLoginResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableLoginResponse &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  rpc_result_code_ = other.rpc_result_code_;

  if (OB_FAIL(ob_write_string(allocator, other.login_res_.server_version_, login_res_.server_version_))) {
    LOG_WDIAG("fail to call ob_write_string for server_version", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.login_res_.credential_, login_res_.credential_))) {
    LOG_WDIAG("fail to call ob_write_string for credential_", K(ret));
  } else {
    login_res_.server_capabilities_ = other.login_res_.server_capabilities_;
    login_res_.reserved1_ = other.login_res_.reserved1_;  // always 0 for now
    login_res_.reserved2_ = other.login_res_.reserved2_;  // always 0 for now
    login_res_.tenant_id_ = other.login_res_.tenant_id_;
    login_res_.user_id_ = other.login_res_.user_id_;
    login_res_.database_id_ = other.login_res_.database_id_;
  }

  return ret;
}
int ObRpcTableLoginResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other) || other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_LOGIN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid to copy ObRpcTableLoginResponse object", K(ret), K(other));
  } else {
    ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableLoginResponse *>(other)));
  }
  return ret;
}

int ObRpcTableLoginResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableLoginResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化login_result
    OB_UNIS_ENCODE(login_res_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}

int ObRpcTableOperationResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  //table_res_.set_entity(res_entity_); // set res entity for deserialize
  if (OB_FAIL(table_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}

/*int ObRpcTableOperationResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableOperationResponse &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  rpc_result_code_ = other.rpc_result_code_;

  if (OB_FAIL(table_res_.deep_copy(allocator, res_entity_, other.table_res_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableOperationResult", K(ret));
  }

  return ret;
}

int ObRpcTableOperationResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other) || other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_EXECUTE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid to copy ObRpcTableOperationResponse object", K(ret), K(other));
  } else {
    ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableOperationResponse *>(other)));
  }
  return ret;
}
*/
int64_t ObRpcTableOperationResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += table_res_.get_serialize_size();
  return len;
}

int ObRpcTableOperationResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableOperationResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化table_res
    OB_UNIS_ENCODE(table_res_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}
/*
int ObRpcTableBatchOperationResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableBatchOperationResponse &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  rpc_result_code_ = other.rpc_result_code_;

  if (OB_FAIL(batch_res_.deep_copy(allocator, res_entity_factory_, other.batch_res_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableBatchOperationResult", K(ret));
  }

  return ret;
}

int ObRpcTableBatchOperationResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other) || other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_BATCH_EXECUTE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid to copy ObRpcTableBatchOperationResponse object", K(ret), K(other));
  } else {
    ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableBatchOperationResponse *>(other)));
  }
  return ret;
}*/

int ObRpcTableBatchOperationResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  //batch_res_.set_entity_factory(&res_entity_factory_);  // set entity factory for deserialize
  if (OB_FAIL(batch_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}

int64_t ObRpcTableBatchOperationResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += batch_res_.get_serialize_size();
  return len;
}

int ObRpcTableBatchOperationResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableBatchOperationResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化batch_res
    OB_UNIS_ENCODE(batch_res_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}
/*
int ObRpcTableQueryResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryResponse &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  rpc_result_code_ = other.rpc_result_code_;

  if (OB_FAIL(query_res_.deep_copy(allocator, other.query_res_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableQueryResult", K(ret));
  }

  return ret;
}

int ObRpcTableQueryResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other) || (other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_EXECUTE_QUERY
                            && other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid to copy ObRpcTableQueryResponse object", K(ret), K(other));
  } else {
    ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableQueryResponse *>(other)));
  }
  return ret;
}
*/
int ObRpcTableQueryResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(query_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}

int64_t ObRpcTableQueryResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += query_res_.get_serialize_size();
  return len;
}

int ObRpcTableQueryResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableQueryResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化query_res
    OB_UNIS_ENCODE(query_res_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}

int ObRpcTableQueryAndMutateResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(query_and_mutate_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize query and mutate wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}
/*
int ObRpcTableQueryAndMutateResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryAndMutateResponse &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  rpc_result_code_ = other.rpc_result_code_;

  if (OB_FAIL(query_and_mutate_res_.deep_copy(allocator, other.query_and_mutate_res_))) {
    LOG_WDIAG("fail to call deep_copy for ObTableQueryAndMutateResult", K(ret));
  }

  return ret;
}

int ObRpcTableQueryAndMutateResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other) || (other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_QUERY_AND_MUTATE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid to copy ObRpcTableQueryAndMutateResponse object", K(ret), K(other));
  } else {
    ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableQueryAndMutateResponse *>(other)));
  }
  return ret;
}
*/
int64_t ObRpcTableQueryAndMutateResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += query_and_mutate_res_.get_serialize_size();
  return len;
}

int ObRpcTableQueryAndMutateResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableQueryAndMutateResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化query_res
    OB_UNIS_ENCODE(query_and_mutate_res_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}

int ObRpcTableMoveResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) { //<ob-4.x, such as ob-3.x/ob-2.x
    if (OB_FAIL(move_result_.deserialize(buf, buf_len, pos))) {
      LOG_WDIAG("deserialize move response wrong", K(buf), K(buf_len), K(ret));
    }
  } else {
    if (OB_FAIL(move_result_.deserialize_v4(buf, buf_len, pos))) {
      LOG_WDIAG("deserialize move response wrong v4", K(buf), K(buf_len), K(ret));
    }
  }

  return ret;
}

// int ObRpcTableMoveResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryAndMutateResponse &other)
// {
//   int ret = OB_SUCCESS;
//   rpc_packet_meta_ = other.rpc_packet_meta_;
//   rpc_result_code_ = other.rpc_result_code_;

//   if (OB_FAIL(query_and_mutate_res_.deep_copy(allocator, other.query_and_mutate_res_))) {
//     LOG_WDIAG("fail to call deep_copy for ObTableQueryAndMutateResult", K(ret));
//   }

//   return ret;
// }

// int ObRpcTableMoveResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
// {
//   int ret = OB_SUCCESS;
//   if (OB_ISNULL(other) || (other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_QUERY_AND_MUTATE)) {
//     ret = OB_INVALID_ARGUMENT;
//     LOG_WDIAG("invalid to copy ObRpcTableQueryAndMutateResponse object", K(ret), K(other));
//   } else {
//     ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableQueryAndMutateResponse *>(other)));
//   }
//   return ret;
// }

int64_t ObRpcTableMoveResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += move_result_.get_serialize_size();
  return len;
}

int ObRpcTableMoveResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableMoveResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化query_res
    OB_UNIS_ENCODE(move_result_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}
/*
int ObRpcTableQuerySyncResponse::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQuerySyncResponse &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  rpc_result_code_ = other.rpc_result_code_;

  if (OB_FAIL(query_res_.deep_copy(allocator, other.query_res_))) {
    LOG_WDIAG("fail to call deep_copy for ObRpcTableQuerySyncResponse", K(ret));
  }

  return ret;
}

int ObRpcTableQuerySyncResponse::deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other) || (other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_EXECUTE_QUERY 
                            && other->get_packet_meta().get_pcode() != obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid to copy ObRpcTableQuerySyncResponse object", K(ret), K(other));
  } else {
    ret = deep_copy(allocator, *(dynamic_cast<ObRpcTableQuerySyncResponse *>(other)));
  }
  return ret;
}*/

int ObRpcTableQuerySyncResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(query_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}

int64_t ObRpcTableQuerySyncResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += query_res_.get_serialize_size();
  return len;
}

int ObRpcTableQuerySyncResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableQuerySyncResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化query_res
    OB_UNIS_ENCODE(query_res_);
    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) { 
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }

  return ret;
}

int ObRpcTableDirectLoadResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(direct_load_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", K(buf), K(buf_len), K(ret));
  }

  return ret;
}

int ObRpcTableLSOperationResponse::analyze_response(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  //ls_res_.set_entity_factory(&res_entity_factory_);
  //ls_res_.set_allocator(&allocator_);
  if (OB_FAIL(ls_res_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize ls operation response wrong", K(ret), K(buf), K(buf_len), K(pos));
  }
  return ret;
}


int64_t ObRpcTableLSOperationResponse::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcResponse::get_encode_size();
  len += ls_res_.get_serialize_size();
  return len;
}

int ObRpcTableLSOperationResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableLSOperationResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)
    // 序列化batch_res
    OB_UNIS_ENCODE(ls_res_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t response_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), response_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + response_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        // success
      }
    }
  }
  return ret;
}


