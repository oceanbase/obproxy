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
#include "obkv/table/ob_rpc_struct.h"
#include "opsql/parser/ob_proxy_parse_result.h"
#include "ob_table_rpc_request.h"
#include "lib/oblog/ob_log.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "share/part/ob_part_mgr_util.h"
#include "lib/hash/ob_hashset.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_entry.h"
#include "lib/utility/ob_tablet_id.h"

using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::share::schema;
using namespace oceanbase::obproxy::opsql;


int64_t ObRpcTableLoginRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  len += login_request_.get_serialize_size();
  return len;
}

int ObRpcTableLoginRequest::deep_copy(common::ObIAllocator &allocator, const ObRpcTableLoginRequest &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;

  if (OB_FAIL(ob_write_string(allocator, other.login_request_.database_name_, login_request_.database_name_))) {
    LOG_WDIAG("failed to clone database_name string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.login_request_.tenant_name_, login_request_.tenant_name_))) {
    LOG_WDIAG("failed to clone tenant string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.login_request_.user_name_, login_request_.user_name_))) {
    LOG_WDIAG("failed to clone user_name string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.login_request_.pass_scramble_, login_request_.pass_scramble_))) {
    LOG_WDIAG("failed to clone pass_scramble string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.login_request_.pass_secret_, login_request_.pass_secret_))) {
    LOG_WDIAG("failed to clone pass_secret string", K(ret));
  } else {
    login_request_.auth_method_ = other.login_request_.auth_method_;  // always 1 for now
    login_request_.client_type_ = other.login_request_.client_type_;  // 1: libobtable; 2: java client
    login_request_.client_version_ = other.login_request_.client_version_;  // always 1 for now
    login_request_.reserved1_ = other.login_request_.reserved1_;
    login_request_.client_capabilities_ = other.login_request_.client_capabilities_;
    login_request_.max_packet_size_ = other.login_request_.max_packet_size_;  // for stream result
    login_request_.reserved2_ = other.login_request_.reserved2_;  // always 0 for now
    login_request_.reserved3_ = other.login_request_.reserved3_;  // always 0 for now
    login_request_.ttl_us_ = other.login_request_.ttl_us_;  // 0 means no TTL
  }

  return ret;
}

int ObRpcTableLoginRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(login_request_.deserialize(buf, buf_len, pos))) {
    LOG_WDIAG("deserialize login request wrong", KP(buf), K(buf_len), K(pos), K(ret));
  }

  return ret;
}

int ObRpcTableLoginRequest::calc_partition_id(ObArenaAllocator &allocator,
                                              ObRpcReq &ob_rpc_req,
                                              ObProxyPartInfo &part_info,
                                              int64_t &partition_id)
{
  UNUSEDx(allocator, ob_rpc_req, part_info, partition_id);
  LOG_WDIAG("try to calculate partition id of login request", K(lbt()));
  return OB_SUCCESS;
}

int ObRpcTableLoginRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableLoginRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化login_request
    OB_UNIS_ENCODE(login_request_);

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t login_request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), login_request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + login_request_size;

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

int ObRpcTableOperationRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(table_request_.deserialize(buf, buf_len, pos, this))) {
    LOG_WDIAG("deserialize table operation request wrong", KP(buf), K(buf_len), K(ret));
  } else {
    LOG_DEBUG("get position", KPC(this));
  }

  return ret;
}

int ObRpcTableOperationRequest::calc_partition_id(ObArenaAllocator &allocator,
                                                  ObRpcReq &ob_rpc_req,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  int64_t ls_id;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  if(OB_FAIL(calc_partition_id_by_sub_rowkey(allocator, part_info, 0, partition_id, ls_id))) {
    LOG_WDIAG("fail to calc_partition_id_by_sub_rowkey", K(ret), K(rpc_trace_id));
  } else {
    obkv_info.set_definitely_single(true);
    obkv_info.set_partition_id(partition_id);
    obkv_info.set_ls_id(ls_id);
    LOG_DEBUG("calc partition id for table query request", K(ls_id), K(partition_id), KP(this), K(rpc_trace_id));
  }
  return ret;
}

int64_t ObRpcTableOperationRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    len += table_request_.get_serialize_size();
  } else {
    len += table_request_.get_serialize_size_v4();
  }
  return len;
}

int ObRpcTableOperationRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  LOG_DEBUG("ObRpcTableOperationRequest::encode", K(buf), K(buf_len), K(pos));
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableOperationRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化table_request
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableOperationRequest::encode less v4", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(table_request_);
    } else {
      LOG_DEBUG("ObRpcTableOperationRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(table_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for table request");
      }
    }

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

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
        LOG_DEBUG("ObRpcTableOperationRequest::encode succ", K(buf), K(buf_len), K(pos));
        // success
      }
    }
  }

  return ret;
}

/*int ObRpcTableOperationRequest::deep_copy(common::ObIAllocator &allocator, const ObRpcTableOperationRequest &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;
  
  if (OB_FAIL(ob_write_string(allocator, other.table_request_.credential_, table_request_.credential_))) {
    LOG_WDIAG("failed to clone credential string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.table_request_.table_name_, table_request_.table_name_))) {
    LOG_WDIAG("failed to clone table name string", K(ret));
  } else if (OB_FAIL(table_request_.table_operation_.deep_copy(allocator, request_entity_, other.table_request_.table_operation_))) {
    LOG_WDIAG("failed to deep copy table operation", K(ret));
  } else {
    table_request_.table_id_ = other.table_request_.table_id_;  // for optimize purpose
    table_request_.partition_id_ = other.table_request_.partition_id_;  // for optimize purpose
    table_request_.entity_type_ = other.table_request_.entity_type_;  // for optimize purpose
    table_request_.consistency_level_ = other.table_request_.consistency_level_;
    table_request_.returning_rowkey_ = other.table_request_.returning_rowkey_;
    table_request_.returning_affected_entity_ = other.table_request_.returning_affected_entity_;
    table_request_.returning_affected_rows_ = other.table_request_.returning_affected_rows_;
    table_request_.binlog_row_image_type_ = other.table_request_.binlog_row_image_type_;
  }

  return ret;
}
*/
ObRpcTableBatchOperationRequest::ObRpcTableBatchOperationRequest() : batch_request_(), partid_to_index_map_()
{
  LOG_DEBUG("partid_to_index_map_  map init", K(ObModIds::OB_HASH_ALIAS_TABLE_MAP), "addr", &partid_to_index_map_);
  partid_to_index_map_.create(common::OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                     ObModIds::OB_HASH_ALIAS_TABLE_MAP);
}

int ObRpcTableBatchOperationRequest::init_as_sub_batch_operation_request(const ObRpcTableBatchOperationRequest &request,
                                                                         const common::ObIArray<int64_t> &sub_index)
{
  int ret = OB_SUCCESS;
  ObRpcTableBatchOperationRequest &new_quest = const_cast<ObRpcTableBatchOperationRequest &>(request);
  SUB_REQUEST_BUF_ARR sub_op_buf_arr;
  set_credential(new_quest.get_credential());
  // ob_write_string()
  set_table_name(new_quest.get_table_name());
  set_table_id(new_quest.get_table_id());
  set_partition_id(new_quest.get_partition_id());
  set_entity_type(new_quest.get_entity_type());
  set_consistency_level(new_quest.get_consistency_level());
  set_option_flag(new_quest.get_option_flag());
  set_return_affected_entity(new_quest.get_return_affected_entity());
  set_return_affected_rows(new_quest.get_return_affected_rows());
  set_image_type(new_quest.get_image_type());
  get_table_operation().set_readonly(new_quest.get_table_operation().is_readonly());
  get_table_operation().set_same_properties_names(new_quest.get_table_operation().is_same_properties_names());
  get_table_operation().set_same_type(new_quest.get_table_operation().is_same_type());
  if (OB_FAIL(request.get_sub_req_buf_arr(sub_op_buf_arr, sub_index))) {
    LOG_WDIAG("fail to get sub req buf arr", K(ret));
  } else if (OB_FAIL(get_table_operation().set_table_ops(sub_op_buf_arr))) {
    LOG_WDIAG("fail to set sub table operation", K(ret));
  }
  LOG_DEBUG("init sub batch operation succ", KPC(this)); 
  return ret;
}

int ObRpcTableBatchOperationRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // set entity factory for deserialize
  if (OB_FAIL(batch_request_.deserialize(buf, buf_len, pos, this))) {
    LOG_WDIAG("deserialize table operation request wrong", KP(buf), K(buf_len), K(pos));
  } else {
    LOG_DEBUG("get position", KPC(this));
  }

  return ret;
}

int ObRpcTableBatchOperationRequest::calc_partition_id(ObArenaAllocator &allocator,
                                                       ObRpcReq &ob_rpc_req,
                                                       ObProxyPartInfo &part_info,
                                                       int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  partid_to_index_map_.reuse();
  int64_t ls_id;

  for (int i = 0; i < get_sub_req_count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(calc_partition_id_by_sub_rowkey(allocator, part_info, i, partition_id, ls_id))) {
      LOG_WDIAG("fail to calc sub req rowkey for batch operation", "index", i, K(ret), K(rpc_trace_id));
    } else {
      ObSEArray<int64_t, 4> *p_batch_index = const_cast<ObSEArray<int64_t, 4> *>(partid_to_index_map_.get(partition_id));
      if (OB_ISNULL(p_batch_index)) {
        // 当前没有partition_id节点，生成一个
        ObSEArray<int64_t, 4> batch_index;
        if (OB_FAIL(batch_index.push_back(i))) {
          LOG_WDIAG("fail to push back", K(ret), K(i), K(rpc_trace_id));
        } else if (OB_FAIL(partid_to_index_map_.set_refactored(partition_id, batch_index))) {
          LOG_WDIAG("fail to set refactored", K(ret), K(partition_id), K(batch_index), K(rpc_trace_id));
        } else {
          // success
        }
      } else if (OB_FAIL(p_batch_index->push_back(i))) {
        LOG_WDIAG("fail to push back", K(ret), K(i), K(rpc_trace_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (partid_to_index_map_.size() == 1) {
      obkv_info.set_definitely_single(true);
      obkv_info.set_ls_id(ls_id);
      obkv_info.set_partition_id(partition_id);
      // partition_id already setted
    } else if (partid_to_index_map_.size() > 1 && get_batch_operation_as_atomic()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("batch request with atomic flag is a shard request", "count", partid_to_index_map_.size(),
                "addr:", &partid_to_index_map_, K(ret), K(rpc_trace_id));
    } else if (partid_to_index_map_.size() > 1) {
      obkv_info.set_shard(true);
      partition_id = common::OB_INVALID_INDEX;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected batch partition id calculation result", K(ret), K(rpc_trace_id), "part_id_count",
                partid_to_index_map_.size(), KP(this));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("calc partition id for table batch request", KP(this), K(rpc_trace_id));
  }
  return ret;
}

int64_t ObRpcTableBatchOperationRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    len += batch_request_.get_serialize_size();
  } else {
    len += batch_request_.get_serialize_size_v4();
  }
  return len;
}

int ObRpcTableBatchOperationRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableBatchOperationRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化batch_request
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableBatchOperationRequest::encode less v4", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(batch_request_);
    } else {
      LOG_DEBUG("ObRpcTableBatchOperationRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(batch_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for batch request");
      }
    }

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

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
int ObRpcTableBatchOperationRequest::deep_copy(common::ObIAllocator &allocator, const ObRpcTableBatchOperationRequest &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;

  if (OB_FAIL(ob_write_string(allocator, other.batch_request_.credential_, batch_request_.credential_))) {
    LOG_WDIAG("failed to clone credential string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.batch_request_.table_name_, batch_request_.table_name_))) {
    LOG_WDIAG("failed to clone table name string", K(ret));
  } else if (OB_FAIL(batch_request_.batch_operation_.deep_copy(allocator, request_entity_factory_, other.batch_request_.batch_operation_))) {
    LOG_WDIAG("failed to call deep_copy for ObTableBatchOperation", K(ret));
  } else {
    batch_request_.table_id_ = other.batch_request_.table_id_;
    batch_request_.partition_id_ = other.batch_request_.partition_id_;
    batch_request_.entity_type_ = other.batch_request_.entity_type_;
    batch_request_.consistency_level_ = other.batch_request_.consistency_level_;
    batch_request_.option_flag_ = other.batch_request_.option_flag_;
    batch_request_.returning_affected_entity_ = other.batch_request_.returning_affected_entity_;
    batch_request_.returning_affected_rows_ = other.batch_request_.returning_affected_rows_;
    batch_request_.batch_operation_as_atomic_ = other.batch_request_.batch_operation_as_atomic_;
    batch_request_.binlog_row_image_type_ = other.batch_request_.binlog_row_image_type_;
  }

  return ret;
}
*/
int ObRpcTableQueryRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  // query_request_.query_.set_deserialize_allocator(&allocator_);

  if (OB_FAIL(query_request_.deserialize(buf, buf_len, pos, this))) {
    LOG_WDIAG("deserialize table query operation request wrong", KP(buf), K(buf_len), K(pos));
  } else {
    LOG_DEBUG("get position", KPC(this));
  }
  return ret;
}

int ObRpcTableQueryRequest::calc_partition_id(common::ObArenaAllocator &allocator,
                                              ObRpcReq &ob_rpc_req,
                                              proxy::ObProxyPartInfo &part_info,
                                              int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObSEArray<int64_t, 1> partition_ids;
  ObSEArray<int64_t, 1> ls_ids;
  partition_ids_.reuse(); // clear before calc
  if (OB_FAIL(calc_partition_id_by_sub_range(allocator, part_info, 0, partition_ids, ls_ids))) {
    LOG_WDIAG("fail to calc part id for table query", K(ret), K(rpc_trace_id));
  } else if (OB_FALSE_IT(set_partition_ids(partition_ids))) {
  } else if (partition_ids.count() == 1) {
    partition_id = partition_ids.at(0);
    obkv_info.set_definitely_single(true);
    obkv_info.set_partition_id(partition_ids.at(0));
    obkv_info.set_ls_id(partition_ids.at(0));
    set_partition_id(partition_ids.at(0));
  } else if (partition_ids.count() > 1) {
    obkv_info.set_shard(true);
    partition_id = common::OB_INVALID_INDEX;
  } else {
    obkv_info.set_empty_query_result(true);
    partition_id = common::OB_INVALID_INDEX;
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("calc partition id for table query request", K(ls_ids), K(partition_ids), KP(this), K(rpc_trace_id));
  }
  return ret;
}
/*
int ObRpcTableQueryRequest::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryRequest &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;

  if (OB_FAIL(ob_write_string(allocator, other.query_request_.credential_, query_request_.credential_))) {
    LOG_WDIAG("failed to clone credential string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.query_request_.table_name_, query_request_.table_name_))) {
    LOG_WDIAG("failed to clone table name string", K(ret));
  } else if (OB_FAIL(query_request_.query_.deep_copy(allocator, other.query_request_.query_))) {
    LOG_WDIAG("failed to call deep_copy for ObTableQuery", K(ret));
  } else {
    query_request_.table_id_ = other.query_request_.table_id_;
    query_request_.partition_id_ = other.query_request_.partition_id_;
    query_request_.entity_type_ = other.query_request_.entity_type_;
    query_request_.consistency_level_ = other.query_request_.consistency_level_;
  }

  return ret;
}
*/
int64_t ObRpcTableQueryRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    len += query_request_.get_serialize_size();
  } else {
    len += query_request_.get_serialize_size_v4();
  }
  return len;
}

int ObRpcTableQueryRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableQueryRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化query_request
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableQueryRequest::encode less v4", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(query_request_);
    } else {
      LOG_DEBUG("ObRpcTableQueryRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(query_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for query request");
      }
    }

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

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

int ObRpcTableQueryAndMutateRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  //query_and_mutate_request_.query_and_mutate_.set_deserialize_allocator(&allocator_);
  //query_and_mutate_request_.query_and_mutate_.get_mutations().set_entity_factory(&request_entity_factory_);

  if (OB_FAIL(query_and_mutate_request_.deserialize(buf, buf_len, pos, this))) {
    LOG_WDIAG("deserialize table query operation request wrong", KP(buf), K(buf_len), K(pos));
  } else {
    LOG_DEBUG("get position", KPC(this));
  }

  return ret;
}

int ObRpcTableQueryAndMutateRequest::calc_partition_id(common::ObArenaAllocator &allocator,
                                                       ObRpcReq &ob_rpc_req,
                                                       proxy::ObProxyPartInfo &part_info,
                                                       int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObSEArray<int64_t, 1> partition_ids;
  ObSEArray<int64_t, 1> ls_ids;
  partition_ids_.reuse(); // clear before calc
  if (OB_FAIL(calc_partition_id_by_sub_range(allocator, part_info, 0, partition_ids, ls_ids))) {
    LOG_WDIAG("fail to calc part id for table query", K(ret), K(rpc_trace_id));
  } else if (OB_FALSE_IT(set_partition_ids(partition_ids))) {
  } else if (partition_ids.count() == 1) {
    partition_id = partition_ids.at(0);
    set_partition_id(partition_ids.at(0));
    obkv_info.set_definitely_single(true);
    obkv_info.set_partition_id(partition_ids.at(0));
    obkv_info.set_ls_id(partition_ids.at(0));
  } else if (partition_ids.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    partition_id = common::OB_INVALID_INDEX;
    LOG_WDIAG("OB_TABLE_API_QUERY_AND_MUTATE get more than one partition_id, not support", K(ret), K(partition_ids),
              K(rpc_trace_id));
  } else {
    obkv_info.set_empty_query_result(true);
    partition_id = common::OB_INVALID_INDEX;
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("calc partition id for query and mutate request", K(ls_ids), K(partition_ids), KP(this), K(rpc_trace_id));
  }
  return ret;
}

int64_t ObRpcTableQueryAndMutateRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    len += query_and_mutate_request_.get_serialize_size();
  } else {
    len += query_and_mutate_request_.get_serialize_size_v4();
  }
  return len;
}

/*int ObRpcTableQueryAndMutateRequest::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryAndMutateRequest &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;

  if (OB_FAIL(ob_write_string(allocator, other.query_and_mutate_request_.credential_, query_and_mutate_request_.credential_))) {
    LOG_WDIAG("failed to clone credential string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.query_and_mutate_request_.table_name_, query_and_mutate_request_.table_name_))) {
    LOG_WDIAG("failed to clone table name string", K(ret));
  } else if (OB_FAIL(query_and_mutate_request_.query_and_mutate_.deep_copy(allocator, request_entity_factory_,other.query_and_mutate_request_.query_and_mutate_))) {
    LOG_WDIAG("failed to call deep_copy for ObTableQueryAndMutate", K(ret));
  } else {
    query_and_mutate_request_.table_id_ = other.query_and_mutate_request_.table_id_;
    query_and_mutate_request_.partition_id_ = other.query_and_mutate_request_.partition_id_;
    query_and_mutate_request_.entity_type_ = other.query_and_mutate_request_.entity_type_;
    query_and_mutate_request_.binlog_row_image_type_ = other.query_and_mutate_request_.binlog_row_image_type_;
  }

  return ret;
}*/

int ObRpcTableQueryAndMutateRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableQueryAndMutateRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化query_request
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableQueryAndMutateRequest::encode less v4", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(query_and_mutate_request_);
    } else {
      LOG_DEBUG("ObRpcTableQueryAndMutateRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(query_and_mutate_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for query request");
      }
    }

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

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

int ObRpcTableQuerySyncRequest::set_partition_ids(ObIArray<int64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  partition_ids_.reuse();
  if (OB_FAIL(partition_ids_.reserve(partition_ids.count()))) {
    LOG_WDIAG("fail to reserve mem for partition ids", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
    if (OB_FAIL(partition_ids_.push_back(partition_ids.at(i)))) {
      LOG_WDIAG("fail to push back partition id", K(ret));
    }
  }
  return ret;
}

int ObRpcTableQuerySyncRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  //query_request_.query_.set_deserialize_allocator(&allocator_);
  if (OB_FAIL(query_request_.deserialize(buf, buf_len, pos, this))) {
    LOG_WDIAG("deserialize table sync query operation request wrong", KP(buf), K(buf_len), K(pos));
  } else {
    LOG_DEBUG("get position", KPC(this));
  }
  return ret;
}

int ObRpcTableQuerySyncRequest::calc_partition_id(common::ObArenaAllocator &allocator,
                                                  ObRpcReq &ob_rpc_req,
                                                  proxy::ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObTableQueryAsyncEntry *query_async_entry = obkv_info.query_async_entry_;
  ObSEArray<int64_t, 1> partition_ids;
  ObSEArray<int64_t, 1> ls_ids;
  partition_ids_.reuse(); // clear before calc
  if (OB_FAIL(calc_partition_id_by_sub_range(allocator, part_info, 0, partition_ids, ls_ids))) {
    LOG_WDIAG("fail to calc part id for table query", K(ret), K(rpc_trace_id));
  } else if (OB_FALSE_IT(set_partition_ids(partition_ids))) {
  } else if (partition_ids.count() == 1) {
    partition_id = partition_ids.at(0);
    obkv_info.set_definitely_single(true);
    obkv_info.set_partition_id(partition_ids.at(0));
    obkv_info.set_ls_id(partition_ids.at(0));
    set_partition_id(partition_ids.at(0));
    if (OB_NOT_NULL(query_async_entry)) {
      query_async_entry->reset_tablet_ids();
      query_async_entry->get_tablet_ids().push_back(partition_id);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("single async query without query_async_entry", K(ret), K(obkv_info), K(rpc_trace_id));
    }
  } else if (partition_ids.count() > 1) {
    obkv_info.set_partition_id(partition_ids.at(0));
    partition_id = partition_ids.at(0);
    if (OB_NOT_NULL(query_async_entry)) {
      query_async_entry->reset_tablet_ids();
      query_async_entry->get_tablet_ids().assign(partition_ids);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("shard async query without query_async_entry", K(ret), K(obkv_info), K(rpc_trace_id));
    }
  } else {
    obkv_info.set_empty_query_result(true);
    partition_id = common::OB_INVALID_INDEX;
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("calc partition id for table query sync request", K(ls_ids), K(partition_ids), KP(this), K(rpc_trace_id));
  }
  return ret;
}

/*
int ObRpcTableQuerySyncRequest::deep_copy(common::ObIAllocator &allocator, const ObRpcTableQuerySyncRequest &other)
{
  int ret = OB_SUCCESS;
  rpc_packet_meta_ = other.rpc_packet_meta_;

  if (OB_FAIL(ob_write_string(allocator, other.query_request_.credential_, query_request_.credential_))) {
    LOG_WDIAG("failed to clone credential string", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.query_request_.table_name_, query_request_.table_name_))) {
    LOG_WDIAG("failed to clone table name string", K(ret));
  } else if (OB_FAIL(query_request_.query_.deep_copy(allocator, other.query_request_.query_))) {
    LOG_WDIAG("failed to call deep_copy for ObTableQuery", K(ret));
  } else {
    query_request_.table_id_ = other.query_request_.table_id_;
    query_request_.partition_id_ = other.query_request_.partition_id_;
    query_request_.entity_type_ = other.query_request_.entity_type_;
    query_request_.consistency_level_ = other.query_request_.consistency_level_;
  }

  return ret;
}*/

int64_t ObRpcTableQuerySyncRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    len += query_request_.get_serialize_size();
  } else {
    len += query_request_.get_serialize_size_v4();
  }
  return len;
}
int ObRpcTableQuerySyncRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;
  check_sum_pos = pos;

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableQuerySyncRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化query_request
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableQuerySyncRequest::encode less v4", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(query_request_);
    } else {
      LOG_DEBUG("ObRpcTableQuerySyncRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(query_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for sync query request");
      }
    }
    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

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

int ObRpcTableDirectLoadRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  // deserialize in in-place mode to avoid alloc buffer in decode procedure.
  // direct_load_request_.query_.set_deserialize_allocator(&allocator_);

  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("not supported load data direct in ob-cluster which less than 4.0.0", K(ret), K(cluster_version_));
  } else {
    if (OB_FAIL(direct_load_request_.deserialize_get_position_v4(buf, buf_len, pos, REQUEST_REWRITE_ARG_VALUE))) {
      LOG_WDIAG("deserialize_v4 table direct load operation request wrong", KP(buf), K(buf_len), K(pos));
    } else {
      LOG_DEBUG("get position", KPC(this));
    }
  }

  return ret;
}

int ObRpcTableDirectLoadRequest::calc_partition_id(ObArenaAllocator &allocator,
                                                   ObRpcReq &ob_rpc_req,
                                                   ObProxyPartInfo &part_info,
                                                   int64_t &partition_id)
{
  UNUSEDx(allocator, ob_rpc_req, part_info, partition_id);
  LOG_WDIAG("try to calculate partition id of login request", K(lbt()));
  return OB_SUCCESS;
}

ObRpcTableLSOperationRequest::ObRpcTableLSOperationRequest() : ls_request_(), tablet_id_index_map_(),
                                   ls_id_tablet_id_map_() {
  tablet_id_index_map_.create(common::OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                              ObModIds::OB_HASH_ALIAS_TABLE_MAP);
  ls_id_tablet_id_map_.create(common::OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                              ObModIds::OB_HASH_ALIAS_TABLE_MAP);
}

int ObRpcTableLSOperationRequest::analyze_request(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // set entity factory for deserialize
  //ls_request_.ls_op_.set_entity_factory(&request_entity_factory_);
  //ls_request_.ls_op_.set_deserialize_allocator(&allocator_);
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("not supported handle LSOps in ob-cluster which less than 4.0.0", K(ret), K(cluster_version_));
  } else {
    if (OB_FAIL(ls_request_.deserialize(buf, buf_len, pos, this))) {
      LOG_WDIAG("deserialize_v4 table operation request wrong", KP(buf), K(buf_len), K(pos));
    } else {
      LOG_DEBUG("get position", KPC(this));
    }
  }

  return ret;
}

int ObRpcTableLSOperationRequest::calc_partition_id(common::ObArenaAllocator &allocator,
                                                    ObRpcReq &ob_rpc_req,
                                                    proxy::ObProxyPartInfo &part_info,
                                                    int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id;
  int64_t ls_id;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();

  tablet_id_index_map_.reuse(); // clear before calc
  ls_id_tablet_id_map_.reuse();
  int offset = 0;
  ObSEArray<ObTableTabletOp, SUB_REQ_COUNT> &tablet_ops = get_operation().get_tablet_ops();
  if (tablet_ops.count() > 1) {
    LOG_DEBUG("received multi tablet ops request, maybe retrying inner request", K(rpc_trace_id), K(&ob_rpc_req),
              "is_inner_request_retrying", obkv_info.is_inner_req_retrying());
  }
  for (int64_t i = 0; i < tablet_ops.count() && OB_SUCC(ret); ++i) {
    int64_t single_ops_count = tablet_ops.at(i).get_single_ops().count();
    for (int64_t j = 0; j < single_ops_count && OB_SUCC(ret); j++) {
      // partition id calculation depends on op_type
      if (OB_FAIL(calc_partition_id_by_sub_rowkey(allocator, part_info, offset, tablet_id, ls_id))) {
        LOG_WDIAG("fail to calc tablet id for single operation", K(ret), K(offset), K(rpc_trace_id));
      } else if (OB_FAIL(record_ls_tablet_index(ls_id, tablet_id, offset))) {
        LOG_WDIAG("fail to record log stream id/ tablet_id/ index", K(ls_id), K(tablet_id), "tablet_index", i,
                  "single_index", j, "offset", offset, K(ret));
      } else {
        offset++;
        LOG_DEBUG("log stream operation partition calc succ log stream id/tablet_id/index", K(ls_id), K(tablet_id),
                  "tablet_index", i, "single_index", j, "offset", offset, K(rpc_trace_id));
      }
    }
    if (OB_SUCC(ret)) {
      // we still use tablet id to determine whether this is a single partition req
      // because if one ls_id contians multi tabelt_id, we have to rewrite whole req
      if (tablet_id_index_map_.size() == 1) {
        obkv_info.set_definitely_single(true);
        obkv_info.set_ls_id(ls_id);
        obkv_info.set_partition_id(tablet_id);
        partition_id = tablet_id;
      } else if (tablet_id_index_map_.size() > 1) {
        obkv_info.set_shard(true);
        partition_id = common::OB_INVALID_INDEX;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected log stream partition id calculation result", K(ret), K(rpc_trace_id), "part_id_count",
                  tablet_id_index_map_.size(), KP(this));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("calc partition id for table lsop request", KP(this), K(rpc_trace_id));
  }
  return ret;
}

int ObRpcTableLSOperationRequest::record_ls_tablet_index(const int ls_id,
                                                         const int tablet_id,
                                                         const int index)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> *p_tablet_index_batch = const_cast<ObSEArray<int64_t, 4> *>(tablet_id_index_map_.get(tablet_id));
  if (OB_ISNULL(p_tablet_index_batch)) {
    ObSEArray<int64_t, 4> index_arr;
    if (OB_FAIL(index_arr.push_back(index))) {
      LOG_WDIAG("fail to push back index", K(ret));
    } else if (OB_FAIL(tablet_id_index_map_.set_refactored(tablet_id, index_arr))) {
      LOG_WDIAG("fail to record index", K(ret));
    } else {
      // new tablet id, record ls_id
      ObSEArray<int64_t, 4> *p_ls_tablet_batch = const_cast<ObSEArray<int64_t, 4> *>(ls_id_tablet_id_map_.get(ls_id));
      if (OB_ISNULL(p_ls_tablet_batch)) {
        ObSEArray<int64_t, 4> tablet_id_arr;
        if (OB_FAIL(tablet_id_arr.push_back(tablet_id))) {
          LOG_WDIAG("fail to push back tablet id", K(ret));
        } else if (OB_FAIL(ls_id_tablet_id_map_.set_refactored(ls_id, tablet_id_arr))) {
          LOG_WDIAG("fail to record ls id and tablet id record", K(ret));
        }
      } else if (OB_FAIL(p_ls_tablet_batch->push_back(tablet_id))) {
        LOG_WDIAG("fail to push tablet id", K(ret));
      }
    }
  } else if (OB_FAIL(p_tablet_index_batch->push_back(index))){
    LOG_WDIAG("fail to push back index", K(ret));
  }

  return ret;
}
int64_t ObRpcTableLSOperationRequest::get_encode_size() const
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
    LOG_WDIAG("not supported handle LSOps in ob-cluster which less than 4.0.0", K(ret), K(cluster_version_));
  } else {
    len += ls_request_.get_serialize_size();
  }
  return len;
}

int ObRpcTableLSOperationRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableLSOperationRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化batch_request
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("not supported handle LSOps in ob-cluster which less than 4.0.0", K(ret), K(cluster_version_));
    } else {
      LOG_DEBUG("ObRpcTableLSOperationRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(ls_request_.serialize(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for ls request");
      }
    }

    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

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
int ObRpcTableLSOperationRequest::init_as_sub_ls_operation_request(const ObRpcTableLSOperationRequest &request,
                                                                   const int64_t ls_id,
                                                                   const common::ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS; 
  set_credential(request.get_credential());
  set_entity_type(request.get_entity_type());
  set_consistency_level(request.get_consistency_level());
  set_table_id(request.get_table_id());
  set_ls_id(ls_id);
  set_table_name(request.get_table_name());
  set_packet_meta(request.get_packet_meta());
  set_option_flag(request.get_option_flag());
  set_dictionary(request.get_all_rowkey_names(), request.get_all_properties_names());

  if (OB_FAIL(init_tablet_ops(request, tablet_ids))) {
    LOG_WDIAG("fail to init tablet ops", K(ret));
  }
  return ret;
}

int ObRpcTableLSOperationRequest::init_tablet_ops(const ObRpcTableLSOperationRequest &root_request,
                                                  const common::ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> *index = NULL;
  const TABLET_ID_INDEX_MAP &tablet_id_map = root_request.get_tablet_id_index_map();
  const ObTableLSOp &root_lsop = root_request.get_operation();
  const ObTableTabletOp &root_tablet_op = root_lsop.get_tablet_ops().at(0); //used for set option flag

  int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
  int64_t sub_req_count = 0;

  // calc sub req count
  for (int i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    index = const_cast<ObSEArray<int64_t, 4>*>(tablet_id_map.get(tablet_ids.at(i)));
    if (OB_ISNULL(index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected tablet single index", K(ret));
    } else {
      sub_req_count += index->count();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_rowkey_info(sub_req_count))) {
      LOG_WDIAG("fail to init rowkey info", K(ret));
    } else if (OB_FAIL(get_operation().get_tablet_ops().prepare_allocate(tablet_ids.count()))) {
      LOG_WDIAG("fail to prepare allocate for tablet ops", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    // init tablet_op
    SUB_REQUEST_BUF_ARR single_op_buf_arr;
    ObTableTabletOp &tablet_op = get_operation().get_tablet_ops().at(i);
    tablet_id = tablet_ids.at(i);
    index = const_cast<ObSEArray<int64_t, 4>*>(tablet_id_map.get(tablet_ids.at(i)));
    if (OB_ISNULL(index)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to get tablet id index", K(ret));
    } else {
      tablet_op.set_tablet_id(tablet_id);
      tablet_op.set_option_flag(root_tablet_op.get_option_flag());
      if (OB_FAIL(root_request.get_sub_req_buf_arr(single_op_buf_arr, *index))) {
        LOG_WDIAG("fail to get single op buf from original request", K(ret));
      } else if (OB_FAIL(tablet_op.set_single_ops(single_op_buf_arr))) {
        LOG_WDIAG("fail to set singel ops", K(ret));
      }
      for (int j = 0; OB_SUCC(ret) && j < index->count(); j++) {
        int64_t offset = index->at(j);
        if (OB_NOT_NULL(root_request.get_sub_req_buf_arr())
                   && OB_FAIL(add_sub_req_buf(root_request.get_sub_req_buf_arr()->at(offset)))) {
          LOG_WDIAG("fail to init req buf for sub req", K(ret));
        } else if (OB_NOT_NULL(root_request.get_sub_req_rowkey_val_arr())
                   && OB_FAIL(add_sub_req_rowkey_val(root_request.get_sub_req_rowkey_val_arr()->at(offset)))) {
          LOG_WDIAG("fail to init req buf for sub req", K(ret));
        } else if (OB_NOT_NULL(root_request.get_sub_req_columns_arr())
                   && OB_FAIL(add_sub_req_columns(root_request.get_sub_req_columns_arr()->at(offset)))) {
          LOG_WDIAG("fail to init req buf for sub req", K(ret));
        } else if (OB_NOT_NULL(root_request.get_sub_req_range_arr())
                   && root_request.get_sub_req_range_arr()->count() > 0
                   && OB_FAIL(add_sub_req_range(root_request.get_sub_req_range_arr()->at(offset)))) {
          LOG_WDIAG("fail to init req buf for sub req", K(ret));
        }
      }
    }
  }
  return ret;
}
