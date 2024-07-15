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
#include "ob_rpc_struct.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "lib/utility/serialization.h"
#include "share/part/ob_part_mgr_util.h"
#include "lib/hash/ob_hashset.h"
#include "proxy/route/obproxy_expr_calculator.h"

using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::share::schema;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::common::serialization;

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

void inline shrink_copy_char_buf(char *dst, char *src, int64_t n)
{
  int64_t i = n;
  for(i = n; i > 0; i--) {
    dst[i] = src[i];
  }
}

DEFINE_SERIALIZE(ObRpcEzHeader)
{
  int ret = OB_SUCCESS;

  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
  } else if (buf_len - pos < EZ_HEADER_LEN) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(buf + pos, ObRpcEzHeader::MAGIC_HEADER_FLAG, 4);
    pos += 4;
    if (OB_FAIL(encode_i32(buf, buf_len, pos, ez_payload_size_))) {
      LOG_WDIAG("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, chid_))) {
      LOG_WDIAG("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, reserved_))) {
      LOG_WDIAG("Encode error", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRpcEzHeader)
{
  int ret = OB_SUCCESS;

  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
  } else if (data_len - pos < EZ_HEADER_LEN) {
    ret = OB_INVALID_DATA;
  } else {
    MEMCPY(magic_header_flag_, buf + pos, 4);
    pos += 4;
    if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&ez_payload_size_)))) {
      LOG_WDIAG("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&chid_)))) {
      LOG_WDIAG("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&reserved_)))) {
      LOG_WDIAG("Decode error", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRpcEzHeader)
{
  int64_t len = 0;
  len += 4;   // magic size
  len += 4;   // ez_payload_size
  len += 4;   // chid
  len += 4;   // reserved
  return len;
}

DEFINE_SERIALIZE(ObRpcPacketMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(ez_header_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(const_cast<ObRpcPacketHeader *>(&(this->rpc_header_))->serialize(buf, buf_len, pos))) {
      RPC_WARN("encode object fail",
               "name", MSTR(obj), K(buf_len), K(pos), K(ret));
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObRpcPacketMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(ez_header_);
  OB_UNIS_DECODE(rpc_header_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRpcPacketMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(ez_header_);
  len += ObRpcPacketHeader::get_encoded_size();
  return len;
}

const ObRpcRequest &ObRpcRequest::operator =(const ObRpcRequest &other)
{
  if (this != &other) {
    rpc_packet_meta_ = other.rpc_packet_meta_;
  }
  return *this;
}

void ObRpcRequest::reset()
{
  rpc_packet_meta_.reset();
  if (OB_NOT_NULL(sub_request_buf_arr_)) {
    sub_request_buf_arr_->~SUB_REQUEST_BUF_ARR();
    allocator_.free(sub_request_buf_arr_);
    sub_request_buf_arr_ = NULL;
  }
  if (OB_NOT_NULL(sub_request_rowkey_val_arr_)) {
    sub_request_rowkey_val_arr_->~SUB_REQUEST_ROWKEY_VAL_ARR();
    allocator_.free(sub_request_rowkey_val_arr_);
    sub_request_rowkey_val_arr_ = NULL;
  }
  if (OB_NOT_NULL(sub_request_rowkey_range_arr_)) {
    sub_request_rowkey_range_arr_->~SUB_REQUEST_RANGE_ARR();
    allocator_.free(sub_request_rowkey_range_arr_);
    sub_request_rowkey_range_arr_ = NULL;
  }
  if (OB_NOT_NULL(sub_request_columns_arr_)) {
    sub_request_columns_arr_->~SUB_REQUEST_ROWKEY_COLUMNS_ARR();
    allocator_.free(sub_request_columns_arr_);
    sub_request_columns_arr_ = NULL;
  }
  all_rowkey_names_ = NULL;
  index_name_.reset();
  batch_size_ = -1;
  LOG_DEBUG("ObRpcRequest reset", K(this));
}

int ObRpcRequest::analyze_request(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_NOT_IMPLEMENT;

  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);

  return ret;
}

int ObRpcRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_NOT_IMPLEMENT;

  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);

  return ret;
}

int64_t ObRpcRequest::get_encode_size() const
{
  int64_t len = 0;
  len += rpc_packet_meta_.get_serialize_size();
  return len;
}

int64_t ObRpcRequest::get_part_id_position() const
{
  return partition_id_position_;
}

int64_t ObRpcRequest::get_table_id_position() const
{
  return table_id_position_;
}

int64_t ObRpcRequest::get_ls_id_position() const
{
  return ls_id_postition_;
}

int64_t ObRpcRequest::get_part_id_len() const
{
  return partition_id_len_;
}

int64_t ObRpcRequest::get_table_id_len() const
{
  return table_id_len_;
}

int64_t ObRpcRequest::get_payload_len_position() const
{
  return payload_len_position_;
}

int64_t ObRpcRequest::get_payload_len_len() const
{
  return table_id_position_;
}

int64_t ObRpcRequest::get_ls_id_len() const
{
  return ls_id_len_;
}

inline int ObRpcRequest::adjust_req_buf_if_need(char *buf, int64_t buf_len, int64_t table_id, int64_t partition_id)
{
  int ret = OB_SUCCESS;
  int64_t request_len = rpc_packet_meta_.get_ez_header().ez_payload_size_;
  // bool need_adjust = false;
  if (OB_LIKELY(OB_NOT_NULL(buf) && buf_len >= request_len)) {
    int64_t table_id_need_len = encoded_length_vi64(table_id);
    int64_t partition_id_need_len = encoded_length_vi64(partition_id);
    int64_t added_len = 0;
    int64_t table_id_diff_len = 0;
    int64_t part_id_diff_len = 0;
    int64_t payload_len_diff_len = 0;
    // bool need_adjust_payload_len = false;
    if (get_part_id_len() < partition_id_need_len) {
      // need_adjust = true;
      part_id_diff_len = partition_id_need_len - get_part_id_len();
    }
    if (get_table_id_len() < table_id_need_len) {
      // need_adjust = true;
      table_id_diff_len = partition_id_need_len - get_table_id_len();
    }

    added_len = part_id_diff_len + table_id_diff_len;
    int64_t new_request_payload_len = (request_payload_len_ + added_len);
    int64_t new_payload_len_len = encoded_length_vi64(new_request_payload_len);
    if (OB_UNLIKELY(get_payload_len_len() < new_payload_len_len)) {
      // need_adjust_payload_len = true;
      payload_len_diff_len = new_payload_len_len - get_payload_len_len();
      added_len += payload_len_diff_len;
    }
    if (OB_LIKELY(buf_len >= request_len + added_len)) {
      if (added_len > 0) { //need shrink the buffer
        int64_t pos_new = request_len + added_len - 1; //to tail
        int64_t pos_old = request_len - 1;
        /**
         * origin buf:
         *                               payload_len                  B    part_id
         *   |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _|_ _|_ _ _ _ _ _ _ _|_ _|_ _ _ _ |_ _|_ _ _ _|
         *                                             A      table_id             C
         *
         * new buf:
         *                               payload_len                        B    part_id
         *   |_ _ _ _ _ _ _ _ _ _ _ _ _ _ _|_ _ _|_ _ _ _ _ _ _ _|_ _ _|_ _ _ _ |_ _ _|_ _ _ _|
         *                                             A         table_id                 C
         */
        int64_t middle_a_len = get_table_id_position() - get_payload_len_position() - get_payload_len_len();
        int64_t middle_b_len = get_part_id_position() - get_table_id_position() - get_table_id_len();
        int64_t middle_c_len = request_len - get_part_id_position() - get_part_id_len();
        if (part_id_diff_len > 0) {
          shrink_copy_char_buf(buf + pos_new, buf + pos_old, middle_a_len);
          pos_old = partition_id_position_ - 1;
          partition_id_len_ += part_id_diff_len;
          partition_id_position_ = new_request_payload_len - middle_a_len - partition_id_len_;
          pos_new = partition_id_position_ - 1;
        }
        if (table_id_diff_len > 0) {
          shrink_copy_char_buf(buf + pos_new, buf + pos_old, middle_b_len);
          pos_old = table_id_position_ - 1;
          table_id_len_ += table_id_diff_len;
          table_id_position_ = partition_id_position_ - middle_b_len - table_id_len_;
          pos_new = table_id_position_ - 1;
        }
        if (payload_len_diff_len > 0) {
          shrink_copy_char_buf(buf + pos_new, buf + pos_old, middle_c_len);
          payload_len_len_ += payload_len_diff_len;
          //payload_len_position_ not changed
        }
        request_buf_has_changed_ = true; //could be read buffer directly use ObRpcRequest object(just used buffer)
        request_len += added_len;
      }
      //1.write new payload len
      int64_t pos = payload_len_position_;
      if (OB_FAIL(encode_with_len_vi64(buf, buf_len, pos, new_request_payload_len, payload_len_len_))) {
        LOG_WDIAG("adjust_req_buf_if_need rewrite new payload len failed", K(ret), K(pos), K(new_request_payload_len), K(payload_len_len_));
      } else if (FALSE_IT(pos = table_id_position_)) {
      //2.write new table id
      } else if (OB_FAIL(encode_with_len_vi64(buf, buf_len, pos, table_id, table_id_len_))) {
        LOG_WDIAG("adjust_req_buf_if_need rewrite new table id failed", K(ret), K(pos), K(table_id), K(table_id_len_));
      } else if (FALSE_IT(pos = partition_id_position_)) {
      //3.write new partition id
      } else if (OB_FAIL(encode_with_len_vi64(buf, buf_len, pos, partition_id, partition_id_len_))) {
        LOG_WDIAG("adjust_req_buf_if_need rewrite new table id failed", K(ret), K(pos), K(partition_id), K(partition_id_len_));
      } else if (FALSE_IT(pos = 4)) {
      //4.write all length of rpc request
      } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, (int32_t)(request_len)))) {
        LOG_WDIAG("adjust_req_buf_if_need rewrite request len failed", K(ret), K(pos), K(request_len));
      }
    } else{
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to adjust payload len", K(ret), KP(buf), K(buf_len));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to adjust payload len", K(ret), KP(buf), K(buf_len));
  }
  return OB_SUCCESS;
}

int ObRpcRequest::init_rowkey_info(int64_t sub_req_count)
{
  int ret = OB_SUCCESS;
  if (request_info_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc request already inited", K(ret));
  } else if (sub_req_count == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("empty request to init", K(sub_req_count), K(ret));
  } else {
    ObRpcPacketCode pcode = rpc_packet_meta_.get_pcode();
    request_info_inited_ = true;
    sub_request_count_ = sub_req_count;
    ObArenaAllocator *allocator = &allocator_;
    if (pcode != obrpc::OB_TABLE_API_EXECUTE
        && OB_ISNULL(sub_request_buf_arr_ = OB_NEWx(SUB_REQUEST_BUF_ARR, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to init sub request buf arr", K(ret));
    } else if (pcode != obrpc::OB_TABLE_API_EXECUTE
               && OB_ISNULL(sub_request_rowkey_range_arr_ = OB_NEWx(SUB_REQUEST_RANGE_ARR, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to init sub request range arr", K(ret));
    } else if (OB_ISNULL(sub_request_columns_arr_ = OB_NEWx(SUB_REQUEST_ROWKEY_COLUMNS_ARR, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to init sub request columns arr", K(ret));
    } else if (OB_ISNULL(sub_request_rowkey_val_arr_ = OB_NEWx(SUB_REQUEST_ROWKEY_VAL_ARR, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to init sub request rowkey arr", K(ret));
    }
  }
  return ret;
}

int ObRpcRequest::add_sub_req_buf(const ObRpcFieldBuf &proxy_buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_request_buf_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected sub request buf", K(ret));
  } else if (OB_FAIL(sub_request_buf_arr_->push_back(proxy_buf))) {
    LOG_WDIAG("fail to push back proxy buf", K(ret));
  }
  return ret;
}

int ObRpcRequest::add_sub_req_rowkey_val(const ROWKEY_VALUE &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_request_rowkey_val_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected sub request rowkey arr", K(ret));
  } else if (OB_FAIL(sub_request_rowkey_val_arr_->push_back(rowkey))) {
    LOG_WDIAG("fail to push back rowkey val arr", K(ret));
  }
  return ret;
}

int ObRpcRequest::add_sub_req_range(const RANGE_VALUE &range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_request_rowkey_range_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected sub request range arr", K(ret));
  } else if (OB_FAIL(sub_request_rowkey_range_arr_->push_back(range))) {
    LOG_WDIAG("fail to push back rowkey range arr", K(ret));
  }
  return ret;
}

int ObRpcRequest::add_sub_req_columns(const ROWKEY_COLUMN &columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_request_columns_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected sub request columns arr", K(ret));
  } else if (OB_FAIL(sub_request_columns_arr_->push_back(columns))) {
    LOG_WDIAG("fail to push back columns arr", K(ret));
  }
  return ret;
}

int ObRpcRequest::calc_partition_id_by_sub_rowkey(ObArenaAllocator &allocator,
                                                  ObProxyPartInfo &part_info,
                                                  const int64_t sub_req_index,
                                                  int64_t &partition_id,
                                                  int64_t &ls_id)
{
  int ret = OB_SUCCESS;
  opsql::ObExprResolverResult resolve_result;
  if (OB_ISNULL(get_sub_req_rowkey_val_arr())
      || sub_req_index >= get_sub_req_rowkey_val_arr()->count()
      || OB_ISNULL(get_sub_req_columns_arr())
      || (get_sub_req_columns_arr()->count() > 0 && sub_req_index >= get_sub_req_columns_arr()->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected rowkey state", K(sub_req_index), "rowkey_arr", get_sub_req_rowkey_val_arr(), "columns_arr",
              get_sub_req_columns_arr(), K(ret));
  } else {
    ROWKEY_VALUE &rowkey_value = get_sub_req_rowkey_val_arr()->at(sub_req_index);
    ObRowkey rowkey;
    if (rowkey_value.count() > 0) {
      rowkey.assign(&rowkey_value.at(0), rowkey_value.count());
    }
    ROWKEY_COLUMN column_names;
    // if ObTableOperation, rowkey columns is empty
    if (get_sub_req_columns_arr()->count() > 0) {
      column_names.assign(get_sub_req_columns_arr()->at(sub_req_index));
    }
    ObSEArray<int64_t, 1> partition_ids;
    ObSEArray<int64_t, 1> ls_ids;
    ObSEArray<int64_t, 1> rowkey_index; // empty array
    if (part_info.has_first_part()) {
      ObRowkey &eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].start_key_;
      if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_index(part_info.get_part_key_info(), column_names,
                                                       part_info.get_part_columns(), PART_KEY_LEVEL_ONE,
                                                       rowkey_index))) {
        LOG_WDIAG("fail to call eval rowkey index for first part", K(part_info), K(ret));
      } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_values(allocator, rowkey, rowkey_index, eval_rowkey))) {
        LOG_WDIAG("fail to call eval rowkey for first part", K(rowkey), K(ret));
      } else {
        // for range part, end key must to be set
        resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].end_key_ = eval_rowkey;
        resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].border_flag_.set_inclusive_start();
        resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].border_flag_.set_inclusive_end();
      }
    }
    if (OB_SUCC(ret) && part_info.has_sub_part()) {
      ObRowkey &eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].start_key_;
      if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_index(part_info.get_part_key_info(), column_names,
                                                       part_info.get_sub_part_columns(), PART_KEY_LEVEL_TWO,
                                                       rowkey_index))) {
        LOG_WDIAG("fail to call eval rowkey index for first part", K(part_info), K(ret));
      } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_values(allocator, rowkey, rowkey_index, eval_rowkey))) {
        LOG_WDIAG("fail to call eval rowkey for first part", K(rowkey), K(ret));
      } else {
        // for range part, end key must to be set
        resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].end_key_ = eval_rowkey;
        resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].border_flag_.set_inclusive_start();
        resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].border_flag_.set_inclusive_end();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRpcExprCalcTool::do_partition_id_calc_for_obkv(resolve_result, part_info, allocator, partition_ids,
                                                                   ls_ids))) {
        LOG_WDIAG("fail to calc partition id for table", K(ret));
      } else if (partition_ids.count() != 1 || ls_ids.count() != 1) {
        // client_info.
        // TODO RPC need update it is a shard request
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("table/single operation get part ids/ log stream ids is not one", K(partition_ids), K(ls_ids),
                  K(ret));
      } else {
        partition_id = partition_ids.at(0);
        ls_id = ls_ids.at(0);
      }
    } else {
      LOG_WDIAG("fail to calc partition id for table", K(ret));
    }
  }

  return ret;
}

int ObRpcRequest::calc_partition_id_by_sub_range(common::ObArenaAllocator &allocator,
                                                 proxy::ObProxyPartInfo &part_info,
                                                 const int64_t sub_req_index,
                                                 ObIArray<int64_t> &partition_ids,
                                                 ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObExprResolverResult resolve_result;
  UNUSED(ls_ids);
  if (OB_ISNULL(get_sub_req_range_arr())
      || sub_req_index >= get_sub_req_range_arr()->count()
      || OB_ISNULL(get_sub_req_columns_arr())
      || sub_req_index >= get_sub_req_range_arr()->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected req range", KP(get_sub_req_range_arr()), K(ret));
  } else {
    RANGE_VALUE &scan_ranges = get_sub_req_range_arr()->at(sub_req_index);
    ROWKEY_COLUMN &rowkey_columns = get_sub_req_columns_arr()->at(sub_req_index);
    for (int64_t i = 0; i < scan_ranges.count(); ++i) {
      const ObNewRange &range = scan_ranges.at(i);
      const ObRowkey &start_rowkey = range.start_key_;
      const ObRowkey &end_rowkey = range.end_key_;
      int64_t rowkey_columns_count = rowkey_columns.count();
      // query operation not need log stream id temporary
      LOG_DEBUG("ObProxyExprCalculator::calc_partition_id_for_table_query range ", K(range), K(rowkey_columns));

      if (range.is_whole_range()) {
        LOG_DEBUG("scan_ranges is whole range, generate resolve_result and send to all parts");

        if (part_info.has_first_part()) {
          resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].start_key_.set_min_row();
          resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].end_key_.set_max_row();
        }
        if (part_info.has_sub_part()) {
          resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].start_key_.set_min_row();
          resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].end_key_.set_max_row();
        }

        if (OB_FAIL(ObRpcExprCalcTool::do_partition_id_calc_for_obkv(resolve_result, part_info, allocator,
                                                                     partition_ids, ls_ids))) {
          LOG_WDIAG("fail to calc partition id for table query", K(ret));
        } else {
          // success
          LOG_DEBUG("ObProxyExprCalculator::calc_partition_id_for_table_query partition id ", K(partition_ids));
        }
      } else {
        if (part_info.has_first_part()) {
          ObRowkey &start_eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].start_key_;
          ObRowkey &end_eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].end_key_;
          const common::ObIArray<common::ObString> &part_columns = part_info.get_part_columns();
          ObSEArray<int64_t, 1> rowkey_index;

          if (0 != rowkey_columns_count && start_rowkey.get_obj_cnt() != rowkey_columns_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("rowkey_columns size is not equal to rowkey size", K(rowkey_columns_count), "rowkey size",
                      start_rowkey.get_obj_cnt(), K(ret));
          } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_index(part_info.get_part_key_info(), rowkey_columns,
                                                                  part_columns, PART_KEY_LEVEL_ONE, rowkey_index))) {
            LOG_WDIAG("fail to call eval_rowkey_index", K(rowkey_columns), K(part_columns), K(rowkey_index), K(ret));
          } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_values(allocator, start_rowkey, rowkey_index,
                                                                   start_eval_rowkey))) {
            LOG_WDIAG("fail to call eval start rowkey for first part", K(start_rowkey), K(ret));
          } else if (OB_FAIL(
                         ObRpcExprCalcTool::eval_rowkey_values(allocator, end_rowkey, rowkey_index, end_eval_rowkey))) {
            LOG_WDIAG("fail to call eval start rowkey for first part", K(start_rowkey), K(ret));
          } else {
            resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].border_flag_ = range.border_flag_;
          }
        }
        if (OB_SUCC(ret) && part_info.has_sub_part()) {
          ObRowkey &start_eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].start_key_;
          ObRowkey &end_eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].end_key_;
          const common::ObIArray<common::ObString> &part_columns = part_info.get_sub_part_columns();
          ObSEArray<int64_t, 1> rowkey_index;

          if (0 != rowkey_columns_count && start_rowkey.get_obj_cnt() != rowkey_columns_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("rowkey_columns size is not equal to rowkey size", K(rowkey_columns_count), "rowkey size",
                      start_rowkey.get_obj_cnt(), K(ret));
          } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_index(part_info.get_part_key_info(), rowkey_columns,
                                                                  part_columns, PART_KEY_LEVEL_TWO, rowkey_index))) {
            LOG_WDIAG("fail to call eval_rowkey_index", K(rowkey_columns), K(part_columns), K(rowkey_index), K(ret));
          } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_values(allocator, start_rowkey, rowkey_index,
                                                                   start_eval_rowkey))) {
            LOG_WDIAG("fail to call eval start rowkey for first part", K(start_rowkey), K(ret));
          } else if (OB_FAIL(
                         ObRpcExprCalcTool::eval_rowkey_values(allocator, end_rowkey, rowkey_index, end_eval_rowkey))) {
            LOG_WDIAG("fail to call eval start rowkey for first part", K(start_rowkey), K(ret));
          } else {
            resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].border_flag_ = range.border_flag_;
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObRpcExprCalcTool::do_partition_id_calc_for_obkv(resolve_result, part_info, allocator,
                                                                       partition_ids, ls_ids))) {
            LOG_WDIAG("fail to calc partition id for table query", K(ret));
          } else {
            // success
            LOG_DEBUG("ObProxyExprCalculator::calc_partition_id_for_table_query partition id ", K(partition_ids));
          }
        } else {
          LOG_WDIAG("fail to calc parttion id for table table query", K(ret));
        }
      }
    }
  }

  if (partition_ids.count() > 1) {
    // TODO need check partition_ids contains -1 ?
    common::hash::ObHashSet<int64_t> partition_ids_set;
    partition_ids_set.create(partition_ids.count());
    for (int64_t i = 0; i < partition_ids.count(); i++) {
      partition_ids_set.set_refactored(partition_ids.at(i));
    }
    if (partition_ids_set.size() < partition_ids.count()) {
      LOG_DEBUG("partition_ids before delete repeate", K(partition_ids));
      partition_ids.reuse();
      common::hash::ObHashSet<int64_t>::iterator it = partition_ids_set.begin();
      common::hash::ObHashSet<int64_t>::iterator end = partition_ids_set.end();
      for (; it != end; it++) {
        partition_ids.push_back(it->first);
      }
      partition_ids_set.destroy();
      LOG_DEBUG("partition_ids after delete repeate", K(partition_ids));
    }
    // need remove duplicate
  }
  return ret;
}

int ObRpcRequest::get_sub_req_buf_arr(common::ObIArray<ObRpcFieldBuf> &sub_reqs,
                                      const ObIArray<int64_t> &indexes) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_request_buf_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rpc request not inited", K(ret));
  } else if (OB_FAIL(sub_reqs.reserve(indexes.count()))) {
    LOG_WDIAG("fail to reserve mem for sub request", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < indexes.count(); i++) {
    int index = indexes.at(i);
    if (index >= sub_request_buf_arr_->count() || index < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid index for sub req buf", K(ret), K(index), "sub_req_buf_count", sub_request_buf_arr_->count());
    } else if (OB_FAIL(sub_reqs.push_back(sub_request_buf_arr_->at(index)))) {
      LOG_WDIAG("fail to push back single ops", K(ret));
    }
  }
  return ret;
}

///////////////////////////////////////////////////////////////
const ObRpcResponse &ObRpcResponse::operator =(const ObRpcResponse &other)
{
  if (this != &other) {
    rpc_packet_meta_ = other.rpc_packet_meta_;
    rpc_result_code_ = other.rpc_result_code_;
  }
  return *this;
}

void ObRpcResponse::reset()
{
  rpc_result_code_.reset();
  rpc_packet_meta_.reset();
}

int ObRpcResponse::analyze_response(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_NOT_IMPLEMENT;

  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);

  return ret;
}

// 返回error信息时，调用这个函数，只需要序列化result code即可
int ObRpcResponse::encode(char *buf, int64_t &buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;      // 将pos设置为meta之后
  check_sum_pos = pos;   // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcResponse", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化result_code
    OB_UNIS_ENCODE(rpc_result_code_)

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

int ObRpcResponse::deep_copy(common::ObIAllocator &allocator,  const ObRpcResponse *other)
{
 int ret = OB_NOT_IMPLEMENT;

 UNUSED(allocator);
 UNUSED(other);

 return ret;
}

int64_t ObRpcResponse::get_encode_size() const
{
  int64_t len = 0;
  len += rpc_packet_meta_.get_serialize_size();
  len += rpc_result_code_.get_serialize_size();
  return len;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
