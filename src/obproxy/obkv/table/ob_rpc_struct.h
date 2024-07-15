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

#ifndef OBPROXY_OBRPC_STRUCT_H
#define OBPROXY_OBRPC_STRUCT_H

#include "lib/ob_define.h"
#include "ob_proxy_rpc_serialize_utils.h"
#include "proxy/route/obproxy_part_info.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcReq;
}
namespace event
{
class ObIOBufferReader;
}
namespace obkv
{

using namespace oceanbase::obrpc;

static const int64_t SUB_REQ_COUNT = 2;
static const int64_t ROWKEY_COLUMNS_COUNT = 2;

typedef common::ObSEArray<common::ObObj, ROWKEY_COLUMNS_COUNT> ROWKEY_VALUE;
typedef common::ObSEArray<common::ObString, ROWKEY_COLUMNS_COUNT> ROWKEY_COLUMN;
typedef common::ObSEArray<common::ObNewRange, ROWKEY_COLUMNS_COUNT> RANGE_VALUE;

typedef common::ObSEArray<ObRpcFieldBuf, SUB_REQ_COUNT> SUB_REQUEST_BUF_ARR;
typedef common::ObSEArray<ROWKEY_VALUE, SUB_REQ_COUNT> SUB_REQUEST_ROWKEY_VAL_ARR;
typedef common::ObSEArray<ROWKEY_COLUMN, SUB_REQ_COUNT> SUB_REQUEST_ROWKEY_COLUMNS_ARR;
typedef common::ObSEArray<RANGE_VALUE, SUB_REQ_COUNT> SUB_REQUEST_RANGE_ARR;


class ObRpcSubReqBuf
{
public:
  ObRpcSubReqBuf() : buf_(NULL), buf_len_(0) {}
  ObRpcSubReqBuf(char *buf, int64_t buf_len) : buf_(buf), buf_len_(buf_len) {}
public:
  char* buf_;
  int64_t buf_len_;
  TO_STRING_KV(KP(buf_), K(buf_len_));
};


enum ObProxyRpcType {
  OBPROXY_RPC_OBRPC = 0,
  OBPROXY_RPC_HBASE,
  OBPROXY_RPC_UNKOWN
};

class ObRpcEzHeader
{
public:
  static const uint8_t API_VERSION = 1;
  static const uint8_t MAGIC_HEADER_FLAG[4];
  static const uint8_t EZ_HEADER_LEN = 16;
  static const int64_t RPC_PKT_CHANNEL_ID_POS = 8;
  static const int64_t RPC_PKT_CHANNEL_ID_LEN = 4;

  uint8_t  magic_header_flag_[4];
  uint32_t ez_payload_size_;
  uint32_t chid_;
  uint32_t reserved_;

public:
  ObRpcEzHeader() : magic_header_flag_{}, ez_payload_size_(0), chid_(0), reserved_(0) {}
  ObRpcEzHeader(const ObRpcEzHeader &header) {
    // magic_header_flag_ = header.magic_header_flag_;
    MEMCPY(magic_header_flag_, header.magic_header_flag_, 4 * sizeof(uint8_t));
    ez_payload_size_ = header.ez_payload_size_;
    chid_ = header.chid_;
    reserved_ = header.reserved_;
  }
  ~ObRpcEzHeader() = default;

  static ObProxyRpcType check_rpc_magic_type(const char *buffer, int64_t buffer_len);
  ObProxyRpcType get_rpc_magic_type();

  void reset() { memset(this, 0, sizeof(*this));}

  TO_STRING_KV("magic[0]", magic_header_flag_[0],
              "magic[1]", magic_header_flag_[1],
              "magic[2]", magic_header_flag_[2],
              "magic[3]", magic_header_flag_[3],
              K_(ez_payload_size), K_(chid), K_(reserved));

  NEED_SERIALIZE_AND_DESERIALIZE;
};

// magic number
const uint8_t ObRpcEzHeader::MAGIC_HEADER_FLAG[4] = { ObRpcEzHeader::API_VERSION, 0xDB, 0xDB, 0xCE };

ObProxyRpcType ObRpcEzHeader::check_rpc_magic_type(const char *buffer, int64_t buffer_len)
{
  ObProxyRpcType rpc_type = OBPROXY_RPC_UNKOWN;

  if (OB_ISNULL(buffer)) {
    // do nothing
  } else if (buffer_len >= sizeof(ObRpcEzHeader::MAGIC_HEADER_FLAG)
    && 0 == memcmp(buffer, ObRpcEzHeader::MAGIC_HEADER_FLAG, sizeof(ObRpcEzHeader::MAGIC_HEADER_FLAG))) {
    rpc_type = OBPROXY_RPC_OBRPC;
  } else {
    // do nothing
  }

  return rpc_type;
}

ObProxyRpcType ObRpcEzHeader::get_rpc_magic_type()
{
  ObProxyRpcType rpc_type = OBPROXY_RPC_UNKOWN;

  if (0 == memcmp(magic_header_flag_, ObRpcEzHeader::MAGIC_HEADER_FLAG, sizeof(ObRpcEzHeader::MAGIC_HEADER_FLAG))) {
    rpc_type = OBPROXY_RPC_OBRPC;
  } else {
    // do nothing
  }

  return rpc_type;
}

class ObRpcPacketMeta
{
public:
  ObRpcEzHeader ez_header_;
  ObRpcPacketHeader rpc_header_;

public:
  ObRpcPacketMeta() : ez_header_(), rpc_header_() {}
  ObRpcPacketMeta(const ObRpcPacketMeta &pkt_meta)
  {
    ez_header_ = pkt_meta.ez_header_;
    rpc_header_ = pkt_meta.rpc_header_;
  }

  ~ObRpcPacketMeta() {}

  ObRpcPacketCode get_pcode() const {
    return rpc_header_.pcode_;
  }

  void set_ez_header(const ObRpcEzHeader &ez_header) {
    ez_header_ = ez_header;
  }
  ObRpcEzHeader get_ez_header() const {
    return ez_header_;
  }

  void set_rpc_header(const ObRpcPacketHeader &rpc_header) {
    rpc_header_ = rpc_header;
  }
  ObRpcPacketHeader get_rpc_header() const {
    return rpc_header_;
  }

  void reset() {
    ez_header_.reset();
    memset(&rpc_header_, 0, sizeof(rpc_header_)); rpc_header_.flags_ |= (OB_LOG_LEVEL_NONE & 0x7);
  }

  TO_STRING_KV(K_(ez_header), K_(rpc_header));

  NEED_SERIALIZE_AND_DESERIALIZE;
};

class ObRpcRequest
{
public:
  ObRpcRequest()
      : request_payload_len_(0), payload_len_position_(0), payload_len_len_(0), table_id_position_(0), table_id_len_(0),
        partition_id_position_(0), partition_id_len_(0), ls_id_postition_(0), ls_id_len_(0),
        request_buf_has_changed_(0), request_info_inited_(false), sub_request_count_(0), sub_request_buf_arr_(NULL),
        sub_request_rowkey_val_arr_(NULL), sub_request_rowkey_range_arr_(NULL), sub_request_columns_arr_(NULL),
        allocator_(), pcode_(OB_INVALID_RPC_CODE), all_rowkey_names_(NULL), index_name_(), batch_size_(-1),
        rpc_packet_meta_(), cluster_version_(0)
  {
  }
  virtual ~ObRpcRequest() { reset(); }
  const ObRpcRequest &operator =(const ObRpcRequest &other);

  const ObRpcPacketMeta &get_packet_meta() const {return rpc_packet_meta_;}
  int64_t get_rpc_timeout() const { return rpc_packet_meta_.rpc_header_.timeout_ ;}

  ObRpcPacketMeta &get_packet_meta_no_const() {return rpc_packet_meta_;}
  void set_packet_meta(const ObRpcPacketMeta &meta) {rpc_packet_meta_ = meta;}
  void set_reroute_flag(bool flag) {
    if (flag) {
      rpc_packet_meta_.rpc_header_.flags_ |= (ObRpcPacketHeader::REQUIRE_REROUTING_FLAG);
    } else {
      rpc_packet_meta_.rpc_header_.flags_ &= (uint16_t)~(ObRpcPacketHeader::REQUIRE_REROUTING_FLAG);
    }
  };

  virtual void set_cluster_version(int64_t cluster_version) { cluster_version_ = cluster_version; }
  virtual int64_t get_cluster_version() const { return cluster_version_; }

  // this function must be called after setting the rpc_packet_meta
  virtual int analyze_request(const char *buf, const int64_t len, int64_t &pos) = 0;

  void reset();
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) = 0;
  virtual int64_t get_encode_size() const = 0;
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) = 0;
  virtual int64_t get_payload_len_position() const;
  virtual int64_t get_part_id_position() const;
  virtual int64_t get_table_id_position() const;
  virtual int64_t get_ls_id_position() const;

  virtual int64_t get_payload_len_len() const;
  virtual int64_t get_part_id_len() const;
  virtual int64_t get_table_id_len() const;
  virtual int64_t get_ls_id_len() const;

  virtual uint64_t get_table_id() const { return 0; }
  virtual uint64_t get_partition_id() const { return 0; }
  virtual void set_table_id(uint64_t table_id) {
    UNUSED(table_id);
  }
  virtual void set_partition_id(uint64_t part_id) {
    UNUSED(part_id);
  }

  virtual common::ObString get_credential() const { return common::ObString(); };
  virtual common::ObString get_table_name() const { return common::ObString(); };
  virtual bool is_hbase_request() const { return false; }
  virtual bool is_read_weak() const { return false; }
  virtual int adjust_req_buf_if_need(char *buf, int64_t buf_len, int64_t table_id, int64_t partition_id);

  bool is_stream_query() const { return -1 != batch_size_; }
  bool is_query_with_index() const { return !index_name_.empty(); }
  bool is_request_info_inited() const { return request_info_inited_; }
  int init_rowkey_info(int64_t sub_req_count);
  int add_sub_req_buf(const ObRpcFieldBuf &proxy_buf);
  int add_sub_req_rowkey_val(const ROWKEY_VALUE &rowkey);
  int add_sub_req_range(const RANGE_VALUE &range);
  int add_sub_req_columns(const ROWKEY_COLUMN &columns);

  int64_t get_sub_req_count() const { return sub_request_count_; }
  int get_sub_req_buf_arr(common::ObIArray<ObRpcFieldBuf> &single_ops, const ObIArray<int64_t> &indexes) const;
  const SUB_REQUEST_BUF_ARR *get_sub_req_buf_arr() const { return sub_request_buf_arr_; }
  SUB_REQUEST_BUF_ARR *get_sub_req_buf_arr() { return sub_request_buf_arr_; }
  const SUB_REQUEST_ROWKEY_VAL_ARR *get_sub_req_rowkey_val_arr() const { return sub_request_rowkey_val_arr_; }
  SUB_REQUEST_ROWKEY_VAL_ARR *get_sub_req_rowkey_val_arr() { return sub_request_rowkey_val_arr_; }
  const SUB_REQUEST_RANGE_ARR *get_sub_req_range_arr() const { return sub_request_rowkey_range_arr_; }
  SUB_REQUEST_RANGE_ARR *get_sub_req_range_arr() { return sub_request_rowkey_range_arr_; }
  const SUB_REQUEST_ROWKEY_COLUMNS_ARR *get_sub_req_columns_arr() const { return sub_request_columns_arr_; }
  SUB_REQUEST_ROWKEY_COLUMNS_ARR *get_sub_req_columns_arr() { return sub_request_columns_arr_; }
  common::ObIArray<common::ObString> *get_all_rowkey_names() { return all_rowkey_names_; }


  int calc_partition_id_by_sub_rowkey(common::ObArenaAllocator &allocator,
                                      proxy::ObProxyPartInfo &part_info,
                                      const int64_t sub_req_index,
                                      int64_t &partition_id,
                                      int64_t &ls_id);

  int calc_partition_id_by_sub_range(common::ObArenaAllocator &allocator,
                                      proxy::ObProxyPartInfo &part_info,
                                      const int64_t sub_req_index,
                                      ObIArray<int64_t> &partition_id,
                                      ObIArray<int64_t> &ls_id);

  void set_all_rowkey_names(common::ObIArray<common::ObString> *all_rowkey_names) { all_rowkey_names_ = all_rowkey_names; }
  ObString &get_index_name() { return index_name_; }
  int32_t get_batch_size() const { return batch_size_; }
  void set_index_name(ObString index_name) { index_name_ = index_name; }
  void set_batch_size(int32_t batch_size) { batch_size_ = batch_size; }
  VIRTUAL_TO_STRING_KV(K_(rpc_packet_meta),
                       KPC_(sub_request_buf_arr),
                       KPC_(sub_request_columns_arr),
                       KPC_(sub_request_rowkey_val_arr),
                       KPC_(sub_request_rowkey_range_arr),
                       K_(index_name),
                       K_(batch_size),
                       K_(request_payload_len),
                       K_(payload_len_position),
                       K_(payload_len_len),
                       K_(table_id_position),
                       K_(table_id_len),
                       K_(partition_id_position),
                       K_(partition_id_len),
                       K_(ls_id_postition),
                       K_(ls_id_len));

public:
  /* used for rewrite */
  int64_t request_payload_len_;
  int64_t payload_len_position_;
  int64_t payload_len_len_;
  int64_t table_id_position_;
  int64_t table_id_len_;
  int64_t partition_id_position_;
  int64_t partition_id_len_;
  int64_t ls_id_postition_;
  int64_t ls_id_len_;
  bool request_buf_has_changed_; /* need re analyze when to retry */
  /* ent that used for rewrite */

  // sub req info
  bool request_info_inited_;
  int64_t sub_request_count_;
  SUB_REQUEST_BUF_ARR *sub_request_buf_arr_;
  SUB_REQUEST_ROWKEY_VAL_ARR *sub_request_rowkey_val_arr_;
  SUB_REQUEST_RANGE_ARR *sub_request_rowkey_range_arr_;
  SUB_REQUEST_ROWKEY_COLUMNS_ARR *sub_request_columns_arr_;
  ObArenaAllocator allocator_;
  ObRpcPacketCode pcode_;

  ObIArray<ObString> *all_rowkey_names_;
  ObString index_name_;
  int32_t batch_size_; 

protected:
  ObRpcPacketMeta rpc_packet_meta_;
  int64_t cluster_version_;
};

class ObRpcResponse
{
public:
  ObRpcResponse() : rpc_packet_meta_(), rpc_result_code_(), cluster_version_(0) {}
  virtual ~ObRpcResponse() {reset();}
  ObRpcResponse(const ObRpcResponse &response)
    : rpc_packet_meta_(response.rpc_packet_meta_), rpc_result_code_(response.rpc_result_code_) {}
  const ObRpcResponse &operator =(const ObRpcResponse &other);

  virtual void set_cluster_version(int64_t cluster_version) { cluster_version_ = cluster_version; }
  virtual int64_t get_cluster_version() const { return cluster_version_; }

  void set_packet_meta(const ObRpcPacketMeta &meta) {rpc_packet_meta_ = meta;}
  void set_result_code(const ObRpcResultCode &res) {rpc_result_code_ = res;}

  ObRpcPacketMeta &get_packet_meta() {return rpc_packet_meta_;}
  const ObRpcPacketMeta &get_packet_meta() const {return rpc_packet_meta_;}
  const ObRpcResultCode &get_result_code() const {return rpc_result_code_;}
  ObRpcResultCode &get_result_code() {return rpc_result_code_;}
  // this function must be called after setting the rpc_packet_meta
  void reset();
  // derived classes parse different rpc packets by overriding this function
  virtual int analyze_response(const char *buf, const int64_t len, int64_t &pos);

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos);
  virtual int64_t get_encode_size() const;
  virtual int deep_copy(common::ObIAllocator &allocator,  const ObRpcResponse *other);

  TO_STRING_KV(K_(rpc_packet_meta), K_(rpc_result_code));

protected:
  ObRpcPacketMeta rpc_packet_meta_;
  ObRpcResultCode rpc_result_code_;
  int64_t cluster_version_;
};

class ObRpcSimpleResponse
{
public:
  ObRpcSimpleResponse() : rpc_packet_meta_(), rpc_result_code_() {}
  ~ObRpcSimpleResponse() {reset();}
  void reset() {
    rpc_packet_meta_.reset();
    rpc_result_code_.reset();
  }
  ObRpcSimpleResponse(const ObRpcSimpleResponse &response)
    : rpc_packet_meta_(response.rpc_packet_meta_), rpc_result_code_(response.rpc_result_code_) {}

  void set_packet_meta(const ObRpcPacketMeta &meta) {rpc_packet_meta_ = meta;}
  void set_result_code(const ObRpcResultCodeSimplified &res) {rpc_result_code_ = res;}

  ObRpcPacketMeta &get_packet_meta() {return rpc_packet_meta_;}
  const ObRpcPacketMeta &get_packet_meta() const {return rpc_packet_meta_;}
  const ObRpcResultCodeSimplified &get_result_code() const {return rpc_result_code_;}
  ObRpcResultCodeSimplified &get_result_code() {return rpc_result_code_;}

  TO_STRING_KV(K_(rpc_packet_meta), K_(rpc_result_code));

private:
  ObRpcPacketMeta rpc_packet_meta_;
  ObRpcResultCodeSimplified rpc_result_code_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_OBRPC_REQUEST_H */
