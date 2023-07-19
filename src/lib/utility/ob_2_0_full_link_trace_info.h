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

#ifndef _OBPROXY_OB_2_0_FULL_LINK_TRACE_INFO_H_
#define _OBPROXY_OB_2_0_FULL_LINK_TRACE_INFO_H_

#include "lib/charset/ob_mysql_global.h"
#include "common/ob_object.h"
#include "lib/utility/ob_2_0_full_link_trace_util.h"
#include "lib/trace/ob_trace.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{

namespace common
{

using namespace oceanbase::trace;

#define FLT_APP_INFO_BUF_MAX (260)
#define FLT_EXTRA_INFO_DEF(extra_id, id, type) extra_id = id,
typedef common::ObSEArray<common::ObString, 3> FltShowTraceJsonArrayType;

/*
 * oceanbase protocol 2.0, extra info id
 */
enum FullLinkTraceExtraInfoId
{
  // here to add driver's id
  FLT_EXTRA_INFO_DEF(FLT_DRV_SPAN, 1, obmysql::OB_MYSQL_TYPE_VAR_STRING)
  FLT_EXTRA_INFO_DEF(FLT_DRIVER_END, 1000, obmysql::OB_MYSQL_TYPE_NOT_DEFINED)

  // here to add proxy's id
  FLT_EXTRA_INFO_DEF(FLT_PROXY_END, 2000, obmysql::OB_MYSQL_TYPE_NOT_DEFINED)

  // APP_INFO
  FLT_EXTRA_INFO_DEF(FLT_CLIENT_IDENTIFIER, 2001, obmysql::OB_MYSQL_TYPE_VAR_STRING)
  FLT_EXTRA_INFO_DEF(FLT_MODULE, 2002, obmysql::OB_MYSQL_TYPE_VAR_STRING)
  FLT_EXTRA_INFO_DEF(FLT_ACTION, 2003, obmysql::OB_MYSQL_TYPE_VAR_STRING)
  FLT_EXTRA_INFO_DEF(FLT_CLIENT_INFO, 2004, obmysql::OB_MYSQL_TYPE_VAR_STRING)

  // QUERY INFO
  FLT_EXTRA_INFO_DEF(FLT_QUERY_START_TIMESTAMP, 2010, obmysql::OB_MYSQL_TYPE_LONGLONG)
  FLT_EXTRA_INFO_DEF(FLT_QUERY_END_TIMESTAMP, 2011, obmysql::OB_MYSQL_TYPE_LONGLONG)

  // CONTROL INFO
  FLT_EXTRA_INFO_DEF(FLT_LEVEL, 2020, obmysql::OB_MYSQL_TYPE_TINY)
  FLT_EXTRA_INFO_DEF(FLT_SAMPLE_PERCENTAGE, 2021, obmysql::OB_MYSQL_TYPE_DOUBLE)
  FLT_EXTRA_INFO_DEF(FLT_RECORD_POLICY, 2022, obmysql::OB_MYSQL_TYPE_TINY)
  FLT_EXTRA_INFO_DEF(FLT_PRINT_SAMPLE_PCT, 2023, obmysql::OB_MYSQL_TYPE_DOUBLE)
  FLT_EXTRA_INFO_DEF(FLT_SLOW_QUERY_THRES, 2024, obmysql::OB_MYSQL_TYPE_LONGLONG)
  FLT_EXTRA_INFO_DEF(FLT_SHOW_TRACE_ENABLE, 2025, obmysql::OB_MYSQL_TYPE_TINY)     // show trace added
  
  // tdo print_sample_percentage && slow_query_threshold

  // SPAN INFO
  FLT_EXTRA_INFO_DEF(FLT_TRACE_ENABLE, 2030, obmysql::OB_MYSQL_TYPE_TINY)
  FLT_EXTRA_INFO_DEF(FLT_FORCE_PRINT, 2031, obmysql::OB_MYSQL_TYPE_TINY)
  FLT_EXTRA_INFO_DEF(FLT_TRACE_ID, 2032, obmysql::OB_MYSQL_TYPE_VAR_STRING)  // uuid type
  FLT_EXTRA_INFO_DEF(FLT_REF_TYPE, 2033, obmysql::OB_MYSQL_TYPE_TINY)
  FLT_EXTRA_INFO_DEF(FLT_SPAN_ID, 2034, obmysql::OB_MYSQL_TYPE_VAR_STRING)   // uuid type

  // show trace
  FLT_EXTRA_INFO_DEF(FLT_DRV_SHOW_TRACE_SPAN, 2050, obmysql::OB_MYSQL_TYPE_VAR_STRING)
  FLT_EXTRA_INFO_DEF(FLT_PROXY_SHOW_TRACE_SPAN, 2051, obmysql::OB_MYSQL_TYPE_VAR_STRING)

  // END
  FLT_EXTRA_INFO_DEF(FLT_EXTRA_INFO_END, 2040, obmysql::OB_MYSQL_TYPE_NOT_DEFINED)
};

enum FullLinkTraceExtraInfoType
{
  FLT_EXTRA_TYPE_BEGIN = 0,

  FLT_DRIVER_SPAN_INFO = 1,
  FLT_EXTRA_INFO_DRIVER_END = 1000,
  
  FLT_APP_INFO = 2001,
  FLT_QUERY_INFO = 2002,
  FLT_CONTROL_INFO = 2003,
  FLT_SPAN_INFO = 2004,
  FLT_TYPE_SHOW_TRACE_SPAN = 2005,   // show trace
  
  FLT_EXTRA_TYPE_END
};

enum FullLinkTraceRecordPolicy {
  RP_ALL = 1,
  RP_ONLY_SLOW_QUERY = 2,
  RP_SAMPLE_AND_SLOW_QUERY = 3,
  MAX_RECORD_POLICY = 4
};

/*
 * full link trace context
 */
struct FLTCtx {
public:
  FLTCtx() : flt_ext_enable_(false), is_client_support_show_trace_(false), span_start_ts_(0), span_end_ts_(0) {}
  FLTCtx(bool ext_enable, bool client_support_show_trace, int64_t span_start_ts = 0, int64_t span_end_ts = 0)
    : flt_ext_enable_(ext_enable), is_client_support_show_trace_(client_support_show_trace),
      span_start_ts_(span_start_ts), span_end_ts_(span_end_ts) {}
  ~FLTCtx() {}
  
  void reset() {
    flt_ext_enable_ = false;
    is_client_support_show_trace_ = false;
    span_start_ts_ = 0;
    span_end_ts_ = 0;
  }
  
  TO_STRING_KV(K_(flt_ext_enable), K_(is_client_support_show_trace), K_(span_start_ts), K_(span_end_ts));

public:
  bool flt_ext_enable_;                   // full link trace extend capability
  bool is_client_support_show_trace_;     // client support show trace or not
  
  int64_t span_start_ts_;
  int64_t span_end_ts_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(FLTCtx);
};

/*
 * according to the format of FLT, see ob_2_0_full_link_trace_util.h
 * each deserialize function could be invoke more than one in one loop to analyze multi-objs in the same type
 * attention to the manage of pos, pos init to 0 is recommended. otherwise the logic will be error.
 */
class FLTExtraInfo {
public:
  FLTExtraInfo() : type_(FLT_EXTRA_TYPE_END) {}
  virtual ~FLTExtraInfo() {}

  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos);
  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx) = 0;
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                const char *buf, const int64_t len, int64_t &pos) = 0;
  virtual int64_t get_serialize_size(FLTCtx &ctx) = 0;

  bool is_app_info() { return type_ == FLT_APP_INFO; }
  bool is_query_info() { return type_ == FLT_QUERY_INFO; }
  bool is_control_info() { return type_ == FLT_CONTROL_INFO; }
  bool is_span_info() { return type_ == FLT_SPAN_INFO; }

public:
  FullLinkTraceExtraInfoType type_;
};


class FLTControlInfo : public FLTExtraInfo
{
public:
  FLTControlInfo()
    : level_(0),
      sample_percentage_(-1),
      record_policy_(MAX_RECORD_POLICY),
      print_sample_percentage_(-1),
      slow_query_threshold_(-1),
      show_trace_enable_(false),
      is_need_send_(false)
      
  { type_ = FLT_CONTROL_INFO; }
  ~FLTControlInfo () {}

  bool is_valid() const {
    return type_ == FLT_CONTROL_INFO
           && is_level_valid()
           && sample_percentage_ >= 0
           && sample_percentage_ <= 1
           && record_policy_ >= RP_ALL
           && record_policy_ < MAX_RECORD_POLICY
           && print_sample_percentage_ >= 0
           && print_sample_percentage_ <= 1
           && slow_query_threshold_ > 0;
  }

  inline bool is_level_valid() const { return level_ > 0 && level_ <= 3; }

  void reset() {
    level_ = 0;
    sample_percentage_ = -1;
    record_policy_ = MAX_RECORD_POLICY;
    print_sample_percentage_ = -1;
    slow_query_threshold_ = -1;
    show_trace_enable_ = false;
    is_need_send_ = false;
  }

  OB_INLINE void set_need_send(bool sent) { is_need_send_ = sent; }
  OB_INLINE bool is_need_send() const { return is_need_send_; }
  OB_INLINE bool is_show_trace_enable() const { return show_trace_enable_; }

  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(FLTCtx &ctx);

  TO_STRING_KV(K_(level), K_(sample_percentage), K_(record_policy),
               K_(print_sample_percentage), K_(slow_query_threshold), K_(show_trace_enable),
               K_(is_need_send), K_(type));
                        
public:
  int8_t level_;                                // span level
  double sample_percentage_;                    // control trace enable percentage
  FullLinkTraceRecordPolicy record_policy_;     // record policy
  double print_sample_percentage_;              // control force print percentage
  int64_t slow_query_threshold_;                // slow query threshold (us)
  bool show_trace_enable_;                      // show trace enabled or not, session switch

  /*
   * whether the control info needed sent to client,do not seri/deseri it
   * observer could send invalid span info to proxy/client
   * while it has been changed, proxy should send it back to client
   */
  bool is_need_send_;
};

enum FLTSpanRefType {
  SYNC,
  ASYNC,
  MAX_REF_TYPE
};

class FLTSpanInfo : public FLTExtraInfo
{
public:
  FLTSpanInfo()
    : trace_enable_(false),
      force_print_(false),
      ref_type_(MAX_REF_TYPE),
      trace_id_(),
      span_id_()
  {
    type_ = FLT_SPAN_INFO;
  }

  ~FLTSpanInfo() {}

  void reset();

  bool is_valid() const {
    return trace_id_.is_inited()
           && span_id_.is_inited()
           && type_ == FLT_SPAN_INFO
           && ref_type_ != MAX_REF_TYPE;
  }
  
  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  int serialize_as_json_format(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(FLTCtx &ctx);
  int64_t get_show_trace_serialize_size();

  TO_STRING_KV(K_(trace_enable), K_(force_print), K_(ref_type), K_(trace_id), K_(span_id), K_(type));

public:
  bool trace_enable_;
  bool force_print_;
  FLTSpanRefType ref_type_;
  UUID trace_id_;
  UUID span_id_;
};

// resolve from server response ok packet, use TRACE interface to print
class FLTQueryInfo : public FLTExtraInfo
{
public:
  FLTQueryInfo() : query_start_ts_(0), query_end_ts_(0) { type_ = FLT_QUERY_INFO; }
  ~FLTQueryInfo() {}

  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(FLTCtx &ctx);
  
  void reset() {
    query_start_ts_ = 0;
    query_end_ts_ = 0;
  }

  bool is_valid() const {
    return (type_ == FLT_QUERY_INFO)
           && (query_start_ts_ != 0 || query_end_ts_ != 0);
  }
  
  TO_STRING_KV(K_(query_start_ts), K_(query_end_ts), K_(type));
                        
public:
  int64_t query_start_ts_;
  int64_t query_end_ts_;
};


class FLTAppInfo : public FLTExtraInfo
{
public:
  FLTAppInfo() : flt_app_info_buf_len_(0) { type_ = FLT_APP_INFO; }
  //FLTAppInfo(const FLTAppInfo &other) { *this = other; }
  ~FLTAppInfo() {}

  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos);
  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  virtual int deserialize_field(FullLinkTraceExtraInfoId id, const int64_t v_len,
                        const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(FLTCtx &ctx);
  
  FLTAppInfo &operator=(const FLTAppInfo &other);

  void reset() {
    MEMSET(flt_app_info_buf_, 0, sizeof(flt_app_info_buf_));
    flt_app_info_buf_len_ = 0;
    type_ = FLT_APP_INFO;
  }

  bool is_valid() const {
    return flt_app_info_buf_len_ > 0
           && type_ == FLT_APP_INFO;
  }
  
  TO_STRING_KV(K_(flt_app_info_buf), K_(flt_app_info_buf_len), K_(type));

public:
  char flt_app_info_buf_[FLT_APP_INFO_BUF_MAX];     // ref to design document, max: 64*4
  int64_t flt_app_info_buf_len_;                    // ref to ObZoneStateInfo, stored in obmysqlsm
};


// resolve from client request, use TRACE interface to print
class FLTDriverSpanInfo : public FLTExtraInfo
{
public:
  FLTDriverSpanInfo() { type_ = FLT_DRIVER_SPAN_INFO; }
  ~FLTDriverSpanInfo() {}

  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(FLTCtx &ctx);

  void reset() {
    curr_driver_span_.reset();
    type_ = FLT_DRIVER_SPAN_INFO;
  }

  bool is_valid() const {
    return (type_ == FLT_DRIVER_SPAN_INFO)
            && (!curr_driver_span_.empty());
  }
  
  TO_STRING_KV(K_(curr_driver_span), K_(type));
  
public:
  ObString curr_driver_span_;
};

// full link trace log info
class FLTTraceLogInfo {
public:
  FLTTraceLogInfo() : is_inited_(false), proxy_root_span_begin_time_(-1) {}
  ~FLTTraceLogInfo() {}

  void reset() {
    MEMSET(this, 0x0, sizeof(FLTTraceLogInfo));
    is_inited_ = false;
    proxy_root_span_begin_time_ = -1;
    proxy_root_span_ctx_ = NULL;
    server_process_req_ctx_ = NULL;
    server_response_read_ctx_ = NULL;
    cluster_resource_create_ctx_ = NULL;
    partition_location_lookup_ctx_ = NULL;
    do_observer_open_ctx_ = NULL;
    client_response_write_ctx_ = NULL;
    server_request_write_ctx_ = NULL;
  }

  DECLARE_TO_STRING;
  
public:
  bool is_inited_;                        // is init or not
  int64_t proxy_root_span_begin_time_;    // us

  // all ctx pointers here are managed by logic, the real memory is in OBTRACE buffer
  // proxy root
  ObSpanCtx *proxy_root_span_ctx_;

  // another span ids
  // it is recommend to use FLT_BEGIN_SPAN, FLT_END_CURRENT_SPAN() without span id arg
  // it is more efficient, but the sequence should be same
  ObSpanCtx *server_process_req_ctx_;
  ObSpanCtx *server_response_read_ctx_;  
  ObSpanCtx *cluster_resource_create_ctx_; 
  ObSpanCtx *partition_location_lookup_ctx_;
  ObSpanCtx *do_observer_open_ctx_;
  ObSpanCtx *client_response_write_ctx_;
  ObSpanCtx *server_request_write_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(FLTTraceLogInfo);
};

class FLTDrvShowTraceSpanByProxy {
public:
  FLTDrvShowTraceSpanByProxy() {}
  ~FLTDrvShowTraceSpanByProxy() {}
  void reset();
  void reset_curr();
  void reset_last();
  void move_curr_to_last();

  TO_STRING_KV(K(curr_drv_span_info_), K(curr_drv_span_start_ts_), K(curr_drv_span_end_ts_),
               K(last_drv_span_info_), K(last_drv_span_start_ts_), K(last_drv_span_end_ts_));

public:
  FLTSpanInfo curr_drv_span_info_;
  int64_t curr_drv_span_start_ts_;
  int64_t curr_drv_span_end_ts_;
  
  FLTSpanInfo last_drv_span_info_;
  int64_t last_drv_span_start_ts_;
  int64_t last_drv_span_end_ts_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(FLTDrvShowTraceSpanByProxy);
};

class FLTShowTraceJsonSpanInfo : public FLTExtraInfo {
public:
  FLTShowTraceJsonSpanInfo() : flt_drv_show_trace_span_(), span_is_alloc_(false), drv_show_by_proxy_(),
    last_sql_json_span_array_(), curr_sql_json_span_array_() {
    type_ = FLT_TYPE_SHOW_TRACE_SPAN;
  }
  ~FLTShowTraceJsonSpanInfo() { destroy(); }
  int deep_copy_drv_show_trace_span(FLTShowTraceJsonSpanInfo &info);

  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos);
  virtual int serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx);
  virtual int deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                  const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(FLTCtx &ctx);

  void clear();
  void reset();
  void destroy();                             // get thread allocator and free each element
  void move_curr_to_last_span_array();        // move each elem from curr to last

  void reset_flt_drv_show_trace_span();
  void reset_flt_show_trace_json_array(FltShowTraceJsonArrayType &json_array);

  TO_STRING_KV(K_(flt_drv_show_trace_span), K_(span_is_alloc), K_(drv_show_by_proxy));
  
public:
  // record driver show trace span json from driver directly
  ObString flt_drv_show_trace_span_;
  bool span_is_alloc_;

  // record driver show trace span generated by proxy
  FLTDrvShowTraceSpanByProxy drv_show_by_proxy_;

  // record proxy show trace json span for last and now
  FltShowTraceJsonArrayType last_sql_json_span_array_;
  FltShowTraceJsonArrayType curr_sql_json_span_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(FLTShowTraceJsonSpanInfo);
};

struct FLTObjManage {
public:
  FLTObjManage() {}
  ~FLTObjManage() {}

  // deserialize from full_trc string
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int get_extra_info_ref_by_type(FullLinkTraceExtraInfoType type, FLTExtraInfo *&extra);
  
  void reset();

  TO_STRING_KV(K_(saved_control_info), K_(control_info), K_(span_info), K_(query_info), K_(app_info),
               K_(driver_span_info), K_(trace_log_info));

public:
  FLTControlInfo saved_control_info_;     // server->proxy->client, save it while reveive from server
  FLTControlInfo control_info_;           // move saved to curr, use curr
  
  FLTSpanInfo span_info_;                 // trace manage, client->proxy, proxy generate span_id ->server
  FLTQueryInfo query_info_;               // server->proxy, proxy print
  FLTAppInfo app_info_;
  FLTDriverSpanInfo driver_span_info_;    // client->proxy, proxy print; need operator= or print immediately
  
  FLTTraceLogInfo trace_log_info_;        // full link trace info

  FLTShowTraceJsonSpanInfo show_trace_json_info_;   // show trace related
  
private:
  DISALLOW_COPY_AND_ASSIGN(FLTObjManage);
};

} // oommon

} // oceanbase

#endif

