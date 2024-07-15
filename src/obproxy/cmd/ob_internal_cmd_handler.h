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

#ifndef OBPROXY_INTERNAL_CMD_HANDLER_H
#define OBPROXY_INTERNAL_CMD_HANDLER_H

#include "cmd/ob_internal_cmd_processor.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "common/obsm_row.h"
#include "packet/ob_mysql_packet_util.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "obproxy/packet/ob_proxy_packet_writer.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlClientSession;
class Ob20HeaderParam;
}
class ObInternalCmdHandler;

static const int64_t MYSQL_LIST_RETRY = HRTIME_MSECONDS(1);
typedef int (ObInternalCmdHandler::*InternalCmdCSHandler) (proxy::ObMysqlClientSession &cs);

#define SET_CS_HANDLER(handler) \
  (cs_handler_ = (reinterpret_cast<oceanbase::obproxy::InternalCmdCSHandler>(handler)))

struct ObProxyColumnSchema
{
  ObProxyColumnSchema() : cid_(-1), cname_(), ctype_(obmysql::OB_MYSQL_TYPE_NOT_DEFINED) {}
  ObProxyColumnSchema(const int64_t cid, const char *cname, const obmysql::EMySQLFieldType ctype)
                      : cid_(cid), cname_(cname), ctype_(ctype) {}
  ObProxyColumnSchema(const int64_t cid, const int64_t length, const char *cname, const obmysql::EMySQLFieldType ctype)
                      : cid_(cid), cname_(length, cname), ctype_(ctype) {}
  ~ObProxyColumnSchema() {}

  static ObProxyColumnSchema make_schema(const int64_t cid, const char *cname,
                                         const obmysql::EMySQLFieldType ctype)
  {
    return (OB_ISNULL(cname) ? ObProxyColumnSchema() : ObProxyColumnSchema(cid, cname, ctype));
  }

  static ObProxyColumnSchema make_schema(const int64_t cid, common::ObString &cname,
                                         const obmysql::EMySQLFieldType ctype)
  {
    return (cname.empty() ? ObProxyColumnSchema() : ObProxyColumnSchema(cid, cname.length(), cname.ptr(), ctype));
  }

  TO_STRING_KV(K_(cid), K_(cname), K_(ctype));

  int64_t cid_;
  common::ObString cname_;
  obmysql::EMySQLFieldType ctype_;
};

struct ObCSIDHandler
{
  ObCSIDHandler() : is_used_(false), cs_id_(0) { }
  ~ObCSIDHandler() {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(is_used),
         K_(cs_id));
    J_OBJ_END();
    return pos;
  }

  bool is_used_;
  uint32_t cs_id_;
};

typedef common::ObSEArray<ObCSIDHandler, 1> CSIDHanders;


class ObInternalCmdHandler : public event::ObContinuation
{
public:
  ObInternalCmdHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObInternalCmdHandler();

  static bool is_constructor_argument_valid(const event::ObContinuation *cont,
                                            const event::ObMIOBuffer *buf)
  {
    return (OB_LIKELY(NULL != cont)
            && OB_LIKELY(NULL != cont->mutex_)
            && OB_LIKELY(NULL != cont->mutex_->thread_holding_)
            && OB_LIKELY(NULL != buf));
  }

  int init(const bool is_query_cmd = true);
  int reset();//clean buf and reset seq
  void destroy_internal_buf();
  int fill_external_buf();
  event::ObAction &get_action() { return action_; }
  bool is_inited() const { return is_inited_; };
  bool is_buf_empty() const { return original_seq_ == seq_; }
  bool is_argument_valid(const int event, const void *data) const
  {
    return (OB_LIKELY(is_inited_)
            && OB_LIKELY(NULL != data)
            && (OB_LIKELY(EVENT_IMMEDIATE == event)
                || OB_LIKELY(EVENT_INTERVAL == event)
                || OB_LIKELY(EVENT_NONE == event)));
  }

  int handle_cs_with_proxy_conn_id(int event, void *data);
  int handle_cs_with_server_conn_id(int event, void *data);

  virtual CSIDHanders *get_cs_id_array() { return NULL; }

protected:
  int encode_header(const common::ObString *cname, const obmysql::EMySQLFieldType *ctype,
      const int64_t size);
  int encode_header(const ObProxyColumnSchema *column_schema, const int64_t size);
  int encode_row_packet(const common::ObNewRow &row, const bool need_limit_size = true);
  int encode_ok_packet(const int64_t affected_rows, const obmysql::ObMySQLCapabilityFlags &capability);
  int encode_eof_packet();
  virtual int encode_err_packet(const int errcode);

  template<typename T>
  int encode_err_packet(const int errcode, const T &param)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT){
      ret = OB_NOT_INIT;
      WDIAG_ICMD("it has not inited", K(ret));
    } else if (OB_FAIL(reset())) { // before encode err packet, we need clean buf
      WDIAG_ICMD("fail to do reset", K(errcode), K(ret));
    } else {
      char *err_msg = NULL;
      if (OB_FAIL(packet::ObProxyPacketWriter::get_user_err_buf(errcode, err_msg, param))) {
        WDIAG_ICMD("fail to get user err buf", K(errcode), K(ret));
      } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*internal_buf_, seq_, errcode, err_msg))) {
        WDIAG_ICMD("fail to encode err packet buf", K(errcode), K(ret));
      } else {
        INFO_ICMD("succ to encode err packet", K(errcode));
      }
    }
    return ret;
  }

  int handle_callback(int event, void *data);
  //if encode error or others happened, we create an internal error packet and send to client
  //if failed, handle_callback(INTERNAL_CMD_EVENTS_FAILED)
  int internal_error_callback(int errcode);

  int do_privilege_check(const ObProxySessionPrivInfo &session_priv,
                         const share::schema::ObNeedPriv &need_priv,
                         int &errcode);

  int do_cs_handler_with_proxy_conn_id(const event::ObEThread &ethread);
  int do_cs_handler_with_server_conn_id(const event::ObEThread &ethread, bool &is_finished);

public:
  InternalCmdCSHandler cs_handler_;
  int64_t cs_id_;

protected:
  event::ObAction action_;
  event::ObEThread *submit_thread_;
  event::ObMIOBuffer *external_buf_;
  event::ObMIOBuffer *internal_buf_;
  event::ObIOBufferReader *internal_reader_;
  int64_t internal_buf_limited_;
  
  proxy::ObProxyProtocol protocol_;
  proxy::Ob20HeaderParam ob20_param_;

  bool is_inited_;
  bool header_encoded_;
  uint8_t seq_;
  const uint8_t original_seq_;
  int saved_event_;
  ObString like_name_;
  char like_name_buf_[PROXY_LIKE_NAME_MAX_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObInternalCmdHandler);
};

} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_INTERNAL_CMD_HANDLER_H
