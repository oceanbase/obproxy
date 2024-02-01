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

#include "iocore/net/ob_net_vconnection.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "proxy/client/ob_raw_mysql_client.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::packet;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static const int64_t MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

ObRawMysqlClientActor::ObRawMysqlClientActor()
  : is_inited_(false), is_avail_(false), info_(NULL), resp_(NULL), con_(),
     addr_(), request_buf_(NULL), request_reader_(NULL)
{}

int ObRawMysqlClientActor::init(ObClientRequestInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(request_buf_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc request miobuffer", K(ret));
  } else if (OB_ISNULL(request_reader_ = request_buf_->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to alloc reader", K(ret));
  } else {
    info_ = &info;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObRawMysqlClientActor::destroy()
{
  if (NULL != resp_) {
    op_free(resp_);
    resp_ = NULL;
  }
  if (NULL != request_buf_) {
    free_miobuffer(request_buf_);
  }
  request_buf_ = NULL;
  request_reader_ = NULL;
  info_ = NULL;
  (void)con_.close();  // ignore ret

  addr_.reset();
  is_avail_ = false;
  is_inited_ = false;
}

int ObRawMysqlClientActor::sync_raw_execute(const char *sql, const int64_t timeout_ms,
                                            ObClientMysqlResp *&resp)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObRawMysqlClientActor will send sql to observer",
            "observer", addr_, K(sql), K(timeout_ms));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(sql) || OB_UNLIKELY(timeout_ms <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid intput value", K(sql), K(timeout_ms), K(ret));
  } else if (NULL != resp_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resp must be NULL", K_(resp), K(ret));
  } else if (OB_ISNULL(resp_ = op_alloc(ObClientMysqlResp))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate ObClientMysqlResp", K(ret));
  } else if (OB_FAIL(resp_->init())) {
    LOG_WDIAG("fail to init client mysql resp", K(ret));
  } else {
    // Get the password again, it may be changed
    if (!is_avail() &&
        OB_FAIL(connect(addr_, timeout_ms))) {
      if (!is_avail() && info_->change_password()) {
        if (OB_FAIL(connect(addr_, timeout_ms))) {
          LOG_WDIAG("fail to connect using password1", "addr", addr_, K(ret));
        }
      } else {
        LOG_WDIAG("fail to connect using password", "addr", addr_, K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } if (OB_FAIL(send_request(sql))) {
      LOG_WDIAG("fail to post request", K(sql), K(ret));
    } else {
      resp = resp_;
      resp_ = NULL; // set to NULL;
    }
  }

  if (NULL != resp_) {
    op_free(resp_);
    resp_ = NULL;
  }

  if (OB_SUCC(ret)) {
    is_avail_ = true;
  } else {
    is_avail_ = false;
  }

  return ret;
}

int ObRawMysqlClientActor::connect(const ObAddr &addr, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (!addr.is_valid() || timeout_ms <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid addr", K(addr), K(timeout_ms), K(ret));
  } else {
    ObNetVCOptions options;
    options.f_blocking_connect_ = false; // no blocking connect
    options.f_blocking_ = true; // blocking read or send

    ObIpEndpoint ip;
    ip.assign(addr.get_sockaddr());
    if (OB_FAIL(con_.open(options))) {
      LOG_WDIAG("fail to open connection", K(ret));
    } else if (OB_FAIL(con_.connect(ip.sa_, options))) {
      LOG_WDIAG("fail to connect", K(con_.fd_), K(ret));
    }

    const int POLL_EVENT_SIZE = 1;
    int64_t event_count = 0;
    struct pollfd poll_fds[POLL_EVENT_SIZE];
    memset(poll_fds, 0, sizeof(poll_fds));
    poll_fds[0].events = (POLLOUT | POLLERR);
    poll_fds[0].fd = con_.fd_;
    if (OB_SUCC(ret)) {
      // for connect timeout
      if (OB_FAIL(ObSocketManager::poll(poll_fds, POLL_EVENT_SIZE,
                                        static_cast<int>(timeout_ms), event_count))) {
        LOG_WDIAG("fail to poll", K(timeout_ms), K(ret));
      } else {
        if (0 == event_count) {
          ret = OB_TIMEOUT;
          LOG_WDIAG("connect timeout", K(timeout_ms), K(addr), K_(con_.fd), K(ret));
        } else if ((1 == event_count) && (poll_fds[0].fd == con_.fd_)) {
          // first detect sock error, if any
          int32_t optval = -1;
          int32_t optlen = sizeof(int32_t);
          if (OB_FAIL(ObSocketManager::getsockopt(con_.fd_, SOL_SOCKET, SO_ERROR,
                  reinterpret_cast<void *>(&optval), &optlen))) {
            LOG_WDIAG("fail to getsockopt", KERRMSGS, K_(con_.fd),
                     "revents", poll_fds[0].revents, K(ret));
          } else if (0 != optval) {
            ret = OB_ERR_SYS;
            LOG_WDIAG("detect socket error", K(optval), K_(con_.fd),
                     "revents", poll_fds[0].revents, K(ret));
          } else {
            if (poll_fds[0].revents & POLLOUT) {
              // just print client local ip:port
              ObIpEndpoint client_addr;
              int64_t len = sizeof(client_addr);
              if (OB_FAIL(ObSocketManager::getsockname(con_.fd_, &client_addr.sa_, &len))) {
                PROXY_SOCK_LOG(WDIAG, "failed to getsockname", K(addr_), KERRMSGS, K(ret));
              }
              ret = OB_SUCCESS; // ignore ret
              LOG_INFO("mysql raw client connect establish", K_(con_.fd), K(addr), K(client_addr), K(ret));
            } else if (poll_fds[0].revents & POLLERR) {
              ret = OB_ERR_UNEXPECTED;
              LOG_EDIAG("mysql raw client connect error", KERRMSGS, K_(con_.fd), K(addr),
                        "revents", poll_fds[0].revents,  K(ret));
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_EDIAG("unexpected state, fail to poll", K(event_count), KERRMSGS,
                        "revents", poll_fds[0].revents, K_(con_.fd), K(addr), K(ret));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected state, fail to connect", K(event_count), KERRMSGS,
                   K_(con_.fd), K(addr), K(ret));
        }
      }
    }


    // set read && write timeout
    if (OB_SUCC(ret)) {
      struct timeval t;
      t.tv_sec = static_cast<int>(msec_to_sec(timeout_ms));
      t.tv_usec = static_cast<int>(msec_to_usec(timeout_ms % 1000));
      if (OB_FAIL(ObSocketManager::setsockopt(con_.fd_, SOL_SOCKET, SO_SNDTIMEO, &t, sizeof(t)))) {
        LOG_WDIAG("fail to set socket opt send timeout", K_(con_.fd), K(timeout_ms), K(ret));
      } else if (OB_FAIL(ObSocketManager::setsockopt(con_.fd_, SOL_SOCKET, SO_RCVTIMEO, &t, sizeof(t)))) {
        LOG_WDIAG("fail to set socket opt recv timeout", K_(con_.fd), K(timeout_ms), K(ret));
      }
    }

    // do auth job
    if (OB_SUCC(ret)) {
      if (OB_FAIL(read_response(OB_MYSQL_COM_HANDSHAKE))) {
        LOG_WDIAG("fail to read response", K(ret));
      } else if (resp_->is_error_resp()) {
        ret = -resp_->get_err_code();
        LOG_WDIAG("fail to connect", K(ret));
      } else if (OB_FAIL(send_handshake_response())) {
        LOG_WDIAG("fail to send handshake response", K(ret));
      } else if (OB_FAIL(read_response(OB_MYSQL_COM_LOGIN))) {
        LOG_WDIAG("fail to read response", K(ret));
      } else if (resp_->is_error_resp()) {
        ret = -resp_->get_err_code();
        LOG_WDIAG("fail to auth", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_avail_ = true;
  }
  return ret;
}

int ObRawMysqlClientActor::send_request(const char *sql)
{
  int ret = OB_SUCCESS;
  const bool use_compress = false;
  const bool is_checksum_on = false;
  if (OB_ISNULL(sql) || strlen(sql) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(sql), K(ret));
  } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*request_buf_, OB_MYSQL_COM_QUERY, sql,
                                                                use_compress, is_checksum_on, 0))) {
    LOG_WDIAG("fail to write buffer", K(sql), K_(request_buf), K(ret));
  } else {
    resp_->consume_resp_buf(); // consume the handsake response packet data
    if (OB_FAIL(write_request(request_reader_))) {
      LOG_WDIAG("fail to write request", K(ret));
    } else if (OB_FAIL(read_response(OB_MYSQL_COM_QUERY))) {
      LOG_WDIAG("fail to read_response", K(ret));
    }
  }
  return ret;
}

int ObRawMysqlClientActor::send_handshake_response()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObClientUtils::build_handshake_response_packet(resp_, info_, request_buf_))) {
    LOG_WDIAG("fail to build handshake reponse", K(ret));
  } else {
    resp_->consume_resp_buf(); // consume the handsake packet data
    if (OB_FAIL(write_request(request_reader_))) {
      LOG_WDIAG("fail to write request", K(ret));
    }
  }

  return ret;
}

int ObRawMysqlClientActor::write_request(ObIOBufferReader *request_reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(request_reader) || OB_UNLIKELY(request_reader->read_avail() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(request_reader), K(ret));
  } else {
    int64_t buf_len = request_reader->read_avail();
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate mem", "alloc_len", buf_len, K(ret));
    } else {
      char *written_pos = request_reader->copy(buf, buf_len, 0);
      if (OB_UNLIKELY(written_pos != buf + buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("not copy completely", K(written_pos), K(buf), K(buf_len), K(ret));
      } else if (OB_FAIL(write_request(buf, buf_len))) {
        LOG_WDIAG("fail to write_request", K(ret));
      } else if (OB_FAIL(request_reader->consume(request_reader->read_avail()))) {
        LOG_WDIAG("fail to consume ", K(ret));
      } else {/*do nothing*/}
    }
    if (NULL != buf) {
      op_fixed_mem_free(buf, buf_len);
      buf = NULL;
      buf_len = 0;
    }
  }

  return ret;
}

int ObRawMysqlClientActor::write_request(const char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(buf), K(buf_len), K(ret));
  } else {
    int64_t total_write = 0;
    int64_t write_count = 0;
    while ((total_write < buf_len) && OB_SUCC(ret)) {
      if (OB_SUCC(ObSocketManager::write(con_.fd_, buf, buf_len, write_count))) {
        total_write += write_count;
      } else {
        LOG_WDIAG("fail to write", K(buf_len), K(total_write), K_(con_.fd),
                 K(write_count), K(ret));
      }
    }
  }

  return ret;
}

int ObRawMysqlClientActor::read_response(const ObMySQLCmd cmd)
{
  int ret = OB_SUCCESS;
  bool read_complete = false;
  resp_->consume_resp_buf(); // consume former data if has
  resp_->reset(); // clear former state
  ObMIOBuffer *mio_buf = resp_->get_resp_miobuf();

  mio_buf->water_mark_ = INT64_MAX;
  ObIOBufferBlock *block = NULL;
  int64_t block_write_avail = 0;
  char *buf = NULL;
  int64_t read_count = 0;
  while (OB_SUCC(ret) && !read_complete) {
    if (0 == block_write_avail) {
      if (OB_FAIL(mio_buf->check_add_block())) {
        buf = NULL;
        LOG_WDIAG("fail to check and add block", K(ret));
      } else {
        block = mio_buf->first_write_block();
        block_write_avail = block->write_avail();
        if (NULL != block) {
          buf = block->end_;
        }
      }
    }
    if (OB_ISNULL(buf) || OB_ISNULL(block) || block_write_avail <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected error", K(buf), K(block), K(block_write_avail), K(ret));
    } else if (OB_FAIL(ObSocketManager::read(con_.fd_, buf, block_write_avail, read_count))) {
      LOG_WDIAG("fail to read", K(ret));
    } else if (read_count > 0) {
      buf += read_count;
      block_write_avail -= read_count;
      if (OB_FAIL(block->fill(read_count))) {
        LOG_WDIAG("failed to fill iobuffer block", K(ret));
      } else {
        ret = resp_->analyze_resp(cmd);
        if (OB_EAGAIN == ret) {
          // continue read
          ret = OB_SUCCESS;
        } else if ((OB_SUCCESS == ret) && (resp_->is_resp_completed())) {
          // read completed
          read_complete = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("analyze_resp error", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to read", K(read_count), K(ret));
    }
    read_count = 0;
  }
  return ret;
}

int ObRawMysqlClient::init(const ObString &user_name,
                           const ObString &password,
                           const ObString &database,
                           const ObString &cluster_name,
                           const ObString &password1)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(user_name), K(ret));
  } else if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_FAIL(mutex_init(&mutex_))) {
    LOG_WDIAG("fail to init mutex", K(ret));
  } else if (OB_FAIL(info_.set_names(user_name, password, database, cluster_name, password1))) {
    LOG_WDIAG("fail to set names", K(user_name), K(password), K(password1), K(database), K(ret));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = mutex_destroy(&mutex_))) {
      LOG_EDIAG("fail to destroy mutex", K(tmp_ret));
    }
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObRawMysqlClient::destroy()
{
  if (is_inited_) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mutex_destroy(&mutex_))) {
      LOG_EDIAG("fail to destroy mutex", K(ret));
    }
    info_.reset();
    actor_.reset();
    is_inited_ = false;
  }
}

DEF_TO_STRING(ObRawMysqlClient)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(info),
       "target_server", ObArrayWrap<ObProxyReplicaLocation>(target_server_, DEFAULT_SERVER_ADDRS_COUNT));
  J_OBJ_END();
  return pos;
}

int ObRawMysqlClient::set_server_addr(const ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  if (addrs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("addrs are empty", K(ret));
  } else {
    int64_t max_count = DEFAULT_SERVER_ADDRS_COUNT;
    int64_t count = std::min(addrs.count(), max_count);
    for (int64_t i = 0; i < count; ++i) {
      target_server_[i].server_ = addrs.at(i);
      target_server_[i].replica_type_ = common::REPLICA_TYPE_FULL;
      target_server_[i].role_ = common::FOLLOWER;
    }
  }
  return ret;
}

int ObRawMysqlClient::set_target_server(const common::ObIArray<ObProxyReplicaLocation> &replicas)
{
  int ret = OB_SUCCESS;
  if (replicas.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("addrs are empty", K(ret));
  } else {
    int64_t max_count = DEFAULT_SERVER_ADDRS_COUNT;
    int64_t count = std::min(replicas.count(), max_count);
    for (int64_t i = 0; i < count; ++i) {
      target_server_[i] = replicas.at(i);
    }
  }
  return ret;
}

int ObRawMysqlClient::sync_raw_execute(const char *sql, const int64_t timeout_ms,
                                       ObClientMysqlResp *&resp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(sql) || OB_UNLIKELY(timeout_ms <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid intput value", K(sql), K(timeout_ms), K(ret));
  } else if (OB_FAIL(mutex_acquire(&mutex_))) {
    LOG_WDIAG("fail to acquire mutex", K(ret));
  } else {
    bool retry = true;
    if (actor_.is_avail()) {
      if (OB_FAIL(actor_.sync_raw_execute(sql, timeout_ms, resp))) {
        LOG_WDIAG("fail to sync raw execute, will retry", K(sql), K(timeout_ms), K(ret));
      } else {
        retry = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }

    if (retry) {
      ObProxyReplicaLocation replicas[DEFAULT_SERVER_ADDRS_COUNT];
      MEMCPY(replicas, target_server_, sizeof(target_server_));
      std::random_shuffle(replicas + 0, replicas + DEFAULT_SERVER_ADDRS_COUNT);
      int tmp_ret = ret;//we need save last err ret
      ret = OB_SUCCESS;

      //simple parse
      const ObString SELECT_STRING = ObString::make_string("select");
      const ObString sql_string = trim_header_space(ObString::make_string(sql));
      const bool is_strong_stml = (0 != strncasecmp(sql_string.ptr(), SELECT_STRING.ptr(), std::min(SELECT_STRING.length(), sql_string.length())));

      int64_t idx = ObTimeUtility::current_time(); // random
      for (int64_t i = 0; (i < DEFAULT_SERVER_ADDRS_COUNT) && (retry) && OB_SUCC(ret); ++i) {
        idx = (idx + 1) % DEFAULT_SERVER_ADDRS_COUNT;
        if (replicas[idx].is_valid()) {
          if ((!is_strong_stml && replicas[idx].is_weak_read_avail())
              || (is_strong_stml && replicas[idx].is_full_replica())) {
            actor_.reset();
            LOG_INFO("ObRawMysqlClient::sync_raw_execute will connect to",
                      "replica", replicas[idx], K(idx), K(is_strong_stml));
            if (OB_FAIL(actor_.init(info_))) {
              LOG_WDIAG("fail to init actor", K(ret));
            } else if (OB_FAIL(actor_.set_addr(replicas[idx].server_))) {
              LOG_WDIAG("fail to set addr", "replica", replicas[idx], K(ret));
            } else if (OB_FAIL(actor_.sync_raw_execute(sql, timeout_ms, resp))) {
              LOG_WDIAG("fail to sync raw execute", "replica", replicas[idx], K(sql), K(timeout_ms), K(ret));
              tmp_ret = ret;//we need save last err ret
              ret = OB_SUCCESS;
            } else {
              retry = false;
            }
          } else {
            LOG_INFO("current replica is not available, try next", "replica", replicas[idx], K(idx), K(is_strong_stml), K(sql));
          }
        }
      }

      if (retry && OB_SUCC(ret)) {
        ret = tmp_ret;//we need reset last err ret
      }
    }

    // do not forget to release mutex
    int mutex_ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != (mutex_ret = mutex_release(&mutex_)))) {
      LOG_WDIAG("fail to release mutex", K(mutex_ret));
    }
  }
  return ret;
}

int ObRawMysqlClient::disconnect()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mutex_acquire(&mutex_))) {
    LOG_WDIAG("fail to acquire mutex", K(ret));
  } else {
    actor_.reset();
    if (OB_FAIL(mutex_release(&mutex_))) {
      LOG_WDIAG("fail to release mutex", K(ret));
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
