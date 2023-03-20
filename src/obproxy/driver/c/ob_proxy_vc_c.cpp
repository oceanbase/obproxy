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

#include "ob_proxy_vc_c.h"
#include "proxy/client/ob_driver_client.h"
#include "ob_proxy_init.h"
#include "lib/oblog/ob_log.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

#include <errno.h>

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;

int ob_proxy_vc_c_init(const char *config, void **p_driver_client)
{
  int ret = OB_SUCCESS;
  ObDriverClient *driver_client = NULL;
  
  if (OB_ISNULL(config) || OB_ISNULL(p_driver_client)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid argument config=%p, p_driver_client=%p, ret=%d", config, p_driver_client, ret);
  } else if (OB_FAIL(init_obproxy_client(config))) {
    MPRINT("init obproxy client failed, ret=%d", ret);
  } else if (OB_ISNULL(driver_client = new(std::nothrow) ObDriverClient())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client is null unexpected", K(ret));
  } else if (OB_FAIL(driver_client->init())) {
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client init failed", K(ret));
  } else {
    *p_driver_client = static_cast<void *>(driver_client);
    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "create driver_client success", KP(driver_client));
  }

  if(OB_FAIL(ret) && OB_NOT_NULL(driver_client)) {
    delete driver_client;
  }

  return ret;
}

int ob_proxy_vc_c_connect(void *driver_client_handle, int64_t conn_timeout, int64_t sock_timeout)
{
  int ret = OB_SUCCESS;
  ObDriverClient *driver_client;

  if (OB_ISNULL(driver_client_handle) || conn_timeout < 0 || sock_timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "invalid argument", KP(driver_client_handle), K(conn_timeout), K(sock_timeout), K(ret));
  } else {
    driver_client = static_cast<ObDriverClient *>(driver_client_handle);
    driver_client->set_connect_timeout(conn_timeout);

    if (conn_timeout == 0 && OB_FAIL(driver_client->sync_connect())) {
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client connect failed", K(ret));
    } else if (conn_timeout > 0 && OB_FAIL(driver_client->sync_connect_with_timeout())) {
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client connect with timeout failed", K(conn_timeout), K(ret));
    } else {
      driver_client->set_send_timeout(sock_timeout);
      driver_client->set_recv_timeout(sock_timeout);
      OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "driver client connect success", K(driver_client));
    }   
  }

  return ret;
}

int ob_proxy_vc_c_recv(void *driver_client_handle, char *buffer, int64_t offset, int64_t mlen, int64_t *recv_len, int64_t flag)
{
  int ret = OB_SUCCESS;
  int64_t rlen = 0;
  ObDriverClient *driver_client;
  UNUSED(flag);

  if (OB_ISNULL(driver_client_handle) || OB_ISNULL(buffer) || OB_ISNULL(recv_len)) {
    ret = OB_INVALID_ARGUMENT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "invalid argument", KP(driver_client_handle), K(buffer), K(recv_len), K(ret));
  } else {
    driver_client = static_cast<ObDriverClient *>(driver_client_handle);
    if (OB_FAIL(driver_client->sync_recv_with_timeout(buffer + offset, mlen, rlen))) {
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "sync recv failed", K(ret));
    } else {
      if (rlen < 0) {
        switch (errno) {
          case ECONNRESET:
          case EPIPE:
            OBPROXY_DRIVER_CLIENT_LOG(WARN, "Connection reset", "errno", errno);
            break;
          case EBADF:
            OBPROXY_DRIVER_CLIENT_LOG(WARN, "Socket closed", "errno", errno);
            break;
          case EINTR:
            OBPROXY_DRIVER_CLIENT_LOG(WARN, "Operation interrupted", "errno", errno);
            break;
          default:
            OBPROXY_DRIVER_CLIENT_LOG(WARN, "socket read error", "errno", errno);
            break;
        }
        ret = OB_ERR_UNEXPECTED;
      } else if (rlen == 0) {
        ret = OB_ERR_UNEXPECTED;
        OBPROXY_DRIVER_CLIENT_LOG(WARN, "peer close", KP(driver_client), K(ret));
      } else {
        OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "read success", K(rlen));
        *recv_len = rlen;
      }
    }
  }
 
  return ret;
}

int ob_proxy_vc_c_send(void *driver_client_handle, const char *buffer, int64_t offset, int64_t mlen, int64_t *send_len, int64_t flag)
{
  int ret = OB_SUCCESS;
  int64_t wlen = 0;
  ObDriverClient *driver_client;
  UNUSED(flag);

  if (OB_ISNULL(driver_client_handle) || OB_ISNULL(buffer) || OB_ISNULL(send_len)) {
    ret = OB_INVALID_ARGUMENT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "invalid argument", KP(driver_client_handle), K(buffer), K(send_len), K(ret));
  } else {
    driver_client = static_cast<ObDriverClient *>(driver_client_handle);

    if (OB_FAIL(driver_client->sync_send_with_timeout(buffer + offset, mlen, wlen))) {
      ret = OB_ERR_UNEXPECTED; //send error;
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "sync_send_with_timeout failed", K(wlen), K(ret));
    } else {
      if (wlen > 0) {
        OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "sync write success", K(wlen));
        *send_len = wlen;
      } else {
        ret = ob_get_sys_errno();
        if (errno == ECONNRESET) {
          OBPROXY_DRIVER_CLIENT_LOG(WARN, "wlen is unexpected, connection reset", K(wlen), K(ret));
        } else {
          OBPROXY_DRIVER_CLIENT_LOG(WARN, "wlen is unexpected, write failed", K(wlen), K(ret));
        }
      }
    }
  }

  return ret;
}

int ob_proxy_vc_c_close(void *driver_client_handle)
{
  int ret = OB_SUCCESS;
  ObDriverClient *driver_client;

  if (OB_ISNULL(driver_client_handle)) {
    ret = OB_INVALID_ARGUMENT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "invalid argument", KP(driver_client_handle), K(ret));
  } else {
    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "free driver client", KP(driver_client_handle));
    driver_client = static_cast<ObDriverClient *>(driver_client_handle);
    delete driver_client;
    driver_client = NULL;
  }

  return ret;
}

int ob_proxy_vc_set_timeout(void *driver_client_handle, int type, int timeout)
{
  int ret = OB_SUCCESS;
  ObDriverClient *driver_client;
  const int C_DRIVER_CONNECT_TIMEOUT = 0;
  const int C_DRIVER_READ_TIMEOUT = 1;
  const int C_DRIVER_WRITE_TIMEOUT = 2;
  const int C_DRIVER_TIMEOUT_MAX = 3;

  if (OB_ISNULL(driver_client_handle) || type < 0 || C_DRIVER_TIMEOUT_MAX <= type) {
    ret = OB_INVALID_ARGUMENT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "invalid argument", KP(driver_client_handle), K(type), K(ret));
  } else {
    driver_client = static_cast<ObDriverClient *>(driver_client_handle);

    if (C_DRIVER_CONNECT_TIMEOUT == type) {
      driver_client->set_connect_timeout(timeout);
    } else if (C_DRIVER_READ_TIMEOUT == type) {
      driver_client->set_recv_timeout(timeout);
    } else if (C_DRIVER_WRITE_TIMEOUT == type) {
      driver_client->set_send_timeout(timeout);
    }

    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "driver client set timeout succ", K(type), K(timeout));
  }

  return ret;
}

int ob_proxy_vc_get_timeout(void *driver_client_handle, int type, int *timeout)
{
  int ret = OB_SUCCESS;
  ObDriverClient *driver_client;
  const int C_DRIVER_CONNECT_TIMEOUT = 0;
  const int C_DRIVER_READ_TIMEOUT = 1;
  const int C_DRIVER_WRITE_TIMEOUT = 2;
  const int C_DRIVER_TIMEOUT_MAX = 3;

  if (OB_ISNULL(driver_client_handle) || OB_ISNULL(timeout) || type < C_DRIVER_CONNECT_TIMEOUT || C_DRIVER_TIMEOUT_MAX <= type) {
    ret = OB_INVALID_ARGUMENT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "invalid argument", KP(driver_client_handle), KP(timeout), K(type), K(ret));
  } else {
    driver_client = static_cast<ObDriverClient *>(driver_client_handle);

    if (C_DRIVER_CONNECT_TIMEOUT == type) {
      *timeout = static_cast<int>(driver_client->get_connect_timeout());
    } else if (C_DRIVER_READ_TIMEOUT == type) {
      *timeout = static_cast<int>(driver_client->get_recv_timeout());
    } else if (C_DRIVER_WRITE_TIMEOUT == type) {
      *timeout = static_cast<int>(driver_client->get_send_timeout());
    }

    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "driver client get timeout succ", K(type), "timeout", *timeout);
  }

  return ret;
}