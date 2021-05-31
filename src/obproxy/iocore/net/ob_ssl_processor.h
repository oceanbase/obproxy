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

#ifndef OBPROXY_SSL_PROCESSOR_H_
#define OBPROXY_SSL_PROCESSOR_H_

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "iocore/eventsystem/ob_lock.h"
#include "lib/lock/ob_drw_lock.h"

#define OB_SSL_SUCC_RET 1

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObSSLProcessor
{
public:
  ObSSLProcessor() : ssl_ctx_(NULL), ssl_inited_(false) {}
  ~ObSSLProcessor() {}
  int init();
  SSL* create_new_ssl();
  void release_ssl(SSL* ssl, const bool can_shutdown_ssl);
  void openssl_lock(int n);
  void openssl_unlock(int n);
  bool is_client_ssl_supported();
  bool is_server_ssl_supported();
  int update_key(const common::ObString &source_type,
                 const common::ObString &ca,
                 const common::ObString &public_key,
                 const common::ObString &private_key);

private:
  int update_key_from_file(const common::ObString &ca,
                           const common::ObString &public_key,
                           const common::ObString &private_key);

  int update_key_from_dbmesh(const common::ObString &ca,
                             const common::ObString &public_key,
                             const common::ObString &private_key);
private:
  static void *malloc_for_ssl(size_t num);
  static void *realloc_for_ssl(void *p, size_t num);
  static void free_for_ssl(void *str);

private:
  SSL_CTX *ssl_ctx_;
  bool ssl_inited_;
};

extern ObSSLProcessor g_ssl_processor;

} // end net
} // end obproxy
} // end oceanbase

#endif

