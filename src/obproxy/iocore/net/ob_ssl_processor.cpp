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

#define USING_LOG_PREFIX PROXY_NET

#include "iocore/net/ob_ssl_processor.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "obutils/ob_proxy_config.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/alloc/malloc_hook.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

ObSSLProcessor g_ssl_processor;

int ObSSLProcessor::init()
{
  int ret = OB_SUCCESS;

  CRYPTO_set_mem_functions(ObSSLProcessor::malloc_for_ssl, ObSSLProcessor::realloc_for_ssl,
                           ObSSLProcessor::free_for_ssl);
  // SSL_library_init() always returns "1", so it is safe to discard the return value.
  SSL_library_init();
  OpenSSL_add_all_algorithms();
  SSL_load_error_strings();


  SSL_CTX *ssl_ctx = SSL_CTX_new(SSLv23_method());
  if (OB_ISNULL(ssl_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ssl ctx is null", K(ret));
  } else {
    ssl_ctx_ = ssl_ctx;
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, NULL);
  }

  return ret;
}

void *ObSSLProcessor::malloc_for_ssl(size_t size)
{
  void *ptr = NULL;
  lib::glibc_hook_opt = lib::GHO_HOOK;
  __COMPILER_BARRIER();
  ptr = malloc(size);
  lib::glibc_hook_opt = lib::GHO_NOHOOK;
  return ptr;
}

void *ObSSLProcessor::realloc_for_ssl(void *ptr, size_t size)
{
  void *nptr = NULL;
  lib::glibc_hook_opt = lib::GHO_HOOK;
  __COMPILER_BARRIER();
  nptr = realloc(ptr, size);
  lib::glibc_hook_opt = lib::GHO_NOHOOK;
  return nptr;
}

void ObSSLProcessor::free_for_ssl(void *ptr)
{
  lib::glibc_hook_opt = lib::GHO_HOOK;
  __COMPILER_BARRIER();
  free(ptr);
  lib::glibc_hook_opt = lib::GHO_NOHOOK;
}

int ObSSLProcessor::update_key(const ObString &source_type,
                               const ObString &ca,
                               const ObString &public_key,
                               const ObString &private_key)
{
  int ret = OB_SUCCESS;
  if (source_type == "DBMESH") {
    if (OB_FAIL(update_key_from_dbmesh(ca, public_key, private_key))) {
      LOG_WARN("update key from dbmesh failed", K(ret), K(ca), K(public_key), K(private_key));
    }
  } else if (source_type == "FILE") {
    if (OB_FAIL(update_key_from_file(ca, public_key, private_key))) {
      LOG_WARN("update key from file failed", K(ret), K(ca), K(public_key), K(private_key));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown source type", K(source_type), K(ret));
  }

  return ret;
}

int ObSSLProcessor::update_key_from_file(const ObString &ca,
                                         const ObString &public_key,
                                         const ObString &private_key)
{
  int ret = OB_SUCCESS;
  if (ca.empty() || public_key.empty() || private_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(ca), K(public_key), K(private_key));
  } else {
    if (OB_SSL_SUCC_RET != SSL_CTX_load_verify_locations(ssl_ctx_, ca.ptr(), NULL)) {
      ret = OB_SSL_ERROR;
      LOG_WARN("load verify location failed", K(ca), K(ret));
    } else if (OB_SSL_SUCC_RET != SSL_CTX_use_certificate_chain_file(ssl_ctx_, public_key.ptr())) {
      ret = OB_SSL_ERROR;
      LOG_WARN("use certificate file failed", K(ret), K(public_key));
    } else if (OB_SSL_SUCC_RET != SSL_CTX_use_PrivateKey_file(ssl_ctx_, private_key.ptr(), SSL_FILETYPE_PEM)) {
      ret = OB_SSL_ERROR;
      LOG_WARN("use private key file failed", K(ret), K(private_key));
    } else if (OB_SSL_SUCC_RET != SSL_CTX_check_private_key(ssl_ctx_)) {
      ret = OB_SSL_ERROR;
      LOG_WARN("check private key failed", K(ret), K(ca), K(public_key), K(private_key));
    } else {
      ATOMIC_STORE(&ssl_inited_, true);
      LOG_INFO("ssl inited using file succ");
    }
  }

  return ret;
}

int ObSSLProcessor::update_key_from_dbmesh(const ObString &ca,
                                           const ObString &public_key,
                                           const ObString &private_key)
{
  int ret = OB_SUCCESS;
  if (ca.empty() || public_key.empty() || private_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(ca), K(public_key), K(private_key));
  } else {
    //load ca cert
    BIO *cbio = BIO_new_mem_buf((void*)ca.ptr(), -1);
    X509 *cert_x509 = PEM_read_bio_X509(cbio, NULL, 0, NULL);
    X509_STORE *x509_store = X509_STORE_new();
    if (NULL == cert_x509) {
      ret = OB_SSL_ERROR;
      LOG_WARN("pem read bio x509 failed", K(ret));
    } else if (OB_SSL_SUCC_RET != X509_STORE_add_cert(x509_store, cert_x509)) {
      ret = OB_SSL_ERROR;
      LOG_WARN("x509 store add cert failed", K(ret));
    } else {
      SSL_CTX_set_cert_store(ssl_ctx_, x509_store);
    }

    // load app cert chain
    if (OB_SUCC(ret)) {
      cbio = BIO_new_mem_buf((void*)public_key.ptr(), -1);
      STACK_OF(X509_INFO) *inf = PEM_X509_INFO_read_bio(cbio, NULL, NULL, NULL);
      int is_first = 1;
      for (int i = 0; OB_SUCC(ret) && i < sk_X509_INFO_num(inf); i++) {
        X509_INFO *itmp = sk_X509_INFO_value(inf, i);
        if (itmp->x509) {
          if (is_first) {
            is_first = 0;
            if (OB_SSL_SUCC_RET != SSL_CTX_use_certificate(ssl_ctx_, itmp->x509)) {
              ret = OB_SSL_ERROR;
              sk_X509_INFO_pop_free(inf, X509_INFO_free); //cleanup
              LOG_WARN("ssl ctx use cerjtificate failed", K(ret));
            }
          } else {
            if (OB_SSL_SUCC_RET != SSL_CTX_add_extra_chain_cert(ssl_ctx_, itmp->x509)) {
              ret = OB_SSL_ERROR;
              sk_X509_INFO_pop_free(inf, X509_INFO_free); //cleanup
              LOG_WARN("ssl ctx add extra chain cert failed", K(ret));
            } else {
              /*
               * Above function doesn't increment cert reference count. NULL the info
               * reference to it in order to prevent it from being freed during cleanup.
               */
              itmp->x509 = NULL;
            }
          }
        }
      }
      sk_X509_INFO_pop_free(inf, X509_INFO_free); //cleanup
    }

    //load private key
    if (OB_SUCC(ret)) {
      RSA *rsa = NULL;
      cbio = BIO_new_mem_buf((void*)private_key.ptr(), -1);
      if (NULL == (rsa = PEM_read_bio_RSAPrivateKey(cbio, NULL, 0, NULL))) {
        ret = OB_SSL_ERROR;
        LOG_WARN("pem read bio rsaprivatekey failed", K(ret));
      } else if (OB_SSL_SUCC_RET != SSL_CTX_use_RSAPrivateKey(ssl_ctx_, rsa)) {
        ret = OB_SSL_ERROR;
        LOG_WARN("ssl ctx use rsaprivatekey failed", K(ret));
      } else {
        LOG_DEBUG("update ssl key from dbmesh");
      }
    }

    if (OB_SUCC(ret)) {
      ATOMIC_STORE(&ssl_inited_, true);
      LOG_INFO("ssl inited using dbmesh succ");
    }
  }

  return ret;
}

SSL* ObSSLProcessor::create_new_ssl()
{
  SSL *new_ssl = NULL;
  new_ssl = SSL_new(ssl_ctx_);

  return new_ssl;
}

void ObSSLProcessor::release_ssl(SSL* ssl, const bool can_shutdown_ssl)
{
  if (NULL != ssl) {
    if (can_shutdown_ssl) {
      SSL_shutdown(ssl);
    }
    SSL_free(ssl);
  }
}

bool ObSSLProcessor::is_client_ssl_supported()
{
  return get_global_proxy_config().enable_client_ssl && ATOMIC_LOAD(&ssl_inited_);
}

bool ObSSLProcessor::is_server_ssl_supported()
{
  return get_global_proxy_config().enable_server_ssl && ATOMIC_LOAD(&ssl_inited_);
}

} // end net
} // end obproxy
} // end oceanbase
