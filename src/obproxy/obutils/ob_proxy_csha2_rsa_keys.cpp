/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "obutils/ob_proxy_csha2_rsa_keys.h"

#include <cstdio>
#include <cstring>

#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include "lib/file/file_directory_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

static const char *const FIXED_PRIV_PATH = ".conf/rsa_private.pem";
static const char *const FIXED_PUB_PATH = ".conf/rsa_public.pem";

static RSA *g_rsa_priv = NULL;
static char g_resolved_priv_path[FileDirectoryUtils::MAX_PATH + 1];
static char g_resolved_pub_path[FileDirectoryUtils::MAX_PATH + 1];
static bool g_paths_ready = false;

static int ensure_parent_dir_of_file(const char *filepath)
{
  int ret = OB_SUCCESS;
  char buf[FileDirectoryUtils::MAX_PATH + 1];
  if (OB_ISNULL(filepath) || filepath[0] == '\0') {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t n = static_cast<int64_t>(STRLEN(filepath));
    if (n <= 0 || n >= static_cast<int64_t>(sizeof(buf))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("caching_sha2_password key path too long", K(n), K(ret));
    } else {
      MEMCPY(buf, filepath, static_cast<size_t>(n) + 1);
      char *slash = strrchr(buf, '/');
      if (NULL != slash && slash != buf) {
        *slash = '\0';
        if (OB_FAIL(FileDirectoryUtils::create_full_path(buf))) {
          LOG_WDIAG("fail to create parent dir for key file", K(buf), K(ret));
        }
      }
    }
  }
  return ret;
}

static int remove_existing_key_pem_if_any(const char *path)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_FAIL(FileDirectoryUtils::is_exists(path, exists))) {
    LOG_WDIAG("fail to stat key path before regenerate", K(path), K(ret));
  } else if (exists) {
    LOG_INFO("removing existing caching_sha2_password RSA key file before regenerate",
             K(path));
    if (OB_FAIL(FileDirectoryUtils::delete_file(path))) {
      LOG_WDIAG("fail to remove existing key file before regenerate", K(path), K(ret));
    } else {
      LOG_INFO("removed existing caching_sha2_password RSA key file", K(path));
    }
  }
  return ret;
}

// Generate 2048-bit RSA key pair and write unencrypted private PEM + SPKI public PEM (aligned with typical MySQL/OBSERVER wallet).
static int generate_rsa_and_write_pem(const char *priv_path, const char *pub_path, RSA *&out_rsa)
{
  int ret = OB_SUCCESS;
  out_rsa = NULL;
  RSA *rsa = RSA_new();
  BIGNUM *e = BN_new();
  FILE *fp = NULL;

  if (OB_ISNULL(rsa) || OB_ISNULL(e)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("BN_new/RSA_new failed", K(ret));
  } else if (0 == BN_set_word(e, RSA_F4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("BN_set_word failed", K(ret));
  } else {
    ERR_clear_error();
    if (1 != RSA_generate_key_ex(rsa, 2048, e, NULL)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("RSA_generate_key_ex failed", K(ret), "openssl_err", ERR_get_error());
    }
  }
  if (NULL != e) {
    BN_free(e);
    e = NULL;
  }

  if (OB_FAIL(ret)) {
    if (NULL != rsa) {
      RSA_free(rsa);
      rsa = NULL;
    }
  } else if (NULL == (fp = fopen(priv_path, "wb"))) {
    ret = OB_IO_ERROR;
    LOG_WDIAG("fail to open private key file for write", K(priv_path), KERRMSGS, K(ret));
    RSA_free(rsa);
    rsa = NULL;
  } else if (1 != PEM_write_RSAPrivateKey(fp, rsa, NULL, NULL, 0, NULL, NULL)) {
    ret = OB_IO_ERROR;
    LOG_WDIAG("PEM_write_RSAPrivateKey failed", K(priv_path), K(ret));
    fclose(fp);
    fp = NULL;
    RSA_free(rsa);
    rsa = NULL;
  } else {
    fclose(fp);
    fp = NULL;
    if (NULL == (fp = fopen(pub_path, "wb"))) {
      ret = OB_IO_ERROR;
      LOG_WDIAG("fail to open public key file for write", K(pub_path), KERRMSGS, K(ret));
      RSA_free(rsa);
      rsa = NULL;
    } else if (1 != PEM_write_RSA_PUBKEY(fp, rsa)) {
      ret = OB_IO_ERROR;
      LOG_WDIAG("PEM_write_RSA_PUBKEY failed", K(pub_path), K(ret));
      fclose(fp);
      fp = NULL;
      RSA_free(rsa);
      rsa = NULL;
    } else {
      fclose(fp);
      fp = NULL;
      out_rsa = rsa;
      rsa = NULL;
      LOG_INFO("generated caching_sha2_password RSA key pair for obproxy at startup",
               K(priv_path), K(pub_path));
    }
  }

  if (NULL != rsa) {
    RSA_free(rsa);
    rsa = NULL;
  }
  return ret;
}

void init_csha2_rsa_keys()
{
  int ret = OB_SUCCESS;
  const char *priv_eff = FIXED_PRIV_PATH;
  const char *pub_eff = FIXED_PUB_PATH;

  if (NULL != g_rsa_priv) {
    RSA_free(g_rsa_priv);
    g_rsa_priv = NULL;
  }
  g_paths_ready = false;

  const int64_t lp = static_cast<int64_t>(STRLEN(priv_eff));
  const int64_t lq = static_cast<int64_t>(STRLEN(pub_eff));
  if (lp <= 0 || lp > FileDirectoryUtils::MAX_PATH || lq <= 0 || lq > FileDirectoryUtils::MAX_PATH) {
    LOG_WDIAG("invalid caching_sha2_password key path length; proxy runs without client-leg RSA csha2",
              K(lp), K(lq));
  } else {
    MEMCPY(g_resolved_priv_path, priv_eff, static_cast<size_t>(lp) + 1);
    MEMCPY(g_resolved_pub_path, pub_eff, static_cast<size_t>(lq) + 1);

    LOG_INFO("caching_sha2_password RSA key paths (relative to process cwd, keys regenerated each startup)",
            K(priv_eff), K(pub_eff));

    RSA *rsa = NULL;
    if (OB_FAIL(ensure_parent_dir_of_file(priv_eff))) {
      LOG_WDIAG("caching_sha2_password RSA init: ensure parent dir for private key path failed",
                K(priv_eff), K(ret));
    } else if (OB_FAIL(ensure_parent_dir_of_file(pub_eff))) {
      LOG_WDIAG("caching_sha2_password RSA init: ensure parent dir for public key path failed",
                K(pub_eff), K(ret));
    } else if (OB_FAIL(remove_existing_key_pem_if_any(priv_eff))) {
      LOG_WDIAG("caching_sha2_password RSA init: remove existing private PEM failed",
                K(priv_eff), K(ret));
    } else if (OB_FAIL(remove_existing_key_pem_if_any(pub_eff))) {
      LOG_WDIAG("caching_sha2_password RSA init: remove existing public PEM failed",
                K(pub_eff), K(ret));
    } else if (OB_FAIL(generate_rsa_and_write_pem(priv_eff, pub_eff, rsa))) {
      LOG_WDIAG("caching_sha2_password RSA init: generate key pair or write PEM failed",
                K(priv_eff), K(pub_eff), K(ret));
    }

    if (OB_SUCC(ret)) {
      g_rsa_priv = rsa;
      g_paths_ready = true;
    } else {
      if (NULL != rsa) {
        RSA_free(rsa);
        rsa = NULL;
      }
      g_rsa_priv = NULL;
      g_paths_ready = false;
      LOG_WDIAG("caching_sha2_password RSA key init failed; proxy continues without non-TLS client-leg RSA "
                "(clients needing ODP public key or RSA decrypt will fail that connection)",
                K(ret), K(priv_eff), K(pub_eff));
    }
  }
}

RSA *ob_proxy_csha2_rsa_get_private_key()
{
  return g_rsa_priv;
}

const char *ob_proxy_csha2_rsa_get_public_key_path()
{
  return g_paths_ready ? g_resolved_pub_path : FIXED_PUB_PATH;
}

const char *ob_proxy_csha2_rsa_get_private_key_path()
{
  return g_paths_ready ? g_resolved_priv_path : FIXED_PRIV_PATH;
}

} // namespace obutils
} // namespace obproxy
} // namespace oceanbase
