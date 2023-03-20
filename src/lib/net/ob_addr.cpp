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

#define USING_LOG_PREFIX LIB

#include "lib/net/ob_addr.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{

// --------------------------------------------------------
// class ObAddr implements
// --------------------------------------------------------
int ObAddr::convert_ipv4_addr(const char *ip)
{
  int ret = OB_SUCCESS;
  in_addr in;

  if (!OB_ISNULL(ip)) {
    MEMSET(&in, 0, sizeof (in));
    int rt = inet_pton(AF_INET, ip, &in);
    if (rt != 1) { // wrong ip or error
      in.s_addr = 0;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("convert ipv4 addr failed", K(ip));
    } else {
      ip_.v4_ = ntohl(in.s_addr);
    }
  }
  return ret;
}

int ObAddr::convert_ipv6_addr(const char *ip)
{
  int ret = OB_SUCCESS;
  in6_addr in6;
  if (!OB_ISNULL(ip)) {
    memset(&in6, 0, sizeof(in6));
    ret = inet_pton(AF_INET6, ip, &in6);
    if (ret != 1) {
      memset(&in6, 0, sizeof(in6));
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("convert ipv6 addr failed", K(ip));
    } else {
      ret = OB_SUCCESS;
      // Stored here in big-endian format
      MEMCPY(ip_.v6_, in6.s6_addr, sizeof(ip_.v6_));
    }
  }
  return ret;
}

int ObAddr::parse_from_cstring(const char *ipport)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_ADDR_LENGTH] = "";
  int port = 0;

  if (!OB_ISNULL(ipport)) {
    MEMCPY(buf, ipport, MIN(strlen(ipport), sizeof (buf) - 1));
    char *pport = strrchr(buf, ':');
    if (NULL != pport) {
      *(pport++) = '\0';
      char *end = NULL;
      port = static_cast<int>(strtol(pport, &end, 10));
      if (NULL == end
          || end - pport != static_cast<int64_t>(strlen(pport))) {
        ret = OB_INVALID_ARGUMENT;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCC(ret)) {
    if ('[' != buf[0]) {  // IPV4 format
      if (false == set_ipv4_addr(buf, port)) {
        ret = OB_INVALID_ARGUMENT;
      }
    } else {              // IPV6 format
      const char *ipv6 = buf + 1;
      if (']' == buf[strlen(buf) - 1]) {
        buf[strlen(buf) - 1] = '\0';
        if (!set_ipv6_addr(ipv6, port)) {
          ret = OB_INVALID_ARGUMENT;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

int64_t ObAddr::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;
  if (NULL != buffer && size > 0) {
    // databuff_printf(buffer, size, pos, "version=%d ", version_);
    if (version_ == IPV4) {
      if (port_ > 0) {
        databuff_printf(buffer, size, pos, "\"%d.%d.%d.%d:%d\"",
                        (ip_.v4_ >> 24) & 0xFF,
                        (ip_.v4_ >> 16) & 0xFF,
                        (ip_.v4_ >> 8) & 0xFF,
                        (ip_.v4_) & 0xFF, port_);
      } else {
        databuff_printf(buffer, size, pos, "\"%d.%d.%d.%d\"",
                        (ip_.v4_ >> 24) & 0xFF,
                        (ip_.v4_ >> 16) & 0xFF,
                        (ip_.v4_ >> 8) & 0xFF,
                        (ip_.v4_) & 0xFF);
      }
    } else if (version_ == IPV6) {
      char buf[MAX_IP_ADDR_LENGTH];
      struct in6_addr in6;
      memset(buf, 0, sizeof(buf));
      MEMCPY(in6.s6_addr, ip_.v6_, sizeof(ip_.v6_));
      inet_ntop(AF_INET6, &in6, buf, MAX_IP_ADDR_LENGTH);
      if (port_ > 0) {
        databuff_printf(buffer, size, pos, "\"[%s]:%d\"", buf, port_);
      } else {
        databuff_printf(buffer, size, pos, "\"%s\"", buf);
      }
    }
  }
  return pos;
}

bool ObAddr::ip_to_string(char *buffer, const int32_t size) const
{
  bool res = false;
  if (NULL != buffer && size > 0) {
    if (version_ == IPV4) {
      snprintf(buffer, size, "%d.%d.%d.%d",
               (ip_.v4_ >> 24) & 0XFF,
               (ip_.v4_ >> 16) & 0xFF,
               (ip_.v4_ >> 8) & 0xFF,
               (ip_.v4_) & 0xFF);
    } else if (version_ == IPV6) {
      char buf[MAX_IP_ADDR_LENGTH];
      struct in6_addr in6;
      memset(buf, 0, sizeof(buf));
      MEMCPY(in6.s6_addr, ip_.v6_, sizeof(ip_.v6_));
      inet_ntop(AF_INET6, &in6, buf, MAX_IP_ADDR_LENGTH);
      snprintf(buffer, size, "%s", buf);
    }
    res = true;
  }
  return res;
}

int ObAddr::ip_port_to_string(char *buffer, const int32_t size) const
{
  int ret = OB_SUCCESS;
  int ret_len = 0;
  if (NULL == buffer || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (version_ == IPV6) {
    char buf[MAX_IP_ADDR_LENGTH];
    struct in6_addr in6;
    memset(buf, 0, sizeof(buf));
    MEMCPY(in6.s6_addr, ip_.v6_, sizeof(ip_.v6_));
    inet_ntop(AF_INET6, &in6, buf, MAX_IP_ADDR_LENGTH);
    ret_len = snprintf(buffer, size, "[%s]:%d", buf, port_);
  } else {
    ret_len = snprintf(buffer, size, "%d.%d.%d.%d:%d",
                           (ip_.v4_ >> 24) & 0XFF,
                           (ip_.v4_ >> 16) & 0xFF,
                           (ip_.v4_ >> 8) & 0xFF,
                           (ip_.v4_) & 0xFF,
                           port_);
  }

  if (ret_len < 0) {
    ret = OB_ERR_SYS;
  } else if (ret_len >= size) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

bool ObAddr::set_ipv6_addr(const char *ip, const int32_t port)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  if (NULL == ip || port < 0) {
    bret = false;
  } else if (OB_FAIL(convert_ipv6_addr(ip))) {
    bret = false;
  } else {
    version_ = IPV6;
    port_ = port;
  }

  return bret;
}

bool ObAddr::set_ipv4_addr(const char *ip, const int32_t port)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  if (NULL == ip || port < 0) {
    bret = false;
  } else if (OB_FAIL(convert_ipv4_addr(ip))) {
    bret = false;
  } else {
    version_ = IPV4;
    port_ = port;
  }
  return bret;
}

bool ObAddr::set_ip_addr(const ObString &ip, const int32_t port)
{
  bool ret = true;
  char ip_buf[MAX_IP_ADDR_LENGTH] = "";
  if (ip.length() >= MAX_IP_ADDR_LENGTH) {
    ret = false;
  } else {
    // ObString may be not terminated by '\0'
    MEMCPY(ip_buf, ip.ptr(), ip.length());
    ret = set_ip_addr(ip_buf, port);
  }
  return ret;
}

bool ObAddr::is_valid() const
{
  bool valid = true;
  if (port_ <= 0) {
    valid = false;
  } else if (IPV4 == version_) {
    valid = (0 != ip_.v4_);
  } else if (IPV6 == version_) {
    valid = ((0 != ip_.v6_[0]) || (0 != ip_.v6_[1]) || (0 != ip_.v6_[2]) || (0 != ip_.v6_[3]));
  } else {
    valid = false;
  }
  return valid;
}

bool ObAddr::set_ipv4_addr(const uint32_t ip, const int32_t port)
{
  version_ = IPV4;
  ip_.v4_ = ip;
  port_ = port;
  return true;
}

//this is only for test
void ObAddr::reset_ipv4_10(int ip)
{
  ip_.v4_ = ip_.v4_ & 0xFFFFFF00L;
  ip_.v4_ += ip;
}

int64_t ObAddr::get_ipv4_server_id() const
{
  int64_t server_id = 0;
  if (IPV4 == version_) {
    server_id = ip_.v4_;
    server_id <<= 32;
    server_id |= port_;
  }
  return server_id;
}

void ObAddr::set_ipv4_server_id(const int64_t ipv4_server_id)
{
  version_ = IPV4;
  ip_.v4_  = static_cast<int32_t>(0x00000000ffffffff & (ipv4_server_id >> 32));
  port_ = static_cast<int32_t>(0x00000000ffffffff & ipv4_server_id);
}

bool ObAddr::operator <(const ObAddr &rv) const
{
  int64_t ipcmp = 0;
  if (version_ != rv.version_) {
    LOG_ERROR("comparision between different IP versions hasn't supported!");
  } else if (IPV4 == version_) {
    ipcmp = static_cast<int64_t>(ip_.v4_) - static_cast<int64_t>(rv.ip_.v4_);
  } else if (IPV6 == version_) {
    int pos = 0;
    for (; ipcmp == 0 && pos < IPV6_LEN; pos++) {
      ipcmp = ip_.v6_[pos] - rv.ip_.v6_[pos];
    }
  }
  return (ipcmp < 0) || (0 == ipcmp && port_ < rv.port_);
}

bool ObAddr::is_equal_except_port(const ObAddr &rv) const
{
  return version_ == rv.version_
      && 0 == MEMCMP(this, &rv.ip_, sizeof (rv.ip_));
}

uint64_t ObAddr::get_ipv6_high() const
{
  const uint64_t *p = reinterpret_cast<const uint64_t *>(&ip_.v6_[0]);
  return *p;
}

uint64_t ObAddr::get_ipv6_low() const
{
  const uint64_t *p = reinterpret_cast<const uint64_t *>(&ip_.v6_[2]);
  return *p;
}

void ObAddr::set_max()
{
  port_ = UINT32_MAX;
  memset(&ip_, 1, sizeof (ip_));
}

void ObAddr::set_port(int32_t port)
{
  port_ = port;
}

struct sockaddr_storage ObAddr::get_sockaddr() const
{
  struct sockaddr_storage sock_addr;
  memset(&sock_addr, 0, sizeof(struct sockaddr_storage));
  if (version_ == IPV4) {
    struct sockaddr_in in;
    memset(&in, 0, sizeof(struct sockaddr_in));
    in.sin_family = AF_INET;
    in.sin_port = (htons)(static_cast<uint16_t>(port_));
    in.sin_addr.s_addr = htonl(ip_.v4_);
    MEMCPY(&sock_addr, &in, sizeof(in));
  } else if (version_ == IPV6) {
    struct sockaddr_in6 in6;
    memset(&in6, 0, sizeof(struct sockaddr_in6));
    in6.sin6_family = AF_INET6;
    in6.sin6_port = (htons)(static_cast<uint16_t>(port_));
    MEMCPY(in6.sin6_addr.s6_addr, ip_.v6_, sizeof(ip_.v6_));
    MEMCPY(&sock_addr, &in6, sizeof(in6));
  }

  return sock_addr;
}

void ObAddr::set_sockaddr(const struct sockaddr &sock_addr)
{
  if (sock_addr.sa_family == AF_INET) {
    version_ = IPV4;
    const struct sockaddr_in &in = (struct sockaddr_in&)(sock_addr);
    port_ = ntohs(in.sin_port);
    ip_.v4_ = ntohl(in.sin_addr.s_addr);
  } else if (sock_addr.sa_family == AF_INET6) {
    version_ = IPV6;
    const struct sockaddr_in6 &in6 = (struct sockaddr_in6&)(sock_addr);
    port_ = ntohs(in6.sin6_port);
    MEMCPY(ip_.v6_, in6.sin6_addr.s6_addr, sizeof(ip_.v6_));
  }
}

bool ObAddr::set_ip_addr(const char *ip, const int32_t port)
{
  bool bret = false;
  const char *ch = strchr(ip, ':');
  if (NULL != ch) {
    bret = set_ipv6_addr(ip, port);
  } else {
    bret = set_ipv4_addr(ip, port);
  }
  return bret;
}

bool ObAddr::set_ipv6_addr(const uint64_t ipv6_high, const uint64_t ipv6_low, const int32_t port)
{
  bool ret = true;
  uint64_t *high = reinterpret_cast<uint64_t *>(&ip_.v6_[0]);
  uint64_t *low = reinterpret_cast<uint64_t *>(&ip_.v6_[8]);
  *high = ipv6_high;
  *low = ipv6_low;
  version_ = IPV6;
  port_= port;
  return ret;
}

OB_SERIALIZE_MEMBER(ObAddr, version_, ip_.v6_[0], ip_.v6_[1], ip_.v6_[2], ip_.v6_[3], port_);

} // end namespace common
} // end namespace oceanbase
