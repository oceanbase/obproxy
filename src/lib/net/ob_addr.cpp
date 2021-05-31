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
uint32_t ObAddr::convert_ipv4_addr(const char *ip)
{
  in_addr binary;
  int iret = 0;
  uint32_t result = 0;

  if (!OB_ISNULL(ip)) {
    memset(&binary, 0, sizeof (binary));
    iret = inet_pton(AF_INET, ip, &binary);
    if (iret == -1) {        // no support family
      binary.s_addr = 0;
    } else if (iret == 0) {  // invalid ip string
      binary.s_addr = 0;
    }
    result = ntohl(binary.s_addr);
  }
  return result;
}

int ObAddr::parse_from_cstring(const char *ipport)
{
  int ret = OB_SUCCESS;
  char buf[INET6_ADDRSTRLEN] = "";
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
      if (false == set_ipv4_addr(buf, port))
      {
        ret = OB_INVALID_ARGUMENT;
      }
    } else {              // IPV6 format
      const char *ipv6 = buf + 1;
      if (']' == buf[strlen(buf) - 1]) {
        buf[strlen(buf) - 1] = '\0';
        IGNORE_RETURN set_ipv6_addr(ipv6, port);
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
    }
    res = true;
  }
  return res;
}

int ObAddr::ip_port_to_string(char *buffer, const int32_t size) const
{
  int ret = OB_SUCCESS;
  if (NULL == buffer || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (version_ == IPV6) {
    ret = OB_NOT_SUPPORTED;
  } else {
    int ret_len = snprintf(buffer, size, "%d.%d.%d.%d:%d",
                           (ip_.v4_ >> 24) & 0XFF,
                           (ip_.v4_ >> 16) & 0xFF,
                           (ip_.v4_ >> 8) & 0xFF,
                           (ip_.v4_) & 0xFF,
                           port_);
    if (ret_len < 0) {
      ret = OB_ERR_SYS;
    } else if (ret_len >= size) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

bool ObAddr::set_ipv6_addr(const char *ip, const int32_t port)
{
  UNUSED(ip);
  UNUSED(port);
  _OB_LOG(WARN, "set ipv6 address is not complete");
  return false;
}


bool ObAddr::set_ipv4_addr(const char *ip, const int32_t port)
{
  bool ret = true;
  if (NULL == ip || port <= 0) {
    ret = false;
  } else {
    version_ = IPV4;
    ip_.v4_ = convert_ipv4_addr(ip);
    port_ = port;
  }
  return ret;
}

bool ObAddr::set_ipv4_addr(const ObString &ip, const int32_t port)
{
  bool ret = true;
  char ip_buf[OB_IP_STR_BUFF] = "";
  if (ip.length() >= OB_IP_STR_BUFF) {
    ret = false;
  } else {
    // ObString may be not terminated by '\0'
    MEMCPY(ip_buf, ip.ptr(), ip.length());
    ret = set_ipv4_addr(ip_buf, port);
  }
  return ret;
}

bool ObAddr::is_valid() const
{
  bool valid = true;
  if (port_ <= 0) {
    valid = false;
  } else {
    valid = (IPV4 == version_) && (0 != ip_.v4_);
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
  if ((version_ != rv.version_) || (IPV4 != version_)) {
    LOG_ERROR("comparision between different IP versions hasn't supported!");
  }
  int ipcmp = ip_.v4_ - rv.ip_.v4_;
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

OB_SERIALIZE_MEMBER(ObAddr, version_, ip_.v6_[0], ip_.v6_[1], ip_.v6_[2], ip_.v6_[3], port_);

} // end namespace common
} // end namespace oceanbase
