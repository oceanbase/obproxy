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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ifaddrs.h>
#include "iocore/net/ob_inet.h"
#include "iocore/net/ob_net_def.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

uint64_t ObIpEndpoint::hash(const uint64_t hash) const
{
  if (is_ip6()) {
    return murmurhash(&sin6_, sizeof(sin6_), hash);
  } else {
    return murmurhash(&sin_, sizeof(sin_), hash);
  }
}

int64_t ObIpEndpoint::to_string(char *buf, const int64_t buf_len) const
{
  char ip_buff[MAX_IP_ADDR_LENGTH];
  int64_t pos = 0;
  J_OBJ_START();
  if (is_ip6()) {
    databuff_printf(buf, buf_len, pos, "[%s]:%u",
                    ops_ip_ntop(*this, ip_buff, sizeof(ip_buff)),
                    ops_ip_port_host_order(*this));
  } else {
    databuff_printf(buf, buf_len, pos, "%s:%u",
                    ops_ip_ntop(*this, ip_buff, sizeof(ip_buff)),
                    ops_ip_port_host_order(*this));
  }
  J_OBJ_END();
  return pos;
}

int64_t ObIpEndpoint::to_plain_string(char *buf, const int64_t buf_len) const
{
  char ip_buff[MAX_IP_ADDR_LENGTH];
  int64_t pos = 0;
  if (is_ip6()) {
    databuff_printf(buf, buf_len, pos, "[%s]:%u",
                    ops_ip_ntop(*this, ip_buff, sizeof(ip_buff)),
                    ops_ip_port_host_order(*this));
  } else {
    databuff_printf(buf, buf_len, pos, "%s:%u",
                    ops_ip_ntop(*this, ip_buff, sizeof(ip_buff)),
                    ops_ip_port_host_order(*this));
  }
  return pos;
}

const char *ops_ip_ntop(const struct sockaddr &addr, char *dst, int64_t size)
{
  const char *zret = 0;
  switch (addr.sa_family) {
    case AF_INET:
      zret = inet_ntop(AF_INET, &ops_ip4_addr_cast(addr), dst, static_cast<uint32_t>(size));
      break;
    case AF_INET6:
      zret = inet_ntop(AF_INET6, &ops_ip6_addr_cast(addr), dst, static_cast<uint32_t>(size));
      break;
    default:
      zret = dst;
      int64_t ret_len = 0;
      ret_len = snprintf(dst, size, "*Not IP address [%u]*", addr.sa_family);
      if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= size)) {
        // do nothing
      }
      break;
  }
  return zret;
}

const char *ops_ip_family_name(const int family)
{
  return AF_INET == family ? "IPv4"
         : AF_INET6 == family ? "IPv6"
         : "Unspec";
}

int ops_ip_nptop(const sockaddr &addr, char *dst, const int64_t size)
{
  int ret = OB_SUCCESS;
  char buff[INET6_ADDRSTRLEN];
  if (OB_ISNULL(dst) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t ret_len = 0;
    ret_len = snprintf(dst, size, "%s:%u",
              ops_ip_ntop(addr, buff, sizeof(buff)),
              ops_ip_port_host_order(addr));
    if (OB_UNLIKELY(ret_len <= 0)
        || OB_UNLIKELY(ret_len >= size)) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ops_ip_parse(ObString &src, ObString &addr, ObString &port)
{
  addr.reset();
  port.reset();

  // Let's see if we can find out what's in the address string.
  if (src) {
    while (src && isspace(*src)) {
      ++src;
    }
    // Check for brackets.
    if ('[' == *src) {
      // Ugly. In a number of places we must use bracket notation
      // to support port numbers. Rather than mucking with that
      // everywhere, we'll tweak it here. Experimentally we can't
      // depend on getaddrinfo to handle it. Note that the text
      // buffer size includes space for the null, so a bracketed
      // address is at most that size - 1 + 2 -> size+1.
      //
      // It just gets better. In order to bind link local addresses
      // the scope_id must be set to the interface index. That's
      // most easily done by appending a %intf (where "intf" is the
      // name of the interface) to the address. Which makes
      // the address potentially larger than the standard maximum.
      // So we can't depend on that sizing.
      ++src; // skip bracket.
      addr = src.split_on(']');
      if (!addr.empty() && (':' == *src)) { // found the closing bracket and port colon
        ++src; // skip colon.
        port = src;
      } // else it's a fail for unclosed brackets.
    } else {
      // See if there's exactly 1 colon
      ObString tmp = src.after(':');

      if (!tmp.empty() && !tmp.find(':')) { // 1 colon and no others
        src.clip(tmp.ptr() - 1); // drop port from address.
        port = tmp;
      } // else 0 or > 1 colon and no brackets means no port.
      addr = src;
    }
    // clip port down to digits.
    if (!port.empty()) {
      const char *spot = port.ptr();

      while (isdigit(*spot)) {
        ++spot;
      }
      port.clip(spot);
    }
  }
  return !addr.empty() ? OB_SUCCESS : OB_ERROR; // true if we found an address.
}

int ops_ip_pton(const ObString &src, sockaddr &ip)
{
  int ret = OB_SUCCESS;
  ObString new_src = src;
  ObString addr;
  ObString port;

  ops_ip_invalidate(ip);
  if (OB_FAIL(ops_ip_parse(new_src, addr, port))) {
  } else if (addr.empty()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Copy if not terminated.
    char *tmp = static_cast<char *>(alloca(addr.length() + 1));
    MEMCPY(tmp, addr.ptr(), addr.length());
    tmp[addr.length()] = 0;
    addr.assign(tmp, addr.length());

    if (addr.find(':')) { // colon -> IPv6
      in6_addr addr6;
      if (1 == inet_pton(AF_INET6, addr.ptr(), &addr6)) {
        ops_ip6_set(ip, addr6);
      } else {
        ret = ob_get_sys_errno();
      }
    } else { // no colon -> must be IPv4
      in_addr addr4;
      if (1 == inet_aton(addr.ptr(), &addr4)) {
        ops_ip4_set(ip, addr4.s_addr);
      } else {
        ret = ob_get_sys_errno();
      }
    }
    // If we had a successful conversion, set the port.
    int32_t int_port = 0;
    if (ops_is_ip(ip)) {
        if ((!port.empty() && (int_port = atoi(port.ptr())) < 0)) {
          int_port = 0;
        }
      ops_ip_port_cast(ip) = (htons)(static_cast<uint16_t>(int_port));
    }
  }
  return ret;
}

int ops_ip_to_hex(const sockaddr &src, char *dst, int64_t len, int64_t &ret_len)
{
  int ret = OB_SUCCESS;
  ret_len = 0;
  uint8_t n1 = 0;
  uint8_t n0 = 0;
  char const *dst_limit = dst + len - 1; // reserve null space.

  if (OB_ISNULL(dst)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ops_is_ip(src)) {
    const uint8_t *data = ops_ip_addr8_cast(src);

    for (const uint8_t *src_limit = data + ops_ip_addr_size(src)
         ; data < src_limit && dst + 1 < dst_limit
         ; ++data, ret_len+=2
        ) {
      n1 = (*data >> 4) & 0xF; // high nybble.
      n0 = *data & 0xF; // low nybble.

      *dst++ = static_cast<uint8_t>(n1 > 9 ? n1 + 'A' - 10 : n1 + '0');
      *dst++ = static_cast<uint8_t>(n0 > 9 ? n0 + 'A' - 10 : n0 + '0');
    }
  } else {
    ret = OB_ERROR;
  }
  *dst = 0; // terminate but don't include that in the length.
  return ret;
}

sockaddr &ops_ip_set(sockaddr &dst, const ObIpAddr &addr, in_port_t port)
{
  if (AF_INET == addr.family_) {
    ops_ip4_set(dst, addr.addr_.ip4_, port);
  } else if (AF_INET6 == addr.family_) {
    ops_ip6_set(dst, addr.addr_.ip6_, port);
  } else {
    ops_ip_invalidate(dst);
  }
  return dst;
}

int ObIpAddr::load(const char *text)
{
  int ret = OB_SUCCESS;
  ObIpEndpoint ip;
  if (OB_FAIL(ops_ip_pton(text, ip))) {
  } else {
    *this = ip;
  }
  return ret;
}

int64_t ObIpAddr::to_string(char *dest, const int64_t len) const
{
  ObIpEndpoint ip;
  ip.assign(*this);
  return ip.to_string(dest, len);
}

const char *ObIpAddr::get_ip_str(char *buf, const int64_t len) const
{
  ObIpEndpoint ip;
  ip.assign(*this);
  ops_ip_ntop(ip, buf, len);
  return buf;
}

bool ObIpAddr::is_multicast() const
{
  return (AF_INET == family_ && 0xe == (addr_.byte_[0] >> 4)) ||
         (AF_INET6 == family_ && IN6_IS_ADDR_MULTICAST(&addr_.ip6_));
}

bool operator==(const ObIpAddr &lhs, const sockaddr &rhs)
{
  bool zret = false;
  if (lhs.family_ == rhs.sa_family) {
    if (AF_INET == lhs.family_) {
      zret = (lhs.addr_.ip4_ == ops_ip4_addr_cast(rhs));
    } else if (AF_INET6 == lhs.family_) {
      zret = (0 == MEMCMP(&lhs.addr_.ip6_, &ops_ip6_addr_cast(rhs), sizeof(in6_addr)));
    } else { // map all non-IP to the same thing.
      zret = true;
    }
  } // else different families, not equal.
  return zret;
}

int ops_ip_check_characters(ObString &text, int &family)
{
  int ret = OB_SUCCESS;
  bool found_colon = false;
  bool found_hex = false;
  family = 0;

  if (OB_ISNULL(text.ptr()) || OB_UNLIKELY(text.length() <=0 )) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (char const *p = text.ptr(), *limit = p + text.length()
         ; p < limit
         ; ++p
        )
    {
      if (':' == *p) {
        found_colon = true;
      } else if ('.' == *p || isdigit(*p)) {
        /* empty */;
      } else if (isxdigit(*p)) {
        found_hex = true;
      }
    }
  }

  family = found_hex && !found_colon ? AF_UNSPEC
         : found_colon ? AF_INET6
         : AF_INET;
  return ret;
}


int get_local_addr_list(ObIpAddr *ipaddr, const int64_t total_cnt,
                        bool need_all, int64_t &valid_cnt)
{
  int ret = OB_SUCCESS;
  char hname[MAX_HOST_NAME_LEN];
  hname[0] = '\0';
  valid_cnt = 0;

  if (OB_ISNULL(ipaddr) || OB_UNLIKELY(total_cnt <=0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(::gethostname(hname, MAX_HOST_NAME_LEN))) {
    ret = ob_get_sys_errno();
  } else  {
    struct addrinfo *addrs = NULL;
    struct addrinfo hints;

    bzero(&hints, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (OB_FAIL(::getaddrinfo(hname, NULL, &hints, &addrs))) {
      ret = ob_get_sys_errno();
      addrs = NULL;
    } else {
      struct addrinfo *curr = NULL;
      struct in_addr *addr = NULL;
      for (curr = addrs; NULL != curr; curr = curr->ai_next) {
        if (NULL != curr->ai_addr) {
          addr = &((reinterpret_cast<struct sockaddr_in *>(curr->ai_addr))->sin_addr);
          if ((NULL != addr) && (need_all || (addr->s_addr != htonl(INADDR_LOOPBACK) && addr->s_addr != htonl(INADDR_ANY)))) {
            ipaddr[valid_cnt] = addr->s_addr;
            ++valid_cnt;
          }
        }
      }
    }

    if (NULL != addrs) {
      freeaddrinfo(addrs);
    }
  }

  return ret;
}

int get_local_addr_list_by_nic(ObIpAddr *ipaddr, const int64_t total_cnt, const bool need_all, int64_t &valid_cnt)
{
  int ret = OB_SUCCESS;
  valid_cnt = 0;

  struct ifaddrs *if_addr = NULL;
  struct in_addr *addr = NULL;

  if (-1 == ::getifaddrs(&if_addr)) {
    ret = ob_get_sys_errno();
    if_addr = NULL;
  } else {
    struct ifaddrs *if_addr_struct = if_addr;
    while ((NULL != if_addr_struct) && (valid_cnt < total_cnt)) {
      if (AF_INET == if_addr_struct->ifa_addr->sa_family) { // check it is IP4
        // is a valid IP4 Address
        addr = &((reinterpret_cast<struct sockaddr_in *>(if_addr_struct->ifa_addr))->sin_addr);
        if ((NULL != addr) && (need_all || (addr->s_addr != htonl(INADDR_LOOPBACK) && addr->s_addr != htonl(INADDR_ANY)))) {
          ipaddr[valid_cnt] = addr->s_addr;
          ++valid_cnt;
        }
      } else if (AF_INET6 == if_addr_struct->ifa_addr->sa_family) { // check it is IP6
        // is a valid IP6 Address
        // add later
      }
      if_addr_struct = if_addr_struct->ifa_next;
    }
  }

  if (NULL != if_addr) {
    freeifaddrs(if_addr);
  }

  return ret;
}

int get_addr_by_host(char *hname, ObIpAddr *ipaddr, const int64_t total_cnt,
                     bool need_all, int64_t &valid_cnt)
{
  int ret = OB_SUCCESS;

  struct addrinfo *addrs = NULL;
  struct addrinfo hints;
  valid_cnt = 0;

  bzero(&hints, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if (OB_FAIL(::getaddrinfo(hname, NULL, &hints, &addrs))) {
    ret = ob_get_sys_errno();
    addrs = NULL;
  } else {
    struct addrinfo *curr = NULL;
    struct in_addr *addr = NULL;
    for (curr = addrs; NULL != curr; curr = curr->ai_next) {
      if (NULL != curr->ai_addr) {
        addr = &((reinterpret_cast<struct sockaddr_in *>(curr->ai_addr))->sin_addr);
        if (valid_cnt < total_cnt && (NULL != addr)
            && (need_all || (addr->s_addr != htonl(INADDR_LOOPBACK) && addr->s_addr != htonl(INADDR_ANY)))) {
          ipaddr[valid_cnt] = addr->s_addr;
          ++valid_cnt;
        }
      }
    }
  }

  if (NULL != addrs) {
    freeaddrinfo(addrs);
  }

  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
