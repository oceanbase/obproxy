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
 *
 */

#ifndef OBPROXY_INET_H
#define OBPROXY_INET_H

#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

#if ! OB_HAS_IN6_IS_ADDR_UNSPECIFIED
#if defined(IN6_IS_ADDR_UNSPECIFIED)
#undef IN6_IS_ADDR_UNSPECIFIED
#endif
static inline bool IN6_IS_ADDR_UNSPECIFIED(const in6_addr *addr)
{
  const uint64_t *w = reinterpret_cast<const uint64_t *>(addr);
  return 0 == w[0] && 0 == w[1];
}
#endif

// Buffer size sufficient for IPv6 address and port.
static const int64_t INET6_ADDRPORTSTRLEN      = INET6_ADDRSTRLEN + 6;
// Convenience type for address formatting.
typedef char ip_text_buffer[INET6_ADDRSTRLEN];
typedef char ip_port_text_buffer[INET6_ADDRPORTSTRLEN];

static const int64_t MAX_HOST_NAME_LEN         = 256;
static const int64_t MAX_LOCAL_ADDR_LIST_COUNT = 64;

// Size in bytes of an IPv6 address.
static const int64_t IP6_SIZE                  = sizeof(in6_addr);

struct ObIpAddr;

// A union to hold the standard IP address structures.
// By standard we mean sockaddr compliant.
//
// We use the term "endpoint" because these contain more than just the
// raw address, all of the data for an IP endpoint is present.
//
// @internal This might be useful to promote to avoid strict aliasing
// problems.  Experiment with it here to see how it works in the
// field.
//
// @internal sockaddr_storage is not present because it is so
// large and the benefits of including it are small. Use of this
// structure will make it easy to add if that becomes necessary.
union ObIpEndpoint
{
  ObIpEndpoint() { memset(this, 0, sizeof(ObIpEndpoint)); }
  ~ObIpEndpoint() {}

  void reset() { memset(this, 0, sizeof(ObIpEndpoint)); }
  explicit ObIpEndpoint(const sockaddr &addr) {
    memset(this, 0, sizeof(ObIpEndpoint));
    assign(addr);
  }
  ObIpEndpoint (const ObIpEndpoint &point) { MEMCPY(this, &point, sizeof(ObIpEndpoint)); }
  void operator= (const ObIpEndpoint &point) { MEMCPY(this, &point, sizeof(ObIpEndpoint)); }

  ObIpEndpoint &assign(const sockaddr &ip);

  ObIpEndpoint &assign(const ObIpAddr &addr, const in_port_t port = 0);
  // Test for valid IP address.
  bool is_valid() const;

  // Test for IPv4.
  bool is_ip4() const;

  // Test for IPv6.
  bool is_ip6() const;

  uint16_t family() const;
  // Set to be any address for family family.
  // family must be AF_INET or AF_INET6.
  // @return This object.
  // family: Address family.
  ObIpEndpoint &set_to_any_addr(const int family);

  // Set to be loopback for family family.
  // family must be AF_INET or AF_INET6.
  // @return This object.
  // family: Address family.
  ObIpEndpoint &set_to_loopback(const int family);

  // Port in network order.
  in_port_t &port();

  // Port in network order.
  in_port_t port() const;

  int64_t get_port_host_order() const;
  uint32_t get_ip4_host_order() const;

  uint64_t hash(const uint64_t hash = 0) const { return common::murmurhash(&sa_, sizeof(sockaddr), hash); }

  int64_t to_string(char *buffer, const int64_t size) const;

  struct sockaddr sa_; // Generic address.
  struct sockaddr_in sin_; // IPv4
  struct sockaddr_in6 sin6_; // IPv6
};

// Reset an address to invalid.
// @note Useful for marking a member as not yet set.
inline void ops_ip_invalidate(sockaddr &addr)
{
  addr.sa_family = AF_UNSPEC;
}

inline void ops_ip_invalidate(sockaddr_in6 &addr)
{
  addr.sin6_family = AF_UNSPEC;
}

inline void ops_ip_invalidate(ObIpEndpoint &ip)
{
  ip.sa_.sa_family = AF_UNSPEC;
}

// Test for IP protocol.
// @return true if the address is IP, false otherwise.
inline bool ops_is_ip(const sockaddr &addr)
{
  return (AF_INET == addr.sa_family || AF_INET6 == addr.sa_family);
}

inline bool ops_is_ip(const ObIpEndpoint &addr)
{
  return (AF_INET == addr.sa_.sa_family || AF_INET6 == addr.sa_.sa_family);
}

inline bool ops_is_ip(int family)
{
  return (AF_INET == family || AF_INET6 == family);
}

// Test for IPv4 protocol.
// @return true if the address is IPv4, false otherwise.
inline bool ops_is_ip4(const sockaddr &addr)
{
  return AF_INET == addr.sa_family;
}

inline bool ops_is_ip4(const ObIpEndpoint &addr)
{
  return AF_INET == addr.sa_.sa_family;
}

// Test for IPv6 protocol.
// @return true if the address is IPv6, false otherwise.
inline bool ops_is_ip6(const sockaddr &addr)
{
  return AF_INET6 == addr.sa_family;
}

inline bool ops_is_ip6(const ObIpEndpoint &addr)
{
  return AF_INET6 == addr.sa_.sa_family;
}

// @return true if the address families are compatible.
inline bool ops_ip_are_compatible(const sockaddr &lhs, const sockaddr &rhs)
{
  return lhs.sa_family == rhs.sa_family;
}

inline bool ops_ip_are_compatible(const ObIpEndpoint &lhs, const ObIpEndpoint &rhs)
{
  return ops_ip_are_compatible(lhs.sa_, rhs.sa_);
}

// lhs: Address family to test.
inline bool ops_ip_are_compatible(const int lhs, const sockaddr &rhs)
{
  return lhs == rhs.sa_family;
}

// rhs: Family to test.
inline bool ops_ip_are_compatible(const sockaddr &lhs, const int rhs)
{
  return lhs.sa_family == rhs;
}

// IP address casting.
// sa_cast to cast to sockaddr*.
// ss_cast to cast to sockaddr_storage*.
// ip4_cast converts to sockaddr_in (because that's effectively an IPv4 addr).
// ip6_cast converts to sockaddr_in6

// sockaddr_storage -> sockaddr
inline sockaddr &ops_ip_sa_cast(sockaddr_storage &a)
{
  return *static_cast<struct sockaddr *>(static_cast<void *>(&a));
}

inline const sockaddr &ops_ip_sa_cast(const sockaddr_storage &a)
{
  return *static_cast<const sockaddr *>(static_cast<const void *>(&a));
}

// sockaddr_in -> sockaddr
inline sockaddr &ops_ip_sa_cast(sockaddr_in &a)
{
  return *static_cast<struct sockaddr *>(static_cast<void *>(&a));
}

inline const sockaddr &ops_ip_sa_cast(const sockaddr_in &a)
{
  return *static_cast<const sockaddr *>(static_cast<const void *>(&a));
}

// sockaddr_in6 -> sockaddr
inline sockaddr &ops_ip_sa_cast(sockaddr_in6 &a)
{
  return *static_cast<struct sockaddr *>(static_cast<void *>(&a));
}

inline const sockaddr &ops_ip_sa_cast(const sockaddr_in6 &a)
{
  return *static_cast<const sockaddr *>(static_cast<const void *>(&a));
}

// sockaddr -> sockaddr_storage
inline sockaddr_storage &ops_ip_ss_cast(sockaddr &a)
{
  return *static_cast<struct sockaddr_storage *>(static_cast<void *>(&a));
}

inline const sockaddr_storage &ops_ip_ss_cast(const sockaddr &a)
{
  return *static_cast<const sockaddr_storage *>(static_cast<const void *>(&a));
}

// sockaddr -> sockaddr_in
inline sockaddr_in &ops_ip4_cast(sockaddr &a)
{
  return *static_cast<struct sockaddr_in *>(static_cast<void *>(&a));
}

inline const sockaddr_in &ops_ip4_cast(const sockaddr &a)
{
  return *static_cast<const sockaddr_in *>(static_cast<const void *>(&a));
}

// sockaddr -> sockaddr_in6
inline sockaddr_in6 &ops_ip6_cast(sockaddr &a)
{
  return *static_cast<sockaddr_in6 *>(static_cast<void *>(&a));
}

inline const sockaddr_in6 &ops_ip6_cast(const sockaddr &a)
{
  return *static_cast<const sockaddr_in6 *>(static_cast<const void *>(&a));
}

// sockaddr_in6 -> sockaddr_in
inline sockaddr_in &ops_ip4_cast(sockaddr_in6 &a)
{
  return *static_cast<sockaddr_in *>(static_cast<void *>(&a));
}

inline const sockaddr_in &ops_ip4_cast(const sockaddr_in6 &a)
{
  return *static_cast<const sockaddr_in *>(static_cast<const void *>(&a));
}

// @return The sockaddr size for the family of addr.
inline int64_t ops_ip_size(const sockaddr &addr)
{
  return AF_INET == addr.sa_family ? sizeof(sockaddr_in)
         : AF_INET6 == addr.sa_family ? sizeof(sockaddr_in6)
         : 0 ;
}
inline int64_t ops_ip_size(const ObIpEndpoint &addr)
{
  return AF_INET == addr.sa_.sa_family ? sizeof(sockaddr_in)
         : AF_INET6 == addr.sa_.sa_family ? sizeof(sockaddr_in6)
         : 0 ;
}

// @return The size of the IP address only.
inline int64_t ops_ip_addr_size(const sockaddr &addr)
{
  return AF_INET == addr.sa_family ? sizeof(in_addr_t)
         : AF_INET6 == addr.sa_family ? sizeof(in6_addr)
         : 0 ;
}

inline int64_t ops_ip_addr_size(const ObIpEndpoint &addr)
{
  return AF_INET == addr.sa_.sa_family ? sizeof(in_addr_t)
         : AF_INET6 == addr.sa_.sa_family ? sizeof(in6_addr)
         : 0 ;
}

// Get a reference to the port in an address.
// @note Because this is direct access, the port value is in network order.
// @see ops_ip_port_host_order.
// @return A reference to the port value in an IPv4 or IPv6 address.
// @internal This is primarily for internal use but it might be handy for
// clients so it is exposed.
inline in_port_t& ops_ip_port_cast(sockaddr &sa)
{
  static in_port_t dummy = 0;
  return ops_is_ip4(sa)
         ? ops_ip4_cast(sa).sin_port
         : ops_is_ip6(sa)
         ? ops_ip6_cast(sa).sin6_port
         : (dummy = 0) ;
}

inline const in_port_t &ops_ip_port_cast(const sockaddr &sa)
{
  return ops_ip_port_cast(const_cast<sockaddr &>(sa));
}

inline const in_port_t &ops_ip_port_cast(const ObIpEndpoint &ip)
{
  return ops_ip_port_cast(const_cast<sockaddr &>(ip.sa_));
}

inline in_port_t &ops_ip_port_cast(ObIpEndpoint &ip)
{
  return ops_ip_port_cast(ip.sa_);
}

// Access the IPv4 address.
//
// If this is not an IPv4 address a zero valued address is returned.
// @note This is direct access to the address so it will be in
// network order.
//
// @return A reference to the IPv4 address in addr.
inline in_addr_t &ops_ip4_addr_cast(sockaddr &addr)
{
  static in_addr_t dummy = 0;
  return ops_is_ip4(addr)
         ? ops_ip4_cast(addr).sin_addr.s_addr
         : (dummy = 0) ;
}

inline const in_addr_t &ops_ip4_addr_cast(const sockaddr &addr)
{
  static in_addr_t dummy = 0;
  return ops_is_ip4(addr)
         ? ops_ip4_cast(addr).sin_addr.s_addr
         : static_cast<const in_addr_t &>(dummy = 0) ;
}

inline in_addr_t &ops_ip4_addr_cast(ObIpEndpoint &ip)
{
  return ops_ip4_addr_cast(ip.sa_);
}

inline const in_addr_t &ops_ip4_addr_cast(const ObIpEndpoint &ip)
{
  return ops_ip4_addr_cast(ip.sa_);
}

// Access the IPv6 address.
//
// If this is not an IPv6 address a zero valued address is returned.
// @note This is direct access to the address so it will be in
// network order.
//
// @return A reference to the IPv6 address in addr.
inline in6_addr &ops_ip6_addr_cast(sockaddr &addr)
{
  return ops_ip6_cast(addr).sin6_addr;
}

inline const in6_addr &ops_ip6_addr_cast(const sockaddr &addr)
{
  return ops_ip6_cast(addr).sin6_addr;
}

inline in6_addr &ops_ip6_addr_cast(ObIpEndpoint &ip)
{
  return ip.sin6_.sin6_addr;
}

inline const in6_addr &ops_ip6_addr_cast(const ObIpEndpoint &ip)
{
  return ip.sin6_.sin6_addr;
}

// Cast an IP address to an array of uint32_t.
// @note The size of the array is dependent on the address type which
// must be checked independently of this function.
// @return A pointer to the address information in addr or NULL
// if addr is not an IP address.
inline uint32_t *ops_ip_addr32_cast(sockaddr &addr)
{
  uint32_t *zret = 0;
  switch (addr.sa_family) {
    case AF_INET:
      zret = reinterpret_cast<uint32_t *>(&ops_ip4_addr_cast(addr));
      break;
    case AF_INET6:
      zret = reinterpret_cast<uint32_t *>(&ops_ip6_addr_cast(addr));
      break;
  }
  return zret;
}

inline const uint32_t *ops_ip_addr32_cast(const sockaddr &addr)
{
  return ops_ip_addr32_cast(const_cast<sockaddr &>(addr));
}

// Cast an IP address to an array of uint8_t.
// @note The size of the array is dependent on the address type which
// must be checked independently of this function.
// @return A pointer to the address information in addr or NULL
// if addr is not an IP address.
// @see ops_ip_addr_size
inline uint8_t *ops_ip_addr8_cast(sockaddr &addr)
{
  uint8_t *zret = 0;
  switch (addr.sa_family) {
    case AF_INET:
      zret = reinterpret_cast<uint8_t *>(&ops_ip4_addr_cast(addr));
      break;
    case AF_INET6:
      zret = reinterpret_cast<uint8_t *>(&ops_ip6_addr_cast(addr));
      break;
  }
  return zret;
}

inline const uint8_t *ops_ip_addr8_cast(const sockaddr &addr)
{
  return ops_ip_addr8_cast(const_cast<sockaddr &>(addr));
}

inline uint8_t *ops_ip_addr8_cast(ObIpEndpoint &ip)
{
  return ops_ip_addr8_cast(ip.sa_);
}

inline const uint8_t *ops_ip_addr8_cast(const ObIpEndpoint &ip)
{
  return ops_ip_addr8_cast(ip.sa_);
}

// Check for loopback.
// @return true if this is an IP loopback address, false otherwise.
inline bool ops_is_ip_loopback(const sockaddr &ip)
{
  return (AF_INET == ip.sa_family && 0x7F == ops_ip_addr8_cast(ip)[0])
         || (AF_INET6 == ip.sa_family && IN6_IS_ADDR_LOOPBACK(&ops_ip6_addr_cast(ip)));
}

inline bool ops_is_ip_loopback(const ObIpEndpoint &ip)
{
  return ops_is_ip_loopback(ip.sa_);
}

// Check for multicast.
// @return @true if ip is multicast.
inline bool ops_is_ip_multicast(const sockaddr &ip)
{
  return (AF_INET == ip.sa_family && 0xe == *ops_ip_addr8_cast(ip))
         || (AF_INET6 == ip.sa_family && IN6_IS_ADDR_MULTICAST(&ops_ip6_addr_cast(ip)));
}

inline bool ops_is_ip_multicast(const ObIpEndpoint &ip)
{
  return ops_is_ip_multicast(ip.sa_);
}

// Check for Private.
// @return @true if ip is private.
inline bool ops_is_ip_private(const sockaddr &ip)
{
  bool zret = false;
  if (ops_is_ip4(ip)) {
    in_addr_t a = ops_ip4_addr_cast(ip);
    zret = ((a & htonl(0xFFFF0000)) == htonl(0xC0A80000));  // 192.168.0.0/16
  } else if (ops_is_ip6(ip)) {
    in6_addr a = ops_ip6_addr_cast(ip);
    zret = ((a.s6_addr[0] & 0xFE) == 0xFC); // fc00::/7
  }
  return zret;
}

inline bool ops_is_ip_private(const ObIpEndpoint &ip)
{
  return ops_is_ip_private(ip.sa_);
}

// Check for being "any" address.
// @return true if ip is the any / unspecified address.
inline bool ops_is_ip_any(const sockaddr &ip)
{
  return (ops_is_ip4(ip) && INADDR_ANY == ops_ip4_addr_cast(ip))
         || (ops_is_ip6(ip) && IN6_IS_ADDR_UNSPECIFIED(&ops_ip6_addr_cast(ip)));
}

// @name Address operators

// Copy the address from src to dst if it's IP.
// This attempts to do a minimal copy based on the type of src.
// If src is not an IP address type it is @b not copied and
// dst is marked as invalid.
// @return true if src was an IP address, false otherwise.
// dst: Destination object.
// src: Source object.
inline bool ops_ip_copy(sockaddr &dst, const sockaddr &src)
{
  int64_t n = 0;
  int64_t n2 = 0;
  switch (src.sa_family) {
    case AF_INET:
      n = sizeof(sockaddr_in);
      break;

    case AF_INET6:
      n = sizeof(sockaddr_in6);
      break;
  }
  switch (dst.sa_family) {
    case AF_INET:
      n2 = sizeof(sockaddr_in);
      break;

    case AF_INET6:
      n2 = sizeof(sockaddr_in6);
      break;

    default:
      n2 = sizeof(sockaddr_in);
      break;
  }
  if (n && n <= n2) {
    MEMCPY(&dst, &src, n);
#if HAVE_STRUCT_SOCKADDR_SA_LEN
    dst.sa_len = n;
#endif
  } else {
    ops_ip_invalidate(dst);
  }
  return n != 0;
}

// ops_ip_copy: Copy ip and port to dst.
// ip: host ip network host order
// port: host port network host order
inline bool ops_ip_copy(sockaddr &dst, const uint32_t ip, const uint16_t port)
{
  struct sockaddr_in sock_addr;
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = (htons)(port);
  sock_addr.sin_addr.s_addr = htonl(ip);
  memset(&sock_addr.sin_zero, 0, 8);
  return ops_ip_copy(dst, ops_ip_sa_cast(sock_addr));
}

// ops_ip_copy: Copy ip and port to dst.
// ip: host ip network host order
// port: host port network host order
inline bool ops_ip_copy(ObIpEndpoint &dst, const uint32_t ip, const uint16_t port)
{
  return ops_ip_copy(dst.sa_, ip, port);
}

inline bool ops_ip_copy(ObIpEndpoint &dst,const sockaddr &src)
{
  return ops_ip_copy(dst.sa_, src);
}

inline bool ops_ip_copy(ObIpEndpoint &dst, const ObIpEndpoint &src)
{
  return ops_ip_copy(dst.sa_, src.sa_);
}

inline bool ops_ip_copy(sockaddr &dst, const ObIpEndpoint &src)
{
  return ops_ip_copy(dst, src.sa_);
}

// Compare two addresses.
// This is useful for IPv4, IPv6, and the unspecified address type.
// If the addresses are of different types they are ordered
//
// Non-IP < IPv4 < IPv6
//
//  - all non-IP addresses are the same ( including AF_UNSPEC )
//  - IPv4 addresses are compared numerically (host order)
//  - IPv6 addresses are compared byte wise in network order (MSB to LSB)
//
// @return
//   - -1 if lhs is less than rhs.
//   - 0 if lhs is identical to rhs.
//   - 1 if lhs is greater than rhs.
//
// @internal This looks like a lot of code for an inline but I think it
// should compile down to something reasonable.
inline int ops_ip_addr_cmp(const sockaddr &lhs, const sockaddr &rhs)
{
  int zret = 0;
  uint16_t rtype = rhs.sa_family;
  uint16_t ltype = lhs.sa_family;

  // We lump all non-IP addresses into a single equivalence class
  // that is less than an IP address. This includes AF_UNSPEC.
  if (AF_INET == ltype) {
    if (AF_INET == rtype) {
      in_addr_t la = ntohl(ops_ip4_cast(lhs).sin_addr.s_addr);
      in_addr_t ra = ntohl(ops_ip4_cast(rhs).sin_addr.s_addr);

      if (la < ra) {
        zret = -1;
      } else if (la > ra) {
        zret = 1;
      } else {
        zret = 0;
      }
    } else if (AF_INET6 == rtype) { // IPv4 < IPv6
      zret = -1;
    } else { // IP > not IP
      zret = 1;
    }
  } else if (AF_INET6 == ltype) {
    if (AF_INET6 == rtype) {
      const sockaddr_in6 lhs_in6 = ops_ip6_cast(lhs);

      zret = MEMCMP(&lhs_in6.sin6_addr,
                    &ops_ip6_cast(rhs).sin6_addr,
                    sizeof(lhs_in6.sin6_addr));
    } else {
      zret = 1; // IPv6 greater than any other type.
    }
  } else if (AF_INET == rtype || AF_INET6 == rtype) {
    // ltype is non-IP so it's less than either IP type.
    zret = -1;
  } else {
    // Both types are non-IP so they're equal.
    zret = 0;
  }
  return zret;
}

inline int ops_ip_addr_cmp(const ObIpEndpoint &lhs, const ObIpEndpoint &rhs)
{
  return ops_ip_addr_cmp(lhs.sa_, rhs.sa_);
}

// Check if two addresses are equal.
// @return true if lhs and rhs point to equal addresses,
// false otherwise.
inline bool ops_ip_addr_eq(const sockaddr &lhs, const sockaddr &rhs)
{
  return 0 == ops_ip_addr_cmp(lhs, rhs);
}

inline bool ops_ip_addr_eq(const ObIpEndpoint &lhs, const ObIpEndpoint &rhs)
{
  return 0 == ops_ip_addr_cmp(lhs.sa_, rhs.sa_);
}

// Compare address and port for equality.
inline bool ops_ip_addr_port_eq(const sockaddr &lhs, const sockaddr &rhs)
{
  bool zret = false;
  if (lhs.sa_family == rhs.sa_family && ops_ip_port_cast(lhs) == ops_ip_port_cast(rhs)) {
    if (AF_INET == lhs.sa_family) {
      zret = ops_ip4_cast(lhs).sin_addr.s_addr == ops_ip4_cast(rhs).sin_addr.s_addr;
    } else if (AF_INET6 == lhs.sa_family) {
      zret = 0 == MEMCMP(&ops_ip6_cast(lhs).sin6_addr, &ops_ip6_cast(rhs).sin6_addr, sizeof(in6_addr));
    }
  }
  return zret;
}

inline bool ops_ip_addr_port_eq(const ObIpEndpoint &lhs, const sockaddr &rhs)
{
  return ops_ip_addr_port_eq(lhs.sa_, rhs);
}

inline bool operator==(const ObIpEndpoint &lhs, const ObIpEndpoint &rhs)
{
  return ops_ip_addr_port_eq(lhs.sa_, rhs.sa_);
}

inline bool operator!=(const ObIpEndpoint &lhs, const ObIpEndpoint &rhs)
{
  return !ops_ip_addr_port_eq(lhs.sa_, rhs.sa_);
}

// port int64_t -> in_port_t
inline in_port_t ops_port_host_order(const int64_t port)
{
  return (ntohs)(static_cast<uint16_t>(port));
}

inline in_port_t ops_port_net_order(const int64_t port)
{
  return (htons)(static_cast<uint16_t>(port));
}

// Get IP TCP/UDP port.
// @return The port in host order for an IPv4 or IPv6 address,
// or zero if neither.
// addr: Address with port.
inline in_port_t ops_ip_port_host_order(const sockaddr &addr)
{
  // We can discard the const because this function returns
  // by value.
  return (ntohs)(ops_ip_port_cast(const_cast<sockaddr &>(addr)));
}

inline in_port_t ops_ip_port_host_order(const ObIpEndpoint &ip)
{
  // We can discard the const because this function returns
  // by value.
  return (ntohs)(ops_ip_port_cast(const_cast<sockaddr &>(ip.sa_)));
}

// Extract the IPv4 address.
// @return Host order IPv4 address.
inline in_addr_t ops_ip4_addr_host_order(const ObIpEndpoint &ip)
{
  return ntohl(ops_ip4_addr_cast(ip.sa_));
}

inline in_addr_t ops_ip4_addr_host_order(const sockaddr &addr)
{
  return ntohl(ops_ip4_addr_cast(const_cast<sockaddr &>(addr)));
}

// Write IPv4 data to storage dst.
// dst: Destination storage.
// addr: address, IPv4 network order.
// port: port, network order.
inline sockaddr &ops_ip4_set(sockaddr_in &dst, const in_addr_t addr, const in_port_t port = 0)
{
  ob_zero(dst);
#if HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
  dst.sin_len = sizeof(sockaddr_in);
#endif
  dst.sin_family = AF_INET;
  dst.sin_addr.s_addr = addr;
  dst.sin_port = port;
  return ops_ip_sa_cast(dst);
}

inline sockaddr &ops_ip4_set(ObIpEndpoint &dst, const in_addr_t ip4, const in_port_t port = 0)
{
  return ops_ip4_set(dst.sin_, ip4, port);
}

inline sockaddr &ops_ip4_set(sockaddr &dst, const in_addr_t ip4, const in_port_t port = 0)
{
  return ops_ip4_set(ops_ip4_cast(dst), ip4, port);
}

// Write IPv6 data to storage dst.
// @return dst cast to sockaddr*.
// dst: Destination storage.
// addr: address in network order.
// port: Port, network order.
inline sockaddr &ops_ip6_set(sockaddr_in6 &dst, const in6_addr &addr, const in_port_t port = 0)
{
  ob_zero(dst);
#if HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
  dst.sin6_len = sizeof(sockaddr_in6);
#endif
  dst.sin6_family = AF_INET6;
  MEMCPY(&dst.sin6_addr, &addr, sizeof (addr));
  dst.sin6_port = port;
  return ops_ip_sa_cast(dst);
}

inline sockaddr &ops_ip6_set(ObIpEndpoint &dst, const in6_addr &addr, const in_port_t port = 0)
{
  return ops_ip6_set(dst.sin6_, addr, port);
}

inline sockaddr &ops_ip6_set(sockaddr &dst, const in6_addr &addr, const in_port_t port = 0)
{
  return ops_ip6_set(ops_ip6_cast(dst), addr, port);
}

// Storage for an IP address.
// In some cases we want to store just the address and not the
// ancillary information (such as port, or flow data).
// @note This is not easily used as an address for system calls.
struct ObIpAddr
{
  // Default construct (invalid address).
  ObIpAddr() : family_(AF_UNSPEC) { }
  ~ObIpAddr() { }

  // Construct as IPv4 addr.
  explicit ObIpAddr(in_addr_t addr) : family_(AF_INET)
  {
    addr_.ip4_ = addr;
  }

  // Construct as IPv6 addr.
  explicit ObIpAddr(const in6_addr &addr) : family_(AF_INET6)
  {
    addr_.ip6_ = addr;
  }

  // Construct from sockaddr.
  explicit ObIpAddr(const sockaddr &addr) { assign(addr); }

  // Construct from sockaddr_in6.
  explicit ObIpAddr(const sockaddr_in6 &addr) { assign(ops_ip_sa_cast(addr)); }

  // Construct from ObIpEndpoint.
  explicit ObIpAddr(const ObIpEndpoint &addr) { assign(addr.sa_); }

  // Assign sockaddr storage, addr May be NULL
  ObIpAddr &assign(const sockaddr &addr);

  // Assign
  ObIpAddr &operator=(const ObIpEndpoint &ip) { return assign(ip.sa_); }
  ObIpAddr &operator=(in_addr_t ip);
  ObIpAddr &operator=(const in6_addr &ip);

  bool operator < (const ObIpAddr &rv) const;

  // Load from string.
  // The address is copied to this object if the conversion is successful,
  // otherwise this object is invalidated.
  // @return 0 on success, non-zero on failure.
  // str: Null terminated input string.
  int load(const char *str);

  // Output to a string.
  // @return pos.
  // dest: [out] Destination string buffer
  // len: [in] Size of buffer.
  int64_t to_string(char *dest, const int64_t len) const;
  const char *get_ip_str(char *buf, const int64_t len) const;

  // Equality.
  bool operator==(const ObIpAddr &that) const
  {
    return AF_INET == family_
           ? (AF_INET == that.family_ && addr_.ip4_ == that.addr_.ip4_)
           : AF_INET6 == family_
           ? (AF_INET6 == that.family_
              && 0 == MEMCMP(&addr_.ip6_, &that.addr_.ip6_, IP6_SIZE)
             )
           : (AF_UNSPEC == family_  && AF_UNSPEC == that.family_);
  }

  bool operator!=(const ObIpAddr &that) { return !(*this == that); }

  // Test for same address family.
  // return true if that is the same address family as this.
  bool is_compatible_with(const ObIpAddr &that);

  // Get the address family.
  // @return The address family.
  uint16_t family() const;

  // Test for IPv4.
  bool is_ip4() const;

  // Test for IPv6.
  bool is_ip6() const;

  // Test for validity.
  bool is_valid() const { return AF_INET == family_ || AF_INET6 == family_; }

  // Make invalid.
  ObIpAddr &invalidate() { family_ = AF_UNSPEC; return *this; }

  // Test for multicast
  bool is_multicast() const;

  uint16_t family_; // Protocol family.

  // Address data.
  union
  {
    in_addr_t ip4_;           // IPv4 address storage.
    in6_addr ip6_;            // IPv6 address storage.
    uint8_t byte_[IP6_SIZE];  // As raw bytes.
  } addr_;
};

inline bool ObIpAddr::operator < (const ObIpAddr &rv) const
{
  bool bret = false;
  if (is_ip4() && rv.is_ip4()) {
    bret = (0 < MEMCMP(&addr_.ip4_, &rv.addr_.ip4_, sizeof(in_addr_t)));
  } else if (is_ip6() && rv.is_ip6()){
    bret = (0 < MEMCMP(&addr_.ip6_, &rv.addr_.ip6_, sizeof(in6_addr)));
  }
  return bret;
}

inline ObIpAddr& ObIpAddr::operator = (in_addr_t ip)
{
  family_ = AF_INET;
  addr_.ip4_ = ip;
  return *this;
}

inline ObIpAddr& ObIpAddr::operator = (const in6_addr &ip)
{
  family_ = AF_INET6;
  addr_.ip6_ = ip;
  return *this;
}

inline uint16_t ObIpAddr::family() const { return family_; }

inline bool ObIpAddr::is_compatible_with(const ObIpAddr &that)
{
  return is_valid() && family_ == that.family_;
}

inline bool ObIpAddr::is_ip4() const { return AF_INET == family_; }

inline bool ObIpAddr::is_ip6() const { return AF_INET6 == family_; }

// Assign sockaddr storage.
inline ObIpAddr& ObIpAddr::assign(const sockaddr &addr)
{
  family_ = addr.sa_family;
  if (ops_is_ip4(addr)) {
    addr_.ip4_ = ops_ip4_addr_cast(addr);
  } else if (ops_is_ip6(addr)) {
    addr_.ip6_ = ops_ip6_addr_cast(addr);
  } else {
    family_ = AF_UNSPEC;
  }
  return *this;
}

// Associated operators.
bool operator==(const ObIpAddr &lhs, const sockaddr &rhs);

inline bool operator==(const sockaddr &lhs, const ObIpAddr &rhs)
{
  return rhs == lhs;
}

inline bool operator!=(const ObIpAddr &lhs, const sockaddr &rhs)
{
  return !(lhs == rhs);
}

inline bool operator!=(const sockaddr &lhs, const ObIpAddr &rhs)
{
  return !(rhs == lhs);
}

inline bool operator==(const ObIpAddr &lhs, const ObIpEndpoint &rhs)
{
  return lhs == rhs.sa_;
}

inline bool operator==(const ObIpEndpoint &lhs, const ObIpAddr &rhs)
{
  return lhs.sa_ == rhs;
}

inline bool operator!=(const ObIpAddr &lhs, const ObIpEndpoint &rhs)
{
  return !(lhs == rhs.sa_);
}

inline bool operator!=(const ObIpEndpoint &lhs, const ObIpAddr &rhs)
{
  return !(rhs == lhs.sa_);
}

inline bool operator!=(const ObIpEndpoint &lhs, const sockaddr &rhs)
{
  return !ops_ip_addr_port_eq(lhs, rhs);
}
// Write IP addr to storage dst.
// @return @s dst.
// dst: Destination storage.
// addr: source address.
// port: port, network order.
sockaddr &ops_ip_set(sockaddr &dst, const ObIpAddr &addr, in_port_t port = 0);

inline ObIpEndpoint& ObIpEndpoint::assign(const ObIpAddr &addr, const in_port_t port)
{
  ops_ip_set(sa_, addr, port);
  return *this;
}

inline ObIpEndpoint& ObIpEndpoint::assign(const sockaddr &ip)
{
  ops_ip_copy(sa_, ip);
  return *this;
}

inline in_port_t& ObIpEndpoint::port()
{
  return ops_ip_port_cast(sa_);
}

inline in_port_t ObIpEndpoint::port() const
{
  return ops_ip_port_cast(sa_);
}

inline int64_t ObIpEndpoint::get_port_host_order() const
{
  return ops_ip_port_host_order(sa_);
}

inline uint32_t ObIpEndpoint::get_ip4_host_order() const
{
  return ops_ip4_addr_host_order(sa_);
}

inline bool ObIpEndpoint::is_valid() const
{
  return ops_is_ip(*this);
}

inline bool ObIpEndpoint::is_ip4() const { return AF_INET == sa_.sa_family; }
inline bool ObIpEndpoint::is_ip6() const { return AF_INET6 == sa_.sa_family; }
inline uint16_t ObIpEndpoint::family() const { return sa_.sa_family; }

inline ObIpEndpoint& ObIpEndpoint::set_to_any_addr(const int family)
{
  ob_zero(*this);
  sa_.sa_family = (uint16_t)family;
  if (AF_INET == family) {
    sin_.sin_addr.s_addr = INADDR_ANY;
#if HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
    sin_.sin_len = sizeof(sockaddr_in);
#endif
  } else if (AF_INET6 == family) {
    sin6_.sin6_addr = in6addr_any;
#if HAVE_STRUCT_SOCKADDR_IN6_SIN6_LEN
    sin6_.sin6_len = sizeof(sockaddr_in6);
#endif
  }
  return *this;
}

inline ObIpEndpoint& ObIpEndpoint::set_to_loopback(const int family)
{
  ob_zero(*this);
  sa_.sa_family = (uint16_t)family;
  if (AF_INET == family) {
    sin_.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
#if HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
    sin_.sin_len = sizeof(sockaddr_in);
#endif
  } else if (AF_INET6 == family) {
    sin6_.sin6_addr = in6addr_loopback;
#if HAVE_STRUCT_SOCKADDR_IN6_SIN6_LEN
    sin6_.sin6_len = sizeof(sockaddr_in6);
#endif
  }
  return *this;
}

// Parse a string for pieces of an IP address.
//
// This doesn't parse the actual IP address, but picks it out from
// src. It is intended to deal with the brackets that can optionally
// surround an IP address (usually IPv6) which in turn are used to
// differentiate between an address and an attached port. E.g.
// @code
//   [FE80:9312::192:168:1:1]:80
// @endcode
// addr or port can be NULL in which case that value isn't returned.
//
// @return 0 if an address was found, non-zero otherwise.
// src: [in] String to search.
// addr: [out] Range containing IP address.
// port: [out] Range containing port.
int ops_ip_parse(common::ObString &src, common::ObString &addr, common::ObString &port);

// Check to see if a buffer contains only IP address characters.
// @family
// - AF_UNSPEC - not a numeric address.
// - AF_INET - only digits and dots.
// - AF_INET6 - colons found.
int ops_ip_check_characters(common::ObString &text, int &family);

// Get a string name for an IP address family.
// @return The string name (never NULL).
const char *ops_ip_family_name(const int family);

// Write a null terminated string for addr to dst.
// A buffer of size INET6_ADDRSTRLEN suffices, including a terminating null.
// addr: Address.
// dst: Output buffer.
// size: Length of buffer.
const char *ops_ip_ntop(const struct sockaddr &addr, char *dst, const int64_t size);

const char *ops_ip_ntop(const sockaddr &addr, char *dst, const int64_t size);

inline const char *ops_ip_ntop(const ObIpEndpoint &addr, char *dst, const int64_t size)
{
  return ops_ip_ntop(addr.sa_, dst, size);
}

// Write a null terminated string for addr to dst with port.
// A buffer of size INET6_ADDRPORTSTRLEN suffices, including a terminating null.
// addr: Address.
// dst: Output buffer.
// size: Length of buffer.
int ops_ip_nptop(const sockaddr &addr, char *dst, const int64_t size);

inline int ops_ip_nptop(const ObIpEndpoint &addr, char *dst, const int64_t size)
{
  return ops_ip_nptop(addr.sa_, dst, size);
}

inline int ops_ip_nptop(const sockaddr_in &addr, char *dst, const int64_t size)
{
  return ops_ip_nptop(ops_ip_sa_cast(addr), dst, size);
}

// Convert text to an IP address and write it to addr.
//
// text is expected to be an explicit address, not a hostname.  No
// hostname resolution is done. The call must provide an ip large
// enough to hold the address value.
//
// This attempts to recognize and process a port value if
// present. The port in ip is set appropriately, or to zero if no
// port was found or it was malformed.
//
// @note The return values are logically reversed from inet_pton.
// @note This uses getaddrinfo internally and so involves memory
// allocation.
//
// @return 0 on success, non-zero on failure.
// text:  [in] text
// addr:  [out] address
int ops_ip_pton(const common::ObString &text, sockaddr &addr);

inline int ops_ip_pton(const char *text, sockaddr_in6 &addr)
{
  return ops_ip_pton(common::ObString(text), ops_ip_sa_cast(addr));
}

inline int ops_ip_pton(const common::ObString &text, ObIpEndpoint &addr)
{
  return ops_ip_pton(text, addr.sa_);
}

inline int ops_ip_pton(const char *text, ObIpEndpoint &addr)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(text)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ops_ip_pton(common::ObString(text), addr.sa_))) {
  }
  return ret;
}

inline int ops_ip_pton(const char *text, sockaddr &addr)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(text)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ops_ip_pton(common::ObString(text), addr))) {
  }
  return ret;
}

inline int ops_ip_pton(char const *text, ObIpAddr &addr)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(text)) {
  } else if (OB_FAIL(addr.load(text))) {
  }
  return ret;
}

// Convert address to string as a hexidecimal value.
// The string is always null terminated, the output string is clipped
// if dst is insufficient.
// addr: Address to convert. Must be IP.
// dst: Destination buffer.
// len: Length of dst.
// ret_len: The length of the resulting string (not including null).
int ops_ip_to_hex(const sockaddr &addr, char *dst, int64_t len, int64_t &ret_len);

// get local addr list by hostname
// if need_all == false, INADDR_LOOPBACK(127.0.0.1) and INADDR_ANY(0.0.0.0) will be excluded.
// if need_all == true by default, all addrs will be contained.
// if it has any available addr, valid_cnt > 0 else is 0.
int get_local_addr_list(ObIpAddr *ipaddr, const int64_t total_cnt,
                        const bool need_all, int64_t &valid_cnt);

// get local addr list by nic
int get_local_addr_list_by_nic(ObIpAddr *ipaddr, const int64_t total_cnt,
                               const bool need_all, int64_t &valid_cnt);
int get_addr_by_host(char *hname, ObIpAddr *ipaddr, const int64_t total_cnt,
                     bool need_all, int64_t &valid_cnt);

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_INET_H
