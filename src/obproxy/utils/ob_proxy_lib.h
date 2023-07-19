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

#ifndef OBPROXY_LIB_H
#define OBPROXY_LIB_H

// Features
#define PACKAGE_NAME                   "OceanBase"
#define APP_NAME                       "ObProxy"
#define APP_VERSION                    "1.0.0"
#define BUILD_NUMBER                   ""

#define OB_HAS_IN6_IS_ADDR_UNSPECIFIED 1
#define OB_HAS_SO_MARK                 0
#define OB_HAS_IP_TOS                  1
#define OB_HAS_TESTS                   0
#define OB_DETAILED_SLOW_QUERY         0
//#define OB_HAS_EVENT_DEBUG
//#define OB_HAS_LOCK_CONTENTION_PROFILING
//#define USE_MYSQL_DEBUG_LISTS
//#define TRACK_BUFFER_USER
//#define OB_HAS_MEMORY_TRACKER

#define OB_MAX_THREADS_IN_EACH_THREAD_TYPE  256
#define OB_MAX_NUMBER_EVENT_THREADS  512

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/param.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/in_systm.h>
#include <netinet/tcp.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <signal.h>
#include <wait.h>
#include <pwd.h>
#include <poll.h>
#include <sys/epoll.h>
#include <values.h>
#include <alloca.h>
#include <dirent.h>
#include <cpio.h>
struct ifafilt;
#include <net/if.h>

// Gnu C++ doesn't define __STDC__ == 0 as needed to
// have ip_hl be defined.
#if defined(__GNUC__) && !defined(__STDC__)
#define __STDC__ 0
#endif

#include <endian.h>
#include <sys/ioctl.h>
#if defined(linux)
typedef unsigned int in_addr_t;
#endif
#include <sys/sysinfo.h>
#include <sys/sysctl.h>
#include <dlfcn.h>
#include <math.h>
#include <float.h>
#include <sys/sysmacros.h>
#include <sys/prctl.h>

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/lock/ob_mutex.h"
#include "lib/time/ob_hrtime.h"
#include "lib/list/ob_intrusive_list.h"
#include "lib/list/ob_atomic_list.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "utils/ob_proxy_version.h"
#include "utils/ob_proxy_xflush_def.h"


static const char *const OB_PROXYSYS_TENANT_NAME       = "proxysys";
static const char *const OB_PROXYSYS_USER_NAME         = "root";
static const char *const OB_INSPECTOR_USER_NAME        = "inspector";

//default cluster name
static const char *const OB_META_DB_CLUSTER_NAME       = "MetaDataBase";
static const char *const OB_PROXY_DEFAULT_CLUSTER_NAME = "obcloud";

//dbp runtime env
static const char *const OB_PROXY_DBP_RUNTIME_ENV      = "dbpcloud";

static const int64_t OB_DEFAULT_CLUSTER_ID             = 0;

//const size for proxy
static const int64_t OB_PROXY_CONFIG_BUFFER_SIZE       = 16 * 1024;
enum ObContMagic
{
  OB_CONT_MAGIC_ALIVE = 0xAABBCCDD,
  OB_CONT_MAGIC_DEAD = 0xDDCCBBAA
};

// DbMesh
static const int64_t OBPROXY_MAX_DBMESH_ID                 = INT64_MAX;
static const int64_t OBPROXY_MAX_DISASTER_STATUS_LENGTH    = 8; // REMOTE|LOCAL
static const int64_t OBPROXY_MAX_TNT_ID_LENGTH             = 128;

static const int64_t OB_PROXY_MAX_CLUSTER_NAME_LENGTH      = 256;
static const int64_t OB_PROXY_FULL_USER_NAME_MAX_LEN       = oceanbase::common::OB_MYSQL_FULL_USER_NAME_MAX_LEN + OB_PROXY_MAX_CLUSTER_NAME_LENGTH;//username@tenantname#clustername
static const int64_t OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH = OB_PROXY_MAX_CLUSTER_NAME_LENGTH + oceanbase::common::OB_MAX_TENANT_NAME_LENGTH + 1; //tenantname#clustername

static const int64_t OB_PROXY_CLUSTER_RESOURCE_ITEM_COUNT  = 12;
static const int64_t OB_PROXY_MAX_IDC_NAME_LENGTH          = oceanbase::common::MAX_ZONE_LENGTH;

static const int64_t OB_MAX_PROXY_BINARY_VERSION_LEN       = 256;
static const int64_t OB_MAX_PROXY_INFO_LEN                 = 256;
static const int64_t OB_MAX_PROXY_HOT_UPGRADE_CMD_LEN      = 64;
static const int64_t OB_MAX_PROXY_HOT_UPGRADE_STATUS_LEN   = 64;
static const int64_t OB_MAX_UNAME_INFO_LEN                 = 512;
static const int64_t OB_MAX_PROXY_RELOAD_CONFIG_STATUS_LEN = 64;
static const int64_t OB_MAX_PROXY_MD5_LEN                  = 64;
static const int64_t OB_DEFAULT_PROXY_MD5_LEN              = 32;

const static int64_t PRINT_SQL_LEN                         = 1024;
const static int64_t PRINT_JSON_LEN                        = 16 * 1024;

// Attention!! must confirm OB_NORMAL_MYSQL_CLIENT_COUNT >= OB_META_MYSQL_CLIENT_COUNT
// or will dead lock
static const int64_t OB_META_MYSQL_CLIENT_COUNT            = 2;
static const int64_t OB_NORMAL_MYSQL_CLIENT_COUNT          = 64;

static const int64_t OB_PROXY_WARN_LOG_BUF_LENGTH          = (1 << 20) * 1;
static const int64_t OB_PROXY_WARN_LOG_AVG_LENGTH          = 512;

static const int64_t OB_PROXY_MAX_INNER_TABLE_COLUMN_NUM = 64;

// errno define
#define OPS_START_ERRNO 20000
#define NET_ERRNO                         OPS_START_ERRNO+100
#define ENET_CONNECT_TIMEOUT              (NET_ERRNO+2)
#define ENET_CONNECT_FAILED               (NET_ERRNO+3)
#define MSYQL_ERRNO                       OPS_START_ERRNO+200
#define EMYSQL_ERROR                      (MSYQL_ERRNO+0)

namespace oceanbase
{
namespace obproxy
{

// Determine the element count for an array.
template<typename T, unsigned N>
static inline unsigned countof(const T (&)[N]) { return N; }

// default ob_capability_flag for proxy
// TODO: update this default value when proxy has supported new features
static const uint64_t OBPROXY_DEFAULT_CAPABILITY_FLAG =
    (OB_CAP_PARTITION_TABLE
     | OB_CAP_CHANGE_USER
     | OB_CAP_READ_WEAK
     | OB_CAP_CHECKSUM
     | OB_CAP_SAFE_WEAK_READ
     | OB_CAP_CHECKSUM_SWITCH
     | OB_CAP_OB_PROTOCOL_V2
     | OB_CAP_EXTRA_OK_PACKET_FOR_STATISTICS
     | OB_CAP_PL_ROUTE
     | OB_CAP_PROXY_REROUTE
     | OB_CAP_PROXY_SESSION_SYNC
     | OB_CAP_PROXY_FULL_LINK_TRACING
     | OB_CAP_PROXY_NEW_EXTRA_INFO
     | OB_CAP_PROXY_SESSION_VAR_SYNC
     | OB_CAP_PROXY_READ_STALE_FEEDBACK
     | OB_CAP_PROXY_FULL_LINK_TRACING_EXT
     | OB_CAP_SERVER_DUP_SESS_INFO_SYNC
    );

#define OBPROXY_SYS_ERRNO_START -10000
/*
 * get errno define from the following include:
 * /usr/include/asm-generic/errno.h
 * /usr/include/asm-generic/errno-base.h
 *
*/
static const int OB_SYS_EPERM           = OBPROXY_SYS_ERRNO_START + -EPERM; // 1   Operation not permitted
static const int OB_SYS_ENOENT          = OBPROXY_SYS_ERRNO_START + -ENOENT; // 2   No such file or directory
static const int OB_SYS_ESRCH           = OBPROXY_SYS_ERRNO_START + -ESRCH; // 3   No such process
static const int OB_SYS_EINTR           = OBPROXY_SYS_ERRNO_START + -EINTR; // 4   Interrupted system call
static const int OB_SYS_EIO             = OBPROXY_SYS_ERRNO_START + -EIO; // 5   I/O error
static const int OB_SYS_ENXIO           = OBPROXY_SYS_ERRNO_START + -ENXIO; // 6   No such device or address
static const int OB_SYS_E2BIG           = OBPROXY_SYS_ERRNO_START + -E2BIG; // 7   Argument list too long
static const int OB_SYS_ENOEXEC         = OBPROXY_SYS_ERRNO_START + -ENOEXEC; // 8   Exec format error
static const int OB_SYS_EBADF           = OBPROXY_SYS_ERRNO_START + -EBADF; // 9   Bad file number
static const int OB_SYS_ECHILD          = OBPROXY_SYS_ERRNO_START + -ECHILD; // 10   No child processes
static const int OB_SYS_EAGAIN          = OBPROXY_SYS_ERRNO_START + -EAGAIN; // 11   Try again
static const int OB_SYS_ENOMEM          = OBPROXY_SYS_ERRNO_START + -ENOMEM; // 12   Out of memory
static const int OB_SYS_EACCES          = OBPROXY_SYS_ERRNO_START + -EACCES; // 13   Permission denied
static const int OB_SYS_EFAULT          = OBPROXY_SYS_ERRNO_START + -EFAULT; // 14   Bad address
static const int OB_SYS_ENOTBLK         = OBPROXY_SYS_ERRNO_START + -ENOTBLK; // 15   Block device required
static const int OB_SYS_EBUSY           = OBPROXY_SYS_ERRNO_START + -EBUSY; // 16   Device or resource busy
static const int OB_SYS_EEXIST          = OBPROXY_SYS_ERRNO_START + -EEXIST; // 17   File exists
static const int OB_SYS_EXDEV           = OBPROXY_SYS_ERRNO_START + -EXDEV; // 18   Cross-device link
static const int OB_SYS_ENODEV          = OBPROXY_SYS_ERRNO_START + -ENODEV; // 19   No such device
static const int OB_SYS_ENOTDIR         = OBPROXY_SYS_ERRNO_START + -ENOTDIR; // 20   Not a directory
static const int OB_SYS_EISDIR          = OBPROXY_SYS_ERRNO_START + -EISDIR; // 21   Is a directory
static const int OB_SYS_EINVAL          = OBPROXY_SYS_ERRNO_START + -EINVAL; // 22   Invalid argument
static const int OB_SYS_ENFILE          = OBPROXY_SYS_ERRNO_START + -ENFILE; // 23   File table overflow
static const int OB_SYS_EMFILE          = OBPROXY_SYS_ERRNO_START + -EMFILE; // 24   Too many open files
static const int OB_SYS_ENOTTY          = OBPROXY_SYS_ERRNO_START + -ENOTTY; // 25   Not a typewriter
static const int OB_SYS_ETXTBSY         = OBPROXY_SYS_ERRNO_START + -ETXTBSY; // 26   Text file busy
static const int OB_SYS_EFBIG           = OBPROXY_SYS_ERRNO_START + -EFBIG; // 27   File too large
static const int OB_SYS_ENOSPC          = OBPROXY_SYS_ERRNO_START + -ENOSPC; // 28   No space left on device
static const int OB_SYS_ESPIPE          = OBPROXY_SYS_ERRNO_START + -ESPIPE; // 29   Illegal seek
static const int OB_SYS_EROFS           = OBPROXY_SYS_ERRNO_START + -EROFS; // 30   Read-only file system
static const int OB_SYS_EMLINK          = OBPROXY_SYS_ERRNO_START + -EMLINK; // 31   Too many links
static const int OB_SYS_EPIPE           = OBPROXY_SYS_ERRNO_START + -EPIPE; // 32   Broken pipe
static const int OB_SYS_EDOM            = OBPROXY_SYS_ERRNO_START + -EDOM; // 33   Math argument out of domain of func
static const int OB_SYS_ERANGE          = OBPROXY_SYS_ERRNO_START + -ERANGE; // 34   Math result not representable
static const int OB_SYS_EDEADLK         = OBPROXY_SYS_ERRNO_START + -EDEADLK; // 35   Resource deadlock would occur
static const int OB_SYS_ENAMETOOLONG    = OBPROXY_SYS_ERRNO_START + -ENAMETOOLONG; // 36   File name too long
static const int OB_SYS_ENOLCK          = OBPROXY_SYS_ERRNO_START + -ENOLCK; // 37   No record locks available
static const int OB_SYS_ENOSYS          = OBPROXY_SYS_ERRNO_START + -ENOSYS; // 38   Function not implemented
static const int OB_SYS_ENOTEMPTY       = OBPROXY_SYS_ERRNO_START + -ENOTEMPTY; // 39   Directory not empty
static const int OB_SYS_ELOOP           = OBPROXY_SYS_ERRNO_START + -ELOOP; // 40   Too many symbolic links encountered
static const int OB_SYS_EWOULDBLOCK     = OBPROXY_SYS_ERRNO_START + -EWOULDBLOCK; // EAGAIN   Operation would block
static const int OB_SYS_ENOMSG          = OBPROXY_SYS_ERRNO_START + -ENOMSG; // 42   No message of desired type
static const int OB_SYS_EIDRM           = OBPROXY_SYS_ERRNO_START + -EIDRM; // 43   Identifier removed
static const int OB_SYS_ECHRNG          = OBPROXY_SYS_ERRNO_START + -ECHRNG; // 44   Channel number out of range
static const int OB_SYS_EL2NSYNC        = OBPROXY_SYS_ERRNO_START + -EL2NSYNC; // 45   Level 2 not synchronized
static const int OB_SYS_EL3HLT          = OBPROXY_SYS_ERRNO_START + -EL3HLT; // 46   Level 3 halted
static const int OB_SYS_EL3RST          = OBPROXY_SYS_ERRNO_START + -EL3RST; // 47   Level 3 reset
static const int OB_SYS_ELNRNG          = OBPROXY_SYS_ERRNO_START + -ELNRNG; // 48   Link number out of range
static const int OB_SYS_EUNATCH         = OBPROXY_SYS_ERRNO_START + -EUNATCH; // 49   Protocol driver not attached
static const int OB_SYS_ENOCSI          = OBPROXY_SYS_ERRNO_START + -ENOCSI; // 50   No CSI structure available
static const int OB_SYS_EL2HLT          = OBPROXY_SYS_ERRNO_START + -EL2HLT; // 51   Level 2 halted
static const int OB_SYS_EBADE           = OBPROXY_SYS_ERRNO_START + -EBADE; // 52   Invalid exchange
static const int OB_SYS_EBADR           = OBPROXY_SYS_ERRNO_START + -EBADR; // 53   Invalid request descriptor
static const int OB_SYS_EXFULL          = OBPROXY_SYS_ERRNO_START + -EXFULL; // 54   Exchange full
static const int OB_SYS_ENOANO          = OBPROXY_SYS_ERRNO_START + -ENOANO; // 55   No anode
static const int OB_SYS_EBADRQC         = OBPROXY_SYS_ERRNO_START + -EBADRQC; // 56   Invalid request code
static const int OB_SYS_EBADSLT         = OBPROXY_SYS_ERRNO_START + -EBADSLT; // 57   Invalid slot
static const int OB_SYS_EDEADLOCK       = OBPROXY_SYS_ERRNO_START + -EDEADLOCK; // EDEADLK
static const int OB_SYS_EBFONT          = OBPROXY_SYS_ERRNO_START + -EBFONT; // 59   Bad font file format
static const int OB_SYS_ENOSTR          = OBPROXY_SYS_ERRNO_START + -ENOSTR; // 60   Device not a stream
static const int OB_SYS_ENODATA         = OBPROXY_SYS_ERRNO_START + -ENODATA; // 61   No data available
static const int OB_SYS_ETIME           = OBPROXY_SYS_ERRNO_START + -ETIME; // 62   Timer expired
static const int OB_SYS_ENOSR           = OBPROXY_SYS_ERRNO_START + -ENOSR; // 63   Out of streams resources
static const int OB_SYS_ENONET          = OBPROXY_SYS_ERRNO_START + -ENONET; // 64   Machine is not on the network
static const int OB_SYS_ENOPKG          = OBPROXY_SYS_ERRNO_START + -ENOPKG; // 65   Package not installed
static const int OB_SYS_EREMOTE         = OBPROXY_SYS_ERRNO_START + -EREMOTE; // 66   Object is remote
static const int OB_SYS_ENOLINK         = OBPROXY_SYS_ERRNO_START + -ENOLINK; // 67   Link has been severed
static const int OB_SYS_EADV            = OBPROXY_SYS_ERRNO_START + -EADV; // 68   Advertise error
static const int OB_SYS_ESRMNT          = OBPROXY_SYS_ERRNO_START + -ESRMNT; // 69   Srmount error
static const int OB_SYS_ECOMM           = OBPROXY_SYS_ERRNO_START + -ECOMM; // 70   Communication error on send
static const int OB_SYS_EPROTO          = OBPROXY_SYS_ERRNO_START + -EPROTO; // 71   Protocol error
static const int OB_SYS_EMULTIHOP       = OBPROXY_SYS_ERRNO_START + -EMULTIHOP; // 72   Multihop attempted
static const int OB_SYS_EDOTDOT         = OBPROXY_SYS_ERRNO_START + -EDOTDOT; // 73   RFS specific error
static const int OB_SYS_EBADMSG         = OBPROXY_SYS_ERRNO_START + -EBADMSG; // 74   Not a data message
static const int OB_SYS_EOVERFLOW       = OBPROXY_SYS_ERRNO_START + -EOVERFLOW; // 75   Value too large for defined data type
static const int OB_SYS_ENOTUNIQ        = OBPROXY_SYS_ERRNO_START + -ENOTUNIQ; // 76   Name not unique on network
static const int OB_SYS_EBADFD          = OBPROXY_SYS_ERRNO_START + -EBADFD; // 77   File descriptor in bad state
static const int OB_SYS_EREMCHG         = OBPROXY_SYS_ERRNO_START + -EREMCHG; // 78   Remote address changed
static const int OB_SYS_ELIBACC         = OBPROXY_SYS_ERRNO_START + -ELIBACC; // 79   Can not access a needed shared library
static const int OB_SYS_ELIBBAD         = OBPROXY_SYS_ERRNO_START + -ELIBBAD; // 80   Accessing a corrupted shared library
static const int OB_SYS_ELIBSCN         = OBPROXY_SYS_ERRNO_START + -ELIBSCN; // 81   .lib section in a.out corrupted
static const int OB_SYS_ELIBMAX         = OBPROXY_SYS_ERRNO_START + -ELIBMAX; // 82   Attempting to link in too many shared libraries
static const int OB_SYS_ELIBEXEC        = OBPROXY_SYS_ERRNO_START + -ELIBEXEC; // 83   Cannot exec a shared library directly
static const int OB_SYS_EILSEQ          = OBPROXY_SYS_ERRNO_START + -EILSEQ; // 84   Illegal byte sequence
static const int OB_SYS_ERESTART        = OBPROXY_SYS_ERRNO_START + -ERESTART; // 85   Interrupted system call should be restarted
static const int OB_SYS_ESTRPIPE        = OBPROXY_SYS_ERRNO_START + -ESTRPIPE; // 86   Streams pipe error
static const int OB_SYS_EUSERS          = OBPROXY_SYS_ERRNO_START + -EUSERS; // 87   Too many users
static const int OB_SYS_ENOTSOCK        = OBPROXY_SYS_ERRNO_START + -ENOTSOCK; // 88   Socket operation on non-socket
static const int OB_SYS_EDESTADDRREQ    = OBPROXY_SYS_ERRNO_START + -EDESTADDRREQ; // 89   Destination address required
static const int OB_SYS_EMSGSIZE        = OBPROXY_SYS_ERRNO_START + -EMSGSIZE; // 90   Message too long
static const int OB_SYS_EPROTOTYPE      = OBPROXY_SYS_ERRNO_START + -EPROTOTYPE; // 91   Protocol wrong type for socket
static const int OB_SYS_ENOPROTOOPT     = OBPROXY_SYS_ERRNO_START + -ENOPROTOOPT; // 92   Protocol not available
static const int OB_SYS_EPROTONOSUPPORT = OBPROXY_SYS_ERRNO_START + -EPROTONOSUPPORT; // 93   Protocol not supported
static const int OB_SYS_ESOCKTNOSUPPORT = OBPROXY_SYS_ERRNO_START + -ESOCKTNOSUPPORT; // 94   Socket type not supported
static const int OB_SYS_EOPNOTSUPP      = OBPROXY_SYS_ERRNO_START + -EOPNOTSUPP; // 95   Operation not supported on transport endpoint
static const int OB_SYS_EPFNOSUPPORT    = OBPROXY_SYS_ERRNO_START + -EPFNOSUPPORT; // 96   Protocol family not supported
static const int OB_SYS_EAFNOSUPPORT    = OBPROXY_SYS_ERRNO_START + -EAFNOSUPPORT; // 97   Address family not supported by protocol
static const int OB_SYS_EADDRINUSE      = OBPROXY_SYS_ERRNO_START + -EADDRINUSE; // 98   Address already in use
static const int OB_SYS_EADDRNOTAVAIL   = OBPROXY_SYS_ERRNO_START + -EADDRNOTAVAIL; // 99   Cannot assign requested address
static const int OB_SYS_ENETDOWN        = OBPROXY_SYS_ERRNO_START + -ENETDOWN; // 100  Network is down
static const int OB_SYS_ENETUNREACH     = OBPROXY_SYS_ERRNO_START + -ENETUNREACH; // 101  Network is unreachable
static const int OB_SYS_ENETRESET       = OBPROXY_SYS_ERRNO_START + -ENETRESET; // 102  Network dropped connection because of reset
static const int OB_SYS_ECONNABORTED    = OBPROXY_SYS_ERRNO_START + -ECONNABORTED; // 103  Software caused connection abort
static const int OB_SYS_ECONNRESET      = OBPROXY_SYS_ERRNO_START + -ECONNRESET; // 104  Connection reset by peer
static const int OB_SYS_ENOBUFS         = OBPROXY_SYS_ERRNO_START + -ENOBUFS; // 105  No buffer space available
static const int OB_SYS_EISCONN         = OBPROXY_SYS_ERRNO_START + -EISCONN; // 106  Transport endpoint is already connected
static const int OB_SYS_ENOTCONN        = OBPROXY_SYS_ERRNO_START + -ENOTCONN; // 107  Transport endpoint is not connected
static const int OB_SYS_ESHUTDOWN       = OBPROXY_SYS_ERRNO_START + -ESHUTDOWN; // 108  Cannot send after transport endpoint shutdown
static const int OB_SYS_ETOOMANYREFS    = OBPROXY_SYS_ERRNO_START + -ETOOMANYREFS; // 109  Too many references: cannot splice
static const int OB_SYS_ETIMEDOUT       = OBPROXY_SYS_ERRNO_START + -ETIMEDOUT; // 110  Connection timed out
static const int OB_SYS_ECONNREFUSED    = OBPROXY_SYS_ERRNO_START + -ECONNREFUSED; // 111  Connection refused
static const int OB_SYS_EHOSTDOWN       = OBPROXY_SYS_ERRNO_START + -EHOSTDOWN; // 112  Host is down
static const int OB_SYS_EHOSTUNREACH    = OBPROXY_SYS_ERRNO_START + -EHOSTUNREACH; // 113  No route to host
static const int OB_SYS_EALREADY        = OBPROXY_SYS_ERRNO_START + -EALREADY; // 114  Operation already in progress
static const int OB_SYS_EINPROGRESS     = OBPROXY_SYS_ERRNO_START + -EINPROGRESS; // 115  Operation now in progress
static const int OB_SYS_ESTALE          = OBPROXY_SYS_ERRNO_START + -ESTALE; // 116  Stale NFS file handle
static const int OB_SYS_EUCLEAN         = OBPROXY_SYS_ERRNO_START + -EUCLEAN; // 117  Structure needs cleaning
static const int OB_SYS_ENOTNAM         = OBPROXY_SYS_ERRNO_START + -ENOTNAM; // 118  Not a XENIX named type file
static const int OB_SYS_ENAVAIL         = OBPROXY_SYS_ERRNO_START + -ENAVAIL; // 119  No XENIX semaphores available
static const int OB_SYS_EISNAM          = OBPROXY_SYS_ERRNO_START + -EISNAM; // 120  Is a named type file
static const int OB_SYS_EREMOTEIO       = OBPROXY_SYS_ERRNO_START + -EREMOTEIO; // 121  Remote I/O error
static const int OB_SYS_EDQUOT          = OBPROXY_SYS_ERRNO_START + -EDQUOT; // 122  Quota exceeded
static const int OB_SYS_ENOMEDIUM       = OBPROXY_SYS_ERRNO_START + -ENOMEDIUM; // 123  No medium found
static const int OB_SYS_EMEDIUMTYPE     = OBPROXY_SYS_ERRNO_START + -EMEDIUMTYPE; // 124  Wrong medium type
static const int OB_SYS_ECANCELED       = OBPROXY_SYS_ERRNO_START + -ECANCELED; // 125  Operation Canceled
static const int OB_SYS_ENOKEY          = OBPROXY_SYS_ERRNO_START + -ENOKEY; // 126  Required key not available
static const int OB_SYS_EKEYEXPIRED     = OBPROXY_SYS_ERRNO_START + -EKEYEXPIRED; // 127  Key has expired
static const int OB_SYS_EKEYREVOKED     = OBPROXY_SYS_ERRNO_START + -EKEYREVOKED; // 128  Key has been revoked
static const int OB_SYS_EKEYREJECTED    = OBPROXY_SYS_ERRNO_START + -EKEYREJECTED; // 129  Key was rejected by service
static const int OB_SYS_EOWNERDEAD      = OBPROXY_SYS_ERRNO_START + -EOWNERDEAD; // 130  Owner died
static const int OB_SYS_ENOTRECOVERABLE = OBPROXY_SYS_ERRNO_START + -ENOTRECOVERABLE; // 131  State not recoverable
static const int OB_SYS_ERFKILL         = OBPROXY_SYS_ERRNO_START + -ERFKILL; // 132  Operation not possible due to RF-kill

//In the Linux 5u system does not define the EHWPOISON error code
//static const int OB_SYS_EHWPOISON     = OBPROXY_SYS_ERRNO_START + -EHWPOISON; // 133  Memory page has hardware error
static const int OB_SYS_EHWPOISON       = OBPROXY_SYS_ERRNO_START + -133; // 133  Memory page has hardware error

inline int32_t ob_get_sys_errno(int err_code)
{
  return OBPROXY_SYS_ERRNO_START + -err_code;
}

inline int32_t ob_get_sys_errno()
{
  return OBPROXY_SYS_ERRNO_START + -errno;
}

inline int32_t ob_get_sys_errno(char *&msg)
{
  msg = strerror(errno);
  return OBPROXY_SYS_ERRNO_START + -errno;
}

// Set data to zero.
//
// Calls memset on t with a value of zero and a length of
// sizeof(t). This can be used on ordinary and array variables. While
// this can be used on variables of intrinsic type it's inefficient.
//
// @note Because this uses templates it cannot be used on unnamed or
// locally scoped structures / classes. This is an inherent
// limitation of templates.
//
// Examples:
// @code
// foo bar; // value.
// ob_zero(bar); // zero bar.
//
// foo *bar; // pointer.
// ob_zero(bar); // WRONG - makes the pointer
// bar zero.
// ob_zero(*bar); // zero what bar points at.
//
// foo bar[ZOMG]; // Array of structs.
// ob_zero(bar); // Zero all structs in array.
//
// foo *bar[ZOMG]; // array of pointers.
// ob_zero(bar); // zero all pointers in the array.
// @endcode
template < typename T >
inline void ob_zero(T& t)
{
  memset(&t, 0, sizeof(t));
}

inline int64_t ob_roundup(const int64_t n, const int64_t prec)
{
    return (((n + prec - 1) / prec) * prec);
}

inline common::ObString get_print_json(const common::ObString &json)
{
  int32_t len = 0;
  if (json.length() >= PRINT_JSON_LEN) {
    len = PRINT_JSON_LEN;
  } else {
    len = json.length();
  }
  return common::ObString(len, json.ptr());
}

#define MPRINT(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
#define MPRINT_STDOUT(format, ...) fprintf(stdout, format "\n", ##__VA_ARGS__)

#define IS_SPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')


} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_LIB_H
