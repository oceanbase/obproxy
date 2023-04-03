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
 * **************************************************************
 *
 * @brief Define module statement and module-LOG macro. This file should be included in ob_log.h.
 * You have to include ob_log.h to use the macro defined in this file.
 */

#ifndef OCEANBASE_LIB_OBLOG_OB_LOG_MODULE_
#define OCEANBASE_LIB_OBLOG_OB_LOG_MODULE_
#include <stdint.h>
#include <stdio.h>

namespace oceanbase
{
namespace common
{
#define LOG_MOD_BEGIN(ModName) \
  struct OB_LOG_##ModName      \
  {                            \
    enum                       \
    {

#define LOG_MOD_END(ModName)   \
    };                         \
  };

#define DEFINE_LOG_SUB_MOD(ModName)  M_##ModName,

//Currently it is divided by directory. Each module can be adjusted according to the specific situation during the use process
//After adding or modifying modules in this file, remember to make corresponding modifications in ob_log_module.ipp
//The number of modules is limited, see OB_LOG_MAX_PAR_MOD_SIZE for details
//statement of parent modules
LOG_MOD_BEGIN(ROOT)                      // src directory
DEFINE_LOG_SUB_MOD(CLIENT)               // client
DEFINE_LOG_SUB_MOD(CLOG)                 // clog
DEFINE_LOG_SUB_MOD(COMMON)               // observer
DEFINE_LOG_SUB_MOD(ELECT)                // election
DEFINE_LOG_SUB_MOD(LIB)                  // lib
DEFINE_LOG_SUB_MOD(PROXY)                // obproxy
DEFINE_LOG_SUB_MOD(RPC)                  // rpc
DEFINE_LOG_SUB_MOD(RS)                   // rootserver
DEFINE_LOG_SUB_MOD(SERVER)               // rpc, common/server_framework
DEFINE_LOG_SUB_MOD(SHARE)                // share
DEFINE_LOG_SUB_MOD(SQL)                  // sql
DEFINE_LOG_SUB_MOD(STORAGE)              // storage, blocksstable
DEFINE_LOG_SUB_MOD(TLOG)                 // liboblog
DEFINE_LOG_SUB_MOD(STORAGETEST)          // storagetest
DEFINE_LOG_SUB_MOD(LOGTOOL)              // logtool
LOG_MOD_END(ROOT)

//statement of LIB's sub-modules
LOG_MOD_BEGIN(LIB)                       // lib directory
DEFINE_LOG_SUB_MOD(ALLOC)                // allocator
DEFINE_LOG_SUB_MOD(CONT)                 // all container except hash: list, queue, array, hash
DEFINE_LOG_SUB_MOD(FILE)                 // file
DEFINE_LOG_SUB_MOD(HASH)                 // hash
DEFINE_LOG_SUB_MOD(LOCK)                 // lock
DEFINE_LOG_SUB_MOD(MYSQLC)               // internal mysqlclient
DEFINE_LOG_SUB_MOD(STRING)               // string
DEFINE_LOG_SUB_MOD(TIME)                 // time
DEFINE_LOG_SUB_MOD(UTIL)                 // utility
DEFINE_LOG_SUB_MOD(CHARSET)              // charset
LOG_MOD_END(LIB)

// statements of RPC's sub-modules
LOG_MOD_BEGIN(RPC)
DEFINE_LOG_SUB_MOD(FRAME)
DEFINE_LOG_SUB_MOD(OBRPC)
DEFINE_LOG_SUB_MOD(OBMYSQL)
DEFINE_LOG_SUB_MOD(TEST)
LOG_MOD_END(RPC)

//statement of COMMON's sub-modules
LOG_MOD_BEGIN(COMMON)                    // common directory
DEFINE_LOG_SUB_MOD(CACHE)                // cache
DEFINE_LOG_SUB_MOD(EXPR)                 // expression
DEFINE_LOG_SUB_MOD(LEASE)                // lease
DEFINE_LOG_SUB_MOD(MYSQLP)               // mysql_proxy
DEFINE_LOG_SUB_MOD(PRI)                  // privilege
DEFINE_LOG_SUB_MOD(STAT)                 // stat
DEFINE_LOG_SUB_MOD(UPSR)                 // ups_reclaim
LOG_MOD_END(COMMON)

//statement of SHARE's sub-modules
LOG_MOD_BEGIN(SHARE)                     // share directory
DEFINE_LOG_SUB_MOD(CONFIG)               // config
DEFINE_LOG_SUB_MOD(FILE)                 // file
DEFINE_LOG_SUB_MOD(INNERT)               // inner_table
DEFINE_LOG_SUB_MOD(INTERFACE)            // interface
DEFINE_LOG_SUB_MOD(LOG)                  // log
DEFINE_LOG_SUB_MOD(PT)                   // partition_table
DEFINE_LOG_SUB_MOD(SCHEMA)               // schema
DEFINE_LOG_SUB_MOD(TRIGGER)              // trigger
LOG_MOD_END(SHARE)

//statement of storage's sub-modules
LOG_MOD_BEGIN(STORAGE)                   // storage, blocksstable directory
DEFINE_LOG_SUB_MOD(REDO)                 // replay
DEFINE_LOG_SUB_MOD(COMPACTION)
DEFINE_LOG_SUB_MOD(BSST)                // blocksstable
DEFINE_LOG_SUB_MOD(MEMT)  // memtable
DEFINE_LOG_SUB_MOD(TRANS)  // transaction
DEFINE_LOG_SUB_MOD(REPLAY)  // replay engine
LOG_MOD_END(STORAGE)

// statement of clog's sub-modules
LOG_MOD_BEGIN(CLOG)
DEFINE_LOG_SUB_MOD(EXTLOG)               // external fetch log
DEFINE_LOG_SUB_MOD(CSR)                  // cursor cache
LOG_MOD_END(CLOG)

//statement of SQL's sub-modules
LOG_MOD_BEGIN(SQL)                       // sql directory
DEFINE_LOG_SUB_MOD(ENG)                  // engine
DEFINE_LOG_SUB_MOD(EXE)                  // execute
DEFINE_LOG_SUB_MOD(OPT)                  // optimizer
DEFINE_LOG_SUB_MOD(JO)                   // join order
DEFINE_LOG_SUB_MOD(PARSER)               // parser
DEFINE_LOG_SUB_MOD(PC)                   // plan_cache
DEFINE_LOG_SUB_MOD(RESV)                 // resolver
DEFINE_LOG_SUB_MOD(REWRITE)              // rewrite
DEFINE_LOG_SUB_MOD(SESSION)              // session
DEFINE_LOG_SUB_MOD(CG)                   // code_generator
DEFINE_LOG_SUB_MOD(MONITOR)              // monitor
LOG_MOD_END(SQL)

// observer submodules
LOG_MOD_BEGIN(SERVER)
DEFINE_LOG_SUB_MOD(OMT)                  // user mode scheduler
LOG_MOD_END(SERVER)

//statement of PROXY's sub-modules
LOG_MOD_BEGIN(PROXY)                       // proxy directory
DEFINE_LOG_SUB_MOD(EVENT)                  // event
DEFINE_LOG_SUB_MOD(NET)                    // net
DEFINE_LOG_SUB_MOD(SOCK)                   // socket
DEFINE_LOG_SUB_MOD(TXN)                    // transaction
DEFINE_LOG_SUB_MOD(TUNNEL)                 // tunnel
DEFINE_LOG_SUB_MOD(SM)                     // state machine
DEFINE_LOG_SUB_MOD(CS)                     // client session
DEFINE_LOG_SUB_MOD(SS)                     // server session
DEFINE_LOG_SUB_MOD(PVC)                    // plugin virtual connection
DEFINE_LOG_SUB_MOD(TRANSFORM)              // transform
DEFINE_LOG_SUB_MOD(API)                    // api
DEFINE_LOG_SUB_MOD(ICMD)                   // internal cmd
LOG_MOD_END(PROXY)

// liboblog submodules
LOG_MOD_BEGIN(TLOG)
DEFINE_LOG_SUB_MOD(FETCHER)                 // fetcher
DEFINE_LOG_SUB_MOD(PARSER)                  // Parser
DEFINE_LOG_SUB_MOD(SEQUENCER)               // Sequencer
DEFINE_LOG_SUB_MOD(FORMATTER)               // Formatter
DEFINE_LOG_SUB_MOD(COMMITTER)               // committer
DEFINE_LOG_SUB_MOD(TAILF)                   // oblog_tailf
LOG_MOD_END(TLOG)

// storagetest submodules
LOG_MOD_BEGIN(STORAGETEST)
DEFINE_LOG_SUB_MOD(TEST)                  // Parser
LOG_MOD_END(STORAGETEST)
} //namespace common
} //namespace oceanbase

#define OB_LOG_LEVEL_NONE 7
#define OB_LOG_LEVEL_NP -1  //set this level, would not print log
#define OB_LOG_LEVEL_ERROR 0
//#define OB_LOG_LEVEL_USER_ERROR  1
#define OB_LOG_LEVEL_WARN  2
#define OB_LOG_LEVEL_INFO  3
#define OB_LOG_LEVEL_TRACE 4
#define OB_LOG_LEVEL_DEBUG 5
#define OB_LOG_LEVEL(level) OB_LOG_LEVEL_##level, __FILE__, __LINE__, __FUNCTION__
#define OB_LOG_NUM_LEVEL(level) level, __FILE__, __LINE__, __FUNCTION__
#define OB_LOGGER ::oceanbase::common::ObLogger::get_logger()
#define OB_LOG_NEED_TO_PRINT(level) OB_LOGGER.need_to_print(OB_LOG_LEVEL_##level)
#define OB_XFLUSH_LOG_NEED_TO_PRINT(level) OB_LOGGER.need_to_print_xflush(OB_LOG_LEVEL_##level)
#define OB_MONITOR_LOG_NEED_TO_PRINT(level) OB_LOGGER.need_to_print_monitor(OB_LOG_LEVEL_##level)
#define OB_NEED_USE_ASYNC_LOG (OB_LOGGER.is_async_log_used() && !OB_LOGGER.get_trace_mode())
#define IS_DEBUG_ENABLED() OB_LOGGER.need_to_print(OB_LOG_LEVEL_DEBUG)

#define OB_PRINT(modName, level, infoString, args...)                                            \
  OB_LOGGER.log_message_kv(modName, OB_LOG_LEVEL(level), infoString, ##args)
#define _OB_PRINT(modName, level, _fmt_, args...)                                                \
  (OB_NEED_USE_ASYNC_LOG ? OB_LOGGER.async_log_message(modName, OB_LOG_LEVEL(level), _fmt_, ##args) : OB_LOGGER.log_message(modName, OB_LOG_LEVEL(level), _fmt_, ##args))

#define OB_PRINT_TYPE(type, modName, level, infoString, args...)                                            \
  OB_LOGGER.log_message_kv(type, modName, OB_LOG_LEVEL(level), infoString, ##args)
#define _OB_PRINT_TYPE(type, modName, level, _fmt_, args...)                                                \
  (OB_NEED_USE_ASYNC_LOG ? OB_LOGGER.async_log_message(type, modName, OB_LOG_LEVEL(level), _fmt_, ##args) : OB_LOGGER.log_message(type, modName, OB_LOG_LEVEL(level), _fmt_, ##args))

#define OB_FORCE_ROTATE_LOG(type, version)                                                       \
  OB_LOGGER.force_rotate_log(type, version)

#define OB_LOG(level, infoString, args...)                                                       \
  (OB_LOG_NEED_TO_PRINT(level) ? OB_PRINT("", level, infoString, ##args) : (void) 0)
#define _OB_LOG(level, _fmt_, args...)                                                           \
  (OB_LOG_NEED_TO_PRINT(level) ? _OB_PRINT("", level, _fmt_, ##args) : (void) 0)

#define OBPROXY_XFLUSH_LOG(level, infoString, args...)                                           \
  (OB_XFLUSH_LOG_NEED_TO_PRINT(level) ? OB_PRINT(NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_XFLUSH_LOG(level, _fmt_, args...)                                               \
  (OB_XFLUSH_LOG_NEED_TO_PRINT(level) ? _OB_PRINT(NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_DIGEST_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_DIGEST_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_DIGEST_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_DIGEST_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_ERROR_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_ERROR_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_ERROR_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_ERROR_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_SLOW_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_SLOW_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_SLOW_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_SLOW_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_STAT_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_STAT_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_STAT_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_STAT_FILE, NULL, level, _fmt_, ##args) : (void) 0)

 
#define OBPROXY_CONFIG_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_CONFIG_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_CONFIG_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_CONFIG_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_CONFIG_LOG_ROTATE(level, infoString)                                             \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_FORCE_ROTATE_LOG(FD_CONFIG_FILE, infoString) : (void) 0)

#define OBPROXY_LIMIT_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_LIMIT_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_LIMIT_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_LIMIT_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_POOL_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_POOL_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_POOL_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_POOL_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_POOL_STAT_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_POOL_STAT_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_POOL_STAT_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_POOL_STAT_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_TRACE_LOG(level, infoString, args...)                                           \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_TRACE_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_TRACE_LOG(level, _fmt_, args...)                                               \
  (OB_MONITOR_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_TRACE_FILE, NULL, level, _fmt_, ##args) : (void) 0)

#define OBPROXY_DRIVER_CLIENT_LOG(level, infoString, args...)                                           \
  (OB_LOG_NEED_TO_PRINT(level) ? OB_PRINT_TYPE(FD_DRIVER_CLIENT_FILE, NULL, level, infoString, ##args) : (void) 0)
#define _OBPROXY_DRIVER_CLIENT_LOG(level, _fmt_, args...)                                               \
  (OB_LOG_NEED_TO_PRINT(level) ? _OB_PRINT_TYPE(FD_DRIVER_CLIENT_FILE, NULL, level, _fmt_, ##args) : (void) 0)


#define _OB_NUM_LEVEL_LOG(level, _fmt_, args...)                                                 \
  (OB_LOGGER.need_to_print(level) ?                                                              \
   OB_LOGGER.log_message("", OB_LOG_NUM_LEVEL(level), _fmt_, ##args) : (void) 0)

#define _OB_NUM_LEVEL_PRINT(level, _fmt_, args...)                                               \
   OB_LOGGER.log_message("", OB_LOG_NUM_LEVEL(level), _fmt_, ##args)

#define _OB_LOG_US(level, _fmt_, args...)                                                        \
  _OB_LOG(level, "[%ld][%ld] " _fmt_,                                                            \
          ::oceanbase::common::ObLogger::get_cur_tv().tv_sec,                                    \
          ::oceanbase::common::ObLogger::get_cur_tv().tv_usec, ##args)

#define SET_OB_LOG_TRACE_MODE()  OB_LOGGER.set_trace_mode(true)
#define CANCLE_OB_LOG_TRACE_MODE() OB_LOGGER.set_trace_mode(false)
#define PRINT_OB_LOG_TRACE_BUF(level)                                                            \
  (OB_LOG_NEED_TO_PRINT(level) ? OB_LOGGER.print_trace_buffer(OB_LOG_LEVEL(level)) : (void) 0)

//for tests/ob_log_test or others
#define OB_LOG_MOD_NEED_TO_PRINT(parMod, level)                                                  \
  OB_LOGGER.need_to_print(::oceanbase::common::OB_LOG_ROOT::M_##parMod, OB_LOG_LEVEL_##level)
//for tests/ob_log_test or others
#define OB_LOG_SUBMOD_NEED_TO_PRINT(parMod, subMod, level)                                       \
  OB_LOGGER.need_to_print(::oceanbase::common::OB_LOG_ROOT::M_##parMod,                          \
                          ::oceanbase::common::OB_LOG_##parMod::M_##subMod, OB_LOG_LEVEL_##level)

//define macro for module log.
//For some module are defined as macro like '#define SQL sql=[%.*s]' ,
//define this macro without OB_LOG_MOD_NEED_TO_PRINT
#define OB_MOD_LOG(parMod, level, info_string, args...)                                          \
  ((OB_LOGGER.need_to_print(::oceanbase::common::OB_LOG_ROOT::M_##parMod, OB_LOG_LEVEL_##level)) \
  ? OB_PRINT("["#parMod"] ", level, info_string, ##args) : (void) 0)
#define _OB_MOD_LOG(parMod, level, _fmt_, args...)                                               \
  ((OB_LOGGER.need_to_print(oceanbase::common::OB_LOG_ROOT::M_##parMod, OB_LOG_LEVEL_##level))   \
  ? _OB_PRINT("["#parMod"] ", level, _fmt_, ##args) : (void) 0)

#define OB_SUB_MOD_LOG(parMod, subMod, level, info_string, args...)                              \
  ((OB_LOGGER.need_to_print(::oceanbase::common::OB_LOG_ROOT::M_##parMod,                        \
                            ::oceanbase::common::OB_LOG_##parMod::M_##subMod,                    \
                            OB_LOG_LEVEL_##level)) ?                                             \
   OB_PRINT("["#parMod"."#subMod"] ", level, info_string, ##args) : (void) 0)
#define _OB_SUB_MOD_LOG(parMod, subMod, level, _fmt_, args...)                                   \
  ((OB_LOGGER.need_to_print(oceanbase::common::OB_LOG_ROOT::M_##parMod,                          \
                            oceanbase::common::OB_LOG_##parMod::M_##subMod,                      \
                            OB_LOG_LEVEL_##level)) ?                                             \
   _OB_PRINT("["#parMod"."#subMod"] ", level, _fmt_, ##args) : (void) 0)

// define macro for module obproxy
#define OBPROXY_MOD_LOG(parMod, level, info_string, args...)                                                      \
  ((OB_LOGGER.get_id_level_map().get_level(::oceanbase::common::OB_LOG_ROOT::M_##parMod) >= OB_LOG_LEVEL_##level) \
  ? OB_PRINT("["#parMod"] ", level, info_string, ##args) : (void) 0)
#define _OBPROXY_MOD_LOG(parMod, level, _fmt_, args...)                                                           \
  ((OB_LOGGER.get_id_level_map().get_level(oceanbase::common::OB_LOG_ROOT::M_##parMod) >= OB_LOG_LEVEL_##level)   \
  ? _OB_PRINT("["#parMod"] ", level, _fmt_, ##args) : (void) 0)

#define OBPROXY_SUB_MOD_LOG(parMod, subMod, level, info_string, args...)                                          \
  ((OB_LOGGER.get_id_level_map().get_level(::oceanbase::common::OB_LOG_ROOT::M_##parMod,                          \
                            ::oceanbase::common::OB_LOG_##parMod::M_##subMod) >= OB_LOG_LEVEL_##level) ?          \
   OB_PRINT("["#parMod"."#subMod"] ", level, info_string, ##args) : (void) 0)
#define _OBPROXY_SUB_MOD_LOG(parMod, subMod, level, _fmt_, args...)                                               \
  ((OB_LOGGER.get_id_level_map().get_level(oceanbase::common::OB_LOG_ROOT::M_##parMod,                            \
                            oceanbase::common::OB_LOG_##parMod::M_##subMod) >= OB_LOG_LEVEL_##level) ?            \
   _OB_PRINT("["#parMod"."#subMod"] ", level, _fmt_, ##args) : (void) 0)

//define ParMod_LOG
#define BLSST_LOG(level, info_string, args...) OB_MOD_LOG(BLSST, level, info_string, ##args)
#define _BLSST_LOG(level, _fmt_, args...) _OB_MOD_LOG(BLSST, level, _fmt_, ##args)
#define CLIENT_LOG(level, info_string, args...) OB_MOD_LOG(CLIENT, level, info_string, ##args)
#define _CLIENT_LOG(level, _fmt_, args...) _OB_MOD_LOG(CLIENT, level, _fmt_, ##args)
#define CLOG_LOG(level, info_string, args...) OB_MOD_LOG(CLOG, level, info_string, ##args)
#define _CLOG_LOG(level, _fmt_, args...) _OB_MOD_LOG(CLOG, level, _fmt_, ##args)
#define EXTLOG_LOG(level, info_string, args...) OB_SUB_MOD_LOG(CLOG, EXTLOG, level, info_string, ##args)
#define _EXTLOG_LOG(level, info_string, args...) _OB_SUB_MOD_LOG(CLOG, EXTLOG, level, info_string, ##args)
#define CSR_LOG(level, info_string, args...) OB_SUB_MOD_LOG(CLOG, CSR, level, info_string, ##args)
#define _CSR_LOG(level, info_string, args...) _OB_SUB_MOD_LOG(CLOG, CSR, level, info_string, ##args)
#define COMMON_LOG(level, info_string, args...) OB_MOD_LOG(COMMON, level, info_string, ##args)
#define _COMMON_LOG(level, _fmt_, args...) _OB_MOD_LOG(COMMON, level, _fmt_, ##args)
#define ELECT_LOG(level, info_string, args...) OB_MOD_LOG(ELECT, level, info_string, ##args)
#define _ELECT_LOG(level, _fmt_, args...) _OB_MOD_LOG(ELECT, level, _fmt_, ##args)
#define IMPS_LOG(level, info_string, args...) OB_MOD_LOG(IMPS, level, info_string, ##args)
#define _IMPS_LOG(level, _fmt_, args...) _OB_MOD_LOG(IMPS, level, _fmt_, ##args)
#define LIB_LOG(level, info_string, args...) OB_MOD_LOG(LIB, level, info_string, ##args)
#define _LIB_LOG(level, _fmt_, args...) _OB_MOD_LOG(LIB, level, _fmt_, ##args)
#define MEMT_LOG(level, info_string, args...) OB_MOD_LOG(MEMT, level, info_string, ##args)
#define _MEMT_LOG(level, _fmt_, args...) _OB_MOD_LOG(MEMT, level, _fmt_, ##args)
#define MRSST_LOG(level, info_string, args...) OB_MOD_LOG(MRSST, level, info_string, ##args)
#define _MRSST_LOG(level, _fmt_, args...) _OB_MOD_LOG(MRSST, level, _fmt_, ##args)
#define MYSQL_LOG(level, info_string, args...) OB_MOD_LOG(MYSQL, level, info_string, ##args)
#define _MYSQL_LOG(level, _fmt_, args...) _OB_MOD_LOG(MYSQL, level, _fmt_, ##args)
#define PROXY_LOG(level, info_string, args...) OBPROXY_MOD_LOG(PROXY, level, info_string, ##args)
#define _PROXY_LOG(level, _fmt_, args...) _OBPROXY_MOD_LOG(PROXY, level, _fmt_, ##args)
#define PS_LOG(level, info_string, args...) OB_MOD_LOG(PS, level, info_string, ##args)
#define _PS_LOG(level, _fmt_, args...) _OB_MOD_LOG(PS, level, _fmt_, ##args)
#define RPC_LOG(level, info_string, args...) OB_MOD_LOG(RPC, level, info_string, ##args)
#define _RPC_LOG(level, _fmt_, args...) _OB_MOD_LOG(RPC, level, _fmt_, ##args)
#define RS_LOG(level, info_string, args...) OB_MOD_LOG(RS, level, info_string, ##args)
#define _RS_LOG(level, _fmt_, args...) _OB_MOD_LOG(RS, level, _fmt_, ##args)
#define SERVER_LOG(level, info_string, args...) OB_MOD_LOG(SERVER, level, info_string, ##args)
#define _SERVER_LOG(level, _fmt_, args...) _OB_MOD_LOG(SERVER, level, _fmt_, ##args)
#define SHARE_LOG(level, info_string, args...) OB_MOD_LOG(SHARE, level, info_string, ##args)
#define _SHARE_LOG(level, _fmt_, args...) _OB_MOD_LOG(SHARE, level, _fmt_, ##args)
#define SQL_LOG(level, info_string, args...) OB_MOD_LOG(SQL, level, info_string, ##args)
#define _SQL_LOG(level, _fmt_, args...) _OB_MOD_LOG(SQL, level, _fmt_, ##args)
#define STORAGE_LOG(level, info_string, args...) OB_MOD_LOG(STORAGE, level, info_string, ##args)
#define _STORAGE_LOG(level, _fmt_, args...) _OB_MOD_LOG(STORAGE, level, _fmt_, ##args)
#define TRANS_LOG(level, info_string, args...) OB_SUB_MOD_LOG(STORAGE, TRANS, level, info_string, ##args)
#define _TRANS_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(STORAGE, TRANS, level, _fmt_, ##args)
#define REPLAY_LOG(level, info_string, args...) OB_SUB_MOD_LOG(STORAGE, REPLAY, level, info_string, ##args)
#define _REPLAY_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(STORAGE, REPLAY, level, _fmt_, ##args)
#define OBLOG_LOG(level, info_string, args...) OB_MOD_LOG(TLOG, level, info_string, ##args)
#define _OBLOG_LOG(level, _fmt_, args...) _OB_MOD_LOG(TLOG, level, _fmt_, ##args)
#define LOGTOOL_LOG(level, info_string, args...) OB_MOD_LOG(LOGTOOL, level, info_string, ##args)
#define _LOGTOOL_LOG(level, _fmt_, args...) _OB_MOD_LOG(LOGTOOL, level, _fmt_, ##args)

//dfine ParMod_SubMod_LOG
#define LIB_ALLOC_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, ALLOC, level,     \
                                                                info_string, ##args)
#define _LIB_ALLOC_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, ALLOC, level,         \
                                                                _fmt_, ##args)
#define LIB_BTREE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, BTREE, level,             \
                                                                info_string, ##args)
#define _LIB_BTREE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, BTREE, level,                 \
                                                                _fmt_, ##args)
#define LIB_CHARSET_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, CHARSET, level,         \
                                                                info_string, ##args)
#define _LIB_CHARSET_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, CHARSET, level,             \
                                                                _fmt_, ##args)
#define LIB_CMBTREE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, CMBTREE, level,         \
                                                                info_string, ##args)
#define _LIB_CMBTREE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, CMBTREE, level,             \
                                                                _fmt_, ##args)
#define LIB_CONTAIN_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, CONTAIN, level,         \
                                                                info_string, ##args)
#define _LIB_CONTAIN_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, CONTAIN, level,             \
                                                                _fmt_, ##args)
#define LIB_CPU_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, CPU, level,                 \
                                                                info_string, ##args)
#define _LIB_CPU_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, CPU, level,                     \
                                                                _fmt_, ##args)
#define LIB_ENCRYPT_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, ENCRYPT, level,         \
                                                                info_string, ##args)
#define _LIB_ENCRYPT_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, ENCRYPT, level,             \
                                                                _fmt_, ##args)
#define LIB_FILE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, FILE, level,               \
                                                                info_string, ##args)
#define _LIB_FILE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, FILE, level,                   \
                                                                _fmt_, ##args)
#define LIB_HASH_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, HASH, level,               \
                                                                info_string, ##args)
#define _LIB_HASH_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, HASH, level,                   \
                                                                _fmt_, ##args)
#define LIB_JASON_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, JASON, level,             \
                                                                info_string, ##args)
#define _LIB_JASON_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, JASON, level,                 \
                                                                _fmt_, ##args)
#define LIB_LIST_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, LIST, level,               \
                                                                info_string, ##args)
#define _LIB_LIST_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, LIST, level,                   \
                                                                _fmt_, ##args)
#define LIB_LOCK_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, LOCK, level,               \
                                                                info_string, ##args)
#define _LIB_LOCK_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, LOCK, level,                   \
                                                                _fmt_, ##args)
#define LIB_MYSQLC_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, MYSQLC, level,           \
                                                                info_string, ##args)
#define _LIB_MYSQLC_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, MYSQLC, level,               \
                                                                _fmt_, ##args)
#define LIB_NUM_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, NUM, level,                 \
                                                                info_string, ##args)
#define _LIB_NUM_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, NUM, level,                     \
                                                                _fmt_, ##args)
#define LIB_OBJP_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, OBJP, level,               \
                                                                info_string, ##args)
#define _LIB_OBJP_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, OBJP, level,                   \
                                                                _fmt_, ##args)
#define LIB_PROB_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, PROB, level,               \
                                                                info_string, ##args)
#define _LIB_PROB_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, PROB, level,                   \
                                                                _fmt_, ##args)
#define LIB_PROFILE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, PROFILE, level,         \
                                                                info_string, ##args)
#define _LIB_PROFILE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, PROFILE, level,             \
                                                                _fmt_, ##args)
#define LIB_QUEUE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, QUEUE, level,             \
                                                                info_string, ##args)
#define _LIB_QUEUE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, QUEUE, level,                 \
                                                                _fmt_, ##args)
#define LIB_REGEX_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, REGEX, level,             \
                                                                info_string, ##args)
#define _LIB_REGEX_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, REGEX, level,                 \
                                                                _fmt_, ##args)
#define LIB_STRING_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, STRING, level,           \
                                                                info_string, ##args)
#define _LIB_STRING_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, STRING, level,               \
                                                                _fmt_, ##args)
#define LIB_TASK_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, TASK, level,               \
                                                                info_string, ##args)
#define _LIB_TASK_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, TASK, level,                   \
                                                                _fmt_, ##args)
#define LIB_TRHEADL_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, THREADL, level,         \
                                                                info_string, ##args)
#define _LIB_TRHEADL_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, THREADL, level,             \
                                                                _fmt_, ##args)
#define LIB_TIME_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, TIME, level,               \
                                                                info_string, ##args)
#define _LIB_TIME_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, TIME, level,                   \
                                                                _fmt_, ##args)
#define LIB_UTILITY_LOG(level, info_string, args...) OB_SUB_MOD_LOG(LIB, UTILITY, level,         \
                                                                info_string, ##args)
#define _LIB_UTILITY_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(LIB, UTILITY, level,             \
                                                                _fmt_, ##args)

#define RPC_FRAME_LOG(level, _fmt_, args...)    \
  OB_SUB_MOD_LOG(RPC, FRAME, level, _fmt_, ##args)

#define _RPC_FRAME_LOG(level, _fmt_, args...)   \
  _OB_SUB_MOD_LOG(RPC, FRAME, level, _fmt_, ##args)

#define RPC_OBRPC_LOG(level, _fmt_, args...)    \
  OB_SUB_MOD_LOG(RPC, OBRPC, level, _fmt_, ##args)

#define _RPC_OBRPC_LOG(level, _fmt_, args...)   \
  _OB_SUB_MOD_LOG(RPC, OBRPC, level, _fmt_, ##args)

#define RPC_OBMYSQL_LOG(level, _fmt_, args...)  \
  OB_SUB_MOD_LOG(RPC, OBMYSQL, level, _fmt_, ##args)

#define _RPC_OBMYSQL_LOG(level, _fmt_, args...) \
  _OB_SUB_MOD_LOG(RPC, OBMYSQL, level, _fmt_, ##args)

#define RPC_TEST_LOG(level, _fmt_, args...)     \
  OB_SUB_MOD_LOG(RPC, TEST, level, _fmt_, ##args)

#define _RPC_TEST_LOG(level, _fmt_, args...)    \
  _OB_SUB_MOD_LOG(RPC, TEST, level, _fmt_, ##args)


#define COMMON_CACHE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, CACHE, level,       \
                                                                info_string, ##args)
#define _COMMON_CACHE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, CACHE, level,           \
                                                                _fmt_, ##args)
#define COMMON_EXPR_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, EXPR, level,         \
                                                                info_string, ##args)
#define _COMMON_EXPR_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, EXPR, level,             \
                                                                _fmt_, ##args)
#define COMMON_LEASE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, LEASE, level,       \
                                                                info_string, ##args)
#define _COMMON_LEASE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, LEASE, level,           \
                                                                _fmt_, ##args)
#define COMMON_MYSQLP_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, MYSQLP, level,     \
                                                                info_string, ##args)
#define _COMMON_MYSQLP_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, MYSQLP, level,         \
                                                                _fmt_, ##args)
#define COMMON_PRI_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, PRI, level,           \
                                                                info_string, ##args)
#define _COMMON_PRI_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, PRI, level,               \
                                                                _fmt_, ##args)
#define COMMON_STAT_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, STAT, level,         \
                                                                info_string, ##args)
#define _COMMON_STAT_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, STAT, level,             \
                                                                _fmt_, ##args)
#define COMMON_UPSR_LOG(level, info_string, args...) OB_SUB_MOD_LOG(COMMON, UPSR, level,         \
                                                                info_string, ##args)
#define _COMMON_UPSR_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(COMMON, UPSR, level,             \
                                                                _fmt_, ##args)

#define SHARE_CONFIG_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, CONFIG,level,        \
                                                                info_string,  ##args)
#define _SHARE_CONFIG_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, CONFIG,level,            \
                                                                _fmt_,  ##args)
#define SHARE_FILE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, FILE,level,            \
                                                                info_string,  ##args)
#define _SHARE_FILE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, FILE,level,                \
                                                                _fmt_,  ##args)
#define SHARE_INNERT_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, INNERT,level,        \
                                                                info_string,  ##args)
#define _SHARE_INNERT_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, INNERT,level,            \
                                                                _fmt_,  ##args)
#define SHARE_INTERFACE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, INTERFACE,level,  \
                                                                info_string,  ##args)
#define _SHARE_INTERFACE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, INTERFACE,level,      \
                                                                _fmt_,  ##args)
#define SHARE_LOG_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, LOG,level,              \
                                                                info_string,  ##args)
#define _SHARE_LOG_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, LOG,level,                  \
                                                                _fmt_,  ##args)
#define SHARE_PT_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, PT,level,                \
                                                                info_string,  ##args)
#define _SHARE_PT_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, PT,level,                    \
                                                                _fmt_,  ##args)
#define SHARE_SCHEMA_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, SCHEMA,level,        \
                                                                info_string,  ##args)
#define _SHARE_SCHEMA_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, SCHEMA,level,            \
                                                                _fmt_,  ##args)
#define SHARE_TRIGGER_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SHARE, TRIGGER,level,      \
                                                                info_string,  ##args)
#define _SHARE_TRIGGER_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SHARE, TRIGGER,level,          \
                                                                _fmt_,  ##args)

#define STORAGE_REDO_LOG(level, info_string, args...) OB_SUB_MOD_LOG(STORAGE, REDO, level,       \
                                                                info_string,  ##args)
#define _STORAGE_REDO_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(STORAGE, REDO, level,           \
                                                                _fmt_,  ##args)
#define STORAGE_COMPACTION_LOG(level, info_string, args...) OB_SUB_MOD_LOG(STORAGE, COMPACTION, level,       \
                                                                 info_string,  ##args)
#define _STORAGE_COMPACTION_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(STORAGE, COMPACTION, level,           \
                                                                 _fmt_,  ##args)

#define SQL_ENG_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, ENG, level,                 \
                                                                info_string,  ##args)
#define _SQL_ENG_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, ENG, level,                     \
                                                                _fmt_,  ##args)
#define SQL_EXE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, EXE, level,                 \
                                                                info_string,  ##args)
#define _SQL_EXE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, EXE, level,                     \
                                                                _fmt_,  ##args)
#define SQL_OPT_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, OPT, level,                 \
                                                                info_string, ##args)
#define _SQL_OPT_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, OPT, level,                     \
                                                                _fmt_, ##args)
#define SQL_JO_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, JO, level,                   \
                                                                info_string, ##args)
#define _SQL_JO_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, JO, level,                       \
                                                                _fmt_, ##args)
#define SQL_PARSER_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, PARSER, level,           \
                                                                info_string, ##args)
#define _SQL_PARSER_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, PARSER, level,               \
                                                                _fmt_, ##args)
#define SQL_PC_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, PC, level,                   \
                                                                info_string, ##args)
#define _SQL_PC_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, PC, level,               \
                                                                _fmt_, ##args)
#define _SQL_PLANCACHE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, PLANCACHE, level,         \
                                                                _fmt_, ##args)
#define SQL_RESV_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, RESV, level,               \
                                                                info_string, ##args)
#define _SQL_RESV_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, RESV, level,                   \
                                                                _fmt_, ##args)
#define SQL_REWRITE_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, REWRITE, level,         \
                                                                info_string, ##args)
#define _SQL_REWRITE_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, REWRITE, level,             \
                                                                _fmt_, ##args)
#define SQL_SESSION_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, SESSION, level,         \
                                                                info_string, ##args)
#define _SQL_SESSION_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, SESSION, level,             \
                                                                _fmt_, ##args)
#define SQL_CG_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, CG, level,                   \
                                                               info_string, ##args)
#define _SQL_CG_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, CG, level,                       \
                                                           _fmt_, ##args)
#define SQL_MONITOR_LOG(level, info_string, args...) OB_SUB_MOD_LOG(SQL, MONITOR, level,         \
                                                                info_string, ##args)
#define _SQL_MONITOR_LOG(level, _fmt_, args...) _OB_SUB_MOD_LOG(SQL, MONITOR, level,             \
                                                                _fmt_, ##args)

// observer submodule definitions
#define SERVER_OMT_LOG(level, info_string, args...)     \
  OB_SUB_MOD_LOG(SERVER, OMT, level, info_string, ##args)
#define _SERVER_OMT_LOG(level, _fmt_, args...)          \
  _OB_SUB_MOD_LOG(SERVER, OMT, level, _fmt_, ##args)


#define PROXY_EVENT_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, EVENT, level,         \
                                                                    info_string, ##args)
#define _PROXY_EVENT_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, EVENT, level,             \
                                                                _fmt_, ##args)
#define PROXY_NET_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, NET, level,             \
                                                                  info_string, ##args)
#define _PROXY_NET_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, NET, level,                 \
                                                                _fmt_, ##args)
#define PROXY_SOCK_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, SOCK, level,           \
                                                                  info_string, ##args)
#define _PROXY_SOCK_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, SOCK, level,               \
                                                                _fmt_, ##args)
#define PROXY_TXN_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, TXN, level,             \
                                                                  info_string, ##args)
#define _PROXY_TXN_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, TXN, level,                 \
                                                                _fmt_, ##args)
#define PROXY_TUNNEL_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, TUNNEL, level,       \
                                                                  info_string, ##args)
#define _PROXY_TUNNEL_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, TUNNEL, level,           \
                                                                _fmt_, ##args)
#define PROXY_SM_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, SM, level,               \
                                                                  info_string, ##args)
#define _PROXY_SM_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, SM, level,                   \
                                                                _fmt_, ##args)
#define PROXY_CS_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, CS, level,               \
                                                                  info_string, ##args)
#define _PROXY_CS_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, CS, level,                   \
                                                                _fmt_, ##args)
#define PROXY_SS_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, SS, level,               \
                                                                  info_string, ##args)
#define _PROXY_SS_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, SS, level,                   \
                                                                _fmt_, ##args)
#define PROXY_PVC_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, PVC, level,             \
                                                                  info_string, ##args)
#define _PROXY_PVC_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, PVC, level,                 \
                                                                _fmt_, ##args)
#define PROXY_TRANSFORM_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, TRANSFORM, level, \
                                                                  info_string, ##args)
#define _PROXY_TRANSFORM_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, TRANSFORM, level,     \
                                                                _fmt_, ##args)
#define PROXY_API_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, API, level,             \
                                                                  info_string, ##args)
#define _PROXY_API_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, API, level,                 \
                                                                _fmt_, ##args)
#define PROXY_ICMD_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, ICMD, level,           \
                                                                  info_string, ##args)
#define _PROXY_ICMD_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, ICMD, level,               \
                                                                _fmt_, ##args)
#define PROXY_CMD_LOG(level, info_string, args...) OBPROXY_SUB_MOD_LOG(PROXY, ICMD, level,           \
                                                                  info_string, ##args)
#define _PROXY_CMD_LOG(level, _fmt_, args...) _OBPROXY_SUB_MOD_LOG(PROXY, ICMD, level,               \
                                                                _fmt_, ##args)
#define STORAGETEST_LOG(level, info_string, args...) OB_SUB_MOD_LOG(STORAGETEST, TEST, level,    \
                                                                   info_string, ##args)
#define _STORAGETEST_LOG(level, info_string, args...) _OB_SUB_MOD_LOG(STORAGETEST, TEST, level,  \
                                                                  info_string, ##args)

// liboblog submod definition
#define OBLOG_FETCHER_LOG(level, fmt, args...) OB_SUB_MOD_LOG(TLOG, FETCHER, level, fmt, ##args)
#define _OBLOG_FETCHER_LOG(level, fmt, args...) _OB_SUB_MOD_LOG(TLOG, FETCHER, level, fmt, ##args)
#define OBLOG_PARSER_LOG(level, fmt, args...) OB_SUB_MOD_LOG(TLOG, PARSER, level, fmt, ##args)
#define _OBLOG_PARSER_LOG(level, fmt, args...) _OB_SUB_MOD_LOG(TLOG, PARSER, level, fmt, ##args)
#define OBLOG_SEQUENCER_LOG(level, fmt, args...) OB_SUB_MOD_LOG(TLOG, SEQUENCER, level, fmt, ##args)
#define _OBLOG_SEQUENCER_LOG(level, fmt, args...) _OB_SUB_MOD_LOG(TLOG, SEQUENCER, level, fmt, ##args)
#define OBLOG_FORMATTER_LOG(level, fmt, args...) OB_SUB_MOD_LOG(TLOG, FORMATTER, level, fmt, ##args)
#define _OBLOG_FORMATTER_LOG(level, fmt, args...) _OB_SUB_MOD_LOG(TLOG, FORMATTER, level, fmt, ##args)
#define OBLOG_COMMITTER_LOG(level, fmt, args...) OB_SUB_MOD_LOG(TLOG, COMMITTER, level, fmt, ##args)
#define _OBLOG_COMMITTER_LOG(level, fmt, args...) _OB_SUB_MOD_LOG(TLOG, COMMITTER, level, fmt, ##args)
#define OBLOG_TAILF_LOG(level, fmt, args...) OB_SUB_MOD_LOG(TLOG, TAILF, level, fmt, ##args)
#define _OBLOG_TAILF_LOG(level, fmt, args...) _OB_SUB_MOD_LOG(TLOG, TAILF, level, fmt, ##args)


// used for the log return for user;
// if you want to return ERROR message for user, you should use  LOG_USER_ERROR or LOG_USER_ERROR_MSG
// if you want to return WARN message for user, you should use  LOG_USER_WARN  or LOG_USER_WARN_MSG
// the LOG_USER_*** supports string format,
// while the LOG_USER_***MSG dose not support and only can be used for executor on the remote/distribute msg.
#define _LOG_USER_MSG(level, errcode, umsg, args...)                      \
  do {\
    OB_LOGGER.log_user_message(level, errcode, umsg, ##args);\
    (OB_NEED_USE_ASYNC_LOG ? OB_LOGGER.async_log_message("", OB_LOG_NUM_LEVEL(level), umsg, ##args) : OB_LOGGER.log_message("", OB_LOG_NUM_LEVEL(level), umsg, ##args));\
  } while(0)

#define ret__USER_ERROR_MSG umsg
#define LOG_USER(level, errcode, args...)                                \
  do                                                                    \
  {                                                                     \
    const char* umsg = ::oceanbase::common::ob_str_user_error(errcode); \
    if (NULL == umsg) {                                                 \
      _LOG_USER_MSG(level, errcode,  "Unknown user error, err=%d", errcode); \
    }                                                                   \
    else {                                                              \
      _LOG_USER_MSG(level, errcode, LOG_MACRO_JOIN(errcode, __USER_ERROR_MSG), ##args);\
    }                                                                   \
  } while(0)
#define LOG_USER_ERROR(errcode, args...)                                \
  LOG_USER(::oceanbase::common::ObLogger::USER_ERROR, errcode, ##args)
#define LOG_USER_WARN(errcode, args...)                                 \
  LOG_USER(::oceanbase::common::ObLogger::USER_WARN, errcode, ##args)
#define LOG_USER_NOTE(errcode, args...)                                 \
  LOG_USER(::oceanbase::common::ObLogger::USER_NOTE, errcode, ##args)

#define LOG_USER_ERROR_MSG(errcode, msg, args...)                       \
    _LOG_USER_MSG(::oceanbase::common::ObLogger::USER_ERROR, errcode, msg, ##args);

#define LOG_USER_WARN_MSG(errcode, msg)                                  \
  OB_LOGGER.log_user_message_info("", ::oceanbase::common::ObLogger::USER_WARN, OB_LOG_LEVEL(WARN), errcode, msg)

#define LOG_USER_NOTE_MSG(errcode, msg)                                  \
  OB_LOGGER.log_user_message_info("", ::oceanbase::common::ObLogger::USER_NOTE, OB_LOG_LEVEL(WARN), errcode, msg)

#define LOG_MACRO_JOIN(x, y) LOG_MACRO_JOIN1(x, y)
#define LOG_MACRO_JOIN1(x, y) x##y

#define _LOG_MACRO_JOIN(x, y) _LOG_MACRO_JOIN1(x, y)
#define _LOG_MACRO_JOIN1(x, y) _##x##y

// define USING_LOG_PREFIX in .cpp file to use LOG_ERROR, LOG_WARN ... macros
//
// example:
//    #define USING_LOG_PREFIX COMMON
//    LOG_ERROR(...) will expand to COMMON_LOG(ERROR, ...)
#ifdef USING_LOG_PREFIX

#define LOG_XXX_MACROS_SHOULD_ONLY_BE_USED_IN_CPP_FILES__0 USING_LOG_PREFIX
#define GET_LOG_PREFIX() \
    LOG_MACRO_JOIN(LOG_XXX_MACROS_SHOULD_ONLY_BE_USED_IN_CPP_FILES__, __INCLUDE_LEVEL__)

#define LOG_ERROR(args...) LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (ERROR, ##args)
#define _LOG_ERROR(args...) _LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (ERROR, ##args)
#define LOG_WARN(args...) LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (WARN, ##args)
#define _LOG_WARN(args...) _LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (WARN, ##args)
#define LOG_INFO(args...) LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (INFO, ##args)
#define _LOG_INFO(args...) _LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (INFO, ##args)
#define LOG_TRACE(args...) LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (TRACE, ##args)
#define _LOG_TRACE(args...) _LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (TRACE, ##args)
#define LOG_DEBUG(args...) LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (DEBUG, ##args)
#define _LOG_DEBUG(args...) _LOG_MACRO_JOIN(GET_LOG_PREFIX(), _LOG) (DEBUG, ##args)

// print expr is human-readable format
#define LOG_PRINT_EXPR(level, statement, expr, args...)                                                  \
  {                                                                                                      \
    const int64_t TMP_BUF_LEN = 1024;                                                                    \
    char tmp_buf[TMP_BUF_LEN] = {0};                                                                     \
    int64_t pos = 0;                                                                                     \
    int tmp_ret = OB_SUCCESS;                                                                            \
    if (OB_ISNULL(expr)) {                                                                               \
      snprintf(tmp_buf, TMP_BUF_LEN, "NULL");                                                            \
    } else {                                                                                             \
      ObLoggerTraceMode mode;                                                                            \
      if (OB_SIZE_OVERFLOW != (tmp_ret = (expr)->get_name(tmp_buf, TMP_BUF_LEN - 1, pos))) {             \
        mode.print_trace_buffer(OB_LOG_LEVEL(WARN));                                                     \
      }                                                                                                  \
    }                                                                                                    \
    LOG_##level(statement, (OB_SUCCESS != tmp_ret) ? "expr(incomplete)" : "expr", tmp_buf, ##args);      \
  } while (0)

#endif

// When key is the name of value, can use K(value) for key and value.
// example:
//   SQL_LOG(INFO, "test for K", K(value));
// When key is the name of a member in class, can use K_
// example:
//   value_ is a member of class T.Then in class T, can use
//   SQL_LOG(INFO, "test for K_", K_(value))
#define K(x) #x, x
#define K_(x) #x, x##_
#define GETK(obj, member) #member, (obj).get_##member()

// If you declare a static const member in Class and do not define it in cpp file,
// you have to use LITERAL_K() to replace K or use static_cast<type> to print the member.
// Defining the member in cpp file is suggested.
#define LITERAL(x) static_cast<__typeof__(x)>(x)
#define LITERAL_(x) static_cast<__typeof__(x##_)>(x##_)
#define LITERAL_K(x) #x, static_cast<__typeof__(x)>(x)
#define LITERAL_K_(x) #x, static_cast<__typeof__(x##_)>(x##_)

//If you want to print the address of a variable with type 'char *' or 'const char *', you
//can use this macro P.
#define P(x) reinterpret_cast<const void *>(x)

#define KP(x) #x, reinterpret_cast<const void*>(x)
#define KP_(x) #x, reinterpret_cast<const void*>(x##_)

namespace oceanbase
{
namespace common
{
template <typename T>
struct ObLogPrintPointerCnt
{
  explicit ObLogPrintPointerCnt(T v) : v_(v)
  { }
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (NULL == v_) {
      pos = snprintf(buf, len, "NULL");
      if (pos <= 0) {
        pos = 0;
      } else if (pos >= len) {
        pos = len - 1;
      } else { }//do nothing
    } else {
      pos += v_->to_string(buf, len);
    }
    return pos;
  }
  T v_;
};
}
}

#define PC(x) ::oceanbase::common::ObLogPrintPointerCnt<__typeof__(x)>(x)
#define KPC(x) #x, ::oceanbase::common::ObLogPrintPointerCnt<__typeof__(x)>(x)
#define KPC_(x) #x, ::oceanbase::common::ObLogPrintPointerCnt<__typeof__(x##_)>(x##_)

//As strerror(err_num) not thread-safe, need to call ERRMSG, KERRMSG to print errmsg of errno.
//And call ERRNOMSG, KERRNOMSG to print errmsg of err_num specified.
#define ERRMSG ::oceanbase::common::ObLogPrintErrMsg()
#define KERRMSG "errmsg", ::oceanbase::common::ObLogPrintErrMsg()
#define KERRMSGS K(errno), "errmsg", ::oceanbase::common::ObLogPrintErrMsg()

#define ERRNOMSG(num) ::oceanbase::common::ObLogPrintErrNoMsg(num)
#define KERRNOMSG(num) "errmsg", ::oceanbase::common::ObLogPrintErrNoMsg(num)
#define KERRNOMSGS(num) K(num), "errmsg", ::oceanbase::common::ObLogPrintErrNoMsg(num)

#endif
