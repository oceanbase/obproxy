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

#ifndef OCEANBASE_COMMON_OB_DEFINE_H_
#define OCEANBASE_COMMON_OB_DEFINE_H_
// common system headers
#include <stdint.h>  // for int64_t etc.
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <execinfo.h>
#include <sys/syscall.h>
#include <unistd.h>
// basic headers, do not add other headers here
#include "lib/alloc/alloc_assist.h"     // MEMSET etc.
#include "lib/oblog/ob_log.h"  // log utilities
#include "lib/ob_errno.h"      // error codes define
#include "lib/utility/ob_macro_utils.h"  // useful utilities of macro
#include "lib/utility/ob_template_utils.h"  // useful utilities of C++ template
#include "lib/tbsys.h"                  // should be removed in future

namespace oceanbase
{
namespace common
{
const int64_t OB_ALL_SERVER_CNT                                = INT64_MAX;
const uint16_t OB_COMPACT_COLUMN_INVALID_ID                    = UINT16_MAX;
const int64_t OB_INVALID_TIMESTAMP                             = -1;
const uint64_t OB_INVALID_ID                                   = UINT64_MAX;
const int64_t OB_LATEST_VERSION                                = 0;
const uint32_t OB_INVALID_FILE_ID                              = UINT32_MAX;
const int16_t OB_COMPACT_INVALID_INDEX                         = -1;
const int OB_INVALID_INDEX                                     = -1;
const int64_t OB_INVALID_INDEX_INT64                           = -1;
const int OB_INVALID_SIZE                                      = -1;
const int OB_INVALID_COUNT                                     = -1;
const int OB_INVALID_PTHREAD_KEY                               = -1;
const int64_t OB_INVALID_VERSION                               = -1;
const int64_t OB_INVALID_STMT_ID                               = -1;
const int64_t OB_INVALID_PARTITION_ID                          = 65535;
const int64_t OB_MIN_CLUSTER_ID                                = 1;
const int64_t OB_MAX_CLUSTER_ID                                = 4294901759;
const int64_t OB_INVALID_CLUSTER_ID                            = -1;
const int64_t OB_INVALID_ORG_CLUSTER_ID                        = 0;
const int64_t OB_MAX_ITERATOR                                  = 16;
const int64_t MAX_IP_ADDR_LENGTH                               = 64;
const int64_t MAX_IP_PORT_LENGTH                               = MAX_IP_ADDR_LENGTH + 5;
const int64_t MAX_IP_PORT_SQL_LENGTH                           = MAX_IP_ADDR_LENGTH + 10;
const int64_t OB_MAX_SQL_ID_LENGTH                             = 32;
const int64_t MAX_ZONE_LENGTH                                  = 128;
const int64_t MAX_REGION_LENGTH                                = 128;
const int64_t MAX_PROXY_IDC_LENGTH                             = 128;
const int32_t MAX_ZONE_NUM                                     = 64;
const int64_t MAX_OPERATOR_NAME_LENGTH                         = 32;
const int64_t MAX_ZONE_LIST_LENGTH                             = MAX_ZONE_LENGTH * MAX_ZONE_NUM;
const int64_t MAX_ZONE_STATUS_LENGTH                           = 16;
const int64_t MAX_RESOURCE_POOL_NAME_LEN                       = 128;
const int32_t MAX_REPLICA_COUNT_PER_ZONE                       = 5;
const int32_t MAX_REPLICA_COUNT_TOTAL                          = MAX_ZONE_NUM
                                        *MAX_REPLICA_COUNT_PER_ZONE;
const int64_t MAX_RESOURCE_POOL_LENGTH                         = 128;
const int64_t MAX_RESOURCE_POOL_COUNT_OF_TENANT                = 16;
const int64_t MAX_RESOURCE_POOL_LIST_LENGTH                    = MAX_RESOURCE_POOL_LENGTH
    * MAX_RESOURCE_POOL_COUNT_OF_TENANT;
const int64_t MAX_UNIT_CONFIG_LENGTH                           = 128;
const int64_t MAX_PATH_SIZE                                    = 1024;
const int64_t DEFAULT_BUF_LENGTH                               = 4096;
const int64_t MAX_MEMBER_LIST_LENGTH                           = MAX_ZONE_NUM * (MAX_IP_PORT_LENGTH + 17 /* timestamp length*/  + 1);
const int64_t OB_MAX_MEMBER_NUMBER                             = 7;
const int64_t MAX_VALUE_LENGTH                                 = 4096;
const int64_t MAX_LLC_BITMAP_LENGTH                            = 4096;
const int64_t MAX_ROOTSERVICE_EVENT_NAME_LENGTH                = 256;
const int64_t MAX_ROOTSERVICE_EVENT_VALUE_LENGTH               = 256;
const int64_t MAX_ROOTSERVICE_EVENT_DESC_LENGTH                = 64;
const int64_t MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH          = 512;
const int64_t MAX_ELECTION_EVENT_DESC_LENGTH                   = 64;
const int64_t MAX_ELECTION_EVENT_EXTRA_INFO_LENGTH             = 512;
const int64_t MAX_BUFFER_SIZE                                  = 1024 * 1024;
typedef int64_t ObDateTime;
typedef int64_t ObPreciseDateTime;
typedef ObPreciseDateTime ObModifyTime;
typedef ObPreciseDateTime ObCreateTime;

const int32_t NOT_CHECK_FLAG                                   = 0;
const int64_t MAX_SERVER_COUNT                                 = 1024;
const uint64_t OB_SERVER_USER_ID                               = 0;
const int64_t OB_MAX_INDEX_PER_TABLE                           = 128;
const int64_t OB_MAX_SSTABLE_PER_TABLE                         = OB_MAX_INDEX_PER_TABLE + 1;
const int64_t OB_MAX_SQL_LENGTH                                = 32 * 1024;
const int64_t OB_SHORT_SQL_LENGTH                              = 1 * 1024; // 1KB
const int64_t OB_MEDIUM_SQL_LENGTH                             = 2 * OB_SHORT_SQL_LENGTH; // 2KB
const int64_t OB_MAX_SERVER_ADDR_SIZE                          = 128;
const int64_t OB_MAX_JOIN_INFO_NUMBER                          = 10;
static const int64_t OB_MAX_USER_ROW_KEY_LENGTH                = 16 * 1024L; // 16K
static const int64_t OB_MAX_ROW_KEY_LENGTH                     = 17 *
                                             1024L; // 1K for extra varchar columns of root table
const int64_t OB_MAX_ROW_KEY_SPLIT                             = 32;
const int64_t OB_USER_MAX_ROWKEY_COLUMN_NUMBER                 = 64;
const int64_t OB_MAX_ROWKEY_COLUMN_NUMBER                      = 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
const int64_t OB_MAX_COLUMN_NAME_LENGTH                        = 128; // Compatible with oracle, OB code logic is greater than Times 
const int64_t OB_MAX_COLUMN_NAMES_LENGTH                       = 2 * 1024;
const int64_t OB_MAX_APP_NAME_LENGTH                           = 128;
const int64_t OB_MAX_OPERATOR_PROPERTY_LENGTH                  = 256;
const int64_t OB_MAX_DATA_SOURCE_NAME_LENGTH                   = 128;
const int64_t OB_TRIGGER_TYPE_LENGTH                           = 32;
const int64_t OB_MAX_YUNTI_USER_LENGTH                         = 128;
const int64_t OB_MAX_YUNTI_GROUP_LENGTH                        = 128;
const int64_t OB_MAX_INSTANCE_NAME_LENGTH                      = 128;
const int64_t OB_MAX_HOST_NAME_LENGTH                          = 128;
const int64_t OB_MAX_HOST_NUM                                  = 128;
const int64_t OB_MAX_MS_TYPE_LENGTH                            = 10;
const int64_t OB_DEFAULT_MAX_PARALLEL_COUNT                    = 32;
const int64_t OB_RPC_SCAN_DEFAULT_MEM_LIMIT                    = 1024 * 1024 * 512;
const int64_t OB_RPC_SCAN_MIN_MEM_LIMIT                        = 2 * 1024 * 1024;
const int64_t OB_MAX_DEBUG_MSG_LEN                             = 1024;
const int64_t OB_MAX_COMPRESSOR_NAME_LENGTH                    = 128;
const int64_t OB_MAX_SUBQUERY_LAYER_NUM                        = 8;
const uint64_t OB_DEFAULT_GROUP_CONCAT_MAX_LEN                 = 1024;
const int64_t OB_DEFAULT_OB_INTERM_RESULT_MEM_LIMIT            = 2L * 1024L * 1024L * 1024L;
// The maximum table name length that the user can specify
const int64_t OB_MAX_USER_TABLE_NAME_LENGTH                    = 65;  // Compatible with mysql, the OB code logic is greater than the time error
// The actual maximum table name length of table_schema (the index table will have an additional prefix, so the actual length is greater than OB_MAX_USER_TABLE_NAME_LENGTH)
const int64_t OB_MAX_TABLE_NAME_LENGTH                         = 128;
const int64_t OB_MAX_TABLE_TYPE_LENGTH                         = 64;
const int64_t OB_MAX_INFOSCHEMA_TABLE_NAME_LENGTH              = 64;
const int64_t OB_MAX_FILE_NAME_LENGTH                          = 512;
const int64_t OB_MAX_TENANT_NAME_LENGTH                        = 64;
const int64_t OB_MAX_TENANT_NAME_LENGTH_STORE                  = 128;
const int64_t OB_MAX_TENANT_INFO_LENGTH                        = 4096;
const int64_t OB_MAX_PARTITION_NAME_LENGTH                     = 64;
const int64_t OB_MAX_PARTITION_DESCRIPTION_LENGTH              = 1024;
const int64_t OB_MAX_PARTITION_COMMENT_LENGTH                  = 1024;
const int64_t OB_MAX_PARTITION_METHOD_LENGTH                   = 18;
const int64_t OB_MAX_NODEGROUP_LENGTH                          = 12;
const int64_t OB_MAX_TEXT_PS_NAME_LENGTH                       = 128;
//change from 128 to 64, according to production definition document


const int64_t OB_MAX_CHAR_LEN                                  = 3;
const int64_t OB_MAX_QB_NAME_LENGTH                            = 20;  // Compatible with Oracle, hint specifies the length of the maximum qb_name.
const int64_t OB_MAX_DATABASE_NAME_LENGTH                      = 128; // Not compatible with mysql (mysql is 64), the logic is greater than when an error is reported
const int64_t OB_MAX_TABLEGROUP_NAME_LENGTH                    = 128; // OB code logic is greater than or equal to an error, so modify it to 65
const int64_t OB_MAX_ALIAS_NAME_LENGTH                         = 255; // Compatible with mysql, 255 visible characters. Plus 256 bytes at the end of 0
const int64_t OB_FIRST_PARTTITION_ID                           = 0;
const int64_t OB_MAX_USER_NAME_LENGTH                          = 64;
const int64_t OB_MAX_USER_NAME_LENGTH_STORE                    = 128;
const int64_t OB_MAX_INFOSCHEMA_GRANTEE_LEN                    = 81;
const int64_t OB_MAX_USER_INFO_LENGTH                          = 4096;
const int64_t OB_MAX_COMMAND_LENGTH                            = 4096;
const int64_t OB_MAX_SESSION_STATE_LENGTH                      = 128;
const int64_t OB_MAX_SESSION_INFO_LENGTH                       = 128;
const int64_t OB_MAX_VERSION_LENGTH                            = 256;
const int64_t COLUMN_CHECKSUM_LENGTH                           = 8 * 1024;
const int64_t OB_MAX_SYS_PARAM_INFO_LENGTH                     = 1024;
const int64_t OB_MAX_FUNC_EXPR_LENGTH                          = 128;
const int64_t OB_MAX_CACHE_NAME_LENGTH                         = 127;
const int64_t OB_MAX_WAIT_EVENT_NAME_LENGTH                    = 64;
const int64_t OB_MAX_WAIT_EVENT_PARAM_LENGTH                   = 64;
const int64_t OB_MAX_TWO_OPERATOR_EXPR_LENGTH                  = 256;
const int64_t OB_MAX_OPERATOR_NAME_LENGTH                      = 128;
const int64_t OB_MAX_SECTION_NAME_LENGTH                       = 128;
const int64_t OB_MAX_FLAG_NAME_LENGTH                          = 128;
const int64_t OB_MAX_FLAG_VALUE_LENGTH                         = 512;
const int64_t OB_MAX_TOKEN_BUFFER_LENGTH                       = 80;
const int64_t OB_MAX_PACKET_LENGTH                             = 1 << 26; // max packet length, 64MB
const int64_t OB_MAX_ROW_NUMBER_PER_QUERY                      = 65536;
const int64_t OB_MAX_BATCH_NUMBER                              = 100;
const int64_t OB_MAX_TABLET_LIST_NUMBER                        = 64;
const int64_t OB_MAX_DISK_NUMBER                               = 16; // must no more than ObTimer::MAX_TASK_NUM
const int64_t OB_MAX_TIME_STR_LENGTH                           = 64;
const int64_t OB_IP_PORT_STR_BUFF                              = 64;
const int64_t OB_RANGE_STR_BUFSIZ                              = 512;
const int64_t OB_MAX_FETCH_CMD_LENGTH                          = 2048;
const int64_t OB_MAX_EXPIRE_INFO_STRING_LENGTH                 = 4096;
const int64_t OB_MAX_PART_FUNC_EXPR_LENGTH                     = 4096;
const int64_t OB_MAX_PART_FUNC_BIN_EXPR_LENGTH                 = 2 * OB_MAX_PART_FUNC_EXPR_LENGTH;
const int64_t OB_MAX_PART_FUNC_BIN_EXPR_STRING_LENGTH          = 2 * OB_MAX_PART_FUNC_BIN_EXPR_LENGTH + 1;
const int64_t OB_MAX_THREAD_AIO_BUFFER_MGR_COUNT               = 32;
const int64_t OB_MAX_GET_ROW_NUMBER                            = 10240;
const uint64_t OB_FULL_ROW_COLUMN_ID                           = 0;
const uint64_t OB_DELETE_ROW_COLUMN_ID                         = 0;
const int64_t OB_DIRECT_IO_ALIGN_BITS                          = 9;
const int64_t OB_DIRECT_IO_ALIGN                               = 1 << OB_DIRECT_IO_ALIGN_BITS;
const int64_t OB_MAX_COMPOSITE_SYMBOL_COUNT                    = 256;
const int64_t OB_SERVER_STATUS_LENGTH                          = 64;
const int64_t OB_SERVER_VERSION_LENGTH                         = 256;
const int64_t OB_SERVER_TYPE_LENGTH                            = 64;
const int64_t OB_MAX_HOSTNAME_LENGTH                           = 60;
const int64_t OB_MAX_USERNAME_LENGTH                           = 32;
const int64_t OB_MAX_PASSWORD_LENGTH                           = 128;
const int64_t OB_MAX_ERROR_CODE_LEN                            = 8;
const int64_t OB_MAX_ERROR_MSG_LEN                             = 512;
const int64_t OB_MAX_RESULT_MESSAGE_LENGTH                     = 1024;
const int64_t OB_MAX_DEFINER_LENGTH= OB_MAX_USER_NAME_LENGTH_STORE + OB_MAX_HOST_NAME_LENGTH + 1; //user@host
const int64_t OB_MAX_SECURITY_TYPE_LENGTH                      = 7; //definer or invoker
const int64_t OB_MAX_READ_ONLY_STATE_LENGTH                    = 16;
//At present, the log module reads and writes the buffer using OB_MAX_LOG_BUFFER_SIZE,
//the length of the transaction submitted to the log module is required to be less than the length of the log module can read
//and write the log, minus the length of the log header, the BLOCK header and the EOF, here is defined a length minus 1024B
const int64_t OB_MAX_LOG_ALLOWED_SIZE                          = 1965056L; //OB_MAX_LOG_BUFFER_SIZE - 1024B
const int64_t OB_MAX_LOG_BUFFER_SIZE                           = 1966080L;  // 1.875MB
const int64_t OB_MAX_TRIGGER_VCHAR_PARAM_LENGTH                = 128;
const int64_t OB_TRIGGER_MSG_LENGTH                            = 3 * MAX_IP_ADDR_LENGTH
                                      + OB_TRIGGER_TYPE_LENGTH + 3 * OB_MAX_TRIGGER_VCHAR_PARAM_LENGTH;
const int32_t OB_SAFE_COPY_COUNT                               = 3;
const int32_t OB_DEFAULT_REPLICA_NUM                           = 3;
const int32_t OB_DEC_AND_LOCK                                  = 2626; /* used by remoe_plan in ObPsStore */
const int32_t OB_MAX_SCHEMA_VERSION_INTERVAL                   = 40 * 1000 * 1000;  // 40s

const int32_t OB_MAX_SUB_GET_REQUEST_NUM                       = 256;
const int32_t OB_DEFAULT_MAX_GET_ROWS_PER_SUBREQ               = 20;

const int64_t OB_MPI_MAX_PARTITION_NUM                         = 128;
const int64_t OB_MPI_MAX_TASK_NUM                              = 256;

static const int64_t OB_MAX_TABLE_NUM_PER_STMT                 = 256;
static const int64_t OB_TMP_BUF_SIZE_256                       = 256;
static const int64_t OB_SCHEMA_MGR_MAX_USED_TID_MAP_BUCKET_NUM = 64;
static const int64_t OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM         = 8;

//plan cache
const int64_t OB_PC_NOT_PARAM_COUNT                            = 8;
const int64_t OB_PC_SPECIAL_PARAM_COUNT                        = 16;
const int64_t OB_PC_RAW_PARAM_COUNT                            = 128;
const int64_t OB_PLAN_CACHE_PERCENTAGE                         = 20;
const int64_t OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE              = 90;
const int64_t OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE               = 50;

// time zone info
const int64_t OB_MAX_TZ_ABBR_LEN = 32;  // according to statistics
const int64_t OB_MAX_TZ_NAME_LEN = 64;  // according to statistics
const int64_t OB_INVALID_TZ_ID = -1;
const int64_t OB_INVALID_TZ_TRAN_TIME = INT64_MIN;

// OceanBase Log Synchronization Type
const int64_t OB_LOG_NOSYNC                                    = 0;
const int64_t OB_LOG_SYNC                                      = 1;
const int64_t OB_LOG_DELAYED_SYNC                              = 2;
const int64_t OB_LOG_NOT_PERSISTENT                            = 4;

const int64_t OB_MAX_UPS_LEASE_DURATION_US                     = INT64_MAX;

const int64_t OB_EXECABLE                                      = 1;
const int64_t OB_WRITEABLE                                     = 2;
const int64_t OB_READABLE                                      = 4;
const int64_t OB_SCHEMA_START_VERSION                          = 100;
const int64_t OB_SYS_PARAM_ROW_KEY_LENGTH                      = 192;
const int64_t OB_MAX_SYS_PARAM_NAME_LENGTH                     = 128;
const int64_t OB_MAX_SYS_PARAM_VALUE_LENGTH                    = 1024;
const int64_t OB_MAX_SYS_PARAM_NUM                             = 500;
const int64_t OB_MAX_PREPARE_STMT_NUM_PER_SESSION              = 512;
const int64_t OB_MAX_VAR_NUM_PER_SESSION                       = 1024;
// The maximum time set by the user through hint/set session.ob_query_timeout/set session.ob_tx_timeout is 102 years
// The purpose of this is to avoid that when the user enters a value that is too large, adding the current timestamp causes the MAX_INT64 to overflow
const int64_t OB_MAX_USER_SPECIFIED_TIMEOUT                    =  102L * 365L * 24L * 60L * 60L * 1000L * 1000L;
const int64_t OB_MAX_PROCESS_TIMEOUT                           = 5L * 60L * 1000L * 1000L; // 5m
const int64_t OB_DEFAULT_SESSION_TIMEOUT                       = 100L * 1000L * 1000L; // 10s
const int64_t OB_DEFAULT_STMT_TIMEOUT                          = 30L * 1000L * 1000L; // 30s
const int64_t OB_DEFAULT_INTERNAL_TABLE_QUERY_TIMEOUT          = 10L * 1000L * 1000L; // 10s
const int64_t OB_DEFAULT_STREAM_WAIT_TIMEOUT                   = 10L * 1000L * 1000L; // 10s
const int64_t OB_DEFAULT_JOIN_BATCH_COUNT                      = 10000;
const int64_t OB_AIO_TIMEOUT_US                                = 5L * 1000L * 1000L; //5s
const int64_t OB_DEFAULT_TENANT_COUNT                          = 100000; //10w
const int64_t OB_ONLY_SYS_TENANT_COUNT                         = 2;
const int64_t OB_MAX_SERVER_SESSION_CNT                        = 32767;
const int64_t OB_MAX_SERVER_TENANT_CNT                         = 1000;
const int64_t OB_RECYCLE_MACRO_BLOCK_DURATION                  = 10 * 60 * 1000 * 1000LL; // 10 minutes
const int64_t OB_MAX_PARTITION_NUM_PER_SERVER                  = 100000; //10w
const int64_t OB_MAX_TIME                                      = 3020399000000;
// Max add partition member timeout.
// Used to make sure no member added after lease expired + %OB_MAX_ADD_MEMBER_TIMEOUT
const int64_t OB_MAX_ADD_MEMBER_TIMEOUT                        = 60L * 1000L * 1000L; // 1 minute
const int64_t OB_MAX_PACKET_FLY_TS                             = 100 * 1000L; // 100ms
//Oceanbase network protocol
/*  4bytes  4bytes  4bytes   4bytes
 * -----------------------------------
 * | flag |  dlen  | chid | reserved |
 * -----------------------------------
 */
const uint32_t OB_NET_HEADER_LENGTH                            = 16;  //16 bytes packet header
const uint32_t OB_MAX_RPC_PACKET_LENGTH
                                                               = static_cast<uint32_t>(OB_MAX_PACKET_LENGTH - OB_NET_HEADER_LENGTH);

const int OB_TBNET_PACKET_FLAG                                 = 0x416e4574;
const int OB_SERVER_ADDR_STR_LEN                               = 128; //used for buffer size of easy_int_addr_to_str

/*   3bytes   1 byte
 * ------------------
 * |   len  |  seq  |
 * ------------------
 */
const int64_t OB_MYSQL_HEADER_LENGTH                           = 4; /** 3bytes length + 1byte seq*/

const int64_t OB_UPS_START_MAJOR_VERSION                       = 2;
const int64_t OB_UPS_START_MINOR_VERSION                       = 1;

const int64_t OB_NEWEST_DATA_VERSION                           = -2;

const int32_t OB_CONNECTION_FREE_TIME_S                        = 240;

/// @see ob_object.cpp and ob_expr_obj.cpp
static const float OB_FLOAT_EPSINON                            = static_cast<float>(1e-6);
static const double OB_DOUBLE_EPSINON                          = 1e-14;

const uint64_t OB_UPS_MAX_MINOR_VERSION_NUM                    = 2048;
const int64_t OB_MAX_COMPACTSSTABLE_NUM                        = 64;
const int32_t OB_UPS_LIMIT_RATIO                               = 2;

const int64_t OB_MERGED_VERSION_INIT                           = 1;

const int64_t OB_TRACE_BUFFER_SIZE                             = 4 * 1024; //4k
const int64_t OB_TRACE_STAT_BUFFER_SIZE= 200; //200


const int64_t OB_MAX_VERSION_COUNT                             = 64;// max version count
const int64_t OB_EASY_HANDLER_COST_TIME                        = 5 * 1000; // 5ms

enum DBServerType
{
  DB_MYSQL = 0,
  DB_OB_MYSQL,
  DB_OB_ORACLE,
  DB_MAX
};

enum ObServerRole
{
  OB_INVALID = 0,
  OB_ROOTSERVER = 1,  // rs
  OB_CHUNKSERVER = 2, // cs
  OB_MERGESERVER = 3, // ms
  OB_UPDATESERVER = 4, // ups
  OB_PROXYSERVER = 5,
  OB_SERVER = 6,
  OB_PROXY = 7,
  OB_OBLOG = 8, // liboblog
};


enum ObServerManagerOp
{
  OB_SHUTDOWN = 1, OB_RESTART = 2, OB_ADD = 3, OB_DELETE = 4,
};

static const int OB_FAKE_MS_PORT                     = 2828;
static const uint64_t OB_MAX_PS_PARAM_COUNT          = 65535;
static const uint64_t OB_MAX_PS_FIELD_COUNT          = 65535;
// OB_ALL_MAX_COLUMN_ID must <= 65535, it is used in ob_cs_create_plan.h
static const uint64_t OB_ALL_MAX_COLUMN_ID           = 65535;
// internal columns id
const uint64_t OB_NOT_EXIST_COLUMN_ID                = 0;
const uint64_t OB_HIDDEN_PK_INCREMENT_COLUMN_ID      = 1;  //hidden pk contain 3 column (seq, cluster_id, partition_id)
const uint64_t OB_CREATE_TIME_COLUMN_ID              = 2;
const uint64_t OB_MODIFY_TIME_COLUMN_ID              = 3;
const uint64_t OB_HIDDEN_PK_CLUSTER_COLUMN_ID        = 4;
const uint64_t OB_HIDDEN_PK_PARTITION_COLUMN_ID      = 5;
const int64_t OB_END_RESERVED_COLUMN_ID_NUM          = 16;
const uint64_t OB_APP_MIN_COLUMN_ID                  = 16;
const uint64_t OB_ACTION_FLAG_COLUMN_ID              = OB_ALL_MAX_COLUMN_ID
                                          - OB_END_RESERVED_COLUMN_ID_NUM + 1; /* 65520 */
const uint64_t OB_MAX_TMP_COLUMN_ID                  = OB_ALL_MAX_COLUMN_ID
                                      - OB_END_RESERVED_COLUMN_ID_NUM;

const char *const OB_UPDATE_MSG_FMT                  = " Rows matched: %ld  Changed: %ld  Warnings: %ld";
const char *const OB_INSERT_MSG_FMT                  = " Records: %ld  Duplicates: %ld  Warnings: %ld";
const char OB_PADDING_CHAR                           = ' ';
const char OB_PADDING_BINARY                         = '\0';
const char *const OB_VALUES                          = "__values";
// hidden primary key name
const char *const OB_HIDDEN_PK_INCREMENT_COLUMN_NAME = "__pk_increment"; //hidden
const char *const OB_HIDDEN_PK_CLUSTER_COLUMN_NAME   = "__pk_cluster_id";
const char *const OB_HIDDEN_PK_PARTITION_COLUMN_NAME = "__pk_partition_id";


// internal index prefix
const char *const OB_INDEX_PREFIX                    = "__idx_";

// internal user
const char *const OB_INTERNAL_USER                   = "__ob_server";

const char *const OB_SERVER_ROLE_VAR_NAME            = "__ob_server_role";
//trace id
const char *const OB_TRACE_ID_VAR_NAME               = "__ob_trace_id";
const int64_t MAX_IP_BUFFER_LEN                      = 32;

///////////////////////////////////////////////////////////
//                 SYSTEM TABLES                         //
///////////////////////////////////////////////////////////
// SYTEM TABLES ID (0, 500), they should not be mutated
static const uint64_t OB_NOT_EXIST_TABLE_TID         = 0;
///////////////////////////////////////////////////////////
//                 VIRUTAL TABLES                        //
///////////////////////////////////////////////////////////
// virtual table ID for SHOW statements start from 601
static const uint64_t OB_LAST_SHOW_TID               = 611;
///////////////////////////////////////////////////////////
//            ini schema                                 //
///////////////////////////////////////////////////////////
const char *const OB_BACKUP_SCHEMA_FILE_PATTERN      = "etc/%s.schema.bin";

////////////////////////////////////////////////////////////
//                  schema variables length               //
////////////////////////////////////////////////////////////
static const int64_t TEMP_ROWKEY_LENGTH                     = 64;
static const int64_t SERVER_TYPE_LENGTH                     = 16;
static const int64_t SERVER_STAT_LENGTH                     = 64;
static const int64_t TABLE_MAX_KEY_LENGTH                   = 128;
static const int64_t TABLE_MAX_VALUE_LENGTH                 = 128;
static const int64_t MAX_ZONE_INFO_LENGTH                   = 4096;
static const int64_t UPS_SESSION_TYPE_LENGTH                = 64;
static const int64_t UPS_MEMTABLE_LOG_LENGTH                = 128;
static const int64_t COLUMN_TYPE_LENGTH                     = 64;
static const int64_t COLUMN_NULLABLE_LENGTH                 = 4;
static const int64_t COLUMN_KEY_LENGTH                      = 4;
static const int64_t COLUMN_DEFAULT_LENGTH                  = 4 * 1024;
static const int64_t COLUMN_EXTRA_LENGTH                    = 4 * 1024;
static const int64_t DATABASE_DEFINE_LENGTH                 = 4 * 1024;
static const int64_t TABLE_DEFINE_LENGTH                    = 4 * 1024;
static const int64_t TENANT_DEFINE_LENGTH                   = 4 * 1024;
static const int64_t ROW_FORMAT_LENGTH                      = 10;
static const int64_t MAX_ENGINE_LENGTH                      = 64;
static const int64_t MAX_CHARSET_LENGTH                     = 128;
static const int64_t MAX_CHARSET_DESCRIPTION_LENGTH         = 64;
static const int64_t MAX_COLLATION_LENGTH                   = 128;
static const int64_t MAX_TABLE_STATUS_CREATE_OPTION_LENGTH  = 1024;
static const int64_t MAX_BOOL_STR_LENGTH                    = 4;
static const int64_t INDEX_SUB_PART_LENGTH= 256;
static const int64_t INDEX_PACKED_LENGTH                    = 256;
static const int64_t INDEX_NULL_LENGTH                      = 128;
static const int64_t MAX_GRANT_LENGTH                       = 1024;
static const int64_t MAX_SQL_PATH_LENGTH                    = 512;
static const int64_t MAX_TENANT_COMMENT_LENGTH              = 4096;
static const int64_t MAX_DATABASE_COMMENT_LENGTH            = 2048;
static const int64_t MAX_TABLE_COMMENT_LENGTH               = 4096;
static const int64_t MAX_INDEX_COMMENT_LENGTH               = 2048;
static const int64_t MAX_TABLEGROUP_COMMENT_LENGTH          = 4096;
static const int64_t MAX_VERSION_LENGTH                     = 128;
static const int64_t MAX_FREEZE_STATUS_LENGTH               = 64;
static const int64_t MAX_FREEZE_SUBMIT_STATUS_LENGTH        = 64;
static const int64_t MAX_REPLAY_LOG_TYPE_LENGTH             = 64;
//columns
static const int64_t MAX_TABLE_CATALOG_LENGTH               = 4096;
static const int64_t MAX_COLUMN_COMMENT_LENGTH              = 2048;  // Consistent with mysql, changed from 1024 to 2048
static const int64_t MAX_COLUMN_KEY_LENGTH                  = 3;
static const int64_t MAX_NUMERIC_PRECISION_LENGTH           = 9;
static const int64_t MAX_NUMERIC_SCALE_LENGTH               = 9;
static const int64_t MAX_COLUMN_PRIVILEGE_LENGTH            = 200;
static const int64_t MAX_PRIVILEGE_CONTEXT_LENGTH           = 80;
static const int64_t MAX_INFOSCHEMA_COLUMN_PRIVILEGE_LENGTH = 64;
static const int64_t MAX_COLUMN_YES_NO_LENGTH               = 3;
static const int64_t MAX_COLUMN_VARCHAR_LENGTH              = 262143;
static const int64_t MAX_COLUMN_CHAR_LENGTH                 = 255;

////////////////////////////////////////////////////////////
//             table id range definition                  //
////////////////////////////////////////////////////////////

// must keep same with generate_inner_table_schema.py
// don't use share/inner_table/ob_inner_table_schema.h to avoid dependence.
static const int64_t OB_SCHEMA_CODE_VERSION                 = 1;
const uint64_t OB_MAX_CORE_TABLE_ID                         = 100;
const uint64_t OB_MAX_SYS_TABLE_ID                          = 10000;
const uint64_t OB_MAX_VIRTUAL_TABLE_ID                      = 20000;
const uint64_t OB_MAX_SYS_VIEW_ID                           = 30000;
static const uint64_t OB_MIN_USER_TABLE_ID                  = 50000;
static const uint64_t OB_MIN_SHADOW_COLUMN_ID               = 32767;
static const uint64_t OB_MAX_SYS_POOL_ID                    = 100;

// ddl related
static const char *const OB_SYS_USER_NAME                   = "root";
static const char *const OB_SYS_TENANT_NAME                 = "sys";

static const int64_t DEFAULT_MAX_SYS_MEMORY                 = 32L << 30;
static const int64_t DEFAULT_MIN_SYS_MEMORY                 = 28L << 30;
static const double DEFAULT_MAX_SYS_CPU                     = 5.;

static const double MIN_SYS_TENANT_QUOTA                    = 2.5;
static const double MIN_TENANT_QUOTA                        = .5;
static const double EXT_LOG_TENANT_CPU                      = 4.;
static const int64_t EXT_LOG_TENANT_MEMORY_LIMIT            = 4L << 30;

static const uint64_t OB_INVALID_TENANT_ID                  = 0;
static const uint64_t OB_SYS_TENANT_ID                      = 1;
static const uint64_t OB_SERVER_TENANT_ID                   = 500;
static const uint64_t OB_ELECT_TENANT_ID                    = 501;
static const uint64_t OB_LOC_CORE_TENANT_ID                 = 502;
static const uint64_t OB_LOC_ROOT_TENANT_ID                 = 503;
static const uint64_t OB_LOC_SYS_TENANT_ID                  = 504;
static const uint64_t OB_LOC_USER_TENANT_ID                 = 505;
static const uint64_t OB_EXT_LOG_TENANT_ID                  = 506;
static const uint64_t OB_MAX_RESERVED_TENANT_ID             = 1000;

static const uint64_t OB_SYS_USER_ID                        = 1;
static const uint64_t OB_EMPTY_USER_ID                      = 2;
static const uint64_t OB_SYS_TABLEGROUP_ID                  = 1;
static const char* const  OB_SYS_TABLEGROUP_NAME            = "oceanbase";
static const uint64_t OB_SYS_DATABASE_ID                    = 1;
static const char* const  OB_SYS_DATABASE_NAME              = "oceanbase";
static const uint64_t OB_INFORMATION_SCHEMA_ID              = 2;
static const char* const  OB_INFORMATION_SCHEMA_NAME        = "information_schema";
static const uint64_t OB_MYSQL_SCHEMA_ID                    = 3;
static const char* const OB_MYSQL_SCHEMA_NAME               = "mysql";
static const char* const OB_TEST_SCHEMA_NAME                = "test";
static const uint64_t OB_SYS_UNIT_CONFIG_ID                 = 1;
static const char * const OB_SYS_UNIT_CONFIG_NAME           = "sys_unit_config";
static const uint64_t OB_SYS_RESOURCE_POOL_ID               = 1;
static const uint64_t OB_SYS_UNIT_ID                        = 1;
static const uint64_t OB_INIT_SERVER_ID                     = 1;

static const uint64_t OB_SCHEMATA_TID                       = 2001;
static const char* const  OB_SCHEMATA_TNAME                 = "schemata";
static const char* const OB_MYSQL50_TABLE_NAME_PREFIX       = "#mysql50#";

static const uint64_t OB_USER_TENANT_ID                     = 1000;
static const uint64_t OB_USER_TABLEGROUP_ID                 = 1000;
static const uint64_t OB_USER_DATABASE_ID                   = 1000;
static const uint64_t OB_USER_ID                            = 1000;
static const uint64_t OB_USER_UNIT_CONFIG_ID                = 1000;
static const uint64_t OB_USER_RESOURCE_POOL_ID              = 1000;
static const uint64_t OB_USER_UNIT_ID                       = 1000;
static const uint64_t OB_USER_SEQUENCE_ID                   = 0;
static const uint64_t OB_USER_OUTLINE_ID                    = 1000;
static const uint64_t OB_USER_START_DATABASE_ID             = 1050;//OB_USER_DATABASE_ID = 1000; 50 are reserved to initialize the user database added by default when the tenant is initialized, such as the test library

static const char* const OB_PRIMARY_INDEX_NAME              = "PRIMARY";

static const int64_t OB_MAX_CONFIG_URL_LENGTH               = 512;

static const double OB_UNIT_MIN_CPU                         = 0.1;
static const int64_t OB_UNIT_MIN_MEMORY                     = 1024LL * 1024LL * 1024LL; //1G
static const int64_t OB_UNIT_MIN_DISK_SIZE                  = 512LL * 1024LL * 1024LL; //512MB
static const int64_t OB_UNIT_MIN_IOPS                       = 128;
static const int64_t OB_UNIT_MIN_SESSION_NUM                = 64;

// for array log print
static const int64_t OB_LOG_KEEP_SIZE                       = 512;
//no need keep size for async
static const int64_t OB_ASYNC_LOG_KEEP_SIZE                 = 0;
static const char* const OB_LOG_ELLIPSIS                    = "...";

static const char *const DEFAULT_REGION_NAME                = "default_region";
static const char *const DEFAULT_PROXY_IDC_NAME             = "default_idc";

// for obproxy
static const char *const OB_MYSQL_CLIENT_MODE               = "__mysql_client_type";
static const char *const OB_MYSQL_CLIENT_OBPROXY_MODE       = "__ob_proxy";
static const char *const OB_MYSQL_CONNECTION_ID             = "__connection_id";
static const char *const OB_MYSQL_GLOBAL_VARS_VERSION       = "__global_vars_version";
static const char *const OB_MYSQL_PROXY_CONNECTION_ID       = "__proxy_connection_id";
static const char *const OB_MYSQL_CLUSTER_NAME              = "__cluster_name";
static const char *const OB_MYSQL_CLUSTER_ID                = "__cluster_id";
static const char *const OB_MYSQL_CLIENT_IP                 = "__client_ip";
static const char *const OB_MYSQL_CAPABILITY_FLAG           = "__proxy_capability_flag";
static const char *const OB_MYSQL_PROXY_SESSION_VARS        = "__proxy_session_vars";
static const char *const OB_MYSQL_SCRAMBLE                  = "__proxy_scramble";
static const char *const OB_MYSQL_PROXY_VERSION             = "__proxy_version";

// another conn attr for different client type which is supported ob2.0 protocol or not
static const char *const OB_MYSQL_CLIENT_LIBOBCLIENT_MODE = "__ob_libobclient";
static const char *const OB_MYSQL_CLIENT_JDBC_CLIENT_MODE = "__ob_jdbc_client";

// for java client
static const char *const OB_MYSQL_JAVA_CLIENT_MODE_NAME     = "__ob_java_client";

// conn attr which transparent transit to observer
static const char *const OB_MYSQL_OB_CLIENT = "__ob_client";

// for obproxy and observer compatibility
enum ObCapabilityFlagShift
{
  OB_CAP_PARTITION_TABLE_SHIFT = 0,
  OB_CAP_CHANGE_USER_SHIFT,
  OB_CAP_READ_WEAK_SHIFT,
  OB_CAP_CHECKSUM_SHIFT,
  OB_CAP_SAFE_WEAK_READ_SHIFT,
  OB_CAP_PRIORITY_HIT_SHIFT,
  OB_CAP_CHECKSUM_SWITCH_SHIFT,
  OB_CAP_OCJ_ENABLE_EXTRA_OK_PACKET_SHIFT,
  OB_CAP_OB_PROTOCOL_V2_SHIFT,                      // 8
  OB_CAP_EXTRA_OK_PACKET_FOR_STATISTICS_SHIFT,
  OB_CAP_ABUNDANT_FEEDBACK,
  OB_CAP_PL_ROUTE_SHIFT,
  OB_CAP_PROXY_REROUTE_SHIFT,
  OB_CAP_PROXY_SESSION_SYNC_SHIFT,                  // 13
  OB_CAP_PROXY_FULL_LINK_TRACING_SHIFT,             // 14
  OB_CAP_PROXY_NEW_EXTRA_INFO_SHIFT,                // 15
  OB_CAP_PROXY_SESSION_VAR_SYNC_SHIFT,              // 16
  OB_CAP_PROXY_READ_STALE_FEEDBACK_SHIFT,           // 17
  OB_CAP_PROXY_FULL_LINK_TRACING_EXT_SHIFT,         // 18
  OB_CAP_SERVER_DUP_SESS_INFO_SYNC_SHIFT,           // 19
};

#define OB_TEST_CAPABILITY(cap, tg_cap) (((cap) & (tg_cap)) == (tg_cap))
#define OB_CAP_GET_TYPE(i)        (1LL << i)
#define OB_CAP_PARTITION_TABLE    OB_CAP_GET_TYPE(common::OB_CAP_PARTITION_TABLE_SHIFT)
#define OB_CAP_CHANGE_USER        OB_CAP_GET_TYPE(common::OB_CAP_CHANGE_USER_SHIFT)
#define OB_CAP_READ_WEAK          OB_CAP_GET_TYPE(common::OB_CAP_READ_WEAK_SHIFT)
#define OB_CAP_CHECKSUM           OB_CAP_GET_TYPE(common::OB_CAP_CHECKSUM_SHIFT)
#define OB_CAP_SAFE_WEAK_READ     OB_CAP_GET_TYPE(common::OB_CAP_SAFE_WEAK_READ_SHIFT)
#define OB_CAP_PRIORITY_HIT       OB_CAP_GET_TYPE(common::OB_CAP_PRIORITY_HIT_SHIFT)
#define OB_CAP_CHECKSUM_SWITCH    OB_CAP_GET_TYPE(common::OB_CAP_CHECKSUM_SWITCH_SHIFT)
#define OB_CAP_OB_PROTOCOL_V2     OB_CAP_GET_TYPE(common::OB_CAP_OB_PROTOCOL_V2_SHIFT)
#define OB_CAP_EXTRA_OK_PACKET_FOR_STATISTICS   OB_CAP_GET_TYPE(common::OB_CAP_EXTRA_OK_PACKET_FOR_STATISTICS_SHIFT)
#define OB_CAP_PL_ROUTE           OB_CAP_GET_TYPE(common::OB_CAP_PL_ROUTE_SHIFT)
#define OB_CAP_PROXY_REROUTE      OB_CAP_GET_TYPE(common::OB_CAP_PROXY_REROUTE_SHIFT)
#define OB_CAP_PROXY_SESSION_SYNC OB_CAP_GET_TYPE(common::OB_CAP_PROXY_SESSION_SYNC_SHIFT)
#define OB_CAP_PROXY_FULL_LINK_TRACING OB_CAP_GET_TYPE(common::OB_CAP_PROXY_FULL_LINK_TRACING_SHIFT)
#define OB_CAP_PROXY_NEW_EXTRA_INFO OB_CAP_GET_TYPE(common::OB_CAP_PROXY_NEW_EXTRA_INFO_SHIFT)
#define OB_CAP_PROXY_SESSION_VAR_SYNC OB_CAP_GET_TYPE(common::OB_CAP_PROXY_SESSION_VAR_SYNC_SHIFT)
#define OB_CAP_PROXY_READ_STALE_FEEDBACK   OB_CAP_GET_TYPE(common::OB_CAP_PROXY_READ_STALE_FEEDBACK_SHIFT)
#define OB_CAP_PROXY_FULL_LINK_TRACING_EXT OB_CAP_GET_TYPE(common::OB_CAP_PROXY_FULL_LINK_TRACING_EXT_SHIFT)
#define OB_CAP_SERVER_DUP_SESS_INFO_SYNC OB_CAP_GET_TYPE(common::OB_CAP_SERVER_DUP_SESS_INFO_SYNC_SHIFT)

// for obproxy debug
#define OBPROXY_DEBUG 0

#if OBPROXY_DEBUG
static const char* const OB_MYSQL_PROXY_SESSION_ID = "session_id";
static const char* const OB_MYSQL_PROXY_TIMESTAMP = "proxy_time_stamp";
static const char* const OB_MYSQL_PROXY_SYNC_VERSION = "proxy_sync_version";

static const char* const OB_MYSQL_SERVER_SESSION_ID = "server_session_id";
static const char* const OB_MYSQL_SERVER_SQL = "sql";
static const char* const OB_MYSQL_SERVER_HANDLE_TIMESTAMP = "server_handle_time_stamp";
static const char* const OB_MYSQL_SERVER_RECEIVED_TIMESTAMP = "server_receive_time_stamp";
#endif


#define OB_TENANT_ID_SHIFT 40
OB_INLINE uint64_t extract_tenant_id(uint64_t id)
{
  return id >> OB_TENANT_ID_SHIFT;
}

OB_INLINE uint64_t extract_pure_id(uint64_t id)
{
  return id & (~(UINT64_MAX << OB_TENANT_ID_SHIFT));
}

OB_INLINE uint64_t combine_id(uint64_t tenant_id, uint64_t pure_id)
{
  return (tenant_id << OB_TENANT_ID_SHIFT) | extract_pure_id(pure_id);
}

#define OB_PART_ID_SHIFT 32
#define OB_PART_IDS_BITNUM 28

OB_INLINE bool is_core_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (OB_INVALID_ID != id) && (id <= OB_MAX_CORE_TABLE_ID);
}

OB_INLINE bool is_sys_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id <= OB_MAX_SYS_TABLE_ID);
}

OB_INLINE bool is_virtual_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_SYS_TABLE_ID && id <= OB_MAX_VIRTUAL_TABLE_ID);
}

OB_INLINE bool is_sys_view(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_VIRTUAL_TABLE_ID && id <= OB_MAX_SYS_VIEW_ID);
}

OB_INLINE bool is_reserved_id(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id > OB_MAX_SYS_VIEW_ID && id <= OB_MIN_USER_TABLE_ID);
}

OB_INLINE bool is_inner_table(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id <= OB_MIN_USER_TABLE_ID);
}

OB_INLINE bool is_inner_table_with_partition(const uint64_t tid)
{
  const uint64_t id = extract_pure_id(tid);
  return (id <= OB_MAX_SYS_TABLE_ID);
}


OB_INLINE bool is_sys_resource_pool(const uint64_t resource_pool_id)
{
  return (OB_INVALID_ID != resource_pool_id) && (resource_pool_id <= OB_MAX_SYS_POOL_ID);
}

// ob_malloc & ob_tc_malloc
static const int64_t OB_MALLOC_NORMAL_BLOCK_SIZE             = (1LL << 13);                 // 8KB
static const int64_t OB_MALLOC_MIDDLE_BLOCK_SIZE             = (1LL << 16);                 // 64KB
static const int64_t OB_MALLOC_BIG_BLOCK_SIZE                = (1LL << 21) - (1LL << 10);// 2MB (-1KB)

const int64_t OB_MAX_MYSQL_RESPONSE_PACKET_SIZE              = OB_MALLOC_BIG_BLOCK_SIZE;

/// Maximum number of elements/columns a row can contain
static const int64_t OB_USER_ROW_MAX_COLUMNS_COUNT           = 512;
static const int64_t OB_ROW_MAX_COLUMNS_COUNT                =
    OB_USER_ROW_MAX_COLUMNS_COUNT + 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER; // used in ObRow
static const int64_t OB_MAX_TIMESTAMP_LENGTH                 = 32;
static const int64_t OB_COMMON_MEM_BLOCK_SIZE                = 64 * 1024;
static const int64_t OB_MAX_USER_ROW_LENGTH                  = 1572864L; // 1.5M
static const int64_t OB_MAX_ROW_LENGTH                       = OB_MAX_USER_ROW_LENGTH
                                         + 64L * 1024L/*for root table extra columns*/;
static const int64_t OB_MAX_MONITOR_INFO_LENGTH              = 65535;
static const int64_t OB_MAX_CHAR_LENGTH                      = 256; // Compatible with mysql, unit character mysql is 256
static const int64_t OB_MAX_VARCHAR_LENGTH                   = 256 * 1024L; // Unit byte
static const int64_t OB_MAX_VARCHAR_LENGTH_KEY               = 16 * 1024L;  //KEY key varchar maximum length limit
static const int64_t OB_OLD_MAX_VARCHAR_LENGTH               = 64 * 1024; // for compatible purpose
static const int64_t OB_MAX_DEFAULT_VALUE_LENGTH             = OB_MAX_VARCHAR_LENGTH;
static const int64_t OB_MAX_BINARY_LENGTH                    = 255;
static const int64_t OB_MAX_VARBINARY_LENGTH                 = 64 * 1024L;
static const int64_t OB_MAX_DECIMAL_PRECISION                = 65;
static const int64_t OB_MAX_DECIMAL_SCALE                    = 30;
static const int64_t OB_DECIMAL_NOT_SPECIFIED                = -1;
static const int64_t OB_MAX_DOUBLE_FLOAT_SCALE               = 30;
static const int64_t OB_MAX_DOUBLE_FLOAT_PRECISION           = 53;
static const int64_t OB_MAX_INTEGER_DISPLAY_WIDTH            = 255; //TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT
static const int64_t OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH       = 255;
static const int64_t OB_MAX_COLUMN_NUMBER                    = OB_ROW_MAX_COLUMNS_COUNT; // used in ObSchemaManagerV2
static const int64_t OB_MAX_PARTITION_KEY_COLUMN_NUMBER      = OB_MAX_ROWKEY_COLUMN_NUMBER;
static const int64_t OB_MAX_USER_DEFINED_COLUMNS_COUNT       =
    OB_ROW_MAX_COLUMNS_COUNT - OB_APP_MIN_COLUMN_ID;
static const int64_t OB_CAST_TO_VARCHAR_MAX_LENGTH           = 256;
static const int64_t OB_CAST_BUFFER_LENGTH                   = 256;
static const int64_t OB_PREALLOCATED_NUM                     = 21;  // half of 42
static const int64_t OB_PREALLOCATED_COL_ID_NUM              = 128;
static const int64_t OB_MAX_DATE_PRECISION                   = 0;
static const int64_t OB_MAX_DATETIME_PRECISION               = 6;
static const int64_t OB_MAX_TIMESTAMP_TZ_PRECISION           = 9;

const char *const SYS_DATE                                   = "$SYS_DATE";
const char *const OB_DEFAULT_COMPRESS_FUNC_NAME              = "none";

static const int64_t OB_MYSQL_FULL_USER_NAME_MAX_LEN         = OB_MAX_USER_NAME_LENGTH + OB_MAX_TENANT_NAME_LENGTH;//username@tenantname
static const int64_t OB_MAX_CONFIG_NAME_LEN                  = 128;
static const int64_t OB_MAX_CONFIG_VALUE_LEN                 = 4096;
static const int64_t OB_MAX_CONFIG_TYPE_LENGTH               = 128;
static const int64_t OB_MAX_CONFIG_INFO_LEN                  = 4096;
static const int64_t OB_MAX_CONFIG_SECTION_LEN               = 128;
static const int64_t OB_MAX_CONFIG_VISIBLE_LEVEL_LEN         = 64;
static const int64_t OB_MAX_CONFIG_NEED_REBOOT_LEN           = 64;
static const int64_t OB_MAX_CONFIG_NUMBER                    = 1024;
static const int64_t OB_MAX_EXTRA_CONFIG_LENGTH              = 4096;

static const int64_t OB_TABLET_MAX_REPLICA_COUNT             = 6;

//all_outline related
const int64_t OB_MAX_OUTLINE_CATEGORY_NAME_LENGTH            = 64;
const int64_t OB_MAX_OUTLINE_SIGNATURE_LENGTH                = OB_MAX_VARBINARY_LENGTH;
const int64_t OB_MAX_OF_MAX_CONCURRENT_PARAM_LENGTH          = OB_MAX_VARBINARY_LENGTH;
const int64_t OB_MAX_HINT_FORMAT_LENGTH                      = 16;

static const int64_t OB_MAX_PROGRESSIVE_MERGE_NUM            = 64;
static const int64_t OB_DEFAULT_PROGRESSIVE_MERGE_NUM        = 0;
static const int64_t OB_DEFAULT_MACRO_BLOCK_SIZE             = 2 << 20; // 2MB
static const int64_t OB_DEFAULT_SSTABLE_BLOCK_SIZE           = 16 * 1024; // 16KB
static const int64_t OB_DEFAULT_MAX_TABLET_SIZE              = 256 * 1024 * 1024; // 256MB
static const int32_t OB_DEFAULT_CHARACTER_SET                = 33; //UTF8
static const int64_t OB_MYSQL_PACKET_BUFF_SIZE               = 6 * 1024; //6KB
static const int64_t OB_MAX_THREAD_NUM                       = 4096;
static const int64_t OB_MAX_CPU_NUM                          = 256;
static const int64_t OB_MAX_STATICS_PER_TABLE                = 128;

static const int64_t OB_INDEX_WRITE_START_DELAY              = 20 * 1000 * 1000; //20s

static const int64_t MAX_SQL_ERR_MSG_LENGTH                  = 256;
static const int64_t MSG_SIZE                                = MAX_SQL_ERR_MSG_LENGTH;
static const int64_t OB_DUMP_ROOT_TABLE_TYPE                 = 1;
static const int64_t OB_DUMP_UNUSUAL_TABLET_TYPE             = 2;
static const int64_t OB_MAX_SYS_VAR_VAL_LENGTH               = 128;

static const int64_t OB_MAX_TRACE_ID_LENGTH                  = 48;
static const int64_t OB_MAX_OBPROXY_TRACE_ID_LENGTH          = 128;

// bitset defines
static const int64_t OB_DEFAULT_BITSET_SIZE                  = OB_MAX_TABLE_NUM_PER_STMT;
static const int64_t OB_DEFAULT_BITSET_SIZE_FOR_BASE_COLUMN  = 64;
static const int64_t OB_DEFAULT_BITSET_SIZE_FOR_ALIAS_COLUMN = 32;
static const int64_t OB_MAX_BITSET_SIZE                      = OB_ROW_MAX_COLUMNS_COUNT;
static const int64_t OB_DEFAULT_STATEMEMT_LEVEL_COUNT        = 16;

// max number of existing ObIStores for each partition,
// which contains ssstore, memstore and frozen stores
static const int64_t MAX_STORE_CNT_IN_STORAGE                = 64;
static const int64_t OB_MAX_PARTITION_NUM                    = 8192;

//Used to indicate the visible range of configuration items and whether to restart after modification to take effect
static const char *const OB_CONFIG_SECTION_DEFAULT           = "DEFAULT";
static const char *const OB_CONFIG_VISIBLE_LEVEL_USER        = "USER";
static const char *const OB_CONFIG_VISIBLE_LEVEL_SYS         = "SYS";
static const char *const OB_CONFIG_VISIBLE_LEVEL_DBA         = "DBA";
static const char *const OB_CONFIG_VISIBLE_LEVEL_MEMORY      = "MEMORY";
static const char *const OB_CONFIG_NEED_REBOOT               = "true";
static const char *const OB_CONFIG_NOT_NEED_REBOOT           = "false";

//Precision in user data type
static const int16_t MAX_SCALE_FOR_TEMPORAL                  = 6;
static const int16_t MIN_SCALE_FOR_TEMPORAL                  = 0;
static const int16_t MAX_SCALE_FOR_ORACLE_TEMPORAL           = 9;
static const int16_t DEFAULT_SCALE_FOR_INTEGER               = 0;
static const int16_t DEFAULT_LENGTH_FOR_NUMERIC              = -1;
static const int16_t DEFAULT_SCALE_FOR_DATE                  = 0;
static const int16_t DEFAULT_SCALE_FOR_YEAR                  = 0;
static const int16_t SCALE_UNKNOWN_YET                       = -1;
static const int16_t PRECISION_UNKNOWN_YET                   = -1;
static const int16_t LENGTH_UNKNOWN_YET                      = -1;
static const int16_t DEFAULT_PRECISION_FOR_BOOL              = 1;
static const int16_t DEFAULT_PRECISION_FOR_TEMPORAL          = -1;
static const int16_t DEFAULT_LENGTH_FOR_TEMPORAL             = -1;
static const int16_t DEFAULT_PRECISION_FOR_STRING            = -1;
static const int16_t DEFAULT_SCALE_FOR_STRING                = -1;
static const int16_t DEFAULT_SCALE_FOR_TEXT                  = 0;
static const int16_t DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS = 6;  //SEE : https://docs.oracle.com/cd/B19306_01/server.102/b14225/ch4datetime.htm
static const int16_t DEFAULT_SCALE_FOR_ORACLE_TIMESTAMP      = 6;

enum ObDmlType
{
  OB_DML_UNKNOW = 0,
  OB_DML_REPLACE = 1,
  OB_DML_INSERT = 2,
  OB_DML_UPDATE = 3,
  OB_DML_DELETE = 4,
  OB_DML_MERGED = 5,
  OB_DML_NUM,
};

//check whether an id is valid
OB_INLINE bool is_valid_id(const uint64_t id)
{
  return (OB_INVALID_ID != id);
}
//check whether an index is valid
OB_INLINE bool is_valid_idx(const int64_t idx)
{
  return (0 <= idx);
}

//check whether an tenant_id is valid
OB_INLINE bool is_valid_tenant_id(const uint64_t tenant_id)
{
  return (0 < tenant_id);
}

//check whether transaction version is valid
OB_INLINE bool is_valid_trans_version(const int64_t trans_version)
{
  // When the observer has not performed any transactions, publish_version is 0
  return trans_version >= 0;
}

OB_INLINE bool is_valid_membership_version(const int64_t membership_version)
{
  // When the observer does not perform any member changes, membership_version is 0
  return membership_version >= 0;
}

inline bool is_schema_error(int err)
{
  bool ret = false;
  switch(err) {
    case OB_TENANT_EXIST:
    case OB_TENANT_NOT_EXIST:
    case OB_ERR_BAD_DATABASE:
    case OB_DATABASE_EXIST:
    case OB_TABLEGROUP_NOT_EXIST:
    case OB_TABLEGROUP_EXIST:
    case OB_TABLE_NOT_EXIST:
    case OB_ERR_TABLE_EXIST:
    case OB_ERR_BAD_FIELD_ERROR:
    case OB_ERR_COLUMN_DUPLICATE:
    case OB_ERR_USER_EXIST:
    case OB_ERR_USER_NOT_EXIST:
    case OB_ERR_NO_PRIVILEGE:
    case OB_ERR_NO_DB_PRIVILEGE:
    case OB_ERR_NO_TABLE_PRIVILEGE:
    case OB_SCHEMA_ERROR:
    case OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH:
      ret = true;
      break;
    default:
      break;
  }
  return ret;
}

inline bool is_get_location_timeout_error(int err)
{
  return OB_GET_LOCATION_TIME_OUT == err;
}

inline bool is_partition_change_error(int err)
{
  bool ret = false;
  switch (err) {
    case OB_PARTITION_NOT_EXIST:
    case OB_LOCATION_NOT_EXIST:
      ret = true;
      break;
    default:
      break;
  }
  return ret;
}

inline bool is_server_down_error(int err)
{
  bool ret = false;
  ret = (OB_CONNECT_ERROR == err);
  return ret;
}

inline bool is_server_status_error(int err)
{
  bool ret = false;
  ret = (OB_SERVER_IS_INIT == err || OB_SERVER_IS_STOPPING == err);
  return ret;
}

inline bool is_unit_migrate(int err)
{
  return OB_TENANT_NOT_IN_SERVER == err;
}

inline bool is_process_timeout_error(int err)
{
  bool ret = false;
  ret = (OB_TIMEOUT == err);
  return ret;
}

inline bool is_master_changed_error(int err)
{
  bool ret = false;
  switch (err) {
    case OB_LOCATION_LEADER_NOT_EXIST:
    case OB_NOT_MASTER:
    case OB_RS_NOT_MASTER:
    case OB_RS_SHUTDOWN:
      ret = true;
      break;
    default:
      ret = false;
      break;
  }
  return ret;
}

inline bool is_distributed_not_supported_err(int err)
{
  return OB_ERR_DISTRIBUTED_NOT_SUPPORTED == err;
}

inline bool is_not_supported_err(int err)
{
  return OB_ERR_DISTRIBUTED_NOT_SUPPORTED == err || OB_NOT_SUPPORTED == err;
}

/*
 * |--- 4 bits ---|--- 2 bits ---|--- 2 bits ---| LSB
 * |---  clog  ---|-- SSStore ---|--- MemStore--| LSB
 */
static const int64_t SSSTORE_BITS_SHIFT    = 2;
static const int64_t CLOG_BITS_SHIFT       = 4;
static const int64_t ENCRYPTION_BITS_SHIFT = 8;
// replica type associated with memstore
static const int64_t WITH_MEMSTORE      = 0;
static const int64_t WITHOUT_MEMSTORE   = 1;
// replica type associated with ssstore
static const int64_t WITH_SSSTORE       = 0 << SSSTORE_BITS_SHIFT;
static const int64_t WITHOUT_SSSTORE    = 1 << SSSTORE_BITS_SHIFT;
// replica type associated with clog
static const int64_t SYNC_CLOG          = 0 << CLOG_BITS_SHIFT;
static const int64_t ASYNC_CLOG         = 1 << CLOG_BITS_SHIFT;
// replica type associated with encryption
const int64_t WITHOUT_ENCRYPTION        = 0 << ENCRYPTION_BITS_SHIFT;
const int64_t WITH_ENCRYPTION           = 1 << ENCRYPTION_BITS_SHIFT;

// Need to manually maintain the replica_type_to_str function in utility.cpp,
// Currently there are only three types: REPLICA_TYPE_FULL, REPLICA_TYPE_READONLY, and REPLICA_TYPE_LOGONLY
enum ObReplicaType
{
  // Almighty copy: is a member of paxos; has ssstore; has memstore
  REPLICA_TYPE_FULL = (SYNC_CLOG | WITH_SSSTORE | WITH_MEMSTORE), // 0
  // Backup copy: Paxos member; ssstore; no memstore
  REPLICA_TYPE_BACKUP = (SYNC_CLOG | WITH_SSSTORE | WITHOUT_MEMSTORE), // 1
  // Memory copy; no ssstore; memstore
  //REPLICA_TYPE_MMONLY = (SYNC_CLOG | WITHOUT_SSSTORE | WITH_MEMSTORE), // 4
  // Journal copy: Paxos member; no ssstore; no memstore
  REPLICA_TYPE_LOGONLY = (SYNC_CLOG | WITHOUT_SSSTORE | WITHOUT_MEMSTORE), // 5
  // Read-only copy: not a member of paxos; ssstore; memstore
  REPLICA_TYPE_READONLY = (ASYNC_CLOG | WITH_SSSTORE | WITH_MEMSTORE), // 16
  // Incremental copy: not a member of paxos; no ssstore; memstore
  REPLICA_TYPE_MEMONLY = (ASYNC_CLOG | WITHOUT_SSSTORE | WITH_MEMSTORE), // 20
  // Encrypted log copy: encrypted; paxos member; no sstore; no memstore
  REPLICA_TYPE_ENCRYPTION_LOGONLY = (WITH_ENCRYPTION | SYNC_CLOG | WITHOUT_SSSTORE | WITHOUT_MEMSTORE), // 261
  // invalid value
  REPLICA_TYPE_MAX,
};

class ObReplicaTypeCheck
{
public:
  static bool is_replica_type_valid(const int32_t replica_type)
  {
    return (replica_type >= REPLICA_TYPE_FULL && replica_type < REPLICA_TYPE_MAX);
  }
  static bool is_can_elected_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type);
  }
  static bool is_readonly_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_READONLY == replica_type);
  }
  static bool is_paxos_replica(const int32_t replica_type)
  {
    return (replica_type >= REPLICA_TYPE_FULL && replica_type <= REPLICA_TYPE_LOGONLY)
           || (REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type);
  }
  static bool is_writable_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type);
  }
  static bool is_readable_replica(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type);
  }
  static bool is_replica_with_sstable(const int32_t replica_type)
  {
    return (REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type);
  }
  static bool can_as_data_source(const int32_t dest_replica_type, const int32_t src_replica_type)
  {
    return  (dest_replica_type == src_replica_type
             || REPLICA_TYPE_FULL == src_replica_type); // TODO temporarily only supports the same type or F as the data source
  }
  static bool is_logonly_replica(const int32_t replica_type)
  {
    return REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type || REPLICA_TYPE_LOGONLY == replica_type;
  }
};

enum ObNameCaseMode
{
  OB_NAME_CASE_INVALID = -1,
  OB_ORIGIN_AND_SENSITIVE = 0,//stored using lettercase, and name comparisons are case sensitive
  OB_LOWERCASE_AND_INSENSITIVE = 1,//stored using lowercase, and name comparisons are case insensitive
  OB_ORIGIN_AND_INSENSITIVE = 2,//stored using lettercase, and name comparisons are case insensitive
  OB_NAME_CASE_MAX,
};

enum ObFreezeStatus
{
  INIT_STATUS = 0,
  PREPARED_SUCCEED,
  COMMIT_SUCCEED,
  FREEZE_STATUS_MAX,
};

char *lbt();
} // end namespace common
} // end namespace oceanbase


// For the serialize function pos is both an input parameter and an output parameter,
// serialize writes the serialized data from (buf+pos),
// Update pos after writing is completed. If the data after writing exceeds (buf+buf_len),
// serialize returned failed.
//
// For the deserialize function pos is both an input parameter and an output parameter,
// deserialize reads data from (buf+pos) for deserialization,
// Update pos after completion. If the data required for deserialization exceeds (buf+data_len),
// deserialize returned failed.

#define NEED_SERIALIZE_AND_DESERIALIZE \
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  int64_t get_serialize_size(void) const

#define INLINE_NEED_SERIALIZE_AND_DESERIALIZE \
  inline int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  inline int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  inline int64_t get_serialize_size(void) const

#define VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos); \
  virtual int64_t get_serialize_size(void) const

#define PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE \
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0; \
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0; \
  virtual int64_t get_serialize_size(void) const = 0

#define DEFINE_SERIALIZE(TypeName) \
  int TypeName::serialize(char* buf, const int64_t buf_len, int64_t& pos) const

#define DEFINE_DESERIALIZE(TypeName) \
  int TypeName::deserialize(const char* buf, const int64_t data_len, int64_t& pos)

#define DEFINE_GET_SERIALIZE_SIZE(TypeName) \
  int64_t TypeName::get_serialize_size(void) const

#define DATABUFFER_SERIALIZE_INFO \
  data_buffer_.get_data(), data_buffer_.get_capacity(), data_buffer_.get_position()

#define DIO_ALIGN_SIZE 512
#define TC_INIT_PRIORITY 120
#define MALLOC_INIT_PRIORITY 128
#define NORMAL_INIT_PRIORITY (MALLOC_INIT_PRIORITY + 1)

//judge int64_t multiplication whether overflow
inline bool is_multi_overflow64(int64_t a, int64_t b)
{
  bool ret = false;
  if (0 == b || 0 == a) {
    ret = false;
  }
  //min / -1 will overflow, so can't use the next rule to judge
  else if (-1 == b) {
    if (INT64_MIN == a) {
      ret = true;
    } else {
      ret = false;
    }
  } else if (a > 0 && b > 0) {
    ret = INT64_MAX / b < a;
  } else if (a < 0 && b < 0) {
    ret = INT64_MAX / b > a;
  } else if (a > 0 && b < 0) {
    ret = INT64_MIN / b < a;
  } else {
    ret = INT64_MIN / b > a;
  }
  return ret;
}
#define IS_MULTI_OVERFLOW64(a, b) is_multi_overflow64(a, b)

#define IS_ADD_OVERFLOW64(a, b, ret) \
  ((0 == ((static_cast<uint64_t>(a) >> 63) ^ (static_cast<uint64_t>(b) >> 63))) \
   && (1 == ((static_cast<uint64_t>(a) >> 63) ^ (static_cast<uint64_t>(ret) >> 63))))

#ifdef __ENABLE_PRELOAD__
#include "lib/utility/ob_preload.h"
#endif

struct ObNumberDesc
{
  union
  {
    uint32_t desc_;
    struct
    {
      uint8_t len_;
      uint8_t cap_;
      uint8_t flag_;
      union
      {
        uint8_t se_;
        struct
        {
          uint8_t exp_:7;
          uint8_t sign_:1;
        };
      };
    };
  };
};

#define DEFINE_ALLOCATOR_WRAPPER \
  class IAllocator \
  { \
  public: \
    virtual ~IAllocator() {}; \
    virtual void *alloc(const int64_t size) = 0; \
  }; \
  template <class T> \
  class TAllocator : public IAllocator \
  { \
  public: \
    explicit TAllocator(T &allocator) : allocator_(allocator) {}; \
    void *alloc(const int64_t size) {return allocator_.alloc(size);}; \
  private: \
    T &allocator_; \
  };

// need define:
//   ObRow row;
//   ObObj obj;
#define ROW_CELL_SET_VAL(table_id, type, val) \
  obj.set_##type(val);\
  if (OB_SUCCESS == ret  \
      && OB_SUCCESS != (ret = row.set_cell(table_id, ++column_id, obj))) \
  {\
    _OB_LOG(WARN, "failed to set cell=%s, ret=%d", to_cstring(obj), ret); \
  }

inline int64_t &get_tid_cache()
{
  static __thread int64_t tid = -1;
  return tid;
}

// should be called after fork/daemon
inline void reset_tid_cache()
{
  get_tid_cache() = -1;
}

inline int64_t gettid()
{
  int64_t &tid = get_tid_cache();
  if (OB_UNLIKELY(tid <= 0)) {
    tid = static_cast<int64_t>(syscall(__NR_gettid));
  }
  return tid;
}
#define GETTID() gettid()

#endif // OCEANBASE_COMMON_DEFINE_H_
