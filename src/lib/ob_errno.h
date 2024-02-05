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

#ifndef OCEANBASE_LIB_OB_ERRNO_H_
#define OCEANBASE_LIB_OB_ERRNO_H_
#include "mysql_errno.h"
#include <stdint.h>
#
namespace oceanbase
{
namespace common
{

/**
 * proxy new error code definition:
 * proxy new error code scope: [10000， -11000)
 * proxy common error code scope: [-10000, -10100)
 * original proxy related error code scope: [-10100, -10300)
 * sharding related error code scope: [-10300, -10500)
 * kv related error code scope: [-10500, -10700)
 * proxy client related error code scope: [-10700, -10900)
 */

  static const int OB_INTERNAL_ERROR = -600;
  static const int OB_ORA_FATAL_ERROR = -603;
  static const int64_t OB_MAX_ERROR_CODE = 11000;
  static const int64_t OB_PROXY_MAX_ERROR_CODE = -11000;
  static const int64_t OB_PROXY_COMMON_MAX_ERROR_CODE = -10100;
  static const int64_t OB_PROXY_ORIGINAL_MAX_ERROR_CODE = -10300;
  static const int64_t OB_PROXY_SHARDING_MAX_ERROR_CODE = -10500;
  static const int64_t OB_PROXY_KV_MAX_ERROR_CODE = -10700;
  static const int64_t OB_PROXY_CLIENT_MAX_ERROR_CODE = -10900;
  static const int OB_LAST_ERROR_CODE = -8003;
  static const int OB_ERR_SQL_START = -5000;
  static const int OB_ERR_SQL_END = -5999;
  static const int OB_SUCCESS = 0;
  static const int OB_ERROR = -4000;
  static const int OB_OBJ_TYPE_ERROR = -4001;
  static const int OB_INVALID_ARGUMENT = -4002;
  static const int OB_ARRAY_OUT_OF_RANGE = -4003;
  static const int OB_SERVER_LISTEN_ERROR = -4004;
  static const int OB_INIT_TWICE = -4005;
  static const int OB_NOT_INIT = -4006;
  static const int OB_NOT_SUPPORTED = -4007;
  static const int OB_ITER_END = -4008;
  static const int OB_IO_ERROR = -4009;
  static const int OB_ERROR_FUNC_VERSION = -4010;
  static const int OB_PACKET_NOT_SENT = -4011;
  static const int OB_TIMEOUT = -4012;
  static const int OB_ALLOCATE_MEMORY_FAILED = -4013;
  static const int OB_INNER_STAT_ERROR = -4014;
  static const int OB_ERR_SYS = -4015;
  static const int OB_ERR_UNEXPECTED = -4016;
  static const int OB_ENTRY_EXIST = -4017;
  static const int OB_ENTRY_NOT_EXIST = -4018;
  static const int OB_SIZE_OVERFLOW = -4019;
  static const int OB_REF_NUM_NOT_ZERO = -4020;
  static const int OB_CONFLICT_VALUE = -4021;
  static const int OB_ITEM_NOT_SETTED = -4022;
  static const int OB_EAGAIN = -4023;
  static const int OB_BUF_NOT_ENOUGH = -4024;
  static const int OB_PARTIAL_FAILED = -4025;
  static const int OB_READ_NOTHING = -4026;
  static const int OB_FILE_NOT_EXIST = -4027;
  static const int OB_DISCONTINUOUS_LOG = -4028;
  static const int OB_SCHEMA_ERROR = -4029;
  static const int OB_TENANT_OUT_OF_MEM = -4030;
  static const int OB_UNKNOWN_OBJ = -4031;
  static const int OB_NO_MONITOR_DATA = -4032;
  static const int OB_SERIALIZE_ERROR = -4033;
  static const int OB_DESERIALIZE_ERROR = -4034;
  static const int OB_AIO_TIMEOUT = -4035;
  static const int OB_NEED_RETRY = -4036;
  static const int OB_TOO_MANY_SSTABLE = -4037;
  static const int OB_NOT_MASTER = -4038;
  static const int OB_DECRYPT_FAILED = -4041;
  static const int OB_USER_NOT_EXIST = -4042;
  static const int OB_PASSWORD_WRONG = -4043;
  static const int OB_SKEY_VERSION_WRONG = -4044;
  static const int OB_NOT_REGISTERED = -4048;
  static const int OB_WAITQUEUE_TIMEOUT = -4049;
  static const int OB_NOT_THE_OBJECT = -4050;
  static const int OB_ALREADY_REGISTERED = -4051;
  static const int OB_LAST_LOG_RUINNED = -4052;
  static const int OB_NO_CS_SELECTED = -4053;
  static const int OB_NO_TABLETS_CREATED = -4054;
  static const int OB_INVALID_ERROR = -4055;
  static const int OB_DECIMAL_OVERFLOW_WARN = -4057;
  static const int OB_DECIMAL_UNLEGAL_ERROR = -4058;
  static const int OB_OBJ_DIVIDE_ERROR = -4060;
  static const int OB_NOT_A_DECIMAL = -4061;
  static const int OB_DECIMAL_PRECISION_NOT_EQUAL = -4062;
  static const int OB_EMPTY_RANGE = -4063;
  static const int OB_SESSION_KILLED = -4064;
  static const int OB_LOG_NOT_SYNC = -4065;
  static const int OB_DIR_NOT_EXIST = -4066;
  static const int OB_SESSION_NOT_FOUND = -4067;
  static const int OB_INVALID_LOG = -4068;
  static const int OB_INVALID_DATA = -4070;
  static const int OB_ALREADY_DONE = -4071;
  static const int OB_CANCELED = -4072;
  static const int OB_LOG_SRC_CHANGED = -4073;
  static const int OB_LOG_NOT_ALIGN = -4074;
  static const int OB_LOG_MISSING = -4075;
  static const int OB_NEED_WAIT = -4076;
  static const int OB_NOT_IMPLEMENT = -4077;
  static const int OB_DIVISION_BY_ZERO = -4078;
  static const int OB_EXCEED_MEM_LIMIT = -4080;
  static const int OB_RESULT_UNKNOWN = -4081;
  static const int OB_NO_RESULT = -4084;
  static const int OB_QUEUE_OVERFLOW = -4085;
  static const int OB_TERM_LAGGED = -4097;
  static const int OB_TERM_NOT_MATCH = -4098;
  static const int OB_START_LOG_CURSOR_INVALID = -4099;
  static const int OB_LOCK_NOT_MATCH = -4100;
  static const int OB_DEAD_LOCK = -4101;
  static const int OB_PARTIAL_LOG = -4102;
  static const int OB_CHECKSUM_ERROR = -4103;
  static const int OB_INIT_FAIL = -4104;
  static const int OB_NOT_ENOUGH_STORE = -4106;
  static const int OB_BLOCK_SWITCHED = -4107;
  static const int OB_STATE_NOT_MATCH = -4109;
  static const int OB_READ_ZERO_LOG = -4110;
  static const int OB_BLOCK_NEED_FREEZE = -4111;
  static const int OB_BLOCK_FROZEN = -4112;
  static const int OB_IN_FATAL_STATE = -4113;
  static const int OB_IN_STOP_STATE = -4114;
  static const int OB_UPS_MASTER_EXISTS = -4115;
  static const int OB_LOG_NOT_CLEAR = -4116;
  static const int OB_FILE_ALREADY_EXIST = -4117;
  static const int OB_UNKNOWN_PACKET = -4118;
  static const int OB_RPC_PACKET_TOO_LONG = -4119;
  static const int OB_LOG_TOO_LARGE = -4120;
  static const int OB_RPC_SEND_ERROR = -4121;
  static const int OB_RPC_POST_ERROR = -4122;
  static const int OB_LIBEASY_ERROR = -4123;
  static const int OB_CONNECT_ERROR = -4124;
  static const int OB_NOT_FREE = -4125;
  static const int OB_INIT_SQL_CONTEXT_ERROR = -4126;
  static const int OB_SKIP_INVALID_ROW = -4127;
  static const int OB_RPC_PACKET_INVALID = -4128;
  static const int OB_NO_TABLET = -4133;
  static const int OB_SNAPSHOT_DISCARDED = -4138;
  static const int OB_DATA_NOT_UPTODATE = -4139;
  static const int OB_ROW_MODIFIED = -4142;
  static const int OB_VERSION_NOT_MATCH = -4143;
  static const int OB_BAD_ADDRESS = -4144;
  static const int OB_ENQUEUE_FAILED = -4146;
  static const int OB_INVALID_CONFIG = -4147;
  static const int OB_STMT_EXPIRED = -4149;
  static const int OB_ERR_MIN_VALUE = -4150;
  static const int OB_ERR_MAX_VALUE = -4151;
  static const int OB_ERR_NULL_VALUE = -4152;
  static const int OB_RESOURCE_OUT = -4153;
  static const int OB_ERR_SQL_CLIENT = -4154;
  static const int OB_META_TABLE_WITHOUT_USE_TABLE = -4155;
  static const int OB_DISCARD_PACKET = -4156;
  static const int OB_OPERATE_OVERFLOW = -4157;
  static const int OB_INVALID_DATE_FORMAT = -4158;
  static const int OB_POOL_REGISTERED_FAILED = -4159;
  static const int OB_POOL_UNREGISTERED_FAILED = -4160;
  static const int OB_INVALID_ARGUMENT_NUM = -4161;
  static const int OB_LEASE_NOT_ENOUGH = -4162;
  static const int OB_LEASE_NOT_MATCH = -4163;
  static const int OB_UPS_SWITCH_NOT_HAPPEN = -4164;
  static const int OB_EMPTY_RESULT = -4165;
  static const int OB_CACHE_NOT_HIT = -4166;
  static const int OB_NESTED_LOOP_NOT_SUPPORT = -4167;
  static const int OB_LOG_INVALID_MOD_ID = -4168;
  static const int OB_LOG_MODULE_UNKNOWN = -4169;
  static const int OB_LOG_LEVEL_INVALID = -4170;
  static const int OB_LOG_PARSER_SYNTAX_ERR = -4171;
  static const int OB_INDEX_OUT_OF_RANGE = -4172;
  static const int OB_INT_UNDERFLOW = -4173;
  static const int OB_UNKNOWN_CONNECTION = -4174;
  static const int OB_ERROR_OUT_OF_RANGE = -4175;
  static const int OB_CACHE_SHRINK_FAILED = -4176;
  static const int OB_OLD_SCHEMA_VERSION = -4177;
  static const int OB_RELEASE_SCHEMA_ERROR = -4178;
  static const int OB_OP_NOT_ALLOW = -4179;
  static const int OB_NO_EMPTY_ENTRY = -4180;
  static const int OB_ERR_ALREADY_EXISTS = -4181;
  static const int OB_SEARCH_NOT_FOUND = -4182;
  static const int OB_BEYOND_THE_RANGE = -4183;
  static const int OB_CS_OUTOF_DISK_SPACE = -4184;
  static const int OB_COLUMN_GROUP_NOT_FOUND = -4185;
  static const int OB_CS_COMPRESS_LIB_ERROR = -4186;
  static const int OB_ITEM_NOT_MATCH = -4187;
  static const int OB_INVALID_DATE_FORMAT_END = -4190;
  static const int OB_HASH_EXIST = -4200;
  static const int OB_HASH_NOT_EXIST = -4201;
  static const int OB_HASH_GET_TIMEOUT = -4204;
  static const int OB_HASH_PLACEMENT_RETRY = -4205;
  static const int OB_HASH_FULL = -4206;
  static const int OB_PACKET_PROCESSED = -4207;
  static const int OB_WAIT_NEXT_TIMEOUT = -4208;
  static const int OB_LEADER_NOT_EXIST = -4209;
  static const int OB_PREPARE_MAJOR_FREEZE_FAILED = -4210;
  static const int OB_COMMIT_MAJOR_FREEZE_FAILED = -4211;
  static const int OB_ABORT_MAJOR_FREEZE_FAILED = -4212;
  static const int OB_MAJOR_FREEZE_NOT_FINISHED = -4213;
  static const int OB_PARTITION_NOT_LEADER = -4214;
  static const int OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT = -4215;
  static const int OB_CURL_ERROR = -4216;
  static const int OB_MAJOR_FREEZE_NOT_ALLOW = -4217;
  static const int OB_PREPARE_FREEZE_FAILED = -4218;
  static const int OB_INVALID_DATE_VALUE = -4219;
  static const int OB_INACTIVE_SQL_CLIENT = -4220;
  static const int OB_INACTIVE_RPC_PROXY = -4221;
  static const int OB_INTERVAL_WITH_MONTH = -4222;
  static const int OB_TOO_MANY_DATETIME_PARTS = -4223;
  static const int OB_DATA_OUT_OF_RANGE = -4224;
  static const int OB_PARTITION_NOT_EXIST = -4225;
  static const int OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD = -4226;
  static const int OB_ERR_NO_DEFAULT_FOR_FIELD = -4227;
  static const int OB_ERR_FIELD_SPECIFIED_TWICE = -4228;
  static const int OB_ERR_TOO_LONG_TABLE_COMMENT = -4229;
  static const int OB_ERR_TOO_LONG_FIELD_COMMENT = -4230;
  static const int OB_ERR_TOO_LONG_INDEX_COMMENT = -4231;
  static const int OB_NOT_FOLLOWER = -4232;
  static const int OB_ERR_OUT_OF_LOWER_BOUND = -4233;
  static const int OB_ERR_OUT_OF_UPPER_BOUND = -4234;
  static const int OB_BAD_NULL_ERROR = -4235;
  static const int OB_OBCONFIG_RETURN_ERROR = -4236;
  static const int OB_OBCONFIG_APPNAME_MISMATCH = -4237;
  static const int OB_ERR_VIEW_SELECT_DERIVED = -4238;
  static const int OB_CANT_MJ_PATH = -4239;
  static const int OB_ERR_NO_JOIN_ORDER_GENERATED = -4240;
  static const int OB_ERR_NO_PATH_GENERATED = -4241;
  static const int OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH = -4242;
  static const int OB_FILE_NOT_OPENED = -4243;
  static const int OB_TIMER_TASK_HAS_SCHEDULED = -4244;
  static const int OB_TIMER_TASK_HAS_NOT_SCHEDULED = -4245;
  static const int OB_PARSE_DEBUG_SYNC_ERROR = -4246;
  static const int OB_UNKNOWN_DEBUG_SYNC_POINT = -4247;
  static const int OB_ERR_INTERRUPTED = -4248;
  static const int OB_ERR_DATA_TRUNCATED = -4249;
  static const int OB_NOT_RUNNING = -4250;
  static const int OB_INVALID_PARTITION = -4251;
  static const int OB_ERR_TIMEOUT_TRUNCATED = -4252;
  static const int OB_ERR_TOO_LONG_TENANT_COMMENT = -4253;
  static const int OB_ERR_NET_PACKET_TOO_LARGE = -4254;
  static const int OB_TRACE_DESC_NOT_EXIST = -4255;
  static const int OB_ERR_NO_DEFAULT = -4256;
  static const int OB_ERR_COMPRESS_DECOMPRESS_DATA = -4257;
  static const int OB_ERR_INCORRECT_STRING_VALUE = -4258;
  static const int OB_ERR_DISTRIBUTED_NOT_SUPPORTED = -4259;
  static const int OB_IS_CHANGING_LEADER = -4260;
  static const int OB_DATETIME_FUNCTION_OVERFLOW = -4261;
  static const int OB_ERR_DOUBLE_TRUNCATED = -4262;
  static const int OB_MINOR_FREEZE_NOT_ALLOW = -4263;
  static const int OB_LOG_OUTOF_DISK_SPACE = -4264;
  static const int OB_ERR_KILL_CLIENT_SESSION = -4401;
  static const int OB_IMPORT_NOT_IN_SERVER = -4505;
  static const int OB_CONVERT_ERROR = -4507;
  static const int OB_BYPASS_TIMEOUT = -4510;
  static const int OB_RS_STATE_NOT_ALLOW = -4512;
  static const int OB_NO_REPLICA_VALID = -4515;
  static const int OB_NO_NEED_UPDATE = -4517;
  static const int OB_CACHE_TIMEOUT = -4518;
  static const int OB_ITER_STOP = -4519;
  static const int OB_ZONE_ALREADY_MASTER = -4523;
  static const int OB_IP_PORT_IS_NOT_SLAVE_ZONE = -4524;
  static const int OB_ZONE_IS_NOT_SLAVE = -4525;
  static const int OB_ZONE_IS_NOT_MASTER = -4526;
  static const int OB_CONFIG_NOT_SYNC = -4527;
  static const int OB_IP_PORT_IS_NOT_ZONE = -4528;
  static const int OB_MASTER_ZONE_NOT_EXIST = -4529;
  static const int OB_ZONE_INFO_NOT_EXIST = -4530;
  static const int OB_GET_ZONE_MASTER_UPS_FAILED = -4531;
  static const int OB_MULTIPLE_MASTER_ZONES_EXIST = -4532;
  static const int OB_INDEXING_ZONE_INVALID = -4533;
  static const int OB_ROOT_TABLE_RANGE_NOT_EXIST = -4537;
  static const int OB_ROOT_MIGRATE_CONCURRENCY_FULL = -4538;
  static const int OB_ROOT_MIGRATE_INFO_NOT_FOUND = -4539;
  static const int OB_NOT_DATA_LOAD_TABLE = -4540;
  static const int OB_DATA_LOAD_TABLE_DUPLICATED = -4541;
  static const int OB_ROOT_TABLE_ID_EXIST = -4542;
  static const int OB_INDEX_TIMEOUT = -4543;
  static const int OB_ROOT_NOT_INTEGRATED = -4544;
  static const int OB_INDEX_INELIGIBLE = -4545;
  static const int OB_REBALANCE_EXEC_TIMEOUT = -4546;
  static const int OB_MERGE_NOT_STARTED = -4547;
  static const int OB_MERGE_ALREADY_STARTED = -4548;
  static const int OB_ROOTSERVICE_EXIST = -4549;
  static const int OB_RS_SHUTDOWN = -4550;
  static const int OB_SERVER_MIGRATE_IN_DENIED = -4551;
  static const int OB_REBALANCE_TASK_CANT_EXEC = -4552;
  static const int OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT = -4553;
  static const int OB_DATA_SOURCE_NOT_EXIST = -4600;
  static const int OB_DATA_SOURCE_TABLE_NOT_EXIST = -4601;
  static const int OB_DATA_SOURCE_RANGE_NOT_EXIST = -4602;
  static const int OB_DATA_SOURCE_DATA_NOT_EXIST = -4603;
  static const int OB_DATA_SOURCE_SYS_ERROR = -4604;
  static const int OB_DATA_SOURCE_TIMEOUT = -4605;
  static const int OB_DATA_SOURCE_CONCURRENCY_FULL = -4606;
  static const int OB_DATA_SOURCE_WRONG_URI_FORMAT = -4607;
  static const int OB_SSTABLE_VERSION_UNEQUAL = -4608;
  static const int OB_UPS_RENEW_LEASE_NOT_ALLOWED = -4609;
  static const int OB_UPS_COUNT_OVER_LIMIT = -4610;
  static const int OB_NO_UPS_MAJORITY = -4611;
  static const int OB_INDEX_COUNT_REACH_THE_LIMIT = -4613;
  static const int OB_TASK_EXPIRED = -4614;
  static const int OB_TABLEGROUP_NOT_EMPTY = -4615;
  static const int OB_INVALID_SERVER_STATUS = -4620;
  static const int OB_WAIT_ELEC_LEADER_TIMEOUT = -4621;
  static const int OB_WAIT_ALL_RS_ONLINE_TIMEOUT = -4622;
  static const int OB_ALL_REPLICAS_ON_MERGE_ZONE = -4623;
  static const int OB_MACHINE_RESOURCE_NOT_ENOUGH = -4624;
  static const int OB_NOT_SERVER_CAN_HOLD_SOFTLY = -4625;
  static const int OB_RESOURCE_POOL_ALREADY_GRANTED = -4626;
  static const int OB_SERVER_ALREADY_DELETED = -4628;
  static const int OB_SERVER_NOT_DELETING = -4629;
  static const int OB_SERVER_NOT_IN_WHITE_LIST = -4630;
  static const int OB_SERVER_ZONE_NOT_MATCH = -4631;
  static const int OB_OVER_ZONE_NUM_LIMIT = -4632;
  static const int OB_ZONE_STATUS_NOT_MATCH = -4633;
  static const int OB_RESOURCE_UNIT_IS_REFERENCED = -4634;
  static const int OB_DIFFERENT_PRIMARY_ZONE = -4636;
  static const int OB_SERVER_NOT_ACTIVE = -4637;
  static const int OB_RS_NOT_MASTER = -4638;
  static const int OB_CANDIDATE_LIST_ERROR = -4639;
  static const int OB_PARTITION_ZONE_DUPLICATED = -4640;
  static const int OB_ZONE_DUPLICATED = -4641;
  static const int OB_NOT_ALL_ZONE_ACTIVE = -4642;
  static const int OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST = -4643;
  static const int OB_REPLICA_NUM_NOT_MATCH = -4644;
  static const int OB_ZONE_LIST_POOL_LIST_NOT_MATCH = -4645;
  static const int OB_INVALID_TENANT_NAME = -4646;
  static const int OB_EMPTY_RESOURCE_POOL_LIST = -4647;
  static const int OB_RESOURCE_UNIT_NOT_EXIST = -4648;
  static const int OB_RESOURCE_UNIT_EXIST = -4649;
  static const int OB_RESOURCE_POOL_NOT_EXIST = -4650;
  static const int OB_RESOURCE_POOL_EXIST = -4651;
  static const int OB_WAIT_LEADER_SWITCH_TIMEOUT = -4652;
  static const int OB_LOCATION_NOT_EXIST = -4653;
  static const int OB_LOCATION_LEADER_NOT_EXIST = -4654;
  static const int OB_ZONE_NOT_ACTIVE = -4655;
  static const int OB_UNIT_NUM_OVER_SERVER_COUNT = -4656;
  static const int OB_POOL_SERVER_INTERSECT = -4657;
  static const int OB_NOT_SINGLE_RESOURCE_POOL = -4658;
  static const int OB_INVALID_RESOURCE_UNIT = -4659;
  static const int OB_STOP_SERVER_IN_MULTIPLE_ZONES = -4660;
  static const int OB_SESSION_ENTRY_EXIST = -4661;
  static const int OB_GOT_SIGNAL_ABORTING = -4662;
  static const int OB_SERVER_NOT_ALIVE = -4663;
  static const int OB_GET_LOCATION_TIME_OUT = -4664;
  static const int OB_UNIT_IS_MIGRATING = -4665;
  static const int OB_CLUSTER_NO_MATCH = -4666;
  static const int OB_CHECK_ZONE_MERGE_ORDER = -4667;
  static const int OB_INVALID_MACRO_BLOCK_TYPE = -4668;
  static const int OB_CLUSTER_NOT_EXIST = -4669;
  static const int OB_STANDBY_WEAK_READ_ONLY = -4688;
  static const int OB_ERR_PARSER_INIT = -5000;
  static const int OB_ERR_PARSE_SQL = -5001;
  static const int OB_ERR_RESOLVE_SQL = -5002;
  static const int OB_ERR_GEN_PLAN = -5003;
  static const int OB_ERR_PARSER_SYNTAX = -5006;
  static const int OB_ERR_COLUMN_SIZE = -5007;
  static const int OB_ERR_COLUMN_DUPLICATE = -5008;
  static const int OB_ERR_OPERATOR_UNKNOWN = -5010;
  static const int OB_ERR_STAR_DUPLICATE = -5011;
  static const int OB_ERR_ILLEGAL_ID = -5012;
  static const int OB_ERR_ILLEGAL_VALUE = -5014;
  static const int OB_ERR_COLUMN_AMBIGUOUS = -5015;
  static const int OB_ERR_LOGICAL_PLAN_FAILD = -5016;
  static const int OB_ERR_SCHEMA_UNSET = -5017;
  static const int OB_ERR_ILLEGAL_NAME = -5018;
  static const int OB_TABLE_NOT_EXIST = -5019;
  static const int OB_ERR_TABLE_EXIST = -5020;
  static const int OB_ERR_EXPR_UNKNOWN = -5022;
  static const int OB_ERR_ILLEGAL_TYPE = -5023;
  static const int OB_ERR_PRIMARY_KEY_DUPLICATE = -5024;
  static const int OB_ERR_KEY_NAME_DUPLICATE = -5025;
  static const int OB_ERR_CREATETIME_DUPLICATE = -5026;
  static const int OB_ERR_MODIFYTIME_DUPLICATE = -5027;
  static const int OB_ERR_ILLEGAL_INDEX = -5028;
  static const int OB_ERR_INVALID_SCHEMA = -5029;
  static const int OB_ERR_INSERT_NULL_ROWKEY = -5030;
  static const int OB_ERR_COLUMN_NOT_FOUND = -5031;
  static const int OB_ERR_DELETE_NULL_ROWKEY = -5032;
  static const int OB_ERR_USER_EMPTY = -5034;
  static const int OB_ERR_USER_NOT_EXIST = -5035;
  static const int OB_ERR_NO_PRIVILEGE = -5036;
  static const int OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY = -5037;
  static const int OB_ERR_WRONG_PASSWORD = -5038;
  static const int OB_ERR_USER_IS_LOCKED = -5039;
  static const int OB_ERR_UPDATE_ROWKEY_COLUMN = -5040;
  static const int OB_ERR_UPDATE_JOIN_COLUMN = -5041;
  static const int OB_ERR_INVALID_COLUMN_NUM = -5042;
  static const int OB_ERR_PREPARE_STMT_NOT_FOUND = -5043;
  static const int OB_ERR_SYS_VARIABLE_UNKNOWN = -5044;
  static const int OB_ERR_OLDER_PRIVILEGE_VERSION = -5046;
  static const int OB_ERR_LACK_OF_ROWKEY_COL = -5047;
  static const int OB_ERR_USER_EXIST = -5050;
  static const int OB_ERR_PASSWORD_EMPTY = -5051;
  static const int OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE = -5052;
  static const int OB_ERR_WRONG_DYNAMIC_PARAM = -5053;
  static const int OB_ERR_PARAM_SIZE = -5054;
  static const int OB_ERR_FUNCTION_UNKNOWN = -5055;
  static const int OB_ERR_CREAT_MODIFY_TIME_COLUMN = -5056;
  static const int OB_ERR_MODIFY_PRIMARY_KEY = -5057;
  static const int OB_ERR_PARAM_DUPLICATE = -5058;
  static const int OB_ERR_TOO_MANY_SESSIONS = -5059;
  static const int OB_ERR_TOO_MANY_PS = -5061;
  static const int OB_ERR_HINT_UNKNOWN = -5063;
  static const int OB_ERR_WHEN_UNSATISFIED = -5064;
  static const int OB_ERR_QUERY_INTERRUPTED = -5065;
  static const int OB_ERR_SESSION_INTERRUPTED = -5066;
  static const int OB_ERR_UNKNOWN_SESSION_ID = -5067;
  static const int OB_ERR_PROTOCOL_NOT_RECOGNIZE = -5068;
  static const int OB_ERR_WRITE_AUTH_ERROR = -5069;
  static const int OB_ERR_PARSE_JOIN_INFO = -5070;
  static const int OB_ERR_ALTER_INDEX_COLUMN = -5071;
  static const int OB_ERR_MODIFY_INDEX_TABLE = -5072;
  static const int OB_ERR_INDEX_UNAVAILABLE = -5073;
  static const int OB_ERR_NOP_VALUE = -5074;
  static const int OB_ERR_PS_TOO_MANY_PARAM = -5080;
  static const int OB_ERR_READ_ONLY = -5081;
  static const int OB_ERR_INVALID_TYPE_FOR_OP = -5083;
  static const int OB_ERR_CAST_VARCHAR_TO_BOOL = -5084;
  static const int OB_ERR_CAST_VARCHAR_TO_NUMBER = -5085;
  static const int OB_ERR_CAST_VARCHAR_TO_TIME = -5086;
  static const int OB_ERR_CAST_NUMBER_OVERFLOW = -5087;
  static const int OB_INTEGER_PRECISION_OVERFLOW = -5088;
  static const int OB_DECIMAL_PRECISION_OVERFLOW = -5089;
  static const int OB_SCHEMA_NUMBER_PRECISION_OVERFLOW = -5090;
  static const int OB_SCHEMA_NUMBER_SCALE_OVERFLOW = -5091;
  static const int OB_ERR_INDEX_UNKNOWN = -5092;
  static const int OB_NUMERIC_OVERFLOW = -5093;
  static const int OB_ERR_TOO_MANY_JOIN_TABLES = -5094;
  static const int OB_ERR_VARCHAR_TOO_LONG = -5098;
  static const int OB_ERR_SYS_CONFIG_UNKNOWN = -5099;
  static const int OB_ERR_LOCAL_VARIABLE = -5100;
  static const int OB_ERR_GLOBAL_VARIABLE = -5101;
  static const int OB_ERR_VARIABLE_IS_READONLY = -5102;
  static const int OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR = -5103;
  static const int OB_ERR_EXPIRE_INFO_TOO_LONG = -5104;
  static const int OB_ERR_EXPIRE_COND_TOO_LONG = -5105;
  static const int OB_INVALID_ARGUMENT_FOR_EXTRACT = -5106;
  static const int OB_INVALID_ARGUMENT_FOR_IS = -5107;
  static const int OB_INVALID_ARGUMENT_FOR_LENGTH = -5108;
  static const int OB_INVALID_ARGUMENT_FOR_SUBSTR = -5109;
  static const int OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC = -5110;
  static const int OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME = -5111;
  static const int OB_ERR_USER_VARIABLE_UNKNOWN = -5112;
  static const int OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME = -5113;
  static const int OB_INVALID_NUMERIC = -5114;
  static const int OB_ERR_REGEXP_ERROR = -5115;
  static const int OB_SQL_LOG_OP_SETCHILD_OVERFLOW = -5116;
  static const int OB_SQL_EXPLAIN_FAILED = -5117;
  static const int OB_SQL_OPT_COPY_OP_FAILED = -5118;
  static const int OB_SQL_OPT_GEN_PLAN_FALIED = -5119;
  static const int OB_SQL_OPT_CREATE_RAWEXPR_FAILED = -5120;
  static const int OB_SQL_OPT_JOIN_ORDER_FAILED = -5121;
  static const int OB_SQL_OPT_ERROR = -5122;
  static const int OB_SQL_RESOLVER_NO_MEMORY = -5130;
  static const int OB_SQL_DML_ONLY = -5131;
  static const int OB_ERR_NO_GRANT = -5133;
  static const int OB_ERR_NO_DB_SELECTED = -5134;
  static const int OB_SQL_PC_OVERFLOW = -5135;
  static const int OB_SQL_PC_PLAN_DUPLICATE = -5136;
  static const int OB_SQL_PC_PLAN_EXPIRE = -5137;
  static const int OB_SQL_PC_NOT_EXIST = -5138;
  static const int OB_SQL_PARAMS_LIMIT = -5139;
  static const int OB_SQL_PC_PLAN_SIZE_LIMIT = -5140;
  static const int OB_ERR_UNKNOWN_CHARSET = -5142;
  static const int OB_ERR_UNKNOWN_COLLATION = -5143;
  static const int OB_ERR_COLLATION_MISMATCH = -5144;
  static const int OB_ERR_WRONG_VALUE_FOR_VAR = -5145;
  static const int OB_UNKNOWN_PARTITION = -5146;
  static const int OB_PARTITION_NOT_MATCH = -5147;
  static const int OB_ER_PASSWD_LENGTH = -5148;
  static const int OB_ERR_INSERT_INNER_JOIN_COLUMN = -5149;
  static const int OB_TENANT_NOT_IN_SERVER = -5150;
  static const int OB_TABLEGROUP_NOT_EXIST = -5151;
  static const int OB_SUBQUERY_TOO_MANY_ROW = -5153;
  static const int OB_ERR_BAD_DATABASE = -5154;
  static const int OB_CANNOT_USER = -5155;
  static const int OB_TENANT_EXIST = -5156;
  static const int OB_TENANT_NOT_EXIST = -5157;
  static const int OB_DATABASE_EXIST = -5158;
  static const int OB_TABLEGROUP_EXIST = -5159;
  static const int OB_ERR_INVALID_TENANT_NAME = -5160;
  static const int OB_EMPTY_TENANT = -5161;
  static const int OB_WRONG_DB_NAME = -5162;
  static const int OB_WRONG_TABLE_NAME = -5163;
  static const int OB_WRONG_COLUMN_NAME = -5164;
  static const int OB_ERR_COLUMN_SPEC = -5165;
  static const int OB_ERR_DB_DROP_EXISTS = -5166;
  static const int OB_ERR_DATA_TOO_LONG = -5167;
  static const int OB_ERR_WRONG_VALUE_COUNT_ON_ROW = -5168;
  static const int OB_ERR_CREATE_USER_WITH_GRANT = -5169;
  static const int OB_ERR_NO_DB_PRIVILEGE = -5170;
  static const int OB_ERR_NO_TABLE_PRIVILEGE = -5171;
  static const int OB_INVALID_ON_UPDATE = -5172;
  static const int OB_INVALID_DEFAULT = -5173;
  static const int OB_ERR_UPDATE_TABLE_USED = -5174;
  static const int OB_ERR_COULUMN_VALUE_NOT_MATCH = -5175;
  static const int OB_ERR_INVALID_GROUP_FUNC_USE = -5176;
  static const int OB_CANT_AGGREGATE_2COLLATIONS = -5177;
  static const int OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD = -5178;
  static const int OB_ERR_TOO_LONG_IDENT = -5179;
  static const int OB_ERR_WRONG_TYPE_FOR_VAR = -5180;
  static const int OB_WRONG_USER_NAME_LENGTH = -5181;
  static const int OB_ERR_PRIV_USAGE = -5182;
  static const int OB_ILLEGAL_GRANT_FOR_TABLE = -5183;
  static const int OB_ERR_REACH_AUTOINC_MAX = -5184;
  static const int OB_ERR_NO_TABLES_USED = -5185;
  static const int OB_CANT_REMOVE_ALL_FIELDS = -5187;
  static const int OB_TOO_MANY_PARTITIONS_ERROR = -5188;
  static const int OB_NO_PARTS_ERROR = -5189;
  static const int OB_WRONG_SUB_KEY = -5190;
  static const int OB_KEY_PART_0 = -5191;
  static const int OB_ERR_UNKNOWN_TIME_ZONE = -5192;
  static const int OB_ERR_WRONG_AUTO_KEY = -5193;
  static const int OB_ERR_TOO_MANY_KEYS = -5194;
  static const int OB_ERR_TOO_MANY_ROWKEY_COLUMNS = -5195;
  static const int OB_ERR_TOO_LONG_KEY_LENGTH = -5196;
  static const int OB_ERR_TOO_MANY_COLUMNS = -5197;
  static const int OB_ERR_TOO_LONG_COLUMN_LENGTH = -5198;
  static const int OB_ERR_TOO_BIG_ROWSIZE = -5199;
  static const int OB_ERR_UNKNOWN_TABLE = -5200;
  static const int OB_ERR_BAD_TABLE = -5201;
  static const int OB_ERR_TOO_BIG_SCALE = -5202;
  static const int OB_ERR_TOO_BIG_PRECISION = -5203;
  static const int OB_ERR_M_BIGGER_THAN_D = -5204;
  static const int OB_ERR_TOO_BIG_DISPLAYWIDTH = -5205;
  static const int OB_WRONG_GROUP_FIELD = -5206;
  static const int OB_NON_UNIQ_ERROR = -5207;
  static const int OB_ERR_NONUNIQ_TABLE = -5208;
  static const int OB_ERR_CANT_DROP_FIELD_OR_KEY = -5209;
  static const int OB_ERR_MULTIPLE_PRI_KEY = -5210;
  static const int OB_ERR_KEY_COLUMN_DOES_NOT_EXITS = -5211;
  static const int OB_ERR_AUTO_PARTITION_KEY = -5212;
  static const int OB_ERR_CANT_USE_OPTION_HERE = -5213;
  static const int OB_ERR_WRONG_OBJECT = -5214;
  static const int OB_ERR_ON_RENAME = -5215;
  static const int OB_ERR_WRONG_KEY_COLUMN = -5216;
  static const int OB_ERR_BAD_FIELD_ERROR = -5217;
  static const int OB_ERR_WRONG_FIELD_WITH_GROUP = -5218;
  static const int OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS = -5219;
  static const int OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION = -5220;
  static const int OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS = -5221;
  static const int OB_ERR_TRUNCATED_WRONG_VALUE = -5222;
  static const int OB_ERR_WRONG_IDENT_NAME = -5223;
  static const int OB_WRONG_NAME_FOR_INDEX = -5224;
  static const int OB_ILLEGAL_REFERENCE = -5225;
  static const int OB_REACH_MEMORY_LIMIT = -5226;
  static const int OB_ERR_PASSWORD_FORMAT = -5227;
  static const int OB_ERR_NON_UPDATABLE_TABLE = -5228;
  static const int OB_ERR_WARN_DATA_OUT_OF_RANGE = -5229;
  static const int OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR = -5230;
  static const int OB_ERR_VIEW_INVALID = -5231;
  static const int OB_ERR_OPTION_PREVENTS_STATEMENT = -5233;
  static const int OB_ERR_DB_READ_ONLY = -5234;
  static const int OB_ERR_TABLE_READ_ONLY = -5235;
  static const int OB_ERR_LOCK_OR_ACTIVE_TRANSACTION = -5236;
  static const int OB_ERR_SAME_NAME_PARTITION_FIELD = -5237;
  static const int OB_ERR_TABLENAME_NOT_ALLOWED_HERE = -5238;
  static const int OB_ERR_VIEW_RECURSIVE = -5239;
  static const int OB_ERR_QUALIFIER = -5240;
  static const int OB_ERR_WRONG_VALUE = -5241;
  static const int OB_ERR_VIEW_WRONG_LIST = -5242;
  static const int OB_SYS_VARS_MAYBE_DIFF_VERSION = -5243;
  static const int OB_ERR_AUTO_INCREMENT_CONFLICT = -5244;
  static const int OB_ERR_TASK_SKIPPED = -5245;
  static const int OB_ERR_NAME_BECOMES_EMPTY = -5246;
  static const int OB_ERR_REMOVED_SPACES = -5247;
  static const int OB_WARN_ADD_AUTOINCREMENT_COLUMN = -5248;
  static const int OB_WARN_CHAMGE_NULL_ATTRIBUTE = -5249;
  static const int OB_ERR_INVALID_CHARACTER_STRING = -5250;
  static const int OB_ERR_KILL_DENIED = -5251;
  static const int OB_ERR_COLUMN_DEFINITION_AMBIGUOUS = -5252;
  static const int OB_ERR_EMPTY_QUERY = -5253;
  static const int OB_ERR_CUT_VALUE_GROUP_CONCAT = -5254;
  static const int OB_ERR_FILED_NOT_FOUND_PART = -5255;
  static const int OB_ERR_PRIMARY_CANT_HAVE_NULL = -5256;
  static const int OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR = -5257;
  static const int OB_ERR_INVALID_BLOCK_SIZE = -5258;
  static const int OB_ERR_UNKNOWN_STORAGE_ENGINE = -5259;
  static const int OB_ERR_TENANT_IS_LOCKED = -5260;
  static const int OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF = -5261;
  static const int OB_ERR_AGGREGATE_ORDER_FOR_UNION = -5263;
  static const int OB_ERR_OUTLINE_EXIST = -5264;
  static const int OB_OUTLINE_NOT_EXIST = -5265;
  static const int OB_WARN_OPTION_BELOW_LIMIT = -5266;
  static const int OB_INVALID_OUTLINE = -5267;
  static const int OB_REACH_MAX_CONCURRENT_NUM = -5268;
  static const int OB_ERR_OPERATION_ON_RECYCLE_OBJECT = -5269;
  static const int OB_ERR_OBJECT_NOT_IN_RECYCLEBIN = -5270;
  static const int OB_ERR_CON_COUNT_ERROR = -5271;
  static const int OB_ERR_OUTLINE_CONTENT_EXIST = -5272;
  static const int OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST = -5273;
  static const int OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR = -5274;
  static const int OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR = -5275;
  static const int OB_ERR_PARTITION_COLUMN_LIST_ERROR = -5276;
  static const int OB_ERR_TOO_MANY_VALUES_ERROR = -5277;
  static const int OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED = -5278;
  static const int OB_ERR_PARTITION_INTERVAL_ERROR = -5279;
  static const int OB_ERR_SAME_NAME_PARTITION = -5280;
  static const int OB_ERR_RANGE_NOT_INCREASING_ERROR = -5281;
  static const int OB_ERR_PARSE_PARTITION_RANGE = -5282;
  static const int OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF = -5283;
  static const int OB_NO_PARTITION_FOR_GIVEN_VALUE = -5284;
  static const int OB_EER_NULL_IN_VALUES_LESS_THAN = -5285;
  static const int OB_ERR_PARTITION_CONST_DOMAIN_ERROR = -5286;
  static const int OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS = -5287;
  static const int OB_ERR_BAD_FT_COLUMN = -5288;
  static const int OB_ERR_KEY_DOES_NOT_EXISTS = -5289;
  static const int OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN = -5290;
  static const int OB_ERR_BAD_CTXCAT_COLUMN = -5291;
  static const int OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN = -5292;
  static const int OB_ERR_DEPENDENT_BY_GENERATED_COLUMN = -5293;
  static const int OB_ERR_TOO_MANY_ROWS = -5294;
  static const int OB_WRONG_FIELD_TERMINATORS = -5295;
  static const int OB_ERR_UNEXPECTED_TZ_TRANSITION = -5296;
  static const int OB_ERR_INVALID_TIMEZONE_REGION_ID = -5341;
  static const int OB_ERR_INVALID_HEX_NUMBER = -5342;
  static const int OB_ERR_INVALID_NUMBER_FORMAT_MODEL = -5608;
  static const int OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE = -5629;
  static const int OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE = -5630;
  static const int OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE = -5631;
  static const int OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE = -5632;
  static const int OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE = -5633;
  static const int OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY = -5634;
  static const int OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY = -5635;
  static const int OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY = -5636;
  static const int OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED = -5637;
  static const int OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH = -5638;
  static const int OB_ERR_INVALID_YEAR_VALUE = -5639;
  static const int OB_ERR_INVALID_MONTH = -5641;
  static const int OB_ERR_INVALID_DAY_OF_THE_WEEK = -5642;
  static const int OB_ERR_INVALID_DAY_OF_YEAR_VALUE = -5643;
  static const int OB_ERR_INVALID_HOUR12_VALUE = -5644;
  static const int OB_ERR_INVALID_HOUR24_VALUE = -5645;
  static const int OB_ERR_INVALID_MINUTES_VALUE = -5646;
  static const int OB_ERR_INVALID_SECONDS_VALUE = -5647;
  static const int OB_ERR_INVALID_SECONDS_IN_DAY_VALUE = -5648;
  static const int OB_ERR_INVALID_JULIAN_DATE_VALUE = -5649;
  static const int OB_ERR_AM_OR_PM_REQUIRED = -5650;
  static const int OB_ERR_BC_OR_AD_REQUIRED = -5651;
  static const int OB_ERR_FORMAT_CODE_APPEARS_TWICE = -5652;
  static const int OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE = -5653;
  static const int OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD = -5654;
  static const int OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR = -5655;
  static const int OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE = -5656;
  static const int OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE = -5657;
  static const int OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT = -5658;
  static const int OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT = -5659;
  static const int OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE = -5660;
  static const int OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE = -5661;
  static const int OB_ERR_FORMAT_CODE_CANNOT_APPEAR = -5662;
  static const int OB_ERR_NON_NUMERIC_CHARACTER_VALUE = -5663;
  static const int OB_INVALID_MERIDIAN_INDICATOR_USE = -5664;
  static const int OB_ERR_DAY_OF_MONTH_RANGE = -5667;
  static const int OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL = -5708;
  static const int OB_ERR_INVALID_TIME_ZONE_HOUR = -5709;
  static const int OB_ERR_INVALID_TIME_ZONE_MINUTE = -5710;
  static const int OB_ERR_NOT_A_VALID_TIME_ZONE = -5711;
  static const int OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER = -5712;
  static const int OB_ERR_REROUTE = -5727;
  static const int OB_ERR_REGEXP_NOMATCH = -5809;
  static const int OB_ERR_REGEXP_BADPAT = -5810;
  static const int OB_ERR_REGEXP_EESCAPE = -5811;
  static const int OB_ERR_REGEXP_EBRACK = -5812;
  static const int OB_ERR_REGEXP_EPAREN = -5813;
  static const int OB_ERR_REGEXP_ESUBREG = -5814;
  static const int OB_ERR_REGEXP_ERANGE = -5815;
  static const int OB_ERR_REGEXP_ECTYPE = -5816;
  static const int OB_ERR_REGEXP_ECOLLATE = -5817;
  static const int OB_ERR_REGEXP_EBRACE = -5818;
  static const int OB_ERR_REGEXP_BADBR = -5819;
  static const int OB_ERR_REGEXP_BADRPT = -5820;
  static const int OB_ERR_REGEXP_ASSERT = -5821;
  static const int OB_ERR_REGEXP_INVARG = -5822;
  static const int OB_ERR_REGEXP_MIXED = -5823;
  static const int OB_ERR_REGEXP_BADOPT = -5824;
  static const int OB_ERR_REGEXP_ETOOBIG = -5825;
  static const int OB_INVALID_ROWID = -5802;
  static const int OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR = -5898;
  static const int OB_ERR_FETCH_OUT_SEQUENCE = -5931;
  static const int OB_TRANSACTION_SET_VIOLATION = -6001;
  static const int OB_TRANS_ROLLBACKED = -6002;
  static const int OB_ERR_EXCLUSIVE_LOCK_CONFLICT = -6003;
  static const int OB_ERR_SHARED_LOCK_CONFLICT = -6004;
  static const int OB_TRY_LOCK_ROW_CONFLICT = -6005;
  static const int OB_CLOCK_OUT_OF_ORDER = -6201;
  static const int OB_MASK_SET_NO_NODE = -6203;
  static const int OB_TRANS_HAS_DECIDED = -6204;
  static const int OB_TRANS_INVALID_STATE = -6205;
  static const int OB_TRANS_STATE_NOT_CHANGE = -6206;
  static const int OB_TRANS_PROTOCOL_ERROR = -6207;
  static const int OB_TRANS_INVALID_MESSAGE = -6208;
  static const int OB_TRANS_INVALID_MESSAGE_TYPE = -6209;
  static const int OB_TRANS_TIMEOUT = -6210;
  static const int OB_TRANS_KILLED = -6211;
  static const int OB_TRANS_STMT_TIMEOUT = -6212;
  static const int OB_TRANS_CTX_NOT_EXIST = -6213;
  static const int OB_PARTITION_IS_FROZEN = -6214;
  static const int OB_PARTITION_IS_NOT_FROZEN = -6215;
  static const int OB_TRANS_INVALID_LOG_TYPE = -6219;
  static const int OB_TRANS_SQL_SEQUENCE_ILLEGAL = -6220;
  static const int OB_TRANS_CANNOT_BE_KILLED = -6221;
  static const int OB_TRANS_STATE_UNKNOWN = -6222;
  static const int OB_TRANS_IS_EXITING = -6223;
  static const int OB_TRANS_NEED_ROLLBACK = -6224;
  static const int OB_TRANS_UNKNOWN = -6225;
  static const int OB_ERR_READ_ONLY_TRANSACTION = -6226;
  static const int OB_PARTITION_IS_NOT_STOPPED = -6227;
  static const int OB_PARTITION_IS_STOPPED = -6228;
  static const int OB_PARTITION_IS_BLOCKED = -6229;
  static const int OB_TRANS_RPC_TIMEOUT = -6230;
  static const int OB_TRANS_FREE_ROUTE_NOT_SUPPORTED = -6279;
  static const int OB_LOG_ID_NOT_FOUND = -6301;
  static const int OB_LSR_THREAD_STOPPED = -6302;
  static const int OB_NO_LOG = -6303;
  static const int OB_LOG_ID_RANGE_ERROR = -6304;
  static const int OB_LOG_ITER_ENOUGH = -6305;
  static const int OB_CLOG_INVALID_ACK = -6306;
  static const int OB_CLOG_CACHE_INVALID = -6307;
  static const int OB_EXT_HANDLE_UNFINISH = -6308;
  static const int OB_CURSOR_NOT_EXIST = -6309;
  static const int OB_STREAM_NOT_EXIST = -6310;
  static const int OB_STREAM_BUSY = -6311;
  static const int OB_FILE_RECYCLED = -6312;
  static const int OB_REPLAY_EAGAIN_TOO_MUCH_TIME = -6313;
  static const int OB_MEMBER_CHANGE_FAILED = -6314;
  static const int OB_ELECTION_WARN_LOGBUF_FULL = -7000;
  static const int OB_ELECTION_WARN_LOGBUF_EMPTY = -7001;
  static const int OB_ELECTION_WARN_NOT_RUNNING = -7002;
  static const int OB_ELECTION_WARN_IS_RUNNING = -7003;
  static const int OB_ELECTION_WARN_NOT_REACH_MAJORITY = -7004;
  static const int OB_ELECTION_WARN_INVALID_SERVER = -7005;
  static const int OB_ELECTION_WARN_INVALID_LEADER = -7006;
  static const int OB_ELECTION_WARN_LEADER_LEASE_EXPIRED = -7007;
  static const int OB_ELECTION_WARN_INVALID_MESSAGE = -7010;
  static const int OB_ELECTION_WARN_MESSAGE_NOT_INTIME = -7011;
  static const int OB_ELECTION_WARN_NOT_CANDIDATE = -7012;
  static const int OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER = -7013;
  static const int OB_ELECTION_WARN_PROTOCOL_ERROR = -7014;
  static const int OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE = -7015;
  static const int OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE = -7021;
  static const int OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER = -7022;
  static const int OB_ELECTION_WARN_NO_PREPARE_MESSAGE = -7024;
  static const int OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE = -7025;
  static const int OB_ELECTION_NOT_EXIST = -7026;
  static const int OB_ELECTION_MGR_IS_RUNNING = -7027;
  static const int OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE = -7029;
  static const int OB_ELECTION_ASYNC_LOG_WARN_INIT = -7030;
  static const int OB_ELECTION_WAIT_LEADER_MESSAGE = -7031;
  static const int OB_SERVER_IS_INIT = -8001;
  static const int OB_SERVER_IS_STOPPING = -8002;
  static const int OB_PACKET_CHECKSUM_ERROR = -8003;
  static const int OB_SSL_ERROR = -8004;
  static const int OB_UNSUPPORTED_PS = -8005;
  static const int OB_SEND_INIT_SQL_ERROR = -8006;
  static const int OB_INTERNAL_CMD_VALUE_TOO_LONG = -8007;
  static const int OB_ERR_LIMIT = -8009;
  static const int OB_ERR_ABORTING_CONNECTION = -8010;
  static const int OB_SHARD_CONF_INVALID = -8055;
  static const int OB_SEQUENCE_ERROR = -8056;
  static const int OB_EXPR_CALC_ERROR = -8057;
  static const int OB_EXPR_COLUMN_NOT_EXIST = -8058;
  static const int OB_ERROR_UNSUPPORT_EXPR_TYPE = -8059;
  static const int OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP = -8061;
  static const int OB_ERR_MORE_TABLES_WITH_TABLE_HINT = -8062;
  static const int OB_ERR_GET_PHYSIC_INDEX_BY_RULE = -8063;
  static const int OB_ERR_TESTLOAD_ALIPAY_COMPATIBLE = -8064;
  static const int OB_ERR_NULL_DB_VAL_TESTLOAD_TABLE_MAP = -8065;
  static const int OB_ERR_BATCH_INSERT_FOUND = -8066;
  static const int OB_ERR_UNSUPPORT_DIFF_TOPOLOGY = -8067;
  static const int OB_SESSION_POOL_CMD_ERROR = -8101;
  static const int OB_SESSION_POOL_FULL_ERROR = -8102;
  static const int OB_ERR_NO_ZONE_SHARD_TPO = -8200;
  static const int OB_ERR_NO_DEFAULT_SHARD_TPO = -8201;
  static const int OB_ERR_NO_TABLE_RULE = -8202;
  static const int OB_ERR_DISTRIBUTED_TRANSACTION_NOT_SUPPORTED = -8203;
  static const int OB_ERR_SHARD_DDL_UNEXPECTED = -8204;
  static const int OB_ERR_CAN_NOT_PASS_WHITELIST = -8205;

  // obproxy common error code
  static const int OB_PROXY_INTERNAL_ERROR = -10001;
  static const int OB_CLIENT_RECEIVING_PACKET_CONNECTION_ERROR = -10010;
  static const int OB_CLIENT_HANDLING_REQUEST_CONNECTION_ERROR = -10011;
  static const int OB_CLIENT_TRANSFERRING_PACKET_CONNECTION_ERROR = -10012;
  static const int OB_SERVER_BUILD_CONNECTION_ERROR = -10013;
  static const int OB_SERVER_RECEIVING_PACKET_CONNECTION_ERROR = -10014;
  static const int OB_SERVER_CONNECT_TIMEOUT = -10015;
  static const int OB_SERVER_TRANSFERRING_PACKET_CONNECTION_ERROR = -10016;
  static const int OB_PLUGIN_TRANSFERRING_ERROR = -10017;
  static const int OB_PROXY_INTERNAL_REQUEST_FAIL = -10018;
  static const int OB_PROXY_NO_NEED_RETRY = -10019;
  static const int OB_PROXY_HOLD_CONNECTION = -10020;
  static const int OB_PROXY_INVALID_USER = -10021;
  static const int OB_PROXY_INACTIVITY_TIMEOUT = -10022;
  static const int OB_PROXY_RECONNECT_COORDINATOR = -10023;
  static const int OB_PROXY_INVALID_COORDINATOR = -10024;
  static const int OB_PROXY_PROXY_ID_OVER_LIMIT = -10025;

  // obproxy related error code
  static const int OB_PROXY_FETCH_RSLIST_FAIL = -10101;
  static const int OB_CONNECT_BINLOG_ERROR = -10102;
  static const int OB_PROXY_CLUSTER_RESOURCE_EXPIRED = -10103;

  // sharding related error code
  static const int OB_PROXY_SHARD_HINT_NOT_SUPPORTED = -10304;
  static const int OB_PROXY_SHARD_INVALID_CONFIG = -10305;

  // kv related error code


  // proxy client related error code


#define OB_SUCCESS__USER_ERROR_MSG "Success"
#define OB_ERROR__USER_ERROR_MSG "Common error"
#define OB_OBJ_TYPE_ERROR__USER_ERROR_MSG "Object type error"
#define OB_INVALID_ARGUMENT__USER_ERROR_MSG "Incorrect arguments to %s"
#define OB_ARRAY_OUT_OF_RANGE__USER_ERROR_MSG "Array index out of range"
#define OB_SERVER_LISTEN_ERROR__USER_ERROR_MSG "Failed to listen to the port"
#define OB_INIT_TWICE__USER_ERROR_MSG "The object is initialized twice"
#define OB_NOT_INIT__USER_ERROR_MSG "The object is not initialized"
#define OB_NOT_SUPPORTED__USER_ERROR_MSG "%s not supported"
#define OB_ITER_END__USER_ERROR_MSG "End of iteration"
#define OB_IO_ERROR__USER_ERROR_MSG "IO error"
#define OB_ERROR_FUNC_VERSION__USER_ERROR_MSG "Wrong RPC command version"
#define OB_PACKET_NOT_SENT__USER_ERROR_MSG "Can not send packet"
#define OB_TIMEOUT__USER_ERROR_MSG "Timeout"
#define OB_ALLOCATE_MEMORY_FAILED__USER_ERROR_MSG "No memory or reach tenant memory limit"
#define OB_INNER_STAT_ERROR__USER_ERROR_MSG "Inner state error"
#define OB_ERR_SYS__USER_ERROR_MSG "System error"
#define OB_ERR_UNEXPECTED__USER_ERROR_MSG "%s"
#define OB_ENTRY_EXIST__USER_ERROR_MSG "Entry already exist"
#define OB_ENTRY_NOT_EXIST__USER_ERROR_MSG "Entry not exist"
#define OB_SIZE_OVERFLOW__USER_ERROR_MSG "Size overflow"
#define OB_REF_NUM_NOT_ZERO__USER_ERROR_MSG "Reference count is not zero"
#define OB_CONFLICT_VALUE__USER_ERROR_MSG "Conflict value"
#define OB_ITEM_NOT_SETTED__USER_ERROR_MSG "Item not set"
#define OB_EAGAIN__USER_ERROR_MSG "Try again"
#define OB_BUF_NOT_ENOUGH__USER_ERROR_MSG "Buffer not enough"
#define OB_PARTIAL_FAILED__USER_ERROR_MSG "Partial failed"
#define OB_READ_NOTHING__USER_ERROR_MSG "Nothing to read"
#define OB_FILE_NOT_EXIST__USER_ERROR_MSG "File not exist"
#define OB_DISCONTINUOUS_LOG__USER_ERROR_MSG "Log entry not continuous"
#define OB_SCHEMA_ERROR__USER_ERROR_MSG "Schema error"
#define OB_TENANT_OUT_OF_MEM__USER_ERROR_MSG "Over tenant memory limits"
#define OB_UNKNOWN_OBJ__USER_ERROR_MSG "Unknown object"
#define OB_NO_MONITOR_DATA__USER_ERROR_MSG "No monitor data"
#define OB_SERIALIZE_ERROR__USER_ERROR_MSG "Serialize error"
#define OB_DESERIALIZE_ERROR__USER_ERROR_MSG "Deserialize error"
#define OB_AIO_TIMEOUT__USER_ERROR_MSG "Asynchronous IO error"
#define OB_NEED_RETRY__USER_ERROR_MSG "Need retry"
#define OB_TOO_MANY_SSTABLE__USER_ERROR_MSG "Too many sstable"
#define OB_NOT_MASTER__USER_ERROR_MSG "The observer or zone is not the master"
#define OB_DECRYPT_FAILED__USER_ERROR_MSG "Decrypt error"
#define OB_USER_NOT_EXIST__USER_ERROR_MSG "Can not find any matching row in the user table"
#define OB_PASSWORD_WRONG__USER_ERROR_MSG "Access denied for user '%.*s'@'%.*s' (using password: %s)"
#define OB_SKEY_VERSION_WRONG__USER_ERROR_MSG "Wrong skey version"
#define OB_NOT_REGISTERED__USER_ERROR_MSG "Not registered"
#define OB_WAITQUEUE_TIMEOUT__USER_ERROR_MSG "Task timeout and not executed"
#define OB_NOT_THE_OBJECT__USER_ERROR_MSG "Not the object"
#define OB_ALREADY_REGISTERED__USER_ERROR_MSG "Already registered"
#define OB_LAST_LOG_RUINNED__USER_ERROR_MSG "Corrupted log entry"
#define OB_NO_CS_SELECTED__USER_ERROR_MSG "No ChunkServer selected"
#define OB_NO_TABLETS_CREATED__USER_ERROR_MSG "No tablets created"
#define OB_INVALID_ERROR__USER_ERROR_MSG "Invalid entry"
#define OB_DECIMAL_OVERFLOW_WARN__USER_ERROR_MSG "Decimal overflow warning"
#define OB_DECIMAL_UNLEGAL_ERROR__USER_ERROR_MSG "Decimal overflow error"
#define OB_OBJ_DIVIDE_ERROR__USER_ERROR_MSG "Divide error"
#define OB_NOT_A_DECIMAL__USER_ERROR_MSG "Not a decimal"
#define OB_DECIMAL_PRECISION_NOT_EQUAL__USER_ERROR_MSG "Decimal precision error"
#define OB_EMPTY_RANGE__USER_ERROR_MSG "Empty range"
#define OB_SESSION_KILLED__USER_ERROR_MSG "Session killed"
#define OB_LOG_NOT_SYNC__USER_ERROR_MSG "Log not sync"
#define OB_DIR_NOT_EXIST__USER_ERROR_MSG "Directory not exist"
#define OB_SESSION_NOT_FOUND__USER_ERROR_MSG "RPC session not found"
#define OB_INVALID_LOG__USER_ERROR_MSG "Invalid log"
#define OB_INVALID_DATA__USER_ERROR_MSG "Invalid data"
#define OB_ALREADY_DONE__USER_ERROR_MSG "Already done"
#define OB_CANCELED__USER_ERROR_MSG "Operation canceled"
#define OB_LOG_SRC_CHANGED__USER_ERROR_MSG "Log source changed"
#define OB_LOG_NOT_ALIGN__USER_ERROR_MSG "Log not aligned"
#define OB_LOG_MISSING__USER_ERROR_MSG "Log entry missed"
#define OB_NEED_WAIT__USER_ERROR_MSG "Need wait"
#define OB_NOT_IMPLEMENT__USER_ERROR_MSG "Not implemented feature"
#define OB_DIVISION_BY_ZERO__USER_ERROR_MSG "Divided by zero"
#define OB_EXCEED_MEM_LIMIT__USER_ERROR_MSG "exceed memory limit"
#define OB_RESULT_UNKNOWN__USER_ERROR_MSG "Unknown result"
#define OB_NO_RESULT__USER_ERROR_MSG "No result"
#define OB_QUEUE_OVERFLOW__USER_ERROR_MSG "Queue overflow"
#define OB_TERM_LAGGED__USER_ERROR_MSG "Term lagged"
#define OB_TERM_NOT_MATCH__USER_ERROR_MSG "Term not match"
#define OB_START_LOG_CURSOR_INVALID__USER_ERROR_MSG "Invalid log cursor"
#define OB_LOCK_NOT_MATCH__USER_ERROR_MSG "Lock not match"
#define OB_DEAD_LOCK__USER_ERROR_MSG "Deadlock"
#define OB_PARTIAL_LOG__USER_ERROR_MSG "Incomplete log entry"
#define OB_CHECKSUM_ERROR__USER_ERROR_MSG "Data checksum error"
#define OB_INIT_FAIL__USER_ERROR_MSG "Initialize error"
#define OB_NOT_ENOUGH_STORE__USER_ERROR_MSG "not enough commitlog store"
#define OB_BLOCK_SWITCHED__USER_ERROR_MSG "block switched when fill commitlog"
#define OB_STATE_NOT_MATCH__USER_ERROR_MSG "Server state or role not the same as expected"
#define OB_READ_ZERO_LOG__USER_ERROR_MSG "Read zero log"
#define OB_BLOCK_NEED_FREEZE__USER_ERROR_MSG "block need freeze"
#define OB_BLOCK_FROZEN__USER_ERROR_MSG "block frozen"
#define OB_IN_FATAL_STATE__USER_ERROR_MSG "In FATAL state"
#define OB_IN_STOP_STATE__USER_ERROR_MSG "In STOP state"
#define OB_UPS_MASTER_EXISTS__USER_ERROR_MSG "Master UpdateServer already exists"
#define OB_LOG_NOT_CLEAR__USER_ERROR_MSG "Log not clear"
#define OB_FILE_ALREADY_EXIST__USER_ERROR_MSG "File already exist"
#define OB_UNKNOWN_PACKET__USER_ERROR_MSG "Unknown packet"
#define OB_RPC_PACKET_TOO_LONG__USER_ERROR_MSG "RPC packet to send too long"
#define OB_LOG_TOO_LARGE__USER_ERROR_MSG "Log too large"
#define OB_RPC_SEND_ERROR__USER_ERROR_MSG "RPC send error"
#define OB_RPC_POST_ERROR__USER_ERROR_MSG "RPC post error"
#define OB_LIBEASY_ERROR__USER_ERROR_MSG "Libeasy error"
#define OB_CONNECT_ERROR__USER_ERROR_MSG "Connect error"
#define OB_NOT_FREE__USER_ERROR_MSG "Not free"
#define OB_INIT_SQL_CONTEXT_ERROR__USER_ERROR_MSG "Init SQL context error"
#define OB_SKIP_INVALID_ROW__USER_ERROR_MSG "Skip invalid row"
#define OB_RPC_PACKET_INVALID__USER_ERROR_MSG "RPC packet is invalid"
#define OB_NO_TABLET__USER_ERROR_MSG "No tablets"
#define OB_SNAPSHOT_DISCARDED__USER_ERROR_MSG "Request to read too old versioned data"
#define OB_DATA_NOT_UPTODATE__USER_ERROR_MSG "State is stale"
#define OB_ROW_MODIFIED__USER_ERROR_MSG "Row modified"
#define OB_VERSION_NOT_MATCH__USER_ERROR_MSG "Version not match"
#define OB_BAD_ADDRESS__USER_ERROR_MSG "Bad address"
#define OB_ENQUEUE_FAILED__USER_ERROR_MSG "Enqueue error"
#define OB_INVALID_CONFIG__USER_ERROR_MSG "Invalid config"
#define OB_STMT_EXPIRED__USER_ERROR_MSG "Expired statement"
#define OB_ERR_MIN_VALUE__USER_ERROR_MSG "Min value"
#define OB_ERR_MAX_VALUE__USER_ERROR_MSG "Max value"
#define OB_ERR_NULL_VALUE__USER_ERROR_MSG "%s"
#define OB_RESOURCE_OUT__USER_ERROR_MSG "Out of resource"
#define OB_ERR_SQL_CLIENT__USER_ERROR_MSG "Internal SQL client error"
#define OB_META_TABLE_WITHOUT_USE_TABLE__USER_ERROR_MSG "Meta table without use table"
#define OB_DISCARD_PACKET__USER_ERROR_MSG "Discard packet"
#define OB_OPERATE_OVERFLOW__USER_ERROR_MSG "%s value is out of range in '%s'"
#define OB_INVALID_DATE_FORMAT__USER_ERROR_MSG "%s=%d must between %d and %d"
#define OB_POOL_REGISTERED_FAILED__USER_ERROR_MSG "register pool failed"
#define OB_POOL_UNREGISTERED_FAILED__USER_ERROR_MSG "unregister pool failed"
#define OB_INVALID_ARGUMENT_NUM__USER_ERROR_MSG "Invalid argument num"
#define OB_LEASE_NOT_ENOUGH__USER_ERROR_MSG "reserved lease not enough"
#define OB_LEASE_NOT_MATCH__USER_ERROR_MSG "ups lease not match with rs"
#define OB_UPS_SWITCH_NOT_HAPPEN__USER_ERROR_MSG "ups switch not happen"
#define OB_EMPTY_RESULT__USER_ERROR_MSG "Empty result"
#define OB_CACHE_NOT_HIT__USER_ERROR_MSG "Cache not hit"
#define OB_NESTED_LOOP_NOT_SUPPORT__USER_ERROR_MSG "Nested loop not support"
#define OB_LOG_INVALID_MOD_ID__USER_ERROR_MSG "Invalid log module id"
#define OB_LOG_MODULE_UNKNOWN__USER_ERROR_MSG "Unknown module name. Invalid Setting:'%.*s'. Syntax:parMod.subMod:level, parMod.subMod:level"
#define OB_LOG_LEVEL_INVALID__USER_ERROR_MSG "Invalid level. Invalid setting:'%.*s'. Syntax:parMod.subMod:level, parMod.subMod:level"
#define OB_LOG_PARSER_SYNTAX_ERR__USER_ERROR_MSG "Syntax to set log_level error. Invalid setting:'%.*s'. Syntax:parMod.subMod:level, parMod.subMod:level"
#define OB_INDEX_OUT_OF_RANGE__USER_ERROR_MSG "Index out of range"
#define OB_INT_UNDERFLOW__USER_ERROR_MSG "Int underflow"
#define OB_UNKNOWN_CONNECTION__USER_ERROR_MSG "Unknown thread id: %lu"
#define OB_ERROR_OUT_OF_RANGE__USER_ERROR_MSG "Out of range"
#define OB_CACHE_SHRINK_FAILED__USER_ERROR_MSG "shrink cache failed, no available cache"
#define OB_OLD_SCHEMA_VERSION__USER_ERROR_MSG "Schema version too old"
#define OB_RELEASE_SCHEMA_ERROR__USER_ERROR_MSG "Release schema error"
#define OB_OP_NOT_ALLOW__USER_ERROR_MSG "%s not allowed"
#define OB_NO_EMPTY_ENTRY__USER_ERROR_MSG "No empty entry"
#define OB_ERR_ALREADY_EXISTS__USER_ERROR_MSG "Already exist"
#define OB_SEARCH_NOT_FOUND__USER_ERROR_MSG "Value not found"
#define OB_BEYOND_THE_RANGE__USER_ERROR_MSG "Key out of range"
#define OB_CS_OUTOF_DISK_SPACE__USER_ERROR_MSG "ChunkServer out of disk space"
#define OB_COLUMN_GROUP_NOT_FOUND__USER_ERROR_MSG "Column group not found"
#define OB_CS_COMPRESS_LIB_ERROR__USER_ERROR_MSG "ChunkServer failed to get compress library"
#define OB_ITEM_NOT_MATCH__USER_ERROR_MSG "Item not match"
#define OB_INVALID_DATE_FORMAT_END__USER_ERROR_MSG "Invalid date format"
#define OB_HASH_EXIST__USER_ERROR_MSG "hash map/set entry exist"
#define OB_HASH_NOT_EXIST__USER_ERROR_MSG "hash map/set entry not exist"
#define OB_HASH_GET_TIMEOUT__USER_ERROR_MSG "hash map/set get timeout"
#define OB_HASH_PLACEMENT_RETRY__USER_ERROR_MSG "hash map/set retry"
#define OB_HASH_FULL__USER_ERROR_MSG "hash map/set full"
#define OB_PACKET_PROCESSED__USER_ERROR_MSG "packet processed"
#define OB_WAIT_NEXT_TIMEOUT__USER_ERROR_MSG "wait next packet timeout"
#define OB_LEADER_NOT_EXIST__USER_ERROR_MSG "partition has not leader"
#define OB_PREPARE_MAJOR_FREEZE_FAILED__USER_ERROR_MSG "prepare major freeze failed"
#define OB_COMMIT_MAJOR_FREEZE_FAILED__USER_ERROR_MSG "commit major freeze failed"
#define OB_ABORT_MAJOR_FREEZE_FAILED__USER_ERROR_MSG "abort major freeze failed"
#define OB_MAJOR_FREEZE_NOT_FINISHED__USER_ERROR_MSG "last major freeze not finish"
#define OB_PARTITION_NOT_LEADER__USER_ERROR_MSG "partition is not leader partition"
#define OB_WAIT_MAJOR_FREEZE_RESPONSE_TIMEOUT__USER_ERROR_MSG "wait major freeze response timeout"
#define OB_CURL_ERROR__USER_ERROR_MSG "curl error"
#define OB_MAJOR_FREEZE_NOT_ALLOW__USER_ERROR_MSG "%s"
#define OB_PREPARE_FREEZE_FAILED__USER_ERROR_MSG "prepare freeze failed"
#define OB_INVALID_DATE_VALUE__USER_ERROR_MSG "Incorrect datetime value: '%s' for column '%s' at row %d"
#define OB_INACTIVE_SQL_CLIENT__USER_ERROR_MSG "Inactive sql client, only read allowed"
#define OB_INACTIVE_RPC_PROXY__USER_ERROR_MSG "Inactive rpc proxy, can not send RPC request"
#define OB_INTERVAL_WITH_MONTH__USER_ERROR_MSG "Interval with year or month can not be converted to microseconds"
#define OB_TOO_MANY_DATETIME_PARTS__USER_ERROR_MSG "Interval has too many datetime parts"
#define OB_DATA_OUT_OF_RANGE__USER_ERROR_MSG "Out of range value for column '%.*s' at row %lld"
#define OB_PARTITION_NOT_EXIST__USER_ERROR_MSG "Partition entry not exists"
#define OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD__USER_ERROR_MSG "Incorrect integer value: '%.*s'"
#define OB_ERR_NO_DEFAULT_FOR_FIELD__USER_ERROR_MSG "Field \'%s\' doesn't have a default value"
#define OB_ERR_FIELD_SPECIFIED_TWICE__USER_ERROR_MSG "Column \'%s\' specified twice"
#define OB_ERR_TOO_LONG_TABLE_COMMENT__USER_ERROR_MSG "Comment for table is too long (max = %ld)"
#define OB_ERR_TOO_LONG_FIELD_COMMENT__USER_ERROR_MSG "Comment for field is too long (max = %ld)"
#define OB_ERR_TOO_LONG_INDEX_COMMENT__USER_ERROR_MSG "Comment for index is too long (max = %ld)"
#define OB_NOT_FOLLOWER__USER_ERROR_MSG "The observer or zone is not a follower"
#define OB_ERR_OUT_OF_LOWER_BOUND__USER_ERROR_MSG "smaller than container lower bound"
#define OB_ERR_OUT_OF_UPPER_BOUND__USER_ERROR_MSG "bigger than container upper bound"
#define OB_BAD_NULL_ERROR__USER_ERROR_MSG "Column '%.*s' cannot be null"
#define OB_OBCONFIG_RETURN_ERROR__USER_ERROR_MSG "ObConfig return error code"
#define OB_OBCONFIG_APPNAME_MISMATCH__USER_ERROR_MSG "Appname mismatch with obconfig result"
#define OB_ERR_VIEW_SELECT_DERIVED__USER_ERROR_MSG "View's SELECT contains a subquery in the FROM clause"
#define OB_CANT_MJ_PATH__USER_ERROR_MSG "Can not use merge-join to join the tables without join conditions"
#define OB_ERR_NO_JOIN_ORDER_GENERATED__USER_ERROR_MSG "No join order generated"
#define OB_ERR_NO_PATH_GENERATED__USER_ERROR_MSG "No join path generated"
#define OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH__USER_ERROR_MSG "Schema error"
#define OB_FILE_NOT_OPENED__USER_ERROR_MSG "file not opened"
#define OB_TIMER_TASK_HAS_SCHEDULED__USER_ERROR_MSG "Timer task has been scheduled"
#define OB_TIMER_TASK_HAS_NOT_SCHEDULED__USER_ERROR_MSG "Timer task has not been scheduled"
#define OB_PARSE_DEBUG_SYNC_ERROR__USER_ERROR_MSG "parse debug sync string error"
#define OB_UNKNOWN_DEBUG_SYNC_POINT__USER_ERROR_MSG "unknown debug sync point"
#define OB_ERR_INTERRUPTED__USER_ERROR_MSG "task is interrupted while running"
#define OB_ERR_DATA_TRUNCATED__USER_ERROR_MSG "Data truncated for argument"
#define OB_NOT_RUNNING__USER_ERROR_MSG "module is not running"
#define OB_INVALID_PARTITION__USER_ERROR_MSG "partition not valid"
#define OB_ERR_TIMEOUT_TRUNCATED__USER_ERROR_MSG "Timeout value truncated to 102 years"
#define OB_ERR_TOO_LONG_TENANT_COMMENT__USER_ERROR_MSG "Comment for tenant is too long (max = %ld)"
#define OB_ERR_NET_PACKET_TOO_LARGE__USER_ERROR_MSG "Got a packet bigger than \'max_allowed_packet\' bytes"
#define OB_TRACE_DESC_NOT_EXIST__USER_ERROR_MSG "trace log title or key not exist describle"
#define OB_ERR_NO_DEFAULT__USER_ERROR_MSG "Variable '%.*s' doesn't have a default value"
#define OB_ERR_COMPRESS_DECOMPRESS_DATA__USER_ERROR_MSG "compress data or decompress data failed"
#define OB_ERR_INCORRECT_STRING_VALUE__USER_ERROR_MSG "Incorrect string value for column '%.*s' at row %lld"
#define OB_ERR_DISTRIBUTED_NOT_SUPPORTED__USER_ERROR_MSG "%s not supported"
#define OB_IS_CHANGING_LEADER__USER_ERROR_MSG "the partition is changing leader"
#define OB_DATETIME_FUNCTION_OVERFLOW__USER_ERROR_MSG "Datetime overflow"
#define OB_ERR_DOUBLE_TRUNCATED__USER_ERROR_MSG "Truncated incorrect DOUBLE value: '%.*s'"
#define OB_MINOR_FREEZE_NOT_ALLOW__USER_ERROR_MSG "%s"
#define OB_LOG_OUTOF_DISK_SPACE__USER_ERROR_MSG "Log out of disk space"
#define OB_ERR_KILL_CLIENT_SESSION__USER_ERROR_MSG "Client session interrupted"
#define OB_IMPORT_NOT_IN_SERVER__USER_ERROR_MSG "Import not in service"
#define OB_CONVERT_ERROR__USER_ERROR_MSG "Convert error"
#define OB_BYPASS_TIMEOUT__USER_ERROR_MSG "Bypass timeout"
#define OB_RS_STATE_NOT_ALLOW__USER_ERROR_MSG "RootServer state error"
#define OB_NO_REPLICA_VALID__USER_ERROR_MSG "No replica is valid"
#define OB_NO_NEED_UPDATE__USER_ERROR_MSG "No need to update"
#define OB_CACHE_TIMEOUT__USER_ERROR_MSG "Cache timeout"
#define OB_ITER_STOP__USER_ERROR_MSG "Iteration was stopped"
#define OB_ZONE_ALREADY_MASTER__USER_ERROR_MSG "The zone is the master already"
#define OB_IP_PORT_IS_NOT_SLAVE_ZONE__USER_ERROR_MSG "Not slave zone"
#define OB_ZONE_IS_NOT_SLAVE__USER_ERROR_MSG "Not slave zone"
#define OB_ZONE_IS_NOT_MASTER__USER_ERROR_MSG "Not master zone"
#define OB_CONFIG_NOT_SYNC__USER_ERROR_MSG "Configuration not sync"
#define OB_IP_PORT_IS_NOT_ZONE__USER_ERROR_MSG "Not a zone address"
#define OB_MASTER_ZONE_NOT_EXIST__USER_ERROR_MSG "Master zone not exist"
#define OB_ZONE_INFO_NOT_EXIST__USER_ERROR_MSG "Zone info \'%s\' not exist"
#define OB_GET_ZONE_MASTER_UPS_FAILED__USER_ERROR_MSG "Failed to get master UpdateServer"
#define OB_MULTIPLE_MASTER_ZONES_EXIST__USER_ERROR_MSG "Multiple master zones"
#define OB_INDEXING_ZONE_INVALID__USER_ERROR_MSG "indexing zone is not exist anymore or not active"
#define OB_ROOT_TABLE_RANGE_NOT_EXIST__USER_ERROR_MSG "Tablet range not exist"
#define OB_ROOT_MIGRATE_CONCURRENCY_FULL__USER_ERROR_MSG "Migrate concurrency full"
#define OB_ROOT_MIGRATE_INFO_NOT_FOUND__USER_ERROR_MSG "Migrate info not found"
#define OB_NOT_DATA_LOAD_TABLE__USER_ERROR_MSG "No data to load"
#define OB_DATA_LOAD_TABLE_DUPLICATED__USER_ERROR_MSG "Duplicated table data to load"
#define OB_ROOT_TABLE_ID_EXIST__USER_ERROR_MSG "Table ID exist"
#define OB_INDEX_TIMEOUT__USER_ERROR_MSG "Building index timeout"
#define OB_ROOT_NOT_INTEGRATED__USER_ERROR_MSG "Root not integrated"
#define OB_INDEX_INELIGIBLE__USER_ERROR_MSG "index data not unique"
#define OB_REBALANCE_EXEC_TIMEOUT__USER_ERROR_MSG "execute replication or migration task timeout"
#define OB_MERGE_NOT_STARTED__USER_ERROR_MSG "global merge not started"
#define OB_MERGE_ALREADY_STARTED__USER_ERROR_MSG "merge already started"
#define OB_ROOTSERVICE_EXIST__USER_ERROR_MSG "rootservice already exist"
#define OB_RS_SHUTDOWN__USER_ERROR_MSG "rootservice is shutdown"
#define OB_SERVER_MIGRATE_IN_DENIED__USER_ERROR_MSG "server migrate in denied"
#define OB_REBALANCE_TASK_CANT_EXEC__USER_ERROR_MSG "rebalance task can not executing now"
#define OB_PARTITION_CNT_REACH_ROOTSERVER_LIMIT__USER_ERROR_MSG "rootserver can not hold more partition"
#define OB_DATA_SOURCE_NOT_EXIST__USER_ERROR_MSG "Data source not exist"
#define OB_DATA_SOURCE_TABLE_NOT_EXIST__USER_ERROR_MSG "Data source table not exist"
#define OB_DATA_SOURCE_RANGE_NOT_EXIST__USER_ERROR_MSG "Data source range not exist"
#define OB_DATA_SOURCE_DATA_NOT_EXIST__USER_ERROR_MSG "Data source data not exist"
#define OB_DATA_SOURCE_SYS_ERROR__USER_ERROR_MSG "Data source sys error"
#define OB_DATA_SOURCE_TIMEOUT__USER_ERROR_MSG "Data source timeout"
#define OB_DATA_SOURCE_CONCURRENCY_FULL__USER_ERROR_MSG "Data source concurrency full"
#define OB_DATA_SOURCE_WRONG_URI_FORMAT__USER_ERROR_MSG "Data source wrong URI format"
#define OB_SSTABLE_VERSION_UNEQUAL__USER_ERROR_MSG "SSTable version not equal"
#define OB_UPS_RENEW_LEASE_NOT_ALLOWED__USER_ERROR_MSG "ups should not renew its lease"
#define OB_UPS_COUNT_OVER_LIMIT__USER_ERROR_MSG "ups count over limit"
#define OB_NO_UPS_MAJORITY__USER_ERROR_MSG "ups not form a majority"
#define OB_INDEX_COUNT_REACH_THE_LIMIT__USER_ERROR_MSG "created index tables count has reach the limit:128"
#define OB_TASK_EXPIRED__USER_ERROR_MSG "task expired"
#define OB_TABLEGROUP_NOT_EMPTY__USER_ERROR_MSG "tablegroup is not empty"
#define OB_INVALID_SERVER_STATUS__USER_ERROR_MSG "server status is not valid"
#define OB_WAIT_ELEC_LEADER_TIMEOUT__USER_ERROR_MSG "wait elect partition leader timeout"
#define OB_WAIT_ALL_RS_ONLINE_TIMEOUT__USER_ERROR_MSG "wait all rs online timeout"
#define OB_ALL_REPLICAS_ON_MERGE_ZONE__USER_ERROR_MSG "all replicas of partition group are on zones to merge"
#define OB_MACHINE_RESOURCE_NOT_ENOUGH__USER_ERROR_MSG "machine resource is not enough to hold a new unit"
#define OB_NOT_SERVER_CAN_HOLD_SOFTLY__USER_ERROR_MSG "not server can hole the unit and not over soft limit"
#define OB_RESOURCE_POOL_ALREADY_GRANTED__USER_ERROR_MSG "resource pool \'%s\' has already been granted to a tenant"
#define OB_SERVER_ALREADY_DELETED__USER_ERROR_MSG "server has already been deleted"
#define OB_SERVER_NOT_DELETING__USER_ERROR_MSG "server is not in deleting status"
#define OB_SERVER_NOT_IN_WHITE_LIST__USER_ERROR_MSG "server not in server white list"
#define OB_SERVER_ZONE_NOT_MATCH__USER_ERROR_MSG "server zone not match"
#define OB_OVER_ZONE_NUM_LIMIT__USER_ERROR_MSG "zone num has reach max zone num"
#define OB_ZONE_STATUS_NOT_MATCH__USER_ERROR_MSG "zone status not match"
#define OB_RESOURCE_UNIT_IS_REFERENCED__USER_ERROR_MSG "resource unit \'%s\' is referenced by some resource pool"
#define OB_DIFFERENT_PRIMARY_ZONE__USER_ERROR_MSG "table schema primary zone different with other table in sampe tablegroup"
#define OB_SERVER_NOT_ACTIVE__USER_ERROR_MSG "server is not active"
#define OB_RS_NOT_MASTER__USER_ERROR_MSG "The RootServer is not the master"
#define OB_CANDIDATE_LIST_ERROR__USER_ERROR_MSG "The candidate list is invalid"
#define OB_PARTITION_ZONE_DUPLICATED__USER_ERROR_MSG "The chosen partition servers belong to same zone."
#define OB_ZONE_DUPLICATED__USER_ERROR_MSG "Duplicated zone \'%s\' in zone list %s"
#define OB_NOT_ALL_ZONE_ACTIVE__USER_ERROR_MSG "Not all zone in zone list are active"
#define OB_PRIMARY_ZONE_NOT_IN_ZONE_LIST__USER_ERROR_MSG "primary zone \'%s\' not in zone list %s"
#define OB_REPLICA_NUM_NOT_MATCH__USER_ERROR_MSG "replica num not same with zone count"
#define OB_ZONE_LIST_POOL_LIST_NOT_MATCH__USER_ERROR_MSG "zone list %s not a subset of resource pool zone list %s"
#define OB_INVALID_TENANT_NAME__USER_ERROR_MSG "tenant name \'%s\' over max_tenant_name_length %ld"
#define OB_EMPTY_RESOURCE_POOL_LIST__USER_ERROR_MSG "resource pool list is empty"
#define OB_RESOURCE_UNIT_NOT_EXIST__USER_ERROR_MSG "resource unit \'%s\' not exist"
#define OB_RESOURCE_UNIT_EXIST__USER_ERROR_MSG "resource unit \'%s\' already exist"
#define OB_RESOURCE_POOL_NOT_EXIST__USER_ERROR_MSG "resource pool \'%s\' not exist"
#define OB_RESOURCE_POOL_EXIST__USER_ERROR_MSG "resource pool \'%s\' exist"
#define OB_WAIT_LEADER_SWITCH_TIMEOUT__USER_ERROR_MSG "wait leader switch timeout"
#define OB_LOCATION_NOT_EXIST__USER_ERROR_MSG "location not exist"
#define OB_LOCATION_LEADER_NOT_EXIST__USER_ERROR_MSG "location leader not exist"
#define OB_ZONE_NOT_ACTIVE__USER_ERROR_MSG "zone not active"
#define OB_UNIT_NUM_OVER_SERVER_COUNT__USER_ERROR_MSG "resource pool unit num is bigger than zone server count"
#define OB_POOL_SERVER_INTERSECT__USER_ERROR_MSG "resource pool list %s unit servers intersect"
#define OB_NOT_SINGLE_RESOURCE_POOL__USER_ERROR_MSG "create tenant only support single resource pool now, but pool list is %s"
#define OB_INVALID_RESOURCE_UNIT__USER_ERROR_MSG "invalid resource unit, %s\'s min value is %s"
#define OB_STOP_SERVER_IN_MULTIPLE_ZONES__USER_ERROR_MSG "Can not stop server in multiple zones, there are already servers stopped in zone:%s"
#define OB_SESSION_ENTRY_EXIST__USER_ERROR_MSG "Session already exist"
#define OB_GOT_SIGNAL_ABORTING__USER_ERROR_MSG "%s: Got signal %d. Aborting!"
#define OB_SERVER_NOT_ALIVE__USER_ERROR_MSG "server is not alive"
#define OB_GET_LOCATION_TIME_OUT__USER_ERROR_MSG "Timeout"
#define OB_UNIT_IS_MIGRATING__USER_ERROR_MSG "Unit is migrating, can not migrate again"
#define OB_CLUSTER_NO_MATCH__USER_ERROR_MSG "cluster name is not match to \'%s\'"
#define OB_CHECK_ZONE_MERGE_ORDER__USER_ERROR_MSG "Please check new zone in zone_merge_order. You can show parameters like 'zone_merge_order'"
#define OB_INVALID_MACRO_BLOCK_TYPE__USER_ERROR_MSG "the macro block type does not exist"
#define OB_CLUSTER_NOT_EXIST__USER_ERROR_MSG "cluster \'%s\' not exist"
#define OB_ERR_PARSER_INIT__USER_ERROR_MSG "Failed to init SQL parser"
#define OB_ERR_PARSE_SQL__USER_ERROR_MSG "%s near \'%.*s\' at line %d"
#define OB_ERR_RESOLVE_SQL__USER_ERROR_MSG "Resolve error"
#define OB_ERR_GEN_PLAN__USER_ERROR_MSG "Generate plan error"
#define OB_ERR_PARSER_SYNTAX__USER_ERROR_MSG "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use"
#define OB_ERR_COLUMN_SIZE__USER_ERROR_MSG "The used SELECT statements have a different number of columns"
#define OB_ERR_COLUMN_DUPLICATE__USER_ERROR_MSG "Duplicate column name '%.*s'"
#define OB_ERR_OPERATOR_UNKNOWN__USER_ERROR_MSG "Unknown operator"
#define OB_ERR_STAR_DUPLICATE__USER_ERROR_MSG "Duplicated star"
#define OB_ERR_ILLEGAL_ID__USER_ERROR_MSG "%s"
#define OB_ERR_ILLEGAL_VALUE__USER_ERROR_MSG "Illegal value"
#define OB_ERR_COLUMN_AMBIGUOUS__USER_ERROR_MSG "Ambiguous column"
#define OB_ERR_LOGICAL_PLAN_FAILD__USER_ERROR_MSG "Generate logical plan error"
#define OB_ERR_SCHEMA_UNSET__USER_ERROR_MSG "Schema not set"
#define OB_ERR_ILLEGAL_NAME__USER_ERROR_MSG "Illegal name"
#define OB_TABLE_NOT_EXIST__USER_ERROR_MSG "Table \'%s.%s\' doesn\'t exist"
#define OB_ERR_TABLE_EXIST__USER_ERROR_MSG "Table '%.*s' already exists"
#define OB_ERR_EXPR_UNKNOWN__USER_ERROR_MSG "Unknown expression"
#define OB_ERR_ILLEGAL_TYPE__USER_ERROR_MSG "unsupport MySQL type %d. Maybe you should use java.sql.Timestamp instead of java.util.Date."
#define OB_ERR_PRIMARY_KEY_DUPLICATE__USER_ERROR_MSG "Duplicate entry \'%s\' for key \'%.*s\'"
#define OB_ERR_KEY_NAME_DUPLICATE__USER_ERROR_MSG "Duplicate key name \'%.*s\'"
#define OB_ERR_CREATETIME_DUPLICATE__USER_ERROR_MSG "Duplicated createtime"
#define OB_ERR_MODIFYTIME_DUPLICATE__USER_ERROR_MSG "Duplicated modifytime"
#define OB_ERR_ILLEGAL_INDEX__USER_ERROR_MSG "Illegal index"
#define OB_ERR_INVALID_SCHEMA__USER_ERROR_MSG "Invalid schema"
#define OB_ERR_INSERT_NULL_ROWKEY__USER_ERROR_MSG "Insert null rowkey"
#define OB_ERR_COLUMN_NOT_FOUND__USER_ERROR_MSG "Column not found"
#define OB_ERR_DELETE_NULL_ROWKEY__USER_ERROR_MSG "Delete null rowkey"
#define OB_ERR_USER_EMPTY__USER_ERROR_MSG "No user"
#define OB_ERR_USER_NOT_EXIST__USER_ERROR_MSG "User not exist"
#define OB_ERR_NO_PRIVILEGE__USER_ERROR_MSG "Access denied; you need (at least one of) the %s privilege(s) for this operation"
#define OB_ERR_NO_AVAILABLE_PRIVILEGE_ENTRY__USER_ERROR_MSG "No privilege entry"
#define OB_ERR_WRONG_PASSWORD__USER_ERROR_MSG "Incorrect password"
#define OB_ERR_USER_IS_LOCKED__USER_ERROR_MSG "User locked"
#define OB_ERR_UPDATE_ROWKEY_COLUMN__USER_ERROR_MSG "Can not update rowkey column"
#define OB_ERR_UPDATE_JOIN_COLUMN__USER_ERROR_MSG "Can not update join column"
#define OB_ERR_INVALID_COLUMN_NUM__USER_ERROR_MSG "Operand should contain %d column(s)"
#define OB_ERR_PREPARE_STMT_NOT_FOUND__USER_ERROR_MSG "statement not prepared, stmt_id=%u"
#define OB_ERR_SYS_VARIABLE_UNKNOWN__USER_ERROR_MSG "Unknown system variable '%.*s'"
#define OB_ERR_OLDER_PRIVILEGE_VERSION__USER_ERROR_MSG "Older privilege version"
#define OB_ERR_LACK_OF_ROWKEY_COL__USER_ERROR_MSG "Primary key column(s) not specified in the WHERE clause"
#define OB_ERR_USER_EXIST__USER_ERROR_MSG "User exists"
#define OB_ERR_PASSWORD_EMPTY__USER_ERROR_MSG "Empty password"
#define OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE__USER_ERROR_MSG "Failed to grant privelege"
#define OB_ERR_WRONG_DYNAMIC_PARAM__USER_ERROR_MSG "Incorrect arguments number to EXECUTE, need %ld arguments but give %ld"
#define OB_ERR_PARAM_SIZE__USER_ERROR_MSG "Incorrect parameter count in the call to native function '%.*s'"
#define OB_ERR_FUNCTION_UNKNOWN__USER_ERROR_MSG "%s %s does not exist"
#define OB_ERR_CREAT_MODIFY_TIME_COLUMN__USER_ERROR_MSG "CreateTime or ModifyTime column cannot be modified"
#define OB_ERR_MODIFY_PRIMARY_KEY__USER_ERROR_MSG "Primary key cannot be modified"
#define OB_ERR_PARAM_DUPLICATE__USER_ERROR_MSG "Duplicated parameters"
#define OB_ERR_TOO_MANY_SESSIONS__USER_ERROR_MSG "Too many sessions"
#define OB_ERR_TOO_MANY_PS__USER_ERROR_MSG "Too many prepared statements"
#define OB_ERR_HINT_UNKNOWN__USER_ERROR_MSG "Unknown hint"
#define OB_ERR_WHEN_UNSATISFIED__USER_ERROR_MSG "When condition not satisfied"
#define OB_ERR_QUERY_INTERRUPTED__USER_ERROR_MSG "Query execution was interrupted"
#define OB_ERR_SESSION_INTERRUPTED__USER_ERROR_MSG "Session interrupted"
#define OB_ERR_UNKNOWN_SESSION_ID__USER_ERROR_MSG "Unknown session ID"
#define OB_ERR_PROTOCOL_NOT_RECOGNIZE__USER_ERROR_MSG "Incorrect protocol"
#define OB_ERR_WRITE_AUTH_ERROR__USER_ERROR_MSG "Write auth packet error"
#define OB_ERR_PARSE_JOIN_INFO__USER_ERROR_MSG "Wrong join info"
#define OB_ERR_ALTER_INDEX_COLUMN__USER_ERROR_MSG "Cannot alter index column"
#define OB_ERR_MODIFY_INDEX_TABLE__USER_ERROR_MSG "Cannot modify index table"
#define OB_ERR_INDEX_UNAVAILABLE__USER_ERROR_MSG "Index unavailable"
#define OB_ERR_NOP_VALUE__USER_ERROR_MSG "NOP cannot be used here"
#define OB_ERR_PS_TOO_MANY_PARAM__USER_ERROR_MSG "Prepared statement contains too many placeholders"
#define OB_ERR_READ_ONLY__USER_ERROR_MSG "The server is read only now"
#define OB_ERR_INVALID_TYPE_FOR_OP__USER_ERROR_MSG "invalid obj type for type promotion, left_type=%d right_type=%d"
#define OB_ERR_CAST_VARCHAR_TO_BOOL__USER_ERROR_MSG "Can not cast varchar value to bool type"
#define OB_ERR_CAST_VARCHAR_TO_NUMBER__USER_ERROR_MSG "Not a number Can not cast varchar value to number type"
#define OB_ERR_CAST_VARCHAR_TO_TIME__USER_ERROR_MSG "Not timestamp Can not cast varchar value to timestamp type"
#define OB_ERR_CAST_NUMBER_OVERFLOW__USER_ERROR_MSG "Result value was out of range when cast to number"
#define OB_INTEGER_PRECISION_OVERFLOW__USER_ERROR_MSG "value larger than specified precision(%ld,%ld) allowed for this column"
#define OB_DECIMAL_PRECISION_OVERFLOW__USER_ERROR_MSG "value(%s) larger than specified precision(%ld,%ld) allowed for this column"
#define OB_SCHEMA_NUMBER_PRECISION_OVERFLOW__USER_ERROR_MSG "Precision was out of range"
#define OB_SCHEMA_NUMBER_SCALE_OVERFLOW__USER_ERROR_MSG "Scale value was out of range"
#define OB_ERR_INDEX_UNKNOWN__USER_ERROR_MSG "Unknown index"
#define OB_NUMERIC_OVERFLOW__USER_ERROR_MSG "numeric overflow"
#define OB_ERR_TOO_MANY_JOIN_TABLES__USER_ERROR_MSG "too many joined tables"
#define OB_ERR_VARCHAR_TOO_LONG__USER_ERROR_MSG "Data too long(%d>%ld) for column '%s'"
#define OB_ERR_SYS_CONFIG_UNKNOWN__USER_ERROR_MSG "System config unknown"
#define OB_ERR_LOCAL_VARIABLE__USER_ERROR_MSG "Variable \'%.*s\' is a SESSION variable and can't be used with SET GLOBAL"
#define OB_ERR_GLOBAL_VARIABLE__USER_ERROR_MSG "Variable \'%.*s\' is a GLOBAL variable and should be set with SET GLOBAL"
#define OB_ERR_VARIABLE_IS_READONLY__USER_ERROR_MSG "%.*s variable '%.*s' is read-only. Use SET %.*s to assign the value"
#define OB_ERR_INCORRECT_GLOBAL_LOCAL_VAR__USER_ERROR_MSG "Variable '%.*s' is a %.*s variable"
#define OB_ERR_EXPIRE_INFO_TOO_LONG__USER_ERROR_MSG "length(%d) of expire_info is larger than the max allowed(%ld)"
#define OB_ERR_EXPIRE_COND_TOO_LONG__USER_ERROR_MSG "total length(%ld) of expire_info and its expression is larger than the max allowed(%ld)"
#define OB_INVALID_ARGUMENT_FOR_EXTRACT__USER_ERROR_MSG "EXTRACT() expected timestamp or a string as date argument"
#define OB_INVALID_ARGUMENT_FOR_IS__USER_ERROR_MSG "Invalid operand type for IS operator, lval=%s"
#define OB_INVALID_ARGUMENT_FOR_LENGTH__USER_ERROR_MSG "function LENGTH() expected a varchar argument"
#define OB_INVALID_ARGUMENT_FOR_SUBSTR__USER_ERROR_MSG "invalid input format. ret=%d text=%s start=%s length=%s"
#define OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC__USER_ERROR_MSG "TIME_TO_USEC() expected timestamp or a string as date argument"
#define OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME__USER_ERROR_MSG "USEC_TO_TIME expected a interger number as usec argument"
#define OB_ERR_USER_VARIABLE_UNKNOWN__USER_ERROR_MSG "Variable %.*s does not exists"
#define OB_ILLEGAL_USAGE_OF_MERGING_FROZEN_TIME__USER_ERROR_MSG "MERGING_FROZEN_TIME() system function only be used in daily merging."
#define OB_INVALID_NUMERIC__USER_ERROR_MSG "Invalid numeric char '%c'"
#define OB_ERR_REGEXP_ERROR__USER_ERROR_MSG "Got error 'empty (sub)expression' from regexp"
#define OB_SQL_LOG_OP_SETCHILD_OVERFLOW__USER_ERROR_MSG "Logical operator child index overflow"
#define OB_SQL_EXPLAIN_FAILED__USER_ERROR_MSG "fail to explain plan"
#define OB_SQL_OPT_COPY_OP_FAILED__USER_ERROR_MSG "fail to copy logical operator"
#define OB_SQL_OPT_GEN_PLAN_FALIED__USER_ERROR_MSG "fail to generate plan"
#define OB_SQL_OPT_CREATE_RAWEXPR_FAILED__USER_ERROR_MSG "fail to create raw expr"
#define OB_SQL_OPT_JOIN_ORDER_FAILED__USER_ERROR_MSG "fail to generate join order"
#define OB_SQL_OPT_ERROR__USER_ERROR_MSG "optimizer general error"
#define OB_SQL_RESOLVER_NO_MEMORY__USER_ERROR_MSG "sql resolver no memory"
#define OB_SQL_DML_ONLY__USER_ERROR_MSG "plan cache support dml only"
#define OB_ERR_NO_GRANT__USER_ERROR_MSG "No such grant defined"
#define OB_ERR_NO_DB_SELECTED__USER_ERROR_MSG "No database selected"
#define OB_SQL_PC_OVERFLOW__USER_ERROR_MSG "plan cache is overflow"
#define OB_SQL_PC_PLAN_DUPLICATE__USER_ERROR_MSG "plan exists in plan cache already"
#define OB_SQL_PC_PLAN_EXPIRE__USER_ERROR_MSG "plan is expired"
#define OB_SQL_PC_NOT_EXIST__USER_ERROR_MSG "no plan exist"
#define OB_SQL_PARAMS_LIMIT__USER_ERROR_MSG "too many params, plan cache not support"
#define OB_SQL_PC_PLAN_SIZE_LIMIT__USER_ERROR_MSG "plan is too big to add to plan cache"
#define OB_ERR_UNKNOWN_CHARSET__USER_ERROR_MSG "Unknown character set: '%.*s'"
#define OB_ERR_UNKNOWN_COLLATION__USER_ERROR_MSG "Unknown collation: '%.*s'"
#define OB_ERR_COLLATION_MISMATCH__USER_ERROR_MSG "COLLATION '%.*s' is not valid for CHARACTER SET '%.*s'"
#define OB_ERR_WRONG_VALUE_FOR_VAR__USER_ERROR_MSG "Variable \'%.*s\' can't be set to the value of \'%.*s\'"
#define OB_UNKNOWN_PARTITION__USER_ERROR_MSG "Unkown partition '%.*s' in table '%.*s'"
#define OB_PARTITION_NOT_MATCH__USER_ERROR_MSG "Found a row not matching the given partition set"
#define OB_ER_PASSWD_LENGTH__USER_ERROR_MSG " Password hash should be a 40-digit hexadecimal number"
#define OB_ERR_INSERT_INNER_JOIN_COLUMN__USER_ERROR_MSG "Insert inner join column error"
#define OB_TENANT_NOT_IN_SERVER__USER_ERROR_MSG "Tenant not in this server"
#define OB_TABLEGROUP_NOT_EXIST__USER_ERROR_MSG "tablegroup not exist"
#define OB_SUBQUERY_TOO_MANY_ROW__USER_ERROR_MSG "Subquery returns more than 1 row"
#define OB_ERR_BAD_DATABASE__USER_ERROR_MSG "Unknown database '%.*s'"
#define OB_CANNOT_USER__USER_ERROR_MSG "Operation %.*s failed for %.*s"
#define OB_TENANT_EXIST__USER_ERROR_MSG "tenant \'%s\' already exist"
#define OB_TENANT_NOT_EXIST__USER_ERROR_MSG "Unknown tenant '%.*s'"
#define OB_DATABASE_EXIST__USER_ERROR_MSG "Can't create database '%.*s'; database exists"
#define OB_TABLEGROUP_EXIST__USER_ERROR_MSG "tablegroup already exist"
#define OB_ERR_INVALID_TENANT_NAME__USER_ERROR_MSG "invalid tenant name specified in connection string"
#define OB_EMPTY_TENANT__USER_ERROR_MSG "tenant is empty"
#define OB_WRONG_DB_NAME__USER_ERROR_MSG "Incorrect database name '%.*s'"
#define OB_WRONG_TABLE_NAME__USER_ERROR_MSG "Incorrect table name '%.*s'"
#define OB_WRONG_COLUMN_NAME__USER_ERROR_MSG "Incorrect column name '%.*s'"
#define OB_ERR_COLUMN_SPEC__USER_ERROR_MSG "Incorrect column specifier for column '%.*s'"
#define OB_ERR_DB_DROP_EXISTS__USER_ERROR_MSG "Can't drop database '%.*s'; database doesn't exist"
#define OB_ERR_DATA_TOO_LONG__USER_ERROR_MSG "Data too long for column '%.*s' at row %lld"
#define OB_ERR_WRONG_VALUE_COUNT_ON_ROW__USER_ERROR_MSG "column count does not match value count at row '%d'"
#define OB_ERR_CREATE_USER_WITH_GRANT__USER_ERROR_MSG "You are not allowed to create a user with GRANT"
#define OB_ERR_NO_DB_PRIVILEGE__USER_ERROR_MSG "Access denied for user '%.*s'@'%.*s' to database '%.*s'"
#define OB_ERR_NO_TABLE_PRIVILEGE__USER_ERROR_MSG "%.*s command denied to user '%.*s'@'%.*s' for table '%.*s'"
#define OB_INVALID_ON_UPDATE__USER_ERROR_MSG "Invalid ON UPDATE clause for \'%s\' column"
#define OB_INVALID_DEFAULT__USER_ERROR_MSG "Invalid default value for \'%.*s\'"
#define OB_ERR_UPDATE_TABLE_USED__USER_ERROR_MSG "You can\'t specify target table \'%s\' for update in FROM clause"
#define OB_ERR_COULUMN_VALUE_NOT_MATCH__USER_ERROR_MSG "Column count doesn\'t match value count at row %ld"
#define OB_ERR_INVALID_GROUP_FUNC_USE__USER_ERROR_MSG "Invalid use of group function"
#define OB_CANT_AGGREGATE_2COLLATIONS__USER_ERROR_MSG "Illegal mix of collations"
#define OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD__USER_ERROR_MSG "Field \'%.*s\' is of a not allowed type for this type of partitioning"
#define OB_ERR_TOO_LONG_IDENT__USER_ERROR_MSG "Identifier name \'%.*s\' is too long"
#define OB_ERR_WRONG_TYPE_FOR_VAR__USER_ERROR_MSG "Incorrect argument type to variable '%.*s'"
#define OB_WRONG_USER_NAME_LENGTH__USER_ERROR_MSG "String '%.*s' is too long for user name (should be no longer than 16)"
#define OB_ERR_PRIV_USAGE__USER_ERROR_MSG "Incorrect usage of DB GRANT and GLOBAL PRIVILEGES"
#define OB_ILLEGAL_GRANT_FOR_TABLE__USER_ERROR_MSG "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used"
#define OB_ERR_REACH_AUTOINC_MAX__USER_ERROR_MSG "Failed to read auto-increment value from storage engine"
#define OB_ERR_NO_TABLES_USED__USER_ERROR_MSG "No tables used"
#define OB_CANT_REMOVE_ALL_FIELDS__USER_ERROR_MSG "You can't delete all columns with ALTER TABLE; use DROP TABLE instead"
#define OB_TOO_MANY_PARTITIONS_ERROR__USER_ERROR_MSG "Too many partitions (including subpartitions) were defined"
#define OB_NO_PARTS_ERROR__USER_ERROR_MSG "Number of partitions = 0 is not an allowed value"
#define OB_WRONG_SUB_KEY__USER_ERROR_MSG "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys"
#define OB_KEY_PART_0__USER_ERROR_MSG "Key part \'%.*s\' length cannot be 0"
#define OB_ERR_UNKNOWN_TIME_ZONE__USER_ERROR_MSG "Unknown or incorrect time zone: \'%.*s\'"
#define OB_ERR_WRONG_AUTO_KEY__USER_ERROR_MSG "Incorrect table definition; there can be only one auto column"
#define OB_ERR_TOO_MANY_KEYS__USER_ERROR_MSG "Too many keys specified; max %d keys allowed"
#define OB_ERR_TOO_MANY_ROWKEY_COLUMNS__USER_ERROR_MSG "Too many key parts specified; max %d parts allowed"
#define OB_ERR_TOO_LONG_KEY_LENGTH__USER_ERROR_MSG "Specified key was too long; max key length is %d bytes"
#define OB_ERR_TOO_MANY_COLUMNS__USER_ERROR_MSG "Too many columns"
#define OB_ERR_TOO_LONG_COLUMN_LENGTH__USER_ERROR_MSG "Column length too big for column '%s' (max = %d)"
#define OB_ERR_TOO_BIG_ROWSIZE__USER_ERROR_MSG "Row size too large"
#define OB_ERR_UNKNOWN_TABLE__USER_ERROR_MSG "Unknown table '%.*s' in %.*s"
#define OB_ERR_BAD_TABLE__USER_ERROR_MSG "Unknown table '%.*s'"
#define OB_ERR_TOO_BIG_SCALE__USER_ERROR_MSG "Too big scale %d specified for column '%s'. Maximum is %ld."
#define OB_ERR_TOO_BIG_PRECISION__USER_ERROR_MSG "Too big precision %d specified for column '%s'. Maximum is %ld."
#define OB_ERR_M_BIGGER_THAN_D__USER_ERROR_MSG "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s')."
#define OB_ERR_TOO_BIG_DISPLAYWIDTH__USER_ERROR_MSG "Display width out of range for column '%s' (max = %ld)"
#define OB_WRONG_GROUP_FIELD__USER_ERROR_MSG "Can't group on '%.*s'"
#define OB_NON_UNIQ_ERROR__USER_ERROR_MSG "Column '%.*s' in %.*s is ambiguous"
#define OB_ERR_NONUNIQ_TABLE__USER_ERROR_MSG "Not unique table/alias: \'%.*s\'"
#define OB_ERR_CANT_DROP_FIELD_OR_KEY__USER_ERROR_MSG "Can't DROP '%.*s'; check that column/key exists"
#define OB_ERR_MULTIPLE_PRI_KEY__USER_ERROR_MSG "Multiple primary key defined"
#define OB_ERR_KEY_COLUMN_DOES_NOT_EXITS__USER_ERROR_MSG "Key column '%.*s' doesn't exist in table"
#define OB_ERR_AUTO_PARTITION_KEY__USER_ERROR_MSG "auto-increment column '%.*s' should not be part of partition key"
#define OB_ERR_CANT_USE_OPTION_HERE__USER_ERROR_MSG "Incorrect usage/placement of '%s'"
#define OB_ERR_WRONG_OBJECT__USER_ERROR_MSG "\'%s.%s\' is not %s"
#define OB_ERR_ON_RENAME__USER_ERROR_MSG "Error on rename of \'%s.%s\' to \'%s.%s\'"
#define OB_ERR_WRONG_KEY_COLUMN__USER_ERROR_MSG "The used storage engine can't index column '%.*s'"
#define OB_ERR_BAD_FIELD_ERROR__USER_ERROR_MSG "Unknown column '%.*s' in '%.*s'"
#define OB_ERR_WRONG_FIELD_WITH_GROUP__USER_ERROR_MSG "\'%.*s\' is not in GROUP BY"
#define OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS__USER_ERROR_MSG "Transaction characteristics can't be changed while a transaction is in progress"
#define OB_ERR_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION__USER_ERROR_MSG "Cannot execute statement in a READ ONLY transaction."
#define OB_ERR_MIX_OF_GROUP_FUNC_AND_FIELDS__USER_ERROR_MSG "Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP columns is illegal if there is no GROUP BY clause"
#define OB_ERR_TRUNCATED_WRONG_VALUE__USER_ERROR_MSG "Truncated incorrect %.*s value: '%.*s'"
#define OB_ERR_WRONG_IDENT_NAME__USER_ERROR_MSG "wrong ident name"
#define OB_WRONG_NAME_FOR_INDEX__USER_ERROR_MSG "Incorrect index name '%.*s'"
#define OB_ILLEGAL_REFERENCE__USER_ERROR_MSG "Reference '%.*s' not supported (reference to group function)"
#define OB_REACH_MEMORY_LIMIT__USER_ERROR_MSG "plan cache memory used reach the high water mark."
#define OB_ERR_PASSWORD_FORMAT__USER_ERROR_MSG "The password hash doesn't have the expected format. Check if the correct password algorithm is being used with the PASSWORD() function."
#define OB_ERR_NON_UPDATABLE_TABLE__USER_ERROR_MSG "The target table tt of the UPDATE is not updatable"
#define OB_ERR_WARN_DATA_OUT_OF_RANGE__USER_ERROR_MSG "Out of range value for column '%.*s' at row %ld"
#define OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR__USER_ERROR_MSG "Constant or random or timezone-dependent expressions in (sub)partitioning function are not allowed"
#define OB_ERR_VIEW_INVALID__USER_ERROR_MSG "View \'%.*s.%.*s\' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"
#define OB_ERR_OPTION_PREVENTS_STATEMENT__USER_ERROR_MSG "The MySQL server is running with the --read-only option so it cannot execute this statement"
#define OB_ERR_DB_READ_ONLY__USER_ERROR_MSG "The database is read only so it cannot execute this statement"
#define OB_ERR_TABLE_READ_ONLY__USER_ERROR_MSG "The table is read only so it cannot execute this statement"
#define OB_ERR_LOCK_OR_ACTIVE_TRANSACTION__USER_ERROR_MSG "Can't execute the given command because you have active locked tables or an active transaction"
#define OB_ERR_SAME_NAME_PARTITION_FIELD__USER_ERROR_MSG "Duplicate partition field name '%.*s'"
#define OB_ERR_TABLENAME_NOT_ALLOWED_HERE__USER_ERROR_MSG "Table \'%.*s\' from one of the SELECTs cannot be used in global ORDER clause"
#define OB_ERR_VIEW_RECURSIVE__USER_ERROR_MSG "\'%.*s'.'%.*s\' contains view recursion"
#define OB_ERR_QUALIFIER__USER_ERROR_MSG "Column part of USING clause cannot have qualifier"
#define OB_ERR_WRONG_VALUE__USER_ERROR_MSG "Incorrect %s value: '%s'"
#define OB_ERR_VIEW_WRONG_LIST__USER_ERROR_MSG "View's SELECT and view's field list have different column counts"
#define OB_SYS_VARS_MAYBE_DIFF_VERSION__USER_ERROR_MSG "system variables' version maybe different"
#define OB_ERR_AUTO_INCREMENT_CONFLICT__USER_ERROR_MSG "Auto-increment value in UPDATE conflicts with internally generated values"
#define OB_ERR_TASK_SKIPPED__USER_ERROR_MSG "some tasks are skipped, skipped server addr is '%s', the orginal error code is %d"
#define OB_ERR_NAME_BECOMES_EMPTY__USER_ERROR_MSG "Name \'%.*s\' has become ''"
#define OB_ERR_REMOVED_SPACES__USER_ERROR_MSG "Leading spaces are removed from name \'%.*s\'"
#define OB_WARN_ADD_AUTOINCREMENT_COLUMN__USER_ERROR_MSG "Alter table add auto_increment column is dangerous, table_name=\'%.*s\', column_name=\'%s\'"
#define OB_WARN_CHAMGE_NULL_ATTRIBUTE__USER_ERROR_MSG "Alter table change nullable column to not nullable is dangerous, table_name=\'%.*s\', column_name=\'%.*s\'"
#define OB_ERR_INVALID_CHARACTER_STRING__USER_ERROR_MSG "Invalid %.*s character string: \'%.*s\'"
#define OB_ERR_KILL_DENIED__USER_ERROR_MSG "You are not owner of thread %lu"
#define OB_ERR_COLUMN_DEFINITION_AMBIGUOUS__USER_ERROR_MSG "Column definition is ambiguous. Column has both NULL and NOT NULL attributes"
#define OB_ERR_EMPTY_QUERY__USER_ERROR_MSG "Query was empty"
#define OB_ERR_CUT_VALUE_GROUP_CONCAT__USER_ERROR_MSG "Row %ld was cut by GROUP_CONCAT()"
#define OB_ERR_FILED_NOT_FOUND_PART__USER_ERROR_MSG "Field in list of fields for partition function not found in table"
#define OB_ERR_PRIMARY_CANT_HAVE_NULL__USER_ERROR_MSG "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead"
#define OB_ERR_PARTITION_FUNC_NOT_ALLOWED_ERROR__USER_ERROR_MSG "The PARTITION function returns the wrong type"
#define OB_ERR_INVALID_BLOCK_SIZE__USER_ERROR_MSG "Invalid block size, block size should between 16384 and 1048576"
#define OB_ERR_UNKNOWN_STORAGE_ENGINE__USER_ERROR_MSG "Unknown storage engine \'%.*s\'"
#define OB_ERR_TENANT_IS_LOCKED__USER_ERROR_MSG "Tenant \'%.*s\' is locked"
#define OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF__USER_ERROR_MSG "A %s must include all columns in the table's partitioning function"
#define OB_ERR_AGGREGATE_ORDER_FOR_UNION__USER_ERROR_MSG "Expression #%d of ORDER BY contains aggregate function and applies to a UNION"
#define OB_ERR_OUTLINE_EXIST__USER_ERROR_MSG "Outline '%.*s' already exists"
#define OB_OUTLINE_NOT_EXIST__USER_ERROR_MSG "Outline \'%.*s.%.*s\' doesn\'t exist"
#define OB_WARN_OPTION_BELOW_LIMIT__USER_ERROR_MSG "The value of \'%s\' should be no less than the value of \'%s\'"
#define OB_INVALID_OUTLINE__USER_ERROR_MSG "invalid outline ,error info:%s"
#define OB_REACH_MAX_CONCURRENT_NUM__USER_ERROR_MSG "SQL reach max concurrent num %ld"
#define OB_ERR_OPERATION_ON_RECYCLE_OBJECT__USER_ERROR_MSG "can not perform DDL/DML over objects in Recycle Bin"
#define OB_ERR_OBJECT_NOT_IN_RECYCLEBIN__USER_ERROR_MSG "object not in RECYCLE BIN"
#define OB_ERR_CON_COUNT_ERROR__USER_ERROR_MSG "Too many connections"
#define OB_ERR_OUTLINE_CONTENT_EXIST__USER_ERROR_MSG "Outline content '%.*s' of outline '%.*s' already exists when added"
#define OB_ERR_OUTLINE_MAX_CONCURRENT_EXIST__USER_ERROR_MSG "Max concurrent in outline '%.*s' already exists when added"
#define OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR__USER_ERROR_MSG "VALUES value for partition \'%.*s\' must have type INT"
#define OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR__USER_ERROR_MSG "Partition column values of incorrect type"
#define OB_ERR_PARTITION_COLUMN_LIST_ERROR__USER_ERROR_MSG "Inconsistency in usage of column lists for partitioning"
#define OB_ERR_TOO_MANY_VALUES_ERROR__USER_ERROR_MSG "Cannot have more than one value for this type of RANGE partitioning"
#define OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED__USER_ERROR_MSG "This partition function is not allowed"
#define OB_ERR_PARTITION_INTERVAL_ERROR__USER_ERROR_MSG "Partition interval must have type INT"
#define OB_ERR_SAME_NAME_PARTITION__USER_ERROR_MSG "Duplicate partition name \'%.*s\'"
#define OB_ERR_RANGE_NOT_INCREASING_ERROR__USER_ERROR_MSG "VALUES LESS THAN value must be strictly increasing for each partition"
#define OB_ERR_PARSE_PARTITION_RANGE__USER_ERROR_MSG "Wrong number of partitions defined, mismatch with previous setting"
#define OB_ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF__USER_ERROR_MSG "A PRIMARY KEY must include all columns in the table\'s partitioning function"
#define OB_NO_PARTITION_FOR_GIVEN_VALUE__USER_ERROR_MSG "Table has no partition for value"
#define OB_EER_NULL_IN_VALUES_LESS_THAN__USER_ERROR_MSG "Not allowed to use NULL value in VALUES LESS THAN"
#define OB_ERR_PARTITION_CONST_DOMAIN_ERROR__USER_ERROR_MSG "Partition constant is out of partition function domain"
#define OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS__USER_ERROR_MSG "Too many fields in \'list of partition fields\'"
#define OB_ERR_BAD_FT_COLUMN__USER_ERROR_MSG "Column '%.*s' cannot be part of FULLTEXT index"
#define OB_ERR_KEY_DOES_NOT_EXISTS__USER_ERROR_MSG "Key '%.*s' doesn't exist in table '%.*s'"
#define OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN__USER_ERROR_MSG "The value specified for generated column '%.*s' in table '%.*s' is not allowed"
#define OB_ERR_BAD_CTXCAT_COLUMN__USER_ERROR_MSG "The CTXCAT column must be contiguous in the index column list"
#define OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN__USER_ERROR_MSG "'%s' is not supported for generated columns."
#define OB_ERR_DEPENDENT_BY_GENERATED_COLUMN__USER_ERROR_MSG "Column '%.*s' has a generated column dependency"
#define OB_ERR_TOO_MANY_ROWS__USER_ERROR_MSG "Result consisted of more than one row"
#define OB_WRONG_FIELD_TERMINATORS__USER_ERROR_MSG "Field separator argument is not what is expected; check the manual"
#define OB_ERR_UNEXPECTED_TZ_TRANSITION__USER_ERROR_MSG "unexpected time zone info transition"
#define OB_ERR_INVALID_TIMEZONE_REGION_ID__USER_ERROR_MSG "timezone region ID is invalid"
#define OB_ERR_INVALID_HEX_NUMBER__USER_ERROR_MSG "invalid hex number"
#define OB_ERR_INVALID_NUMBER_FORMAT_MODEL__USER_ERROR_MSG "invalid number format model"
#define OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE__USER_ERROR_MSG "year conflicts with Julian date"
#define OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE__USER_ERROR_MSG "day of year conflicts with Julian date"
#define OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE__USER_ERROR_MSG "month conflicts with Julian date"
#define OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE__USER_ERROR_MSG "day of month conflicts with Julian date"
#define OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE__USER_ERROR_MSG "day of week conflicts with Julian date"
#define OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY__USER_ERROR_MSG "hour conflicts with seconds in day"
#define OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY__USER_ERROR_MSG "minutes of hour conflicts with seconds in day"
#define OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY__USER_ERROR_MSG "seconds of minute conflicts with seconds in day"
#define OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED__USER_ERROR_MSG "date not valid for month specified"
#define OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH__USER_ERROR_MSG "input value not long enough for date format"
#define OB_ERR_INVALID_YEAR_VALUE__USER_ERROR_MSG "(full) year must be between -4713 and +9999, and not be 0"
#define OB_ERR_INVALID_MONTH__USER_ERROR_MSG "not a valid month"
#define OB_ERR_INVALID_DAY_OF_THE_WEEK__USER_ERROR_MSG "not a valid day of the week"
#define OB_ERR_INVALID_DAY_OF_YEAR_VALUE__USER_ERROR_MSG "day of year must be between 1 and 365 (366 for leap year)"
#define OB_ERR_INVALID_HOUR12_VALUE__USER_ERROR_MSG "hour must be between 1 and 12"
#define OB_ERR_INVALID_HOUR24_VALUE__USER_ERROR_MSG "hour must be between 0 and 23"
#define OB_ERR_INVALID_MINUTES_VALUE__USER_ERROR_MSG "minutes must be between 0 and 59"
#define OB_ERR_INVALID_SECONDS_VALUE__USER_ERROR_MSG "seconds must be between 0 and 59"
#define OB_ERR_INVALID_SECONDS_IN_DAY_VALUE__USER_ERROR_MSG "seconds in day must be between 0 and 86399"
#define OB_ERR_INVALID_JULIAN_DATE_VALUE__USER_ERROR_MSG "julian date must be between 1 and 5373484"
#define OB_ERR_AM_OR_PM_REQUIRED__USER_ERROR_MSG "AM/A.M. or PM/P.M. required"
#define OB_ERR_BC_OR_AD_REQUIRED__USER_ERROR_MSG "BC/B.C. or AD/A.D. required"
#define OB_ERR_FORMAT_CODE_APPEARS_TWICE__USER_ERROR_MSG "format code appears twice"
#define OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE__USER_ERROR_MSG "day of week may only be specified once"
#define OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD__USER_ERROR_MSG "signed year precludes use of BC/AD"
#define OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR__USER_ERROR_MSG "Julian date precludes use of day of year"
#define OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE__USER_ERROR_MSG "year may only be specified once"
#define OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE__USER_ERROR_MSG "hour may only be specified once"
#define OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT__USER_ERROR_MSG "AM/PM conflicts with use of A.M./P.M."
#define OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT__USER_ERROR_MSG "BC/AD conflicts with use of B.C./A.D."
#define OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE__USER_ERROR_MSG "month may only be specified once"
#define OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE__USER_ERROR_MSG "day of week may only be specified once"
#define OB_ERR_FORMAT_CODE_CANNOT_APPEAR__USER_ERROR_MSG "format code cannot appear in date input format"
#define OB_ERR_NON_NUMERIC_CHARACTER_VALUE__USER_ERROR_MSG "a non-numeric character was found where a numeric was expected"
#define OB_INVALID_MERIDIAN_INDICATOR_USE__USER_ERROR_MSG "'HH24' precludes use of meridian indicator"
#define OB_ERR_DAY_OF_MONTH_RANGE__ORA_USER_ERROR_MSG "ORA-01847: day of month must be between 1 and last day of month"
#define OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL__USER_ERROR_MSG "the leading precision of the interval is too small"
#define OB_ERR_INVALID_TIME_ZONE_HOUR__USER_ERROR_MSG "time zone hour must be between -12 and 14"
#define OB_ERR_INVALID_TIME_ZONE_MINUTE__USER_ERROR_MSG "time zone minute must be between -59 and 59"
#define OB_ERR_NOT_A_VALID_TIME_ZONE__USER_ERROR_MSG "not a valid time zone"
#define OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER__USER_ERROR_MSG "date format is too long for internal buffer"
#define OB_INVALID_ROWID__USER_ERROR_MSG "invalid ROWID"
#define OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR__USER_ERROR_MSG "Datetime/Interval internal error"
#define OB_ERR_FETCH_OUT_SEQUENCE__USER_ERROR_MSG "ORA-01002: fetch out of sequence"
#define OB_TRANSACTION_SET_VIOLATION__USER_ERROR_MSG "Transaction set changed during the execution"
#define OB_TRANS_ROLLBACKED__USER_ERROR_MSG "transaction is rolled back"
#define OB_ERR_EXCLUSIVE_LOCK_CONFLICT__USER_ERROR_MSG "Lock wait timeout exceeded; try restarting transaction"
#define OB_ERR_SHARED_LOCK_CONFLICT__USER_ERROR_MSG "Shared lock conflict"
#define OB_TRY_LOCK_ROW_CONFLICT__USER_ERROR_MSG "Try lock row conflict"
#define OB_CLOCK_OUT_OF_ORDER__USER_ERROR_MSG "Clock out of order"
#define OB_MASK_SET_NO_NODE__USER_ERROR_MSG "Mask set has no node"
#define OB_TRANS_HAS_DECIDED__USER_ERROR_MSG "Transaction has been decided"
#define OB_TRANS_INVALID_STATE__USER_ERROR_MSG "Transaction state invalid"
#define OB_TRANS_STATE_NOT_CHANGE__USER_ERROR_MSG "Transaction state not changed"
#define OB_TRANS_PROTOCOL_ERROR__USER_ERROR_MSG "Transaction protocol error"
#define OB_TRANS_INVALID_MESSAGE__USER_ERROR_MSG "Transaction message invalid"
#define OB_TRANS_INVALID_MESSAGE_TYPE__USER_ERROR_MSG "Transaction message type invalid"
#define OB_TRANS_TIMEOUT__USER_ERROR_MSG "Transaction is timeout"
#define OB_TRANS_KILLED__USER_ERROR_MSG "Transaction is killed"
#define OB_TRANS_STMT_TIMEOUT__USER_ERROR_MSG "Statement is timeout"
#define OB_TRANS_CTX_NOT_EXIST__USER_ERROR_MSG "Transaction context does not exist"
#define OB_PARTITION_IS_FROZEN__USER_ERROR_MSG "Partition is frozen"
#define OB_PARTITION_IS_NOT_FROZEN__USER_ERROR_MSG "Partition is not frozen"
#define OB_TRANS_INVALID_LOG_TYPE__USER_ERROR_MSG "Transaction invalid log type"
#define OB_TRANS_SQL_SEQUENCE_ILLEGAL__USER_ERROR_MSG "SQL sequence illegal"
#define OB_TRANS_CANNOT_BE_KILLED__USER_ERROR_MSG "Transaction context cannot be killed"
#define OB_TRANS_STATE_UNKNOWN__USER_ERROR_MSG "Transaction state unknown"
#define OB_TRANS_IS_EXITING__USER_ERROR_MSG "Transaction exiting"
#define OB_TRANS_NEED_ROLLBACK__USER_ERROR_MSG "transaction need rollback"
#define OB_TRANS_UNKNOWN__USER_ERROR_MSG "Transaction result is unknown"
#define OB_ERR_READ_ONLY_TRANSACTION__USER_ERROR_MSG "Cannot execute statement in a READ ONLY transaction"
#define OB_PARTITION_IS_NOT_STOPPED__USER_ERROR_MSG "Partition is not stopped"
#define OB_PARTITION_IS_STOPPED__USER_ERROR_MSG "Partition has been stopped"
#define OB_PARTITION_IS_BLOCKED__USER_ERROR_MSG "Partition has been blocked"
#define OB_TRANS_RPC_TIMEOUT__USER_ERROR_MSG "transaction rpc timeout"
#define OB_TRANS_FREE_ROUTE_NOT_SUPPORTED__USER_ERROR_MSG "Query is not supported to be executed on txn participant node"
#define OB_LOG_ID_NOT_FOUND__USER_ERROR_MSG "log id not found"
#define OB_LSR_THREAD_STOPPED__USER_ERROR_MSG "log scan runnable thread stop"
#define OB_NO_LOG__USER_ERROR_MSG "no log ever scanned"
#define OB_LOG_ID_RANGE_ERROR__USER_ERROR_MSG "log id range error"
#define OB_LOG_ITER_ENOUGH__USER_ERROR_MSG "iter scans enough files"
#define OB_CLOG_INVALID_ACK__USER_ERROR_MSG "invalid ack msg"
#define OB_CLOG_CACHE_INVALID__USER_ERROR_MSG "clog cache invalid"
#define OB_EXT_HANDLE_UNFINISH__USER_ERROR_MSG "external executor handle do not finish"
#define OB_CURSOR_NOT_EXIST__USER_ERROR_MSG "cursor not exist"
#define OB_STREAM_NOT_EXIST__USER_ERROR_MSG "stream not exist"
#define OB_STREAM_BUSY__USER_ERROR_MSG "stream busy"
#define OB_FILE_RECYCLED__USER_ERROR_MSG "file recycled"
#define OB_REPLAY_EAGAIN_TOO_MUCH_TIME__USER_ERROR_MSG "replay eagain cost too much time"
#define OB_MEMBER_CHANGE_FAILED__USER_ERROR_MSG "member change log sync failed"
#define OB_ELECTION_WARN_LOGBUF_FULL__USER_ERROR_MSG "The log buffer is full"
#define OB_ELECTION_WARN_LOGBUF_EMPTY__USER_ERROR_MSG "The log buffer is empty"
#define OB_ELECTION_WARN_NOT_RUNNING__USER_ERROR_MSG "The object is not running"
#define OB_ELECTION_WARN_IS_RUNNING__USER_ERROR_MSG "The object is running"
#define OB_ELECTION_WARN_NOT_REACH_MAJORITY__USER_ERROR_MSG "Election does not reach majority"
#define OB_ELECTION_WARN_INVALID_SERVER__USER_ERROR_MSG "The server is not valid"
#define OB_ELECTION_WARN_INVALID_LEADER__USER_ERROR_MSG "The leader is not valid"
#define OB_ELECTION_WARN_LEADER_LEASE_EXPIRED__USER_ERROR_MSG "The leader lease is expired"
#define OB_ELECTION_WARN_INVALID_MESSAGE__USER_ERROR_MSG "The message is not valid"
#define OB_ELECTION_WARN_MESSAGE_NOT_INTIME__USER_ERROR_MSG "The message is not intime"
#define OB_ELECTION_WARN_NOT_CANDIDATE__USER_ERROR_MSG "The server is not candidate"
#define OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER__USER_ERROR_MSG "The server is not candidate or voter"
#define OB_ELECTION_WARN_PROTOCOL_ERROR__USER_ERROR_MSG "Election protocol error"
#define OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE__USER_ERROR_MSG "The task run time out of range"
#define OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE__USER_ERROR_MSG "Last operation has not done"
#define OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER__USER_ERROR_MSG "Current server is not leader"
#define OB_ELECTION_WARN_NO_PREPARE_MESSAGE__USER_ERROR_MSG "There is not prepare message"
#define OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE__USER_ERROR_MSG "There is more than one prepare message"
#define OB_ELECTION_NOT_EXIST__USER_ERROR_MSG "Election does not exist"
#define OB_ELECTION_MGR_IS_RUNNING__USER_ERROR_MSG "Election manager is running"
#define OB_ELECTION_WARN_NO_MAJORITY_PREPARE_MESSAGE__USER_ERROR_MSG "Election msg pool not have majority prepare message"
#define OB_ELECTION_ASYNC_LOG_WARN_INIT__USER_ERROR_MSG "Election async log init error"
#define OB_ELECTION_WAIT_LEADER_MESSAGE__USER_ERROR_MSG "Election waiting leader message"
#define OB_SERVER_IS_INIT__USER_ERROR_MSG "Server is initializing"
#define OB_SERVER_IS_STOPPING__USER_ERROR_MSG "Server is stopping"
#define OB_PACKET_CHECKSUM_ERROR__USER_ERROR_MSG "Packet checksum error"

// obproxy common error code
#define OB_PROXY_INTERNAL_ERROR__USER_ERROR_MSG "An internal error occured in obproxy"
#define OB_CLIENT_RECEIVING_PACKET_CONNECTION_ERROR__USER_ERROR_MSG "An unexpected connection event received from client while obproxy reading request"
#define OB_CLIENT_HANDLING_REQUEST_CONNECITON_ERROR__USER_ERROR_MSG "An unexpected connection event received from client while obproxy handling response"
#define OB_CLIENT_TRANSFERRING_PACKET_CONNECTION_ERROR__USER_ERROR_MSG "An unexpected connection event received from client while obproxy transferring response"
#define OB_SERVER_BUILD_CONNECTION_ERROR__USER_ERROR_MSG "OBProxy fail to build connection to observer"
#define OB_SERVER_RECEIVING_PACKET_CONNECTION_ERROR__USER_ERROR_MSG "An unexpected connection event received while obproxy reading response"
#define OB_SERVER_CONNECT_TIMEOUT__USER_ERROR_MSG "An timeout event received while obproxy reading response"
#define OB_SERVER_TRANSFERRING_PACKET_CONNECTION_ERROR__USER_ERROR_MSG "An unexpected connection event received while obproxy transferring request"
#define OB_PLUGIN_TRANSFERRING_ERROR__USER_ERROR_MSG "OBProxy plugin transferring failed"
#define OB_PROXY_INTERNAL_REQUEST_FAIL__USER_ERROR_MSG "OBProxy internal request execution failed"
#define OB_PROXY_NO_NEED_RETRY__USER_ERROR_MSG  "OBProxy cannot retry this request"
#define OB_PROXY_HOLD_CONNECTION__USER_ERROR_MSG "OBProxy internal error occured"
#define OB_PROXY_INVALID_USER__USER_ERROR_MSG "Invalid obproxy user"
#define OB_PROXY_INACTIVITY_TIMEOUT__USER_ERROR_MSG "OBProxy inactivity timeout"
#define OB_PROXY_RECONNECT_COORDINATOR__USER_ERROR_MSG "OBProxy try to reconnect coordinator in transaction"
#define OB_PROXY_INVALID_COORDINATOR__USER_ERROR_MSG "OBProxy transaction coordinator is invalid"
#define OB_PROXY_PROXY_ID_OVER_LIMIT__USER_ERROR_MSG "When changing proxy_id or client_session_id_version and client_session_id_version is version 1, proxy_id must be set to less than 255"
// obproxy related error code
#define OB_PROXY_FETCH_RSLIST_FAIL__USER_ERROR_MSG "ObProxy fail to fetch rootserver list"
#define OB_PROXY_CLUSTER_RESOURCE_EXPIRED__USER_ERROR_MSG "ObProxy cluster resource expired"

// sharding related error code
#define OB_PROXY_SHARD_HINT_NOT_SUPPORTED__USER_ERROR_MSG "ObProxy shard hint usage not supported"
#define OB_PROXY_SHARD_INVALID_CONFIG__USER_ERROR_MSG "ObProxy shard config is invalid"

// kv related error code


// proxy client related error code


  const char* ob_strerror(int oberr);
  const char* ob_sqlstate(int oberr);
  const char* ob_str_user_error(int oberr);
  int ob_mysql_errno(int oberr);
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_LIB_OB_ERRNO_H_
