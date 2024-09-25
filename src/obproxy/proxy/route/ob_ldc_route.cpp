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

#define USING_LOG_PREFIX PROXY

#include "ob_ldc_route.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//ANP, BNP, AMP, BMP;
//ANT, BNT, AMT, BMT;
//CNP, CMP;
//CNT, CMT
static ObRouteType route_order_cursor_of_merge_idc_order[] = {
    ROUTE_TYPE_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANRP, BNRP, AMRP, BMRP, ANWP, BNWP, AMWP, BMWP;
//ANRT, BNRT, AMRT, BMRT, ANWT, BNWT, AMWT, BMWT;
//CNRP, CMRP, CNWP, CMWP
//CNRT, CMRT, CNWT, CMWT
static ObRouteType route_order_cursor_of_readonly_zone_first[] = {
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE,
    ROUTE_TYPE_MAX
};

//ANRP, BNRP, AMRP, BMRP;
//ANRT, BNRT, AMRT, BMRT;
//CNRP, CMRP
//CNRT, CMRT
static ObRouteType route_order_cursor_of_only_readonly_zone[] = {
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANRP, BNRP, ANWP, BNWP, AMRP, BMRP, AMWP, BMWP;
//ANRT, BNRT, ANWT, BNWT, AMRT, BMRT, AMWT, BMWT;
//CNRP, CNWP, CMRP, CMWP
//CNRT, CNWT, CMRT, CMWT
static ObRouteType route_order_cursor_of_unmerge_zone_first[] = {
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};


//ANWP, BNWP, AMWP, BMWP;
//ANWT, BNWT, AMWT, BMWT;
//CNWP, CMWP
//CNWT, CMWT
static ObRouteType route_order_cursor_of_only_readwrite_zone[] = {
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANP, BNP, AMP, BMP;
//CNP, CMP;
//ANT, BNT, AMT, BMT;
//CNT, CMT
static ObRouteType route_order_cursor_of_merge_idc_order_optimized[] = {
    ROUTE_TYPE_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_MERGE_REGION,

    ROUTE_TYPE_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANRP, BNRP, AMRP, BMRP, ANWP, BNWP, AMWP, BMWP;
//CNRP, CMRP, CNWP, CMWP
//ANRT, BNRT, AMRT, BMRT, ANWT, BNWT, AMWT, BMWT;
//CNRT, CMRT, CNWT, CMWT
static ObRouteType route_order_cursor_of_readonly_zone_first_optimized[] = {
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE,
    ROUTE_TYPE_MAX
};

//ANRP, BNRP, AMRP, BMRP;
//CNRP, CMRP
//ANRT, BNRT, AMRT, BMRT;
//CNRT, CMRT
static ObRouteType route_order_cursor_of_only_readonly_zone_optimized[] = {
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANRP, BNRP, ANWP, BNWP, AMRP, BMRP, AMWP, BMWP;
//CNRP, CNWP, CMRP, CMWP
//ANRT, BNRT, ANWT, BNWT, AMRT, BMRT, AMWT, BMWT;
//CNRT, CNWT, CMRT, CMWT
static ObRouteType route_order_cursor_of_unmerge_zone_first_optimized[] = {
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};


//ANWP, BNWP, AMWP, BMWP;
//CNWP, CMWP
//ANWT, BNWT, AMWT, BMWT;
//CNWT, CMWT
static ObRouteType route_order_cursor_of_only_readwrite_zone_optimized[] = {
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, AMPF, BMPF;
//ANPL, BNPL, AMPL, BMPL;
//ANT, BNT, AMT, BMT;
//CNPF, CMPF;
//CNPL, CMPL;
//CNT, CMT
static ObRouteType route_order_cursor_of_follower_first[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, ANPL, BNPL;
//AMPF, BMPF, AMPL, BMPL;
//ANT, BNT, AMT, BMT;
//CNPF, CNPL;
//CMPF, CMPL;
//CNT, CMT
static ObRouteType route_order_cursor_of_unmerge_follower_first[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE,

    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};


//ANPF, BNPF, AMPF, BMPF;
//ANPL, BNPL, AMPL, BMPL;
//CNPF, CMPF;
//CNPL, CMPL;
//ANT, BNT, AMT, BMT;
//CNT, CMT
static ObRouteType route_order_cursor_of_follower_first_optimized[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, ANPL, BNPL;
//AMPF, BMPF, AMPL, BMPL;
//CNPF, CNPL;
//CMPF, CMPL;
//ANT, BNT, AMT, BMT;
//CNT, CMT
static ObRouteType route_order_cursor_of_unmerge_follower_first_optimized[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE,

    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPD, BNPD, AMPD, BMPD
//CNPD, CMPD
//ANP, BNP, AMP, BMP;
//CNP, CMP;
//ANT, BNT, AMT, BMT;
//CNT, CMT
static ObRouteType route_order_cursor_of_dup_strong_read_order[] = {
    ROUTE_TYPE_DUP_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_DUP_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_DUP_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_DUP_PARTITION_MERGE_REGION,
    ROUTE_TYPE_DUP_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_DUP_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_MERGE_REGION,

    ROUTE_TYPE_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, AMPF, BMPF;
//ANT, BNT, AMT, BMT;
//CNPF, CMPF;
//CNT, CMT
static ObRouteType route_order_cursor_of_follower_only[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, AMPF, BMPF;
//CNPF, CMPF;
//ANT, BNT, AMT, BMT;
//CNT, CMT
static ObRouteType route_order_cursor_of_follower_only_optimized[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

static ObRouteType route_order_cursor_of_proxy_primary_zone_name_only[] = {
  ROUTE_TYPE_MAX
};

static ObRouteType route_order_cursor_of_target_db_server_only[] = {
  ROUTE_TYPE_MAX
};

static ObRouteType route_order_cursor_of_primary_zone_first[] = {
  ROUTE_TYPE_MAX
};

//ANP, BNP, AMP, BMP;
//ANT, BNT, AMT, BMT;
//CNP, CMP;
//CNT, CMT
static ObRouteType route_order_cursor_of_target_replica_type_with_leader[] = {
    ROUTE_TYPE_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, AMPF, BMPF;
//ANPL, BNPL, AMPL, BMPL;
//ANT, BNT, AMT, BMT;
//CNPF, CMPF;
//CNPL, CMPL;
//CNT, CMT
static ObRouteType route_order_cursor_of_target_replica_type_follower_first[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

//ANPF, BNPF, AMPF, BMPF;
//ANT, BNT, AMT, BMT;
//CNPF, CMPF;
//CNT, CMT
static ObRouteType route_order_cursor_of_target_replica_type_follower_only[] = {
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION,

    ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_UNMERGE_REGION,
    ROUTE_TYPE_NONPARTITION_MERGE_LOCAL,
    ROUTE_TYPE_NONPARTITION_MERGE_REGION,

    ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE,

    ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE,
    ROUTE_TYPE_NONPARTITION_MERGE_REMOTE,

    ROUTE_TYPE_MAX
};

static ObRouteType route_order_cursor_of_weight_load_balance[] = {
  ROUTE_TYPE_MAX
};

const ObRouteType *ObLDCRoute::route_order_cursor_[] = {
    route_order_cursor_of_merge_idc_order,
    route_order_cursor_of_readonly_zone_first,
    route_order_cursor_of_only_readonly_zone,
    route_order_cursor_of_unmerge_zone_first,
    route_order_cursor_of_only_readwrite_zone,
    route_order_cursor_of_merge_idc_order_optimized,
    route_order_cursor_of_readonly_zone_first_optimized,
    route_order_cursor_of_only_readonly_zone_optimized,
    route_order_cursor_of_unmerge_zone_first_optimized,
    route_order_cursor_of_only_readwrite_zone_optimized,
    route_order_cursor_of_follower_first,
    route_order_cursor_of_unmerge_follower_first,
    route_order_cursor_of_follower_first_optimized,
    route_order_cursor_of_unmerge_follower_first_optimized,
    route_order_cursor_of_dup_strong_read_order,
    route_order_cursor_of_follower_only,
    route_order_cursor_of_follower_only_optimized,
    route_order_cursor_of_proxy_primary_zone_name_only,
    route_order_cursor_of_target_db_server_only,
    route_order_cursor_of_primary_zone_first,
    route_order_cursor_of_target_replica_type_with_leader,
    route_order_cursor_of_target_replica_type_follower_first,
    route_order_cursor_of_target_replica_type_follower_only,
    route_order_cursor_of_weight_load_balance,
};

int64_t ObLDCRoute::route_order_size_[] = {
    sizeof(route_order_cursor_of_merge_idc_order) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_readonly_zone_first) / sizeof(ObRouteType),//25
    sizeof(route_order_cursor_of_only_readonly_zone) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_unmerge_zone_first) / sizeof(ObRouteType),//25
    sizeof(route_order_cursor_of_only_readwrite_zone) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_merge_idc_order_optimized) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_readonly_zone_first_optimized) / sizeof(ObRouteType),//25
    sizeof(route_order_cursor_of_only_readonly_zone_optimized) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_unmerge_zone_first_optimized) / sizeof(ObRouteType),//25
    sizeof(route_order_cursor_of_only_readwrite_zone_optimized) / sizeof(ObRouteType),//13

    sizeof(route_order_cursor_of_follower_first) / sizeof(ObRouteType),//19
    sizeof(route_order_cursor_of_unmerge_follower_first) / sizeof(ObRouteType),//19
    sizeof(route_order_cursor_of_follower_first_optimized) / sizeof(ObRouteType),//19
    sizeof(route_order_cursor_of_unmerge_follower_first_optimized) / sizeof(ObRouteType),//19

    sizeof(route_order_cursor_of_dup_strong_read_order) / sizeof(ObRouteType),//18

    sizeof(route_order_cursor_of_follower_only) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_follower_only_optimized) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_proxy_primary_zone_name_only) / sizeof(ObRouteType),//1
    sizeof(route_order_cursor_of_target_db_server_only) / sizeof(ObRouteType),//1
    sizeof(route_order_cursor_of_primary_zone_first) / sizeof(ObRouteType),//1
    sizeof(route_order_cursor_of_target_replica_type_with_leader) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_target_replica_type_follower_first) / sizeof(ObRouteType),//19
    sizeof(route_order_cursor_of_target_replica_type_follower_only) / sizeof(ObRouteType),//13
    sizeof(route_order_cursor_of_weight_load_balance) / sizeof(ObRouteType),//1
};

const ObLDCItem *ObLDCRoute::get_next_item()
{
  ObLDCItem *ret_item = NULL;
  if (!location_.is_empty()) {
    const int64_t *site_start_index_array = location_.get_site_start_index_array();
    ObLDCItem *item_array = location_.get_item_array();
    // 目前PROXY_PRIMARY_ZONE_NAME_ONLY、TARGET_DB_SERVER_ONLY、PRIMARY_ZONE_FIRST没有机器列表，不关心observer类型
    bool not_check_route_type = is_not_check_route_type();
    ObRouteType route_type = get_route_type(curr_cursor_index_);
    ObIDCType idc_type = not_check_route_type ? ObIDCType::OTHER_REGION : get_idc_type(route_type);
    bool need_break = not_check_route_type ? false : (ROUTE_TYPE_MAX == route_type);
    while (!need_break) {
      if (next_index_in_site_ >= site_start_index_array[idc_type + 1]) {
        LOG_DEBUG("need try next cursor type", K_(curr_cursor_index),
                  "curr_route_type", get_route_type_string(route_type),
                  K_(next_index_in_site), "site_start_index_array",
                  ObArrayWrap<int64_t>(site_start_index_array, MAX_IDC_TYPE + 1));
        ++curr_cursor_index_;
        route_type = get_route_type(curr_cursor_index_);
        if (ROUTE_TYPE_MAX == route_type) {
          LOG_DEBUG("it is reach end now");
          need_break = true;
        } else {
          idc_type = get_idc_type(route_type);
          next_index_in_site_ = site_start_index_array[idc_type];
        }
      } else {
        ret_item = item_array + next_index_in_site_;
        ++next_index_in_site_;
        // 对新增路由策略，采用随机的方式，没有机器优先级，无需比较
        if (!ret_item->is_used_
            && (not_check_route_type
                || (is_same_role(route_type, *ret_item)
                    && is_same_partition_type(route_type, *ret_item)
                    && is_same_zone_type(route_type, *ret_item)
                    && is_same_dup_replica_type(route_type, *ret_item)
                    && (disable_merge_status_check_ || is_same_merge_type(route_type, *ret_item))))) {
          ret_item->is_used_ = true;
          need_break = true;
          LOG_DEBUG("succ to get_next_replica", KPC(ret_item), K_(disable_merge_status_check),
                    "curr_route_type", get_route_type_string(route_type), K(not_check_route_type));
        } else {
          LOG_DEBUG("item is not excepted, try next", KPC(ret_item),
                    "curr_route_type", get_route_type_string(route_type),
                    K_(disable_merge_status_check),
                    K_(curr_cursor_index), K_(next_index_in_site));
          ret_item = NULL;
        }
      }
    }
  } else {
    //set to max idx
    curr_cursor_index_ = route_order_size_[policy_] - 1;
  }
  return ret_item;
}

const ObLDCItem *ObLDCRoute::get_next_primary_zone_item()
{
  ObLDCItem *ret_item = NULL;

  if (!location_.is_primary_zone_empty()) {
    ObLDCItem *pz_item_array = location_.get_primary_zone_item_array();
    int64_t pz_array_count = location_.primary_zone_count();
    for (int64_t i = 0; i < pz_array_count; ++i) {
      ret_item = &pz_item_array[i];
      if (!ret_item->is_used_) {
        ret_item->is_used_ = true;
        break;
      } else {
        ret_item = NULL;
      }
    }
  }

  return ret_item;
}

const ObLDCItem *ObLDCRoute::get_next_weight_item()
{
  ObLDCItem *ret_item = NULL;
  if (!is_weight_zone_empty()) {
    bool finish = false;
    if (weight_zone_index_ >= location_.get_weight_zone_count()) {
      weight_zone_index_ = -1;
    }
    ObLDCLocation::ObWeightZoneArray &weight_zone_array = *location_.get_all_weight_zone_array();
    while (!finish && NULL == ret_item)  {
      if (!finish && -1 == weight_zone_index_) {
        weight_zone_index_ = location_.get_rand_zone_index();
        finish = (-1 == weight_zone_index_);
      }
      LOG_DEBUG("random weight zone index", K_(weight_zone_index), K(finish));
      if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
        if (-1 != weight_zone_index_) {
          const ObConfigVariableString &zone_name = weight_zone_array.at(weight_zone_index_)->zone_name_;
          LOG_DEBUG("weight zone name is ", K(zone_name));
        }
      }
      if (!finish && NULL == ret_item && -1 != weight_zone_index_) {
        ObWeightZoneItems* weight = weight_zone_array.at(weight_zone_index_);
        if (OB_NOT_NULL(weight)) {
          ObIArray<ObLDCItem> &item_array = weight->weight_zone_item_array_;
          for (int64_t i = 0; i < item_array.count(); ++i) {
            if (!item_array.at(i).is_used_) {
              ret_item = &item_array.at(i);
              ret_item->is_used_ = true;
              break;
            }
          }
        }
      }// end if
      // 触发重试时，会先选择某个zone的replica。所有replica重试后，需要重置
      if (!finish && NULL == ret_item && -1 != weight_zone_index_) {
        weight_zone_index_ = -1;
      }
    }// end while
  }
  return ret_item;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
