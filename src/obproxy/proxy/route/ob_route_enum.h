/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_OB_ROUTE_ENUM_H
#define OBPROXY_OB_ROUTE_ENUM_H

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum class ObRouteInfoType {
  INVALID = 0,
  USE_GLOBAL_INDEX,
  USE_OBPROXY_ROUTE_ADDR,
  USE_CURSOR,
  USE_PREPARE_EXECUTED_ADDR,
  USE_PIECES_DATA,
  USE_CONFIG_TARGET_DB,
  USE_COMMENT_TARGET_DB,
  USE_TEST_SVR_ADDR,
  USE_LAST_SESSION,
  USE_LAST_INSERT_ID_SESSION,
  USE_SHARD_TXN_SESSION,
  USE_LOCK_SESSION,
  USE_CACHED_SESSION,
  USE_SINGLE_LEADER,
  USE_SINGLE_LEADERS_FOLLOWER,
  USE_PARTITION_LOCATION_LOOKUP,
  USE_COORDINATOR_SESSION,
  USE_ROUTE_POLICY,
  USE_BINLOG_SERVICE_LOOKUP,
  USE_CDC_COORDINATOR_LOOKUP,
  USE_CDC_MSGSERVICE_LOOKUP
};
const char *get_route_info_type_name(const ObRouteInfoType type);


/*
 * A: same idc
 * B: same region
 * C: other region
 *
 * N: normal, no merge
 * M: merging
 *
 * R: readonly zone
 * W: readwrite zone
 *
 * P: with partition
 * T: without partiton
 *
 * L: leader
 * F: follower
 *
 * D: dup replica
 *
 * e.g. ANRP=same idc + unmerge + readonly zone + with partiton
 *      BMWT=same region + merging + readwrite zone + without partiton
 *
 * */
enum ObRoutePolicyEnum
{
  //for non-dml strong read, we use unmerge_idc order better, inner use
  MERGE_IDC_ORDER = 0,    //ANP, BNP, AMP, BMP;
                          //ANT, BNT, AMT, BMT;
                          //CNP, CMP;
                          //CNT, CMT

  READONLY_ZONE_FIRST, //ANRP, BNRP, AMRP, BMRP, ANWP, BNWP, AMWP, BMWP;
                       //ANRT, BNRT, AMRT, BMRT, ANWT, BNWT, AMWT, BMWT;
                       //CNRP, CMRP, CNWP, CMWP
                       //CNRT, CMRT, CNWT, CMWT
  ONLY_READONLY_ZONE,  //ANRP, BNRP, AMRP, BMRP;
                       //ANRT, BNRT, AMRT, BMRT;
                       //CNRP, CMRP
                       //CNRT, CMRT
  UNMERGE_ZONE_FIRST,  //ANRP, BNRP, ANWP, BNWP, AMRP, BMRP, AMWP, BMWP;
                       //ANRT, BNRT, ANWT, BNWT, AMRT, BMRT, AMWT, BMWT;
                       //CNRP, CNWP, CMRP, CMWP
                       //CNRT, CNWT, CMRT, CMWT
  //readonly zone can accept some dml
  ONLY_READWRITE_ZONE,   //ANWP, BNWP, AMWP, BMWP;
                         //ANWT, BNWT, AMWT, BMWT;
                         //CNWP, CMWP
                         //CNWT, CMWT

  //optimize for partition only remote exist(local/region not exist or force_congested)
  MERGE_IDC_ORDER_OPTIMIZED,  //ANP, BNP, AMP, BMP;
                              //CNP, CMP;
                              //ANT, BNT, AMT, BMT;
                              //CNT, CMT

  READONLY_ZONE_FIRST_OPTIMIZED, //ANRP, BNRP, AMRP, BMRP, ANWP, BNWP, AMWP, BMWP;
                                 //CNRP, CMRP, CNWP, CMWP
                                 //ANRT, BNRT, AMRT, BMRT, ANWT, BNWT, AMWT, BMWT;
                                 //CNRT, CMRT, CNWT, CMWT
  ONLY_READONLY_ZONE_OPTIMIZED,  //ANRP, BNRP, AMRP, BMRP;
                                 //CNRP, CMRP
                                 //ANRT, BNRT, AMRT, BMRT;
                                 //CNRT, CMRT
  UNMERGE_ZONE_FIRST_OPTIMIZED,  //ANRP, BNRP, ANWP, BNWP, AMRP, BMRP, AMWP, BMWP;
                                 //CNRP, CNWP, CMRP, CMWP
                                 //ANRT, BNRT, ANWT, BNWT, AMRT, BMRT, AMWT, BMWT;
                                 //CNRT, CNWT, CMRT, CMWT
  //readonly zone can accept some dml
  ONLY_READWRITE_ZONE_OPTIMIZED,   //ANWP, BNWP, AMWP, BMWP;
                                   //CNWP, CMWP
                                   //ANWT, BNWT, AMWT, BMWT;
                                   //CNWT, CMWT

  //for weak read, follower first
  FOLLOWER_FIRST,         //ANPF, BNPF, AMPF, BMPF;
                          //ANPL, BNPL, AMPL, BMPL;
                          //ANT, BNT, AMT, BMT;
                          //CNPF, CMPF;
                          //CNPL, CMPL;
                          //CNT, CMT

  UNMERGE_FOLLOWER_FIRST, //ANPF, BNPF, ANPL, BNPL;
                          //AMPF, BMPF, AMPL, BMPL;
                          //ANT, BNT, AMT, BMT;
                          //CNPF, CNPL;
                          //CMPF, CMPL;
                          //CNT, CMT

  FOLLOWER_FIRST_OPTIMIZED, //ANPF, BNPF, AMPF, BMPF;
                            //ANPL, BNPL, AMPL, BMPL;
                            //CNPF, CMPF;
                            //CNPL, CMPL;
                            //ANT, BNT, AMT, BMT;
                            //CNT, CMT

  UNMERGE_FOLLOWER_FIRST_OPTIMIZED, //ANPF, BNPF, ANPL, BNPL;
                                    //AMPF, BMPF, AMPL, BMPL;
                                    //CNPF, CNPL;
                                    //CMPF, CMPL;
                                    //ANT, BNT, AMT, BMT;
                                    //CNT, CMT

  // for strong with dup replica
  DUP_REPLICA_FIRST, //ANPD, BNPD, AMPD, BMPD, CNPD, CMPD,
                     //ANP, BNP, AMP, BMP,
                     //CNP, CMP
                     //ANT, BNT, AMT, BMT,
                     //CNT, CMT

  //for weak read, follower only
  FOLLOWER_ONLY,          //ANPF, BNPF, AMPF, BMPF;
                          //ANT, BNT, AMT, BMT;
                          //CNPF, CMPF;
                          //CNT, CMT

  FOLLOWER_ONLY_OPTIMIZED, //ANPF, BNPF, AMPF, BMPF;
                            //CNPF, CMPF;
                            //ANT, BNT, AMT, BMT;
                            //CNT, CMT

  PROXY_PRIMARY_ZONE_NAME_ONLY,  // z11,z12,...;z21,z22,..;...

  TARGET_DB_SERVER_ONLY,
  PRIMARY_ZONE_FIRST,
  TARGET_REPLICA_TYPE_WITH_LEADER,  // 选LEADER，和FOLLOWER优先级相同
  TARGET_REPLICA_TYPE_FOLLOWER_FIRST, // 选LEADER，但FOLLOWER优先级更高
  TARGET_REPLICA_TYPE_FOLLOWER_ONLY,  // 不会路由到LEADER
  WEAKREAD_WEIGHT_LOAD_BALANCE,

  MAX_ROUTE_POLICY_COUNT,
};

common::ObString get_route_policy_enum_string(const ObRoutePolicyEnum policy);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif
