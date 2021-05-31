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

#ifndef OBPROXY_LDC_STRUCT_H
#define OBPROXY_LDC_STRUCT_H
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum ObIDCType
{
  SAME_IDC = 0,
  SAME_REGION,
  OTHER_REGION,
  MAX_IDC_TYPE,
};
const common::ObString get_idc_type_string(const ObIDCType type);

/*
 * |--- n bits  ---|--- 2 bits  ---|--- 2 bits  ---|--- 2 bits ---|--- 2 bits ---|--- 2 bits ---| LSB
 * | new attribute |-- role type --|-- pl status --|  zone type   | merge status |     IDC      | LSB
 */
static const int64_t MERGE_BITS_SHIFT = 2;
static const int64_t ZONE_TYPE_BITS_SHIFT = 4;
static const int64_t PARTITION_BITS_SHIFT = 6;
static const int64_t ROLE_TYPE_BITS_SHIFT = 8;
static const int64_t NEW_ATTRIBUTE_BITS_SHIFT = 10;
static const int64_t IN_SAME_IDC = static_cast<int64_t>(SAME_IDC); //LOCAL
static const int64_t IN_SAME_REGION = static_cast<int64_t>(SAME_REGION); //REGION
static const int64_t IN_OTHER_REGION = static_cast<int64_t>(OTHER_REGION);//REMOTE
static const int64_t UNKNOWN_REGION = static_cast<int64_t>(MAX_IDC_TYPE);//UNKNOWN

static const int64_t IN_UNMERGING = 0 << MERGE_BITS_SHIFT; //UNMERGE
static const int64_t IN_MERGING = 1 << MERGE_BITS_SHIFT;//MERGE
static const int64_t UNKNOWN_MERGE_STATUS = 3 << MERGE_BITS_SHIFT;//UNKNOWN

static const int64_t IS_READWRITE_ZONE = 0 << ZONE_TYPE_BITS_SHIFT; //READWRITE
static const int64_t IS_READONLY_ZONE = 1 << ZONE_TYPE_BITS_SHIFT; //READONLY
static const int64_t UNKNOWN_ZONE_TYPE = 3 << ZONE_TYPE_BITS_SHIFT;//UNKNOWN

static const int64_t WITH_PARTITION_TYPE = 0 << PARTITION_BITS_SHIFT;//PARTITION
static const int64_t WITHOUT_PARTITION_TYPE = 1 << PARTITION_BITS_SHIFT;//NONPARTITION
static const int64_t UNKNOWN_PARTITION_TYPE = 3 << PARTITION_BITS_SHIFT;//UNKNOWN

static const int64_t IS_FOLLOWER = 0 << ROLE_TYPE_BITS_SHIFT; //FOLLOWER
static const int64_t IS_LEADER = 1 << ROLE_TYPE_BITS_SHIFT; //LEADER
static const int64_t UNKNOWN_ROLE = 3 << ROLE_TYPE_BITS_SHIFT;//UNKNOWN

static const int64_t MAX_IDC_VALUE = ((1 << MERGE_BITS_SHIFT) -1);//0x0011
static const int64_t MAX_MERGE_VALUE = (((1 << (ZONE_TYPE_BITS_SHIFT - MERGE_BITS_SHIFT)) -1) << MERGE_BITS_SHIFT);//0x01100
static const int64_t MAX_ZONE_TYPE_VALUE = (((1 << (PARTITION_BITS_SHIFT - ZONE_TYPE_BITS_SHIFT)) -1) << ZONE_TYPE_BITS_SHIFT);//0x0011 0000
static const int64_t MAX_PARTITION_TYPE_VALUE = (((1 << (ROLE_TYPE_BITS_SHIFT - PARTITION_BITS_SHIFT)) -1) << PARTITION_BITS_SHIFT);//0x1100 0000
static const int64_t MAX_ROLE_TYPE_VALUE = (((1 << (NEW_ATTRIBUTE_BITS_SHIFT - ROLE_TYPE_BITS_SHIFT)) -1) << ROLE_TYPE_BITS_SHIFT);//0x0011 0000 0000

enum ObRouteType
{
  ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL  = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READWRITE_ZONE | IN_UNMERGING | IN_SAME_IDC), // 768
  ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READWRITE_ZONE | IN_UNMERGING | IN_SAME_REGION), // 769
  ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READWRITE_ZONE | IN_UNMERGING | IN_OTHER_REGION), // 770
  ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL    = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READWRITE_ZONE | IN_MERGING | IN_SAME_IDC), // 772
  ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION   = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READWRITE_ZONE | IN_MERGING | IN_SAME_REGION), // 723
  ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE   = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READWRITE_ZONE | IN_MERGING | IN_OTHER_REGION), // 724

  ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL   = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READONLY_ZONE | IN_UNMERGING | IN_SAME_IDC), // 784
  ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION  = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READONLY_ZONE | IN_UNMERGING | IN_SAME_REGION), // 785
  ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE  = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READONLY_ZONE | IN_UNMERGING | IN_OTHER_REGION), // 786
  ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL     = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READONLY_ZONE | IN_MERGING | IN_SAME_IDC), // 788
  ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION    = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READONLY_ZONE | IN_MERGING | IN_SAME_REGION), // 789
  ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE    = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | IS_READONLY_ZONE | IN_MERGING | IN_OTHER_REGION), // 790

  ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL  = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READWRITE_ZONE | IN_UNMERGING | IN_SAME_IDC), // 832
  ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READWRITE_ZONE | IN_UNMERGING | IN_SAME_REGION), // 833
  ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READWRITE_ZONE | IN_UNMERGING | IN_OTHER_REGION), // 834
  ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL    = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READWRITE_ZONE | IN_MERGING | IN_SAME_IDC), // 836
  ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION   = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READWRITE_ZONE | IN_MERGING | IN_SAME_REGION), // 837
  ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE   = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READWRITE_ZONE | IN_MERGING | IN_OTHER_REGION), // 838

  ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL   = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READONLY_ZONE | IN_UNMERGING | IN_SAME_IDC), // 848
  ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION  = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READONLY_ZONE | IN_UNMERGING | IN_SAME_REGION), // 849
  ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE  = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READONLY_ZONE | IN_UNMERGING | IN_OTHER_REGION), // 850
  ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL     = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READONLY_ZONE | IN_MERGING | IN_SAME_IDC), // 852
  ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION    = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READONLY_ZONE | IN_MERGING | IN_SAME_REGION), // 853
  ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE    = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | IS_READONLY_ZONE | IN_MERGING | IN_OTHER_REGION), // 854

  ROUTE_TYPE_PARTITION_UNMERGE_LOCAL  = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_IDC), // 816
  ROUTE_TYPE_PARTITION_UNMERGE_REGION = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_REGION), // 817
  ROUTE_TYPE_PARTITION_UNMERGE_REMOTE = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_OTHER_REGION), // 818
  ROUTE_TYPE_PARTITION_MERGE_LOCAL    = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_IDC), // 820
  ROUTE_TYPE_PARTITION_MERGE_REGION   = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_REGION), // 821
  ROUTE_TYPE_PARTITION_MERGE_REMOTE   = (UNKNOWN_ROLE | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_OTHER_REGION), // 822

  ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL  = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_IDC), // 880
  ROUTE_TYPE_NONPARTITION_UNMERGE_REGION = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_REGION), // 881
  ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_OTHER_REGION), // 882
  ROUTE_TYPE_NONPARTITION_MERGE_LOCAL    = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_IDC), // 884
  ROUTE_TYPE_NONPARTITION_MERGE_REGION   = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_REGION), // 885
  ROUTE_TYPE_NONPARTITION_MERGE_REMOTE   = (UNKNOWN_ROLE | WITHOUT_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_OTHER_REGION), // 886

  ROUTE_TYPE_LEADER = (IS_LEADER | WITH_PARTITION_TYPE | MAX_ZONE_TYPE_VALUE | MAX_MERGE_VALUE | MAX_IDC_VALUE), // 319

  ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL  = (IS_FOLLOWER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_IDC), // 48
  ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION = (IS_FOLLOWER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_REGION), // 49
  ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE = (IS_FOLLOWER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_OTHER_REGION), // 50
  ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL    = (IS_FOLLOWER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_IDC), // 52
  ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION   = (IS_FOLLOWER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_REGION), // 53
  ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE   = (IS_FOLLOWER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_OTHER_REGION), // 54

  ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL  = (IS_LEADER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_IDC), // 304
  ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION = (IS_LEADER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_SAME_REGION), // 305
  ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE = (IS_LEADER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_UNMERGING | IN_OTHER_REGION), // 306
  ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL    = (IS_LEADER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_IDC), // 308
  ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION   = (IS_LEADER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_SAME_REGION), // 309
  ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE   = (IS_LEADER | WITH_PARTITION_TYPE | UNKNOWN_ZONE_TYPE | IN_MERGING | IN_OTHER_REGION), // 310

  ROUTE_TYPE_MAX = (MAX_ROLE_TYPE_VALUE | MAX_PARTITION_TYPE_VALUE | MAX_ZONE_TYPE_VALUE | MAX_MERGE_VALUE | MAX_IDC_VALUE),//1023
};
common::ObString get_route_type_string(const ObRouteType type);

class ObRouteTypeCheck
{
public:
  static bool is_route_type_valid(const int32_t route_type)
  {
    return (route_type >= 0 && route_type < ROUTE_TYPE_MAX);
  }
  static bool is_same_idc_type(const int32_t route_type)
  {
    return IN_SAME_IDC == (route_type & MAX_IDC_VALUE);
  }
  static bool is_same_region_type(const int32_t route_type)
  {
    return IN_SAME_REGION == (route_type & MAX_IDC_VALUE);
  }
  static bool is_other_region_type(const int32_t route_type)
  {
    return IN_OTHER_REGION == (route_type & MAX_IDC_VALUE);
  }
  static bool is_unmerge_type(const int32_t route_type)
  {
    return IN_UNMERGING == (route_type & MAX_MERGE_VALUE);
  }
  static bool is_merge_type(const int32_t route_type)
  {
    return IN_MERGING == (route_type & MAX_MERGE_VALUE);
  }
  static bool is_readonly_zone_type(const int32_t route_type)
  {
    return IS_READONLY_ZONE == (route_type & MAX_ZONE_TYPE_VALUE);
  }
  static bool is_readwrite_zone_type(const int32_t route_type)
  {
    return IS_READWRITE_ZONE == (route_type & MAX_ZONE_TYPE_VALUE);
  }
  static bool is_unknown_zone_type(const int32_t route_type)
  {
    return UNKNOWN_ZONE_TYPE == (route_type & MAX_ZONE_TYPE_VALUE);
  }
  static bool is_with_partition_type(const int32_t route_type)
  {
    return WITH_PARTITION_TYPE == (route_type & MAX_PARTITION_TYPE_VALUE);
  }
  static bool is_without_partition_type(const int32_t route_type)
  {
    return WITHOUT_PARTITION_TYPE == (route_type & MAX_PARTITION_TYPE_VALUE);
  }
  static bool is_leader_role(const int32_t route_type)
  {
    return IS_LEADER == (route_type & MAX_ROLE_TYPE_VALUE);
  }
  static bool is_follower_role(const int32_t route_type)
  {
    return IS_FOLLOWER == (route_type & MAX_ROLE_TYPE_VALUE);
  }
};

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
  MAX_ROUTE_POLICY_COUNT,
};
common::ObString get_route_policy_enum_string(const ObRoutePolicyEnum policy);
bool is_route_policy_enum_valid(const ObRoutePolicyEnum policy);


common::ObString get_route_policy_enum_string(const ObRoutePolicyEnum policy)
{
  static const common::ObString string_array[MAX_ROUTE_POLICY_COUNT] =
  {
      common::ObString::make_string("MERGE_IDC_ORDER"),
      common::ObString::make_string("READONLY_ZONE_FIRST"),
      common::ObString::make_string("ONLY_READONLY_ZONE"),
      common::ObString::make_string("UNMERGE_ZONE_FIRST"),
      common::ObString::make_string("ONLY_READWRITE_ZONE"),
      common::ObString::make_string("MERGE_IDC_ORDER_OPTIMIZED"),
      common::ObString::make_string("READONLY_ZONE_FIRST_OPTIMIZED"),
      common::ObString::make_string("ONLY_READONLY_ZONE_OPTIMIZED"),
      common::ObString::make_string("UNMERGE_ZONE_FIRST_OPTIMIZED"),
      common::ObString::make_string("ONLY_READWRITE_ZONE_OPTIMIZED"),
      common::ObString::make_string("FOLLOWER_FIRST"),
      common::ObString::make_string("UNMERGE_FOLLOWER_FIRST"),
      common::ObString::make_string("FOLLOWER_FIRST_OPTIMIZED"),
      common::ObString::make_string("UNMERGE_FOLLOWER_FIRST_OPTIMIZED"),
  };

  common::ObString string;
  if (OB_LIKELY(policy >= MERGE_IDC_ORDER) && OB_LIKELY(policy < MAX_ROUTE_POLICY_COUNT)) {
    string = string_array[policy];
  }
  return string;
}

bool is_route_policy_enum_valid(const ObRoutePolicyEnum policy)
{
  return (MERGE_IDC_ORDER <= policy && policy < MAX_ROUTE_POLICY_COUNT);
}

common::ObString get_route_type_string(const ObRouteType type)
{
  const char *str = "";
  switch (type) {
    case ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION:
      str = "ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL:
      str = "ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION:
      str = "ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION";
      break;
    case ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE:
      str = "ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE";
      break;
    case ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION:
      str = "ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL:
      str = "ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION:
      str = "ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION";
      break;
    case ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE:
      str = "ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE";
      break;
    case ROUTE_TYPE_PARTITION_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_PARTITION_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_PARTITION_UNMERGE_REGION:
      str = "ROUTE_TYPE_PARTITION_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_PARTITION_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_PARTITION_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_PARTITION_MERGE_LOCAL:
      str = "ROUTE_TYPE_PARTITION_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_PARTITION_MERGE_REGION:
      str = "ROUTE_TYPE_PARTITION_MERGE_REGION";
      break;
    case ROUTE_TYPE_PARTITION_MERGE_REMOTE:
      str = "ROUTE_TYPE_PARTITION_MERGE_REMOTE";
      break;
    case ROUTE_TYPE_LEADER:
      str = "ROUTE_TYPE_LEADER";
      break;
    case ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION:
      str = "ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL:
      str = "ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION:
      str = "ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION";
      break;
    case ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE:
      str = "ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE";
      break;
    case ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION:
      str = "ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL:
      str = "ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION:
      str = "ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION";
      break;
    case ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE:
      str = "ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE";
      break;
    case ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_NONPARTITION_UNMERGE_REGION:
      str = "ROUTE_TYPE_NONPARTITION_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_NONPARTITION_MERGE_LOCAL:
      str = "ROUTE_TYPE_NONPARTITION_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_NONPARTITION_MERGE_REGION:
      str = "ROUTE_TYPE_NONPARTITION_MERGE_REGION";
      break;
    case ROUTE_TYPE_NONPARTITION_MERGE_REMOTE:
      str = "ROUTE_TYPE_NONPARTITION_MERGE_REMOTE";
      break;
      // follower first
    case ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION:
      str = "ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL:
      str = "ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION:
      str = "ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION";
      break;
    case ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE:
      str = "ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE";
      break;

    case ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL:
      str = "ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL";
      break;
    case ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION:
      str = "ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION";
      break;
    case ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE:
      str = "ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE";
      break;
    case ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL:
      str = "ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL";
      break;
    case ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION:
      str = "ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION";
      break;
    case ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE:
      str = "ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE";
      break;

    case ROUTE_TYPE_MAX:
      str = "ROUTE_TYPE_MAX";
      break;
    default:
      str = "ROUTE_TYPE_UNKNOWN";
  }
  return common::ObString::make_string(str);
}


const common::ObString get_idc_type_string(const ObIDCType type)
{
  static const common::ObString string_array[MAX_IDC_TYPE] =
  {
      common::ObString::make_string("SAME_IDC"),
      common::ObString::make_string("SAME_REGION"),
      common::ObString::make_string("OTHER_REGION"),
  };

  common::ObString string;
  if (OB_LIKELY(type >= SAME_IDC) && OB_LIKELY(type < MAX_IDC_TYPE)) {
    string = string_array[type];
  }
  return string;
}

enum ObProxyRoutePolicyEnum {
  FOLLOWER_FIRST_ENUM = 0,
  UNMERGE_FOLLOWER_FIRST_ENUM,
  MAX_PROXY_ROUTE_POLICY,
};

bool is_valid_proxy_route_policy(const ObProxyRoutePolicyEnum policy);
const common::ObString get_proxy_route_policy_enum_string(const ObRoutePolicyEnum policy);
ObProxyRoutePolicyEnum get_proxy_route_policy(const common::ObString &value);

bool is_valid_proxy_route_policy(const ObProxyRoutePolicyEnum policy)
{
  return MAX_PROXY_ROUTE_POLICY != policy;
}

const common::ObString get_proxy_route_policy_enum_string(const ObProxyRoutePolicyEnum policy)
{
  common::ObString string;
  static const common::ObString string_array[MAX_PROXY_ROUTE_POLICY] =
  {
      common::ObString::make_string("FOLLOWER_FIRST"),
      common::ObString::make_string("UNMERGE_FOLLOWER_FIRST"),
  };

  if (OB_LIKELY(policy >= FOLLOWER_FIRST_ENUM)
      && OB_LIKELY(policy < MAX_PROXY_ROUTE_POLICY)) {
    string = string_array[policy];
  }
  return string;
}

ObProxyRoutePolicyEnum get_proxy_route_policy(const common::ObString &value)
{
  ObProxyRoutePolicyEnum ret_enum = MAX_PROXY_ROUTE_POLICY;
  if (!value.empty()) {
    for (int32_t i = 0; i < static_cast<int32_t>(MAX_PROXY_ROUTE_POLICY) && MAX_PROXY_ROUTE_POLICY == ret_enum; i++) {
      if (0 == get_proxy_route_policy_enum_string(static_cast<ObProxyRoutePolicyEnum>(i)).case_compare(value)) {
        ret_enum = static_cast<ObProxyRoutePolicyEnum>(i);
      }
    }
  }
  return ret_enum;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_LDC_STRUCT_H */
