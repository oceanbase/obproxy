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

#ifndef _OB_COST_CONSTS_DEF_H_
#define _OB_COST_CONSTS_DEF_H_ 1

namespace oceanbase
{
namespace common
{

/**
 * Calculate the cpu cost of a basic operator
 */
const int64_t CPU_OPERATOR_COST = 1;

const int64_t NO_ROW_FAKE_COST = 100;
///The basic cost of a rpc
const int64_t NET_BASE_COST = 100;
///The basic cost of rpc sending a row of data
const double NET_PER_ROW_COST = 0.4;

///The cost of updating a row of data. Temporarily written
const int64_t UPDATE_ONE_ROW_COST = 1;
///The cost of DELETE a row of data. Temporarily written
const int64_t DELETE_ONE_ROW_COST = 1;
/**
 * The cpu cost of processing a row of data
 */
const int64_t CPU_TUPLE_COST = 1;

/**
 * The default selection rate of equivalent expressions such as ("A = b")
 */
const double DEFAULT_EQ_SEL = 0.005;

/**
 * The default selection rate for non-equivalent expressions (such as "A <b")
 */
const double DEFAULT_INEQ_SEL = 1.0 / 3.0;

/**
 * The default selection rate that can't be guessed: half and half
 */
const double DEFAULT_SEL = 0.5;

/**
 * The default distinct number
 */
const int64_t DEFAULT_DISTINCT = 10;


//Oracle give range that exceeds limit a additional selectivity
//For get, it starts with 1 / DISTINCT at boarder, linearly decrease across another limit range.
//For scan, the probability of get row decrease with the same formula.

/*
 *                     -------------------
 *                    /|                 | \
 *                   / |                 |x \
 *                  /  |        A        |xx|\
 *                 /   |                 |xx|y\
 *                ------------------------------
 *          min - r   min                max a   max + r
 * */
//We can control the decreasing range r by changine DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO
// r = (max - min) * DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO
//For scan, assuming even distribution, area A represents probability of row falling in range (min,max), which is 1.0
//if we have range i > a, then its probability is area of y
//if we have range i < a, then its probability is A plus x
//the logic is in ObOptimizerCost::do_calc_range_selectivity

const double DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO = 0.5;



const double DEFAULT_GET_EXIST_RATE = 1.0;
const double DEFAULT_CACHE_HIT_RATE = 0.8;
const double DEFAULT_BLOCK_CACHE_HIT_RATE = DEFAULT_CACHE_HIT_RATE;
const double DEFAULT_ROW_CACHE_HIT_RATE = DEFAULT_CACHE_HIT_RATE;

const double LN_2 = 0.69314718055994530941723212145818;
#define LOGN(x) (log(x))
#define LOG2(x) (LOGN(static_cast<double>(x)) / LN_2)


//cost params for table scan
//all one-time cost during a scan process
const double SCAN_STARTUP_T = 10.0;
//one-time cost during processing a row
const double SCAN_ROW_CONST_T = 0.186;
//cost related to processing one obj
const double SCAN_ROW_ONE_COLUMN_T = 0.018;
//cost related to processing a micro block
const double SCAN_MICRO_CONST_T = 6.95;
//additional cost related to processing a micro block if micro block cache miss
const double SCAN_MICRO_BLOCK_CACHE_MISS_T = 20.58;
//cost related to processing a macro block
const double SCAN_MACRO_CONST_T = 14.0;

const double SCAN_OPEN_CACHE_HIT_PER_MICRO_T = 1.0;
const double SCAN_OPEN_CACHE_MISS_PER_MICRO_T = 8.0;


//cost params for table get
const double GET_STARTUP_T = 10.0;
const double GET_ROW_NOT_EXIST_T = 1.0;
const double GET_ROW_CONST_T = 7.67;
const double GET_ROW_ONE_COLUMN_T = 0.037;
const double GET_ROW_CACHE_HIT_T = 1.85;
const double GET_ROW_CACHE_MISS_T = 9.56;
const double GET_BLOCK_CACHE_MISS_T = 15.3;

const double GET_OPEN_ROW_CONST_T = 5.36;
const double GET_OPEN_BF_NOT_EXIST_T = 1.0;
const double GET_OPEN_ROW_CACHE_HIT_T = 3.80;
const double GET_OPEN_ROW_CACHE_MISS_T = 4.73;
const double GET_OPEN_BLOCK_CACHE_HIT_T = 0.067;
const double GET_OPEN_BLOCK_CACHE_MISS_T = 7.76;

const double DEFAULT_IO_RTT_T = 2000;

const int64_t MICRO_PER_MACRO_16K_2M = 115;

//default sizes and counts when stat is not available
const int64_t DEFAULT_ROW_SIZE = 100;
const int64_t DEFAULT_MICRO_BLOCK_SIZE = 16L * 1024;
const int64_t DEFAULT_MACRO_BLOCK_SIZE = 2L * 1024 * 1024;
//if stat is not available, consider relatively small table
//note that default micro block size is 16KB, 128 micros per macro
const int64_t DEFAULT_MACRO_COUNT = 10;

const int64_t DEFAULT_MICRO_COUNT = DEFAULT_MACRO_COUNT * MICRO_PER_MACRO_16K_2M;

const int64_t OB_EST_DEFAULT_ROW_COUNT = 1000;

const int64_t EST_DEF_COL_NUM_DISTINCT = 500;

const double EST_DEF_COL_NULL_RATIO = 0.01;

const double EST_DEF_COL_NOT_NULL_RATIO = 1 - EST_DEF_COL_NULL_RATIO;

const double EST_DEF_VAR_EQ_SEL = EST_DEF_COL_NOT_NULL_RATIO / EST_DEF_COL_NUM_DISTINCT;

const double OB_COLUMN_DISTINCT_RATIO = 2.0 / 3.0;

const int64_t OB_EST_DEFAULT_NUM_NULL = static_cast<int64_t>(static_cast<double>(OB_EST_DEFAULT_ROW_COUNT) * EST_DEF_COL_NULL_RATIO);

const int64_t OB_EST_DEFAULT_DATA_SIZE = OB_EST_DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE;

const int64_t OB_EST_DEFAULT_MACRO_BLOCKS = OB_EST_DEFAULT_DATA_SIZE / DEFAULT_MACRO_BLOCK_SIZE;

const int64_t OB_EST_DEFAULT_MICRO_BLOCKS = OB_EST_DEFAULT_DATA_SIZE / DEFAULT_MICRO_BLOCK_SIZE;

const double OB_DEFAULT_HALF_OPEN_RANGE_SEL = 0.2;

const double OB_DEFAULT_CLOSED_RANGE_SEL = 0.1;

const double ESTIMATE_UNRELIABLE_RATIO = 0.25;

const int64_t OB_DEFAULT_UNEXPECTED_PATH_COST = INT32_MAX;


//these are cost params for operators

//fixed cost for every sort
const double SORT_ONCE_T = 60;
//fixed cost for comparing one column
const double SORT_COMPARE_COL_T = 0.0047;
//fixed cost for every row
const double SORT_ROW_ONCE_T = 1.125;


//fixed setup time for calc other conds(filters) in join
const double JOIN_OTHER_COND_INIT_T = 0.15;
//average cost for calc a filter (sampled with filter t1.a + t2.a < K).
//can be inaccurate
const double JOIN_OTHER_COND_SINGLE_T = 0.17;


/*
 * EQUAL conditions are much cheaper than OTHER conds, because they don't use
 * post expr mechanism. But calc counts may be larger
 */

//fixed setup time for calc equal conds(filters) in join
const double JOIN_EQUAL_COND_INIT_T = 0.054;
//average cost for calc a equal cond
const double JOIN_EQUAL_COND_SINGLE_T = 0.04;

//cost params for material
const double MATERIAL_ONCE_T = 38;
const double MATERIAL_ROW_READ_ONCE_T = 0.15;
const double MATERIAL_ROW_COL_READ_T = 0.023;
const double MATERIAL_ROW_WRITE_ONCE_T = 0.15;
const double MATERIAL_ROW_COL_WRITE_T = 0.023;


//cost params for nestloop join
const double NESTLOOP_ONCE_T = 25.465184;
//each left scan cause a right rescan and etc, so more expensive
const double NESTLOOP_ROW_LEFT_T = 0.186347;
const double NESTLOOP_ROW_RIGHT_T = 0.033857;
const double NESTLOOP_ROW_OUT_T = 0.249701;
const double NESTLOOP_ROW_QUAL_FAIL_T = 0.128629;

const double BLK_NESTLOOP_ONCE_START_T = 8.030292;
const double BLK_NESTLOOP_ONCE_RES_T = 0.249750;
const double BLK_NESTLOOP_QUAL_FAIL_T = 0.129177;
const double BLK_NESTLOOP_ROW_LEFT_T = 0.19983;
const double BLK_NESTLOOP_ROW_RIGHT_T = 0.034;
const double BLK_NESTLOOP_CACHE_SCAN_T = 0.264018;
const double BLK_NESTLOOP_CACHE_COUNT_T = 54.348476;

//cost params for merge join
const double MERGE_ONCE_T = 0.005255;
//cost for scan left rows
//for simplicity, dont separate used from unused, fitting result is acceptable
const double MERGE_ROW_LEFT_T = 0.005015;
//cost for right rows.
//unlike NL, this splits into three type
//because right rows can come from "right cache"
const double MERGE_ROW_RIGHT_OP_T = 0.000001;
const double MERGE_ROW_RIGHT_CACHE_T = 0.000001;
const double MERGE_ROW_RIGHT_UNUSED_T = 0.006961; //very small, can ignore?
const double MARGE_ROW_QUAL_FAIL_T = 0.132598;
//there is a cache clear operation each match group
const double MERGE_MATCH_GROUP_T = 0.808002;
const double MERGE_ROW_ONCE_RES_T = 0.751524;



//cost params for merge group by
const double MGB_STARTUP_T = 177.0;
//for each input row, there are:
// cost for judging whether it starts a new group(Proportional to Ngroup_col)
// cost for calc each aggr column(Proportional to Naggr_col)
const double MGB_INPUT_ONCE_T = 1.5;
const double MGB_INPUT_AGGR_COL_T = 0.073;
const double MGB_INPUT_GROUP_COL_T = 0.011;
//for each output row(i.e. a group), there are
// cost for deep copy first input row twice(Proportional to Ninput_col)
// cost for calc and copy each aggr column(Proportional to Naggr_col)
//Here, we use AVG aggr func as the sampling target.
const double MGB_RES_ONCE_T = 1.68;
const double MGB_RES_AGGR_COL_T = 3.66;
const double MGB_RES_INPUT_COL_T = 0.018;

const int64_t MGB_DEFAULT_GROUP_COL_COUNT = 1;
const int64_t MGB_DEFAULT_INPUT_COL_COUNT = 10;

//cost param for distinct.Refering Merge group by
const double MERGE_DISTINCT_STARTUP_T = 50;
const double MERGE_DISTINCT_ONE_T = 1.5;
const double MERGE_DISTINCT_COL_T = 0.018;

//cost params for hash group by
const double HGB_STARTUP_T = 979;

const double HGB_INPUT_ONCE_T = 0.144;

const double HGB_INPUT_AGGR_COL_T = 0.366;

const double HGB_INPUT_GROUP_COL_T = 0.0824;

const double HGB_RES_ONCE_T = 0.857;

const double HGB_RES_INPUT_COL_T = 0.043;

const double HGB_RES_AGGR_COL_T = 2.500;

//cost param for virtual table
const double VIRTUAL_INDEX_GET_COST = 20; // typical get latency for hash table

const double SCALAR_STARTUP_T = 177.0;
const double SCALAR_INPUT_ONCE_T = 0.144;
const double SCALAR_INPUT_AGGR_COL_T = 0.073;

const int64_t HALF_OPEN_RANGE_MIN_ROWS = 20;
const int64_t CLOSE_RANGE_MIN_ROWS = 10;
} //namespace common
} //namespace oceanabse



#endif /* _OB_COST_CONSTS_DEF_H_ */
