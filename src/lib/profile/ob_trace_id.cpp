/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "lib/atomic/ob_atomic.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_serialization_helper.h"
using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

uint64_t ObCurTraceId::SeqGenerator::seq_generator_ = 0;


} // end namespace common
} // end namespace oceanbase
