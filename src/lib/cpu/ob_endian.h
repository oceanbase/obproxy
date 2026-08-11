/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_ENDIAN_H
#define OCEANBASE_COMMON_ENDIAN_H

#include <endian.h>
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define OB_LITTLE_ENDIAN
#elif (__BYTE_ORDER == __BIG_ENDIAN)
#define OB_BIG_ENDIAN
#endif

#endif
