/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef TBSYS_TIMEUTIL_H_
#define TBSYS_TIMEUTIL_H_

#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

namespace obsys {

	/**
	 * @brief Simple encapsulation of linux time operation
	 */
class CTimeUtil {
public:
    /**
     * ms timestamp
     */
    static int64_t getTime();
    /**
     * get current time
     */
    static int64_t getMonotonicTime();
    /**
     * format int into 20080101101010
     */
    static char *timeToStr(time_t t, char *dest);
    /**
     * format string to time(local)
     */
  //static int strToTime(char *str);
};

}

#endif
