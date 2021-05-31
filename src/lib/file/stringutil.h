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

#ifndef TBSYS_STRINGUTIL_H
#define TBSYS_STRINGUTIL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <time.h>
#include <vector>

//using namespace std;
 
namespace obsys {

	/** 
	 * @brief String manipulation, and the encapsulation of conversion
	 */
    class CStringUtil {
        public:
            // Convert string to int
            static int strToInt(const char *str, int d);
            // Is an integer
            static int isInt(const char *str);
            // Convert to lowercase
            static char *strToLower(char *str);
            // Convert to uppercase
            static char *strToUpper(char *str);
            // trim
            static char *trim(char *str, const char *what = " ", int mode = 3);
            // hash_value
            static int hashCode(const char *str);
            // Get the prime number of the hash value of a str
            static int getPrimeHash(const char *str);
            // Separate the string with delim and put it in the list
            static void split(char *str, const char *delim, std::vector<char*> &list);
            // urldecode
            static char *urlDecode(const char *src, char *dest);
            // http://murmurhash.googlepages.com/
            static unsigned int murMurHash(const void *key, int len);
            // Convert bytes into readable, such as 10K 12M, etc.
            static std::string formatByteSize(double bytes);
    };   
}

#endif
///////////////////END
