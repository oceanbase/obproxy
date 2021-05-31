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

#ifndef TBSYS_CONFIG_H
#define TBSYS_CONFIG_H

#include <string>
#include <ext/hash_map>
#include "lib/file/stringutil.h"

//using namespace std;
//using namespace __gnu_cxx;

namespace obsys {
/** 
* @brief Generate the hash value of the string
*/
    struct str_hash {
        size_t operator()(const std::string& str) const {
            return __gnu_cxx::__stl_hash_string(str.c_str());
        }
    };
    /** 
     * @brief String comparison
     */
    struct char_equal {
        bool operator()(const char* s1, const char* s2) const {
            return strcmp(s1, s2) == 0;
        }
    };
    typedef __gnu_cxx::hash_map<std::string, std::string, str_hash> STR_STR_MAP;
    typedef STR_STR_MAP::iterator STR_STR_MAP_ITER;
    typedef __gnu_cxx::hash_map<std::string, STR_STR_MAP*, str_hash> STR_MAP;
    typedef STR_MAP::iterator STR_MAP_ITER;
    
    #define TBSYS_CONFIG obsys::CConfig::getCConfig()
    
    /** 
     * @brief Parse the configuration file and store the configuration items in the memory in the form of key-value
     */
    class           CConfig {
        public:
            CConfig();
            ~CConfig();
            
            // Load a file
            int load(const char *filename);
            // Load a buffer
            int loadContent(const char * content);
            // Take a string
            const char *getString(const char *section, const std::string& key, const char *d = NULL);
            // Take a list of strings
            std::vector<const char*> getStringList(const char *section, const std::string& key);
            // Take an integer
            int getInt(char const *section, const std::string& key, int d = 0);
            // Take an integer list
            std::vector<int> getIntList(const char *section, const std::string& key);
            // Take all the keys under a section
            int getSectionKey(const char *section, std::vector<std::string> &keys);
            // Get the names of all sections
            int getSectionName(std::vector<std::string> &sections);
            // Complete configuration file string
            std::string toString();
            // Get static instance
            static CConfig& getCConfig();
            
        private:
            // Two-layer map
            STR_MAP m_configMap;
            
        private:
            // Parse string
            int parseValue(char *str, char *key, char *val);
            int parseLine(STR_STR_MAP *&m, char *data);
            int getLine(char * buf, const int buf_len,
                const char * content, const int content_len, int & pos);
            char *isSectionName(char *str);     
    };
}

#endif
