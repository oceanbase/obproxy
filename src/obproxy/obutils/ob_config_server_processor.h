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

#ifndef OBPROXY_CONFIG_SERVER_PROCESSOR_H
#define OBPROXY_CONFIG_SERVER_PROCESSOR_H

#include "lib/tbsys.h"
#include "lib/lock/ob_drw_lock.h"
#include "utils/ob_proxy_table_define.h"
#include "obutils/ob_proxy_json_config_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObClientSessionInfo;
}
namespace obutils
{
class ObAsyncCommonTask;
class ObProxyConfig;

typedef int64_t (* write_func)(void *ptr, int64_t size, int64_t nmemb, void *stream);

enum ObConfigServerHeaderVersion
{
  HEADER_VERSION_ORIGINAL = 1,
  HEADER_VERSION_ZLIB_COMPRESS,
};

class ObConfigServerProcessor
{
public:
  ObConfigServerProcessor();
  virtual ~ObConfigServerProcessor();

  //init config server processor, if with config
  //server,get json config info
  int init(const bool load_local_config_succ);
  bool is_init() { return is_inited_; }

  //get the newest rs list for the specified cluster,if succeed,
  //copy its web rs list to rs list, and then set the newest
  //web rs list to json config info
  int get_newest_cluster_rs_list(const common::ObString &cluster_name,
                                 const int64_t cluster_id,
                                 common::ObIArray<common::ObAddr> &rs_list,
                                 const bool need_update_dummy_entry = true);

  int refresh_idc_list(const common::ObString &cluster_name, const int64_t cluster_id, ObProxyIDCList &idc_list);


  //get the rs list for the specified cluster from member json config info,
  //if succeed, copy its web rs list to rs list
  int get_cluster_rs_list(const common::ObString &cluster_name,
                          const int64_t cluster_id,
                          common::ObIArray<common::ObAddr> &rs_list) const;
  bool has_slave_clusters(const common::ObString &cluster_name);
  int get_next_master_cluster_rslist(const common::ObString &cluster_name,
                                     common::ObIArray<common::ObAddr> &rs_list);
  int get_master_cluster_id(const common::ObString &cluster_name, int64_t &cluster_id) const;
  int set_master_cluster_id(const common::ObString &cluster_name, int64_t cluster_id);
  int get_rs_list_hash(const common::ObString &cluster_name, const int64_t cluster_id, uint64_t &rs_list_hash) const;
  int get_cluster_idc_region(const common::ObString &cluster_name, const int64_t cluster_id,
                             const common::ObString &idc_name, ObProxyNameString &region_name) const;

  int get_proxy_meta_table_info(ObProxyMetaTableInfo &table_info) const;
  int get_proxy_meta_table_username(ObProxyConfigString &username) const;
  int get_proxy_meta_table_login_info(ObProxyLoginInfo &info) const;

  int get_proxy_json_config_info(ObProxyJsonConfigInfo &json_info) const;
  int inc_and_get_create_failure_count(const common::ObString &cluster_name, const int64_t cluster_id, int64_t &new_failure_count);
  int get_create_failure_count(const common::ObString &cluster_name, const int64_t cluster_id, int64_t &new_failure_count);

  int64_t get_cluster_count() const;

  int get_cluster_url(const common::ObString &cluster_name, common::ObIAllocator &allocator, char *&buf, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID) const;
  int get_idc_url(const common::ObString &cluster_name, common::ObIAllocator &allocator, char *&buf, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID) const;
  static int get_idc_url(const char *rs_url_buf, const int64_t rs_url_buf_len,
                         char *&idc_url_buf, const int64_t idc_url_buf_len, const int64_t cluster_id = OB_DEFAULT_CLUSTER_ID);

  //if user hasn't specify a cluster to connect,
  //we will use the first cluster as the default cluster,
  //except the meta_db_cluster
  int get_default_cluster_name(char *buffer, const int64_t len) const;

  bool is_real_meta_cluster_exist() const;
  bool is_cluster_name_exists(const common::ObString &cluster_name) const;
  int get_cluster_info(const common::ObString &cluster_name,
                       const bool is_from_default, ObProxyConfigString &real_meta_cluster_name) const;

  //in test mode,rs list and port come from cmd line,set ip and port to
  //the only one cluster with default cluster name
  int set_default_rs_list(const common::ObString &cluster_name);

  //fetch new proxy bin for hot upgrade
  int do_fetch_proxy_bin(const char *bin_save_path, const char *binary_name);

  ObProxyKernelRelease get_kernel_release() const { return kernel_release_; }
  int check_kernel_release(const char *release_str) const;
  int check_kernel_release(const common::ObString &release_string) const;

  int start_refresh_task();
  //set config server refresh task interval, once updated in update config,
  //delete old task and start a new one with new refresh interval
  int set_refresh_interval();
  int set_refresh_ldg_interval();

  int delete_rslist(const common::ObString &cluster_name, const int64_t cluster_id);
  int update_rslist(const common::ObString &cluster_name, const int64_t cluster_id,
                    const LocationList &web_rs_list, const uint64_t cur_rs_list_hash);

  ObProxyJsonConfigInfo *acquire();
  void release(ObProxyJsonConfigInfo *json_info);
  void release(ObProxyLdgInfo *ldg_info);

  static int do_repeat_task();
  static int do_ldg_repeat_task();
  static void update_interval();
  static void update_ldg_interval();

  int refresh_config_server();
  ObAsyncCommonTask *get_refresh_cont() { return refresh_cont_; }
  ObAsyncCommonTask *get_refresh_ldg_cont() { return refresh_ldg_cont_; }
  int get_ldg_primary_role_instance(const ObString &tenant_name,
                                    const ObString &cluster_name,
                                    ObProxyObInstance* &instance);

  DECLARE_TO_STRING;

private:
  int swap(ObProxyJsonConfigInfo *json_info);
  int swap_with_rslist(ObProxyJsonConfigInfo *json_info, const bool is_metadb_changed);

  //get proxy _kernel_release, now we only support 5u,6u and 7u linux server
  int get_kernel_release_by_uname(ObProxyKernelRelease &version) const;
  int get_kernel_release_by_redhat(ObProxyKernelRelease &version) const;
  int get_kernel_release_by_glibc(ObProxyKernelRelease &version) const;

  int init_proxy_kernel_release();

  int convert_root_addr_to_addr(const LocationList &web_rs_list,
                                common::ObIArray<common::ObAddr> &rs_list) const;

  //1. fetch config string from config_server_url, if failed try to load config from local
  //2. convert config string to json format
  //3. parse config version to see if it is updated, if version is the same,
  //no need to parse anymore, else parse new config and copy it to memmber json_config_info
  int dump_json_config_to_local(char *json_info_buf, const int64_t buf_len, const int64_t data_len);
  int load_config_from_local();
  int dump_rslist_info_to_local();
  int load_rslist_info_from_local();
  int dump_idc_list_info_to_local();
  int load_idc_list_info_from_local();

  //add record header before write json config info and rslist to local file, so that
  //the two files  will be invalid if changed by mannual
  int add_serialized_file_header(char *buf, const int64_t buf_len,
                                 const int64_t compressed_data_len, const int64_t data_len);

  //refresh all  rslist
  int refresh_all_rslist();
  int refresh_all_idc_list();

  int refresh_json_config_info(const bool force_refresh = false);
  int get_json_config_info(const char *url, const bool version_only = false);
  int parse_json_config_info(const common::ObString &json_str, const bool version_only = false);
  virtual int do_fetch_json_info(const char *url, common::ObString &json);
  int handle_content_string(char *content, const int64_t content_length);
  int init_json(const common::ObString &json_str, json::Value *&json_root, common::ObArenaAllocator &allocator);

  int fetch_newest_cluster_rslist(const common::ObString &appname, const int64_t cluster_id,
                                  LocationList &web_rslist, const bool need_update_dummy_entry = true);
  int fetch_rs_list_from_url(const char *url, const common::ObString &appname, const int64_t cluster_id,
                             LocationList &web_rslist, const bool need_update_dummy_entry = true);
  int refresh_idc_list_from_url(const char *url, const common::ObString &cluster_name,
                                const int64_t cluster_id, ObProxyIDCList &idc_list);

  int build_proxy_bin_url(const char *binary_name, common::ObIAllocator &allocator, char *&bin_url);

  static int64_t write_data(void *ptr, int64_t size, int64_t nmemb, void *stream);
  static int64_t write_proxy_bin(void *ptr, int64_t size, int64_t nmemb, void *stream);
  int fetch_by_curl(const char *url, int64_t timeout, void *stream,
                    write_func write_func_callback = NULL);
  int refresh_ldg_config_info();
private:
  static const int64_t OBPROXY_MAX_JSON_INFO_SIZE = 64 * 1024; // 64K
  static const int16_t OB_PROXY_CONFIG_MAGIC = static_cast<int16_t>(0X4A43); // "JC",short for JsonConfig
  static const int64_t ITEM_STR_SIZE = 512;
  static const int64_t CURL_CONNECTION_TIMEOUT = 10;
  static const int64_t CURL_TRANSFER_TIMEOUT = 5;
  static const int64_t CURL_TRANSFER_TIMEOUT_LARGE = 120;

  static const ObConfigServerHeaderVersion HEADER_VERSION = HEADER_VERSION_ZLIB_COMPRESS;

  bool is_inited_;
  bool dump_config_res_;
  bool need_dump_rslist_res_;
  bool need_dump_idc_list_res_;

  ObAsyncCommonTask *refresh_cont_;
  ObAsyncCommonTask *refresh_ldg_cont_;

  ObProxyJsonConfigInfo *json_config_info_;
  mutable obsys::CRWLock json_info_lock_;
  obutils::ObProxyConfig &proxy_config_;
  ObProxyKernelRelease kernel_release_;
  ObProxyLdgInfo *ldg_info_;
  mutable common::DRWLock ldg_info_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObConfigServerProcessor);
};


ObConfigServerProcessor &get_global_config_server_processor();
}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_CONFIG_SERVER_PROCESSOR_H */
