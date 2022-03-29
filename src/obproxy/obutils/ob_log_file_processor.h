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

#ifndef OBPROXY_LOG_FILE_PROCESSOR_H
#define OBPROXY_LOG_FILE_PROCESSOR_H
#include "lib/container/ob_array.h"
#include "lib/compress/zstd_1_3_8/ob_zstd_compressor_1_3_8.h"
#include "utils/ob_layout.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObAsyncCommonTask;
}

struct ObProxyLogFileStruct
{
  ObProxyLogFileStruct() : size_(0), mtime_(0) { full_path_[0] = '\0'; }
  ~ObProxyLogFileStruct() { }

  bool operator<(const ObProxyLogFileStruct &st) const
  {
    return mtime_ < st.mtime_;
  }
  DECLARE_TO_STRING;

  int64_t size_;
  int64_t mtime_;
  char full_path_[common::OB_MAX_FILE_NAME_LENGTH];
};

class ObLogFileProcessor
{
public:
  ObLogFileProcessor();
  ~ObLogFileProcessor() { destroy(); }

  static int match_file_name(const char *layout_dir, const char *name,
                             event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator,
                             ObProxyLogFileStruct &file_st,
                             bool &need_further_handle);
  static int get_disk_size(const char *dir, int64_t &avail_size);
  static int do_repeat_task();
  static void update_interval();

  void destroy();
  int init();
  int start_cleanup_log_file();
  int set_log_cleanup_interval();


  //1.traverse all the log files in the dir, ignore files that are used now, put the rest into an ob_array
  //2.if in hot upgrade status, only put current pid log files into ob_array
  //3.cleanup files in the sorted ob_array
  int cleanup_log_file();
  obutils::ObAsyncCommonTask *get_cleanup_cont() { return cleanup_cont_; }

private:
  //if total_size_ is larger than threshhold, delete files in
  //sorted array until total_size_ is smaller than threshhold
  //if in hot upgrade status, log_size_threshhold is divided by both parent and child
  //process according to their proportions in total size
  int do_cleanup_log_file(common::ObArray<ObProxyLogFileStruct> &log_array,
                          int64_t &total_size);
  int do_cleanup_invalid_log_file(common::ObArray<ObProxyLogFileStruct> &log_array);
  int do_cleanup_compress_log_file(common::ObArray<ObProxyLogFileStruct> &log_array,
                                   common::ObArray<ObProxyLogFileStruct> &compress_log_array,
                                   int64_t &total_size);
  int log_compress_block(char *dest, size_t dest_size,
                         const char *src, size_t src_size,
                         size_t &return_size);
  int log_compress(ObProxyLogFileStruct &log_st, const common::ObString &file_name, const common::ObString &compression_file_name,
                   int64_t &total_size, char *src_buf, int src_size, char *dest_buf, int dest_size);

private:
  static const int64_t MAX_INVALID_PROXY_LOG_NUM = 10;

  bool is_inited_;
  common::zstd_1_3_8::ObZstdCompressor_1_3_8 zstd_compressor_1_3_8_;
  obutils::ObAsyncCommonTask *cleanup_cont_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFileProcessor);
};

ObLogFileProcessor &get_global_log_file_processor();
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_LOG_FILE_PROCESSOR_H */
