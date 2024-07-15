#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#include "lib/ob_define.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
#include "obproxy/obkv/table/ob_table_rpc_request.h"
#include "obproxy/obkv/table/ob_table_rpc_response.h"
#include "ob_mysql_test_utils.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_rowkey.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_analyzer.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

class TestObkvRequestAnalyzer : public ::testing::Test
{
public:
};

TEST_F(TestObkvRequestAnalyzer, test_login)
{
  const char *packet_hex =  "01dbdbce000000cb000000010000"
                        "0000000011018005000700000000ea32"
                        "c8080000000000000001000000000000"
                        "00010000000000000000000000000000"
                        "0001d36d1c6487474092000000000098"
                        "96800005dc8088d2d788000000280000"
                        "00000000000000000000000000000000"
                        "00000000000000000000000000000000"
                        "0000ffffffffffffffff000000000000"
                        "000001490102010000000000056d7973"
                        "716c000561646d696e0014e602355abb"
                        "2ec966385056a5022f0bbf613feb9000"
                        "14793647777149333279546559735778"
                        "33665464680004746573740000";

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[300];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  ASSERT_EQ(meta.ez_header_.ez_payload_size_, 203);
  ASSERT_EQ(meta.ez_header_.chid_, 1);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  ASSERT_EQ(pos, 128);
  ASSERT_EQ(pcode, 4353); // LOGIN
  ASSERT_EQ(hlen, 128);
  ASSERT_EQ(priority, 5);
  ASSERT_EQ(flags, 7);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 300);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  pos = hlen + RPC_NET_HEADER_LENGTH;
  ObRpcTableLoginRequest request;
  // request.set_packet_meta(meta);
  request.analyze_request(packet_str, packet_str_len, pos);

  // LOG_INFO("all deserialize done", K(request));

  uint8_t auth_method = request.get_auth_method();
  uint8_t client_type = request.get_client_type();
  uint8_t client_version = request.get_client_version();
  uint32_t cap = request.get_client_capabilities();
  uint32_t max_packet_size = request.get_max_packet_size();
  ObString tenant_name = request.get_tenant_name();
  ObString user_name = request.get_user_name();
  ObString pass_secret = request.get_pass_secret();
  ObString pass_scramble = request.get_pass_scramble();
  ObString db_name = request.get_database_name();
  int64_t ttl_us = request.get_ttl_us();

  ASSERT_EQ(auth_method, 1);
  ASSERT_EQ(client_type, 2);
  ASSERT_EQ(client_version, 1);
  ASSERT_EQ(cap, 0);
  ASSERT_EQ(max_packet_size, 0);
  ASSERT_STREQ(tenant_name.ptr(), "mysql");
  ASSERT_STREQ(user_name.ptr(), "admin");
  ASSERT_STREQ(db_name.ptr(), "test");
  ASSERT_EQ(ttl_us, 0);
  ASSERT_EQ(pass_secret.length(), 20);
  ASSERT_EQ(pass_scramble.length(), 20);     // size always 20

  // test encode
  char encode_str[300] = {0};
  int64_t encode_buf_len = 300;
  int64_t encode_pos = 0;
  // int64_t meta_serialize_size = meta.get_serialize_size();
  ASSERT_EQ(OB_SUCCESS, request.encode(encode_str, encode_buf_len, encode_pos));

  // test rpc_req
  ObRpcReq rpc_req;
  rpc_req.set_request_buf(packet_str);
  rpc_req.set_request_buf_len(packet_str_len);
  ObProxyRpcReqAnalyzeCtx ctx;
  ObRpcReqAnalyzeNewStatus status = RPC_ANALYZE_NEW_CONT;
  ObProxyRpcReqAnalyzer::analyze_rpc_req(ctx, status, rpc_req);
  ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
  ASSERT_TRUE(obkv_info.is_auth());
  ASSERT_FALSE(obkv_info.is_batch());
  LOG_INFO("success", K(rpc_req), K(obkv_info));
  ASSERT_EQ(RPC_ANALYZE_NEW_DONE, status);
};

TEST_F(TestObkvRequestAnalyzer, test_login_response)
{
  const char *packet_hex =    "01dbdbce000000fc000000010000"   // .W..............
                          "0000000011019c008000000000005dba"   // ..............].
                          "4b750000000000000000000000000000"   // Ku..............
                          "00000000000000000000000000000000"   // ................
                          "00012b1222d5e66c4427000000000000"   // ..+."..lD'......
                          "0000000000000000000000000028df67"   // .............(.g
                          "2b4e0000001b000000180000008e0000"   // +N..............
                          "00000000000000000000000000000000"   // ................
                          "00000000000000000001000000000000"   // ................
                          "0000ffffffffffffffff000000020002"   // ................
                          "004d000000000005dcfddf672a3a0184"   // .M.........g*:..
                          "808080000001000001d08080800080e4"   // ................
                          "a7770000104f6365616e426173652032"   // .w...OceanBase 2
                          "2e322e37370024019e8080800001ea07"   // .2.77.$.........
                          "e987808080c0fa01e887808080c0fa01"   // ................
                          "0090bf98ea94cb92b5d40100ea07e987"   // ................
                          "808080c0fa01e887808080c0fa01";      // ..............

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[300];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  // LOG_INFO("after deserialize meta", K(meta));

  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  ASSERT_EQ(meta.ez_header_.ez_payload_size_, 252);
  ASSERT_EQ(meta.ez_header_.chid_, 1);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  pos = hlen + RPC_NET_HEADER_LENGTH;
  ASSERT_EQ(pos, 172);
  ASSERT_EQ(pcode, 4353); // LOGIN
  ASSERT_EQ(hlen, 156);
  ASSERT_EQ(priority, 0);
  ASSERT_EQ(flags, 32768);

  ObRpcResultCode result_code;
  // int64_t result_code_pos = pos;
  result_code.deserialize(packet_str, packet_str_len, pos);
  ASSERT_EQ(pos, 182);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 300);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  ObRpcTableLoginResponse response;
  response.set_packet_meta(meta);
  response.set_result_code(result_code);
  ret = response.analyze_response(packet_str, packet_str_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  // LOG_INFO("all deserialize done", K(response));
  int32_t server_cap = response.get_server_capabilities();
  const ObString &version = response.get_server_version();
  // const ObString &credential = response.get_credential();
  int64_t tenant_id = response.get_tenant_id();
  int64_t user_id = response.get_user_id();
  int64_t database_id = response.get_database_id();

  ASSERT_EQ(server_cap, 250212864);
  ASSERT_STREQ(version.ptr(), "OceanBase 2.2.77");
  ASSERT_EQ(tenant_id, 1002);
  ASSERT_EQ(user_id, 1101710651032553);
  ASSERT_EQ(database_id, 1101710651032552);

  // test rpc_req
  ObRpcReq rpc_req;
  rpc_req.set_request_buf(packet_str);
  rpc_req.set_request_buf_len(packet_str_len);
  ObProxyRpcReqAnalyzeCtx ctx;
  ObRpcReqAnalyzeNewStatus status = RPC_ANALYZE_NEW_CONT;
  ObProxyRpcReqAnalyzer::analyze_rpc_req(ctx, status, rpc_req);
  ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
  // ASSERT_TRUE(obkv_info->is_auth());
  ASSERT_FALSE(obkv_info.is_batch());
  ASSERT_TRUE(obkv_info.is_resp());
  ASSERT_TRUE(obkv_info.is_resp_completed());
  LOG_INFO("success", K(rpc_req), K(obkv_info));
  ASSERT_EQ(RPC_ANALYZE_NEW_DONE, status);

  // // test encode
  // char encode_str[300] = {0};
  // int64_t encode_buf_len = 300;
  // int64_t encode_pos = 0;
  // int64_t meta_serialize_size = meta.get_serialize_size();
  // ASSERT_EQ(OB_SUCCESS, response.encode(encode_str, encode_buf_len, encode_pos));
  // std::cout << "pos" << pos << std::endl;
  // std::cout << "encode_pos" << encode_pos << std::endl;
  // std::cout << "packet_str_len" << packet_str_len << std::endl;
  // std::cout << "encode_buf_len" << encode_buf_len << std::endl;
  // for(int i = 0; i < 300; ++i) {
  //   printf("%2hhX\t", encode_str[i]);
  // }
  // ASSERT_EQ(encode_pos - meta_serialize_size, packet_str_len - pos);
  // ASSERT_STREQ(encode_str + meta_serialize_size, packet_str + pos);
};

TEST_F(TestObkvRequestAnalyzer, test_table_operation)
{
  /**
   * @brief
   * table api code:
   *    client.insert("test_varchar_table", "test", new String[] { "c2" },
   *            new String[] { "bar" });
   */
  const char *packet_hex =    "01dbdbce000000ea000000020000"  //   ..............
                          "0000000011028005000700000000493f"  // ..............I?
                          "296d00000000000003ea000000000000"  // )m..............
                          "00010000000000000000000000000000"  // ................
                          "0002d36d1c648747409200000002540b"  // ...m.d.G@.....T.
                          "e4000005dc8088db4f80000000280000"  // ........O....(..
                          "00000000000000000000000000000000"  // ................
                          "00000000000000000000000000000000"  // ................
                          "0000ffffffffffffffff000000000000"  // ................
                          "0000016824019e8080800001ea07e987"  // ...h$...........
                          "808080c0fa01e887808080c0fa010090"  // ................
                          "bf98ea94cb92b5d4010012746573745f"  // ...........test_
                          "766172636861725f7461626c6500ffff"  // varchar_table...
                          "ffffffffffffff010000011c01011901"  // ................
                          "16002d0a047465737400010263320016"  // ..-..test...c2..
                          "002d0a036261720000000001";         //     .-..bar.....

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[300];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  // LOG_INFO("after deserialize meta", K(pos),
  //   "magic0", meta.ez_header_.magic_header_flag_[0],
  //   "magic1", meta.ez_header_.magic_header_flag_[1],
  //   "magic2", meta.ez_header_.magic_header_flag_[2],
  //   "magic3", meta.ez_header_.magic_header_flag_[3],
  //   "payload_size", meta.ez_header_.ez_payload_size_,
  //   "chid", meta.ez_header_.chid_,
  //   "reserved", meta.ez_header_.reserved_);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  ASSERT_EQ(meta.ez_header_.ez_payload_size_, 234);
  ASSERT_EQ(meta.ez_header_.chid_, 2);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  uint64_t checksum = meta.rpc_header_.checksum_;
  // uint64_t tenant_id = meta.rpc_header_.tenant_id_;
  uint64_t priv_tid = meta.rpc_header_.priv_tenant_id_;
  uint64_t session_id = meta.rpc_header_.session_id_;
  // uint64_t trace_id0 = meta.rpc_header_.trace_id_[0];
  // uint64_t trace_id1 = meta.rpc_header_.trace_id_[1];
  // uint64_t timeout = meta.rpc_header_.timeout_;
  // uint64_t timestamp = meta.rpc_header_.timestamp_;
  // LOG_INFO("after deserialize rpc_header", K(pcode), K(hlen), K(priority),
  //   K(flags), K(checksum), K(tenant_id), K(priv_tid), K(session_id),
  //   K(trace_id0), K(trace_id1), K(timeout), K(timestamp));

  pos = hlen + RPC_NET_HEADER_LENGTH;
  ASSERT_EQ(pos, 144);
  ASSERT_EQ(pcode, 4354); // TABLE API
  ASSERT_EQ(hlen, 128);
  ASSERT_EQ(priority, 5);
  ASSERT_EQ(checksum, 1228876141);
  ASSERT_EQ(flags, 7);
  ASSERT_EQ(priv_tid, 1);
  ASSERT_EQ(session_id, 0);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 300);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  ObRpcTableOperationRequest request;
  request.set_packet_meta(meta);
  request.analyze_request(packet_str, packet_str_len, pos);

  // LOG_INFO("all deserialize done", K(request));

  const ObString &credential = request.get_credential();
  const ObString &table_name = request.get_table_name();
  uint64_t table_id = request.get_table_id();
  uint64_t part_id = request.get_partition_id();
  ObTableEntityType entity_type = request.get_entity_type();
  const ObTableOperation &operation = request.get_table_operation();
  ObTableConsistencyLevel level = request.get_consistency_level();
  bool return_rowkey = request.get_return_rowkey();
  bool return_affected_entity = request.get_return_affected_entity();
  bool return_affected_rows = request.get_return_affected_rows();
  // ObBinlogRowImageType image_type = request.get_image_type();

  // LOG_INFO("get request", K(operation), "credential_size", credential.length());

  ASSERT_EQ(credential.length(), 36);
  ASSERT_STREQ(table_name.ptr(), "test_varchar_table");
  ASSERT_EQ(table_id, 0xffffffffffffffff);
  ASSERT_EQ(part_id, 0);
  ASSERT_EQ(static_cast<int>(entity_type), 0);
  ASSERT_EQ(static_cast<int>(level), 0);
  ASSERT_EQ(return_rowkey, false);
  ASSERT_EQ(return_affected_entity, false);
  ASSERT_EQ(return_affected_rows, true);

  ObTableOperationType::Type type = operation.type();
  ASSERT_EQ(type, ObTableOperationType::INSERT);

  const ObITableEntity &entity = operation.entity();
  int64_t rowkey_cnt = entity.get_rowkey_size();
  ASSERT_EQ(rowkey_cnt, 1);

  ObObj obj;
  ret = entity.get_rowkey_value(0, obj);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj.get_type(), ObVarcharType);
  ASSERT_STREQ(obj.get_string_ptr(), "test");

  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  ret = entity.get_properties(properties);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
    ASSERT_EQ(0, properties.count());
  } else {
    ASSERT_EQ(1, properties.count());
    const std::pair<ObString, ObObj> &kv_pair = properties.at(0);
    ASSERT_STREQ("c2", kv_pair.first.ptr());
    ASSERT_EQ(kv_pair.second.get_type(), ObVarcharType);
    ASSERT_STREQ(kv_pair.second.get_string_ptr(), "bar");
  }

  // // test encode
  // char encode_str[400] = {0};
  // int64_t encode_buf_len = 400;
  // int64_t encode_pos = 0;
  // int64_t meta_serialize_size = meta.get_serialize_size();
  // ASSERT_EQ(OB_SUCCESS, request.encode(encode_str, encode_buf_len, encode_pos));
  // encode_str[encode_pos--] = '\0';   // 最后一个为新加字段
  // // std::cout << "pos " << pos << std::endl;
  // // std::cout << "meta_size " << meta_serialize_size << std::endl;
  // // std::cout << "packet_str_len " << packet_str_len << std::endl;
  // // std::cout << "encode_pos " << encode_pos << std::endl;
  // for (int i = 0; i < encode_pos; ++i) {
  //   printf("%02hhX\t", encode_str[i]);
  // }
  // for(int64_t i = meta_serialize_size, j = pos; i < encode_pos && j < 400; ++i, ++j) {
  //   if (encode_str[i] != packet_str[j]) {
  //     printf("i:%ld,j:%ld,%02hhX,%02hhX\n", i, j, encode_str[i], packet_str[j]);
  //   }
  // }
  // ASSERT_EQ(encode_pos - meta_serialize_size, packet_str_len - pos);
  // ASSERT_STREQ(encode_str + meta_serialize_size, packet_str + pos);

  ObRpcReq rpc_req;
  rpc_req.set_request_buf(packet_str);
  rpc_req.set_request_buf_len(packet_str_len);
  ObProxyRpcReqAnalyzeCtx ctx;
  ObRpcReqAnalyzeNewStatus status = RPC_ANALYZE_NEW_CONT;
  ObProxyRpcReqAnalyzer::analyze_rpc_req(ctx, status, rpc_req);
  ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
  ASSERT_FALSE(obkv_info.is_auth());
  ASSERT_FALSE(obkv_info.is_batch());
  ASSERT_FALSE(obkv_info.is_resp());
  ASSERT_FALSE(obkv_info.is_resp_completed());
  LOG_INFO("success", K(rpc_req), K(obkv_info));
  ASSERT_EQ(RPC_ANALYZE_NEW_DONE, status);
};

TEST_F(TestObkvRequestAnalyzer, test_operation_response)
{
  const char *packet_hex =    "01dbdbce000000c1000000020000"   //.:..............
                          "0000000011029c00800000000000102d"   //...............-
                          "b9c00000000000000000000000000000"   //................
                          "000000000000000000000000497e6458"   //............I~dX
                          "6d820005d1620ddf5a42000000000000"   //m....b..ZB......
                          "0000000000000000000000000028df6a"   //.............(.j
                          "ebe6000000170000000f000000000000"   //................
                          "00000000000000000000000000000000"   //................
                          "00000000000000000001000000000000"   //................
                          "0000ffffffffffffffff000000000000"   //................
                          "0000000000000005dcfddf6aea6b0184"   //...........j.k..
                          "80808000000100000195808080000185"   //................
                          "80808000000100010001018280808000"   //................
                          "000001";                            //...

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[300];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  // LOG_INFO("after deserialize meta", K(meta));

  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  ASSERT_EQ(meta.ez_header_.ez_payload_size_, 193);
  ASSERT_EQ(meta.ez_header_.chid_, 2);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  pos = hlen + RPC_NET_HEADER_LENGTH;
  ASSERT_EQ(pos, 172);
  ASSERT_EQ(pcode, 4354); // TABLE API
  ASSERT_EQ(hlen, 156);
  ASSERT_EQ(priority, 0);
  ASSERT_EQ(flags, 32768);

  ObRpcResultCode result_code;
  // int64_t result_code_pos = pos;
  result_code.deserialize(packet_str, packet_str_len, pos);
  ASSERT_EQ(pos, 182);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 300);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  ObRpcTableOperationResponse response;
  response.set_packet_meta(meta);
  response.set_result_code(result_code);
  ret = response.analyze_response(packet_str, packet_str_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  // LOG_INFO("all deserialize done", K(response));
  ObTableOperationType::Type type = response.get_type();
  int64_t affected_rows = response.get_affected_rows();
  ASSERT_EQ(type , ObTableOperationType::INSERT);
  ASSERT_EQ(affected_rows, 1);

  ObRpcReq rpc_req;
  rpc_req.set_request_buf(packet_str);
  rpc_req.set_request_buf_len(packet_str_len);
  ObProxyRpcReqAnalyzeCtx ctx;
  ObRpcReqAnalyzeNewStatus status = RPC_ANALYZE_NEW_CONT;
  ObProxyRpcReqAnalyzer::analyze_rpc_req(ctx, status, rpc_req);
  ObRpcOBKVInfo &obkv_info = rpc_req.get_obkv_info();
  ASSERT_FALSE(obkv_info.is_auth());
  ASSERT_FALSE(obkv_info.is_batch());
  ASSERT_TRUE(obkv_info.is_resp());
  ASSERT_TRUE(obkv_info.is_resp_completed());
  LOG_INFO("success", K(rpc_req), K(obkv_info));
  ASSERT_EQ(RPC_ANALYZE_NEW_DONE, status);
};

TEST_F(TestObkvRequestAnalyzer, test_batch_operation)
{
  /**
   * @brief
   * table api code:
   *    TableBatchOps batchOps = client.batch("test_varchar_table");
   *    batchOps.insert("test", new String[] { "c2" },
   *            new String[] { "bar" });
   *    batchOps.get("test", new String[] { "c2" });
   *    batchOps.delete("test");
   */
  const char *packet_hex =    "01dbdbce0000011c000000050000" //   ..............
                          "0000000011038005000700000000540a" // ..............T.
                          "9b6700000000000003ea000000000000" // .g..............
                          "00010000000000000000000000000000" // ................
                          "0005d36d1c6487474092000000000098" // ...m.d.G@.......
                          "96800005dc8088dc2658000000280000" // ........&X...(..
                          "00000000000000000000000000000000" // ................
                          "00000000000000000000000000000000" // ................
                          "0000ffffffffffffffff000000000000" // ................
                          "000001990124019e8080800001ea07e9" // .....$..........
                          "87808080c0fa01e887808080c0fa0100" // ................
                          "90bf98ea94cb92b5d401001274657374" // ............test
                          "5f766172636861725f7461626c6500ff" // _varchar_table..
                          "ffffffffffffffff0100014c03011c01" // ...........L....
                          "01190116002d0a047465737400010263" // .....-..test...c
                          "320016002d0a03626172000117000114" // 2...-..bar......
                          "0116002d0a0474657374000102633200" // ...-..test...c2.
                          "00063f0a010f02010c0116002d0a0474" // ..?.........-..t
                          "6573740000000000000000010000";    // est...........

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[400];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  // LOG_INFO("after deserialize meta", K(pos),
  //   "magic0", meta.ez_header_.magic_header_flag_[0],
  //   "magic1", meta.ez_header_.magic_header_flag_[1],
  //   "magic2", meta.ez_header_.magic_header_flag_[2],
  //   "magic3", meta.ez_header_.magic_header_flag_[3],
  //   "payload_size", meta.ez_header_.ez_payload_size_,
  //   "chid", meta.ez_header_.chid_,
  //   "reserved", meta.ez_header_.reserved_);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  // ASSERT_EQ(meta.ez_header_.ez_payload_size_, 234);
  ASSERT_EQ(meta.ez_header_.chid_, 5);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  uint64_t checksum = meta.rpc_header_.checksum_;
  // uint64_t tenant_id = meta.rpc_header_.tenant_id_;
  uint64_t priv_tid = meta.rpc_header_.priv_tenant_id_;
  uint64_t session_id = meta.rpc_header_.session_id_;
  // uint64_t trace_id0 = meta.rpc_header_.trace_id_[0];
  // uint64_t trace_id1 = meta.rpc_header_.trace_id_[1];
  // uint64_t timeout = meta.rpc_header_.timeout_;
  // uint64_t timestamp = meta.rpc_header_.timestamp_;
  // LOG_INFO("after deserialize rpc_header", K(pcode), K(hlen), K(priority),
  //   K(flags), K(checksum), K(tenant_id), K(priv_tid), K(session_id),
  //   K(trace_id0), K(trace_id1), K(timeout), K(timestamp));
  pos = hlen + RPC_NET_HEADER_LENGTH;
  // ASSERT_EQ(pos, 144);
  ASSERT_EQ(pcode, 4355); // TABLE API
  ASSERT_EQ(hlen, 128);
  ASSERT_EQ(priority, 5);
  ASSERT_EQ(checksum, 1409981287);
  ASSERT_EQ(flags, 7);
  ASSERT_EQ(priv_tid, 1);
  ASSERT_EQ(session_id, 0);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 400);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  ObRpcTableBatchOperationRequest request;
  request.set_packet_meta(meta);
  request.analyze_request(packet_str, packet_str_len, pos);

  // LOG_INFO("all deserialize done", K(request));

  const ObString &credential = request.get_credential();
  const ObString &table_name = request.get_table_name();
  uint64_t table_id = request.get_table_id();
  uint64_t part_id = request.get_partition_id();
  ObTableEntityType entity_type = request.get_entity_type();
  const ObTableBatchOperation &batch_operation = request.get_table_operation();
  ObTableConsistencyLevel level = request.get_consistency_level();
  bool return_rowkey = request.get_return_rowkey();
  bool return_affected_entity = request.get_return_affected_entity();
  bool return_affected_rows = request.get_return_affected_rows();
  // ObBinlogRowImageType image_type = request.get_image_type();

  // LOG_INFO("get request", K(operation), "credential_size", credential.length());

  ASSERT_EQ(credential.length(), 36);
  ASSERT_STREQ(table_name.ptr(), "test_varchar_table");
  ASSERT_EQ(table_id, 0xffffffffffffffff);
  ASSERT_EQ(part_id, 0);
  ASSERT_EQ(static_cast<int>(entity_type), 0);
  ASSERT_EQ(static_cast<int>(level), 0);
  ASSERT_EQ(return_rowkey, false);
  ASSERT_EQ(return_affected_entity, false);
  ASSERT_EQ(return_affected_rows, true);

  bool is_readonly = batch_operation.is_readonly();
  bool is_same_type = batch_operation.is_same_type();
  bool is_same_properites = batch_operation.is_same_properties_names();
  int64_t operation_count = batch_operation.count();

  ASSERT_EQ(is_readonly, false);
  ASSERT_EQ(is_same_type, false);
  ASSERT_EQ(is_same_properites, false);
  ASSERT_EQ(operation_count, 3);

  {
    // first is insert
    const ObTableOperation &operation = batch_operation.at(0);

    ObTableOperationType::Type type = operation.type();
    ASSERT_EQ(type, ObTableOperationType::INSERT);

    const ObITableEntity &entity = operation.entity();
    int64_t rowkey_cnt = entity.get_rowkey_size();
    ASSERT_EQ(rowkey_cnt, 1);

    ObObj obj;
    ret = entity.get_rowkey_value(0, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(obj.get_type(), ObVarcharType);
    ASSERT_STREQ(obj.get_string_ptr(), "test");

    ObSEArray<std::pair<ObString, ObObj>, 8> properties;
    ret = entity.get_properties(properties);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
      ASSERT_EQ(0, properties.count());
    } else {
      ASSERT_EQ(1, properties.count());
      const std::pair<ObString, ObObj> &kv_pair = properties.at(0);
      ASSERT_STREQ("c2", kv_pair.first.ptr());
      ASSERT_EQ(kv_pair.second.get_type(), ObVarcharType);
      ASSERT_STREQ(kv_pair.second.get_string_ptr(), "bar");
    }
  }
  {
    // second is get
    const ObTableOperation &operation = batch_operation.at(1);

    ObTableOperationType::Type type = operation.type();
    ASSERT_EQ(type, ObTableOperationType::GET);

    const ObITableEntity &entity = operation.entity();
    int64_t rowkey_cnt = entity.get_rowkey_size();
    ASSERT_EQ(rowkey_cnt, 1);

    ObObj obj;
    ret = entity.get_rowkey_value(0, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(obj.get_type(), ObVarcharType);
    ASSERT_STREQ(obj.get_string_ptr(), "test");

    ObSEArray<std::pair<ObString, ObObj>, 8> properties;
    ret = entity.get_properties(properties);
    if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
      ASSERT_EQ(0, properties.count());
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(1, properties.count());
      const std::pair<ObString, ObObj> &kv_pair = properties.at(0);
      ASSERT_STREQ("c2", kv_pair.first.ptr());
      ASSERT_EQ(kv_pair.second.get_type(), ObNullType);
    }
  }
  {
    // first is insert
    const ObTableOperation &operation = batch_operation.at(2);

    ObTableOperationType::Type type = operation.type();
    ASSERT_EQ(type, ObTableOperationType::DEL);

    const ObITableEntity &entity = operation.entity();
    int64_t rowkey_cnt = entity.get_rowkey_size();
    ASSERT_EQ(rowkey_cnt, 1);

    ObObj obj;
    ret = entity.get_rowkey_value(0, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(obj.get_type(), ObVarcharType);
    ASSERT_STREQ(obj.get_string_ptr(), "test");

    ObSEArray<std::pair<ObString, ObObj>, 8> properties;
    ret = entity.get_properties(properties);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(0, properties.count());
  }
  // test encode
  char encode_str[500] = {0};
  int64_t encode_buf_len = 500;
  int64_t encode_pos = 0;
  // int64_t meta_serialize_size = meta.get_serialize_size();
  ASSERT_EQ(OB_SUCCESS, request.encode(encode_str, encode_buf_len, encode_pos));
  // std::cout << "pos " << pos << std::endl;
  // std::cout << "meta_size " << meta_serialize_size << std::endl;
  // std::cout << "packet_str_len " << packet_str_len << std::endl;
  // std::cout << "encode_pos " << encode_pos << std::endl;
  // ASSERT_EQ(encode_pos - meta_serialize_size, packet_str_len - pos);
  // ASSERT_STREQ(encode_str + meta_serialize_size, packet_str + pos);
};

TEST_F(TestObkvRequestAnalyzer, batch_operation_response)
{
  const char *packet_hex =    "01dbdbce0000010b000000050000"   //.h..............
                          "0000000011039c008000000000002882"   //..............(.
                          "388a0000000000000000000000000000"   //8...............
                          "000000000000000000000000497e6458"   //............I~dX
                          "6d820005d161df5db898000000000000"   //m....a.]........
                          "0000000000000000000000000028df6b"   //.............(.k
                          "b0be0000001800000012000000000000"   //................
                          "00000000000000000000000000000000"   //................
                          "00000000000000000001000000000000"   //................
                          "0000ffffffffffffffff000000000000"   //................
                          "0000000000000005dcfddf6baea20184"   //...........k....
                          "808080000001000001df808080000301"   //................
                          "95808080000185808080000001000100"   //................
                          "0101828080800000000101a280808000"   //................
                          "018580808000000100010000018f8080"   //................
                          "800000010263320016022dff03626172"   //.....c2...-..bar
                          "00000195808080000185808080000001"   //................
                          "00010002018280808000000001";        //.............

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[400];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  // LOG_INFO("after deserialize meta", K(meta));

  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  ASSERT_EQ(meta.ez_header_.ez_payload_size_, 267);
  ASSERT_EQ(meta.ez_header_.chid_, 5);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  pos = hlen + RPC_NET_HEADER_LENGTH;
  ASSERT_EQ(pos, 172);
  ASSERT_EQ(pcode, 4355); // TABLE API
  ASSERT_EQ(hlen, 156);
  ASSERT_EQ(priority, 0);
  ASSERT_EQ(flags, 32768);

  ObRpcResultCode result_code;
  // int64_t result_code_pos = pos;
  result_code.deserialize(packet_str, packet_str_len, pos);
  ASSERT_EQ(pos, 182);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 400);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  ObRpcTableBatchOperationResponse response;
  response.set_packet_meta(meta);
  response.set_result_code(result_code);
  ret = response.analyze_response(packet_str, packet_str_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  // LOG_INFO("all deserialize done", K(response));

  const ObTableBatchOperationResult &batch_result = response.get_batch_result();
  {
    const ObTableOperationResult &result = batch_result.at(0);
    ObTableOperationType::Type type = result.type();
    int64_t affected_rows = result.get_affected_rows();
    const ObITableEntity *entity;
    ret = result.get_entity(entity);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(type , ObTableOperationType::INSERT);
    ASSERT_EQ(affected_rows, 1);
    ASSERT_EQ(0, entity->get_rowkey_size());
    ASSERT_EQ(0, entity->get_properties_count());
  }
  {
    const ObTableOperationResult &result = batch_result.at(1);
    ObTableOperationType::Type type = result.type();
    int64_t affected_rows = result.get_affected_rows();
    const ObITableEntity *entity;
    ret = result.get_entity(entity);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(type , ObTableOperationType::GET);
    ASSERT_EQ(affected_rows, 0);
    ASSERT_EQ(0, entity->get_rowkey_size());
    // ASSERT_EQ(1, entity->get_properties_count());
    ObSEArray<std::pair<ObString, ObObj>, 8> properties;
    ret = entity->get_properties(properties);
    if (OBKV_ENTITY_MODE == EntityMode::LAZY_MODE) {
      ASSERT_EQ(0, properties.count());
    } else {
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_EQ(properties.count(), 1);
      ASSERT_STREQ(properties.at(0).first.ptr(), "c2");
      ASSERT_EQ(properties.at(0).second.get_type(), ObVarcharType);
      ASSERT_STREQ(properties.at(0).second.get_string_ptr(), "bar");
    }
  }
  {
    const ObTableOperationResult &result = batch_result.at(2);
    ObTableOperationType::Type type = result.type();
    int64_t affected_rows = result.get_affected_rows();
    const ObITableEntity *entity;
    ret = result.get_entity(entity);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(type , ObTableOperationType::DEL);
    ASSERT_EQ(affected_rows, 1);
    ASSERT_EQ(0, entity->get_rowkey_size());
    ASSERT_EQ(0, entity->get_properties_count());
  }
};

TEST_F(TestObkvRequestAnalyzer, test_query)
{
  /**
   * @brief
   * table api code:
   *    TableQuery tableQuery = obTableClient.query("testRange");
   *    tableQuery.addScanRange(new Object[] { "ah", "partition".getBytes(), timeStamp },
   *        new Object[] { "az", "partition".getBytes(), timeStamp });
   *    tableQuery.select("K", "Q", "T", "V");
   *    QueryResultSet result = tableQuery.execute();
   */

  const char *packet_hex =  "01dbdbce0000013a000000070000"   //   .......:......
                        "000000001104800500070000000094b6"   // ................
                        "52bc000000000000044b000000000000"   // R........K......
                        "00010000000000000000000000000000"   // ................
                        "0005ff176f4897a94c57000000000098"   // ....oH..LW......
                        "96800005dce9b57bb7a0000000280000"   // .......{.....(..
                        "00000000000000000000000000000000"   // ................
                        "00000000000000000000000000000000"   // ................
                        "0000ffffffffffffffff000000000000"   // ................
                        "000001b7012201208f87ae29cb08eb87"   // .....". ...)....
                        "808080e092029b88808080e0920200e7"   // ................
                        "c699eb98aa91bf1f0009746573745261"   // ..........testRa
                        "6e676500ffffffffffffffffff010100"   // nge.............
                        "00017901ffffffffffffffffff010303"   // ..y.............
                        "16002d0a0261680016002d0a09706172"   // ..-..ah...-..par
                        "746974696f6e0005053f0ad8aed1de83"   // tition...?......
                        "300316002d0a02787a0016002d0a0970"   // 0...-..xz...-..p
                        "6172746974696f6e0005053f0ad8aed1"   // artition...?....
                        "de833004014b00015100015400015600"   // ..0..K..Q..T..V.
                        "0000ffffffff0f00010000ffffffff0f"   // ................
                        "ffffffffffffffffff010100";          // ............

  int64_t packet_hex_len = strlen(packet_hex);
  int64_t packet_str_len = packet_hex_len / 2;
  char packet_str[400];
  int ret = OB_SUCCESS;
  ret = oceanbase::obproxy::ObMysqlTestUtils::covert_hex_to_string(packet_hex, packet_hex_len, packet_str);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  ObRpcPacketMeta meta;
  meta.deserialize(packet_str, packet_str_len, pos);

  // LOG_INFO("after deserialize meta", K(pos),
  //   "magic0", meta.ez_header_.magic_header_flag_[0],
  //   "magic1", meta.ez_header_.magic_header_flag_[1],
  //   "magic2", meta.ez_header_.magic_header_flag_[2],
  //   "magic3", meta.ez_header_.magic_header_flag_[3],
  //   "payload_size", meta.ez_header_.ez_payload_size_,
  //   "chid", meta.ez_header_.chid_,
  //   "reserved", meta.ez_header_.reserved_);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[0], meta.ez_header_.MAGIC_HEADER_FLAG[0]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[1], meta.ez_header_.MAGIC_HEADER_FLAG[1]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[2], meta.ez_header_.MAGIC_HEADER_FLAG[2]);
  ASSERT_EQ(meta.ez_header_.magic_header_flag_[3], meta.ez_header_.MAGIC_HEADER_FLAG[3]);
  ASSERT_EQ(meta.ez_header_.ez_payload_size_, 314);
  ASSERT_EQ(meta.ez_header_.chid_, 7);
  ASSERT_EQ(meta.ez_header_.reserved_, 0);

  uint32_t pcode = meta.rpc_header_.pcode_;
  uint8_t  hlen = meta.rpc_header_.hlen_;
  uint8_t  priority = meta.rpc_header_.priority_;
  uint16_t flags = meta.rpc_header_.flags_;
  uint64_t checksum = meta.rpc_header_.checksum_;
  // uint64_t tenant_id = meta.rpc_header_.tenant_id_;
  uint64_t priv_tid = meta.rpc_header_.priv_tenant_id_;
  uint64_t session_id = meta.rpc_header_.session_id_;
  // uint64_t trace_id0 = meta.rpc_header_.trace_id_[0];
  // uint64_t trace_id1 = meta.rpc_header_.trace_id_[1];
  // uint64_t timeout = meta.rpc_header_.timeout_;
  // uint64_t timestamp = meta.rpc_header_.timestamp_;
  // LOG_INFO("after deserialize rpc_header", K(pcode), K(hlen), K(priority),
  //   K(flags), K(checksum), K(priv_tid), K(session_id));

  pos = hlen + RPC_NET_HEADER_LENGTH;
  ASSERT_EQ(pos, 144);
  ASSERT_EQ(pcode, 4356);
  ASSERT_EQ(hlen, 128);
  ASSERT_EQ(priority, 5);
  ASSERT_EQ(checksum, 2494976700);
  ASSERT_EQ(flags, 7);
  ASSERT_EQ(priv_tid, 1);
  ASSERT_EQ(session_id, 0);

  // ObMIOBuffer client_read_buffer(packet_str + pos, packet_str_len - pos, 400);
  // ObIOBufferReader *client_buffer_reader = client_read_buffer.alloc_reader();

  ObRpcTableQueryRequest request;
  request.set_packet_meta(meta);
  request.analyze_request(packet_str, packet_str_len, pos);

  // LOG_INFO("all deserialize done", K(request));

  const ObString &credential = request.get_credential();
  const ObString &table_name = request.get_table_name();
  uint64_t table_id = request.get_table_id();
  uint64_t part_id = request.get_partition_id();
  ObTableEntityType entity_type = request.get_entity_type();
  const ObTableQuery &query = request.get_query();
  ObTableConsistencyLevel level = request.get_consistency_level();
  // ObBinlogRowImageType image_type = request.get_image_type();

  // LOG_INFO("get request", K(operation), "credential_size", credential.length());

  ASSERT_EQ(credential.length(), 34);
  ASSERT_STREQ(table_name.ptr(), "testRange");
  ASSERT_EQ(table_id, 0xffffffffffffffff);
  ASSERT_EQ(part_id, 1);
  ASSERT_EQ(static_cast<int>(entity_type), 0);
  ASSERT_EQ(static_cast<int>(level), 0);


  int64_t range_count = query.get_range_count();
  ASSERT_EQ(range_count, 1);
  const ObIArray<common::ObNewRange> &range_array = query.get_scan_ranges();
  const ObNewRange &range = range_array.at(0);
  const ObRowkey &start_key = range.get_start_key();
  const ObRowkey &end_key = range.get_end_key();
  int64_t start_key_count = start_key.get_obj_cnt();
  int64_t end_key_count = end_key.get_obj_cnt();
  ASSERT_EQ(start_key_count, 3);
  ASSERT_EQ(end_key_count, 3);
  const ObObj *start_obobj = start_key.get_obj_ptr();
  const ObObj *end_obobj = end_key.get_obj_ptr();

  ASSERT_EQ(0, strncmp(start_obobj[0].get_string_ptr(), "ah", 2));
  ASSERT_EQ(start_obobj[0].get_type(), ObVarcharType);
  ASSERT_EQ(0, strncmp(start_obobj[1].get_string_ptr(), "partition", 9));
  ASSERT_EQ(start_obobj[1].get_type(), ObVarcharType);
  ASSERT_EQ(start_obobj[2].get_int(), 1650271213400);
  ASSERT_EQ(start_obobj[2].get_type(), ObIntType);

  ASSERT_EQ(0, strncmp(end_obobj[0].get_string_ptr(), "xz", 2));
  ASSERT_EQ(end_obobj[0].get_type(), ObVarcharType);
  ASSERT_EQ(0, strncmp(end_obobj[1].get_string_ptr(), "partition", 9));
  ASSERT_EQ(end_obobj[1].get_type(), ObVarcharType);
  ASSERT_EQ(end_obobj[2].get_int(), 1650271213400);
  ASSERT_EQ(end_obobj[2].get_type(), ObIntType);

  const ObIArray<ObString> &select_array = query.get_select_columns();
  ASSERT_EQ(select_array.count(), 4);
  ASSERT_STREQ(select_array.at(0).ptr(), "K");
  ASSERT_STREQ(select_array.at(1).ptr(), "Q");
  ASSERT_STREQ(select_array.at(2).ptr(), "T");
  ASSERT_STREQ(select_array.at(3).ptr(), "V");

  // filter当前未实现，都是空

  int32_t limit = query.get_limit();
  int32_t offset = query.get_offset();
  common::ObQueryFlag::ScanOrder order = query.get_scan_order();
  const ObString &index_name = query.get_index_name();

  ASSERT_EQ(limit, -1);
  ASSERT_EQ(offset, 0);
  ASSERT_EQ(static_cast<int>(order), 1);
  ASSERT_STREQ(index_name.ptr(), "");

  // htable_filter当前未实现，table client传过来的都是空，也就是只有头部api_version = 1, payload_size = 0



};


} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}