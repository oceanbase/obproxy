#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "lib/list/ob_intrusive_list.h"
#include "lib/utility/utility.h"

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

class TestObRpcRequestList : public ::testing::Test
{
public:
  TestObRpcRequestList():allocator_() {}
  virtual void SetUp();
  virtual void TearDown();
  int prepare_rpc_req_queue(ObRpcReqList &list, ObRpcReq *&req, const int64_t count);
private:
  ObArenaAllocator allocator_;
};

void TestObRpcRequestList::SetUp()
{

}

void TestObRpcRequestList::TearDown()
{
  allocator_.reuse();
}

int TestObRpcRequestList::prepare_rpc_req_queue(ObRpcReqList &list, ObRpcReq *&req_arr, int64_t count)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  int64_t req_list_size = static_cast<int64_t>(sizeof(ObRpcReq)) * count;
  if (OB_ISNULL(ptr = allocator_.alloc(req_list_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (FALSE_IT(req_arr = static_cast<ObRpcReq*>(ptr))) {
  } else {
    for (int i = 0; i < count; i++) {
      ObRpcReq *req = new (&req_arr[i]) ObRpcReq();
      ret = list.push_back(req);
    }
  }
  return ret;
}

TEST_F(TestObRpcRequestList, test_list)
{
  int ret = OB_SUCCESS;
  {
    ObRpcReqList list;
    ObRpcReq req;
    ret = list.push_back(&req);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(list.size(), 1);
    LOG_INFO("list:", K(list));
  }

  {
    const int TEST_COUNT = 10;
    ObRpcReqList list;
    ObRpcReq *req = NULL;
    ret = prepare_rpc_req_queue(list , req ,TEST_COUNT);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(list.size(), TEST_COUNT);
    LOG_INFO("list:", K(list));
    ObRpcReqList::iterator iter = list.begin();
    ObRpcReqList::iterator end = list.end();
    for (int i = 0; i < TEST_COUNT && iter != end; iter++, i++) {
      ASSERT_EQ(*iter, &req[i]);
      LOG_INFO("test", K(*iter), K(&req[i]));
    }
    while (!list.empty()) {
      ObRpcReq *tmp;
      ret = list.pop_front(tmp);
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    ASSERT_EQ(list.size(), 0);
  }

  {
    const int TEST_COUNT = 10;
    ObRpcReqList list;
    ObRpcReq *req = NULL;
    ret = prepare_rpc_req_queue(list , req ,TEST_COUNT);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(list.size(), TEST_COUNT);
    LOG_INFO("list:", K(list));
    while (!list.empty()) {
      ret = list.pop_back();
      ASSERT_EQ(ret, OB_SUCCESS);
    }
    ASSERT_EQ(list.size(), 0);
  }

    {
    const int TEST_COUNT = 10;
    ObRpcReqList list;
    ObRpcReq *req = NULL;
    ret = prepare_rpc_req_queue(list , req ,TEST_COUNT);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(list.size(), TEST_COUNT);
    LOG_INFO("list:", K(list));
    while (!list.empty()) {
      ObRpcReq *tmp_req;
      ret = list.pop_back(tmp_req);
      ASSERT_EQ(ret, OB_SUCCESS);
      ASSERT_TRUE(tmp_req != NULL);
    }
    ASSERT_EQ(list.size(), 0);
  }

  {
    const int TEST_COUNT = 100;
    ObRpcReqList list;
    ObRpcReq *req = NULL;
    int ret = prepare_rpc_req_queue(list , req ,TEST_COUNT);
    ASSERT_EQ(ret, OB_SUCCESS);
    list.erase(&req[0]);
    list.erase(&req[1]);
    list.erase(&req[2]);
    list.erase(&req[3]);
    list.erase(&req[4]);
    ObRpcReqList::iterator iter = list.begin();
    ObRpcReqList::iterator end = list.end();
    for (int i = 5; i < TEST_COUNT && iter != end; iter++, i++) {
      ASSERT_EQ(*iter, &req[i]);
    }
    ASSERT_EQ(list.size(), TEST_COUNT -  5);
  }
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