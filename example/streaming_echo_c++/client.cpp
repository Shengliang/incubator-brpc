// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// A client sending requests to server in batch every 1 second.

#include "echo.pb.h"
#include <brpc/channel.h>
#include <brpc/stream.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <numeric>

DEFINE_bool(send_attachment, true, "Carry attachment along with requests");
DEFINE_string(connection_type, "",
              "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8001", "IP Address of server");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");

int main(int argc, char *argv[]) {
  // Parse gflags. We recommend you to use gflags as well.
  GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

  // A Channel represents a communication line to a Server. Notice that
  // Channel is thread-safe and can be shared by all threads in your program.
  brpc::Channel channel;

  // Initialize the channel, NULL means using default options.
  brpc::ChannelOptions options;
  options.protocol = brpc::PROTOCOL_BAIDU_STD;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms /*milliseconds*/;
  options.max_retry = FLAGS_max_retry;
  if (channel.Init(FLAGS_server.c_str(), NULL) != 0) {
    LOG(ERROR) << "Fail to initialize channel";
    return -1;
  }

  // Normally, you should not call a Channel directly, but instead construct
  // a stub Service wrapping it. stub can be shared by all threads as well.
  example::EchoService_Stub stub(&channel);
  brpc::Controller cntl;

  brpc::StreamId stream;
  if (brpc::StreamCreate(&stream, cntl, NULL) != 0) {
    LOG(ERROR) << "Fail to create stream";
    return -1;
  }
  LOG(INFO) << "Created Stream=" << stream;
  example::EchoRequest request;
  example::EchoResponse response;
  request.set_message("I'm a RPC to connect stream");
  stub.Echo(&cntl, &request, &response, NULL);
  if (cntl.Failed()) {
    LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
    return -1;
  }

  uint64_t LSN = 0ULL;

  auto streaming = [&](void const *msg, uint32_t len) -> int {
    butil::IOBuf pkt;
    pkt.append(msg, 4);
    pkt.append(&len, sizeof(uint32_t));
    pkt.append(&LSN, sizeof(uint64_t));
    uint32_t unit_size = 1024;
    while (!brpc::IsAskedToQuit() && len > 0) {
      uint32_t sz = std::min(len, unit_size);
      pkt.append(msg, sz);
      len -= sz;
    }
    int rc = 0;
    do {
      if (rc == EAGAIN)
        sleep(0.1);
      rc = brpc::StreamWrite(stream, pkt);
    } while (rc == EAGAIN);
    return 0;
  };

  char page[1024 * 4 * 4];
  // std::iota(std::begin(page), std::end(page), 0);
  for (int i = 0; i < 16 * 1024; ++i)
    page[i] = 'A';
  uint32_t len = 0;
  while (!brpc::IsAskedToQuit()) {
    ++LSN;
    uint32_t sz = std::min(len, (uint32_t)sizeof(page));
    streaming(page, sz);
    sleep(0.2);
    ++len;
    len %= 1024;
  }

  CHECK_EQ(0, brpc::StreamClose(stream));
  LOG(INFO) << "EchoClient is going to quit";
  return 0;
}
