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
#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <brpc/stream.h>
#include <butil/logging.h>
#include <brpc/selective_channel.h>
#include <gflags/gflags.h>
#include <numeric>

int N = 26;
DEFINE_int32(thread_num, N, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_bool(send_attachment, true, "Carry attachment along with requests");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(server, "10.231.229.157:8114", "IP Address of server, port + i");
DEFINE_string(load_balancer, "rr", "Name of load balancer");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(backup_ms, -1, "backup timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");


bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");

struct MmChannel {
  // A Channel represents a communication line to a Server. Notice that
  // Channel is thread-safe and can be shared by all threads in your program.
  brpc::Channel channel;
  brpc::StreamId streamId;
  brpc::Controller cntl;
  butil::EndPoint pt;
  int id;

  MmChannel() { } 
  ~MmChannel() { 
    CHECK_EQ(0, brpc::StreamClose(streamId));
    LOG(INFO) << "EchoClient is going to quit" << streamId;
  } 

  int Init(butil::EndPoint pt, int id) {
    this->id = id;
    this->pt = pt;
    brpc::ChannelOptions sch_options;
    sch_options.timeout_ms = FLAGS_timeout_ms;
    sch_options.backup_request_ms = FLAGS_backup_ms;
    sch_options.max_retry = FLAGS_max_retry;
    pt.port += id;
    if (channel.Init(pt, NULL) != 0) {
       LOG(ERROR) << "Fail to initialize channel";
       return -1;
    }

    if (brpc::StreamCreate(&streamId, cntl, NULL) != 0) {
      LOG(ERROR) << "Fail to create stream";
      return -1;
    }
    LOG(INFO) << "Created Stream=" << streamId << " at port:" << pt.port;

    return 0;
  }

  int Write(butil::IOBuf & pkt) {
      return brpc::StreamWrite(streamId, pkt);
  }
};


static void* sender(void* arg) {
    example::EchoRequest request;
    example::EchoResponse response;
    request.set_message("I'm a RPC to connect stream");

    int log_id = 0;
    auto &channel = *static_cast<MmChannel*>(arg);
    example::EchoService_Stub stub(&channel.channel);
    channel.cntl.set_log_id(log_id++);
    stub.Echo(&channel.cntl, &request, &response, NULL);
    if (channel.cntl.Failed()) {
      LOG(ERROR) << "Fail to connect stream, " << channel.cntl.ErrorText();
      g_error_count << 1;
      bthread_usleep(50000);
      return NULL;
    }
    g_latency_recorder << channel.cntl.latency_us();
  uint64_t LSN = 0ULL;

  auto streaming = [&](MmChannel & channel, void const *msg, uint32_t len) -> int {
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
      rc = channel.Write(pkt);
      g_latency_recorder << channel.cntl.latency_us();
    } while (rc == EAGAIN);
    return 0;
  };

  char page[1024 * 4 * 4];
  for (int i = 0; i < 16 * 1024; ++i)
     page[i] = 'A' + channel.id;

  LOG(INFO) << "stream channel:" << channel.id << " " << page[0];
  uint32_t len = 0;
  while (!brpc::IsAskedToQuit()) {
    ++LSN;
    uint32_t sz = std::min(len, (uint32_t)sizeof(page));
    streaming(channel, page, sz);
    sleep(0.2);
    ++len;
    len %= 1024;
  }
  return NULL;
}

int main(int argc, char *argv[]) {
  // Parse gflags. We recommend you to use gflags as well.
  GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

  N = FLAGS_thread_num;

  std::vector<MmChannel> channels(N);
  int id = 0;
  butil::EndPoint pt;

    if (str2endpoint(FLAGS_server.c_str(), &pt) != 0 &&
       hostname2endpoint(FLAGS_server.c_str(), &pt) != 0) {
       LOG(ERROR) << "Invalid address='" << FLAGS_server << "'";
       return -1;
    }

  for(auto& channel : channels) {
    channel.Init(pt, id++);
  }

  std::vector<bthread_t> bids(N);
  std::vector<pthread_t> pids(N);
  if (!FLAGS_use_bthread) {
     for (int i = 0; i < N; ++i) {
         if (pthread_create(&pids[i], NULL, sender, &channels[i]) != 0) {
             LOG(ERROR) << "Fail to create pthread";
             return -1;
         }
     }
  } else {
     for (int i = 0; i < N; ++i) {
         if (bthread_start_background(&bids[i], NULL, sender, &channels[i]) != 0) {
             LOG(ERROR) << "Fail to create pthread";
             return -1;
         }
     }
  }

  while (!brpc::IsAskedToQuit()) {
      sleep(1);
      LOG(INFO) << "Sending EchoRequest at qps=" << g_latency_recorder.qps(1) << " latency=" << g_latency_recorder.latency(1);
  }

  for (int i = 0; i < N; ++i) {
    if (!FLAGS_use_bthread)
       pthread_join(pids[i], NULL);
    else
       bthread_join(bids[i], NULL);
  }
  return 0;
}
