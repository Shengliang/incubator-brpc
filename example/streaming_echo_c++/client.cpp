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
#include <butil/synchronization/condition_variable.h>
#include "brpc/rdma/rdma_helper.h"
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
DEFINE_int32(timeout_ms, 1000, "RPC timeout in milliseconds");
DEFINE_int32(backup_ms, -1, "backup timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_bool(use_rdma, false, "use rdma or not");


bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");

class StreamReceiver : public brpc::StreamInputHandler {
public:
  virtual int on_received_messages(brpc::StreamId id,
                                   butil::IOBuf *const messages[],
                                   size_t size) {
    if (size == 0)
      return 0;

    auto *iobuf = messages[0];
    uint64_t LSN = 0ULL;
    int n = 0;
    char code[5];
    code[4] = '\0';
    if (iobuf->size() == 12) {
      n = iobuf->cutn(code, 4);
      n += iobuf->cutn(&LSN, 8);
    }
    _nack << 1;
    //LOG(INFO) << "Received ACK from Stream=" << id << " LSN:" << LSN << " " << code << " " <<  _nack.get_value();
    return 0;
  }

  virtual void on_idle_timeout(brpc::StreamId id) {
    LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
  }
  virtual void on_closed(brpc::StreamId id) {
    LOG(INFO) << "Stream=" << id << " is closed";
  }
  bvar::Adder<size_t> _nack;
};

  struct WriteControl {
    WriteControl() : cv(&mu) {}
    mutable butil::Mutex mu;
    mutable butil::ConditionVariable cv;
    bool is_writable = false;
    int error_code = 0;
    void on_writable(int ec) {
	    BAIDU_SCOPED_LOCK(mu);
	    is_writable = true;
	    error_code = ec;
	    cv.Broadcast();
    }
  };
  void mm_channel_on_writable(brpc::StreamId, void* arg, int error_code) {
     auto ctrl = reinterpret_cast<WriteControl*>(arg);
     ctrl->on_writable(error_code);
  };

struct MmChannel {
  // A Channel represents a communication line to a Server. Notice that
  // Channel is thread-safe and can be shared by all threads in your program.
  brpc::Channel channel;
  brpc::StreamId streamId;
  brpc::Controller stream_cntl;
  brpc::ChannelOptions sch_options;
  brpc::StreamOptions stream_options;
  StreamReceiver _receiver;
  butil::EndPoint pt;
  int id;
  uint64_t LSN = 0;

  MmChannel() { } 
  ~MmChannel() { 
    CHECK_EQ(0, brpc::StreamClose(streamId));
    LOG(INFO) << "EchoClient is going to quit:" << streamId;
  } 

  void SetEndPoint(int id, butil::EndPoint pt) {
    this->id = id;
    this->pt = pt;
  }
  int Init() {
    sch_options.protocol = FLAGS_protocol;
    sch_options.connection_type = FLAGS_connection_type;
    sch_options.timeout_ms = FLAGS_timeout_ms;
    sch_options.backup_request_ms = FLAGS_backup_ms;
    sch_options.max_retry = FLAGS_max_retry;
    sch_options.use_rdma = FLAGS_use_rdma;
    if (channel.Init(pt, &sch_options) != 0) {
       LOG(ERROR) << "Fail to initialize channel";
       return -1;
    }

    stream_options.handler = &_receiver;
    if (brpc::StreamCreate(&streamId, stream_cntl, &stream_options) != 0) {
      LOG(ERROR) << "Fail to create stream";
      return -1;
    }
    LOG(INFO) << "Created Stream=" << streamId << " at port:" << pt.port;

    return 0;
  }


  int Write(butil::IOBuf & pkt) {
      int rc = 0;
      while(!brpc::IsAskedToQuit()) {
         // wait until the stream is writable or an error occurred
         WriteControl wctrl;
         brpc::StreamWait(streamId, NULL, mm_channel_on_writable, &wctrl);
         BAIDU_SCOPED_LOCK(wctrl.mu);
         while(!wctrl.is_writable && !brpc::IsAskedToQuit()) {
            wctrl.cv.Wait();
         }
         if (wctrl.error_code != 0) {
	         LOG(ERROR) << "StreamWrite: error occurred: " << wctrl.error_code;
	         break;
	 }
         rc = brpc::StreamWrite(streamId, pkt);
         g_latency_recorder << stream_cntl.latency_us();
	 if (rc != EAGAIN)
            break;
      }
      return rc;
  }
};


static void* sender(void* arg) {
    auto &channel = *static_cast<MmChannel*>(arg);

    do {
     channel.stream_cntl.Reset();
     channel.Init();

     example::EchoRequest request;
     example::EchoResponse response;
     request.set_message("I'm a RPC to connect stream");
     example::EchoService_Stub stub(&channel.channel);
     stub.Echo(&channel.stream_cntl, &request, &response, NULL);
     if (channel.stream_cntl.Failed()) {
       //LOG(ERROR) << "Fail to connect stream, " << channel.stream_cntl.ErrorText();
       g_error_count << 1;
       sleep(1);
     } else {
       LOG(INFO) << "channel:" << channel.id << " ok ";
        break;
     }
    } while (channel.stream_cntl.Failed() && !brpc::IsAskedToQuit());


  auto streaming = [&](MmChannel & channel, void const *msg, uint32_t len) -> int {
    butil::IOBuf pkt;
    pkt.append(msg, 4);
    pkt.append(&len, sizeof(uint32_t));
    pkt.append(&channel.LSN, sizeof(uint64_t));
    uint32_t unit_size = 1024;
    while (!brpc::IsAskedToQuit() && len > 0) {
      uint32_t sz = std::min(len, unit_size);
      pkt.append(msg, sz);
      len -= sz;
    }
    return channel.Write(pkt);
  };

  char page[1024 * 4 * 4];
  for (int i = 0; i < 16 * 1024; ++i)
     page[i] = 'A' + channel.id;

  uint32_t len = 0;
  while (!brpc::IsAskedToQuit()) {
    ++channel.LSN;
    uint32_t sz = std::min(len, (uint32_t)sizeof(page));
    //LOG(INFO) << "stream channel:" << channel.id << " " << page[0] << " LSN:" << LSN << " size:" << sz;
    streaming(channel, page, sz);
    sleep(0.01);
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
  butil::EndPoint pt;

    if (str2endpoint(FLAGS_server.c_str(), &pt) != 0 &&
       hostname2endpoint(FLAGS_server.c_str(), &pt) != 0) {
       LOG(ERROR) << "Invalid address='" << FLAGS_server << "'";
       return -1;
    }

  for(int i = 0; i < N; ++i) {
     LOG(INFO) << "init channel:" << i << " " << pt.port;
     channels[i].stream_cntl.set_log_id(i);
     channels[i].SetEndPoint(i, pt);
     pt.port++;
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
  for (int i = 0; i < N; ++i) {
     LOG(INFO) << channels[i].id << " streamID:" << channels[i].streamId << " LSN:" << channels[i].LSN;
  }
  return 0;
}
