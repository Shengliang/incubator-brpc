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

// A server to receive EchoRequest and send back EchoResponse.

#include "echo.pb.h"
#include <brpc/server.h>
#include <brpc/stream.h>
#include <butil/synchronization/condition_variable.h>
#include "brpc/rdma/rdma_helper.h"
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(port, 8100, "TCP Port of this server");
DEFINE_int32(server_num, 1, "Number of servers");
DEFINE_bool(use_rdma, false, "use rdma or not");
DEFINE_int32(idle_timeout_s, -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000,
             "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

bvar::LatencyRecorder g_latency_recorder("server");
bvar::Adder<int> g_error_count("server_error_count");

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
class StreamReceiver : public brpc::StreamInputHandler {
public:
  int Write(brpc::StreamId streamId, butil::IOBuf & pkt) {
      int rc = EAGAIN;
      while(!brpc::IsAskedToQuit()) {
          // wait until the stream is writable or an error occurred
          WriteControl wctrl;
          brpc::StreamWait(streamId, NULL, mm_channel_on_writable, &wctrl);
          BAIDU_SCOPED_LOCK(wctrl.mu);
          while(!wctrl.is_writable && !brpc::IsAskedToQuit()) {
             wctrl.cv.Wait();
          }
          if (wctrl.error_code != 0) {
	          // LOG(ERROR) << "StreamWrite: error occurred: " << wctrl.error_code;
                  g_error_count << 1;
	          break;
	  }
          rc = brpc::StreamWrite(streamId, pkt);
          if (rc != EAGAIN) break;
      }
      return rc;
  }
  virtual int on_received_messages(brpc::StreamId streamId,
                                   butil::IOBuf *const messages[],
                                   size_t size) {
    if (size == 0)
      return 0;
    auto *iobuf = messages[0];
    int n = 0;
    code[4] = '\0';
    if (iobuf->size() >= 16) {
      n = iobuf->cutn(code, 4);
      n += iobuf->cutn(&len, 4);
      n += iobuf->cutn(&LSN, 8);
    }

    size_t i = 0;
    int data_size = 0;
    std::string msg;
    uint32_t curr_len = len;
    while (i < size && iobuf->size() > 0 && curr_len > 0 && !brpc::IsAskedToQuit()) {
      iobuf = messages[i];
      uint32_t sz = std::min(curr_len, 128U);
      std::string bufstr;
      uint32_t cp_sz = iobuf->cutn(&bufstr, sz);
      msg += bufstr;
      data_size += cp_sz;
      if (iobuf->size() == 0) {
        ++i;
      }
      curr_len -= cp_sz;
    }

    _nreq << 1;
#if 0
    LOG(INFO)
        << "Received from Stream=" << streamId << ": " << size << " LSN:" << LSN
        << " len:" << len << " curr_len:"
        << curr_len << " n:" << n << " " << code << ":[" << msg.substr(0,16) << "]";
#endif

    return 0;
  }
  virtual void on_idle_timeout(brpc::StreamId streamId) {
    LOG(INFO) << "Stream=" << streamId << " has no data transmission for a while";
  }
  virtual void on_closed(brpc::StreamId streamId) {
    LOG(INFO) << "Stream=" << streamId << " is closed";
  }
  bvar::Adder<size_t> _nreq;
  bvar::Adder<size_t> _nerr;
  uint64_t LSN = 0ULL;
  uint32_t len = 0;
  char code[5];
};

// Your implementation of example::EchoService
class StreamingEchoService : public example::EchoService {
public:
  StreamingEchoService() : _streamId(brpc::INVALID_STREAM_ID){};
  virtual ~StreamingEchoService() { brpc::StreamClose(_streamId); };
  virtual void Echo(google::protobuf::RpcController *controller,
                    const example::EchoRequest * /*request*/,
                    example::EchoResponse *response,
                    google::protobuf::Closure *done) {
    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
    brpc::StreamOptions stream_options;
    stream_options.handler = &_receiver;
    if (brpc::StreamAccept(&_streamId, *cntl, &stream_options) != 0) {
      cntl->SetFailed("Fail to accept stream");
      return;
    }
    response->set_message("Accepted stream");
  }
  size_t num_requests() const { return _receiver._nreq.get_value(); }
  size_t num_errors() const { return _receiver._nerr.get_value(); }

  StreamReceiver _receiver;
  brpc::StreamId _streamId;
private:
};

int main(int argc, char *argv[]) {
  // Parse gflags. We recommend you to use gflags as well.
  GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

  brpc::Server* servers = new brpc::Server[FLAGS_server_num];
  StreamingEchoService* echo_service_impls = new StreamingEchoService[FLAGS_server_num];

  for (int i = 0; i < FLAGS_server_num; ++i) {
     servers[i].set_version(butil::string_printf("example/streaming_echo_c++[%d]", i)); 
     if (servers[i].AddService(&echo_service_impls[i], brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
       LOG(ERROR) << "Fail to add service";
       return -1;
     }
     brpc::ServerOptions options;
     options.idle_timeout_sec = FLAGS_idle_timeout_s;
     options.use_rdma = FLAGS_use_rdma;
     int port = FLAGS_port + i;
     if (servers[i].Start(port, &options) != 0) {
       LOG(ERROR) << "Fail to start EchoServer";
       return -1;
     }
     LOG(INFO) << " Start echo server:" << port;
  }

  std::vector<size_t> last_num_requests(FLAGS_server_num);
  while(!brpc::IsAskedToQuit()) {
     sleep(1);
     size_t cur_total = 0;
     for (int i = 0; i < FLAGS_server_num; ++i) {
         const size_t current_num_requests =
                 echo_service_impls[i].num_requests();
         const size_t current_num_errs =
                 echo_service_impls[i].num_errors();
         size_t diff = current_num_requests - last_num_requests[i];
         cur_total += diff;
         last_num_requests[i] = current_num_requests;
         auto LSN = echo_service_impls[i]._receiver.LSN;
	 char * code = echo_service_impls[i]._receiver.code;
         LOG(INFO) << "S[" << i << "]=" << diff << ' ' << current_num_errs << ' ' << LSN << ' ' << code << noflush;
         butil::IOBuf ack_pkt;
         ack_pkt.append(code, 4);
         ack_pkt.append(&LSN, sizeof(uint64_t));
         int rc = echo_service_impls[i]._receiver.Write(echo_service_impls[i]._streamId, ack_pkt);
         if (rc != 0) echo_service_impls[i]._receiver._nerr << 1;
     }
     LOG(INFO) << "[total=" << cur_total << ']';
  }
  for (int i = 0; i < FLAGS_server_num; ++i) {
	  servers[i].Stop(FLAGS_logoff_ms);
  }
  for (int i = 0; i < FLAGS_server_num; ++i) {
	  servers[i].Join();
  }

  delete [] servers;
  delete [] echo_service_impls;
  return 0;
}
