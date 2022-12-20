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
#include <butil/logging.h>
#include <gflags/gflags.h>

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(port, 8114, "TCP Port of this server");
DEFINE_int32(server_num, 26, "Number of servers");
DEFINE_int32(idle_timeout_s, -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000,
             "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

class StreamReceiver : public brpc::StreamInputHandler {
public:
  virtual int on_received_messages(brpc::StreamId id,
                                   butil::IOBuf *const messages[],
                                   size_t size) {
    if (size == 0)
      return 0;
    auto *iobuf = messages[0];
    uint64_t LSN = 0ULL;
    uint32_t len = 0;
    int n = 0;
    char code[5];
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
        << "Received from Stream=" << id << ": " << size << " LSN:" << LSN
        << " len:" << len << " curr_len:"
        << curr_len << " n:" << n << " " << code << ":[" << msg.substr(0,16) << "]";
#endif
    return 0;
  }
  virtual void on_idle_timeout(brpc::StreamId id) {
    LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
  }
  virtual void on_closed(brpc::StreamId id) {
    LOG(INFO) << "Stream=" << id << " is closed";
  }
  bvar::Adder<size_t> _nreq;
};

// Your implementation of example::EchoService
class StreamingEchoService : public example::EchoService {
public:
  StreamingEchoService() : _sd(brpc::INVALID_STREAM_ID){};
  virtual ~StreamingEchoService() { brpc::StreamClose(_sd); };
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
    if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
      cntl->SetFailed("Fail to accept stream");
      return;
    }
    response->set_message("Accepted stream");
  }
  size_t num_requests() const { return _receiver._nreq.get_value(); }

private:
  StreamReceiver _receiver;
  brpc::StreamId _sd;
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
         size_t diff = current_num_requests - last_num_requests[i];
         cur_total += diff;
         last_num_requests[i] = current_num_requests;
         LOG(INFO) << "S[" << i << "]=" << diff << ' ' << noflush;
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
