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

#include <brpc/channel.h>
#include "brpc/rdma/rdma_helper.h"
#include <brpc/selective_channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/time/time.h>
#include <butil/synchronization/condition_variable.h>
#include "echo.pb.h"
#include <gflags/gflags.h>
#include <numeric>
#include <queue>
#include <utility>
#include <unordered_map>

/*Test
 * one node
  ./echo_client --server=0.0.0.0:8111 --thread_num=1 --port=8111 --seed=r

 * 3 Nodes
 ./echo_client --server=0.0.0.0:8111 --thread_num=3 --port=8111 --seed=a
 ./echo_client --server=0.0.0.0:8111 --thread_num=3 --port=8112 --seed=A
 ./echo_client --server=0.0.0.0:8111 --thread_num=3 --port=8113 --seed=r
 */
static std::atomic<int> client_done_cnt;
static std::atomic<int> server_done_cnt;
struct queue_elem_type {
    brpc::StreamId stream_id;
    butil::IOBuf* iobuf;
    queue_elem_type(brpc::StreamId id = brpc::INVALID_STREAM_ID, butil::IOBuf *const iobuf_ = nullptr) : stream_id(id) {
        iobuf = new butil::IOBuf(*iobuf_);
        LOG(INFO) << "new " << iobuf;
    }
    ~queue_elem_type() {
       if (iobuf != nullptr) {
         LOG(INFO) << "delete " << iobuf;
         delete iobuf;
	 iobuf = nullptr;
       }
       stream_id = brpc::INVALID_STREAM_ID;
    }
    /* Support move operator only */
    queue_elem_type& operator=(queue_elem_type&& other) {
	    this->~queue_elem_type();
	    std::swap(iobuf, other.iobuf);
	    std::swap(stream_id, other.stream_id);
	    return *this;
    }
    queue_elem_type(const queue_elem_type&other) = delete;
    queue_elem_type& operator=(const queue_elem_type& other) = delete;
};

//int N = 26;
int N = 1;
DEFINE_int32(thread_num, N, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_bool(send_attachment, true, "Carry attachment along with requests");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
//DEFINE_string(server, "10.231.229.157:8100", "IP Address of server, port + i");
DEFINE_string(server, "0.0.0.0:8100", "IP Address of server, port + i");
DEFINE_string(seed, "AAAA", "Seed String");
DEFINE_string(load_balancer, "rr", "Name of load balancer");
DEFINE_int32(timeout_ms, 1000, "RPC timeout in milliseconds");
DEFINE_int32(backup_ms, -1, "backup timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_bool(use_rdma, false, "use rdma or not");

DEFINE_int32(port, 8200, "TCP Port of this server");
DEFINE_int32(server_port_num, 1, "Number of server ports");
DEFINE_int32(idle_timeout_s, -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000,
             "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

bvar::LatencyRecorder g_latency_recorder("latency");
bvar::Adder<int> g_error_count("error_count");

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
	std::unordered_set<brpc::StreamId> rx_stream_ids;
public:
  virtual int on_received_messages(brpc::StreamId streamId,
                                   butil::IOBuf *const messages[],
                                   size_t size) {
    if (shutdown) return 0;
    if (size == 0)
      return 0;
    rx_stream_ids.insert(streamId);
    for (size_t i = 0; i < size; ++i) {
        std::unique_ptr<queue_elem_type> p(new queue_elem_type(streamId, messages[i]));
        incoming_queue.push(std::move(p));
    }
    return 0;
  }

  std::unique_ptr<queue_elem_type> read() {
    if (incoming_queue.empty()) return nullptr;
    auto p = std::move(incoming_queue.front());
    incoming_queue.pop();
    return p;
  }

  int process() {
    auto p = read();
    if (p == nullptr) return 0;
    auto streamId = p->stream_id;
    auto iobuf = p->iobuf;
    if (iobuf == nullptr) return 0;

    uint64_t LSN = 0ULL;
    int n = 0;
    char code[5];
    code[4] = '\0';
    if (iobuf->size() == 12) {
      n = iobuf->cutn(code, 4);
      n += iobuf->cutn(&LSN, 8);
    }
    _nack << 1;
    LOG(INFO) << "Client Received ACK from Stream=" << streamId << "," << m_stream_id << " port:" << m_port << " LSN:" << LSN << " " << code << " " <<  _nack.get_value();
    return 0;
  }

  virtual void on_idle_timeout(brpc::StreamId id) {
    if (shutdown) return;
    LOG(INFO) << "Client Stream=" << id << " has no data transmission for a while";
  }
  virtual void on_closed(brpc::StreamId id) {
    if (shutdown) return;
    LOG(INFO) << "Client Stream=" << id << " is closed port:" << m_port;
  }
  bvar::Adder<size_t> _nack;
  int m_port;
  brpc::StreamId m_stream_id;
  void setPort(int port) { m_port = port; }
  void setStreamId(brpc::StreamId streamId) { m_stream_id = streamId; }
  bool shutdown = false;
  std::queue<std::unique_ptr<queue_elem_type>> incoming_queue;
};

class ServerStreamReceiver : public brpc::StreamInputHandler {
	std::unordered_set<brpc::StreamId> rx_stream_ids;
public:
  int Write(brpc::StreamId streamId, butil::IOBuf & pkt) {
      int rc = EAGAIN;
      while(!brpc::IsAskedToQuit() && !shutdown) {
          // wait until the stream is writable or an error occurred
          WriteControl wctrl;
          brpc::StreamWait(streamId, NULL, mm_channel_on_writable, &wctrl);
          BAIDU_SCOPED_LOCK(wctrl.mu);
          while(!wctrl.is_writable && !brpc::IsAskedToQuit() && !shutdown) {
             wctrl.cv.Wait();
          }
          if (wctrl.error_code != 0) {
             // LOG(ERROR) << "StreamWrite: error occurred: " << wctrl.error_code;
             g_error_count << 1;
             break;
	  }
	  if (shutdown) break;
          rc = brpc::StreamWrite(streamId, pkt);
          if (rc != EAGAIN) break;
      }
      return rc;
  }
  virtual int on_received_messages(brpc::StreamId streamId,
                                   butil::IOBuf *const messages[],
                                   size_t size) {
    if (shutdown) return 0;
    if (size == 0)
      return 0;
    rx_stream_ids.insert(streamId);
    for (size_t i = 0; i < size; ++i) {
        std::unique_ptr<queue_elem_type> p(new queue_elem_type(streamId, messages[i]));
        incoming_queue.push(std::move(p));
    }
    return 0;

  }

  std::unique_ptr<queue_elem_type> read() {
    if (incoming_queue.empty()) return nullptr;
    auto p = std::move(incoming_queue.front());
    incoming_queue.pop();
    return p;
  }

  int process() {
    auto p = read();
    if (p == nullptr) return 0;
    auto iobuf = p->iobuf;
    int n = 0;
    code[4] = '\0';
    if (iobuf->size() >= 16) {
      n = iobuf->cutn(code, 4);
      n += iobuf->cutn(&len, 4);
      n += iobuf->cutn(&LSN, 8);
    }
    while (p != nullptr && !brpc::IsAskedToQuit() && !shutdown) {
         p = read();
    }

    _nreq << 1;
    if(shutdown) return 0;
#if 1
    std::stringstream ss;
    for(auto id:rx_stream_ids) {
	    ss << id << " ";
    }
    LOG(INFO)
        << "Server Received from Stream=" << m_stream_id << " port:" << m_port << " " << " LSN:" << LSN
        << " len:" << len << " n:" << n << " " << code << " stream_ids:" << ss.str();
#endif

    return 0;
  }
  virtual void on_idle_timeout(brpc::StreamId streamId) {
    if (shutdown) return;
    LOG(INFO) << "Server Stream=" << streamId << " has no data transmission for a while";
  }
  virtual void on_closed(brpc::StreamId streamId) {
    if (shutdown) return;
    LOG(INFO) << "Server Stream=" << streamId << " is closed. port:" << m_port << " stream_id:" << m_stream_id;
  }
  bvar::Adder<size_t> _nreq;
  bvar::Adder<size_t> _nerr;
  uint64_t LSN = 0ULL;
  uint32_t len = 0;
  char code[5];
  int m_port;
  brpc::StreamId m_stream_id;
  void setPort(int port) { m_port = port; }
  void setStreamId(brpc::StreamId streamId) { m_stream_id = streamId; }
  bool shutdown = false;
  std::queue<std::unique_ptr<queue_elem_type>> incoming_queue;
};

// Your implementation of example::EchoService
class StreamingEchoService : public example::EchoService {
public:
  brpc::StreamOptions _stream_options;
  void setPort(int port) { _receiver.setPort(port); };
  StreamingEchoService() {};
  virtual ~StreamingEchoService() {
      Shutdown();
  };
  void Shutdown() {
      if (shutdown_done) return;
      _receiver.shutdown = true;
      LOG(INFO) << "Shutdown Port:" << _receiver.m_port << " stream:" << _receiver.m_stream_id << " closed.";
      brpc::StreamClose(_receiver.m_stream_id);
      shutdown_done = true;
  }
  void OpenStream(google::protobuf::RpcController *controller,
                    const example::EchoRequest * /*request*/,
                    example::EchoResponse *response,
                    google::protobuf::Closure *done) override {
    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
    _stream_options.handler = &_receiver;
    if (brpc::StreamAccept(&_receiver.m_stream_id, *cntl, &_stream_options) != 0) {
      cntl->SetFailed("Fail to accept stream");
      return;
    }
    response->set_message("Accepted stream");
    LOG(INFO) << "Server Service: Accepted Stream=" << _receiver.m_stream_id;

  }
  void CloseStream(google::protobuf::RpcController *controller,
                    const example::EchoRequest * /*request*/,
                    example::EchoResponse *response,
                    google::protobuf::Closure *done) override {
    brpc::ClosureGuard done_guard(done);
    LOG(INFO) << "Server Service: Close Stream=" << _receiver.m_stream_id;
    Shutdown();
    server_done_cnt.fetch_add(1);
  }
  size_t num_requests() const { return _receiver._nreq.get_value(); }
  size_t num_errors() const { return _receiver._nerr.get_value(); }

  ServerStreamReceiver _receiver;
  bool shutdown_done = false;
private:
};



struct StreamChannel :  public ::brpc::Channel {
  // A Channel represents a communication line to a Server. Notice that
  // Channel is thread-safe and can be shared by all threads in your program.
  brpc::StreamId streamId;
  brpc::Controller stream_cntl;
  brpc::ChannelOptions sch_options;
  brpc::StreamOptions stream_options;
  StreamReceiver _receiver;
  butil::EndPoint pt;
  int id;
  uint64_t LSN = 0;

  StreamChannel() { }
  ~StreamChannel() {
    _receiver.shutdown = true;
    LOG(INFO) << "EchoClient is going to quit:" << streamId;
    CHECK_EQ(0, brpc::StreamClose(streamId));
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
    int error = ::brpc::Channel::Init(pt, &sch_options);
    if (error != 0) {
       LOG(ERROR) << "Fail to initialize channel at port:" << pt.port;
       return -1;
    }

    _receiver.setPort(pt.port);
    stream_options.handler = &_receiver;
    if (brpc::StreamCreate(&streamId, stream_cntl, &stream_options) != 0) {
      LOG(ERROR) << "Fail to create stream at port:" << pt.port;
      return -1;
    }
    _receiver.setStreamId(streamId);

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
              //LOG(ERROR) << "StreamWrite: error occurred: " << wctrl.error_code;
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
    auto &channel = *static_cast<StreamChannel*>(arg);

    int cnt = 1000000;
    do {
     channel.stream_cntl.Reset();
     channel.Init();

     example::EchoRequest request;
     example::EchoResponse response;
     request.set_message("I'm a RPC to connect stream");
     example::EchoService_Stub stub(&channel);
     stub.OpenStream(&channel.stream_cntl, &request, &response, NULL);
     if (channel.stream_cntl.Failed()) {
       LOG(ERROR) << "Fail to connect stream, " << channel.stream_cntl.ErrorText() << " port:" << channel.pt.port;
       sleep(1);
       g_error_count << 1;
     } else {
       LOG(INFO) << "channel:" << channel.id << " port:" << channel.pt.port << " OK.";
       break;
     }
    } while (--cnt && !brpc::IsAskedToQuit());


  auto streaming = [&](StreamChannel & channel, void const *msg, uint32_t len) -> int {
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
     page[i] = FLAGS_seed.c_str()[0] + channel.id;

  uint32_t len = 0;
  while (!brpc::IsAskedToQuit()) {
    ++channel.LSN;
    uint32_t sz = std::min(len, (uint32_t)sizeof(page));
    channel._receiver.process();
    LOG(INFO) << "stream channel:" << channel.id << " " << page[0] << " LSN:" << channel.LSN << " size:" << sz;
    streaming(channel, page, sz);
    sleep(1);
    ++len;
    len %= 1024;
  }
  LOG(INFO) << "client sender quit:" << channel.pt.port ;
  {
     example::EchoRequest request;
     example::EchoResponse response;
     request.set_message("I'm a RPC to close the stream");
     channel.stream_cntl.Reset();
     example::EchoService_Stub stub(&channel);
     stub.CloseStream(&channel.stream_cntl, &request, &response, NULL);
  }
  client_done_cnt.fetch_add(1);
  return NULL;
}

static void* server_main(void* arg) {

  brpc::Server* servers = new brpc::Server[FLAGS_server_port_num];
  StreamingEchoService* echo_service_impls = new StreamingEchoService[FLAGS_server_port_num];

  for (int i = 0; i < FLAGS_server_port_num; ++i) {
     int port = FLAGS_port + i;
     echo_service_impls[i].setPort(port);
     servers[i].set_version(butil::string_printf("example/streaming_echo_c++[%d]", i));
     if (servers[i].AddService(&echo_service_impls[i], brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
       LOG(ERROR) << "Fail to add service @port:" << port;
       return NULL;
     }
     brpc::ServerOptions options;
     options.idle_timeout_sec = FLAGS_idle_timeout_s;
     options.use_rdma = FLAGS_use_rdma;
     if (servers[i].Start(port, &options) != 0) {
       LOG(ERROR) << "Fail to start EchoServer";
       return NULL;
     }
     LOG(INFO) << " Start echo server at port:" << port;
  }

  std::vector<size_t> last_num_requests(FLAGS_server_port_num);
  bool done = false;
  while(!brpc::IsAskedToQuit() && !done) {
     sleep(1);
     size_t cur_total = 0;
     for (int i = 0; i < FLAGS_server_port_num; ++i) {
         if (echo_service_impls[i].shutdown_done) {
	     done = true;
	     break;
	 }
         int port = FLAGS_port + i;
         const size_t current_num_requests =
                 echo_service_impls[i].num_requests();
         const size_t current_num_errs =
                 echo_service_impls[i].num_errors();
         size_t diff = current_num_requests - last_num_requests[i];
         cur_total += diff;
         last_num_requests[i] = current_num_requests;
         auto LSN = echo_service_impls[i]._receiver.LSN;
	 char * code = echo_service_impls[i]._receiver.code;
         LOG(INFO) << "Write[" << port << " Stream:" << echo_service_impls[i]._receiver.m_stream_id << "]=" << diff << ' ' << current_num_errs << ' ' << LSN << ' ' << code << noflush;
         butil::IOBuf ack_pkt;
         ack_pkt.append(code, 4);
         ack_pkt.append(&LSN, sizeof(uint64_t));
         int rc = echo_service_impls[i]._receiver.Write(echo_service_impls[i]._receiver.m_stream_id, ack_pkt);
         if (rc != 0) echo_service_impls[i]._receiver._nerr << 1;
         echo_service_impls[i]._receiver.process();
     }
     LOG(INFO) << "[total=" << cur_total << ']';
  }

  for (int i = 0; i < FLAGS_server_port_num; ++i) {
     int port = FLAGS_port + i;
     echo_service_impls[i].Shutdown();
     servers[i].RunUntilAskedToQuit();
     LOG(INFO) << "Server port quit:" << port;
  }
  /*
  for (int i = 0; i < FLAGS_server_port_num; ++i) {
	  servers[i].Stop(FLAGS_logoff_ms);
  }
  for (int i = 0; i < FLAGS_server_port_num; ++i) {
	  servers[i].Join();
  }
  */

  delete [] servers;
  delete [] echo_service_impls;
  LOG(INFO) << "Server main quit." << done;
  server_done_cnt.fetch_add(1);
  return NULL;
}

static void* client_main(void* arg) {
  N = FLAGS_thread_num;
  std::vector<StreamChannel> channels(N);
  butil::EndPoint pt;

    if (str2endpoint(FLAGS_server.c_str(), &pt) != 0 &&
       hostname2endpoint(FLAGS_server.c_str(), &pt) != 0) {
       LOG(ERROR) << "Invalid address='" << FLAGS_server << "'";
       return NULL;
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
             return NULL;
         }
     }
  } else {
     for (int i = 0; i < N; ++i) {
         if (bthread_start_background(&bids[i], NULL, sender, &channels[i]) != 0) {
             LOG(ERROR) << "Fail to create pthread";
             return NULL;
         }
     }
  }

  while (!brpc::IsAskedToQuit() && client_done_cnt.load() == 0 && server_done_cnt.load() == 0) {
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
     LOG(INFO) << "Client:" << channels[i].id << " streamID:" << channels[i].streamId << " LSN:" << channels[i].LSN;
  }
  return NULL;
}



int main(int argc, char *argv[]) {
  // Parse gflags. We recommend you to use gflags as well.
  GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
  pthread_t pids;
  pthread_t pidc;
  if (pthread_create(&pids, NULL, server_main, NULL) != 0) {
      LOG(ERROR) << "Fail to create pthread server";
      return -1;
  }
  if (pthread_create(&pidc, NULL, client_main, NULL) != 0) {
      LOG(ERROR) << "Fail to create pthread client";
      return -1;
  }
  pthread_join(pids, NULL);
  pthread_join(pidc, NULL);
  return 0;
}
