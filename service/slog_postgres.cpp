#include <fcntl.h>
#include <optional>
#include <unordered_map>

#include <zmq.hpp>

#include "service/service_utils.h"

#define UNIX_SOCKET_FILE "slogora"

void report_and_exit(const char* msg) {
  perror(msg);
  exit(-1);
}

struct PGBackend {
  std::optional<int> buffer_sz;
  std::vector<char> buffer;
};

struct ReadTuple {
  uint32_t db;
  uint32_t relation;
  uint32_t blockno;
  uint16_t offset;
};

std::unordered_map<std::string, PGBackend> backends;

void handle_message(const zmq::message_t& identity_msg, const zmq::message_t& data_msg) {
  auto identity = identity_msg.to_string();
  auto it = backends.find(identity);

  if (data_msg.size() == 0) {
    if (it == backends.end()) {
      LOG(INFO) << "New connection established";
      backends.emplace(identity, PGBackend());
    } else {
      LOG(INFO) << "Connection destroyed";
      backends.erase(it);
    }
    return;
  }

  if (it != backends.end()) {
    auto& backend = it->second;
    const char* data = data_msg.data<char>();
    std::copy(data, data + data_msg.size(), std::back_inserter(backend.buffer));

    if (backend.buffer_sz == std::nullopt && backend.buffer.size() >= sizeof(backend.buffer_sz)) {
      backend.buffer_sz = *reinterpret_cast<int*>(backend.buffer.data());
    }

    if (backend.buffer_sz != std::nullopt && backend.buffer.size() == static_cast<size_t>(*backend.buffer_sz)) {
      char* buffer = backend.buffer.data();
      int size = *backend.buffer_sz;
      int consumed = sizeof(size);
      printf("Read tuples:\n");
      while (consumed < size) {
        if (static_cast<size_t>(size - consumed) < 14) {
          LOG(WARNING) << "Skipped trailing " << size - consumed << " bytes";
          break;
        }
        ReadTuple tuple;
        auto cursor = buffer + consumed;
        tuple.db = *reinterpret_cast<uint32_t*>(cursor); cursor += sizeof(uint32_t);
        tuple.relation = *reinterpret_cast<uint32_t*>(cursor); cursor += sizeof(uint32_t);
        tuple.blockno = *reinterpret_cast<uint32_t*>(cursor); cursor += sizeof(uint32_t);
        tuple.offset = *reinterpret_cast<uint16_t*>(cursor); cursor += sizeof(uint16_t);
        consumed = cursor - buffer;
        printf("%d %d %d %d\n", tuple.db, tuple.relation, tuple.blockno, tuple.offset);
      }

      // char commit = 'c';
      // data_msg.rebuild(&commit, 1);
      // sock.send(identity_msg, zmq::send_flags::sndmore);
      // sock.send(data_msg, zmq::send_flags::none);
    }
  }
} 

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);
  
  zmq::context_t ctx;
  zmq::socket_t sock(ctx, ZMQ_STREAM);
  // socket.set(zmq::sockopt::stream_notify, 0);
  sock.bind(std::string("ipc:///tmp/") + UNIX_SOCKET_FILE);
  
  for (;;) {
    zmq::message_t identity_msg;
    if (!sock.recv(identity_msg)) return false;
    zmq::message_t data_msg;
    do {
      if (!sock.recv(data_msg)) break;
      handle_message(identity_msg, data_msg);
    } while (data_msg.more());
  }

  return 0;
}