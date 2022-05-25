#include "module/txn_generator.h"

#include <sstream>

#include "common/constants.h"
#include "connection/zmq_utils.h"
#include "proto/api.pb.h"

using std::shared_ptr;
using std::unique_ptr;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;
using std::chrono::operator""ms;
using std::chrono::operator""us;

namespace slog {
namespace {
void ConnectToServers(const ConfigurationPtr& config, zmq::socket_t& socket, RegionId region, ReplicaId rep) {
  socket.set(zmq::sockopt::sndhwm, 0);
  socket.set(zmq::sockopt::rcvhwm, 0);
  for (int p = 0; p < config->num_partitions(); p++) {
    std::ostringstream endpoint_s;
    if (config->protocol() == "ipc") {
      endpoint_s << "tcp://localhost:" << config->server_port();
    } else {
      endpoint_s << "tcp://" << config->address(region, rep, p) << ":" << config->server_port();
    }
    auto endpoint = endpoint_s.str();
    LOG(INFO) << "Connecting to " << endpoint;
    socket.connect(endpoint);
  }
}

bool RecordFinishedTxn(TxnGenerator::TxnInfo& info, int generator_id, Transaction* txn) {
  if (info.finished) {
    LOG(ERROR) << "Received response for finished txn";
    return false;
  }
  info.recv_at = system_clock::now();
  info.generator_id = generator_id;
  delete info.txn;
  info.txn = txn;
  info.finished = true;
  return true;
}

static int generator_id = 0;

}  // namespace

TxnGenerator::TxnGenerator(std::unique_ptr<Workload>&& workload)
    : id_(generator_id++),
      workload_(std::move(workload)),
      elapsed_time_(std::chrono::nanoseconds(0)),
      timer_running_(false),
      sent_txns_(0),
      committed_txns_(0),
      aborted_txns_(0),
      restarted_txns_(0) {
  CHECK(workload_ != nullptr) << "Must provide a valid workload";
}
const Workload& TxnGenerator::workload() const { return *workload_; }
size_t TxnGenerator::sent_txns() const { return sent_txns_; }
size_t TxnGenerator::committed_txns() const { return committed_txns_; }
size_t TxnGenerator::aborted_txns() const { return aborted_txns_; }
size_t TxnGenerator::restarted_txns() const { return restarted_txns_; }
void TxnGenerator::IncSentTxns() { sent_txns_++; }
void TxnGenerator::IncCommittedTxns() { committed_txns_++; };
void TxnGenerator::IncAbortedTxns() { aborted_txns_++; };
void TxnGenerator::IncRestartedTxns() { restarted_txns_++; };
std::chrono::nanoseconds TxnGenerator::elapsed_time() const {
  if (timer_running_) {
    return std::chrono::steady_clock::now() - start_time_;
  }
  return elapsed_time_.load();
}

void TxnGenerator::StartTimer() {
  timer_running_ = true;
  start_time_ = std::chrono::steady_clock::now();
}

void TxnGenerator::StopTimer() {
  timer_running_ = false;
  elapsed_time_ = std::chrono::steady_clock::now() - start_time_;
}

bool TxnGenerator::timer_running() const { return timer_running_; }

SynchronousTxnGenerator::SynchronousTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                                                 std::unique_ptr<Workload>&& workload, RegionId region, ReplicaId rep,
                                                 uint32_t num_txns, int num_clients, int duration_s,
                                                 std::shared_ptr<RateLimiter> rate_limiter, bool dry_run)
    : TxnGenerator(std::move(workload)),
      config_(config),
      socket_(context, ZMQ_DEALER),
      poller_(kModuleTimeout),
      region_(region),
      replica_(rep),
      num_txns_(num_txns),
      num_clients_(num_clients),
      duration_(duration_s * 1000),
      rate_limiter_(rate_limiter),
      dry_run_(dry_run) {
  CHECK_GT(duration_s, 0) << "Duration must be set for synchronous txn generator";
}

SynchronousTxnGenerator::~SynchronousTxnGenerator() {
  for (auto& txn : generated_txns_) {
    delete txn.first;
    txn.first = nullptr;
  }
  for (auto& txn : txns_) {
    delete txn.txn;
    txn.txn = nullptr;
  }
}

void SynchronousTxnGenerator::SetUp() {
  if (num_txns_ <= 0) {
    LOG(INFO) << "No txn is pre-generated";
  } else {
    LOG(INFO) << "Generating " << num_txns_ << " transactions";
    for (size_t i = 0; i < num_txns_; i++) {
      generated_txns_.push_back(workload_->NextTransaction());
    }
  }

  if (!dry_run_) {
    ConnectToServers(config_, socket_, region_, replica_);
    poller_.PushSocket(socket_);
    for (int i = 0; i < num_clients_; i++) {
      SendNextTxn();
    }
  }

  LOG(INFO) << "Start sending transactions with " << num_clients_ << " concurrent clients";

  StartTimer();
}

bool SynchronousTxnGenerator::Loop() {
  if (dry_run_) {
    return true;
  }

  bool duration_reached = elapsed_time() >= duration_;
  if (poller_.NextEvent()) {
    if (api::Response res; RecvDeserializedProtoWithEmptyDelim(socket_, res)) {
      auto& info = txns_[res.stream_id()];

      auto txn = res.mutable_txn()->release_txn();
      if (txn->status() == TransactionStatus::ABORTED) {
        if (txn->abort_code() == AbortCode::RESTARTED || txn->abort_code() == AbortCode::RATE_LIMITED) {
          info.restarts++;
          std::this_thread::sleep_for(5us);
          SendTxn(res.stream_id()); /* Restart */
          IncRestartedTxns();
        } else {
          IncAbortedTxns();
        }
      } else {
        if (RecordFinishedTxn(info, id_, txn)) {
          IncCommittedTxns();
          if (!duration_reached) {
            SendNextTxn();
          }
        }
      }
    }
  }

  if (duration_reached && aborted_txns() + committed_txns() == txns_.size()) {
    StopTimer();
    return true;
  }
  return false;
}

void SynchronousTxnGenerator::SendNextTxn() {
  while (!rate_limiter_->RequestWithMutex()) {
    std::this_thread::sleep_for(5us);
  }

  int id = sent_txns();

  TxnInfo info;
  if (generated_txns_.empty()) {
    auto [txn, profile] = workload_->NextTransaction();
    info.txn = txn;
    info.profile = profile;
  } else {
    auto [txn, profile] = generated_txns_[id % generated_txns_.size()];
    info.txn = new Transaction(*txn);
    info.profile = profile;
  }
  txns_.push_back(std::move(info));

  SendTxn(id);

  IncSentTxns();
  CHECK_EQ(txns_.size(), sent_txns());
}

void SynchronousTxnGenerator::SendTxn(int i) {
  auto& info = txns_[i];

  api::Request req;
  req.set_stream_id(i);
  req.mutable_txn()->set_allocated_txn(info.txn);

  SendSerializedProtoWithEmptyDelim(socket_, req);

  info.sent_at = system_clock::now();
  info.txn = req.mutable_txn()->release_txn();
}

ConstantRateTxnGenerator::ConstantRateTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                                                   unique_ptr<Workload>&& workload, RegionId region, ReplicaId replica,
                                                   uint32_t num_txns, int duration_s,
                                                   std::shared_ptr<RateLimiter> rate_limiter, bool dry_run)
    : TxnGenerator(std::move(workload)),
      config_(config),
      socket_(context, ZMQ_DEALER),
      poller_(kModuleTimeout),
      region_(region),
      replica_(replica),
      num_txns_(num_txns),
      duration_(duration_s * 1000),
      rate_limiter_(rate_limiter),
      dry_run_(dry_run) {}

ConstantRateTxnGenerator::~ConstantRateTxnGenerator() {
  for (auto& txn : generated_txns_) {
    delete txn.first;
    txn.first = nullptr;
  }
  for (auto& txn : txns_) {
    delete txn.txn;
    txn.txn = nullptr;
  }
}

void ConstantRateTxnGenerator::SetUp() {
  CHECK_GT(num_txns_, 0) << "There must be at least one transaction";
  LOG(INFO) << "Generating " << num_txns_ << " transactions";
  for (size_t i = 0; i < num_txns_; i++) {
    generated_txns_.push_back(workload_->NextTransaction());
  }

  if (!dry_run_) {
    ConnectToServers(config_, socket_, region_, replica_);
    poller_.PushSocket(socket_);
  }

  // Schedule sending new txns
  poller_.AddTimedCallback(1us, [this]() { SendNextTxn(); });
  LOG(INFO) << "Start sending transactions";

  StartTimer();
}

void ConstantRateTxnGenerator::SendNextTxn() {
  // If duration is set, keep sending txn until duration is reached, otherwise send until all generated txns are sent
  if (duration_ > 0ms) {
    if (elapsed_time() >= duration_) {
      return;
    }
  } else if (sent_txns() >= generated_txns_.size()) {
    return;
  }

  while (!rate_limiter_->RequestWithMutex()) {
    std::this_thread::sleep_for(5us);
  }

  const auto& selected_txn = generated_txns_[sent_txns() % generated_txns_.size()];

  api::Request req;
  req.mutable_txn()->set_allocated_txn(new Transaction(*selected_txn.first));
  req.set_stream_id(sent_txns());
  if (!dry_run_) {
    SendSerializedProtoWithEmptyDelim(socket_, req);
  }

  TxnInfo info;
  info.txn = req.mutable_txn()->release_txn();
  info.profile = selected_txn.second;
  info.sent_at = system_clock::now();
  txns_.push_back(std::move(info));

  IncSentTxns();

  poller_.AddTimedCallback(1us, [this]() { SendNextTxn(); });

  CHECK_EQ(txns_.size(), sent_txns());
}

bool ConstantRateTxnGenerator::Loop() {
  if (poller_.NextEvent()) {
    if (api::Response res; RecvDeserializedProtoWithEmptyDelim(socket_, res)) {
      CHECK_LT(res.stream_id(), txns_.size());

      auto txn = res.mutable_txn()->release_txn();
      auto& info = txns_[res.stream_id()];
      if (txn->status() == TransactionStatus::ABORTED) {
        IncAbortedTxns();
      } else if (RecordFinishedTxn(info, id_, txn)) {
        IncCommittedTxns();
      }
    }
  }

  bool stop = false;
  auto recv_txns = aborted_txns() + committed_txns();
  // If duration is set, keep sending txn until duration is reached, otherwise send until all generated txns are sent
  if (duration_ > 0ms) {
    bool duration_reached = elapsed_time() >= duration_;
    stop = duration_reached && (dry_run_ || recv_txns == sent_txns());
  } else {
    stop = sent_txns() >= generated_txns_.size() && (dry_run_ || recv_txns >= txns_.size());
  }
  if (stop) {
    StopTimer();
    return true;
  }
  return false;
}

}  // namespace slog