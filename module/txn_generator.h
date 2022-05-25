#pragma once

#include <glog/logging.h>

#include <atomic>
#include <vector>
#include <zmq.hpp>

#include "common/rate_limiter.h"
#include "connection/poller.h"
#include "module/base/module.h"
#include "workload/workload.h"

namespace slog {

class TxnGenerator {
 public:
  struct TxnInfo {
    Transaction* txn = nullptr;
    TransactionProfile profile;
    std::chrono::system_clock::time_point sent_at;
    std::chrono::system_clock::time_point recv_at;
    bool finished = false;
    int generator_id;
    int restarts = 0;
  };

  TxnGenerator(std::unique_ptr<Workload>&& workload);
  const Workload& workload() const;
  size_t sent_txns() const;
  size_t committed_txns() const;
  size_t aborted_txns() const;
  size_t restarted_txns() const;
  void IncSentTxns();
  void IncCommittedTxns();
  void IncAbortedTxns();
  void IncRestartedTxns();

  std::chrono::nanoseconds elapsed_time() const;

  virtual const std::vector<TxnInfo>& txn_infos() const = 0;

 protected:
  void StartTimer();
  void StopTimer();
  bool timer_running() const;

  int id_;
  std::unique_ptr<Workload> workload_;

 private:
  std::chrono::steady_clock::time_point start_time_;
  std::atomic<std::chrono::nanoseconds> elapsed_time_;
  bool timer_running_;
  std::atomic<size_t> sent_txns_;
  std::atomic<size_t> committed_txns_;
  std::atomic<size_t> aborted_txns_;
  std::atomic<size_t> restarted_txns_;
};

// This generators simulates synchronous clients, each of which sends a new
// txn only after it receives response from the previous txn
class SynchronousTxnGenerator : public Module, public TxnGenerator {
 public:
  /**
   * If num_txns is set to 0, the txns are generated on-the-fly
   */
  SynchronousTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context, std::unique_ptr<Workload>&& workload,
                          RegionId region, ReplicaId rep, uint32_t num_txns, int num_clients, int duration_s,
                          std::shared_ptr<RateLimiter> rate_limiter, bool dry_run);
  ~SynchronousTxnGenerator();
  void SetUp() final;
  bool Loop() final;
  const std::vector<TxnInfo>& txn_infos() const final { return txns_; }
  std::string name() const final { return "Synchronous-Txn-Generator"; }

 private:
  void SendNextTxn();
  void SendTxn(int i);

  ConfigurationPtr config_;
  zmq::socket_t socket_;
  Poller poller_;
  RegionId region_;
  ReplicaId replica_;
  uint32_t num_txns_;
  int num_clients_;
  std::chrono::milliseconds duration_;
  std::shared_ptr<RateLimiter> rate_limiter_;
  bool dry_run_;
  std::vector<std::pair<Transaction*, TransactionProfile>> generated_txns_;
  std::vector<TxnInfo> txns_;
};

// This generator sends txn at a constant rate
class ConstantRateTxnGenerator : public Module, public TxnGenerator {
 public:
  ConstantRateTxnGenerator(const ConfigurationPtr& config, zmq::context_t& context,
                           std::unique_ptr<Workload>&& workload, RegionId region, ReplicaId rep, uint32_t num_txns,
                           int duration_s, std::shared_ptr<RateLimiter> rate_limiter, bool dry_run);
  ~ConstantRateTxnGenerator();
  void SetUp() final;
  bool Loop() final;
  const std::vector<TxnInfo>& txn_infos() const final { return txns_; }
  std::string name() const final { return "Constant-Txn-Generator"; }

 private:
  void SendNextTxn();

  ConfigurationPtr config_;
  zmq::socket_t socket_;
  Poller poller_;
  RegionId region_;
  ReplicaId replica_;
  uint32_t num_txns_;
  std::chrono::milliseconds duration_;
  std::shared_ptr<RateLimiter> rate_limiter_;
  bool dry_run_;

  std::vector<std::pair<Transaction*, TransactionProfile>> generated_txns_;
  std::vector<TxnInfo> txns_;
};

}  // namespace slog