#include <gtest/gtest.h>

#include <condition_variable>
#include <vector>

#include "common/proto_utils.h"
#include "paxos/simulated_multi_paxos.h"
#include "test/test_utils.h"

using namespace slog;
using namespace std;

using Pair = pair<uint32_t, uint32_t>;

const Channel kTestChannel = 1;

class TestSimulatedMultiPaxos : public SimulatedMultiPaxos {
 public:
  TestSimulatedMultiPaxos(const shared_ptr<Broker>& broker, Members members, const MachineId& me)
      : SimulatedMultiPaxos(kTestChannel, broker, members, me, kTestModuleTimeout) {}

  Pair Poll() {
    unique_lock<mutex> lock(m_);
    // Wait until committed_ is not null
    bool ok = cv_.wait_for(lock, std::chrono::milliseconds(2000), [this] { return committed_ != nullptr; });
    if (!ok) {
      CHECK(false) << "Poll timed out";
    }
    Pair ret = *committed_;
    committed_.reset();
    return ret;
  }

 protected:
  void OnCommit(uint32_t slot, int64_t value, MachineId) final {
    {
      lock_guard<mutex> g(m_);
      CHECK(committed_ == nullptr) << "The result needs to be read before committing another one";
      committed_.reset(new Pair(slot, value));
    }
    cv_.notify_all();
  }

 private:
  unique_ptr<Pair> committed_;
  mutex m_;
  condition_variable cv_;
};

class PaxosTest : public ::testing::Test {
 protected:
  void AddAndStartNewPaxos(const ConfigurationPtr& config) {
    AddAndStartNewPaxos(config, config->all_machine_ids(), config->all_machine_ids(), config->local_machine_id());
  }

  void AddAndStartNewPaxos(const ConfigurationPtr& config, const vector<MachineId>& acceptors,
                           const vector<MachineId>& learners, MachineId me) {
    auto broker = Broker::New(config, kTestModuleTimeout);
    auto paxos = make_shared<TestSimulatedMultiPaxos>(broker, Members(acceptors, learners), me);
    auto sender = make_unique<Sender>(broker->config(), broker->context());
    auto paxos_runner = new ModuleRunner(paxos);

    broker->StartInNewThreads();
    paxos_runner->StartInNewThread();

    brokers_.emplace_back(broker);
    senders_.push_back(move(sender));
    paxos_runners_.emplace_back(paxos_runner);

    paxi.push_back(paxos);
  }

  void Propose(int index, int value) {
    auto env = make_unique<internal::Envelope>();
    env->mutable_request()->mutable_paxos_propose()->set_value(value);
    senders_[index]->Send(move(env), kTestChannel);
  }

  vector<shared_ptr<TestSimulatedMultiPaxos>> paxi;

 private:
  vector<shared_ptr<Broker>> brokers_;
  vector<unique_ptr<ModuleRunner>> paxos_runners_;
  vector<unique_ptr<Sender>> senders_;
};

TEST_F(PaxosTest, ProposeWithoutForwarding) {
  auto configs = MakeTestConfigurations("paxos", 1, 1, 3);
  for (auto config : configs) {
    AddAndStartNewPaxos(config);
  }

  Propose(0, 111);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0U, ret.first);
    ASSERT_EQ(111U, ret.second);
  }
}

TEST_F(PaxosTest, ProposeWithForwarding) {
  auto configs = MakeTestConfigurations("paxos", 1, 1, 3);
  for (auto config : configs) {
    AddAndStartNewPaxos(config);
  }

  Propose(1, 111);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0U, ret.first);
    ASSERT_EQ(111U, ret.second);
  }
}

TEST_F(PaxosTest, ProposeMultipleValues) {
  auto configs = MakeTestConfigurations("paxos", 1, 1, 3);
  for (auto config : configs) {
    AddAndStartNewPaxos(config);
  }

  Propose(0, 111);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0U, ret.first);
    ASSERT_EQ(111U, ret.second);
  }

  Propose(1, 222);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(1U, ret.first);
    ASSERT_EQ(222U, ret.second);
  }

  Propose(2, 333);
  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(2U, ret.first);
    ASSERT_EQ(333U, ret.second);
  }
}

TEST_F(PaxosTest, ExtraLearners) {
  auto configs = MakeTestConfigurations("paxos", 2, 1, 3);
  vector<MachineId> acceptors;
  vector<MachineId> learners;
  for (int p = 0; p < configs[0]->num_partitions(); p++) {
    acceptors.push_back(MakeMachineId(0, 0, p));
    learners.push_back(MakeMachineId(0, 0, p));
    learners.push_back(MakeMachineId(1, 0, p));
  }

  for (auto config : configs) {
    AddAndStartNewPaxos(config, acceptors, learners, config->local_machine_id());
  }

  Propose(configs.size() - 1, 111);

  for (auto& paxos : paxi) {
    auto ret = paxos->Poll();
    ASSERT_EQ(0U, ret.first);
    ASSERT_EQ(111U, ret.second);
  }
}