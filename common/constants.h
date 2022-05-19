#pragma once

#include <chrono>
#include <string>

#include "common/types.h"

namespace slog {

const auto kModuleTimeout = std::chrono::milliseconds(1000);

const Channel kServerChannel = 1;
const Channel kForwarderChannel = 2;
const Channel kSequencerChannel = 3;
const Channel kBatcherChannel = 4;
const Channel kMultiHomeOrdererChannel = 5;
const Channel kClockSynchronizerChannel = 6;
const Channel kSchedulerChannel = 7;
const Channel kLocalPaxos = 8;
const Channel kGlobalPaxos = 9;
const Channel kDeadlockResolverChannel = 10;
// Broker channels are in [kBrokerChannel, kLogManagerChannel)
const Channel kBrokerChannel = 11;
// LogManager channels are in [kLogManagerChannel, kWorkerChannel)
const Channel kLogManagerChannel = 15;
// Worker channels are in [kWorkerChannel, kMaxChannel)
const Channel kWorkerChannel = 45;
// The broker considers anything larger than or equal to kMaxChannel as a tag.
// Tags are like channels but can be added/removed on the fly. Tags are used
// by log managers and workers. Log managers use tag to instruct the broker to
// distribute messages to the correct logs. Workers use tag to tell the broker
// which txn it is processing.
const Channel kMaxChannel = 70;
// Since workers use TxnId for their tags and TxnId starts from kMaxNumMachines,
// anything larger than or equal to kMaxNumMachines must be reserved for workers' tags.
// The range [kMaxChannel, kMaxNumMachines) is reserved for the log managers.
const uint32_t kMaxNumMachines = 100;

constexpr Channel kMaxNumBrokers = kLogManagerChannel - kBrokerChannel;
constexpr Channel kMaxNumLogManagers = kWorkerChannel - kLogManagerChannel;
constexpr Channel kMaxNumLogs = kMaxNumMachines - kMaxChannel;
constexpr Channel kMaxNumWorkers = kMaxChannel - kWorkerChannel;

const uint32_t kPaxosDefaultLeaderPosition = 0;

const size_t kLockTableSizeLimit = 1000000;

const int kRecvRetries = 4000;

// We never use 0 for txn id
const TxnId kSentinelTxnId = 0;

/****************************
 *      Statistic Keys
 ****************************/

/* Server */
const char TXN_ID_COUNTER[] = "txn_id_counter";
const char NUM_PENDING_RESPONSES[] = "num_pending_responses";
const char NUM_PARTIALLY_FINISHED_TXNS[] = "num_partially_finished_txns";
const char PENDING_RESPONSES[] = "pending_responses";
const char PARTIALLY_FINISHED_TXNS[] = "partially_finished_txns";

/* Forwarder */
const char FORW_LATENCIES_NS[] = "forw_latencies_us";
const char FORW_BATCH_SIZE[] = "forw_batch_size";
const char FORW_NUM_PENDING_TXNS[] = "forw_num_pending_txns";
const char FORW_PENDING_TXNS[] = "forw_pending_txns";

/* Sequencer */
const char SEQ_NUM_FUTURE_TXNS[] = "seq_num_future_txns";
const char SEQ_FUTURE_TXNS[] = "seq_future_txns";
const char SEQ_PROCESS_FUTURE_TXN_CALLBACK_ID[] = "seq_process_future_txn_callback_id";
const char SEQ_BATCH_SIZE[] = "seq_batch_size";

/* Multi-home orderer */
const char MHO_BATCH_SIZE_PCTLS[] = "mho_batch_size_pctls";
const char MHO_BATCH_DURATION_MS_PCTLS[] = "mho_batch_duration_ms_pctls";

/* Scheduler */
const char ALL_TXNS[] = "all_txns";
const char NUM_ALL_TXNS[] = "num_all_txns";
const char NUM_LOCKED_KEYS[] = "num_locked_keys";
const char LOCK_MANAGER_TYPE[] = "lock_manager_type";
const char NUM_TXNS_WAITING_FOR_LOCK[] = "num_txns_waiting_for_lock";
const char NUM_WAITING_FOR_PER_TXN[] = "num_waiting_for_per_txn";
const char LOCK_TABLE[] = "lock_table";
const char WAITED_BY_GRAPH[] = "waited_by_graph";
const char NUM_DEADLOCKS_RESOLVED[] = "num_deadlocks_resolved";
const char TXN_ID[] = "id";
const char TXN_DONE[] = "done";
const char TXN_ABORTING[] = "aborting";
const char TXN_NUM_LO[] = "num_lo";
const char TXN_NUM_DISPATCHES[] = "num_dispatches";
const char TXN_EXPECTED_NUM_LO[] = "expected_num_lo";
const char TXN_MULTI_HOME[] = "multi_home";
const char TXN_MULTI_PARTITION[] = "multi_partition";

}  // namespace slog