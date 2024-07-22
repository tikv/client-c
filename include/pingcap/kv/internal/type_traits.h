#pragma once

#include <kvproto/coprocessor.pb.h>
#include <kvproto/metapb.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#include <pingcap/kv/internal/conn.h>

namespace pingcap
{
namespace kv
{

#define RPC_NAME(METHOD) RpcTrait##METHOD

#define GRPC_TRAIT(METHOD)                                                   \
    struct RPC_NAME(METHOD)                                                  \
    {                                                                        \
        static const char * errMsg()                                         \
        {                                                                    \
            return #METHOD " Failed";                                        \
        }                                                                    \
        template <typename... Args>                                          \
        static auto call(                                                    \
            std::shared_ptr<KvConnClient> client,                            \
            Args &&... args)                                                 \
        {                                                                    \
            return client->stub->METHOD(std::forward<Args>(args)...);        \
        }                                                                    \
    };                                                                       \
    struct RPC_NAME(Async##METHOD)                                           \
    {                                                                        \
        static const char * err_msg()                                        \
        {                                                                    \
            return "Async" #METHOD " Failed";                                \
        }                                                                    \
        template <typename... Args>                                          \
        static auto call(                                                    \
            std::shared_ptr<KvConnClient> client,                            \
            Args &&... args)                                                 \
        {                                                                    \
            return client->stub->Async##METHOD(std::forward<Args>(args)...); \
        }                                                                    \
    };

#define M(method) GRPC_TRAIT(method)
// Commands using a transactional interface.
M(KvGet)
M(KvScan)
M(KvPrewrite)
M(KvPessimisticLock)
M(KVPessimisticRollback)
M(KvTxnHeartBeat)
M(KvCheckTxnStatus)
M(KvCheckSecondaryLocks)
M(KvCommit)
M(KvCleanup)
M(KvBatchGet)
M(KvBatchRollback)
M(KvScanLock)
M(KvResolveLock)
M(KvGC)
M(KvDeleteRange)
M(KvPrepareFlashbackToVersion)
M(KvFlashbackToVersion)

// Raw commands; no transaction support.
M(RawGet)
M(RawBatchGet)
M(RawPut)
M(RawBatchPut)
M(RawDelete)
M(RawBatchDelete)
M(RawScan)
M(RawDeleteRange)
M(RawBatchScan)
// Get TTL of the key. Returns 0 if TTL is not set for the key.
M(RawGetKeyTTL)
// Compare if the value in database equals to `RawCASRequest.previous_value` before putting the new value. If not, this request will have no effect and the value in the database will be returned.
M(RawCompareAndSwap)
M(RawChecksum)

// Store commands (sent to a each TiKV node in a cluster, rather than a certain region).
M(UnsafeDestroyRange)
M(RegisterLockObserver)
M(CheckLockObserver)
M(RemoveLockObserver)
M(PhysicalScanLock)

// Commands for executing SQL in the TiKV coprocessor (i.e., 'pushed down' to TiKV rather than
// executed in TiDB).
M(Coprocessor)
M(CoprocessorStream)
M(BatchCoprocessor)

// Command for executing custom user requests in TiKV coprocessor_v2.
M(RawCoprocessor)

// Raft commands (sent between TiKV nodes).
M(Raft)
M(BatchRaft)
M(Snapshot)
M(TabletSnapshot)

// Sent from PD or TiDB to a TiKV node.
M(SplitRegion)
// Sent from TiFlash or TiKV to a TiKV node.
M(ReadIndex)

// Commands for debugging transactions.
M(MvccGetByKey)
M(MvccGetByStartTs)

// Batched commands.
M(BatchCommands)

// These are for mpp execution.
M(DispatchMPPTask)
M(CancelMPPTask)
M(EstablishMPPConnection)
M(IsAlive)
M(ReportMPPTaskStatus)

/// CheckLeader sends all information (includes region term and epoch) to other stores.
/// Once a store receives a request, it checks term and epoch for each region, and sends the regions whose
/// term and epoch match with local information in the store.
/// After the client collected all responses from all stores, it checks if got a quorum of responses from
/// other stores for every region, and decides to advance resolved ts from these regions.
M(CheckLeader)

/// Get the minimal `safe_ts` from regions at the store
M(GetStoreSafeTS)

/// Get the information about lock waiting from TiKV.
M(GetLockWaitInfo)

/// Compact a specified key range. This request is not restricted to raft leaders and will not be replicated.
/// It only compacts data on this node.
/// TODO: Currently this RPC is designed to be only compatible with TiFlash.
/// Shall be move out in https://github.com/pingcap/kvproto/issues/912
M(Compact)
/// Get the information about history lock waiting from TiKV.
M(GetLockWaitHistory)

/// Get system table from TiFlash
M(GetTiFlashSystemTable)

// These are for TiFlash disaggregated architecture
/// Try to lock a S3 object, atomically
M(tryAddLock)
/// Try to delete a S3 object, atomically
M(tryMarkDelete)
/// Build the disaggregated task on TiFlash write node
M(EstablishDisaggTask)
/// Cancel the disaggregated task on TiFlash write node
M(CancelDisaggTask)
/// Exchange page data between TiFlash write node and compute node
M(FetchDisaggPages)
/// Compute node get configuration from Write node
M(GetDisaggConfig)

#undef M

} // namespace kv
} // namespace pingcap
