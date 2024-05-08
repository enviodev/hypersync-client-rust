@0x9289a56a18f880c5;

struct QueryResponseData {
    blocks @0 :Data;
    transactions @1 :Data;
    logs @2 :Data;
    traces @3 :Data;
}

struct RollbackGuard {
    hash @0 :Data;
    blockNumber @1 :UInt64;
    timestamp @2 :Int64;
    firstBlockNumber @3 :UInt64;
    firstParentHash @4 :Data;
}

struct QueryResponse {
    archiveHeight @0 :Int64;
    nextBlock @1 :UInt64;
    totalExecutionTime @2 :UInt64;
    data @3 :QueryResponseData;
    rollbackGuard @4 :RollbackGuard;
}
