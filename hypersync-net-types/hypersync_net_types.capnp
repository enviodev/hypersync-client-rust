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

struct BlockSelection {
    hash @0 :List(Data);
    miner @1 :List(Data);
}

struct LogSelection {
    address @0 :List(Data);
    addressFilter @1 :Data;
    topics @2 :List(List(Data));
}

struct AuthorizationSelection {
    chainId @0 :List(UInt64);
    address @1 :List(Data);
}

struct TransactionSelection {
    from @0 :List(Data);
    fromFilter @1 :Data;
    to @2 :List(Data);
    toFilter @3 :Data;
    sighash @4 :List(Data);
    status @5 :UInt8;
    kind @6 :List(UInt8);
    contractAddress @7 :List(Data);
    contractAddressFilter @8 :Data;
    hash @9 :List(Data);
    authorizationList @10 :List(AuthorizationSelection);
}

struct TraceSelection {
    from @0 :List(Data);
    fromFilter @1 :Data;
    to @2 :List(Data);
    toFilter @3 :Data;
    address @4 :List(Data);
    addressFilter @5 :Data;
    callType @6 :List(Text);
    rewardType @7 :List(Text);
    kind @8 :List(Text);
    sighash @9 :List(Data);
}

struct FieldSelection {
    block @0 :List(Text);
    transaction @1 :List(Text);
    log @2 :List(Text);
    trace @3 :List(Text);
}

enum JoinMode {
    default @0;
    joinAll @1;
    joinNothing @2;
}

struct Query {
    fromBlock @0 :UInt64;
    toBlock @1 :UInt64;
    logs @2 :List(LogSelection);
    transactions @3 :List(TransactionSelection);
    traces @4 :List(TraceSelection);
    blocks @5 :List(BlockSelection);
    includeAllBlocks @6 :Bool;
    fieldSelection @7 :FieldSelection;
    maxNumBlocks @8 :UInt64;
    maxNumTransactions @9 :UInt64;
    maxNumLogs @10 :UInt64;
    maxNumTraces @11 :UInt64;
    joinMode @12 :JoinMode;
}
