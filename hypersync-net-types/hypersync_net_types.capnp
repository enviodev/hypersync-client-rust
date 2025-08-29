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
    status @5 :OptUInt8;
    type @6 :List(UInt8);
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
    type @8 :List(Text);
    sighash @9 :List(Data);
}

struct FieldSelection {
    block @0 :List(BlockField);
    transaction @1 :List(TransactionField);
    log @2 :List(LogField);
    trace @3 :List(TraceField);
}

enum JoinMode {
    default @0;
    joinAll @1;
    joinNothing @2;
}

enum BlockField {
    number @0;
    hash @1;
    parentHash @2;
    sha3Uncles @3;
    logsBloom @4;
    transactionsRoot @5;
    stateRoot @6;
    receiptsRoot @7;
    miner @8;
    extraData @9;
    size @10;
    gasLimit @11;
    gasUsed @12;
    timestamp @13;
    mixHash @14;
    nonce @15;
    difficulty @16;
    totalDifficulty @17;
    uncles @18;
    baseFeePerGas @19;
    blobGasUsed @20;
    excessBlobGas @21;
    parentBeaconBlockRoot @22;
    withdrawalsRoot @23;
    withdrawals @24;
    l1BlockNumber @25;
    sendCount @26;
    sendRoot @27;
}

enum TransactionField {
    blockHash @0;
    blockNumber @1;
    gas @2;
    hash @3;
    input @4;
    nonce @5;
    transactionIndex @6;
    value @7;
    cumulativeGasUsed @8;
    effectiveGasPrice @9;
    gasUsed @10;
    logsBloom @11;
    from @12;
    gasPrice @13;
    to @14;
    v @15;
    r @16;
    s @17;
    maxPriorityFeePerGas @18;
    maxFeePerGas @19;
    chainId @20;
    contractAddress @21;
    type @22;
    root @23;
    status @24;
    yParity @25;
    accessList @26;
    authorizationList @27;
    l1Fee @28;
    l1GasPrice @29;
    l1GasUsed @30;
    l1FeeScalar @31;
    gasUsedForL1 @32;
    maxFeePerBlobGas @33;
    blobVersionedHashes @34;
    blobGasPrice @35;
    blobGasUsed @36;
    depositNonce @37;
    depositReceiptVersion @38;
    l1BaseFeeScalar @39;
    l1BlobBaseFee @40;
    l1BlobBaseFeeScalar @41;
    l1BlockNumber @42;
    mint @43;
    sighash @44;
    sourceHash @45;
}

enum LogField {
    transactionHash @0;
    blockHash @1;
    blockNumber @2;
    transactionIndex @3;
    logIndex @4;
    address @5;
    data @6;
    removed @7;
    topic0 @8;
    topic1 @9;
    topic2 @10;
    topic3 @11;
}

enum TraceField {
    transactionHash @0;
    blockHash @1;
    blockNumber @2;
    transactionPosition @3;
    type @4;
    error @5;
    from @6;
    to @7;
    author @8;
    gas @9;
    gasUsed @10;
    actionAddress @11;
    address @12;
    balance @13;
    callType @14;
    code @15;
    init @16;
    input @17;
    output @18;
    refundAddress @19;
    rewardType @20;
    sighash @21;
    subtraces @22;
    traceAddress @23;
    value @24;
}

struct QueryBody {
    logs @1 :List(LogSelection);
    transactions @2 :List(TransactionSelection);
    traces @3 :List(TraceSelection);
    blocks @4 :List(BlockSelection);
    includeAllBlocks @5 :Bool;
    fieldSelection @6 :FieldSelection;
    maxNumBlocks @7 :OptUInt64;
    maxNumTransactions @8 :OptUInt64;
    maxNumLogs @9 :OptUInt64;
    maxNumTraces @10 :OptUInt64;
    joinMode @0 :JoinMode;
}

struct BlockRange {
    fromBlock @0 :UInt64;
    toBlock @1 :OptUInt64;
}

struct Query {
    blockRange @0 :BlockRange;
    body @1 :QueryBody;
}

struct OptUInt64 {
    value @0 :UInt64;
}

struct OptUInt8 {
    value @0 :UInt8;
}
