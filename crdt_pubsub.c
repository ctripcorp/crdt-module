#include "./crdt_pubsub.h"
int publishCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int receivers = RedisModule_CrdtPubsubPublishMessage(argv[1], argv[2]);
    long long gid = RedisModule_CurrentGid();
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.PUBLISH", "ssl", argv[1], argv[2], gid);
    return RedisModule_ReplyWithLongLong(ctx, receivers);
}
int crdtPublishCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 4)  return RedisModule_WrongArity(ctx);
    long long gid;
    if (redisModuleStringToGid(ctx, argv[3], &gid) != REDISMODULE_OK) {
        return 0;
    }
    int receivers = RedisModule_CrdtPubsubPublishMessage(argv[1], argv[2]);
    RedisModule_CrdtReplicateVerbatim(gid, ctx);
    return RedisModule_ReplyWithLongLong(ctx, receivers);
}
int initPubsubModule(RedisModuleCtx* ctx) {
    if (RedisModule_CreateCommand(ctx, "CRDTPUBLISH", 
        publishCommand, NULL, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CRDT.PUBLISH", 
        crdtPublishCommand, NULL, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
