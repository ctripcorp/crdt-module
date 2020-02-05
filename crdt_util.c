#include "crdt_util.h"

CrdtCommon* getCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index) {
    long long gid;
    if ((RedisModule_StringToLongLong(argv[start_index],&gid) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return NULL;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[start_index+1],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return NULL;
    }
    VectorClock *vclock = getVectorClockFromString(argv[start_index+2]);
    return createCommon(gid, timestamp, vclock);
}
RedisModuleKey* getWriteRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type) {
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv,
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);                      
    int type = RedisModule_KeyType(moduleKey);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(moduleKey) != redismodule_type) {
        RedisModule_CloseKey(moduleKey);
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return NULL;
    }
    return moduleKey;
}

void* getCurrentValue(RedisModuleKey *moduleKey) {
    int type = RedisModule_KeyType(moduleKey);
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ModuleTypeGetValue(moduleKey);
    }
    return NULL;
}
void* getTombstone(RedisModuleKey *moduleKey) {
    void* tombstone = RedisModule_ModuleTypeGetTombstone(moduleKey);
    return tombstone;
}