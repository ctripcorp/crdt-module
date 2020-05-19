#include "crdt_util.h"
int redisModuleStringToGid(RedisModuleCtx *ctx, RedisModuleString *argv, long long *gid) {
    if ((RedisModule_StringToLongLong(argv,gid) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return REDISMODULE_ERR;
    }
    if(RedisModule_CheckGid(*gid) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be < 15");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
CrdtMeta* getMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index) {
    long long gid;
    if ((redisModuleStringToGid(ctx, argv[start_index],&gid) != REDISMODULE_OK)) {
        return NULL;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[start_index+1],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return NULL;
    }
    VectorClock *vclock = getVectorClockFromString(argv[start_index+2]);
    return createMeta(gid, timestamp, vclock);
}
CrdtMeta* mergeMeta(CrdtMeta* target, CrdtMeta* other) {
    VectorClock* result = vectorClockMerge(getMetaVectorClock(target), getMetaVectorClock(other));
    if(compareCrdtMeta(target, other) > 0) {
        return createMeta(other->gid, other->timestamp, result);
    }else{
        return createMeta(target->gid, target->timestamp, result);
    }
}
CrdtMeta* addOrCreateMeta(CrdtMeta* target, CrdtMeta* other) {
    VectorClock* vc = getMetaVectorClock(target);
    if(target == NULL || vc  == NULL) {
        return dupMeta(other);
    }
    if(compareCrdtMeta(target, other) > 0) {
        target->gid = other->gid;
        target->timestamp = other->timestamp;
    }
    
    merge(vc, getMetaVectorClock(other));
    // clone(&target->vectorClock ,vectorClockMerge(vc, getMetaVectorClock(other)));
    return target;
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

VectorClock* rdbLoadVectorClock(RedisModuleIO *rdb) {
    size_t vcLength, strLength;
    char* vcStr = RedisModule_LoadStringBuffer(rdb, &vcLength);
    sds vclockSds = sdsnewlen(vcStr, vcLength);
    VectorClock* result = sdsToVectorClock(vclockSds);
    sdsfree(vclockSds);
    RedisModule_Free(vcStr);
    return result;
}
int rdbSaveVectorClock(RedisModuleIO *rdb, VectorClock* vectorClock) {
    sds vclockStr = vectorClockToSds(vectorClock);
    RedisModule_SaveStringBuffer(rdb, vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    return CRDT_OK;
}