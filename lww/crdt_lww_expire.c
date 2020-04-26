#include "crdt_lww_expire.h"
#include "../ctrip_crdt_common.h"
#include "../crdt_util.h"
CrdtLWWExpire* retrieveCrdtLWWExpire(void* obj) {
    CrdtLWWExpire* expire = (CrdtLWWExpire*)obj;
    return expire;
}

int crdtExpireAddObj(CrdtExpire* obj, CrdtExpireObj* data) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(obj);
    if(expire->data == NULL) {
        expire->data = dupExpireObj(data);
        return 1;
    }
    int result = compareCrdtMeta(expire->data->meta, data->meta);
    CrdtMeta *old = expire->data->meta;
    expire->data->meta = mergeMeta(old, data->meta);
    freeCrdtMeta(old);
    if(result > 0) {
        expire->data->expireTime = data->expireTime;
    }
    return result;
}


CrdtExpireObj* crdtExpireGetObj(CrdtExpire* obj) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(obj);
     return expire->data;
}

CrdtExpire* crdtExpireDup(CrdtExpire* obj) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(obj);
    CrdtLWWExpire* copy = createCrdtLWWExpire();
    copy->parent.parent.type = expire->parent.parent.type;
    copy->data = dupExpireObj(expire->data);
    return copy;
}

void crdtExpireFree(CrdtExpire* obj) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(obj);
    if(expire->data != NULL) {
        freeCrdtExpireObj(expire->data);
        expire->data = NULL;
    }
    RedisModule_Free(expire);
}


CrdtLWWExpire* createCrdtLWWExpire(void) {
    CrdtLWWExpire* expire = RedisModule_Alloc(sizeof(CrdtLWWExpire));
    expire->parent.parent.type = 0;
    expire->parent.parent.type |= CRDT_EXPIRE;
    expire->data = NULL;
    return expire;
}


void *RdbLoadCrdtLWWExpire(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CrdtLWWExpire *expire = createCrdtLWWExpire();
    int type = RedisModule_LoadSigned(rdb);
    expire->parent.parent.type |= type;
    RedisModule_Debug(logLevel, "load expire type %lld %lld", expire->parent.parent.type, type);
    int gid = RedisModule_LoadSigned(rdb);
    long long timestamp = RedisModule_LoadSigned(rdb);
    VectorClock* vectorClock = rdbLoadVectorClock(rdb);
    long long expireTime = RedisModule_LoadSigned(rdb);
    if(expire->data != NULL) { 
        freeCrdtExpireObj(expire->data);
    }
    expire->data = createCrdtExpireObj(createMeta(gid, timestamp,vectorClock), expireTime);
    
    return expire;
}

void RdbSaveCrdtLWWExpire(RedisModuleIO *rdb, void *value) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(value);
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    RedisModule_Debug(logLevel, "save expire data type :%lld, %lld", expire->parent.parent.type, getDataType(expire->parent.parent.type));
    RedisModule_SaveSigned(rdb, getDataType(expire->parent.parent.type));
    RedisModule_SaveSigned(rdb, expire->data->meta->gid);
    RedisModule_SaveSigned(rdb, expire->data->meta->timestamp);
    rdbSaveVectorClock(rdb, expire->data->meta->vectorClock);
    RedisModule_SaveSigned(rdb, expire->data->expireTime);
}

void AofRewriteCrdtLWWExpire(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(value);
    CrdtExpireObj* obj = crdtExpireGetObj(expire);
    sds vclockSds = vectorClockToSds(obj->meta->vectorClock);
    RedisModule_EmitAOF(aof, "CRDT.EXPIRE", "sllcll", key, obj->meta->gid, obj->meta->timestamp, vclockSds, obj->expireTime, getDataType(expire->parent.parent.type) );
    sdsfree(vclockSds);
}
size_t crdtLWWExpireMemUsageFunc(const void *value){
    //todo 
    return 1;
}

void crdtLWWExpireDigestFunc(RedisModuleDigest *md, void *value) {
    //todo 
}

CrdtObject* CrdtLWWExpireFilter(CrdtObject* common, int gid, long long logic_time) {
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(common);
    RedisModule_Debug(logLevel, "filter expire null %lld, %lld", expire->data->meta->gid, gid);
    if(expire->data->meta->gid != gid) {
        return NULL;
    }
    
    VectorClockUnit* unit = getVectorClockUnit(expire->data->meta->vectorClock, gid);
    if(unit->logic_time > logic_time) {
        return crdtExpireDup(expire);
    }  
    return NULL;
}
CrdtObject* CrdtLWWExpireMerge(CrdtObject* target, CrdtObject* other) {
    if(target == NULL) {
        return crdtExpireDup(other);
    }
    CrdtLWWExpire* o = retrieveCrdtLWWExpire(other);
    CrdtLWWExpire* result = crdtExpireDup(target);
    if(compareCrdtMeta(result->data->meta, o->data->meta) > COMPARE_META_EQUAL) {
        result->data->expireTime = o->data->expireTime;
    }
    CrdtMeta* old = result->data->meta;
    result->data->meta = mergeMeta(result->data->meta, o->data->meta);
    freeCrdtMeta(old);
    return result;
}

//expire tombstone 
CrdtLWWExpireTombstone* retrieveCrdtLWWExpireTombstone(void* data) {
    CrdtLWWExpireTombstone* tombstone = (CrdtLWWExpireTombstone*)data;
    return tombstone;
}

int CrdtExpireTombstoneAdd(void* data, CrdtMeta* meta) {
    CrdtLWWExpireTombstone* expire = retrieveCrdtLWWExpireTombstone(data);
    CrdtMeta* old = expire->meta;
    if(old == NULL) {
        expire->meta = dupMeta(meta);
        return COMPARE_META_VECTORCLOCK_GT;
    }
    int result = compareCrdtMeta(old, meta);
    expire->meta = mergeMeta(old, meta);
    freeCrdtMeta(old);
    return result;
}
int CrdtExpireIsExpire(void* data, CrdtMeta* meta) {
    CrdtLWWExpireTombstone* expire = retrieveCrdtLWWExpireTombstone(data);
    return compareCrdtMeta(meta, expire->meta) > COMPARE_META_EQUAL;
}


CrdtLWWExpireTombstone* createCrdtLWWExpireTombstone(int dataType) {
    CrdtLWWExpireTombstone* tombstone = RedisModule_Alloc(sizeof(CrdtLWWExpireTombstone));
    tombstone->parent.parent.type = 0;
    tombstone->parent.parent.type |= CRDT_EXPIRE;
    tombstone->parent.parent.type |= CRDT_TOMBSTONE;
    tombstone->parent.parent.type |= dataType;
    RedisModule_Debug(logLevel, "expire tombstone type %lld %lld", tombstone->parent.parent.type, dataType);
    tombstone->meta = NULL;
    return tombstone;
}
void *RdbLoadCrdtLWWExpireTombstone(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    int dataType = RedisModule_LoadSigned(rdb);
    CrdtLWWExpireTombstone *tombstone = createCrdtLWWExpireTombstone(dataType);
    RedisModule_Debug(logLevel, "load expire tombstone %lld ,%lld", dataType,tombstone->parent.parent.type);
    int gid = RedisModule_LoadSigned(rdb);
    long long timestamp = RedisModule_LoadSigned(rdb);
    VectorClock* vectorClock = rdbLoadVectorClock(rdb);
    tombstone->meta = createMeta(gid, timestamp, vectorClock);
    return tombstone;
}

void RdbSaveCrdtLWWExpireTombstone(RedisModuleIO *rdb, void *value) {
    CrdtLWWExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(value);
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    RedisModule_Debug(logLevel, "save expire tombstone %lld, %lld", getDataType(tombstone->parent.parent.type), tombstone->parent.parent.type);
    RedisModule_SaveSigned(rdb, getDataType(tombstone->parent.parent.type));
    RedisModule_SaveSigned(rdb, tombstone->meta->gid);
    RedisModule_SaveSigned(rdb, tombstone->meta->timestamp);
    rdbSaveVectorClock(rdb, tombstone->meta->vectorClock);
}

void AofRewriteCrdtLWWExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CrdtLWWExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(value);
    sds vclockSds = vectorClockToSds(tombstone->meta->vectorClock);
    RedisModule_EmitAOF(aof, "CRDT.PERSIST", "sllcl", key, tombstone->meta->gid, tombstone->meta->timestamp, vclockSds, getDataType(tombstone->parent.parent.type));
    sdsfree(vclockSds);
}

size_t crdtLWWExpireTombstoneMemUsageFunc(const void *value) {
    //todo 
    return 0;
}
void crdtLWWExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    //todo
}

void freeCrdtLWWExpireTombstone(void* value) {
    CrdtLWWExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(value);
    if(tombstone->meta != NULL) {
        freeCrdtMeta(tombstone->meta);
        tombstone->meta = NULL;
    }
    RedisModule_Free(tombstone);
}
CrdtLWWExpireTombstone* LWWExpireTombstoneDup(CrdtTombstone* target) {
    CrdtLWWExpireTombstone* t = retrieveCrdtLWWExpireTombstone(target);
    CrdtLWWExpireTombstone* result = createCrdtExpireTombstone(getDataType(t->parent.parent.type));
    result->meta = dupMeta(t->meta);
    return result;
}
CrdtTombstone* crdtLWWExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other) {
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if(target == NULL) {
        return LWWExpireTombstoneDup(other);
    }
    if(other == NULL) {
        return LWWExpireTombstoneDup(target);
    }
    CrdtLWWExpireTombstone* t = retrieveCrdtLWWExpireTombstone(target);
    CrdtLWWExpireTombstone* o = retrieveCrdtLWWExpireTombstone(other);
    CrdtLWWExpireTombstone* result = createCrdtExpireTombstone(getDataType(t->parent.parent.type));
    result->meta = mergeMeta(t->meta, o->meta);
    return result;
}

CrdtTombstone* crdtLWWExpireTombstoneFilter(CrdtTombstone* target,int gid, long long logic_time) {
    CrdtLWWExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(target);
    RedisModule_Debug(logLevel, "wwwww %lld, %lld", tombstone->meta->gid , gid);
    if(tombstone->meta->gid != gid) {
        return NULL;
    }
    VectorClockUnit* unit = getVectorClockUnit(tombstone->meta->vectorClock, gid);
    if(unit->logic_time > logic_time) {
        return LWWExpireTombstoneDup(tombstone);
    }  
    return NULL;
}

int crdtLWWExpireTombstonePurage(CrdtTombstone* t, CrdtObject* o) {
    CrdtLWWExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(t);
    CrdtLWWExpire* expire = retrieveCrdtLWWExpire(o);
    if(compareCrdtMeta(expire->data->meta, tombstone->meta) > 0) {
        return 1;
    }
    return 0;
}
int crdtLWWExpireTombstoneGc(void* target, VectorClock* clock) {
    CrdtLWWExpireTombstone* t = retrieveCrdtLWWExpireTombstone(target);
    RedisModule_Debug(logLevel, "gc expire tombstone");
    return isVectorClockMonoIncr(t->meta->vectorClock, clock);
}

int crdtGetExpireTombstoneCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    long long result = -1;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    CrdtLWWExpireTombstone *obj = RedisModule_GetCrdtExpireTombstone(moduleKey);
    if (obj == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithLongLong(ctx, obj->meta->gid);
    RedisModule_ReplyWithLongLong(ctx, obj->meta->timestamp);
    sds vclockSds = vectorClockToSds(obj->meta->vectorClock);
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
}