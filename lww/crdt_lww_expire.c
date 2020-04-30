#include "crdt_lww_expire.h"
#include "../ctrip_crdt_common.h"
#include "../crdt_util.h"
/**
 *  Crdt_LWW_Expire  Get Set Function
 */
int getCrdtLWWExpireGid(Crdt_LWW_Expire* expire) {
    return (int)(expire->gid);
}
void setCrdtLWWExpireGid(Crdt_LWW_Expire* expire, int gid) {
    expire->gid = gid;
}
long long  getCrdtLWWExpireTimestamp(Crdt_LWW_Expire* expire) {
    return expire->timestamp;
}
void setCrdtLWWExpireTimestamp(Crdt_LWW_Expire* expire, long long timestamp) {
    expire->timestamp = timestamp;
}
VectorClock*  getCrdtLWWExpireVectorClock(Crdt_LWW_Expire* expire) {
    if(isNullVectorClock(expire->vc)) return NULL;
    return &expire->vc;
}
void setCrdtLWWExpireVectorClock(Crdt_LWW_Expire* expire, VectorClock* vc) {
    if(getCrdtLWWExpireVectorClock(expire) != NULL) {
        freeInnerClocks(getCrdtLWWExpireVectorClock(expire));
    }
    if(vc == NULL) {
        expire->vc.length = -1;
    }else{
        cloneVectorClock(&expire->vc, vc);
        freeVectorClock(vc);
    }
}
CrdtMeta* getCrdtLWWExpireMeta(Crdt_LWW_Expire* expire) {
    if(getCrdtLWWExpireVectorClock(expire) == NULL) return NULL;
    return &expire->gid;
}
void setCrdtLWWExpireMeta(Crdt_LWW_Expire* expire, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWExpireGid(expire, -1);
        setCrdtLWWExpireTimestamp(expire, -1);
        setCrdtLWWExpireVectorClock(expire, NULL);
    } else {
        setCrdtLWWExpireGid(expire, getMetaGid(meta));
        setCrdtLWWExpireTimestamp(expire,getMetaTimestamp(meta));
        setCrdtLWWExpireVectorClock(expire, dupVectorClock(getMetaVectorClock(meta)));
    }
    freeCrdtMeta(meta);
}
long long getCrdtLWWExpireExpireTime(Crdt_LWW_Expire* expire) {
    return expire->expireTime;
}
void setCrdtLWWExpireExpireTime(Crdt_LWW_Expire* expire, long long expireTime) {
    expire->expireTime = expireTime;
}
/**
 *  Crdt_LWW_ExpireTombstone  Get Set Function
 */
int getCrdtLWWExpireTombstoneGid(Crdt_LWW_ExpireTombstone* t) {
    return t->gid;
}
void setCrdtLWWExpireTombstoneGid(Crdt_LWW_ExpireTombstone* t, int gid) {
    t->gid = gid;
}

long long  getCrdtLWWExpireTombstoneTimestamp(Crdt_LWW_ExpireTombstone* t) {
    return t->timestamp;
}
void setCrdtLWWExpireTombstoneTimestamp(Crdt_LWW_ExpireTombstone* t, long long timestamp) {
    t->timestamp = timestamp;
}
VectorClock* getCrdtLWWExpireTombstoneVectorClock(Crdt_LWW_ExpireTombstone* t) {
    if(isNullVectorClock(t->vectorClock)) return NULL;
    return &t->vectorClock;
}
void setCrdtLWWExpireTombstoneVectorClock(Crdt_LWW_ExpireTombstone* t, VectorClock* vc) {
     if(getCrdtLWWExpireTombstoneVectorClock(t) != NULL) {
        freeInnerClocks(getCrdtLWWExpireTombstoneVectorClock(t));
    }
    if(vc == NULL) {
        t->vectorClock.length = -1;
    }else{
        cloneVectorClock(&t->vectorClock, vc);
        freeVectorClock(vc);
    }
}
CrdtMeta* getCrdtLWWExpireTombstoneMeta(Crdt_LWW_ExpireTombstone* t) {
    if(getCrdtLWWExpireTombstoneVectorClock(t) == NULL) return NULL;
    return (CrdtMeta*)&t->gid;
}
void setCrdtLWWExpireTombstoneMeta(Crdt_LWW_ExpireTombstone* t, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWExpireTombstoneGid(t, -1);
        setCrdtLWWExpireTombstoneTimestamp(t, -1);
        setCrdtLWWExpireTombstoneVectorClock(t, NULL);
    }else{
        setCrdtLWWExpireTombstoneGid(t, getMetaGid(meta));
        setCrdtLWWExpireTombstoneTimestamp(t, getMetaTimestamp(meta));
        setCrdtLWWExpireTombstoneVectorClock(t, dupVectorClock(getMetaVectorClock(meta)));
    }
    freeCrdtMeta(meta);
}
Crdt_LWW_Expire* retrieveCrdtLWWExpire(void* obj) {
    Crdt_LWW_Expire* expire = (Crdt_LWW_Expire*)obj;
    assert(getType(expire->type) == CRDT_EXPIRE);
    return expire;
}

int crdtExpireAddObj(CrdtExpire* obj, CrdtExpireObj* data) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(obj);
    if(getCrdtLWWExpireTombstoneVectorClock(expire) == NULL) {
        setCrdtLWWExpireMeta(expire, dupMeta(data->meta));
        setCrdtLWWExpireExpireTime(expire, data->expireTime);
        return 1;
    }
    int result = compareCrdtMeta(getCrdtLWWExpireMeta(expire), data->meta);
    setCrdtLWWExpireMeta(expire, mergeMeta(getCrdtLWWExpireMeta(expire), data->meta));
    if(result > 0) {
        setCrdtLWWExpireExpireTime(expire, data->expireTime);
    }
    return result;
}


CrdtExpireObj* crdtExpireGetObj(CrdtExpire* obj) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(obj);
    return createCrdtExpireObj(dupMeta(getCrdtLWWExpireMeta(expire)), getCrdtLWWExpireExpireTime(expire));
}

CrdtExpire* crdtExpireDup(CrdtExpire* obj) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(obj);
    Crdt_LWW_Expire* copy = createCrdtLWWExpire();
    copy->type = expire->type;
    setCrdtLWWExpireGid(copy, getCrdtLWWExpireGid(expire));
    setCrdtLWWExpireTimestamp(copy, getCrdtLWWExpireTimestamp(expire));
    setCrdtLWWExpireVectorClock(copy, dupVectorClock(getCrdtLWWExpireVectorClock(expire)));
    setCrdtLWWExpireExpireTime(copy, getCrdtExpireLastExpireTime(expire));
    return copy;
}

void crdtExpireFree(CrdtExpire* obj) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(obj);
    setCrdtLWWExpireVectorClock(expire ,NULL);
    RedisModule_Free(expire);
}


Crdt_LWW_Expire* createCrdtLWWExpire(void) {
    Crdt_LWW_Expire* expire = RedisModule_Alloc(sizeof(Crdt_LWW_Expire));
    expire->type = 0;
    setType((CrdtObject*)expire, CRDT_EXPIRE); 
    expire->vc.length = -1;
    expire->expireTime = -1;
    expire->gid = -1;
    expire->timestamp = -1;
    return expire;
}


void *RdbLoadCrdtLWWExpire(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    Crdt_LWW_Expire *expire = createCrdtLWWExpire();
    int type = RedisModule_LoadSigned(rdb);
    setDataType(expire, type);
    setCrdtLWWExpireGid(expire, RedisModule_LoadSigned(rdb));
    setCrdtLWWExpireTimestamp(expire, RedisModule_LoadSigned(rdb));
    setCrdtLWWExpireVectorClock(expire, rdbLoadVectorClock(rdb));
    long long t = RedisModule_LoadSigned(rdb);
    setCrdtLWWExpireExpireTime(expire, t);
    return expire;
}

void RdbSaveCrdt_LWW_Expire(RedisModuleIO *rdb, void *value) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(value);
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    RedisModule_SaveSigned(rdb, getDataType(expire->type));
    RedisModule_SaveSigned(rdb, getCrdtLWWExpireGid(expire));
    RedisModule_SaveSigned(rdb, getCrdtLWWExpireTimestamp(expire));
    rdbSaveVectorClock(rdb, getCrdtLWWExpireVectorClock(expire));
    RedisModule_SaveSigned(rdb, getCrdtLWWExpireExpireTime(expire));
}

void AofRewriteCrdtLWWExpire(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(value);
    sds vclockSds = vectorClockToSds(getCrdtExpireLastVectorClock(expire));
    RedisModule_EmitAOF(aof, "CRDT.EXPIRE", "sllcll", key, getCrdtLWWExpireGid(expire), getCrdtLWWExpireTimestamp(expire), vclockSds, getCrdtLWWExpireTimestamp(expire), getDataType(expire->type) );
    sdsfree(vclockSds);
}
size_t Crdt_LWW_ExpireMemUsageFunc(const void *value){
    //todo 
    return 1;
}

void Crdt_LWW_ExpireDigestFunc(RedisModuleDigest *md, void *value) {
    //todo 
}

CrdtObject* Crdt_LWW_ExpireFilter(CrdtObject* common, int gid, long long logic_time) {
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(common);
    if(getCrdtLWWExpireGid(expire) != gid) {
        return NULL;
    }
    
    VectorClockUnit* unit = getVectorClockUnit(getCrdtLWWExpireVectorClock(expire), gid);
    if(unit == NULL) return NULL;
    long long vcu = get_logic_clock(*unit);
    if(vcu > logic_time) {
        return crdtExpireDup(expire);
    }  
    return NULL;
}
CrdtObject* Crdt_LWW_ExpireMerge(CrdtObject* target, CrdtObject* other) {
    if(target == NULL) {
        return crdtExpireDup(other);
    }
    Crdt_LWW_Expire* o = retrieveCrdtLWWExpire(other);
    Crdt_LWW_Expire* result = crdtExpireDup(target);
    if(compareCrdtMeta(getCrdtLWWExpireMeta(result), getCrdtLWWExpireMeta(o)) > COMPARE_META_EQUAL) {
        setCrdtLWWExpireExpireTime(result, 
            getCrdtLWWExpireExpireTime(o));
    }
    CrdtMeta* m = mergeMeta(getCrdtLWWExpireMeta(result), getCrdtLWWExpireMeta(o));
    setCrdtLWWExpireMeta(result, m);
    return result;
}

//expire tombstone 
Crdt_LWW_ExpireTombstone* retrieveCrdtLWWExpireTombstone(void* data) {
    Crdt_LWW_ExpireTombstone* tombstone = (Crdt_LWW_ExpireTombstone*)data;
    return tombstone;
}

int CrdtExpireTombstoneAdd(void* data, CrdtMeta* meta) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(data);
    if(getCrdtLWWExpireTombstoneVectorClock(tombstone) == NULL) {
        setCrdtLWWExpireTombstoneMeta(tombstone, dupMeta(meta));
        return COMPARE_META_VECTORCLOCK_GT;
    }
    int result = compareCrdtMeta(getCrdtLWWExpireTombstoneMeta(tombstone), meta);
    CrdtMeta* m = mergeMeta(getCrdtLWWExpireTombstoneMeta(tombstone), meta);
    setCrdtLWWExpireTombstoneMeta(tombstone, m);
    return result;
}
int CrdtExpireIsExpire(void* data, CrdtMeta* meta) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(data);
    return compareCrdtMeta(meta, getCrdtLWWExpireTombstoneMeta(tombstone)) > COMPARE_META_EQUAL;
}


Crdt_LWW_ExpireTombstone* createCrdtLWWExpireTombstone(int dataType) {
    Crdt_LWW_ExpireTombstone* tombstone = RedisModule_Alloc(sizeof(Crdt_LWW_ExpireTombstone));
    tombstone->type = 0;
    setType((CrdtObject*)tombstone,  CRDT_EXPIRE_TOMBSTONE);
    setDataType((CrdtObject*)tombstone, dataType);
    tombstone->vectorClock.length = -1;
    tombstone->gid = -1;
    tombstone->timestamp = -1;
    return tombstone;
}
void *RdbLoadCrdtLWWExpireTombstone(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    int dataType = RedisModule_LoadSigned(rdb);
    Crdt_LWW_ExpireTombstone *tombstone = createCrdtLWWExpireTombstone(dataType);
    setCrdtLWWExpireTombstoneGid(tombstone, RedisModule_LoadSigned(rdb));
    setCrdtLWWExpireTombstoneTimestamp(tombstone, RedisModule_LoadSigned(rdb));
    setCrdtLWWExpireTombstoneVectorClock(tombstone, rdbLoadVectorClock(rdb));
    return tombstone;
}

void RdbSaveCrdt_LWW_ExpireTombstone(RedisModuleIO *rdb, void *value) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(value);
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    RedisModule_SaveSigned(rdb, getDataType(tombstone->type));
    RedisModule_SaveSigned(rdb, getCrdtLWWExpireTombstoneGid(tombstone));
    RedisModule_SaveSigned(rdb, getCrdtLWWExpireTombstoneTimestamp(tombstone));
    rdbSaveVectorClock(rdb, getCrdtLWWExpireTombstoneVectorClock(tombstone));
}

void AofRewriteCrdtLWWExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(value);
    sds vclockSds = vectorClockToSds(getCrdtLWWExpireTombstoneVectorClock(tombstone));
    RedisModule_EmitAOF(aof, "CRDT.PERSIST", "sllcl", key, getCrdtLWWExpireTombstoneGid(tombstone), getCrdtLWWExpireTombstoneTimestamp(tombstone), vclockSds, getDataType(tombstone->type));
    sdsfree(vclockSds);
}

size_t Crdt_LWW_ExpireTombstoneMemUsageFunc(const void *value) {
    //todo 
    return 0;
}
void Crdt_LWW_ExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    //todo
}

void freeCrdt_LWW_ExpireTombstone(void* value) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(value);
    setCrdtLWWExpireTombstoneVectorClock(tombstone, NULL);
    RedisModule_Free(tombstone);
}
Crdt_LWW_ExpireTombstone* LWWExpireTombstoneDup(CrdtTombstone* target) {
    Crdt_LWW_ExpireTombstone* t = retrieveCrdtLWWExpireTombstone(target);
    Crdt_LWW_ExpireTombstone* result = createCrdtExpireTombstone(getDataType(t->type));
    setCrdtLWWExpireTombstoneGid(result, getCrdtLWWExpireTombstoneGid(t));
    setCrdtLWWExpireTombstoneTimestamp(result, getCrdtLWWExpireTombstoneTimestamp(t));
    setCrdtLWWExpireTombstoneVectorClock(result, dupVectorClock(getCrdtLWWExpireTombstoneVectorClock(t)));
    return result;
}
CrdtTombstone* Crdt_LWW_ExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other) {
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if(target == NULL) {
        return LWWExpireTombstoneDup(other);
    }
    if(other == NULL) {
        return LWWExpireTombstoneDup(target);
    }
    Crdt_LWW_ExpireTombstone* t = retrieveCrdtLWWExpireTombstone(target);
    Crdt_LWW_ExpireTombstone* o = retrieveCrdtLWWExpireTombstone(other);
    Crdt_LWW_ExpireTombstone* result = createCrdtExpireTombstone(getDataType(t->type));
    CrdtMeta* meta = mergeMeta(getCrdtLWWExpireTombstoneMeta(t), getCrdtLWWExpireTombstoneMeta(o));
    setCrdtLWWExpireTombstoneMeta(t, meta);
    return result;
}

CrdtTombstone* Crdt_LWW_ExpireTombstoneFilter(CrdtTombstone* target,int gid, long long logic_time) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(target);
    if(getCrdtLWWExpireTombstoneGid(tombstone) != gid) {
        return NULL;
    }
    VectorClockUnit* unit = getVectorClockUnit(getCrdtLWWExpireTombstoneVectorClock(tombstone), gid);
    if(unit == NULL) return NULL;
    long long vcu = get_logic_clock(*unit);
    if(vcu > logic_time) {
        return LWWExpireTombstoneDup(tombstone);
    }  
    return NULL;
}

int Crdt_LWW_ExpireTombstonePurage(CrdtTombstone* t, CrdtObject* o) {
    Crdt_LWW_ExpireTombstone* tombstone = retrieveCrdtLWWExpireTombstone(t);
    Crdt_LWW_Expire* expire = retrieveCrdtLWWExpire(o);
    if(compareCrdtMeta(getCrdtLWWExpireMeta(expire), getCrdtLWWExpireTombstoneMeta(tombstone)) > 0) {
        return 1;
    }
    return 0;
}
int Crdt_LWW_ExpireTombstoneGc(void* target, VectorClock* clock) {
    Crdt_LWW_ExpireTombstone* t = retrieveCrdtLWWExpireTombstone(target);
    return isVectorClockMonoIncr(getCrdtLWWExpireTombstoneVectorClock(t), clock);
}

int crdtGetExpireTombstoneCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    long long result = -1;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    Crdt_LWW_ExpireTombstone *tombstone = RedisModule_GetCrdtExpireTombstone(moduleKey);
    if (tombstone == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithLongLong(ctx, getCrdtLWWExpireTombstoneGid(tombstone));
    RedisModule_ReplyWithLongLong(ctx, getCrdtLWWExpireTombstoneTimestamp(tombstone));
    sds vclockSds = vectorClockToSds(getCrdtLWWExpireTombstoneVectorClock(tombstone));
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
}