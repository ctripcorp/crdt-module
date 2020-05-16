#include "crdt_lww_hashmap.h"
/**
 *  LWW Hash Get Set Function
 */ 
VectorClock getCrdtLWWHashLastVc(CRDT_LWW_Hash* r) {
    return r->lastVc;
}
void setCrdtLWWHashLastVc(CRDT_LWW_Hash* r, VectorClock vc) {
    if(!isNullVectorClock(getCrdtLWWHashLastVc(r))) {
        freeInnerClocks(getCrdtLWWHashLastVc(r));
    } 
    r->lastVc = vc;
}
/**
 *  LWW Hash TOMBSTONE Get Set  Function
 */ 
int getCrdtLWWHashTombstoneMaxDelGid(CRDT_LWW_HashTombstone* t) {
    return (t->maxDelGid);
}
void setCrdtLWWHashTombstoneMaxDelGid(CRDT_LWW_HashTombstone* t, int gid) {
    t->maxDelGid = gid;
}
long long getCrdtLWWHashTombstoneMaxDelTimestamp(CRDT_LWW_HashTombstone* t) {
    return t->maxDelTimestamp;
}
void setCrdtLWWHashTombstoneMaxDelTimestamp(CRDT_LWW_HashTombstone* t, long long time) {
    t->maxDelTimestamp = time;
}
VectorClock getCrdtLWWHashTombstoneMaxDelVectorClock(CRDT_LWW_HashTombstone* t) {
    return t->maxDelvectorClock;
}
void setCrdtLWWHashTombstoneMaxDelVectorClock(CRDT_LWW_HashTombstone* t, VectorClock vc) {
    if(!isNullVectorClock(getCrdtLWWHashTombstoneMaxDelVectorClock(t))) {
        freeInnerClocks(getCrdtLWWHashTombstoneMaxDelVectorClock(t));
    } 
    t->maxDelvectorClock = vc;
}
CrdtMeta* getCrdtLWWHashTombstoneMaxDelMeta(CRDT_LWW_HashTombstone* t) {
    if(isNullVectorClock(getCrdtLWWHashTombstoneMaxDelVectorClock(t))) return NULL;
    return (CrdtMeta*)t;
}
void setCrdtLWWHashTombstoneMaxDelMeta(CRDT_LWW_HashTombstone* t, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWHashTombstoneMaxDelGid(t, -1);
        setCrdtLWWHashTombstoneMaxDelTimestamp(t, -1);
        setCrdtLWWHashTombstoneMaxDelVectorClock(t, LOGIC_CLOCK_UNDEFINE);
    }else{
        setCrdtLWWHashTombstoneMaxDelGid(t, getMetaGid(meta));
        setCrdtLWWHashTombstoneMaxDelTimestamp(t, getMetaTimestamp(meta));
        setCrdtLWWHashTombstoneMaxDelVectorClock(t, dupVectorClock(getMetaVectorClock(meta)));
    }
    
    freeCrdtMeta(meta);
}
VectorClock getCrdtLWWHashTombstoneLastVc(CRDT_LWW_HashTombstone* t) {
    return t->lastVc;
}
void setCrdtLWWHashTombstoneLastVc(CRDT_LWW_HashTombstone* t, VectorClock vc) {
    if(!isNullVectorClock(getCrdtLWWHashTombstoneLastVc(t))) {
        freeInnerClocks(getCrdtLWWHashTombstoneLastVc(t));
    } 
    t->lastVc = vc; 
}
/**
 * createHash
 */
int changeCrdtLWWHash(CRDT_Hash* hash, CrdtMeta* meta) {
    struct CRDT_LWW_Hash* map = (struct CRDT_LWW_Hash*)hash;
    setCrdtLWWHashLastVc(map , vectorClockMerge(getCrdtLWWHashLastVc(map), getMetaVectorClock(meta)));
    setMetaVectorClock(meta, dupVectorClock(getCrdtLWWHashLastVc(map)));
    return CRDT_OK;
}
CRDT_Hash* dupCrdtLWWHash(void* data) {
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(data);
    CRDT_LWW_Hash* result = createCrdtLWWHash();
    setCrdtLWWHashLastVc(result, dupVectorClock(getCrdtLWWHashLastVc(crdtHash)));
    if (dictSize(crdtHash->map)) {
        dictIterator *di = dictGetIterator(crdtHash->map);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            CRDT_Register *crdtRegister = dictGetVal(de);

            dictAdd(result->map, sdsdup(field), dupCrdtRegister(crdtRegister));
        }
        dictReleaseIterator(di);
    }
    return (CRDT_Hash*)result;
}

void updateLastVCLWWHash(void* data, VectorClock vc) {
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(data);
    setCrdtLWWHashLastVc(crdtHash, vectorClockMerge(getCrdtLWWHashLastVc(crdtHash), vc));
}


void* createCrdtLWWHash() {
    CRDT_LWW_Hash *crdtHash = RedisModule_Alloc(sizeof(CRDT_LWW_Hash));
    crdtHash->type = 0;
    setType((CrdtObject*)crdtHash , CRDT_DATA);
    setDataType((CrdtObject*)crdtHash , CRDT_HASH_TYPE);
    dict *hash = dictCreate(&crdtHashDictType, NULL);
    crdtHash->map = hash;
    crdtHash->lastVc = LOGIC_CLOCK_UNDEFINE;
    return crdtHash;
}

CRDT_LWW_Hash* retrieveCrdtLWWHash(void* obj) {
    if(obj == NULL) return NULL;
    CRDT_LWW_Hash* result = (CRDT_LWW_Hash*)obj;
    assert(result->parent.parent.type & CRDT_HASH_TYPE);
    return result;
} 
//hash lww tombstone
CrdtMeta* updateMaxDelCrdtLWWHashTombstone(void* data, CrdtMeta* meta) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    setCrdtLWWHashTombstoneMaxDelMeta(target, mergeMeta(getCrdtLWWHashTombstoneMaxDelMeta(target), meta));
    return getCrdtLWWHashTombstoneMaxDelMeta(target);
}
int isExpireCrdtLWWHashTombstone(void* data, CrdtMeta* meta) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    return compareCrdtMeta(meta,getCrdtLWWHashTombstoneMaxDelMeta(target)) > COMPARE_META_EQUAL? CRDT_OK: CRDT_NO;
}
CRDT_HashTombstone* dupCrdtLWWHashTombstone(void* data) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    CRDT_LWW_HashTombstone* result = createCrdtHashTombstone();
    setCrdtLWWHashTombstoneMaxDelMeta(result, dupMeta(getCrdtLWWHashTombstoneMaxDelMeta(target)));
    setCrdtLWWHashTombstoneLastVc(result, dupVectorClock(getCrdtLWWHashTombstoneLastVc(target)));
    if (dictSize(target->map)) {
        dictIterator *di = dictGetIterator(target->map);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
            dictAdd(result->map, sdsdup(field), dupCrdtRegisterTombstone(crdtRegisterTombstone));
        }
        dictReleaseIterator(di);
    }
    return (CRDT_HashTombstone*)result;
}
int gcCrdtLWWHashTombstone(void* data, VectorClock clock) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    if(isVectorClockMonoIncr(getCrdtLWWHashTombstoneLastVc(target), clock) == CRDT_OK) {
        return CRDT_OK;
    }
    return CRDT_NO;
}

int changeCrdtLWWHashTombstone(void* data, CrdtMeta* meta) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    setCrdtLWWHashTombstoneLastVc(target, vectorClockMerge(getCrdtLWWHashTombstoneLastVc(target), getMetaVectorClock(meta)));
    return CRDT_OK;
}

void* createCrdtLWWHashTombstone() {
    CRDT_LWW_HashTombstone *crdtHashTombstone = RedisModule_Alloc(sizeof(CRDT_LWW_HashTombstone));
    crdtHashTombstone->type = 0;
    crdtHashTombstone->maxDelGid = 0;
    crdtHashTombstone->maxDelTimestamp = 0;
    setDataType((CrdtObject*)crdtHashTombstone, CRDT_HASH_TYPE);
    setType((CrdtObject*)crdtHashTombstone,  CRDT_TOMBSTONE);
    dict *hash = dictCreate(&crdtHashTombstoneDictType, NULL);
    crdtHashTombstone->map = hash;
    crdtHashTombstone->maxDelvectorClock = LOGIC_CLOCK_UNDEFINE;
    crdtHashTombstone->lastVc = LOGIC_CLOCK_UNDEFINE;
    return crdtHashTombstone;
}
CRDT_LWW_HashTombstone* retrieveCrdtLWWHashTombstone(void* data) {
    if(data == NULL) return NULL;
    CRDT_LWW_HashTombstone* result = (CRDT_LWW_HashTombstone*)data;
    assert(getDataType(result) == CRDT_HASH_TYPE);
    return result;
}






/**
 * Hash Module API
 */
void *RdbLoadCrdtLWWHash(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_Hash *crdtHash = createCrdtLWWHash();
    setCrdtLWWHashLastVc(crdtHash, rdbLoadVectorClock(rdb));
    if(RdbLoadCrdtBasicHash(rdb, encver, crdtHash) == CRDT_NO) return NULL;
    return crdtHash;
}
void RdbSaveCrdtLWWHash(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_Hash *crdtHash = retrieveCrdtLWWHash(value);
    rdbSaveVectorClock(rdb, getCrdtLWWHashLastVc(crdtHash));
    RdbSaveCrdtBasicHash(rdb, crdtHash);
}
void AofRewriteCrdtLWWHash(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    //todo: currently do nothing when aof
}
void freeCrdtLWWHash(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(obj); 
    if(crdtHash->map != NULL) {dictRelease(crdtHash->map);}
    setCrdtLWWHashLastVc(crdtHash, LOGIC_CLOCK_UNDEFINE);
    RedisModule_Free(crdtHash);
}
size_t crdtLWWHashMemUsageFunc(const void *value) {
    return 1;
}
void crdtLWWHashDigestFunc(RedisModuleDigest *md, void *value) {
    //todo: currently do nothing when digest
}

/**
 * Hash Tombstone Module API
 */
#define HASH_MAXDEL 1
#define NO_HASH_MAXDEL 0
void *RdbLoadCrdtLWWHashTombstone(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_HashTombstone *crdtHashTombstone = createCrdtLWWHashTombstone();
    if(RedisModule_LoadSigned(rdb) == HASH_MAXDEL) {
        int gid = RedisModule_LoadSigned(rdb);
        if(RedisModule_CheckGid(gid) == REDISMODULE_ERR) {
            return NULL;
        }
        setCrdtLWWHashTombstoneMaxDelGid(crdtHashTombstone, gid);
        setCrdtLWWHashTombstoneMaxDelTimestamp(crdtHashTombstone, RedisModule_LoadSigned(rdb));
        setCrdtLWWHashTombstoneMaxDelVectorClock(crdtHashTombstone, rdbLoadVectorClock(rdb));
    }
    setCrdtLWWHashTombstoneLastVc(crdtHashTombstone, rdbLoadVectorClock(rdb));
    if(RdbLoadCrdtBasicHashTombstone(rdb, encver, crdtHashTombstone) == CRDT_NO) return NULL;
    return crdtHashTombstone;
}
void RdbSaveCrdtLWWHashTombstone(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_HashTombstone *crdtHashTombstone = retrieveCrdtLWWHashTombstone(value);
    if(isNullVectorClock(getCrdtLWWHashTombstoneMaxDelVectorClock(crdtHashTombstone))) {
        RedisModule_SaveSigned(rdb, NO_HASH_MAXDEL);
    }else{
        RedisModule_SaveSigned(rdb, HASH_MAXDEL);
        RedisModule_SaveSigned(rdb, getCrdtLWWHashTombstoneMaxDelGid(crdtHashTombstone));
        RedisModule_SaveSigned(rdb, getCrdtLWWHashTombstoneMaxDelTimestamp(crdtHashTombstone));
        rdbSaveVectorClock(rdb, getCrdtLWWHashTombstoneMaxDelVectorClock(crdtHashTombstone));
    }
    rdbSaveVectorClock(rdb, getCrdtLWWHashTombstoneLastVc(crdtHashTombstone));
    
    RdbSaveCrdtBasicHashTombstone(rdb, crdtHashTombstone);
}
void AofRewriteCrdtLWWHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    //todo: currently do nothing when aof
}
void freeCrdtLWWHashTombstone(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_HashTombstone* crdtHash = retrieveCrdtLWWHashTombstone(obj); 
    if(crdtHash->map != NULL) {
        dictRelease(crdtHash->map);
        crdtHash->map = NULL;
    }
    setCrdtLWWHashTombstoneLastVc(crdtHash, LOGIC_CLOCK_UNDEFINE);
    setCrdtLWWHashTombstoneMaxDelVectorClock(crdtHash, LOGIC_CLOCK_UNDEFINE);
    RedisModule_Free(crdtHash);
}
size_t crdtLWWHashTombstoneMemUsageFunc(const void *value) {
    return 1;
}
void crdtLWWHashTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    //todo: currently do nothing when digest
}