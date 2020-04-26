#include "crdt_lww_hashmap.h"

/**
 * createHash
 */
int changeCrdtLWWHash(CRDT_Hash* hash, CrdtMeta* meta) {
    struct CRDT_LWW_Hash* map = (struct CRDT_LWW_Hash*)hash;
    VectorClock* old = map->lastVc;
    map->lastVc = vectorClockMerge(old, meta->vectorClock);
    freeVectorClock(meta->vectorClock);
    meta->vectorClock = dupVectorClock(map->lastVc);
    if(old) freeVectorClock(old);
    return CRDT_OK;
}
CRDT_Hash* dupCrdtLWWHash(void* data) {
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(data);
    CRDT_LWW_Hash* result = createCrdtLWWHash();
    result->lastVc = dupVectorClock(crdtHash->lastVc);
    if (dictSize(crdtHash->parent.map)) {
        dictIterator *di = dictGetIterator(crdtHash->parent.map);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            CRDT_Register *crdtRegister = dictGetVal(de);

            dictAdd(result->parent.map, sdsdup(field), dupCrdtRegister(crdtRegister));
        }
        dictReleaseIterator(di);
    }
    return result;
}
VectorClock* getLastVcLWWHash(void* data) {
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(data);
    return crdtHash->lastVc;
}
void updateLastVCLWWHash(void* data, VectorClock* vc) {
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(data);
    VectorClock* old = crdtHash->lastVc;
    crdtHash->lastVc = vectorClockMerge(old, vc);
    freeVectorClock(old);
}


void* createCrdtLWWHash() {
    CRDT_LWW_Hash *crdtHash = RedisModule_Alloc(sizeof(CRDT_LWW_Hash));
    crdtHash->parent.parent.parent.type = 0;
    crdtHash->parent.parent.parent.type |= CRDT_DATA;
    crdtHash->parent.parent.parent.type |= CRDT_HASH_TYPE;
    dict *hash = dictCreate(&crdtHashDictType, NULL);
    crdtHash->parent.map = hash;
    crdtHash->lastVc = NULL;
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
    appendCrdtMeta(target->maxDelMeta, meta);
    return target->maxDelMeta;
}
int isExpireCrdtLWWHashTombstone(void* data, CrdtMeta* meta) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    return compareCrdtMeta(meta,target->maxDelMeta) > COMPARE_META_EQUAL? CRDT_OK: CRDT_NO;
}
CRDT_HashTombstone* dupCrdtLWWHashTombstone(void* data) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    CRDT_LWW_HashTombstone* result = createCrdtHashTombstone();
    freeCrdtMeta(result->maxDelMeta);
    result->maxDelMeta = dupMeta(target->maxDelMeta);
    if (dictSize(target->parent.map)) {
        dictIterator *di = dictGetIterator(target->parent.map);
        dictEntry *de;
        while ((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
            dictAdd(result->parent.map, sdsdup(field), dupCrdtRegisterTombstone(crdtRegisterTombstone));
        }
        dictReleaseIterator(di);
    }
    return result;
}
int gcCrdtLWWHashTombstone(void* data, VectorClock* clock) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    if(isVectorClockMonoIncr(target->lastVc, clock) == CRDT_OK) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
CrdtMeta* getMaxDelCrdtLWWHashTombstone(void* data) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    return target->maxDelMeta;
}
int changeCrdtLWWHashTombstone(void* data, CrdtMeta* meta) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    VectorClock* old = target->lastVc;
    target->lastVc = vectorClockMerge(target->lastVc, meta->vectorClock);
    if(old) freeVectorClock(old);
    return CRDT_OK;
}
static CrdtHashTombstoneMethod LWW_Hash_Tombstone_Methods = {
    .updateMaxDel = updateMaxDelCrdtLWWHashTombstone,
    .isExpire = isExpireCrdtLWWHashTombstone,
    .dup = dupCrdtLWWHashTombstone,
    .gc = gcCrdtLWWHashTombstone,
    .getMaxDel = getMaxDelCrdtLWWHashTombstone,
    .change = changeCrdtLWWHashTombstone
};
void* createCrdtLWWHashTombstone() {
    CRDT_LWW_HashTombstone *crdtHashTombstone = RedisModule_Alloc(sizeof(CRDT_LWW_HashTombstone));
    crdtHashTombstone->maxDelMeta = createMeta(-1, -1, NULL);
    crdtHashTombstone->parent.parent.parent.type = 0;
    crdtHashTombstone->parent.parent.parent.type |= CRDT_HASH_TYPE;
    crdtHashTombstone->parent.parent.parent.type |= CRDT_DATA;
    crdtHashTombstone->parent.parent.parent.type |= CRDT_TOMBSTONE;
    RedisModule_Debug(logLevel, "hash tombstone type %lld", crdtHashTombstone->parent.parent.parent.type);
    crdtHashTombstone->lastVc = NULL;
    dict *hash = dictCreate(&crdtHashTombstoneDictType, NULL);
    crdtHashTombstone->parent.map = hash;
    return crdtHashTombstone;
}
CRDT_LWW_HashTombstone* retrieveCrdtLWWHashTombstone(void* data) {
    if(data == NULL) return NULL;
    CRDT_LWW_HashTombstone* result = (CRDT_LWW_HashTombstone*)data;
    assert(result->parent.parent.type & CRDT_HASH_TYPE);
    assert(result->parent.method == &LWW_Hash_Tombstone_Methods);
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
    crdtHash->lastVc = rdbLoadVectorClock(rdb);
    if(RdbLoadCrdtBasicHash(rdb, encver, &crdtHash->parent) == CRDT_NO) return NULL;
    return crdtHash;
}
void RdbSaveCrdtLWWHash(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_Hash *crdtHash = retrieveCrdtLWWHash(value);
    rdbSaveVectorClock(rdb, crdtHash->lastVc);
    RdbSaveCrdtBasicHash(rdb, &crdtHash->parent);
}
void AofRewriteCrdtLWWHash(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    //todo: currently do nothing when aof
}
void freeCrdtLWWHash(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(obj); 
    if(crdtHash->parent.map != NULL) {dictRelease(crdtHash->parent.map);}
    freeVectorClock(crdtHash->lastVc);
    RedisModule_Free(crdtHash);
}
size_t crdtLWWHashMemUsageFunc(const void *value) {
    CRDT_LWW_Hash *crdtHash = retrieveCrdtLWWHash((void*)value);
    size_t valSize = sizeof(CRDT_LWW_Hash) + crdtBasicHashMemUsageFunc(&crdtHash->parent);
    int vclcokNum = crdtHash->lastVc->length;
    size_t vclockSize = vclcokNum * sizeof(VectorClockUnit) + sizeof(VectorClock);
    return valSize + vclockSize;
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
        crdtHashTombstone->maxDelMeta->gid = RedisModule_LoadSigned(rdb);
        crdtHashTombstone->maxDelMeta->timestamp = RedisModule_LoadSigned(rdb);
        crdtHashTombstone->maxDelMeta->vectorClock = rdbLoadVectorClock(rdb);
    }
    crdtHashTombstone->lastVc = rdbLoadVectorClock(rdb);
    if(RdbLoadCrdtBasicHashTombstone(rdb, encver, &crdtHashTombstone->parent) == CRDT_NO) return NULL;
    return crdtHashTombstone;
}
void RdbSaveCrdtLWWHashTombstone(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_HashTombstone *crdtHashTombstone = retrieveCrdtLWWHashTombstone(value);
    if(crdtHashTombstone->maxDelMeta->vectorClock == NULL) {
        RedisModule_SaveSigned(rdb, NO_HASH_MAXDEL);
    }else{
        RedisModule_SaveSigned(rdb, HASH_MAXDEL);
        RedisModule_SaveSigned(rdb, crdtHashTombstone->maxDelMeta->gid);
        RedisModule_SaveSigned(rdb, crdtHashTombstone->maxDelMeta->timestamp);
        rdbSaveVectorClock(rdb, crdtHashTombstone->maxDelMeta->vectorClock);
    }
    rdbSaveVectorClock(rdb, crdtHashTombstone->lastVc);
    
    RdbSaveCrdtBasicHashTombstone(rdb, &crdtHashTombstone->parent);
}
void AofRewriteCrdtLWWHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    //todo: currently do nothing when aof
}
void freeCrdtLWWHashTombstone(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_HashTombstone* crdtHash = retrieveCrdtLWWHashTombstone(obj); 
    if(crdtHash->parent.map != NULL) {
        dictRelease(crdtHash->parent.map);
        crdtHash->parent.map = NULL;
    }
    freeVectorClock(crdtHash->lastVc);
    freeCrdtMeta(crdtHash->maxDelMeta);
    RedisModule_Free(crdtHash);
}
size_t crdtLWWHashTombstoneMemUsageFunc(const void *value) {
    CRDT_LWW_HashTombstone *crdtHash = retrieveCrdtLWWHashTombstone((void*)value);
    size_t valSize = sizeof(CRDT_LWW_HashTombstone) + crdtBasicHashTombstoneMemUsageFunc(&crdtHash->parent);
    int vclcokNum = crdtHash->maxDelMeta->vectorClock->length;
    size_t vclockSize = vclcokNum * sizeof(VectorClockUnit) + sizeof(VectorClock);
    return valSize + vclockSize;
}
void crdtLWWHashTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    //todo: currently do nothing when digest
}