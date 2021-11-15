#include "crdt_lww_hashmap.h"
/**
 *  LWW Hash Get Set Function
 */ 
VectorClock getCrdtHashLastVc(CRDT_Hash* hash) {
    CRDT_LWW_Hash* r = retrieveCrdtLWWHash(hash);
    return r->lastVc;
}

void setCrdtHashLastVc(CRDT_Hash* hash, VectorClock vc) {
    CRDT_LWW_Hash* r = retrieveCrdtLWWHash(hash);
    VectorClock old = getCrdtHashLastVc(hash);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    r->lastVc = vc;
}

void mergeCrdtHashLastVc(CRDT_Hash* hash, VectorClock vc) {
    VectorClock old = getCrdtHashLastVc(hash);
    VectorClock now = vectorClockMerge(old, vc);
    setCrdtHashLastVc(hash, now);
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
        freeVectorClock(getCrdtLWWHashTombstoneMaxDelVectorClock(t));
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
        setCrdtLWWHashTombstoneMaxDelVectorClock(t, newVectorClock(0));
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
        freeVectorClock(getCrdtLWWHashTombstoneLastVc(t));
    } 
    t->lastVc = vc; 
}
/**
 * createHash
 */
int changeCrdtLWWHash(CRDT_Hash* hash, CrdtMeta* meta) {
    struct CRDT_LWW_Hash* map = (struct CRDT_LWW_Hash*)hash;
    setCrdtHashLastVc((CRDT_Hash*)map , vectorClockMerge(getCrdtHashLastVc((CRDT_Hash*)map), getMetaVectorClock(meta)));
    setMetaVectorClock(meta, dupVectorClock(getCrdtHashLastVc((CRDT_Hash*)map)));
    return CRDT_OK;
}
CRDT_Hash* dupCrdtLWWHash(void* data) {
    CRDT_LWW_Hash* crdtHash = retrieveCrdtLWWHash(data);
    CRDT_LWW_Hash* result = createCrdtLWWHash();
    setCrdtHashLastVc((CRDT_Hash*)result, dupVectorClock(getCrdtHashLastVc((CRDT_Hash*)crdtHash)));
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
    setCrdtHashLastVc((CRDT_Hash*)crdtHash, vectorClockMerge(getCrdtHashLastVc((CRDT_Hash*)crdtHash), vc));
}

void* createCrdtLWWHash() {
    CRDT_LWW_Hash *crdtHash = RedisModule_Alloc(sizeof(CRDT_LWW_Hash));
    initCrdtObject((CrdtObject*)crdtHash);
    setType((CrdtObject*)crdtHash , CRDT_DATA);
    setDataType((CrdtObject*)crdtHash , CRDT_HASH_TYPE);
    dict *hash = dictCreate(&crdtHashDictType, NULL);
    crdtHash->map = hash;
    crdtHash->lastVc = newVectorClock(0);
    return crdtHash;
}

CRDT_LWW_Hash* retrieveCrdtLWWHash(void* obj) {
    if(obj == NULL) return NULL;
    CRDT_LWW_Hash* result = (CRDT_LWW_Hash*)obj;
    assert(result->parent.parent.type & CRDT_HASH_TYPE);
    return result;
} 
//hash lww tombstone
CrdtMeta* updateMaxDelCrdtLWWHashTombstone(void* data, CrdtMeta* meta,int* compare) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    setCrdtLWWHashTombstoneMaxDelMeta(target, mergeMeta(getCrdtLWWHashTombstoneMaxDelMeta(target), meta, compare));
    return getCrdtLWWHashTombstoneMaxDelMeta(target);
}
int compareCrdtLWWHashTombstone(void* data, CrdtMeta* meta) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(data);
    return compareCrdtMeta(meta,getCrdtLWWHashTombstoneMaxDelMeta(target));
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
    initCrdtObject((CrdtObject*)crdtHashTombstone);
    setDataType((CrdtObject*)crdtHashTombstone, CRDT_HASH_TYPE);
    setType((CrdtObject*)crdtHashTombstone,  CRDT_TOMBSTONE);
    dict *hash = dictCreate(&crdtHashTombstoneDictType, NULL);
    crdtHashTombstone->map = hash;
    crdtHashTombstone->maxDelvectorClock = newVectorClock(0);
    crdtHashTombstone->lastVc = newVectorClock(0);
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
void *sioLoadCrdtLWWHash(sio *io, int version, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_Hash *crdtHash = createCrdtLWWHash();
    setCrdtHashLastVc((CRDT_Hash*)crdtHash, rdbLoadVectorClock(io, version));
    if(sioLoadCrdtBasicHash(io, encver, crdtHash) == CRDT_NO) return NULL;
    return crdtHash;
}
void sioSaveCrdtLWWHash(sio *io, void *value) {
    saveCrdtRdbHeader(io, LWW_TYPE);
    CRDT_LWW_Hash *crdtHash = retrieveCrdtLWWHash(value);
    rdbSaveVectorClock(io, getCrdtHashLastVc((CRDT_Hash*)crdtHash), CRDT_RDB_VERSION);
    sioSaveCrdtBasicHash(io, crdtHash);
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
    setCrdtHashLastVc((CRDT_Hash*)crdtHash, newVectorClock(0));
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
void *sioLoadCrdtLWWHashTombstone(sio *io, int version, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_HashTombstone *crdtHashTombstone = createCrdtLWWHashTombstone();
    if(sioLoadSigned(io) == HASH_MAXDEL) {
        int gid = sioLoadSigned(io);
        if(RedisModule_CheckGid(gid) == REDISMODULE_ERR) {
            return NULL;
        }
        setCrdtLWWHashTombstoneMaxDelGid(crdtHashTombstone, gid);
        setCrdtLWWHashTombstoneMaxDelTimestamp(crdtHashTombstone, sioLoadSigned(io));
        setCrdtLWWHashTombstoneMaxDelVectorClock(crdtHashTombstone, rdbLoadVectorClock(io, version));
    }
    setCrdtLWWHashTombstoneLastVc(crdtHashTombstone, rdbLoadVectorClock(io, version));
    if(sioLoadCrdtBasicHashTombstone(io, encver, crdtHashTombstone) == CRDT_NO) return NULL;
    return crdtHashTombstone;
}
void sioSaveCrdtLWWHashTombstone(sio *io, void *value) {
    saveCrdtRdbHeader(io, LWW_TYPE);
    CRDT_LWW_HashTombstone *crdtHashTombstone = retrieveCrdtLWWHashTombstone(value);
    if(isNullVectorClock(getCrdtLWWHashTombstoneMaxDelVectorClock(crdtHashTombstone))) {
        sioSaveSigned(io, NO_HASH_MAXDEL);
    }else{
        sioSaveSigned(io, HASH_MAXDEL);
        sioSaveSigned(io, getCrdtLWWHashTombstoneMaxDelGid(crdtHashTombstone));
        sioSaveSigned(io, getCrdtLWWHashTombstoneMaxDelTimestamp(crdtHashTombstone));
        rdbSaveVectorClock(io, getCrdtLWWHashTombstoneMaxDelVectorClock(crdtHashTombstone), CRDT_RDB_VERSION);
    }
    rdbSaveVectorClock(io, getCrdtLWWHashTombstoneLastVc(crdtHashTombstone), CRDT_RDB_VERSION);
    
    sioSaveCrdtBasicHashTombstone(io, crdtHashTombstone);
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
    setCrdtLWWHashTombstoneLastVc(crdtHash, newVectorClock(0));
    setCrdtLWWHashTombstoneMaxDelVectorClock(crdtHash, newVectorClock(0));
    RedisModule_Free(crdtHash);
}
size_t crdtLWWHashTombstoneMemUsageFunc(const void *value) {
    return 1;
}
void crdtLWWHashTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    //todo: currently do nothing when digest
}
sds crdtHashInfo(void* data) {
    CRDT_LWW_Hash* hash = retrieveCrdtLWWHash(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtHashLastVc((CRDT_Hash*)data));
    result = sdscatprintf(result, "type: lww_hash,  last-vc: %s\n",
            vcStr);
    sdsfree(vcStr);
    dictIterator* di = dictGetIterator(hash->map);
    dictEntry* de = NULL;
    int num = 5;
    while((de = dictNext(di)) != NULL && num > 0) {
        sds info = crdtRegisterInfo(dictGetVal(de));
        result = sdscatprintf(result, "  key: %s, %s\n", (sds)dictGetKey(de), info);
        sdsfree(info);
        num--;
    }
    if(num == 0 && de != NULL) {
        result = sdscatprintf(result, "  ...\n");
    }
    dictReleaseIterator(di);
    return result;
}

sds crdtHashTombstoneInfo(void* data) {
    CRDT_LWW_HashTombstone* tombstone = retrieveCrdtLWWHashTombstone(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtHashTombstoneLastVc((CRDT_HashTombstone*)tombstone));
    VectorClock maxVc = getCrdtLWWHashTombstoneMaxDelVectorClock(tombstone);
    if(isNullVectorClock(maxVc)) {
        result = sdscatprintf(result, "type: lww_hash_tombstone,  last-vc: %s\n",
            vcStr);
    }else{
        sds maxDelVcStr = vectorClockToSds(maxVc);
        result = sdscatprintf(result, "type: lww_hash_tombstone,  last-vc: %s, max-del-gid: %d, max-del-time: %lld, max-del-vc: %s\n",
            vcStr, getCrdtLWWHashTombstoneMaxDelGid(tombstone), 
            getCrdtLWWHashTombstoneMaxDelTimestamp(tombstone),
            maxDelVcStr);
        sdsfree(maxDelVcStr);
    }
    dictIterator* di = dictGetIterator(tombstone->map);
    dictEntry* de = NULL;
    int num = 5;
    while((de = dictNext(di)) != NULL && num > 0) {
        sds info = crdtRegisterTombstoneInfo(dictGetVal(de));
        result = sdscatprintf(result, "  key: %s, %s\n", (sds)dictGetKey(de), info);
        sdsfree(info);
        num--;
    }
    if(num == 0 && de != NULL) {
        result = sdscatprintf(result, "  ...\n");
    }
    dictReleaseIterator(di);

    sdsfree(vcStr);
    return result;
}

CRDT_HashTombstone* createCrdtHashFilterTombstone(CRDT_HashTombstone* common) {
    CRDT_LWW_HashTombstone* target = retrieveCrdtLWWHashTombstone(common);
    CRDT_LWW_HashTombstone* result = createCrdtLWWHashTombstone();
    result->map->type = &crdtHashFileterDictType;
    result->maxDelGid = target->maxDelGid;
    result->maxDelTimestamp = target->maxDelTimestamp;
    result->maxDelvectorClock = target->maxDelvectorClock;
    return (CRDT_HashTombstone*)result;
}
