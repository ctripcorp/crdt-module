#ifndef XREDIS_CRDT_SET_H
#define XREDIS_CRDT_SET_H
#include "include/rmutil/sds.h"
#include "ctrip_crdt_common.h"
#include "include/redismodule.h"
#include "crdt_util.h"
#include "crdt_expire.h"

#define CRDT_SET_DATATYPE_NAME "crdt_setr"
#define CRDT_SET_TOMBSTONE_DATATYPE_NAME "crdt_sett"
static RedisModuleType *CrdtSet;
static RedisModuleType *CrdtSetTombstone;
typedef  CrdtTombstone CRDT_SetTombstone;
typedef CrdtObject CRDT_Set;
int initCrdtSetModule(RedisModuleCtx *ctx);
RedisModuleType* getCrdtSet();
RedisModuleType* getCrdtSetTombstone();

//set type modulemethod
void *RdbLoadCrdtSet(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtSet(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtSet(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtSetMemUsageFunc(const void *value);
void crdtSetDigestFunc(RedisModuleDigest *md, void *value);
void freeCrdtSet(void* set);
dict* getSetDict(CRDT_Set* set);

//set tombstone type modulemethod
void *RdbLoadCrdtSetTombstone(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtSetTombstone(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtSetTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtSetTombstoneMemUsageFunc(const void *value);
void crdtSetTombstoneDigestFunc(RedisModuleDigest *md, void *value);
void freeCrdtSetTombstone(void* set);

//adbout set

CRDT_Set* createCrdtSet();
CRDT_Set* dupCrdtSet(CRDT_Set* set);
int updateCrdtSetLastVc(CRDT_Set* set, VectorClock vc);
int setCrdtSetLastVc(CRDT_Set* set, VectorClock);
VectorClock getCrdtSetLastVc(CRDT_Set* set);
dictEntry* findSetDict(CRDT_Set* set, sds field);
int removeSetDict(CRDT_Set* set, sds field,  CrdtMeta* meta);
int addSetDict(CRDT_Set* set, sds field, CrdtMeta* meta);
int updateSetDict(CRDT_Set* set, dictEntry* de, CrdtMeta* meta);
size_t getSetDictSize(CRDT_Set* data);
dictIterator* getSetDictIterator(CRDT_Set* data);
int crdtSetDelete(int dbId, void* keyRobj, void *key, void *value);
int setValueIterPurge(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta);
int appendSet(CRDT_Set* targe, CRDT_Set* src);

sds getRandomSetKey(CRDT_Set* set);
//about set tombstone
int removeSetTombstoneDict(CRDT_SetTombstone* tom, sds field);
VectorClock getCrdtSetTombstoneLastVc(CRDT_SetTombstone* tom);
CRDT_SetTombstone* createCrdtSetTombstone();
int updateCrdtSetTombstoneLastVcByMeta(CRDT_SetTombstone* tom, CrdtMeta* meta);
int updateCrdtSetTombstoneLastVc(CRDT_SetTombstone* tom, VectorClock vc);
sds crdtSetInfo(void *data);
sds setIterInfo(void *data);


//about method
static CrdtDataMethod SetDataMethod = {
    .propagateDel = crdtSetDelete,
    .info = crdtSetInfo,
};
CrdtObject *crdtSetMerge(CrdtObject *currentVal, CrdtObject *value);
CrdtObject** crdtSetFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* length);
void freeSetFilter(CrdtObject** filters, int num);
static CrdtObjectMethod SetCommonMethod = {
    .merge = crdtSetMerge,
    .filterAndSplit = crdtSetFilter,
    .freefilter = freeSetFilter,
};
int addSetTombstoneDictValue(CRDT_Set* data, sds field, CrdtMeta* meta);
//about tombstone method
CrdtTombstone* crdtSetTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other);
CrdtObject** crdtSetTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) ;
int crdtSetTombstoneGc(CrdtTombstone* target, VectorClock clock);
sds crdtSetTombstoneInfo(void *t);
void freeSetTombstoneFilter(CrdtObject** filters, int num);
int crdtSetTombstonePurge(CrdtTombstone* tombstone, CrdtObject* target);
static CrdtTombstoneMethod SetTombstoneCommonMethod = {
    .merge = crdtSetTombstoneMerge,
    .filterAndSplit =  crdtSetTombstoneFilter,
    .freefilter = freeSetTombstoneFilter,
    .gc = crdtSetTombstoneGc,
    .purge = crdtSetTombstonePurge,
    .info = crdtSetTombstoneInfo,
};
int setTombstoneIterPurge(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta);
int purgeSetDelMax(CRDT_Set* s, CRDT_SetTombstone* t, CrdtMeta* meta);
VectorClock getCrdtSetTombstoneMaxDelVc(CRDT_SetTombstone* t);
int updateCrdtSetTombstoneMaxDel(CRDT_SetTombstone* t, VectorClock vc);
CRDT_SetTombstone* dupCrdtSetTombstone(CRDT_SetTombstone* tom);
int appendSetTombstone(CRDT_SetTombstone* a, CRDT_SetTombstone* b);
dictEntry* findSetTombstoneDict(CRDT_SetTombstone* tom, sds field);
sds setTombstoneIterInfo(void *data);

size_t getSetTombstoneDictSize(CRDT_SetTombstone* data);
static inline int isCrdtSet(void* data) {
    CRDT_Set* set = (CRDT_Set*)data;
    if(set != NULL && (getDataType((CrdtObject*)set) == CRDT_SET_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
static inline int isCrdtSetTombstone(void *data) {
    CRDT_SetTombstone* tombstone = (CRDT_SetTombstone*)data;
    if(tombstone != NULL && (getDataType((CrdtObject*)tombstone) ==  CRDT_SET_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
static inline CRDT_Set* retrieveCrdtSet(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_Set* result = (CRDT_Set*)t;
    // assert(result->map != NULL);
    return result;
}
static inline CRDT_SetTombstone* retrieveCrdtSetTombstone(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_SetTombstone* result = (CRDT_SetTombstone*)t;
    // assert(result->map != NULL);
    return result;
}


#endif
