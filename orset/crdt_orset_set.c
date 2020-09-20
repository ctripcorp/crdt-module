#include "crdt_orset_set.h"
#define NDEBUG
#include <assert.h>
CRDT_ORSET_SET* retrieveCrdtORSETSet(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_ORSET_SET* result = (CRDT_ORSET_SET*)t;
    assert(result->map != NULL);
    return result;
}
CRDT_Set* createCrdtSet() {
    CRDT_ORSET_SET* set = RedisModule_Alloc(sizeof(CRDT_ORSET_SET));
    set->type = 0;
    setType((CrdtObject*)set , CRDT_DATA);
    setDataType((CrdtObject*)set , CRDT_SET_TYPE);
    dict *map = dictCreate(&crdtSetDictType, NULL);
    set->dict = map;
    set->lastVc = newVectorClock(0);
    return set;
}

VectorClock dictGetVectorClock(dictEntry* de) {
    void* v = dictGetVal(de);
    return LL2VC(v);
}

VectorClock getCrdtSetLastVc(CRDT_Set* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    return set->lastVc;
}
int setCrdtSetLastVc(CRDT_Set* data, VectorClock vc) {
    CRDT_ORSET_SET* r = retrieveCrdtORSETSet(data);
    VectorClock old = getCrdtSetLastVc(r);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    r->lastVc = vc;
    return 1;
}
int updateCrdtSetLastVc(CRDT_Set* data, VectorClock vc) {
    CRDT_ORSET_SET* r = retrieveCrdtORSETSet(data);
    VectorClock now = vectorClockMerge(r->lastVc,vc);
    return setCrdtSetLastVc(r, now);
}
dict* getSetDict(CRDT_ORSET_SET* set) {
    return (dict*)(set->dict);
}
void freeCrdtSet(void* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    dictRelease(map);
    setCrdtSetLastVc(set, newVectorClock(0));
    RedisModule_Free(set);
}
dictEntry* findSetDict(CRDT_Set* data, sds field) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    return dictFind(map, field);
}
size_t getSetDictSize(CRDT_Set* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    return dictSize(map);
}
int removeSetDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    dictEntry* de = dictFind(map, field);
    if(de == NULL) {
        return 0;
    }
    void* v = dictGetVal(de);
    VectorClock vc = LL2VC(v);
    appendVCForMeta(meta, vc);
    dictDelete(map, field);
    return 1;
}
int addSetDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    VectorClock vc = dupVectorClock(getMetaVectorClock(meta));
    return dictAdd(map, sdsdup(field), VC2LL(vc));
}
int updateSetDict(CRDT_Set* data, dictEntry* de, CrdtMeta* meta) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    VectorClock vc = LL2VC(dictGetVal(de));
    VectorClock v = vectorClockMerge(vc, getMetaVectorClock(meta));
    freeVectorClock(vc);
    dictSetVal(map, de, VC2LL(v));
    return 1;
}
CRDT_Set* dupCrdtSet(CRDT_Set* targe) {
    CRDT_Set* result = createCrdtSet();
    dict* targe_map = getSetDict(targe);
    dict* result_map = getSetDict(result);
    dictIterator* di = dictGetSafeIterator(targe_map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock v = dupVectorClock(dictGetVectorClock(de));
        dictAdd(result_map, sdsdup(field), VC2LL(v));
    }
    dictReleaseIterator(di);
    updateCrdtSetLastVc(result, getCrdtSetLastVc(targe));
    return result;
}
dictIterator* getSetDictIterator(CRDT_Set* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    return dictGetSafeIterator(map);
}
void *RdbLoadCrdtORSETSet(RedisModuleIO *rdb, int version, int encver) {
    CRDT_Set* set = createCrdtSet();
    VectorClock lastvc = rdbLoadVectorClock(rdb, version);
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    if (len >= UINT64_MAX) return NULL;
    
    size_t strLength;
    dict* map = getSetDict(set);
    updateCrdtSetLastVc(set, lastvc);
    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        sds field = sdsnewlen(str, strLength);
        VectorClock vc = rdbLoadVectorClock(rdb, version);
        /* Add pair to hash table */
        dictAdd(map, field, VC2LL(vc));
        RedisModule_ZFree(str);
    }
    return set;
}
void RdbSaveSetDict(RedisModuleIO *rdb, dict* map) {
    RedisModule_SaveUnsigned(rdb, dictSize(map));
    dictIterator *di = dictGetSafeIterator(map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dictGetVectorClock(de);
        RedisModule_SaveStringBuffer(rdb, field, sdslen(field));
        rdbSaveVectorClock(rdb, vc, CRDT_RDB_VERSION);
    }
    dictReleaseIterator(di);
}
void RdbSaveCrdtSet(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    CRDT_ORSET_SET *set = retrieveCrdtORSETSet(value);
    rdbSaveVectorClock(rdb, getCrdtSetLastVc(set), CRDT_RDB_VERSION);
    dict* map = getSetDict(set);
    RdbSaveSetDict(rdb, map);
}
void AofRewriteCrdtSet(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}
size_t crdtSetMemUsageFunc(const void *value) {
    return 1;
}
int crdtSetDigestFunc(RedisModuleDigest *md, void *value) {
    
}

sds crdtSetInfo(void *data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtSetLastVc(set));
    result = sdscatprintf(result, "type: orset_set,  last-vc: %s",
            vcStr);
    sdsfree(vcStr);
    return result;
}



//tombstone

CRDT_SetTombstone* createCrdtSetTombstone() {
    CRDT_ORSET_SETTOMBSTONE* tombstone = RedisModule_Alloc(sizeof(CRDT_ORSET_SETTOMBSTONE));
    tombstone->type = 0;
    setType((CrdtObject*)tombstone , CRDT_TOMBSTONE);
    setDataType((CrdtObject*)tombstone , CRDT_SET_TYPE);
    dict *map = dictCreate(&crdtSetDictType, NULL);
    tombstone->dict = map;
    tombstone->lastVc = newVectorClock(0);
    tombstone->maxDelvectorClock = newVectorClock(0);
    return tombstone;
}

sds setIterInfo(void *data) {
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(LL2VC(data));
    result = sdscatprintf(result, "type: orset_set, vector-clock: %s",
            vcStr);
    sdsfree(vcStr);
    return result;

}
CRDT_ORSET_SETTOMBSTONE* retrieveCrdtORSETSetTombstone(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_ORSET_SETTOMBSTONE* result = (CRDT_ORSET_SETTOMBSTONE*)t;
    return result;
}

VectorClock getCrdtSetTombstoneLastVc(CRDT_SetTombstone* t) {
    CRDT_ORSET_SETTOMBSTONE* tom = (CRDT_ORSET_SETTOMBSTONE*)t;
    return tom->lastVc;
}
dict* getSetTombstoneDict(CRDT_ORSET_SETTOMBSTONE* tombstone) {
    return (dict*)(tombstone->dict);
}

int addSetTombstoneDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    dict* map = getSetTombstoneDict(tom);
    VectorClock vc = getMetaVectorClock(meta);
    return dictAdd(map, sdsdup(field), VC2LL(vc));
}
int removeSetTombstoneDict(CRDT_SetTombstone* data, sds field) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    dict* map = getSetTombstoneDict(tom);
    return dictDelete(map, field);
}
int setCrdtSetTombstoneLastVc(CRDT_SetTombstone* data, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* r = retrieveCrdtORSETSetTombstone(data);
    VectorClock old = getCrdtSetTombstoneLastVc(r);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    r->lastVc = vc;
    return 1;
}

int updateCrdtSetTombstoneLastVc(CRDT_SetTombstone* data, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* r = retrieveCrdtORSETSetTombstone(data);
    VectorClock now = vectorClockMerge(r->lastVc, vc);
    return setCrdtSetTombstoneLastVc(r, now);
}
int updateCrdtSetTombstoneLastVcByMeta(CRDT_SetTombstone* data, CrdtMeta* meta) {
    return updateCrdtSetTombstoneLastVc(data, getMetaVectorClock(meta));
}
void *RdbLoadCrdtORSETSetTombstone(RedisModuleIO *rdb, int version, int encver) {
    CRDT_ORSET_SETTOMBSTONE* tom = createCrdtSetTombstone();
    
    VectorClock lastvc = rdbLoadVectorClock(rdb, version);
    VectorClock maxdel = rdbLoadVectorClock(rdb, version);
    
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    if (len >= UINT64_MAX) {
        freeCrdtSetTombstone(tom);
        return NULL;
    }
    size_t strLength;
    dict* map = getSetTombstoneDict(tom);
    setCrdtSetTombstoneMaxDel(tom, maxdel);
    updateCrdtSetTombstoneLastVc(tom, lastvc);
    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        sds field = sdsnewlen(str, strLength);
        VectorClock vc = rdbLoadVectorClock(rdb, version);
        /* Add pair to hash table */
        dictAdd(map, field, VC2LL(vc));
        RedisModule_ZFree(str);
    }
    return tom;
}
void RdbSaveCrdtSetTombstone(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    CRDT_ORSET_SETTOMBSTONE *tom = retrieveCrdtORSETSetTombstone(value);
    rdbSaveVectorClock(rdb, getCrdtSetTombstoneLastVc(tom), CRDT_RDB_VERSION);
    rdbSaveVectorClock(rdb, getCrdtSetTombstoneMaxDelVc(tom), CRDT_RDB_VERSION);
    dict* map = getSetTombstoneDict(tom);
    RdbSaveSetDict(rdb, map);
}
void AofRewriteCrdtSetTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}
size_t crdtSetTombstoneMemUsageFunc(const void *value) {
    return 1;
}
int crdtSetTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    
}
void freeCrdtSetTombstone(void* data) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSet(data);
    dict* map = getSetTombstoneDict(tom);
    dictRelease(map);
    setCrdtSetTombstoneLastVc(tom, newVectorClock(0));
    if(!isNullVectorClock(tom->maxDelvectorClock)) {
        freeVectorClock(tom->maxDelvectorClock);
    }
    RedisModule_Free(tom);
}


CrdtObject** crdtSetTombstoneFilter(CrdtTombstone* t, int gid, long long logic_time, long long maxsize,int* num)  {
    CRDT_ORSET_SETTOMBSTONE* targe = retrieveCrdtORSETSetTombstone(t);
    CRDT_ORSET_SETTOMBSTONE** result = NULL;
    dict* map = getSetTombstoneDict(targe);
    dictIterator *di = dictGetSafeIterator(map);
    dictEntry *de;
    int current_memory = 0;
    CRDT_ORSET_SETTOMBSTONE* tom = NULL;
    dict* m = NULL;
    RedisModule_Debug(logLevel, "crdtSetTombstoneFilter start");
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dictGetVectorClock(de);
        VectorClockUnit unit = getVectorClockUnit(vc, gid);
        long long vcu = get_logic_clock(unit);
        if(vcu <= logic_time) {
            continue;
        }  
        size_t size = ((int)get_len(vc)) * sizeof(VectorClockUnit) + sdslen(dictGetKey(de));
        if(size > maxsize) {
            freeSetTombstoneFilter((CrdtObject**)result, *num);
            *num = -1;
            RedisModule_Debug(logLevel, "[CRDT][FILTER] set_tombstone key {%s} value too big", field);
            return NULL;
        }
        if(current_memory + size > maxsize) {
            tom = NULL;
        }
        if(tom == NULL) {
            tom = createCrdtSetTombstone();
            m = getSetTombstoneDict(tom);
            current_memory = 0;
            m->type = &crdtSetFileterDictType;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(CRDT_Set*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(CRDT_Set*));
            }
            result[(*num)-1] = tom;
        }
        current_memory += size;
        dictAdd(m, field, vc);
        updateCrdtSetTombstoneLastVc(tom, vc);
    }
    RedisModule_Debug(logLevel, "crdtSetTombstoneFilter end");
    dictReleaseIterator(di);
    if(tom == NULL && *num == 0) {
        VectorClockUnit unit = getVectorClockUnit(getCrdtSetTombstoneMaxDelVc(targe), gid);
        long long vcu = get_logic_clock(unit);
        if(vcu <= logic_time) {
            RedisModule_Debug(logLevel, "??????? %lld", vcu);
            return NULL;
        }  
        tom = createCrdtSetTombstone();
        m = getSetTombstoneDict(tom);
        m->type = &crdtSetFileterDictType;
        (*num)++;
        result = RedisModule_Alloc(sizeof(CRDT_Set*));
        result[(*num)-1] = tom;
    }
    RedisModule_Debug(logLevel, "crdtSetTombstoneFilter end 2");
    if(tom != NULL) {
        updateCrdtSetTombstoneLastVc(tom, getCrdtSetTombstoneLastVc(targe));
        updateCrdtSetTombstoneMaxDel(tom, getCrdtSetTombstoneMaxDelVc(targe));
    }
    RedisModule_Debug(logLevel, "crdtSetTombstoneFilter end 3");
    return (CrdtObject**)result;
}
int crdtSetTombstoneGc(CrdtTombstone* data, VectorClock clock) {
    // CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    // return isVectorClockMonoIncr(tom->lastVc, clock);
    return 0;
}

void freeSetTombstoneFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtSetTombstone(filters[i]);
    }
    RedisModule_Free(filters);
}
sds crdtSetTombstoneInfo(void *data) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtSetTombstoneLastVc(tom));
    sds maxVcStr = vectorClockToSds(tom->maxDelvectorClock);
    result = sdscatprintf(result, "type: orset_set_tombstone,  last-vc: %s, maxdel-vc: %s, %lld",
            vcStr, maxVcStr, VC2LL(tom->maxDelvectorClock));
    sdsfree(vcStr);
    sdsfree(maxVcStr);
    return result;
}
int crdtSetTombstonePurge(CrdtTombstone* tombstone, CrdtData* data) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(tombstone);
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict(set);
    dict* tmap = getSetTombstoneDict(tom);
    dictIterator* di = dictGetSafeIterator(map);
    VectorClock maxdel = getCrdtSetTombstoneMaxDelVc(tom);
    dictEntry* de = NULL;
    RedisModule_Debug(logLevel, "purge start");
    while((de = dictNext(di) )!= NULL) {
        sds field =  dictGetKey(de);
        dictEntry* tde = dictFind(tmap, field);
        VectorClock vc = dictGetVectorClock(de);
        if(isVectorClockMonoIncr(vc, maxdel)) {
            dictDelete(map, field);
            continue;
        }
        VectorClock tvc = NULL;
        if(tde != NULL) {
            tvc = dictGetVectorClock(tde);
            if(isVectorClockMonoIncr(tvc, vc)) {
                dictDelete(tmap, field);
                tvc = NULL;
            } else if(isVectorClockMonoIncr(vc, tvc)) {
                dictDelete(map, field);
                continue;
            }
        }
        VectorClock m = vectorClockMerge(tvc, maxdel);
        purgeSetIter(map, field, de, m);
        freeVectorClock(m);
    }
    RedisModule_Debug(logLevel, "purge end");
    dictReleaseIterator(di);
    RedisModule_Debug(logLevel, "purge dictReleaseIterator");
    if(dictSize(map) == 0) {
        return PURGE_VAL;
    }
    return 0;
}


int setTombstoneIterPurge(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    VectorClock vc = dupVectorClock(getMetaVectorClock(meta));
    if(t != NULL) {
        CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
        if(isVectorClockMonoIncr(vc, tom->maxDelvectorClock)) {
            freeVectorClock(vc);
            return 0;
        } else {
            vc = purgeVectorClock(vc, tom->maxDelvectorClock);
        }
        dict* tom_map = getSetTombstoneDict(tom);
        dictEntry* tde = dictFind(tom_map, field);
        if(tde != NULL) {
            VectorClock tvc = dictGetVectorClock(tde);
            if(isVectorClockMonoIncr(tvc, vc)) {
                dictDelete(tom_map, field);
            } else if(isVectorClockMonoIncr(tvc, vc)) {
                freeVectorClock(vc);
                return 0;    
            } else {
                vc = purgeVectorClock(vc, tvc);
            }
        }
    }
    
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(s);
    dict* set_map = getSetDict(set);
    dictEntry* de = dictFind(set_map, field);
    if(de != NULL) {
        VectorClock old = dictGetVectorClock(de);
        VectorClock v = vectorClockMerge(vc, old);
        dictSetVal(set_map, de, VC2LL(v));
        freeVectorClock(old);
    } else {
        dictAdd(set_map, sdsdup(field),  VC2LL(vc));
    }
    return 1;
}
int purgeSetIter(dict* di, sds field,  dictEntry* de, VectorClock vc) {
    VectorClock old = dictGetVectorClock(de);
    if(isVectorClockMonoIncr(old, vc)) {
        dictDelete(di, dictGetKey(de));
    } else if(isVectorClockMonoIncr(vc, old)) {
        return 0;
    } else {
        old = purgeVectorClock(old, vc);
        dictSetVal(di, de, old);
    }
}
int setValueIterPurge(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    VectorClock vc = getMetaVectorClock(meta);
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    if(isVectorClockMonoIncr(vc, tom->maxDelvectorClock)) {
        return 0;
    }
    if(s != NULL) {
        CRDT_ORSET_SET* set = retrieveCrdtORSETSet(s);
        dict* set_map = getSetDict(set);
        dictEntry* de = dictFind(set_map, field);
        if(de != NULL) {
            purgeSetIter(set_map, field, de, vc);
            // VectorClock v = vectorClockMerge(vc, old);
            // dictSetVal(set_map, de, VC2LL(v));
            // freeVectorClock(old);
        } 
    }
    dict* tom_map = getSetTombstoneDict(tom);
    dictEntry* tde = dictFind(tom_map, field);
    if(tde != NULL) {
        VectorClock tvc = dictGetVectorClock(tde);
        VectorClock ntvc = vectorClockMerge(tvc, vc);
        dictSetVal(tom_map, tde, VC2LL(ntvc));
        freeVectorClock(tvc);
    } else {
        dictAdd(tom_map, field, dupVectorClock(vc));
    }
    return 1;
}

int purgeSetDelMax(CRDT_Set* s, CRDT_SetTombstone* t, CrdtMeta* meta) {
    VectorClock vc = getMetaVectorClock(meta);
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    if(isVectorClockMonoIncr(vc, tom->maxDelvectorClock)) {
        return 0;
    }
    updateCrdtSetTombstoneMaxDel(tom, vc);
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(s);
    dict* map = getSetDict(set);
    dictEntry* de = NULL;
    dictIterator* di = dictGetSafeIterator(map);
    while((de = dictNext(di)) != NULL) {
        purgeSetIter(map, dictGetKey(de), de, vc);
    }
    dictReleaseIterator(di);
    map = getSetTombstoneDict(tom);
    de = NULL;
    di = dictGetSafeIterator(map);
    while((de = dictNext(di)) != NULL) {
        VectorClock v = dictGetVectorClock(de);
        if(isVectorClockMonoIncr(v, vc)) {
            dictDelete(map, dictGetKey(de));
        }
    }
    dictReleaseIterator(di);
    return 1;
}

int appendSet(CRDT_Set* t, CRDT_Set* s) {
    CRDT_ORSET_SET* targe = retrieveCrdtORSETSet(t);
    CRDT_ORSET_SET* src = retrieveCrdtORSETSet(s);
    dict* src_map = getSetDict(src);
    dict* targe_map = getSetDict(targe);
    dictEntry* de = NULL;
    dictIterator* di = dictGetSafeIterator(src_map);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        dictEntry* d = dictFind(targe_map, field);
        VectorClock vc = dictGetVectorClock(de);
        if(d == NULL) {
            VectorClock nvc = dupVectorClock(vc);
            dictAdd(targe_map, sdsdup(field), VC2LL(nvc));
        } else {
            VectorClock tvc = dictGetVectorClock(d);
            VectorClock nvc = vectorClockMerge(tvc, vc);
            dictSetVal(targe_map, d, VC2LL(nvc));
            freeVectorClock(vc);
        }
    }
    return 1;
}
void freeSetFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtSet(filters[i]);
    }
    RedisModule_Free(filters);
}
CrdtObject** crdtSetFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* num) {
    CRDT_ORSET_SET* targe = retrieveCrdtORSETSet(common);
    CRDT_ORSET_SET** result = NULL;
    dict* map = getSetDict(targe);
    dictIterator *di = dictGetSafeIterator(map);
    dictEntry *de;
    int current_memory = 0;
    CRDT_Set* set = NULL;
    dict* m = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dictGetVectorClock(de);
        VectorClockUnit unit = getVectorClockUnit(vc, gid);
        long long vcu = get_logic_clock(unit);
        if(vcu <= logic_time) {
            continue;
        }  
        size_t size = ((int)get_len(vc)) * sizeof(VectorClockUnit) + sdslen(dictGetKey(de));
        if(size > maxsize) {
            freeSetFilter((CrdtObject**)result, *num);
            *num = -1;
            RedisModule_Debug(logLevel, "[CRDT][FILTER] set key {%s} value too big", field);
            return NULL;
        }
        if(current_memory + size > maxsize) {
            set = NULL;
        }
        if(set == NULL) {
            set = createCrdtSet();
            m = getSetDict(set);
            current_memory = 0;
            m->type = &crdtSetFileterDictType;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(CRDT_Set*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(CRDT_Set*));
            }
            result[(*num)-1] = set;
        }
        current_memory += size;
        dictAdd(m, field, vc);
        updateCrdtSetLastVc(set, vc);
    }
    dictReleaseIterator(di);
    if(set != NULL) {
        setCrdtSetLastVc(set, getCrdtSetLastVc(targe));
    }
    return (CrdtObject**)result;
}

#include <stdio.h>
#include <execinfo.h>
#define STACK_SIZE 1000
static void printStack(void)
{
    
    void *trace[STACK_SIZE];
    size_t size = backtrace(trace, STACK_SIZE);
    char **symbols = (char **)backtrace_symbols(trace,size);
    size_t i = 0;
    for(; i<size; i++)
    {
        printf("%d--->%s\n", i, symbols[i]);
    }
    // zfree(symbols);
    return;
}

int setCrdtSetTombstoneMaxDel(CRDT_SetTombstone* data, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    VectorClock old = getCrdtSetTombstoneMaxDelVc(tom);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    sds s = vectorClockToSds(vc);
    RedisModule_Debug(logLevel, "setCrdtSetTombstoneMaxDel %s %lld", s, VC2LL(vc));
    sdsfree(s);
    printStack();
    tom->maxDelvectorClock = vc;
    return 1;
}
VectorClock getCrdtSetTombstoneMaxDelVc(CRDT_SetTombstone* t) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    return tom->maxDelvectorClock;
}
int updateCrdtSetTombstoneMaxDel(CRDT_SetTombstone* t, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    VectorClock now = vectorClockMerge(tom->maxDelvectorClock,vc);
    return setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, now);
}


CRDT_SetTombstone* dupCrdtSetTombstone(CRDT_SetTombstone* targe) {
    CRDT_ORSET_SETTOMBSTONE* result = createCrdtSetTombstone();
    dict* targe_map = getSetTombstoneDict(targe);
    dict* result_map = getSetTombstoneDict(result);
    dictIterator* di = dictGetSafeIterator(targe_map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock v = dupVectorClock(dictGetVectorClock(de));
        dictAdd(result_map, sdsdup(field), VC2LL(v));
    }
    dictReleaseIterator(di);
    updateCrdtSetLastVc((CRDT_SetTombstone*)result, getCrdtSetLastVc(targe));
    updateCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)result, getCrdtSetTombstoneMaxDelVc(targe));
    return (CRDT_SetTombstone*)result;
}

int appendSetTombstone(CRDT_SetTombstone* t, CRDT_SetTombstone* s) {
    CRDT_ORSET_SETTOMBSTONE* targe = retrieveCrdtORSETSetTombstone(t);
    CRDT_ORSET_SETTOMBSTONE* src = retrieveCrdtORSETSetTombstone(s);
    dict* src_map = getSetTombstoneDict(src);
    dict* targe_map = getSetTombstoneDict(targe);
    dictEntry* de = NULL;
    dictIterator* di = dictGetSafeIterator(src_map);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        dictEntry* d = dictFind(targe_map, field);
        VectorClock vc = dictGetVectorClock(de);
        if(d == NULL) {
            VectorClock nvc = dupVectorClock(vc);
            dictAdd(targe_map, sdsdup(field), VC2LL(nvc));
        } else {
            VectorClock tvc = dictGetVectorClock(d);
            VectorClock nvc = vectorClockMerge(tvc, vc);
            dictSetVal(targe_map, d, VC2LL(nvc));
            freeVectorClock(vc);
        }
    }
    return 1;
}
