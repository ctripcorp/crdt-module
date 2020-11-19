#include "crdt_orset_set.h"
#define NDEBUG
#include <assert.h>
void *RdbLoadCrdtSet(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return RdbLoadCrdtORSETSet(rdb, version, encver);
    }
    return NULL;
}

void *RdbLoadCrdtSetTombstone(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return RdbLoadCrdtORSETSetTombstone(rdb, version, encver);
    }
    return NULL;
}


CRDT_ORSET_SET* retrieveCrdtORSETSet(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_ORSET_SET* result = (CRDT_ORSET_SET*)t;
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
    return (CRDT_Set* )set;
}

VectorClock dictGetVectorClock(dictEntry* de) {
    long long v = dictGetUnsignedIntegerVal(de);
    return LL2VC(v);
}
int dictAddVectorClock(dict* map, sds field, VectorClock vc) {
    dictEntry* de = dictAddOrFind(map, field);
    dictSetUnsignedIntegerVal(de, VC2LL(vc));
    return 1;
}

VectorClock getCrdtSetLastVc(CRDT_Set* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    return set->lastVc;
}
int setCrdtSetLastVc(CRDT_Set* data, VectorClock vc) {
    CRDT_ORSET_SET* r = retrieveCrdtORSETSet(data);
    VectorClock old = getCrdtSetLastVc(data);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    r->lastVc = vc;
    return 1;
}
int updateCrdtSetLastVc(CRDT_Set* data, VectorClock vc) {
    CRDT_ORSET_SET* r = retrieveCrdtORSETSet(data);
    VectorClock now = vectorClockMerge(r->lastVc,vc);
    return setCrdtSetLastVc(data, now);
}

int updateCrdtSetLastVcuByVectorClock(CRDT_Set* data, int gid, VectorClock vc) {
    CRDT_ORSET_SET* r = retrieveCrdtORSETSet(data);
    VectorClock v = r->lastVc;
    mergeLogicClock(&v, &vc, gid);
    r->lastVc = v;
    return 1;
}


dict* getSetDict(CRDT_Set* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    return (dict*)(set->dict);
}
void freeCrdtSet(void* data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict((CRDT_Set*)set);
    dictRelease(map);
    setCrdtSetLastVc((CRDT_Set*)set, newVectorClock(0));
    RedisModule_Free(set);
}
dictEntry* findSetDict(CRDT_Set* data, sds field) {
    dict* map = getSetDict(data);
    return dictFind(map, field);
}
size_t getSetDictSize(CRDT_Set* data) {
    dict* map = getSetDict(data);
    return dictSize(map);
}


#define COMPARE_VECTORCLOCK_LT -1
#define COMPARE_VECTORCLOCK_GT 1
#define COMPARE_VECTORCLOCK_CONFFLICT 0
int purgeSetIter(dict* di, sds field,  dictEntry* de, VectorClock vc) {
    VectorClock old = dictGetVectorClock(de);
    if(isVectorClockMonoIncr(old, vc)) {
        dictDelete(di, dictGetKey(de));
        return COMPARE_VECTORCLOCK_GT;
    } else if(isVectorClockMonoIncr(vc, old)) {
        return COMPARE_VECTORCLOCK_LT;
    } else {
        old = purgeVectorClock(old, vc);
        dictSetUnsignedIntegerVal(de, VC2LL(old));
        return COMPARE_VECTORCLOCK_CONFFLICT;
    }
    
}
int removeSetDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
    dict* map = getSetDict(data);
    return dictDelete(map, field) == DICT_OK ? 1: 0;
}

int addSetDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
    dict* map = getSetDict(data);
    VectorClock vc = dupVectorClock(getMetaVectorClock(meta));
    return  dictAddVectorClock(map, sdsdup(field), vc);
}

int updateSetDict(CRDT_Set* data, dictEntry* de, CrdtMeta* meta) {
    // CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    VectorClock vc = LL2VC(dictGetUnsignedIntegerVal(de));
    VectorClock v = vectorClockMerge(vc, getMetaVectorClock(meta));
    freeVectorClock(vc);
    dictSetUnsignedIntegerVal(de, VC2LL(v));
    return 1;
}

CRDT_Set* dupCrdtSet(CRDT_Set* target) {
    CRDT_Set* result = createCrdtSet();
    dict* target_map = getSetDict(target);
    dict* result_map = getSetDict(result);
    dictIterator* di = dictGetIterator(target_map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock v = dupVectorClock(dictGetVectorClock(de));
        dictAddVectorClock(result_map, sdsdup(field), v);
    }
    dictReleaseIterator(di);
    setCrdtSetLastVc(result, dupVectorClock(getCrdtSetLastVc(target)));
    return result;
}

dictIterator* getSetDictIterator(CRDT_Set* data) {
    dict* map = getSetDict(data);
    return dictGetIterator(map);
}

void *RdbLoadCrdtORSETSet(RedisModuleIO *rdb, int version, int encver) {
    CRDT_Set* set = createCrdtSet();
    VectorClock lastvc = rdbLoadVectorClock(rdb, version);
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    if (len >= UINT64_MAX) return NULL;
    
    size_t strLength;
    dict* map = getSetDict(set);
    setCrdtSetLastVc(set, lastvc);
    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        sds field = sdsnewlen(str, strLength);
        VectorClock vc = rdbLoadVectorClock(rdb, version);
        /* Add pair to hash table */
        dictAddVectorClock(map, field, vc);
        RedisModule_ZFree(str);
    }
    return set;
}

void RdbSaveSetDict(RedisModuleIO *rdb, dict* map) {
    RedisModule_SaveUnsigned(rdb, dictSize(map));
    dictIterator *di = dictGetIterator(map);
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
    rdbSaveVectorClock(rdb, getCrdtSetLastVc((CRDT_Set*)set), CRDT_RDB_VERSION);
    dict* map = getSetDict((CRDT_Set*)set);
    RdbSaveSetDict(rdb, map);
}

void AofRewriteCrdtSet(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

size_t crdtSetMemUsageFunc(const void *value) {
    return 1;
}

void crdtSetDigestFunc(RedisModuleDigest *md, void *value) {
    
}

sds crdtSetInfo(void *data) {
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtSetLastVc((CRDT_Set*)set));
    result = sdscatprintf(result, "type: orset_set,  last-vc: %s",
            vcStr);
    sdsfree(vcStr);
    return result;
}


sds getRandomSetKey(CRDT_Set* data) {
    dict* di = getSetDict(data);
    dictEntry* de = dictGetFairRandomKey(di);
    return dictGetKey(de);
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
    return (CRDT_SetTombstone*)tombstone;
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
int setCrdtSetTombstoneMaxDel(CRDT_SetTombstone* data, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    VectorClock old = getCrdtSetTombstoneMaxDelVc(data);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    }
    tom->maxDelvectorClock = vc;
    return 1;
}

dict* getSetTombstoneDict(CRDT_ORSET_SETTOMBSTONE* tombstone) {
    return (dict*)(tombstone->dict);
}

int addSetTombstoneDictValue(CRDT_Set* data, sds field, CrdtMeta* meta) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    dict* map = getSetTombstoneDict(tom);
    VectorClock vc = getMetaVectorClock(meta);
    return dictAddVectorClock(map, sdsdup(field), dupVectorClock(vc));
}

int removeSetTombstoneDict(CRDT_SetTombstone* data, sds field) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    dict* map = getSetTombstoneDict(tom);
    return dictDelete(map, field);
}

int setCrdtSetTombstoneLastVc(CRDT_SetTombstone* data, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* r = retrieveCrdtORSETSetTombstone(data);
    VectorClock old = getCrdtSetTombstoneLastVc(data);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    r->lastVc = vc;
    return 1;
}

int updateCrdtSetTombstoneLastVc(CRDT_SetTombstone* data, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* r = retrieveCrdtORSETSetTombstone(data);
    VectorClock now = vectorClockMerge(r->lastVc, vc);
    return setCrdtSetTombstoneLastVc(data, now);
}

int updateCrdtSetTombstoneLastVcByMeta(CRDT_SetTombstone* data, CrdtMeta* meta) {
    return updateCrdtSetTombstoneLastVc(data, getMetaVectorClock(meta));
}

void *RdbLoadCrdtORSETSetTombstone(RedisModuleIO *rdb, int version, int encver) {
    CRDT_ORSET_SETTOMBSTONE* tom = (CRDT_ORSET_SETTOMBSTONE* )createCrdtSetTombstone();
    
    VectorClock lastvc = rdbLoadVectorClock(rdb, version);
    VectorClock maxdel = rdbLoadVectorClock(rdb, version);
    
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    if (len >= UINT64_MAX) {
        freeCrdtSetTombstone(tom);
        return NULL;
    }
    size_t strLength;
    dict* map = getSetTombstoneDict(tom);
    
    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        sds field = sdsnewlen(str, strLength);
        VectorClock vc = rdbLoadVectorClock(rdb, version);
        /* Add pair to hash table */
        dictAddVectorClock(map, field, vc);
        RedisModule_ZFree(str);
    }
    setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, maxdel);
    setCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, lastvc);
    return tom;
}
void RdbSaveCrdtSetTombstone(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    CRDT_ORSET_SETTOMBSTONE *tom = retrieveCrdtORSETSetTombstone(value);
    rdbSaveVectorClock(rdb, getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom), CRDT_RDB_VERSION);
    rdbSaveVectorClock(rdb, getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)tom), CRDT_RDB_VERSION);
    dict* map = getSetTombstoneDict(tom);
    RdbSaveSetDict(rdb, map);
}

void AofRewriteCrdtSetTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

size_t crdtSetTombstoneMemUsageFunc(const void *value) {
    return 1;
}

void crdtSetTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    
}

void freeCrdtSetTombstone(void* data) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    dict* map = getSetTombstoneDict(tom);
    dictRelease(map);
    setCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, newVectorClock(0));
    setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, newVectorClock(0));
    RedisModule_Free(tom);
}


CrdtObject** crdtSetTombstoneFilter(CrdtTombstone* t, int gid, long long logic_time, long long maxsize,int* num)  {
    CRDT_ORSET_SETTOMBSTONE* target = retrieveCrdtORSETSetTombstone(t);
    CRDT_ORSET_SETTOMBSTONE** result = NULL;
    dict* map = getSetTombstoneDict(target);
    dictIterator *di = dictGetIterator(map);
    dictEntry *de = NULL;
    int current_memory = 0;
    CRDT_ORSET_SETTOMBSTONE* tom = NULL;
    dict* m = NULL;

    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dictGetVectorClock(de);
        VectorClockUnit unit = getVectorClockUnit(vc, gid);
        if(get_logic_clock(unit) < logic_time) {
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
            tom = (CRDT_ORSET_SETTOMBSTONE*)(createCrdtSetTombstone());
            m = getSetTombstoneDict(tom);
            current_memory = 0;
            m->type = &crdtSetFileterDictType;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(CRDT_ORSET_SETTOMBSTONE*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(CRDT_ORSET_SETTOMBSTONE*));
            }
            result[(*num)-1] = tom;
        }
        current_memory += size;
        dictAddVectorClock(m, field, vc);
        updateCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, vc);
    }
    dictReleaseIterator(di);
    if(tom == NULL && *num == 0) {
        VectorClockUnit unit = getVectorClockUnit(getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)target), gid);
        if(get_logic_clock(unit) < logic_time) {
            return NULL;
        }  
        tom = (CRDT_ORSET_SETTOMBSTONE*)(createCrdtSetTombstone());
        m = getSetTombstoneDict(tom);
        m->type = &crdtSetFileterDictType;
        (*num)++;
        result = RedisModule_Alloc(sizeof(CRDT_ORSET_SETTOMBSTONE*));
        result[(*num)-1] = tom;
    }
    if(tom != NULL) {
        updateCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)target));
        updateCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)target));
    }

    return (CrdtObject**)result;
}

int crdtSetTombstoneGc(CrdtTombstone* data, VectorClock clock) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    return isVectorClockMonoIncr(getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom), clock);
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
    sds vcStr = vectorClockToSds(getCrdtSetTombstoneLastVc(data));
    sds maxVcStr = vectorClockToSds(tom->maxDelvectorClock);
    result = sdscatprintf(result, "type: orset_set_tombstone,  last-vc: %s, maxdel-vc: %s",
            vcStr, maxVcStr);
    sdsfree(vcStr);
    sdsfree(maxVcStr);
    return result;
}

int crdtSetTombstonePurge(CrdtTombstone* tombstone, CrdtData* data) {
    if(!isCrdtSetTombstone(tombstone) || !isCrdtSet(data)) {
        return 0;
    }
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(tombstone);
    CRDT_ORSET_SET* set = retrieveCrdtORSETSet(data);
    dict* map = getSetDict((CRDT_Set*)set);
    dict* tmap = getSetTombstoneDict(tom);
    dictIterator* di = dictGetSafeIterator(map);
    VectorClock maxdel = getCrdtSetTombstoneMaxDelVc(tombstone);
    dictEntry* de = NULL;
    while((de = dictNext(di) )!= NULL) {
        sds field =  dictGetKey(de);
        dictEntry* tde = dictFind(tmap, field);
        VectorClock vc = dictGetVectorClock(de);
        if(isVectorClockMonoIncr(vc, maxdel)) {
            dictDelete(map, field);
            continue;
        }
        VectorClock tvc = newVectorClock(0);
        if(tde != NULL) {
            tvc = dictGetVectorClock(tde);
            if(isVectorClockMonoIncr(tvc, vc)) {
                dictDelete(tmap, field);
                tvc = newVectorClock(0);
            } else if(isVectorClockMonoIncr(vc, tvc)) {
                dictDelete(map, field);
                continue;
            }
        }
        VectorClock m = vectorClockMerge(tvc, maxdel);
        purgeSetIter(map, field, de, m);
        freeVectorClock(m);
    }
    dictReleaseIterator(di);
    if(dictSize(map) == 0) {
        return PURGE_VAL;
    }
    return 0;
}



int setTombstoneIterPurge(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    VectorClock vc = dupVectorClock(getMetaVectorClock(meta));
    if(t != NULL) {
        CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
        if(isVectorClockMonoIncr(vc, getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)tom))) {
            freeVectorClock(vc);
            return 0;
        } else {
            vc = purgeVectorClock(vc, getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)tom));
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
    dict* set_map = getSetDict(s);
    dictEntry* de = dictFind(set_map, field);
    if(de != NULL) {
        VectorClock old = dictGetVectorClock(de);
        VectorClock v = vectorClockMerge(vc, old);
        dictSetUnsignedIntegerVal(de, VC2LL(v));
        freeVectorClock(old);
    } else {
        dictAddVectorClock(set_map, sdsdup(field),  vc);
    }
    return 1;
}

int setValueIterPurge(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    VectorClock vc = getMetaVectorClock(meta);
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    if(isVectorClockMonoIncr(vc, getCrdtSetTombstoneMaxDelVc(t))) {
        return 0;
    }
    if(s != NULL) {
        dict* set_map = getSetDict(s);
        dictEntry* de = dictFind(set_map, field);
        if(de != NULL) {
            if(purgeSetIter(set_map, field, de, vc) == COMPARE_VECTORCLOCK_LT) {
                return 0;
            }
        } 
    }
    
    dict* tom_map = getSetTombstoneDict(tom);
    dictEntry* tde = dictFind(tom_map, field);
    if(tde != NULL) {
        VectorClock tvc = dictGetVectorClock(tde);
        VectorClock ntvc = vectorClockMerge(tvc, vc);
        dictSetUnsignedIntegerVal(tde, VC2LL(ntvc));
        freeVectorClock(tvc);
    } else {
        dictAddVectorClock(tom_map, sdsdup(field), dupVectorClock(vc));
    }
    return 1;
}

int purgeSetDelMax(CRDT_Set* s, CRDT_SetTombstone* t, CrdtMeta* meta) {
    VectorClock vc = getMetaVectorClock(meta);
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    if(isVectorClockMonoIncr(vc, getCrdtSetTombstoneMaxDelVc(t))) {
        return 0;
    }
    updateCrdtSetTombstoneMaxDel(t, vc);
    dict* map = getSetDict(s);
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
    dict* src_map = getSetDict(s);
    dict* target_map = getSetDict(t);
    dictEntry* de = NULL;
    dictIterator* di = dictGetIterator(src_map);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        dictEntry* d = dictFind(target_map, field);
        VectorClock vc = dictGetVectorClock(de);
        if(d == NULL) {
            dictAddVectorClock(target_map, sdsdup(field), dupVectorClock(vc));
        } else {
            VectorClock tvc = dictGetVectorClock(d);
            VectorClock nvc = vectorClockMerge(tvc, vc);
            dictSetUnsignedIntegerVal(d, VC2LL(nvc));
            freeVectorClock(tvc);
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
CrdtObject** crdtSetFilter(CrdtObject* t, int gid, long long logic_time, long long maxsize, int* num) {
    CRDT_ORSET_SET* target = retrieveCrdtORSETSet(t);
    CRDT_Set** result = NULL;
    dict* map = getSetDict((CRDT_Set*)target);
    dictIterator *di = dictGetIterator(map);
    dictEntry *de;
    int current_memory = 0;
    CRDT_Set* set = NULL;
    dict* m = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dictGetVectorClock(de);
        VectorClockUnit unit = getVectorClockUnit(vc, gid);
        if(get_logic_clock(unit) < logic_time) {
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
        dictAddVectorClock(m, field, vc);
        updateCrdtSetLastVc(set, vc);
    }
    dictReleaseIterator(di);
    if(set != NULL) {
        setCrdtSetLastVc(set, getCrdtSetLastVc(t));
    }
    return (CrdtObject**)result;
}


VectorClock getCrdtSetTombstoneMaxDelVc(CRDT_SetTombstone* t) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    return tom->maxDelvectorClock;
}

int updateCrdtSetTombstoneMaxDel(CRDT_SetTombstone* t, VectorClock vc) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(t);
    VectorClock now = vectorClockMerge(tom->maxDelvectorClock,vc);
    return setCrdtSetTombstoneMaxDel(t, now);
}


CRDT_SetTombstone* dupCrdtSetTombstone(CRDT_SetTombstone* t) {
    CRDT_ORSET_SETTOMBSTONE* target = retrieveCrdtORSETSetTombstone(t);
    CRDT_ORSET_SETTOMBSTONE* result = (CRDT_ORSET_SETTOMBSTONE*)createCrdtSetTombstone();
    dict* target_map = getSetTombstoneDict(target);
    dict* result_map = getSetTombstoneDict(result);
    dictIterator* di = dictGetIterator(target_map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock v = dupVectorClock(dictGetVectorClock(de));
        dictAddVectorClock(result_map, sdsdup(field), v);
    }
    dictReleaseIterator(di);
    setCrdtSetTombstoneLastVc((CRDT_SetTombstone*)result, dupVectorClock(getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)target)));
    setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)result, dupVectorClock(getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)target)));

    return (CRDT_SetTombstone*)result;
}

int appendSetTombstone(CRDT_SetTombstone* t, CRDT_SetTombstone* s) {
    CRDT_ORSET_SETTOMBSTONE* target = retrieveCrdtORSETSetTombstone(t);
    CRDT_ORSET_SETTOMBSTONE* src = retrieveCrdtORSETSetTombstone(s);
    dict* src_map = getSetTombstoneDict(src);
    dict* target_map = getSetTombstoneDict(target);
    dictEntry* de = NULL;
    dictIterator* di = dictGetIterator(src_map);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        dictEntry* d = dictFind(target_map, field);
        VectorClock vc = dictGetVectorClock(de);
        if(d == NULL) {
            VectorClock nvc = dupVectorClock(vc);
            dictAddVectorClock(target_map, sdsdup(field), nvc);
        } else {
            VectorClock tvc = dictGetVectorClock(d);
            VectorClock nvc = vectorClockMerge(tvc, vc);
            dictSetUnsignedIntegerVal(d, VC2LL(nvc));
            freeVectorClock(tvc);
        }
    }
    return 1;
}


dictEntry* findSetTombstoneDict(CRDT_SetTombstone* tom, sds field) {
    CRDT_ORSET_SETTOMBSTONE* tombstone = retrieveCrdtORSETSetTombstone(tom);
    dict* map = getSetTombstoneDict(tombstone);
    return dictFind(map, field);
}

size_t getSetTombstoneDictSize(CRDT_SetTombstone* data) {
    CRDT_ORSET_SETTOMBSTONE* tom = retrieveCrdtORSETSetTombstone(data);
    dict* map = getSetTombstoneDict(tom);
    return dictSize(map);
}


sds setTombstoneIterInfo(void *data) {
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(LL2VC(data));
    result = sdscatprintf(result, "type: orset_set_tombstone, vector-clock: %s",
            vcStr);
    sdsfree(vcStr);
    return result;

}
