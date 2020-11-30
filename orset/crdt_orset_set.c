#include "crdt_orset_set.h"
#define NDEBUG
#include <assert.h>
/**    module function **/
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

//utils
crdt_orset_set* retrieve_crdt_orset_set(void* t) {
    if(t == NULL) {
        return NULL;
    }
    crdt_orset_set* result = (crdt_orset_set*)t;
    assert(result->map != NULL);
    return result;
}


crdt_orset_set_tombstone* retrieve_set_tombstone(void* t) {
    if(t == NULL) {
        return NULL;
    }
    crdt_orset_set_tombstone* result = (crdt_orset_set_tombstone*)t;
    return result;
}

VectorClock dict_entry_get_vc(dictEntry* de) {
    long long v = dictGetUnsignedIntegerVal(de);
    return LL2VC(v);
}

void dict_entry_set_vc(dictEntry* de, VectorClock vc) {
    dictSetUnsignedIntegerVal(de, VC2LL(vc));
}

// 1 add 
// 0 update 
//-1 update fail
int dict_try_add_vcu(dict* d, sds field, int gid, long long vcu) {
    dictEntry* de = dictFind(d, field);
    VectorClock vc;
    int result = de ? 0: 1;
    if(de) {    
        vc = dict_entry_get_vc(de);
        int index = 0;
        long long ovcu = get_vcu_from_vc(vc, gid, &index);
        if(index == -1) {
            vc = addVectorClockUnit(vc, gid, vcu);
        } else{
            if(ovcu < vcu) {
                set_clock_unit_by_index(&vc, index, init_clock((char)gid, vcu));
            } else {
                return -1;
            }
        }
        
    } else {
        de = dictAddRaw(d, sdsdup(field), NULL);
        vc = newVectorClockFromGidAndClock(gid, vcu);
    }
    dict_entry_set_vc(de, vc);
    return result;
}

int dict_add_or_update_vc(dict* d, sds field, VectorClock other, int dup_field) {
    dictEntry* de = dictFind(d, field);
    VectorClock vc;
    int result = de ? 0: 1;
    if(de) {    
        vc = dict_entry_get_vc(de);
        VectorClock mvc = vectorClockMerge(vc, other);
        freeVectorClock(vc);
        vc = mvc;
    } else {
        de = dictAddRaw(d, dup_field? sdsdup(field): field, NULL);
        vc = dupVectorClock(other);
    }
    dict_entry_set_vc(de, vc);
    return result;
}

int dict_try_clean_vc(dict* d, sds field, VectorClock* other) {
    dictEntry* de = dictFind(d, field);
    VectorClock vc;
    if(de) {    
        vc = dict_entry_get_vc(de);
        int result = purget_vc(other, &vc);
        dict_entry_set_vc(de, vc);
        if(result == PURGE_VAL) {
            dictDelete(d, field);
        } 
        return result;
    } 
    return PURGE_VAL;
}

int dict_merge_dict(dict* target, dict* other) {
    dictIterator* di = dictGetSafeIterator(other);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dict_entry_get_vc(de);
        dictEntry* rde =  dictFind(target, field);
        VectorClock rvc;
        if(rde == NULL) {
            rde = dictAddRaw(target, sdsdup(field), NULL);
            rvc = dupVectorClock(vc);
        } else {
            rvc = dict_entry_get_vc(rde);
            VectorClock mvc = vectorClockMerge(rvc, vc);
            freeVectorClock(rvc);
            rvc = mvc;
        }
        dict_entry_set_vc(rde, rvc);
    }
    dictReleaseIterator(di);
}

#define COMPARE_VECTORCLOCK_LT -1
#define COMPARE_VECTORCLOCK_GT 1
#define COMPARE_VECTORCLOCK_CONFFLICT 0
int purget_dict_entry(dict* di,  sds field,  dictEntry* de, VectorClock vc) {
    VectorClock old = dict_entry_get_vc(de);
    if(isVectorClockMonoIncr(old, vc)) {
        dictDelete(di, dictGetKey(de));
        return COMPARE_VECTORCLOCK_GT;
    } else if(isVectorClockMonoIncr(vc, old)) {
        return COMPARE_VECTORCLOCK_LT;
    } else {
        old = purgeVectorClock(old, vc);
        dict_entry_set_vc(de, old);
        return COMPARE_VECTORCLOCK_CONFFLICT;
    }
}

void save_set_dict_to_rdb(RedisModuleIO *rdb, dict* map) {
    RedisModule_SaveUnsigned(rdb, dictSize(map));
    dictIterator *di = dictGetIterator(map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dict_entry_get_vc(de);
        RedisModule_SaveStringBuffer(rdb, field, sdslen(field));
        rdbSaveVectorClock(rdb, vc, CRDT_RDB_VERSION);
    }
    dictReleaseIterator(di);
}

sds set_element_info(dictEntry* de) {
    sds result = sdsempty();
    VectorClock vc = dict_entry_get_vc(de);
    sds vcStr = vectorClockToSds(vc);
    result = sdscatprintf(result, "type: set_element, vector-clock: %s",
            vcStr);
    sdsfree(vcStr);
    return result;
}

/****************  abdout set **************/
/****************  set  public  function +****************/
CRDT_Set* createCrdtSet() {
    crdt_orset_set* set = RedisModule_Alloc(sizeof(crdt_orset_set));
    set->type = 0;
    setType((CrdtObject*)set , CRDT_DATA);
    setDataType((CrdtObject*)set , CRDT_SET_TYPE);
    dict *map = dictCreate(&crdtSetDictType, NULL);
    set->dict = map;
    set->lastVc = newVectorClock(0);
    return (CRDT_Set* )set;
}

void freeCrdtSet(void* data) {
    crdt_orset_set* set = retrieve_crdt_orset_set(data);
    dict* dict = getSetDict((CRDT_Set*)data);
    dictRelease(dict);
    setCrdtSetLastVc((CRDT_Set*)set, newVectorClock(0));
    RedisModule_Free(set);
}

VectorClock getCrdtSetLastVc(CRDT_Set* data) {
    crdt_orset_set* set = retrieve_crdt_orset_set(data);
    return set->lastVc;
}

int setCrdtSetLastVc(CRDT_Set* data, VectorClock vc) {
    crdt_orset_set* s = retrieve_crdt_orset_set(data);
    VectorClock old = getCrdtSetLastVc(data);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    s->lastVc = vc;
    return 1;
}

int updateCrdtSetLastVc(CRDT_Set* data, VectorClock vc) {
    crdt_orset_set* s = retrieve_crdt_orset_set(data);
    VectorClock now = vectorClockMerge(s->lastVc,vc);
    return setCrdtSetLastVc(data, now);
}

int updateCrdtSetLastVcuByVectorClock(CRDT_Set* data, int gid, VectorClock vc) {
    crdt_orset_set* s = retrieve_crdt_orset_set(data);
    VectorClock v = s->lastVc;
    mergeLogicClock(&v, &vc, gid);
    s->lastVc = v;
    return 1;
}

dict* getSetDict(CRDT_Set* data) {
    crdt_orset_set* set = retrieve_crdt_orset_set(data);
    return (dict*)(set->dict);
}

dictIterator* getSetIterator(CRDT_Set* data) {
    dict* map = getSetDict(data);
    return dictGetSafeIterator(map);
}

dictEntry* findSetDict(CRDT_Set* data, sds field) {
    dict* map = getSetDict(data);
    return dictFind(map, field);
}

size_t getSetSize(CRDT_Set* data) {
    dict* map = getSetDict(data);
    return dictSize(map);
}

int removeSetDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
    dict* map = getSetDict(data);
    return dictDelete(map, field) == DICT_OK ? 1: 0;
}

sds setIterInfo(dictEntry* de) {
    sds result = sdsempty();
    VectorClock vc = dict_entry_get_vc(de);
    sds vcStr = vectorClockToSds(vc);
    result = sdscatprintf(result, "type: orset_set, vector-clock: %s",
            vcStr);
    sdsfree(vcStr);
    return result;
}


// add 1
// update 0
// int addOrUpdateSetElementVcu(CRDT_Set* data, sds field, CrdtMeta* meta) {
//     dictEntry* de = NULL;
//     dict* map = getSetDict(data);
//     int gid = getMetaGid(meta);
//     long long vcu = get_vcu_by_meta(meta);
//     de = dictFind(map, field);
//     VectorClock vc;
//     if (de == NULL) {
//         de = dictAddRaw(map, sdsdup(field), NULL);
//         vc = newVectorClockFromGidAndClock(gid, vcu);
//     } else {
//         VectorClock vc = dict_entry_get_vc(de);
//         int index = 0;
//         long long old_vcu = get_vcu_from_vc(vc, gid, &index);
//         if(old_vcu == 0) {
//             vc = addVectorClockUnit(vc, gid, vcu);
//         } else {
//             clk clock_unit = init_clock((char)gid, vcu);
//             set_clock_unit_by_index(&vc, index, clock_unit);
//         }
//     }
//     dict_entry_set_vc(de, vc);
// }


CRDT_Set* dupCrdtSet(CRDT_Set* target) {
    CRDT_Set* result = createCrdtSet();
    dict* target_map = getSetDict(target);
    dict* result_map = getSetDict(result);
    dictIterator* di = dictGetIterator(target_map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock v = dupVectorClock(dict_entry_get_vc(de));
        dictEntry* rde = dictAddRaw(result_map, sdsdup(field), NULL);
        dict_entry_set_vc(rde, v);
    }
    dictReleaseIterator(di);
    setCrdtSetLastVc(result, dupVectorClock(getCrdtSetLastVc(target)));
    return result;
}



sds getRandomSetKey(CRDT_Set* data) {
    dict* di = getSetDict(data);
    dictEntry* de = dictGetFairRandomKey(di);
    return dictGetKey(de);
}

//module 


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
        dict_add_or_update_vc(map, field, vc, 0);
        freeVectorClock(vc);
        RedisModule_Free(str);
    }
    return set;
}
void RdbSaveCrdtSet(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    crdt_orset_set *set = retrieve_crdt_orset_set(value);
    rdbSaveVectorClock(rdb, getCrdtSetLastVc((CRDT_Set*)set), CRDT_RDB_VERSION);
    dict* map = getSetDict((CRDT_Set*)set);
    save_set_dict_to_rdb(rdb, map);
}
void AofRewriteCrdtSet(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

size_t crdtSetMemUsageFunc(const void *value) {
    return 1;
}

void crdtSetDigestFunc(RedisModuleDigest *md, void *value) {
    
}

sds crdtSetInfo(void *data) {
    crdt_orset_set* set = retrieve_crdt_orset_set(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtSetLastVc((CRDT_Set*)set));
    result = sdscatprintf(result, "type: orset_set,  last-vc: %s\n",
            vcStr);
    dictIterator* di = dictGetIterator(set->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds info = set_element_info(de);
        result = sdscatprintf(result, "  key: %s, %s\n", dictGetKey(de), info);
        sdsfree(info);
    }
    dictReleaseIterator(di);
    sdsfree(vcStr);
    return result;
}
void freeSetFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtSet(filters[i]);
    }
    RedisModule_Free(filters);
}
CrdtObject** crdtSetFilter(CrdtObject* t, int gid, long long logic_time, long long maxsize, int* num) {
    crdt_orset_set* target = retrieve_crdt_orset_set(t);
    CRDT_Set** result = NULL;
    dict* map = getSetDict((CRDT_Set*)target);
    dictIterator *di = dictGetIterator(map);
    dictEntry *de;
    int current_memory = 0;
    CRDT_Set* set = NULL;
    dict* m = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dict_entry_get_vc(de);
        // VectorClockUnit unit = getVectorClockUnit(vc, gid);
        // if(get_logic_clock(unit) < logic_time) {
        if(get_vcu_from_vc(vc, gid, NULL) < logic_time) {
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
        // dictAddVectorClock(m, field, vc);
        dict_add_or_update_vc(m, field, vc, 0);
        updateCrdtSetLastVc(set, vc);
    }
    dictReleaseIterator(di);
    if(set != NULL) {
        setCrdtSetLastVc(set, getCrdtSetLastVc(t));
    }
    return (CrdtObject**)result;
}


crdt_orset_set* replace_set_value(crdt_orset_set* target, crdt_orset_set* other) {
    dict* d = target->dict;
    target->dict = other->dict;
    other->dict = d;
    
    VectorClock vc = target->lastVc;
    target->lastVc = other->lastVc;
    other->lastVc = vc;

    return target;
}
//only used sync merge object
CrdtObject *crdtSetMerge(CrdtObject *currentVal, CrdtObject *value) {
    crdt_orset_set* target = retrieve_crdt_orset_set(currentVal);
    crdt_orset_set* other = retrieve_crdt_orset_set(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    crdt_orset_set* result = createCrdtSet();
    if (target == NULL) {
        return replace_set_value(result, other);
    }
    result = replace_set_value(result, target);
    if(value == NULL) return result;
    dict* d = getSetDict(other);
    dict* rd = getSetDict(result);
    dict_merge_dict(rd, d);
    updateCrdtSetLastVc(result, getCrdtSetLastVc(other));
    return (CrdtObject*)result;
}
// int addSetDict(CRDT_Set* data, sds field, CrdtMeta* meta) {
//     dict* map = getSetDict(data);
//     VectorClock vc = dupVectorClock(getMetaVectorClock(meta));
//     return  dictAddVectorClock(map, sdsdup(field), vc);
// }
/****************  set tombstone  public  function ****************/

CRDT_SetTombstone* createCrdtSetTombstone() {
    crdt_orset_set_tombstone* tombstone = RedisModule_Alloc(sizeof(crdt_orset_set_tombstone));
    tombstone->type = 0;
    setType((CrdtObject*)tombstone , CRDT_TOMBSTONE);
    setDataType((CrdtObject*)tombstone , CRDT_SET_TYPE);
    dict *map = dictCreate(&crdtSetDictType, NULL);
    tombstone->dict = map;
    tombstone->lastVc = newVectorClock(0);
    tombstone->maxDelvectorClock = newVectorClock(0);
    return (CRDT_SetTombstone*)tombstone;
}

dict* getSetTombstoneDict(crdt_orset_set_tombstone* tombstone) {
    return (dict*)(tombstone->dict);
}

VectorClock getCrdtSetTombstoneMaxDelVc(CRDT_SetTombstone* t) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    return tom->maxDelvectorClock;
}

CRDT_SetTombstone* dupCrdtSetTombstone(CRDT_SetTombstone* t) {
    crdt_orset_set_tombstone* target = retrieve_set_tombstone(t);
    crdt_orset_set_tombstone* result = (crdt_orset_set_tombstone*)createCrdtSetTombstone();
    dict* target_map = getSetTombstoneDict(target);
    dict* result_map = getSetTombstoneDict(result);
    dictIterator* di = dictGetIterator(target_map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock v = dupVectorClock(dict_entry_get_vc(de));
        dict_add_or_update_vc(result_map, field, v, 1);
    }
    dictReleaseIterator(di);
    setCrdtSetTombstoneLastVc((CRDT_SetTombstone*)result, dupVectorClock(getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)target)));
    setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)result, dupVectorClock(getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)target)));

    return (CRDT_SetTombstone*)result;
}

void freeCrdtSetTombstone(void* data) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(data);
    dict* map = getSetTombstoneDict(tom);
    dictRelease(map);
    setCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, newVectorClock(0));
    setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, newVectorClock(0));
    RedisModule_Free(tom);
}

VectorClock getCrdtSetTombstoneLastVc(CRDT_SetTombstone* t) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    return tom->lastVc;
}

int setCrdtSetTombstoneMaxDel(CRDT_SetTombstone* data, VectorClock vc) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(data);
    VectorClock old = getCrdtSetTombstoneMaxDelVc(data);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    }
    tom->maxDelvectorClock = vc;
    return 1;
}

int setCrdtSetTombstoneLastVc(CRDT_SetTombstone* data, VectorClock vc) {
    crdt_orset_set_tombstone* r = retrieve_set_tombstone(data);
    VectorClock old = getCrdtSetTombstoneLastVc(data);
    if(!isNullVectorClock(old)) {
        freeVectorClock(old);
    } 
    r->lastVc = vc;
    return 1;
}

int updateCrdtSetTombstoneLastVc(CRDT_SetTombstone* data, VectorClock vc) {
    crdt_orset_set_tombstone* r = retrieve_set_tombstone(data);
    VectorClock now = vectorClockMerge(r->lastVc, vc);
    return setCrdtSetTombstoneLastVc(data, now);
}

int updateCrdtSetTombstoneLastVcByMeta(CRDT_SetTombstone* data, CrdtMeta* meta) {
    return updateCrdtSetTombstoneLastVc(data, getMetaVectorClock(meta));
}




int addOrUpdateSetTombstoneDictValue(CRDT_Set* data, sds field, CrdtMeta* meta) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(data);
    dict* map = getSetTombstoneDict(tom);
    dictEntry* de = dictFind(map, field);
    VectorClock vc;
    if(de) {
        vc = dict_entry_get_vc(de);
        VectorClock mvc  = vectorClockMerge(vc, getMetaVectorClock(meta));
        freeVectorClock(vc);
        vc = mvc;
    } else {
        vc = dupVectorClock(getMetaVectorClock(meta));
        de = dictAddRaw(map, sdsdup(field), NULL);
    }
    dict_entry_set_vc(de, vc);
    return 1;
}


dictEntry* findSetTombstoneDict(CRDT_SetTombstone* tom, sds field) {
    dict* map = getSetTombstoneDict(tom);
    return dictFind(map, field);
}

//module function
void *RdbLoadCrdtORSETSetTombstone(RedisModuleIO *rdb, int version, int encver) {
    crdt_orset_set_tombstone* tom = (crdt_orset_set_tombstone* )createCrdtSetTombstone();
    
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
        dict_add_or_update_vc(map, field, vc, 0);
        freeVectorClock(vc);
        RedisModule_Free(str);
    }
    setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, maxdel);
    setCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, lastvc);
    return tom;
}
void RdbSaveCrdtSetTombstone(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    crdt_orset_set_tombstone *tom = retrieve_set_tombstone(value);
    rdbSaveVectorClock(rdb, getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom), CRDT_RDB_VERSION);
    rdbSaveVectorClock(rdb, getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)tom), CRDT_RDB_VERSION);
    dict* map = getSetTombstoneDict(tom);
    save_set_dict_to_rdb(rdb, map);
}

void AofRewriteCrdtSetTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

size_t crdtSetTombstoneMemUsageFunc(const void *value) {
    return 1;
}

void crdtSetTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    
}

int setStartGc() {
    set_gc_stats = 1;
    return set_gc_stats;
}

int setStopGc() {
    set_gc_stats = 0;
    return set_gc_stats;
}

int crdtSetTombstoneGc(CrdtTombstone* data, VectorClock clock) {
    if(!set_gc_stats) {
        return 0;
    }
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(data);
    int result = isVectorClockMonoIncr(getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom), clock);
    if(result) {
        #if defined(DEBUG) 
            sds info = crdtSetTombstoneInfo(tom);
            RedisModule_Debug("notice", "[gc] set_tombstone :%s", info);
            sdsfree(info);
        #endif
    }
    return result;
}

void freeSetTombstoneFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtSetTombstone(filters[i]);
    }
    RedisModule_Free(filters);
}

CrdtObject** crdtSetTombstoneFilter(CrdtTombstone* t, int gid, long long logic_time, long long maxsize,int* num)  {
    crdt_orset_set_tombstone* target = retrieve_set_tombstone(t);
    crdt_orset_set_tombstone** result = NULL;
    dict* map = getSetTombstoneDict(target);
    dictIterator *di = dictGetIterator(map);
    dictEntry *de = NULL;
    int current_memory = 0;
    crdt_orset_set_tombstone* tom = NULL;
    dict* m = NULL;

    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dict_entry_get_vc(de);
        if(get_vcu(vc, gid) < logic_time) {
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
            tom = (crdt_orset_set_tombstone*)(createCrdtSetTombstone());
            m = getSetTombstoneDict(tom);
            current_memory = 0;
            m->type = &crdtSetFileterDictType;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(crdt_orset_set_tombstone*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(crdt_orset_set_tombstone*));
            }
            result[(*num)-1] = tom;
        }
        current_memory += size;
        dict_add_or_update_vc(m, field, vc, 0);
        updateCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, vc);
    }
    dictReleaseIterator(di);
    if(tom == NULL && *num == 0) {
        VectorClockUnit unit = getVectorClockUnit(getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)target), gid);
        if(get_logic_clock(unit) < logic_time) {
            return NULL;
        }  
        tom = (crdt_orset_set_tombstone*)(createCrdtSetTombstone());
        m = getSetTombstoneDict(tom);
        m->type = &crdtSetFileterDictType;
        result = RedisModule_Alloc(sizeof(crdt_orset_set_tombstone*));
        result[0] = tom;
        *num = 1;
    }
    if(tom != NULL) {
        updateCrdtSetTombstoneLastVc((CRDT_SetTombstone*)tom, getCrdtSetTombstoneLastVc((CRDT_SetTombstone*)target));
        updateCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, getCrdtSetTombstoneMaxDelVc((CRDT_SetTombstone*)target));
    }

    return (CrdtObject**)result;
}


sds crdtSetTombstoneInfo(void *data) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(data);
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtSetTombstoneLastVc(data));
    sds maxVcStr = vectorClockToSds(tom->maxDelvectorClock);
    result = sdscatprintf(result, "type: orset_set_tombstone,  last-vc: %s, maxdel-vc: %s\n",
            vcStr, maxVcStr);
    sdsfree(vcStr);
    sdsfree(maxVcStr);
    dictIterator* di = dictGetIterator(tom->dict);
    dictEntry* de;
    while((de = dictNext(di)) != NULL) {
        sds info = set_element_info(de);
        result = sdscatprintf(result, "  key: %s, vc: %s\n", dictGetKey(de), info);
        sdsfree(info);
    }
    dictReleaseIterator(di);
    return result;
}

int purget_vc(VectorClock* stvc, VectorClock* svc) {
    if (isNullVectorClock(*stvc) ) {
        return PURGE_TOMBSTONE;
    }
    if (isNullVectorClock(*svc)) {
        return PURGE_VAL;
    }
    VectorClock nstvc = newVectorClock(0);
    VectorClock nsvc = newVectorClock(0);
    for(int i = 0, len = get_len(*stvc); i < len; i++) {
        clk* st_clk = get_clock_unit_by_index(stvc, i);
        unsigned char gid = (unsigned char) get_gid(*st_clk);
        long long st_vcu = get_logic_clock(*st_clk);
        int index = 0;
        long long s_vcu = get_vcu_from_vc(*svc, gid, &index);
        if (index == -1 || (index != -1 && s_vcu <= st_vcu)) {
            nstvc = addVectorClockUnit(nstvc, gid, st_vcu);
        } else {
            nsvc = addVectorClockUnit(nsvc, gid, s_vcu);
        }
    }
    for(int i = 0, len = get_len(*svc); i < len; i++) {
        clk* s_clk = get_clock_unit_by_index(svc, i);
        unsigned char gid = (unsigned char) get_gid(*s_clk);
        int si = 0, ti = 0;
        get_vcu_from_vc(nsvc, gid, &si);
        get_vcu_from_vc(nstvc, gid, &ti);
        if( si != -1 || ti != -1) {
            continue;
        }
        nsvc = addVectorClockUnit(nsvc, gid, (long long) (get_logic_clock(*s_clk)));
    }
    freeVectorClock(*stvc);
    freeVectorClock(*svc);
    *stvc = nstvc;
    *svc = nsvc;
    if (isNullVectorClock(*stvc) ) {
        return PURGE_TOMBSTONE;
    }
    if (isNullVectorClock(*svc)) {
        return PURGE_VAL;
    }
    return PURGE_NONE;
}

// abdout tombstone and set purge
int crdtSetTombstonePurge(CrdtTombstone* tombstone, CrdtData* data) {
    if(!isCrdtSetTombstone(tombstone) || !isCrdtSet(data)) {
        return 0;
    }
    crdt_orset_set* set = retrieve_crdt_orset_set(data);
    dict* sd = getSetDict(data);
    dict* std = getSetTombstoneDict(tombstone);
    dictIterator* di = dictGetSafeIterator(sd);
    VectorClock maxdel = getCrdtSetTombstoneMaxDelVc(tombstone);
    dictEntry* de = NULL;
    while((de = dictNext(di))!= NULL) {
        sds field = dictGetKey(de);
        VectorClock vc = dict_entry_get_vc(de);
        if(isVectorClockMonoIncr(vc, maxdel)) {
            dictDelete(sd, field);
            continue;
        }
        vc = purgeVectorClock(vc, maxdel);
        dictEntry* tde = dictFind(std, field);
        if(tde) {
            VectorClock tvc = dict_entry_get_vc(tde);
            if(isVectorClockMonoIncr(tvc, vc)) {
                dictDelete(std, field);
            } else if(isVectorClockMonoIncr(vc, tvc)) {
                dictDelete(sd, field);
                continue;
            } else {
                purget_vc(&tvc, &vc);
                dict_entry_set_vc(tde, tvc);
            }
        }
        dict_entry_set_vc(de, vc);
    }
    dictReleaseIterator(di);
    if(dictSize(sd) == 0) {
        return PURGE_VAL;
    }
    if(isNullVectorClock(maxdel) && dictSize(std) == 0) {
        return PURGE_TOMBSTONE;
    }
    return PURGE_NONE;
}


VectorClock delVectorClockUnit(VectorClock vc, int gid) {
    int index = 0;
    long long vcu = get_vcu_from_vc(vc, gid, &index);
    if(index == -1) return vc;
    int len = get_len(vc);
    VectorClock nvc = newVectorClock(len - 1);
    int ni = 0;
    for(int i = 0; i < len; i++) {
        if(i != index) {
            clk* c = get_clock_unit_by_index(&vc, i);
            set_clock_unit_by_index(&nvc, ni++, *c);
        }
    } 
    freeVectorClock(vc);
    return nvc;
}

int dict_try_clean_vcu(dict* d, sds field, int gid, long long vcu) {
    dictEntry* de = dictFind(d, field);
    if(de == NULL) return 1;
    VectorClock vc = dict_entry_get_vc(de);
    long long current_vcu = get_vcu(vc,gid);
    if(current_vcu >= vcu) return 0;
    vc = delVectorClockUnit(vc, gid);
    dict_entry_set_vc(de, vc);
    if(isNullVectorClock(vc)) {
        dictDelete(d, field);
    } 
    return 1;
}
// about set method
int setTryAdd(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    crdt_orset_set* set = retrieve_crdt_orset_set(s);
    int gid = getMetaGid(meta);
    long long vcu = get_vcu_by_meta(meta);
    if(t) { 
        VectorClock maxdel =  getCrdtSetTombstoneMaxDelVc(t);
        if(get_vcu(maxdel, gid) > vcu) {
            return 0;
        }
        dict* td = getSetTombstoneDict(t);
        if(dict_try_clean_vcu(td, field, gid, vcu) == 0) {
            return 0;
        }
    }
    dict* sd = getSetDict(s);
    return dict_try_add_vcu(sd, field, gid, vcu) >= 0? 1: 0;
}

int setAdd(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    crdt_orset_set* set = retrieve_crdt_orset_set(s);
    int gid = getMetaGid(meta);
    long long vcu = get_vcu_by_meta(meta);
    if(t) {
        dict* td = getSetTombstoneDict(t);
        dict_try_clean_vcu(td, field, gid, vcu);
    }
    dict* sd = getSetDict(s);
    int result = dict_try_add_vcu(sd, field, gid, vcu);
    assert(result >= 0);
    return result;
}

int setTryRem(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    VectorClock vc = dupVectorClock(getMetaVectorClock(meta));
    if(s) {
        crdt_orset_set* set = retrieve_crdt_orset_set(s);
        dict* sd = getSetDict(s);
        if(dict_try_clean_vc(sd, field, &vc) == PURGE_TOMBSTONE) {
            freeVectorClock(vc);
            return 0;
        }
    }
    VectorClock maxdel = getCrdtSetTombstoneMaxDelVc(t);
    if(isVectorClockMonoIncr(vc, maxdel)) {
        freeVectorClock(vc);
        return 0;
    }
    dict* td = getSetTombstoneDict(t);
    int result = dict_add_or_update_vc(td, field, vc, 1);
    freeVectorClock(vc);
    return result;
}

//1 rem success
//0 rem success
int setRem(CRDT_Set* s, CRDT_SetTombstone* t, sds field, CrdtMeta* meta) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    VectorClock vc = getMetaVectorClock(meta);
    if(s) {
        crdt_orset_set* set = retrieve_crdt_orset_set(s);
        dict* sd = getSetDict(s);
        vc = dupVectorClock(vc);
        int result = dict_try_clean_vc(sd, field, &vc);
        if(result == -2) { //no value 
            freeVectorClock(vc);
            return 0;
        }
        assert(result == PURGE_VAL);
        dict* td = getSetTombstoneDict(t);
        dict_add_or_update_vc(td, field, vc, 1);
        freeVectorClock(vc);
        return 1;
    }
    return 0;
}

int setDel(CRDT_Set* s, CRDT_SetTombstone* t, CrdtMeta* meta) {
    appendVCForMeta(meta, getCrdtSetLastVc(s));
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    VectorClock vc = getMetaVectorClock(meta);
    assert(isVectorClockMonoIncr(getCrdtSetTombstoneMaxDelVc(t), vc));
    if (!isNullVectorClock(tom->maxDelvectorClock)) {
        freeVectorClock(tom->maxDelvectorClock);    
    }
    tom->maxDelvectorClock = dupVectorClock(vc);  
    if(!isNullVectorClock(tom->lastVc)) {
        freeVectorClock(tom->lastVc);
    }
    tom->lastVc = dupVectorClock(vc);
    dictRelease(tom->dict);
    tom->dict = dictCreate(&crdtSetDictType, NULL);
    return 1;
}

int setTryDel(CRDT_Set* s, CRDT_SetTombstone* t, CrdtMeta* meta) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    VectorClock vc = getMetaVectorClock(meta);
    if(isVectorClockMonoIncr(vc, getCrdtSetTombstoneMaxDelVc(t))) {
        return 0;
    }
    if(s) {
        crdt_orset_set* set = retrieve_crdt_orset_set(s);
        dict* sd = getSetDict(s);
        dictIterator* sdi = dictGetSafeIterator(sd);
        dictEntry* de = NULL;
        while((de = dictNext(sdi)) != NULL) {
            VectorClock svc = dict_entry_get_vc(de);
            svc = purgeVectorClock(svc, vc);
            dict_entry_set_vc(de, svc);
            if(isNullVectorClock(svc)) {
                sds field = dictGetKey(de);
                dictDelete(sd, field); 
            } 
        }  
        dictReleaseIterator(sdi);
    }
    
    dict* td = getSetTombstoneDict(t);
    dictIterator* tdi = dictGetSafeIterator(td);
    dictEntry* tde = NULL;
    while((tde = dictNext(tdi)) != NULL) {
        VectorClock tvc = dict_entry_get_vc(tde);
        tvc = purgeVectorClock(tvc, vc);
        dict_entry_set_vc(tde, tvc);
        if(isNullVectorClock(tvc)) {
            sds field = dictGetKey(tde);
            dictDelete(td, field); 
        } 
    }  
    dictReleaseIterator(tdi);
    updateCrdtSetTombstoneMaxDel(t, vc);
    updateCrdtSetTombstoneLastVc(t, vc);
    return 1;
}

crdt_orset_set_tombstone* replace_set_tombstone_value(crdt_orset_set_tombstone* target, crdt_orset_set_tombstone* other) {
    dict* d = target->dict;
    target->dict = other->dict;
    other->dict = d;
    
    VectorClock vc = target->lastVc;
    target->lastVc = other->lastVc;
    other->lastVc = vc;

    vc = target->maxDelvectorClock;
    target->maxDelvectorClock = other->maxDelvectorClock;
    other->maxDelvectorClock = vc;
    return target;
}

CrdtTombstone* crdtSetTombstoneMerge(CrdtTombstone* currentVal, CrdtTombstone* value) {
    crdt_orset_set_tombstone* target = retrieveCrdtSetTombstone(currentVal);
    crdt_orset_set_tombstone* other = retrieveCrdtSetTombstone(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    crdt_orset_set_tombstone* result = createCrdtSetTombstone();
    if (target == NULL) {
        return replace_set_tombstone_value(result, other);
    }
    result = replace_set_tombstone_value(result, target);
    if(other == NULL) return result;
    dict* d = getSetTombstoneDict(other);
    dict* rd = getSetTombstoneDict(result);
    dict_merge_dict(rd, d);
    updateCrdtSetTombstoneLastVc(result, getCrdtSetTombstoneLastVc(other));
    updateCrdtSetTombstoneMaxDel(result, getCrdtSetTombstoneMaxDelVc(other));

    return (CrdtTombstone*)result;
}


sds setTombstoneIterInfo(dictEntry* data) {
    sds result = sdsempty();
    VectorClock vc = dict_entry_get_vc(data);
    sds vcStr = vectorClockToSds(vc);
    result = sdscatprintf(result, "type: orset_set_tombstone, vector-clock: %s",
            vcStr);
    sdsfree(vcStr);
    return result;

}


int updateCrdtSetTombstoneMaxDel(CRDT_SetTombstone* t, VectorClock vc) {
    crdt_orset_set_tombstone* tom = retrieve_set_tombstone(t);
    VectorClock now = vectorClockMerge(tom->maxDelvectorClock,vc);
    return setCrdtSetTombstoneMaxDel((CRDT_SetTombstone*)tom, now);
}

int isNullSetTombstone(CRDT_SetTombstone* t) {
    int is_null = 1;
    dict* map = getSetTombstoneDict(t);
    if(dictSize(map) != 0) {
        is_null = 0;
    }
    if(!isNullVectorClock(getCrdtSetTombstoneMaxDelVc(t))) {
        is_null = 0;
    }
    return is_null;
}