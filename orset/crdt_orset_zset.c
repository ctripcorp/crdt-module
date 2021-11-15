#include "crdt_orset_zset.h"
#include <math.h>
/******************* crdt_zset dict +*******************/



void dictCrdtSSDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    crdt_element el = *(crdt_element*)&val;
    free_internal_crdt_element(el);
    free_external_crdt_element(el);
}

static dictType crdtZSetDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtSSDestructor   /* val destructor */
};


void save_zset_dict_to_rdb(sio *io, dict* d) {
    sioSaveUnsigned(io, dictSize(d));
    dictIterator *di = dictGetIterator(d);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        sioSaveStringBuffer(io, field, sdslen(field));
        save_crdt_element_to_rdb(io, el);
    }
    dictReleaseIterator(di);
}
/******************* crdt_zset dict +*******************/



/******************* crdt_zset function +*******************/

CRDT_SS* createCrdtZset() {
    struct crdt_zset* s = RedisModule_Alloc(sizeof(crdt_zset));
    initCrdtObject((CrdtObject*)s);
    setDataType((CrdtObject*)s, CRDT_ZSET_TYPE);
    setType((CrdtObject*)s, CRDT_DATA);
    s->dict = dictCreate(&crdtZSetDictType, NULL);
    s->zsl = zslCreate();
    s->lastvc = newVectorClock(0);
    return (CRDT_SS*)s;
}

crdt_zset* retrieve_crdt_zset(CRDT_SS* rc) {
    return (crdt_zset*)rc;
}

void free_crdt_zset(crdt_zset* zset) {
    freeVectorClock(zset->lastvc);
    dictRelease(zset->dict);
    zslFree(zset->zsl);
    RedisModule_Free(zset);
}

void *load_zset_by_rdb(sio *io, int version, int encver) {
    crdt_zset* set = (crdt_zset*)createCrdtZset();
    VectorClock lastvc = rdbLoadVectorClock(io, version);
    set->lastvc = lastvc;
    uint64_t len = sioLoadUnsigned(io);
    for(int i = 0; i < len; i++) {
        // char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        // sds field = sdsnewlen(str, strLength);
        sds field = sioLoadSds(io);
        crdt_element el = load_crdt_element_from_rdb(io);
        dictEntry* entry = dictAddRaw(set->dict, field, NULL);
        dict_set_element(set->dict, entry, el);
        double score = 0;
        if(!get_double_score_by_element(el, &score)) {
            return NULL;
        }
        zslInsert(set->zsl, score, sdsdup(field));
        // RedisModule_ZFree(str);
    }
    return set;
}

crdt_zset* dup_crdt_zset(crdt_zset* target) {
    crdt_zset* result = (crdt_zset*)createCrdtZset();
    result->lastvc = dupVectorClock(target->lastvc);
    dictIterator* di = dictGetIterator(target->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        crdt_element rel = dup_crdt_element(el);
        dictEntry* rde = dictAddRaw(result->dict, sdsdup(field), NULL);
        dict_set_element(result->dict, rde, rel);
        double score = 0;
        crdtAssert(get_double_score_by_element(rel, &score));
        zslInsert(result->zsl, score, sdsdup(field));
    }
    dictReleaseIterator(di);
    return result;
}

int zset_update_zsl(crdt_zset* ss, double o_score, sds field, crdt_element el) {
    double n_score = 0;
    crdtAssert(get_double_score_by_element(el, &n_score));
    if(n_score != o_score) {
        zskiplistNode* node;
        crdtAssert(zslDelete(ss->zsl, o_score, field, &node));
        zslInsert(ss->zsl, n_score, node->ele);
        node->ele = NULL;
        zslFreeNode(node);
    }
    return n_score != o_score? 1: 0;
}   


crdt_zset* zset_replace_value(crdt_zset* target, crdt_zset* other) {
    dict* d = target->dict;
    target->dict = other->dict;
    other->dict = d;

    zskiplist* zskiplist = target->zsl;
    target->zsl = other->zsl;
    other->zsl = zskiplist;

    VectorClock vc = target->lastvc;
    target->lastvc = other->lastvc;
    other->lastvc = vc;

    return target;
}
//only used peer sync merge object
crdt_zset *crdt_zset_merge(crdt_zset *target, crdt_zset *other) {
    crdt_zset* result = (crdt_zset*)createCrdtZset();
    if (target == NULL && other ==  NULL) {
        return NULL;
    }
    if(target == NULL) {
        //dup_crdt_zset
        return zset_replace_value(result, other);
    }
    result = zset_replace_value(result, target);
    if(other == NULL) {
        return result;
    }
    dictIterator* di = dictGetIterator(other->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        double eold_score = 0;
        crdtAssert(get_double_score_by_element(el, &eold_score));
        dictEntry* rde = dictFind(result->dict, field);
        crdt_element rel;
        if (rde == NULL) {
            rel = el;
            rde = dictAddRaw(result->dict, sdsdup(field), NULL);
            dict_set_element(result->dict, rde, rel);
            double rscore = 0;
            crdtAssert(get_double_score_by_element(rel, &rscore));
            zslInsert(result->zsl, rscore, sdsdup(field));
        } else {
            rel = dict_get_element(rde);
            double old_score = 0;
            crdtAssert(get_double_score_by_element(rel, &old_score));
            rel = merge_crdt_element(rel, el);
            free_external_crdt_element(el);
            dict_set_element(result->dict, rde, rel);
            zset_update_zsl(result, old_score, field, rel);
        }
        zslDelete(other->zsl, eold_score, field, NULL);
        // dict_set_element(de, create_crdt_element());
        dict_clean_element(other->dict, de);
        result->lastvc = append_vc_from_element(result->lastvc, rel);
    }
    dictReleaseIterator(di);
    updateCrdtSSLastVc((CRDT_SS *)result, other->lastvc);
    return result;
}

sds crdtZSetInfo(void *data) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    dictIterator* it = dictGetIterator(zset->dict);
    dictEntry* de ;
    sds result = sdsempty();
    long long size = dictSize(zset->dict);
    sds vc_info = vectorClockToSds(zset->lastvc);
    result = sdscatprintf(result, "type: orset_zset, vc: %s, size: %lld", vc_info, size);
    sdsfree(vc_info);
    int num = 5;
    while((de = dictNext(it)) != NULL && num > 0) {
        crdt_element element = dict_get_element(de);
        sds element_info = get_element_info(element);
        result = sdscatprintf(result, "\n1)  key: %s ", (sds)dictGetKey(de));
        result = sdscatprintf(result, "\n%s", element_info);
        sdsfree(element_info);
        num--;
    } 
    if(num == 0 && de != NULL) {
        result = sdscatprintf(result, "\n...");
    } 
    result = sdscatprintf(result, "\n");
    dictReleaseIterator(it);
    return result;
}

void freeCrdtSS(void* ss) {
    crdt_zset* set = retrieve_crdt_zset(ss);
    free_crdt_zset(set);
}

void freeSSFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtSS(filters[i]);
    }
    RedisModule_Free(filters);
}

VectorClock getCrdtSSLastVc(CRDT_SS* data) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    return zset->lastvc;
}

void updateCrdtSSLastVc(CRDT_SS* data, VectorClock vc) {
    if(isNullVectorClock(vc)) return;
    crdt_zset* zset = retrieve_crdt_zset(data);
    VectorClock old_vc = zset->lastvc;
    zset->lastvc = vectorClockMerge(zset->lastvc, vc);
    if(!isNullVectorClock(old_vc)) {
        freeVectorClock(old_vc);
    }
}

/******************* crdt_zset function -*******************/

/******************* crdt_zset tombstone function +*******************/

CRDT_SSTombstone* createCrdtZsetTombstone() {
    crdt_zset_tombstone* st = RedisModule_Alloc(sizeof(crdt_zset_tombstone));
    initCrdtObject((CrdtObject*)st);
    setDataType((CrdtObject*)st, CRDT_ZSET_TYPE);
    setType((CrdtObject*)st, CRDT_TOMBSTONE);
    st->dict = dictCreate(&crdtZSetDictType, NULL);
    st->lastvc = newVectorClock(0);
    st->maxdelvc = newVectorClock(0);
    return (CRDT_SSTombstone*)st;
}

crdt_zset_tombstone* retrieve_crdt_zset_tombstone(CRDT_SSTombstone* rt) {
    return (crdt_zset_tombstone*)rt;
}

void free_crdt_zset_tombstone(crdt_zset_tombstone* tombstone) {
    dictRelease(tombstone->dict);
    freeVectorClock(tombstone->lastvc);
    freeVectorClock(tombstone->maxdelvc);
    RedisModule_Free(tombstone);
}

void * load_zset_tombstone_by_rdb(sio *io, int version, int encver) {
    crdt_zset_tombstone* zset_tombstone = (crdt_zset_tombstone*)createCrdtZsetTombstone();
    VectorClock lastvc = rdbLoadVectorClock(io, version);
    zset_tombstone->lastvc = lastvc;
    zset_tombstone->maxdelvc = rdbLoadVectorClock(io, version);
    uint64_t len = sioLoadUnsigned(io);
    size_t strLength;
    for(int i = 0; i < len; i++) {
        char* str = sioLoadStringBuffer(io, &strLength);
        sds field = sdsnewlen(str, strLength);
        crdt_element el = load_crdt_element_from_rdb(io);
        dictEntry* entry = dictAddOrFind(zset_tombstone->dict, field);
        // dict_set_element(entry, el);
        dict_set_element(zset_tombstone->dict, entry, el);
        RedisModule_ZFree(str);
    }
    return zset_tombstone;
}

crdt_zset_tombstone* dup_crdt_zset_tombstone(crdt_zset_tombstone* target) {
    crdt_zset_tombstone* result = (crdt_zset_tombstone*)createCrdtZsetTombstone();
    result->lastvc = dupVectorClock(target->lastvc);
    result->maxdelvc = dupVectorClock(target->maxdelvc);
    dictIterator* di = dictGetIterator(target->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        crdt_element rel = dup_crdt_element(el);
        dictEntry* rde = dictAddRaw(result->dict, sdsdup(field), NULL);
        // dict_set_element(rde, rel);
        dict_set_element(result->dict, rde, rel);
    }
    dictReleaseIterator(di);
    return result;
}

crdt_zset_tombstone* replace_zset_tombstone_value(crdt_zset_tombstone* target, crdt_zset_tombstone* other) {
    dict* d = target->dict;
    target->dict = other->dict;
    other->dict = d;

    VectorClock vc = target->maxdelvc;
    target->maxdelvc = other->maxdelvc;
    other->maxdelvc = vc;

    vc = target->lastvc;
    target->lastvc = other->lastvc;
    other->lastvc = vc;
    return target;
}

crdt_zset_tombstone* crdt_zset_tombstone_merge(crdt_zset_tombstone* target, crdt_zset_tombstone* other) {
    crdt_zset_tombstone* result = (crdt_zset_tombstone*)createCrdtZsetTombstone();
    if (target == NULL && other ==  NULL) {
        return NULL;
    }
    if(target == NULL) {
        return replace_zset_tombstone_value(result, other);
    }
    result = replace_zset_tombstone_value(result, target);
    if(other == NULL) {
        return result;
    }
    dictIterator* di = dictGetIterator(other->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        dictEntry* rde = dictFind(result->dict, field);
        crdt_element rel;
        if(rde == NULL) {
            rde = dictAddRaw(result->dict, sdsdup(field), NULL);
            rel = el;
        } else {
            rel = dict_get_element(rde);
            rel = merge_crdt_element(rel, el);
            free_external_crdt_element(el);
        }
        dict_set_element(result->dict, rde, rel);
        dict_clean_element(other->dict, de);
        result->lastvc = append_vc_from_element(result->lastvc, rel);
    }
    dictReleaseIterator(di);
    updateCrdtSSTLastVc((CRDT_SSTombstone *)result, other->lastvc);
    updateCrdtSSTMaxDel((CRDT_SSTombstone *)result, other->maxdelvc);
    return result;
}

void freeCrdtSST(void* ss) {
    crdt_zset_tombstone* set_tombstone = retrieve_crdt_zset_tombstone(ss);
    free_crdt_zset_tombstone(set_tombstone);
}

void freeSSTFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtSST(filters[i]);
    }
    RedisModule_Free(filters);
}

/******************* crdt_zset tombstone function -*******************/


/*****************  about module type  + ******************/

//public 
//about sorted set module type
static void sioSaveCrdtSS(sio *io, void *value) {
    saveCrdtRdbHeader(io, ORSET_TYPE);
    crdt_zset* zset = retrieve_crdt_zset(value);
    rdbSaveVectorClock(io, getCrdtSSLastVc((CRDT_SS*)zset), CRDT_RDB_VERSION);
    save_zset_dict_to_rdb(io, zset->dict);
} 
void RdbSaveCrdtSS(RedisModuleIO *rdb, void *value) {
    sio *io = rdbStreamCreate(rdb);
    sioSaveCrdtSS(io, value);
    rdbStreamRelease(io);
} 

static void *sioLoadCrdtSS(sio *io, int encver) {
    long long header = loadCrdtRdbHeader(io);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return load_zset_by_rdb(io, version, encver);
    }
    return NULL;
}

void *RdbLoadCrdtSS(RedisModuleIO *rdb, int encver) {
    sio *io = rdbStreamCreate(rdb);
    void *res = sioLoadCrdtSS(io, encver);
    rdbStreamRelease(io);
    return res;
}

CrdtObject** crdtSSFilter2(CrdtObject* data, int gid, VectorClock min_vc, long long maxsize, int* num) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    dictIterator *di = dictGetSafeIterator(zset->dict);
    dictEntry *de;
    crdt_zset** result = NULL;
    crdt_zset* current = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        VectorClock vc = element_get_vc(el);
        if(!not_less_than_vc(min_vc, vc)) {
            freeVectorClock(vc);
            continue;
        }
        freeVectorClock(vc);
        // if(element_get_vcu_by_gid(el, gid) < logic_time) {
        //     continue;
        // }
        int memory = get_crdt_element_memory(el);
        if(memory + sdslen(field) > maxsize) {
            freeSSFilter((CrdtObject**)result, *num);
            printf("[filter crdt_zset] memory error: key-%s \n", field);
            *num = -1;
            return NULL;
        }
        if(current_memory + sdslen(field) + memory > maxsize) {
            current = NULL;
        }
        if(current == NULL) {
            current = (crdt_zset*)createCrdtZset();
            current_memory = 0;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(crdt_zset*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(crdt_zset*));
            }
            result[(*num)-1] = current;
        }
        current_memory += sdslen(field) + memory;
        dictEntry* de = dictAddOrFind(current->dict, sdsdup(field));
        dict_set_element(current->dict, de, el);
    }
    dictReleaseIterator(di);
    if(current != NULL) {
        current->lastvc = dupVectorClock(zset->lastvc);
    }
    return (CrdtObject**)result;
}

CrdtObject** crdtSSFilter(CrdtObject* data, int gid, long long logic_time, long long maxsize, int* num) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    dictIterator *di = dictGetSafeIterator(zset->dict);
    dictEntry *de;
    crdt_zset** result = NULL;
    crdt_zset* current = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        if(element_get_vcu_by_gid(el, gid) < logic_time) {
            continue;
        }
        int memory = get_crdt_element_memory(el);
        if(memory + sdslen(field) > maxsize) {
            freeSSFilter((CrdtObject**)result, *num);
            printf("[filter crdt_zset] memory error: key-%s \n", field);
            *num = -1;
            return NULL;
        }
        if(current_memory + sdslen(field) + memory > maxsize) {
            current = NULL;
        }
        if(current == NULL) {
            current = (crdt_zset*)createCrdtZset();
            current_memory = 0;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(crdt_zset*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(crdt_zset*));
            }
            result[(*num)-1] = current;
        }
        current_memory += sdslen(field) + memory;
        dictEntry* de = dictAddOrFind(current->dict, sdsdup(field));
        dict_set_element(current->dict, de, el);
    }
    dictReleaseIterator(di);
    if(current != NULL) {
        current->lastvc = dupVectorClock(zset->lastvc);
    }
    return (CrdtObject**)result;
}


CrdtObject *crdtSSMerge(CrdtObject *a, CrdtObject *b) {
    crdt_zset* target = retrieve_crdt_zset(a);
    crdt_zset* other = retrieve_crdt_zset(b);
    return (CrdtObject*)crdt_zset_merge(target, other);
}

void AofRewriteCrdtSS(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

void crdtSSDigestFunc(RedisModuleDigest *md, void *value) {

}

size_t crdtSSMemUsageFunc(const void *value) {
    return 1;
}

int getScore(CRDT_SS* ss, sds field, double* d) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    dictEntry* de =  dictFind(zset->dict,field);
    if(de == NULL) {
        return 0;
    } 
    crdt_element el = dict_get_element(de);
    return get_double_score_by_element(el, d);
}

size_t crdtZsetLength(CRDT_SS* ss) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    return dictSize(zset->dict);
}


//about sorted set tombstone module type
static void sioSaveCrdtSST(sio *io, void *value) {
    saveCrdtRdbHeader(io, ORSET_TYPE);
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(value);
    rdbSaveVectorClock(io, getCrdtSSTLastVc((CRDT_SSTombstone*)zset_tombstone), CRDT_RDB_VERSION);
    rdbSaveVectorClock(io, zset_tombstone->maxdelvc, CRDT_RDB_VERSION);
    save_zset_dict_to_rdb(io, zset_tombstone->dict);
} 
void RdbSaveCrdtSST(RedisModuleIO *rdb, void *value) {
    sio *io = rdbStreamCreate(rdb);
    sioSaveCrdtSST(io, value);
    rdbStreamRelease(io);
} 

static void *sioLoadCrdtSST(sio *io, int encver) {
    long long header = loadCrdtRdbHeader(io);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return load_zset_tombstone_by_rdb(io, version, encver);
    }
    return NULL;
}
void *RdbLoadCrdtSST(RedisModuleIO *rdb, int encver) {
    sio *io = rdbStreamCreate(rdb);
    void *res = sioLoadCrdtSST(io, encver);
    rdbStreamRelease(io);
    return res;
}

CrdtTombstone** crdtSSTFilter2(CrdtTombstone* target, int gid, VectorClock min_vc, long long maxsize,int* num) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(target);
    dictIterator *di = dictGetSafeIterator(zset_tombstone->dict);
    dictEntry *de;
    crdt_zset_tombstone** result = NULL;
    crdt_zset_tombstone* current = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        VectorClock vc = element_get_vc(el);
        if(!not_less_than_vc(min_vc, vc)) {
            freeVectorClock(vc);
            continue;
        }
        freeVectorClock(vc);
        // if(element_get_vcu_by_gid(el, gid) < logic_time) {
        //     continue;
        // }
        int memory = get_crdt_element_memory(el);
        if(memory + sdslen(field) > maxsize) {
            freeSSTFilter((CrdtObject**)result, *num);       
            printf("[filter crdt_zset] memory error: key-%s \n", field);
            *num = -1;
            return NULL;
        }
        if(current_memory + sdslen(field) + memory > maxsize) {
            current = NULL;
        }
        if(current == NULL) {
            current = (crdt_zset_tombstone*)createCrdtZsetTombstone();
            current_memory = 0;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(crdt_zset*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(crdt_zset*));
            }
            result[(*num)-1] = current;
        }
        current_memory += sdslen(field) + memory;
        dictEntry* de = dictAddOrFind(current->dict, sdsdup(field));
        // dict_set_element(de, el);
        dict_set_element(current->dict, de, el);
    }
    dictReleaseIterator(di);
    if(current == NULL && *num == 0) {
        // long long vcu = get_vcu(zset_tombstone->maxdelvc, gid);
        // if(vcu < logic_time) {
        //     return NULL;
        // } 
        if(!not_less_than_vc(min_vc, zset_tombstone->maxdelvc)) {
            return NULL;
        }
        current = (crdt_zset_tombstone*)createCrdtZsetTombstone();
        result = RedisModule_Alloc(sizeof(crdt_zset*));
        result[0] = current;
        *num = 1;
    }
    if(current != NULL) {
        current->lastvc = dupVectorClock(zset_tombstone->lastvc);
        current->maxdelvc = dupVectorClock(zset_tombstone->maxdelvc);
    }
    return (CrdtObject**)result;
}

CrdtTombstone** crdtSSTFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* num) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(target);
    dictIterator *di = dictGetSafeIterator(zset_tombstone->dict);
    dictEntry *de;
    crdt_zset_tombstone** result = NULL;
    crdt_zset_tombstone* current = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = dict_get_element(de);
        if(element_get_vcu_by_gid(el, gid) < logic_time) {
            continue;
        }
        int memory = get_crdt_element_memory(el);
        if(memory + sdslen(field) > maxsize) {
            freeSSTFilter((CrdtObject**)result, *num);       
            printf("[filter crdt_zset] memory error: key-%s \n", field);
            *num = -1;
            return NULL;
        }
        if(current_memory + sdslen(field) + memory > maxsize) {
            current = NULL;
        }
        if(current == NULL) {
            current = (crdt_zset_tombstone*)createCrdtZsetTombstone();
            current_memory = 0;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(crdt_zset*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(crdt_zset*));
            }
            result[(*num)-1] = current;
        }
        current_memory += sdslen(field) + memory;
        dictEntry* de = dictAddOrFind(current->dict, sdsdup(field));
        // dict_set_element(de, el);
        dict_set_element(current->dict, de, el);
    }
    dictReleaseIterator(di);
    if(current == NULL && *num == 0) {
        long long vcu = get_vcu(zset_tombstone->maxdelvc, gid);
        if(vcu < logic_time) {
            return NULL;
        } 
        current = (crdt_zset_tombstone*)createCrdtZsetTombstone();
        result = RedisModule_Alloc(sizeof(crdt_zset*));
        result[0] = current;
        *num = 1;
    }
    if(current != NULL) {
        current->lastvc = dupVectorClock(zset_tombstone->lastvc);
        current->maxdelvc = dupVectorClock(zset_tombstone->maxdelvc);
    }
    return (CrdtObject**)result;
}

CrdtTombstone* crdtSSTMerge(CrdtTombstone* a, CrdtTombstone* b) {
    crdt_zset_tombstone* target = retrieve_crdt_zset_tombstone(a);
    crdt_zset_tombstone* other = retrieve_crdt_zset_tombstone(b);
    return (CrdtTombstone*)crdt_zset_tombstone_merge(target, other);
}

sds crdtZsetTombstoneInfo(void* tombstone) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(tombstone);
    dictIterator* it = dictGetIterator(zset_tombstone->dict);
    dictEntry* de ;
    sds result = sdsempty();
    long long size = dictSize(zset_tombstone->dict);
    sds vc_info = vectorClockToSds(zset_tombstone->lastvc);
    sds max_vc_info = vectorClockToSds(zset_tombstone->maxdelvc);
    result = sdscatprintf(result, "type: orset_zset_tombstone, vc: %s, maxvc:%s, size: %lld", vc_info, max_vc_info, size);
    sdsfree(vc_info);
    sdsfree(max_vc_info);
    int num = 5;
    while((de = dictNext(it)) != NULL) {
        crdt_element element = dict_get_element(de);
        sds element_info = get_element_info(element);
        result = sdscatprintf(result, "\n1) tombstone key: %s ", (sds)dictGetKey(de));
        result = sdscatprintf(result, "\n%s", element_info);
        sdsfree(element_info);
        num--;
    } 
    if(num == 0 && de != NULL) {
        result = sdscatprintf(result, "\n   ...\n");
    }
    dictReleaseIterator(it);
    return result;
}

int zsetStopGc() {
    zset_gc_stats = 0;
    return zset_gc_stats;
}
int zsetStartGc() {
    zset_gc_stats = 1;
    return zset_gc_stats;
}

int crdtZsetTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    if(!zset_gc_stats) {
        return 0;
    }
    int result = isVectorClockMonoIncr(getCrdtSSTLastVc(target), clock);
    if(result) {
        #if defined(DEBUG)  
            sds info = crdtZsetTombstoneInfo(target);
            RedisModule_Debug("notice", "[gc] zset_tombstone :%s", info);
            sdsfree(info);
        #endif
    }
    return result;
}

int crdtZsetTombstonePurge(CrdtTombstone* tombstone, CrdtData* value) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* zset = retrieve_crdt_zset(value);
    if(zset_tombstone == NULL) return PURGE_TOMBSTONE;
    if(zset == NULL) return PURGE_VAL;
    dictIterator *tdi = dictGetSafeIterator(zset_tombstone->dict);
    dictEntry *tde, *de;
    while((tde = dictNext(tdi)) != NULL) {
        sds field = dictGetKey(tde);
        de = dictFind(zset->dict, field);
        if(de == NULL) {
            continue;
        } 
        crdt_element tel = dict_get_element(tde);
        crdt_element el = dict_get_element(de);
        double old_score = 0;
        crdtAssert(get_double_score_by_element(el, &old_score));
        int result = purge_element(&tel, &el);
        // dict_set_element(tde, tel);
        dict_set_element(zset_tombstone->dict, tde, tel);
        // dict_set_element(de, el);
        dict_set_element(zset->dict, de, el);
        if(result == PURGE_VAL) {
            dictDelete(zset->dict, field);
            zslDelete(zset->zsl, old_score, field, NULL);
        }else{
            zset_update_zsl(zset, old_score, field, el);
            dictDelete(zset_tombstone->dict, field);
        }
    }
    dictReleaseIterator(tdi);
    if(dictSize(zset_tombstone->dict) == 0) {
        updateCrdtSSLastVc((CRDT_SS*)zset, zset_tombstone->lastvc);
        return PURGE_TOMBSTONE;
    }
    if(dictSize(zset->dict) == 0) {
        updateCrdtSSTLastVc((CRDT_SSTombstone*)zset_tombstone, zset->lastvc);
        return PURGE_VAL;
    }
    updateCrdtSSLastVc((CRDT_SS*)zset, zset_tombstone->lastvc);
    updateCrdtSSTLastVc((CRDT_SSTombstone*)zset_tombstone, zset->lastvc);
    return 0;
}

void AofRewriteCrdtSST(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

void crdtSSTDigestFunc(RedisModuleDigest *md, void *value) {

}

size_t crdtSSTMemUsageFunc(const void *value) {
    return 1;
}

VectorClock getCrdtSSTLastVc(CRDT_SSTombstone* data) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(data);
    return zset_tombstone->lastvc;
}

size_t zsetTombstoneLength(CRDT_SSTombstone* ss) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(ss);
    return dictSize(zset_tombstone->dict);
}

void updateCrdtSSTMaxDel(CRDT_SSTombstone* data, VectorClock vc) {
    if(isNullVectorClock(vc)) return;
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(data);
    VectorClock old_vc = zset_tombstone->maxdelvc;
    zset_tombstone->maxdelvc = vectorClockMerge(zset_tombstone->maxdelvc, vc);
    if(!isNullVectorClock(old_vc)) {
        freeVectorClock(old_vc);
    }
}

VectorClock getCrdtSSTMaxDelVc(CRDT_SSTombstone* data) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(data);
    return zset_tombstone->maxdelvc;
}

void updateCrdtSSTLastVc(CRDT_SSTombstone* data, VectorClock vc) {
    if(isNullVectorClock(vc)) return;
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(data);
    VectorClock old_vc = zset_tombstone->lastvc;
    zset_tombstone->lastvc = vectorClockMerge(zset_tombstone->lastvc, vc);
    if(!isNullVectorClock(old_vc)) {
        freeVectorClock(old_vc);
    }
}



int initSSTombstoneFromSS(CRDT_SSTombstone* tombstone,CrdtMeta* del_meta, CRDT_SS* value, sds* del_counters) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    int gid = getMetaGid(del_meta);
    long long vcu = get_vcu_by_meta(del_meta);
    updateCrdtSSTLastVc((CRDT_SSTombstone*)sst, getMetaVectorClock(del_meta));
    updateCrdtSSTMaxDel((CRDT_SSTombstone*)sst, getMetaVectorClock(del_meta));
    dictIterator* dit = dictGetIterator(sst->dict); 
    dictEntry* tde = NULL;
    VectorClock vc = getMetaVectorClock(del_meta);
    while((tde = dictNext(dit)) != NULL) {
        crdt_element tel = dict_get_element(tde);
        tel = element_try_clean_by_vc(tel, vc, NULL);
        // dict_set_element(tde, tel);
        dict_set_element(sst->dict ,tde, tel);
    }
    dictReleaseIterator(dit);

    dictIterator* next = dictGetIterator(ss->dict);
    dictEntry* de = NULL;
    
    int index = 0;
    while((de = dictNext(next)) != NULL) {
        crdt_element el = dict_get_element(de);
        el = element_clean(el, gid, vcu, 1);
        // dict_set_element(de, create_crdt_element());
        dict_clean_element(ss->dict, de);
        
        sds info = get_field_and_delete_counter_str(dictGetKey(de), el, 0);
        if(info) {
            del_counters[index++] = info;
        }
        tde = dictAddRaw(sst->dict, sdsdup(dictGetKey(de)), NULL);
        // dict_set_element(tde, el);
        dict_set_element(sst->dict, tde, el);
        
    }
    dictReleaseIterator(next);
    //to do clean tombstone
    return index;
}

/*****************  about module type  - ******************/




dict* getZsetDict(CRDT_SS* current) {
    if(current == NULL) return NULL;
    crdt_zset* zset = retrieve_crdt_zset(current);
    return zset->dict;
}

//crdt_zset public function
//public 
unsigned long zsetGetRank(CRDT_SS* current, double score, sds ele) {
    if(current == NULL) return 0;
    crdt_zset* zset = retrieve_crdt_zset(current);
    return zslGetRank(zset->zsl, score, ele);
}

zskiplistNode* zset_get_zsl_element_by_rank(CRDT_SS* current, int reverse, long start) {
    crdt_zset* zset = retrieve_crdt_zset(current);
    size_t llen = dictSize(zset->dict);
    zskiplistNode *ln;
    if (reverse) {
        ln = zset->zsl->tail;
        if (start > 0)
            ln = zslGetElementByRank(zset->zsl,llen-start);
    } else {
        ln = zset->zsl->header->level[0].forward;
        if (start > 0)
            ln = zslGetElementByRank(zset->zsl,start+1);
    }
    return ln;
}

zskiplistNode* zslInRange(CRDT_SS* current, zrangespec* range, int reverse) {
    crdt_zset* zset = retrieve_crdt_zset(current);
    if(reverse) {
        return zslLastInRange(zset->zsl, range);
    } else {
        return zslFirstInRange(zset->zsl, range);
    }
}

zskiplistNode* zslInLexRange(CRDT_SS* current, zlexrangespec* range, int reverse) {
    crdt_zset* zset = retrieve_crdt_zset(current);
    if(reverse) {
        return zslLastInLexRange(zset->zsl, range);
    } else {
        return zslFirstInLexRange(zset->zsl, range);
    }
}
//*+

int zset_add_element(crdt_zset* ss,sds field, crdt_element el) {
    dictEntry* de = dictAddRaw(ss->dict, sdsdup(field), NULL);
    // dict_set_element(de,  el);
    dict_set_element(ss->dict, de, el);
    double n_score = 0;
    crdtAssert(get_double_score_by_element(el, &n_score));
    zslInsert(ss->zsl, n_score, sdsdup(field));
    return 1;
}
typedef crdt_element (*ElementMergeFunc)(crdt_element el, void* v);

crdt_element element_merge_element(crdt_element el, void* v) {
    crdt_element other = *(crdt_element*)v;
    return merge_crdt_element(el, other);
}








int zset_update_element(crdt_zset* ss, dictEntry* de, sds field, void* value, ElementMergeFunc merge_fun) {
    crdt_element el = dict_get_element(de);
    double o_score = 0;
    crdtAssert(get_double_score_by_element(el, &o_score));
    el = merge_fun(el, value);
    // dict_set_element(de, el);
    dict_set_element(ss->dict, de, el);
    zset_update_zsl(ss, o_score, field, el);
    return 1;
}

//*-




//try 

int zsetTryAdd(CRDT_SS* value, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    VectorClock vc = getMetaVectorClock(meta);
    g_counter_meta* gcounters[get_len(vc)];
    // union all_type v = {.f = 0};
    // long long type = 0;
    ctrip_value v = {.type = VALUE_TYPE_NONE, .value.i = 0};
    int gcounter_len = str_2_value_and_g_counter_metas(info, &v, gcounters);
    crdtAssert(gcounter_len != -1);
    crdtAssert(v.type == VALUE_TYPE_DOUBLE);
    crdt_tag* b = (crdt_tag*)create_base_tag_by_meta(meta, v);
    crdt_element rel =  create_element_from_vc_and_g_counter(vc, gcounter_len, gcounters, b);
    free_ctrip_value(v);
    if(sst) {
        if(isVectorClockMonoIncr(vc, sst->maxdelvc) ) {
            free_internal_crdt_element(rel);
            free_external_crdt_element(rel);
            return 0;
        }
        dictEntry* tde = dictFind(sst->dict, field);
        if(tde != NULL) {
            crdt_element tel = dict_get_element(tde);
            int result = purge_element(&tel, &rel);
            // dict_set_element(tde, tel);
            dict_set_element(sst->dict, tde, tel);
            if(result == PURGE_VAL) {
                free_external_crdt_element(rel);
                return 0;
            } else if(result == PURGE_TOMBSTONE) {
                dictDelete(sst->dict, field);
            }
        }
    }
    dictEntry* de = dictFind(ss->dict, field);
    if(de) {
        zset_update_element(ss, de, field, &rel, element_merge_element);
        free_external_crdt_element(rel);
    } else {
        zset_add_element(ss, field, rel);
    }
    return 1;
}



int zsetTryIncrby(CRDT_SS* value, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds score_str) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    VectorClock vc = getMetaVectorClock(meta);
    double score = 0;
    int gid = getMetaGid(meta);
    // long long type = 0; 
    // union all_type v = {.d=0};
    ctrip_value v = {.type = VALUE_TYPE_NONE, .value.i = 0};
    int gcounter_len = str_2_value_and_g_counter_metas(score_str, &v, NULL);
    assert(gcounter_len == 0);
    assert(v.type == VALUE_TYPE_DOUBLE);
    score = v.value.d;
    crdt_tag_add_counter* a = create_add_tag(gid);
    a->add_counter.f = score;
    a->add_vcu = get_vcu_by_meta(meta);
    a->counter_type = VALUE_TYPE_DOUBLE;
    free_ctrip_value(v);
    if(tombstone) {
        if(isVectorClockMonoIncr(vc, sst->maxdelvc) ) {
            free_crdt_tag((crdt_tag*)a);
            return 0;
        }
        dictEntry* tde = dictFind(sst->dict, field);
        if(tde != NULL) {
            crdt_element tel = dict_get_element(tde);
            int index;
            crdt_tag* tag = element_get_tag_by_gid(tel, gid, &index);
            if(tag) {
                tag = merge_crdt_tag(tag, (crdt_tag*)a);
                tel = element_set_tag_by_index(tel, index, tag); 
                free_crdt_tag((crdt_tag*)a);
                if(is_deleted_tag(tag)) {
                    // dict_set_element(tde, tel);
                    dict_set_element(sst->dict, tde, tel);
                    return 0;
                }  
            } else {
                tel = element_add_tag(tel, (crdt_tag*)a);
            }
            // dict_set_element(tde, create_crdt_element());
            dict_clean_element(sst->dict, tde);
            dictDelete(sst->dict, field);
            zset_add_element(ss, field, tel);
            return 1;
        }
    }
    dictEntry* de = dictFind(ss->dict, field);
    if(de) {
        zset_update_element(ss, de, field, a, element_merge_tag);
    } else {
        crdt_element el =  create_crdt_element();
        el = element_add_tag(el, (crdt_tag*)a);
        zset_add_element(ss, field, el);
    }
    return 1;
}

int zsetTryRem(CRDT_SSTombstone* tombstone,CRDT_SS* value, sds info, CrdtMeta* meta) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    VectorClock vc = getMetaVectorClock(meta);
    g_counter_meta* gcounters[get_len(vc)];
    // unfree v(ctrip_value)  because field(sds) = v.value.s
    ctrip_value v = {.type = VALUE_TYPE_NONE, .value.i = 0};
    int gcounter_len = str_2_value_and_g_counter_metas(info, &v, gcounters);
    crdtAssert(gcounter_len != -1);
    crdtAssert(v.type == VALUE_TYPE_SDS);
    sds field = v.value.s;
    dictEntry* de = NULL;
    crdt_element rel = create_element_from_vc_and_g_counter(vc, gcounter_len, gcounters, NULL);
    if(ss) {
        de = dictFind(ss->dict, field);
        if(de) {
            crdt_element el =  dict_get_element(de);
            double o_score = 0;
            crdtAssert(get_double_score_by_element(el, &o_score));
            int result = purge_element(&rel, &el);
            // dict_set_element(de, el);
            dict_set_element(ss->dict, de, el);
            if(result == PURGE_TOMBSTONE) {
                zset_update_zsl(ss, o_score, field, el);
                free_ctrip_value(v);
                free_external_crdt_element(rel);
                return 0;
            } 
            zslDelete(ss->zsl, o_score, field, NULL);
            dictDelete(ss->dict, field);
        }
        
    }
    if(sst) {
         dictEntry* tde = dictFind(sst->dict, field);
        if(tde) {
            crdt_element tel =  dict_get_element(tde);
            rel = merge_crdt_element(rel, tel);
            free_external_crdt_element(tel);
        } else {
            tde = dictAddRaw(sst->dict,sdsdup(field), NULL);
        }   
        // dict_set_element(tde, rel);
        dict_set_element(sst->dict, tde, rel);
    }
    free_ctrip_value(v);
    return 1;
    
}



//
// sds element_add_counter_by_tag(crdt_element* el,  crdt_tag_add_counter* rtag) {
//     int gid = get_tag_gid(rtag);
//     int index = 0;
//     crdt_tag* tag = element_get_tag_by_gid(*el, gid, &index);
//     if(tag) {
//         tag = tag_add_or_update(tag, rtag, 1);
//         if(tag == NULL) {
//             return NULL;
//         }
//         element_set_tag_by_index(el, index, tag);
//         free_crdt_tag(rtag);
//         return get_add_value_sds_from_tag(tag);
//     } else {
//         *el = element_add_tag(*el, rtag);
//         return get_add_value_sds_from_tag(rtag);
//     }
// }

int add_score_is_nan(crdt_element el, double score) {
    double d = 0;
    crdtAssert(get_double_add_counter_score_by_element(el, 0, &d));
    d += score;
    if(isnan(d)) {
        return 1;
    }
    return 0;
}

sds zsetAdd2(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field, int* flags, double score,  double* newscore) {
    int incr = (*flags &ZADD_INCR) != 0;
    int nx = (*flags  & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; /* We'll return our response flags. */
    /* NaN as input is an error regardless of all the other parameters. */
    if (isnan(score)) {
        *flags = ZADD_NAN;
        return 0;
    }
    crdt_zset* ss = retrieve_crdt_zset(value);
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    int gid = getMetaGid(meta);
    crdt_tag_base b = {.base_data_type = VALUE_TYPE_DOUBLE};
    crdt_tag_add_counter a = {.counter_type = VALUE_TYPE_DOUBLE}; 
    sds result ;
    if(incr) {
        a.add_counter.f = score;
        a.add_vcu = get_vcu_by_meta(meta);
        init_crdt_add_tag_head((crdt_tag*)&a, gid);
    } else {
        b.base_timespace = getMetaTimestamp(meta);
        b.base_vcu = get_vcu_by_meta(meta);
        b.score.f = score;
        init_crdt_base_tag_head(&b, gid);
    }
     updateCrdtSSLastVc((CRDT_SS*)ss, getMetaVectorClock(meta));
    if(sst) {
        dictEntry* tde = dictUnlink(sst->dict, field);
        if(tde) {
            crdt_element tel = dict_get_element(tde);
            if(incr) {
                if(add_score_is_nan(tel, score)) {
                    *flags = ZADD_NAN; 
                    return NULL;
                }
                result = element_add_counter_by_tag2(&tel, &a);
                crdtAssert(get_len(getMetaVectorClock(meta)) >= get_element_len(tel));
            } else {
                tel = element_merge_tag2(tel, &b);
                result = get_base_value_sds_from_element(tel, gid);
                crdtAssert(get_len(getMetaVectorClock(meta)) >=  get_element_len(tel));
            }

            // dict_set_element(tde, create_crdt_element());
            dict_clean_element(sst->dict, tde);
            dictFreeUnlinkedEntry(sst->dict, tde);
            zset_add_element(ss, field, tel);
            *newscore = score;
            *flags |= ZADD_ADDED;
            return result;
        }
    }
    crdt_element el;
    if(ss) {
        dictEntry* de = dictFind(ss->dict, field);
        if(de) {
            if (nx) {
                *flags |= ZADD_NOP;
                return sdsempty();
            }
            el = dict_get_element(de);
            double old_score = 0;
            crdtAssert(get_double_score_by_element(el, &old_score));
            if(incr) {
                if(add_score_is_nan(el, score)) {
                    *flags = ZADD_NAN; 
                    return NULL;
                }
                result = element_add_counter_by_tag2(&el, &a);
                get_double_score_by_element(el, newscore);
                crdtAssert(get_len(getMetaVectorClock(meta)) >= get_element_len(el));
            } else {
                el = element_clean(el, -1, 0, 0);
                el = element_merge_tag2(el, &b);
                result = get_base_value_sds_from_element(el, gid);
                // result = sdscatprintf(result, ",%s", get_delete_counter_sds_from_element(el));
                *newscore = score;
                crdtAssert(get_len(getMetaVectorClock(meta)) >= get_element_len(el));
            }
            // dict_set_element(de, el);
            dict_set_element(ss->dict, de, el);
            if(zset_update_zsl(ss, old_score, field, el)) {
                *flags |= ZADD_UPDATED;
            }
            return result;
        } else if(!xx) {
            el = create_crdt_element();
            el = element_complete_by_vc(el, getMetaVectorClock(meta), gid);
            if(incr) {
                el = element_add_tag(el, dup_crdt_tag((crdt_tag*)&a));
                result = get_add_value_sds_from_tag((crdt_tag*)&a);
                crdtAssert(get_len(getMetaVectorClock(meta)) >= get_element_len(el));
            } else {
                el = element_add_tag(el, dup_crdt_tag((crdt_tag*)&b));
                result = get_base_value_sds_from_element(el, gid);
                crdtAssert(get_len(getMetaVectorClock(meta)) >= get_element_len(el));
            }
            zset_add_element(ss, field, el);
            *newscore = score;
            *flags |= ZADD_ADDED;
            return result;
        } else {
            *flags |= ZADD_NOP;
            return sdsempty();
        }
    }
    return NULL;
}

sds zsetAdd(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field, int* flags, double score, double* newscore) {
    /* Turn options into simple to check vars. */
    int incr = (*flags & ZADD_INCR) != 0;
    int nx = (*flags & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; /* We'll return our response flags. */
    /* NaN as input is an error regardless of all the other parameters. */
    if (isnan(score)) {
        *flags = ZADD_NAN;
        return 0;
    }
    crdt_zset* ss = retrieve_crdt_zset(value);
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_tag* rtag; 
    int gid = getMetaGid(meta);
    sds result ;
    if(incr) {
        crdt_tag_add_counter* a = create_add_tag(gid);
        a->add_counter.f = score;
        a->counter_type = VALUE_TYPE_DOUBLE;
        a->add_vcu = get_vcu_by_meta(meta);
        rtag = (crdt_tag*)a;
    } else {
        crdt_tag_base* b = create_base_tag(gid);
        b->base_data_type = VALUE_TYPE_DOUBLE;
        b->base_timespace = getMetaTimestamp(meta);
        b->base_vcu = get_vcu_by_meta(meta);
        b->score.f = score;
        rtag = (crdt_tag*)b;
    }
    updateCrdtSSLastVc((CRDT_SS*)ss, getMetaVectorClock(meta));
    if(sst) {
        dictEntry* tde = dictUnlink(sst->dict, field);
        if(tde) {
            crdt_element tel = dict_get_element(tde);
            if(incr) {
                if(add_score_is_nan(tel, score)) {
                    *flags = ZADD_NAN; 
                    free_crdt_tag(rtag);
                    return NULL;
                }
                result = element_add_counter_by_tag(&tel, (crdt_tag_add_counter*)rtag);
            } else {
                tel = element_merge_tag(tel, rtag);
                result = get_base_value_sds_from_element(tel, gid);
            }
            // dict_set_element(tde, create_crdt_element());
            dict_clean_element(sst->dict, tde);
            dictFreeUnlinkedEntry(sst->dict, tde);
            zset_add_element(ss, field, tel);
            *newscore = score;
            *flags |= ZADD_ADDED;
            return result;
        }
    }
    crdt_element el;
    if(ss) {
        dictEntry* de = dictFind(ss->dict, field);
        if(de) {
            if (nx) {
                free_crdt_tag(rtag);
                *flags |= ZADD_NOP;
                return sdsempty();
            }
            el = dict_get_element(de);
            double old_score = 0;
            crdtAssert(get_double_score_by_element(el, &old_score));
            if(incr) {
                if(add_score_is_nan(el, score)) {
                    free_crdt_tag(rtag);
                    *flags = ZADD_NAN; 
                    return NULL;
                }
                result = element_add_counter_by_tag(&el, (crdt_tag_add_counter*)rtag);
                get_double_score_by_element(el, newscore);
            } else {
                el = element_clean(el, -1, 0, 0);
                el = element_merge_tag(el, rtag);
                result = get_base_value_sds_from_element(el, gid);
                // result = sdscatprintf(result, ",%s", get_delete_counter_sds_from_element(el));
                *newscore = score;
            }
            // dict_set_element(de, el);
            dict_set_element(ss->dict, de, el);
            if(zset_update_zsl(ss, old_score, field, el)) {
                *flags |= ZADD_UPDATED;
            }
            return result;
        } else if(!xx) {
            el = create_crdt_element();
            el = element_add_tag(el, rtag);
            zset_add_element(ss, field, el);
            *newscore = score;
            if(incr) {
                result = get_add_value_sds_from_tag(rtag);
            } else {
                result = get_base_value_sds_from_element(el, gid);
            }
            *flags |= ZADD_ADDED;
            return result;
        } else {
            free_crdt_tag(rtag);
            *flags |= ZADD_NOP;
            return sdsempty();
        }
    }
    return NULL;
}


sds delete_set_element(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field, double* score) {
    crdt_zset* ss = retrieve_crdt_zset(value);
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    dictEntry* de = dictUnlink(ss->dict, field);
    if(de == NULL) {
        return NULL;
    }
    crdtAssert(!dictFind(sst->dict, field));
    int gid = getMetaGid(meta);
    // crdt_tag_base* del_tag = create_base_tag(gid);
    // del_tag->base_vcu = get_vcu_by_meta(meta);
    // del_tag->base_timespace = DELETED_TIME;
    
    crdt_element el = dict_get_element(de);
    if(score != NULL) {
        crdtAssert(get_double_score_by_element(el, score));
    }

    
    el = element_clean(el, gid, get_vcu_by_meta(meta), 1);
    // el = element_merge_tag(el, del_tag);
    // free_internal_crdt_element_array(el);
    // dict_set_element(de, create_crdt_element());
    dict_clean_element(ss->dict, de);
    //free field must last
    dictFreeUnlinkedEntry(ss->dict,de);
    union all_type field_value = {.s = field};
    sds field_info = value_to_sds(VALUE_TYPE_SDS, field_value);
    
    dictEntry* tde = dictAddRaw(sst->dict, sdsdup(field), NULL);
    // dict_set_element(tde, el);
    dict_set_element(sst->dict, tde, el);
    sds meta_info = get_delete_counter_sds_from_element(el);
    if(meta_info != NULL) {
        field_info = sdscatprintf(field_info, ",%s", meta_info);
        sdsfree(meta_info);
    } 
    
    return field_info;
}

sds zsetRem(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field) {
    double score = 0;
    sds result = delete_set_element(value, tombstone, meta, field, &score);
    if(result == NULL) return NULL;
    crdt_zset* ss = retrieve_crdt_zset(value);
    /* Delete from skiplist. */
    int retval = zslDelete(ss->zsl,score,field,NULL);
    crdtAssert(retval);
    return result;
}


int zsetTryDel(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta) {
    crdt_zset* ss = retrieve_crdt_zset(value);
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    VectorClock vc = getMetaVectorClock(meta);
    if(sst) {
        dictIterator* di = NULL;
        di = dictGetSafeIterator(sst->dict);
        dictEntry *de;
        while((de = dictNext(di)) != NULL) {
            crdt_element el = dict_get_element(de);
            el = element_try_clean_by_vc(el, vc, NULL);
            // dict_set_element(de, el);
            dict_set_element(sst->dict, de, el);
        }
        dictReleaseIterator(di);
    }
    if(ss) {
        dictIterator* di = NULL;
        di = dictGetSafeIterator(ss->dict);
        dictEntry *de;
        while((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            crdt_element el = dict_get_element(de);
            double old_score = 0;
            crdtAssert(get_double_score_by_element(el, &old_score));
            int deleted = 1;
            crdt_element rel = element_try_clean_by_vc(el, vc, &deleted);
            if(deleted) {
                // dict_set_element(de, create_crdt_element());
                dict_clean_element(ss->dict, de);
                dictEntry* tde = dictAddRaw(sst->dict, sdsdup(field), NULL);
                // dict_set_element(tde, rel);
                dict_set_element(sst->dict, tde, rel);
                zslDelete(ss->zsl, old_score, field, NULL);
                dictDelete(ss->dict, field);
            } else {
                // dict_set_element(de, rel);
                dict_set_element(ss->dict, de, rel);
                zset_update_zsl(ss, old_score, field, rel);
            }
        }
        dictReleaseIterator(di);
    }
    return 1;
}

double getZScoreByDictEntry(dictEntry* de) {
    crdt_element el = dict_get_element(de);
    double score = 0;
    crdtAssert(get_double_score_by_element(el, &score));
    return score;
}

long zsetRank(CRDT_SS* ss, sds ele, int reverse) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    zskiplist *zsl = zset->zsl;
    dictEntry *de;
    double score;
    unsigned long llen;
    unsigned long rank = 0;
    llen = crdtZsetLength(ss);
    de = dictFind(zset->dict,ele);
    if (de != NULL) {
        crdt_element el = dict_get_element(de);
        crdtAssert(get_double_score_by_element(el, &score));
        rank = zslGetRank(zsl,score,ele);
        /* Existing elements always have a rank. */
        crdtAssert(rank != 0);
        if (reverse)
            return llen-rank;
        else
            return rank-1;
    } else {
        return -1;
    }
}

unsigned long  zslDeleteRangeByRank(CRDT_SS* current, CRDT_SSTombstone* tombstone, CrdtMeta* meta, unsigned int start, unsigned int end, sds* callback_items, long long* callback_item_byte_size) {
    crdt_zset* zset = retrieve_crdt_zset(current);
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zset->zsl->header;
    for (i = zset->zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    long long byte_size = 0;
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zset->zsl,x,update);
        sds callback_item = delete_set_element(current, tombstone, meta, x->ele, NULL);
        crdtAssert(callback_item != NULL);
        byte_size += sdslen(callback_item);
        callback_items[removed++] = callback_item;
        // dictDelete(zset->dict,x->ele);
        zslFreeNode(x);
        traversed++;
        x = next;
    }
    *callback_item_byte_size = byte_size;
    return removed;
}

unsigned long  zslDeleteRangeByScore(CRDT_SS* current, CRDT_SSTombstone* tombstone, CrdtMeta* meta, zrangespec *range, sds* callback_items, long long* callback_byte_size) {
    crdt_zset* zset = retrieve_crdt_zset(current);
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zset->zsl->header;
    for (i = zset->zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ?
            x->level[i].forward->score <= range->min :
            x->level[i].forward->score < range->min))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;
    long long byte_size = 0;
    /* Delete nodes while in range. */
    while (x &&
           (range->maxex ? x->score < range->max : x->score <= range->max))
    {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zset->zsl,x,update);
        sds callback_item = delete_set_element(current, tombstone, meta, x->ele, NULL);
        crdtAssert(callback_item != NULL);
        byte_size += sdslen(callback_item);
        callback_items[removed++] = callback_item;
        // dictDelete(dict,x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        x = next;
    }
    *callback_byte_size = byte_size;
    return removed;
}

unsigned long  zslDeleteRangeByLex(CRDT_SS* current, CRDT_SSTombstone* tombstone, CrdtMeta* meta, zlexrangespec *range, sds* callback_items, long long* callback_byte_size) {
    crdt_zset* zset = retrieve_crdt_zset(current);
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zset->zsl->header;
    for (i = zset->zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;
    long long byte_size = 0;
    /* Delete nodes while in range. */
    while (x && zslLexValueLteMax(x->ele,range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zset->zsl,x,update);
        sds callback_item = delete_set_element(current, tombstone, meta, x->ele, NULL);
        crdtAssert(callback_item != NULL);
        byte_size += sdslen(callback_item);
        callback_items[removed++] = callback_item;
        // dictDelete(dict,x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        x = next;
    }
    *callback_byte_size = byte_size;
    return removed;
}

sds getZsetElementInfo(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field) {
    crdt_zset* zs = retrieve_crdt_zset(current);
    crdt_zset_tombstone* zst = retrieve_crdt_zset_tombstone(tombstone);
    dictEntry* de = NULL;
    sds result = NULL;
    if(zs != NULL) {
        de = dictFind(zs->dict, field);
        if(de) {
            if(result == NULL) result = sdsempty();
            crdt_element element = dict_get_element(de);
            sds element_info = get_element_info(element);
            result = sdscatprintf(result, "value)  key: %s \n", (sds)dictGetKey(de));
            result = sdscatprintf(result, "%s", element_info);
            sdsfree(element_info);
        }
        
    }

    if(zst != NULL) {
        de = dictFind(zst->dict, field);
        if(de) {
            if(result == NULL) result = sdsempty();
            crdt_element element = dict_get_element(de);
            sds element_info = get_element_info(element);
            result = sdscatprintf(result, "tombstone)  key: %s \n", (sds)dictGetKey(de));
            result = sdscatprintf(result, "%s", element_info);
            sdsfree(element_info);
        }
    }
    return result;
}

int isNullZsetTombstone(CRDT_SSTombstone* tom) {
    crdt_zset_tombstone* zst = retrieve_crdt_zset_tombstone(tom);
    int result = 1;
    if(zst == NULL) {
        return result;
    }
    if(zsetTombstoneLength(tom) != 0) {
        return 0;
    }
    if(!isNullVectorClock(zst->maxdelvc)) {
        return 0;
    }
    return result;
}

void zsetTombstoneTryResizeDict(CRDT_SSTombstone* tombstone) {
    crdt_zset_tombstone* sst =  retrieve_crdt_zset_tombstone(tombstone);
    if(htNeedsResize(sst->dict)) {
        dictResize(sst->dict);
    } 
}

void zsetTryResizeDict(CRDT_SS* current) {
    crdt_zset* ss = retrieve_crdt_zset(current);
    if(htNeedsResize(ss->dict)) {
        dictResize(ss->dict);
    } 
}
