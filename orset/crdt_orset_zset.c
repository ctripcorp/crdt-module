#include "crdt_orset_zset.h"

/******************* crdt_zset dict +*******************/

void dictCrdtSSDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    free_crdt_element(val);
}

static dictType crdtZSetDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtSSDestructor   /* val destructor */
};


void save_zset_dict_to_rdb(RedisModuleIO *rdb, dict* d) {
    RedisModule_SaveUnsigned(rdb, dictSize(d));
    dictIterator *di = dictGetIterator(d);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = get_element_by_dictEntry(de);
        RedisModule_SaveStringBuffer(rdb, field, sdslen(field));
        save_crdt_element_to_rdb(rdb, el);
    }
    dictReleaseIterator(di);
}
/******************* crdt_zset dict +*******************/



/******************* crdt_zset function +*******************/

CRDT_SS* create_crdt_zset() {
    struct crdt_zset* s = RedisModule_Alloc(sizeof(crdt_zset));
    s->type = 0;
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
}

void *load_zset_by_rdb(RedisModuleIO *rdb, int version, int encver) {
    crdt_zset* set = create_crdt_zset();
    VectorClock lastvc = rdbLoadVectorClock(rdb, version);
    set->lastvc = lastvc;
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    size_t strLength;
    for(int i = 0; i < len; i++) {
        // char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        // sds field = sdsnewlen(str, strLength);
        sds field = RedisModule_LoadSds(rdb);
        crdt_element el = load_crdt_element_from_rdb(rdb);
        dictEntry* entry = dictAddOrFind(set->dict, field);
        set_element_by_dictEntry(entry, el);
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
    crdt_zset* result = create_crdt_zset();
    result->lastvc = dupVectorClock(target->lastvc);
    dictIterator* di = dictGetIterator(target->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = get_element_by_dictEntry(de);
        crdt_element rel = dup_crdt_element(el);
        dictEntry* rde = dictAddRaw(result->dict, sdsdup(field), NULL);
        set_element_by_dictEntry(rde, rel);
        double score = 0;
        assert(get_double_score_by_element(rel, &score));
        zslInsert(result->zsl, score, sdsdup(field));
    }
    dictReleaseIterator(di);
    return result;
}

crdt_zset *crdt_zset_merge(crdt_zset *target, crdt_zset *other) {
    if (target == NULL && other ==  NULL) {
        return NULL;
    }
    if(target == NULL) {
        return (CrdtObject*)dup_crdt_zset(other);
    }
    crdt_zset* result = dup_crdt_zset(target);
    if(other == NULL) {return result;}
    dictIterator* di = dictGetIterator(other->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = get_element_by_dictEntry(de);
        double eold_score = 0;
        assert(get_double_score_by_element(el, &eold_score));
        dictEntry* rde = dictFind(result->dict, field);
        crdt_element rel = create_crdt_element();
        if (rde == NULL) {
            rel = el;
            rde = dictAddRaw(result->dict, sdsdup(field), NULL);
            set_element_by_dictEntry(rde, rel);
            double rscore = 0;
            assert(get_double_score_by_element(rel, &rscore));
            zslInsert(result->zsl, rscore, sdsdup(field));
        } else {
            rel = get_element_by_dictEntry(rde);
            double old_score = 0;
            assert(get_double_score_by_element(rel, &old_score));
            rel = merge_crdt_element(rel, el);
            set_element_by_dictEntry(rde, rel);
            zset_update_zsl(result, old_score, field, rel);
        }
        zslDelete(other->zsl, eold_score, field, NULL);
        set_element_by_dictEntry(de, create_crdt_element());
    }
    dictReleaseIterator(di);
    VectorClock old_vc = result->lastvc;
    result->lastvc = vectorClockMerge(result->lastvc, other->lastvc);
    freeVectorClock(old_vc);
    return result;
}

sds crdtZSetInfo(void *data) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    dictIterator* it = dictGetIterator(zset->dict);
    dictEntry* de ;
    sds result = sdsempty();
    long long size = dictSize(zset->dict);
    sds vc_info = vectorClockToSds(zset->lastvc);
    result = sdscatprintf(result, "1) type: orset_zset, vc: %s, size: %lld\n", vc_info, size);
    sdsfree(vc_info);
    while((de = dictNext(it)) != NULL) {
        crdt_element element = get_element_by_dictEntry(de);
        sds element_info = get_element_info(element);
        result = sdscatprintf(result, "2)  key: %s \n", dictGetKey(de));
        result = sdscatprintf(result, "%s", element_info);
        sdsfree(element_info);
    } 
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
    crdt_zset* zset = retrieve_crdt_zset(data);
    VectorClock old_vc = zset->lastvc;
    zset->lastvc = vectorClockMerge(zset->lastvc, vc);
    if(!isNullVectorClock(old_vc)) {
        freeVectorClock(old_vc);
    }
}

/******************* crdt_zset function -*******************/

/******************* crdt_zset tombstone function +*******************/

CRDT_SSTombstone* create_crdt_zset_tombstone() {
    crdt_zset_tombstone* st = RedisModule_Alloc(sizeof(crdt_zset_tombstone));
    st->type = 0;
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
}

void * load_zset_tombstone_by_rdb(RedisModuleIO *rdb, int version, int encver) {
    crdt_zset_tombstone* zset_tombstone = create_crdt_zset_tombstone();
    VectorClock lastvc = rdbLoadVectorClock(rdb, version);
    zset_tombstone->lastvc = lastvc;
    zset_tombstone->maxdelvc = rdbLoadVectorClock(rdb, version);
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    size_t strLength;
    for(int i = 0; i < len; i++) {
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        sds field = sdsnewlen(str, strLength);
        crdt_element el = load_crdt_element_from_rdb(rdb);
        dictEntry* entry = dictAddOrFind(zset_tombstone->dict, field);
        set_element_by_dictEntry(entry, el);
        RedisModule_ZFree(str);
    }
    return zset_tombstone;
}

crdt_zset_tombstone* dup_crdt_zset_tombstone(crdt_zset_tombstone* target) {
    crdt_zset_tombstone* result = create_crdt_zset_tombstone();
    result->lastvc = dupVectorClock(target->lastvc);
    result->maxdelvc = dupVectorClock(target->maxdelvc);
    dictIterator* di = dictGetIterator(target->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = get_element_by_dictEntry(de);
        crdt_element rel = dup_crdt_element(el);
        dictEntry* rde = dictAddRaw(result->dict, sdsdup(field), NULL);
        set_element_by_dictEntry(rde, rel);
    }
    dictReleaseIterator(di);
    return result;
}

crdt_zset_tombstone* crdt_zset_tombstone_merge(crdt_zset_tombstone* target, crdt_zset_tombstone* other) {
    if (target == NULL && other ==  NULL) {
        return NULL;
    }
    if(target == NULL) {
        return (CrdtObject*)dup_crdt_zset_tombstone(other);
    }
    crdt_zset_tombstone* result = dup_crdt_zset_tombstone(target);
    if(other == NULL) {
        return result;
    }
    dictIterator* di = dictGetIterator(other->dict);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        crdt_element el = get_element_by_dictEntry(de);
        dictEntry* rde = dictFind(result->dict, field);
        crdt_element rel;
        if(rde == NULL) {
            rde = dictAddRaw(result->dict, sdsdup(field), NULL);
            rel = el;
        } else {
            rel = get_element_by_dictEntry(rde);
            // for(int i = 0, len = el.len; i < len; i++) {
            //     crdt_tag* tag = element_get_tag_by_index(el, i);
            //     int rindex = 0;
            //     crdt_tag* rtag = element_get_tag_by_gid(rel, tag->gid, &rindex);
            //     if(rtag == NULL) {
            //         rtag = dup_crdt_tag(tag);
            //         rel = add_tag_by_element(rel, rtag);
            //     } else {
            //         rtag = merge_crdt_tag(rtag, tag);
            //         element_set_tag_by_index(&rel, rindex, rtag);
            //     }
            // }
            rel = merge_crdt_element(rel, el);
        }
        set_element_by_dictEntry(rde, rel);
        set_element_by_dictEntry(de, create_crdt_element());
    }
    dictReleaseIterator(di);
    VectorClock vc = result->lastvc;
    result->lastvc = vectorClockMerge(result->lastvc , other->lastvc);
    if(!isNullVectorClock(vc)) {
        freeVectorClock(vc);
    }
    vc = result->maxdelvc;
    result->maxdelvc = vectorClockMerge(vc, other->maxdelvc);
    if(!isNullVectorClock(vc)) {
        freeVectorClock(vc);
    }
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
void RdbSaveCrdtSS(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    crdt_zset* zset = retrieve_crdt_zset(value);
    rdbSaveVectorClock(rdb, getCrdtSSLastVc(zset), CRDT_RDB_VERSION);
    save_zset_dict_to_rdb(rdb, zset->dict);
} 

void *RdbLoadCrdtSS(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return load_zset_by_rdb(rdb, version, encver);
    }
    return NULL;
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
        crdt_element el = get_element_by_dictEntry(de);
        int memory = get_crdt_element_memory(el);
        if(memory + sdslen(field) > maxsize) {
            printf("[filter crdt_zset] memory error: key-%s \n", field);
            *num = -1;
            return NULL;
        }
        if(current_memory + sdslen(field) + memory > maxsize) {
            current = NULL;
        }
        if(current == NULL) {
            current = create_crdt_zset();
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
        set_element_by_dictEntry(de, el);
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
    crdt_element el = get_element_by_dictEntry(de);
    return get_double_score_by_element(el, d);
}

size_t crdtZsetLength(CRDT_SS* ss) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    return dictSize(zset->dict);
}


//about sorted set tombstone module type
void RdbSaveCrdtSST(RedisModuleIO *rdb, void *value) {
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(value);
    rdbSaveVectorClock(rdb, getCrdtSSTLastVc(zset_tombstone), CRDT_RDB_VERSION);
    rdbSaveVectorClock(rdb, zset_tombstone->maxdelvc, CRDT_RDB_VERSION);
    save_zset_dict_to_rdb(rdb, zset_tombstone->dict);
} 

void *RdbLoadCrdtSST(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return load_zset_tombstone_by_rdb(rdb, version, encver);
    }
    return NULL;
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
        crdt_element el = get_element_by_dictEntry(de);
        int memory = get_crdt_element_memory(el);
        if(memory + sdslen(field) > maxsize) {
            printf("[filter crdt_zset] memory error: key-%s \n", field);
            *num = -1;
            return NULL;
        }
        if(current_memory + sdslen(field) + memory > maxsize) {
            current = NULL;
        }
        if(current == NULL) {
            current = create_crdt_zset_tombstone();
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
        set_element_by_dictEntry(de, el);
    }
    dictReleaseIterator(di);
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
    result = sdscatprintf(result, "1) type: orset_zset_tombstone, vc: %s, maxvc:%s, size: %lld\n", vc_info, max_vc_info, size);
    sdsfree(vc_info);
    sdsfree(max_vc_info);
    while((de = dictNext(it)) != NULL) {
        crdt_element element = get_element_by_dictEntry(de);
        sds element_info = get_element_info(element);
        result = sdscatprintf(result, "2)  key: %s \n", dictGetKey(de));
        result = sdscatprintf(result, "%s", element_info);
        sdsfree(element_info);
    } 
    dictReleaseIterator(it);
    return result;
}

int crdtZsetTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    // return isVectorClockMonoIncr(getCrdtSSTLastVc(target), clock);
    return 0;
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
        crdt_element tel = get_element_by_dictEntry(tde);
        crdt_element el = get_element_by_dictEntry(de);
        double old_score = 0;
        assert(get_double_score_by_element(el, &old_score));
        int result = purge_element(&tel, &el);
        set_element_by_dictEntry(tde, tel);
        set_element_by_dictEntry(de, el);
        if(result == PURGE_VAL) {
            dictDelete(zset->dict, field);
            zslDelete(zset->zsl, old_score, field, NULL);
        }else{
            // double now_score = 0;
            // assert(get_double_score_by_element(el, &now_score));
            // if(now_score != old_score) {
            //     zskiplistNode* node;
            //     assert(zslDelete(zset->zsl, old_score, field, &node));
            //     zslInsert(zset->zsl, now_score, node->ele);
            //     zslFreeNode(node);
            // }
            zset_update_zsl(zset, old_score, field, el);
            dictDelete(zset_tombstone->dict, field);
        }
    }
    dictReleaseIterator(tdi);
    updateCrdtSSLastVc(zset, zset_tombstone->lastvc);
    updateCrdtSSTLastVc(zset_tombstone, zset->lastvc);
    if(dictSize(zset_tombstone->dict) == 0) {
        return PURGE_TOMBSTONE;
    }
    if(dictSize(zset->dict) == 0) {
        return PURGE_VAL;
    }
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
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(data);
    VectorClock old_vc = zset_tombstone->maxdelvc;
    zset_tombstone->maxdelvc = vectorClockMerge(zset_tombstone->maxdelvc, vc);
    if(!isNullVectorClock(old_vc)) {
        freeVectorClock(old_vc);
    }
}

void updateCrdtSSTLastVc(CRDT_SSTombstone* data, VectorClock vc) {
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
    updateCrdtSSTLastVc(sst, getMetaVectorClock(del_meta));
    updateCrdtSSTMaxDel(sst, getMetaVectorClock(del_meta));
    dictIterator* dit = dictGetIterator(sst->dict); 
    dictEntry* tde = NULL;
    VectorClock vc = getMetaVectorClock(del_meta);
    while((tde = dictNext(dit)) != NULL) {
        crdt_element tel = get_element_by_dictEntry(tde);
        tel = clean_element_by_vc(tel, vc, NULL);
        set_element_by_dictEntry(tde, tel);
    }
    dictReleaseIterator(dit);

    dictIterator* next = dictGetIterator(ss->dict);
    dictEntry* de = NULL;
    
    int index = 0;
    while((de = dictNext(next)) != NULL) {
        crdt_element el = get_element_by_dictEntry(de);
        crdt_element del_el = create_crdt_element();
        for(int i = 0; i < el.len; i++) {
            crdt_tag* tag = element_get_tag_by_index(el, i);
            tag = clean_crdt_tag(tag, tag->gid == gid? vcu: -1);
            del_el = add_tag_by_element(del_el, tag);
        }
        free_crdt_element_array(el);
        set_element_by_dictEntry(de, create_crdt_element());
        if(del_el.len != 0) {
            sds meta_info = get_delete_counter_sds_from_element(del_el);
            union all_type field_value = {.s = dictGetKey(de)};
            sds field_info = value_to_sds(VALUE_TYPE_SDS, field_value);
            if(meta_info == NULL) {
                del_counters[index++] = field_info;
            } else {
                del_counters[index++] = sdscatprintf(field_info, ",%s", meta_info);
                sdsfree(meta_info);
            }
            tde = dictAddRaw(sst->dict, sdsdup(dictGetKey(de)), NULL);
            set_element_by_dictEntry(tde, del_el);
        }
    }
    dictReleaseIterator(next);
    //to do clean tombstone
    return index;
}

/*****************  about module type  - ******************/






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

zskiplistNode* zslInLexRange(CRDT_SS* current, zrangespec* range, int reverse) {
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
    set_element_by_dictEntry(de,  el);
    double n_score = 0;
    assert(get_double_score_by_element(el, &n_score));
    zslInsert(ss->zsl, n_score, sdsdup(field));
    return 1;
}
typedef crdt_element (*ElementMergeFunc)(crdt_element el, void* v);

crdt_element element_merge_element(crdt_element el, void* v) {
    crdt_element other = *(crdt_element*)v;
    return merge_crdt_element(el, other);
}

crdt_element element_clean(crdt_element el) {
    for(int i = 0; i < el.len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        tag = clean_crdt_tag(tag, -1);
        element_set_tag_by_index(&el, i, tag);
    }
    return el;
}

crdt_element element_merge_tag(crdt_element el, void* v) {
    crdt_tag* tag = (crdt_tag*)v;
    int gid = get_tag_gid(tag);
    int index = 0;
    crdt_tag* ntag = element_get_tag_by_gid(el, gid, &index);
    if(ntag) {
        ntag = merge_crdt_tag(ntag, tag);
        free_crdt_tag(tag);
        element_set_tag_by_index(&el, index, ntag);
    } else {
        el = add_tag_by_element(el, tag);
    }
    return el;
}

int zset_update_zsl(crdt_zset* ss, double o_score, sds field, crdt_element el) {
    double n_score = 0;
    assert(get_double_score_by_element(el, &n_score));
    if(n_score != o_score) {
        zskiplistNode* node;
        assert(zslDelete(ss->zsl, o_score, field, &node));
        zslInsert(ss->zsl, n_score, node->ele);
        node->ele = NULL;
        zslFreeNode(node);
    }
    return n_score != o_score? 1: 0;
}   

int zset_update_element(crdt_zset* ss, dictEntry* de, sds field, void* value, ElementMergeFunc merge_fun) {
    crdt_element el = get_element_by_dictEntry(de);
    double o_score = 0;
    assert(get_double_score_by_element(el, &o_score));
    el = merge_fun(el, value);
    set_element_by_dictEntry(de, el);
    zset_update_zsl(ss, o_score, field, el);
    return 1;
}

//*-




//try 
crdt_tag* create_base_tag_by_meta(CrdtMeta* meta, int type, union all_type v) {
    crdt_tag_base* b = create_base_tag(getMetaGid(meta));
    b->base_timespace = meta->timestamp;
    b->base_vcu = get_vcu_by_meta(meta);
    b->base_data_type = type;
    copy_tag_data_from_all_type(type, &b->score, v);
    return b;
}
int zsetTryAdd(CRDT_SS* value, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    VectorClock vc = getMetaVectorClock(meta);
    g_counter_meta* gcounters[get_len(vc)];
    union all_type v = {.f = 0};
    long long type = 0;
    int gcounter_len = str_2_value_and_g_counter_metas(info, &type, &v, gcounters);
    assert(gcounter_len != -1);
    assert(type == VALUE_TYPE_DOUBLE);
    crdt_tag* b = create_base_tag_by_meta(meta, type, v);
    crdt_element rel =  create_element_from_vc_and_g_counter(vc, gcounter_len, gcounters, b);
    if(sst) {
        if(isVectorClockMonoIncr(vc, sst->maxdelvc) ) {
            return 0;
        }
        dictEntry* tde = dictFind(sst->dict, field);
        if(tde != NULL) {
            crdt_element tel = get_element_by_dictEntry(tde);
            int result = purge_element(&tel, &rel);
            set_element_by_dictEntry(tde, tel);
            if(result == PURGE_VAL) {
                return 0;
            } else if(result == PURGE_TOMBSTONE) {
                dictDelete(sst->dict, field);
            }
        }
    }
    dictEntry* de = dictFind(ss->dict, field);
    if(de) {
        zset_update_element(ss, de, field, &rel, element_merge_element);
        long long* a = &rel;
        free_crdt_element((void*)a);
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
    long long type = 0; 
    union all_type v = {.d=0};
    int gcounter_len = str_2_value_and_g_counter_metas(score_str, &type, &v, NULL);
    assert(gcounter_len == 0);
    assert(type == VALUE_TYPE_DOUBLE);
    score = v.d;
    crdt_tag_add_counter* a = create_add_tag(gid);
    a->add_counter.f = score;
    a->add_vcu = get_vcu_by_meta(meta);
    a->counter_type = VALUE_TYPE_DOUBLE;
    if(tombstone) {
        if(isVectorClockMonoIncr(vc, sst->maxdelvc) ) {
            return 0;
        }
        dictEntry* tde = dictFind(sst->dict, field);
        if(tde != NULL) {
            int added = 0;
            crdt_element tel = get_element_by_dictEntry(tde);
            int index;
            crdt_tag* tag = element_get_tag_by_gid(tel, gid, &index);
            if(tag) {
                tag = merge_crdt_tag(tag, a);
                element_set_tag_by_index(&tel, index, tag); 
                if(is_deleted_tag(tag)) {
                    free_crdt_tag(a);
                    return 0;
                }  
            } else {
                tel = add_tag_by_element(tel, a);
            }
            set_element_by_dictEntry(tde, create_crdt_element());
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
        el = add_tag_by_element(el, a);
        zset_add_element(ss, field, el);
    }
    return 1;
}

int zsetTryRem(CRDT_SSTombstone* tombstone,CRDT_SS* value, sds info, CrdtMeta* meta) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    VectorClock vc = getMetaVectorClock(meta);
    int gid = getMetaGid(meta);
    g_counter_meta* gcounters[get_len(vc)];
    long long type = 0;
    union all_type v = {.f = 0};
    int gcounter_len = str_2_value_and_g_counter_metas(info, &type, &v, gcounters);
    assert(gcounter_len != -1);
    assert(type == VALUE_TYPE_SDS);
    sds field = v.s;
    dictEntry* de = NULL;
    crdt_element rel = create_element_from_vc_and_g_counter(vc, gcounter_len, gcounters, NULL);
    if(ss) {
        de = dictFind(ss->dict, field);
        if(de) {
            crdt_element el =  get_element_by_dictEntry(de);
            double o_score = 0;
            assert(get_double_score_by_element(el, &o_score));
            int result = purge_element(&rel, &el);
            set_element_by_dictEntry(de, el);
            if(result == PURGE_TOMBSTONE) {
                zset_update_zsl(ss, o_score, field, el);
                return 0;
            } 
            zslDelete(ss->zsl, o_score, field, NULL);
            dictDelete(ss->dict, field);
        }
        
    }
    if(sst) {
         dictEntry* tde = dictFind(sst->dict, field);
        if(tde) {
            crdt_element tel =  get_element_by_dictEntry(tde);
            rel = merge_crdt_element(rel, tel);
            set_element_by_dictEntry(tde, rel);
        } else {
            tde = dictAddRaw(sst->dict,field, NULL);
            set_element_by_dictEntry(tde, rel);
        }   
    }
}



//
sds element_add_counter_by_tag(crdt_element* el,  crdt_tag_add_counter* rtag) {
    int gid = get_tag_gid(rtag);
    int index = 0;
    crdt_tag* tag = element_get_tag_by_gid(*el, gid, &index);
    if(tag) {
        tag = tag_add_or_update(tag, rtag, 1);
        if(tag == NULL) {
            return NULL;
        }
        element_set_tag_by_index(el, index, tag);
        free_crdt_tag(rtag);
        return get_add_value_sds_from_tag(tag);
    } else {
        *el = add_tag_by_element(*el, rtag);
        return get_add_value_sds_from_tag(rtag);
    }
}

int add_score_is_nan(crdt_element el, double score) {
    double d = 0;
    assert(get_double_add_counter_score_by_element(el, 0, &d));
    d += score;
    if(isnan(d)) {
        return 1;
    }
    return 0;
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
        rtag = a;
    } else {
        crdt_tag_base* b = create_base_tag(gid);
        b->base_data_type = VALUE_TYPE_DOUBLE;
        b->base_timespace = getMetaTimestamp(meta);
        b->base_vcu = get_vcu_by_meta(meta);
        b->score.f = score;
        rtag = b;
    }
    updateCrdtSSLastVc(ss, getMetaVectorClock(meta));
    if(sst) {
        dictEntry* tde = dictUnlink(sst->dict, field);
        if(tde) {
            crdt_element tel = get_element_by_dictEntry(tde);
            if(incr) {
                if(add_score_is_nan(tel, score)) {
                    *flags = ZADD_NAN; 
                    return NULL;
                }
                result = element_add_counter_by_tag(&tel, rtag);
            } else {
                tel = element_merge_tag(tel, rtag);
                result = get_base_value_sds_from_element(tel, gid);
            }
            set_element_by_dictEntry(tde, create_crdt_element());
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
            el = get_element_by_dictEntry(de);
            double old_score = 0;
            assert(get_double_score_by_element(el, &old_score));
            if(incr) {
                if(add_score_is_nan(el, score)) {
                    *flags = ZADD_NAN; 
                    return NULL;
                }
                result = element_add_counter_by_tag(&el, rtag);
                get_double_score_by_element(el, newscore);
            } else {
                el = element_clean(el);
                el = element_merge_tag(el, rtag);
                result = get_base_value_sds_from_element(el, gid);
                // result = sdscatprintf(result, ",%s", get_delete_counter_sds_from_element(el));
                *newscore = score;
            }
            set_element_by_dictEntry(de, el);
            if(zset_update_zsl(ss, old_score, field, el)) {
                *flags |= ZADD_UPDATED;
            }
            return result;
        } else if(!xx) {
            el = create_crdt_element();
            el = add_tag_by_element(el, rtag);
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
            *flags |= ZADD_NOP;
            return sdsempty();
        }
    }
    return NULL;
}




sds zsetRem(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field) {
    crdt_zset* ss = retrieve_crdt_zset(value);
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    dictEntry* de = dictUnlink(ss->dict, field);
    if(de == NULL) {
        return NULL;
    }
    assert(!dictFind(sst->dict, field));
    int gid = getMetaGid(meta);
    crdt_tag_base* del_tag = create_base_tag(gid);
    del_tag->base_vcu = get_vcu_by_meta(meta);
    del_tag->base_timespace = DELETED_TIME;

    VectorClock vc = getMetaVectorClock(meta);
    
    crdt_element el = get_element_by_dictEntry(de);
    double score = 0;
    assert(get_double_score_by_element(el, &score));
    
    int counter_num = 0;
    el = element_clean(el);
    el = element_merge_tag(el, del_tag);
    // free_crdt_element_array(el);
    set_element_by_dictEntry(de, create_crdt_element());
    dictFreeUnlinkedEntry(ss->dict,de);
    /* Delete from skiplist. */
    int retval = zslDelete(ss->zsl,score,field,NULL);
    assert(retval);
    union all_type field_value = {.s = field};
    sds field_info = value_to_sds(VALUE_TYPE_SDS, field_value);
    
    dictEntry* tde = dictAddRaw(sst->dict, sdsdup(field), NULL);
    set_element_by_dictEntry(tde, el);
    sds meta_info = get_delete_counter_sds_from_element(el);
    if(meta_info != NULL) {
        field_info = sdscatprintf(field_info, ",%s", meta_info);
        sdsfree(meta_info);
    } 
    
    return field_info;
}


int zsetTryDel(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta) {
    crdt_zset* ss = retrieve_crdt_zset(value);
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    VectorClock vc = getMetaVectorClock(meta);
    int gid = getMetaGid(meta);
    if(sst) {
        dictIterator* di = NULL;
        di = dictGetSafeIterator(sst->dict);
        dictEntry *de;
        while((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            crdt_element el = get_element_by_dictEntry(de);
            el = clean_element_by_vc(el, vc, NULL);
            set_element_by_dictEntry(de, el);
        }
        dictReleaseIterator(di);
    }
    if(ss) {
        dictIterator* di = NULL;
        di = dictGetSafeIterator(ss->dict);
        dictEntry *de;
        while((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            crdt_element el = get_element_by_dictEntry(de);
            double old_score = 0;
            assert(get_double_score_by_element(el, &old_score));
            int deleted = 1;
            crdt_element rel = clean_element_by_vc(el, vc, &deleted);
            if(deleted) {
                set_element_by_dictEntry(de, create_crdt_element());
                dictEntry* tde = dictAddRaw(sst->dict, sdsdup(field), NULL);
                set_element_by_dictEntry(tde, rel);
                zslDelete(ss->zsl, old_score, field, NULL);
                dictDelete(ss->dict, field);
            } else {
                set_element_by_dictEntry(de, rel);
                zset_update_zsl(ss, old_score, field, rel);
            }
        }
        dictReleaseIterator(di);
    }
    return 1;
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
        crdt_element el = get_element_by_dictEntry(de);
        assert(get_double_score_by_element(el, &score));
        rank = zslGetRank(zsl,score,ele);
        /* Existing elements always have a rank. */
        assert(rank != 0);
        if (reverse)
            return llen-rank;
        else
            return rank-1;
    } else {
        return -1;
    }
}