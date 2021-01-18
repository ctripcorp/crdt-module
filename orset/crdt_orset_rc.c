#include "crdt_orset_rc.h"


int rcStartGc() {
    rc_gc_stats = 1;
    return rc_gc_stats;
}
int rcStopGc() {
    rc_gc_stats = 0;
    return rc_gc_stats;
}
/************ utils *************/
crdt_orset_rc* retrieve_crdt_rc(CRDT_RC* rc) {
    return (crdt_orset_rc*)rc;
}

crdt_rc_tombstone* retrieve_crdt_rc_tombstone(CRDT_RCTombstone* rct) {
    return (crdt_rc_tombstone*)rct;
}


void reset_rc_type(CRDT_RC* rc) {
    initCrdtObject((CrdtObject*)rc);
    setDataType((CrdtObject*)rc, CRDT_RC_TYPE);
    setType((CrdtObject*)rc, CRDT_DATA);
}
#if defined(TCL_TEST)
    crdt_element get_element_from_rc(crdt_orset_rc* rc) {
        return (crdt_element)rc;
    }
    void set_rc_from_element(crdt_orset_rc* rc, crdt_element el) {
        rc->len = el->len;
        rc->tags = el->tags;
    }
    crdt_element get_element_from_rc_tombstone(crdt_rc_tombstone* rct) {
        return (crdt_element)rct;
    }

    void set_rc_tombstone_from_element(crdt_rc_tombstone* rct, crdt_element el) {
        rct->len = el->len;
        rct->tags = el->tags;
    }
#else
    crdt_element get_element_from_rc(crdt_orset_rc* rc) {
        return *(crdt_element*)rc;
    }
    void set_rc_from_element(crdt_orset_rc* rc, crdt_element el) {
      rc->len = el.len;
      rc->tags = el.tags;   
    }

    crdt_element get_element_from_rc_tombstone(crdt_rc_tombstone* rct) {
        return *(crdt_element*)rct;
    }

    void set_rc_tombstone_from_element(crdt_rc_tombstone* rct, crdt_element el) {
        rct->len = el.len;
        rct->tags = el.tags; 
    }
#endif

void freeCrdtRc(void* r) {
    crdt_element el = get_element_from_rc(r);
    free_internal_crdt_element(el);
    RedisModule_Free(r);
}

crdt_orset_rc* dup_crdt_rc(crdt_orset_rc* other) {
    crdt_orset_rc* dup = (crdt_orset_rc*)createCrdtRc();
    crdt_element el = dup_crdt_element(get_element_from_rc(other));
    set_rc_from_element(dup, el);
    free_external_crdt_element(el);
    return dup;
}

// int getCrdtRcType(CRDT_RC* rc) {
//      crdt_element el = *(crdt_element*)rc;
//      return get_element_type(el);
// }

//crdt_rc
crdt_orset_rc* load_rc_by_rdb(RedisModuleIO *rdb) {
    // crdt_element* rc = (crdt_element*)retrieve_crdt_rc(createCrdtRc());
    crdt_orset_rc* rc = (crdt_orset_rc*)createCrdtRc();
    crdt_element el = load_crdt_element_from_rdb(rdb);
    set_rc_from_element(rc, el);
    reset_rc_type((CRDT_RC*)rc);
    free_external_crdt_element(el);
    return rc;
}


//crdt_rc_tombstone
void reset_rc_tombstone_type(CRDT_RCTombstone* rct) {
    initCrdtObject((CrdtObject*)rct);
    setDataType((CrdtObject*)rct, CRDT_RC_TYPE);
    setType((CrdtObject*)rct, CRDT_TOMBSTONE);
}

CRDT_RC* createCrdtRc() {
    crdt_orset_rc* rc = RedisModule_Alloc(sizeof(crdt_orset_rc));
    crdt_element tel = get_element_from_rc(rc);
    reset_crdt_element(&tel);
    set_rc_from_element(rc, tel);
    reset_rc_type((CRDT_RC*)rc);
    return (CRDT_RC*)rc;
}

CRDT_RCTombstone* createCrdtRcTombstone() {
    crdt_rc_tombstone* rct = RedisModule_Alloc(sizeof(crdt_rc_tombstone));
    crdt_element tel = get_element_from_rc_tombstone(rct);
    reset_crdt_element(&tel);
    set_rc_tombstone_from_element(rct, tel);
    reset_rc_tombstone_type((CRDT_RCTombstone*)rct);
    return (CRDT_RCTombstone*)rct;
}

void freeCrdtRcTombstone(void* r) {
    crdt_element el = get_element_from_rc_tombstone(r);
    free_internal_crdt_element(el);
    RedisModule_Free(r);
}

crdt_rc_tombstone* dup_crdt_rc_tombstone(crdt_rc_tombstone* other) {
    crdt_rc_tombstone* dup = (crdt_rc_tombstone*)createCrdtRcTombstone();
    crdt_element oel = dup_crdt_element(get_element_from_rc_tombstone(other));
    set_rc_tombstone_from_element(dup, oel);
    free_external_crdt_element(oel);
    reset_rc_tombstone_type((CRDT_RCTombstone*) dup);
    return dup;
}

crdt_rc_tombstone* load_rc_tombstone_by_rdb(RedisModuleIO *rdb) {
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(createCrdtRcTombstone());
    // *rct = load_crdt_element_from_rdb(rdb);
    crdt_element tel = load_crdt_element_from_rdb(rdb);
    set_rc_tombstone_from_element(rct, tel);
    free_external_crdt_element(tel);
    reset_rc_tombstone_type((CRDT_RCTombstone*)rct);
    return (crdt_rc_tombstone*)rct;
}



sds initRcTombstoneFromRc(CRDT_RCTombstone *tombstone, CrdtMeta* meta, CRDT_RC* data) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(tombstone);
    crdt_element el = get_element_from_rc(rc);
    el = element_clean(el, getMetaGid(meta), get_vcu_by_meta(meta), 1);
    crdt_element tel = get_element_from_rc_tombstone(rct);
    el = move_crdt_element(&tel, el);
    set_rc_tombstone_from_element(rct, tel);
    // reset_crdt_element(&el);
    set_rc_from_element(rc, el);
    return get_delete_counter_sds_from_element(tel);
}

VectorClock getCrdtRcLastVc(void* data) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    crdt_element el = get_element_from_rc(rc);
    VectorClock vc = element_get_vc(el);
    return vc;
}
void freeRcLastVc(VectorClock vc) {
    freeVectorClock(vc);
} 


sds crdtRcInfo(void* value) {
    sds result = sdsnew("1) type: orset_rc\r\n");
    crdt_element el = get_element_from_rc(value);
    sds element_info = get_element_info(el);
    result = sdscatprintf(result," %s\n", element_info);
    sdsfree(element_info);
    return result;
}

CrdtObject** crdtRcFilter2(CrdtObject* target, int gid, VectorClock min_vc, long long maxsize, int* length) {
    crdt_orset_rc* rc = retrieve_crdt_rc(target);
    crdt_element el = get_element_from_rc(rc);
    VectorClock myself_vc = element_get_vc(el);
    if(!not_less_than_vc(min_vc, myself_vc)) {
        freeVectorClock(myself_vc);
        return NULL;
    }
    freeVectorClock(myself_vc);
    //value + gid + time + vectorClock
    if (get_crdt_element_memory(el) > maxsize) {
        *length  = -1;
        return NULL;
    }
    *length = 1;
    CrdtObject** re = RedisModule_Alloc(sizeof(crdt_orset_rc*));
    re[0] = target;
    return re;
}

CrdtObject** crdtRcFilter(CrdtObject* target, int gid, long long logic_time, long long maxsize, int* length) {
    crdt_orset_rc* rc = retrieve_crdt_rc(target);
    crdt_element el = get_element_from_rc(rc);
    if(element_get_vcu_by_gid(el, gid) < logic_time) {
        return NULL;
    }
    //value + gid + time + vectorClock
    if (get_crdt_element_memory(el) > maxsize) {
        *length  = -1;
        return NULL;
    }
    *length = 1;
    CrdtObject** re = RedisModule_Alloc(sizeof(crdt_orset_rc*));
    re[0] = target;
    return re;
}

void freeRcFilter(CrdtObject** filters, int num) {
    RedisModule_Free(filters);
}


//methods
sds rcIncrby(CRDT_RC* data, CrdtMeta* meta, int type, union all_type* value) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    int gid = getMetaGid(meta);
    int index = 0;
    crdt_element el = get_element_from_rc(rc);
    crdt_tag* tag = element_get_tag_by_gid(el, gid, &index);
    crdt_tag_add_counter* a = (crdt_tag_add_counter*)create_add_tag_from_all_type(gid, type, *value);
    a->add_vcu = get_vcu_by_meta(meta);
    if(tag) {  
        tag = tag_add_tag(tag, a);
        el = element_set_tag_by_index(el, index, tag);
        free_crdt_tag((crdt_tag*)a);
    } else {
        el = element_add_tag(el, (crdt_tag*)a);
        tag = (crdt_tag*)a;
    }
    // move_crdt_element((crdt_element*)rc, el);
    set_rc_from_element(rc, el);
    return get_add_value_sds_from_tag(tag);
}


void rc_tombstone_2_rc(CRDT_RC* data, CRDT_RCTombstone* tombstone) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(tombstone);
    crdt_element tel = get_element_from_rc_tombstone(rct);
    crdt_element el = get_element_from_rc(rc);
    tel = move_crdt_element(&el, tel);
    // reset_crdt_element(&tel);
    set_rc_from_element(rc, el);
    set_rc_tombstone_from_element(rct, tel);
    // *rc = *rct;
    reset_rc_type((CRDT_RC*)rc);
}

int rcTryIncrby(CRDT_RC* data, CRDT_RCTombstone* tombstone, CrdtMeta* meta, sds value) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    crdt_element el = get_element_from_rc(rc);
    int gid = getMetaGid(meta);
    ctrip_value v = {.type = VALUE_TYPE_NONE, .value.i = 0};
    int gcounter_len = str_2_value_and_g_counter_metas(value, &v, NULL);
    assert(gcounter_len == 0);
    crdt_tag_add_counter* a;
    if(v.type != VALUE_TYPE_LONGDOUBLE) {
        a = create_add_tag(gid);
        copy_tag_data_from_all_type(v.type, &a->add_counter,v.value) ;
        a->add_vcu = get_vcu_by_meta(meta);
        a->counter_type = v.type;
    } else {
        crdt_tag_ld_add_counter* lda = (crdt_tag_ld_add_counter*)create_ld_add_tag(gid);
        lda->add_counter = v.value.f;
        lda->add_vcu = get_vcu_by_meta(meta);
        a = (crdt_tag_add_counter*)lda;
    }
    free_ctrip_value(v);
    if(tombstone) {
        crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(tombstone);
        crdt_element tel = get_element_from_rc_tombstone(rct);
        int index;
        crdt_tag* tag = element_get_tag_by_gid(tel, gid, &index);
        if(tag) {
            tag = merge_crdt_tag(tag, (crdt_tag*)a);
            tel = element_set_tag_by_index(tel, index, tag); 
            free_crdt_tag((crdt_tag*)a);
            if(is_deleted_tag(tag)) {
                return PURGE_VAL;
            }  
        } else {
            tel = element_add_tag(tel, (crdt_tag*)a);
        }
        tel = move_crdt_element(&el, tel);
        // reset_crdt_element(&tel);
        set_rc_tombstone_from_element(rct, tel);
        set_rc_from_element(rc, el);
        return PURGE_TOMBSTONE;
    }
    if(data) {
        el = element_merge_tag(el, a);
    }
    set_rc_from_element(rc, el);
    return PURGE_TOMBSTONE;
}

int rcTryDel(CRDT_RC* current,CRDT_RCTombstone* tombstone, CrdtMeta* meta, sds info) {
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(tombstone);
    VectorClock vc = getMetaVectorClock(meta);
    g_counter_meta* gcounters[get_len(vc)];
    int gcounter_len = 0;
    if(info) {
        gcounter_len = str_to_g_counter_metas(info, sdslen(info), gcounters);
        assert(gcounter_len != -1);
    }
    crdt_element rel = create_element_from_vc_and_g_counter(vc, gcounter_len, gcounters, NULL);
    crdt_element tel = get_element_from_rc_tombstone(rct);
    if(current) {
        crdt_orset_rc* rc = retrieve_crdt_rc(current);
        crdt_element el = get_element_from_rc(rc);
        int result = purge_element(&rel, &el);
        if(result == PURGE_VAL) {
            rel = move_crdt_element(&tel, rel);
        }
        set_rc_tombstone_from_element(rct, tel);
        set_rc_from_element(rc, el);
        free_external_crdt_element(rel);
        return result;
    }
    tel = merge_crdt_element(tel, rel);
    set_rc_tombstone_from_element(rct, tel);
    free_external_crdt_element(rel);
    return PURGE_VAL;
}

void initCrdtRcFromTombstone(CRDT_RC* rc, CRDT_RCTombstone* t) {
    if(t == NULL) return;
    return rc_tombstone_2_rc(rc, t);
}




int rcAdd2(CRDT_RC* data, CrdtMeta* meta, sds val, char* buf) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    int gid = getMetaGid(meta);
    crdt_element el = get_element_from_rc(rc);
    el = element_clean(el, -1, 0, 0);
    crdt_tag_base b = {.base_data_type = VALUE_TYPE_SDS, .base_timespace = getMetaTimestamp(meta), .base_vcu = get_vcu_by_meta(meta), .score.s = val};
    init_crdt_base_tag_head(&b, gid);
    el = element_merge_tag2(el, &b);
    append_meta_vc_from_element(meta, el);
    set_rc_from_element(rc, el);
    return write_base_value_to_buf(el, gid, buf);
}

sds rcAdd(CRDT_RC* data,  CrdtMeta* meta, sds val) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    int gid = getMetaGid(meta);
    
    crdt_element el = get_element_from_rc(rc);
    el = element_clean(el, -1, 0, 0);
    crdt_tag_base b = {.base_data_type = VALUE_TYPE_SDS, .base_timespace = getMetaTimestamp(meta), .base_vcu = get_vcu_by_meta(meta), .score.s = val};
    init_crdt_base_tag_head(&b, gid);
    el = element_merge_tag2(el, &b);

    append_meta_vc_from_element(meta, el);

    sds result = get_base_value_sds_from_element(el, gid);
    set_rc_from_element(rc, el);
    return result;
}

int rcTryAdd(CRDT_RC* data, CRDT_RCTombstone* tombstone, CrdtMeta* meta, sds value) {
    crdt_orset_rc* rc = retrieve_crdt_rc(data);
    crdt_element el = get_element_from_rc(rc);
    VectorClock vc = getMetaVectorClock(meta);
    g_counter_meta* gcounters[get_len(vc)];
    ctrip_value v = {.type = VALUE_TYPE_NONE, .value.i = 0};
    int gcounter_len = str_2_value_and_g_counter_metas(value, &v, gcounters);
    assert(gcounter_len != -1);
    crdt_tag* b = (crdt_tag*)create_base_tag_by_meta(meta, v);
    free_ctrip_value(v);
    crdt_element rel =  create_element_from_vc_and_g_counter(vc, gcounter_len, gcounters, b);
    if(tombstone) {
        crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(tombstone);
        crdt_element tel = get_element_from_rc_tombstone(rct);
        int result = purge_element(&tel, &rel);
        if(result == PURGE_VAL) {
            set_rc_tombstone_from_element(rct, tel);
            free_external_crdt_element(rel);
            return PURGE_VAL;
        }
        set_rc_tombstone_from_element(rct, tel);
    }
    el = merge_crdt_element(el, rel);
    // move_crdt_element((crdt_element*)data, rel);
    set_rc_from_element(rc, el);
    free_external_crdt_element(rel);
    return PURGE_TOMBSTONE;
}

/*****  value  ***/
void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value) {
    crdt_orset_rc* rc = retrieve_crdt_rc(value);
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    crdt_element el = get_element_from_rc(rc);
    save_crdt_element_to_rdb(rdb, el);
}
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        if(version >= 1) {
            return load_rc_by_rdb(rdb);
        }
    }
    return NULL;
}


void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcMemUsageFunc(const void *value) {
    return 1;
}



void crdtRcDigestFunc(RedisModuleDigest *md, void *value) {

}

CrdtObject* crdtRcMerge(CrdtObject* currentVal, CrdtObject* value) {
    
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    if(currentVal == NULL) {
        return (CrdtObject* )dup_crdt_rc((crdt_orset_rc*)value);
    }
    if(value == NULL) {
        return (CrdtObject* )dup_crdt_rc((crdt_orset_rc*)currentVal);
    }
    crdt_element other = get_element_from_rc((crdt_orset_rc*)value);
    crdt_orset_rc* result = dup_crdt_rc((crdt_orset_rc*)currentVal);
    crdt_element el = get_element_from_rc(result);
    el =  merge_crdt_element(el, other);
    // move_crdt_element(result, rel);
    // move_crdt_element(other, create_crdt_element());
    reset_crdt_element(&other);
    set_rc_from_element((crdt_orset_rc*)value, other);
    set_rc_from_element(result, el);
    return (CrdtObject* )result;
}

void crdtRcUpdateLastVC(void* rc, VectorClock vc) {
   
}


/***  abouot ****/


//about sorted set tombstone module type
void RdbSaveCrdtRcTombstone(RedisModuleIO *rdb, void *value) {
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(value);
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    crdt_element tel = get_element_from_rc_tombstone(rct);
    save_crdt_element_to_rdb(rdb, tel);
}
void *RdbLoadCrdtRcTombstone(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        if(version >= 1) {
            return load_rc_tombstone_by_rdb(rdb);
        }
    }
    return NULL;
}

void AofRewriteCrdtRcTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcTombstoneMemUsageFunc(const void *value) {
    return 1;
}

void crdtRcTombstoneDigestFunc(RedisModuleDigest *md, void *value) {

}

CrdtTombstone** crdtRcTombstoneFilter2(CrdtTombstone* target, int gid, VectorClock min_vc, long long maxsize,int* length) {
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(target);
    crdt_element tel = get_element_from_rc_tombstone(rct);
    // long long vcu = element_get_vcu_by_gid(tel, gid);
    // if(vcu < logic_time) {
    //     return NULL;
    // }
    VectorClock myself_vc = element_get_vc(tel);
    if(!not_less_than_vc(min_vc, myself_vc)) {
        freeVectorClock(myself_vc);
        return NULL;
    }
    freeVectorClock(myself_vc);
    //value + gid + time + vectorClock
    if (get_crdt_element_memory(tel) > maxsize) {
        *length  = -1;
        return NULL;
    }

    *length = 1;
    CrdtTombstone** re = RedisModule_Alloc(sizeof(crdt_rc_tombstone*));
    re[0] = (CrdtTombstone*)rct;
    return re;
}

CrdtTombstone** crdtRcTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) {
    crdt_rc_tombstone* rct = retrieve_crdt_rc_tombstone(target);
    crdt_element tel = get_element_from_rc_tombstone(rct);
    long long vcu = element_get_vcu_by_gid(tel, gid);
    if(vcu < logic_time) {
        return NULL;
    }
    //value + gid + time + vectorClock
    if (get_crdt_element_memory(tel) > maxsize) {
        *length  = -1;
        return NULL;
    }

    *length = 1;
    CrdtTombstone** re = RedisModule_Alloc(sizeof(crdt_rc_tombstone*));
    re[0] = (CrdtTombstone*)rct;
    return re;
}


void freeCrdtRcTombstoneFilter(CrdtTombstone** filters, int num) {
    RedisModule_Free(filters);
}

CrdtTombstone* crdtRcTombstoneMerge(CrdtTombstone* currentVal, CrdtTombstone* value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    if(currentVal == NULL) {
        return (CrdtTombstone*)dup_crdt_rc_tombstone((crdt_rc_tombstone*)value);
    }
    if(value == NULL) {
        return (CrdtTombstone*)dup_crdt_rc_tombstone((crdt_rc_tombstone*)currentVal);
    }
    crdt_element other = get_element_from_rc_tombstone((crdt_rc_tombstone*)value);
    crdt_rc_tombstone* result = (dup_crdt_rc_tombstone((crdt_rc_tombstone*)currentVal));
    crdt_element tel = get_element_from_rc_tombstone(result);
    tel = merge_crdt_element(tel, other);
    set_rc_tombstone_from_element(result, tel);
    reset_crdt_element(&other);
    set_rc_tombstone_from_element((crdt_rc_tombstone*)value, other);
    set_rc_tombstone_from_element(result, tel);
    return (CrdtTombstone* )result;
}



int crdtRcTombstonePurge(CRDT_RCTombstone* tombstone, CRDT_RC* r) {
    crdt_element el = get_element_from_rc((crdt_orset_rc*)r);
    crdt_element tel = get_element_from_rc_tombstone((crdt_rc_tombstone*)tombstone);
    int result = purge_element(&tel, &el);
    set_rc_from_element((crdt_orset_rc*)r, el);
    set_rc_tombstone_from_element((crdt_rc_tombstone*)tombstone, tel);
    return result;
}

int crdtRcTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    if(!rc_gc_stats) {
        return 0;
    }
    crdt_element tel = get_element_from_rc_tombstone((crdt_rc_tombstone*)target);
    VectorClock vc = element_get_vc(tel);
    int result = isVectorClockMonoIncr(vc,clock);
    freeVectorClock(vc);
    if(result) {
        #if defined(DEBUG) 
            sds info = crdtRcTombstoneInfo(target);
            RedisModule_Debug("notice", "[gc] rc_tombstone:%s", info);
            sdsfree(info);
        #endif
    }
    return result;
}

sds crdtRcTombstoneInfo(void* value) {
    sds result = sdsnew("1) type: orset_rc_tombstone\r\n");
    crdt_element el = get_element_from_rc(value);
    sds element_info = get_element_info(el);
    result = sdscatprintf(result," %s\n", element_info);
    sdsfree(element_info);
    return result;
}
VectorClock getCrdtRcTombstoneLastVc(CrdtTombstone* tombstone) {
    if(tombstone == NULL) return newVectorClock(0);
    crdt_element el = get_element_from_rc_tombstone((crdt_rc_tombstone*)tombstone);
    return element_get_vc(el);
}

/////////////////// value function 



int get_crdt_rc_value(CRDT_RC* rc, ctrip_value* value) {
    crdt_element el = get_element_from_rc((crdt_orset_rc*)rc);
    return element_get_value(el, value);
}


int get_tag_add_value(crdt_element el, int gid, ctrip_value* value) {
    crdt_tag* tag = element_get_tag_by_gid(el, gid, NULL);
    if(tag == NULL) return 0;
    return get_tag_counter_value(tag, value, 0);
}

int get_rc_tag_add_value(CRDT_RC* rc, int gid, ctrip_value* value) {
    crdt_element el = get_element_from_rc(retrieve_crdt_rc(rc));
    return get_tag_add_value(el, gid, value);
}

int get_rc_tombstone_tag_add_value(CRDT_RCTombstone* rct, int gid, ctrip_value* value) {
    crdt_element el = get_element_from_rc_tombstone(retrieve_crdt_rc_tombstone(rct));
    return get_tag_add_value(el, gid, value);
}


