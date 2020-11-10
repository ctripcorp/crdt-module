#include "crdt_orset_zset.h"
#include "../gcounter/g_counter.h"
#define BAD 7
#define BA 3
#define AD 6
//  generic function
crdt_zset_tombstone* retrieve_crdt_zset_tombstone(CRDT_SSTombstone* rt) {
    return (crdt_zset_tombstone*)rt;
}

crdt_zset* retrieve_crdt_zset(CRDT_SS* rc) {
    return (crdt_zset*)rc;
}



void dictCrdtSSDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    freeCrdtSS(val);
}


static dictType crdtSetDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtSSDestructor   /* val destructor */
};


CRDT_SS* create_crdt_zset() {
    struct crdt_zset* s = RedisModule_Alloc(sizeof(crdt_zset));
    s->type = 0;
    setDataType((CrdtObject*)s, CRDT_ZSET_TYPE);
    setType((CrdtObject*)s, CRDT_DATA);
    s->dict = dictCreate(&crdtSetDictType, NULL);
    s->zsl = zslCreate();
    s->lastvc = newVectorClock(0);
    return (CRDT_SS*)s;
}

CRDT_SSTombstone* create_crdt_ss_tombstone() {
    struct crdt_zset_tombstone* st = RedisModule_Alloc(sizeof(crdt_zset_tombstone*));
    st->type = 0;
    setDataType((CrdtObject*)st, CRDT_ZSET_TYPE);
    setType((CrdtObject*)st, CRDT_TOMBSTONE);
    st->dict = dictCreate(&crdtSetDictType, NULL);
    st->lastvc = newVectorClock(0);
    return (CRDT_SSTombstone*)st;
}

void updateZsetLastVectorClock(CRDT_SS* data, VectorClock vc) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    VectorClock old_vc = zset->lastvc;
    zset->lastvc = vectorClockMerge(zset->lastvc, vc);
    if(!isNullVectorClock(old_vc)) {
        freeVectorClock(old_vc);
    }
}

VectorClock getCrdtSSLastVc(CRDT_SS* data) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    return zset->lastvc;
}


// redismodule
// ===== sorted set ========
void *RdbLoadCrdtSS(RedisModuleIO *rdb, int encver) {
    return NULL;
}

void RdbSaveCrdtSS(RedisModuleIO *rdb, void *value) {
    
} 

void AofRewriteCrdtSS(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

size_t crdtSSMemUsageFunc(const void *value) {
    return 1;
}

void freeCrdtSS(void* ss) {

}

void crdtSSDigestFunc(RedisModuleDigest *md, void *value) {

}
// ====== sorted set tombstone ========
void *RdbLoadCrdtSST(RedisModuleIO *rdb, int encver) {
    return NULL;
}

void RdbSaveCrdtSST(RedisModuleIO *rdb, void *value) {
    
} 

void AofRewriteCrdtSST(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    
}

size_t crdtSSTMemUsageFunc(const void *value) {
    return 1;
}

void freeCrdtSST(void* ss) {

}

void crdtSSTDigestFunc(RedisModuleDigest *md, void *value) {

}

size_t get_el_len(crdt_zset_element el) {
    return el.len;
}

crdt_zset_tag* element_get_tag_by_index(crdt_zset_element el, int index) {
    assert(el.len > index && index >= 0);
    if(el.len == 0) {
        return NULL;
    }
    if(el.len == 1) {
        long long ll = el.tags;
        return *(crdt_zset_tag**)&ll;
    }
    crdt_zset_tag** tags = (crdt_zset_tag**)(el.tags);
    return tags[index];
}

void element_set_tag_by_index(crdt_zset_element* el, int index, crdt_zset_tag* tag) {
    assert(el->len > index && index >= 0);
    if(el->len == 1) {
        el->tags = *(long long*)tag;
        return;
    }
    crdt_zset_tag** tags = (crdt_zset_tag**)(el->tags);
    tags[index] = tag;
} 

crdt_zset_tag* element_get_tag_by_gid(crdt_zset_element el, int gid) {
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            return tag;
        }
    }
    return NULL;
}

double get_base_score(crdt_zset_tag* tag, long long* time) {
    crdt_zset_tag_base* b;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_base_and_add_del_counter *bad;
    switch(tag->type) {
        case TAG_BASE:
            b = (crdt_zset_tag_base*)tag;
            *time = b->base_timespace;
            return b->score;
        break;
        case BA:
            ba = (crdt_zset_tag_base_and_add_counter*)tag;
            *time = ba->base_timespace;
            return ba->score;
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
            *time = bad->base_timespace;
            return bad->score;
        break;
        default:
            return 0;
        break;
    }
}

double get_counter_score(crdt_zset_tag* tag) {
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_base_and_add_del_counter* bad;
    crdt_zset_tag_add_del_counter* ad;
    crdt_zset_tag_add_counter* a;
    switch (tag->type)
    {
    case BA:
        /* code */
        ba = (crdt_zset_tag_base_and_add_counter*)tag;
        return ba->add_counter;
        break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        return bad->add_counter - bad->del_counter;
        break;
    case TAG_ADD_COUNTER:
        a = (crdt_zset_tag_add_counter*)tag;
        return a->add_counter;
        break;
    case AD:
        ad = (crdt_zset_tag_add_del_counter*)tag;
        return ad->add_counter - ad->del_counter;
        break;
    case TAG_BASE:
        return 0;
        break;
    default:
        assert(1 == 0);
        return 0;
        break;
    }
}

double get_score_by_element(crdt_zset_element el) {
    double base = 0;
    double counter = 0;
    long long time = 0;
    for(int i = 0; i < el.len; i++) {
        long long cureen_time = 0;
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        double score = get_base_score(tag, &cureen_time);
        if(cureen_time > time) {
            base = score;
            time = cureen_time;
        }
        counter += get_counter_score(tag);
    }
    return base + counter;
}

#define EMPTY_ELEMENT {.len=0,.tags=0} 

//transon
 crdt_zset_tag_base_and_add_counter* B2BA(crdt_zset_tag_base* b) {
    assert(b->type == TAG_BASE);
    crdt_zset_tag_base_and_add_counter* ba = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_counter));
    ba->type = BA;
    ba->gid = (unsigned long long)(b->gid);
    ba->base_timespace = (long long)(b->base_timespace);
    ba->score = b->score;
    ba->base_vcu = b->base_vcu;
    RedisModule_Free(b);
    return ba;
 }




crdt_zset_element add_tag_by_element(crdt_zset_element el, crdt_zset_tag* tag) {
    if(el.len == 0) {
        crdt_zset_element e = {.len = 1, .tags = *(long long*)&tag};
        return e;
    } else if(el.len == 1) {
        crdt_zset_tag** tags = RedisModule_Alloc(sizeof(crdt_zset_tag*) * 2);
        long long a = el.tags;
        tags[0] = *(crdt_zset_tag**)&a;
        tags[1] = tag;
        el.len = 2;
        el.tags = *(long long*)&tags;
        return el;
    } else {
        long long a = el.tags;
        crdt_zset_tag* t = *(crdt_zset_tag**)(&a);
        crdt_zset_tag** tags = RedisModule_Realloc(t, sizeof(crdt_zset_tag*) * (el.len + 1));
        tags[el.len] = *tags;
        el.len = el.len + 1;
        el.tags = *(long long*)&tags;
        return el;
    }
    
}

crdt_zset_tag *create_base_tag(CrdtMeta* meta, double score) {
    crdt_zset_tag_base* base = RedisModule_Alloc(sizeof(crdt_zset_tag_base));
    base->base_timespace = getMetaTimestamp(meta);
    base->base_vcu = get_vcu_by_meta(meta);
    base->score = score;
    base->type = TAG_BASE;
    base->gid = getMetaGid(meta);
    return (crdt_zset_tag*)base;
}

crdt_zset_tag_base_and_add_del_counter* A2BAD(crdt_zset_tag_add_counter* a) {
    crdt_zset_tag_base_and_add_del_counter* bad = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));
    bad->type = BAD;
    bad->add_counter = a->add_counter;
    bad->add_vcu = a->add_vcu;
    bad->gid = a->gid;
    
    bad->del_counter = 0;
    bad->del_vcu = 0;

    bad->base_timespace = 0;
    bad->base_vcu = 0;
    bad->score = 0;
    
    RedisModule_Free(a);
    return bad;
}

crdt_zset_tag_base_and_add_del_counter* AD2BAD(crdt_zset_tag_add_del_counter* ad) {
    crdt_zset_tag_base_and_add_del_counter* bad = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));
    bad->add_counter = ad->add_counter;
    bad->add_vcu = ad->add_vcu;
    bad->del_counter = ad->del_counter;
    bad->del_vcu = ad->del_vcu;
    bad->gid = ad->gid;
    bad->type = BAD;

    bad->base_timespace = 0;
    bad->score = 0;
    bad->base_vcu = 0;

    RedisModule_Free(ad);
    return bad;
}

crdt_zset_tag_base_and_add_del_counter* BA2BAD(crdt_zset_tag_base_and_add_counter* ba) {
    crdt_zset_tag_base_and_add_del_counter* bad = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));

    bad->base_timespace = ba->base_timespace;
    bad->base_vcu = ba->base_vcu;
    bad->score = ba->score;
    bad->gid = ba->gid;
    bad->add_counter = ba->add_counter;
    bad->add_vcu = ba->add_vcu;
    bad->type = BAD;

    bad->del_counter = 0;
    bad->del_vcu = 0;
    return bad;
}

crdt_zset_tag* reset_base_tag(crdt_zset_tag* tag, CrdtMeta* meta, double score) {
    crdt_zset_tag_base* base;
    crdt_zset_tag_base_and_add_del_counter* bad;
    switch (tag->type)
    {
    case TAG_BASE:
        /* code */
        base = (crdt_zset_tag_base*)tag;
        base->base_timespace = getMetaTimestamp(meta);
        base->base_vcu = get_vcu_by_meta(meta);
        base-> score = score;
        return (crdt_zset_tag*)base;
        break;
    case BA:
        bad = BA2BAD((crdt_zset_tag_base_and_add_counter*)tag);
        goto callback_bad;
    break;
    case AD:
        bad = AD2BAD((crdt_zset_tag_add_del_counter*)tag);
        goto callback_bad;
    break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        goto callback_bad;
    break;
    case TAG_ADD_COUNTER:
        bad = A2BAD((crdt_zset_tag_add_counter*)tag);
        goto callback_bad;
    break;
    default:
        assert(1 == 0);
        break;
    }
callback_bad:
    bad->del_counter = bad->add_counter;
    bad->del_vcu = bad->add_vcu;
    bad->base_timespace = getMetaTimestamp(meta);
    bad->base_vcu = get_vcu_by_meta(meta);
    bad->score = score;
    return (crdt_zset_tag*)bad;
}

crdt_zset_tag* clean_tag(crdt_zset_tag* tag) {
    if(tag == NULL) { return tag; }
    crdt_zset_tag_base_and_add_del_counter* bad;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_add_del_counter* ad;
    crdt_zset_tag_add_counter* a;
    switch (tag->type)
    {
    case TAG_BASE:
        // crdt_zset_tag_base* base = (crdt_zset_tag_base*)tag;
        RedisModule_Free(tag);
        return NULL;
        break;
    case TAG_ADD_COUNTER:
        a = (crdt_zset_tag_add_counter*)tag;
        ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
        ad->add_counter = a->add_counter;
        ad->add_vcu = a->add_vcu;
        ad->gid = a->gid;
        ad->del_counter = a->add_counter;
        ad->del_vcu = a->add_vcu;
        RedisModule_Free(a);
        return (crdt_zset_tag*)ad;
        break;
    case AD:
        ad = (crdt_zset_tag_add_del_counter*)tag;
        ad->del_counter = ad->add_counter;
        ad->del_vcu = ad->add_vcu;
        return (crdt_zset_tag*)ad;
        break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        crdt_zset_tag_add_del_counter* ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
        ad->type = AD;
        ad->gid = bad->gid;
        ad->add_counter = bad->add_counter;
        ad->add_vcu = bad->add_vcu;
        if(bad->del_vcu > bad->add_vcu) {
            ad->del_counter = bad->del_counter;
            ad->del_vcu = bad->del_vcu;
        } else {
            ad->del_counter = bad->add_counter;
            ad->del_vcu = bad->add_vcu;
        }
        RedisModule_Free(bad);
        return (crdt_zset_tag*)ad;
        break;
    case BA:
        ba = (crdt_zset_tag_base_and_add_counter*)tag;
        ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
        ad->type = AD;
        ad->gid = ba->gid;
        ad->add_counter = ba->add_counter;
        ad->add_vcu = ba->add_vcu;
        ad->del_counter = ba->add_counter;
        ad->del_vcu = ba->add_vcu;
        RedisModule_Free(ba);
        return (crdt_zset_tag*)ad;
        break;
    default:
        assert(1 == 0);
        break;
    }

}

crdt_zset_element update_base(crdt_zset_element el, CrdtMeta* meta, double score) {
    int gid = getMetaGid(meta);
    int added = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            tag = reset_base_tag(tag, meta, score);
            added = 1;
        } else {
            tag = clean_tag(tag);
        }
        element_set_tag_by_index(&el, i, tag);
    }
    if(added == 0) {
        crdt_zset_tag* tag = create_base_tag(meta, score);
        el = add_tag_by_element(el, tag);
    }
    return el;
}





int zsetAdd(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double score) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    dictEntry *de;
    zskiplistNode *znode;
    zskiplist* zsl = zset->zsl;
    updateZsetLastVectorClock(ss, getMetaVectorClock(meta));
    de = dictFind(zset->dict, field);
    if(de != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        double curscore = get_score_by_element(el);
        el = update_base(el, meta, score);
        dictSetSignedIntegerVal(de, *(long long*)&el);
        double newscore = get_score_by_element(el);
        if(curscore != newscore) {
            zskiplistNode *node;
            zslDelete(zsl,curscore,field,&node);
            znode = zslInsert(zsl,newscore,node->ele);
            node->ele = NULL;
            zslFreeNode(node);
        }
        return 0;
    } else {
        crdt_zset_tag* tag =  create_base_tag(meta, score);
        crdt_zset_element  e = {.len = 0};
        e = add_tag_by_element(e, tag);
        znode = zslInsert(zsl, score, sdsdup(field));
        // dictAdd(zset->dict, field, ele);
        de = dictAddOrFind(zset->dict, sdsdup(field));
        dictSetSignedIntegerVal(de, *(long long*)&e);
        return 1;
    }
    
}

crdt_zset_tag* create_add_counter_tag(CrdtMeta* meta, double sorted) {
    crdt_zset_tag_add_counter* add = RedisModule_Alloc(sizeof(crdt_zset_tag_add_counter));
    add->add_counter = sorted;
    add->add_vcu = get_vcu_by_meta(meta);
    add->type = TAG_ADD_COUNTER;
    add->gid = getMetaGid(meta);
    return (crdt_zset_tag*)add;
}

double getScore(CRDT_SS* ss, sds field) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    dictEntry* de =  dictFind(zset->dict,field);
    if(de == NULL) {
        return 0;
    } 
    crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
    return get_score_by_element(el);
}

size_t getZSetSize(CRDT_SS* ss) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    return dictSize(zset->dict);
}




crdt_zset_tag* add_counter(crdt_zset_tag* tag, CrdtMeta* meta, double score) {
    if(tag == NULL) { return tag; }
    crdt_zset_tag_base_and_add_del_counter* bad = NULL;
    crdt_zset_tag_base_and_add_counter* ba = NULL;
    crdt_zset_tag_add_del_counter* ad = NULL;
    crdt_zset_tag_add_counter* a = NULL;
    switch (tag->type) {
        case TAG_BASE:
            ba = B2BA((crdt_zset_tag_base*)tag);
            ba->add_counter = score;
            ba->add_vcu = get_vcu_by_meta(meta);
            return (crdt_zset_tag*)ba;
        break;
        case BA:
            ba = (crdt_zset_tag_base_and_add_counter*)(tag);
            ba->add_counter += score;
            ba->add_vcu = get_vcu_by_meta(meta);
            return (crdt_zset_tag*)ba;
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*)(tag);
            bad->add_counter += score;
            bad->add_vcu = get_vcu_by_meta(meta);
            return (crdt_zset_tag*)ba;
        break; 
        case AD:
            ad = (crdt_zset_tag_add_del_counter*)(tag);
            ad->add_counter += score;
            ad->add_vcu = get_vcu_by_meta(meta);
            return (crdt_zset_tag*)ad;
        break;
        case TAG_ADD_COUNTER:
            a = (crdt_zset_tag_add_counter*)(tag);
            a->add_counter += score;
            a->add_vcu = get_vcu_by_meta(meta);
            return (crdt_zset_tag*)a;
        break;
        default:
            assert(1 == 0);
        break;
    };
    return NULL;
}




crdt_zset_element zset_update_add_counter(crdt_zset_element el, CrdtMeta* meta, double score){
    int gid = getMetaGid(meta);
    int updated = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            tag = add_counter(tag, meta, score);
            updated = 1;
            element_set_tag_by_index(&el, i, tag);
        } 
        
    }
    if(updated == 0) {
        crdt_zset_tag* tag = create_add_counter_tag(meta, score);
        el = add_tag_by_element(el, tag);
    }
    return el;
}

double zsetIncr(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double score) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    dictEntry* de =  dictFind(zset->dict, field);
    zskiplist* zsl = zset->zsl;
    zskiplistNode *znode;
    updateZsetLastVectorClock(ss, getMetaVectorClock(meta));
    if(de != NULL) {
        long long v = dictGetSignedIntegerVal(de);
        crdt_zset_element el = *(crdt_zset_element*)&v;
        double curscore = get_score_by_element(el);
        el = zset_update_add_counter(el, meta, score);
        dictSetSignedIntegerVal(de, *(long long*)&el);
        double newscore = get_score_by_element(el);
        if(curscore != newscore) {
            zskiplistNode *node;
            zslDelete(zsl,curscore,field,&node);
            znode = zslInsert(zsl,newscore,node->ele);
            node->ele = NULL;
            zslFreeNode(node);
        }
        return newscore;
    } else {
        crdt_zset_tag* tag =  create_add_counter_tag(meta, score);
        crdt_zset_element  e = {.len = 0};
        e = add_tag_by_element(e, tag);
        znode = zslInsert(zsl, score, sdsdup(field));
        de = dictAddOrFind(zset->dict, sdsdup(field));
        dictSetSignedIntegerVal(de, *(long long*)&e);
        
        return score;
    }
}   

sds getTagInfo(crdt_zset_tag* tag) {
    sds result = sdsempty();
    crdt_zset_tag_base* b;
    crdt_zset_tag_add_counter* a;
    crdt_zset_tag_add_del_counter* ad;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_base_and_add_del_counter* bad;
    switch(tag->type) {
        case TAG_BASE:
            b = (crdt_zset_tag_base*)(tag);
            result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f",b->gid, b->base_vcu, b->base_timespace, b->score);
        break;
        case TAG_ADD_COUNTER:
            a = (crdt_zset_tag_add_counter*)tag;
            result = sdscatprintf(result, "gid: %d, add_vcu: %lld, add: %.13f", a->gid, a->add_vcu, a->add_counter);
        break;
        case BA:
            ba = (crdt_zset_tag_base_and_add_counter*)tag;
            result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f, add_vcu: %lld, add: %.13f", ba->gid, ba->base_vcu, ba->base_timespace, ba->score, ba->add_vcu, ba->add_counter);
        break;
        case AD:
            ad = (crdt_zset_tag_add_del_counter*)tag;
            result = sdscatprintf(result, "gid: %d, add_vcu: %lld, add: %.13f, del_vcu: %lld, del: %.13f", ad->gid, ad->add_vcu, ad->add_counter, ad->del_vcu, ad->del_counter);
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
            result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f, add_vcu: %lld, add: %.13f, del_vcu: %lld, del: %.13f", bad->gid, bad->base_vcu, bad->base_timespace, bad->score, bad->add_vcu, bad->add_counter, bad->del_vcu, bad->del_counter);
        break;
        default:
            assert(1 == 0);
        break;
    }
    return result;
}

sds getElementInfo(crdt_zset_element el) {
    sds result = sdsempty();
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        sds tag_info = getTagInfo(tag);
        result = sdscatprintf(result,"   %s\n", tag_info);
        sdsfree(tag_info);
    }
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
        crdt_zset_element element = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        sds element_info = getElementInfo(element);
        result = sdscatprintf(result, "2)  key: %s \n", dictGetKey(de));
        result = sdscatprintf(result, "%s", element_info);
        sdsfree(element_info);
    } 
    dictReleaseIterator(it);
    return result;
}


/* Finds an element by its rank. The rank argument needs to be 1-based. */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
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

int zsetTag2DelGcounter(void* data, int index, g_counter_meta* value) {
    crdt_zset_element el = *(crdt_zset_element*)data;
    crdt_zset_tag* tag = element_get_tag_by_index(el, index);
    value->data_type = VALUE_TYPE_DOUBLE;
    // value->gid = a.gid;
    // value->vcu = a.vcu;
    crdt_zset_tag_add_del_counter* ad;
    crdt_zset_tag_base_and_add_del_counter* bad;
    switch (tag->type) {
        case AD:
            ad = (crdt_zset_tag_add_del_counter*)tag;
            value->gid = ad->gid;
            value->vcu = ad->del_vcu;
            value->conv.d = ad->del_counter;
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*) tag;
            value->gid = bad->gid;
            value->vcu = bad->del_vcu;
            value->conv.d = bad->del_counter;
        break;
        default:
        return 0;
        break;
    }
    return 1;
}

int initSSTombstoneFromSS(CRDT_SSTombstone* tombstone,CrdtMeta* del_meta, CRDT_SS* value, sds* del_counters) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    sst->lastvc = vectorClockMerge(getMetaVectorClock(del_meta),ss->lastvc);
    sst->maxdelvc = dupVectorClock(sst->lastvc);
    dictIterator* next = dictGetIterator(ss->dict);
    dictEntry* de = NULL;
    int i = 0;
    dictEntry* del_de = NULL;
    while((de = dictNext(next)) != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        crdt_zset_element del_el = {.len = 0, .tags = 0};
        for(int i = 0; i < el.len; i++) {
            crdt_zset_tag* tag = element_get_tag_by_index(el, i);
            crdt_zset_tag* del_tag = clean_tag(tag);
            if(del_tag != NULL) {
                del_el = add_tag_by_element(del_el, del_tag);
            } 
        }
        if(del_el.len != 0) {
            del_counters[i++] = sdsdup(dictGetKey(de));
            del_counters[i++] = g_counter_metas_to_sds(&del_el, zsetTag2DelGcounter, del_el.len);
            del_de = dictAddOrFind(sst->dict, sdsdup(dictGetKey(de)));
            dictSetSignedIntegerVal(del_de, *(long long*)&del_el);
        }
    }
    return 1;
}