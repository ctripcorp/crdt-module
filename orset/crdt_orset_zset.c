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

CRDT_SSTombstone* create_crdt_zset_tombstone() {
    crdt_zset_tombstone* st = RedisModule_Alloc(sizeof(crdt_zset_tombstone));
    st->type = 0;
    setDataType((CrdtObject*)st, CRDT_ZSET_TYPE);
    setType((CrdtObject*)st, CRDT_TOMBSTONE);
    st->dict = dictCreate(&crdtSetDictType, NULL);
    st->lastvc = newVectorClock(0);
    st->maxdelvc = newVectorClock(0);
    return (CRDT_SSTombstone*)st;
}

int zsetLength(CRDT_SS* ss) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    return dictSize(zset->dict);
}

void updateCrdtSSLastVc(CRDT_SS* data, VectorClock vc) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    VectorClock old_vc = zset->lastvc;
    zset->lastvc = vectorClockMerge(zset->lastvc, vc);
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

VectorClock getCrdtSSLastVc(CRDT_SS* data) {
    crdt_zset* zset = retrieve_crdt_zset(data);
    return zset->lastvc;
}

VectorClock getCrdtSSTLastVc(CRDT_SSTombstone* data) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(data);
    return zset_tombstone->lastvc;
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

int is_deleted_tag(crdt_zset_tag* tag) {
    crdt_zset_tag_add_del_counter* ad;
    crdt_zset_tag_base_and_add_del_counter* bad;
    crdt_zset_tag_base* b;
    switch (tag->type)
    {
    case AD:
        ad = (crdt_zset_tag_add_del_counter*)tag;
        if(ad->add_vcu == ad->del_vcu) {
            return 1;
        }
        break;
    case TAG_BASE:
        b = (crdt_zset_tag_base*)tag;
        if(b->base_timespace == DEL_TIME) {
            assert(b->score == 0);
            return 1;
        }
        break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        if(bad->add_vcu == bad->del_vcu && bad->base_timespace == DEL_TIME) {
            assert(bad->score == 0);
            return 1;
        }
        break;    
    case BA:
    case TAG_ADD_COUNTER:
        return 0;
    default:
        printf("[is_deleted_tag]tag type is error\n");
        assert(1==0);
        break;
    }
    return 0;
}

crdt_zset_tag* element_get_tag_by_index(crdt_zset_element el, int index) {
    assert(el.len > index && index >= 0);
    if(el.len == 0) {
        return NULL;
    }
    if(el.len == 1) {
        long long ll = el.tags;
        return ll;
    }
    crdt_zset_tag** tags = (crdt_zset_tag**)(el.tags);
    return tags[index];
}

void element_set_tag_by_index(crdt_zset_element* el, int index, crdt_zset_tag* tag) {
    assert(el->len > index && index >= 0);
    assert(tag != NULL);
    if(el->len == 1) {
        el->tags = tag;
        return;
    }
    crdt_zset_tag** tags = (crdt_zset_tag**)(el->tags);
    tags[index] = tag;
} 

crdt_zset_tag* element_get_tag_by_gid(crdt_zset_element el, int gid, int* index) {
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            if(index != NULL) *index = i;
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
    if(isnan(base) || isnan(counter)) {
        printf("score %.17f %.17f\n", base , counter);
        assert(1 == 0);
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
    ba->add_counter = 0;
    ba->add_vcu = 0;
    RedisModule_Free(b);
    return ba;
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
    assert(bad->type == BAD);
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
    RedisModule_Free(ba);
    return bad;
}

crdt_zset_tag_add_del_counter* A2AD(crdt_zset_tag_add_counter* a) {
    crdt_zset_tag_add_del_counter* ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
    ad->add_counter = a->add_counter;
    ad->add_vcu = a->add_vcu;
    ad->del_counter = 0;
    ad->del_vcu = 0;
    ad->gid = a->gid;
    ad->type = AD;
    RedisModule_Free(a);
    return ad;
}
crdt_zset_tag_add_del_counter*  B2AD(crdt_zset_tag_base* b) {
    crdt_zset_tag_add_del_counter* ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
    ad->add_counter = 0;
    ad->add_vcu = 0;
    ad->del_counter = 0;
    ad->del_vcu = 0;
    ad->gid = b->gid;
    ad->type = AD;
    RedisModule_Free(b);
    return ad;
}
crdt_zset_tag_add_counter* BA2A(crdt_zset_tag_base_and_add_counter* ba) {
    crdt_zset_tag_add_counter* a = RedisModule_Alloc(sizeof(crdt_zset_tag_add_counter));
    a->add_counter = ba->add_counter;
    a->add_vcu = ba->add_vcu;
    a->gid = ba->gid;
    a->type = TAG_ADD_COUNTER;
    RedisModule_Free(ba);
    return a;
}
crdt_zset_tag_add_del_counter* BA2AD(crdt_zset_tag_base_and_add_counter* ba) {
    crdt_zset_tag_add_del_counter* ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
    ad->add_counter = ba->add_counter;
    ad->add_vcu = ba->add_vcu;
    ad->gid = ba->gid;
    ad->type = AD;
    ad->del_counter = 0;
    ad->del_vcu = 0;
    RedisModule_Free(ba);
    return ad;
}
crdt_zset_tag_add_del_counter*  BAD2AD(crdt_zset_tag_base_and_add_del_counter* bad) {
    crdt_zset_tag_add_del_counter* ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
    ad->add_counter = bad->add_counter;
    ad->add_vcu = bad->add_vcu;
    ad->del_counter = bad->del_counter;
    ad->del_vcu = bad->del_vcu;
    ad->gid = bad->gid;
    ad->type = AD;
    RedisModule_Free(bad);
    return ad;
}

crdt_zset_tag_base_and_add_del_counter* B2BAD(crdt_zset_tag_base* b) {
    crdt_zset_tag_base_and_add_del_counter* bad = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));
    bad->base_timespace = b->base_timespace;
    bad->base_vcu = b->base_vcu;
    bad->gid = b->gid;
    bad->score = b->score;
    bad->type = BAD;
    bad->add_counter = 0;
    bad->add_vcu = 0;
    bad->del_counter = 0;
    bad->del_vcu = 0;
    RedisModule_Free(b);
    return bad;
}
static int sort_tag_by_gid(const void *a, const void *b) {
    const crdt_zset_tag *tag_a = *(crdt_zset_tag**)a, *tag_b = *(crdt_zset_tag**)b;
    /* We sort the vector clock unit by gid*/
    if (tag_a->gid > tag_b->gid)
        return 1;
    else if (tag_a->gid == tag_b->gid)
        return 0;
    else
        return -1;
}

crdt_zset_element add_tag_by_element(crdt_zset_element el, crdt_zset_tag* tag) {
    if(tag == NULL) {return el;}
    if(el.len == 0) {
        crdt_zset_element e = {.len = 1, .tags = *(long long*)&tag};
        return e;
    } else if(el.len == 1) {
        crdt_zset_tag** tags = RedisModule_Alloc(sizeof(crdt_zset_tag*) * 2);
        long long a = el.tags;
        tags[0] = *(crdt_zset_tag**)&a;
        tags[1] = tag;
        el.len = 2;
        qsort(tags, 2, sizeof(crdt_zset_tag*), sort_tag_by_gid);
        el.tags = *(long long*)&tags;
        return el;
    } else {
        long long a = el.tags;
        crdt_zset_tag* t = *(crdt_zset_tag**)(&a);
        crdt_zset_tag** tags = RedisModule_Realloc(t, sizeof(crdt_zset_tag*) * (el.len + 1));
        tags[el.len] = *tags;
        el.len = el.len + 1;
        qsort(tags, el.len, sizeof(crdt_zset_tag*), sort_tag_by_gid);
        el.tags = *(long long*)&tags;
        return el;
    }
    
}

crdt_zset_tag* create_base_tag_by_meta(CrdtMeta* meta, double score) {
    crdt_zset_tag_base* base = RedisModule_Alloc(sizeof(crdt_zset_tag_base));
    base->base_timespace = getMetaTimestamp(meta);
    base->base_vcu = get_vcu_by_meta(meta);
    base->score = score;
    base->type = TAG_BASE;
    base->gid = getMetaGid(meta);
    return (crdt_zset_tag*)base;
}


crdt_zset_tag* reset_base_tag(crdt_zset_tag* tag, CrdtMeta* meta, double score, int is_del, g_counter_meta* g_meta) {
    crdt_zset_tag_base* base;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_base_and_add_del_counter* bad;
    switch (tag->type)
    {
    case TAG_BASE:
        /* code */
        if(g_meta) {
            bad = B2BAD((crdt_zset_tag_base*)tag);
            goto callback_bad;
        } else {
            base = (crdt_zset_tag_base*)tag;
            if(base->base_vcu < get_vcu_by_meta(meta)) {
                base->base_timespace = getMetaTimestamp(meta);
                base->base_vcu = get_vcu_by_meta(meta);
                base-> score = score;
            }
            return (crdt_zset_tag*)base;
        }
        break;
    case BA:
        if(is_del || g_meta) {
            bad = BA2BAD((crdt_zset_tag_base_and_add_counter*)tag);
            goto callback_bad;
        }
        //can delete ,becase incrby after zadd must had g_meta
        {
            ba = tag;
            ba->base_timespace = getMetaTimestamp(meta);
            ba->base_vcu = get_vcu_by_meta(meta);
            ba->score = score;
            return ba;
        }
        
    break;
    case AD:
        bad = AD2BAD((crdt_zset_tag_add_del_counter*)tag);
        assert(bad->type == BAD);
        goto callback_bad;
    break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        goto callback_bad;
    break;
    case TAG_ADD_COUNTER:
        if(is_del || g_meta) {
            bad = A2BAD((crdt_zset_tag_add_counter*)tag);
            goto callback_bad;
        }
       
        //can delete ,becase incrby after zadd must had g_meta
        {
            crdt_zset_tag_add_counter* a = tag;
            ba = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_counter));
            ba-> type = BA;
            ba->add_counter = a->add_counter;
            ba->add_vcu = a->add_vcu;
            ba->gid = a->gid;

            ba->base_timespace = getMetaTimestamp(meta);
            ba->base_vcu = get_vcu_by_meta(meta);
            ba->score = score;
            RedisModule_Free(a);
            return ba;
        }
    break;
    default:
        assert(1 == 0);
        break;
    }
callback_bad:
    if(g_meta) {
        bad->del_counter = g_meta->conv.d;
        bad->del_vcu = g_meta->vcu;
    } 
    if(is_del) {
        bad->del_counter = bad->add_counter;
        bad->del_vcu = bad->add_vcu;
    }
    if(bad->base_vcu < get_vcu_by_meta(meta)) {
        bad->base_timespace = getMetaTimestamp(meta);
        bad->base_vcu = get_vcu_by_meta(meta);
        bad->score = score;
    }
    return (crdt_zset_tag*)bad;
}

crdt_zset_tag* clean_tag(crdt_zset_tag* tag, int isDelete) {
    if(tag == NULL) { return tag; }
    crdt_zset_tag_base_and_add_del_counter* bad;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_add_del_counter* ad;
    crdt_zset_tag_add_counter* a;
    crdt_zset_tag_base* b;
    switch (tag->type)
    {
    case TAG_BASE:
        // crdt_zset_tag_base* base = (crdt_zset_tag_base*)tag;
        b = (crdt_zset_tag_base*)tag;
        b->score = 0;
        b->base_timespace = DEL_TIME;
        return b;
        break;
    case TAG_ADD_COUNTER:
        ad = A2AD(tag);
        goto add_del;
        return (crdt_zset_tag*)ad;
        break;
    case AD:
        ad = (crdt_zset_tag_add_del_counter*)tag;
        goto add_del;
        break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        goto base_add_del;
        break;
    case BA:
        bad = BA2BAD(tag);
        goto base_add_del;
        break;
    default:
        assert(1 == 0);
        break;
    }
add_del:
    ad->del_counter = ad->add_counter;
    ad->del_vcu = ad->add_vcu;
    return ad;
base_add_del:
    bad->base_timespace = DEL_TIME;
    bad->score = 0;
    bad->del_counter = bad->add_counter;
    bad->del_vcu = bad->add_vcu;
    return bad;
}

crdt_zset_element update_base(crdt_zset_element el, CrdtMeta* meta, double score) {
    int gid = getMetaGid(meta);
    int updated = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            tag = reset_base_tag(tag, meta, score, 1, NULL);
            updated = 1;
        } else {
            tag = clean_tag(tag, 0);
        }
        element_set_tag_by_index(&el, i, tag);
    }
    if(updated == 0) {
        crdt_zset_tag* tag = create_base_tag_by_meta(meta, score);
        el = add_tag_by_element(el, tag);
    }
    return el;
}


crdt_zset_tag* update_tag_add_counter(crdt_zset_tag* tag, CrdtMeta* meta, double score, int is_incr) {
    if(tag == NULL) { return tag; }
    crdt_zset_tag_base_and_add_del_counter* bad = NULL;
    crdt_zset_tag_base_and_add_counter* ba = NULL;
    crdt_zset_tag_add_del_counter* ad = NULL;
    crdt_zset_tag_add_counter* a = NULL;
    long long vcu = get_vcu_by_meta(meta);
    switch (tag->type) {
        case TAG_BASE:
            ba = B2BA((crdt_zset_tag_base*)tag);
            if(ba->add_vcu < vcu) {
                ba->add_counter = score;
                ba->add_vcu = vcu;
            }
            return (crdt_zset_tag*)ba;
        break;
        case BA:
            ba = (crdt_zset_tag_base_and_add_counter*)(tag);
            if(ba->add_vcu < vcu) {
                ba->add_counter = is_incr? ba->add_counter + score: score;
                ba->add_vcu = vcu;
            }
            return (crdt_zset_tag*)ba;
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*)(tag);
            if(bad->add_vcu < vcu) {
                bad->add_counter = is_incr? bad->add_counter + score: score;
                bad->add_vcu = vcu;
            }
            return (crdt_zset_tag*)bad;
        break; 
        case AD:
            ad = (crdt_zset_tag_add_del_counter*)(tag);
            if(ad->add_vcu < vcu) {
                ad->add_counter = is_incr? ad->add_counter + score: score;
                ad->add_vcu = vcu;
            }
            return (crdt_zset_tag*)ad;
        break;
        case TAG_ADD_COUNTER:
            a = (crdt_zset_tag_add_counter*)(tag);
            if(a->add_vcu < vcu) {
                a->add_counter = is_incr? a->add_counter + score: score;
                a->add_vcu = vcu;
            }
            return (crdt_zset_tag*)a;
        break;
        default:
            RedisModule_Debug(logLevel, "add_counter type error");
            assert(1 == 0);
        break;
    };
    return NULL;
}

//create_null_tag
crdt_zset_tag_add_counter* create_null_add_counter_tag(int gid) {
    crdt_zset_tag_add_counter* a = RedisModule_Alloc(sizeof(crdt_zset_tag_add_counter));
    a->gid = gid;
    a->type = TAG_ADD_COUNTER;
    a->add_counter = 0;
    a->add_vcu = 0;
    return a;
}

crdt_zset_tag_base_and_add_del_counter* create_null_base_add_del_counter_tag(int gid) {
    crdt_zset_tag_base_and_add_del_counter* bad = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));
    bad->gid = gid;
    bad->type = BAD;
    bad->base_timespace = 0;
    bad->base_vcu = 0;
    bad->score = 0;
    
    bad->add_counter = 0;
    bad->add_vcu = 0;
    
    bad->del_counter = 0;
    bad->del_vcu = 0;
    return bad;
}

crdt_zset_tag_base* create_null_base_tag(int gid) {
    crdt_zset_tag_base* b = RedisModule_Alloc(sizeof(crdt_zset_tag_base));
    b->gid = gid;
    b->type = TAG_BASE;
    b->base_timespace = 0;
    b->base_vcu = 0;
    b->score = 0;
    return b;
}
crdt_zset_tag* create_add_counter_tag(CrdtMeta* meta, double sorted) {
    crdt_zset_tag_add_counter* add = create_null_add_counter_tag(getMetaGid(meta));
    add->add_counter = sorted;
    add->add_vcu = get_vcu_by_meta(meta);
    return (crdt_zset_tag*)add;
}


crdt_zset_element zset_update_add_counter(crdt_zset_element el, CrdtMeta* meta, double score){
    int gid = getMetaGid(meta);
    int updated = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            tag = update_tag_add_counter(tag, meta, score, 1);
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
            gcounter_meta_set_value(value, VALUE_TYPE_DOUBLE, &ad->del_counter, 1);
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*) tag;
            value->gid = bad->gid;
            value->vcu = bad->del_vcu;
            gcounter_meta_set_value(value, VALUE_TYPE_DOUBLE, &bad->del_counter, 1);
        break;
        default:
        return 0;
        break;
    }
    return 1;
}

sds add_counter_score_to_sds(crdt_zset_tag* tag) {
    crdt_zset_tag_add_counter* a;
    crdt_zset_tag_base_and_add_del_counter* bad;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_add_del_counter* ad;
    double v = 0;
    switch (tag->type)
    {
    case TAG_ADD_COUNTER:
        a = tag;
        v = a->add_counter;
        break;
    case BAD:
        bad = tag;
        v = bad->add_counter;
        break;
    case AD:
        ad = tag;
        v = ad->add_counter;
        break;
    case BA:
        ba = tag;
        v = ba->add_counter;
        break;
    default:
        RedisModule_Debug(logLevel, "add_counter_score_to_sds error");
        assert( 1 == 0);
        break;
    }
    char buf[256];
    int len = d2string(buf, sizeof(buf), v);
    return sdsnewlen(buf, len);
}
int push_item(sds field, sds* callback_items, int* index, int* callback_byte_size, sds item) {
    if(item != NULL) {
        callback_items[*index] = sdsdup(field);
        callback_items[*index+1] = item;
        *callback_byte_size += sdslen(field) + sdslen(item);
        *index = *index+2;
        return 1;
    }
    return 0;
}
sds score2sds(double* score) {
    char buf[256];
    int len = d2string(buf, sizeof(buf), *score);
    sds value = sdsnewlen(buf, len);
    long double ld  = 0;
    assert(string2ld(value, sdslen(value), &ld));
    *score = (double)ld;
    assert(!isnan(*score));
    return value;
}
int zsetAdd(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, int* flags, double score, double* newscore, sds* callback_items, int* callback_len, int* callback_byte_size) {
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
    
    crdt_zset* zset = retrieve_crdt_zset(ss);
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(sst);
    dictEntry *de;
    zskiplistNode *znode;
    // zskiplist* zsl = zset->zsl;
    updateCrdtSSLastVc(ss, getMetaVectorClock(meta));
    de = dictFind(zset->dict, field);
    if(de != NULL) {
        if (nx) {
            *flags |= ZADD_NOP;
            return 1;
        }
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        double curscore = get_score_by_element(el);
        if(incr) {
            el = zset_update_add_counter(el, meta, score);
            push_item(field, callback_items, callback_len, callback_byte_size, add_counter_score_to_sds(element_get_tag_by_gid(el, getMetaGid(meta), NULL)));
        } else {
            sds value = score2sds(&score);
            el = update_base(el, meta, score);
            sds del_str = g_counter_metas_to_sds(&el, zsetTag2DelGcounter, el.len);     
            if(del_str != NULL) {
                value = sdscatprintf(value, ",%s", del_str);
                sdsfree(del_str);
            }  
            push_item(field, callback_items, callback_len, callback_byte_size, value);
            // callback_items[*callback_len++] = sdsnewlen((char*)&score, sizeof(double));
            
            
        }
        dictSetSignedIntegerVal(de, *(long long*)&el);
        double nscore = get_score_by_element(el);
        *newscore = nscore;
        
        if(curscore != nscore) {
            zskiplistNode *node;
            assert(zslDelete(zset->zsl,curscore,field,&node));
            znode = zslInsert(zset->zsl,nscore,node->ele);
            node->ele = NULL;
            zslFreeNode(node);
            *flags |= ZADD_UPDATED;
        }
        return 1;
    } else if(!xx) {
        dictEntry* tde = NULL;
        if(zset_tombstone) {
            tde = dictUnlink(zset_tombstone->dict, field);
        }
        crdt_zset_tag* tag =  NULL;
        if(tde) {
            crdt_zset_element tel = *(crdt_zset_element*)&dictGetSignedIntegerVal(tde);
            if(incr) {
                tel = zset_update_add_counter(tel, meta, score);
                push_item(field, callback_items, callback_len, callback_byte_size, add_counter_score_to_sds(element_get_tag_by_gid(tel, getMetaGid(meta), NULL)));
            } else {
                sds value = score2sds(&score);
                tel = update_base(tel, meta, score);
                sds del_str = g_counter_metas_to_sds(&tel, zsetTag2DelGcounter, tel.len);     
                if(del_str != NULL) {
                    value = sdscatprintf(value, ",%s", del_str);
                    sdsfree(del_str);
                }  
                push_item(field, callback_items, callback_len, callback_byte_size, value);
            }
            dictFreeUnlinkedEntry(zset_tombstone->dict, tde);
            dictEntry* de = dictAddOrFind(zset->dict, sdsdup(field));
            dictSetSignedIntegerVal(de, *(long long*)&tel);
            double nscore = get_score_by_element(tel);
            zslInsert(zset->zsl, nscore, sdsdup(field));

        } else {
            crdt_zset_element  e = {.len = 0};
            if(incr) {
                tag = create_add_counter_tag(meta, score);
                push_item(field, callback_items, callback_len, callback_byte_size, add_counter_score_to_sds(tag));
            } else {
                sds value = score2sds(&score);
                tag = create_base_tag_by_meta(meta, score);
                push_item(field, callback_items, callback_len, callback_byte_size, value);
            }
            e = add_tag_by_element(e, tag);
            
            znode = zslInsert(zset->zsl, score, sdsdup(field));
            // dictAdd(zset->dict, field, ele);
            
            de = dictAddOrFind(zset->dict, sdsdup(field));
            dictSetSignedIntegerVal(de, *(long long*)&e);
        
        }
       
        if (newscore) *newscore = score;
        *flags |= ZADD_ADDED;
        return 1;
    } else {
        *flags |= ZADD_NOP;
        return 1;
    }
    return 0;
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

size_t getZsetTombstoneSize(CRDT_SSTombstone* sst) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(sst);
    return dictSize(zset_tombstone->dict);
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        if (x->ele && sdscmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

long zsetRank(CRDT_SS* ss, sds ele, int reverse) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    zskiplist *zsl = zset->zsl;
    dictEntry *de;
    double score;
    unsigned long llen;
    unsigned long rank = 0;
    llen = getZSetSize(ss);
    de = dictFind(zset->dict,ele);
    if (de != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        score = get_score_by_element(el);
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










double zsetIncr(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double score) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    dictEntry* de =  dictFind(zset->dict, field);
    zskiplist* zsl = zset->zsl;
    zskiplistNode *znode;
    updateCrdtSSLastVc(ss, getMetaVectorClock(meta));
    if(de != NULL) {
        long long v = dictGetSignedIntegerVal(de);
        crdt_zset_element el = *(crdt_zset_element*)&v;
        double curscore = get_score_by_element(el);
        el = zset_update_add_counter(el, meta, score);
        dictSetSignedIntegerVal(de, *(long long*)&el);
        double newscore = get_score_by_element(el);
        if(curscore != newscore) {
            zskiplistNode *node;
            assert(zslDelete(zsl,curscore,field,&node));
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

void free_elements(crdt_zset_element el) {
    if(el.len > 1) {
        long long a = el.tags;
        crdt_zset_tag* t = *(crdt_zset_tag**)(&a);
        RedisModule_Free(t);
    }
}
crdt_zset_tag* create_null_tag(int gid, long long vcu) {
    crdt_zset_tag_base* b = RedisModule_Alloc(sizeof(crdt_zset_tag_base));
    b->type = TAG_BASE;
    b->gid = gid;
    b->base_timespace = 0;
    b->base_vcu = vcu;
    b->score = 0;
    return b;
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



int initSSTombstoneFromSS(CRDT_SSTombstone* tombstone,CrdtMeta* del_meta, CRDT_SS* value, sds* del_counters) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(value);
    sst->lastvc = vectorClockMerge(getMetaVectorClock(del_meta),ss->lastvc);
    sst->maxdelvc = dupVectorClock(ss->lastvc);
    dictIterator* next = dictGetIterator(ss->dict);
    dictEntry* de = NULL;
    int i = 0;
    dictEntry* del_de = NULL;
    while((de = dictNext(next)) != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        crdt_zset_element del_el = {.len = 0, .tags = 0};
        for(int i = 0; i < el.len; i++) {
            crdt_zset_tag* tag = element_get_tag_by_index(el, i);
            crdt_zset_tag* del_tag = clean_tag(tag, 1);
            if(del_tag != NULL) {
                del_el = add_tag_by_element(del_el, del_tag);
            } 
        }
        if(del_el.len != 0) {
            sds meta_info = g_counter_metas_to_sds(&del_el, zsetTag2DelGcounter, del_el.len);
            if(meta_info == NULL) {
                del_counters[i++] = sdsdup(dictGetKey(de));
            } else {
                sds v = sdsdup(dictGetKey(de));
                v = sdscatprintf(v, ",%s", meta_info);
                sdsfree(meta_info);
                del_counters[i++] = v;
            }
            del_de = dictAddOrFind(sst->dict, sdsdup(dictGetKey(de)));
            dictSetSignedIntegerVal(del_de, *(long long*)&del_el);
        }
    }
    return i;
}

double parseScore(sds info, int* len, g_counter_meta* g) {
    char* split_index = strstr(info, ",");
    long double ld = 0;
    if(split_index == NULL) {
        if(!string2ld(info, sdslen(info), &ld)) {
            RedisModule_Debug(logLevel, "[parseScore] score parse score error:  str is only");
            assert( 1 == 0);
        }
    } else { 
        if(!string2ld(info, split_index - info, &ld)) {
            RedisModule_Debug(logLevel, "[parseScore] score parse score error: has gcounter");
            assert( 1 == 0);
        }
        size_t index = split_index - info + 1;
        if(sdslen(info) > index) {
            char* data = info;
            *len = str_to_g_counter_meta(data + index, sdslen(info) - index,  g);
            assert(*len > 0);
        } 
    }
    double score = (double)ld;
    
    return score;
}
int find_g_meta(g_counter_meta** gcounters, int gcounter_len, int gid) {
    for(int j = 0; j < gcounter_len; j++) {
        g_counter_meta* meta = gcounters[j];
        if(meta == NULL) continue;
        if(meta->gid == gid) return j;
    }
    return -1;
}



crdt_zset_tag* clean_base_tag(crdt_zset_tag* tag, int vcu, g_counter_meta* meta) {
    crdt_zset_tag_base* b;
    crdt_zset_tag_add_counter* a;
    crdt_zset_tag_add_del_counter* ad = NULL;
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_base_and_add_del_counter* bad;
    switch(tag->type) {
        case TAG_BASE:
            b = (crdt_zset_tag_base*)tag;
            if(b->base_vcu <= vcu) {
                if(meta != NULL) {
                    bad = B2BAD(b);
                    goto base_add_del;
                } 
                b->base_vcu = vcu;
                b->base_timespace = DEL_TIME;
                b->score = 0;
                return b;
            } else {
                if(meta) {
                    bad = B2BAD(b);
                    goto bad_update_del_counter;
                } else {
                    return b;
                }
            }
        break;
        case TAG_ADD_COUNTER:
            a = (crdt_zset_tag_add_counter*)tag;
            if(meta) {
                ad = A2AD(a);
                goto add_del;
            }else{
                return a;
            }
        break;
        case BA:
            ba = (crdt_zset_tag_base_and_add_counter*)tag;
            if(ba->base_vcu <= vcu && !meta) {
                ba->base_vcu = vcu;
                ba->base_timespace = 0;
                ba->score = 0;
                return ba;
            } else {
                bad = BA2BAD(ba);
                goto base_add_del;   
            }
        break;
        case AD:
            ad = (crdt_zset_tag_add_del_counter*)tag;
            goto add_del;
        break;
        case BAD:
            bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
            if(bad->base_vcu <= vcu) {
                goto base_add_del;
            } else {
                goto bad_update_del_counter;
            }
        break;
        default:
            assert(1 == 0);
        break;
    }
add_del:
    if(meta && ad->del_vcu < meta->vcu) {
        ad->del_counter = meta->conv.d;
        ad->del_vcu = meta->vcu;
    }
    return ad;
base_add_del:
    if(bad->base_vcu <= vcu) {
        bad->base_vcu = vcu;
        bad->base_timespace = DEL_TIME;
        bad->score = 0;
    }
bad_update_del_counter:
    if(meta && bad->del_vcu < meta->vcu) {
        bad->del_counter = meta->conv.d;
        bad->del_vcu = meta->vcu;
    }
    return bad;
}


crdt_zset_tag* add_delete_counter(crdt_zset_tag* tag, g_counter_meta* g_meta) {
    if(!g_meta) {
        return tag;
    }
    crdt_zset_tag_add_del_counter* ad = NULL;
    crdt_zset_tag_base_and_add_del_counter* bad = NULL;
    if(tag == NULL) {
        ad = RedisModule_Alloc(sizeof(crdt_zset_tag_add_del_counter));
        ad->type = AD;
        ad->add_counter = 0;
        ad->add_vcu = 0;
        ad->del_counter = 0;
        ad->del_vcu = 0;
        goto callback_add_del;
    }
    switch(tag->type) {
        case TAG_BASE:
            bad = B2BAD(tag);
            goto callback_base_add_del;
        break;
        case TAG_ADD_COUNTER:
            ad = A2AD(tag);
            goto callback_add_del;
        break;
        case BA:
            bad = BA2BAD(tag);
            goto callback_base_add_del;
        break;
        case AD:
            ad = tag;
            goto callback_add_del;
        break;
        case BAD:
            bad = tag;
            goto callback_base_add_del;
        break;
        default:
            assert(1 == 0);
        break;
    }
callback_add_del:
    ad->del_counter = g_meta->conv.d;
    ad->del_vcu = g_meta->vcu;
    ad->gid = g_meta->gid;
    ad->type = AD;
    return ad;
callback_base_add_del:
    bad->del_counter = g_meta->conv.d;
    bad->del_vcu = g_meta->vcu;
    bad->gid = g_meta->gid;
    bad->type = BAD;
    return bad;

}

int zsetTryAdd(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(current);
    VectorClock vc = getMetaVectorClock(meta);
    int gcounter_len = 0;
    g_counter_meta* gcounters[get_len(vc)];
    double score = parseScore(info, &gcounter_len, gcounters);
    int gid = getMetaGid(meta);
    if(sst) {
        if(isVectorClockMonoIncr(vc, sst->maxdelvc) ) {
            return 0;
        }
        dictEntry* tde = dictFind(sst->dict, field);
        if(tde != NULL) {
            crdt_zset_element tel = *(crdt_zset_element*)&dictGetSignedIntegerVal(tde);
            crdt_zset_element rel = {.len = 0};
            int added = 0;
            for(int i = 0, len = get_len(vc); i < len; i++) {
                clk* c = get_clock_unit_by_index(&vc, i);
                int c_gid = get_gid(*c);
                int c_vcu = get_logic_clock(*c);
                int g_index = find_g_meta(gcounters, gcounter_len, c_gid);
                int index = 0;
                crdt_zset_tag* tag =  element_get_tag_by_gid(tel, c_gid, &index);
                if (c_gid == gid) {
                    if (tag) {
                        tag = reset_base_tag(tag, meta, score, 0, g_index == -1? NULL: gcounters[g_index]);
                        if(!is_deleted_tag(tag)) {
                            added = 1;
                        }
                        element_set_tag_by_index(&tel, index, tag);
                        rel = add_tag_by_element(rel, tag);
                    } else {
                        if(g_index == -1) {
                            crdt_zset_tag_base* b = create_null_base_tag(c_gid);
                            b->score = score;
                            b->base_vcu = get_vcu_by_meta(meta);
                            b->base_timespace = getMetaTimestamp(meta);
                            tag = b;
                        } else {
                            crdt_zset_tag_base_and_add_del_counter* bad = create_null_base_add_del_counter_tag(c_gid);
                            bad->base_timespace = DEL_TIME;
                            bad->base_vcu = c_vcu;
                            bad->del_counter = gcounters[g_index]->conv.d;
                            bad->del_vcu = gcounters[g_index]->vcu;
                            bad->score = score;
                            tag = bad;  
                            
                        }
                        added = 1;
                        rel = add_tag_by_element(rel, tag);
                    }
                } else {
                    if(tag) {
                        tag = clean_base_tag(tag, c_vcu, g_index == -1? NULL: gcounters[g_index]);
                        element_set_tag_by_index(&tel, index, tag);
                        rel = add_tag_by_element(rel, tag);
                    } else {
                        
                        if(g_index != -1) {
                            crdt_zset_tag_base_and_add_del_counter* bad = create_null_base_add_del_counter_tag(c_gid);
                            bad->base_timespace = DEL_TIME;
                            bad->base_vcu = c_vcu;
                            bad->del_counter = gcounters[g_index]->conv.d;
                            bad->del_vcu = gcounters[g_index]->vcu;
                            // bad->add_counter = gcounters[g_index]->conv.d;
                            // bad->add_vcu = gcounters[g_index]->vcu;
                            rel = add_tag_by_element(rel, bad);
                        } else {
                            crdt_zset_tag_base* b = create_null_base_tag(c_gid);
                            b->base_timespace = DEL_TIME;
                            rel = add_tag_by_element(rel, b);
                        }
                    }
                    
                }
                if(g_index != -1) {
                    free_g_counter_meta(gcounters[g_index]);
                    gcounters[g_index] = NULL;
                }
            }

            for(int i = 0, len = tel.len; i < len; i++) {
                crdt_zset_tag* tag = element_get_tag_by_index(tel, i);                
                int t_gid = tag->gid;
                if(element_get_tag_by_gid(rel, t_gid, NULL)) {
                    continue;
                }
                rel = add_tag_by_element(rel, tag);
            }
            //only to do assert  can delete
            for(int i = 0; i < gcounter_len; i++) {
                if(gcounters[i]) {
                    sds vc_str = vectorClockToSds(vc);
                    printf("code error gcounter exist not used : %s %d\n", vc_str, gcounters[i]->gid);
                    assert( 1 == 0);
                }
            }
            //=============

            free_elements(tel);
            if(added) {
                //only to do assert  can delete
                if(dictFind(ss->dict, field)) {
                    printf("info: %s\n", crdtZSetInfo(ss));
                    printf("tombstone: %s\n", crdtZsetTombstoneInfo(sst));
                    printf("[tryAdd]code error value and tombstone can't all exist\n");
                    assert( 1 == 0);
                }
                //=============

                dictEntry* de = dictAddOrFind(ss->dict, sdsdup(field));
                dictSetSignedIntegerVal(de, *(long long*)&rel);

                dictDelete(sst->dict, field);

                double score = get_score_by_element(rel);
                zslInsert(ss->zsl, score, sdsdup(field));
            } else {
                dictSetSignedIntegerVal(tde, *(long long*)&rel);
            }
            return added;
        } 
    }
    dictEntry* de = dictFind(ss->dict, field);
    // double curscore = get_score_by_element(el);
    if(de != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        double curscore = get_score_by_element(el);
        for(int i = 0, len = get_len(vc); i < len; i++) {
            clk* c = get_clock_unit_by_index(&vc, i);
            int c_gid = get_gid(*c);
            int c_vcu = get_logic_clock(*c);
            int index = 0;
            crdt_zset_tag* tag =  element_get_tag_by_gid(el, c_gid, &index);
            int g_index = find_g_meta(gcounters, gcounter_len, c_gid);
            if(tag == NULL) {
                if(c_gid == gid) {
                    tag = create_base_tag_by_meta(meta, score);
                } else {
                    tag = create_null_tag(c_gid, c_vcu);
                }
                el = add_tag_by_element(el, tag);
            } else {
                if(c_gid == gid) {
                    // tag = reset_base_tag(tag, c_vcu, g_index == -1? NULL: gcounters[g_index]);
                    tag = reset_base_tag(tag, meta, score, 0, g_index == -1? NULL: gcounters[g_index]);
                } else {
                    tag = clean_base_tag(tag, c_vcu, g_index == -1? NULL: gcounters[g_index]);
                }
                element_set_tag_by_index(&el, index, tag);
            }
            if(g_index != -1) {
                free_g_counter_meta(gcounters[g_index]);
                gcounters[g_index] = NULL;
            }
        }
        for(int i = 0; i < gcounter_len; i++) {
            g_counter_meta* g_meta = gcounters[i];
            if(g_meta == NULL) {
                continue;
            }
            int index = 0;
            crdt_zset_tag* tag =  element_get_tag_by_gid(el, g_meta->gid, &index);
            if(tag != NULL) {
                tag = add_delete_counter(tag, g_meta);
                element_set_tag_by_index(&el, index, tag);
            } else {
                tag = add_delete_counter(tag, g_meta);
                el = add_tag_by_element(el, tag);
            }
            free_g_counter_meta(gcounters[i]);
            gcounters[i] = NULL;
        }
        dictSetSignedIntegerVal(de,  *(long long*)&el);
        double nscore = get_score_by_element(el);
        if(curscore != nscore) {
            zskiplistNode *node;
            assert(zslDelete(ss->zsl,curscore,field,&node));
            zslInsert(ss->zsl,nscore,node->ele);
            node->ele = NULL;
            zslFreeNode(node);
        }
    } else {
        de = dictAddOrFind(ss->dict, sdsdup(field));
        crdt_zset_element el = {.len =0};
        int added = 0;
        for(int i = 0; i < gcounter_len; i++) {
            g_counter_meta* g_meta = gcounters[i];
            crdt_zset_tag* tag = NULL;
            if(g_meta->gid == gid) {
                crdt_zset_tag_base_and_add_del_counter* bad =  RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));
                bad->score = score;
                bad->type = BAD;
                bad->gid = gid;
                bad->base_vcu = get_vcu_by_meta(meta);
                bad->base_timespace = getMetaTimestamp(meta);
                bad->add_counter = g_meta->conv.d;
                bad->add_vcu = g_meta->vcu;
                bad->del_counter = g_meta->conv.d;
                bad->del_vcu = g_meta->vcu;
                tag = (crdt_zset_tag*)bad;
                added = 1;
            } else {
                tag = add_delete_counter(NULL, g_meta);
            }
            el = add_tag_by_element(el, tag);
            free_g_counter_meta(gcounters[i]);
            gcounters[i] = NULL;
        }
        if(added == 0) {
            crdt_zset_tag* tag = create_base_tag_by_meta(meta, score);
            el = add_tag_by_element(el, tag);
        }
        dictSetSignedIntegerVal(de,  *(long long*)&el);
        zslInsert(ss->zsl, score, sdsdup(field));
    }
    return 1;
}
//zsetTryIncrby key gid time vc field score
int zsetTryIncrby(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds score_str) {
    crdt_zset_tombstone* sst = retrieve_crdt_zset_tombstone(tombstone);
    crdt_zset* ss = retrieve_crdt_zset(current);
    VectorClock vc = getMetaVectorClock(meta);
    long double ld = 0;
    int gid = getMetaGid(meta);
    if(!string2ld(score_str, sdslen(score_str), &ld)) {
        RedisModule_Debug(logLevel, "[zsetTryIncrby] parse double error");
        assert( 1 == 0 );
        return 0;
    }
    double score = (double)ld;
    if(tombstone) {
        if(isVectorClockMonoIncr(vc, sst->maxdelvc) ) {
            return 0;
        }
        dictEntry* tde = dictFind(sst->dict, field);
        if(tde != NULL) {
            int added = 0;
            crdt_zset_element tel = *(crdt_zset_element*)&dictGetSignedIntegerVal(tde);
            int index;
            crdt_zset_tag* tag = element_get_tag_by_gid(tel, gid, &index);
            if (tag) {
                tag = update_tag_add_counter(tag, meta, score, 0);
                element_set_tag_by_index(&tel, index, tag);
            } else {
                added = 1;
                crdt_zset_tag_add_counter* a =  create_null_add_counter_tag(gid);
                a->add_vcu = get_vcu_by_meta(meta);
                a->add_counter = score;
                tel = add_tag_by_element(tel, a);
            }
            if(added) {
                dictEntry* de = dictAddOrFind(ss->dict, sdsdup(field));
                dictSetSignedIntegerVal(de, *(long long*)&tel);
                zslInsert(ss->zsl, score, sdsdup(field));
                dictDelete(sst->dict, field);
            } else {
                dictSetSignedIntegerVal(tde, *(long long*)&tel);
            }
            return added;
        }
    }
    dictEntry* de = dictFind(ss->dict, field);
    // double curscore = get_score_by_element(el);
    if(de != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        int index = -1;
        crdt_zset_tag* tag = element_get_tag_by_gid(el, gid, &index);
        double old_score = get_score_by_element(el);
        if(tag == NULL) {
            crdt_zset_tag_add_counter* a = create_null_add_counter_tag(gid);
            el = add_tag_by_element(el, update_tag_add_counter(a, meta, score, 0));
        }else{
            tag = update_tag_add_counter(tag, meta, score, 0);
            element_set_tag_by_index(&el, index, tag);
        }
        double new_score = get_score_by_element(el);
        dictSetSignedIntegerVal(de,  *(long long*)&el);
        zskiplistNode *node;
        assert(zslDelete(ss->zsl,old_score, field,&node));
        zslInsert(ss->zsl, new_score, node->ele);
        node->ele = NULL;
        zslFreeNode(node);
    } else {    
        de = dictAddOrFind(ss->dict, sdsdup(field));
        crdt_zset_element el = {.len =0};
        crdt_zset_tag_add_counter* a =  create_null_add_counter_tag(gid);
        el = add_tag_by_element(el, update_tag_add_counter(a, meta, score, 0));
        dictSetSignedIntegerVal(de,  *(long long*)&el);
        zslInsert(ss->zsl, score, sdsdup(field));
    }
    return 1;
}



//only del no merge
sds zsetDel(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, int* stats) {
    crdt_zset* zset = retrieve_crdt_zset(ss);
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(sst);
    dictEntry* de = dictUnlink(zset->dict, field);
    if(de == NULL) {
        return NULL;
    }
    VectorClock vc = getMetaVectorClock(meta);
    crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
    crdt_zset_element del_el = {.len = 0, .tags = 0};
    double score = get_score_by_element(el);
    int counter_num = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = element_get_tag_by_index(el, i);
        crdt_zset_tag* del_tag = clean_tag(tag, 0);
        if(del_tag != NULL) {
            counter_num ++;
            del_el = add_tag_by_element(del_el, del_tag);
        } else { 
            del_el = add_tag_by_element(del_el, tag);
        }
    }
    free_elements(el);
    dictFreeUnlinkedEntry(zset->dict,de);
    /* Delete from skiplist. */
    int retval = zslDelete(zset->zsl,score,field,NULL);
    assert(retval);
    if(counter_num != 0) {  
        dictEntry* del_de = dictAddOrFind(zset_tombstone->dict, sdsdup(field));
        dictSetSignedIntegerVal(del_de, *(long long*)&del_el);
        *stats = 2;
        sds gmeta_str = g_counter_metas_to_sds(&del_el, zsetTag2DelGcounter, del_el.len);
        if(gmeta_str == NULL) {
            return sdsdup(field);
        }
        sds r = sdscatprintf(sdsdup(field), ",%s", gmeta_str);
        sdsfree(gmeta_str);
        return r;
    } 
    *stats = 1;
    return sdsdup(field);
}
sds parseField(sds info, int* len, g_counter_meta* g) {
    char* split_index = strstr(info, ",");
    if(split_index == NULL) {
        return sdsdup(info);
    } else { 
        sds field = sdsnewlen(info, split_index - info);
        size_t index = split_index - info + 1;
        if(sdslen(info) > index) {
            char* data = info;
            *len = str_to_g_counter_meta(data + index, sdslen(info) - index,  g);
            assert(*len > 0);
        } 
        return field;
    }
}
int zsetTryRem(CRDT_SSTombstone* sst,CRDT_SS* ss, sds info, CrdtMeta* meta) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(sst);
    crdt_zset* zset = retrieve_crdt_zset(ss);
    VectorClock vc = getMetaVectorClock(meta);
    int gid = getMetaGid(meta);
    int gcounter_len = 0;
    g_counter_meta* gcounters[get_len(vc)];
    sds field = parseField(info, &gcounter_len, gcounters);
    dictEntry* de = NULL;
    if(zset) {
        de = dictFind(zset->dict, field);
    }
    dictEntry* tde = dictFind(zset_tombstone->dict, field);
    if(de != NULL && tde != NULL) {
        printf("zset try zrem  value and tombstone all exist: %s \n", field);
        return -1;
    }
    if (de) {
       crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
       crdt_zset_element result_el = {.len = 0};
       double old_score = get_score_by_element(el);
       int deleted = 1;
       for(int i = 0, len = el.len; i < len; i++) {
            crdt_zset_tag* tag = element_get_tag_by_index(el, i);
            long long c_vcu = get_vcu(vc, tag->gid);
            int meta_index = find_g_meta(gcounters, gcounter_len, tag->gid); 
            if(c_vcu == 0) {
               deleted = 0;
            } 
            tag = clean_base_tag(tag, c_vcu, meta_index != -1? gcounters[meta_index]: NULL);
            if(!is_deleted_tag(tag)) {
                deleted = 0;
            }
            result_el = add_tag_by_element(result_el, tag);
            if(meta_index != -1) {
                free_g_counter_meta(gcounters[meta_index]);
                gcounters[meta_index] = NULL;
            }
        }
        free_elements(el);
        for(int i = 0, len = get_len(vc); i < len; i++) {
            clk* c = get_clock_unit_by_index(&vc, i);
            int c_gid = get_gid(*c);
            long long vcu = get_logic_clock(*c);
            if(element_get_tag_by_gid(result_el, c_gid, NULL)) {
                continue;
            }
            int meta_index = find_g_meta(gcounters, gcounter_len, c_gid); 
            if(meta_index != -1) {
                crdt_zset_tag_base_and_add_del_counter* bad = create_null_base_add_del_counter_tag(c_gid);
                bad->base_timespace = DEL_TIME;
                bad->base_vcu = vcu;
                bad->del_counter = gcounters[meta_index]->conv.d;
                bad->del_vcu = gcounters[meta_index]->vcu;
                result_el = add_tag_by_element(result_el, bad);
                free_g_counter_meta(gcounters[meta_index]);
                gcounters[meta_index] = NULL;
            } else {
                crdt_zset_tag_base* b = create_null_base_tag(c_gid);
                b->base_timespace = DEL_TIME;
                b->base_vcu = vcu;
                result_el = add_tag_by_element(result_el, b);
            }
        }
        for(int i = 0; i< gcounter_len; i++) {
            if(gcounters[i]) {
                printf("[zsetTryRem] error: gcounter must is null\n");
                assert(1 == 0);
            }
        }
        if(deleted) {
            dictDelete(zset->dict, field);
            tde = dictAddOrFind(zset_tombstone->dict, sdsdup(field));
            dictSetSignedIntegerVal(tde, *(long long*)&result_el);
            zslDelete(zset->zsl, old_score, field, NULL);
        } else {
            double score = get_score_by_element(result_el);
            dictSetSignedIntegerVal(de, *(long long*)&result_el);
            zskiplistNode* node;
            assert(zslDelete(zset->zsl, old_score, field, &node));
            zslInsert(zset->zsl, score, node->ele);
            node->ele = NULL;
            zslFreeNode(node);
        }
        
        return deleted;
    }
    crdt_zset_element tel = {.len = 0 };
    if(tde) {
        tel = *(crdt_zset_element*)&dictGetSignedIntegerVal(tde);
    } else {
        tde = dictAddOrFind(zset_tombstone->dict, field);
    }
    for(int i = 0, len = get_len(vc); i < len; i++) {
        clk* c = get_clock_unit_by_index(&vc, i);
        int c_gid = get_gid(*c);
        long long vcu = get_logic_clock(*c);
        int index = 0;
        crdt_zset_tag* tag = element_get_tag_by_gid(tel, c_gid, &index);
        int meta_index = find_g_meta(gcounters, gcounter_len, c_gid); 
        if(tag) {
            tag = clean_base_tag(tag, vcu, meta_index != -1? gcounters[meta_index]: NULL);
            element_set_tag_by_index(&tel, index, tag);
        } else {
            if(meta_index != -1) {
                crdt_zset_tag_base_and_add_del_counter* bad = create_null_base_add_del_counter_tag(c_gid);
                bad->base_timespace = DEL_TIME;
                bad->base_vcu = vcu;
                bad->del_counter = gcounters[meta_index]->conv.d;
                bad->del_vcu = gcounters[meta_index]->vcu;
                tel = add_tag_by_element(tel, bad);
                free_g_counter_meta(gcounters[meta_index]);
                gcounters[meta_index] = NULL;
            } else {
                crdt_zset_tag_base* b = create_null_base_tag(c_gid);
                b->base_timespace = DEL_TIME;
                b->base_vcu = vcu;
                tel = add_tag_by_element(tel, b);
            }
        }
        
    }
    dictSetSignedIntegerVal(tde, *(long long*)&tel);
    return 1;
}
// tombstone method
CrdtTombstone* crdtZsetTombstoneMerge(CrdtTombstone* target, CrdtTombstone* src) {
    return NULL;
}
CrdtTombstone** crdtZsetTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) {
    return NULL;
}
void freeCrdtZsetTombstoneFilter(CrdtTombstone** filters, int num) {

}
int crdtZsetTombstonePurge(CrdtTombstone* tombstone, CrdtData* r) {
    return 0;
}
sds crdtZsetTombstoneInfo(void* tombstone) {
    crdt_zset_tombstone* zset_tombstone = retrieve_crdt_zset_tombstone(tombstone);
    dictIterator* it = dictGetIterator(zset_tombstone->dict);
    dictEntry* de ;
    sds result = sdsempty();
    long long size = dictSize(zset_tombstone->dict);
    sds vc_info = vectorClockToSds(zset_tombstone->lastvc);
    result = sdscatprintf(result, "1) type: orset_zset_tombstone, vc: %s, size: %lld\n", vc_info, size);
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
int crdtZsetTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    return 0;
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


