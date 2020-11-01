#include "crdt_orset_zset.h"

#define BAD 7
#define BA 3
#define AD 6
//  generic function
crdt_zset_tombstone* retrieveCrdtSSTombstone(CRDT_SSTombstone* rt) {
    return (crdt_zset_tombstone*)rt;
}

crdt_zset* retrieveCrdtSS(CRDT_SS* rc) {
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
    s->dict = dictCreate(&crdtSetDictType, NULL);
    s->zsl = zslCreate();
    return (CRDT_SS*)s;
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

crdt_zset_tag* getOneTags(crdt_zset_element el) {
    return (crdt_zset_tag*)el.tags;
}
crdt_zset_tag** getTags(crdt_zset_element el) {
    if(el.len == 0) {
        return NULL;
    }
    if(el.len == 1) {
        crdt_zset_tag* t = getOneTags(el);
        return &t;
    }
    return el.tags;
}

int getTagByGid(crdt_zset_element el, int gid) {
    crdt_zset_tag** tag = (el.tags);
    for(int i = 0; i < el.len; i++) {
        if(tag[i]->gid == gid) {
            return tag[i];
        }
    }
    return NULL;
    
}

double getBase(crdt_zset_tag* tag, long long* time) {
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

double getCounter(crdt_zset_tag* tag) {
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
    default:
        return 0;
        break;
    }
}

double getScoreByElement(crdt_zset_element el) {
    crdt_zset_tag** tags = getTags(el);
    double base = 0;
    double counter = 0;
    long long time = 0;
    for(int i = 0; i < el.len; i++) {
        long long cureen_time = 0;
        double score = getBase(tags[i], &cureen_time);
        if(cureen_time > time) {
            base = score;
            time = cureen_time;
        }
        counter += getCounter(tags[i]);
    }
    return base + counter;
}

#define EMPTY_ELEMENT {.len=0,.tags=0} 

crdt_zset_element add_tag(crdt_zset_element el, crdt_zset_tag* tag) {
    if(el.len == 0) {
        crdt_zset_element e = {.len = 0, .tags = 0};
        e.len = 1;
        e.tags = tag;
        return e;
    } else if(el.len == 1) {
        crdt_zset_tag** tags = RedisModule_Alloc(sizeof(crdt_zset_tag*) * 2);
        tags[0] = el.tags;
        tags[1] = tag;
        el.len = 2;
        el.tags = tags;
        return el;
    } else {
        crdt_zset_tag** tags = RedisModule_Realloc(el.tags, sizeof(crdt_zset_tag*) * (el.len + 1));
        tags[el.len] = tags;
        el.len = el.len + 1;
        el.tags = tags;
        return el;
    }
    
}

crdt_zset_tag *createBaseTag(CrdtMeta* meta, double score) {
    crdt_zset_tag_base* base = RedisModule_Alloc(sizeof(crdt_zset_tag_base));
    base->base_timespace = getMetaTimestamp(meta);
    base->base_vcu = get_vcu_by_meta(meta);
    base->score = score;
    base->type = TAG_BASE;
    base->gid = getMetaGid(meta);
    return base;
}


crdt_zset_tag* changeBaseAndCleanTag(crdt_zset_tag* tag, CrdtMeta* meta, double score) {
    crdt_zset_tag_base_and_add_counter* ba;
    crdt_zset_tag_base* base;
    crdt_zset_tag_base_and_add_del_counter* bad;
    crdt_zset_tag_add_del_counter* ad;
    switch (tag->type)
    {
    case TAG_BASE:
        /* code */
        base = (crdt_zset_tag_base*)tag;
        base->base_timespace = getMetaTimestamp(meta);
        base->base_vcu = get_vcu_by_meta(meta);
        base-> score = score;
        return base;
        break;
    case BA:
        ba = (crdt_zset_tag_base_and_add_counter*)tag;
        ba->base_timespace = getMetaTimestamp(meta);
        ba->base_vcu = get_vcu_by_meta(meta);
        ba->score = score;
        return ba;
    break;
    case AD:
        bad = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_del_counter));
        ad = (crdt_zset_tag_add_del_counter*)tag;
        bad->add_counter = ad->add_counter;
        bad->add_vcu = ad->add_vcu;
        bad->del_counter = ad->del_counter;
        bad->del_vcu = ad->del_vcu;
        bad->gid = ad->gid;
        bad->type = AD;
        bad->base_timespace = getMetaTimestamp(meta);
        bad->score = score;
        bad->base_vcu = get_vcu_by_meta(meta);
        RedisModule_Free(ad);
        return bad;
    break;
    case BAD:
        bad = (crdt_zset_tag_base_and_add_del_counter*)tag;
        bad->base_timespace = getMetaTimestamp(meta);
        bad->score = score;
        bad->base_vcu = get_vcu_by_meta(meta);
        return bad;
    break;
    case TAG_ADD_COUNTER:
        ba = RedisModule_Alloc(sizeof(crdt_zset_tag_base_and_add_counter));
        crdt_zset_tag_add_counter* a = (crdt_zset_tag_add_counter*)tag;
        ba->add_counter = a->add_counter;
        ba->add_vcu = a->add_vcu;
        ba->base_timespace = getMetaTimestamp(meta);
        ba->base_vcu = get_vcu_by_meta(meta);
        ba->score = score;
        return ba;
    break;
    default:
        assert(1 == 0);
        break;
    }
}

crdt_zset_tag* cleanTag(crdt_zset_tag* tag) {
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
        return ad;
        break;
    case AD:
        ad = (crdt_zset_tag_add_del_counter*)tag;
        ad->del_counter = ad->add_counter;
        ad->del_vcu = ad->add_vcu;
        return ad;
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
        return ad;
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
        return ad;
        break;
    default:
        assert(1 == 0);
        break;
    }

}

crdt_zset_element update_base(crdt_zset_element el, CrdtMeta* meta, double score) {
    int gid = getMetaGid(meta);
    crdt_zset_tag** tags = getTags(el);
    int added = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_zset_tag* tag = tags[i];
        if(tag->gid == gid) {
            tag = changeBaseAndCleanTag(tag, meta, score);
            added = 1;
        } else {
            tag = cleanTag(tag);
        }
        tags[i] = tag;
    }
    if(added == 0) {
        crdt_zset_tag* tag = createBaseTag(meta, score);
        el = add_tag(el, tag);
    }
    return el;
}





int zsetAdd(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double score) {
    crdt_zset* zset = retrieveCrdtSS(ss);
    int gid = getMetaGid(meta);
    dictEntry *de;
    zskiplistNode *znode;
    zskiplist* zsl = zset->zsl;
    de = dictFind(zset->dict, field);
    if(de != NULL) {
        crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
        double curscore = getScoreByElement(el);
        el = update_base(el, meta, score);
        dictSetSignedIntegerVal(de, *(long long*)&el);
        double newscore = getScoreByElement(el);
        if(curscore != newscore) {
            zskiplistNode *node;
            zslDelete(zsl,curscore,field,&node);
            znode = zslInsert(zsl,newscore,node->ele);
            node->ele = NULL;
            zslFreeNode(node);
        }
        return 0;
    } else {
        crdt_zset_tag* tag =  createBaseTag(meta, score);
        crdt_zset_element  e = {.len = 0};
        e = add_tag(e, tag);
        znode = zslInsert(zsl, score, sdsdup(field));
        // dictAdd(zset->dict, field, ele);
        de = dictAddOrFind(zset->dict, sdsdup(field));
        dictSetSignedIntegerVal(de, *(long long*)&e);
        return 1;
    }
    
}

double getScore(CRDT_SS* ss, sds field) {
    crdt_zset* zset = retrieveCrdtSS(ss);
    dictEntry* de =  dictFind(zset->dict,field);
    if(de == NULL) {
        return 0;
    } 
    crdt_zset_element el = *(crdt_zset_element*)&dictGetSignedIntegerVal(de);
    return getScoreByElement(el);
}