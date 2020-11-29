#include "g_counter_element.h"


/**************   about  tag ***********************/
//private 
void set_tag_data_type(crdt_tag* tag, int type) {
    tag->data_type = type;
}
void set_tag_type(crdt_tag* tag, int type) {
    tag->type = type;
}
void set_tag_gid(crdt_tag* tag, int gid) {
    tag->gid = gid;
}

//public 
//about tag type
int get_tag_type(crdt_tag* tag) {
    return tag->type;
}

int is_tombstone_tag(crdt_tag* tag) {
    return tag->type == TOMBSTONE_TAG? 1: 0;
}

int is_deleted_tag(crdt_tag* tag) {
    switch (get_tag_type(tag))
    {
    case  TAG_B: {
        crdt_tag_base* b = (crdt_tag_base*)tag;
        return b->base_timespace == DELETED_TIME?  1: 0;
    }
        break;
    case TAG_BAD: {
        crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
        if(bad->add_vcu == bad->del_vcu && bad->base_timespace == DELETED_TIME) {
            return 1;
        }
    }
    break;
    default:
        break;
    }
    return 0;
}

int get_tag_gid(crdt_tag* tag) {
    return tag->gid;
}

#define dup_tag_add(tag1, tag2) do { \
    tag1->counter_type = tag2->counter_type; \
    tag1->add_vcu = tag2->add_vcu; \
    tag1->add_counter = tag2->add_counter; \
} while(0)

#define dup_tag_del(tag1, tag2) do { \
    tag1->counter_type = tag2->counter_type; \
    tag1->del_vcu = tag2->del_vcu; \
    tag1->del_counter = tag2->del_counter; \
} while(0)


#define dup_tag_base(tag1, tag2) do { \
    tag1->base_data_type = tag2->base_data_type; \
    tag1->base_timespace = tag2->base_timespace; \
    tag1->base_vcu = tag2->base_vcu; \
    tag1->score = tag2->score; \
} while(0)

#define tag_set_add(tag1 , tag2) do { \
    if(tag1->counter_type == tag2->counter_type || tag1->counter_type == VALUE_TYPE_NONE) { \
        if(tag1->add_vcu < tag2->add_vcu) { \
            dup_tag_add(tag1, tag2); \
        } \
    } else { \
        printf("[add_tag_merge] Unimplemented code"); \
        assert(1==0);\
    }\
} while(0)

#define tag_set_del(tag1 , tag2) do { \
    if(tag1->counter_type == tag2->counter_type || tag1->counter_type == VALUE_TYPE_NONE) { \
        if(tag1->del_vcu < tag2->del_vcu) { \
            dup_tag_del(tag1, tag2); \
        } \
    } else { \
        printf("[add_tag_merge] Unimplemented code"); \
        assert(1==0);\
    }\
} while(0)

#define tag_set_base(tag1 , tag2) do {\
    if(tag1->base_data_type == tag2->base_data_type || tag1->base_data_type == VALUE_TYPE_NONE || tag2->base_data_type == VALUE_TYPE_NONE) { \
        if(tag1->base_vcu < tag2->base_vcu || (tag1->base_vcu == tag2->base_vcu && tag2->base_timespace == DELETED_TIME)) { \
            dup_tag_base(tag1, tag2); \
        } \
    } else { \
        printf("[tag_set_base] Unimplemented code"); \
        assert(1==0);\
    } \
} while(0)




//abdout create tag
//create base tag
crdt_tag* create_base_tag(int gid) {
    crdt_tag_base* b = counter_malloc(sizeof(crdt_tag_base));
    set_tag_gid(b, gid);
    set_tag_type(b, TAG_B);
    set_tag_data_type(b, VALUE_TAG);
    b->base_timespace = DELETED_TIME;
    b->base_vcu = 0;
    b->base_data_type = VALUE_TYPE_NONE;
    b->score.i = 0;
    return (crdt_tag*)b;
}
//create add tag 
crdt_tag* create_add_tag(int gid) {
    crdt_tag_add_counter* a = counter_malloc(sizeof(crdt_tag_add_counter));
    set_tag_gid(a, gid);
    set_tag_type(a, TAG_A);
    set_tag_data_type(a, VALUE_TAG);
    a->add_vcu = 0;
    a->counter_type = VALUE_TYPE_NONE;
    a->add_counter.i = 0;
    return (crdt_tag*)a;
}
//create base and add counter tag
crdt_tag* create_base_add_tag(int gid) {
    crdt_tag_base_and_add_counter* ba = counter_malloc(sizeof(crdt_tag_base_and_add_counter));
    set_tag_gid(ba, gid);
    set_tag_type(ba, TAG_BA);
    set_tag_data_type(ba, VALUE_TAG);
    ba->base_data_type = VALUE_TYPE_NONE;
    ba->base_timespace = DELETED_TIME;
    ba->base_vcu = 0;
    ba->counter_type = VALUE_TYPE_NONE;
    ba->add_vcu = 0;
    ba->add_counter.i = 0;
    return (crdt_tag*)ba;
}
crdt_tag* create_base_add_del_tag(int gid) {
    crdt_tag_base_and_add_del_counter* bad = counter_malloc(sizeof(crdt_tag_base_and_add_del_counter));
    set_tag_gid(bad, gid);
    set_tag_data_type(bad, VALUE_TAG);
    set_tag_type(bad, TAG_BAD);

    bad->base_data_type = VALUE_TYPE_NONE;
    bad->base_timespace = DELETED_TIME;
    bad->base_vcu = 0;
    
    bad->counter_type = VALUE_TYPE_NONE;
    bad->add_vcu = 0;
    bad->add_counter.i = 0;
    bad->del_vcu = 0;
    bad->del_counter.i = 0;
    return (crdt_tag*)bad;
}
//to do
crdt_tag* create_tombstone_base_tag(crdt_tag* tag) {
    return NULL;
}
crdt_tag* create_tombstone_base_add_del_tag(crdt_tag* tag) {
    return NULL;
}


crdt_tag* dup_crdt_tag(crdt_tag* tag) {
    int gid = get_tag_gid(tag);
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            crdt_tag_add_counter* ra = create_add_tag(gid);
            ra->counter_type = a->counter_type;
            dup_tag_add(ra, a);
            return (crdt_tag*)ra;
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            crdt_tag_base* rb = create_base_tag(gid);
            dup_tag_base(rb, b);
            return (crdt_tag*)rb;
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            crdt_tag_base_and_add_counter* rba = create_base_add_tag(gid);
            dup_tag_base(rba, ba);
            dup_tag_add(rba, ba);
            return (crdt_tag*)rba;
        }
        break;
        case TAG_BAD:
            assert(!is_tombstone_tag(tag));
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            crdt_tag_base_and_add_del_counter* rbad = create_base_add_del_tag(gid);
            dup_tag_base(rbad, bad);
            dup_tag_add(rbad, bad);
            rbad->del_vcu = bad->del_vcu;
            rbad->del_counter = bad->del_counter;
            return (crdt_tag*)rbad;
        break;
        default:
            printf("[dup_crdt_tag] type %d is error", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
}

sds get_tag_info(crdt_tag* tag) {
    sds result = sdsempty();
    switch(get_tag_type(tag)) {
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)(tag);
            result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f",get_tag_gid(b), b->base_vcu, b->base_timespace, b->score.f);
        }
        break;
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            result = sdscatprintf(result, "gid: %d, add_vcu: %lld, add: %.13f", get_tag_gid(a), a->add_vcu, a->add_counter.f);
            break;
        }
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f, add_vcu: %lld, add: %.13f", get_tag_gid(ba), ba->base_vcu, ba->base_timespace, ba->score.f, ba->add_vcu, ba->add_counter.f);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f, add_vcu: %lld, add: %.13f, del_vcu: %lld, del: %.13f", get_tag_gid(bad), bad->base_vcu, bad->base_timespace, bad->score.f, bad->add_vcu, bad->add_counter.f, bad->del_vcu, bad->del_counter.f);
        }
        break;
        default:
            assert(1 == 0);
        break;
    }
    return result;
}

void free_crdt_tag(crdt_tag* tag) {
    //to do
    //sds free
    counter_free(tag);
}
// #define merge_add(a, b)


crdt_tag_base_and_add_counter* A2BA(crdt_tag_add_counter* a) {
    crdt_tag_base_and_add_counter* ba = create_base_add_tag(get_tag_gid(a));
    dup_tag_add(ba, a);
    return ba;
}

crdt_tag_base_and_add_counter* B2BA(crdt_tag_base* b) {
    crdt_tag_base_and_add_counter* ba = create_base_add_tag(get_tag_gid(b));
    dup_tag_base(ba, b);
    return ba;
}

crdt_tag_base_and_add_del_counter* A2BAD(crdt_tag_add_counter* a) {
    crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(get_tag_gid(a));
    dup_tag_add(bad, a);
    return bad;
}

crdt_tag_base_and_add_del_counter* B2BAD(crdt_tag_base* b) {
    crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(get_tag_gid(b));
    dup_tag_base(bad, b);
    return bad;
}

crdt_tag_base_and_add_del_counter* BA2BAD(crdt_tag_base_and_add_counter* ba) {
    crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(get_tag_gid(ba));
    dup_tag_base(bad, ba);
    dup_tag_add(bad, ba);
    return bad;
}

crdt_tag* merge_add_tag(crdt_tag* target, crdt_tag_add_counter* other) {
    crdt_tag_base_and_add_counter* rba;
    switch(get_tag_type(target)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)target;
            tag_set_add(a, other);
            return (crdt_tag*)a;
        }
        break;
        case TAG_B: 
            rba = B2BA(target);
            goto callback_base_add;
        break;
        case TAG_BA: 
            rba = (crdt_tag_base_and_add_counter*)target;
            goto callback_base_add;
        break;
        case TAG_BAD:  {
            crdt_tag_base_and_add_del_counter* rbad = (crdt_tag_base_and_add_del_counter*)target;
            tag_set_add(rbad, other);
            return (crdt_tag*)rbad;
        }
        break;
        default:
            printf("[add_tag_merge] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_add:
    tag_set_add(rba, other);
    return (crdt_tag*)rba;
}


crdt_tag* merge_base_tag(crdt_tag* target, crdt_tag_base* other) {
    crdt_tag_base_and_add_counter* rba;
    switch(get_tag_type(target)) {
        case TAG_A: 
            rba = A2BA((crdt_tag_add_counter*)target);
            goto callback_base_add;
        break;
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)target;
            tag_set_base(b, other);
            return (crdt_tag*)b;
        }
        break;
        case TAG_BA: {
            rba = (crdt_tag_base_and_add_counter*)target;
            goto callback_base_add;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)target;
            tag_set_base(bad, other);
            return (crdt_tag*)bad;
        }
        break;
        default:
            printf("[add_tag_merge] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_add:
    tag_set_base(rba, other);
    return (crdt_tag*)rba;
}

crdt_tag* merge_base_add_tag(crdt_tag* target, crdt_tag_base_and_add_counter* other) {
    crdt_tag_base_and_add_counter* ba;
    switch(get_tag_type(target)) {
        case TAG_A: 
            ba = A2BA(target);
            goto callback_base_add;
        break;
        case TAG_B: 
            ba = B2BA(target);
            goto callback_base_add;
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)target;
            goto callback_base_add;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)target;
            tag_set_add(bad, other);
            tag_set_base(bad, other);
            return (crdt_tag*)bad;
        }
        break;
        default:
            printf("[add_tag_merge] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_add:
    tag_set_add(ba, other);
    tag_set_base(ba, other);
    return (crdt_tag*)ba;
}

crdt_tag* merge_base_add_del_tag(crdt_tag* target, crdt_tag_base_and_add_del_counter* tag) {
    crdt_tag_base_and_add_del_counter* bad;
    switch(get_tag_type(target)) {
        case TAG_A: 
            bad = A2BAD(target); 
            goto callback_base_add_del;
        break;
        case TAG_B: 
            bad = B2BAD(target);
            goto callback_base_add_del;
        break;
        case TAG_BA: 
            bad = BA2BAD(target);
            goto callback_base_add_del;
        break;
        case TAG_BAD: 
            bad = (crdt_tag_base_and_add_del_counter*)target;
            goto callback_base_add_del;
        break;
        default:
            printf("[add_tag_merge] target type %d is error", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
callback_base_add_del:
    tag_set_add(bad, tag);
    tag_set_base(bad, tag);
    tag_set_del(bad, tag);
    return (crdt_tag*)bad;
}

crdt_tag* merge_crdt_tag(crdt_tag* target, crdt_tag* other) {
    switch(get_tag_type(other)) {
        case TAG_A:
            return merge_add_tag(target, (crdt_tag_add_counter*)other);
        break;
        case TAG_B:
            return merge_base_tag(target, (crdt_tag_base*)other);
        break;
        case TAG_BA:
            return merge_base_add_tag(target, (crdt_tag_base_and_add_counter*)other);
        break;
        case TAG_BAD:
            return merge_base_add_del_tag(target, (crdt_tag_base_and_add_del_counter*)other);
        break;
        default:
            printf("[merge_crdt_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
}

#define clean_base(tag) do { \
    switch(tag->base_data_type) { \
        case VALUE_TYPE_LONGLONG: \
            tag->score.i = 0;\
        break;\
        case VALUE_TYPE_SDS:\
            if(tag->score.s) { sdsfree(tag->score.s);} \
            tag->score.s = sdsempty(); \
        break;\
        case VALUE_TYPE_DOUBLE:\
            tag->score.f = 0;\
        break;\
        case VALUE_TYPE_NONE: \
        break;\
        default:\
            printf("clean_base %d\n", tag->base_data_type);\
            assert(1==0);\
        break;\
    }\
} while(0)

crdt_tag* clean_crdt_tag(crdt_tag* tag, long long base_vcu) {
    if(tag == NULL) { return tag; }
    crdt_tag_base_and_add_del_counter* bad;
    switch (get_tag_type(tag)) {
    case TAG_B: {
        // crdt_zset_tag_base* base = (crdt_zset_tag_base*)tag;
        crdt_tag_base* b = (crdt_tag_base*)tag;
        if(base_vcu != -1) b->base_vcu = base_vcu;
        clean_base(b);
        b->base_timespace = DELETED_TIME;
        return b;
    }
        break;
    case TAG_A: 
        bad = A2BAD(tag);
        goto callback_base_add_del;
        break;
    case TAG_BAD:
        bad = (crdt_tag_base_and_add_del_counter*)tag;
        goto callback_base_add_del;
        break;
    case TAG_BA:
        bad = BA2BAD(tag);
        goto callback_base_add_del;
        break;
    default:
        assert(1 == 0);
        break;
    }

callback_base_add_del:
    if(base_vcu != -1) bad->base_vcu = base_vcu;
    bad->base_timespace = DELETED_TIME;
    clean_base(bad);
    bad->del_counter = bad->add_counter;
    bad->del_vcu = bad->add_vcu;
    return bad;
}


#define reset_value_by_meta(tag_value, meta) { \
    switch (meta->data_type) {\
        case VALUE_TYPE_DOUBLE:\
            tag_value.f = meta->value.f;\
            break;\
        case VALUE_TYPE_LONGLONG:\
            tag_value.i = meta->value.i; \
        break;\
        case VALUE_TYPE_SDS:\
            if (tag_value.s) { \
                sdsfree(tag_value.s);\
            } \
            tag_value.s = sdsdup(meta->value.s);\
        break;\
        case VALUE_TYPE_LONGDOUBLE:\
            printf("[reset_value_by_meta] type is long double\n");\
            assert(1 == 0);\
        break;\
        default:\
            printf("[reset_value_by_meta] type is err %d\n", meta->data_type);\
            break;\
    }\
} while(0)

crdt_tag* try_clean_tag(crdt_tag* tag, long long base_vcu, g_counter_meta* meta) { 
    crdt_tag_base_and_add_del_counter* bad;
    crdt_tag_base_and_add_counter* ba;
    meta->data_type;
    switch (get_tag_type(tag)) {
    case TAG_A: {
        crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
        if(meta) {
            bad = A2BAD(a);
            goto callback_base_add_del;
        } else {
            ba = A2BA(a);
            goto callback_base_add;
        }
    }
        break;
    case TAG_B: {
        crdt_tag_base* b = (crdt_tag_base*)tag;
        if(b->base_vcu < base_vcu) {
            if(meta) {
                bad = B2BAD(b);
                goto callback_base_add_del;
            }
            b->base_timespace = DELETED_TIME;
            clean_base(b);
            return (crdt_tag*)b;
        } else if(meta) {
            bad = B2BAD(b);
            goto callback_update_del_counter;
        } 
        return (crdt_tag*)b;
    }
    break;
    case TAG_BA:
        ba = (crdt_tag_base_and_add_counter*)tag;
        if(ba->base_vcu < base_vcu) {
            if(meta) {
                bad = BA2BAD(ba);
                goto callback_base_add_del;
            } else {
                goto callback_base_add; 
            }
        } if(meta) {
            goto callback_update_del_counter;
        } 
        return (crdt_tag*)ba;
    break;
    case TAG_BAD:
        bad = (crdt_tag_base_and_add_del_counter*)tag;
        goto callback_base_add_del;
    break;
    default:
        break;
    }

callback_base_add:
    if(ba->base_vcu < base_vcu) {
        ba->base_vcu = base_vcu;
        ba->base_timespace = DELETED_TIME;
        clean_base(ba);
    }
    return (crdt_tag*)ba;
callback_base_add_del:
    if(bad->base_vcu < base_vcu) {
        bad->base_vcu = base_vcu;
        bad->base_timespace = DELETED_TIME;
        clean_base(bad);
    }
callback_update_del_counter:
    if(meta) {
        if(bad->add_vcu < meta->vcu) {
            bad->counter_type = meta->data_type;
            reset_value_by_meta(bad->add_counter, meta);
        }
        if(bad->del_vcu < meta->vcu) {
            assert(bad->counter_type == meta->data_type);
            reset_value_by_meta(bad->del_counter, meta);
        }
    }
    return (crdt_tag*)bad;
    
}   

/**************   abdou tag ***********************/


/**************   abdou element  ***********************/


crdt_element get_element_by_dictEntry(dictEntry* di) {
    return  *(crdt_element*)&dictGetSignedIntegerVal(di);
}

void set_element_by_dictEntry(dictEntry* di, crdt_element el) {
    dictSetSignedIntegerVal(di, *(long long*)&el);
}

static int sort_tag_by_gid(const void *a, const void *b) {
    const crdt_tag *tag_a = *(crdt_tag**)a, *tag_b = *(crdt_tag**)b;
    /* We sort the vector clock unit by gid*/
    if (tag_a->gid > tag_b->gid)
        return 1;
    else if (tag_a->gid == tag_b->gid)
        return 0;
    else
        return -1;
}

crdt_element add_tag_by_element(crdt_element el, crdt_tag* tag) {
    if(tag == NULL) {return el;}
    if(el.len == 0) {
        crdt_element e = {.len = 1, .tags = tag};
        return e;
    } else if(el.len == 1) {
        crdt_tag** tags = RedisModule_Alloc(sizeof(crdt_tag*) * 2);
        long long a = el.tags;
        tags[0] = (crdt_tag*)a;
        tags[1] = tag;
        el.len = 2;
        qsort(tags, 2, sizeof(crdt_tag*), sort_tag_by_gid);
        el.tags = tags;
        return el;
    } else {
        crdt_tag** tags  = (crdt_tag**)el.tags;
        tags = RedisModule_Realloc(tags, sizeof(crdt_tag*) * (el.len + 1));
        tags[el.len] = tag;
        el.len = el.len + 1;
        qsort(tags, el.len, sizeof(crdt_tag*), sort_tag_by_gid);
        el.tags = tags;
        return el;
    }
}

crdt_tag* element_get_tag_by_index(crdt_element el, int index) {
    assert(el.len > index && index >= 0);
    if(el.len == 0) {
        return NULL;
    }
    if(el.len == 1) {
        long long ll = el.tags;
        return ll;
    }
    crdt_tag** tags = (crdt_tag**)(el.tags);
    return tags[index];
}

crdt_tag* element_get_tag_by_gid(crdt_element el, int gid, int* index) {
    for(int i = 0; i < el.len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            if(index != NULL) *index = i;
            return tag;
        }
    }
    return NULL;
}

void element_set_tag_by_index(crdt_element* el, int index, crdt_tag* tag) {
    assert(el->len > index && index >= 0);
    assert(tag != NULL);
    if(el->len == 1) {
        el->tags = tag;
        return;
    }
    crdt_tag** tags = (crdt_tag**)(el->tags);
    tags[index] = tag;
}
#define get_double_base(tag1, score, time) do {\
    switch (tag1->base_data_type) { \
    case VALUE_TYPE_SDS: { \
       long double ld = 0; \
        if(!string2ld(tag1->score.s, sdslen(tag1->score.s), &ld)) {\
            return 0; \
        } \
        *score = (double)ld; \
    } \
        break; \
    case VALUE_TYPE_DOUBLE: \
        *score = tag1->score.f; \
    break; \
    case VALUE_TYPE_LONGLONG: \
        *score = (double)(tag1->score.i); \
    break; \
    } \
    *time = tag1->base_timespace; \
    return 1; \
} while(0);

int get_double_base_score(crdt_tag* tag, double* score, long long* timespace) {
    switch(get_tag_type(tag)) {
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_double_base(ba, score, timespace)
            return 1;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            get_double_base(bad, score, timespace);
            return 1;
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            get_double_base(b, score, timespace);
            return 1;
        }
        break;
        case TAG_A: 
            *timespace = 0;
            *score = 0;
            return 1;
        break;
        default:
            printf("[get_double_base_score] target type %d is error", get_tag_type(tag));
            assert(1 == 0);
            return 0;
        break;
    }
}

#define get_double_add_counter(tag1, score) do { \
    switch (tag1->counter_type) { \
        case VALUE_TYPE_SDS: { \
            long double ld = 0; \
            if(!string2ld(tag1->add_counter.s, sdslen(tag1->add_counter.s), &ld)) {\
                return 0; \
            } \
            *score = (double)ld; \
            break; \
        } \
        case VALUE_TYPE_DOUBLE: \
            *score = tag1->add_counter.f; \
        break; \
        case VALUE_TYPE_LONGLONG: \
            *score = (double)(tag1->add_counter.i); \
        break; \
        default: \
            printf("[get_double_add_counter] target type %d is error", get_tag_type(tag));\
            assert(1 == 0);\
        break; \
    } \
} while(0)


#define get_double_del_counter(tag1, score) do { \
    switch (tag1->counter_type) { \
        case VALUE_TYPE_SDS: { \
            long double ld = 0; \
            if(!string2ld(tag1->del_counter.s, sdslen(tag1->del_counter.s), &ld)) {\
                return 0; \
            } \
            *score = (double)ld; \
            break; \
        } \
        case VALUE_TYPE_DOUBLE: \
            *score = tag1->del_counter.f; \
        break; \
        case VALUE_TYPE_LONGLONG: \
            *score = (double)(tag1->del_counter.i); \
        break; \
    } \
} while(0)


int get_double_counter_score(crdt_tag* tag, double* score, int had_del_counter) {
    switch(get_tag_type(tag)) {
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_double_add_counter(ba, score);
            return 1;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            double add = 0, del = 0;
            double* a = &add; 
            double* d = &del;
            get_double_add_counter(bad, a);
            if(had_del_counter) {
                get_double_del_counter(bad, d);
                *score = (add - del);
            } else{
                *score = add;
            }
            return 1;
        }
        break;
        case TAG_B:
            *score = 0;
            return 1;
        break;
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            get_double_add_counter(a, score);
            return 1;
        }
        break;
        default:
            printf("[get_double_base_score] target type %d is error", get_tag_type(tag));
            assert(1 == 0);
            return 0;
        break;
    }
}


int get_double_score_by_element(crdt_element el, double* score) {
    double base = 0;
    double counter = 0;
    long long time = 0;
    for(int i = 0; i < el.len; i++) {
        long long cureen_time = 0;
        crdt_tag* tag = element_get_tag_by_index(el, i);
        double score = 0;
        if(!get_double_base_score(tag, &score, &cureen_time)) {
            return 0;
        }
        if(cureen_time > time) {
            base = score;
            time = cureen_time;
        }
        double c = 0;
        if(!get_double_counter_score(tag, &c, 1)) {
            return 0;
        }
        counter += c;
    }
    if(isnan(base) || isnan(counter)) {
        printf("score %.17f %.17f\n", base , counter);
        assert(1 == 0);
    }
    *score = base + counter;
    return 1;
}

int get_double_add_counter_score_by_element(crdt_element el, int had_del_counter, double* score) {
    double counter = 0;
    for(int i = 0; i < el.len; i++) {
        long long cureen_time = 0;
        crdt_tag* tag = element_get_tag_by_index(el, i);
        double c = 0;
        if(!get_double_counter_score(tag, &c, had_del_counter)) {
            return 0;
        }
        counter += c;
    }
    if(isnan(counter)) {
        printf("score %.17f %.17f\n", counter);
        assert(1 == 0);
    }
    *score = counter;
    return  1;
}


size_t get_tag_memory(crdt_tag* tag) {
    switch(get_tag_type(tag)) {
        case TAG_B:
            return sizeof(crdt_tag_base);
        break;
        case TAG_A:
            return sizeof(crdt_tag_add_counter);
        break;
        case TAG_BA:
            return sizeof(crdt_tag_base_and_add_counter);
        break;
        case TAG_BAD:
            return sizeof(crdt_tag_base_and_add_del_counter);
        break;
        default:
            printf("filter type error: type is %d", tag->type);
            assert( 1 == 0);
        break;
    } 
}

int get_crdt_element_memory(crdt_element el) {
    size_t memory = 0;
    for(int i = 0; i < el.len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        size_t memory = get_tag_memory(tag);
    }
    return memory;
}
crdt_element dup_crdt_element(crdt_element el) {
    crdt_element rel = {.len = 0};
    for(int i = 0, len = el.len; i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        crdt_tag* rtag = dup_crdt_tag(tag);
        rel = add_tag_by_element(rel, rtag);
    }
    return rel;
}

sds get_element_info(crdt_element el) {
    sds result = sdsempty();
    for(int i = 0; i < el.len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        sds tag_info = get_tag_info(tag);
        result = sdscatprintf(result,"   %s\n", tag_info);
        sdsfree(tag_info);
    }
    return result;
}

int purge_element(crdt_element* tel, crdt_element* el) {
    crdt_element rel = create_crdt_element();
    int purge_tag_value = 1;
    for (int i = 0, len = tel->len; i<len; i++) {
        crdt_tag* ttag = element_get_tag_by_index(*tel, i);
        int index = 0;
        crdt_tag* tag = element_get_tag_by_gid(*el, ttag->gid, &index);
        if(tag != NULL) {
            ttag = merge_crdt_tag(ttag, tag);
            if(!is_deleted_tag(ttag)) {
                purge_tag_value = 0;
            }
        }
        rel = add_tag_by_element(rel, ttag);
    }
    for (int i = 0, len = el->len; i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(*el, i);
        if(element_get_tag_by_gid(rel, tag->gid, NULL)) {
            continue;
        }
        purge_tag_value = 0;
        rel = add_tag_by_element(rel, tag);
    }
    free_crdt_element_array(*el);
    free_crdt_element_array(*tel);
    crdt_element null_element = create_crdt_element();
    if(purge_tag_value == 1) {
        *el = null_element;
        *tel = rel;
        return 1;
    } else {
        *el = rel;
        *tel = null_element;
        return -1;
    }
}

crdt_element clean_element_by_vc(crdt_element el, VectorClock vc, int* is_deleted) {
    crdt_element rel = create_crdt_element();
    for(int i = 0, len = el.len; i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        long long c_vcu = get_vcu(vc, tag->gid);
        if(c_vcu == 0) {
            if(is_deleted) *is_deleted = 0;
            rel = add_tag_by_element(rel, tag);
            continue;
        } 
        tag = try_clean_tag(tag, c_vcu, NULL);
        if(is_deleted != NULL && !is_deleted_tag(tag)) {
            *is_deleted = 0;
        }
        rel = add_tag_by_element(rel, tag);
    }
    free_crdt_element_array(el);
    for(int i = 0, len = get_len(vc); i < len; i++) {
        clk* c = get_clock_unit_by_index(&vc, i);
        int c_gid = get_gid(*c);
        long long vcu = get_logic_clock(*c);
        if(element_get_tag_by_gid(rel, c_gid, NULL)) {
            continue;
        }
        crdt_tag_base* b = create_base_tag(c_gid);
        b->base_vcu = vcu;
        rel = add_tag_by_element(rel, b);
    } 
    return rel;
}

crdt_element create_crdt_element() {
    crdt_element el = {.len = 0};
    return el;
}

void free_crdt_element_array(crdt_element el) {
    if(el.len > 1) {
        el.len = 0;
        counter_free(el.tags);
    }
}

void free_crdt_element(void *val) {
    crdt_element el = *(crdt_element*)&val;
    if(el.len == 0) return;
    for(int i = 0; i < el.len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        free_crdt_tag(tag);
    }
    free_crdt_element_array(el);
}



sds sds_exec_add(sds a, sds b) {
    printf("[sds_exec_add] Unimplemented code");
    assert(1 == 0);
}

#define add_counter(tag1, vcu,type, value, incr) do {\
    if(tag1->add_vcu < vcu) {\
        if(tag1->counter_type == type) {\
            switch (type) { \
            case VALUE_TYPE_DOUBLE:\
                if(incr) { \
                    tag1->add_counter.f += value->f; \
                } else {\
                    tag1->add_counter.f = value->f;\
                }\
                break; \
            case VALUE_TYPE_LONGLONG: \
                if(incr) { \
                    tag1->add_counter.i += value->i; \
                } else {\
                    tag1->add_counter.i = value->i;\
                }\
                break;\
            case VALUE_TYPE_SDS:\
                if(incr) { \
                    tag1->add_counter.s = sds_exec_add(tag1->add_counter.s , value->s);\
                } else { \
                    sdsfree(tag1->add_counter.s);\
                    tag1->add_counter.s = sdsdup(value->s);\
                }\
                break;\
            default: \
                printf("[add_counter] type Unimplemented code"); \
                break; \
            }    \
        } else {\
            printf("[add_counter] Unimplemented code"); \
        }\
        tag1->add_vcu = vcu;\
    } \
} while(0)

crdt_tag* tag_add_counter(crdt_tag* tag, long long vcu, int type, union tag_data* value , int incr) {
    crdt_tag_base_and_add_counter* ba ;
    switch (get_tag_type(tag))
    {
    case TAG_A: {
        crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
        add_counter(a, vcu, type, value, incr);
        return (crdt_tag*)a;
    }
        break;
    case TAG_B: 
        ba = B2BA(tag);
        goto callback_base_add;
        break;
    case TAG_BA: 
        ba = (crdt_tag_base_and_add_counter*)(tag);
        goto callback_base_add;
        break;
    case TAG_BAD: {
        crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)(tag);
        add_counter(bad, vcu, type, value, incr);
        return (crdt_tag*)bad;
    }
    break;
    default:
        break;
    }
callback_base_add:
    add_counter(ba, vcu, type, value, incr);
    return (crdt_tag*)ba;
}

#define add_counter(tag1, value, incr) do {\
    if(tag1->add_vcu < value->add_vcu) {\
        if(tag1->counter_type == value->counter_type || tag1->counter_type == VALUE_TYPE_NONE) {\
            switch (value->counter_type) { \
            case VALUE_TYPE_DOUBLE:\
                if(incr) { \
                    tag1->add_counter.f += value->add_counter.f; \
                } else {\
                    tag1->add_counter.f = value->add_counter.f;\
                }\
                break; \
            case VALUE_TYPE_LONGLONG: \
                if(incr) { \
                    tag1->add_counter.i += value->add_counter.i; \
                } else {\
                    tag1->add_counter.i = value->add_counter.i;\
                }\
                break;\
            case VALUE_TYPE_SDS:\
                if(incr) { \
                    tag1->add_counter.s = sds_exec_add(tag1->add_counter.s , value->add_counter.s);\
                } else { \
                    sdsfree(tag1->add_counter.s);\
                    tag1->add_counter.s = sdsdup(value->add_counter.s);\
                }\
                break;\
            default: \
                printf("[add_counter] type Unimplemented code"); \
                break; \
            } \
            tag1-> counter_type = value->counter_type;   \
        } else {\
            printf("[add_counter] Unimplemented code"); \
        }\
        tag1->add_vcu = value->add_vcu;\
    } \
} while(0)

crdt_tag* tag_add_or_update(crdt_tag* tag, crdt_tag_add_counter* other, int incr) {
    crdt_tag_base_and_add_counter* ba ;
    switch (get_tag_type(tag))
    {
    case TAG_A: {
        crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
        add_counter(a, other, incr);
        return (crdt_tag*)a;
    }
        break;
    case TAG_B: 
        ba = B2BA(tag);
        goto callback_base_add;
        break;
    case TAG_BA: 
        ba = (crdt_tag_base_and_add_counter*)(tag);
        goto callback_base_add;
        break;
    case TAG_BAD: {
        crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)(tag);
        add_counter(bad, other ,incr);
        return (crdt_tag*)bad;
    }
    break;
    default:
        break;
    }
callback_base_add:
    add_counter(ba, other, incr);
    return (crdt_tag*)ba;
}

#ifndef COUNTER_BENCHMARK_MAIN

#define load_add(tag, rdb) do { \
    tag->counter_type = RedisModule_LoadUnsigned(rdb); \
    tag->add_vcu = RedisModule_LoadUnsigned(rdb); \
    switch(tag->counter_type) { \
        case VALUE_TYPE_LONGLONG: \
            tag->add_counter.i = RedisModule_LoadUnsigned(rdb); \
        break; \
        case VALUE_TYPE_DOUBLE: \
            tag->add_counter.f = RedisModule_LoadDouble(rdb); \
        break;\
        case VALUE_TYPE_SDS: { \
            tag->add_counter.s = RedisModule_LoadSds(rdb); \
        }\
        break; \
    } \
} while(0)

#define load_base(tag, rdb) do { \
    tag->base_vcu = RedisModule_LoadUnsigned(rdb);\
    tag->base_timespace = RedisModule_LoadUnsigned(rdb);\
    tag->base_data_type = RedisModule_LoadUnsigned(rdb); \
    switch(tag->base_data_type) { \
        case VALUE_TYPE_LONGLONG: \
            tag->score.i = RedisModule_LoadUnsigned(rdb); \
        break; \
        case VALUE_TYPE_DOUBLE: \
            tag->score.f = RedisModule_LoadDouble(rdb); \
        break;\
        case VALUE_TYPE_SDS: { \
            tag->score.s = RedisModule_LoadSds(rdb); \
        } \
        break; \
        default:\
            printf("[add_counter] type Unimplemented code"); \
        break;\
    } \
} while(0)

#define load_del(tag, rdb) do { \
    tag->del_vcu = RedisModule_LoadUnsigned(rdb); \
    switch(tag->counter_type) { \
        case VALUE_TYPE_LONGLONG: \
            tag->del_counter.i = RedisModule_LoadUnsigned(rdb); \
        break; \
        case VALUE_TYPE_DOUBLE: \
            tag->del_counter.f = RedisModule_LoadDouble(rdb); \
        break;\
        case VALUE_TYPE_SDS: { \
            tag->del_counter.s = RedisModule_LoadSds(rdb); \
        } \
        break; \
    } \
} while(0)

crdt_tag* load_crdt_tag_from_rdb(RedisModuleIO *rdb) {
    uint64_t gid = RedisModule_LoadUnsigned(rdb);
    uint64_t type = RedisModule_LoadUnsigned(rdb);
    uint64_t data_type = RedisModule_LoadUnsigned(rdb);
    switch(type) {
        case TAG_A: {
            crdt_tag_add_counter* a = create_add_tag(gid);

            load_add(a, rdb);
            return (crdt_tag*)a;
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = create_base_tag(gid);
            load_base(b,rdb);
            return (crdt_tag*)b;
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = create_base_add_tag(gid);
            load_base(ba, rdb);
            load_add(ba, rdb);
            return (crdt_tag*)ba;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(gid);
            load_base(bad, rdb);
            load_add(bad, rdb);
            load_del(bad, rdb);
            return bad;
        }
        break;
        default:
            printf("load tag error  \n");
            return NULL;
            break;
    }
}

crdt_element load_crdt_element_from_rdb(RedisModuleIO *rdb) {
    crdt_element el = create_crdt_element();
    uint64_t len = RedisModule_LoadUnsigned(rdb);
    for(int i = 0; i < len; i++) {
        crdt_tag* tag = load_crdt_tag_from_rdb(rdb);
        el = add_tag_by_element(el, tag);
    }
    return el;
}

#define save_add(tag, rdb) do { \
    RedisModule_SaveUnsigned(rdb, tag->counter_type); \
    RedisModule_SaveUnsigned(rdb, tag->add_vcu);\
    switch(tag->counter_type) { \
        case VALUE_TYPE_LONGLONG: \
            RedisModule_SaveUnsigned(rdb, tag->add_counter.i); \
        break; \
        case VALUE_TYPE_DOUBLE: \
            RedisModule_SaveDouble(rdb, tag->add_counter.f); \
        break;\
        case VALUE_TYPE_SDS: \
            RedisModule_SaveStringBuffer(rdb, tag->add_counter.s, sdslen(tag->add_counter.s)); \
        break; \
    } \
} while(0)

#define save_base(tag, rdb) do { \
    RedisModule_SaveUnsigned(rdb, tag->base_vcu);\
    RedisModule_SaveUnsigned(rdb, tag->base_timespace);\
    RedisModule_SaveUnsigned(rdb, tag->base_data_type); \
    switch(tag->base_data_type) { \
        case VALUE_TYPE_LONGLONG: \
            RedisModule_SaveUnsigned(rdb, tag->score.i); \
        break; \
        case VALUE_TYPE_DOUBLE: \
            RedisModule_SaveDouble(rdb, tag->score.f); \
        break;\
        case VALUE_TYPE_SDS: \
           RedisModule_SaveStringBuffer(rdb, tag->score.s, sdslen(tag->score.s)); \
        break; \
    } \
} while(0)

#define save_del(tag, rdb) do { \
    RedisModule_SaveUnsigned(rdb, tag->del_vcu);\
    switch(tag->counter_type) { \
        case VALUE_TYPE_LONGLONG: \
            RedisModule_SaveUnsigned(rdb, tag->del_counter.i); \
        break; \
        case VALUE_TYPE_DOUBLE: \
            RedisModule_SaveDouble(rdb, tag->del_counter.f); \
        break;\
        case VALUE_TYPE_SDS: \
           RedisModule_SaveStringBuffer(rdb, tag->del_counter.s, sdslen(tag->del_counter.s)); \
        break; \
    } \
} while(0)

void save_crdt_tag_to_rdb(RedisModuleIO *rdb, crdt_tag* tag) {
    RedisModule_SaveUnsigned(rdb, tag->gid);
    RedisModule_SaveUnsigned(rdb, tag->type);
    RedisModule_SaveUnsigned(rdb, tag->data_type);
    switch (get_tag_type(tag))
    {
    case TAG_A: {
        crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
        save_add(a, rdb);
        break;
    }
    case TAG_B: {
        crdt_tag_base* b = (crdt_tag_base*)tag;
        save_base(b, rdb);
    }
        break;
    case TAG_BA: {
        crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
        save_base(ba, rdb);
        save_add(ba, rdb);
    }
        break;
    case TAG_BAD: {
        crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
        save_base(bad, rdb);
        save_add(bad, rdb);
        save_del(bad, rdb);
    }
        break;
    default:
        printf("save tag to rdb error\n");
        assert(1 == 0);
        break;
    }
}
void save_crdt_element_to_rdb(RedisModuleIO *rdb, crdt_element el) {
    RedisModule_SaveUnsigned(rdb, el.len);
    for(int i = 0, len = el.len; i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        save_crdt_tag_to_rdb(rdb, tag);
    }
}
#endif

/**********************  abdou  element -***********************/

int tag_to_g_counter_meta(void* data, int index, g_counter_meta* value) {
    crdt_element el = *(crdt_element*)data;
    crdt_tag* tag = element_get_tag_by_index(el, index);
    value->data_type = VALUE_TYPE_DOUBLE;
    switch (get_tag_type(tag)) {
        case TAG_BAD:{
            crdt_tag_base_and_add_del_counter* bad =  (crdt_tag_base_and_add_del_counter*)tag;
            value->gid = get_tag_gid(tag);
            value->vcu = bad->del_vcu;
            value->data_type = bad->counter_type;
            switch(bad->counter_type) {
                case VALUE_TYPE_NONE:
                return 0;
                case VALUE_TYPE_DOUBLE:
                    gcounter_meta_set_value(value, VALUE_TYPE_DOUBLE, &bad->del_counter.f, 1);
                break; 
                case VALUE_TYPE_LONGLONG:
                    gcounter_meta_set_value(value, VALUE_TYPE_LONGLONG, &bad->del_counter.i, 1);
                break;
                case VALUE_TYPE_SDS:
                    gcounter_meta_set_value(value, VALUE_TYPE_SDS, &bad->del_counter.s, 1);
                break;
            }
        }
        break;
        default:
        return 0;
        break;
    }
    return 1;
}

#define get_all_type_from_base(tag, t, v) do {\
    t = tag->base_data_type;\
    switch(tag->base_data_type) {\
        case VALUE_TYPE_DOUBLE:\
            v.d = tag->score.f;\
        break;\
        case VALUE_TYPE_LONGLONG:\
            v.i = tag->score.i;\
        break;\
        case VALUE_TYPE_SDS:\
            v.s = tag->score.s;\
        break;\
        default:\
            printf("[get_all_type_from_base]type Unimplemented code \n");\
        break;\
    }\
} while(0)

sds get_base_value_sds_from_tag(crdt_tag* tag) {
    union all_type v = {.f = 0};
    int data_type = 0;
    switch(get_tag_type(tag)) {
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            get_all_type_from_base(b, data_type, v);
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_all_type_from_base(ba, data_type, v);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            get_all_type_from_base(bad, data_type, v);
        }
        break;
        default:
            printf("[get_base_value_sds_from_tag] type error :%d \n", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
    sds r = value_to_sds(data_type, v);
    printf("[value_to_sds]%d %s\n",data_type, r);
    return r;
}

sds get_base_value_sds_from_element(crdt_element el, int gid) {
    char buf[1000];
    int len = 0;
    crdt_tag* tag = element_get_tag_by_gid(el, gid, NULL);
    union all_type v = {.f = 0};
    int data_type = 0;
    switch(get_tag_type(tag)) {
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            get_all_type_from_base(b, data_type, v);
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_all_type_from_base(ba, data_type, v);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            get_all_type_from_base(bad, data_type, v);
        }
        break;
        default:
            printf("[get_base_value_sds_from_tag] type error :%d \n", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
    len += value_to_str(buf + len, data_type, v);
    sds dr = get_delete_counter_sds_from_element(el);
    if(dr != NULL) {
        buf[len++] = ',';
        memcpy(buf + len , dr, sdslen(dr));
        len += sdslen(dr);
    }
    return sdsnewlen(buf, len);
}

#define get_all_type_from_add(tag, t, v) do {\
    t = tag->counter_type;\
    switch(tag->counter_type) {\
        case VALUE_TYPE_DOUBLE:\
            v.d = tag->add_counter.f;\
        break;\
        case VALUE_TYPE_LONGLONG:\
            v.i = tag->add_counter.i;\
        break;\
        case VALUE_TYPE_SDS:\
            v.s = tag->add_counter.s;\
        break;\
        default:\
            printf("[get_all_type_from_base]type Unimplemented code \n");\
        break;\
    }\
} while(0)

sds get_add_value_sds_from_tag(crdt_tag* tag) {
    union all_type v = {.f = 0};
    int data_type;
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            get_all_type_from_add(a, data_type, v);
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_all_type_from_add(ba, data_type, v);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            get_all_type_from_add(bad, data_type, v);
        }
        break;
        default:
            printf("[get_base_value_sds_from_tag] type error :%d \n", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
    return value_to_sds(data_type, v);
}

sds get_delete_counter_sds_from_element(crdt_element el) {
    int max_len = el.len * (21 + 5 + 17);
    char buf[max_len];
    int len =  g_counter_metas_to_str(buf, &el, tag_to_g_counter_meta, el.len);
    if(len == 0) return NULL;
    return sdsnewlen(buf, len);
}

crdt_element merge_crdt_element(crdt_element a, crdt_element b) {
    for(int i = 0, len = b.len; i < len; i++) {
        crdt_tag* btag = element_get_tag_by_index(b, i);
        int index = 0;
        crdt_tag* atag = element_get_tag_by_gid(a, get_tag_gid(btag), &index);
        if(atag) {
            atag = merge_crdt_tag(atag, btag);
            free_crdt_tag(btag);
            element_set_tag_by_index(&a, index, atag);
        } else {
            a = add_tag_by_element(a, btag);
        }
    }
    free_crdt_element_array(b);
    return a;
}

void bad_merge_g_meta(crdt_tag_base_and_add_del_counter* bad, g_counter_meta* meta) {
    bad->counter_type = meta->data_type;
    bad->add_vcu = meta->vcu;
    copy_tag_data_from_all_type(meta->data_type, &bad->add_counter , meta->value);
    bad->del_vcu = meta->vcu;
    copy_tag_data_from_all_type(meta->data_type, &bad->del_counter , meta->value);
}

int find_meta_by_gid(int gid, int gcounter_len, g_counter_meta** metas) {
    for(int i = 0; i < gcounter_len; i++) {
        g_counter_meta* meta = metas[i];
        if(meta->gid == gid) {
            return i;
        }
    }
    return -1;
}


crdt_element create_element_from_vc_and_g_counter(VectorClock vc, int gcounter_len, g_counter_meta** metas, crdt_tag* base_tag) {
    crdt_element el =  create_crdt_element();
    int added = 0;
    
    for(int i = 0, len = get_len(vc); i < len; i++) {
        clk* c = get_clock_unit_by_index(&vc, i);
        int gid = get_gid(*c);
        int vcu = get_logic_clock(*c);
        int g_index =  find_meta_by_gid(gid, gcounter_len, metas);
        crdt_tag* tag = NULL;
        if(base_tag && base_tag->gid == gid) {
            if(g_index == -1) {
                tag = base_tag;
            } else {
                crdt_tag_base_and_add_del_counter* bad = B2BAD(base_tag);
                bad_merge_g_meta(bad, metas[g_index]);
                tag = bad;
            }
            added = 1;
        }else if(g_index == -1) {
            crdt_tag_base* b =  create_base_tag(gid);
            b->base_vcu = vcu;
            tag = b;
        } else {
            crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(gid);
            bad_merge_g_meta(bad, metas[g_index]);
            bad->base_vcu = vcu;
            tag = bad;
        }
        el = add_tag_by_element(el, tag);
    }
    return el;
}
