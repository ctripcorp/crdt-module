#include "g_counter_element.h"
#include <string.h>
#include <math.h>

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
    return tag->data_type == TOMBSTONE_TAG? 1: 0;
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
        if(bad->counter_type != VALUE_TYPE_LONGDOUBLE) {
            if(bad->add_vcu == bad->del_vcu && bad->base_timespace == DELETED_TIME) {
                return 1;
            }
        } else {
            crdt_tag_base_and_ld_add_del_counter* ldbad = (crdt_tag_base_and_ld_add_del_counter*)tag;
            if(ldbad->add_vcu == ldbad->del_vcu && ldbad->base_timespace == DELETED_TIME) {
                return 1;
            }
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
    tag1->add_vcu = tag2->add_vcu; \
    if(tag1->counter_type == VALUE_TYPE_SDS) {\
        sdsfree(tag1->add_counter.s);\
    }\
    tag1->counter_type = tag2->counter_type; \
    if(tag2->counter_type == VALUE_TYPE_SDS) {\
        tag1->add_counter.s = sdsdup(tag2->add_counter.s);\
    } else {\
        tag1->add_counter = tag2->add_counter; \
    }\
} while(0)

#define dup_tag_del(tag1, tag2) do { \
    tag1->del_vcu = tag2->del_vcu; \
    if(tag1->counter_type == VALUE_TYPE_SDS) {\
        sdsfree(tag1->del_counter.s);\
    }\
    if(tag2->counter_type == VALUE_TYPE_SDS) {\
        tag1->del_counter.s = sdsdup(tag2->del_counter.s);\
    } else {\
        tag1->del_counter = tag2->del_counter; \
    }\
} while(0)


#define dup_tag_base(tag1, tag2) do { \
    tag1->base_timespace = tag2->base_timespace; \
    tag1->base_vcu = tag2->base_vcu; \
    if(tag1->base_data_type == VALUE_TYPE_SDS) {\
        sdsfree(tag1->score.s);\
    }\
    tag1->base_data_type = tag2->base_data_type; \
    if(tag2->base_data_type == VALUE_TYPE_SDS) {\
        tag1->score.s = sdsdup(tag2->score.s);\
    } else { \
        tag1->score = tag2->score; \
    }\
} while(0)

#define tag_set_add(tag1 , tag2) do { \
    if(tag1->add_vcu < tag2->add_vcu ) { \
        if(tag1->counter_type != VALUE_TYPE_LONGDOUBLE && tag2->counter_type != VALUE_TYPE_LONGDOUBLE) {\
            dup_tag_add(tag1, tag2); \
        } else if(tag1->counter_type == VALUE_TYPE_LONGDOUBLE && tag2->counter_type == VALUE_TYPE_LONGDOUBLE ) {\
            dup_tag_add(tag1, tag2); \
        } else { \
            printf("[add_tag_merge] Unimplemented code"); \
            assert(1==0);\
        }\
    } \
} while(0)

#define tag_set_del(tag1 , tag2) do { \
    if(tag1->del_vcu < tag2->del_vcu) { \
        if(tag1->counter_type != VALUE_TYPE_LONGDOUBLE && tag2->counter_type != VALUE_TYPE_LONGDOUBLE) {\
            dup_tag_del(tag1, tag2); \
        } else if(tag1->counter_type == VALUE_TYPE_LONGDOUBLE && tag2->counter_type == VALUE_TYPE_LONGDOUBLE ) {\
            dup_tag_del(tag1, tag2); \
        } else { \
            printf("[add_tag_merge] Unimplemented code\n"); \
            assert(1==0);\
        }\
    } \
} while(0)

#define tag_set_base(tag1 , tag2) do {\
    if(tag1->base_data_type == tag2->base_data_type || tag1->base_data_type == VALUE_TYPE_NONE || tag2->base_data_type == VALUE_TYPE_NONE) { \
        if(tag1->base_vcu < tag2->base_vcu || (tag1->base_vcu == tag2->base_vcu && tag2->base_timespace == DELETED_TIME)) { \
            dup_tag_base(tag1, tag2); \
        } \
    } else { \
        printf("[tag_set_base] Unimplemented code\n"); \
        assert(1==0);\
    } \
} while(0)




//abdout create tag
//create base tag
crdt_tag_base* create_base_tag(int gid) {
    crdt_tag_base* b = counter_malloc(sizeof(crdt_tag_base));
    set_tag_gid((crdt_tag*)b, gid);
    set_tag_type((crdt_tag*)b, TAG_B);
    set_tag_data_type((crdt_tag*)b, VALUE_TAG);
    b->base_timespace = DELETED_TIME;
    b->base_vcu = 0;
    b->base_data_type = VALUE_TYPE_NONE;
    b->score.i = 0;
    return b;
}

crdt_tag_base_tombstone* create_base_tombstone(int gid) {
    crdt_tag_base_tombstone* tb = counter_malloc(sizeof(crdt_tag_base_tombstone));
    set_tag_gid((crdt_tag*)tb, gid);
    set_tag_type((crdt_tag*)tb, TAG_B);
    set_tag_data_type((crdt_tag*)tb, TOMBSTONE_TAG);
    tb->base_vcu = 0;
    return tb;
}

crdt_tag_base_add_del_tombstone* create_base_add_del_tombstone(int gid) {
    crdt_tag_base_add_del_tombstone* tbad = counter_malloc(sizeof(crdt_tag_base_add_del_tombstone));
    set_tag_gid((crdt_tag*)tbad, gid);
    set_tag_type((crdt_tag*)tbad, TAG_BAD);
    set_tag_data_type((crdt_tag*)tbad, TOMBSTONE_TAG);
    tbad->base_vcu = 0;
    tbad->counter_type = VALUE_TYPE_NONE;
    tbad->add_vcu = 0;
    tbad->add_counter.i = 0;
    tbad->del_vcu = 0;
    tbad->del_counter.i = 0;
    return tbad;
}

crdt_tag_base_ld_add_del_tombstone* create_base_ld_add_del_tombstone(int gid) {
    crdt_tag_base_ld_add_del_tombstone* tldbad = counter_malloc(sizeof(crdt_tag_base_ld_add_del_tombstone));
    set_tag_gid((crdt_tag*)tldbad, gid);
    set_tag_type((crdt_tag*)tldbad, TAG_BAD);
    set_tag_data_type((crdt_tag*)tldbad, TOMBSTONE_TAG);
    tldbad->base_vcu = 0;
    tldbad->counter_type = VALUE_TYPE_LONGDOUBLE;
    tldbad->add_vcu = 0;
    tldbad->add_counter = 0;
    tldbad->del_vcu = 0;
    tldbad->del_counter = 0;
    return tldbad;
}

#define init_add_counter(tag)  {\
    set_tag_gid((crdt_tag*)tag, gid);\
    set_tag_type((crdt_tag*)tag, TAG_A);\
    set_tag_data_type((crdt_tag*)tag, VALUE_TAG);\
    tag->add_vcu = 0;\
    tag->counter_type = VALUE_TYPE_NONE;\
}
//create add tag 
crdt_tag_add_counter* create_add_tag(int gid) {
    crdt_tag_add_counter* a = counter_malloc(sizeof(crdt_tag_add_counter));
    init_add_counter(a);
    a->add_counter.i = 0;
    return a;
}

crdt_tag_ld_add_counter* create_ld_add_tag(int gid) {
    crdt_tag_ld_add_counter* lda = counter_malloc(sizeof(crdt_tag_ld_add_counter));
    init_add_counter(lda);
    lda->counter_type = VALUE_TYPE_LONGDOUBLE;
    lda->add_counter = 0;
    return lda;
}

crdt_tag* create_add_tag_from_all_type(int gid, int type, union all_type value) {
    if(type == VALUE_TYPE_LONGDOUBLE) {
        crdt_tag_ld_add_counter* lda = create_ld_add_tag(gid);
        lda->counter_type = VALUE_TYPE_LONGDOUBLE;
        lda->add_counter = value.f;
        return (crdt_tag*)lda;
    } else {
        crdt_tag_add_counter* a = create_add_tag(gid);
        a->counter_type = type;
        copy_tag_data_from_all_type(type, &a->add_counter, value);
        return (crdt_tag*)a;
    }
}

#define init_base_tag(ba, gid) do {\
    set_tag_gid((crdt_tag*)ba, gid);\
    set_tag_type((crdt_tag*)ba, TAG_BA);\
    set_tag_data_type((crdt_tag*)ba, VALUE_TAG);\
    ba->base_data_type = VALUE_TYPE_NONE;\
    ba->base_timespace = DELETED_TIME;\
    ba->base_vcu = 0;\
    ba->add_vcu = 0;\
} while(0)
//create base and add counter tag
crdt_tag_base_and_add_counter* create_base_add_tag(int gid) {
    crdt_tag_base_and_add_counter* ba = counter_malloc(sizeof(crdt_tag_base_and_add_counter));
    init_base_tag(ba, gid);
    ba->counter_type = VALUE_TYPE_NONE;
    ba->add_counter.i = 0;
    return ba;
}

crdt_tag_base_and_ld_add_counter* create_base_ld_add(int gid) {
    crdt_tag_base_and_ld_add_counter* ldba = counter_malloc(sizeof(crdt_tag_base_and_ld_add_counter));
    init_base_tag(ldba, gid);
    ldba->add_counter = 0;
    ldba->counter_type = VALUE_TYPE_LONGDOUBLE;
    return ldba;
}
crdt_tag_base_and_add_del_counter* create_base_add_del_tag(int gid) {
    crdt_tag_base_and_add_del_counter* bad = counter_malloc(sizeof(crdt_tag_base_and_add_del_counter));
    set_tag_gid((crdt_tag*)bad, gid);
    set_tag_data_type((crdt_tag*)bad, VALUE_TAG);
    set_tag_type((crdt_tag*)bad, TAG_BAD);

    bad->base_data_type = VALUE_TYPE_NONE;
    bad->base_timespace = DELETED_TIME;
    bad->base_vcu = 0;
    bad->score.i = 0;
    
    bad->counter_type = VALUE_TYPE_NONE;
    bad->add_vcu = 0;
    bad->add_counter.i = 0;
    bad->del_vcu = 0;
    bad->del_counter.i = 0;
    return bad;
}

crdt_tag_base_and_ld_add_del_counter* create_base_ld_add_del(int gid) {
    crdt_tag_base_and_ld_add_del_counter* ldbad = counter_malloc(sizeof(crdt_tag_base_and_ld_add_del_counter));
    set_tag_gid((crdt_tag*)ldbad, gid);
    set_tag_data_type((crdt_tag*)ldbad, VALUE_TAG);
    set_tag_type((crdt_tag*)ldbad, TAG_BAD);
    ldbad->base_data_type = VALUE_TYPE_NONE;
    ldbad->base_timespace = DELETED_TIME;
    ldbad->base_vcu = 0;
    ldbad->score.i = 0;
    
    ldbad->counter_type = VALUE_TYPE_LONGDOUBLE;
    ldbad->add_vcu = 0;
    ldbad->add_counter = 0;
    ldbad->del_vcu = 0;
    ldbad->del_counter = 0;
    return ldbad;
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
            if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_add_counter* ra = create_add_tag(gid);
                ra->counter_type = a->counter_type;
                dup_tag_add(ra, a);
                return (crdt_tag*)ra;
            } else {
                crdt_tag_ld_add_counter* lda = (crdt_tag_ld_add_counter*)tag;
                crdt_tag_ld_add_counter* rlda = create_ld_add_tag(gid);
                rlda->add_vcu = lda->add_vcu;
                rlda->add_counter = lda->add_counter;
                return (crdt_tag*)rlda;
            }
            
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
            if(ba->counter_type != VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_add_counter* rba = create_base_add_tag(gid);
                dup_tag_base(rba, ba);
                dup_tag_add(rba, ba);
                return (crdt_tag*)rba; 
            } else {
                crdt_tag_base_and_ld_add_counter* ldba = (crdt_tag_base_and_ld_add_counter*)tag;
                crdt_tag_base_and_ld_add_counter* rldba = create_base_ld_add(gid);
                dup_tag_base(rldba, ldba);
                rldba->add_vcu = ldba->add_vcu;
                rldba->add_counter = ldba->add_counter;
                return (crdt_tag*)rldba;
            }
        }
        break;
        case TAG_BAD:
            assert(!is_tombstone_tag(tag));
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            if(bad->counter_type != VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_add_del_counter* rbad = create_base_add_del_tag(gid);
                dup_tag_base(rbad, bad);
                dup_tag_del(rbad, bad);
                dup_tag_add(rbad, bad);
                // rbad->del_vcu = bad->del_vcu;
                // rbad->del_counter = bad->del_counter;
                return (crdt_tag*)rbad;
            } else {
                crdt_tag_base_and_ld_add_del_counter* ldbad = (crdt_tag_base_and_ld_add_del_counter*)tag;
                crdt_tag_base_and_ld_add_del_counter* rldbad = create_base_ld_add_del(gid);
                dup_tag_base(rldbad, ldbad);
                rldbad->del_vcu = ldbad->del_vcu;
                rldbad->del_counter = ldbad->del_counter;
                rldbad->add_vcu = ldbad->add_vcu;
                rldbad->add_counter = ldbad->add_counter;
                return (crdt_tag*)rldbad;
            }
            
            
        break;
        default:
            printf("[dup_crdt_tag] type %d is error", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
}

sds printf_value(sds result, long long type, union tag_data value) {
    switch (type)
    {
    case VALUE_TYPE_SDS:
        return sdscatprintf(result, "%s", value.s);
        break;
    case VALUE_TYPE_LONGLONG:
        return sdscatprintf(result, "%lld", value.i);
    case VALUE_TYPE_DOUBLE:
        return sdscatprintf(result, "%.17f", value.f);
    case VALUE_TYPE_NONE:
        return sdscatprintf(result, "null");
    default:
        printf("[printf_value] type error %lld", type);
        assert(1 == 0);
        return sdsempty();
        break;
    }
    
} 

#define printf_base(result, tag) do {\
    result = sdscatprintf(result,", vcu: %lld, time: %lld, score: ", tag->base_vcu, tag->base_timespace);\
    result = printf_value(result, tag->base_data_type, tag->score);\
} while(0)
#define printf_add(result, tag) do {\
    result = sdscatprintf(result,", add_vcu: %lld, add: ", tag->add_vcu);\
    result = printf_value(result, tag->counter_type, tag->add_counter);\
} while(0)

#define printf_ld_add(result, tag) do {\
    result = sdscatprintf(result,", add_vcu: %lld, add: %.17Lf", tag->add_vcu, tag->add_counter);\
} while(0)

#define printf_del(result, tag) do {\
    result = sdscatprintf(result,", del_vcu: %lld, del: ", tag->del_vcu);\
    result = printf_value(result, tag->counter_type, tag->del_counter);\
} while(0)
#define printf_ld_del(result, tag) do {\
    result = sdscatprintf(result,", del_vcu: %lld, del: %.17Lf", tag->del_vcu, tag->del_counter);\
} while(0)

sds get_tag_info(crdt_tag* tag) {
    sds result = sdscatprintf(sdsempty(), "gid: %d", get_tag_gid(tag));
    switch(get_tag_type(tag)) {
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)(tag);
            printf_base(result, b);
            // result = sdscatprintf(result, "gid: %d, vcu: %lld, time: %lld, score: %.13f",get_tag_gid(b), b->base_vcu, b->base_timespace, b->score.f);
        }
        break;
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            if(a->counter_type == VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_ld_add_counter* lda = (crdt_tag_ld_add_counter*)tag;
                printf_ld_add(result, lda);
            } else {
                printf_add(result, a);
            }   
            break;
        }
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            printf_base(result, ba);
            if(ba->counter_type == VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_ld_add_counter* ldba = (crdt_tag_base_and_ld_add_counter*)tag;
                printf_ld_add(result, ldba);
            } else {
                printf_add(result, ba);
            }
            
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            printf_base(result, bad);
            if(bad->counter_type == VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_ld_add_del_counter* ldbad = (crdt_tag_base_and_ld_add_del_counter*)tag;
                printf_ld_add(result, ldbad);
                printf_ld_del(result, ldbad);
            } else {
                printf_add(result, bad);
                printf_del(result, bad);
            }
        }
        break;
        default:
            printf("[get_tag_info] tag:%d", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
    return result;
}


#define free_add_value(tag) do {\
    if(tag->counter_type == VALUE_TYPE_SDS) {\
        sdsfree(tag->add_counter.s);\
    }\
} while(0)

#define free_del_value(tag) do {\
    if(tag->counter_type == VALUE_TYPE_SDS) {\
        sdsfree(tag->del_counter.s);\
    }\
} while(0)

#define free_base_value(tag) do {\
    if(tag->base_data_type == VALUE_TYPE_SDS) {\
        sdsfree(tag->score.s);\
    }\
} while(0)
void free_crdt_tag(crdt_tag* tag) {
    //to do
    //sds free
    
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            free_add_value(a);
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            free_base_value(b);
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            free_base_value(ba);
            free_add_value(ba);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            free_base_value(bad);
            free_add_value(bad);
            free_del_value(bad);
        }
        break;
    }
    counter_free(tag);
}
// #define merge_add(a, b)


#define dup_tag_ld_add_from_nld_add(tag1, tag2) do { \
    tag1->add_vcu = tag2->add_vcu; \
    long double ld = 0;\
    tag_data_get_ld(tag2->counter_type, tag2->add_counter, &ld);\
    tag1->add_counter = ld;\
} while(0)

#define dup_tag_ld_add_from_nld_del(tag1, tag2) do { \
    tag1->del_vcu = tag2->del_vcu; \
    long double ld = 0;\
    tag_data_get_ld(tag2->counter_type, tag2->del_counter, &ld);\
    tag1->del_counter = ld;\
} while(0)


#define dup_tag_ld_add(tag1, a, ldclass) do {\
    if(a->counter_type == VALUE_TYPE_LONGDOUBLE) {\
        ldclass* lda = (ldclass*)a;\
        tag1->add_vcu = lda->add_vcu;\
        tag1->add_counter = lda->add_counter; \
    } else {\
        dup_tag_ld_add_from_nld_add(tag1, a);\
    }\
} while(0)

crdt_tag_base_and_add_counter* A2BA(crdt_tag_add_counter* a) {
    if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
        crdt_tag_base_and_add_counter* ba = create_base_add_tag(get_tag_gid((crdt_tag*)a));
        dup_tag_add(ba, a);
        free_crdt_tag((crdt_tag*)a);
        return ba;
    } else {
        crdt_tag_base_and_ld_add_counter* ldba = create_base_ld_add(get_tag_gid((crdt_tag*)a));
        dup_tag_ld_add_from_nld_add(ldba, a);
        free_crdt_tag((crdt_tag*)a);
        return (crdt_tag_base_and_add_counter*)ldba;
    }
}

crdt_tag_base_and_add_counter* B2BA(crdt_tag_base* b) {
    crdt_tag_base_and_add_counter* ba = create_base_add_tag(get_tag_gid((crdt_tag*)b));
    dup_tag_base(ba, b);
    free_crdt_tag((crdt_tag*)b);
    return ba;
}
crdt_tag_base_and_ld_add_counter* B2LDBA(crdt_tag_base* b) {
    crdt_tag_base_and_ld_add_counter* ldba = create_base_ld_add(get_tag_gid((crdt_tag*)b));
    dup_tag_base(ldba, b);
    free_crdt_tag((crdt_tag*)b);
    return ldba;
}

crdt_tag_base_and_ld_add_counter* BA2LDBA(crdt_tag_base_and_add_counter* ba) {
    if(ba->counter_type == VALUE_TYPE_LONGDOUBLE) {
        return (crdt_tag_base_and_ld_add_counter*)ba;
    }
    crdt_tag_base_and_ld_add_counter* ldba = create_base_ld_add(get_tag_gid((crdt_tag*)ba));
    dup_tag_base(ldba, ba);
    dup_tag_ld_add_from_nld_add(ldba, ba);
    free_crdt_tag((crdt_tag*)ba);
    return ldba;
}   
crdt_tag_base_and_ld_add_del_counter* BA2LDBAD(crdt_tag_base_and_add_counter* ba) {
    crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(get_tag_gid((crdt_tag*)ba));
    dup_tag_base(ldbad, ba);
    dup_tag_ld_add(ldbad, ba, crdt_tag_base_and_ld_add_counter);
    free_crdt_tag((crdt_tag*)ba);
    return ldbad;
}   

crdt_tag_base_and_add_del_counter* A2BAD(crdt_tag_add_counter* a) {
    if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
        crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(get_tag_gid((crdt_tag*)a));
        dup_tag_add(bad, a);
        bad->base_vcu = a->add_vcu;
        free_crdt_tag((crdt_tag*)a);
        return bad;
    } else {
        crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(get_tag_gid((crdt_tag*)a));
        dup_tag_ld_add(ldbad, a, crdt_tag_ld_add_counter);
        ldbad->base_vcu = a->add_vcu;
        assert(ldbad->counter_type == VALUE_TYPE_LONGDOUBLE);
        free_crdt_tag((crdt_tag*)a);
        return (crdt_tag_base_and_add_del_counter*)ldbad;
    }
    
}

crdt_tag_base_add_del_tombstone* A2TBAD(crdt_tag_add_counter* a) {
    if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
        crdt_tag_base_add_del_tombstone* tbad = create_base_add_del_tombstone(get_tag_gid((crdt_tag*)a));
        dup_tag_add(tbad, a);
        free_crdt_tag((crdt_tag*)a);
        return tbad;
    } else {
        crdt_tag_base_ld_add_del_tombstone* tldbad = create_base_ld_add_del_tombstone(get_tag_gid((crdt_tag*)a));
        dup_tag_ld_add(tldbad, a, crdt_tag_ld_add_counter);
        free_crdt_tag((crdt_tag*)a);
        return (crdt_tag_base_add_del_tombstone*)tldbad;
    }
}

crdt_tag_base_tombstone* B2TB(crdt_tag_base* b) {
    crdt_tag_base_tombstone* tb = create_base_tombstone(get_tag_gid((crdt_tag*)b));
    tb->base_vcu = b->base_vcu;
    free_crdt_tag((crdt_tag*)b);
    return tb;
}

crdt_tag_base_add_del_tombstone* BA2TBAD(crdt_tag_base_and_add_counter* ba) {
    printf("[BA2TBAD] Unimplemented code");
    assert(1 == 0);
    return NULL;
}

crdt_tag_base_add_del_tombstone* BAD2TBAD(crdt_tag_base_and_add_del_counter* bad) {
    printf("[BAD2TBAD] Unimplemented code");
    assert(1 == 0);
    return NULL;
}

crdt_tag_base_and_add_del_counter* B2BAD(crdt_tag_base* b) {
    crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(get_tag_gid((crdt_tag*)b));
    dup_tag_base(bad, b);
    free_crdt_tag((crdt_tag*)b);
    return bad;
}

crdt_tag_base_and_add_del_counter* BA2BAD(crdt_tag_base_and_add_counter* ba) {
    if(ba->counter_type != VALUE_TYPE_LONGDOUBLE) {
        crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(get_tag_gid((crdt_tag*)ba));
        dup_tag_base(bad, ba);
        dup_tag_add(bad, ba);
        free_crdt_tag((crdt_tag*)ba);
        return bad;
    } else {
        crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(get_tag_gid((crdt_tag*)ba));
        // crdt_tag_base_and_ld_add_counter* ldba = (crdt_tag_base_and_ld_add_counter*)ba;
        dup_tag_base(ldbad, ba);
        dup_tag_ld_add(ldbad, ba, crdt_tag_base_and_ld_add_counter);
        free_crdt_tag((crdt_tag*)ba);
        return (crdt_tag_base_and_add_del_counter*)ldbad;
    }
}

crdt_tag_base_and_ld_add_del_counter* B2LDBAD(crdt_tag_base* b) {
    crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(get_tag_gid((crdt_tag*)b));
    dup_tag_base(ldbad, b);
    free_crdt_tag((crdt_tag*)b);
    return ldbad;
}

crdt_tag_ld_add_counter* A2LDA(crdt_tag_add_counter* a) {
    if(a->counter_type == VALUE_TYPE_LONGDOUBLE) {
        return (crdt_tag_ld_add_counter*)a;
    }
    crdt_tag_ld_add_counter* lda = create_ld_add_tag(get_tag_gid((crdt_tag*)a));
    dup_tag_ld_add_from_nld_add(lda, a);
    free_crdt_tag((crdt_tag*)a);
    return lda;
}

crdt_tag_base_and_ld_add_counter* A2LDBA(crdt_tag_add_counter* a) {
    crdt_tag_base_and_ld_add_counter* ldba = create_base_ld_add(get_tag_gid((crdt_tag*)a));
    dup_tag_ld_add(ldba, a, crdt_tag_ld_add_counter);    
    free_crdt_tag((crdt_tag*)a);
    return ldba;
}

crdt_tag_base_and_ld_add_del_counter* A2LDBAD(crdt_tag_add_counter* a) {
    crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(get_tag_gid((crdt_tag*)a));
    dup_tag_ld_add(ldbad, a, crdt_tag_ld_add_counter);   
    free_crdt_tag((crdt_tag*)a);
    return ldbad;
}


crdt_tag_base_and_ld_add_del_counter* BAD2LDBAD(crdt_tag_base_and_add_del_counter* bad) {
    if(bad->counter_type == VALUE_TYPE_LONGDOUBLE) return (crdt_tag_base_and_ld_add_del_counter*)bad;
    crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(get_tag_gid((crdt_tag*)bad));
    dup_tag_base(ldbad, bad);
    dup_tag_ld_add_from_nld_add(ldbad, bad);
    dup_tag_ld_add_from_nld_del(ldbad, bad);
    free_crdt_tag((crdt_tag*)bad);
    return ldbad;
}


#define tag_set_ld_ld_add(ldvalue, other) do {\
    if(ldvalue->add_vcu < other->add_vcu) { \
        ldvalue->add_vcu = other->add_vcu;\
        ldvalue->add_counter = other->add_counter;\
    }\
} while(0)

#define tag_set_ld_nld_add(ldvalue, other) do {\
    if(ldvalue->add_vcu < other->add_vcu) { \
        ldvalue->add_vcu = other->add_vcu;\
        long double ld = 0;\
        assert(tag_data_get_ld(other->counter_type, other->add_counter, &ld));\
        ldvalue->add_counter = ld;\
    }\
} while(0)

#define tag_set_ld_ld_del(ldvalue, other) do {\
    if(ldvalue->del_vcu < other->del_vcu) { \
        ldvalue->del_vcu = other->del_vcu;\
        ldvalue->del_counter = other->del_counter;\
    }\
} while(0)

#define tag_set_ld_nld_del(ldvalue, other) do {\
    if(ldvalue->del_vcu < other->del_vcu) { \
        ldvalue->del_vcu = other->del_vcu;\
        long double ld = 0;\
        assert(tag_data_get_ld(other->counter_type, other->del_counter, &ld));\
        ldvalue->del_counter = ld;\
    }\
} while(0)


#define tag_set_ld_add(ldvalue, other, ldclass) do {\
    if(del->counter_type == VALUE_TYPE_LONGDOUBLE) {\
        ldclass* ldother = (ldclass*)other;\
        tag_set_ld_ld_add(ldvalue, ldother);\
    } else {\
        tag_set_ld_nld_add(ldvalue, other);\
    }\
} while(0)

#define tag_set_ld_del(ldvalue, other, ldclass) do {\
    if(del->counter_type == VALUE_TYPE_LONGDOUBLE) {\
        ldclass* ldother = (ldclass*)other;\
        tag_set_ld_ld_del(ldvalue, ldother);\
    } else {\
        tag_set_ld_nld_del(ldvalue, other);\
    }\
} while(0)

crdt_tag* merge_ld_add_tag(crdt_tag* target, crdt_tag_ld_add_counter* other)  {
    crdt_tag_base_and_ld_add_counter* rldba = NULL;
    switch(get_tag_type(target)) {
        case TAG_A: {
            crdt_tag_ld_add_counter* lda = A2LDA((crdt_tag_add_counter*)target);
            tag_set_ld_ld_add(lda, other);
            return (crdt_tag*)lda;
        }
        case TAG_B: {
            rldba = B2LDBA((crdt_tag_base*)target);
            goto callback_base_ld_add;
        }
        case TAG_BA: {
            rldba = BA2LDBA((crdt_tag_base_and_add_counter*)target);
            goto callback_base_ld_add;
        }
        case TAG_BAD: {
            crdt_tag_base_and_ld_add_del_counter* rldbad = BAD2LDBAD((crdt_tag_base_and_add_del_counter*)target);
            tag_set_ld_ld_add(rldbad, other);
            return (crdt_tag*)rldbad;
        }
        default:
            printf("[merge_ld_add_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_ld_add:
    tag_set_ld_ld_add(rldba, other);
    return (crdt_tag*)rldba;
}

crdt_tag* merge_add_tag(crdt_tag* target, crdt_tag_add_counter* other) {
    crdt_tag_base_and_add_counter* rba;
    crdt_tag_base_and_ld_add_counter* rldba;
    switch(get_tag_type(target)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)target;
            if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
                tag_set_add(a, other);
                return (crdt_tag*)a;
            } 
            crdt_tag_ld_add_counter* lda = A2LDA(a);
            tag_set_ld_nld_add(lda, other);
            return (crdt_tag*)lda;
        }
        break;
        case TAG_B: {
            rba = B2BA((crdt_tag_base*)target);
            goto callback_base_add;
        }
        break;
        case TAG_BA: 
            rba = (crdt_tag_base_and_add_counter*)target;
            if(rba->counter_type != VALUE_TYPE_LONGDOUBLE) {
                goto callback_base_add;
            } 
            rldba = BA2LDBA(rba);
            goto callback_base_ld_add;
        break;
        case TAG_BAD:  {
            crdt_tag_base_and_add_del_counter* rbad = (crdt_tag_base_and_add_del_counter*)target;
            if(rbad->counter_type != VALUE_TYPE_LONGDOUBLE ) {
                tag_set_add(rbad, other);
                return (crdt_tag*)rbad;
            } 
            crdt_tag_base_and_ld_add_del_counter* rldbad = BAD2LDBAD(rbad);
            tag_set_ld_nld_add(rldbad, other);
            return (crdt_tag*)rldbad;
        }
        break;
        default:
            printf("[merge_add_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_add:
    tag_set_add(rba, other);
    return (crdt_tag*)rba;
callback_base_ld_add:
    tag_set_ld_nld_add(rldba, other);
    return (crdt_tag*)rldba;
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
//to do
crdt_tag* merge_base_tombstone_tag(crdt_tag* target, crdt_tag_base_tombstone* other) {
    printf("[merge_base_tombstone_tag] Unimplemented code");
    assert(1 == 0);
}
//to do
crdt_tag* merge_base_add_del_tombstone_tag(crdt_tag* target, crdt_tag_base_add_del_tombstone* other) {
    printf("[merge_base_add_del_tombstone_tag] Unimplemented code");
    assert(1 == 0);
}
//to do
crdt_tag* merge_base_ld_add_del_tombstone_tag(crdt_tag* target, crdt_tag_base_ld_add_del_tombstone* other) {
    printf("[merge_base_ld_add_del_tombstone_tag] Unimplemented code");
    assert(1 == 0);
}

crdt_tag* merge_base_ld_add_tag(crdt_tag* target, crdt_tag_base_and_ld_add_counter* other) {
    crdt_tag_base_and_ld_add_counter* ldba;
    switch(get_tag_type(target)) {
        case TAG_A:
            ldba = A2LDBA((crdt_tag_add_counter*)target);
            goto callback_base_ld_add;
        case TAG_B:
            ldba = B2LDBA((crdt_tag_base*)target);
            goto callback_base_ld_add;
        case TAG_BA:
            ldba = BA2LDBA((crdt_tag_base_and_add_counter*)target);
            goto callback_base_ld_add;
        case TAG_BAD: {
            crdt_tag_base_and_ld_add_del_counter* ldbad = BAD2LDBAD((crdt_tag_base_and_add_del_counter*)target);
            tag_set_base(ldbad, other);
            tag_set_ld_ld_add(ldbad, other);
            return (crdt_tag*)ldbad;
        }
        default:
            printf("[merge_base_ld_add_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_ld_add:
    tag_set_ld_ld_add(ldba, other);
    tag_set_base(ldba, other);
    return (crdt_tag*)ldba;
}

crdt_tag* merge_base_add_tag(crdt_tag* target, crdt_tag_base_and_add_counter* other) {
    crdt_tag_base_and_add_counter* ba;
    crdt_tag_base_and_ld_add_counter* ldba;
    switch(get_tag_type(target)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)target;
            if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
                ba = A2BA(a);
                goto callback_base_add;
            } else {
                ldba = A2LDBA(a);
                goto callback_base_ld_add;
            }
        }
        break;
        case TAG_B: 
            ba = B2BA((crdt_tag_base*)target);
            goto callback_base_add;
        break;
        case TAG_BA: {
            ba = (crdt_tag_base_and_add_counter*)target;
            if(ba->counter_type != VALUE_TYPE_LONGDOUBLE) {
                goto callback_base_add;
            } else {
                ldba = BA2LDBA(ba);
                goto callback_base_ld_add;
            }
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)target;
            if(bad->counter_type != VALUE_TYPE_LONGDOUBLE) {
                tag_set_add(bad, other);
                tag_set_base(bad, other);
                return (crdt_tag*)bad;
            } else {
                crdt_tag_base_and_ld_add_del_counter* ldbad = BAD2LDBAD(bad);
                tag_set_base(ldbad, other);
                tag_set_ld_nld_add(ldbad, other);
                return (crdt_tag*)ldbad;
            }
            
        }
        break;
        default:
            printf("[merge_base_add_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_add:
    tag_set_base(ba, other);
    tag_set_add(ba, other);
    return (crdt_tag*)ba;
callback_base_ld_add:
    tag_set_base(ldba, other);
    tag_set_ld_nld_add(ldba, other);
    return (crdt_tag*)ldba;
}

crdt_tag* merge_base_ld_add_del_tag(crdt_tag* target, crdt_tag_base_and_ld_add_del_counter* tag) {
    crdt_tag_base_and_ld_add_del_counter* ldbad;
    switch(get_tag_type(target)) {
        case TAG_A: 
            ldbad = A2LDBAD((crdt_tag_add_counter*)target); 
            goto callback_base_ld_add_del;
        break;
        case TAG_B: 
            ldbad = B2LDBAD((crdt_tag_base*)target);
            goto callback_base_ld_add_del;
        break;
        case TAG_BA: 
            ldbad = BA2LDBAD((crdt_tag_base_and_add_counter*)target);
            goto callback_base_ld_add_del;
        break;
        case TAG_BAD: 
            ldbad = BAD2LDBAD((crdt_tag_base_and_add_del_counter*)target);
            goto callback_base_ld_add_del;
        break;
        default:
            printf("[merge_base_ld_add_del_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_ld_add_del:
    tag_set_base(ldbad, tag);
    tag_set_ld_ld_add(ldbad, tag);
    tag_set_ld_ld_del(ldbad, tag);
    return (crdt_tag*)ldbad;
}

crdt_tag* merge_base_add_del_tag(crdt_tag* target, crdt_tag_base_and_add_del_counter* tag) {
    crdt_tag_base_and_add_del_counter* bad;
    crdt_tag_base_and_ld_add_del_counter* ldbad;
    switch(get_tag_type(target)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)target;
            if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
                bad = A2BAD(a); 
                goto callback_base_add_del;
            } else {
                ldbad = A2LDBAD(a);
                goto callback_base_ld_add_del;
            }
        }
        break;
        case TAG_B: 
            bad = B2BAD((crdt_tag_base*)target);
            goto callback_base_add_del;
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)target;
            if (ba->counter_type != VALUE_TYPE_LONGDOUBLE) {
                bad = BA2BAD(ba);
                goto callback_base_add_del;
            } else {
                ldbad = (crdt_tag_base_and_ld_add_del_counter*)BA2BAD(ba);
                goto callback_base_ld_add_del;
            }
        }
        break;
        case TAG_BAD: {
            bad = (crdt_tag_base_and_add_del_counter*)target;
            if(bad->counter_type != VALUE_TYPE_LONGDOUBLE) {
                goto callback_base_add_del;
            } else {
                ldbad = (crdt_tag_base_and_ld_add_del_counter*)target;
                goto callback_base_ld_add_del;
            }
            
        }
        break;
        default:
            printf("[merge_base_add_del_tag] target type %d is error", get_tag_type(target));
            assert(1 == 0);
        break;
    }
callback_base_add_del:
    tag_set_del(bad, tag);
    tag_set_add(bad, tag);
    tag_set_base(bad, tag);
    return (crdt_tag*)bad;
callback_base_ld_add_del:
    tag_set_base(ldbad, tag);
    tag_set_ld_nld_add(ldbad, tag);
    tag_set_ld_nld_del(ldbad, tag);
    return (crdt_tag*)ldbad;
}

crdt_tag* merge_crdt_tag(crdt_tag* target, crdt_tag* other) {
    switch(get_tag_type(other)) {
        case TAG_A: {
            crdt_tag_add_counter* oa = (crdt_tag_add_counter*)other;
            if(oa->counter_type != VALUE_TYPE_LONGDOUBLE) {
                return merge_add_tag(target, oa);
            } else {
                return merge_ld_add_tag(target, (crdt_tag_ld_add_counter*)other);
            }
        }
        break;
        case TAG_B:
            if(!is_tombstone_tag(other)) {
                return merge_base_tag(target, (crdt_tag_base*)other);
            } else {
                return merge_base_tombstone_tag(target, (crdt_tag_base_tombstone*)other);
            } 
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* oba = (crdt_tag_base_and_add_counter*)other;
            if(oba->counter_type != VALUE_TYPE_LONGDOUBLE) {
                return merge_base_add_tag(target, oba);
            } else {
                return merge_base_ld_add_tag(target, (crdt_tag_base_and_ld_add_counter*)other);
            }
        }
        break;
        case TAG_BAD: {
            if(!is_tombstone_tag(other)) {
                crdt_tag_base_and_add_del_counter* obad = (crdt_tag_base_and_add_del_counter*)other;
                if(obad->counter_type != VALUE_TYPE_LONGDOUBLE) {
                    return merge_base_add_del_tag(target, obad);
                } else {
                    return merge_base_ld_add_del_tag(target, (crdt_tag_base_and_ld_add_del_counter*)other);
                }
            } else {
                crdt_tag_base_add_del_tombstone* otbad = (crdt_tag_base_add_del_tombstone*)other;
                if(otbad->counter_type != VALUE_TYPE_LONGDOUBLE) {
                    return merge_base_add_del_tombstone_tag(target, otbad);
                } else {
                    return merge_base_ld_add_del_tombstone_tag(target, (crdt_tag_base_ld_add_del_tombstone*)otbad);
                }
            }
            
        }
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
            tag->score.s = 0; \
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
    tag->base_data_type = VALUE_TYPE_NONE;\
} while(0)


crdt_tag* clean_crdt_tag_to_tombstone_tag(crdt_tag* tag, long long base_vcu) {
    if(tag == NULL) { return tag; }
    crdt_tag_base_add_del_tombstone* tbad;
    switch (get_tag_type(tag)) {
    case TAG_B: {
        crdt_tag_base_tombstone* b = NULL;;
        if(is_tombstone_tag(tag)) {
            b = (crdt_tag_base_tombstone*)tag;
        } else {
            b = B2TB((crdt_tag_base*)tag);   
        }
        b->base_vcu = base_vcu;
        return (crdt_tag*)b;
    }
        break;
    case TAG_A: 
        tbad = A2TBAD((crdt_tag_add_counter*)tag);
        goto callback_base_add_del;
        break;
    case TAG_BAD:
        if(is_tombstone_tag(tag)) {
            tbad = (crdt_tag_base_add_del_tombstone*)tag;
        } else {
            tbad = BAD2TBAD((crdt_tag_base_and_add_del_counter*)tag);
        }
        goto callback_base_add_del;
        break;
    case TAG_BA:
        tbad = BA2TBAD((crdt_tag_base_and_add_counter*)tag);
        goto callback_base_add_del;
        break;
    default:
        assert(1 == 0);
        break;
    }

callback_base_add_del:
    if(tbad->counter_type != VALUE_TYPE_LONGDOUBLE) {
        tbad->del_counter = tbad->add_counter;
        tbad->del_vcu = tbad->add_vcu;
        return (crdt_tag*)tbad;
    } else {
        crdt_tag_base_ld_add_del_tombstone* tldbad = (crdt_tag_base_ld_add_del_tombstone*)tbad;
        tldbad->del_counter = tldbad->add_counter;
        tldbad->del_vcu = tldbad->add_vcu;
        return (crdt_tag*)tldbad;
    }
}


crdt_tag* clean_crdt_tag(crdt_tag* tag, long long base_vcu) {
    if(tag == NULL) { return tag; }
    crdt_tag_base_and_add_del_counter* bad;
    switch (get_tag_type(tag)) {
    case TAG_B: {
        crdt_tag_base* b = (crdt_tag_base*)tag;
        if(base_vcu != -1) b->base_vcu = base_vcu;
        clean_base(b);
        b->base_timespace = DELETED_TIME;
        return (crdt_tag*)b;
    }
        break;
    case TAG_A: 
        bad = A2BAD((crdt_tag_add_counter*)tag);
        goto callback_base_add_del;
        break;
    case TAG_BAD:
        bad = (crdt_tag_base_and_add_del_counter*)tag;
        goto callback_base_add_del;
        break;
    case TAG_BA:
        bad = BA2BAD((crdt_tag_base_and_add_counter*)tag);
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
    if(bad->counter_type != VALUE_TYPE_LONGDOUBLE) {
        bad->del_counter = bad->add_counter;
        bad->del_vcu = bad->add_vcu;
        return (crdt_tag*)bad;
    } else {
        crdt_tag_base_and_ld_add_del_counter* ldbad = (crdt_tag_base_and_ld_add_del_counter*)bad;
        ldbad->del_counter = ldbad->add_counter;
        ldbad->del_vcu = ldbad->add_vcu;
        return (crdt_tag*)ldbad;
    }
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
    crdt_tag_base_and_add_del_counter* bad = NULL;
    crdt_tag_base_and_add_counter* ba = NULL;
    // meta->data_type;
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
            b->base_vcu = base_vcu;
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
#if defined(TCL_TEST)
    crdt_element dict_get_element(dictEntry* di) {
        return  dictGetVal(di);
    }

    void dict_set_element(dict* d, dictEntry* di, crdt_element el) {
        dictSetVal(d, di, el);
    }

    

    crdt_tag* element_get_tag_by_index(crdt_element el, int index) {
        return el->tags[index];
    }

    crdt_element element_set_tag_by_index(crdt_element el, int index, crdt_tag* tag) {
        el->tags[index] = tag;
        return el;
    }
    int get_element_len(crdt_element el) {
        return el->len;
    }
    crdt_element element_add_tag(crdt_element el, crdt_tag* tag) {
        if(tag == NULL) {return el;}
        if(get_element_len(el) == 0) {
            el->tags = counter_malloc(sizeof(crdt_tag*) * 1);
            el->tags[0] = tag;
        } else {
            el->tags = counter_realloc(el->tags, sizeof(crdt_tag*) * (el->len + 1));
            el->tags[el->len] = tag;
        }
        el->len += 1;
        qsort(el->tags, el->len, sizeof(crdt_tag*), sort_tag_by_gid);
        return el;
    }
    crdt_element create_crdt_element() {
        crdt_element el = counter_malloc(sizeof(TestElement));
        el->tags = NULL;
        el->len = 0;
        return el;
    }
    
    void free_external_crdt_element(crdt_element el) {
        counter_free(el);
    }

    void free_internal_crdt_element_array(crdt_element el) {
        if(el->len > 0) counter_free(el->tags);
        el->len = 0;
    }

    void dict_clean_element(dict* d, dictEntry* di) {
        // crdt_element el = dictGetVal(di);
        // free_internal_crdt_element_array(el);
        // dictGetVal(di) = NULL;
        dictSetVal(d, di, NULL);
    }

    crdt_element move_crdt_element(crdt_element* rc, crdt_element el) {
        (*rc)->len = get_element_len(el);
        (*rc)->tags = el->tags;
        el->len = 0;
        el->tags = NULL;
        return el;
    }
    int reset_crdt_element(crdt_element* rc) {
       (*rc)->len = 0;
       (*rc)->tags = NULL;
       return 1;
    }
    
    
#else
    crdt_element dict_get_element(dictEntry* di) {
        return  *(crdt_element*)&dictGetSignedIntegerVal(di);
    }

    void dict_set_element(dict* d,dictEntry* di, crdt_element el) {
        dictSetSignedIntegerVal(di, *(long long*)&el);
    }

    void dict_clean_element(dict* d, dictEntry* di) {
        dict_set_element(d, di, create_crdt_element());
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

    crdt_element element_set_tag_by_index(crdt_element el, int index, crdt_tag* tag) {
        assert(el.len > index && index >= 0);
        assert(tag != NULL);
        if(el.len == 1) {
            el.tags = tag;
            return el;
        }
        crdt_tag** tags = (crdt_tag**)(el.tags);
        tags[index] = tag;
        return el;
    }

    int get_element_len(crdt_element el) {
        return el.len;
    }

    crdt_element element_add_tag(crdt_element el, crdt_tag* tag) {
        if(tag == NULL) {return el;}
        if(el.len == 0) {
            crdt_element e = {.len = 1, .tags = tag};
            return e;
        } else if(el.len == 1) {
            crdt_tag** tags = counter_malloc(sizeof(crdt_tag*) * 2);
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
    crdt_element create_crdt_element() {
        crdt_element el = {.len = 0};
        return el;
    }
    void free_internal_crdt_element_array(crdt_element el) {
        if(el.len > 1) {
            el.len = 0;
            counter_free(el.tags);
        }
    }
    crdt_element move_crdt_element(crdt_element* rc, crdt_element el) {
        rc->len = get_element_len(el);
        rc->tags = el.tags;
        el.len = 0;
        el.tags = 0;
        return el;
    }

    int reset_crdt_element(crdt_element* rc) {
        rc->len = 0;
        rc->tags = 0;
        return 1;
    }


    void free_external_crdt_element(crdt_element el) {

    }
#endif

void free_internal_crdt_element(crdt_element el) {
    // crdt_element el = *(crdt_element*)&val;
    int len = get_element_len(el);
    if(len == 0) return;
    for(int i = 0 ; i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        free_crdt_tag(tag);
    }
    free_internal_crdt_element_array(el);
}

crdt_tag* element_get_tag_by_gid(crdt_element el, int gid, int* index) {
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            if(index != NULL) *index = i;
            return tag;
        }
    }
    return NULL;
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
    // double base = 0;
    // double counter = 0;
    // long long time = 0;
    // for(int i = 0; i < el.len; i++) {
    //     long long cureen_time = 0;
    //     crdt_tag* tag = element_get_tag_by_index(el, i);
    //     double score = 0;
    //     if(!get_double_base_score(tag, &score, &cureen_time)) {
    //         return 0;
    //     }
    //     if(cureen_time > time) {
    //         base = score;
    //         time = cureen_time;
    //     }
    //     double c = 0;
    //     if(!get_double_counter_score(tag, &c, 1)) {
    //         return 0;
    //     }
    //     counter += c;
    // }
    // if(isnan(base) || isnan(counter)) {
    //     printf("score %.17f %.17f\n", base , counter);
    //     assert(1 == 0);
    // }
    // *score = base + counter;
    // return 1;
    ctrip_value value = {.type = VALUE_TYPE_NONE, .value.i = 0};
    assert(element_get_value(el, &value));
    assert(value.type == VALUE_TYPE_DOUBLE);
    *score = value.value.d;
    return 1;
}

int get_double_add_counter_score_by_element(crdt_element el, int had_del_counter, double* score) {
    double counter = 0;
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        double c = 0;
        if(!get_double_counter_score(tag, &c, had_del_counter)) {
            return 0;
        }
        counter += c;
    }
    if(isnan(counter)) {
        printf("score %.17f is nan\n", counter);
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
            printf("filter type error: type is %lld", tag->type);
            assert( 1 == 0);
        break;
    } 
}

int get_crdt_element_memory(crdt_element el) {
    size_t memory = 0;
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        memory += get_tag_memory(tag);
    }
    return memory;
}
crdt_element dup_crdt_element(crdt_element el) {
    crdt_element rel = create_crdt_element();
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        crdt_tag* rtag = dup_crdt_tag(tag);
        rel = element_add_tag(rel, rtag);
    }
    return rel;
}

sds get_element_info(crdt_element el) {
    sds result = sdsempty();
    for(int i = 0 ,len = get_element_len(el); i < len; i++) {
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
    for (int i = 0, len = get_element_len(*tel); i<len; i++) {
        crdt_tag* ttag = element_get_tag_by_index(*tel, i);
        crdt_tag* tag = element_get_tag_by_gid(*el, ttag->gid, NULL);
        if(tag != NULL) {
            ttag = merge_crdt_tag(ttag, tag);
            if(!is_deleted_tag(ttag)) {
                purge_tag_value = 0;
            }
        }
        rel = element_add_tag(rel, ttag);
    }
    for (int i = 0, len = get_element_len(*el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(*el, i);
        if(element_get_tag_by_gid(rel, tag->gid, NULL)) {
            free_crdt_tag(tag);
            continue;
        }
        purge_tag_value = 0;
        rel = element_add_tag(rel, tag);
    }
    free_internal_crdt_element_array(*el);
    free_internal_crdt_element_array(*tel);
    if(purge_tag_value == 1) {
        // move_crdt_element(el, create_crdt_element());
        reset_crdt_element(el);
        rel = move_crdt_element(tel, rel);
        free_external_crdt_element(rel);
        return PURGE_VAL;
    } else {
        // move_crdt_element(tel, create_crdt_element());
        reset_crdt_element(tel);
        rel = move_crdt_element(el, rel);
        free_external_crdt_element(rel);
        return PURGE_TOMBSTONE;
    }
}

crdt_element element_clean(crdt_element el, int gid, long long vcu, int add_self) {
    int added = 0;
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        if(tag->gid == gid) {
            added = 1;
        }
        tag = clean_crdt_tag(tag, tag->gid == gid? vcu: -1);
        el = element_set_tag_by_index(el, i, tag);
    }
    if(add_self && added == 0) {
        crdt_tag_base* b = create_base_tag(gid);
        b->base_vcu = vcu;
        el = element_add_tag(el, (crdt_tag*)b);
    }
    return el;
}


crdt_element element_try_clean_by_vc(crdt_element el, VectorClock vc, int* is_deleted) {
    crdt_element rel = create_crdt_element();
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        long long c_vcu = get_vcu(vc, tag->gid);
        if(c_vcu == 0) {
            if(is_deleted) *is_deleted = 0;
            rel = element_add_tag(rel, tag);
            continue;
        } 
        tag = try_clean_tag(tag, c_vcu, NULL);
        if(is_deleted != NULL && !is_deleted_tag(tag)) {
            *is_deleted = 0;
        }
        rel = element_add_tag(rel, tag);
    }
    free_internal_crdt_element_array(el);
    for(int i = 0, len = get_len(vc); i < len; i++) {
        clk* c = get_clock_unit_by_index(&vc, i);
        int c_gid = get_gid(*c);
        long long vcu = get_logic_clock(*c);
        if(element_get_tag_by_gid(rel, c_gid, NULL)) {
            continue;
        }
        crdt_tag_base* b = create_base_tag(c_gid);
        b->base_vcu = vcu;
        rel = element_add_tag(rel, (crdt_tag*)b);
    } 
    free_external_crdt_element(el);
    return rel;
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

// crdt_tag* tag_add_counter(crdt_tag* tag, long long vcu, int type, union tag_data* value , int incr) {
//     crdt_tag_base_and_add_counter* ba ;
//     switch (get_tag_type(tag))
//     {
//     case TAG_A: {
//         crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
//         add_counter(a, vcu, type, value, incr);
//         return (crdt_tag*)a;
//     }
//         break;
//     case TAG_B: 
//         ba = B2BA(tag);
//         goto callback_base_add;
//         break;
//     case TAG_BA: 
//         ba = (crdt_tag_base_and_add_counter*)(tag);
//         goto callback_base_add;
//         break;
//     case TAG_BAD: {
//         crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)(tag);
//         add_counter(bad, vcu, type, value, incr);
//         return (crdt_tag*)bad;
//     }
//     break;
//     default:
//         break;
//     }
// callback_base_add:
//     add_counter(ba, vcu, type, value, incr);
//     return (crdt_tag*)ba;
// }

#define nld_plus_nld_add_counter(tag1, value, incr) do {\
    assert(tag1->counter_type != VALUE_TYPE_LONGDOUBLE && value->counter_type != VALUE_TYPE_LONGDOUBLE); \
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
                printf("[nld_plus_nld_add_counter] type Unimplemented code"); \
                break; \
            } \
            tag1-> counter_type = value->counter_type;   \
        } else {\
            printf("[nld_plus_nld_add_counter] Unimplemented code"); \
        }\
        tag1->add_vcu = value->add_vcu;\
    } \
} while(0)

#define ld_plus_ld_add_counter(tag1, other, incr) do {\
    assert(tag1->counter_type == VALUE_TYPE_LONGDOUBLE && other->counter_type == VALUE_TYPE_LONGDOUBLE);\
    if(tag1->add_vcu < other->add_vcu) {\
        tag1->add_vcu = other->add_vcu;\
        if(incr) { \
            tag1->add_counter += other->add_counter; \
        } else { \
            tag1->add_counter = other->add_counter;\
        }\
    }   \
} while(0)

// switch (other->counter_type)\
//     {\
//     case VALUE_TYPE_DOUBLE:\
//         if(incr) { \
//             tag1->add_counter += (long double)other->add_counter.f;\
//         } else {\
//             tag1->add_counter = (long double)other->add_counter.f;\
//         }\
//         break;\
//     case VALUE_TYPE_LONGLONG:\
//         if(incr) { \
//             tag1->add_counter += (long double)other->add_counter.i;\
//         } else {\
//             tag1->add_counter = (long double)other->add_counter.i;\
//         }\
//         break;\
//     case VALUE_TYPE_SDS: {\
//         long double ld = 0;\
//         if(!string2ld(other->add_counter.s, sdslen(other->add_counter.s), &ld)) {\
//             printf("[ld_plus_nld_add_counter] other value:%s", other->add_counter.s);\
//             assert(1 == 0);\
//         }\
//         if(incr) {\
//             tag1->add_counter += ld;\
//         } else {\
//             tag1->add_counter = ld;\
//         }\
//     }\
//         break;\
//     default:\
//         printf("[ld_plus_nld_add_counter] type Unimplemented code"); \
//         assert(1 == 0);\
//         break;\
//     }
#define ld_plus_nld_add_counter(tag1, other, incr) do {\
    assert(tag1->counter_type == VALUE_TYPE_LONGDOUBLE && other->counter_type != VALUE_TYPE_LONGDOUBLE);\
    if(tag1->add_vcu < other->add_vcu) {\
        long double ld = 0;\
        if(!tag_data_get_ld(other->counter_type, other->add_counter, &ld)) {\
            printf("[ld_plus_nld_add_counter]tag_data_get_ld error");\
            assert(1 == 0);\
        }\
        if(incr) {\
            tag1->add_counter += ld;\
        } else {\
            tag1->add_counter = ld;\
        }\
        tag1->add_vcu = other->add_vcu;\
    }\
} while(0)


crdt_tag* tag_add_ld_tag(crdt_tag* tag, crdt_tag_ld_add_counter* other) {
    crdt_tag_base_and_ld_add_counter* ldba = NULL;
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_ld_add_counter* lda = A2LDA((crdt_tag_add_counter*)tag);
            ld_plus_ld_add_counter(lda, other, 1);
            return (crdt_tag*)lda;
        }
        break;
        case TAG_B: {
            ldba = B2LDBA((crdt_tag_base*)tag);
            goto callback_ldba;
        }
        break;
        case TAG_BA: {
            ldba =  BA2LDBA((crdt_tag_base_and_add_counter*)tag);
            goto callback_ldba;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_ld_add_del_counter* ldbad = BAD2LDBAD((crdt_tag_base_and_add_del_counter*)tag);
            ld_plus_ld_add_counter(ldbad, other, 1);
            return (crdt_tag*)ldbad;
        }
        break;
        default: 
        break;
    }
callback_ldba:
    ld_plus_ld_add_counter(ldba, other, 1);
    return (crdt_tag*)ldba;
}
#define ld_plus_add_counter(tag1, tag2, class, change_class_func) do {\
    if(tag1->counter_type == VALUE_TYPE_LONGDOUBLE) {\
        class* ldtag = change_class_func(tag1);\
        ld_plus_nld_add_counter(ldtag, tag2, 1);\
        return (crdt_tag*)ldtag;\
    } else {\
        nld_plus_nld_add_counter(tag1, tag2, 1);\
        return (crdt_tag*)tag1;\
    }\
} while(0)

crdt_tag* tag_add_nld_tag(crdt_tag* tag, crdt_tag_add_counter* other) {
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            ld_plus_add_counter(a, other, crdt_tag_ld_add_counter, A2LDA);
        }
        break;
        case TAG_B: {
            crdt_tag_base_and_add_counter* ba = B2BA((crdt_tag_base*)tag);
            nld_plus_nld_add_counter(ba, other, 1);
            return (crdt_tag*)ba;
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            ld_plus_add_counter(ba, other, crdt_tag_base_and_ld_add_counter, BA2LDBA); 
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            ld_plus_add_counter(bad, other, crdt_tag_base_and_ld_add_del_counter, BAD2LDBAD);
        }
        break;
        default: 
            printf("[tag_add_nld_tag] type error:%d", get_tag_type(tag));
            assert(1 == 0);
        break;
    }
    return NULL;
}


crdt_tag* tag_add_tag(crdt_tag* tag, crdt_tag_add_counter* other) {
    if(other->counter_type == VALUE_TYPE_LONGDOUBLE) {
        return tag_add_ld_tag(tag, (crdt_tag_ld_add_counter*)other);
    } else {
        return tag_add_nld_tag(tag, other);
    }
}

#ifndef COUNTER_BENCHMARK_MAIN

#define load_add(tag, rdb) do { \
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

#define load_ld_add(tag, rdb) do {\
    tag->add_vcu = RedisModule_LoadUnsigned(rdb); \
    long double ld = RedisModule_LoadLongDouble(rdb);\
    tag->add_counter = ld;\
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
        case VALUE_TYPE_NONE: {\
            tag->score.i = 0;\
        }\
        break;\
        default:\
            printf("[load_base] type Unimplemented code"); \
            assert(1 == 0);\
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

#define load_ld_del(tag, rdb) do {\
    tag->del_vcu = RedisModule_LoadUnsigned(rdb); \
    tag->del_counter = RedisModule_LoadLongDouble(rdb);\
} while(0)

crdt_tag* load_crdt_tag_from_rdb(RedisModuleIO *rdb) {
    uint64_t gid = RedisModule_LoadUnsigned(rdb);
    uint64_t type = RedisModule_LoadUnsigned(rdb);
    uint64_t data_type = RedisModule_LoadUnsigned(rdb);
    //future support tombstone tag
    assert(data_type != TOMBSTONE_TAG);
    switch(type) {
        case TAG_A: {
            uint64_t counter_type = RedisModule_LoadUnsigned(rdb); 
            if(counter_type != VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_add_counter* a = create_add_tag(gid);
                a->counter_type = counter_type;
                load_add(a, rdb);
                return (crdt_tag*)a;
            }
            crdt_tag_ld_add_counter* lda = create_ld_add_tag(gid);
            load_ld_add(lda, rdb);
            return (crdt_tag*)lda;
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = create_base_tag(gid);
            load_base(b,rdb);
            return (crdt_tag*)b;
        }
        break;
        case TAG_BA: {
            uint64_t counter_type = RedisModule_LoadUnsigned(rdb); 
            if(counter_type != VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_add_counter* ba = create_base_add_tag(gid);
                ba->counter_type = counter_type;
                load_add(ba, rdb);
                load_base(ba, rdb);
                return (crdt_tag*)ba;
            }
            crdt_tag_base_and_ld_add_counter* ldba = create_base_ld_add(gid);
            load_ld_add(ldba, rdb);
            load_base(ldba, rdb);
            return (crdt_tag*)ldba;
        }
        break;
        case TAG_BAD: {
            uint64_t counter_type = RedisModule_LoadUnsigned(rdb); 
            if(counter_type != VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_add_del_counter* bad = create_base_add_del_tag(gid);
                bad->counter_type = counter_type;
                load_add(bad, rdb);
                load_del(bad, rdb);
                load_base(bad, rdb); 
                return (crdt_tag*)bad;
            }
            crdt_tag_base_and_ld_add_del_counter* ldbad = create_base_ld_add_del(gid);
            load_ld_add(ldbad, rdb);
            load_ld_del(ldbad, rdb);
            load_base(ldbad, rdb);
            return (crdt_tag*)ldbad;
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
        el = element_add_tag(el, tag);
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

#define save_ld_add(tag, rdb) do { \
    RedisModule_SaveUnsigned(rdb, tag->counter_type); \
    RedisModule_SaveUnsigned(rdb, tag->add_vcu);\
    RedisModule_SaveLongDouble(rdb, tag->add_counter);\
} while(0)

#define save_ld_del(tag, rdb) do { \
    RedisModule_SaveUnsigned(rdb, tag->del_vcu);\
    RedisModule_SaveLongDouble(rdb, tag->del_counter);\
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
        if(a->counter_type != VALUE_TYPE_LONGDOUBLE) {
            save_add(a, rdb);
        } else {
            crdt_tag_ld_add_counter* lda = (crdt_tag_ld_add_counter*)tag;
            save_ld_add(lda, rdb);
        }
        break;
    }
    case TAG_B: {
        crdt_tag_base* b = (crdt_tag_base*)tag;
        save_base(b, rdb);
    }
        break;
    case TAG_BA: {
        crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
        if(ba->counter_type != VALUE_TYPE_LONGDOUBLE) {
            save_add(ba, rdb);
            save_base(ba, rdb);
            
        } else {
            crdt_tag_base_and_ld_add_counter* ldba = (crdt_tag_base_and_ld_add_counter*)tag;
            save_ld_add(ldba, rdb);
            save_base(ldba, rdb);
            
        }
        
    }
        break;
    case TAG_BAD: {
        crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
        if(bad->counter_type != VALUE_TYPE_LONGDOUBLE) {
            save_add(bad, rdb);
            save_del(bad, rdb);
            save_base(bad, rdb);
        } else {
            crdt_tag_base_and_ld_add_del_counter* ldbad = (crdt_tag_base_and_ld_add_del_counter*)tag;
            save_ld_add(ldbad, rdb);
            save_ld_del(ldbad, rdb);
            save_base(ldbad, rdb);
        }
        
    }
        break;
    default:
        printf("save tag to rdb error\n");
        assert(1 == 0);
        break;
    }
}
void save_crdt_element_to_rdb(RedisModuleIO *rdb, crdt_element el) {
    int len = get_element_len(el);
    RedisModule_SaveUnsigned(rdb, len);
    for(int i = 0; i < len; i++) {
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
            value->data_type = bad->counter_type;
            switch(bad->counter_type) {
                case VALUE_TYPE_NONE:
                return 0;
                case VALUE_TYPE_DOUBLE:
                    value->vcu = bad->del_vcu;
                    gcounter_meta_set_value(value, VALUE_TYPE_DOUBLE, &bad->del_counter.f, 1);
                break; 
                case VALUE_TYPE_LONGLONG:
                    value->vcu = bad->del_vcu;
                    gcounter_meta_set_value(value, VALUE_TYPE_LONGLONG, &bad->del_counter.i, 1);
                break;
                case VALUE_TYPE_SDS:
                    value->vcu = bad->del_vcu;
                    gcounter_meta_set_value(value, VALUE_TYPE_SDS, &bad->del_counter.s, 1);
                break;
                case VALUE_TYPE_LONGDOUBLE: {
                    crdt_tag_base_and_ld_add_del_counter *ldbad = (crdt_tag_base_and_ld_add_del_counter*)bad;
                    value->vcu = ldbad->del_vcu;
                    gcounter_meta_set_value(value, VALUE_TYPE_LONGDOUBLE, &ldbad->del_counter, 1);
                }
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
    int max_len = 21 + 1 + 256;
    int len = 0;
    crdt_tag* tag = element_get_tag_by_gid(el, gid, NULL);
    assert(tag != NULL);
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
    if(data_type == VALUE_TYPE_SDS) {
        max_len += sdslen(v.s);
    }
    char buf[max_len];
    len += value_to_str(buf + len, data_type, v);
    sds dr = get_delete_counter_sds_from_element(el);
    if(dr != NULL) {
        buf[len++] = ',';
        memcpy(buf + len , dr, sdslen(dr));
        len += sdslen(dr);
        sdsfree(dr);
    }
    return sdsnewlen(buf, len);
}

#define get_all_type_from_add(tag, t, v, class) do {\
    t = tag->counter_type;\
    if(tag->counter_type != VALUE_TYPE_LONGDOUBLE) {\
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
    } else {\
        class* lda = (class*)tag;\
        data_type = VALUE_TYPE_LONGDOUBLE;\
        v.f = lda->add_counter;\
    }\
} while(0)

sds get_add_value_sds_from_tag(crdt_tag* tag) {
    union all_type v = {.f = 0};
    int data_type;
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            get_all_type_from_add(a, data_type, v, crdt_tag_ld_add_counter);
        }
        break;
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_all_type_from_add(ba, data_type, v, crdt_tag_base_and_ld_add_counter);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            get_all_type_from_add(bad, data_type, v, crdt_tag_base_and_ld_add_del_counter);
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
    int elmenet_len = get_element_len(el);
    int max_len = elmenet_len * (21 + 5 + 17);
    char buf[max_len];
    int len =  g_counter_metas_to_str(buf, &el, tag_to_g_counter_meta, elmenet_len);
    if(len == 0) return NULL;
    return sdsnewlen(buf, len);
}

crdt_element merge_crdt_element(crdt_element a, crdt_element b) {
    for(int i = 0, len = get_element_len(b); i < len; i++) {
        crdt_tag* btag = element_get_tag_by_index(b, i);
        int index = 0;
        crdt_tag* atag = element_get_tag_by_gid(a, get_tag_gid(btag), &index);
        if(atag) {
            atag = merge_crdt_tag(atag, btag);
            free_crdt_tag(btag);
            a = element_set_tag_by_index(a, index, atag);
        } else {
            a = element_add_tag(a, btag);
        }
    }
    free_internal_crdt_element_array(b);
    return a;
}

crdt_element element_merge_tag(crdt_element el, void* v) {
    crdt_tag* tag = (crdt_tag*)v;
    int gid = get_tag_gid(tag);
    int index = 0;
    crdt_tag* ntag = element_get_tag_by_gid(el, gid, &index);
    if(ntag) {
        ntag = merge_crdt_tag(ntag, tag);
        free_crdt_tag(tag);
        el = element_set_tag_by_index(el, index, ntag);
    } else {
        el = element_add_tag(el, tag);
    }
    return el;
}

long long get_tag_vcu(crdt_tag* tag) {
    switch(get_tag_type(tag)) {
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            return a->add_vcu;
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            return b->base_vcu;
        }
        break; 
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            return max(ba->base_vcu, ba->add_vcu);
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            return max(bad->base_vcu, bad->add_vcu);
        }
        break;
        default:
            printf("[element_get_vcu_by_gid] type %d is error\n", get_tag_type(tag) );
            assert(1 == 0);
        break;
    }
}
long long element_get_vcu_by_gid(crdt_element el, int gid) {
    crdt_tag* tag = element_get_tag_by_gid(el, gid, NULL);
    if(tag == NULL) return 0;
    return get_tag_vcu(tag);
}

// void bad_merge_g_meta(crdt_tag_base_and_add_del_counter* bad, g_counter_meta* meta) {
    
// }
crdt_tag* base_merge_g_meta(crdt_tag_base* b, g_counter_meta* meta) {
    if(!meta) return (crdt_tag*)b;
    if(meta->data_type != VALUE_TYPE_LONGDOUBLE) {
        crdt_tag_base_and_add_del_counter* bad = b? B2BAD(b): (crdt_tag_base_and_add_del_counter*)create_base_add_del_tag(meta->gid);
        bad->counter_type = meta->data_type;
        bad->add_vcu = meta->vcu;
        copy_tag_data_from_all_type(meta->data_type, &bad->add_counter , meta->value);
        bad->del_vcu = meta->vcu;
        copy_tag_data_from_all_type(meta->data_type, &bad->del_counter , meta->value);
        return (crdt_tag*)bad;
    } else {
        crdt_tag_base_and_ld_add_del_counter* ldbad = b? B2LDBAD(b): create_base_ld_add_del(meta->gid);
        ldbad->add_vcu = meta->vcu;
        ldbad->add_counter = meta->value.f;
        ldbad->del_vcu = meta->vcu;
        ldbad->del_counter = meta->value.f;
        return (crdt_tag*)ldbad;
    }
}

int find_meta_by_gid(int gid, int gcounter_len, g_counter_meta** metas) {
    for(int i = 0; i < gcounter_len; i++) {
        g_counter_meta* meta = metas[i];
        if(meta != NULL && meta->gid == gid) {
            return i;
        }
    }
    return -1;
}


crdt_element create_element_from_vc_and_g_counter(VectorClock vc, int gcounter_len, g_counter_meta** metas, crdt_tag* base_tag) {
    crdt_element el =  create_crdt_element();
    int added = 0;
    assert(gcounter_len <= get_len(vc));
    for(int i = 0, len = get_len(vc); i < len; i++) {
        clk* c = get_clock_unit_by_index(&vc, i);
        int gid = get_gid(*c);
        int vcu = get_logic_clock(*c);
        int g_index =  find_meta_by_gid(gid, gcounter_len, metas);
        crdt_tag* tag = NULL;
        if(base_tag && base_tag->gid == gid) {
            tag = base_tag;
            added = 1;
        }else  {
            crdt_tag_base* b =  create_base_tag(gid);
            b->base_vcu = vcu;
            tag = (crdt_tag*)b;
        } 
        if(g_index != -1) {
            tag = base_merge_g_meta((crdt_tag_base*)tag, metas[g_index]);
            free_g_counater_maeta(metas[g_index]);
            metas[g_index] = NULL;
        }
        el = element_add_tag(el, tag);
    }
    for(int i = 0; i < gcounter_len; i++) {
        assert(metas[i] == NULL);
    }
    return el;
}


sds element_add_counter_by_tag(crdt_element* el,  crdt_tag_add_counter* rtag) {
    int gid = get_tag_gid((crdt_tag*)rtag);
    int index = 0;
    crdt_tag* tag = element_get_tag_by_gid(*el, gid, &index);
    if(tag) {
        tag = tag_add_tag(tag, rtag);
        *el = element_set_tag_by_index(*el, index, tag);
        free_crdt_tag((crdt_tag*)rtag);
        return get_add_value_sds_from_tag(tag);
    } else {
        *el = element_add_tag(*el, (crdt_tag*)rtag);
        return get_add_value_sds_from_tag((crdt_tag*)rtag);
    }
}

int d2ll(double d, long long* ll) {
    return 0;
}

int get_element_type(crdt_element el) {
    double value = 0;
    if(!get_double_score_by_element(el, &value)) {
        return VALUE_TYPE_SDS;
    }
    if(d2ll(value, NULL)) {
        return VALUE_TYPE_LONGLONG;
    } 
    return VALUE_TYPE_DOUBLE;
}


int plus_or_minus_ll_ll(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_LONGLONG);
    assert(b->type == VALUE_TYPE_LONGLONG);
    long long old_value = a->value.i;
    long long incr = b->value.i;
    if ((incr < 0 && old_value < 0 && incr < (LLONG_MIN-old_value)) ||
        (incr > 0 && old_value > 0 && incr > (LLONG_MAX-old_value))) {
        return PLUS_ERROR_OVERFLOW;
    }

    if(plus) {
        a->value.i += b->value.i;
    } else {
        a->value.i -= b->value.i;
    }
    return 1;
}

int plus_or_minus_ll_ld(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_LONGLONG);
    assert(b->type == VALUE_TYPE_LONGDOUBLE);
    long double ld = 0;
    if(plus) {
        ld = a->value.i + b->value.f;
    } else {
        ld = a->value.i - b->value.f;
    }
    if (isnan(ld) || isinf(ld)) {
        return PLUS_ERROR_NAN;
    }
    a->value.f = ld;
    a->type = VALUE_TYPE_LONGDOUBLE;
    return 1;
}

int plus_or_minus_ll_d(ctrip_value* a, ctrip_value* b, int plus){
    assert(a->type == VALUE_TYPE_LONGLONG);
    assert(b->type == VALUE_TYPE_DOUBLE);
    a->type = VALUE_TYPE_DOUBLE;
    if(plus) {
        a->value.d += a->value.i + b->value.d;
    } else {
        a->value.d = a->value.i - b->value.d;
    }
    return 1;
}

int plus_or_minus_d_ll(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_DOUBLE);
    assert(b->type == VALUE_TYPE_LONGLONG);
    a->type = VALUE_TYPE_DOUBLE;
    if(plus) {
        a->value.d  += b->value.i;
    } else {
        a->value.d  -= b->value.i;
    }
    return 1;
}

int plus_or_minus_d_ld(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_DOUBLE);
    assert(b->type == VALUE_TYPE_LONGDOUBLE);
    a->type = VALUE_TYPE_LONGDOUBLE;
    if(plus) {
        a->value.f  = (long double)a->value.d + b->value.f;
    } else {
        a->value.f =  (long double)a->value.d - b->value.f;
    }
    return 1;
}

int plus_or_minus_d_d(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_DOUBLE);
    assert(b->type == VALUE_TYPE_DOUBLE);
    a->type = VALUE_TYPE_DOUBLE;
    if(plus) {
        a->value.d  += b->value.d;
    } else {
        a->value.d  -= b->value.d;
    }
    return 1;
}

int plus_or_minus_d(ctrip_value* a, ctrip_value* b, int plus) {
    switch (b->type)
    {
    case VALUE_TYPE_LONGLONG:
        return plus_or_minus_d_ll(a, b, plus);
        break;
    case VALUE_TYPE_LONGDOUBLE:
        return plus_or_minus_d_ld(a, b, plus);
    case VALUE_TYPE_SDS:
        if(sds_change_value(VALUE_TYPE_LONGLONG, &b->value)) {
            b->type = VALUE_TYPE_LONGLONG;
            return plus_or_minus_d_ll(a, b, plus);
        }
        if(sds_change_value(VALUE_TYPE_LONGDOUBLE, &b->value)) {
            b->type = VALUE_TYPE_LONGDOUBLE;
            return plus_or_minus_d_ld(a, b, plus);
        }
        goto error;
        break;
    case VALUE_TYPE_DOUBLE:
        return plus_or_minus_d_d(a, b, plus);
        break;
    case VALUE_TYPE_NONE:
        return 1;
    default:
        goto error;
        break;
    }
error:
    printf("[plus_ll]type error [%lld + %lld]\n", a->type, b->type);
    assert(1 == 0);
    return 0;
}

int plus_or_minus_ll(ctrip_value* a, ctrip_value* b, int plus) {
    switch (b->type)
    {
    case VALUE_TYPE_LONGLONG:
        return plus_or_minus_ll_ll(a, b, plus);
        break;
    case VALUE_TYPE_LONGDOUBLE:
        return plus_or_minus_ll_ld(a, b, plus);
    case VALUE_TYPE_SDS:
        if(sds_change_value(VALUE_TYPE_LONGLONG, &b->value)) {
            b->type = VALUE_TYPE_LONGLONG;
            return plus_or_minus_ll_ll(a, b, plus);
        }
        if(sds_change_value(VALUE_TYPE_LONGDOUBLE, &b->value)) {
            b->type = VALUE_TYPE_LONGDOUBLE;
            return plus_or_minus_ll_ld(a, b, plus);
        }
        goto error;
        break;
    case VALUE_TYPE_DOUBLE:
        return plus_or_minus_ll_d(a, b, plus);
        break;
    default:
        goto error;
        break;
    }
error:
    printf("[plus_ll]type error [%lld + %lld]\n", a->type, b->type);
    assert(1 == 0);
    return 0;
}

int plus_or_minus_ld_ll(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_LONGDOUBLE);
    assert(b->type == VALUE_TYPE_LONGLONG);
    long double ld;
    if(plus) {
        ld = a->value.f + b->value.i;
    } else {
        ld = a->value.f - b->value.i;
    }
    if (isnan(ld) || isinf(ld)) {
        return PLUS_ERROR_NAN;
    }
    a->value.f = ld;
    return 1;
}

int plus_or_minus_ld_ld(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_LONGDOUBLE);
    assert(b->type == VALUE_TYPE_LONGDOUBLE);
    // long double ld = a->value.f;
    // if(plus) {
    //     ld += b->value.f;
    // } else {
    //     ld -= b->value.f;
    // }
    long double ld = 0;
    if(plus) {
        ld = a->value.f + b->value.f;
    } else {
        ld = a->value.f - b->value.f;
    }
    if (isnan(ld) || isinf(ld)) {
        return PLUS_ERROR_NAN;
    }
    a->value.f = ld;
    return 1;
}

int plus_or_minus_ld_d(ctrip_value* a, ctrip_value* b, int plus) {
    assert(a->type == VALUE_TYPE_LONGDOUBLE);
    assert(b->type == VALUE_TYPE_DOUBLE);
    a->type = VALUE_TYPE_LONGDOUBLE;
    if(plus) {
        a->value.f += (long double)b->value.d;
    } else {
        a->value.f -= (long double)b->value.d;
    }
    return 1;
}

int plus_or_minus_ld(ctrip_value* a, ctrip_value* b, int plus) {
    switch (b->type)
    {
    case VALUE_TYPE_LONGLONG:
        return plus_or_minus_ld_ll(a, b, plus);
        break;
    case VALUE_TYPE_LONGDOUBLE:
        return plus_or_minus_ld_ld(a, b, plus);
    case VALUE_TYPE_SDS:
        if(sds_change_value(VALUE_TYPE_LONGLONG, &b->value)) {
            b->type = VALUE_TYPE_LONGLONG;
            return plus_or_minus_ld_ll(a, b, plus);
        }
        if(sds_change_value(VALUE_TYPE_LONGDOUBLE, &b->value)) {
            b->type = VALUE_TYPE_LONGDOUBLE;
            return plus_or_minus_ld_ld(a, b, plus);
        }
        goto error;
    case VALUE_TYPE_DOUBLE:
        return plus_or_minus_ld_d(a, b, plus);
    break;
    break;
    default:
        goto error;

    }
error:
    printf("[plus_ld]type error [%lld + %lld]\n", a->type, b->type);
    assert(1 == 0);
    return 0;
}

int plus_or_minus_null(ctrip_value* a, ctrip_value* b, int plus) {
    switch (b->type)
    {
    case VALUE_TYPE_SDS: {
        if(sds_change_value(VALUE_TYPE_LONGLONG, &b->value)) {
            b->type = VALUE_TYPE_LONGLONG;
            a->type = VALUE_TYPE_LONGLONG;
            if(plus) {
                a->value.i = b->value.i;
            } else {
                a->value.i = -b->value.i;
            }
            return 1;
        }
        if(sds_change_value(VALUE_TYPE_LONGDOUBLE, &a->value)) {
            b->type = VALUE_TYPE_LONGDOUBLE;
            a->type = VALUE_TYPE_LONGDOUBLE;
            if(plus) {
                a->value.f = b->value.f;
            } else {
                a->value.f = -b->value.f;
            }
            return 1;
        }
    }
    break;
    case VALUE_TYPE_LONGLONG: {
        a->type = VALUE_TYPE_LONGLONG;
        if(plus) {
            a->value.i = b->value.i;
        } else {
            a->value.i = -b->value.i;
        }
        return 1;
    }
    break;
    case VALUE_TYPE_LONGDOUBLE: {
        a->type = VALUE_TYPE_LONGDOUBLE;
        if(plus) {
            a->value.f = b->value.f;
        } else {
            a->value.f = -b->value.f;
        }
        return 1;
    }
    case VALUE_TYPE_NONE:
        return 1;
        break;
    case VALUE_TYPE_DOUBLE: {
        a->type = VALUE_TYPE_DOUBLE;
        if(plus) {
            a->value.d = b->value.d;
        } else {
            a->value.d = -b->value.d;
        }
    }
    return 1;
    default:
        break;
    }
    return 0;
}


int plus_or_minus_ctrip_value(ctrip_value* a, ctrip_value* b, int plus) {
    switch(a->type) {
        case VALUE_TYPE_SDS: {
            if(sds_change_value(VALUE_TYPE_LONGLONG, &a->value)) {
                a->type = VALUE_TYPE_LONGLONG;
                return plus_or_minus_ll(a, b, plus);
            }
            if(sds_change_value(VALUE_TYPE_LONGDOUBLE, &a->value)) {
                a->type = VALUE_TYPE_LONGDOUBLE;
                return plus_or_minus_ld(a, b, plus);
            }
        }
        break;
        case VALUE_TYPE_LONGLONG: {
            return plus_or_minus_ll(a, b, plus);
        }
        break;
        case VALUE_TYPE_DOUBLE: {
            return plus_or_minus_d(a, b, plus);
        }   
        break;
        case VALUE_TYPE_LONGDOUBLE: {
            return plus_or_minus_ld(a, b, plus);
        }
        break;
        case VALUE_TYPE_NONE: {
            return plus_or_minus_null(a, b, plus);
        }
        default:
        goto error;
        break;
        
    }
error:
    printf("[plus_or_minus_ctrip_value]type error [%lld + %lld]\n", a->type, b->type);
    assert(1 == 0);
    return 0;
}

#define get_base_value(tag1, value, time) do {\
    switch (tag1->base_data_type) { \
    case VALUE_TYPE_SDS: { \
        value->type = VALUE_TYPE_SDS;\
        value->value.s = tag1->score.s;\
    } \
        break; \
    case VALUE_TYPE_DOUBLE: \
        value->type = VALUE_TYPE_DOUBLE;\
        value->value.d = tag1->score.f;\
    break; \
    case VALUE_TYPE_LONGLONG: \
        value->type = VALUE_TYPE_LONGLONG;\
        value->value.i = tag1->score.i;\
    break; \
    } \
    *time = tag1->base_timespace; \
    return 1; \
} while(0);

int get_tag_base_value(crdt_tag* tag, long long* time, ctrip_value* value) {
    switch(get_tag_type(tag)) {
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            get_base_value(ba, value, time);
            return 1;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            get_base_value(bad, value, time);
            return 1;
        }
        break;
        case TAG_B: {
            crdt_tag_base* b = (crdt_tag_base*)tag;
            get_base_value(b, value, time);
            return 1;
        }
        break;
        case TAG_A: 
            *time = 0;
            value->type = VALUE_TYPE_NONE;
            return 1;
        break;
        default:
            printf("[get_double_base_score] target type %d is error: %p", get_tag_type(tag), tag);
            assert(1 == 0);
            return 0;
        break;
    }
}

#define get_add_counter(tag1, v) do { \
    v->type = tag1->counter_type;\
    switch (tag1->counter_type) { \
        case VALUE_TYPE_SDS: { \
            v->value.s = tag1->add_counter.s;\
            break; \
        } \
        case VALUE_TYPE_DOUBLE: \
            v->value.d = tag1->add_counter.f; \
        break; \
        case VALUE_TYPE_LONGLONG: \
            v->value.i = tag1->add_counter.i; \
        break; \
        default: \
            printf("[get_add_counter] target type %d , value: %d, is error", get_tag_type((crdt_tag*)tag1), tag1->counter_type);\
            assert(1 == 0);\
        break; \
    } \
} while(0)


#define get_del_counter(tag1, v) do { \
    v->type = tag1->counter_type;\
    switch (tag1->counter_type) { \
        case VALUE_TYPE_SDS: { \
            v->value.s = tag1->del_counter.s;\
            break; \
        } \
        case VALUE_TYPE_DOUBLE: \
            v->value.d = tag1->del_counter.f; \
        break; \
        case VALUE_TYPE_LONGLONG: \
            v->value.i = tag1->del_counter.i; \
        break; \
    } \
} while(0)

#define get_ld_add_counter(tag, v) do {\
    v->type = VALUE_TYPE_LONGDOUBLE;\
    v->value.f = tag->add_counter;\
} while(0)

#define get_ld_del_counter(tag, v) do {\
    v->type = VALUE_TYPE_LONGDOUBLE;\
    v->value.f = tag->del_counter;\
} while(0)

int get_tag_counter_value(crdt_tag* tag, ctrip_value* value, int had_del_counter) {
    switch(get_tag_type(tag)) {
        case TAG_BA: {
            crdt_tag_base_and_add_counter* ba = (crdt_tag_base_and_add_counter*)tag;
            if(ba->counter_type == VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_ld_add_counter* ldba = (crdt_tag_base_and_ld_add_counter*)tag;
                get_ld_add_counter(ldba, value);
            } else {
                get_add_counter(ba, value);
            }
            return 1;
        }
        break;
        case TAG_BAD: {
            crdt_tag_base_and_add_del_counter* bad = (crdt_tag_base_and_add_del_counter*)tag;
            ctrip_value av = {.type = VALUE_TYPE_NONE};
            ctrip_value dv = {.type = VALUE_TYPE_NONE};
            ctrip_value* a = &av;
            ctrip_value* d = &dv;
            if(bad->counter_type == VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_base_and_ld_add_del_counter* ldbad = (crdt_tag_base_and_ld_add_del_counter*)tag;
                get_ld_add_counter(ldbad, a);
                if(had_del_counter) {
                    get_ld_del_counter(ldbad, d);
                    plus_or_minus_ctrip_value(a, d, 0);
                }
            } else {
                get_add_counter(bad, a);
                if(had_del_counter) {
                    get_del_counter(bad, d);
                    plus_or_minus_ctrip_value(a, d, 0);
                } 
            }
            value->type = a->type;
            value->value = a->value;
            
            return 1;
        }
        break;
        case TAG_B:
            value->type = VALUE_TYPE_NONE;
            return 1;
        break;
        case TAG_A: {
            crdt_tag_add_counter* a = (crdt_tag_add_counter*)tag;
            if(a->counter_type == VALUE_TYPE_LONGDOUBLE) {
                crdt_tag_ld_add_counter* lda = (crdt_tag_ld_add_counter*)tag;
                get_ld_add_counter(lda, value);
            } else {
                get_add_counter(a, value);
            }
            
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

sds get_field_and_delete_counter_str(sds field, crdt_element del_el, int must_field) {
    sds meta_info = get_delete_counter_sds_from_element(del_el);
    union all_type field_value = {.s = field};
    if(meta_info != NULL) {
        sds field_info = sdscatprintf(value_to_sds(VALUE_TYPE_SDS, field_value), ",%s", meta_info);
        sdsfree(meta_info);
        return field_info;
    } else if(must_field) {
        return value_to_sds(VALUE_TYPE_SDS, field_value);
    }
    return NULL;
}

int element_get_value(crdt_element el, ctrip_value* value) {
    ctrip_value base_value = {.type = VALUE_TYPE_NONE};
    ctrip_value counter_value = {.type = VALUE_TYPE_NONE};
    long long base_time = 0;
    for(int i = 0, len = get_element_len(el); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(el, i);
        ctrip_value current_value = {.type = VALUE_TYPE_NONE};
        long long current_time = 0;
        get_tag_base_value(tag, &current_time, &current_value);
        if(current_time > base_time) {
            base_value.type = current_value.type;
            base_value.value = current_value.value;
            base_time = current_time;
        }
        get_tag_counter_value(tag, &current_value, 1);
        if(current_value.type != VALUE_TYPE_NONE) {
            assert(plus_or_minus_ctrip_value(&counter_value, &current_value,1));
        }
        
    }
    if(base_value.type == VALUE_TYPE_SDS && counter_value.type == VALUE_TYPE_NONE) {
        value->type = base_value.type;
        value->value = base_value.value;
        return 1;
    }
    plus_or_minus_ctrip_value(&base_value, &counter_value, 1);
    value->type = base_value.type;
    value->value = base_value.value;
    return 1;
}

VectorClock element_get_vc(crdt_element r) {
    VectorClock vc = newVectorClock(0);
    for(int i = 0, len = get_element_len(r); i < len; i++) {
        crdt_tag* tag = element_get_tag_by_index(r, i);
        vc = addVectorClockUnit(vc, get_tag_gid(tag), get_tag_vcu(tag));
    }
    return vc;
}

crdt_tag_base* create_base_tag_by_meta(CrdtMeta* meta, ctrip_value v) {
    crdt_tag_base* b = create_base_tag(getMetaGid(meta));
    b->base_timespace = meta->timestamp;
    b->base_vcu = get_vcu_by_meta(meta);
    b->base_data_type = v.type;
    copy_tag_data_from_all_type(v.type, &b->score, v.value);
    assert(get_tag_gid((crdt_tag*)b) == getMetaGid(meta));
    return b;
}
