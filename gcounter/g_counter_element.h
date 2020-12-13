
#include "../ctrip_vector_clock.h"
#include "../ctrip_crdt_common.h"
#include "../include/rmutil/dict.h"
#include "g_counter.h"

#ifdef COUNTER_BENCHMARK_MAIN
//main
#define counter_malloc(size) zmalloc(size)
#define counter_calloc(count) zcalloc(count, 1)
#define counter_realloc(ptr, size) zrealloc(ptr, size)
#define counter_free(ptr) zfree(ptr)
#else
//build module so
#include "../include/redismodule.h"
#define counter_malloc(size) RedisModule_Alloc(size)
#define counter_calloc(count) RedisModule_Calloc(count, 1)
#define counter_realloc(ptr, size) RedisModule_Realloc(ptr, size)
#define counter_free(ptr) RedisModule_Free(ptr)
#endif


typedef struct crdt_tag {
    unsigned long long data_type: 1; //0 value 1 tombstone
    unsigned long long type:3; //1 base 2  addcounter 4  delcounter 
    unsigned long long gid: 4;
    unsigned long long ops: 56;
} __attribute__ ((packed, aligned(1))) crdt_tag;

#if defined(TCL_TEST)
    typedef struct TestElement {
        char type;
        long long len; 
        crdt_tag** tags; //crdt_tag** 
    } __attribute__ ((packed, aligned(1))) TestElement; 
    typedef TestElement* crdt_element;
    
#else
    typedef struct {
        unsigned long long opts:8;
        unsigned long long len: 4; 
        unsigned long long tags:52; //crdt_tag** 
    } __attribute__ ((packed, aligned(1))) crdt_element;
    
#endif



//base 
typedef struct  { //24
    unsigned long long tag_head:8;
    unsigned long long base_data_type:4; //base sds 1 long long 2 double 3
    unsigned long long base_vcu:52;
    long long base_timespace;
    union tag_data score;
} __attribute__ ((packed, aligned(1))) crdt_tag_base;



//add_counter 
typedef struct   { //16
    unsigned long long tag_head:8;
    unsigned long long counter_type:4; //base sds 1 long long 2 double 3
    unsigned long long add_vcu: 52;
    union tag_data add_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_add_counter;

//add_counter 
typedef struct   { //16
    unsigned long long tag_head:8;
    unsigned long long counter_type:4; //base sds 1 long long 2 double 3 long double 4
    unsigned long long add_vcu: 52;
    long double add_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_ld_add_counter;


//base + add_counter 
typedef struct   { //40
    unsigned long long tag_head:8;
    unsigned long long base_data_type:4;
    unsigned long long base_vcu:52;
    long long base_timespace;
    union tag_data score;
    unsigned long long counter_type:4;
    unsigned long long add_vcu:60;
    union tag_data add_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_and_add_counter;

//base + ld + add_counter 
typedef struct   { //40
    unsigned long long tag_head:8;
    unsigned long long base_data_type:4;
    unsigned long long base_vcu:52;
    long long base_timespace;
    union tag_data score;
    unsigned long long counter_type:4;
    unsigned long long add_vcu:60;
    long double add_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_and_ld_add_counter;

//base + add_counter + del_dounter
typedef struct   { //56
    unsigned long long tag_head:8;
    unsigned long long base_data_type:4;
    unsigned long long base_vcu: 52;
    long long base_timespace;
    union tag_data score;
    unsigned long long counter_type:4;
    unsigned long long add_vcu:60;
    union tag_data add_counter;
    unsigned long long del_vcu;
    union tag_data del_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_and_add_del_counter;

//base + ld + add_counter + del_dounter
typedef struct   { //72
    unsigned long long tag_head:8;
    unsigned long long base_data_type:4;
    unsigned long long base_vcu: 52;
    long long base_timespace;
    union tag_data score;
    unsigned long long counter_type:4;
    unsigned long long add_vcu:60;
    long double add_counter;
    unsigned long long del_vcu;
    long double del_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_and_ld_add_del_counter;

//base tombstone 
typedef struct  { //8
    unsigned long long tag_head: 8;
    unsigned long long base_vcu: 56;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_tombstone;


//base + add + del tombstone
typedef struct  { //40
    unsigned long long tag_head: 8;
    unsigned long long base_vcu: 52;
    unsigned long long counter_type:4;
    unsigned long long add_vcu;
    union tag_data add_counter;
    unsigned long long del_vcu;
    union tag_data del_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_add_del_tombstone;

typedef struct  { //56
    unsigned long long tag_head: 8;
    unsigned long long base_vcu: 52;
    unsigned long long counter_type:4;
    unsigned long long add_vcu;
    long double add_counter;
    unsigned long long del_vcu;
    long double del_counter;
} __attribute__ ((packed, aligned(1))) crdt_tag_base_ld_add_del_tombstone;



/**********************  abdou  tag +***********************/

//static
//about data type
#define VALUE_TAG 0
#define TOMBSTONE_TAG 1


//about type
#define TAG_B 1
#define TAG_A 2
#define TAG_D 4
#define TAG_BA 3
#define TAG_BAD 7





#define DELETED_TIME -1
//private 
void set_tag_data_type(crdt_tag* tag, int type); //about value or tombstone
void set_tag_type(crdt_tag* tag, int type); //about base or base + add conter 
void set_tag_gid(crdt_tag* tag, int gid);

//public 
//about tag type
int get_tag_type(crdt_tag* tag);
int is_tombstone_tag(crdt_tag* tag);
int is_deleted_tag(crdt_tag* tag);
int get_tag_gid(crdt_tag* tag);
//abdout create tag
void init_crdt_base_tag_head(crdt_tag_base* b, int gid);
crdt_tag_base* create_base_tag(int gid);
crdt_tag_base* create_base_tag_by_meta(CrdtMeta* meta, ctrip_value v);
//add tag
void init_crdt_add_tag_head(crdt_tag* a, int gid);
crdt_tag_ld_add_counter* create_ld_add_tag(int gid);
crdt_tag_add_counter* create_add_tag(int gid);
crdt_tag* create_add_tag_from_all_type(int gid, int type, union all_type value);

crdt_tag* dup_crdt_tag(crdt_tag* tag);
crdt_tag* merge_crdt_tag(crdt_tag* target, crdt_tag* other);
void free_crdt_tag(crdt_tag* tag);
crdt_tag* clean_crdt_tag(crdt_tag* tag, long long vcu);
long long get_tag_vcu(crdt_tag* tag) ;

sds get_tag_info(crdt_tag* tag);
crdt_tag* tag_add_tag(crdt_tag* tag, crdt_tag_add_counter* other);

//to do
crdt_tag* create_tombstone_base_tag(crdt_tag* tag);
crdt_tag* create_tombstone_base_add_del_tag(crdt_tag* tag); 

/**********************  abdou  tag -***********************/


/**********************  abdou  element +***********************/
crdt_element create_crdt_element();
void free_internal_crdt_element_array(crdt_element el);
void free_external_crdt_element(crdt_element el);
void free_internal_crdt_element(crdt_element el);
crdt_element dict_get_element(dictEntry* di);
void dict_set_element(dict* d, dictEntry* di, crdt_element el);
void dict_clean_element(dict* d, dictEntry* di);
crdt_element element_add_tag(crdt_element el, crdt_tag* tag);
crdt_tag* element_get_tag_by_index(crdt_element el, int index);
crdt_tag* element_get_tag_by_gid(crdt_element el, int gid, int* index);
crdt_element element_set_tag_by_index(crdt_element el, int index, crdt_tag* tag);
int element_get_value(crdt_element el, ctrip_value* value);
int get_double_score_by_element(crdt_element el, double* score);
int get_double_add_counter_score_by_element(crdt_element el, int had_del_counter, double* score);
int get_crdt_element_memory(crdt_element el);
crdt_element dup_crdt_element(crdt_element el);
sds get_element_info(crdt_element el);
int purge_element(crdt_element* t, crdt_element* v);
crdt_element element_clean(crdt_element el, int gid, long long vcu, int add_self);
crdt_element element_try_clean_by_vc(crdt_element el, VectorClock vc, int* is_deleted);
crdt_element merge_crdt_element(crdt_element a, crdt_element b);
crdt_element element_merge_tag(crdt_element el, void* v);
crdt_element element_merge_tag2(crdt_element el, void* v);
long long element_get_vcu_by_gid(crdt_element el, int gid);
crdt_element move_crdt_element(crdt_element* rc, crdt_element el);
int reset_crdt_element(crdt_element* rc);
#ifndef COUNTER_BENCHMARK_MAIN
crdt_element load_crdt_element_from_rdb(RedisModuleIO *rdb);
void save_crdt_element_to_rdb(RedisModuleIO *rdb, crdt_element el);
#endif
/**********************  abdou  element -***********************/

//counter_meta
int tag_to_g_counter_meta(void* data, int index, g_counter_meta* value);
// sds get_base_value_sds_from_tag(crdt_tag* tag);
sds get_add_value_sds_from_tag(crdt_tag* tag);
sds get_delete_counter_sds_from_element(crdt_element el);
sds get_base_value_sds_from_element(crdt_element el, int gid);
int write_base_value_to_buf(crdt_element el, int gid, char* buf);
crdt_element create_element_from_vc_and_g_counter(VectorClock vc, int gcounter_len, g_counter_meta** metas, crdt_tag* base_tag);
sds element_add_counter_by_tag(crdt_element* el,  crdt_tag_add_counter* rtag);
sds element_add_counter_by_tag2(crdt_element* el, crdt_tag_add_counter* rtag);

int get_tag_base_value(crdt_tag* tag, long long* current_time, ctrip_value* current_value);
int get_tag_counter_value(crdt_tag* tag, ctrip_value* value, int had_del);
sds get_field_and_delete_counter_str(sds field, crdt_element del_el, int must_field) ;
VectorClock element_get_vc(crdt_element r);
void append_meta_vc_from_element(CrdtMeta* meta, crdt_element el);