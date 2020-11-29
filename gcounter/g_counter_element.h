
#include "../ctrip_vector_clock.h"
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




typedef struct crdt_element {
    long long len:4; 
    long long tags:60; //crdt_tag** 
} __attribute__ ((packed, aligned(1))) crdt_element; 

typedef struct crdt_tag {
    unsigned long long data_type: 1; //0 value 1 tombstone
    unsigned long long type:3; //1 base 2  addcounter 4  delcounter 
    unsigned long long gid: 4;
    unsigned long long ops: 56;
} __attribute__ ((packed, aligned(1))) crdt_tag;

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
} __attribute__ ((packed, aligned(1))) crdt_tag_ll_add_counter;


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
crdt_tag* create_base_tag(int gid);
crdt_tag* create_add_tag(int gid);
crdt_tag* create_base_add_tag(int gid);
crdt_tag* create_base_add_del_tag(int gid);

crdt_tag* dup_crdt_tag(crdt_tag* tag);
crdt_tag* merge_crdt_tag(crdt_tag* target, crdt_tag* other);
void free_crdt_tag(crdt_tag* tag);
crdt_tag* clean_crdt_tag(crdt_tag* tag, long long vcu);


sds get_tag_info(crdt_tag* tag);
crdt_tag* tag_add_counter(crdt_tag* tag, long long vcu, int type, union tag_data* value , int incr);
crdt_tag* tag_add_or_update(crdt_tag* tag, crdt_tag_add_counter* other, int incr);

//to do
crdt_tag* create_tombstone_base_tag(crdt_tag* tag);
crdt_tag* create_tombstone_base_add_del_tag(crdt_tag* tag); 

/**********************  abdou  tag -***********************/


/**********************  abdou  element +***********************/
crdt_element create_crdt_element();
void free_crdt_element_array(crdt_element el);
void free_crdt_element(void* el);
crdt_element get_element_by_dictEntry(dictEntry* di);
void set_element_by_dictEntry(dictEntry* di, crdt_element el);

crdt_element add_tag_by_element(crdt_element el, crdt_tag* tag);
crdt_tag* element_get_tag_by_index(crdt_element el, int index);
crdt_tag* element_get_tag_by_gid(crdt_element el, int gid, int* index);
void element_set_tag_by_index(crdt_element* el, int index, crdt_tag* tag);
int get_double_score_by_element(crdt_element el, double* score);
int get_double_add_counter_score_by_element(crdt_element el, int had_del_counter, double* score);
int get_crdt_element_memory(crdt_element el);
crdt_element dup_crdt_element(crdt_element el);
sds get_element_info(crdt_element el);
int purge_element(crdt_element* t, crdt_element* v);
crdt_element clean_element_by_vc(crdt_element el, VectorClock vc, int* is_deleted);
crdt_element merge_crdt_element(crdt_element a, crdt_element b);


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
crdt_element create_element_from_vc_and_g_counter(VectorClock vc, int gcounter_len, g_counter_meta** metas, crdt_tag* base_tag);