
#include "../ctrip_crdt_zset.h"
#include "../ctrip_vector_clock.h"
#include "../include/rmutil/zskiplist.h"
#include "../crdt_util.h"

#define TAG_BASE 1
#define TAG_ADD_COUNTER 2
#define TAG_DEL_COUNTER 4


//tag = tag* when len = 1
//tag = tag** when len > 1
typedef struct crdt_zset_element
{
    long long len:4; 
    long long tags:60; //crdt_zset_tag** 
} __attribute__ ((packed, aligned(1))) crdt_zset_element; 

typedef struct crdt_zset_tag {
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long ops: 57;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag;

//base 
typedef struct  { //24
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned  long long base_vcu: 57;
    long long base_timespace;
    double score;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_base;

//add_counter 
typedef struct  { //16
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long add_vcu: 57;
    double add_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_add_counter;

//add_counter + del_counter
typedef struct  { //32
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long add_vcu: 57;
    double add_counter;
    long long del_vcu;
    double del_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_add_del_counter;


//base + add_counter 
typedef struct  { //40
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long base_vcu: 57;
    long long base_timespace;
    double score;
    long long add_vcu: 57;
    double add_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_base_and_add_counter;


//base + add_counter + del_dounter
typedef struct  { //56
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long base_vcu: 57;
    long long base_timespace;
    double score;
    long long add_vcu: 57;
    double add_counter;
    long long del_vcu;
    double del_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_base_and_add_del_counter;
