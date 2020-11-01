
#include "../ctrip_crdt_zset.h"
#include "../ctrip_vector_clock.h"
#include "../include/rmutil/zskiplist.h"
#include "../crdt_util.h"
#define TAG_BASE 1
#define TAG_ADD_COUNTER 2
#define TAG_DEL_COUNTER 4



typedef struct crdt_zset_element
{
    long long len:4; 
    long long tags:60; //crdt_zset_tag** 
} crdt_zset_element; 


typedef struct crdt_zset_tag {
    long long type: 3; // base 1  addcounter 2  delcounter 4
    long long gid: 4;
} crdt_zset_tag;

typedef struct  { //24
    long long type: 3; // base 1  addcounter 2  delcounter 4
    long long gid: 4;
    long long base_vcu: 57;
    long long base_timespace;
    double score;
} crdt_zset_tag_base;

typedef struct  { //16
    long long type: 3; // base 1  addcounter 2  delcounter 4
    long long gid: 4;
    long long add_vcu: 57;
    double add_counter;
} crdt_zset_tag_add_counter;

typedef struct  { //32
    long long type: 3; // base 1  addcounter 2  delcounter 4
    long long gid: 4;
    long long add_vcu: 57;
    double add_counter;
    long long del_vcu;
    double del_counter;
} crdt_zset_tag_add_del_counter;

typedef struct  { //40
    long long type: 3; // base 1  addcounter 2  delcounter 4
    long long gid: 4;
    long long base_vcu: 57;
    long long base_timespace;
    double score;
    long long add_vcu: 57;
    double add_counter;
} crdt_zset_tag_base_and_add_counter;

typedef struct  { //56
    long long type: 3; // base 1  addcounter 2  delcounter 4
    long long gid: 4;
    long long base_vcu: 57;
    long long base_timespace;
    double score;
    long long add_vcu: 57;
    double add_counter;
    long long del_vcu;
    double del_counter;
} crdt_zset_tag_base_and_add_del_counter;

//