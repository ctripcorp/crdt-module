
#include "../ctrip_crdt_zset.h"
#include "../ctrip_vector_clock.h"
#include "../include/rmutil/zskiplist.h"
#include "../crdt_util.h"
#define TAG_BASE 1
#define TAG_START_COUNTER 2
#define TAG_END_COUNTER 4
//crdtObject
typedef struct crdt_zset
{
    char type; // 类型 data + zset 
    dict* dict;//字典 Map<string, crdt_zset_element>
    zskiplist* zsl;//跳表
    VectorClock lastvc; //最后操作该key的vc   转换成tombstone gc时用
} crdt_zset;

typedef struct crdt_zset_tombstone {
    char type;// 类型 tombstone + zset 
    dict* dict;
    VectorClock lastvc;
} crdt_zset_tombstone;


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