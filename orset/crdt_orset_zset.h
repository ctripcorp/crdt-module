
#include "../ctrip_crdt_zset.h"
#include "../ctrip_vector_clock.h"
#include "../include/rmutil/zskiplist.h"
#include "../crdt_util.h"

#define TAG_NONE 0
#define TAG_BASE 1
#define TAG_ADD_COUNTER 2
#define TAG_DEL_COUNTER 4

/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */

#define DEL_TIME -1
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


//base tombstone
typedef struct {
    unsigned long long type: 3; //base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned  long long base_vcu: 57;//orset  add wins 
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_base_tombstone;

//add_counter 
typedef struct  { //16
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long add_vcu: 57;
    double add_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_add_counter;

//add_counter = del_counter when it is tombstone
//del_counter tombstone
typedef struct {
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long del_vcu: 57;
    double del_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_del_counter_tombstone;

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

//add_counter = del_counter when it is tombstone
//base + del + tombstone
typedef struct {
    unsigned long long type: 3; // base 1  addcounter 2  delcounter 4
    unsigned long long gid: 4;
    unsigned long long del_vcu: 57;
    double del_counter;
} __attribute__ ((packed, aligned(1))) crdt_zset_tag_base_and_del_counter_tombstone;


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

zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range);
