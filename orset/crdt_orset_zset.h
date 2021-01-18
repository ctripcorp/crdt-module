#include "../ctrip_crdt_zset.h"
#include "../ctrip_vector_clock.h"
#include "../crdt_util.h"
#include "../gcounter/g_counter_element.h"

/******************* crdt_zset +*******************/

typedef struct crdt_zset
{
    char type; // data + zset 
    dict* dict;// Map<string, crdt_zset_element>
    zskiplist* zsl;
    VectorClock lastvc; 
} crdt_zset;
// function 
crdt_zset* retrieve_crdt_zset(CRDT_SS* rc);

/******************* crdt_zset  -*******************/

/******************* crdt_zset tombstone  +*******************/

typedef struct crdt_zset_tombstone {
    char type;//  tombstone + zset 
    dict* dict;
    VectorClock lastvc;
    VectorClock maxdelvc;
} crdt_zset_tombstone;
crdt_zset_tombstone* retrieve_crdt_zset_tombstone(CRDT_SSTombstone* rt);

/******************* crdt_zset tombstone  -*******************/
int zset_gc_stats = 1;




