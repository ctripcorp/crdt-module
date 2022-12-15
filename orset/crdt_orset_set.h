#ifndef XREDIS_crdt_orset_set_H
#define XREDIS_crdt_orset_set_H
#include <rmutil/sds.h>
#include "../crdt_util.h"
#include "../crdt_set.h"
#include <rmutil/dict.h>
#include <redismodule.h>
#if defined(TCL_TEST)
typedef struct crdt_orset_set {
    unsigned char  type;
    dict*   dict;
    VectorClock lastVc; //8
} __attribute__ ((packed, aligned(1))) crdt_orset_set;


typedef struct crdt_orset_set_tombstone {
    unsigned char  type;
    dict*   dict;
    VectorClock lastVc; //8
    VectorClock maxDelvectorClock;//8
} __attribute__ ((packed, aligned(1))) crdt_orset_set_tombstone;


#else 
typedef struct crdt_orset_set {
    ULONGLONG   type:8;
    ULONGLONG   dict: 56;
    VectorClock lastVc; //8
} __attribute__ ((packed, aligned(1))) crdt_orset_set;


typedef struct crdt_orset_set_tombstone {
    ULONGLONG   type:8;
    ULONGLONG   dict: 56;
    VectorClock lastVc; //8
    VectorClock maxDelvectorClock;//8
} __attribute__ ((packed, aligned(1))) crdt_orset_set_tombstone;
#endif
int set_gc_stats = 1;
void *RdbLoadCrdtORSETSet(RedisModuleIO *rdb, int version, int encver);



void dictCrdtORSETSetDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    VectorClock vc = LL2VC(val);
    freeVectorClock(vc);
}
static dictType crdtSetDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtORSETSetDestructor   /* val destructor */
};
static dictType crdtSetFileterDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,          /* key destructor */
    NULL/* val destructor */
};

//about tombstone
void *RdbLoadCrdtORSETSetTombstone(RedisModuleIO *rdb, int version, int encver);
// void RdbSaveCrdtORSETSetTombstone(RedisModuleIO *rdb, void *value);
// void AofRewriteCrdtORSETSetTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
// size_t crdtORSETSetTombstoneMemUsageFunc(const void *value);
// int crdtORSETSetTombstoneDigestFunc(RedisModuleDigest *md, void *value);


#endif