#ifndef XREDIS_CRDT_ORSET_SET_H
#define XREDIS_CRDT_ORSET_SET_H
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
#include "../crdt_set.h"
#include "../include/rmutil/dict.h"
typedef struct CRDT_ORSET_SET {
    ULONGLONG   type:8;
    ULONGLONG   dict: 56;
    VectorClock lastVc; //8
} __attribute__ ((packed, aligned(1))) CRDT_ORSET_SET;


typedef struct CRDT_ORSET_SETTOMBSTONE {
    ULONGLONG   type:8;
    ULONGLONG   dict: 56;
    VectorClock lastVc; //8
    VectorClock maxDelvectorClock;//8
} __attribute__ ((packed, aligned(1))) CRDT_ORSET_SETTOMBSTONE;

void *RdbLoadCrdtORSETSet(RedisModuleIO *rdb, int version, int encver);

void *RdbLoadCrdtSet(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return RdbLoadCrdtORSETSet(rdb, version, encver);
    }
    return NULL;
}


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
void *RdbLoadCrdtSetTombstone(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if ( type == ORSET_TYPE ) {
        return RdbLoadCrdtORSETSetTombstone(rdb, version, encver);
    }
    return NULL;
}


#endif