

#ifndef XREDIS_CRDT_CRDT_LWW_HASHMAP_H
#define XREDIS_CRDT_CRDT_LWW_HASHMAP_H
#include "../ctrip_crdt_hashmap.h"
#include "../crdt_register.h"
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
// #define NDEBUG
#include <assert.h>
typedef struct CRDT_LWW_Hash {
    unsigned char type;
    dict* map;
    VectorClock lastVc;
} __attribute__ ((packed, aligned(1))) CRDT_LWW_Hash;

typedef struct CRDT_LWW_HashTombstone {
    unsigned long long type:8;
    unsigned long long maxDelGid:4; 
    unsigned long long maxDelTimestamp:52; //52
    VectorClock maxDelvectorClock;//8
    dict* map; //8
    VectorClock lastVc;//8
} __attribute__ ((packed, aligned(1))) CRDT_LWW_HashTombstone;
void* createCrdtLWWHash();
void* createCrdtLWWHashTombstone();
void freeCrdtLWWHash(void* obj);
void freeCrdtLWWHashTombstone(void* obj);
CRDT_LWW_Hash* retrieveCrdtLWWHash(void* obj);
CRDT_LWW_HashTombstone* retrieveCrdtLWWHashTombstone(void* data);
//common methods
CrdtObject* crdtLWWHashFilter(CrdtObject* common, int gid, long long logic_time);
int crdtLWWHashClean(CrdtObject* current, CrdtTombstone* tombstone);
//private hash module functions 
void *sioLoadCrdtLWWHash(sio *io, int version, int encver);
void sioSaveCrdtLWWHash(sio *io, void *value);
void AofRewriteCrdtLWWHash(RedisModuleIO *aof, RedisModuleString *key, void *value);
void freeCrdtLWWHash(void *crdtHash);
size_t crdtLWWHashMemUsageFunc(const void *value);
void crdtLWWHashDigestFunc(RedisModuleDigest *md, void *value);
//private hash tombstone functions
void *sioLoadCrdtLWWHashTombstone(sio *io, int version, int encver);
void sioSaveCrdtLWWHashTombstone(sio *io, void *value);
void AofRewriteCrdtLWWHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
void freeCrdtLWWHashTombstone(void *crdtHash);
size_t crdtLWWHashTombstoneMemUsageFunc(const void *value);
void crdtLWWHashTombstoneDigestFunc(RedisModuleDigest *md, void *value);

//create hash
void *createCrdtHash(void) {
    return createCrdtLWWHash();
}
void freeCrdtHash(void *data) {
    freeCrdtLWWHash(data);
}
//create hash tombstone
void *createCrdtHashTombstone(void) {
    return createCrdtLWWHashTombstone();
}
void freeCrdtHashTombstone(void *data) {
    freeCrdtLWWHashTombstone(data);
}
//basic hash module functions
static void *sioLoadCrdtHash(sio *io, int encver) {
    long long header = loadCrdtRdbHeader(io);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == LWW_TYPE) {
        return sioLoadCrdtLWWHash(io, version, encver);
    }
    return NULL;
}
void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver) {
    sio *io = rdbStreamCreate(rdb);
    void *res = sioLoadCrdtHash(io, encver);
    rdbStreamRelease(io);
    return res;
}

void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value) {
    sio *io = rdbStreamCreate(rdb);
    sioSaveCrdtLWWHash(io, value);
    rdbStreamRelease(io);
}
void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWHash(aof, key, value);
}

size_t crdtHashMemUsageFunc(const void *value) {
    return crdtLWWHashMemUsageFunc(value);
}
void crdtHashDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWHashDigestFunc(md, value);
}
void *sioLoadCrdtHashTombstone(sio *io, int encver) {
    long long header = loadCrdtRdbHeader(io);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == LWW_TYPE) {
        return sioLoadCrdtLWWHashTombstone(io, version, encver);
    }
    return NULL;
}
//basic hash tombstone module functions
void *RdbLoadCrdtHashTombstone(RedisModuleIO *rdb, int encver) {
    sio *io = rdbStreamCreate(rdb);
    void *res = sioLoadCrdtHashTombstone(io, encver);
    rdbStreamRelease(io);
    return res;
}
void RdbSaveCrdtHashTombstone(RedisModuleIO *rdb, void *value) {
    sio *io = rdbStreamCreate(rdb);
    sioSaveCrdtLWWHashTombstone(io, value);
    rdbStreamRelease(io);
}
void AofRewriteCrdtHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWHashTombstone(aof, key, value);
}

size_t crdtHashTombstoneMemUsageFunc(const void *value) {
    return crdtLWWHashTombstoneMemUsageFunc(value);
}
void crdtHashTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWHashTombstoneDigestFunc(md, value);
}
int changeCrdtLWWHash(CRDT_Hash* hash, CrdtMeta* meta) ;
int changeCrdtHash(CRDT_Hash* hash, CrdtMeta* meta) {
    return changeCrdtLWWHash(hash,meta);
}
CRDT_Hash* dupCrdtLWWHash(void* data);
CRDT_Hash* dupCrdtHash(CRDT_Hash* data) {
    return dupCrdtLWWHash(data);
}
// VectorClock getCrdtLWWHashLastVc(CRDT_LWW_Hash* r);
// VectorClock getCrdtHashLastVc(CRDT_Hash* data) {
//     return getCrdtLWWHashLastVc((CRDT_LWW_Hash*)data);
// }


void updateLastVCLWWHash(void* data, VectorClock vc);
void updateLastVCHash(CRDT_Hash* data, VectorClock vc) {
    return updateLastVCLWWHash(data, vc);
}

//tombstone
CrdtMeta* updateMaxDelCrdtLWWHashTombstone(void* data, CrdtMeta* meta, int* comapre);
CrdtMeta* updateMaxDelCrdtHashTombstone(void* data, CrdtMeta* meta, int* comapre) {
    return updateMaxDelCrdtLWWHashTombstone(data, meta, comapre);
}
int compareCrdtLWWHashTombstone(void* data, CrdtMeta* meta);
int compareCrdtHashTombstone(void* data, CrdtMeta* meta) {
    return compareCrdtLWWHashTombstone(data, meta);
}
CRDT_HashTombstone* dupCrdtLWWHashTombstone(void* data);
CRDT_HashTombstone* dupCrdtHashTombstone(void* data) {
    return dupCrdtLWWHashTombstone(data);
}
int gcCrdtLWWHashTombstone(void* data, VectorClock clock);
int hash_gc_stats = 1;

int hashStartGc() {
    hash_gc_stats = 1;
    return hash_gc_stats;
}

int hashStopGc() {
    hash_gc_stats = 0;
    return hash_gc_stats;
}

int gcCrdtHashTombstone(void* data, VectorClock clock) {
    if(!hash_gc_stats) {
        return 0;
    }
    return gcCrdtLWWHashTombstone(data, clock);
}
VectorClock getCrdtLWWHashTombstoneLastVc(CRDT_LWW_HashTombstone* t);
VectorClock getCrdtHashTombstoneLastVc(CRDT_HashTombstone* t) {
    return getCrdtLWWHashTombstoneLastVc((CRDT_LWW_HashTombstone*)t);
}
void setCrdtLWWHashTombstoneLastVc(CRDT_LWW_HashTombstone* t, VectorClock vc);
void mergeCrdtHashTombstoneLastVc(CRDT_HashTombstone* t, VectorClock vc) {
    CRDT_LWW_HashTombstone* tombstone = (CRDT_LWW_HashTombstone*)t;
    setCrdtLWWHashTombstoneLastVc(tombstone, vectorClockMerge(getCrdtLWWHashTombstoneLastVc(tombstone), vc));
}
CrdtMeta* getCrdtLWWHashTombstoneMaxDelMeta(CRDT_LWW_HashTombstone* data);
CrdtMeta* getMaxDelCrdtHashTombstone(void* data) {
    return getCrdtLWWHashTombstoneMaxDelMeta((CRDT_LWW_HashTombstone*)data);
}
int changeCrdtLWWHashTombstone(void* data, CrdtMeta* meta);
int changeCrdtHashTombstone(void* data, CrdtMeta* meta) {
    return changeCrdtLWWHashTombstone(data, meta);
}

#endif //XREDIS_CRDT_CRDT_LWW_HASHMAP_H
