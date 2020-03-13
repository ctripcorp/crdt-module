

#ifndef XREDIS_CRDT_CRDT_LWW_HASHMAP_H
#define XREDIS_CRDT_CRDT_LWW_HASHMAP_H
#include "../ctrip_crdt_hashmap.h"
#include "../crdt_register.h"
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
// #define NDEBUG
#include <assert.h>
typedef struct CRDT_LWW_Hash {
    CRDT_Hash parent;
    VectorClock* lastVc;
} CRDT_LWW_Hash;

typedef struct CRDT_LWW_HashTombstone {
    CRDT_HashTombstone parent;
    CrdtMeta* maxDelMeta;
    VectorClock* lastVc;
} CRDT_LWW_HashTombstone;
void* createCrdtLWWHash();
void* createCrdtLWWHashTombstone();
void freeCrdtLWWHash(void* obj);
void freeCrdtLWWHashTombstone(void* obj);
CRDT_LWW_Hash* retrieveCrdtLWWHash(void* obj);
CRDT_LWW_HashTombstone* retrieveCrdtLWWHashTombstone(void* data);
//common methods
CrdtObject* crdtLWWHashFilter(CrdtObject* common, long long gid, long long logic_time);
int crdtLWWHashClean(CrdtObject* current, CrdtTombstone* tombstone);
int crdtLWWHashGc(void* target, VectorClock* clock);
//private hash module functions 
void *RdbLoadCrdtLWWHash(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtLWWHash(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtLWWHash(RedisModuleIO *aof, RedisModuleString *key, void *value);
void freeCrdtLWWHash(void *crdtHash);
size_t crdtLWWHashMemUsageFunc(const void *value);
void crdtLWWHashDigestFunc(RedisModuleDigest *md, void *value);
//private hash tombstone functions
void *RdbLoadCrdtLWWHashTombstone(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtLWWHashTombstone(RedisModuleIO *rdb, void *value);
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
void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver) {
    return RdbLoadCrdtLWWHash(rdb, encver);
}
void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value) {
    RdbSaveCrdtLWWHash(rdb, value);
}
void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWHash(aof, key, value);
}

size_t crdtHashMemUsageFunc(const void *value) {
    crdtLWWHashMemUsageFunc(value);
}
void crdtHashDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWHashDigestFunc(md, value);
}
//basic hash tombstone module functions
void *RdbLoadCrdtHashTombstone(RedisModuleIO *rdb, int encver) {
    return RdbLoadCrdtLWWHashTombstone(rdb, encver);
}
void RdbSaveCrdtHashTombstone(RedisModuleIO *rdb, void *value) {
    RdbSaveCrdtLWWHashTombstone(rdb, value);
}
void AofRewriteCrdtHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWHashTombstone(aof, key, value);
}

size_t crdtHashTombstoneMemUsageFunc(const void *value) {
    crdtLWWHashTombstoneMemUsageFunc(value);
}
void crdtHashTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWHashTombstoneDigestFunc(md, value);
}
#endif //XREDIS_CRDT_CRDT_LWW_HASHMAP_H