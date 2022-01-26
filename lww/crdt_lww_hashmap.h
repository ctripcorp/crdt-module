#ifndef XREDIS_CRDT_CRDT_LWW_HASHMAP_H
#define XREDIS_CRDT_CRDT_LWW_HASHMAP_H

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
    unsigned long long maxDelTimestamp:52;
    VectorClock maxDelvectorClock;
    dict* map;
    VectorClock lastVc;
} __attribute__ ((packed, aligned(1))) CRDT_LWW_HashTombstone;

void* createCrdtLWWHash();
void* createCrdtLWWHashTombstone();
void freeCrdtLWWHash(void* obj);
void freeCrdtLWWHashTombstone(void* obj);
CRDT_LWW_Hash* retrieveCrdtLWWHash(void* obj);
CRDT_LWW_HashTombstone* retrieveCrdtLWWHashTombstone(void* data);
VectorClock getCrdtLWWHashLastVc(CRDT_LWW_Hash* r);
void setCrdtLWWHashLastVc(CRDT_LWW_Hash* r, VectorClock vc);
CrdtObject* crdtLWWHashFilter(CrdtObject* common, int gid, long long logic_time);
int crdtLWWHashClean(CrdtObject* current, CrdtTombstone* tombstone);
void *sioLoadCrdtLWWHash(sio *io, int version, int encver);
void sioSaveCrdtLWWHash(sio *io, void *value);
void AofRewriteCrdtLWWHash(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtLWWHashMemUsage(const void *crdtHash, int sample_size);
size_t crdtLWWHashMemUsageFunc(const void *value);
void crdtLWWHashDigestFunc(RedisModuleDigest *md, void *value);
void *sioLoadCrdtLWWHashTombstone(sio *io, int version, int encver);
void sioSaveCrdtLWWHashTombstone(sio *io, void *value);
void AofRewriteCrdtLWWHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
void freeCrdtLWWHashTombstone(void *crdtHash);
size_t crdtLWWHashTombstoneMemUsageFunc(const void *value);
void crdtLWWHashTombstoneDigestFunc(RedisModuleDigest *md, void *value);
int changeCrdtLWWHash(CRDT_LWW_Hash* hash, CrdtMeta* meta) ;
CRDT_LWW_Hash* dupCrdtLWWHash(void* data);
void updateLastVCLWWHash(void* data, VectorClock vc);
CrdtMeta* updateMaxDelCrdtLWWHashTombstone(void* data, CrdtMeta* meta, int* comapre);
int compareCrdtLWWHashTombstone(void* data, CrdtMeta* meta);
CRDT_LWW_HashTombstone* dupCrdtLWWHashTombstone(void* data);
int gcCrdtLWWHashTombstone(void* data, VectorClock clock);
VectorClock getCrdtLWWHashTombstoneLastVc(CRDT_LWW_HashTombstone* t);
void setCrdtLWWHashTombstoneLastVc(CRDT_LWW_HashTombstone* t, VectorClock vc);
CrdtMeta* getCrdtLWWHashTombstoneMaxDelMeta(CRDT_LWW_HashTombstone* data);
int changeCrdtLWWHashTombstone(void* data, CrdtMeta* meta);
CRDT_LWW_HashTombstone* createCrdtLWWHashFilterTombstone(CRDT_LWW_HashTombstone* target);

#endif //XREDIS_CRDT_CRDT_LWW_HASHMAP_H
