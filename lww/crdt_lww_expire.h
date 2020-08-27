
#include "../crdt_expire.h"
#include "../ctrip_crdt_common.h"
#include <assert.h>
typedef struct Crdt_LWW_Expire {//26
    unsigned char type;//1
    long long expireTime;//8
    unsigned char gid;//1
    long long timestamp;//8
    VectorClock vc;//9
} __attribute__ ((packed, aligned(1))) Crdt_LWW_Expire;

Crdt_LWW_Expire* createCrdtLWWExpire();
void* createCrdtExpire() {
    return createCrdtLWWExpire();
}
long long getCrdtLWWExpireExpireTime(Crdt_LWW_Expire* expire);
long long getCrdtExpireLastExpireTime(CrdtExpire* expire) {
    return getCrdtLWWExpireExpireTime(expire);
}

int getCrdtLWWExpireGid(Crdt_LWW_Expire* expire);
int getCrdtExpireLastGid(CrdtExpire* expire) {
    return getCrdtLWWExpireGid(expire);
}

VectorClock*  getCrdtLWWExpireVectorClock(Crdt_LWW_Expire* expire);
VectorClock* getCrdtExpireLastVectorClock(CrdtExpire* expire) {
    return getCrdtLWWExpireVectorClock(expire);
}
long long  getCrdtLWWExpireTimestamp(Crdt_LWW_Expire* expire);
long long getCrdtExpireLastTimestamp(CrdtExpire* expire) {
    return getCrdtLWWExpireTimestamp(expire);
}
CrdtMeta* getCrdtLWWExpireMeta(Crdt_LWW_Expire* expire);
CrdtMeta* getCrdtExpireLastMeta(CrdtExpire* expire) {
    return getCrdtLWWExpireMeta(expire);
}
void *RdbLoadCrdtLWWExpire(RedisModuleIO *rdb, int encver);
void *  RdbLoadCrdtExpire(RedisModuleIO *rdb, int encver) {
    int type = RedisModule_LoadSigned(rdb);
    if( type == LWW_TYPE) {
        return RdbLoadCrdtLWWExpire(rdb, encver);
    }
    return NULL;
}
void AofRewriteCrdtLWWExpire(RedisModuleIO *aof, RedisModuleString *key, void *value);
void AofRewriteCrdtExpire(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWExpire(aof, key, value);
}
void RdbSaveCrdt_LWW_Expire(RedisModuleIO *rdb, void *value);
void RdbSaveCrdtExpire(RedisModuleIO *rdb, void *value) {
    RdbSaveCrdt_LWW_Expire(rdb, value);
}

size_t Crdt_LWW_ExpireMemUsageFunc(const void *value);
size_t crdtExpireMemUsageFunc(const void *value) {
    return Crdt_LWW_ExpireMemUsageFunc(value);
}
void Crdt_LWW_ExpireDigestFunc(RedisModuleDigest *md, void *value);
void crdtExpireDigestFunc(RedisModuleDigest *md, void *value) {
    Crdt_LWW_ExpireDigestFunc(md, value);
}
void crdtExpireFree(CrdtExpire* value);
void freeCrdtExpire(void* value) {
    crdtExpireFree(value);
}
//crdtExpire
typedef struct Crdt_LWW_ExpireTombstone {
    unsigned char type;//1
    unsigned char gid;//1
    long long timestamp;//8
    VectorClock vectorClock;//8
}  __attribute__ ((packed, aligned(1))) Crdt_LWW_ExpireTombstone;
Crdt_LWW_ExpireTombstone* createCrdtLWWExpireTombstone(int dataType);
CrdtExpireTombstone* createCrdtExpireTombstone(int dataType) {
    return createCrdtLWWExpireTombstone(dataType);
}
void *RdbLoadCrdtLWWExpireTombstone(RedisModuleIO *rdb, int encver);
void *RdbLoadCrdtExpireTombstone(RedisModuleIO *rdb, int encver) {
    int type = RedisModule_LoadSigned(rdb);
    if( type == LWW_TYPE) {
        return RdbLoadCrdtLWWExpireTombstone(rdb, encver);
    }
    return NULL;
}
void RdbSaveCrdt_LWW_ExpireTombstone(RedisModuleIO *rdb, void *value);
void RdbSaveCrdtExpireTombstone(RedisModuleIO *rdb, void *value) {
    RdbSaveCrdt_LWW_ExpireTombstone(rdb, value);
}
void AofRewriteCrdtLWWExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
void AofRewriteCrdtExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWExpireTombstone(aof, key, value);
}
size_t Crdt_LWW_ExpireTombstoneMemUsageFunc(const void *value);
size_t crdtExpireTombstoneMemUsageFunc(const void *value) {
    return Crdt_LWW_ExpireTombstoneMemUsageFunc(value);
}
void freeCrdt_LWW_ExpireTombstone(void* value);
void freeCrdtExpireTombstone(void* value) {
    freeCrdt_LWW_ExpireTombstone(value);
}
void Crdt_LWW_ExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value);
void crdtExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    Crdt_LWW_ExpireTombstoneDigestFunc(md, value);
}

CrdtObject* Crdt_LWW_ExpireFilter(CrdtObject* common, int gid, long long logic_time);
CrdtObject* CrdtExpireFilter(CrdtObject* common, int gid, long long logic_time) {
    return Crdt_LWW_ExpireFilter(common, gid, logic_time);
}
CrdtObject* Crdt_LWW_ExpireMerge(CrdtObject* target, CrdtObject* other);
CrdtObject* CrdtExpireMerge(CrdtObject* target, CrdtObject* other) {
    return Crdt_LWW_ExpireMerge(target, other);
}

CrdtTombstone* Crdt_LWW_ExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other);
CrdtTombstone* crdtExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other) {
    return Crdt_LWW_ExpireTombstoneMerge(target, other);
}
CrdtTombstone* Crdt_LWW_ExpireTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time);
CrdtTombstone* crdtExpireTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time) {
    return Crdt_LWW_ExpireTombstoneFilter(target, gid, logic_time);
}
int Crdt_LWW_ExpireTombstonePurge(CrdtTombstone* tombstone, CrdtObject* current); 
int crdtExpireTombstonePurge(CrdtTombstone* tombstone, CrdtObject* current) {
    return Crdt_LWW_ExpireTombstonePurge(tombstone, current);
}

int Crdt_LWW_ExpireTombstoneGc(void* target, VectorClock* clock);
int crdtExpireTombstoneGc(void* target, VectorClock* clock) {
    return Crdt_LWW_ExpireTombstoneGc(target, clock);
}
