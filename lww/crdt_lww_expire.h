
#include "../crdt_expire.h"
#include "../ctrip_crdt_common.h"
typedef struct CrdtLWWExpire {
    CrdtExpire parent;
    CrdtExpireObj* data;
} CrdtLWWExpire;

CrdtLWWExpire* createCrdtLWWExpire();
void* createCrdtExpire() {
    return createCrdtLWWExpire();
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
void* RdbSaveCrdtLWWExpire(RedisModuleIO *rdb, void *value);
void RdbSaveCrdtExpire(RedisModuleIO *rdb, void *value) {
    RdbSaveCrdtLWWExpire(rdb, value);
}

size_t crdtLWWExpireMemUsageFunc(const void *value);
size_t crdtExpireMemUsageFunc(const void *value) {
    return crdtLWWExpireMemUsageFunc(value);
}
void crdtLWWExpireDigestFunc(RedisModuleDigest *md, void *value);
void crdtExpireDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWExpireDigestFunc(md, value);
}
void LWWExpireFree(CrdtExpire* value);
void freeCrdtExpire(void* value) {
    LWWExpireFree(value);
}
//crdtExpire
typedef struct CrdtLWWExpireTombstone {
    CrdtExpireTombstone parent;
    CrdtMeta* meta;
} CrdtLWWExpireTombstone;
CrdtLWWExpireTombstone* createCrdtLWWExpireTombstone(int dataType);
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
void RdbSaveCrdtLWWExpireTombstone(RedisModuleIO *rdb, void *value);
void RdbSaveCrdtExpireTombstone(RedisModuleIO *rdb, void *value) {
    RdbSaveCrdtLWWExpireTombstone(rdb, value);
}
void AofRewriteCrdtLWWExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
void AofRewriteCrdtExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWExpireTombstone(aof, key, value);
}
size_t crdtLWWExpireTombstoneMemUsageFunc(const void *value);
size_t crdtExpireTombstoneMemUsageFunc(const void *value) {
    return crdtLWWExpireTombstoneMemUsageFunc(value);
}
void freeCrdtLWWExpireTombstone(void* value);
void freeCrdtExpireTombstone(void* value) {
    freeCrdtLWWExpireTombstone(value);
}
void crdtLWWExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value);
void crdtExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWExpireTombstoneDigestFunc(md, value);
}

CrdtObject* CrdtLWWExpireFilter(CrdtObject* common, long long gid, long long logic_time);
CrdtObject* CrdtExpireFilter(CrdtObject* common, long long gid, long long logic_time) {
    return CrdtLWWExpireFilter(common, gid, logic_time);
}
CrdtObject* CrdtLWWExpireMerge(CrdtObject* target, CrdtObject* other);
CrdtObject* CrdtExpireMerge(CrdtObject* target, CrdtObject* other) {
    return CrdtLWWExpireMerge(target, other);
}

CrdtTombstone* crdtLWWExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other);
CrdtTombstone* crdtExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other) {
    return crdtLWWExpireTombstoneMerge(target, other);
}
CrdtTombstone* crdtLWWExpireTombstoneFilter(CrdtTombstone* target, long long gid, long long logic_time);
CrdtTombstone* crdtExpireTombstoneFilter(CrdtTombstone* target, long long gid, long long logic_time) {
    return crdtLWWExpireTombstoneFilter(target, gid, logic_time);
}
int crdtLWWExpireTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current); 
int crdtExpireTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current) {
    return crdtLWWExpireTombstonePurage(tombstone, current);
}

int crdtLWWExpireTombstoneGc(void* target, VectorClock* clock);
int crdtExpireTombstoneGc(void* target, VectorClock* clock) {
    return crdtLWWExpireTombstoneGc(target, clock);
}
