#ifndef XREDIS_CRDT_EXPIRE_H
#define XREDIS_CRDT_EXPIRE_H
#include "ctrip_crdt_common.h"
#include "crdt.h"
#include "crdt_util.h"
#define CRDT_EXPIRE_DATATYPE_NAME "crdt_expi"
#define CRDT_EXPIRE_TOMBSTONE_DATATYPE_NAME "crdt_expt"

static RedisModuleType *CrdtExpireType;
static RedisModuleType *CrdtExpireTombstoneType;
RedisModuleType* getCrdtExpireType();
RedisModuleType* getCrdtExpireTombstoneType();
void* createCrdtExpire();
int initCrdtExpireModule(RedisModuleCtx *ctx);
void *RdbLoadCrdtExpire(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtExpire(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtExpire(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtExpireMemUsageFunc(const void *value);
void freeCrdtExpire(void* value);
void crdtExpireDigestFunc(RedisModuleDigest *md, void *value);

CrdtObject* CrdtExpireMerge(CrdtObject* target, CrdtObject* other);
CrdtObject* CrdtExpireFilter(CrdtObject* common, int gid, long long logic_time);
static CrdtObjectMethod CrdtExpireCommonMethod = {
    .merge = CrdtExpireMerge,
    .filter = CrdtExpireFilter,
};
void expirePersist(CrdtExpire* expire,  RedisModuleKey* moduleKey, int dbId, RedisModuleString* key);
int crdtExpireAddObj(CrdtExpire* obj, CrdtExpireObj* data);
CrdtExpireObj* crdtExpireGetObj(CrdtExpire* obj);
CrdtExpire* crdtExpireDup(CrdtExpire* obj);
void crdtExpireFree(CrdtExpire* obj);
static CrdtExpireMethod ExpireMethod = {
    add: crdtExpireAddObj,
    get: crdtExpireGetObj,
    dup: crdtExpireDup,
    free: crdtExpireFree,
    persist: expirePersist,
};

CrdtExpireTombstone* createCrdtExpireTombstone(int dataType);
void *RdbLoadCrdtExpireTombstone(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtExpireTombstone(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtExpireTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtExpireTombstoneMemUsageFunc(const void *value);
void freeCrdtExpireTombstone(void* value);
void crdtExpireTombstoneDigestFunc(RedisModuleDigest *md, void *value);

//utils
void delExpire(RedisModuleKey *moduleKey, CrdtExpire* expire, CrdtMeta* meta);
void addExpireTombstone(RedisModuleKey* moduleKey,int dataType, CrdtMeta* meta);
CrdtExpireObj* addOrUpdateExpire(RedisModuleKey* moduleKey, CrdtData* data, CrdtMeta* meta,long long expireTime);
int tryAddOrUpdateExpire(RedisModuleKey* moduleKey, int type, CrdtExpireObj* obj);
CrdtTombstone* crdtExpireTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other);
CrdtTombstone* crdtExpireTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time);
int crdtExpireTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current);
int crdtExpireTombstoneGc(void* target, VectorClock* clock); 
static CrdtTombstoneMethod ExpireTombstoneCommonMethod = {
    .merge = crdtExpireTombstoneMerge,
    .filter =  crdtExpireTombstoneFilter,
    .gc = crdtExpireTombstoneGc,
    .purage = crdtExpireTombstonePurage,
};
int CrdtExpireIsExpire(void* data, CrdtMeta* meta);
int CrdtExpireTombstoneAdd(void* data, CrdtMeta* meta);
static CrdtExpireTombstoneMethod ExpireTombstoneMethod = {
    .add = CrdtExpireTombstoneAdd,
    .isExpire = CrdtExpireIsExpire
};
//debug
int crdtGetExpireTombstoneCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc);
#endif 