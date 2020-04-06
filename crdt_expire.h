#ifndef XREDIS_CRDT_EXPIRE_H
#define XREDIS_CRDT_EXPIRE_H
#include "ctrip_crdt_common.h"
#include "crdt.h"
#include "crdt_util.h"
#define CRDT_EXPIRE_DATATYPE_NAME "crdt_expi"
#define CRDT_EXPIRE_TOMBSTONE_DATATYPE_NAME "crdt_expt"

void* createCrdtExpire();
int initCrdtExpireModule(RedisModuleCtx *ctx);
void *RdbLoadCrdtExpire(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtExpire(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtExpire(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtExpireMemUsageFunc(const void *value);
void freeCrdtExpire(void* value);
void crdtExpireDigestFunc(RedisModuleDigest *md, void *value);

CrdtObject* CrdtExpireMerge(CrdtObject* target, CrdtObject* other);
CrdtObject* CrdtExpireFilter(CrdtObject* common, long long gid, long long logic_time);
static CrdtObjectMethod CrdtExpireCommonMethod = {
    .merge = CrdtExpireMerge,
    .filter = CrdtExpireFilter,
};
void expirePersist(CrdtExpire* expire,  RedisModuleKey* moduleKey, int dbId, RedisModuleString* key);


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
CrdtTombstone* crdtExpireTombstoneFilter(CrdtTombstone* target, long long gid, long long logic_time);
int crdtExpireTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current);
int crdtExpireTombstoneGc(void* target, VectorClock* clock); 
static CrdtTombstoneMethod ExpireTombstoneCommonMethod = {
    .merge = crdtExpireTombstoneMerge,
    .filter =  crdtExpireTombstoneFilter,
    .gc = crdtExpireTombstoneGc,
    .purage = crdtExpireTombstonePurage,
};
//debug
int crdtGetExpireTombstoneCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc);
#endif 