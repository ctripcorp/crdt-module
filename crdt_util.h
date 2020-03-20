#ifndef XREDIS_CRDT_CRDT_UTIL_H
#define XREDIS_CRDT_CRDT_UTIL_H
#include "include/redismodule.h"
#include "ctrip_vector_clock.h"
#include "utils.h"
#include "crdt.h"
#include "ctrip_crdt_common.h"
CrdtMeta* getMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index);
RedisModuleKey* getWriteRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type);
void* getCurrentValue(RedisModuleKey *moduleKey);
void* getTombstone(RedisModuleKey *moduleKey);
CrdtMeta* mergeMeta(CrdtMeta* target, CrdtMeta* other);
CrdtMeta* addOrCreateMeta(CrdtMeta* target, CrdtMeta* other);

VectorClock* rdbLoadVectorClock(RedisModuleIO *rdb);
int rdbSaveVectorClock(RedisModuleIO *rdb, VectorClock* clock);
#endif //XREDIS_CRDT_CRDT_UTIL_H