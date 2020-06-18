#ifndef XREDIS_CRDT_CRDT_UTIL_H
#define XREDIS_CRDT_CRDT_UTIL_H
#include "include/redismodule.h"
#include "ctrip_vector_clock.h"
#include "utils.h"
#include "crdt.h"
#include "ctrip_crdt_common.h"
int redisModuleStringToGid(RedisModuleCtx *ctx, RedisModuleString *argv, long long *gid);
CrdtMeta* getMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index);
RedisModuleKey* getWriteRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type);
void* getCurrentValue(RedisModuleKey *moduleKey);
void* getTombstone(RedisModuleKey *moduleKey);
CrdtMeta* mergeMeta(CrdtMeta* target, CrdtMeta* other, int* compare);
CrdtMeta* addOrCreateMeta(CrdtMeta* target, CrdtMeta* other);

VectorClock rdbLoadVectorClock(RedisModuleIO *rdb);
int rdbSaveVectorClock(RedisModuleIO *rdb, VectorClock clock);

size_t feedBuf(char* buf, char* src);
size_t feedStr2Buf(char *buf, char* str, size_t strlen);
size_t feedRobj2Buf(char *buf, RedisModuleString* src);
size_t feedGid2Buf(char *buf, int gid);
size_t feedLongLong2Buf(char *buf, long long v);
size_t feedVectorClock2Buf(char *buf, VectorClock vc);
size_t feedMeta2Buf(char *buf, int gid, long long time, VectorClock vc);
size_t feedArgc(char* buf, int argc);
size_t feedKV2Buf(char *buf,char* keystr, size_t keylen, char* valstr, size_t vallen);
RedisModuleKey* getRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type, int mode);
#endif //XREDIS_CRDT_CRDT_UTIL_H