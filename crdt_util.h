#ifndef XREDIS_CRDT_CRDT_UTIL_H
#define XREDIS_CRDT_CRDT_UTIL_H
#include "include/redismodule.h"
#include "ctrip_vector_clock.h"
#include "utils.h"
#include "crdt.h"
#include "ctrip_crdt_common.h"
/*
####
    replication util
####
*/
#define REPLICATION_ARGC_LEN (25)  //*(1) + long long(21) + \r(1) + \n(1)
#define REPLICATION_MAX_STR_LEN (27) //$(1) + long long(21) + \r(1) + \n(1) + strlen(?) + \r(1) + \n(1)
#define REPLICATION_MAX_GID_LEN  (9)//$(1) + gidlen(2) + \r(1) + \n(1) + gid(2) + \r(1) + \n(1)
#define REPLICATION_MAX_LONGLONG_LEN  (28)//$(1) + long long max len 21(2) + \r(1) + \n(1) + long long(21) + \r(1) + \n(1)
#define REPLICATION_MAX_VC_LEN (402) //$(1) + long long(21) + \r(1) + \n(1) + [;(1) + gid(2) + :(1) + long long(21)] * 15 + \r(1) +\n(1)

int redisModuleStringToGid(RedisModuleCtx *ctx, RedisModuleString *argv, long long *gid);
CrdtMeta* getMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index);
int readMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index, CrdtMeta* meta);
RedisModuleKey* getWriteRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type);
void* getCurrentValue(RedisModuleKey *moduleKey);
void* getTombstone(RedisModuleKey *moduleKey);
CrdtMeta* mergeMeta(CrdtMeta* target, CrdtMeta* other, int* compare);
CrdtMeta* addOrCreateMeta(CrdtMeta* target, CrdtMeta* other);

VectorClock rdbLoadVectorClock(RedisModuleIO *rdb, int version);
int rdbSaveVectorClock(RedisModuleIO *rdb, VectorClock clock, int version);

size_t feedBuf(char* buf,const char* src, size_t size);
size_t feedStr2Buf(char *buf, const char* str, size_t strlen);
size_t feedRobj2Buf(char *buf, RedisModuleString* src);
size_t feedGid2Buf(char *buf, int gid);
size_t feedLongLong2Buf(char *buf, long long v);
size_t feedVectorClock2Buf(char *buf, VectorClock vc);
size_t feedMeta2Buf(char *buf, int gid, long long time, VectorClock vc);
size_t feedArgc(char* buf, int argc);
size_t feedKV2Buf(char *buf,const char* keystr, size_t keylen,const char* valstr, size_t vallen);
RedisModuleKey* getRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type, int mode);
#endif //XREDIS_CRDT_CRDT_UTIL_H