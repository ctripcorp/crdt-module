#ifndef XREDIS_CRDT_CRDT_UTIL_H
#define XREDIS_CRDT_CRDT_UTIL_H
#include <redismodule.h>
#include <rmutil/dict.h>
#include <rmutil/adlist.h>
#include "ctrip_vector_clock.h"
#include "utils.h"
#include "crdt.h"
#include "ctrip_crdt_common.h"

#ifdef __LP64__
#define ULONG_MAX       0xffffffffffffffffUL    /* max unsigned long */
#define LONG_MAX        0x7fffffffffffffffL     /* max signed long */
#define LONG_MIN        (-0x7fffffffffffffffL-1) /* min signed long */
#else /* !__LP64__ */
#define ULONG_MAX       0xffffffffUL    /* max unsigned long */
#define LONG_MAX        2147483647L     /* max signed long */
#define LONG_MIN        (-2147483647L-1) /* min signed long */
#endif /* __LP64__ */

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


#define crdtAssert(_e) ((_e)?(void)0 : (_crdtAssert(#_e,__FILE__,__LINE__),exit(1)))
void _crdtAssert(char *estr, char *file, int line);

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
long double rdbLoadLongDouble(RedisModuleIO *rdb, int version);
int rdbSaveLongDouble(RedisModuleIO *rdb, long double ld);

size_t feedBuf(char* buf,const char* src, size_t size);
size_t feedStr2Buf(char *buf, const char* str, size_t strlen);
size_t feedRobj2Buf(char *buf, RedisModuleString* src);
size_t feedGid2Buf(char *buf, int gid);
size_t feedLongLong2Buf(char *buf, long long v);
size_t feedVectorClock2Buf(char *buf, VectorClock vc);
size_t feedMeta2Buf(char *buf, int gid, long long time, VectorClock vc);
size_t feedArgc(char* buf, int argc);
size_t feedKV2Buf(char *buf,const char* keystr, size_t keylen,const char* valstr, size_t vallen);
RedisModuleKey* getRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type, int mode, int* replyed);

//about dict
uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1,
                      const void *key2);
void dictSdsDestructor(void *privdata, void *val);

//cursor
int parseScanCursorOrReply(RedisModuleCtx *ctx, RedisModuleString *inputCursor, unsigned long *cursor);
typedef void (*ScanCallbackFunc)(void *privdata, const dictEntry *de);
void scanGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, dict *ht, int has_value, unsigned long cursor, ScanCallbackFunc scanCallback);


// object utils
int getLongLongFromObjectOrReply(RedisModuleCtx *ctx, RedisModuleString *o, long long *target, const char *msg);
int getLongFromObjectOrReply(RedisModuleCtx *ctx, RedisModuleString *o, long *target, const char *msg);

void replyEmptyScan(RedisModuleCtx *ctx);
#endif //XREDIS_CRDT_CRDT_UTIL_H