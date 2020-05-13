#ifndef XREDIS_CRDT_EXPIRE_H
#define XREDIS_CRDT_EXPIRE_H
#include "ctrip_crdt_common.h"
#include "crdt.h"
#include "crdt_util.h"
#include <assert.h>
#include "include/redismodule.h"
#define CRDT_EXPIRE_DATATYPE_NAME "crdt_expi"
#define CRDT_EXPIRE_TOMBSTONE_DATATYPE_NAME "crdt_expt"





//utils
void setExpire(RedisModuleKey *key, CrdtData *data, long long expiteTime);
int trySetExpire(RedisModuleKey* moduleKey, long long  time, int type, long long expireTime) ;

//debug
// int crdtGetExpireTombstoneCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc);
#endif 