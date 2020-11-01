#include "ctrip_crdt_zset.h"
#include "util.h"
#include "crdt_util.h"
#include "include/rmutil/ziplist.h"
#include "include/rmutil/redisassert.h"
#include <math.h>


/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */



/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/


int isCrdtSSTombstone(CrdtTombstone* tom) {
    if(tom != NULL && getType(tom) == CRDT_TOMBSTONE && (getDataType(tom) ==  CRDT_SET_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}





/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/
int feedSendZaddCommand(RedisModuleCtx* ctx, CrdtMeta* meta, RedisModuleString **argv) {

}


//zadd <key> <sorted> <field>
int zaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    int result = 0;
    double *scores = NULL;
    CrdtMeta zadd_meta = {.gid = 0};
    int scoreidx = 2;
    int elements = (argc - scoreidx)/2;
    scores = RedisModule_Alloc(sizeof(double)*elements);
    for(int i = 0; i < elements; i+=2) {
        if(RedisModule_StringToDouble(argv[i * 2 + scoreidx], &scores[i]) != REDISMODULE_OK) {
            RedisModule_WrongArity(ctx);
            goto error;
        }
    }
    //get value
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    CRDT_SS* current = getCurrentValue(moduleKey);
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone) ) {
        tombstone = NULL;
    }
    initIncrMeta(&zadd_meta);
    if(current == NULL) {
        current = create_crdt_zset();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSS, current);
    } 
    for(int i = 0; i < elements; i++) {
        zsetAdd(current, tombstone, &zadd_meta, RedisModule_GetSds(argv[i*2 + scoreidx + 1]), scores[i]);
    }
    sds vc_info = vectorClockToSds(getMetaVectorClock(&zadd_meta));
    feedSendZaddCommand(ctx, &zadd_meta, argv);
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.zadd", "sllc", argv[1], getMetaGid(&zadd_meta), getMetaTimestamp(&zadd_meta), vc_info);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "zadd", argv[1]);
error:
    if(scores != NULL) RedisModule_ZFree(scores);
    if(zadd_meta.gid != 0) freeIncrMeta(&zadd_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
}



int initCrdtSSModule(RedisModuleCtx *ctx) {
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtSS, 
        .rdb_save = RdbSaveCrdtSS,
        .aof_rewrite = AofRewriteCrdtSS,
        .mem_usage = crdtSSMemUsageFunc,
        .free = freeCrdtSS,
        .digest = crdtSSDigestFunc
    };
    CrdtSS = RedisModule_CreateDataType(ctx, CRDT_SS_DATATYPE_NAME, 0, &tm);
    if (CrdtSS == NULL) return REDISMODULE_ERR;
    RedisModuleTypeMethods tombtm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtSST,
        .rdb_save = RdbSaveCrdtSST,
        .aof_rewrite = AofRewriteCrdtSST,
        .mem_usage = crdtSSTMemUsageFunc,
        .free = freeCrdtSST,
        .digest = crdtSSTDigestFunc,
    };
    CrdtSST = RedisModule_CreateDataType(ctx, CRDT_SS_TOMBSTONE_DATATYPE_NAME, 0, &tombtm);
    if (CrdtSST == NULL) return REDISMODULE_ERR;
    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
    if (RedisModule_CreateCommand(ctx,"zadd",
                                  zaddCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx,"CRDT.zadd",
    //                               CRDT_ZaddCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx,"GET",
    //                               getCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx,"CRDT.GET",
    //                               CRDT_GetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx,"CRDT.DEL_Rc",
    //                               CRDT_DelRcCommand,"write",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx, "SETEX", 
    //                                 setexCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "incrby",
    //                                 incrbyCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "incrbyfloat",
    //                                 incrbyfloatCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "incr",
    //                                 incrCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "decr",
    //                                 decrCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "decrby",
    //                                 decrbyCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "crdt.counter",
    //                                 CRDT_CounterCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "MGET", 
    //                                 mgetCommand, "readonly fast",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
