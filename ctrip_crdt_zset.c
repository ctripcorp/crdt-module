#include "ctrip_crdt_zset.h"
#include "util.h"
#include "crdt_util.h"
#include "include/rmutil/ziplist.h"
#include <assert.h>
#include <math.h>
#include <string.h>


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
    return 1;
}


//zadd <key> <sorted> <field>
int zaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || argc % 2 != 0) return RedisModule_WrongArity(ctx);
    int result = 0;
    
    CrdtMeta zadd_meta = {.gid = 0};
    int scoreidx = 2;
    int elements = (argc - scoreidx)/2;
    // scores = RedisModule_Alloc(sizeof(double)*elements);
    double scores[elements];
    for(int i = 0; i < elements; i+=2) {
        if(RedisModule_StringToDouble(argv[i * 2 + scoreidx], &scores[i]) != REDISMODULE_OK) {
            // RedisModule_WrongArity(ctx);
            return RedisModule_ReplyWithError(ctx, "ERR value is not a valid float");
        }
    }
    //get value
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        goto error; 
    }
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
        result += zsetAdd(current, tombstone, &zadd_meta, RedisModule_GetSds(argv[i*2 + scoreidx + 1]), scores[i]);
    }
    // sds vc_info = vectorClockToSds(getMetaVectorClock(&zadd_meta));
    feedSendZaddCommand(ctx, &zadd_meta, argv);
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.zadd", "sllc", argv[1], getMetaGid(&zadd_meta), getMetaTimestamp(&zadd_meta), vc_info);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "zadd", argv[1]);
    RedisModule_ReplyWithLongLong(ctx, result);
error:
    // if(scores != NULL) RedisModule_ZFree(scores);
    if(zadd_meta.gid != 0) freeIncrMeta(&zadd_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
}
/**
 * ZSCORE <key> <field>
*/
int zscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    double result = 0;
    if(moduleKey == NULL) {
        goto end;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(current) {
        result = getScore(current, RedisModule_GetSds(argv[2]));
    } 
end:
    return RedisModule_ReplyWithDouble(ctx, result);
}
/**
 *  zcard 
 */ 
int zcardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    long long result = 0;
    if(moduleKey == NULL) {
        goto end;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(current) {
        result = getZSetSize(current);
    } 
end:
    return RedisModule_ReplyWithLongLong(ctx, result);
}
/**
 * ZINCRBY <key> <score> <field>
 */ 
int zincrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
     double result = 0;
    if(argc != 4) { return RedisModule_WrongArity(ctx);}
    CrdtMeta zadd_meta = {.gid = 0};
    double score = 0;
    if(RedisModule_StringToDouble(argv[2], &score) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "ERR value is not a valid float");
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        goto error;
    }
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
    result = zsetIncr(current, tombstone, &zadd_meta, RedisModule_GetSds(argv[3]), score);
    RedisModule_ReplyWithDouble(ctx, result);
error:
    if(zadd_meta.gid != 0) freeIncrMeta(&zadd_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}


int crdtZSetDelete(int dbId, void* keyRobj, void *key, void *value) {
    RedisModuleKey *moduleKey = (RedisModuleKey *)key;
    CrdtMeta del_meta = {.gid = 0};
    initIncrMeta(&del_meta);
    VectorClock lastVc = getCrdtSSLastVc(value);
    appendVCForMeta(&del_meta, lastVc);
    CRDT_SSTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isCrdtSSTombstone(tombstone)) {
        tombstone = create_crdt_ss_tombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSST, tombstone);
    }
    int len = get_len(lastVc);
    sds del_counters[len*3];
    int dlen = initSSTombstoneFromSS(tombstone, &del_meta, value, del_counters);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&del_meta));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_SS", "sllca", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds, del_counters, (size_t)dlen);
    sdsfree(vcSds);
    freeIncrMeta(&del_meta);
    return CRDT_OK;
}
//zrange key start end 
int zrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long start;
    long long end;
    int withscores = 0;
    int reverse = 0;
    if(RedisModule_StringToLongLong(argv[2], &start) != REDISMODULE_OK) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_ERR;
    }
    if(RedisModule_StringToLongLong(argv[3], &end) != REDISMODULE_OK) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_ERR;
    }
    if(argc == 5 && !strcasecmp(RedisModule_GetSds(argv[4]), "withscores")) {
        withscores = 1;
    }
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if(moduleKey == NULL) {
        return RedisModule_ReplyWithArray(ctx , 0);
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    size_t llen = getZSetSize(current);
    if(start < 0 ) start = llen + start;
    if(end < 0) end = llen + end;
    if(start < 0) start = 0;
    if(start > end || start >= llen) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_ERR;
    }
    if (end >= llen) end = llen-1;
    size_t rangelen = (end-start)+1;
    RedisModule_ReplyWithArray(ctx, withscores ? (rangelen*2) : rangelen);
    zskiplistNode *ln = zset_get_zsl_element_by_rank(current, reverse, start);

    // if (reverse) {
    //     ln = zsl->tail;
    //     if (start > 0)
    //         ln = zslGetElementByRank(zsl,llen-start);
    // } else {
    //     ln = zsl->header->level[0].forward;
    //     if (start > 0)
    //         ln = zslGetElementByRank(zsl,start+1);
    // }
    while(rangelen--) {
        // serverAssertWithInfo(c,zobj,ln != NULL);
        sds ele = ln->ele;
        RedisModule_ReplyWithStringBuffer(ctx,ele,sdslen(ele));
        if (withscores)
            RedisModule_ReplyWithDouble(ctx,ln->score);
        ln = reverse ? ln->backward : ln->level[0].forward;
    }
    return 1;
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
    if (RedisModule_CreateCommand(ctx,"ZSCORE",
                                  zscoreCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"ZCARD",
                                  zcardCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"zincrby",
                                  zincrbyCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"ZRANGE",
                                  zrangeCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
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
