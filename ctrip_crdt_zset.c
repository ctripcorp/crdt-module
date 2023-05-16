#include "ctrip_crdt_zset.h"
#include "util.h"
#include "crdt_util.h"
#include <rmutil/ziplist.h>
#include <rmutil/sds.h>
#include <assert.h>
#include <math.h>
#include <string.h>

static RedisModuleType *CrdtSS;
static RedisModuleType *CrdtSST;

CrdtTombstoneMethod ZsetTombstoneCommonMethod = {
    .merge = crdtSSTMerge,
    .filterAndSplit =  crdtSSTFilter,
    .filterAndSplit2 =  crdtSSTFilter2,
    .freefilter = freeSSTFilter,
    .gc = crdtZsetTombstoneGc,
    .purge = crdtZsetTombstonePurge,
    .info = crdtZsetTombstoneInfo,
    .getVc = clone_sst_vc,
};

CrdtObjectMethod ZSetCommandMethod = {
    .merge = crdtSSMerge,
    .filterAndSplit = crdtSSFilter,
    .filterAndSplit2 = crdtSSFilter2,
    .freefilter = freeSSFilter,
};

CrdtDataMethod ZSetDataMethod = {
    .propagateDel = crdtZSetDelete,
    .info = crdtZSetInfo,
    .getLastGid = NULL,
};

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/


int isCrdtSSTombstone(CrdtTombstone* tom) {
    if(tom != NULL && getType(tom) == CRDT_TOMBSTONE && (getDataType(tom) ==  CRDT_ZSET_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}





/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/
//crdt.zadd key gid time  vc  field <field score del_counter>
const char* crdt_zadd_head = "$9\r\nCRDT.ZADD\r\n";
int replicationFeedCrdtZaddCommand(RedisModuleCtx* ctx,  char* cmdbuf, CrdtMeta* meta, sds key, sds* callback, int callback_len) {
    // sds vcSds = vectorClockToSds(getMetaVectorClock(meta));
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.zadd", "sllca", key,  getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, callback, callback_len);
    // sdsfree(vcSds);
    static size_t crdt_zadd_head_size = 0;
    if(crdt_zadd_head_size == 0) {
        crdt_zadd_head_size = strlen(crdt_zadd_head);
    }

    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, callback_len + 5);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_zadd_head, crdt_zadd_head_size);
    // cmdlen += feedBuf(cmdbuf + cmdlen, crdt_zadd_head, crdt_zadd_head_size);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    for(int i = 0; i<callback_len; i++) {
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, callback[i], sdslen(callback[i]));
        sdsfree(callback[i]);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}

int replicationCrdtZaddCommand(RedisModuleCtx* ctx, CrdtMeta* meta, sds key, sds* callback, int callback_len, int callback_byte_size) {
    size_t alllen = 16  
    + sdslen(key)
    + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN
    + (callback_len + 1) * (REPLICATION_MAX_LONGLONG_LEN + 2) 
    + callback_byte_size ;
    char* cmdbuf = RedisModule_GetSharedBuffer(alllen);
    if(cmdbuf == NULL) {
        cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtZaddCommand(ctx, cmdbuf, meta, key, callback, callback_len);
        RedisModule_Free(cmdbuf);
    } else {
        replicationFeedCrdtZaddCommand(ctx, cmdbuf, meta, key, callback, callback_len);
        RedisModule_ReturnSharedBuffer(cmdbuf);
    }
    return 1;
}

//CRDT.DEL_SS key gid time vc 
const char* crdt_del_head = "$11\r\nCRDT.DEL_SS\r\n";
int replicationFeedCrdtDelSSCommand(int dbId,  char* cmdbuf, CrdtMeta* meta, sds key, sds* callback, int callback_len) {
    // sds vcSds = vectorClockToSds(getMetaVectorClock(meta));
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.zadd", "sllca", key,  getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, callback, callback_len);
    // sdsfree(vcSds);
    static size_t crdt_del_head_size = 0;
    if(crdt_del_head_size == 0) {
        crdt_del_head_size = strlen(crdt_del_head);
    }

    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, callback_len + 5);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_del_head, crdt_del_head_size);
    // cmdlen += feedBuf(cmdbuf + cmdlen, crdt_zadd_head, crdt_zadd_head_size);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    for(int i = 0; i<callback_len; i++) {
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, callback[i], sdslen(callback[i]));
        sdsfree(callback[i]);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(dbId, cmdbuf, cmdlen);
    return cmdlen;
}

// replicationCrdtDelSSCommand(dbId, &del_meta, RedisModule_GetSds(keyRobj), del_counters, dlen)
int replicationCrdtDelSSCommand(int dbId, CrdtMeta* meta, sds key, sds* callback, int callback_len) {
    int callback_byte_size = 0;
    for(int i = 0; i < callback_len; i++) {
        callback_byte_size += sdslen(callback[i]);
    }
    size_t alllen = 18  
    + sdslen(key)
    + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN
    + (callback_len + 1) * (REPLICATION_MAX_LONGLONG_LEN + 2) 
    + callback_byte_size ;
    char* cmdbuf = RedisModule_GetSharedBuffer(alllen);
    if(cmdbuf == NULL) {
        cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtDelSSCommand(dbId, cmdbuf, meta, key, callback, callback_len);
        RedisModule_Free(cmdbuf);
    } else {
        replicationFeedCrdtDelSSCommand(dbId, cmdbuf, meta, key, callback, callback_len);
        RedisModule_ReturnSharedBuffer(cmdbuf);
    }
    return 1;
}

const char* crdt_zincr_head = "$12\r\nCRDT.Zincrby\r\n";
int replicationFeedCrdtZincrCommand(RedisModuleCtx* ctx,  char* cmdbuf, CrdtMeta* meta, sds key, sds* callback, int callback_len) {
    // sds vcSds = vectorClockToSds(getMetaVectorClock(meta));
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.zincrby", "sllca", key,  getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, callback, callback_len);
    // sdsfree(vcSds);
    static size_t crdt_zincr_head_size = 0;
    if(crdt_zincr_head_size == 0) {
        crdt_zincr_head_size = strlen(crdt_zincr_head);
    }

    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, callback_len + 5);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_zincr_head, crdt_zincr_head_size);
    // cmdlen += feedBuf(cmdbuf + cmdlen, crdt_zadd_head, crdt_zadd_head_size);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    for(int i = 0; i<callback_len; i++) {
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, callback[i], sdslen(callback[i]));
        sdsfree(callback[i]);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}


int replicationCrdtZincrCommand(RedisModuleCtx* ctx, CrdtMeta* meta, sds key, sds* callback, int callback_len, int callback_byte_size) {
    size_t alllen = 20 
    + sdslen(key)
    + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN
    + (callback_len + 1) * (REPLICATION_MAX_LONGLONG_LEN + 2) 
    + callback_byte_size ;
    char* cmdbuf = RedisModule_GetSharedBuffer(alllen);
    if(cmdbuf == NULL) {
        cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtZincrCommand(ctx, cmdbuf, meta, key, callback, callback_len);
        RedisModule_Free(cmdbuf);
    } else {
        replicationFeedCrdtZincrCommand(ctx, cmdbuf, meta, key, callback, callback_len);
        RedisModule_ReturnSharedBuffer(cmdbuf);
    }
    return 1;
}

const char* crdt_zrem_head = "$9\r\nCRDT.Zrem\r\n";
int replicationFeedCrdtZremCommand(RedisModuleCtx* ctx,  char* cmdbuf, CrdtMeta* meta, sds key, sds* callback, int callback_len) {
    static size_t crdt_zrem_head_size = 0;
    if(crdt_zrem_head_size == 0) {
        crdt_zrem_head_size = strlen(crdt_zrem_head);
    }
    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, callback_len + 5);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_zrem_head, crdt_zrem_head_size);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    for(int i = 0; i<callback_len; i++) {
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, callback[i], sdslen(callback[i]));
        sdsfree(callback[i]);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int replicationCrdtZremCommand(RedisModuleCtx* ctx, sds key, CrdtMeta* meta ,sds* callback, int callback_len,int callback_byte_size) {
    size_t alllen = 20 
    + sdslen(key)
    + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN
    + (callback_len + 1) * (REPLICATION_MAX_LONGLONG_LEN + 2) 
    + callback_byte_size ;
    char* cmdbuf = RedisModule_GetSharedBuffer(alllen);
    if(cmdbuf == NULL) {
        cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtZremCommand(ctx, cmdbuf, meta, key, callback, callback_len);
        RedisModule_Free(cmdbuf);
    } else {
        replicationFeedCrdtZremCommand(ctx, cmdbuf, meta, key, callback, callback_len);
        RedisModule_ReturnSharedBuffer(cmdbuf);
    }
    return 1;
}

//zadd <key> <sorted> <field>
int zaddGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    // if (argc < 4 || argc % 2 != 0) return RedisModule_WrongArity(ctx);
    int result = 0;
    double score = 0, newscore;
    // double* scores = NULL;
    CrdtMeta zadd_meta = {.gid = 0};
    int scoreidx = 2;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */
    //get value
    RedisModuleKey* moduleKey = NULL;
    while(scoreidx < argc) {
        char *opt = RedisModule_GetSds(argv[scoreidx]);
        if (!strcasecmp(opt,"nx")) flags |= ZADD_NX;
        else if (!strcasecmp(opt,"xx")) flags |= ZADD_XX;
        else if (!strcasecmp(opt,"ch")) flags |= ZADD_CH;
        else if (!strcasecmp(opt,"incr")) flags |= ZADD_INCR;
        else break;
        scoreidx++;
    }
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;
     /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    int elements = argc - scoreidx;
    if(elements % 2 || !elements) {
        return RedisModule_ReplyWithError(ctx, "ERR syntax error");
    }
    elements /= 2; /* Now this holds the number of score-element pairs. */
    sds callback_items[elements*2];
    int callback_len = 0;
    int callback_byte_size = 0;
    if (nx && xx) {
        return RedisModule_ReplyWithError(ctx,
            "ERR XX and NX options at the same time are not compatible");
    }
    if (incr && elements > 1) {
        return RedisModule_ReplyWithError(ctx,
            "ERR INCR option supports a single increment-element pair");
    }
    double scores[elements];
    sds keys[elements];
    for(int i = 0; i < elements; i+=1) {
        if(RedisModule_StringToDouble(argv[i * 2 + scoreidx], &scores[i]) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "ERR value is not a valid float");
            goto error;
        }
        
        int add = 1;
        sds key = RedisModule_GetSds(argv[i * 2 + scoreidx + 1]);
        for(int j = i + 1; j < elements; j+=1) {
            if(sdscmp(key, RedisModule_GetSds(argv[j * 2 + scoreidx + 1])) == 0) {
                add = 0;
            }
        }
        if(add) {
            keys[i] = key;
        } else {
            keys[i] = NULL;
        }
    }

    moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        goto cleanup; 
    }
   
    CRDT_SS* current = getCurrentValue(moduleKey);
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone) ) {
        tombstone = NULL;
    }
    initIncrMeta(&zadd_meta);
    if(current == NULL) {
        if(xx) goto reply_to_client;
        current = createCrdtZset();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSS, current);
        if(tombstone) {
            appendVCForMeta(&zadd_meta, getCrdtSSTLastVc(tombstone));
        }
    } else {
        appendVCForMeta(&zadd_meta, getCrdtSSLastVc(current));
    }
    for(int i = 0; i < elements; i++) {
        if(keys[i] == NULL) { continue; }
        int retflags = flags;
        score = scores[i];
        sds callback_item = zsetAdd2(current, tombstone, &zadd_meta, keys[i], &retflags, score, &newscore);
        if(callback_item == NULL) {
            RedisModule_ReplyWithError(ctx, nanerr);
            goto cleanup;
        }
        if(sdslen(callback_item) != 0) {
            callback_items[callback_len++] = sdsdup(keys[i]);
            callback_items[callback_len++] = callback_item;
            callback_byte_size += sdslen(keys[i]) + sdslen(callback_item);
        } else {
            sdsfree(callback_item);
        }
        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;
        score = newscore;
    }
    updateCrdtSSLastVc(current, getMetaVectorClock(&zadd_meta));

    if(tombstone) {
        if(isNullZsetTombstone(tombstone)) {
            RedisModule_DeleteTombstone(moduleKey);
            tombstone = NULL;
        } else {
            zsetTombstoneTryResizeDict(tombstone);
        }
    }
    
    if (incr) {
        replicationCrdtZincrCommand(ctx, &zadd_meta, RedisModule_GetSds(argv[1]), callback_items, callback_len, callback_byte_size);
    } else {
        replicationCrdtZaddCommand(ctx, &zadd_meta, RedisModule_GetSds(argv[1]), callback_items, callback_len, callback_byte_size);
    }


reply_to_client:
    if (incr) {
        if (processed) {
            RedisModule_ReplyWithDouble(ctx, score);
        } else {
            RedisModule_ReplyWithNull(ctx);
        }
    } else {
        RedisModule_ReplyWithLongLong(ctx, ch ? added+updated : added);
    }
cleanup:
    if(added || updated) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_ZSET, incr? "zincr":"zadd", argv[1]);
    }
error:
    if(zadd_meta.gid != 0) freeIncrMeta(&zadd_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
}

int zaddCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    if(argc < 4) {return RedisModule_WrongArity(ctx);}
    return zaddGenericCommand(ctx, argv, argc, ZADD_NONE);
}

/**
 * ZINCRBY <key> <score> <field>
 */ 
int zincrbyCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {    
    if(argc != 4) {return RedisModule_WrongArity(ctx);}
    return zaddGenericCommand(ctx, argv, argc, ZADD_INCR);
}

//crdt.zadd key gid time vc  field <score del_counter> 
int crdtZaddCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    if (argc < 7 || argc % 2 == 0) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int need_add = 0;
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        current = createCrdtZset();
        if(tombstone) {
            updateCrdtSSLastVc(current, getCrdtSSTLastVc(tombstone));
        }
        need_add = 1;
    } 
    int result = 0;
    for(int i = 5; i < argc; i += 2) {
        sds field = RedisModule_GetSds(argv[i]);
        sds info = RedisModule_GetSds(argv[i+1]);
        result += zsetTryAdd(current, tombstone, field, &meta, info);
    }
    if(result == 0 ) {
        if(need_add) {
            freeCrdtSS(current);
            current = NULL;
        } 
    } else {
        if(need_add) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtSS, current);
        }
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "zadd", argv[1]);
    }
    if(current) {
        updateCrdtSSLastVc(current, getMetaVectorClock(&meta));
    }
    if(tombstone) {
        if(isNullZsetTombstone(tombstone)) {
            RedisModule_DeleteTombstone(moduleKey);
            tombstone = NULL;
        } else {
            zsetTombstoneTryResizeDict(tombstone);
        }
    }
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

//crdt.zincrby
int crdtZincrbyCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    if (argc < 7 || argc % 2 == 0) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    crdtAssert(meta.gid != 0);
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    int need_add = 0;
    if(current == NULL) {
        current = createCrdtZset();
        if(tombstone) {
            updateCrdtSSLastVc(current, getCrdtSSTLastVc(tombstone));
        }
        need_add = 1;
    } 
    int result = 0;
    for(int i = 5; i < argc; i += 2) {
        sds field = RedisModule_GetSds(argv[i]);
        sds score = RedisModule_GetSds(argv[i+1]);
        result += zsetTryIncrby(current, tombstone, field, &meta, score);
    }
    if(result == 0) {
        if(need_add) {
            freeCrdtSS(current);
            current = NULL;
        }
    } else {
        if(need_add) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtSS, current);
        }
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "zincr", argv[1]);
    }
    if(current) {
        updateCrdtSSLastVc(current, getMetaVectorClock(&meta));
    }
    if(tombstone) {
        if(isNullZsetTombstone(tombstone)) {
            RedisModule_DeleteTombstone(moduleKey);
            tombstone = NULL;
        } else {
            zsetTombstoneTryResizeDict(tombstone);
        }
    }
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}
/**
 * ZSCORE <key> <field>
*/
int zscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        goto error;
    }
    double result = 0;
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(current) {
        if (getScore(current, RedisModule_GetSds(argv[2]), &result)) {
            RedisModule_CloseKey(moduleKey);
            return RedisModule_ReplyWithDouble(ctx, result);
        }
    } 
error:
    RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithNull(ctx);
}

/**
 *  zcard 
 */ 
int zcardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    long long result = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        goto end;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(current) {
        result = crdtZsetLength(current);
    } 
end:
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}


int crdtZSetDelete(int dbId, void* keyRobj, void *key, void *value) {
    sds* del_counters = NULL;
    RedisModuleKey *moduleKey = (RedisModuleKey *)key;
    CrdtMeta del_meta = {.gid = 0};
    initIncrMeta(&del_meta);
    VectorClock lastVc = getCrdtSSLastVc(value);
    appendVCForMeta(&del_meta, lastVc);
    
    CRDT_SSTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isCrdtSSTombstone(tombstone)) {
        tombstone = createCrdtZsetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSST, tombstone);
    }
    int len = crdtZsetLength(value);
    
    del_counters = RedisModule_Alloc(len * sizeof(sds));
    int dlen = initSSTombstoneFromSS(tombstone, &del_meta, value, del_counters);
    crdtAssert(dlen <= len);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&del_meta));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_SS", "sllca", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds, del_counters, (size_t)dlen);
    sdsfree(vcSds);
    for(int i = 0; i < dlen; i++) {
        sdsfree(del_counters[i]);
    }
    freeIncrMeta(&del_meta);
    if(del_counters != NULL) RedisModule_Free(del_counters);
    return CRDT_OK;
}

//crdt.del_ss key gid timespace vc <field,gid:vcu:type:value>...
int crdtDelSSCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 5) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtZsetTombstone();
        if(current) {
            updateCrdtSSTLastVc(tombstone, getCrdtSSLastVc(current));
        }
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSST, tombstone);
    }
    for(int i = 5; i < argc; i += 1) {
        sds field_and_del_counter_info = RedisModule_GetSds(argv[i]);
        zsetTryRem(tombstone, current, field_and_del_counter_info, &meta);
    }
    zsetTryDel(current, tombstone, &meta);
    if(current) {
        if(crdtZsetLength(current) == 0) {
            RedisModule_DeleteKey(moduleKey);
        } else {
            updateCrdtSSLastVc(current, getMetaVectorClock(&meta));
            zsetTryResizeDict(current);
        }
    } 
    RedisModule_Debug(CRDT_DEFAULT_LOG_LEVEL, "crdt.del");
    updateCrdtSSTLastVc(tombstone, getMetaVectorClock(&meta));
    updateCrdtSSTMaxDel(tombstone, getMetaVectorClock(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "del", argv[1]);
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

//zrange key start end 
int zrangeGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    if(argc < 4) { return RedisModule_WrongArity(ctx); }
    long long start;
    long long end;
    int withscores = 0;
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
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        return RedisModule_ReplyWithArray(ctx , 0);
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    size_t llen = crdtZsetLength(current);
    if(start < 0 ) start = llen + start;
    if(end < 0) end = llen + end;
    if(start < 0) start = 0;
    if(start > end || start >= llen) {
        RedisModule_ReplyWithNull(ctx);
        goto end;
    }
    if (end >= llen) end = llen-1;
    size_t rangelen = (end-start)+1;
    RedisModule_ReplyWithArray(ctx, withscores ? (rangelen*2) : rangelen);
    zskiplistNode *ln = zset_get_zsl_element_by_rank(current, reverse, start);

    while(rangelen--) {
        sds ele = ln->ele;
        RedisModule_ReplyWithStringBuffer(ctx,ele,sdslen(ele));
        if (withscores)
            RedisModule_ReplyWithDouble(ctx,ln->score);
        ln = reverse ? ln->backward : ln->level[0].forward;
    }
end:
    RedisModule_CloseKey(moduleKey);
    return 1;
}

int zrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrangeGenericCommand(ctx, argv, argc, 0);
}

int zrevrangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrangeGenericCommand(ctx, argv, argc, 1);
}

//ZREM KEY FILED
int zremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 3) { return RedisModule_WrongArity(ctx);}
    CrdtMeta zrem_meta = {.gid = 0};
    int deleted = 0, keyremoved = 0, i;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        return REDISMODULE_ERR;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithLongLong(ctx , 0);
    }
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone) ) {
        tombstone = NULL;
    }
    initIncrMeta(&zrem_meta);
    appendVCForMeta(&zrem_meta, getCrdtSSLastVc(current));
    if(tombstone == NULL) {
        tombstone = createCrdtZsetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSST, tombstone);
    } 
    sds callback_items[argc-2];
    int callback_byte_size = 0;
    for(i = 2; i < argc; i++) {
        sds callback_item = zsetRem(current, tombstone, &zrem_meta, RedisModule_GetSds(argv[i]));
        if(callback_item) {
            callback_items[deleted++] = callback_item;
            callback_byte_size += sdslen(callback_item);
        } 
        
        if(crdtZsetLength(current) == 0) {
            RedisModule_DeleteKey(moduleKey);
            keyremoved = 1;
            break;
        }
    }
    if (deleted) {
        updateCrdtSSTLastVc(tombstone, getMetaVectorClock(&zrem_meta));
        
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_ZSET, "zrem", argv[1]);
        if (keyremoved) {
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_ZSET, "del", argv[1]);
        } else {
            updateCrdtSSLastVc(current, getMetaVectorClock(&zrem_meta));
            zsetTryResizeDict(current);
        }
        replicationCrdtZremCommand(ctx, RedisModule_GetSds(argv[1]), &zrem_meta ,callback_items, deleted, callback_byte_size);
    }
    RedisModule_ReplyWithLongLong(ctx, deleted);
    if(zrem_meta.gid != 0) freeIncrMeta(&zrem_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}
//crdt.zrem key git time vc [num key <gid:vcu:type:value>]...
int crdtZremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtZsetTombstone();
        if(current) {
            updateCrdtSSTLastVc(tombstone, getCrdtSSLastVc(current));
        }
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSST, tombstone);
    }
    int result = 0;
    for(int i = 5; i < argc; i += 1) {
        sds field_and_del_counter_info = RedisModule_GetSds(argv[i]);
        result += zsetTryRem(tombstone, current, field_and_del_counter_info, &meta);
    }
   
    if(current && result) {
        if(crdtZsetLength(current) == 0) {
            RedisModule_DeleteKey(moduleKey);
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "del", argv[1]);
            current = NULL;
        } else {
            zsetTryResizeDict(current);
        }
    } 
    if(tombstone && isNullZsetTombstone(tombstone)) {
        RedisModule_DeleteTombstone(moduleKey);
        tombstone = NULL;
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "zrem", argv[1]);
    if(tombstone) {
        updateCrdtSSTLastVc(tombstone, getMetaVectorClock(&meta));
    }
    if(current) {
        updateCrdtSSLastVc(current, getMetaVectorClock(&meta));
    }
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

int zrankGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    if(argc != 3) { return RedisModule_WrongArity(ctx); }
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        return RedisModule_ReplyWithNull(ctx);
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    long rank = zsetRank(current, RedisModule_GetSds(argv[2]),reverse);
    if ( rank >= 0 ) {
        RedisModule_ReplyWithLongLong(ctx, rank);
    } else {
        RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_CloseKey(moduleKey);
    return 1;
}
//zrankCommand key ele
int zrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrankGenericCommand(ctx, argv, argc, 0);
}

int zrevrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return zrankGenericCommand(ctx, argv, argc, 1);
}

int genericZrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    if(argc < 4) { return RedisModule_WrongArity(ctx); }
    zrangespec range;
    long long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    int minidx, maxidx;

    sds* fields = NULL;
    double* scores = NULL;

    if(reverse) {
        maxidx = 2; minidx = 3;
    } else {
        maxidx = 3; minidx = 2;
    }
    if (!zslParseRange(RedisModule_GetSds(argv[minidx]), RedisModule_GetSds(argv[maxidx]), &range)) {
        return RedisModule_ReplyWithError(ctx, "min or max is not a float");
    }
    if(argc > 4) {
        int remaining = argc - 4;
        int pos = 4;
        while (remaining) {
            if (remaining >= 1 && !strcasecmp(RedisModule_GetSds(argv[pos]),"withscores")) {
                pos++; remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !strcasecmp(RedisModule_GetSds(argv[pos]),"limit")) {
                sds offset_str = RedisModule_GetSds(argv[pos+1]);
                sds limit_str = RedisModule_GetSds(argv[pos+2]);
                if ((!string2ll(offset_str, sdslen(offset_str), &offset))
                         ||
                    (!string2ll(limit_str, sdslen(limit_str), &limit)
                        ))
                {
                    return RedisModule_WrongArity(ctx);
                }
                pos += 3; remaining -= 3;
            } else {
                return RedisModule_ReplyWithError(ctx,"-ERR syntax error");
            }
        }
    }
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    CRDT_SS* current = RedisModule_ModuleTypeGetValue(moduleKey);
    zskiplistNode* ln = zslInRange(current, &range, reverse);
    if(ln == NULL) {
        if(moduleKey) RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    while (ln && offset--) {
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }
    int max_len = crdtZsetLength(current);
    
    fields = RedisModule_Alloc(max_len * sizeof(sds));
    scores = RedisModule_Alloc(max_len * sizeof(double));
    while (ln && limit--) {
        /* Abort when the node is no longer in range. */
        if (reverse) {
            if (!zslValueGteMin(ln->score,&range)) break;
        } else {
            if (!zslValueLteMax(ln->score,&range)) break;
        }

        
        fields[rangelen] = ln->ele;
        if (withscores) {
            scores[rangelen] = ln->score;
        }
        rangelen++;
        /* Move to next node */
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }
    if (withscores) {
        RedisModule_ReplyWithArray(ctx, rangelen * 2);
    } else {
        RedisModule_ReplyWithArray(ctx, rangelen);
    }
    for(int i = 0; i < rangelen; i++) {
        RedisModule_ReplyWithStringBuffer(ctx,fields[i],sdslen(fields[i]));
        if(withscores) {
            RedisModule_ReplyWithDouble(ctx,scores[i]);
        }
    }
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if (fields != NULL) RedisModule_Free(fields);
    if (scores != NULL) RedisModule_Free(scores);
    return 1;
}

int zrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    return genericZrangebyscoreCommand(ctx, argv, argc, 0);
}

int zrevrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return genericZrangebyscoreCommand(ctx, argv, argc, 1);
}

//zcount key before after
int zcountCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 4) { return RedisModule_WrongArity(ctx); }
    zrangespec range;
    unsigned long rank;
    int count = 0;
    if (!zslParseRange(RedisModule_GetSds(argv[2]), RedisModule_GetSds(argv[3]), &range)) {
        return RedisModule_ReplyWithError(ctx, "min or max is not a float");
    }
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    CRDT_SS* current = RedisModule_ModuleTypeGetValue(moduleKey);
    zskiplistNode *zn = zslInRange(current, &range, 0);
    int all_len = crdtZsetLength(current);
    if(zn != NULL) {
        rank = zsetGetRank(current, zn->score, zn->ele);
        count = (all_len - (rank - 1));
        zn = zslInRange(current, &range, 1);
        /* Use rank of last element, if any, to determine the actual count */
        if (zn != NULL) {
            rank = zsetGetRank(current, zn->score, zn->ele);
            count -= (all_len - rank);
        }
    }
    RedisModule_ReplyWithLongLong(ctx, count);
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    return 1;
}




/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
int genericZrangebylexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int reverse) {
    if(argc < 4) { return RedisModule_WrongArity(ctx); }
    zlexrangespec range;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    int minidx, maxidx;
    sds* fields = NULL;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }
    if (!zslParseLexRange(RedisModule_GetSds(argv[minidx]),RedisModule_GetSds(argv[maxidx]),&range)) {
        RedisModule_ReplyWithError(ctx,"min or max not valid string range item");
        return 1;
    }
    if (argc > 4) {
        int remaining = argc - 4;
        int pos = 4;
        while (remaining) {
            if (remaining >= 3 && !strcasecmp(RedisModule_GetSds(argv[pos]),"limit")) {
                sds offset_str = RedisModule_GetSds(argv[pos+1]);
                sds limit_str = RedisModule_GetSds(argv[pos+2]);
                if (!(string2l(offset_str,sdslen(offset_str), &offset)) ||
                    !(string2l(limit_str, sdslen(limit_str), &limit))) {
                    return RedisModule_WrongArity(ctx);
                }
                pos += 3; remaining -= 3;
            } else {
                zslFreeLexRange(&range);
                return RedisModule_ReplyWithError(ctx, "ERR syntax error");
            }
        }
    }
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        zslFreeLexRange(&range);
        if(replyed) return CRDT_ERROR;
        return RedisModule_ReplyWithArray(ctx, 0);
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if (current == NULL) {
        zslFreeLexRange(&range);
        RedisModule_CloseKey(moduleKey);
        return 1;
    }
    zskiplistNode *ln = zslInLexRange(current, &range, reverse);

    /* No "first" element in the specified interval. */
    if (ln == NULL) {
        // addReply(c, shared.emptymultibulk);
        RedisModule_ReplyWithArray(ctx, 0);
        zslFreeLexRange(&range);
        RedisModule_CloseKey(moduleKey);
        return 1;
    }

    /* We don't know in advance how many matching elements there are in the
        * list, so we push this object that will represent the multi-bulk
        * length in the output buffer, and will "fix" it later */
    int max_len = crdtZsetLength(current);
    
    fields = RedisModule_Alloc(max_len * sizeof(sds));
    /* If there is an offset, just traverse the number of elements without
        * checking the score because that is done in the next loop. */
    while (ln && offset--) {
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }

    while (ln && limit--) {
        /* Abort when the node is no longer in range. */
        if (reverse) {
            if (!zslLexValueGteMin(ln->ele,&range)) break;
        } else {
            if (!zslLexValueLteMax(ln->ele,&range)) break;
        }
        fields[rangelen] = ln->ele;
        rangelen++;

        /* Move to next node */
        if (reverse) {
            ln = ln->backward;
        } else {
            ln = ln->level[0].forward;
        }
    }
    zslFreeLexRange(&range);
    RedisModule_ReplyWithArray(ctx, rangelen);
    for (int i = 0; i < rangelen; i++) {
        RedisModule_ReplyWithStringBuffer(ctx, fields[i], sdslen(fields[i]));
    }
    RedisModule_CloseKey(moduleKey);
    if (fields != NULL) RedisModule_Free(fields);
    return 1;
    
}

int zrangebylexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return genericZrangebylexCommand(ctx, argv, argc, 0);
}

int zrevrangebylexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return genericZrangebylexCommand(ctx,argv, argc, 1);
}

int zlexcountCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 4) { return RedisModule_WrongArity(ctx); }
    zlexrangespec range;
    int count = 0;
    /* Parse the range arguments */
    if (!zslParseLexRange(RedisModule_GetSds(argv[2]),RedisModule_GetSds(argv[3]),&range)) {
        return RedisModule_ReplyWithError(ctx,"min or max not valid string range item");
    }
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        zslFreeLexRange(&range);
        if(replyed) return CRDT_ERROR;
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    CRDT_SS* current = getCurrentValue(moduleKey);

    zskiplistNode *zn;
    unsigned long rank;

    /* Find first element in range */
    zn = zslInLexRange(current, &range, 0);
    // crdt_zset* zset = (crdt_zset*)current;
    size_t length = crdtZsetLength(current);

    /* Use rank of first element, if any, to determine preliminary count */
    if (zn != NULL) {
        // rank = zslGetRank(zset->zsl, zn->score, zn->ele);
        rank = zsetGetRank(current, zn->score, zn->ele);
        count = (length - (rank - 1));

        /* Find last element in range */
        zn = zslInLexRange(current, &range, 1);

        /* Use rank of last element, if any, to determine the actual count */
        if (zn != NULL) {
            // rank = zslGetRank(zset->zsl, zn->score, zn->ele);
            rank = zsetGetRank(current, zn->score, zn->ele);
            count -= (length - rank);
        }
    }
    zslFreeLexRange(&range);
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, count);
}

void zscanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    list *keys = pd[0];
    sds key = NULL, val = NULL;
    key = sdsdup(dictGetKey(de));
    double score = getZScoreByDictEntry((dictEntry *)de);
    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = d2string(buf, sizeof(buf), score);
    val = sdsnewlen(buf, len);
    listAddNodeTail(keys, key);
    listAddNodeTail(keys, val);
}


//zscan key 
int zscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        replyEmptyScan(ctx);
        return CRDT_ERROR;
    }
    CRDT_SS* zset = getCurrentValue(moduleKey);
    if(zset == NULL) {
        replyEmptyScan(ctx);
        RedisModule_CloseKey(moduleKey);
        return REDISMODULE_OK;
    }
    unsigned long cursor;
    if (parseScanCursorOrReply(ctx, argv[2], &cursor) == CRDT_ERROR) return 0;

    scanGenericCommand(ctx, argv, argc, getZsetDict(zset), 1, cursor, zscanCallback);

    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}


#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2
int zremrangeGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int rangetype) {

    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long long start, end, llen;
    CrdtMeta meta = {.gid = 0};
    sds* callback_items = NULL;
    /* Step 1: Parse the range. */
    if (rangetype == ZRANGE_RANK) {
        if(RedisModule_StringToLongLong(argv[2], &start) != REDISMODULE_OK ||
            RedisModule_StringToLongLong(argv[3], &end) != REDISMODULE_OK) {
                return RedisModule_ReplyWithError(ctx,"start or end is not a long long");
            }
    } else if (rangetype == ZRANGE_SCORE) {
        if (!zslParseRange(RedisModule_GetSds(argv[2]),RedisModule_GetSds(argv[3]),&range)) {
            return RedisModule_ReplyWithError(ctx,"min or max is not a float");
        }
    } else if (rangetype == ZRANGE_LEX) {
        if (!zslParseLexRange(RedisModule_GetSds(argv[2]),RedisModule_GetSds(argv[3]),&lexrange)) {
            return RedisModule_ReplyWithError(ctx,"min or max not valid string range item");
        }
    }
    /* Step 2: Lookup & range sanity checks if needed. */
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSS);
    if(moduleKey == NULL) {
        if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
        return REDISMODULE_ERR;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    if(!current) {
        RedisModule_CloseKey(moduleKey);
        if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
        return RedisModule_ReplyWithLongLong(ctx , 0);
    }
    
    llen = crdtZsetLength(current);
    
    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen) {
            RedisModule_ReplyWithLongLong(ctx, 0);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }
    /* Step 3: Perform the range deletion operation. */
    
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtSSTombstone(tombstone) ) {
        tombstone = NULL;
    }
    if(tombstone == NULL) {
        tombstone = createCrdtZsetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSST, tombstone);
    }
    initIncrMeta(&meta);
    appendVCForMeta(&meta, getCrdtSSLastVc(current));
    
    long long callback_byte_size = 0;
    callback_items = RedisModule_Alloc(sizeof(sds) * llen);
    switch(rangetype) {
    case ZRANGE_RANK:
        deleted = zslDeleteRangeByRank(current, tombstone, &meta, start+1, end+1, callback_items, &callback_byte_size);
        break;
    case ZRANGE_SCORE:
        deleted = zslDeleteRangeByScore(current, tombstone, &meta, &range, callback_items, &callback_byte_size);
        break;
    case ZRANGE_LEX:
        deleted = zslDeleteRangeByLex(current, tombstone, &meta, &lexrange, callback_items, &callback_byte_size);
        break;
    }
    if (current && crdtZsetLength(current) == 0) {
        RedisModule_DeleteKey(moduleKey);
        keyremoved = 1;
    }
    updateCrdtSSTLastVc(tombstone, getMetaVectorClock(&meta));
    replicationCrdtZremCommand(ctx, RedisModule_GetSds(argv[1]), &meta ,callback_items, deleted, callback_byte_size);
    /* Step 4: Notifications and reply. */
    if (deleted) {
        char *event[3] = {"zremrangebyrank","zremrangebyscore","zremrangebylex"};
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_ZSET, event[rangetype], argv[1]);
        if (keyremoved)
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_ZSET, "del", argv[1]);
    }
    RedisModule_ReplyWithLongLong(ctx,deleted);

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
    if (meta.gid) {freeIncrMeta(&meta);}
    if (moduleKey) {RedisModule_CloseKey(moduleKey);}
    if (callback_items != NULL) RedisModule_Free(callback_items);
    return REDISMODULE_OK;
}

int zremrangebyscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc != 4) { return RedisModule_WrongArity(ctx); }
    return zremrangeGenericCommand(ctx, argv, argc, ZRANGE_SCORE);
}

int zremrangebyrankCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc != 4) { return RedisModule_WrongArity(ctx); }
    return zremrangeGenericCommand(ctx, argv, argc, ZRANGE_RANK);
}

int zremrangebylexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc != 4) { return RedisModule_WrongArity(ctx); }
    return zremrangeGenericCommand(ctx, argv, argc, ZRANGE_LEX);
}

int crdtZscoreCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSS, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        goto error;
    }
    CRDT_SS* current = getCurrentValue(moduleKey);
    CRDT_SSTombstone* tombstone = getTombstone(moduleKey);
    sds info = getZsetElementInfo(current, tombstone, RedisModule_GetSds(argv[2]));
    if(info != NULL) {
        RedisModule_CloseKey(moduleKey);
        int result = RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
        return result;
    }
error:
    RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithNull(ctx);
}

int initCrdtSSModule(RedisModuleCtx *ctx) {
    initZsetShard();
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
    if (RedisModule_CreateCommand(ctx,"CRDT.zadd",
                                  crdtZaddCommand,"write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"ZSCORE",
                                  zscoreCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.ZSCORE",
                                  crdtZscoreCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"ZCARD",
                                  zcardCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"zincrby",
                                  zincrbyCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "crdt.zincrby" ,
                                    crdtZincrbyCommand, "write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zcount",
                            zcountCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"ZRANGE",
                                zrangeCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return  REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zremrangebylex",
                                zremrangebylexCommand, "write",1,1,1) == REDISMODULE_ERR)         
        return  REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"zrevrange",
                                  zrevrangeCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zrangebyscore", 
                                zrangebyscoreCommand, "readonly", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zrevrangebyscore", 
                                zrevrangebyscoreCommand, "readonly", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"zrank",
                                  zrankCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"zrevrank",
                                  zrevrankCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zrem", 
                                  zremCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.zrem",
                                  crdtZremCommand,"write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.del_ss",
                                crdtDelSSCommand  ,"write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zremrangebyrank", 
                                  zremrangebyrankCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zremrangebyscore", 
                                  zremrangebyscoreCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    
    if (RedisModule_CreateCommand(ctx, "zrangebylex",
                                zrangebylexCommand, "readonly", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zlexcount",
                                zlexcountCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zrevrangebylex",
                                zrevrangebylexCommand, "readonly", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "zscan",
                                zscanCommand, "readonly random", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

VectorClock clone_sst_vc(void* data) {
    return dupVectorClock(getCrdtSSTLastVc((CRDT_SSTombstone*)data));
}

RedisModuleType* getCrdtSS() {
    return CrdtSS;
}
RedisModuleType* getCrdtSST() {
    return CrdtSST;
}



