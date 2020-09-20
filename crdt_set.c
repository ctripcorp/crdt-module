#include "crdt_set.h"


int crdtSetDelete(int dbId, void* keyRobj, void *key, void *value) {
    
    if(value == NULL) {
        return CRDT_ERROR;
    }
    if(!isCrdtSet(value)) {
        return CRDT_ERROR;
    }
    CrdtMeta* meta = createIncrMeta();
    CrdtMeta* del_meta = dupMeta(meta);
    CRDT_Set* current = (CRDT_Set*) value;
    appendVCForMeta(del_meta, getCrdtSetLastVc(current));
    RedisModuleKey *moduleKey = (RedisModuleKey*) key;
    CRDT_SetTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isCrdtSetTombstone(tombstone)) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, del_meta);
    updateCrdtSetTombstoneMaxDel(tombstone, getMetaVectorClock(del_meta));
    sds vcSds = vectorClockToSds(getMetaVectorClock(del_meta));
    // sds maxDeleteVectorClock = vectorClockToSds(getCrdtSetLastVc(current));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Set", "sllcc", keyRobj, getMetaGid(meta), getMetaTimestamp(meta), vcSds, vcSds);
    sdsfree(vcSds);
    freeCrdtMeta(meta);
    freeCrdtMeta(del_meta);
    return CRDT_OK;
}
int sismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0); 
    } 
    sds field = RedisModule_GetSds(argv[2]);
    dictEntry* de = findSetDict(current, field);
    if(de != NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 1); 
    }
    return RedisModule_ReplyWithLongLong(ctx, 0); 
}

int scardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0); 
    } 
    return RedisModule_ReplyWithLongLong(ctx, getSetDictSize(current));
}
int smembersCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithNull(ctx);
    }
    size_t length = getSetDictSize(current);
    RedisModule_ReplyWithArray(ctx, length);
    dictEntry* de = NULL;
    dictIterator* di = getSetDictIterator(current);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        RedisModule_ReplyWithStringBuffer(ctx, field, sdslen(field));
    }
    dictReleaseIterator(di);
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}

int sremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int result = 0;
    CrdtMeta meta = {.gid=0}; 
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    initIncrMeta(&meta);
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    
    appendVCForMeta(&meta, getCrdtSetLastVc(current));
    for(int i = 2; i < argc; i += 1) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        int r = removeSetDict(current, field, &meta);
        if(r) {
            addSetTombstoneDictValue(tombstone, field, &meta);
            result += 1;
        }
    }
    if(getSetDictSize(current) == 0) {
        RedisModule_DeleteKey(moduleKey);
    } else {
        updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, &meta);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "srem", argv[1]);
    char buf[100];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Srem", "sllcv", argv[1], getMetaGid(&meta),getMetaTimestamp(&meta), buf, (void *) (argv + 2), (size_t)(argc-2));
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}
//spop key
int spopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);
    long long num = 0;
    if (argc == 2) {
        num = 1;
    } else if(argc == 3) {
        if ((RedisModule_StringToLongLong(argv[2],&num) != REDISMODULE_OK)) {
            return RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
        }
    } else {
        return RedisModule_ReplyWithError(ctx, "ERR syntax error");
    }
    CrdtMeta meta = {.gid=0}; 
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    initIncrMeta(&meta);
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    int keylen = getSetDictSize(current);
    num = min(keylen, num);
    sds fields[num];
    appendVCForMeta(&meta, getCrdtSetLastVc(current));
    if(num < keylen) {
        for(int i = 0; i < num; i += 1) {
            sds field = sdsdup(getRandomSetKey(current));
            // dictEntry* de = findSetDict(current, field);
            removeSetDict(current, field, &meta);
            addSetTombstoneDictValue(tombstone, field, &meta);
            fields[i] = field;
        }
        updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    } else {
        dictIterator* di = getSetDictIterator(current);
        int i = 0;
        dictEntry* de = NULL;
        while((de = dictNext(di)) != NULL) {
            sds field = sdsdup(dictGetKey(de));
            fields[i++] = field;
            addSetTombstoneDictValue(tombstone, field, &meta);
        }
        dictReleaseIterator(di);
        RedisModule_DeleteKey(moduleKey);
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, &meta);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "srem", argv[1]);
    char buf[100];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Srem", "sllca", argv[1], getMetaGid(&meta),getMetaTimestamp(&meta), buf, fields, num);
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    RedisModule_ReplyWithArray(ctx, num);
    for(int i = 0; i < num; i++) {
        sds field = fields[i];
        RedisModule_ReplyWithStringBuffer(ctx, field, sdslen(field));
        sdsfree(field);
    }
    return CRDT_OK;
}
//sadd key <field> <field1> ...
//crdt.sadd <key> gid vc <field1> <field1>
int saddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int result = 0;
    CrdtMeta meta = {.gid=0};
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    initIncrMeta(&meta);
    CRDT_Set* current = getCurrentValue(moduleKey);
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    
    if(current == NULL) {
        current = createCrdtSet();
        if(tombstone) {
            updateCrdtSetLastVc(current, getCrdtSetTombstoneLastVc(tombstone));
        } 
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSet, current);
    } 
    appendVCForMeta(&meta, getCrdtSetLastVc(current));
    for(int i = 2; i < argc; i += 1) {
        sds field = RedisModule_GetSds(argv[i]);
        dictEntry* de = findSetDict(current, field);
        if(de == NULL) {
            addSetDict(current, field, &meta);
            result += 1;
        } else {
            updateSetDict(current, de, &meta);
        }
        if(tombstone) {
            removeSetTombstoneDict(tombstone, field);
        }
    }
    if(tombstone && isNullVectorClock(getCrdtSetTombstoneMaxDelVc(tombstone)) && getSetTombstoneDictSize(tombstone) == 0) {
        RedisModule_DeleteTombstone(moduleKey);
    }
    
    updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "sadd", argv[1]);
    char buf[100];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Sadd", "sllcv", argv[1], getMetaGid(&meta), getMetaTimestamp(&meta), buf, (void *) (argv + 2), (size_t)(argc-2));
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    return RedisModule_ReplyWithLongLong(ctx, result);
}
//crdt.sadd <key> gid time vc <field1> <field1>
int crdtSaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        current = createCrdtSet();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSet, current);
    } 
    int result = 0;
    for(int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        result += setTombstoneIterPurge(current, tombstone, field, &meta);
    }
    if(tombstone && isNullVectorClock(getCrdtSetTombstoneMaxDelVc(tombstone)) && getSetTombstoneDictSize(tombstone) == 0) {
        RedisModule_DeleteTombstone(moduleKey);
    }
    updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    if(result) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "sadd", argv[1]);
    }
    
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}
//crdt.srem <key>, <gid>, <timestamp>, <vclockStr> k1,k2
int crdtSremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    int result = 0;
    for(int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        result += setValueIterPurge(current, tombstone, field, &meta);
    }
    if(current) {
        if(getSetDictSize(current) == 0) {
            RedisModule_DeleteKey(moduleKey);
            current = NULL;
        } else {
            updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
        }
    }
    updateCrdtSetTombstoneLastVc(tombstone, getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    if(result) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "srem", argv[1]);
    }
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }  
}
int crdtSismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    CRDT_SetTombstone* tombstone =  getTombstone(moduleKey);
    sds field = RedisModule_GetSds(argv[2]);
    dictEntry* de = NULL;
    dictEntry* tde = NULL;
    int num = 0;
    if(current != NULL) {
        de = findSetDict(current, field);
        if(de != NULL)  {
            num += 1;
        }
    } 
    if(tombstone != NULL) {
        tde = findSetTombstoneDict(tombstone, field);
        if(tde != NULL)  {
            num += 1;
        }
    }
    
    if(num == 0) {
        return RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_ReplyWithArray(ctx, num);
    if(de != NULL) {
        void* data = dictGetVal(de);
        sds info = setIterInfo(data);
        RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
    }
    if(tde != NULL) {
        void* data = dictGetVal(tde);
        sds info = setTombstoneIterInfo(data);
        RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
    }
    return CRDT_OK;
    
     
}
//crdt.del_Set <key> gid time vc maxvc
int crdtDelSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_Debug(logLevel, "crdt.del");
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    if(purgeSetDelMax(current, tombstone, &meta)) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "srem", argv[1]);
    }
    updateCrdtSetTombstoneLastVc(tombstone, getMetaVectorClock(&meta));
    if(getSetDictSize(current) == 0) {
        RedisModule_DeleteKey(moduleKey);
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

//hash common methods
CrdtObject *crdtSetMerge(CrdtObject *currentVal, CrdtObject *value) {
    CRDT_Set* target = retrieveCrdtSet(currentVal);
    CRDT_Set* other = retrieveCrdtSet(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return (CrdtObject*)dupCrdtSet(other);
    }
    
    CRDT_Set *result = dupCrdtSet(target);
    appendSet(result, other);
    updateCrdtSetLastVc(result, getCrdtSetLastVc(other));
    return (CrdtObject*)result;
}
CrdtTombstone* crdtSetTombstoneMerge(CrdtTombstone* currentVal, CrdtTombstone* value) {
    CRDT_SetTombstone* target = retrieveCrdtSetTombstone(currentVal);
    CRDT_SetTombstone* other = retrieveCrdtSetTombstone(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return (CrdtTombstone*)dupCrdtSetTombstone(other);
    }
    CRDT_SetTombstone *result = dupCrdtSetTombstone(target);
    appendSetTombstone(result, other);
    updateCrdtSetTombstoneLastVc(result, getCrdtSetTombstoneLastVc(other));
    updateCrdtSetTombstoneMaxDel(result, getCrdtSetTombstoneMaxDelVc(other));
    
    return (CrdtTombstone*)result;
}
int initCrdtSetModule(RedisModuleCtx *ctx) {
    //hash object type
    RedisModuleTypeMethods valueTypeMethods = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtSet,
            .rdb_save = RdbSaveCrdtSet,
            .aof_rewrite = AofRewriteCrdtSet,
            .mem_usage = crdtSetMemUsageFunc,
            .free = freeCrdtSet,
            .digest = crdtSetDigestFunc
    };
    CrdtSet = RedisModule_CreateDataType(ctx, CRDT_SET_DATATYPE_NAME, 0, &valueTypeMethods);
    if (CrdtSet == NULL) return REDISMODULE_ERR;
    //set tombstone type
    RedisModuleTypeMethods tombstoneTypeMethods = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtSetTombstone,
            .rdb_save = RdbSaveCrdtSetTombstone,
            .aof_rewrite = AofRewriteCrdtSetTombstone,
            .mem_usage = crdtSetTombstoneMemUsageFunc,
            .free = freeCrdtSetTombstone,
            .digest = crdtSetTombstoneDigestFunc
    };
    CrdtSetTombstone = RedisModule_CreateDataType(ctx, CRDT_SET_TOMBSTONE_DATATYPE_NAME, 0, &tombstoneTypeMethods);
    if (CrdtSetTombstone == NULL) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"sadd",
                                  saddCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"crdt.sadd",
                                  crdtSaddCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "sismember", sismemberCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "srem", 
                                sremCommand, "write deny-oom", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx,"crdt.srem",
                                  crdtSremCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR; 
    if (RedisModule_CreateCommand(ctx, "scard",
                                scardCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx, "smembers",
                                smembersCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx, "crdt.sismember",
                                crdtSismemberCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx,"crdt.del_set",
                                  crdtDelSetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;    
    if (RedisModule_CreateCommand(ctx,"spop",
                                  spopCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;              
    return REDISMODULE_OK;
}
RedisModuleType* getCrdtSet() {
    return CrdtSet;
}
RedisModuleType* getCrdtSetTombstone() {
    return CrdtSetTombstone;
}