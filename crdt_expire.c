#include "crdt_expire.h"


CrdtExpireObj* addOrUpdateExpire(RedisModuleKey* moduleKey, CrdtData* data, CrdtMeta* meta,long long expireTime) {
    CrdtExpire* expire =  RedisModule_GetCrdtExpire(moduleKey);
    if(expire == NULL) {
        expire = createCrdtExpire();
        expire->dataType = data->dataType;
        RedisModule_SetCrdtExpire(moduleKey, CrdtExpireType, expire);
    } else {
        appendVCForMeta(meta, expire->method->get(expire)->meta->vectorClock);
    }
    CrdtExpireObj *obj = createCrdtExpireObj(dupMeta(meta), expireTime);
    expire->method->add(expire,obj);
    data->method->updateLastVC(data, meta->vectorClock);
    return obj;
}

int tryAddOrUpdateExpire(RedisModuleKey* moduleKey, int type, CrdtExpireObj* obj) {
    CrdtData* data = RedisModule_ModuleTypeGetValue(moduleKey);
    CrdtExpire *expire = RedisModule_GetCrdtExpire(moduleKey);
    CrdtExpireTombstone *tombstone = RedisModule_GetCrdtExpireTombstone(moduleKey);
    if(tombstone != NULL) {
        if(tombstone->method->isExpire(tombstone, obj->meta)) {
            return CRDT_ERROR;
        }   
    }
    if(expire == NULL) {
        expire = createCrdtExpire();
        expire->dataType = type;
        RedisModule_SetCrdtExpire(moduleKey, CrdtExpireType, expire);
    }
    if(expire->dataType != type) {
        return CRDT_ERROR;
    }
    if(data) {
        data->method->updateLastVC(data, obj->meta->vectorClock);
    }
    
    expire->method->add(expire, obj);
    
    return CRDT_OK;
}

//expire <key> <time>
int expireCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    long long time;
    if ((RedisModule_StringToLongLong(argv[2],&time) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);  
    CrdtMeta* meta = NULL;
    CrdtData *data;
    CrdtExpireObj* expireobj = NULL;
    int type = RedisModule_KeyType(moduleKey);
    if(type != REDISMODULE_KEYTYPE_EMPTY) {
        data = RedisModule_ModuleTypeGetValue(moduleKey);
        if (data == NULL) {
            goto end;
        }
    } else {
        goto end;
    }
    meta = createIncrMeta();
    long long expireTime = meta->timestamp + time * 1000;
    expireobj = addOrUpdateExpire(moduleKey, data, meta, expireTime);
end:
    if(meta != NULL) {
        sds vcStr = vectorClockToSds(meta->vectorClock);
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.EXPIRE", "sllcll", argv[1], meta->gid, meta->timestamp, vcStr, expireobj->expireTime, (long long)(data->dataType));
        sdsfree(vcStr);
        freeCrdtMeta(meta);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(expireobj != NULL) freeCrdtExpireObj(expireobj);
    return RedisModule_ReplyWithLongLong(ctx, 0);
}
//CRDT.EXPIRE key gid time vc expireTime type
int crdtExpireCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 7) return RedisModule_WrongArity(ctx);
    long long expireTime;
    CrdtExpireObj* obj = NULL;
    if ((RedisModule_StringToLongLong(argv[5],&expireTime) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: expireTime must be a signed 64 bit integer");
        return NULL;
    } 
    long long t;
    if ((RedisModule_StringToLongLong(argv[6],&t) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: type must be a signed 64 bit integer");
        return NULL;
    } 
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    if(meta == NULL) {
        return 0;
    }
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    obj = createCrdtExpireObj(dupMeta(meta), expireTime);
    tryAddOrUpdateExpire(moduleKey, t, obj);
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
end:
    if(meta != NULL) {
        if (meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCrdtMeta(meta);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(obj != NULL) freeCrdtExpireObj(obj); 
    return RedisModule_ReplyWithLongLong(ctx, 0);
}
void expirePersist(CrdtExpire* expire,  RedisModuleKey* moduleKey, int dbId, RedisModuleString* key) {
    CrdtMeta* meta = createIncrMeta();
    delExpire(moduleKey, expire, meta);
    sds vcStr = vectorClockToSds(meta->vectorClock);
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.persist", "sllcl", key, meta->gid, meta->timestamp, vcStr, (long long)expire->dataType);
    sdsfree(vcStr);
    freeCrdtMeta(meta);
}
//PERSIST key 
int persistCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    CrdtData* data = NULL;
    CrdtMeta* meta = NULL;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    if(moduleKey != NULL) {
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            data = RedisModule_ModuleTypeGetValue(moduleKey);
        }
    }
    int result = 0;
    CrdtExpire *expire = RedisModule_GetCrdtExpire(moduleKey);
    if(expire != NULL) {
        expire->method->persist(expire, moduleKey, RedisModule_GetSelectedDb(ctx), argv[1]);
        result = 1;
    } 
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}
//CRDT.PERSIST key gid timestamp vc type
int crdtPersistCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    CrdtData* data = NULL;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    if(moduleKey != NULL) {
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            data = RedisModule_ModuleTypeGetValue(moduleKey);
        }
    }
    long long dataType;
    if ((RedisModule_StringToLongLong(argv[5],&dataType) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return NULL;
    }
    addExpireTombstone(moduleKey, dataType, meta);
    if(data != NULL) {
        data->method->updateLastVC(data, meta->vectorClock);
    }
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
end: 
    if(meta != NULL) {
        if (meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCrdtMeta(meta);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, 0);
}
//ttl <key>
int ttlCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    long long result = -1;
    CrdtObject* data = NULL;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    if(moduleKey != NULL) {
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            data = RedisModule_ModuleTypeGetValue(moduleKey);
        }
    }
    
    CrdtExpire *expire = RedisModule_GetCrdtExpire(moduleKey);
    if (expire != NULL) {
        if(data != NULL) {
            CrdtExpireObj* obj = expire->method->get(expire);
            if(obj->expireTime == -1) {
                result = -1;
            }else{
                long long ttl = obj->expireTime - RedisModule_Milliseconds();
                if(ttl < 0) {
                    ttl = 0;
                }
                result = (ttl + 500)/1000;
            }
            
        }else{
            result = -2;
        } 
    }
end:
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}
//crdt.ttl 
int crdtTtlCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    long long result = -1;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    CrdtExpire *expire = RedisModule_GetCrdtExpire(moduleKey);
    if (expire == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    CrdtExpireObj* obj = expire->method->get(expire);
    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithLongLong(ctx, obj->meta->gid);
    RedisModule_ReplyWithLongLong(ctx, obj->meta->timestamp);
    sds vclockSds = vectorClockToSds(obj->meta->vectorClock);
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
    RedisModule_ReplyWithLongLong(ctx, obj->expireTime);
}

int initCrdtExpireModule(RedisModuleCtx *ctx) {
    RedisModuleTypeMethods expireTm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtExpire,
        .rdb_save = RdbSaveCrdtExpire,
        .aof_rewrite = AofRewriteCrdtExpire,
        .mem_usage = crdtExpireMemUsageFunc,
        .free = freeCrdtExpire,
        .digest = crdtExpireDigestFunc
    };
    CrdtExpireType = RedisModule_CreateDataType(ctx, CRDT_EXPIRE_DATATYPE_NAME, 0, &expireTm);
    if (CrdtExpireType == NULL) return REDISMODULE_ERR;
   
    RedisModuleTypeMethods crdtExpireTombstoneTm = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtExpireTombstone,
            .rdb_save = RdbSaveCrdtExpireTombstone,
            .aof_rewrite = AofRewriteCrdtExpireTombstone,
            .mem_usage = crdtExpireTombstoneMemUsageFunc,
            .free = freeCrdtExpireTombstone,
            .digest = crdtExpireTombstoneDigestFunc
    };
    CrdtExpireTombstoneType = RedisModule_CreateDataType(ctx, CRDT_EXPIRE_TOMBSTONE_DATATYPE_NAME, 0, &crdtExpireTombstoneTm);
    if (CrdtExpireTombstoneType == NULL) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "EXPIRE", 
        expireCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "TTL", 
        ttlCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "PERSIST", 
        persistCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "CRDT.EXPIRE", 
        crdtExpireCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CRDT.TTL", 
        crdtTtlCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CRDT.PERSIST", 
        crdtPersistCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

void delExpire(RedisModuleKey *moduleKey, CrdtExpire* expire, CrdtMeta* meta) {
    CrdtExpireTombstone *tombstone = RedisModule_GetCrdtExpireTombstone(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtExpireTombstone(expire->dataType);
        RedisModule_SetCrdtExpireTombstone(moduleKey, CrdtExpireTombstoneType, tombstone);
    } 
    appendVCForMeta(meta, expire->method->get(expire)->meta->vectorClock);
    tombstone->method->add(tombstone, meta);
    RedisModule_SetCrdtExpire(moduleKey, CrdtExpireType, NULL);
}
void addExpireTombstone(RedisModuleKey* moduleKey, int dataType, CrdtMeta* meta) {
    CrdtExpire *expire = RedisModule_GetCrdtExpire(moduleKey);
    CrdtExpireTombstone *tombstone = RedisModule_GetCrdtExpireTombstone(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtExpireTombstone(dataType);
        RedisModule_SetCrdtExpireTombstone(moduleKey, CrdtExpireTombstoneType, tombstone);
    } 
    tombstone->method->add(tombstone, meta);
    if(expire != NULL) {
        if(compareCrdtMeta(expire->method->get(expire)->meta, meta) > COMPARE_META_EQUAL) {
            RedisModule_SetCrdtExpire(moduleKey, CrdtExpireType, NULL);
        }   
    }
}

RedisModuleType* getCrdtExpireType() {
    return CrdtExpireType;
}
RedisModuleType* getCrdtExpireTombstoneType() {
    return CrdtExpireTombstoneType;
}