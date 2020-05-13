#include "crdt_expire.h"
#include "crdt_register.h"
void setExpire(RedisModuleKey *key, CrdtData *data, long long expiteTime) {
    if(expiteTime == -1) {
       RedisModule_SetExpire(key, -1); 
       return;
    }
    RedisModule_SetExpire(key, expiteTime - RedisModule_Milliseconds());
}
int expireAt(RedisModuleCtx* ctx, RedisModuleString *key, long long expireTime) {
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
    CrdtData *data = NULL;
    int type = RedisModule_KeyType(moduleKey);
    if(type != REDISMODULE_KEYTYPE_EMPTY) {
        data = RedisModule_ModuleTypeGetValue(moduleKey);
        if (data == NULL) {
            goto end;
        }
    } else {
        goto end;
    }
    setExpire(moduleKey, data, expireTime);
end:
    if(data != NULL) {
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.EXPIRE", "sllll", key, RedisModule_CurrentGid(), RedisModule_Milliseconds(), expireTime, (long long)(getDataType(data)));
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, 0);
}
int expireAtCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    long long time;
    if ((RedisModule_StringToLongLong(argv[2],&time) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }
    return expireAt(ctx, argv[1], time);
}
int trySetExpire(RedisModuleKey* moduleKey, RedisModuleString* key, long long  time, int type, long long expireTime) {
    CrdtData* data = RedisModule_ModuleTypeGetValue(moduleKey);
    if(data == NULL) {
        RedisModule_Debug(logLevel, "key: %s, data is null: %lld", RedisModule_GetSds(key), expireTime);
        return CRDT_ERROR;
    }
    if(getDataType(data) != type) {
         RedisModule_Debug(logLevel, "key: %s ,type diff: %lld", RedisModule_GetSds(key),expireTime);
        return CRDT_ERROR;
    }
    if(expireTime == -1) {
         RedisModule_SetExpire(moduleKey, REDISMODULE_NO_EXPIRE);
         return CRDT_OK;
    }
    if(type == CRDT_REGISTER_TYPE) {
        long long t = getCrdtRegisterLastTimestamp(data);
        if(t > time) {
            return CRDT_ERROR;
        }
    }
    long long et = RedisModule_GetExpire(moduleKey);
    if(expireTime > et) {
        RedisModule_SetExpire(moduleKey, expireTime - RedisModule_Milliseconds());
    }
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
    return expireAt(ctx, argv[1], RedisModule_Milliseconds() + time * 1000);
}
//CRDT.EXPIRE key gid time  expireTime type
int crdtExpireCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 6) return RedisModule_WrongArity(ctx);
    long long expireTime;
    long long gid;
    if ((RedisModule_StringToLongLong(argv[2],&gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: gid must be a signed 64 bit integer");
    } 
    long long time;
    if ((RedisModule_StringToLongLong(argv[3],&time) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: time must be a signed 64 bit integer");
    } 
    if ((RedisModule_StringToLongLong(argv[4],&expireTime) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: expireTime must be a signed 64 bit integer");
    } 
    long long type;
    if ((RedisModule_StringToLongLong(argv[5],&type) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: type must be a signed 64 bit integer");
    } 
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    trySetExpire(moduleKey, argv[1], time, type, expireTime);
    if(moduleKey != NULL) {
        if (gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, 0);
}


//PERSIST key 
int persistCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    CrdtData* data = NULL;
    int result = 0;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    if(moduleKey != NULL) {
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            data = RedisModule_ModuleTypeGetValue(moduleKey);
            if(data == NULL) {
                goto end;   
            }
        }
    }
    long long et = RedisModule_GetExpire(moduleKey);
    if(et == REDISMODULE_NO_EXPIRE) {
        goto end;
    }
    RedisModule_SetExpire(moduleKey, REDISMODULE_NO_EXPIRE);
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.persist", "sll", argv[1], RedisModule_CurrentGid() ,  (long long )getDataType(data));
    
end:  
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    
    return RedisModule_ReplyWithLongLong(ctx, result);
}
//CRDT.PERSIST key gid type
int crdtPersistCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    long long dataType;
    if ((RedisModule_StringToLongLong(argv[3],&dataType) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }
    long long gid;
    if ((RedisModule_StringToLongLong(argv[2],&gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid gid: must be a signed 64 bit integer");
    }
    CrdtData* data = NULL;
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    if(moduleKey != NULL) {
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            data = RedisModule_ModuleTypeGetValue(moduleKey);
            if(data == NULL) {
                goto end;
            }
        }
    }
    if(getDataType(data) != dataType) {
        goto end;
    }
    RedisModule_SetExpire(moduleKey, REDISMODULE_NO_EXPIRE);
end: 
    if (gid == RedisModule_CurrentGid()) {
        RedisModule_CrdtReplicateVerbatim(ctx);
    } else {
        RedisModule_ReplicateVerbatim(ctx);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, 0);
}



int initCrdtExpireModule(RedisModuleCtx *ctx) {
    
    if (RedisModule_CreateCommand(ctx, "EXPIRE", 
        expireCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "EXPIREAT", 
        expireAtCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "PERSIST", 
        persistCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "CRDT.EXPIRE", 
        crdtExpireCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "CRDT.PERSIST", 
        crdtPersistCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}


