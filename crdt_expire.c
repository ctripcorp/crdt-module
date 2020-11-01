#include "crdt_expire.h"
#include "crdt_register.h"
#include <string.h>
int setExpire(RedisModuleKey *key, long long expiteTime) {
    if(expiteTime == REDISMODULE_NO_EXPIRE) {
        RedisModule_SetExpire(key, REDISMODULE_NO_EXPIRE); 
    }else{
        RedisModule_SetExpire(key, expiteTime - RedisModule_Milliseconds());
    }
    return CRDT_OK;
}
const char* crdt_expireat_head = "*6\r\n$11\r\nCRDT.EXPIRE\r\n";
//CRDT.EXPIRE key gid time  expireTime type
const size_t crdt_expireat_basic_str_len = 23 + REPLICATION_MAX_STR_LEN + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN * 3; 
size_t replicationFeedCrdtExpireAtCommand(RedisModuleCtx* ctx, char* cmdbuf, const char* keystr, size_t keylen, int gid, long long nowTime, long long expireTime, int dataType) {
    size_t cmdlen = 0;
    cmdlen +=  feedBuf(cmdbuf + cmdlen, crdt_expireat_head, strlen(crdt_expireat_head));
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, keystr, keylen);
    cmdlen += feedGid2Buf(cmdbuf + cmdlen, gid);
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, nowTime);
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expireTime);
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, dataType);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int expireAt(RedisModuleCtx* ctx, RedisModuleString *key, long long expireTime) {
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
    CrdtData *data = NULL;
    int type = RedisModule_KeyType(moduleKey);
    int result = 0;
    if(type != REDISMODULE_KEYTYPE_EMPTY) {
        data = RedisModule_ModuleTypeGetValue(moduleKey);
        if (data == NULL) {
            goto end;
        }
    } else {
        goto end;
    }
    result = setExpire(moduleKey, expireTime);
end:
    if(data != NULL) {
        size_t keylen = 0;
        const char* keystr = RedisModule_StringPtrLen(key, &keylen);
        size_t alllen = keylen  + crdt_expireat_basic_str_len;
        if(keylen > MAXSTACKSIZE) {
            char* cmdbuf = RedisModule_Alloc(alllen);
            replicationFeedCrdtExpireAtCommand(ctx, cmdbuf, keystr, keylen, RedisModule_CurrentGid(), RedisModule_Milliseconds(), expireTime, (long long)(getDataType(data)));
            RedisModule_Free(cmdbuf);
        } else {
            char cmdbuf[alllen]; 
            replicationFeedCrdtExpireAtCommand(ctx, cmdbuf, keystr, keylen, RedisModule_CurrentGid(), RedisModule_Milliseconds(), expireTime, (long long)(getDataType(data)));
        }


        // RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.EXPIRE", "sllll", key, RedisModule_CurrentGid(), RedisModule_Milliseconds(), expireTime, (long long)(getDataType(data)));
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
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
        RedisModule_Debug(CRDT_DEBUG_LOG_LEVEL, "key: %s, data is null: %lld", RedisModule_GetSds(key), expireTime);
        return CRDT_ERROR;
    }
    if(getDataType(data) != type) {
         RedisModule_Debug(CRDT_DEBUG_LOG_LEVEL, "key: %s ,type diff: %lld", RedisModule_GetSds(key),expireTime);
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
        RedisModule_CrdtReplicateVerbatim(gid, ctx);
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
    result = setExpire(moduleKey, REDISMODULE_NO_EXPIRE);
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
    RedisModule_CrdtReplicateVerbatim(gid, ctx);
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


