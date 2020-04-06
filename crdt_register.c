/*
 * Copyright (c) 2009-2012, CTRIP CORP <RDkjdata at ctrip dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
//
// Created by zhuchen(zhuchen at ctrip dot com) on 2019-04-18.
//

#include "include/redismodule.h"
#include "include/rmutil/sds.h"

#include "crdt_register.h"
#include "ctrip_vector_clock.h"
#include "utils.h"
#include "crdt.h"

/**
 * ==============================================Pre-defined functions=========================================================*/

int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CRDT_GetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);

size_t crdtRegisterMemUsageFunc(const void *value);

CrdtObject* crdtRegisterFilter(CrdtObject* common, long long gid, long long logic_time);

void crdtRegisterDigestFunc(RedisModuleDigest *md, void *value);

int crdtRegisterDelete(int dbId, void *keyRobj, void *key, void *value);


int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/**
 * ==============================================Register module init=========================================================*/

static RedisModuleType *CrdtRegister;

void *crdtRegisterMerge(void *currentVal, void *value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    CRDT_Register *current = (CRDT_Register*) currentVal;
    CRDT_Register *v = (CRDT_Register*) value;
    if(current == NULL) {
        return v->method->dup(v);
    }
    if(v == NULL) {
        return current->method->dup(current);
    }
    return current->method->merge(currentVal, value);
}
CrdtObject* crdtRegisterFilter(CrdtObject* common, long long gid, long long logic_time) {
    CRDT_Register* reg = (CRDT_Register*) common;
    return reg->method->filter(common, gid, logic_time);
}
int crdtRegisterTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current) {
    if(!isRegister(current)) {
        return 0;
    }
    if(!isRegisterTombstone(tombstone)) {
        return 0;
    }
    CRDT_Register* reg = (CRDT_Register*) current;
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*)tombstone;
    return t->method->purage(t, reg);
}

void* crdtRegisterTombstoneMerge(void* target, void* other) {
    if(!isRegisterTombstone(target) || !isRegisterTombstone(other)) {
        return NULL;
    }
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*) target;
    return t->method->merge(t, (CRDT_RegisterTombstone*) other);
}

static RedisModuleType *CrdtRegisterTombstone;

void* crdtRegisterTombstoneFilter(void* target, long long gid, long long logic_time) {
    if(!isRegisterTombstone(target)) {
        return NULL;
    }
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*) target;
    t->method->filter(t, gid, logic_time);
}
int initRegisterModule(RedisModuleCtx *ctx) {
    RedisModuleTypeMethods tm = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtRegister,
            .rdb_save = RdbSaveCrdtRegister,
            .aof_rewrite = AofRewriteCrdtRegister,
            .mem_usage = crdtRegisterMemUsageFunc,
            .free = freeCrdtRegister,
            .digest = crdtRegisterDigestFunc
    };

    CrdtRegister = RedisModule_CreateDataType(ctx, CRDT_REGISTER_DATATYPE_NAME, 0, &tm);
    if (CrdtRegister == NULL) return REDISMODULE_ERR;
    RedisModuleTypeMethods tombtm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtRegisterTombstone,
        .rdb_save = RdbSaveCrdtRegisterTombstone,
        .aof_rewrite = AofRewriteCrdtRegisterTombstone,
        .mem_usage = crdtRegisterTombstoneMemUsageFunc,
        .free = freeCrdtRegisterTombstone,
        .digest = crdtRegisterTombstoneDigestFunc,
    };
    CrdtRegisterTombstone = RedisModule_CreateDataType(ctx, CRDT_REGISTER_TOMBSTONE_DATATYPE_NAME, 0, &tombtm);
    if (CrdtRegisterTombstone == NULL) return REDISMODULE_ERR;

    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
    if (RedisModule_CreateCommand(ctx,"SET",
                                  setCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.SET",
                                  CRDT_SetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"GET",
                                  getCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.GET",
                                  CRDT_GetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_REG",
                                  CRDT_DelRegCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;

}

void freeCrdtRegisterValue(CrdtRegisterValue *value) {
    if(value->meta != NULL) {
        freeCrdtMeta(value->meta);
    }
    RedisModule_Free(value);
}
/***
 * CRDT Lifecycle functionality*/

CRDT_Register* dupCrdtRegister(const CRDT_Register *val) {
    return val->method->dup(val);
}

/*
 * return 0: nothing deleted
 * return 1: delete 1 crdt register
 * broadcast the CRDT.DEL_REG then
 * */
int crdtRegisterDelete(int dbId, void *keyRobj, void *key, void *value) {
    RedisModuleKey *moduleKey = (RedisModuleKey *)key;
    CRDT_Register *current = (CRDT_Register*) value;
    CrdtMeta* meta = createIncrMeta();
    CrdtMeta* del_meta = dupMeta(meta);
    appendVCForMeta(del_meta, current->method->getValue(current)->meta->vectorClock);
    CRDT_RegisterTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isRegisterTombstone(tombstone)) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    tombstone->method->add(tombstone, del_meta);
    CrdtExpire* expire = RedisModule_GetCrdtExpire(moduleKey);

    sds vcSds = vectorClockToSds(del_meta->vectorClock);
    if(expire == NULL) {
        RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_REG", "sllc", keyRobj, del_meta->gid, del_meta->timestamp, vcSds);
    } else {
        delExpire(moduleKey, expire, meta);
        sds expireVcSds = vectorClockToSds(meta->vectorClock);
        RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_REG", "sllcc", keyRobj, del_meta->gid, del_meta->timestamp, vcSds, expireVcSds);
        sdsfree(expireVcSds);
    }
    sdsfree(vcSds);
    freeCrdtMeta(meta);
    freeCrdtMeta(del_meta);
    return CRDT_OK;
}


//CRDT.DEL_REG <key> <gid> <timestamp> <vc>
//      0        1     2         3      4
//CRDT.DEL_REG <key> <gid> <timestamp> <vc> <expire-vc>
//      0        1     2         3      4       5
int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 5) return RedisModule_WrongArity(ctx);
    CrdtMeta* del_meta = getMeta(ctx, argv, 2);
    if(del_meta == NULL) return CRDT_ERROR;
    CrdtMeta* expire_meta = NULL;
    if(argc > 5) {
        VectorClock *del_vclock = getVectorClockFromString(argv[5]);
        expire_meta = createMeta(del_meta->gid, del_meta->timestamp, del_vclock);
    }

    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }

    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_RegisterTombstone* tombstone = NULL;
    if(t != NULL && isRegisterTombstone(t)) {    
        tombstone = (CRDT_RegisterTombstone*)t;
        if(tombstone->method->isExpire(tombstone, del_meta) == CRDT_OK) {
            goto end;
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    tombstone->method->add(tombstone, del_meta);

    CRDT_Register* current = getCurrentValue(moduleKey);
    
    if(current != NULL) {
        if(!isRegister(current)) {
            const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
            RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][drop] key:{%s} ,prev: {%s} ",
                            keyStr ,current->parent.dataType);
            RedisModule_IncrCrdtConflict();      
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
            status = CRDT_ERROR;
            goto end;
        }
        if(tombstone->parent.parent.method->purage(tombstone, current)) {
            RedisModule_DeleteKey(moduleKey);
            deleted = 1;
        }
    }
    if(expire_meta != NULL) {
        addExpireTombstone(moduleKey,CRDT_REGISTER_TYPE, expire_meta);
    }
    RedisModule_MergeVectorClock(del_meta->gid, del_meta->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end: 
    if(del_meta != NULL) {
        if (del_meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCrdtMeta(del_meta);
    }
    if(expire_meta) freeCrdtMeta(expire_meta);
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }
}

int isRegister(void *data) {
    CRDT_Register* tombstone = (CRDT_Register*) data;
    if(tombstone != NULL && tombstone->parent.dataType == CRDT_REGISTER_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}
int isRegisterTombstone(void *data) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL && tombstone->parent.dataType == CRDT_REGISTER_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}

CRDT_Register* addOrUpdateRegister(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, CRDT_Register* tombstone, CRDT_Register* current, CrdtMeta* meta, RedisModuleString* key,sds value) {
    if(current == NULL) {
        current = addRegister(tombstone, meta, value);
        if(current != NULL) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        }
    }else{
        if(!isRegister(current)) {
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][type conflict] {key: %s} prev: {%s}",
                            RedisModule_GetSds(key),current->parent.dataType);
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            RedisModule_IncrCrdtConflict();
            return NULL;
        }
        sds prev = current->method->getInfo(current);
        //tryUpdateRegister function will be change "current" object
        int result = tryUpdateRegister(tombstone, meta, current, value);
        if(isConflictCommon(result)) {
            CRDT_Register* incomeValue = addRegister(NULL, meta, value);
            sds income = current->method->getInfo(incomeValue);
            sds future = current->method->getInfo(current);
            if(result > COMPARE_META_EQUAL) {
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][replace] key:{%s} prev: {%s}, income: {%s}, future: {%s}",
                            RedisModule_GetSds(key), prev, income, future);
            }else{
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][drop] prev: {%s}, income: {%s}, future: {%s}",
                            RedisModule_GetSds(key), prev, income, future);
            }
            freeCrdtRegister(incomeValue);
            sdsfree(income);
            sdsfree(future);
            RedisModule_IncrCrdtConflict();
        }
        sdsfree(prev);
    }
    return current;
}
/**
 * CRDT Operations, including set/get, crdt.set/crdt.get
 * */
//SET key value [EX seconds] [PX milliseconds] [NX|XX]
//CRDT.SET key <val> <gid> <timestamp> <vc> <expire_timestamp> <expire-vc>
//expire_timestamp  -1 cancel expire
//expire_timestamp -2
#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)
#define OBJ_SET_XX (1<<1)
#define OBJ_SET_EX (1<<2)
#define OBJ_SET_PX (1<<3)
#define OBJ_SET_KEEPTTL (1<<4)
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    RedisModuleString* expire = NULL;
    int flags = OBJ_SET_NO_FLAGS;
    int j;
    int unit = UNIT_SECONDS;
    RedisModuleKey* moduleKey = NULL;
    CrdtMeta* meta = NULL;
    CrdtMeta* set_meta = NULL;
    CrdtExpireObj* crdtExpireObj = NULL;
    for (j = 3; j < argc; j++) {
        sds a = RedisModule_GetSds(argv[j]);
        RedisModuleString *next = (j == argc-1) ? NULL : argv[j+1];
        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
             !(flags & OBJ_SET_XX)) {
            flags |= OBJ_SET_NX;    
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_NX)) {
            flags |= OBJ_SET_XX;
        } else if (!strcasecmp(RedisModule_GetSds(argv[j]),"KEEPTTL") &&
                   !(flags & OBJ_SET_EX) && !(flags & OBJ_SET_PX))
        {
            flags |= OBJ_SET_KEEPTTL;
        } else if ((a[0] == 'e' || a[0] == 'E') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_KEEPTTL) &&
                   !(flags & OBJ_SET_PX) && next)
        {
            flags |= OBJ_SET_EX;
            unit = UNIT_SECONDS;
            expire = next;
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_KEEPTTL) &&
                   !(flags & OBJ_SET_EX) && next)
        {
            flags |= OBJ_SET_PX;
            unit = UNIT_MILLISECONDS;
            expire = next;
            j++;
        } else {
            status = CRDT_ERROR;
            RedisModule_ReplyWithError(ctx, "-ERR syntax error");
            goto end;
        }
    }
    
    
    moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);

    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CRDT_Register* current = getCurrentValue(moduleKey);
    if((current != NULL && flags & OBJ_SET_NX) 
        || (current == NULL && flags & OBJ_SET_XX)) {
        RedisModule_ReplyWithNull(ctx);  
        status = CRDT_ERROR;   
        goto end;
    }
        long long milliseconds = 0;
    if (expire) {
        if (RedisModule_StringToLongLong(expire, &milliseconds) != REDISMODULE_OK) {
            status = CRDT_ERROR;
            RedisModule_ReplyWithSimpleString(ctx, "-ERR syntax error\r\n");
            goto end;
        }
        if (milliseconds <= 0) {
            status = CRDT_ERROR;
            RedisModule_ReplyWithSimpleString(ctx,"invalid expire time in set\r\n");
            goto end;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }
    meta = createIncrMeta();
    set_meta = dupMeta(meta);
    if(current != NULL) {
        appendVCForMeta(set_meta, current->method->getValue(current)->meta->vectorClock);
    }
    current = addOrUpdateRegister(ctx, moduleKey, NULL, current, set_meta, argv[1], RedisModule_GetSds(argv[2]));
    if(current == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    if(expire) {
        long long expire_time = meta->timestamp + milliseconds;
        crdtExpireObj = addOrUpdateExpire(moduleKey, current, meta, expire_time);
    }else if(!(flags & OBJ_SET_KEEPTTL)){
        if(RedisModule_GetCrdtExpire(moduleKey) != NULL) {
            addExpireTombstone(moduleKey, CRDT_REGISTER_TYPE, meta);
            crdtExpireObj = createCrdtExpireObj(dupMeta(meta), -1);
        }
    }

    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
    if(expire) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "expire", argv[1]);
    }

end:
    if(set_meta) {
        sds vclockStr = vectorClockToSds(set_meta->vectorClock);
        //crdt.set key value gid timestamp set-vc expire-vc expire-timestamp 
        if(crdtExpireObj != NULL) {
            sds expire_vcStr =  vectorClockToSds(crdtExpireObj->meta->vectorClock);
            RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllccl", argv[1], argv[2], meta->gid, meta->timestamp, vclockStr, expire_vcStr, crdtExpireObj->expireTime);
            sdsfree(expire_vcStr);
        }else{
            RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllc", argv[1], argv[2], meta->gid, meta->timestamp, vclockStr);
        }
        sdsfree(vclockStr);
        freeCrdtMeta(set_meta);
    }
    if(crdtExpireObj != NULL) freeCrdtExpireObj(crdtExpireObj);
    if(meta != NULL) freeCrdtMeta(meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK"); 
    } else {
        return CRDT_ERROR;
    }
    
}
// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-milli>
// 0         1    2     3      4         5        6
// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-vc> <expire-time>
// 0         1    2     3      4         5        6             7
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtExpireObj *expireObj = NULL;
    VectorClock *expire_vclock = NULL;
    long long expire_time = 0;
    if(argc > 6) {
        expire_vclock = getVectorClockFromString(argv[6]);
        if ((RedisModule_StringToLongLong(argv[7], &expire_time) != REDISMODULE_OK)) {
            freeVectorClock(expire_vclock);
            return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        }  
    }

    CrdtMeta* meta = getMeta(ctx, argv, 3);
    int status = CRDT_OK;
    if (meta == NULL) {
        return 0;
    }
    if(expire_vclock != NULL) {
        expireObj = createCrdtExpireObj(createMeta(meta->gid, meta->timestamp, expire_vclock), expire_time);
    }

    //if key is null will be create one key
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if (moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if (tombstone != NULL && !isRegisterTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_Register* current = getCurrentValue(moduleKey);
    current = addOrUpdateRegister(ctx, moduleKey, tombstone, current, meta, argv[1], RedisModule_GetSds(argv[2]));
    if(expireObj) {
        if(expireObj->expireTime == -1) {
            addExpireTombstone(moduleKey, CRDT_REGISTER_TYPE, expireObj->meta);
        } else {
            tryAddOrUpdateExpire(moduleKey, CRDT_REGISTER_TYPE, expireObj);
        }
        
    }
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
end:
    if (meta != NULL) {
        if (meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCrdtMeta(meta);
    }
    if (expireObj != NULL) freeCrdtExpireObj(expireObj);
    if (moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK"); 
    }else{
        return CRDT_ERROR;
    }
}


int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);

    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    CRDT_Register *crdtRegister;

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtRegister) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtRegister = RedisModule_ModuleTypeGetValue(key);
    }
    sds val = crdtRegister->method->get(crdtRegister);
    if(!val) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    }
    sds reply = sdsdup(val);

    RedisModuleString *result = RedisModule_CreateString(ctx, reply, sdslen(reply));
    RedisModule_ReplyWithString(ctx, result);

    RedisModule_CloseKey(key);
    sdsfree(reply);
    return REDISMODULE_OK;
}

// <val> <gid> <timestamp> <vc>
//  0      1       2         3
int CRDT_GetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);

    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    CRDT_Register *crdtRegister;

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtRegister) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtRegister = RedisModule_ModuleTypeGetValue(key);
    }
    CrdtRegisterValue* val = crdtRegister->method->getValue(crdtRegister);
    if(!val->value) {
        RedisModule_Log(ctx, "warning", "empty val for key");
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModuleString *result = RedisModule_CreateString(ctx, val->value, sdslen(val->value));
    RedisModule_ReplyWithString(ctx, result);
    RedisModule_ReplyWithLongLong(ctx, val->meta->gid);
    RedisModule_ReplyWithLongLong(ctx, val->meta->timestamp);
    sds vclockSds = vectorClockToSds(val->meta->vectorClock);
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

/**
 * RedisModule specified functionality
 * */
int mergeCrdtRegisterValue(CrdtRegisterValue* target, CrdtRegisterValue* other) {
    int compareResult = appendCrdtMeta(target->meta, other->meta);
    if(compareResult > COMPARE_META_EQUAL) {
        if(target->value) sdsfree(target->value);
        target->value = sdsdup(other->value);
    } 
    return compareResult;
}
CRDT_Register* addRegister(void *data, CrdtMeta* meta, sds value) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL) {
        if(tombstone->method->isExpire(tombstone, meta) == CRDT_OK) {
            return NULL;
        }
    }
    CRDT_Register* r = createCrdtRegister();
    r->method->set(r, meta, value);
    return r;
}
int tryUpdateRegister(void* data, CrdtMeta* meta, CRDT_Register* reg, sds value) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL) {
        if(tombstone->method->isExpire(tombstone, meta) == CRDT_OK) {
            return COMPARE_META_VECTORCLOCK_LT;
        }
    }
    return reg->method->set(reg, meta, value);
}
VectorClock* crdtRegisterGetLastVC(void* data) {
    CRDT_Register* reg = (CRDT_Register*) data;
    return reg->method->getValue(reg)->meta->vectorClock;
}
void crdtRegisterUpdateLastVC(void *data, VectorClock* vc) {
    CRDT_Register* reg = (CRDT_Register*) data;
    reg->method->updateLastVC(reg, vc);
}