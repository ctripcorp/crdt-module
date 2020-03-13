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

int crdtRegisterDelete(void *ctx, void *keyRobj, void *key, void *value);


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
int crdtRegisterClean(CrdtObject* current, CrdtTombstone* tombstone) {
    if(!isRegister(current)) {
        return 0;
    }
    if(!isRegisterTombstone(tombstone)) {
        return 0;
    }
    CRDT_Register* reg = (CRDT_Register*) current;
    return reg->method->clean(reg, (CRDT_RegisterTombstone*)tombstone);
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
int crdtRegisterDelete(void *ctx, void *keyRobj, void *key, void *value) {
    if(value == NULL) {
        return CRDT_ERROR;
    }
    RedisModuleKey *moduleKey = (RedisModuleKey *) key;
    if(!isRegister(value)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        CrdtObject* v = (CrdtObject*)value;
        RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][crdtRegisterDelete] key:{%s} ,prev: {%s} ",
                            keyStr ,v->type);
        return CRDT_ERROR;
    }
    
    CrdtMeta* meta= createIncrMeta();
    
    CRDT_RegisterTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isRegisterTombstone(tombstone)) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    CrdtMeta* result = tombstone->method->add(tombstone, meta);
    

    sds vcSds = vectorClockToSds(result->vectorClock);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_REG", "sllc", keyRobj, result->gid, result->timestamp, vcSds);
    sdsfree(vcSds);
    freeCrdtMeta(meta);
    return CRDT_OK;
}


//CRDT.DEL_REG <key> <gid> <timestamp> <vc>
//      0        1     2         3      4
int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 4) return RedisModule_WrongArity(ctx);
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    if(meta == NULL) return CRDT_ERROR;

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
        if(tombstone->method->isMonoIncr(tombstone, meta) == CRDT_NO) {
            goto end;
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    tombstone->method->add(tombstone, meta);

    CRDT_Register* current = getCurrentValue(moduleKey);
    
    if(current != NULL) {
        if(!isRegister(current)) {
            const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
            RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][drop] key:{%s} ,prev: {%s} ",
                            keyStr ,current->parent.type);
            RedisModule_IncrCrdtConflict();      
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
            status = CRDT_ERROR;
            goto end;
        }
        if(current->parent.method->clean(current, tombstone)) {
            RedisModule_DeleteKey(moduleKey);
            deleted = 1;
        }
    }
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end: 
    if(meta != NULL) {
        if (meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCrdtMeta(meta);
    }
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }
}

int isRegister(void *data) {
    CRDT_Register* tombstone = (CRDT_Register*) data;
    if(tombstone != NULL && tombstone->parent.type == CRDT_REGISTER_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}
int isRegisterTombstone(void *data) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL && tombstone->parent.type == CRDT_REGISTER_TOMBSTONE_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}

int addOrUpdateRegister(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, CRDT_Register* tombstone, CRDT_Register* current, CrdtMeta* meta, sds value) {
    if(current == NULL) {
        current = addRegister(tombstone, meta, value);
        if(current != NULL) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        }
    }else{
        if(!isRegister(current)) {
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][type conflict] prev: {%s}",
                            current->parent.type);
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            RedisModule_IncrCrdtConflict();
            return CRDT_ERROR;
        }
        sds prev = current->method->info(current);
        //tryUpdateRegister function will be change "current" object
        int result = tryUpdateRegister(tombstone, meta, current, value);
        if(isConflictCommon(result)) {
            CRDT_Register* incomeValue = addRegister(NULL, meta, value);
            sds income = current->method->info(incomeValue);
            sds future = current->method->info(current);
            if(result > COMPARE_COMMON_EQUAL) {
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][replace] prev: {%s}, income: {%s}, future: {%s}",
                            prev, income, future);
            }else{
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][drop] prev: {%s}, income: {%s}, future: {%s}",
                                prev, income, future);
            }
            freeCrdtRegister(incomeValue);
            sdsfree(income);
            sdsfree(future);
            RedisModule_IncrCrdtConflict();
        }
        sdsfree(prev);
    }
    return CRDT_OK;
}
/**
 * CRDT Operations, including set/get, crdt.set/crdt.get
 * */
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    CrdtMeta* meta = createIncrMeta();
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CRDT_Register* current = getCurrentValue(moduleKey);
    if(addOrUpdateRegister(ctx, moduleKey, NULL, current, meta, RedisModule_GetSds(argv[2])) != CRDT_OK) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
end:
    if(meta) {
        sds vclockStr = vectorClockToSds(meta->vectorClock);
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllcl", argv[1], argv[2], meta->gid, meta->timestamp, vclockStr, 0);
        sdsfree(vclockStr);
        freeCrdtMeta(meta);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK"); 
    } else {
        return CRDT_ERROR;
    }
    
}
// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-milli>
// 0         1    2     3      4         5        6
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta* meta = getMeta(ctx, argv, 3);
    int status = CRDT_OK;
    if (meta == NULL) {
        return 0;
    }
    //to do add expire function
    long long expire;
    if ((RedisModule_StringToLongLong(argv[6], &expire) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
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
    if(addOrUpdateRegister(ctx, moduleKey, tombstone, current, meta, RedisModule_GetSds(argv[2])) != CRDT_OK) {
        status = CRDT_ERROR;
        goto end;
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
        return RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
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
    if(compareResult > COMPARE_COMMON_EQUAL) {
        if(target->value) sdsfree(target->value);
        target->value = sdsdup(other->value);
    } 
    return compareResult;
}
CRDT_Register* addRegister(void *data, CrdtMeta* meta, sds value) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL) {
        if(tombstone->method->isMonoIncr(tombstone, meta) == CRDT_NO) {
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
        if(tombstone->method->isMonoIncr(tombstone, meta) == CRDT_NO) {
            return COMPARE_COMMON_VECTORCLOCK_LT;
        }
    }
    return reg->method->set(reg, meta, value);
}
