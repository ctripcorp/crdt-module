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
#include "utils.h"
#include "crdt.h"
#include "config.h"

/**
 * ==============================================Pre-defined functions=========================================================*/

int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int CRDT_GetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int crdtRegisterInsert(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val, long long gid, long long timestamp);

void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver);

void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value);

void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);

/**
 * ==============================================Register module init=========================================================*/

static RedisModuleType *CrdtRegister;

int initRegisterModule(RedisModuleCtx *ctx) {

    RedisModuleTypeMethods tm = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtRegister,
            .rdb_save = RdbSaveCrdtRegister,
            .aof_rewrite = AofRewriteCrdtRegister,
            .mem_usage = NULL,
            .free = freeCrdtRegister,
            .digest = NULL
    };

    CrdtRegister = RedisModule_CreateDataType(ctx, CRDT_REGISTER_DATATYPE_NAME, 0, &tm);
    if (CrdtRegister == NULL) return REDISMODULE_ERR;

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

    return REDISMODULE_OK;

}

/***
 * CRDT Lifecycle functionality*/

void *createCrdtRegister(void) {
    CRDT_Register *crdtRegister = RedisModule_Alloc(sizeof(CRDT_Register));
//    crdtRegister->vc = RedisModule_Alloc(sizeof(VectorClock));
    return crdtRegister;
}

void freeCrdtRegister(void *obj) {
    CRDT_Register *crdtRegister = (CRDT_Register *)obj;
    sdsfree(crdtRegister->val);
    RedisModule_Free(crdtRegister);
}

/**
 * CRDT Operations, including set/get, crdt.set/crdt.get
 * */
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);

    if(crdtRegisterInsert(ctx, argv[1], argv[2], CRDT_GlobalConfig.gid, mstime()) == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        return RedisModule_ReplyWithError(ctx, "");
    }
}

// CRDT.SET key <val> <gid> <timestamp>
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);

    long long gid;
    if ((RedisModule_StringToLongLong(argv[3],&gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[4],&timestamp) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    if(crdtRegisterInsert(ctx, argv[1], argv[2], gid, timestamp) == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        return RedisModule_ReplyWithError(ctx, "Execute fail");
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
        return RedisModule_ReplyWithNull(ctx); //RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
    } else {
        crdtRegister = RedisModule_ModuleTypeGetValue(key);
    }

    if(!crdtRegister->val) {
        sdsfree(crdtRegister->val);
        RedisModule_Log(ctx, "warning", "empty val for key");
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    }
    sds reply = sdsdup(crdtRegister->val);

    RedisModuleString *result = RedisModule_CreateString(ctx, reply, sdslen(reply));
    RedisModule_ReplyWithString(ctx, result);

    RedisModule_CloseKey(key);
    sdsfree(reply);
    return REDISMODULE_OK;
}

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

    if(!crdtRegister->val) {
        RedisModule_Log(ctx, "warning", "empty val for key");
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithArray(ctx, 3);
    RedisModuleString *result = RedisModule_CreateString(ctx, crdtRegister->val, sdslen(crdtRegister->val));
    RedisModule_ReplyWithString(ctx, result);
    RedisModule_ReplyWithLongLong(ctx, crdtRegister->gid);
    RedisModule_ReplyWithLongLong(ctx, crdtRegister->timestamp);

    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int isReplacable(CRDT_Register* target, long long timestamp, long long gid) {
    if(target->timestamp < timestamp) {
        return CRDT_OK;
    } else if(target->timestamp == timestamp) {
        return SECOND_HIGHER_PRIORITY(target->gid, gid);
    }
    return CRDT_ERROR;
}

int crdtRegisterInsert(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val, long long gid, long long timestamp) {

    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, key,
                                                    REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(moduleKey);

    CRDT_Register *target = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        target = RedisModule_ModuleTypeGetValue(moduleKey);
    }

    /* 1. target key not even exist
     * 2. target key exists, but due to LWW, previous one fails
     * either way, we do a update
     * */
    if(!target || isReplacable(target, timestamp, gid)) {
        CRDT_Register *current = createCrdtRegister();
        current->timestamp = timestamp;
        current->gid = gid;
        size_t sdsLength;
        const char *str = RedisModule_StringPtrLen(val, &sdsLength);
        current->val = sdsnewlen(str, sdsLength);

        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
    }

    RedisModule_CloseKey(moduleKey);

    /**!!! important: RedisModule_ModuleTypeSetValue will automatically call free function to free the crdtRegister
     * So the next time we free a register, the sds will be an invalid memory space
     * which , will crash the redis*/
//    if(target) {
//        freeCrdtRegister(target);
//    }

    RedisModule_ReplicateStraightForward(ctx, "CRDT.SET", "ssll", key, val, gid, timestamp);

    return CRDT_OK;
}

/**
 * RedisModule specified functionality
 * */

void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
        return NULL;
    }
    CRDT_Register *crdtRegister = createCrdtRegister();
    crdtRegister->gid = RedisModule_LoadUnsigned(rdb);
    crdtRegister->timestamp = RedisModule_LoadUnsigned(rdb);
    size_t sdsLength;
    char* str = RedisModule_LoadStringBuffer(rdb, &sdsLength);
    sds val = sdsnewlen(str, sdsLength);
    crdtRegister->val = val;

    return crdtRegister;
}

void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value) {
    CRDT_Register *crdtRegister = value;
    RedisModule_SaveUnsigned(rdb, crdtRegister->gid);
    RedisModule_SaveUnsigned(rdb, crdtRegister->timestamp);
    RedisModule_SaveStringBuffer(rdb, crdtRegister->val, sdslen(crdtRegister->val));
}

void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CRDT_Register *crdtRegister = (CRDT_Register *) value;
    RedisModule_EmitAOF(aof, "CRDT.SET", "sll", key, crdtRegister->val, crdtRegister->gid, crdtRegister->timestamp);
}