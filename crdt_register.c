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

int crdtRegisterInsert(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val,
                       long long gid, long long timestamp, RedisModuleString *vectorClock);

void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver);

void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value);

void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);

size_t crdtRegisterMemUsageFunc(const void *value);

void *crdtRegisterMerge(void *currentVal, void *value);

/**
 * ==============================================Register module init=========================================================*/

static RedisModuleType *CrdtRegister;

int initRegisterModule(RedisModuleCtx *ctx) {

    RedisModuleTypeMethods tm = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtRegister,
            .rdb_save = RdbSaveCrdtRegister,
            .aof_rewrite = AofRewriteCrdtRegister,
            .mem_usage = crdtRegisterMemUsageFunc,
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
    crdtRegister->common.merge = crdtRegisterMerge;
    crdtRegister->common.vectorClock = NULL;
    crdtRegister->common.timestamp = -1;
    crdtRegister->val = NULL;
    return crdtRegister;
}

void freeCrdtRegister(void *obj) {
    CRDT_Register *crdtRegister = (CRDT_Register *)obj;
    sdsfree(crdtRegister->val);
    crdtRegister->common.merge = NULL;
    if(crdtRegister->common.vectorClock) {
        freeVectorClock(crdtRegister->common.vectorClock);
    }
    RedisModule_Free(crdtRegister);
    obj = NULL;
    crdtRegister = NULL;
}

int isReplacable(CRDT_Register *target, long long timestamp, long long gid) {
    if(target->common.timestamp < timestamp) {
        return CRDT_OK;
    } else if(target->common.timestamp == timestamp) {
        return SECOND_HIGHER_PRIORITY(target->common.gid, gid);
    }
    return CRDT_ERROR;
}

CRDT_Register* dupCrdtRegister(const CRDT_Register *val) {
    CRDT_Register *dup = createCrdtRegister();
    dup->common.gid = val->common.gid;
    dup->common.timestamp = val->common.timestamp;
    dup->common.vectorClock = dupVectorClock(val->common.vectorClock);
    dup->val = sdsdup(val->val);
    return dup;
}

void *crdtRegisterMerge(void *currentVal, void *value){
    if (currentVal == NULL) {
        return value;
    }
    CRDT_Register *curRegister = currentVal, *targetRegister = value;
    if (isVectorClockMonoIncr(curRegister->common.vectorClock, targetRegister->common.vectorClock) == CRDT_OK
            || isReplacable(curRegister, targetRegister->common.timestamp, targetRegister->common.gid) == CRDT_OK) {
        return dupCrdtRegister(targetRegister);
    } else {
        return dupCrdtRegister(curRegister);
    }
}

/**
 * CRDT Operations, including set/get, crdt.set/crdt.get
 * */
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);

    long long gid = RedisModule_CurrentGid();
    long long delta = 1;
    RedisModule_IncrLocalVectorClock(delta);
    RedisModuleString *vc = RedisModule_CurrentVectorClock(ctx);

    long long timestmap = RedisModule_Milliseconds();
    if(crdtRegisterInsert(ctx, argv[1], argv[2], gid, timestmap, vc) == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        return RedisModule_ReplyWithError(ctx, "");
    }
}

// CRDT.SET key <val> <gid> <timestamp> <vc>
// 0         1    2     3      4         5
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


    if(crdtRegisterInsert(ctx, argv[1], argv[2], gid, timestamp, argv[5]) == CRDT_OK) {
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

    if(!crdtRegister->val) {
        RedisModule_Log(ctx, "warning", "empty val for key");
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModuleString *result = RedisModule_CreateString(ctx, crdtRegister->val, sdslen(crdtRegister->val));
    RedisModule_ReplyWithString(ctx, result);
    RedisModule_ReplyWithLongLong(ctx, crdtRegister->common.gid);
    RedisModule_ReplyWithLongLong(ctx, crdtRegister->common.timestamp);
    sds vclockSds = vectorClockToSds(crdtRegister->common.vectorClock);
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

CRDT_Register *createCrdtRegisterUsingVectorClock(RedisModuleString *val, long long gid,
                                                    long long timestamp, VectorClock *vclock) {
    size_t sdsLength;
    const char *str = RedisModule_StringPtrLen(val, &sdsLength);

    CRDT_Register *current = createCrdtRegister();
    current->common.gid = gid;
    current->common.timestamp = timestamp;
    current->common.vectorClock = vclock;
    current->val = sdsnewlen(str, sdsLength);
    return current;

}
// CRDT.SET key <val> <gid> <timestamp> <vc>
// 0         1    2     3      4         5
int crdtRegisterInsert(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *val,
        long long gid, long long timestamp, RedisModuleString *vectorClock) {

    RedisModuleKey *moduleKey;
    moduleKey = RedisModule_OpenKey(ctx, key,
                                    REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(moduleKey);

    CRDT_Register *target = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        target = RedisModule_ModuleTypeGetValue(moduleKey);
    }

    size_t vcStrLength;
    const char *vcStr = RedisModule_StringPtrLen(vectorClock, &vcStrLength);
    sds vclockSds = sdsnewlen(vcStr, vcStrLength);
    VectorClock *newVclock = sdsToVectorClock(vclockSds);
    sdsfree(vclockSds);
    /* 1. target key not even exist
     * 2. target key exists, but due to LWW, previous one fails
     * either way, we do a update
     * */
    int replicate = 0;
    if(!target || gid == RedisModule_CurrentGid()) {
        RedisModule_MergeVectorClock(gid, vectorClock);
        CRDT_Register *current = createCrdtRegisterUsingVectorClock(val, gid, timestamp, newVclock);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        replicate = 1;
    } else {
        VectorClock *prevVc = target->common.vectorClock;
        if (isVectorClockMonoIncr(prevVc, newVclock) == CRDT_OK
                || isReplacable(target, timestamp, gid) == CRDT_OK) {

            RedisModule_MergeVectorClock(gid, vectorClock);
            CRDT_Register *current = createCrdtRegisterUsingVectorClock(val, gid, timestamp, newVclock);
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
            replicate = 1;
        }
    }
    RedisModule_CloseKey(moduleKey);

    /**!!! important: RedisModule_ModuleTypeSetValue will automatically call free function to free the crdtRegister
     * So the next time we free a register, the sds will be an invalid memory space
     * which , will crash the redis
     * if(target) {
        freeCrdtRegister(target);
        }
     * */

    //update here
    if (replicate == 1) {
        if (gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "sslls", key, val, gid, timestamp, vectorClock);
        } else {
            RedisModule_ReplicateStraightForward(ctx, "CRDT.SET", "sslls", key, val, gid, timestamp, vectorClock);
        }
    }
    return CRDT_OK;
}

/**
 * RedisModule specified functionality
 * */

void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_Register *crdtRegister = createCrdtRegister();
    crdtRegister->common.gid = RedisModule_LoadSigned(rdb);
    crdtRegister->common.timestamp = RedisModule_LoadSigned(rdb);
    size_t vcLength;
    char* vcStr = RedisModule_LoadStringBuffer(rdb, &vcLength);
    sds vclockSds = sdsnewlen(vcStr, vcLength);
    crdtRegister->common.vectorClock = sdsToVectorClock(vclockSds);
    sdsfree(vclockSds);
    RedisModule_Free(vcStr);

    size_t sdsLength;
    char* str = RedisModule_LoadStringBuffer(rdb, &sdsLength);
    sds val = sdsnewlen(str, sdsLength);
    crdtRegister->val = val;
    RedisModule_Free(str);

    return crdtRegister;
}

void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value) {
    CRDT_Register *crdtRegister = value;
    RedisModule_SaveSigned(rdb, crdtRegister->common.gid);
    RedisModule_SaveSigned(rdb, crdtRegister->common.timestamp);
    sds vclockStr = vectorClockToSds(crdtRegister->common.vectorClock);
    RedisModule_SaveStringBuffer(rdb, vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    RedisModule_SaveStringBuffer(rdb, crdtRegister->val, sdslen(crdtRegister->val));
}

void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CRDT_Register *crdtRegister = (CRDT_Register *) value;
    sds vclockSds = vectorClockToSds(crdtRegister->common.vectorClock);
    RedisModule_EmitAOF(aof, "CRDT.SET", "scllc", key, crdtRegister->val, crdtRegister->common.gid, crdtRegister->common.timestamp, vclockSds);
    sdsfree(vclockSds);
}

size_t crdtRegisterMemUsageFunc(const void *value) {
    CRDT_Register *crdtRegister = (CRDT_Register *) value;
    size_t valSize = sizeof(CRDT_Register) + sdsAllocSize(crdtRegister->val);
    int vclcokNum = crdtRegister->common.vectorClock->length;
    size_t vclockSize = vclcokNum * sizeof(VectorClockUnit) + sizeof(VectorClock);
    return valSize + vclockSize;
}
