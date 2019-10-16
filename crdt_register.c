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

void crdtRegisterDigestFunc(RedisModuleDigest *md, void *value);

int crdtRegisterDelete(void *ctx, void *keyRobj, void *key, void *value);

int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
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
            .digest = crdtRegisterDigestFunc
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

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_REG",
                                  CRDT_DelRegCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;

}

/***
 * CRDT Lifecycle functionality*/

void *createCrdtRegister(void) {
    CRDT_Register *crdtRegister = RedisModule_Alloc(sizeof(CRDT_Register));
    crdtRegister->common.gid = -1;
    crdtRegister->common.merge = crdtRegisterMerge;
    crdtRegister->common.delFunc = crdtRegisterDelete;
    crdtRegister->common.vectorClock = NULL;
    crdtRegister->common.timestamp = -1;
    crdtRegister->common.type = CRDT_REGISTER_TYPE;
    crdtRegister->val = NULL;
    return crdtRegister;
}

void freeCrdtRegister(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_Register *crdtRegister = (CRDT_Register *)obj;
    if (crdtRegister->val) {
        sdsfree(crdtRegister->val);
    }
    crdtRegister->common.merge = NULL;
    crdtRegister->common.delFunc = NULL;
    if(crdtRegister->common.vectorClock) {
        freeVectorClock(crdtRegister->common.vectorClock);
    }
    RedisModule_Free(crdtRegister);
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
    if (val->val) {
        dup->val = sdsdup(val->val);
    }
    return dup;
}

CRDT_Register *createCrdtRegisterUsingVectorClock(RedisModuleString *val, long long gid,
                                                  long long timestamp, VectorClock *vclock) {
    CRDT_Register *crdtRegister = createCrdtRegister();
    crdtRegister->common.gid = (int) gid;
    crdtRegister->common.timestamp = timestamp;
    crdtRegister->common.vectorClock = dupVectorClock(vclock);
    crdtRegister->val = moduleString2Sds(val);
    return crdtRegister;
}

void *crdtRegisterMerge(void *currentVal, void *value) {
    CRDT_Register *curRegister = currentVal, *targetRegister = value;
    if (currentVal == NULL) {
        return dupCrdtRegister(value);
    }
    if (isVectorClockMonoIncr(curRegister->common.vectorClock, targetRegister->common.vectorClock) == CRDT_OK) {
        return dupCrdtRegister(targetRegister);
    } else if(isReplacable(curRegister, targetRegister->common.timestamp, targetRegister->common.gid) == CRDT_OK) {
        CRDT_Register *result = dupCrdtRegister(targetRegister);
        freeVectorClock(result->common.vectorClock);
        result->common.vectorClock = vectorClockMerge(curRegister->common.vectorClock, targetRegister->common.vectorClock);
        return result;
    } else {
        return dupCrdtRegister(curRegister);
    }
}

int crdtRegisterDelete(void *ctx, void *keyRobj, void *key, void *value) {
    if(value == NULL) {
        return 0;
    }
    RedisModuleKey *moduleKey = (RedisModuleKey *) key;
    CRDT_Register *crdtRegister = (CRDT_Register *) value;


    long long gid = RedisModule_CurrentGid();
    long long timestamp = mstime();
    RedisModule_IncrLocalVectorClock(1);
    VectorClock *vclock = RedisModule_CurrentVectorClock();

    freeVectorClock(crdtRegister->common.vectorClock);
    crdtRegister->common.vectorClock = vclock;
    crdtRegister->common.gid = (int) gid;
    crdtRegister->common.timestamp = timestamp;

    CRDT_Register *tombstoneCrdtRegister = dupCrdtRegister(crdtRegister);
    RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegister, tombstoneCrdtRegister);

    sds vcSds = vectorClockToSds(vclock);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_REG", "sllc", keyRobj, gid, timestamp, vcSds);
    sdsfree(vcSds);

    return 1;
}

//CRDT.DEL_REG <key> <gid> <timestamp> <vc>
//      0        1     2         3      4
int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);

    long long gid;
    if ((RedisModule_StringToLongLong(argv[2],&gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[3],&timestamp) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    VectorClock *vclock = getVectorClockFromString(argv[4]);
    RedisModule_MergeVectorClock(gid, vclock);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);

    CRDT_Register *tombstone = RedisModule_ModuleTypeGetTombstone(key);
    if (tombstone == NULL || tombstone->common.type != CRDT_REGISTER_TYPE) {
        tombstone = createCrdtRegister();
        tombstone->val = sdsnew("deleted");
        RedisModule_ModuleTombstoneSetValue(key, CrdtRegister, tombstone);
    }
    VectorClock *toFree = tombstone->common.vectorClock;
    tombstone->common.vectorClock = dupVectorClock(vclock);
    tombstone->common.timestamp = timestamp;
    tombstone->common.gid = (int) gid;

    unsigned char deleted = 0;
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        CRDT_Register *crdtObj = RedisModule_ModuleTypeGetValue(key);
        if (crdtObj != NULL && isVectorClockMonoIncr(crdtObj->common.vectorClock, vclock) == CRDT_OK) {
            RedisModule_DeleteKey(key);
            deleted = 1;
        }
    }
    RedisModule_CloseKey(key);

    sds vcSds = vectorClockToSds(vclock);
    if (gid == RedisModule_CurrentGid()) {
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_REG", "sllc", argv[1], gid, timestamp, vcSds);
    } else {
        RedisModule_ReplicateStraightForward(ctx, "CRDT.DEL_REG", "sllc", argv[1], gid, timestamp, vcSds);
    }
    sdsfree(vcSds);

    if (vclock) {
        freeVectorClock(vclock);
    }

    if (toFree) {
        freeVectorClock(toFree);
    }
    return RedisModule_ReplyWithLongLong(ctx, deleted);
}

/**
 * CRDT Operations, including set/get, crdt.set/crdt.get
 * */
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);

    long long gid = RedisModule_CurrentGid();
    RedisModule_IncrLocalVectorClock(1);
    // Abstract the logic of local set to match the common process
    // Here, we consider the op vclock we're trying to broadcasting is our vcu wrapped vector clock
    // for example, our gid is 1,  our vector clock is (1:100,2:1,3:100)
    // A set operation will firstly drive the vclock into (1:101,2:1,3:100).
    VectorClock *currentVectorClock = RedisModule_CurrentVectorClock();

    RedisModuleKey *moduleKey;
    moduleKey = RedisModule_OpenKey(ctx, argv[1],
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(moduleKey);

    CRDT_Register *target = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(moduleKey) != CrdtRegister) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        target = RedisModule_ModuleTypeGetValue(moduleKey);
    }

    /**!!! important: RedisModule_ModuleTypeSetValue will automatically call free function to free the crdtRegister
     * So the next time we free a register, the sds will be an invalid memory space
     * which , will crash the redis
     * do not call --
     * if(target) {
        freeCrdtRegister(target);
        }
     * */
    VectorClock *opVectorClock = NULL;
    if (target) {
        opVectorClock = vectorClockMerge(target->common.vectorClock, currentVectorClock);
        freeVectorClock(currentVectorClock);
    } else {
        opVectorClock = currentVectorClock;
    }
    long long timestamp = mstime();
    CRDT_Register *current = createCrdtRegisterUsingVectorClock(argv[2], gid, timestamp, opVectorClock);
    RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
    RedisModule_CloseKey(moduleKey);

    /*
     * sent to both my slaves and my peer slaves */
    sds vclockStr = vectorClockToSds(opVectorClock);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllc", argv[1], argv[2], gid, timestamp, vclockStr);
    sdsfree(vclockStr);

    if (opVectorClock != NULL) {
        freeVectorClock(opVectorClock);
    }

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-milli>
// 0         1    2     3      4         5        6
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

    VectorClock *vclock = getVectorClockFromString(argv[5]);
    RedisModule_MergeVectorClock(gid, vclock);

    RedisModuleKey *moduleKey;
    moduleKey = RedisModule_OpenKey(ctx, argv[1],
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(moduleKey);

    CRDT_Register *target = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(moduleKey) != CrdtRegister) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        target = RedisModule_ModuleTypeGetValue(moduleKey);
    }

    /* 1. target key not exist, and not deleted(tombstone)
     * 2. target key exists(so tombstone is meaningless, because a key is already exist), but due to LWW, previous one fails
     * either way, we do a update
     * */
    /**!!! important: RedisModule_ModuleTypeSetValue will automatically call free function to free the crdtRegister
     * So the next time we free a register, the sds will be an invalid memory space
     * which , will crash the redis, don't call below command
     * if(target) {
        freeCrdtRegister(target);
        }
     * */
    int replicate = CRDT_NO;
    VectorClock *opVectorClock = NULL;
    // newly added element
    if (target == NULL) {
        if (!isPartialOrderDeleted(moduleKey, vclock)) {
            opVectorClock = vectorClockMerge(NULL, vclock);
            CRDT_Register *current = createCrdtRegisterUsingVectorClock(argv[2], gid, timestamp, opVectorClock);
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
            replicate = CRDT_YES;
        }
    } else {
        VectorClock *currentVclock = target->common.vectorClock;
        if (isVectorClockMonoIncr(currentVclock, vclock) == CRDT_OK) {
            opVectorClock = vectorClockMerge(currentVclock, vclock);
            CRDT_Register *current = createCrdtRegisterUsingVectorClock(argv[2], gid, timestamp, opVectorClock);
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
            replicate = CRDT_YES;
        } else {
            CRDT_Register *current;
            if (isReplacable(target, timestamp, gid) == CRDT_OK) {
                opVectorClock = vectorClockMerge(currentVclock, vclock);
                current = createCrdtRegisterUsingVectorClock(argv[2], gid, timestamp, vclock);
                sds info1 = crdtRegisterInfo(target);
                sds info2 = crdtRegisterInfo(current);
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][replace] current: {%s}, future: {%s}",
                                info1, info2);
                sdsfree(info1);
                sdsfree(info2);
                // update vector clock: because we want to print out the conflict info first, but we still need the correct vector clock
                // that's why I decide to update vclock here
                freeVectorClock(current->common.vectorClock);
                current->common.vectorClock = dupVectorClock(opVectorClock);

                RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
                replicate = CRDT_YES;
            } else {
                current = createCrdtRegisterUsingVectorClock(argv[2], gid, timestamp, vclock);
                sds info1 = crdtRegisterInfo(target), info2 = crdtRegisterInfo(current);
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][drop] current: {%s}, dropped: {%s}",
                                info1, info2);
                sdsfree(info1);
                sdsfree(info2);
                freeCrdtRegister(current);
            }
            RedisModule_IncrCrdtConflict();
        }
    }
    RedisModule_CloseKey(moduleKey);

    if (replicate) {
        sds vclockStr = vectorClockToSds(opVectorClock);
        if (gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllc", argv[1], argv[2], gid, timestamp, vclockStr);
        } else {
            RedisModule_ReplicateStraightForward(ctx, "CRDT.SET", "ssllc", argv[1], argv[2], gid, timestamp, vclockStr);
        }
        sdsfree(vclockStr);
    }
    if (opVectorClock != NULL) {
        freeVectorClock(opVectorClock);
    }

    if (vclock) {
        freeVectorClock(vclock);
    }

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
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

    if(!crdtRegister->val) {
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

/**
 * RedisModule specified functionality
 * */

void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_Register *crdtRegister = createCrdtRegister();
    crdtRegister->common.gid = RedisModule_LoadSigned(rdb);
    crdtRegister->common.type = CRDT_REGISTER_TYPE;
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

void crdtRegisterDigestFunc(RedisModuleDigest *md, void *value) {
    CRDT_Register *crdtRegister = (CRDT_Register *) value;
    RedisModule_DigestAddLongLong(md, crdtRegister->common.gid);
    RedisModule_DigestAddLongLong(md, crdtRegister->common.timestamp);
    sds vclockStr = vectorClockToSds(crdtRegister->common.vectorClock);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) crdtRegister->val, sdslen(crdtRegister->val));
    RedisModule_DigestEndSequence(md);
}

sds crdtRegisterInfo(CRDT_Register *crdtRegister) {
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(crdtRegister->common.vectorClock);
    result = sdscatprintf(result, "gid: %d, timestamp: %lld, vector-clock: %s, val: %s",
            crdtRegister->common.gid, crdtRegister->common.timestamp, vcStr, crdtRegister->val);
    sdsfree(vcStr);
    return result;
}
