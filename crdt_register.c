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
static CrdtCommonMethod RegisterCommonMethod = {
    merge: crdtRegisterMerge,
    delFunc: crdtRegisterDelete
};
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
    crdtRegister->common.method = &RegisterCommonMethod;
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
    crdtRegister->common.method = NULL;
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

//todo: flag to decide whether re-use
void *crdtRegisterMerge(void *currentVal, void *value) {
    CRDT_Register *curRegister = currentVal, *targetRegister = value;
    if (currentVal == NULL) {
        return dupCrdtRegister(value);
    }
    if (isVectorClockMonoIncr(curRegister->common.vectorClock, targetRegister->common.vectorClock) == CRDT_OK) {
        return dupCrdtRegister(targetRegister);
    } else if(isVectorClockMonoIncr(targetRegister->common.vectorClock, curRegister->common.vectorClock) == CRDT_OK) {
        return dupCrdtRegister(curRegister);
    }else if(isReplacable(curRegister, targetRegister->common.timestamp, targetRegister->common.gid) == CRDT_OK) {
        CRDT_Register *result = dupCrdtRegister(targetRegister);
        freeVectorClock(result->common.vectorClock);
        result->common.vectorClock = vectorClockMerge(curRegister->common.vectorClock, targetRegister->common.vectorClock);
        return result;
    } else {
        CRDT_Register *result = dupCrdtRegister(curRegister);
        freeVectorClock(result->common.vectorClock);
        result->common.vectorClock = vectorClockMerge(curRegister->common.vectorClock, targetRegister->common.vectorClock);
        return result;
    }
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
        CrdtCommon* v = (CrdtCommon*)value;
        RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][crdtRegisterDelete] key:{%s} ,prev: {%s} ",
                            keyStr ,v->type);
        return CRDT_ERROR;
    }
    
    CrdtCommon* common= createIncrCommon();
    
    CRDT_Register *tombstone = createCrdtRegister();
    crdtCommonCp(common, tombstone);
    tombstone->val = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);
    RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegister, tombstone);

    sds vcSds = vectorClockToSds(common->vectorClock);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_REG", "sllc", keyRobj, common->gid, common->timestamp, vcSds);
    sdsfree(vcSds);
    freeCommon(common);
    return CRDT_OK;
}


//CRDT.DEL_REG <key> <gid> <timestamp> <vc>
//      0        1     2         3      4
int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 4) return RedisModule_WrongArity(ctx);
    CrdtCommon* common = getCommon(ctx, argv, 2);
    if(common == NULL) return CRDT_ERROR;

    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }

    CrdtCommon* t = getTombstone(moduleKey);
    CRDT_Register* tombstone = NULL;
    if(t != NULL) {
        int result = compareCommon(t, common);
        if(result < COMPARE_COMMON_EQUAL) {
            goto end;
        } else {
            if(isRegister(tombstone)) {
                tombstone = (CRDT_Register*)t;
            }
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtRegister();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegister, tombstone);
    }
    crdtCommonMerge(tombstone, common);
    if(tombstone->val) sdsfree(tombstone->val);
    tombstone->val = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);

    CRDT_Register* current = getCurrentValue(moduleKey);
    if(current != NULL) {
        if(!isRegister(current)) {
            const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
            RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][drop] key:{%s} ,prev: {%s} ",
                            keyStr ,current->common.type);
            RedisModule_IncrCrdtConflict();      
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
            status = CRDT_ERROR;
            goto end;
        }
        int result = compareCommon(&current->common, common);
        if(result > COMPARE_COMMON_EQUAL) {
            RedisModule_DeleteKey(moduleKey);
            deleted = 1;
        }
    }
    RedisModule_MergeVectorClock(common->gid, common->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end: 
    if(common) {
        if (common->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCommon(common);
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
    if(tombstone != NULL && tombstone->common.type == CRDT_REGISTER_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}

int addOrUpdateRegister(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, CRDT_Register* tombstone, CRDT_Register* current, CrdtCommon* common, sds value) {
    if(current == NULL) {
        current = addRegister(tombstone, common, value);
        if(current != NULL) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        }
    }else{
        if(!isRegister(current)) {
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][type conflict] prev: {%s}",
                            current->common.type);
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            RedisModule_IncrCrdtConflict();
            return CRDT_ERROR;
        }
        sds prev = crdtRegisterInfo(current);
        //tryUpdateRegister function will be change "current" object
        int result = tryUpdateRegister(tombstone, common, current, value);
        if(isConflictCommon(result)) {
            CRDT_Register* incomeValue = addRegister(NULL, common, value);
            sds income = crdtRegisterInfo(incomeValue);
            sds future = crdtRegisterInfo(current);
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
    CrdtCommon* common = createIncrCommon();
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CRDT_Register* current = getCurrentValue(moduleKey);
    if(addOrUpdateRegister(ctx, moduleKey, NULL, current, common, RedisModule_GetSds(argv[2])) != CRDT_OK) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
end:
    if(common) {
        sds vclockStr = vectorClockToSds(common->vectorClock);
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllcl", argv[1], argv[2], common->gid, common->timestamp, vclockStr, 0);
        sdsfree(vclockStr);
        freeCommon(common);
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
    CrdtCommon* common = getCommon(ctx, argv, 3);
    int status = CRDT_OK;
    if (common == NULL) {
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
    
    CrdtCommon* tombstone = getTombstone(moduleKey);
    if (tombstone != NULL && !isRegister(tombstone)) {
        if (compareCommon( common, tombstone) > COMPARE_COMMON_EQUAL) {
            goto end; 
        } else {
            tombstone = NULL;
        }
    }
    CRDT_Register* current = getCurrentValue(moduleKey);
    if(addOrUpdateRegister(ctx, moduleKey, tombstone, current, common, RedisModule_GetSds(argv[2])) != CRDT_OK) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_MergeVectorClock(common->gid, common->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
end:
    if (common != NULL) {
        if (common->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCommon(common);
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

CRDT_Register* addRegister(void *data, CrdtCommon* common, sds value) {
    CRDT_Register* tombstone = (CRDT_Register*) data;
    if(tombstone != NULL) {
        if(compareCommon(common, &tombstone->common) > COMPARE_COMMON_EQUAL) {
            return NULL;
        }
    }
    CRDT_Register* r = createCrdtRegister();
    crdtCommonCp(common, r);
    r->val = sdsdup(value);
    return r;
}
int tryUpdateRegister(void* data, CrdtCommon* common, CRDT_Register* reg, sds value) {
    CRDT_Register* tombstone = (CRDT_Register*) data;
    if(tombstone != NULL) {
        if(compareCommon(common, &(tombstone->common)) > COMPARE_COMMON_EQUAL) {
            return COMPARE_COMMON_VECTORCLOCK_LT;
        }
    }
    int compareResult = compareCommon(&(reg->common), common) ;
    if(compareResult == COMPARE_COMMON_EQUAL || compareResult == COMPARE_COMMON_VECTORCLOCK_LT) {
        return compareResult;
    }
    VectorClock* oldClock = reg->common.vectorClock;
    reg->common.vectorClock = vectorClockMerge(oldClock, common->vectorClock);
    freeVectorClock(oldClock);
    if(compareResult > COMPARE_COMMON_EQUAL) {
        if(reg->val) sdsfree(reg->val);
        reg->val = sdsdup(value);
        reg->common.timestamp = common->timestamp;
        reg->common.gid = common->gid;
    } 
    return compareResult;
}