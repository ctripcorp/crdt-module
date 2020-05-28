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
#include <strings.h>
/**
 * ==============================================Pre-defined functions=========================================================*/

int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int mgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CRDT_GetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int setexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);
int msetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
size_t crdtRegisterMemUsageFunc(const void *value);

CrdtObject* crdtRegisterFilter(CrdtObject* common, int gid, long long logic_time);

void crdtRegisterDigestFunc(RedisModuleDigest *md, void *value);

int crdtRegisterDelete(int dbId, void *keyRobj, void *key, void *value);


int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/**
 * ==============================================Register module init=========================================================*/
RedisModuleType* getCrdtRegister() {
    return CrdtRegister;
}
RedisModuleType* getCrdtRegisterTombstone() {
    return CrdtRegisterTombstone;
}

CrdtObject *crdtRegisterMerge(CrdtObject *currentVal, CrdtObject *value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    CRDT_Register *current = (CRDT_Register*) currentVal;
    CRDT_Register *v = (CRDT_Register*) value;
    if(current == NULL) {
        return dupCrdtRegister(v);
    }
    if(v == NULL) {
        return dupCrdtRegister(current);
    }
    return mergeRegister(currentVal, value);
}
CrdtObject* crdtRegisterFilter(CrdtObject* common, int gid, long long logic_time) {
    return filterRegister(common, gid, logic_time);
}
int isRegister(void *data) {
    CRDT_Register* reg = (CRDT_Register*) data;
    if(reg != NULL && getDataType(reg) == CRDT_REGISTER_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}
int isRegisterTombstone(void *data) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL && getDataType(tombstone) == CRDT_REGISTER_TYPE) {
        return CRDT_OK;
    } 
    return CRDT_NO;
}
int crdtRegisterTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current) {
    if(!isRegister((void*)current)) {
        return 0;
    }
    if(!isRegisterTombstone((void*)tombstone)) {
        return 0;
    }
    CRDT_Register* reg = (CRDT_Register*) current;
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*)tombstone;
    return purageRegisterTombstone(t, reg);
}

CrdtTombstone* crdtRegisterTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other) {
    if(!isRegisterTombstone(target) || !isRegisterTombstone(other)) {
        return NULL;
    }
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*) target;
    return mergeRegisterTombstone(t, (CRDT_RegisterTombstone*) other);
}



CrdtObject* crdtRegisterTombstoneFilter(CrdtObject* target, int gid, long long logic_time) {
    if(!isRegisterTombstone(target)) {
        return NULL;
    }
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*) target;
    return filterRegisterTombstone(t, gid, logic_time);
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
    if (RedisModule_CreateCommand(ctx, "MGET", 
                                    mgetCommand, "readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.GET",
                                  CRDT_GetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_REG",
                                  CRDT_DelRegCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "SETEX", 
                                    setexCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "MSET", 
                                    msetCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;

}


/***
 * CRDT Lifecycle functionality*/



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
    appendVCForMeta(del_meta, getCrdtRegisterLastVc(current));
    CRDT_RegisterTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isRegisterTombstone(tombstone)) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    addRegisterTombstone(tombstone, del_meta);
    sds vcSds = vectorClockToSds(getMetaVectorClock(del_meta));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_REG", "sllc", keyRobj, getMetaGid(del_meta), getMetaTimestamp(del_meta), vcSds);
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
        if(isExpireCrdtTombstone(tombstone, del_meta) == CRDT_OK) {
            goto end;
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    addRegisterTombstone(tombstone, del_meta);

    CRDT_Register* current = getCurrentValue(moduleKey);
    
    if(current != NULL) {
        if(isRegister(current) != CRDT_OK) {
            const char* keyStr = RedisModule_StringPtrLen(argv[1], NULL);
            RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][drop] key:{%s} ,prev: {%s} ",
                            keyStr ,current->type);
            RedisModule_IncrCrdtConflict();      
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
            status = CRDT_ERROR;
            goto end;
        }
        if(crdtRegisterTombstonePurage(tombstone, current)) {
            RedisModule_DeleteKey(moduleKey);
            deleted = 1;
        }
    }
    RedisModule_MergeVectorClock(getMetaGid(del_meta), getMetaVectorClockToLongLong(del_meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end: 
    if(del_meta != NULL) {
        if (getMetaGid(del_meta) == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCrdtMeta(del_meta);
    }
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }
}


CRDT_Register* addOrUpdateRegister(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, CRDT_RegisterTombstone* tombstone, CRDT_Register* current, CrdtMeta* meta, RedisModuleString* key,sds value) {
    if(current == NULL) {
        current = addRegister(tombstone, meta, value);
        if(current != NULL) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        }
    }else{
        if(!isRegister(current)) {
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][type conflict] {key: %s} prev: {%d}",
                            RedisModule_GetSds(key),getDataType(current));
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            RedisModule_IncrCrdtConflict();
            return NULL;
        }
        sds prev = crdtRegisterInfo(current);
        //tryUpdateRegister function will be change "current" object
        int result = tryUpdateRegister(tombstone, meta, current, value);
        if(isConflictCommon(result) == CRDT_OK) {
            CRDT_Register* incomeValue = addRegister(NULL, meta, value);
            sds income = crdtRegisterInfo(incomeValue);
            sds future = crdtRegisterInfo(current);
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
int setGenericCommand(RedisModuleCtx *ctx,  int flags, RedisModuleString* key, RedisModuleString* val, RedisModuleString* expire, int unit, int sendtype) {
    int result = 0;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, key, CrdtRegister);
    CrdtMeta* set_meta = NULL;
    if(moduleKey == NULL) {
        result = 0;
        goto end;
    }
    
    CRDT_Register* current = getCurrentValue(moduleKey);
    if((current != NULL && flags & OBJ_SET_NX) 
        || (current == NULL && flags & OBJ_SET_XX)) {
        if(sendtype) RedisModule_ReplyWithNull(ctx);  
        result = 0;   
        goto end;
    }
    long long milliseconds = 0;
    if (expire) {
        if (RedisModule_StringToLongLong(expire, &milliseconds) != REDISMODULE_OK) {
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx, "ERR syntax error\r\n");
            goto end;
        }
        if (milliseconds <= 0) {
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx,"invalid expire time in set\r\n");
            goto end;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }
    set_meta = createIncrMeta();
    if(current != NULL) {
        appendVCForMeta(set_meta, getCrdtRegisterLastVc(current));
    }
    current = addOrUpdateRegister(ctx, moduleKey, NULL, current, set_meta, key, RedisModule_GetSds(val));
    if(current == NULL) {
        result = 0;
        if(sendtype) RedisModule_ReplyWithSimpleString(ctx,"set error\r\n");
        goto end;
    }
    long long expire_time = -2;
    if(expire) {
        expire_time = getMetaTimestamp(set_meta) + milliseconds;
        setExpire(moduleKey, expire_time);
    }else if(!(flags & OBJ_SET_KEEPTTL)){
        setExpire(moduleKey, -1);
        expire_time = -1;
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
    if(expire) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "expire", key);
    }
    result = CRDT_OK;
end:
    if(set_meta) {
        sds vclockStr = vectorClockToSds(getMetaVectorClock(set_meta));
        if(expire_time > -2) {
            if(sendtype) {
                RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllcl", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vclockStr,  expire_time);
            } else {
                RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllcl", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vclockStr,  expire_time);
            }
        }else{
            if(sendtype) {
                RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.SET", "ssllc", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vclockStr);
            } else {
                RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllc", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vclockStr);
            }
        }
        sdsfree(vclockStr);
        freeCrdtMeta(set_meta);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
}
int msetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int start_index = 1;
    int result = 0;
    for (int i = start_index; i < argc; i+=2) {
        RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[i], CrdtRegister);
        if(moduleKey == NULL) {
            return 0;
        }
        RedisModule_CloseKey(moduleKey);
    }
    for (int i = start_index; i < argc; i+=2) {
        result += setGenericCommand(ctx, OBJ_SET_NO_FLAGS, argv[i], argv[i + 1], NULL, UNIT_SECONDS, 1);
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    RedisModuleString* expire = NULL;
    int flags = OBJ_SET_NO_FLAGS;
    int j;
    int unit = UNIT_SECONDS;
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
            RedisModule_ReplyWithError(ctx, "ERR syntax error");
            return CRDT_ERROR;
        }
    }
    int result = setGenericCommand(ctx, flags, argv[1], argv[2], expire, unit, 1);
    if(result == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        return CRDT_ERROR;
    }
    
    
}
//setex key  expire value
int setexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 4) return RedisModule_WrongArity(ctx);
    int result = setGenericCommand(ctx, OBJ_SET_NO_FLAGS | OBJ_SET_EX, argv[1], argv[3], argv[2], UNIT_SECONDS, 1);
    if(result == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        return CRDT_ERROR;
    } 
}
// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-milli>
// 0         1    2     3      4         5        6
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 6) return RedisModule_WrongArity(ctx);
    long long expire_time = -2;
    if(argc > 6) {
        if ((RedisModule_StringToLongLong(argv[6], &expire_time) != REDISMODULE_OK)) {
            return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        }  
    }

    CrdtMeta* meta = getMeta(ctx, argv, 3);
    int status = CRDT_OK;
    if (meta == NULL) {
        return 0;
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
    if(expire_time != -2) {
        trySetExpire(moduleKey, argv[1], getMetaTimestamp(meta),  CRDT_REGISTER_TYPE, expire_time);
    }
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
end:
    if (meta != NULL) {
        if (getMetaGid(meta) == RedisModule_CurrentGid()) {
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
int getGeneric(RedisModuleCtx* ctx, RedisModuleString *key, int sendtype) {
    RedisModule_AutoMemory(ctx);

    RedisModuleKey *modulekey = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);

    CRDT_Register *crdtRegister;

    if (RedisModule_KeyType(modulekey) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(modulekey);
        RedisModule_ReplyWithNull(ctx);
        return CRDT_ERROR;
    } else if (RedisModule_ModuleTypeGetType(modulekey) != CrdtRegister) {
        RedisModule_CloseKey(modulekey);
        if(sendtype) {
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        } else {
            RedisModule_ReplyWithNull(ctx);
        }
        return CRDT_ERROR;
    } else {
        crdtRegister = RedisModule_ModuleTypeGetValue(modulekey);
    }
    sds val = getCrdtRegisterLastValue(crdtRegister);
    if(!val) {
        RedisModule_CloseKey(modulekey);
        RedisModule_ReplyWithNull(ctx);
        return CRDT_ERROR;
    }
    RedisModuleString *result = RedisModule_CreateString(ctx, val, sdslen(val));
    RedisModule_ReplyWithString(ctx, result);
    RedisModule_CloseKey(modulekey);
    return CRDT_OK;
}
int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);

    if (argc != 2) return RedisModule_WrongArity(ctx);

    getGeneric(ctx, argv[1], 1);
    return REDISMODULE_OK;
}
//mget k...
int mgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    RedisModule_ReplyWithArray(ctx, argc - 1);
    for(int i = 1; i < argc; i++) {
        getGeneric(ctx, argv[i], 0);
    }
    return CRDT_OK;
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
    sds val = getCrdtRegisterLastValue(crdtRegister);
    if(!val) {
        RedisModule_Log(ctx, "warning", "empty val for key");
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    }

    RedisModule_ReplyWithArray(ctx, 4);
    
    RedisModuleString *result = RedisModule_CreateString(ctx, val, sdslen(val));
    RedisModule_ReplyWithString(ctx, result);
    RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastGid(crdtRegister));
    RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastTimestamp(crdtRegister));
    sds vclockSds = vectorClockToSds(getCrdtRegisterLastVc(crdtRegister));
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

CRDT_Register* addRegister(void *data, CrdtMeta* meta, sds value) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL) {
        if(isExpireCrdtTombstone(tombstone, meta) == CRDT_OK) {
            return NULL;
        }
    }
    CRDT_Register* r = createCrdtRegister();
    setCrdtRegister(r, meta, value);
    return r;
}
int tryUpdateRegister(void* data, CrdtMeta* meta, CRDT_Register* reg, sds value) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL) {
        if(isExpireCrdtTombstone(tombstone, meta) == CRDT_OK) {
            return COMPARE_META_VECTORCLOCK_LT;
        }
    }
    return setCrdtRegister(reg, meta, value);
}

