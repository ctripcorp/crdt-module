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
// Created by zhuchen(zhuchen at ctrip dot com) on 2019-04-17.
//

#include "include/redismodule.h"

#include "crdt.h"
#include "crdt_set.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
#include "ctrip_crdt_hashmap.h"
#include "crdt_pubsub.h"
#include "ctrip_crdt_common.h"
#include "crdt_statistics.h"
#include "crdt_set.h"
#include "ctrip_crdt_zset.h"
#include <stdlib.h>
#include <stdio.h>
#define CRDT_API_VERSION 1

//crdt.ovc gid vc
int crdtOvcCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    long long gid;
    if ((RedisModule_StringToLongLong(argv[1],&gid) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid gid: must be a signed 64 bit integer");
        return CRDT_ERROR;
    }
    if(RedisModule_CheckGid(gid) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"gid must < 15");
        return CRDT_ERROR;
    }
    RedisModule_CrdtReplicateVerbatim(gid, ctx);
    if(gid != RedisModule_CurrentGid()) {
        VectorClock vclock = getVectorClockFromString(argv[2]);
        RedisModule_UpdateOvc(gid,  VC2LL(vclock));
        freeVectorClock(vclock);
    }
    return RedisModule_ReplyWithOk(ctx); 
}
//crdt.select <gid> <dbid>
int crdtSelectCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    long long id;
    long long gid;
    if ((RedisModule_StringToLongLong(argv[1],&gid) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid gid: must be a signed 64 bit integer");
        return CRDT_ERROR;
    }
    if(RedisModule_CheckGid(gid) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"gid must < 15");
        return CRDT_ERROR;
    }
    if ((RedisModule_StringToLongLong(argv[2],&id) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid id: must be a signed 64 bit integer");
        return CRDT_ERROR;
    }
    RedisModule_CrdtReplicateVerbatim(gid, ctx);
    if (RedisModule_CrdtSelectDb(ctx, gid, id) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"DB index is out of range");
        return CRDT_ERROR;
    }
    return RedisModule_ReplyWithOk(ctx);
}

int delCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) return RedisModule_WrongArity(ctx);

    int numl = 0, dirty, tmp;

    for(int i = 1; i < argc; i++) {
        tmp = numl;
        RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[i], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
        // if not found, skip this step
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            void *crdtObj = RedisModule_ModuleTypeGetValue(moduleKey);
            if (crdtObj == NULL) {
                continue;
            }
            CrdtDataMethod* method = getCrdtDataMethod(crdtObj);
            if(method == NULL) {
                continue;
            }
            int result = method->propagateDel(RedisModule_GetSelectedDb(ctx), argv[i], moduleKey, crdtObj);
            if(result) {
                RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[i]);
                numl ++;
            }
        }
        dirty = numl - tmp;
        if (dirty > 0) {
            RedisModule_DeleteKey(moduleKey);
        }
        RedisModule_CloseKey(moduleKey);
    }

    RedisModule_ReplyWithLongLong(ctx, numl);
    return REDISMODULE_OK;
}
int crdtDebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);

    if (argc < 2) return RedisModule_WrongArity(ctx);

    const char *cmd = RedisModule_StringPtrLen(argv[1], NULL);
    if (!strcasecmp(cmd,"loglevel")) {
        sdsfree(logLevel);
        logLevel = sdsnew(RedisModule_StringPtrLen(argv[2], NULL));
    } else {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

VectorClock getVectorClockFromString(RedisModuleString *vectorClockStr) {
    size_t vcStrLength;
    const char *vcStr = RedisModule_StringPtrLen(vectorClockStr, &vcStrLength);
    sds vclockSds = sdsnewlen(vcStr, vcStrLength);
    VectorClock vclock = stringToVectorClock(vclockSds);
    sdsfree(vclockSds);
    return vclock;
}
#define RDB_VALUE_EOF 0
#define C_OK 0
#define C_ERR -1
int saveValue(void *rio, RedisModuleType* type, void* data) {
    uint64_t id = RedisModule_GetModuleTypeId(type);
    if(RedisModule_SaveLen(rio, id) == C_ERR) return C_ERR;
    return RedisModule_SaveModuleValue(rio,type,data);
}
void RdbSaveCrdtValue(void* db, void *rio, RedisModuleString* key, RedisModuleKey* moduleKey) {
    CrdtObject* data = RedisModule_ModuleTypeGetValue(moduleKey);
    if(data != NULL) {
        switch (getDataType(data))
        {
        case CRDT_REGISTER_TYPE:
            saveValue(rio, getCrdtRegister(), data);
            break;
        case CRDT_HASH_TYPE:
            saveValue(rio, getCrdtHash(), data);
            break;
        case CRDT_SET_TYPE:
            saveValue(rio, getCrdtSet(), data);
            break;
        case CRDT_RC_TYPE:
            saveValue(rio, getCrdtRc(), data);
            break;
        case CRDT_ZSET_TYPE:
            saveValue(rio, getCrdtSS(), data);
            break;
        }   
    }
    CrdtTombstone* tombstone = RedisModule_ModuleTypeGetTombstone(moduleKey);
    if(tombstone != NULL) {
        switch (getDataType(tombstone))
        {
        case CRDT_REGISTER_TYPE:
            saveValue(rio, getCrdtRegisterTombstone(), tombstone);
            break;
        case CRDT_HASH_TYPE:
            saveValue(rio, getCrdtHashTombstone(), tombstone);
            break;
        case CRDT_SET_TYPE:
            saveValue(rio, getCrdtSetTombstone(), tombstone);
            break;
        case CRDT_RC_TYPE:
            saveValue(rio, getCrdtRcTombstone(), tombstone);
            break;
        case CRDT_ZSET_TYPE:
            saveValue(rio, getCrdtSST(), tombstone);
            break;
        }
    }
    
    RedisModule_SaveLen(rio, RDB_VALUE_EOF);
}

int RdbLoadCrdtValue(void* db, RedisModuleString* key, void* rio, RedisModuleKey* moduleKey) {
    int result = C_OK;
    while(1) {
        uint64_t typeId = RedisModule_LoadLen(rio);
        if (RDB_VALUE_EOF == typeId) break;
        RedisModuleType* type = RedisModule_GetModuleTypeById(typeId);
        void* data = NULL;
        if(type != NULL) {
            data = RedisModule_LoadModuleValue(rio, type);
        }
        if(data == NULL) {
            result = C_ERR;
            goto error;
        }
        if(type == getCrdtRegister() || 
            type == getCrdtHash() || 
            type == getCrdtSet() ||
            type == getCrdtRc() || 
            type == getCrdtSS()) {
            RedisModule_ModuleTypeLoadRdbAddValue(moduleKey, type, data);
        }  else if(type == getCrdtRegisterTombstone() ||
            type == getCrdtHashTombstone() ||
            type == getCrdtSetTombstone() ||
            type == getCrdtRcTombstone() ||
            type == getCrdtSST()) {
            RedisModule_ModuleTombstoneLoadRdbAddValue(moduleKey, type, data);
        } else {
            result = C_ERR;
            goto error;
        }
    }
error:
    return result;
}
int isTombstone(CrdtObject* data) {
    return getType(data) == CRDT_TOMBSTONE;
}
int isData(CrdtObject* data) {
    return getType(data) == CRDT_DATA;
}


CrdtDataMethod* getCrdtDataMethod(CrdtObject* data) {
    if(!isData(data)) {
        return NULL;
    }
    switch (getDataType(data)) {
        case CRDT_REGISTER_TYPE:
            return &RegisterDataMethod;
        case CRDT_HASH_TYPE:
            return &HashDataMethod;
        case CRDT_SET_TYPE:
            return &SetDataMethod;
        case CRDT_RC_TYPE:
            return &RcDataMethod;
        case CRDT_ZSET_TYPE:
            return &ZSetDataMethod;
        default:
            return NULL;
    }
}
CrdtObjectMethod* getCrdtObjectMethod(CrdtObject* obj) {
    if(isData(obj)) {
        switch (getDataType(obj)) {
            case CRDT_REGISTER_TYPE:
                return &RegisterCommonMethod;
            case CRDT_HASH_TYPE:
                return &HashCommonMethod;
            case CRDT_SET_TYPE:
                return &SetCommonMethod;
            case CRDT_RC_TYPE:
                return &RcCommonMethod;
            case CRDT_ZSET_TYPE:
                return &ZSetCommandMethod;
            default:
                return NULL;
        }
    } 
    return NULL;
}

CrdtTombstoneMethod* getCrdtTombstoneMethod(CrdtTombstone* tombstone) {
    if(isTombstone(tombstone)) {
        switch (getDataType(tombstone)) {
            case CRDT_REGISTER_TYPE:
                return &RegisterTombstoneMethod;
            case CRDT_HASH_TYPE:
                return &HashTombstoneCommonMethod;
            case CRDT_SET_TYPE:
                return &SetTombstoneCommonMethod;
            case CRDT_RC_TYPE:
                return &RcTombstoneCommonMethod;
            case CRDT_ZSET_TYPE:
                return &ZsetTombstoneCommonMethod;
            default:
                return NULL;
        }
    } 
    return NULL;
}
int dataCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    void *tombstoneObj = RedisModule_ModuleTypeGetTombstone(moduleKey); 
    if (RedisModule_KeyType(moduleKey) == REDISMODULE_KEYTYPE_EMPTY && tombstoneObj == NULL) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithNull(ctx);
    }
    void *crdtObj = RedisModule_ModuleTypeGetValue(moduleKey);
    if(crdtObj == NULL && tombstoneObj == NULL) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithError(ctx, "code error");
    }
    sds objInfo = NULL;
    sds tombstoneInfo = NULL;
    int num = 0;
    if (crdtObj != NULL) {
        CrdtDataMethod* omethod = getCrdtDataMethod(crdtObj);
        objInfo = omethod->info(crdtObj);
        num += 1;
    }
    if (tombstoneObj != NULL) {
        CrdtTombstoneMethod* tmethod = getCrdtTombstoneMethod(tombstoneObj);
        tombstoneInfo = tmethod->info(tombstoneObj);
        num += 1;
    }
    RedisModule_ReplyWithArray(ctx, num);
    if(objInfo != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, objInfo, sdslen(objInfo));
        sdsfree(objInfo);
    }
    if(tombstoneInfo != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, tombstoneInfo, sdslen(tombstoneInfo));
        sdsfree(tombstoneInfo);
    }
    RedisModule_CloseKey(moduleKey);
    return CRDT_OK;
}
//
int crdtGcCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 3) {return RedisModule_WrongArity(ctx);}
    long long ll = 0;
    if(RedisModule_StringToLongLong(argv[2], &ll) != REDISMODULE_OK) {
        return RedisModule_WrongArity(ctx);
    }
    sds module_name = RedisModule_GetSds(argv[1]);
    int result = 0;
    if(strcmp(module_name, "rc") == 0) {
        if(ll) { 
            result = rcStartGc();
        } else {
            result = rcStopGc();
        }
    } else if(strcmp(module_name, "zset") == 0) {
        if(ll) { 
            result = zsetStartGc();
        } else {
            result = zsetStopGc();
        }
    } else if(strcmp(module_name, "set") == 0) {
        if(ll) { 
            result = setStartGc();
        } else {
            result = setStopGc();
        }
    } else if(strcmp(module_name, "hash") == 0) {
        if(ll) {
            result = hashStartGc();
        } else {
            result = hashStopGc();
        }
    } else if(strcmp(module_name, "register") == 0) {
        if(ll) {
            result = registerStartGc();
        } else {
            result = registerStopGc();
        }
    }
    return  RedisModule_ReplyWithLongLong(ctx, result);

    
}
/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    logLevel = CRDT_DEFAULT_LOG_LEVEL;
    if (RedisModule_Init(ctx, MODULE_NAME, CRDT_API_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if(initRegisterModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "register module -- register failed");
        return REDISMODULE_ERR;
    }

    if(initCrdtHashModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "hash module -- hash failed");
        return REDISMODULE_ERR;
    }

    if(initPubsubModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "hash module -- pubsub failed");
        return REDISMODULE_ERR;
    }

    if(initCrdtExpireModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "expire module -- expire failed");
        return REDISMODULE_ERR;
    }

    if(initCrdtSetModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "set module -- set failed");
        return REDISMODULE_ERR;
    }
    if(initRcModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "counter module -- counter failed");
        return REDISMODULE_ERR;
    }
    if(initCrdtSSModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "zset module -- zset failed");
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx,"del",
                                  delCommand,"write",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"crdt.debug",
                                  crdtDebugCommand,"write",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"crdt.statistics",
                                  statisticsCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "crdt.memory", 
                                    memoryCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "crdt.dataInfo", 
                                dataCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR; 
    if (RedisModule_CreateCommand(ctx, "crdt.select", 
                                crdtSelectCommand, "write",  1, 2,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx, "crdt.ovc", 
                                crdtOvcCommand, "write",  1, 2,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;  
    if (RedisModule_CreateCommand(ctx, "crdt.debug_gc", crdtGcCommand, "allow-loading write",  1, 2,1) == REDISMODULE_ERR)  
        return REDISMODULE_ERR;               
    return REDISMODULE_OK;
}

