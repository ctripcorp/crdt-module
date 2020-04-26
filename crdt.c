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
#include "crdt_register.h"
#include "ctrip_crdt_hashmap.h"
#include "crdt_pubsub.h"
#include "crdt_expire.h"
#include "ctrip_crdt_common.h"
#define CRDT_API_VERSION 1

int testCreate() {
    Crdt_Test_Object* a = RedisModule_Alloc(sizeof(Crdt_Test_Object));
    RedisModule_Free(a);
    Crdt_Final_Object* b = RedisModule_Alloc(sizeof(Crdt_Final_Object));
    RedisModule_Free(b);
    CRDT_Test_RegisterTombstone* c = RedisModule_Alloc(sizeof(CRDT_Test_RegisterTombstone));
    RedisModule_Free(c);
    CRDT_Final_RegisterTombstone* d = RedisModule_Alloc(sizeof(CRDT_Final_RegisterTombstone));
    RedisModule_Free(d);
    CRDT_Test_Expire* e = RedisModule_Alloc(sizeof(CRDT_Test_Expire));
    RedisModule_Free(e);
    CRDT_Final_Expire* f = RedisModule_Alloc(sizeof(CRDT_Final_Expire));
    RedisModule_Free(f);
    CRDT_Test_ExpireTombstone* g = RedisModule_Alloc(sizeof(CRDT_Test_ExpireTombstone));
    RedisModule_Free(g);
    CRDT_Final_ExpireTombstone* h = RedisModule_Alloc(sizeof(CRDT_Final_ExpireTombstone));
    RedisModule_Free(h);
    CRDT_Test_Hash* i = RedisModule_Alloc(sizeof(CRDT_Test_Hash));
    RedisModule_Free(i);
    CRDT_Final_Hash* j = RedisModule_Alloc(sizeof(CRDT_Final_Hash));
    RedisModule_Free(j);
    CRDT_Test_HashTombstone* k = RedisModule_Alloc(sizeof(CRDT_Test_HashTombstone));
    RedisModule_Free(k);
    CRDT_Final_HashTombstone* l = RedisModule_Alloc(sizeof(CRDT_Final_HashTombstone));
    RedisModule_Free(l);
    return 1;
}
int delCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    testCreate();
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

VectorClock *getVectorClockFromString(RedisModuleString *vectorClockStr) {
    size_t vcStrLength;
    const char *vcStr = RedisModule_StringPtrLen(vectorClockStr, &vcStrLength);
    sds vclockSds = sdsnewlen(vcStr, vcStrLength);
    VectorClock *vclock = sdsToVectorClock(vclockSds);
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
void RdbSaveCrdtValue(void* db, void *rio, RedisModuleString* key) {
    RedisModuleKey* moduleKey = RedisModule_GetKey(db, key, REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    CrdtObject* data = RedisModule_ModuleTypeGetValue(moduleKey);
    if(data != NULL) {
        switch (getDataType(data->type))
        {
        case CRDT_REGISTER_TYPE:
            saveValue(rio, getCrdtRegister(), data);
            break;
        case CRDT_HASH_TYPE:
            saveValue(rio, getCrdtHash(), data);
            break;
        }   
    }
    CrdtTombstone* tombstone = RedisModule_ModuleTypeGetTombstone(moduleKey);
    if(tombstone != NULL) {
        switch (getDataType(tombstone->type))
        {
        case CRDT_REGISTER_TYPE:
            saveValue(rio, getCrdtRegisterTombstone(), tombstone);
            break;
        case CRDT_HASH_TYPE:
            saveValue(rio, getCrdtHashTombstone(), tombstone);
            break;
        }
    }
    
    CrdtExpire *expire = RedisModule_GetCrdtExpire(moduleKey);
    if(expire != NULL) {
        saveValue(rio, getCrdtExpireType(), expire);
    }
    CrdtExpireTombstone* expire_tombstone = RedisModule_GetCrdtExpireTombstone(moduleKey);
    if(expire_tombstone != NULL) {
        saveValue(rio, getCrdtExpireTombstoneType(), expire_tombstone);
    }
    RedisModule_SaveLen(rio, RDB_VALUE_EOF);
error:
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
}

int RdbLoadCrdtValue(void* db, RedisModuleString* key, void* rio) {
    void *data = NULL;
    RedisModuleKey* moduleKey = RedisModule_GetKey(db, key, REDISMODULE_WRITE| REDISMODULE_TOMBSTONE);
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
        if(type == getCrdtRegister()) {
            RedisModule_ModuleTypeSetValue(moduleKey, type, data);
        } else if(type == getCrdtHash()) {
            RedisModule_ModuleTypeSetValue(moduleKey, type, data);
        } else if(type == getCrdtRegisterTombstone()) {
            RedisModule_ModuleTombstoneSetValue(moduleKey, type, data);
        } else if(type == getCrdtHashTombstone()) {
            RedisModule_ModuleTombstoneSetValue(moduleKey, type, data);
        } else if(type == getCrdtExpireType()) {
            RedisModule_SetCrdtExpire(moduleKey, type, data);
        } else if(type == getCrdtExpireTombstoneType()) {
            RedisModule_SetCrdtExpireTombstone(moduleKey, type, data);
        }else{
            result = C_ERR;
            goto error;
        }
    }
error:
    if(moduleKey != NULL) {RedisModule_CloseKey(moduleKey);}
    return result;
}
int isTombstone(int type) {
    return type & CRDT_TOMBSTONE;
}
int isData(int type) {
    return type & CRDT_DATA;
}
int isExpire(int type) {
    return type & CRDT_EXPIRE;
}
int getDataType(int type) {
    if(type & CRDT_REGISTER_TYPE) {
        return CRDT_REGISTER_TYPE;
    } else if(type & CRDT_HASH_TYPE) {
        return CRDT_HASH_TYPE;
    }else{
        RedisModule_Debug(logLevel, "getDataType over %lld", type);
        exit(0);
        return 0;
    }
}
CrdtDataMethod* getCrdtDataMethod(CrdtObject* data) {
    if(!isData(data->type)) {
        return NULL;
    }
    switch (getDataType(data->type)) {
        case CRDT_REGISTER_TYPE:
            return &RegisterDataMethod;
        case CRDT_HASH_TYPE:
            return &HashDataMethod;
        default:
            return NULL;
    }
}
CrdtObjectMethod* getCrdtObjectMethod(CrdtObject* obj) {
    if(isTombstone(obj->type)) {
        exit(1);
        return NULL;
    }
    if(isData(obj->type)) {
        switch (getDataType(obj->type)) {
            case CRDT_REGISTER_TYPE:
                return &RegisterCommonMethod;
            case CRDT_HASH_TYPE:
                return &HashCommonMethod;
            default:
                return NULL;
        }
    } else if(isExpire(obj->type)) {
        RedisModule_Debug(logLevel, "is expire");
        return &CrdtExpireCommonMethod;
    }
    return NULL;
}

CrdtExpireMethod* getCrdtExpireMethod(CrdtObject* obj) {
    if(!isExpire(obj->type)) {
        return NULL;
    }
    return &ExpireMethod;
}
CrdtTombstoneMethod* getCrdtTombstoneMethod(CrdtTombstone* tombstone) {
    if(!isTombstone(tombstone->type)) {
        exit(1);
        return NULL;
    }
    if(isData(tombstone->type)) {
        switch (getDataType(tombstone->type)) {
            case CRDT_REGISTER_TYPE:
                return &RegisterTombstoneMethod;
            case CRDT_HASH_TYPE:
                return &HashTombstoneCommonMethod;
            default:
                return NULL;
        }
    } else if(isExpire(tombstone->type)) {
        return &ExpireTombstoneCommonMethod;
    }
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    logLevel = sdsnew(CRDT_DEFAULT_LOG_LEVEL);
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

    if (RedisModule_CreateCommand(ctx,"del",
                                  delCommand,"write",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"crdt.debug",
                                  crdtDebugCommand,"write",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
