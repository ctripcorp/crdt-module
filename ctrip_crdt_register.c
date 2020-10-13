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
// Created by zhuchen on 2020/9/25.
//
#include "crdt.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
crdt_rc* retrieveCrdtRc(CRDT_RC* rc) {
    return (crdt_rc*)rc;
}
int replicationCrdtRcCommand(RedisModuleString* key, RedisModuleString* val, CrdtMeta* set_meta, long long expire, rc_element* es) {
    return 1;
}
int replicationCrdtSetCommand(RedisModuleCtx* ctx,  RedisModuleString* key,RedisModuleString* val,CrdtMeta* set_meta, VectorClock vc, long long expire) {
    sds vcSds = vectorClockToSds(vc);
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllcl", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire);
    sdsfree(vcSds);
    return 1;
}
//  generic function
int isCrdtRcTombstone(CrdtTombstone* tom) {
    if(tom != NULL && getType(tom) == CRDT_TOMBSTONE && (getDataType(tom) ==  CRDT_RC_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}

//============================== read command ==============================
int getGeneric(RedisModuleCtx* ctx, RedisModuleString *key, int sendtype) {
    RedisModuleKey *modulekey = RedisModule_OpenKey(ctx, key, REDISMODULE_READ );
    if(modulekey == NULL) {RedisModule_Debug(logLevel, " %s modulekey = null", RedisModule_GetSds(key));}
    CRDT_Register* reg = NULL;
    CRDT_RC* rc = NULL;
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(modulekey) ;
    if (RedisModule_KeyType(modulekey) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_Debug(logLevel, "get val = null");
        RedisModule_CloseKey(modulekey);
        RedisModule_ReplyWithNull(ctx);
        return CRDT_ERROR;
    } else if (mtype == getCrdtRegister()) {
        reg = RedisModule_ModuleTypeGetValue(modulekey);
    } else if (mtype == CrdtRC) {
        rc = RedisModule_ModuleTypeGetValue(modulekey);
    } else if(mtype == NULL) {
        RedisModule_ReplyWithNull(ctx);
        goto error;
    } else {
        if(sendtype) {
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        } else {
            RedisModule_ReplyWithNull(ctx);
        }
        goto error;
    }
    if(reg) {
        sds val = getCrdtRegisterLastValue(reg);
        if (!val) {
            RedisModule_ReplyWithNull(ctx);
            RedisModule_Debug(logLevel, "get sval = null");
            goto error;
        }
        RedisModuleString *result = RedisModule_CreateString(ctx, val, sdslen(val));
        RedisModule_ReplyWithString(ctx, result);
        // RedisModule_FreeString(ctx, result);
        goto next;
    }
    if(rc) {
        int type = getCrdtRcType(rc);
        RedisModuleString *result;
        if(type == VALUETYPE_INT) {
            long long l = getCrdtRcIntValue(rc);
            result = RedisModule_CreateStringFromLongLong(ctx, l);
        } else if(type == VALUETYPE_FLOAT) {
            long double f = getCrdtRcFloatValue(rc);
            result = RedisModule_CreateStringPrintf(ctx, "%lf", f);
        }else{
            RedisModule_ReplyWithError(ctx, "[CRDT_RC][Get] type error");
            goto error;
        }
        RedisModule_ReplyWithString(ctx, result);
    }

next:
    RedisModule_CloseKey(modulekey);
    return CRDT_OK;
error:
    RedisModule_CloseKey(modulekey);
    return CRDT_ERROR;
}
int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    getGeneric(ctx, argv[1], 1);
    return REDISMODULE_OK;
}
//============================== write command ==============================
int incrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = NULL;
    long long increment = 0;
    CRDT_RC* current = NULL;
    if ((RedisModule_StringToLongLong(argv[2],&increment) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return 0;
    }
    moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    if (moduleKey == NULL) {
        goto error;
    }
    CRDT_Register* reg = NULL;
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    if (RedisModule_KeyType(moduleKey) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(moduleKey);
        RedisModule_ReplyWithNull(ctx);
        return CRDT_ERROR;
    } else if (mtype == getCrdtRegister()) {
        reg = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype == CrdtRC) {
        current = RedisModule_ModuleTypeGetValue(moduleKey);
    } else {
        RedisModule_ReplyWithError(ctx, "[CRDT_RC][incry] type error");
        goto error;
    }
    if (reg) {
        sds v = getCrdtRegisterLastValue(reg);
        long long l = 0;
        if (!string2ll(v, sdslen(v), &l)) {
            goto error;
        }
        
        current = createCrdtRc();
        setTypeInt(current);
        setCrdtRcBaseIntValue(current, getCrdtRegisterLastMeta(reg), RedisModule_CurrentGid(),l);
        RedisModule_DeleteKey(moduleKey);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, current);
    }
    CrdtMeta set_meta = {.gid  = 0};
    initIncrMeta(&set_meta);
    // int gid = getMetaGid(&set_meta);
    CrdtTombstone* tom = getTombstone(moduleKey);
    if(tom != NULL && !isCrdtRcTombstone(tom)) {
        tom = NULL;
    }
    long long  result = 0;
    if (current == NULL) {
        current = createCrdtRc();
        setCrdtRcBaseIntValue(current, &set_meta, RedisModule_CurrentGid() ,0);
        setTypeInt(current);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, current);
    } else {
        // if(getCrdtRcType(current) != VALUETYPE_INT) {
        //     goto error;
        // }
    }
    if(tom) {
        moveDelCounter(current, tom);
    }
    int gid = RedisModule_CurrentGid();
    result = addOrCreateIntCounter(current, &set_meta, increment);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&set_meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "sllcl", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), vcSds, result);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrby", argv[1]);
    RedisModule_ReplyWithLongLong(ctx, result);
error:
    if(set_meta.gid != 0) freeIncrMeta(&set_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}
long long setExpireByModuleKey(RedisModuleKey* moduleKey, int flags, RedisModuleString* expire,long long milliseconds, CrdtMeta* meta) {
    long long expire_time = -2;
    if(expire) {
        expire_time = getMetaTimestamp(meta) + milliseconds;
        RedisModule_SetExpire(moduleKey, milliseconds);
    }else if(!(flags & OBJ_SET_KEEPTTL)){
        RedisModule_SetExpire(moduleKey, -1);
        expire_time = -1;
    }
    return expire_time;
}
int setGenericCommand(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, int flags, RedisModuleString* key, RedisModuleString* val, RedisModuleString* expire, int unit, int sendtype) {
    int result = 0;
    CRDT_Register* reg = NULL;
    //get value
    if(moduleKey == NULL) {
        moduleKey = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    }
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    CRDT_RC* rc = NULL;
    if (mtype == getCrdtRegister()) {
        reg = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype == CrdtRC) {
        rc = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype != NULL){
        if(sendtype) RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
        goto error;
    }
    CrdtMeta set_meta = {.gid = 0};
    CRDT_Register* current = getCurrentValue(moduleKey);
    if((current != NULL && flags & OBJ_SET_NX) 
        || (current == NULL && flags & OBJ_SET_XX)) {
        if(sendtype) RedisModule_ReplyWithNull(ctx);   
        goto error;
    }
    long long milliseconds = 0;
    if (expire) {
        if (RedisModule_StringToLongLong(expire, &milliseconds) != REDISMODULE_OK) {
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx, "ERR syntax error\r\n");
            goto error;
        }
        if (milliseconds <= 0) {
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx,"invalid expire time in set\r\n");
            goto error;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }
    initIncrMeta(&set_meta);
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    long long expire_time = -2;
    if(reg) {
        crdtRegisterTryUpdate(reg, &set_meta, RedisModule_GetSds(val), COMPARE_META_VECTORCLOCK_GT);
        sds v = vectorClockToSds(getMetaVectorClock(&set_meta));
        sdsfree(v);
        RedisModule_DeleteTombstone(moduleKey);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtSetCommand(ctx, key, val, &set_meta, getCrdtRegisterLastVc(reg), expire_time);
    } else if(rc) {
        rc_element* es = crdtRcTryUpdate(rc, &set_meta, RedisModule_GetSds(key), tombstone);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(key, val, &set_meta, expire_time, es);
    } else if(tombstone && isCrdtRcTombstone(tombstone)) {
        rc = createCrdtRc();
        sds v = RedisModule_GetSds(val);
        if(isInt(v)) {
            setTypeInt(rc);
        } else if(isFloat(v)) {
            setTypeFloat(rc);
        } else {
            goto error;
        }
        if(tombstone) {
            appendVCForMeta(&set_meta, getCrdtRegisterTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            appendVCForMeta(&set_meta, LL2VC(vc));
        }
        rc_element* es = crdtRcSetValue(rc, &set_meta, v);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(key, val, &set_meta, expire_time, es);
    } else if((tombstone && isRegisterTombstone(tombstone)) || !tombstone) {
        reg = createCrdtRegister();
        if(tombstone) {
            appendVCForMeta(&set_meta, getCrdtRegisterTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            appendVCForMeta(&set_meta, LL2VC(vc));
        }
        crdtRegisterSetValue(reg, &set_meta, RedisModule_GetSds(val));
        RedisModule_ModuleTypeSetValue(moduleKey, getCrdtRegister(), reg);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtSetCommand(ctx, key, val, &set_meta, getCrdtRegisterLastVc(reg), expire_time);
    } else {
        goto error;
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
    if(expire) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "expire", key);
    }
    result = 1;
error:
    if(set_meta.gid != 0) freeIncrMeta(&set_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
}
//set k v
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    RedisModuleString* expire = NULL;
    int flags = OBJ_SET_NO_FLAGS;
    int j;
    int unit = UNIT_SECONDS;
    #if defined(SET_STATISTICS) 
        parse_start(); 
    #endif
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
    int result = setGenericCommand(ctx, NULL, flags, argv[1], argv[2], expire, unit, 1);
    if(result == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx);
    } else {
        return CRDT_ERROR;
    }
}
//setex key  expire value
int setexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 4) return RedisModule_WrongArity(ctx);
    int result = setGenericCommand(ctx, NULL, OBJ_SET_NO_FLAGS | OBJ_SET_EX, argv[1], argv[3], argv[2], UNIT_SECONDS, 1);
    if(result == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx);
    } else {
        return CRDT_ERROR;
    } 
}


// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-milli> <gid:unit:value>
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
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 3, &meta) != CRDT_OK) {
        return 0;
    }
    //if key is null will be create one key
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    // if (moduleKey == NULL) {
    //     RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
    //     status = CRDT_ERROR;
    //     goto end;
    // }
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    CRDT_RC* rc = NULL;
    CRDT_Register* reg = NULL;
    if (mtype == getCrdtRegister()) {
        reg = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype == CrdtRC) {
        rc = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype != NULL){
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    // if (tombstone != NULL && !isRegisterTombstone(tombstone)) {
    //     tombstone = NULL;
    // }
    
    if(rc) {
        //to counter
    } else {
        if(argc <= 7) {
            reg = addOrUpdateRegister(ctx, moduleKey, tombstone, reg, &meta, argv[1], RedisModule_GetSds(argv[2]));
            if(expire_time != -2) {
                trySetExpire(moduleKey, argv[1], getMetaTimestamp(&meta),  CRDT_REGISTER_TYPE, expire_time);
            }
        } else {
            //to counter
            
        }
    }
    
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
    
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if (moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}
//========================= Register moduleType functions =======================
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver) {
    return NULL;
}
void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value) {

}
void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcMemUsageFunc(const void *value) {
    return 1;
}
void freeCrdtRc(void *value) {

}
void crdtRcDigestFunc(RedisModuleDigest *md, void *value) {

}
//========================= RegisterTombstone moduleType functions =======================
void *RdbLoadCrdtRcTombstone(RedisModuleIO *rdb, int encver)  {
    return NULL;
}
void RdbSaveCrdtRcTombstone(RedisModuleIO *rdb, void *value) {

}
void AofRewriteCrdtRcTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcTombstoneMemUsageFunc(const void *value) {
    return 1;
}
void freeCrdtRcTombstone(void *obj) {

}
void crdtRcTombstoneDigestFunc(RedisModuleDigest *md, void *value) {

}

int initRcModule(RedisModuleCtx *ctx) {
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtRc, 
        .rdb_save = RdbSaveCrdtRc,
        .aof_rewrite = AofRewriteCrdtRc,
        .mem_usage = crdtRcMemUsageFunc,
        .free = freeCrdtRc,
        .digest = crdtRcDigestFunc
    };
    CrdtRC = RedisModule_CreateDataType(ctx, CRDT_RC_DATATYPE_NAME, 0, &tm);
    if (CrdtRC == NULL) return REDISMODULE_ERR;
    RedisModuleTypeMethods tombtm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtRcTombstone,
        .rdb_save = RdbSaveCrdtRcTombstone,
        .aof_rewrite = AofRewriteCrdtRcTombstone,
        .mem_usage = crdtRcTombstoneMemUsageFunc,
        .free = freeCrdtRcTombstone,
        .digest = crdtRcTombstoneDigestFunc,
    };
    CrdtRCT = RedisModule_CreateDataType(ctx, CRDT_RC_TOMBSTONE_DATATYPE_NAME, 0, &tombtm);
    if (CrdtRCT == NULL) return REDISMODULE_ERR;
    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
    if (RedisModule_CreateCommand(ctx,"SET",
                                  setCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    if (RedisModule_CreateCommand(ctx,"CRDT.SET",
                                  CRDT_SetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx,"GET",
    //                               getCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "MGET", 
    //                                 mgetCommand, "readonly fast",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx,"CRDT.GET",
    //                               CRDT_GetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx,"CRDT.DEL_REG",
    //                               CRDT_DelRegCommand,"write",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "SETEX", 
                                    setexCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "MSET", 
    //                                 msetCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "CRDT.MSET",
    //                                 CRDT_MSETCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "incrby",
                                    incrbyCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "get",
                                    getCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}



//========================= Virtual functions =======================
long long getCrdtRcIntValue(CRDT_RC* rc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    long long counter = 0;                  
    long long base = 0;
    long long base_time = 0;
    for(int i = 0; i < r->len; i++) {
        rc_base* b = r->elements[i]->base;
        if(b->timespace > base_time) {
            base_time = b->timespace;
            base = b->conv.i;
        }
        counter += r->elements[i]->counter->conv.i;
    }
    return base + counter;
}
long long  addOrCreateIntCounter(CRDT_RC* rc,  CrdtMeta* meta, long long value) {
    int gid = getMetaGid(meta);
    rc_element* e = findElement(rc, gid);
    if(e == NULL) {
        e = createElement(gid);
        appendElement(rc, e);
    }
    if(e->counter == NULL) {
        e->counter = createGcounter();
        clk unit = getVectorClockUnit(getMetaVectorClock(meta), gid);
        if(!isNullVectorClockUnit(unit)) {
            long long vcu = get_logic_clock(unit);
            e->counter->end_clock = vcu;
        }
    }
    e->counter->conv.i += value;
    return getCrdtRcIntValue(rc);
}

rc_element* crdtRcSetValue(CRDT_RC* rc, CrdtMeta* set_meta, sds v) {
    return NULL;
}

rc_element* crdtRcTryUpdate(CRDT_RC* rc, CrdtMeta* set_meta, sds key, CrdtTombstone* tombstone) {
    return NULL;
}

CRDT_RC* createCrdtRc() {
    crdt_rc* rc = RedisModule_Alloc(sizeof(crdt_rc));
    rc->vectorClock = newVectorClock(0);
    rc->type = 0;
    rc->len = 0;
    return rc;
}

long double getCrdtRcFloatValue(CRDT_RC* rc) {
    return (long double)0;
}

sds getCrdtRcStringValue(CRDT_RC* rc) {
    return NULL;
}
int getCrdtRcType(CRDT_RC* rc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    for(int i = 0; i< r->len; i++) {
        if(r->elements[i]->base->type == VALUETYPE_FLOAT) {
            return VALUETYPE_FLOAT;
        }
    }
    
    return VALUETYPE_INT;
}
int isFloat(sds v) {
    return 0;
}
int isInt(sds v) {
    return 0;
}

int moveDelCounter(CRDT_RC* rc, CRDT_RCTombstone* tom) {
    return 0;
}
rc_base* createElementIntBase(int gid, CrdtMeta* meta,  long long v) {
    rc_base* base = RedisModule_Alloc(sizeof(rc_base));
    base->conv.i = v;
    base->vc = dupVectorClock(getMetaVectorClock(meta));
    base->timespace = getMetaTimestamp(meta);
    base->type = VALUETYPE_INT;
    return base;
}
rc_base* createElementFloatBase(int gid, CrdtMeta* meta,  long double v) {
    rc_base* base = RedisModule_Alloc(sizeof(rc_base));
    base->conv.f = v;
    base->vc = dupVectorClock(getMetaVectorClock(meta));
    base->timespace = getMetaTimestamp(meta);
    base->type = VALUETYPE_FLOAT;
    return base;
}
void freeBase(rc_base* base) {
    RedisModule_Free(base);
}
rc_element* createElement(int gid) {
    rc_element* element = RedisModule_Alloc(sizeof(rc_element));
    element->gid = gid;
    element->counter = NULL;
    return element;
}
int appendElement(crdt_rc* rc, rc_element* element) {
    rc->len ++;
    if(rc->len != 1) {
        rc->elements = RedisModule_Realloc(rc->elements, sizeof(rc_element*) * rc->len);
    } else {
        rc->elements = RedisModule_Alloc(sizeof(rc_element*) * 1);
    }
    rc->elements[rc->len-1] = element;
}
rc_element* findElement(crdt_rc* rc, int gid) {
    for(int i = 0; i < rc->len; i++) {
        if(rc->elements[i]->gid == gid) {
            return rc->elements[i];
        }
    }
    return NULL;
}
int setCrdtRcBaseIntValue(CRDT_RC* rc, CrdtMeta* meta, int gid, long long v) {
    crdt_rc* r = retrieveCrdtRc(rc);
    rc_element* element = findElement(r, gid);
    rc_base* base = createElementIntBase(gid, meta, v);
    if(element == NULL) {
        element = createElement(gid);
        appendElement(r, element);
    } else {
        freeBase(element->base);
        element->base = NULL;
    }
    element->base = base;
    return 0;
}
int setTypeInt(CRDT_RC* rc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    
    return 0;
}
int setTypeFloat(CRDT_RC* rc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    // rc->reserved |= VALUETYPE_FLOAT;
    return 0;
}