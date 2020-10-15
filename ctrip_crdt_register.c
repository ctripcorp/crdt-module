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
//  generic function
int isCrdtRcTombstone(CrdtTombstone* tom) {
    if(tom != NULL && getType(tom) == CRDT_TOMBSTONE && (getDataType(tom) ==  CRDT_RC_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
crdt_rc_tombstone* retrieveCrdtRcTombstone(CRDT_RCTombstone* rt) {
    return (crdt_rc_tombstone*)rt;
}
crdt_rc* retrieveCrdtRc(CRDT_RC* rc) {
    return (crdt_rc*)rc;
}
int getRcElementLen(crdt_rc* rc) {
    return rc->len;
}
long long get_vcu(VectorClock vc, int gid) {
    clk unit = getVectorClockUnit(vc, gid);
    if(isNullVectorClockUnit(unit)) {
        return 0;
    }
    long long vcu = get_logic_clock(unit);
    return vcu;
}
int replicationCrdtSetCommand(RedisModuleCtx* ctx,  RedisModuleString* key,RedisModuleString* val,CrdtMeta* set_meta, VectorClock vc, long long expire) {
    sds vcSds = vectorClockToSds(vc);
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllcl", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire);
    sdsfree(vcSds);
    return 1;
}

int replicationCrdtRcCommand(RedisModuleCtx* ctx, RedisModuleString* key, RedisModuleString* val, CrdtMeta* set_meta, crdt_rc* rc ,long long expire, int eslen, sds* del_strs) {
    if(eslen == 0) {
        return replicationCrdtSetCommand(ctx, key, val, set_meta, rc->vectorClock, expire);
    }
    sds vcSds = vectorClockToSds(rc->vectorClock);
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllcla", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire,  del_strs, (size_t)eslen);
    RedisModule_Debug(logLevel, "replicationCrdtRcCommand: %s", del_strs[0]);
    sdsfree(vcSds);
    return 1;
}



//============================== read command ==============================
int getGeneric(RedisModuleCtx* ctx, RedisModuleString *key, int sendtype) {
    RedisModuleKey *modulekey = RedisModule_OpenKey(ctx, key, REDISMODULE_READ );
    CRDT_Register* reg = NULL;
    CRDT_RC* rc = NULL;
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(modulekey) ;
    if (RedisModule_KeyType(modulekey) == REDISMODULE_KEYTYPE_EMPTY) {
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
        if(type == VALUE_TYPE_INTEGER) {
            long long l = getCrdtRcIntValue(rc);
            result = RedisModule_CreateStringFromLongLong(ctx, l);
        } else if(type == VALUE_TYPE_FLOAT) {
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
    CrdtMeta set_meta = {.gid  = 0};
    CRDT_RC* current = NULL;
    if ((RedisModule_StringToLongLong(argv[2],&increment) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        return 0;
    }
    moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    CRDT_Register* reg = NULL;
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    if (RedisModule_KeyType(moduleKey) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(moduleKey);
        RedisModule_ReplyWithNull(ctx);
        return CRDT_ERROR;
    } else if (mtype == getCrdtRegister()) {
        RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
        goto error;
    } else if (mtype == CrdtRC) {
        current = RedisModule_ModuleTypeGetValue(moduleKey);
    } else {
        RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
        goto error;
    }
    
    initIncrMeta(&set_meta);
    // int gid = getMetaGid(&set_meta);
    CrdtTombstone* tom = getTombstone(moduleKey);
    if(tom != NULL && !isCrdtRcTombstone(tom)) {
        tom = NULL;
    }
    long long  result = 0;
    if (current == NULL) {
        current = createCrdtRc();
        setCrdtRcBaseIntValue(current, &set_meta ,0);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, current);
    } else {
        if(getCrdtRcType(current) != VALUE_TYPE_INTEGER) {
            RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
            goto error;
        }
    }
    if(tom) {
        moveDelCounter(current, tom);
    }
    int gid = RedisModule_CurrentGid();
    result = addOrCreateCounter(current, &set_meta, VALUE_TYPE_INTEGER, &increment);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&set_meta));
    rc_element* element = findRcElement(current, gid);
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "slllll", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), element->counter->start_clock, element->counter->end_clock,element->counter->conv.i);
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
int getValType(RedisModuleString* val, long long* int_val, long double* float_val, sds* sds_val) {
    int val_type = VALUE_TYPE_SDS;
    if(RedisModule_StringToLongLong(val, int_val) == REDISMODULE_OK) {
        val_type = VALUE_TYPE_INTEGER;
    } else if(RedisModule_StringToDouble(val, float_val) == REDISMODULE_OK) {
        val_type = VALUE_TYPE_FLOAT;
    } else {
        *sds_val = RedisModule_GetSds(val);
    }
    return val_type;
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
    long long  int_val = 0;
    long double float_val = 0;
    sds sds_val;
    int val_type = getValType(val, &int_val, &float_val, &sds_val);
    initIncrMeta(&set_meta);
    

    CrdtTombstone* tombstone = getTombstone(moduleKey);
    long long expire_time = -2;
    if(reg) {
        crdtRegisterTryUpdate(reg, &set_meta, RedisModule_GetSds(val), COMPARE_META_VECTORCLOCK_GT);
        RedisModule_DeleteTombstone(moduleKey);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtSetCommand(ctx, key, val, &set_meta, getCrdtRegisterLastVc(reg), expire_time);
    } else if(rc) {
        if (val_type == VALUE_TYPE_SDS) {
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            goto error;
        }
        sds* gs[getRcElementLen(rc)];
        int len = crdtRcSetValue(rc, &set_meta, gs, tombstone,  val_type, val_type == VALUE_TYPE_FLOAT? &float_val: &int_val);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(ctx, key, val, &set_meta,rc, expire_time, len, gs);
    } else if(tombstone && isCrdtRcTombstone(tombstone) || (!tombstone && (val_type == VALUE_TYPE_INTEGER || val_type == VALUE_TYPE_FLOAT) )) {
        rc = createCrdtRc();
        if(tombstone) {
            appendVCForMeta(&set_meta, getCrdtRegisterTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            appendVCForMeta(&set_meta, LL2VC(vc));
        }
        sds* gs[getRcElementLen(rc)];
        crdtRcSetValue(rc, &set_meta, gs, tombstone,  val_type, val_type == VALUE_TYPE_FLOAT? &float_val: &int_val);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(ctx, key, val, &set_meta, rc, expire_time, 0, NULL);
    } else if((tombstone && isRegisterTombstone(tombstone)) || (!tombstone  && val_type == VALUE_TYPE_SDS)) {
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
// CRDT.SET key  <gid> <timestamp> <start-logic-clock> <end-logic-clock> <val>
// 0         1    2     3               4                   5               6
int CRDT_CounterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {  
    if (argc < 7) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    long long gid = 0;
    if ((RedisModule_StringToLongLong(argv[2],&gid) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long timestamp = 0;
    if ((RedisModule_StringToLongLong(argv[3],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long start_clock = 0;
    if ((RedisModule_StringToLongLong(argv[4],&start_clock) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long end_clock = 0;
    if ((RedisModule_StringToLongLong(argv[5],&end_clock) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long l = 0;
    long double ld = 0;
    sds s;
    int val_type = getValType( argv[6], &l, &ld, &s);
    if(val_type == VALUE_TYPE_SDS) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        goto end;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if (moduleKey == NULL) {
        return 0;
    }   
    crdt_rc* rc = RedisModule_ModuleTypeGetValue(moduleKey);
    updateCounter(rc, gid, timestamp, start_clock , end_clock, val_type, val_type == VALUE_TYPE_FLOAT? &ld: &l);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrby", argv[1]);
    // RedisModule_MergeClock(gid, end_clock);
end:
    RedisModule_CrdtReplicateVerbatim(gid, ctx);
    if (moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

// CRDT.SET key <val> <gid> <timestamp> <vc> <expire-at-milli> <gid:start:end:value>
// 0         1    2     3      4         5        6
int CRDT_SetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    gcounter_meta* dels[argc - 7];
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
    long long int_val = 0;
    long double float_val = 0;
    sds sds_val;
    int val_type = getValType(argv[2], &int_val, &float_val, &sds_val);
    for(int i = 7; i < argc; i++) {
        gcounter_meta* d = createGcounterMeta(0);
        dels[i - 7]  = d;
        gcounterMetaFromSds(RedisModule_GetSds(argv[i]), d);
    }
    if(rc) {
        //to counter
        if(val_type != VALUE_TYPE_FLOAT && val_type != VALUE_TYPE_INTEGER) {
            RedisModule_WrongArity(ctx);
            goto end;
        }
        crdtRcTrySetValue(rc, &meta, argc - 7, dels, tombstone, val_type, val_type == VALUE_TYPE_FLOAT ? &float_val: &int_val);
    } else if(reg || (val_type  == VALUE_TYPE_SDS)) {
        reg = addOrUpdateRegister(ctx, moduleKey, tombstone, reg, &meta, argv[1], RedisModule_GetSds(argv[2]));
        if(expire_time != -2) {
            trySetExpire(moduleKey, argv[1], getMetaTimestamp(&meta),  CRDT_REGISTER_TYPE, expire_time);
        }
    } else if(val_type == VALUE_TYPE_INTEGER || val_type == VALUE_TYPE_FLOAT){
        rc = createCrdtRc();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
        crdtRcTrySetValue(rc, &meta, argc - 7, dels, tombstone, val_type, val_type == VALUE_TYPE_FLOAT ? &float_val: &int_val);
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
RedisModuleString* element_info(RedisModuleCtx* ctx,rc_element* el) {
    sds base_info = sdsempty();
    sds counter_info = sdsempty();
    if(el->base) {
        if(el->base->type == VALUE_TYPE_FLOAT) {
            base_info = sdscatprintf(base_info, "base: {gid: %d, unit: %lld, time: %lld,value: %lf}",el->gid, el->base->unit, el->base->timespace, el->base->conv.f);
        } else if(el->base->type == VALUE_TYPE_INTEGER){
            base_info = sdscatprintf(base_info, "base: {gid: %d, unit: %lld, time: %lld,value: %lld}",el->gid, el->base->unit, el->base->timespace, el->base->conv.i);
        }
    }
    if(el->counter) {
        gcounter* g = el->counter;
        if(el->counter->type == VALUE_TYPE_FLOAT) {
            counter_info = sdscatprintf(counter_info, "counter: {start_clock: %lld, end_clock: %lld, value: %lf, del_clock: %lld, del_value: %lf}", g->start_clock, g->end_clock, g->conv.f, g->del_end_clock, g->del_conv.f);
        } else {
            counter_info = sdscatprintf(counter_info, "counter: {start_clock: %lld, end_clock: %lld, value: %lld, del_clock: %lld, del_value: %lld}", g->start_clock, g->end_clock, g->conv.i, g->del_end_clock, g->del_conv.i);
        }
    }
    RedisModuleString* result =  RedisModule_CreateStringPrintf(ctx, "gid: %d, %s %s", el->gid, base_info, counter_info);
    sdsfree(base_info);
    sdsfree(counter_info);
    return result;
}

int CRDT_GetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    CRDT_Register *reg = NULL;
    CRDT_RC* rc = NULL;
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    } else if (RedisModule_ModuleTypeGetType(key) == getCrdtRegister()) {
        reg = RedisModule_ModuleTypeGetValue(key);
    } else if (RedisModule_ModuleTypeGetType(key) == CrdtRC) {
        rc = RedisModule_ModuleTypeGetValue(key);
    } else {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if(reg) {
        sds val = getCrdtRegisterLastValue(reg);
        if(!val) {
            RedisModule_Log(ctx, "warning", "empty val for key");
            RedisModule_CloseKey(key);
            return RedisModule_ReplyWithNull(ctx);
        }
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModuleString *result = RedisModule_CreateString(ctx, val, sdslen(val));
        RedisModule_ReplyWithString(ctx, result);
        RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastGid(reg));
        RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastTimestamp(reg));
        sds vclockSds = vectorClockToSds(getCrdtRegisterLastVc(reg));
        RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
        sdsfree(vclockSds);
    } else if(rc) {
        crdt_rc* r = retrieveCrdtRc(rc);
        RedisModule_ReplyWithArray(ctx, r->len + 1);
        sds vclockSds = vectorClockToSds(r->vectorClock);
        RedisModuleString *info = RedisModule_CreateStringPrintf(ctx, "type:%s, vc: %s", r->type == VALUE_TYPE_FLOAT? "float": "int", vclockSds);
        sdsfree(vclockSds);
        RedisModule_ReplyWithString(ctx, info);
        for(int i = 0; i < r->len; i++) {
            rc_element* el = r->elements[i];
            RedisModuleString *el_info = element_info(ctx, el);
            RedisModule_ReplyWithString(ctx, el_info);
        }
    }
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}
int mergeRcTombstone(CRDT_RCTombstone* tombstone, CrdtMeta* meta, int del_len, gcounter_meta** del_counter) {
    crdt_rc_tombstone* t = retrieveCrdtRcTombstone(tombstone);
    if(isVectorClockMonoIncr(getMetaVectorClock(meta), t->vectorClock) > 0) {
        return 0;
    }
    VectorClock vc = getMetaVectorClock(meta);
    
    for(int i = 0; i < t->len; i++) {
        rc_tombstone_element* el = (t->elements[i]);
        long long unit = get_vcu(vc, el->gid);
        if(unit > el->del_unit) {
            el->del_unit = unit;
        }
        for(int j = 0; j < del_len; j++) {
            if(del_counter[j] != NULL && del_counter[j]->gid == el->gid) {
                if(el->counter->del_end_clock < del_counter[j]->end_clock) {
                    el->counter->del_end_clock = del_counter[j]->end_clock;
                    if(del_counter[j]->type == VALUE_TYPE_FLOAT) {
                        el->counter->del_conv.f = del_counter[j]->conv.f;
                    } else if(del_counter[j]->type == VALUE_TYPE_INTEGER) {
                        el->counter->del_conv.i = del_counter[j]->conv.i;
                    }
                }
                freeGcounterMeta(del_counter[j]);
                del_counter[j] = NULL;
            }
        }
    }
    for(int j = 0; j < del_len; j++) {
        if(del_counter[j] != NULL) {
            rc_tombstone_element* el = createRcTombstoneElement(del_counter[j]->gid);
            appendRcTombstoneElement(t, el);
            el->del_unit = get_vcu(vc, el->gid);
            el->counter = createGcounter(del_counter[j]->type);
            el->counter->start_clock = del_counter[j]->start_clock;
            el->counter->del_end_clock = del_counter[j]->end_clock;
            if(del_counter[j]->type == VALUE_TYPE_FLOAT) {
                el->counter->del_conv.f = del_counter[j]->conv.f;
            } else if(del_counter[j]->type == VALUE_TYPE_INTEGER) {
                el->counter->del_conv.i = del_counter[j]->conv.i;
            }
            freeGcounterMeta(del_counter[j]);
            del_counter[j] = NULL;
        }
    }
    for(int j = 0, len = (int)(get_len(vc)); j < len; j++) {
        clk* c =  get_clock_unit_by_index(&vc, (char)j);
        char gid = get_gid(*c);
        if(!findRcTombstoneElement(t, gid)) {
            rc_tombstone_element* el = createRcTombstoneElement(gid);
            el->del_unit = get_logic_clock(*c);
            appendRcTombstoneElement(t, el);
        }
    }
    updateRcTombstoneLastVc(t, vc);
    return 1;
}

int purgeRc(CRDT_RC* r,CRDT_RCTombstone* tombstone) {
    crdt_rc* rc = retrieveCrdtRc(r);
    crdt_rc_tombstone* t = retrieveCrdtRcTombstone(tombstone);
    if(isVectorClockMonoIncr(getCrdtRcLastVc(rc), getCrdtRcTombstoneLastVc(t))) {
        
        return 1;
    }
    for(int i = 0; i < t->len; i++) {
        rc_element* el = findRcElement(rc, t->elements[i]->gid);
        if(el == NULL) {
            el = createRcElement(t->elements[i]->gid);
            appendRcElement(rc, el);
        }
        if(el->base && t->elements[i]->del_unit >= el->base->unit) {
            freeBase(el->base);
            el->base = NULL;
        }
        if(t->elements[i]->counter != NULL) {
            if(el->counter) {
                if(el->counter->end_clock < t->elements[i]->counter->end_clock) {
                    if(t->elements[i]->counter->type  == VALUE_TYPE_FLOAT) {
                        el->counter->conv.f = t->elements[i]->counter->conv.f;
                    } else if(t->elements[i]->counter->type == VALUE_TYPE_INTEGER) {
                        el->counter->conv.i = t->elements[i]->counter->conv.i;
                    }
                }
                if(el->counter->del_end_clock < t->elements[i]->counter->del_end_clock) {
                    if(t->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                        el->counter->del_conv.f = t->elements[i]->counter->del_conv.f;
                    } else if(t->elements[i]->counter->type == VALUE_TYPE_INTEGER) {
                        el->counter->del_conv.i = t->elements[i]->counter->del_conv.i;
                    }
                }
            } else {
                el->counter = t->elements[i]->counter;
                t->elements[i] = NULL;
            }
        }
    }
    return 0;
}
//del_rc <key> <gid> <timestamp> <vc>  <gid:start:end:val>
int CRDT_DelRcCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    gcounter_meta* dels[argc-5];
    if(argc < 5) return RedisModule_WrongArity(ctx);
    CrdtMeta del_meta = {.gid=0};
    if (readMeta(ctx, argv, 2, &del_meta) != CRDT_OK) return CRDT_ERROR;
    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_RCTombstone* tombstone = NULL;
    for(int i = 5; i < argc; i++) {
        gcounter_meta* d = createGcounterMeta(0);
        dels[i - 5]  = d;
        gcounterMetaFromSds(RedisModule_GetSds(argv[i]), d);
    }
    //value and tombstone only one
    if(t != NULL && isCrdtRcTombstone(t)) {
        tombstone = (CRDT_RCTombstone*)t;
        mergeRcTombstone(tombstone, &del_meta, argc-5, dels);
    } else {
        tombstone = createCrdtRcTombstone();
        mergeRcTombstone(tombstone, &del_meta, argc-5, dels);
        crdt_rc* current = getCurrentValue(moduleKey);
        if(current) {
            if(purgeRc(current, tombstone)) {
                RedisModule_DeleteKey(moduleKey);
                RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRCT, tombstone);
            } else {
                freeCrdtRcTombstone(tombstone);
            }
        } else {
            RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRCT, tombstone);
        }
    }
    RedisModule_MergeVectorClock(getMetaGid(&del_meta), getMetaVectorClockToLongLong(&del_meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end: 
    if(getMetaGid(&del_meta) != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&del_meta), ctx);
        freeIncrMeta(&del_meta);
    }
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
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
    if (RedisModule_CreateCommand(ctx,"CRDT.GET",
                                  CRDT_GetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_Rc",
                                  CRDT_DelRcCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

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
    if (RedisModule_CreateCommand(ctx, "crdt.counter",
                                    CRDT_CounterCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
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
        if(b && b->timespace > base_time) {
            base_time = b->timespace;
            base = b->conv.i;
        }
        if(r->elements[i]->counter) {
            counter += r->elements[i]->counter->conv.i - r->elements[i]->counter->del_conv.i;
        }
    }
    return base + counter;
}
long double getCrdtRcFloatValue(CRDT_RC* rc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    long double counter = 0;                  
    long double base = 0;
    long long base_time = 0;
    for(int i = 0; i < r->len; i++) {
        rc_base* b = r->elements[i]->base;
        if(b->timespace > base_time) {
            base_time = b->timespace;
            base = b->conv.f;
        }
        counter += r->elements[i]->counter->conv.f;
    }
    return base + counter;
}
int updateFloatCounter(CRDT_RC* rc, int gid, long long timestamp, long long start_clock, long long end_clock, long double ld) {
    rc_element* e = findRcElement(rc, gid);
    if(e == NULL) {
        e = createRcElement(gid);
        e->counter->start_clock = start_clock;
        e->counter->end_clock = end_clock;
        appendRcElement(rc, e);
    } else {
        assert(e->counter == start_clock);
    }
    e->counter->conv.f = ld;
    return 1;
}
int updateCounter(CRDT_RC* rc, int gid, long long timestamp, long long start_clock, long long end_clock, int type,  void* val) {
    rc_element* e = findRcElement(rc, gid);
    if(e == NULL) {
        e = createRcElement(gid);
        appendRcElement(rc, e);
    } 
    if(e->counter == NULL) {
        e->counter = createGcounter(type);
        e->counter->start_clock = start_clock;
    } else {
        assert(e->counter->start_clock == start_clock);
    }
    e->counter->end_clock = end_clock;
    if(type == VALUE_TYPE_FLOAT) {
        e->counter->conv.f = *(long double*)val;
    } else if(type == VALUE_TYPE_INTEGER) {
        e->counter->conv.i = *(long long*)val;
    }
    return CRDT_OK;
}

long long getVcu(CrdtMeta* meta) {
    int gid = getMetaGid(meta);
    long long vcu = get_vcu(getMetaVectorClock(meta), gid);
    return vcu;
}
long long  addOrCreateCounter(CRDT_RC* rc,  CrdtMeta* meta, int type, void* val) {
    int gid = getMetaGid(meta);
    rc_element* e = findRcElement(rc, gid);
    if(e == NULL) {
        e = createRcElement(gid);
        appendRcElement(rc, e);
    }
    long long vcu = getVcu(meta);
    if(e->counter == NULL) {
        e->counter = createGcounter(VALUE_TYPE_INTEGER);
        e->counter->start_clock = vcu;
    }
    e->counter->end_clock = vcu;
    if(type == VALUE_TYPE_FLOAT) {
        e->counter->conv.f += *(long double*)val;
    } else if(type == VALUE_TYPE_INTEGER) {
        e->counter->conv.i += *(long long*)val;
    }
    return getCrdtRcIntValue(rc);
}

void updateRcTombstoneLastVc(CRDT_RCTombstone* rt, VectorClock vc) {
    crdt_rc_tombstone* r = retrieveCrdtRcTombstone(rt);
    VectorClock tag = r->vectorClock;
    if(tag) {
        r->vectorClock = vectorClockMerge(tag, vc);
        freeVectorClock(tag);
    } else {
        r->vectorClock = dupVectorClock(vc);
    }
}

void crdtRcUpdateLastVC(CRDT_RC* rc, VectorClock vc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    VectorClock tag = r->vectorClock;
    if(tag) {
        r->vectorClock = vectorClockMerge(tag, vc);
        freeVectorClock(tag);
    } else {
        r->vectorClock = dupVectorClock(vc);
    }
}
int setCounterDel(gcounter* counter, gcounter_meta* src) {
    counter->del_end_clock = src->end_clock;
    if(src->type == VALUE_TYPE_FLOAT) {
        counter->del_conv.f = src->conv.f;
    } else if(src->type == VALUE_TYPE_INTEGER) {
        counter->del_conv.i = src->conv.i;
    } else {
        return 0;
    }
    return 1;
}
int crdtRcTrySetValue(CRDT_RC* rc, CrdtMeta* set_meta, int gslen, gcounter_meta** gs, CrdtTombstone* tombstone, int type, void* val) {
    crdt_rc* r = retrieveCrdtRc(rc);
    int gid = getMetaGid(set_meta);
    VectorClock vc = getMetaVectorClock(set_meta);
    crdtRcUpdateLastVC(rc, vc);
    int added = 0;
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->gid == gid) {
            if(r->elements[i]->base) {
                long long unit = get_vcu(vc, r->elements[i]->gid);
                if(r->elements[i]->base->unit < unit) {
                    resetElementBase(r->elements[i]->base, set_meta, type, val);
                }
            } else {
                r->elements[i]->base = createRcElementBase( set_meta, type, val);
            }
            added = 1;
        } else {
            if(r->elements[i]->base) {
                long long unit = get_vcu(vc, r->elements[i]->gid);
                if(unit > r->elements[i]->base->unit) {
                    freeBase(r->elements[i]->base);
                    r->elements[i]->base = NULL;
                }
            }
        }
        for(int i = 0; i < gslen; i++) {
            if(gs[i] != NULL && r->elements[i]->gid == gs[i]->gid) {
                
                if(!r->elements[i]->counter) {
                    r->elements[i]->counter = createGcounter(gs[i]->type);
                }
                if(r->elements[i]->counter->del_end_clock < gs[i]->end_clock) {
                    setCounterDel(r->elements[i]->counter, gs[i]);
                }
                freeGcounterMeta(gs[i]);
                gs[i] = NULL;
            }
        }
    }
    for(int i = 0; i < gslen; i++) {
        if(gs[i] != NULL) {
            rc_element* e = createRcElement(gs[i]->gid);
            e->counter = createGcounter(gs[i]->type);
            setCounterDel(e->counter, gs[i]);
            appendRcElement(rc, e);
            if(gs[i]->gid == gid) {
                e->base = createRcElementBase( set_meta, type, val);
                added = 1;
            }
            freeGcounterMeta(gs[i]);
            gs[i] = NULL;
        }
    }
    if(added == 0) {
        rc_element* e = createRcElement(gid);
        e->base = createRcElementBase( set_meta, type, val);
        appendRcElement(rc, e);
    }
    return 1;
}
int crdtRcSetValue(CRDT_RC* rc, CrdtMeta* set_meta, sds* gs, CrdtTombstone* tombstone, int type, void* val) {
    crdt_rc* r = retrieveCrdtRc(rc);
    int gid = getMetaGid(set_meta);
    int index = 0;
    int added = 0;
    VectorClock vc = getMetaVectorClock(set_meta);
    crdtRcUpdateLastVC(rc, vc);
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->gid == gid) {
            if(r->elements[i]->base) {
                resetElementBase(r->elements[i]->base, set_meta, type, val);
            } else {
                r->elements[i]->base = createRcElementBase( set_meta, type, val);
            }
            added = 1;
        } else {
            freeBase(r->elements[i]->base);
            r->elements[i]->base = NULL;
        }
        
        if(r->elements[i]->counter) {
            r->elements[i]->counter->del_end_clock = r->elements[i]->counter->end_clock;
            if(r->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                r->elements[i]->counter->del_conv.f = r->elements[i]->counter->conv.f;
            } else {
                r->elements[i]->counter->del_conv.i = r->elements[i]->counter->conv.i;
            }
            if(gs != NULL) gs[index] = gcounterDelToSds(r->elements[i]->gid, r->elements[i]->counter);
            index++;
        }
    }
    
    if(added == 0) {
        rc_element* e =  createRcElement(gid);
        e->base = createRcElementBase(set_meta, type, val);
        appendRcElement(r, e);
    }
    return index;
}

CRDT_RC* createCrdtRc() {
    crdt_rc* rc = RedisModule_Alloc(sizeof(crdt_rc));
    rc->vectorClock = newVectorClock(0);
    setDataType(rc, CRDT_RC_TYPE);
    setType(rc, CRDT_DATA);
    rc->len = 0;
    return (CRDT_RC*)rc;
}

CRDT_RCTombstone* createCrdtRcTombstone() {
    crdt_rc_tombstone* rc = RedisModule_Alloc(sizeof(crdt_rc_tombstone));
    rc->vectorClock = newVectorClock(0);
    rc->len = 0;
    rc->elements = NULL;
    return rc;
}
int appendRcTombstoneElement(crdt_rc_tombstone* rt, rc_tombstone_element* element) {
    rt->len ++;
    if(rt->len != 1) {
        rt->elements = RedisModule_Realloc(rt->elements, sizeof(rc_tombstone_element*) * rt->len);
    } else {
        rt->elements = RedisModule_Alloc(sizeof(rc_tombstone_element*) * 1);
    }
    rt->elements[rt->len-1] = element;
    return 1;
}
// rc tombstone functions
rc_tombstone_element* createRcTombstoneElement(int gid) {
    rc_tombstone_element* element = RedisModule_Alloc(sizeof(rc_tombstone_element));
    element->gid = gid;
    element->counter = NULL;
    element->del_unit = 0;
    return element;
}
rc_tombstone_element* findRcTombstoneElement(crdt_rc_tombstone* rt, int gid) {
    for(int i = 0; i < rt->len; i++) {
        if(rt->elements[i]->gid == gid) {
            return rt->elements[i];
        }
    }
    return NULL;
}
int initRcTombstoneFromRc(CRDT_RCTombstone *tombstone, CrdtMeta* meta, CRDT_RC* rc, sds* del_counters) {
    crdt_rc* r = retrieveCrdtRc(rc);
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tombstone);
    updateRcTombstoneLastVc(rt, meta);
    rt->type = r->type;
    int index = 0;
    for(int i = 0; i < r->len; i++) {
        rc_tombstone_element* t = findRcTombstoneElement(rt, r->elements[i]->gid);
        if(t == NULL) {
            t = createRcTombstoneElement(r->elements[i]->gid);
            appendRcTombstoneElement(rt, t);
        }
        if(r->elements[i]->base) {
            if(t->del_unit < r->elements[i]->base->unit) {
                t->del_unit = r->elements[i]->base->unit;
            }
        }
        if(r->elements[i]->counter) {
            if(t->counter == NULL) {
                t->counter = r->elements[i]->counter;
                if(t->counter->del_end_clock < t->counter->end_clock) {
                    t->counter->del_end_clock = t->counter->end_clock;
                    if(t->counter->type == VALUE_TYPE_FLOAT) {
                        t->counter->del_conv.f = t->counter->conv.f;
                    } else if(t->counter->type == VALUE_TYPE_INTEGER) {
                        t->counter->del_conv.i = t->counter->conv.i;
                    }
                }
            } else {
                rc_element* el = r->elements[i];
                assert(t->counter->start_clock == el->counter->start_clock);
                if(t->counter->end_clock < el->counter->end_clock) {
                    t->counter->end_clock = el->counter->end_clock;
                    if(el->counter->type == VALUE_TYPE_FLOAT) {
                        t->counter->conv.f = el->counter->conv.f;
                    } else if(el->counter->type == VALUE_TYPE_INTEGER) {
                        t->counter->conv.i = el->counter->conv.i;
                    } else {
                        assert(1 == -1);
                    }
                }
                long long last_clock;
                void* lastconit;
                if( el->counter->end_clock < el->counter->del_end_clock) {
                    if(t->counter->del_end_clock < el->counter->del_end_clock) {
                        t->counter->del_end_clock = el->counter->del_end_clock;
                        if(el->counter->type == VALUE_TYPE_FLOAT) {
                            t->counter->del_conv.f = el->counter->del_conv.f;
                        } else if(el->counter->type == VALUE_TYPE_INTEGER) {
                            t->counter->del_conv.i = el->counter->del_conv.i;
                        }
                    }
                } else {
                    if(t->counter->del_end_clock < el->counter->end_clock) {
                        t->counter->del_end_clock = el->counter->end_clock;
                        if(el->counter->type == VALUE_TYPE_FLOAT) {
                            t->counter->del_conv.f = el->counter->conv.f;
                        } else if(el->counter->type == VALUE_TYPE_INTEGER) {
                            t->counter->del_conv.i = el->counter->conv.i;
                        }
                    }
                }   
                freeGcounter(el->counter);             
            }
            r->elements[i]->counter = NULL;
            if(del_counters) del_counters[index++] = gcounterDelToSds(t->gid,t->counter);
        }
    } 
    return index;
}



sds getCrdtRcStringValue(CRDT_RC* rc) {
    return NULL;
}
int getCrdtRcType(CRDT_RC* rc) {
    crdt_rc* r = retrieveCrdtRc(rc);
    for(int i = 0; i< r->len; i++) {
        if(r->elements[i]->base) {
            if(r->elements[i]->base->type == VALUE_TYPE_FLOAT) {
                return VALUE_TYPE_FLOAT;
            }
        }
        if(r->elements[i]->counter) {
            if(r->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                return VALUE_TYPE_FLOAT;
            }
        }
    }
    return VALUE_TYPE_INTEGER;
}

int moveDelCounter(CRDT_RC* rc, CRDT_RCTombstone* tom) {
    return 0;
}
rc_base* createRcElementBase(CrdtMeta* meta, int val_type, void* v) {
    rc_base* base = RedisModule_Alloc(sizeof(rc_base));
    resetElementBase(base, meta, val_type, v);
    return base;
}
int resetElementBase(rc_base* base, CrdtMeta* meta, int val_type, void* v) {
    if (val_type == VALUE_TYPE_FLOAT) {
        base->conv.f = *(long double*)v;
    } else if (val_type == VALUE_TYPE_INTEGER) {
        base->conv.i = *(long long*)v;
    } else {
        assert( 1 == 0);
    }
    base->unit = getVcu(meta);
    base->timespace = getMetaTimestamp(meta);
    base->type = val_type;
    return 1;
}
rc_base* createRcElementFloatBase(int gid, CrdtMeta* meta,  long double v) {
    rc_base* base = RedisModule_Alloc(sizeof(rc_base));
    base->conv.f = v;
    base->unit = getVcu(meta);
    base->timespace = getMetaTimestamp(meta);
    base->type = VALUE_TYPE_FLOAT;
    return base;
}
void freeBase(rc_base* base) {
    RedisModule_Free(base);
}
rc_element* createRcElement(int gid) {
    rc_element* element = RedisModule_Alloc(sizeof(rc_element));
    element->gid = gid;
    element->counter = NULL;
    element->base = NULL;
    return element;
}
int appendRcElement(crdt_rc* rc, rc_element* element) {
    rc->len ++;
    if(rc->len != 1) {
        rc->elements = RedisModule_Realloc(rc->elements, sizeof(rc_element*) * rc->len);
    } else {
        rc->elements = RedisModule_Alloc(sizeof(rc_element*) * 1);
    }
    rc->elements[rc->len-1] = element;
    return 1;
}
rc_element* findRcElement(crdt_rc* rc, int gid) {
    for(int i = 0; i < rc->len; i++) {
        if(rc->elements[i]->gid == gid) {
            return rc->elements[i];
        }
    }
    return NULL;
}
int setCrdtRcBaseIntValue(CRDT_RC* rc, CrdtMeta* meta,  long long v) {
    crdt_rc* r = retrieveCrdtRc(rc);
    int gid = getMetaGid(meta);
    rc_element* element = findRcElement(r, gid);
    rc_base* base = createRcElementBase( meta, VALUE_TYPE_INTEGER, &v);
    if(element == NULL) {
        element = createRcElement(gid);
        appendRcElement(r, element);
    } else {
        freeBase(element->base);
        element->base = NULL;
    }
    element->base = base;
    return 0;
}
//
VectorClock  getCrdtRcLastVc(crdt_rc* rc) {
    return rc->vectorClock;
}
VectorClock getCrdtRcTombstoneLastVc(crdt_rc_tombstone* rt) {
    return rt->vectorClock;
}
//========================= CRDT Data functions =======================
int crdtRcDelete(int dbId, void *keyRobj, void *key, void *value) {
    RedisModuleKey *moduleKey = (RedisModuleKey *)key;
    CRDT_RC *current = retrieveCrdtRc(value);
    CrdtMeta del_meta = {.gid = 0};
    initIncrMeta(&del_meta);
    VectorClock lastVc = getCrdtRcLastVc(current);
    appendVCForMeta(&del_meta, lastVc);
    CRDT_RCTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isCrdtRcTombstone(tombstone)) {
        tombstone = createCrdtRcTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRCT, tombstone);
    }
    int len = get_len(lastVc);
    sds del_counters[len];
    int dlen = initRcTombstoneFromRc(tombstone, &del_meta, current, del_counters);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&del_meta));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Rc", "sllca", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds, del_counters, (size_t)dlen);
    sdsfree(vcSds);
    freeIncrMeta(&del_meta);
    return CRDT_OK;
}