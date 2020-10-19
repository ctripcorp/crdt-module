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
const char* crdt_set_head = "*7\r\n$8\r\nCRDT.SET\r\n";
const char* crdt_set_no_expire_head = "*6\r\n$8\r\nCRDT.SET\r\n";
//CRDT.SET key value gid time vc expire
const size_t crdt_set_basic_str_len = 18 + 2 *REPLICATION_MAX_STR_LEN + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + REPLICATION_MAX_LONGLONG_LEN;
size_t replicationFeedCrdtSetCommand(RedisModuleCtx *ctx,char* cmdbuf, const char* keystr, size_t keylen,const char* valstr, size_t vallen, CrdtMeta* meta, VectorClock vc, long long expire_time) {
    size_t cmdlen = 0;
    if(expire_time > -2) {
        cmdlen +=  feedBuf(cmdbuf + cmdlen, crdt_set_head);
    }else{
        cmdlen += feedBuf(cmdbuf + cmdlen, crdt_set_no_expire_head);
    }
    cmdlen += feedKV2Buf(cmdbuf + cmdlen, keystr, keylen, valstr, vallen);
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), vc);
    if(expire_time > -2) {
        cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expire_time);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int replicationCrdtSetCommand(RedisModuleCtx* ctx,  RedisModuleString* key,RedisModuleString* val,CrdtMeta* set_meta, VectorClock vc, long long expire_time) {
    // sds vcSds = vectorClockToSds(vc);
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllcl", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire);
    // sdsfree(vcSds);
    size_t keylen = 0;
    const char* keystr = RedisModule_StringPtrLen(key, &keylen);
    size_t vallen = 0;
    const char* valstr = RedisModule_StringPtrLen(val, &vallen);
    size_t alllen = keylen + vallen + crdt_set_basic_str_len;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtSetCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,set_meta, vc, expire_time);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtSetCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,set_meta, vc, expire_time);
    }
    return 1;
}

int replicationCrdtRcCommand(RedisModuleCtx* ctx, RedisModuleString* key, RedisModuleString* val, CrdtMeta* set_meta, crdt_rc* rc ,long long expire, int eslen, sds* del_strs) {
    sds vcSds = vectorClockToSds(rc->vectorClock);
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Rc", "ssllcla", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire,  del_strs, (size_t)eslen);
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
            result = RedisModule_CreateStringPrintf(ctx, "%Lf", f);
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
int incrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    long double increment = 0;
    if ((RedisModule_StringToLongDouble(argv[2],&increment) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an float or out of range");
        return 0;
    }
    return incrbyGenericCommand(ctx, argv, argc, VALUE_TYPE_FLOAT, &increment);
}
int incrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    long long increment = 0;
    if ((RedisModule_StringToLongLong(argv[2],&increment) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        return 0;
    }
    return incrbyGenericCommand(ctx, argv, argc, VALUE_TYPE_INTEGER, &increment);
}
int decrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    long long increment = 0;
    if ((RedisModule_StringToLongLong(argv[2],&increment) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        return 0;
    }
    increment = -increment;
    return incrbyGenericCommand(ctx, argv, argc, VALUE_TYPE_INTEGER, &increment);
}
int incrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    long long increment = 1;
    return incrbyGenericCommand(ctx, argv, argc, VALUE_TYPE_INTEGER, &increment);
}
int decrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    long long increment = -1;
    return incrbyGenericCommand(ctx, argv, argc, VALUE_TYPE_INTEGER, &increment);
}

int incrbyGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int type, void* increment) {
    RedisModuleKey* moduleKey = NULL;
    CrdtMeta set_meta = {.gid  = 0};
    CRDT_RC* current = NULL;
    moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    CRDT_Register* reg = NULL;
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    if (mtype == getCrdtRegister()) {
        RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
        goto error;
    } else if (mtype == CrdtRC) {
        current = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if(mtype != NULL) {
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
        initCrdtRcFromTombstone(current, tom);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, current);
    } else {
        if(type == VALUE_TYPE_INTEGER && getCrdtRcType(current) != VALUE_TYPE_INTEGER) {
            RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
            goto error;
        }
    }
    int gid = RedisModule_CurrentGid();
    addOrCreateCounter(current, &set_meta, type, increment);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&set_meta));
    rc_element* element = findRcElement(current, gid);
    
    sds vc_info = vectorClockToSds(getMetaVectorClock(&set_meta));
    if (type == VALUE_TYPE_FLOAT) {
        RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "sllcllf", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), vc_info, element->counter->start_clock, element->counter->end_clock,element->counter->conv.f);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrbyfloat", argv[1]);
        RedisModule_ReplyWithLongDouble(ctx, getCrdtRcFloatValue(current));
    } else if(type == VALUE_TYPE_INTEGER) {
        RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "sllclll", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), vc_info, element->counter->start_clock, element->counter->end_clock,element->counter->conv.i);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrby", argv[1]);
        RedisModule_ReplyWithLongLong(ctx, getCrdtRcIntValue(current));
    }
    sdsfree(vc_info);
    
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
    } else if(RedisModule_StringToLongDouble(val, float_val) == REDISMODULE_OK) {
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
    CrdtMeta set_meta = {.gid = 0};
    if (mtype == getCrdtRegister()) {
        reg = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype == CrdtRC) {
        rc = RedisModule_ModuleTypeGetValue(moduleKey);
    } else if (mtype != NULL){
        if(sendtype) RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
        goto error;
    }  
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
    if(tombstone != NULL && !isRegisterTombstone(tombstone) && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
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
            initCrdtRcFromTombstone(rc, tombstone);
        } 
        sds* gs[getRcElementLen(rc)];
        crdtRcSetValue(rc, &set_meta, gs, tombstone,  val_type, val_type == VALUE_TYPE_FLOAT? &float_val: &int_val);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(ctx, key, val, &set_meta, rc, expire_time, 0, NULL);
        if(tombstone) { RedisModule_DeleteTombstone(moduleKey);}
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
        RedisModule_ReplyWithError(ctx, "code error");
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
// CRDT.counter key  <gid> <timestamp> <vc> <start-logic-clock> <end-logic-clock> <val>
// 0            1    2     3              4                  5   6                  7
int CRDT_CounterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {  
    if (argc < 8) return RedisModule_WrongArity(ctx);
    int status = CRDT_ERROR;
    CrdtMeta meta = {.gid = 0};
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        goto end;
    }
    long long start_clock = 0;
    if ((RedisModule_StringToLongLong(argv[5],&start_clock) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"1ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long end_clock = 0;
    if ((RedisModule_StringToLongLong(argv[6],&end_clock) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"2ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long l = 0;
    long double ld = 0;
    sds s;
    int val_type = getValType( argv[7], &l, &ld, &s);
    if(val_type == VALUE_TYPE_SDS) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        goto end;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if (moduleKey == NULL) {
        return 0;
    }   
    CRDT_RCTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
    crdt_rc* rc = RedisModule_ModuleTypeGetValue(moduleKey);
    int need_add = 0;
    if(rc == NULL) {
        need_add = 1;
        rc = createCrdtRc();
    }
    int result = tryUpdateCounter(rc, tombstone, meta.gid, meta.timestamp, start_clock , end_clock, val_type, val_type == VALUE_TYPE_FLOAT? &ld: &l);
    if(result == PURGE_TOMBSTONE) {
        if(tombstone != NULL) {
            RedisModule_DeleteTombstone(moduleKey);
        }
        if(need_add) RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
    } else {
        if(need_add) {
            freeCrdtRc(rc);
        } else {
            RedisModule_DeleteKey(moduleKey);
        }
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrby", argv[1]);
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    status = CRDT_OK;
end:
    RedisModule_CrdtReplicateVerbatim(meta.gid, ctx);
    if(meta.gid != 0) {
        freeIncrMeta(&meta);
    }
    if (moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

// CRDT.rc key <val> <gid> <timestamp> <vc> <expire-at-milli> <gid:start:end:value>
// 0        1    2     3      4         5        6              7
int CRDT_RCCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    CRDT_RC* rc = RedisModule_ModuleTypeGetValue(moduleKey);
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
    long long int_val = 0;
    long double float_val = 0;
    sds sds_val;
    int val_type = getValType(argv[2], &int_val, &float_val, &sds_val);
    if(val_type != VALUE_TYPE_FLOAT && val_type != VALUE_TYPE_INTEGER) {
        RedisModule_WrongArity(ctx);
        goto end;
    }
    for(int i = 7; i < argc; i++) {
        gcounter_meta* d = createGcounterMeta(0);
        dels[i - 7]  = d;
        gcounterMetaFromSds(RedisModule_GetSds(argv[i]), d);
    }
    int need_add = 0;
    if(!rc) {
        rc = createCrdtRc();
        need_add = 1;
    }
    int result = crdtRcTrySetValue(rc, &meta, argc - 7, dels, tombstone, val_type, val_type == VALUE_TYPE_FLOAT ? &float_val: &int_val);
    if(result == PURGE_TOMBSTONE) {
        if(tombstone != NULL) {
            RedisModule_DeleteTombstone(moduleKey);
        }
        if(need_add) RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
    } else {
        if(need_add) {
            freeCrdtRc(rc);
        } 
    }
    if(expire_time != -2) {
        trySetExpire(moduleKey, argv[1], getMetaTimestamp(&meta),  CRDT_RC_TYPE, expire_time);
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
            base_info = sdscatprintf(base_info, "base: {gid: %d, unit: %lld, time: %lld,value: %Lf}",el->gid, el->base->unit, el->base->timespace, el->base->conv.f);
        } else if(el->base->type == VALUE_TYPE_INTEGER){
            base_info = sdscatprintf(base_info, "base: {gid: %d, unit: %lld, time: %lld,value: %lld}",el->gid, el->base->unit, el->base->timespace, el->base->conv.i);
        }
    }
    if(el->counter) {
        gcounter* g = el->counter;
        if(el->counter->type == VALUE_TYPE_FLOAT) {
            counter_info = sdscatprintf(counter_info, "counter: {start_clock: %lld, end_clock: %lld, value: %Lf, del_clock: %lld, del_value: %Lf}", g->start_clock, g->end_clock, g->conv.f, g->del_end_clock, g->del_conv.f);
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
    VectorClock vc = getMetaVectorClock(meta);
    for(int i = 0; i < t->len; i++) {
        rc_tombstone_element* el = (t->elements[i]);
        long long unit = get_vcu(vc, el->gid);
        if(unit > el->del_unit) {
            el->del_unit = unit;
        }
        for(int j = 0; j < del_len; j++) {
            if(del_counter[j] != NULL && del_counter[j]->gid == el->gid) {
                update_del_counter_by_meta(el->counter, del_counter[j]);
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
            update_del_counter_by_meta(el->counter, del_counter[j]);
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

int crdtRcTombstonePurge(CRDT_RCTombstone* tombstone, CRDT_RC* r) {
    crdt_rc* rc = retrieveCrdtRc(r);
    crdt_rc_tombstone* t = retrieveCrdtRcTombstone(tombstone);
    if(isVectorClockMonoIncr(getCrdtRcLastVc(rc), getCrdtRcTombstoneLastVc(t))) {
        // rc.counter.conv -> tombstone.counter.conv 
        for(int i = 0; i < rc->len; i++) {
            rc_tombstone_element* el = findRcTombstoneElement(t, rc->elements[i]->gid);
            assert(el != NULL);
            if(rc->elements[i]->counter) {
                update_add_counter(el->counter, rc->elements[i]->counter);
            }
        }
        return PURGE_VAL;
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
                update_add_counter(el->counter, t->elements[i]->counter);
                update_del_counter(el->counter, t->elements[i]->counter);
                freeGcounter(t->elements[i]->counter);
            } else {
                el->counter = t->elements[i]->counter;
            }
            t->elements[i] = NULL;
        }
    }
    crdtRcUpdateLastVC(rc, t->vectorClock);
    return PURGE_TOMBSTONE;
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
            if(crdtRcTombstonePurge(tombstone, current) == PURGE_VAL) {
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
//========================= Rc moduleType functions =======================

#define ADD 1
#define DEL (1<<1)
gcounter* load_counter(RedisModuleIO* rdb) {
    gcounter* counter = createGcounter(0);
    counter->start_clock = RedisModule_LoadSigned(rdb);
    counter->type = RedisModule_LoadSigned(rdb);
    int flags = RedisModule_LoadSigned(rdb);
    if(flags & ADD) {
        counter->end_clock = RedisModule_LoadSigned(rdb);
        if(counter->type == VALUE_TYPE_FLOAT) {
            counter->conv.f = RedisModule_LoadFloat(rdb);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            counter->conv.i = RedisModule_LoadSigned(rdb);
        }  
    }
    if(flags & DEL) {
        counter->del_end_clock = RedisModule_LoadSigned(rdb);
        if(counter->type == VALUE_TYPE_FLOAT) {
            counter->del_conv.f = RedisModule_LoadFloat(rdb);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            counter->del_conv.i = RedisModule_LoadSigned(rdb);
        }  
    }
    return counter;
}

void save_counter(RedisModuleIO* rdb, gcounter* counter) {
    RedisModule_SaveSigned(rdb, counter->start_clock);
    RedisModule_SaveSigned(rdb, counter->type);
    int flags = 0;
    if(counter->end_clock != 0) {flags |= ADD;}
    if(counter->del_end_clock != 0) {flags |= DEL;}
    RedisModule_SaveSigned(rdb, flags);
    if(counter->end_clock != 0) {
        RedisModule_SaveSigned(rdb, counter->end_clock);
        if(counter->type == VALUE_TYPE_FLOAT) {
            RedisModule_SaveFloat(rdb, counter->conv.f);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            RedisModule_SaveSigned(rdb, counter->conv.i);
        }    
    }
    if(counter->del_end_clock != 0) {
        RedisModule_SaveSigned(rdb, counter->del_end_clock);
        if(counter->type == VALUE_TYPE_FLOAT) {
            RedisModule_SaveFloat(rdb, counter->del_conv.f);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            RedisModule_SaveSigned(rdb, counter->del_conv.i);
        }    
    }
    
}

rc_base* load_base(RedisModuleIO* rdb) {
    rc_base* base = createRcElementBase();
    base->unit = RedisModule_LoadSigned(rdb);
    base->timespace = RedisModule_LoadSigned(rdb);
    base->type = RedisModule_LoadSigned(rdb);
    if(base->type == VALUE_TYPE_FLOAT) {
        base->conv.f = RedisModule_LoadFloat(rdb);
    } else if(base->type == VALUE_TYPE_INTEGER) {
        base->conv.i = RedisModule_LoadSigned(rdb);
    }
    return base;
}

void save_base(RedisModuleIO* rdb, rc_base* base) {
    RedisModule_SaveSigned(rdb, base->unit);
    RedisModule_SaveSigned(rdb, base->timespace);
    RedisModule_SaveSigned(rdb, base->type);
    if(base->type == VALUE_TYPE_FLOAT) {
        RedisModule_SaveFloat(rdb, base->conv.f);
    } else if(base->type == VALUE_TYPE_INTEGER) {
        RedisModule_SaveSigned(rdb, base->conv.i);
    }
}

#define BASE_DATA 1
#define COUNTER_DATA (1<<1)

rc_element* load_rc_element(RedisModuleIO* rdb) {
    rc_element* rc = createRcElement(0);
    rc->gid = RedisModule_LoadSigned(rdb);
    int flags = RedisModule_LoadSigned(rdb);
    if(flags & BASE_DATA) {
        rc->base = load_base(rdb);
    }
    if(flags & COUNTER_DATA) {
        rc->counter = load_counter(rdb);
    }
    return rc;
}

void save_rc_element(RedisModuleIO* rdb, rc_element* el) {
    RedisModule_SaveSigned(rdb, el->gid);
    int flags = 0;
    if(el->base) { flags |= BASE_DATA;}
    if(el->counter) { flags |= COUNTER_DATA;}
    RedisModule_SaveSigned(rdb, flags);
    if(el->base) {save_base(rdb, el->base);}
    if(el->counter) {save_counter(rdb, el->counter);}
}

crdt_rc* RdbLoadCrdtOrSetRc(RedisModuleIO *rdb, long long version, int encver) {
    crdt_rc* rc = createCrdtRc();
    rc->vectorClock = rdbLoadVectorClock(rdb, version);
    int len = RedisModule_LoadUnsigned(rdb);
    for(int i = 0; i < len; i++) {
        rc_element* el = load_rc_element(rdb);
        if(el == NULL) { freeCrdtRc(rc); return NULL;}
        appendRcElement(rc, el);
    }
    return rc;
}
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == ORSET_TYPE) {
        return RdbLoadCrdtOrSetRc(rdb, version, encver);
    }
    return NULL;
}


void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value) {
    crdt_rc* rc = retrieveCrdtRc(value);
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    rdbSaveVectorClock(rdb, rc->vectorClock, CRDT_RDB_VERSION);
    RedisModule_SaveUnsigned(rdb, rc->len);
    for(int i = 0; i < rc->len; i++) {
        save_rc_element(rdb, rc->elements[i]);
    }
}

void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcMemUsageFunc(const void *value) {
    return 1;
}

void freeCrdtRc(void *value) {
    crdt_rc* rc = retrieveCrdtRc(value);
    freeVectorClock(rc->vectorClock);
    for(int i = 0; i < rc->len; i++) {
        freeRcElement(rc->elements[i]);
        rc->elements[i] = NULL;
    }
    RedisModule_Free(rc->elements);
}

void crdtRcDigestFunc(RedisModuleDigest *md, void *value) {

}

//========================= RcTombstone moduleType functions =======================
rc_tombstone_element* load_rc_tombstone_element(RedisModuleIO *rdb) {
    rc_tombstone_element* el = createRcTombstoneElement(0);
    el->gid = RedisModule_LoadUnsigned(rdb);
    el->del_unit = RedisModule_LoadUnsigned(rdb);
    int hasCounter = RedisModule_LoadUnsigned(rdb);
    if(hasCounter) {
        el->counter = load_counter(rdb);
    }
    return el;
}
void save_rc_tombstone_element(RedisModuleIO *rdb, rc_tombstone_element* el) {
    RedisModule_SaveUnsigned(rdb, el->gid);
    RedisModule_SaveUnsigned(rdb, el->del_unit);
    if(!el->counter) { 
        RedisModule_SaveUnsigned(rdb, 0);
        return;
    } 
    RedisModule_SaveUnsigned(rdb, 1);
    save_counter(rdb, el->counter);
}

crdt_rc_tombstone* RdbLoadCrdtOrSetRcTombstone(RedisModuleIO *rdb, int version, int encver) {
    crdt_rc_tombstone* rt = createCrdtRcTombstone();
    rt->vectorClock = rdbLoadVectorClock(rdb, version);
    int len = RedisModule_LoadUnsigned(rdb);
    for(int i = 0; i < len; i++) {
        rc_tombstone_element* el = load_rc_tombstone_element(rdb);
        if(el == NULL) { freeCrdtRcTombstone(rt); return NULL;}
        appendRcTombstoneElement(rt, el);
    }
    return rt;
}

void *RdbLoadCrdtRcTombstone(RedisModuleIO *rdb, int encver)  {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == ORSET_TYPE) {
        return RdbLoadCrdtOrSetRcTombstone(rdb, version, encver);
    }
    return NULL;
}

void RdbSaveCrdtRcTombstone(RedisModuleIO *rdb, void *value) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(value);
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    rdbSaveVectorClock(rdb, rt->vectorClock, CRDT_RDB_VERSION);
    RedisModule_SaveUnsigned(rdb, rt->len);
    for(int i = 0; i < rt->len; i++) {
        save_rc_tombstone_element(rdb, rt->elements[i]);
    }
}
void AofRewriteCrdtRcTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcTombstoneMemUsageFunc(const void *value) {
    return 1;
}
void freeCrdtRcTombstone(void *obj) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(obj);
    freeVectorClock(rt->vectorClock);
    for(int i = 0; i < rt->len; i++) {
        freeRcTombstoneElement(rt->elements[i]);
        rt->elements[i] = NULL;
    }
    RedisModule_Free(rt->elements);
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
    
    // if (RedisModule_CreateCommand(ctx,"CRDT.SET",
    //                               CRDT_SetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.RC",
                                  CRDT_RCCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
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
    if (RedisModule_CreateCommand(ctx, "incrbyfloat",
                                    incrbyfloatCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "incr",
                                    incrCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "decr",
                                    decrCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "decrby",
                                    decrbyCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
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
        if(b && b->timespace > base_time) {
            base_time = b->timespace;
            if(b->type == VALUE_TYPE_FLOAT) {
                base = b->conv.f;
            } else if(b->type == VALUE_TYPE_INTEGER) {
                base = (long double)b->conv.i;
            }
            
        }
        if(r->elements[i]->counter) {
            if(r->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                counter += r->elements[i]->counter->conv.f - r->elements[i]->counter->del_conv.f;
            } else if(r->elements[i]->counter->type == VALUE_TYPE_INTEGER) {
                long long v = (r->elements[i]->counter->conv.i - r->elements[i]->counter->del_conv.i);
                counter += (long double)v;
            } else {
                assert(1 == 0);
            }
            
        }
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
int tryUpdateCounter(CRDT_RC* rc, CRDT_RCTombstone* tom, int gid, long long timestamp, long long start_clock, long long end_clock, int type,  void* val) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tom);
    if(rt != NULL) {
        long long tvcu = get_vcu(rt->vectorClock, gid);
        if(tvcu > end_clock) {
            rc_tombstone_element* el =  findRcTombstoneElement(rt, gid);
            assert(el != NULL && el->counter != NULL);
            assert(el->counter->type == type);
            if(el->counter->end_clock < end_clock) {
                el->counter->end_clock = end_clock;
                if(type == VALUE_TYPE_FLOAT) {
                    el->counter->conv.f = *(long double*)val;
                } else if(type == VALUE_TYPE_INTEGER) {
                    el->counter->conv.i = *(long long*)val;
                }
            }
            return PURGE_VAL;
        } else {
            initCrdtRcFromTombstone(rc, rt);
        }
    }
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
    e->counter->type = type;
    VectorClock vc = newVectorClockFromGidAndClock(gid, end_clock);
    crdtRcUpdateLastVC(rc, vc);
    freeVectorClock(vc);
    return PURGE_TOMBSTONE;
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
        e->counter = createGcounter(type);
        setCounterType(e->counter, type);
        e->counter->start_clock = vcu;
    }
    e->counter->end_clock = vcu;
    if(e->counter->type != type) {
         setCounterType(e->counter, type);
        // float can't to int
        if(type == VALUE_TYPE_FLOAT) {
            long double v = (*(long double*)val);
            long double f = (long double)(e->counter->conv.i);
            e->counter->conv.f = v + f;
        } else {
            assert(1 == 0);
        }
    } else {
        if(type == VALUE_TYPE_FLOAT) {
            e->counter->conv.f += (*(long double*)val);
        } else if(type == VALUE_TYPE_INTEGER) {
            e->counter->conv.i += (*(long long*)val);
        } else {
            assert(1 == 0);
        }
    }
    
    crdtRcUpdateLastVC(rc, getMetaVectorClock(meta));
    return 1;
}

//========================= CRDT Tombstone functions =======================
int crdtRcTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(target);
    return isVectorClockMonoIncr(rt->vectorClock, clock);
    // return 0;
}

CrdtTombstone* crdRcTombstoneMerge(CrdtTombstone* currentVal, CrdtTombstone* value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    if(currentVal == NULL) {
        return dupCrdtRcTombstone(value);
    }
    if(value == NULL) {
        return dupCrdtRcTombstone(currentVal);
    }
    crdt_rc_tombstone *current = retrieveCrdtRcTombstone(currentVal);
    crdt_rc_tombstone *other = retrieveCrdtRcTombstone(value);
    crdt_rc_tombstone* result = dupCrdtRcTombstone(current);
    freeVectorClock(result->vectorClock);
    result->vectorClock = vectorClockMerge(current->vectorClock, other->vectorClock);
    for(int i = 0; i < other->len; i++) {
        rc_tombstone_element * el = findRcTombstoneElement(current, other->elements[i]->gid);
        if(el == NULL) {
            el = dupCrdtRcTombstoneElement(other->elements[i]);
            appendRcTombstoneElement(result, el);
        } else {
            assign_max_rc_tombstone_element(el, other->elements[i]);
        }
    }
    return result;
}

CrdtObject** crdtRcTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) {
    crdt_rc_tombstone* rt = retrieveCrdtRc(target);
    //value + gid + time + vectorClock
    if (crdtRcTombstoneMemUsageFunc(rt) > maxsize) {
        *length  = -1;
        return NULL;
    }
    VectorClockUnit unit = getVectorClockUnit(rt->vectorClock, gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    long long vcu = get_logic_clock(unit);
    if(vcu > logic_time) {
        *length = 1;
        crdt_rc** re = RedisModule_Alloc(sizeof(crdt_rc_tombstone*));
        re[0] = rt;
        return re;
    }  
    return NULL;
}

void freeCrdtRcTombstoneFilter(CrdtTombstone** filters, int num) {
    RedisModule_ZFree(filters);
}
int comp_rc_tombstone_element(void* a, void* b) {
    return (*(rc_tombstone_element**)a)->gid > (*(rc_tombstone_element**)b)->gid? 1: 0;
}
sds crdtRcTombstoneInfo(CRDT_RCTombstone* t) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(t);
    sds result = sdsempty();
    sds vc_info = vectorClockToSds(rt->vectorClock);
    result = sdscatprintf(result, "type: crdt_rc_tombstone, vc: %s\r\n", vc_info);
    sdsfree(vc_info);
    for(int i = 0; i < rt->len; i++) {
        rc_tombstone_element* el = rt->elements[i];
        result = sdscatprintf(result, "   %d) gid: %d, unit: %lld\r\n", i, el->gid, el->del_unit);
        if(el->counter) {
            if(el->counter->type == VALUE_TYPE_FLOAT) {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %Lf}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.f);
                result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %Lf}\r\n",el->counter->del_end_clock, el->counter->del_conv.f);
            } else {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %lld}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.i);
                result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %lld}\r\n",el->counter->del_end_clock, el->counter->del_conv.i);
            }
        }
    }
    return result;
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

void initCrdtRcFromTombstone(CRDT_RC* r, CRDT_RCTombstone* t) {
    if(t == NULL) {
        return;
    }
    crdt_rc* rc = retrieveCrdtRc(r);
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(t);
    rc->vectorClock = dupVectorClock(rt->vectorClock);
    for(int i = 0; i < rt->len; i++) {
        rc_tombstone_element* tel =  rt->elements[i];
        rc_element* rel = findRcElement(rc, tel->gid);
        if(rel == NULL) {
            rel = createRcElement(tel->gid);
            rel->counter = tel->counter;
            tel = NULL;
            appendRcElement(rc, rel);
        } else {
            update_add_counter(rel->counter, tel->counter);
            update_del_counter(rel->counter, tel->counter);
        }
    }
}

int comp_rc_element(void* a, void* b) {
    return (*(rc_element**)a)->gid > (*(rc_element**)b)->gid? 1: 0;
}

sds crdtRcInfo(CRDT_RC* value) {
    crdt_rc* rc = retrieveCrdtRc(value);
    sds result = sdsempty();
    sds vc_info = vectorClockToSds(rc->vectorClock);
    result = sdscatprintf(result, "type: crdt_rc, vc: %s\r\n", vc_info);
    sdsfree(vc_info);
    for(int i = 0; i < rc->len; i++) {
        rc_element* el = rc->elements[i];
        result = sdscatprintf(result, "  %d) gid: %lld\r\n", i, el->gid);
        if(el->base) {
            if(el->base->type) {
                result = sdscatprintf(result, "     base: { clock: %lld, timespace: %lld, value: %Lf} \r\n", el->base->unit, el->base->timespace, el->base->conv.f);
            } else {
                result = sdscatprintf(result, "     base: { clock: %lld, timespace: %lld, value: %lld} \r\n", el->base->unit, el->base->timespace, el->base->conv.i);
            }
        }
        if(el->counter) {
            if(el->counter->type == VALUE_TYPE_FLOAT) {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %Lf}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.f);
                if(el->counter->del_end_clock != 0) result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %Lf}\r\n",el->counter->del_end_clock, el->counter->del_conv.f);
            } else {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %lld}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.i);
                if(el->counter->del_end_clock != 0) result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %lld}\r\n",el->counter->del_end_clock, el->counter->del_conv.i);
            }
        }
    }
    return result;
}

int crdtRcTrySetValue(CRDT_RC* rc, CrdtMeta* set_meta, int gslen, gcounter_meta** gs, CrdtTombstone* tombstone, int type, void* val) {
    crdt_rc* r = retrieveCrdtRc(rc);
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tombstone);
    if(rt != NULL) {
        if(isVectorClockMonoIncr(getMetaVectorClock(set_meta) , rt->vectorClock)) {
            return PURGE_VAL;
        }
    }
    int gid = getMetaGid(set_meta);
    VectorClock vc = getMetaVectorClock(set_meta);
    crdtRcUpdateLastVC(rc, vc);
    int added = 0;
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->gid == gid) {
            if(r->elements[i]->base) {
                long long unit = get_vcu(vc, r->elements[i]->gid);
                if(r->elements[i]->base->unit <= unit) {
                    resetElementBase(r->elements[i]->base, set_meta, type, val);
                }
            } else {
                r->elements[i]->base = createRcElementBase();
                resetElementBase(r->elements[i]->base, set_meta, type, val);
            }
            added = 1;
        } else {
            if(r->elements[i]->base) {
                long long unit = get_vcu(vc, r->elements[i]->gid);
                if(unit >= r->elements[i]->base->unit) {
                    freeBase(r->elements[i]->base);
                    r->elements[i]->base = NULL;
                }
            }
        }
        for(int j = 0; j < gslen; j++) {
            if(gs[j] != NULL && r->elements[i]->gid == gs[j]->gid) {
                if(!r->elements[i]->counter) {
                    r->elements[i]->counter = createGcounter(gs[j]->type);
                }
                assert(r->elements[i]->counter->type == gs[j]->type);
                if(r->elements[i]->counter->del_end_clock < gs[j]->end_clock) {
                    update_del_counter_by_meta(r->elements[i]->counter, gs[j]);
                }
                freeGcounterMeta(gs[j]);
                gs[j] = NULL;
            }
        }
    }
    for(int i = 0; i < gslen; i++) {
        if(gs[i] != NULL) {
            rc_element* e = createRcElement(gs[i]->gid);
            e->counter = createGcounter(gs[i]->type);
            update_del_counter_by_meta(e->counter, gs[i]);
            appendRcElement(rc, e);
            if(gs[i]->gid == gid) {
                e->base = createRcElementBase( );
                resetElementBase(e->base, set_meta, type, val);
                added = 1;
            }
            freeGcounterMeta(gs[i]);
            gs[i] = NULL;
        }
    }
    if(added == 0) {
        rc_element* e = createRcElement(gid);
        e->base = createRcElementBase( );
        resetElementBase(e->base, set_meta, type, val);
        appendRcElement(rc, e);
    }
    return PURGE_TOMBSTONE;
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
            if(!r->elements[i]->base) {
                r->elements[i]->base = createRcElementBase();
            } 
            resetElementBase(r->elements[i]->base, set_meta, type, val);
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
        e->base = createRcElementBase();
        resetElementBase(e->base, set_meta, type, val);
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
    rc->elements = NULL;
    return (CRDT_RC*)rc;
}
CRDT_RC* dupCrdtRc(CRDT_RC* rc) {
    if(rc == NULL) {return NULL;}
    crdt_rc* r = retrieveCrdtRc(rc);
    crdt_rc* dup = createCrdtRc();
    dup->vectorClock = dupVectorClock(r->vectorClock);
    for(int i = 0; i < r->len; i++) {
        rc_element* el = dupRcElement(r->elements[i]);
        appendRcElement(dup, el);
    }
    assert(r->len == dup->len);
    return  dup;
;
}

CRDT_RCTombstone* createCrdtRcTombstone() {
    crdt_rc_tombstone* rc = RedisModule_Alloc(sizeof(crdt_rc_tombstone));
    rc->type = 0;
    setDataType(rc, CRDT_RC_TYPE);
    setType(rc, CRDT_TOMBSTONE);
    rc->vectorClock = newVectorClock(0);
    rc->len = 0;
    rc->elements = NULL;
    assert(isCrdtRcTombstone(rc));
    return rc;
}

CRDT_RCTombstone* dupCrdtRcTombstone(CRDT_RCTombstone* tombstone) { 
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tombstone);
    crdt_rc_tombstone* dup = createCrdtRcTombstone();
    dup->vectorClock = dupVectorClock(rt->vectorClock);
    for(int i = 0; i < rt->len; i++) {
        rc_tombstone_element* el = dupCrdtRcTombstoneElement(rt->elements[i]);
        appendRcTombstoneElement(dup, el);
    }
    assert(dup->len == rt->len);
    return dup;
}

int appendRcTombstoneElement(crdt_rc_tombstone* rt, rc_tombstone_element* element) {
    rt->len ++;
    if(rt->len != 1) {
        rt->elements = RedisModule_Realloc(rt->elements, sizeof(rc_tombstone_element*) * rt->len);
    } else {
        rt->elements = RedisModule_Alloc(sizeof(rc_tombstone_element*) * 1);
    }
    rt->elements[rt->len-1] = element;
    qsort(rt->elements, rt->len, sizeof(rc_tombstone_element*), comp_rc_tombstone_element);
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

void freeRcTombstoneElement(void* element) {
    RedisModule_Free(element);
}

rc_tombstone_element* dupCrdtRcTombstoneElement(rc_tombstone_element* rt) {
    rc_tombstone_element* dup = createCrdtRcTombstone();
    dup->gid = rt->gid;
    dup->del_unit = rt->del_unit;
    dup->counter = dupGcounter(rt->counter);
    return dup;
}

void assign_max_rc_tombstone_element(rc_tombstone_element* target, rc_tombstone_element* src) {
    assert(target->gid == src->gid);
    target->del_unit = max(target->del_unit, src->del_unit);
    assign_max_rc_counter(target->counter, src->counter);
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
    updateRcTombstoneLastVc(rt, getMetaVectorClock(meta));
    int index = 0;
    int added = 0;
    int gid = getMetaGid(meta);
    int vcu = getVcu(meta);
    for(int i = 0; i < r->len; i++) {
        rc_tombstone_element* t = findRcTombstoneElement(rt, r->elements[i]->gid);
        if(t == NULL) {
            t = createRcTombstoneElement(r->elements[i]->gid);
            appendRcTombstoneElement(rt, t);
        }
        if(r->elements[i]->gid == gid) {
            added = 1;
            if(t->del_unit < vcu) {
                t->del_unit = vcu;
            }
        }else if(r->elements[i]->base) {
            if(t->del_unit < r->elements[i]->base->unit) {
                t->del_unit = r->elements[i]->base->unit;
            }
        }
        if(r->elements[i]->counter) {
            if(t->counter == NULL) {
                t->counter = r->elements[i]->counter;
                counter_del(t->counter, t->counter);
            } else {
                rc_element* el = r->elements[i];
                assert(t->counter->start_clock == el->counter->start_clock);
                update_add_counter(t->counter, el->counter);
                if( el->counter->end_clock < el->counter->del_end_clock) {
                    update_del_counter(t->counter, el->counter);
                } else {
                    if(t->counter->del_end_clock < el->counter->end_clock) {
                        counter_del(t->counter, el->counter);
                    }
                }   
                freeGcounter(el->counter);             
            }
            r->elements[i]->counter = NULL;
            if(del_counters) del_counters[index++] = gcounterDelToSds(t->gid,t->counter);
        }
    } 
    if(added == 0) {
        rc_tombstone_element* el = createRcTombstoneElement(gid);
        el->del_unit = vcu;
        appendRcTombstoneElement(rt, el);
    }
    return index;
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

rc_base* createRcElementBase() {
    rc_base* base = RedisModule_Alloc(sizeof(rc_base));
    return base;
}

void assign_max_rc_base(rc_base* target, rc_base* src) {
    if(target->unit < src->unit) {
        target->timespace = src->timespace;
        target->type = src->type;
        if (src->type == VALUE_TYPE_FLOAT) {
            target->conv.f = src->conv.f;
        } else if(src->type == VALUE_TYPE_INTEGER) {
            target->conv.i = src->conv.i;
        }
    }
}

rc_base* dupRcBase(rc_base* base) {
    if(base == NULL) { return NULL;}
    rc_base* dup = createRcElementBase();
    dup->type = base->type;
    dup->timespace = base->timespace;
    dup->unit = base->unit;
    if(dup->type == VALUE_TYPE_FLOAT) {
        dup->conv.f = base->conv.f;
    } else if (dup->type == VALUE_TYPE_INTEGER) {
        dup->conv.i = base->conv.i;
    }
    return dup;
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

void freeRcElement(void* element) {
    RedisModule_Free(element);
}



rc_element* dupRcElement(rc_element* el) {
    rc_element* dup = createRcElement(el->gid);
    dup->base = dupRcBase(el->base);
    dup->counter = dupGcounter(el->counter);
    return dup;
}

void assign_max_rc_element(rc_element* target, rc_element* src) {
    if(target == NULL || src == NULL) return;
    assert(target->gid == src->gid);
    if(src->base) {
        if(!target->base) target->base = createRcElementBase();
        assign_max_rc_base(target->base, src->base);
    }
    if(src->counter) {
        if(!target->counter) target->counter = createGcounter(src->counter->type);
        assign_max_rc_counter(target->counter, src->counter);
    }
    
}

int appendRcElement(crdt_rc* rc, rc_element* element) {
    rc->len ++;
    if(rc->len != 1) {
        rc->elements = RedisModule_Realloc(rc->elements, sizeof(rc_element*) * rc->len);
    } else {
        rc->elements = RedisModule_Alloc(sizeof(rc_element*) * 1);
    }
    rc->elements[rc->len-1] = element;
    qsort(rc->elements, rc->len, sizeof(rc_element*), comp_rc_element);
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

CrdtObject *crdtRcMerge(CrdtObject *currentVal, CrdtObject *value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    if(currentVal == NULL) {
        return dupCrdtRc(value);
    }
    if(value == NULL) {
        return dupCrdtRc(currentVal);
    }
    crdt_rc *current = retrieveCrdtRc(currentVal);
    crdt_rc *other = retrieveCrdtRc(value);
    crdt_rc* result = dupCrdtRc(current);
    freeVectorClock(result->vectorClock);
    result->vectorClock = vectorClockMerge(current->vectorClock, other->vectorClock);
    for(int i = 0; i < other->len; i++) {
        rc_element* el = findRcElement(current, other->elements[i]->gid);
        if(el == NULL) {
            el = dupRcElement(other->elements[i]);
            appendRcElement(result, el);
        } else {
            assign_max_rc_element(el, other->elements[i]);
        }
    }
    return result;
}

CrdtObject** crdtRcFilter(CrdtObject* target, int gid, long long logic_time, long long maxsize, int* length) {
    crdt_rc* rc = retrieveCrdtRc(target);
    //value + gid + time + vectorClock
    if (crdtRcMemUsageFunc(rc) > maxsize) {
        *length  = -1;
        return NULL;
    }
    VectorClockUnit unit = getVectorClockUnit(rc->vectorClock, gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    long long vcu = get_logic_clock(unit);
    if(vcu > logic_time) {
        *length = 1;
        crdt_rc** re = RedisModule_Alloc(sizeof(crdt_rc*));
        re[0] = rc;
        return re;
    }  
    return NULL;
}

void freeRcFilter(CrdtObject** filters, int num) {
    RedisModule_ZFree(filters);
}
//=== type =====
RedisModuleType* getCrdtRc() {return CrdtRC;};
RedisModuleType* getCrdtRcTombstone() {return CrdtRCT;}