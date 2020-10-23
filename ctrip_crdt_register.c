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
#include <strings.h>
//    util function
int isCrdtRcTombstone(CrdtTombstone* tom) {
    if(tom != NULL && getType(tom) == CRDT_TOMBSTONE && (getDataType(tom) ==  CRDT_RC_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
//====================== send command to slaves ==================
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
//crdt.rc key value gid time vc expire 68*n
const char* crdt_rc_head = "$7\r\nCRDT.rc\r\n";
const size_t crdt_rc_basic_str_len = 18 + 2 *REPLICATION_MAX_STR_LEN + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + REPLICATION_MAX_LONGLONG_LEN;
size_t replicationFeedCrdtRCCommand(RedisModuleCtx *ctx,char* cmdbuf, const char* keystr, size_t keylen,const char* valstr, size_t vallen, CrdtMeta* meta, VectorClock vc, long long expire_time, int eslen, sds* del_strs) {
    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, eslen + 7);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_rc_head);
    cmdlen += feedKV2Buf(cmdbuf + cmdlen, keystr, keylen, valstr, vallen);
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), vc);
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expire_time);
    for(int i = 0; i < eslen; i++) {
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, del_strs[i], sdslen(del_strs[i]));
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int replicationCrdtRcCommand(RedisModuleCtx* ctx, RedisModuleString* key, RedisModuleString* val, CrdtMeta* set_meta, CRDT_RC* rc ,long long expire, int eslen, sds* del_strs) {
    // sds vcSds = vectorClockToSds(getCrdtRcLastVc(rc));
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Rc", "ssllcla", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire,  del_strs, (size_t)eslen);
    // sdsfree(vcSds);
    size_t keylen = 0;
    const char* keystr = RedisModule_StringPtrLen(key, &keylen);
    size_t vallen = 0;
    const char* valstr = RedisModule_StringPtrLen(val, &vallen);
    size_t alllen = keylen + vallen + crdt_rc_basic_str_len + 68 * eslen;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtRCCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,set_meta, getCrdtRcLastVc(rc), expire, eslen,  del_strs);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtRCCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,set_meta, getCrdtRcLastVc(rc), expire, eslen, del_strs);
    }
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

int mgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 2) return RedisModule_WrongArity(ctx);
    RedisModule_ReplyWithArray(ctx, argc - 1);
    for(int i = 1; i < argc; i++) {
        getGeneric(ctx, argv[i], 0);
    }
    return CRDT_OK;
}

int getCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    getGeneric(ctx, argv[1], 1);
    return REDISMODULE_OK;
}
//============================== write command ==============================
int crdtRcDelete(int dbId, void *keyRobj, void *key, void *value) {
    RedisModuleKey *moduleKey = (RedisModuleKey *)key;
    CrdtMeta del_meta = {.gid = 0};
    initIncrMeta(&del_meta);
    VectorClock lastVc = getCrdtRcLastVc(value);
    appendVCForMeta(&del_meta, lastVc);
    CRDT_RCTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isCrdtRcTombstone(tombstone)) {
        tombstone = createCrdtRcTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRCT, tombstone);
    }
    int len = get_len(lastVc);
    sds del_counters[len];
    int dlen = initRcTombstoneFromRc(tombstone, &del_meta, value, del_counters);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&del_meta));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Rc", "sllca", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds, del_counters, (size_t)dlen);
    sdsfree(vcSds);
    freeIncrMeta(&del_meta);
    return CRDT_OK;
}


int incrbyGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int type, void* increment) {
    RedisModuleKey* moduleKey = NULL;
    CrdtMeta set_meta = {.gid  = 0};
    CRDT_RC* current = NULL;
    moduleKey = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
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
    if (current == NULL) {
        current = createCrdtRc();
        initCrdtRcFromTombstone(current, tom);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, current);
        RedisModule_DeleteTombstone(moduleKey);
    } else {
        if(type == VALUE_TYPE_INTEGER && getCrdtRcType(current) != VALUE_TYPE_INTEGER) {
            RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
            goto error;
        }
    }
    gcounter* g = addOrCreateCounter(current, &set_meta, type, increment);
    sds vc_info = vectorClockToSds(getMetaVectorClock(&set_meta));
    if (type == VALUE_TYPE_FLOAT) {
        RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "sllcllf", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), vc_info, g->start_clock, g->end_clock, g->conv.f);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrbyfloat", argv[1]);
        RedisModule_ReplyWithLongDouble(ctx, getCrdtRcFloatValue(current));
    } else if(type == VALUE_TYPE_INTEGER) {
        RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "sllclll", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), vc_info, g->start_clock, g->end_clock, g->conv.i);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrby", argv[1]);
        RedisModule_ReplyWithLongLong(ctx, getCrdtRcIntValue(current));
    }
    sdsfree(vc_info);
    
error:
    if(set_meta.gid != 0) freeIncrMeta(&set_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}

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
        sds gs[getRcElementLen(rc)];
        int len = crdtRcSetValue(rc, &set_meta, gs, tombstone,  val_type, val_type == VALUE_TYPE_FLOAT? (void*)&float_val: (void*)&int_val);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(ctx, key, val, &set_meta,rc, expire_time, len, gs);
        for(int i = 0; i < len; i++) {
            sdsfree(gs[i]);
        }
    } else if((tombstone && isCrdtRcTombstone(tombstone)) || (!tombstone && (val_type == VALUE_TYPE_INTEGER || val_type == VALUE_TYPE_FLOAT) )) {
        rc = createCrdtRc();
        if(tombstone) {
            appendVCForMeta(&set_meta, getCrdtRegisterTombstoneLastVc(tombstone));
            initCrdtRcFromTombstone(rc, tombstone);
        } 
        sds gs[getRcElementLen(rc)];
        crdtRcSetValue(rc, &set_meta, gs, tombstone,  val_type, val_type == VALUE_TYPE_FLOAT? (void*)&float_val: (void*)&int_val);
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
    RedisModuleKey* moduleKey = NULL;
    CrdtMeta meta = {.gid = 0};
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        goto end;
    }
    long long start_clock = 0;
    if (RedisModule_StringToLongLong(argv[5],&start_clock) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"1ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long end_clock = 0;
    if (RedisModule_StringToLongLong(argv[6],&end_clock) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"2ERR value: must be a signed 64 bit integer");
        goto end;
    }
    long long l = 0;
    long double ld = 0;
    sds s;
    int val_type = getValType( argv[7], &l, &ld, &s);
    if (val_type == VALUE_TYPE_SDS) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        goto end;
    }
    moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if (moduleKey == NULL) {
        return 0;
    }   
    CRDT_RCTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_RC* rc = RedisModule_ModuleTypeGetValue(moduleKey);
    int need_add = 0;
    if(rc == NULL) {
        need_add = 1;
        rc = createCrdtRc();
    }
    int result = tryUpdateCounter(rc, tombstone, meta.gid, meta.timestamp, start_clock , end_clock, val_type, val_type == VALUE_TYPE_FLOAT? (void*)&ld: (void*)&l );
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
    int result = crdtRcTrySetValue(rc, &meta, argc - 7, dels, tombstone, val_type, val_type == VALUE_TYPE_FLOAT ? (void*)&float_val: (void*) &int_val);
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
        RedisModule_ReplyWithArray(ctx, 5);
        CrdtMeta* lastMeta = getCrdtRcLastMeta(rc);
        RedisModule_ReplyWithLongLong(ctx, getMetaGid(lastMeta));
        RedisModule_ReplyWithLongLong(ctx, getMetaTimestamp(lastMeta));
        sds vclockSds = vectorClockToSds(getMetaVectorClock(lastMeta));
        RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
        sdsfree(vclockSds);
        
        if(getCrdtRcType(rc) == VALUE_TYPE_FLOAT) {
            RedisModule_ReplyWithLongDouble(ctx, getCrdtRcBaseFloatValue(rc, lastMeta));
            RedisModule_ReplyWithLongDouble(ctx, getCrdtRcCouanterFloatValue(rc));
        } else {
            RedisModule_ReplyWithLongLong(ctx, getCrdtRcBaseIntValue(rc, lastMeta));
            RedisModule_ReplyWithLongLong(ctx, getCrdtRcCouanterIntValue(rc));
        }
        freeCrdtMeta(lastMeta);
    }
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
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
        CRDT_RC* current = getCurrentValue(moduleKey);
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


void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcMemUsageFunc(const void *value) {
    return 1;
}



void crdtRcDigestFunc(RedisModuleDigest *md, void *value) {

}

//========================= RcTombstone moduleType functions =======================

void *RdbLoadCrdtRcTombstone(RedisModuleIO *rdb, int encver)  {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == ORSET_TYPE) {
        return RdbLoadCrdtOrSetRcTombstone(rdb, version, encver);
    }
    return NULL;
}


void AofRewriteCrdtRcTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {

}
size_t crdtRcTombstoneMemUsageFunc(const void *value) {
    return 1;
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
    if (RedisModule_CreateCommand(ctx,"CRDT.RC",
                                  CRDT_RCCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"GET",
                                  getCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.GET",
                                  CRDT_GetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_Rc",
                                  CRDT_DelRcCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "SETEX", 
                                    setexCommand, "write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
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
    if (RedisModule_CreateCommand(ctx, "MGET", 
                                    mgetCommand, "readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}



//========================= Virtual functions =======================



//=== type =====
RedisModuleType* getCrdtRc() {return CrdtRC;};
RedisModuleType* getCrdtRcTombstone() {return CrdtRCT;}