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
#include "crdt.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
#include "ctrip_crdt_expire.h"
#include "ctrip_swap.h"
#include <string.h>
#include <strings.h>

/******************    about type  +************************/
RedisModuleType* getCrdtRc() {return CrdtRC;};
RedisModuleType* getCrdtRcTombstone() {return CrdtRCT;}


/******************    util function  +************************/
int isCrdtRcTombstone(CrdtTombstone* tom) {
    if(tom != NULL && getType(tom) == CRDT_TOMBSTONE && (getDataType(tom) ==  CRDT_RC_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}

int isCrdtRc(CrdtObject* data) {
    if(data != NULL && getType(data) == CRDT_DATA && (getDataType(data) ==  CRDT_RC_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}


/*****************  send command *************************/
const char* crdt_counter_head = "*7\r\n$12\r\nCRDT.COUNTER\r\n";
int replicationFeedCrdtCounterCommand(RedisModuleCtx* ctx, char* cmdbuf, sds key, CrdtMeta* meta, int type, sds data) {
    size_t cmdlen = 0;
    static int crdt_counter_head_str_len = 0;
    if(crdt_counter_head_str_len == 0) {
        crdt_counter_head_str_len = strlen(crdt_counter_head);
    }
    cmdlen +=  feedBuf(cmdbuf + cmdlen, crdt_counter_head, crdt_counter_head_str_len);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, type);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, data, sdslen(data));
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}

//CRDT.COUNTER key gid time vc type value
int replicationCrdtCounterCommand(RedisModuleCtx *ctx, sds key,CrdtMeta* meta, int type, sds data) {
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.COUNTER", "sllcllll", argv[1], getMetaGid(&set_meta), getMetaTimestamp(&set_meta), vc_info, g->start_clock, g->end_clock, (size_t)g->type, g->conv.i);
    int alllen = 24 + sdslen(key) + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + 8+ sdslen(data);
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtCounterCommand(ctx, cmdbuf, key, meta, type, data);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtCounterCommand(ctx, cmdbuf, key, meta, type, data);
    }
    return 1;
}

//CRDT.Rc  gid time vc expire  key value
const char* crdt_rc_head = "$7\r\nCRDT.rc\r\n";
const size_t crdt_rc_basic_str_len = 18 + 2 *REPLICATION_MAX_STR_LEN + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + REPLICATION_MAX_LONGLONG_LEN;
int replicationFeedCrdtRCCommand(RedisModuleCtx* ctx, char* cmdbuf,sds key,CrdtMeta* meta, sds value,long long expire) {
    int cmdlen = 0;

    static size_t crdt_rc_head_str_len = 0;
    if (crdt_rc_head_str_len == 0) {
        crdt_rc_head_str_len = strlen(crdt_rc_head);
    }
    cmdlen += feedArgc(cmdbuf + cmdlen, 7);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_rc_head, crdt_rc_head_str_len);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, value, sdslen(value));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expire);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}

int replicationCrdtRcCommand(RedisModuleCtx* ctx, sds key, CrdtMeta* meta, sds value, long long expire) {
    size_t alllen = sdslen(key) + sdslen(value) + crdt_rc_basic_str_len;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtRCCommand(ctx, cmdbuf, key , meta, value, expire);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtRCCommand(ctx, cmdbuf, key, meta, value, expire);
    }
    return alllen;
}

int replicationFeedCrdtRCCommand2(RedisModuleCtx* ctx, char* cmdbuf,sds key,CrdtMeta* meta, char* value, int value_len,long long expire) {
    int cmdlen = 0;

    static size_t crdt_rc_head_str_len = 0;
    if (crdt_rc_head_str_len == 0) {
        crdt_rc_head_str_len = strlen(crdt_rc_head);
    }
    cmdlen += feedArgc(cmdbuf + cmdlen, 7);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_rc_head, crdt_rc_head_str_len);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    // cmdlen += feedStr2Buf(cmdbuf + cmdlen, value, sdslen(value));
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, value, value_len);
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expire);
    // RedisModule_Debug(logLevel, "cmd: %d - %s", cmdlen, cmdbuf);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int replicationCrdtRcCommand2(RedisModuleCtx* ctx, sds key, CrdtMeta* meta, char* value, int value_len, long long expire) {
    size_t alllen = sdslen(key) + value_len + crdt_rc_basic_str_len;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtRCCommand2(ctx, cmdbuf, key , meta, value, value_len, expire);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtRCCommand2(ctx, cmdbuf, key, meta, value, value_len,  expire);
    }
    return alllen;
}


/******************    commands  -************************/
int incrbyGenericCommand(RedisModuleCtx *ctx, RedisModuleString* key, int type, ctrip_value incr_value) {
    RedisModuleKey* moduleKey = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    //register to 
    
    CrdtMeta set_meta = {.gid  = 0};
    CrdtObject* current = getCurrentValue(moduleKey);
    RedisModuleType* mtype= RedisModule_ModuleTypeGetType(moduleKey) ;
    CRDT_RC* rc = NULL;
    if (mtype == getCrdtRegister()) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
        goto error;
        // to do ?
        // rc = register_to_rc(current);
    } else if (mtype == CrdtRC) {
        rc = (CRDT_RC*)current;
    } else if (mtype != NULL){
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
        goto error;
    }  
    initIncrMeta(&set_meta);
    CrdtTombstone* tom = getTombstone(moduleKey);
    if(tom != NULL && !isCrdtRcTombstone(tom)) {
        tom = NULL;
    }
    if(tom) {
        if(type == VALUE_TYPE_LONGLONG) {
            ctrip_value add_value = {.type = VALUE_TYPE_NONE, .value.i = 0};
            if(get_rc_tombstone_tag_add_value(tom, getMetaGid(&set_meta), &add_value)) {
                switch(plus_or_minus_ctrip_value(&add_value, &incr_value, 1)) {
                    case PLUS_ERROR_NAN: 
                        RedisModule_ReplyWithError(ctx, "ERR increment would produce NaN or Infinity");
                        goto error;
                    break;
                    case PLUS_ERROR_OVERFLOW:
                        RedisModule_ReplyWithError(ctx, "ERR increment or decrement would overflow");
                        goto error;
                    break;
                }
            }
        }
    }
    if (current == NULL) {
        rc = createCrdtRc();
        initCrdtRcFromTombstone(rc, tom);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
        RedisModule_DeleteTombstone(moduleKey);
        tom = NULL;
    } else {
        ctrip_value value = {.type=VALUE_TYPE_NONE,.value.i = 0};
        crdtAssert(get_crdt_rc_value(rc, &value));
        if(type == VALUE_TYPE_LONGLONG) {
            if(!value_to_ll(&value)) {
                RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
                goto error;
            }
            ctrip_value add_value = {.type = VALUE_TYPE_NONE, .value.i = 0};
            if(get_rc_tag_add_value(rc, getMetaGid(&set_meta), &add_value)) {
                switch(plus_or_minus_ctrip_value(&add_value, &incr_value, 1)) {
                    case PLUS_ERROR_NAN: 
                        RedisModule_ReplyWithError(ctx, "ERR increment would produce NaN or Infinity");
                        goto error;
                    break;
                    case PLUS_ERROR_OVERFLOW:
                        RedisModule_ReplyWithError(ctx, "ERR increment or decrement would overflow");
                        goto error;
                    break;
                    case SDS_PLUS_ERR:
                        RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
                        goto error;
                    break;
                }
            }
        }
        int plus_result = plus_or_minus_ctrip_value(&value, &incr_value ,1);
        switch(plus_result) {
            case PLUS_ERROR_NAN: 
                RedisModule_ReplyWithError(ctx, "ERR increment would produce NaN or Infinity");
                goto error;
            break;
            case PLUS_ERROR_OVERFLOW:
                RedisModule_ReplyWithError(ctx, "ERR increment or decrement would overflow");
                goto error;
            break;
            case SDS_PLUS_ERR:
                if(type ==VALUE_TYPE_LONGLONG) {
                    RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
                } else {
                    RedisModule_ReplyWithError(ctx, "ERR value is not a valid float");
                }
                goto error;
            break;
        }
        if(value.type == VALUE_TYPE_LONGLONG && (value.value.i > COUNTER_MAX || value.value.i < COUNTER_MIN)) {
            RedisModule_ReplyWithError(ctx, "ERR increment or decrement would overflow");
            goto error;
        }

    } 
    
    sds result = rcIncrby(rc, &set_meta, incr_value.type, &incr_value.value);
    replicationCrdtCounterCommand(ctx, RedisModule_GetSds(key), &set_meta, type, result);
    sdsfree(result);
    if(type == VALUE_TYPE_LONGDOUBLE) {
        RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_STRING, "incrbyfloat", key, moduleKey, NULL);
    } else {
        RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_STRING, "incrby", key, moduleKey, NULL);
    }
    ctrip_value v = {.type = VALUE_TYPE_NONE, .value.i = 0};
    crdtAssert(get_crdt_rc_value(rc, &v));
    switch (v.type)
    {
    case VALUE_TYPE_LONGLONG:
        RedisModule_ReplyWithLongLong(ctx, v.value.i);
        break;
    case VALUE_TYPE_LONGDOUBLE:
        if(type == VALUE_TYPE_LONGLONG) {
            RedisModule_ReplyWithLongLong(ctx, (long long)v.value.f);
        } else {
            RedisModule_ReplyWithLongDouble(ctx, v.value.f);
        }
        break;
    default:
        printf("[incrbyGenericCommand]code error\n");
        RedisModule_ReplyWithError(ctx, "code error");
        break;
    }
error:
    if(set_meta.gid != 0) freeIncrMeta(&set_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}



int incrbyIntCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int incr) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    sds v = RedisModule_GetSds(argv[2]);
    ctrip_value value = {.type = VALUE_TYPE_SDS, .value.s = v};
    if(!value_to_ll(&value)) {
        RedisModule_ReplyWithError(ctx,"ERR value is not an integer or out of range");
        return 0;
    }
    if(value.type == VALUE_TYPE_LONGLONG && (value.value.i > COUNTER_MAX || value.value.i < COUNTER_MIN)) {
        RedisModule_ReplyWithError(ctx, "ERR increment or decrement would overflow");
        return 0;
    }
    if(!incr) {
        value.value.i = -value.value.i;
    }
    return incrbyGenericCommand(ctx, argv[1], VALUE_TYPE_LONGLONG, value);
}

int decrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return incrbyIntCommand(ctx, argv, argc, 0);
}

int incrbyCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return incrbyIntCommand(ctx, argv, argc, 1);
}

int incrbyfloatCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    sds v = RedisModule_GetSds(argv[2]);
    ctrip_value value = {.type = VALUE_TYPE_SDS, .value.s = v};
    if(!value_to_ld(&value)) {
        RedisModule_ReplyWithError(ctx,"ERR value is not a valid float");
        return 0;
    }
    return incrbyGenericCommand(ctx, argv[1], VALUE_TYPE_LONGDOUBLE, value);
}

int incrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    ctrip_value value = {.type = VALUE_TYPE_LONGLONG, .value.i = 1};
    return incrbyGenericCommand(ctx, argv[1], VALUE_TYPE_LONGLONG, value);
}

int decrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    ctrip_value value = {.type = VALUE_TYPE_LONGLONG, .value.i = -1};
    return incrbyGenericCommand(ctx, argv[1], VALUE_TYPE_LONGLONG, value);
}
//crdt.counter key gid time vc type value
int crdtCounterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 7) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    long long type = 0;
    if(RedisModule_StringToLongLong(argv[5], &type) !=  REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: type must be a signed 64 bit integer");
        return 0;
    }
    crdtAssert(meta.gid != 0);
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_RC* current = getCurrentValue(moduleKey);
    int need_add = 0;
    if(current == NULL) {
        current = createCrdtRc();
        need_add = 1;
    } 
    int result = rcTryIncrby(current, tombstone, &meta, RedisModule_GetSds(argv[6]));
    if(result == PURGE_VAL && need_add) {
        freeCrdtRc(current);
        current = NULL;
    } else {
        if(result == PURGE_TOMBSTONE && tombstone != NULL) {
            RedisModule_DeleteTombstone(moduleKey);
            tombstone = NULL;
        }
        if(need_add) RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, current);
        if(type == VALUE_TYPE_LONGDOUBLE) {
             RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrbyfloat", argv[1]);
        } else {
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "incrby", argv[1]);
        }
    }
    RedisModule_DbSetDirty(ctx, argv[1]);
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}
#define TYPE_ERR -1
int check_type(sds val, RedisModuleKey* moduleKey) {
    RedisModuleType* mtype = RedisModule_ModuleTypeGetType(moduleKey);
    if(mtype == getCrdtRegister()) {
        return CRDT_REGISTER_TYPE;
    } else if(mtype == CrdtRC) {
        return CRDT_RC_TYPE;
    } else if(mtype != NULL) {
        return TYPE_ERR;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    if(t != NULL) {
        if(isRegisterTombstone(t)) {
            return CRDT_REGISTER_TYPE;
        } else if(isCrdtRcTombstone(t)) {
            return CRDT_RC_TYPE;
        } 
    } 
    ctrip_value v = {.type = VALUE_TYPE_SDS,.value.s = val};
    if(value_to_ld(&v)) {
        return CRDT_RC_TYPE;
    }
    if(value_to_ll(&v)) {
        return CRDT_RC_TYPE;
    }
    return CRDT_REGISTER_TYPE;
}

sds add_rc_by_modulekey(RedisModuleKey* moduleKey, CrdtMeta* meta, sds value) {
    CRDT_RC* rc = getCurrentValue(moduleKey);
    if(rc == NULL) {
        rc = createCrdtRc();
        CRDT_RCTombstone* tombstone = getTombstone(moduleKey);
        if (tombstone && isCrdtRcTombstone(tombstone)) {
            initCrdtRcFromTombstone(rc, tombstone);
            RedisModule_DeleteTombstone(moduleKey);
        }
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
    } 
    return rcAdd(rc, meta, value);
}

sds add_reg_by_modulekey(RedisModuleKey* moduleKey, CrdtMeta* meta, sds val) {
    CRDT_Register* reg = getCurrentValue(moduleKey);
    if(reg == NULL) {
        reg = createCrdtRegister();
        CRDT_RegisterTombstone* tombstone = getTombstone(moduleKey);
        if(tombstone && isRegisterTombstone(tombstone)) {
            appendVCForMeta(meta, getCrdtRegisterTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            appendVCForMeta(meta, LL2VC(vc));
        }
        crdtRegisterSetValue(reg, meta, val);
        RedisModule_ModuleTypeSetValue(moduleKey, getCrdtRegister(), reg);
        RedisModule_DeleteTombstone(moduleKey);
    } else {
        crdtRegisterTryUpdate(reg, meta, val, COMPARE_META_VECTORCLOCK_GT);
        appendVCForMeta(meta, getCrdtRegisterLastVc(reg));
    }
    return sdsdup(val);
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

const char* crdt_set_head = "*7\r\n$8\r\nCRDT.SET\r\n";
const char* crdt_set_no_expire_head = "*6\r\n$8\r\nCRDT.SET\r\n";
//CRDT.SET key value gid time vc expire
const size_t crdt_set_basic_str_len = 18 + 2 *REPLICATION_MAX_STR_LEN + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + REPLICATION_MAX_LONGLONG_LEN;
size_t replicationFeedCrdtSetCommand(RedisModuleCtx *ctx,char* cmdbuf, sds key, sds val, CrdtMeta* meta, long long expire_time) {
    size_t cmdlen = 0;
    static size_t crdt_set_head_str_len = 0, crdt_set_no_expire_head_str_len = 0;
    if(crdt_set_head_str_len == 0) {
        crdt_set_head_str_len = strlen(crdt_set_head);
        crdt_set_no_expire_head_str_len = strlen(crdt_set_no_expire_head);
    }
    if(expire_time > -2) {
        cmdlen +=  feedBuf(cmdbuf + cmdlen, crdt_set_head, crdt_set_head_str_len);
    }else{
        cmdlen += feedBuf(cmdbuf + cmdlen, crdt_set_no_expire_head, crdt_set_no_expire_head_str_len);
    }
    //will change to
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key));
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, val, sdslen(val));
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    if(expire_time > -2) {
        cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expire_time);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}

int replicationCrdtSetCommand(RedisModuleCtx* ctx, sds key, sds val, CrdtMeta* set_meta,  long long expire_time) {
    // sds vcSds = vectorClockToSds(vc);
    // RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.SET", "ssllcl", key, val, getMetaGid(set_meta), getMetaTimestamp(set_meta), vcSds, expire);
    // sdsfree(vcSds);
    size_t alllen = sdslen(key) + sdslen(val) + crdt_set_basic_str_len;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtSetCommand(ctx, cmdbuf, key, val,set_meta, expire_time);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtSetCommand(ctx, cmdbuf, key, val,set_meta, expire_time);
    }
    return 1;
}
/**
 *  only use mset
 */
int add_rc_by_key(RedisModuleCtx* ctx, void* val, void* tom, CrdtMeta* meta, RedisModuleString* key, sds value, char* buf) {
    CRDT_RC* rc = val;
    if(rc == NULL) {
        rc = createCrdtRc();
        CRDT_RCTombstone* tombstone = tom;
        if (tombstone && isCrdtRcTombstone(tombstone)) {
            initCrdtRcFromTombstone(rc, tombstone);
            // RedisModule_DeleteTombstone(moduleKey);
            RedisModule_DeleteTombstoneByKey(ctx, key);
        }
        RedisModule_DbSetValue(ctx, key, CrdtRC, rc);
    } 
    return rcAdd2(rc, meta, value, buf);
    // return rcAdd(rc, meta, value);
}

int setGenericCommand(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, int flags, RedisModuleString* key, RedisModuleString* val, RedisModuleString* expire, int unit, int sendtype) {
    int result = 0;
    CrdtMeta set_meta = {.gid = 0};
    //get value
    if(moduleKey == NULL) {
        moduleKey = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    }
    CrdtObject* current = getCurrentValue(moduleKey);
    if((current != NULL && flags & OBJ_SET_NX) 
        || (current == NULL && flags & OBJ_SET_XX)) {
        if(sendtype) RedisModule_ReplyWithNull(ctx);   
        goto error;
    }
   
    sds callback_item;
    int type = check_type(RedisModule_GetSds(val), moduleKey);
    if(type == TYPE_ERR) {
        if(sendtype) RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
        goto error;
    }
    long long milliseconds = 0;
    if (expire) {
        if (RedisModule_StringToLongLong(expire, &milliseconds) != REDISMODULE_OK) {
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx, "ERR syntax error");
            goto error;
        }
        if (milliseconds <= 0) {
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx,"invalid expire time in set");
            goto error;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }
    initIncrMeta(&set_meta);
    long long expire_time = -2;
    if(type == CRDT_RC_TYPE) {
        callback_item = add_rc_by_modulekey(moduleKey, &set_meta, RedisModule_GetSds(val));
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtRcCommand(ctx, RedisModule_GetSds(key), &set_meta, callback_item, expire_time);
        sdsfree(callback_item);
    } else if(type == CRDT_REGISTER_TYPE) {
        callback_item = add_reg_by_modulekey(moduleKey, &set_meta, RedisModule_GetSds(val));
        sdsfree(callback_item);
        expire_time = setExpireByModuleKey(moduleKey, flags, expire, milliseconds, &set_meta);
        replicationCrdtSetCommand(ctx, RedisModule_GetSds(key), RedisModule_GetSds(val), &set_meta,  expire_time);
    }
    RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_STRING, "set", key, moduleKey, NULL);
    if(expire) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "expire", key);
    }
    result = 1;
error:
    if(set_meta.gid != 0) freeIncrMeta(&set_meta);
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
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

int setnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 3) return RedisModule_WrongArity(ctx);
    int result = setGenericCommand(ctx, NULL, OBJ_SET_NO_FLAGS | OBJ_SET_NX, argv[1], argv[2], NULL, UNIT_SECONDS, 0);
    return RedisModule_ReplyWithLongLong(ctx, result);
}

int psetexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 4) return RedisModule_WrongArity(ctx);
    int result = setGenericCommand(ctx, NULL, OBJ_SET_NO_FLAGS | OBJ_SET_EX, argv[1], argv[3], argv[2], UNIT_MILLISECONDS, 1);
    if(result == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx);
    } else {
        return CRDT_ERROR;
    } 
}
/**
 * set k v
 *   crdt.set when value is string and tombstone is null   
 *   crdt.rc  when value is int or float
 */

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
    sds del_counter_str = initRcTombstoneFromRc(tombstone, &del_meta, value);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&del_meta));
    //crdt.del_rc key gid time vc del_counter
    if(del_counter_str == NULL || sdslen(del_counter_str) == 0) {
        RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Rc", "sllc", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds);
    } else {
        RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Rc", "sllcc", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds, del_counter_str);
        sdsfree(del_counter_str);
    }
    sdsfree(vcSds);
    freeRcLastVc(lastVc);
    freeIncrMeta(&del_meta);
    return CRDT_OK;
}

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
        RedisModule_ReplyWithStringBuffer(ctx, val, sdslen(val));
        goto next;
    }
    if(rc) {
        ctrip_value value = {.type = VALUE_TYPE_NONE, .value.i = 0};
        crdtAssert(get_crdt_rc_value(rc, &value));
        switch(value.type) {
            case VALUE_TYPE_SDS:
                RedisModule_ReplyWithStringBuffer(ctx, value.value.s, sdslen(value.value.s));
            break;
            case VALUE_TYPE_LONGDOUBLE: {
                // RedisModule_ReplyWithLongDouble(ctx, value.value.f);
                char buf[MAX_LONG_DOUBLE_CHARS];
                int len = ld2string(buf, sizeof(buf), value.value.f, 1);
                RedisModule_ReplyWithStringBuffer(ctx, buf, len);
            }
            break;
            case VALUE_TYPE_DOUBLE: {
                char buf[MAX_LONG_DOUBLE_CHARS];
                int len = d2string(buf, sizeof(buf), value.value.d);
                RedisModule_ReplyWithStringBuffer(ctx, buf, len);
            }
            break;
            case VALUE_TYPE_LONGLONG: {
                // RedisModule_ReplyWithLongLong(ctx, value.value.i);
                //noncrdt redis callback  string not int
                char buf[256];
                int len = ll2string(buf, sizeof(buf), value.value.i);
                RedisModule_ReplyWithStringBuffer(ctx, buf, len);
            }
            break;
            default: {
                sds info = crdtRcInfo(rc);
                RedisModule_Debug(logLevel, "[CRDT_RC] value type error %d,  key: %s ,info: %s", value.type, RedisModule_GetSds(key),  info);
                sdsfree(info);
                if(sendtype) {
                    RedisModule_ReplyWithError(ctx, "[CRDT_RC][Get] type error");
                } else {
                    RedisModule_ReplyWithNull(ctx);
                }
            }
            break;
        }
    }
next:
    RedisModule_CloseKey(modulekey);
    return CRDT_OK;
error:
    RedisModule_CloseKey(modulekey);
    return CRDT_ERROR;
}

int mgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

int crdtRcGeneric(RedisModuleCtx *ctx, RedisModuleString* key, RedisModuleString* value, CrdtMeta* meta, long long expire_time) {
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, key, CrdtRC);
    int result = 0;
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        goto end;
    }
    CRDT_RC* rc = RedisModule_ModuleTypeGetValue(moduleKey);
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
    int need_add = 0;
    if(rc == NULL) {
        rc = createCrdtRc();
        need_add = 1;
    }
    result = rcTryAdd(rc, tombstone, meta, RedisModule_GetSds(value));
    if(result == PURGE_VAL && need_add) {
        freeCrdtRc(rc);
    } else if(result == PURGE_TOMBSTONE) {
        if(need_add) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRC, rc);
        }
        if(tombstone) {
            RedisModule_DeleteTombstone(moduleKey);
        }
        if(expire_time != -2) {
            trySetExpire(moduleKey, key, getMetaTimestamp(meta),  CRDT_RC_TYPE, expire_time);
        }
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
    }
    RedisModule_DbSetDirty(ctx, key);
end:
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    return result;
}



//CRDT.Rc  key gid time vc  value expire
int crdtRcCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 7) return RedisModule_WrongArity(ctx);
    long long expire_time = -2;
    if ((RedisModule_StringToLongLong(argv[6], &expire_time) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }  
    CrdtMeta meta = {.gid = 0};
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    crdtRcGeneric(ctx, argv[1], argv[5], &meta, expire_time);
    RedisModule_CrdtReplicateVerbatim(meta.gid, ctx);
    freeIncrMeta(&meta);
    return RedisModule_ReplyWithOk(ctx);
}
//crdt.mset_rc gid time [k v vc]...
int crdtMsetRcCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6 || argc % 3 != 0) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    long long gid;
    if ((redisModuleStringToGid(ctx, argv[1],&gid) != REDISMODULE_OK)) {
        return 0;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[2],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return 0;
    }
    meta.gid = gid;
    meta.timestamp = timestamp;
    for(int i = 3; i < argc; i+=3) {
        meta.vectorClock = getVectorClockFromString(argv[i+2]);
        crdtRcGeneric(ctx, argv[i], argv[i+1], &meta, -2);
        freeVectorClock(meta.vectorClock);
    }
    RedisModule_CrdtReplicateVerbatim(meta.gid, ctx);
    return RedisModule_ReplyWithOk(ctx);
}
//crdt.del_rc key gid time vc   del_counter
int crdtDelRcCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 5) return RedisModule_WrongArity(ctx);  
    CrdtMeta meta = {.gid = 0};
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRC);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        goto end;
    }
    CRDT_RC* rc = RedisModule_ModuleTypeGetValue(moduleKey);
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL && !isCrdtRcTombstone(tombstone)) {
        tombstone = NULL;
    }
    int need_add = 0;
    if(tombstone == NULL) {
        tombstone = createCrdtRcTombstone();
        need_add = 1;
    }
    int result = rcTryDel(rc, tombstone, &meta, argc > 5 ? RedisModule_GetSds(argv[5]): NULL);
    if(result == PURGE_TOMBSTONE && need_add) {
        freeCrdtRcTombstone(tombstone);
    } else {
        if(result == PURGE_VAL && rc != NULL) {
            RedisModule_DeleteKey(moduleKey);
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "del", argv[1]);
        } 
        if(need_add) {
            RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRCT, tombstone);
        }
    }
    RedisModule_DbSetDirty(ctx, argv[1]);
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
end:
    if(meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(meta.gid, ctx);
        freeIncrMeta(&meta);
    }
    if(moduleKey) {RedisModule_CloseKey(moduleKey);}
    return RedisModule_ReplyWithOk(ctx);    
    
}


typedef sds (*MSetExecFunc)(RedisModuleKey* mk, CrdtMeta* meta, sds val);
const char* crdt_mset_head = "$9\r\nCRDT.MSET\r\n";
const char* crdt_rc_mset_head = "$12\r\nCRDT.MSET_RC\r\n";

int replicationFeedCrdtMsetCommand(RedisModuleCtx* ctx, char* cmdbuf, char* head, MSetExecFunc exec, int len, RedisModuleKey** modulekeys, sds* keys, sds* values, CrdtMeta* mset_meta) {
    int cmdlen = 0;
    static size_t rc_mset_head_size = 0;
    if(rc_mset_head_size == 0) {
        rc_mset_head_size = strlen(crdt_rc_mset_head);
    }
    cmdlen += feedArgc(cmdbuf + cmdlen, len * 3 + 3);
    //will to change 
    cmdlen += feedBuf(cmdbuf + cmdlen , head, rc_mset_head_size);
    // cmdlen += feedBuf(cmdbuf + cmdlen , head);
    cmdlen += feedGid2Buf(cmdbuf + cmdlen, getMetaGid(mset_meta));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, getMetaTimestamp(mset_meta));
    for(int i = 0; i < len; i++) {
        CrdtMeta* m = dupMeta(mset_meta);
        sds value = exec(modulekeys[i], m, values[i]);
        cmdlen += feedStr2Buf(cmdbuf + cmdlen , keys[i], sdslen(keys[i]));
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, value, sdslen(value));
        cmdlen += feedVectorClock2Buf(cmdbuf + cmdlen, getMetaVectorClock(m));
        freeCrdtMeta(m);
        sdsfree(value);
        RedisModule_CloseKey(modulekeys[i]);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int msetGeneric(RedisModuleCtx* ctx, char* head, MSetExecFunc exec, int len, RedisModuleKey** modulekeys, sds* keys, sds* values, CrdtMeta* mset_meta, size_t size) {
    int alllen = strlen(head) + size + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + 8;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtMsetCommand(ctx, cmdbuf, head, exec, len , modulekeys, keys, values, mset_meta);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen];
        replicationFeedCrdtMsetCommand(ctx, cmdbuf, head, exec, len , modulekeys, keys, values, mset_meta);
    }
    return 1;
}





int max_del_counter_size = (21 + 17 + 3) * 16 ;
int replicationFeedCrdtMsetCommandByRc(RedisModuleCtx* ctx, char* cmdbuf, int len,  RedisModuleString** keys, void** crdt_vals, void** crdt_toms, sds* values, CrdtMeta* mset_meta) {
    int cmdlen = 0;
    static int crdt_rc_mset_head_str_len = 0;
    if(crdt_rc_mset_head_str_len == 0) {
        crdt_rc_mset_head_str_len = strlen(crdt_rc_mset_head);
    }
    cmdlen += feedArgc(cmdbuf + cmdlen, len * 3 + 3);
    //will to change 
    //cmdlen += feedBuf(cmdbuf + cmdlen , head, head_size);
    cmdlen += feedBuf(cmdbuf + cmdlen , crdt_rc_mset_head, crdt_rc_mset_head_str_len);
    cmdlen += feedGid2Buf(cmdbuf + cmdlen, getMetaGid(mset_meta));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, getMetaTimestamp(mset_meta));
    for(int i = 0; i < len; i++) {
        // CrdtMeta* m = dupMeta(mset_meta);
        CrdtMeta m = {.gid = getMetaGid(mset_meta), .timestamp = getMetaTimestamp(mset_meta),.vectorClock = dupVectorClock(getMetaVectorClock(mset_meta))};
        char value_buf[sdslen(values[i]) + max_del_counter_size];
        int value_len = add_rc_by_key(ctx, crdt_vals[i], crdt_toms[i], &m, keys[i], values[i], value_buf);
        sds key = RedisModule_GetSds(keys[i]);
        cmdlen += feedStr2Buf(cmdbuf + cmdlen , key, sdslen(key));
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, value_buf, value_len);
        cmdlen += feedVectorClock2Buf(cmdbuf + cmdlen, getMetaVectorClock(&m));
        freeVectorClock(getMetaVectorClock(&m));
        // sdsfree(value);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", keys[i]);
        RedisModule_DbSetDirty(ctx, keys[i]);
        RedisModule_SignalModifiedKey(ctx, keys[i]);
    }
    // RedisModule_Debug(logLevel, "cmd: %d - %s", cmdlen, cmdbuf);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int msetGenericByRc(RedisModuleCtx* ctx, int len, RedisModuleString** keys, void** crdt_vals, void* crdt_toms,  sds* values, CrdtMeta* mset_meta, size_t size) {
    int alllen = strlen(crdt_rc_mset_head) + size + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + 8;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtMsetCommandByRc(ctx, cmdbuf, len , keys, crdt_vals, crdt_toms, values, mset_meta);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen];
        replicationFeedCrdtMsetCommandByRc(ctx, cmdbuf, len , keys, crdt_vals, crdt_toms, values, mset_meta);
    }
    return 1;
}
/**
 *  only use  mset
 */ 
CRDT_Register* add_reg_by_key(RedisModuleCtx* ctx, void* val, void* tom, CrdtMeta* meta, RedisModuleString* key, sds value) {
    CRDT_Register* reg = val;
    if(reg == NULL) {
        reg = createCrdtRegister();
        long long vcll = RedisModule_CurrentVectorClock();
        CrdtMeta m = {.gid = getMetaGid(meta), .timestamp = getMetaTimestamp(meta), .vectorClock = LL2VC(vcll)};
        crdtRegisterSetValue(reg, &m, value);
        // RedisModule_ModuleTypeSetValue(moduleKey, getCrdtRegister(), reg);
        RedisModule_DbSetValue(ctx, key, getCrdtRegister(), reg);
    } else {
        crdtRegisterTryUpdate(reg, meta, value, COMPARE_META_VECTORCLOCK_GT);
        // appendVCForMeta(meta, getCrdtRegisterLastVc(reg));
    }
    return reg;
}

int replicationFeedCrdtMsetCommandByReg(RedisModuleCtx* ctx, char* cmdbuf, int len,  RedisModuleString** keys, void** crdt_vals, void** crdt_toms, sds* values, CrdtMeta* mset_meta) {
    int cmdlen = 0;
    static int crdt_mset_head_size = 0;
    if(crdt_mset_head_size == 0) {
        crdt_mset_head_size = strlen(crdt_mset_head);
    }
    cmdlen += feedArgc(cmdbuf + cmdlen, len * 3 + 3);
    //will to change 
    //cmdlen += feedBuf(cmdbuf + cmdlen , head, head_size);
    cmdlen += feedBuf(cmdbuf + cmdlen , crdt_mset_head, crdt_mset_head_size);
    cmdlen += feedGid2Buf(cmdbuf + cmdlen, getMetaGid(mset_meta));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, getMetaTimestamp(mset_meta));
    for(int i = 0; i < len; i++) {
        CRDT_Register* reg = add_reg_by_key(ctx, crdt_vals[i], crdt_toms[i], mset_meta, keys[i], values[i]);
        sds key = RedisModule_GetSds(keys[i]);
        cmdlen += feedStr2Buf(cmdbuf + cmdlen , key, sdslen(key));
        cmdlen += feedStr2Buf(cmdbuf + cmdlen, values[i], sdslen(values[i]));
        cmdlen += feedVectorClock2Buf(cmdbuf + cmdlen, getCrdtRegisterLastVc(reg));
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", keys[i]);
        RedisModule_DbSetDirty(ctx, keys[i]);
        RedisModule_SignalModifiedKey(ctx, keys[i]);
    }
    // RedisModule_Debug(logLevel, "cmd: %d - %s", cmdlen, cmdbuf);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}

int msetGenericByReg(RedisModuleCtx* ctx, int len, RedisModuleString** keys, void** crdt_vals, void* crdt_toms,  sds* values, CrdtMeta* mset_meta, size_t size) {
    // (char *)crdt_mset_head,
    int alllen = strlen(crdt_mset_head) + size + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN + 8;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtMsetCommandByReg(ctx, cmdbuf, len , keys, crdt_vals, crdt_toms, values, mset_meta);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen];
        replicationFeedCrdtMsetCommandByReg(ctx, cmdbuf, len , keys, crdt_vals, crdt_toms, values, mset_meta);
    }
    return 1;
}

#define TYPE_ERR -1
int check_type2(sds val, CrdtObject* crdt_val, CrdtTombstone* crdt_tom) {
    if(crdt_val != NULL) {
        if(isRegister(crdt_val)) {
            return CRDT_REGISTER_TYPE;
        } else if(isCrdtRc(crdt_val)) {
            return CRDT_RC_TYPE;
        } else {
            return TYPE_ERR;
        }
    }
    
    if(crdt_tom != NULL) {
        if(isRegisterTombstone(crdt_tom)) {
            return CRDT_REGISTER_TYPE;
        } else if(isCrdtRcTombstone(crdt_tom)) {
            return CRDT_RC_TYPE;
        } 
    } 
    ctrip_value v = {.type = VALUE_TYPE_SDS,.value.s = val};
    if(value_to_ld(&v)) {
        return CRDT_RC_TYPE;
    }
    if(value_to_ll(&v)) {
        return CRDT_RC_TYPE;
    }
    return CRDT_REGISTER_TYPE;
}
int msetGenericCommand2(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int is_nx) {
    if (argc % 2 != 1 || argc < 3) {
        RedisModule_WrongArity(ctx);
        return CRDT_ERROR;
    }
    int arraylen = (argc-1)/2;
    void* regs[arraylen];
    void* reg_toms[arraylen];
    int regs_len = 0;
    sds reg_vals[arraylen];
    RedisModuleString* reg_keys[arraylen];
    void* rcs[arraylen];
    void* rc_toms[arraylen];
    int rcs_len = 0;
    sds rc_vals[arraylen];
    RedisModuleString* rc_keys[arraylen];
    int budget_reg_key_val_strlen = 0;
    int budget_rc_key_val_strlen = 0;
    for(int i = 1; i < argc; i += 2) {
        sds key = RedisModule_GetSds(argv[i]);
        int need_add = 1;
        for(int j = i + 2; j < argc; j += 2) {
            sds other = RedisModule_GetSds(argv[j]);
            if(sdscmp(key, other) == 0) {
                need_add = 0;
            }
        }
        if(need_add == 1) {
            #if defined(MSET_STATISTICS)    
                get_modulekey_start();
            #endif
            // RedisModuleKey* moduleKey =  RedisModule_OpenKey(ctx, argv[i], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
            void* current = RedisModule_ModuleGetValue(ctx, argv[i]);
            if(current && is_nx) {
                RedisModule_ReplyWithLongLong(ctx, 0);
                return CRDT_NO;
            }
            void* tombstone = RedisModule_ModuleGetTombstone(ctx, argv[i]);
            sds val  = RedisModule_GetSds(argv[i+1]);
            int type = check_type2(val, current, tombstone);
            if (type == CRDT_REGISTER_TYPE) {
                reg_keys[regs_len] = argv[i];
                reg_vals[regs_len] = val;
                regs[regs_len] = current;
                reg_toms[regs_len++] = tombstone;
                budget_reg_key_val_strlen += sdslen(key) + sdslen(val) + REPLICATION_MAX_VC_LEN;
            } else if (type == CRDT_RC_TYPE) {
                rc_keys[rcs_len] = argv[i];
                rc_vals[rcs_len] = val;
                rcs[rcs_len] = current;
                rc_toms[rcs_len++] = tombstone;
                budget_rc_key_val_strlen += sdslen(key) + sdslen(val) + REPLICATION_MAX_VC_LEN;
            } else if (type == TYPE_ERR){
                RedisModule_ReplyWithError(ctx,"mset value type error");
                return CRDT_ERROR;
            }  
            #if defined(MSET_STATISTICS)    
                get_modulekey_end();
            #endif
        }
    }
    CrdtMeta mset_meta;
    initIncrMeta(&mset_meta);
    
    #if defined(MSET_STATISTICS)    
        write_bakclog_start(); 
    #endif
    if(rcs_len != 0) msetGenericByRc(ctx,  rcs_len, rc_keys, rcs, rc_toms,  rc_vals, &mset_meta, budget_rc_key_val_strlen);
    
    if(regs_len != 0) msetGenericByReg(ctx,  regs_len, reg_keys, regs, reg_toms,  reg_vals, &mset_meta, budget_reg_key_val_strlen);
    
    
    #if defined(MSET_STATISTICS)    
        write_backlog_end();
    #endif
    freeIncrMeta(&mset_meta);
    return CRDT_OK;
}

// int msetGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
//     if (argc % 2 != 1 || argc < 3) {
//         RedisModule_WrongArity(ctx);
//         return CRDT_ERROR;
//     }
//     int arraylen = (argc-1)/2;
//     RedisModuleKey* regs[arraylen];
//     int regs_len = 0;
//     sds reg_vals[arraylen];
//     sds reg_keys[arraylen];
//     RedisModuleKey* rcs[arraylen];
//     int rcs_len = 0;
//     sds rc_vals[arraylen];
//     sds rc_keys[arraylen];
//     int budget_reg_key_val_strlen = 0;
//     int budget_rc_key_val_strlen = 0;
//     for(int i = 1; i < argc; i += 2) {
//         sds key = RedisModule_GetSds(argv[i]);
//         int need_add = 1;
//         for(int j = i + 2; j < argc; j += 2) {
//             sds other = RedisModule_GetSds(argv[j]);
//             if(sdscmp(key, other) == 0) {
//                 need_add = 0;
//             }
//         }
//         if(need_add == 1) {
//             #if defined(MSET_STATISTICS)    
//                 get_modulekey_start();
//             #endif
//             RedisModuleKey* moduleKey =  RedisModule_OpenKey(ctx, argv[i], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
//             sds val  = RedisModule_GetSds(argv[i+1]);
//             int type = check_type(val, moduleKey);
//             if (type == CRDT_REGISTER_TYPE) {
//                 reg_keys[regs_len] = key;
//                 reg_vals[regs_len] = val;
//                 regs[regs_len++] = moduleKey;
//                 budget_reg_key_val_strlen += sdslen(key) + sdslen(val) + REPLICATION_MAX_VC_LEN;
//             } else if (type == CRDT_RC_TYPE) {
//                 rc_keys[rcs_len] = key;
//                 rc_vals[rcs_len] = val;
//                 rcs[rcs_len++] = moduleKey;
//                 budget_rc_key_val_strlen += sdslen(key) + sdslen(val) + REPLICATION_MAX_VC_LEN;
//             } else if (type == TYPE_ERR){
//                 RedisModule_ReplyWithError(ctx,"mset value type error");
//                 return CRDT_ERROR;
//             }  
//             #if defined(MSET_STATISTICS)    
//                 get_modulekey_end();
//             #endif
//         }
//     }
//     CrdtMeta mset_meta;
//     initIncrMeta(&mset_meta);
    
//     #if defined(MSET_STATISTICS)    
//         write_bakclog_start(); 
//     #endif
//     if(rcs_len != 0) msetGeneric(ctx, (char *)crdt_rc_mset_head, add_rc, rcs_len, rcs, rc_keys, rc_vals, &mset_meta, budget_rc_key_val_strlen);
    
//     if(regs_len != 0) msetGeneric(ctx, (char *)crdt_mset_head,  add_reg, regs_len, regs, reg_keys, reg_vals, &mset_meta, budget_reg_key_val_strlen);
    
    
//     #if defined(MSET_STATISTICS)    
//         write_backlog_end();
//     #endif
//     freeIncrMeta(&mset_meta);
//     return CRDT_OK;
// }

int msetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(msetGenericCommand2(ctx, argv, argc, 1) == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, 1);
    }
    return CRDT_ERROR;
}

int msetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // if(msetGenericCommand(ctx, argv, argc) == CRDT_OK) {
    //     return RedisModule_ReplyWithOk(ctx);
    // }
    if(msetGenericCommand2(ctx, argv, argc, 0) == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx);
    }
    return CRDT_ERROR;
}

int crdtGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
        RedisModuleString* result = RedisModule_CreateString(ctx, val, sdslen(val));
        RedisModule_ReplyWithString(ctx, result);
        RedisModule_FreeString(ctx, result);
        RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastGid(reg));
        RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastTimestamp(reg));
        sds vclockSds = vectorClockToSds(getCrdtRegisterLastVc(reg));
        RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
        sdsfree(vclockSds);
    } else if(rc) {
        //use crdt.datainfo
        ctrip_value value = {.type = VALUE_TYPE_NONE, .value.i = 0};
        crdtAssert(get_crdt_rc_value(rc, &value));
        RedisModule_ReplyWithArray(ctx, 2);
        VectorClock vc = getCrdtRcLastVc(rc);
        sds vc_str = vectorClockToSds(vc);
        RedisModule_ReplyWithStringBuffer(ctx, vc_str, sdslen(vc_str));
        sdsfree(vc_str);
        switch (value.type)
        {
        case VALUE_TYPE_LONGDOUBLE:
            RedisModule_ReplyWithLongDouble(ctx, value.value.f);
            break;
        case VALUE_TYPE_LONGLONG:
            RedisModule_ReplyWithLongLong(ctx, value.value.i);
            break;
        case VALUE_TYPE_SDS:
            RedisModule_ReplyWithStringBuffer(ctx, value.value.s, sdslen(value.value.s));
            break;
        default:
            RedisModule_ReplyWithStringBuffer(ctx, "UnknowType", 10);
            break;
        }
    }
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}


/******************    init command  -************************/
int initRcModule(RedisModuleCtx *ctx) {
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_APIVER_1,
        .rdb_load = RdbLoadCrdtRc, 
        .rdb_save = RdbSaveCrdtRc,
        .aof_rewrite = AofRewriteCrdtRc,
        .mem_usage = crdtRcMemUsageFunc,
        .free = freeCrdtRc,
        .digest = crdtRcDigestFunc,
        .lookup_swapping_clients = lookupSwappingClientsWk,
        .setup_swapping_clients = setupSwappingClientsWk,
        .get_data_swaps = getDataSwapsWk,
        .get_complement_swaps = getComplementSwapsWk,
        .swap_ana = swapAnaWk,
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
    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster swap-get|swap-put|swap-del|swap-nop
    if (RedisModule_CreateCommand(ctx,"SET",
                                  setCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.RC",
                                  crdtRcCommand, NULL, "write deny-oom allow-loading swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"GET",
                                  getCommand, NULL, "readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.GET",
                                  crdtGetCommand, NULL, "readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.del_rc",
                                  crdtDelRcCommand, NULL, "write allow-loading swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "SETEX", 
                                    setexCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "SETNX", 
                                    setnxCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "PSETEX", 
                                    psetexCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "incrby",
                                    incrbyCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "incrbyfloat",
                                    incrbyfloatCommand, NULL ,"write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "incr",
                                    incrCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "decr",
                                    decrCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "decrby",
                                    decrbyCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "crdt.counter",
                                    crdtCounterCommand, NULL, "write deny-oom allow-loading swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "MSET", 
                                    msetCommand, NULL, "write deny-oom swap-get",1,-1,2) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "MSETNX", 
                                    msetnxCommand, NULL, "write deny-oom swap-get",1,-1,2) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "MGET", 
                                    mgetCommand, NULL, "readonly fast swap-get",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "crdt.mset_rc", 
                                    crdtMsetRcCommand, NULL, "write deny-oom allow-loading swap-get",3,-1,3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
