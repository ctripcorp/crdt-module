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
#include "include/rmutil/dict.h"
#include "crdt_statistics.h"
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
int CRDT_MSETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
size_t crdtRegisterMemUsageFunc(const void *value);

CrdtObject** crdtRegisterFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* length);

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
    int compare = 0;
    CrdtObject* result = mergeRegister(currentVal, value, &compare);
    if(isConflictCommon(compare)) {
        RedisModule_IncrCrdtConflict(MERGECONFLICT | SET_CONFLICT);
    }
    return result;
}
CrdtObject** crdtRegisterFilter(CrdtObject* common, int gid, long long logic_time,long long maxsize, int* length) {
    return filterRegister(common, gid, logic_time, maxsize, length);
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
int crdtRegisterTombstonePurge(CrdtTombstone* tombstone, CrdtObject* current) {
    if(!isRegister((void*)current)) {
        return 0;
    }
    if(!isRegisterTombstone((void*)tombstone)) {
        return 0;
    }
    CRDT_Register* reg = (CRDT_Register*) current;
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*)tombstone;
    return purgeRegisterTombstone(t, reg);
}

CrdtTombstone* crdtRegisterTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other) {
    if(!isRegisterTombstone(target) || !isRegisterTombstone(other)) {
        return NULL;
    }
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*) target;
    int compare = 0;
    CrdtTombstone* result = mergeRegisterTombstone(t, (CRDT_RegisterTombstone*) other, &compare);
    if(isConflictCommon(compare)) RedisModule_IncrCrdtConflict(MERGECONFLICT | DEL_CONFLICT);
    return result;
}



CrdtObject** crdtRegisterTombstoneFilter(CrdtObject* target, int gid, long long logic_time, long long maxsize, int* length) {
    if(!isRegisterTombstone(target)) {
        return NULL;
    }
    CRDT_RegisterTombstone* t = (CRDT_RegisterTombstone*) target;
    return filterRegisterTombstone(t, gid, logic_time, maxsize, length);
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
    if (RedisModule_CreateCommand(ctx, "CRDT.MSET",
                                    CRDT_MSETCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
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
    CrdtMeta del_meta = {.gid = 0};
    initIncrMeta(&del_meta);
    appendVCForMeta(&del_meta, getCrdtRegisterLastVc(current));
    CRDT_RegisterTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isRegisterTombstone(tombstone)) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    int compare = 0;
    addRegisterTombstone(tombstone, &del_meta, &compare);
    if(isConflictCommon(compare)) RedisModule_IncrCrdtConflict(DEL_CONFLICT | MODIFYCONFLICT);
    sds vcSds = vectorClockToSds(getMetaVectorClock(&del_meta));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_REG", "sllc", keyRobj, getMetaGid(&del_meta), getMetaTimestamp(&del_meta), vcSds);
    sdsfree(vcSds);
    freeIncrMeta(&del_meta);
    return CRDT_OK;
}


//CRDT.DEL_REG <key> <gid> <timestamp> <vc>
//      0        1     2         3      4
//CRDT.DEL_REG <key> <gid> <timestamp> <vc> <expire-vc>
//      0        1     2         3      4       5
int CRDT_DelRegCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 5) return RedisModule_WrongArity(ctx);
    CrdtMeta del_meta = {.gid=0};
    if (readMeta(ctx, argv, 2, &del_meta) != CRDT_OK) return CRDT_ERROR;
    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }

    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_RegisterTombstone* tombstone = NULL;
    int compare = 0;
    if(t != NULL && isRegisterTombstone(t)) {
        tombstone = (CRDT_RegisterTombstone*)t;
        int compare = compareCrdtRegisterTombstone(tombstone, &del_meta);
        if(compare > COMPARE_META_EQUAL) {
            addRegisterTombstone(tombstone, &del_meta, &compare);
            goto end;
        } 
    }
    if(tombstone == NULL) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    
    addRegisterTombstone(tombstone, &del_meta, &compare);
    if(isConflictCommon(compare)) RedisModule_IncrCrdtConflict(DEL_CONFLICT | MODIFYCONFLICT);
    CRDT_Register* current = getCurrentValue(moduleKey);
    if(current != NULL) {
        if(isRegister(current) != CRDT_OK) {
            const char* keyStr = RedisModule_StringPtrLen(argv[1], NULL);
            RedisModule_Log(ctx, logLevel, "[TYPE CONFLICT][CRDT-Register][drop] key:{%s} ,prev: {%s} ",
                            keyStr ,current->type);
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | TYPECONFLICT);      
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);  
            status = CRDT_ERROR;
            goto end;
        }
        int result = compareTombstoneAndRegister(tombstone, current);
        if(isConflictCommon(result)) {
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | SET_DEL_CONFLICT); 
        }
        if(result > COMPARE_META_EQUAL) {
            addRegisterTombstone(tombstone, getCrdtRegisterLastMeta(current), &result);
            RedisModule_DeleteKey(moduleKey);
            deleted = 1;
        } else {
            appendCrdtMeta(getCrdtRegisterLastMeta(current), &del_meta);
            RedisModule_DeleteTombstone(moduleKey);
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


CRDT_Register* addOrUpdateRegister(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, CRDT_RegisterTombstone* tombstone, CRDT_Register* current, CrdtMeta* meta, RedisModuleString* key,sds value) {
    if(tombstone) {
        int result = compareCrdtRegisterTombstone(tombstone, meta);
        if(isConflictCommon(result)) {
            RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
        }
        if(result > COMPARE_META_EQUAL) {
            addRegisterTombstone(tombstone, meta, &result);
            return current;
        } else {
            appendCrdtMeta(meta, getCrdtRegisterTombstoneMeta(tombstone));
            RedisModule_DeleteTombstone(moduleKey);  
            tombstone = NULL;
        }
    }
    if(current == NULL) {
        current = createCrdtRegister();
        crdtRegisterSetValue(current, meta, value);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        //delete different tombstone
        RedisModule_DeleteTombstone(moduleKey);
    }else{
        if(!isRegister(current)) {
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][type conflict] {key: %s} prev: {%d}",
                            RedisModule_GetSds(key),getDataType(current));
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | TYPECONFLICT);
            return NULL;
        }
        int result = compareCrdtMeta(getCrdtRegisterLastMeta(current), meta);
        if(result == COMPARE_META_VECTORCLOCK_LT) { return current; }
        sds prev = NULL;
        int isConflict = isConflictCommon(result);
        if(isConflict == CRDT_YES) {
            prev = crdtRegisterInfo(current);
        }
        crdtRegisterTryUpdate(current, meta, value, result);
        if(isConflict == CRDT_YES) {
            sds income = crdtRegisterInfoFromMetaAndValue(meta, value);
            sds future = crdtRegisterInfo(current);
            if(result > COMPARE_META_EQUAL) {
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][replace] key:{%s} prev: {%s}, income: {%s}, future: {%s}",
                            RedisModule_GetSds(key), prev, income, future);
            }else{
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][drop] key:{%s} prev: {%s}, income: {%s}, future: {%s}",
                            RedisModule_GetSds(key), prev, income, future);
            }
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | SET_CONFLICT);
            sdsfree(income);
            sdsfree(future);
            sdsfree(prev);
        }
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

const char* crdt_mset_head = "$9\r\nCRDT.MSET\r\n";
const size_t crdt_mset_basic_str_len = REPLICATION_ARGC_LEN + 15 + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN;
int replicationFeedCrdtMSetCommand(RedisModuleCtx *ctx, RedisModuleString** argv, char *cmdbuf, CrdtMeta* mset_meta, int argc, CRDT_Register** vals, const char**datas, size_t* datalens) {
    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, argc * 3  + 3);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_mset_head);
    cmdlen += feedGid2Buf(cmdbuf+ cmdlen, getMetaGid(mset_meta));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, getMetaTimestamp(mset_meta));
    for(int i = 0, len = argc; i < len; i+=1) {
        CRDT_Register* current = vals[i];
        RedisModuleString* val = argv[i * 2 + 2];
        RedisModuleString* key = argv[i * 2 + 1];
        if(current == NULL) {
            #if defined(MSET_STATISTICS) 
                add_val_start();
            #endif
            current = createCrdtRegister();
            crdtRegisterSetValue(current, mset_meta, RedisModule_GetSds(val));
            RedisModule_DbSetValue(ctx, key, CrdtRegister, current);
            #if defined(MSET_STATISTICS) 
                add_val_end();
            #endif
        } else {
            #if defined(MSET_STATISTICS) 
                update_val_start();
            #endif
            crdtRegisterTryUpdate(current, mset_meta, RedisModule_GetSds(val), COMPARE_META_VECTORCLOCK_GT);
            #if defined(MSET_STATISTICS) 
                update_val_end();
            #endif
        }
        #if defined(MSET_STATISTICS) 
            send_event_start();
        #endif
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
        #if defined(MSET_STATISTICS) 
            send_event_end();
        #endif
        cmdlen += feedKV2Buf(cmdbuf+ cmdlen, datas[2*i], datalens[2*i], datas[2*i+1], datalens[2*i+1]);
        cmdlen += feedVectorClock2Buf(cmdbuf+ cmdlen, getCrdtRegisterLastVc(current));
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int CRDT_MSETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    if (argc % 3 != 0) return RedisModule_WrongArity(ctx);
    long long gid;
    if ((redisModuleStringToGid(ctx, argv[1],&gid) != REDISMODULE_OK)) {
        return CRDT_ERROR;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[2],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return CRDT_ERROR;
    }
    int result = 0;
    for(int i = 3; i< argc; i+=3) {
        RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[i], CrdtRegister);
        if (moduleKey == NULL) {
            RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
            continue;
        }
        CrdtTombstone* tombstone = getTombstone(moduleKey);
        if (tombstone != NULL && !isRegisterTombstone(tombstone)) {
            tombstone = NULL;
        }
        CRDT_Register* current = getCurrentValue(moduleKey);
        VectorClock vclock = getVectorClockFromString(argv[i+2]);
        CrdtMeta meta;
        meta.gid = gid;
        meta.timestamp = timestamp;
        meta.vectorClock = vclock;
        current = addOrUpdateRegister(ctx, moduleKey, tombstone, current, &meta, argv[1], RedisModule_GetSds(argv[i+1]));
        RedisModule_MergeVectorClock(gid, VC2LL(meta.vectorClock));
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
        RedisModule_CloseKey(moduleKey);
        freeVectorClock(meta.vectorClock);
        result++;
    }
    RedisModule_CrdtReplicateVerbatim(gid, ctx);
    return RedisModule_ReplyWithLongLong(ctx, result); 
}

//CRDT.MSET <gid> <time> {k v vc} ...
int msetGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    int arraylen = (argc-1)/2;
    int index = 0;
    CRDT_Register* vals[arraylen];

    const char* keyOrValStr[arraylen*2];
    size_t keyOrValStrLen[arraylen*2];
    int budget_key_val_strlen = 0;
    for (int i = 1; i < argc; i+=2) {
        int error = 0;
        #if defined(MSET_STATISTICS)    
            get_modulekey_start();
        #endif
        vals[index++] = RedisModule_DbGetValue(ctx, argv[i], CrdtRegister, &error);
        if(error != 0) {
            RedisModule_ReplyWithError(ctx,"mset value type error");
            return CRDT_ERROR;
        }
        #if defined(MSET_STATISTICS)    
            get_modulekey_end();
        #endif
        size_t keylen = 0;
        keyOrValStr[i-1] = RedisModule_StringPtrLen(argv[i], &keylen);
        keyOrValStrLen[i-1] = keylen;
        size_t vallen = 0;
        keyOrValStr[i] = RedisModule_StringPtrLen(argv[i+1], &vallen);
        keyOrValStrLen[i] = vallen;
        budget_key_val_strlen += keylen + vallen + 2 * REPLICATION_MAX_STR_LEN + REPLICATION_MAX_VC_LEN;
    }
    CrdtMeta mset_meta;
    initIncrMeta(&mset_meta);
    long long vc = RedisModule_CurrentVectorClock();
    appendVCForMeta(&mset_meta, LL2VC(vc));
    #if defined(MSET_STATISTICS)    
        write_bakclog_start();
    #endif
    size_t alllen = crdt_mset_basic_str_len + budget_key_val_strlen;
    if(alllen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(alllen);
        replicationFeedCrdtMSetCommand(ctx, argv, cmdbuf, &mset_meta, arraylen, vals, keyOrValStr, keyOrValStrLen);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[alllen]; 
        replicationFeedCrdtMSetCommand(ctx, argv, cmdbuf, &mset_meta, arraylen, vals, keyOrValStr,keyOrValStrLen);
    }
    #if defined(MSET_STATISTICS)    
        write_backlog_end();
    #endif
    freeIncrMeta(&mset_meta);
    return CRDT_OK;
}
int setGenericCommand(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, int flags, RedisModuleString* key, RedisModuleString* val, RedisModuleString* expire, int unit, int sendtype) {
    int result = 0;
#if defined(SET_STATISTICS)    
    get_modulekey_start();
#endif
    if(moduleKey == NULL) moduleKey = getWriteRedisModuleKey(ctx, key, CrdtRegister);
    CrdtMeta set_meta;
    if(moduleKey == NULL) {
        result = 0;
        goto error;
    }
    
    CRDT_Register* current = getCurrentValue(moduleKey);
    if((current != NULL && flags & OBJ_SET_NX) 
        || (current == NULL && flags & OBJ_SET_XX)) {
        if(sendtype) RedisModule_ReplyWithNull(ctx);  
        result = 0;   
        goto error;
    }
#if defined(SET_STATISTICS)   
    get_modulekey_end();
#endif
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
    if (tombstone != NULL && !isRegisterTombstone(tombstone)) {
        tombstone = NULL;
    }
    
    if(current == NULL) {
        #if defined(SET_STATISTICS) 
            add_val_start();
        #endif
        current = createCrdtRegister();
        if(tombstone) {
            appendVCForMeta(&set_meta, getCrdtRegisterTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            appendVCForMeta(&set_meta, LL2VC(vc));
        }
        crdtRegisterSetValue(current, &set_meta, RedisModule_GetSds(val));
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        #if defined(SET_STATISTICS) 
            add_val_end();
        #endif
    } else {
        #if defined(SET_STATISTICS) 
            update_val_start();
        #endif
        crdtRegisterTryUpdate(current, &set_meta, RedisModule_GetSds(val), COMPARE_META_VECTORCLOCK_GT);
        #if defined(SET_STATISTICS) 
            update_val_end();
        #endif
    }
    RedisModule_DeleteTombstone(moduleKey);
    long long expire_time = -2;
    if(expire) {
        expire_time = getMetaTimestamp(&set_meta) + milliseconds;
        RedisModule_SetExpire(moduleKey, milliseconds);
    }else if(!(flags & OBJ_SET_KEEPTTL)){
        RedisModule_SetExpire(moduleKey, -1);
        expire_time = -1;
    }
    #if defined(SET_STATISTICS) 
        send_event_start();
    #endif
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
    if(expire) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "expire", key);
    }
    #if defined(SET_STATISTICS) 
        send_event_end();
    #endif
    result = CRDT_OK;
        {
            #if defined(SET_STATISTICS) 
                write_bakclog_start();
            #endif
            size_t keylen = 0;
            const char* keystr = RedisModule_StringPtrLen(key, &keylen);
            size_t vallen = 0;
            const char* valstr = RedisModule_StringPtrLen(val, &vallen);
            size_t alllen = keylen + vallen + crdt_set_basic_str_len;
            if(alllen > MAXSTACKSIZE) {
                char* cmdbuf = RedisModule_Alloc(alllen);
                replicationFeedCrdtSetCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,&set_meta, getCrdtRegisterLastVc(current), expire_time);
                RedisModule_Free(cmdbuf);
            } else {
                char cmdbuf[alllen]; 
                replicationFeedCrdtSetCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,&set_meta, getCrdtRegisterLastVc(current), expire_time);
            }
            
            freeIncrMeta(&set_meta);
            #if defined(SET_STATISTICS) 
                write_backlog_end();
            #endif
        }
    
error:
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return result;
}
int msetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    if (argc % 2 != 1) return RedisModule_WrongArity(ctx);
    if(msetGenericCommand(ctx, argv, argc) == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx);
    }
    return CRDT_ERROR;
}
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
    #if defined(SET_STATISTICS) 
        parse_end();
    #endif
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

    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 3, &meta) != CRDT_OK) {
        return 0;
    }
    //if key is null will be create one key
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtRegister);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    
    CrdtTombstone* tombstone = getTombstone(moduleKey);
    if (tombstone != NULL && !isRegisterTombstone(tombstone)) {
        tombstone = NULL;
    }
    CRDT_Register* current = getCurrentValue(moduleKey);
    
    current = addOrUpdateRegister(ctx, moduleKey, tombstone, current, &meta, argv[1], RedisModule_GetSds(argv[2]));
    if(expire_time != -2) {
        trySetExpire(moduleKey, argv[1], getMetaTimestamp(&meta),  CRDT_REGISTER_TYPE, expire_time);
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
        if(compareCrdtRegisterTombstone(tombstone, meta) > COMPARE_META_EQUAL) {
            return NULL;
        }
    }
    CRDT_Register* r = createCrdtRegister();
    crdtRegisterSetValue(r, meta, value);
    return r;
}

void freeRegisterFilter(CrdtObject** filters, int num) {
    RedisModule_ZFree(filters);
}
void freeRegisterTombstoneFilter(CrdtObject** filters, int num) {
    RedisModule_ZFree(filters);
}