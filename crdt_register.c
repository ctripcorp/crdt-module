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
#include <time.h>
RedisModuleString* crdt_set_shared; //crdt.set
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
int statisticsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CRDT_MSETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
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
    // if(current == NULL) {
    //     return dupCrdtRegister(v);
    // }
    // if(v == NULL) {
    //     return dupCrdtRegister(current);
    // }
    int compare = 0;
    CrdtObject* result = mergeRegister(currentVal, value, &compare);
    if(isConflictCommon(compare)) {
        RedisModule_IncrCrdtConflict(MERGECONFLICT | NONTYPECONFLICT);
    }
    return result;
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
    int compare = 0;
    CrdtTombstone* result = mergeRegisterTombstone(t, (CRDT_RegisterTombstone*) other, &compare);
    if(isConflictCommon(compare)) RedisModule_IncrCrdtConflict(MERGECONFLICT | NONTYPECONFLICT);
    return result;
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
    crdt_set_shared = RedisModule_CreateString(ctx, "CRDT.SET", 8);
    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
    if (RedisModule_CreateCommand(ctx,"SET",
                                  setCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"statistics",
                                  statisticsCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
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
    CrdtMeta* meta = createIncrMeta();
    CrdtMeta* del_meta = dupMeta(meta);
    appendVCForMeta(del_meta, getCrdtRegisterLastVc(current));
    CRDT_RegisterTombstone *tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isRegisterTombstone(tombstone)) {
        tombstone = createCrdtRegisterTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtRegisterTombstone, tombstone);
    }
    int compare = 0;
    addRegisterTombstone(tombstone, del_meta, &compare);
    if(isConflictCommon(compare)) RedisModule_IncrCrdtConflict(NONTYPECONFLICT | MODIFYCONFLICT);
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
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
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
    int compare = 0;
    addRegisterTombstone(tombstone, del_meta, &compare);
    if(isConflictCommon(compare)) RedisModule_IncrCrdtConflict(NONTYPECONFLICT | MODIFYCONFLICT);
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
    if(isExpireCrdtTombstone(tombstone, meta) == CRDT_OK) {
        return current;
    }
    if(current == NULL) {
        current = createCrdtRegister();
        setCrdtRegister(current, meta, value);
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
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
        appendCrdtRegister(current, meta, value, result);
        if(isConflict == CRDT_YES) {
            sds income = crdtRegisterInfoFromMetaAndValue(meta, value);
            sds future = crdtRegisterInfo(current);
            if(result > COMPARE_META_EQUAL) {
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][replace] key:{%s} prev: {%s}, income: {%s}, future: {%s}",
                            RedisModule_GetSds(key), prev, income, future);
            }else{
                RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][drop] prev: {%s}, income: {%s}, future: {%s}",
                            RedisModule_GetSds(key), prev, income, future);
            }
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | NONTYPECONFLICT);
            sdsfree(income);
            sdsfree(future);
            sdsfree(prev);
        }
        // sds prev = crdtRegisterInfo(current);
        //tryUpdateRegister function will be change "current" object
        // int result = tryUpdateRegister(tombstone, meta, current, value);
        // tryUpdateRegister(tombstone, meta, current, value);
        // if(isConflictCommon(result) == CRDT_YES) {
        //     CRDT_Register* incomeValue = addRegister(NULL, meta, value);
        //     sds income = crdtRegisterInfo(incomeValue);
        //     sds future = crdtRegisterInfo(current);
        //     if(result > COMPARE_META_EQUAL) {
        //         RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][replace] key:{%s} prev: {%s}, income: {%s}, future: {%s}",
        //                     RedisModule_GetSds(key), prev, income, future);
        //     }else{
        //         RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][drop] prev: {%s}, income: {%s}, future: {%s}",
        //                     RedisModule_GetSds(key), prev, income, future);
        //     }
        //     freeCrdtRegister(incomeValue);
        //     sdsfree(income);
        //     sdsfree(future);
        //      RedisModule_IncrCrdtConflict(MODIFYCONFLICT | NONTYPECONFLICT);
        // }
        // sdsfree(prev);
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

const char* crdt_set_command_template = "*7\r\n$8\r\nCRDT.SET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n$%d\r\n%lld\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n";
const char* crdt_set_command_no_expire_template = "*6\r\n$8\r\nCRDT.SET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n$%d\r\n%lld\r\n$%d\r\n%s\r\n";
const char* crdt_set_head = "*7\r\n$8\r\nCRDT.SET\r\n";
const char* crdt_set_no_expire_head = "*6\r\n$8\r\nCRDT.SET\r\n";
size_t replicationFeedCrdtSetCommand(RedisModuleCtx *ctx,char* cmdbuf, char* keystr, size_t keylen, char* valstr, size_t vallen, CrdtMeta* meta, VectorClock vc, long long expire_time) {
    size_t cmdlen = 0;
    if(expire_time > -2) {
        cmdlen =  feedBuf(cmdbuf, crdt_set_head);
    }else{
        cmdlen = feedBuf(cmdbuf, crdt_set_no_expire_head);
    }
    //18
    // cmdlen += feedStr2Buf(cmdbuf + cmdlen, keystr, keylen);//$%d\r\n%s\r\n
    // cmdlen += feedStr2Buf(cmdbuf + cmdlen, valstr, vallen);//$%d\r\n%s\r\n
    cmdlen += feedKV2Buf(cmdbuf + cmdlen, keystr, keylen, valstr, vallen);
    // cmdlen += feedGid2Buf(cmdbuf + cmdlen, getMetaGid(&set_meta));//$%d\r\n%d\r\n
    // cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, getMetaTimestamp(&set_meta));//$%d\r\nlld\r\n
    // cmdlen += feedVectorClock2Buf(cmdbuf + cmdlen, getCrdtRegisterLastVc(current));//$%d\r\n%s\r\n
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), vc);
    if(expire_time > -2) {
        cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, expire_time);
    }
    // RedisModule_Debug(logLevel, "len:%d buf:%s", cmdlen, cmdbuf);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
}
unsigned long long statistics_parse_set_time = 0;
unsigned long long statistics_parse_set_num = 0;
unsigned long long statistics_modulekey_time = 0;
unsigned long long statistics_modulekey_num = 0;
unsigned long long statistics_add_val_time = 0;
unsigned long long statistics_add_val_num = 0;
unsigned long long statistics_merge_val_time = 0;
unsigned long long statistics_merge_val_num = 0;
unsigned long long statistics_set_expire_time = 0;
unsigned long long statistics_set_expire_num = 0;
unsigned long long statistics_send_event_time = 0;
unsigned long long statistics_send_event_num = 0;
unsigned long long statistics_save_backlog_time = 0;
unsigned long long statistics_save_backlog_num = 0;

int statisticsCommand(RedisModuleCtx *ctx,  RedisModuleString **argv, int argc) {
    char infobuf[999]; 
    size_t infolen = sprintf(infobuf, 
        "parse: %lld\r\n"
        "modulekey: %lld\r\n"
        "add_val: %lld\r\n"
        "merge_val: %lld\r\n"
        "set_expire: %lld\r\n"
        "send_event: %lld\r\n"
        "save_backlog: %lld\r\n",
        statistics_parse_set_num == 0? 0: statistics_parse_set_time/statistics_parse_set_num,
        statistics_modulekey_num == 0? 0: statistics_modulekey_time/statistics_modulekey_num,
        statistics_add_val_num == 0? 0: statistics_add_val_time/statistics_add_val_num,
        statistics_merge_val_num == 0? 0: statistics_merge_val_time/statistics_merge_val_num,
        statistics_set_expire_num == 0? 0: statistics_set_expire_time/statistics_set_expire_num,
        statistics_send_event_num == 0? 0: statistics_send_event_time/statistics_send_event_num,
        statistics_save_backlog_num == 0? 0: statistics_save_backlog_time/statistics_save_backlog_num
    );
    infobuf[infolen] = '\0';
    RedisModule_ReplyWithStringBuffer(ctx, infobuf, infolen);
    return REDISMODULE_OK;
}
unsigned long long nstime(void) {
   struct timespec ts;
   clock_gettime(CLOCK_REALTIME, &ts);
   return ts.tv_sec*1000000000+ts.tv_nsec;
}
const char* crdt_mset_head = "$9\r\nCRDT.MSET\r\n";
int replicationFeedCrdtMSetCommand(RedisModuleCtx *ctx, char *cmdbuf, CrdtMeta* mset_meta, int argc, char**datas, size_t* datalens, VectorClock* vcs) {
    size_t cmdlen = 0;
    cmdlen += feedArgc(cmdbuf + cmdlen, argc * 3  + 3);
    cmdlen += feedBuf(cmdbuf + cmdlen, crdt_mset_head);
    cmdlen += feedGid2Buf(cmdbuf+ cmdlen, getMetaGid(mset_meta));
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, getMetaTimestamp(mset_meta));
    for(int i = 0, len = argc; i < len; i+=1) {
        cmdlen += feedKV2Buf(cmdbuf+ cmdlen, datas[2*i], datalens[2*i], datas[2*i+1], datalens[2*i+1]);
        cmdlen += feedVectorClock2Buf(cmdbuf+ cmdlen, vcs[i]);
    }
    // RedisModule_Debug(logLevel, "len:%d buf:%s", cmdlen, cmdbuf);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
int CRDT_MSETCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    if (argc % 3 != 0) return RedisModule_WrongArity(ctx);
    long long gid;
    if ((redisModuleStringToGid(ctx, argv[1],&gid) != REDISMODULE_OK)) {
        return NULL;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[2],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return NULL;
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
        RedisModule_MergeVectorClock(gid, *(long long*)(&vclock));
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", argv[1]);
        RedisModule_CloseKey(moduleKey);
        freeVectorClock(vclock);
        result++;
    }
    
    if (gid == RedisModule_CurrentGid()) {
        RedisModule_CrdtReplicateVerbatim(ctx);
    } else {
        RedisModule_ReplicateVerbatim(ctx);
    }
    return RedisModule_ReplyWithLongLong(ctx, result); 
}
//CRDT.MSET <gid> <time> {k v vc} ...
//mset {k v}...
int msetGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    int result = 0;
    int arraylen = (argc-1)/2;
    RedisModuleKey* modulekeys[arraylen];
    char* datas[arraylen * 2];
    char* datalens[arraylen * 2];
    VectorClock vcs[arraylen];
    int index = 0;
    for (int i = 1; i < argc; i+=2) {
        RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[i], CrdtRegister);
        if(moduleKey == NULL) {
            goto error;
        }
        modulekeys[index++] = moduleKey;
    }
    CrdtMeta mset_meta;
    initIncrMeta(&mset_meta);
    
    size_t budget_key_val_strlen = 0;
    for(int i =0 ;i< arraylen; i++) {
        RedisModuleKey* moduleKey = modulekeys[i];
        CRDT_Register* current = getCurrentValue(moduleKey);
        RedisModuleString* key = argv[i * 2 + 1];
        RedisModuleString* val = argv[i * 2 + 2];
        size_t keylen = 0;
        datas[2*i] = RedisModule_StringPtrLen(key, &keylen);
        datalens[2*i] = keylen;
        size_t vallen = 0;
        datas[2*i+1] = RedisModule_StringPtrLen(val, &vallen);
        datalens[2*i+1] = vallen;
        //1($) + 21(long long) + 2(\r\n) + keylen/vallen + 2(\r\n)
        budget_key_val_strlen += keylen + vallen + 54;
        if(current == NULL) {
            current = createCrdtRegister();
            setCrdtRegister(current, &mset_meta, RedisModule_GetSds(val));
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        } else {
            appendCrdtRegister(current, &mset_meta, RedisModule_GetSds(val), COMPARE_META_VECTORCLOCK_GT);
        }
        vcs[i] = getCrdtRegisterLastVc(current);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
    }

    
    if(budget_key_val_strlen > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(523 + budget_key_val_strlen);
        replicationFeedCrdtMSetCommand(ctx, cmdbuf, &mset_meta, arraylen, datas,datalens, vcs);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[523 + budget_key_val_strlen]; //4 + (4 + 8 + 2) + (24  + keylen + 2 ) + (24  + vallen + 2 ) + (10 + 23) + (5 + 7) + (10 + 23) + (6 + vcunitlen * 25 + 2)
        replicationFeedCrdtMSetCommand(ctx, cmdbuf, &mset_meta, arraylen, datas,datalens, vcs);
    }
    
    freeIncrMeta(&mset_meta);
    
    result = CRDT_OK;
error:
    for(int j = 0; j < index; j++) {
        RedisModule_CloseKey(modulekeys[j]);
    }
    // RedisModule_Debug(logLevel,"setGenericCommand %lld", RedisModule_ZmallocNum() - num);
    return result;
}
int setGenericCommand(RedisModuleCtx *ctx, RedisModuleKey* moduleKey, int flags, RedisModuleString* key, RedisModuleString* val, RedisModuleString* expire, int unit, int sendtype) {
    // size_t num = RedisModule_ZmallocNum();
    int result = 0;
#if defined(STATISTICS)    
    unsigned long long modulekey_start = nstime();
#endif
    if(moduleKey == NULL) moduleKey = getWriteRedisModuleKey(ctx, key, CrdtRegister);
    // CrdtMeta* set_meta = NULL;
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
#if defined(STATISTICS)   
    statistics_modulekey_time += nstime() - modulekey_start;
    statistics_modulekey_num++;
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
    //current = addOrUpdateRegister(ctx, moduleKey, NULL, current, &set_meta, key, RedisModule_GetSds(val));
    if(current == NULL) {
        #if defined(STATISTICS) 
            unsigned long long set_add_start = nstime();
        #endif
        current = createCrdtRegister();
        setCrdtRegister(current, &set_meta, RedisModule_GetSds(val));
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtRegister, current);
        #if defined(STATISTICS) 
            statistics_add_val_time += nstime() - set_add_start;
            statistics_add_val_num++;
        #endif
    } else {
        #if defined(STATISTICS) 
            unsigned long long merge_val_start = nstime();
        #endif
        if(!isRegister(current)) {
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-Register][type conflict] {key: %s} prev: {%d}",
                            RedisModule_GetSds(key),getDataType(current));
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | TYPECONFLICT);
            result = 0;
            if(sendtype) RedisModule_ReplyWithSimpleString(ctx,"set error\r\n");
            goto end;
        }
        // appendVCForMeta(&set_meta, getCrdtRegisterLastVc(current));
        appendCrdtRegister(current, &set_meta, RedisModule_GetSds(val), COMPARE_META_VECTORCLOCK_GT);
        #if defined(STATISTICS) 
            statistics_merge_val_time += nstime() - merge_val_start;
            statistics_merge_val_num++;
        #endif
    }
    #if defined(STATISTICS) 
        unsigned long long set_expire_start = nstime();
    #endif
    long long expire_time = -2;
    if(expire) {
        expire_time = getMetaTimestamp(&set_meta) + milliseconds;
        RedisModule_SetExpire(moduleKey, milliseconds);
    }else if(!(flags & OBJ_SET_KEEPTTL)){
        RedisModule_SetExpire(moduleKey, -1);
        expire_time = -1;
    }
    #if defined(STATISTICS) 
        statistics_set_expire_time += nstime() - set_expire_start;
        statistics_set_expire_num++;
        unsigned long long send_event_start = nstime();
    #endif
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_STRING, "set", key);
    if(expire) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "expire", key);
    }
    #if defined(STATISTICS) 
        statistics_send_event_time += nstime() - send_event_start;
        statistics_send_event_num++;
    #endif
    result = CRDT_OK;
end:
        
        // {
        //     #if defined(STATISTICS) 
        //         unsigned long long statistics_save_backlog_start = nstime();
        //     #endif
        //     VectorClock setvc = getCrdtRegisterLastVc(current);
        //     int vcunitlen = get_len(setvc);
        //     char vcbuf[vcunitlen * 25]; //21 long long + 2 gid + 1 : + 1 ;) 
        //     size_t vclen = vectorClockToString(vcbuf, setvc);
        //     size_t keylen = 0;
        //     char* k = RedisModule_StringPtrLen(key, &keylen);
        //     size_t vallen = 0;
        //     char* v = RedisModule_StringPtrLen(val, &vallen);
        //     int gid = getMetaGid(&set_meta);
        //     long long timestamp = getMetaTimestamp(&set_meta);
        //     char timestampbuf[21];
        //     size_t tlen = sdsll2str(timestampbuf, timestamp);
        //     // size_t vclen = sdslen(vclockStr);
        //     char cmdbuf[123 + vcunitlen * 25 + keylen + vallen]; //4 + (4 + 8 + 2) + (24  + keylen + 2 ) + (24  + vallen + 2 ) + (10 + 23) + (5 + 7) + (10 + 23) + (6 + vcunitlen * 25 + 2)
        //     size_t cmdlen = 0;
        //     if(expire_time > -2) {
        //         char expirebuf[21];
        //         size_t expirelen = sdsll2str(expirebuf, expire_time);
        //         cmdlen = sprintf(cmdbuf, crdt_set_command_template, keylen, k, vallen, v, gid > 10? 2:1 ,gid, tlen,timestamp,vclen, vcbuf, expirelen, expirebuf);
        //     }else{
        //         //RedisModuleString* cmd = RedisModule_CreateStringPrintf(ctx, "*6\r\n$8\r\nCRDT.SET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n$%d\r\n%lld\r\n$%d\r\n%s\r\n", keylen, k, vallen, v, gid > 10? 2:1 ,gid, tlen,timestamp,vclen, vclockStr);
        //         cmdlen = sprintf(cmdbuf, crdt_set_command_no_expire_template, keylen, k, vallen, v, gid > 10? 2:1 ,gid, tlen,timestamp,vclen, vcbuf);
        //     }
        //     #if defined(STATISTICS) 
        //         statistics_save_backlog_time += nstime() - statistics_save_backlog_start;
        //         statistics_save_backlog_num++;
        //     #endif
        //     RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
        //     freeIncrMeta(&set_meta);
            
        // }
        {
            #if defined(STATISTICS) 
                unsigned long long statistics_save_backlog_start = nstime();
            #endif
            size_t keylen = 0;
            const char* keystr = RedisModule_StringPtrLen(key, &keylen);
            size_t vallen = 0;
            const char* valstr = RedisModule_StringPtrLen(val, &vallen);
            if(keylen + vallen > MAXSTACKSIZE) {
                char* cmdbuf = RedisModule_Alloc(523 + keylen + vallen);
                replicationFeedCrdtSetCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,&set_meta, getCrdtRegisterLastVc(current), expire_time);
                RedisModule_Free(cmdbuf);
            } else {
                char cmdbuf[523 + keylen + vallen]; //4 + (4 + 8 + 2) + (24  + keylen + 2 ) + (24  + vallen + 2 ) + (10 + 23) + (5 + 7) + (10 + 23) + (6 + vcunitlen * 25 + 2)
                replicationFeedCrdtSetCommand(ctx, cmdbuf, keystr, keylen, valstr, vallen,&set_meta, getCrdtRegisterLastVc(current), expire_time);
            }
            
            freeIncrMeta(&set_meta);
            #if defined(STATISTICS) 
                statistics_save_backlog_time += nstime() - statistics_save_backlog_start;
                statistics_save_backlog_num++;
            #endif
        }
    
error:
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    // RedisModule_Debug(logLevel,"setGenericCommand %lld", RedisModule_ZmallocNum() - num);
    return result;
}
int msetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    if (argc % 2 != 1) return RedisModule_WrongArity(ctx);
    msetGenericCommand(ctx, argv, argc);
    return RedisModule_ReplyWithOk(ctx);
}
int _msetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    if (argc % 2 != 1) return RedisModule_WrongArity(ctx);
    int start_index = 1;
    int result = 0;
    RedisModuleKey* rs[(argc-1)/2];
    int index = 0;
    for (int i = start_index; i < argc; i+=2) {
        RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[i], CrdtRegister);
        if(moduleKey == NULL) {
            for(int j = 0; j < index; j++) {
                RedisModule_CloseKey(rs[j]);
            }
            return 0;
        }
        rs[index++] = moduleKey;
    }
    for (int i = 0; i < index; i++) {
        result += setGenericCommand(ctx, rs[i], OBJ_SET_NO_FLAGS, argv[2*i + 1], argv[2*i + 2], NULL, UNIT_SECONDS, 1);
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
int setCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    RedisModuleString* expire = NULL;
    int flags = OBJ_SET_NO_FLAGS;
    int j;
    int unit = UNIT_SECONDS;
    #if defined(STATISTICS) 
        unsigned long long statistics_parse_set_start = nstime(); 
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
    #if defined(STATISTICS) 
        statistics_parse_set_time += nstime() - statistics_parse_set_start;
        statistics_parse_set_num++;
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
    // RedisModule_AutoMemory(ctx);
    if(argc < 4) return RedisModule_WrongArity(ctx);
    int result = setGenericCommand(ctx, NULL, OBJ_SET_NO_FLAGS | OBJ_SET_EX, argv[1], argv[3], argv[2], UNIT_SECONDS, 1);
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
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
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
    return appendCrdtRegister(reg, meta, value, COMPARE_META_GID_LT - 1);
}
void updateRegister(void* data, CrdtMeta* meta, CRDT_Register* reg, sds value, int compare) {
    CRDT_RegisterTombstone* tombstone = (CRDT_RegisterTombstone*) data;
    if(tombstone != NULL) {
        if(isExpireCrdtTombstone(tombstone, meta) == CRDT_OK) {
            return;
        }
    }
    setCrdtRegister(reg, meta, value);
}

