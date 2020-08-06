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
// Created by zhuchen on 2019/9/4.
//
#include "ctrip_crdt_hashmap.h"
#include "crdt_register.h"
#include "utils.h"
#include "crdt.h"
#include "crdt_statistics.h"
/*
    util 
*/
int isCrdtHash(void* data) {
    CRDT_Hash* hash = (CRDT_Hash*)data;
    if(hash != NULL && (getDataType((CrdtObject*)hash) == CRDT_HASH_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
int isCrdtHashTombstone(void *data) {
    CRDT_HashTombstone* tombstone = (CRDT_HashTombstone*)data;
    if(tombstone != NULL && (getDataType((CrdtObject*)tombstone) ==  CRDT_HASH_TYPE)) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
CRDT_Hash* retrieveCrdtHash(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_Hash* result = (CRDT_Hash*)t;
    assert(result->map != NULL);
    return result;
}



CRDT_HashTombstone* retrieveCrdtHashTombstone(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_HashTombstone* result = (CRDT_HashTombstone*)t;
    assert(result->map != NULL);
    return result;
}

int crdtHtNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

#define CHANGE_HASH_ERR -1
#define NO_CHANGE_HASH 0
#define ADD_HASH 1
#define UPDATE_HASH 2
int addOrUpdateItem(RedisModuleCtx* ctx, CRDT_HashTombstone* tombstone, CRDT_Hash* current, CrdtMeta* meta,RedisModuleString* key, sds field, sds value) {
    int result_code = NO_CHANGE_HASH;
    if(tombstone) {
        dictEntry* tomDe = dictFind(tombstone->map, field);
        if(tomDe) {
            CRDT_RegisterTombstone* tombstoneValue = dictGetVal(tomDe);
            int result = isExpireCrdtTombstone(tombstoneValue, meta);
            if(isConflictCommon(result)) {
                RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
            }
            if(result > COMPARE_META_EQUAL) {
                return result_code;
            }
        }
    }
    dictEntry* de = dictFind(current->map, field);
    if(de == NULL) {
        CRDT_Register* v = createCrdtRegister();
        crdtRegisterSetValue(v, meta, value);
        dictAdd(current->map, sdsdup(field), v);
        result_code = ADD_HASH;
    } else {
        CRDT_Register* v = dictGetVal(de);
        int result = compareCrdtMeta(getCrdtRegisterLastMeta(v), meta);
        if(result == COMPARE_META_VECTORCLOCK_LT) { return result_code; }
        sds prev = NULL;
        int isConflict = isConflictCommon(result);
        if(isConflict == CRDT_YES) {
            prev = crdtRegisterInfo(v);
        }
        crdtRegisterTryUpdate(v, meta, value, result);
        if(isConflict == CRDT_YES) {
            sds income = crdtRegisterInfoFromMetaAndValue(meta, value);
            sds future = crdtRegisterInfo(v);
            if(result > COMPARE_META_EQUAL) {
                RedisModule_Log(ctx, CRDT_DEBUG_LOG_LEVEL, "[CONFLICT][CRDT-HASH][replace] {key: %s, field: %s} [prev] {%s} [income] {%s} [future] {%s}",
                            RedisModule_GetSds(key), field, prev, income, future);
            }else{
                RedisModule_Log(ctx, CRDT_DEBUG_LOG_LEVEL, "[CONFLICT][CRDT-HASH][drop] {key: %s, field: %s} [prev] {%s} [income] {%s} [future] {%s}",
                            RedisModule_GetSds(key), field, prev, income, future);
            }
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | SET_CONFLICT);
            sdsfree(income);
            sdsfree(future);
            sdsfree(prev);
        }
        if(result > COMPARE_META_EQUAL) {
            result_code = UPDATE_HASH;
        }
    }
    return result_code;
}  

int addOrUpdateHash(RedisModuleCtx* ctx, RedisModuleString* key, RedisModuleKey* moduleKey,CRDT_HashTombstone* tombstone, CRDT_Hash* current, CrdtMeta* meta , RedisModuleString** argv, int start_index, int argc) {
    int need_created = CRDT_NO;
    if(current != NULL) {
        if(!isCrdtHash(current)) {
            const char* keyStr = RedisModule_StringPtrLen(key, NULL);
            RedisModule_Log(ctx, CRDT_DEBUG_LOG_LEVEL, "[CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                            keyStr ,current->type);
            RedisModule_IncrCrdtConflict(MODIFYCONFLICT | TYPECONFLICT);      
            RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);          
            return CHANGE_HASH_ERR;
        }
    } else {
        need_created = CRDT_OK;
        current = createCrdtHash();
    }
    int changed = 0;
    for (int i = start_index; i < argc; i+=2) {
        sds field = RedisModule_GetSds(argv[i]);
        sds value = RedisModule_GetSds(argv[i + 1]);
        int result = addOrUpdateItem(ctx, tombstone, current, meta, key, field, value);
        if(result == ADD_HASH) changed++;
    }
    mergeCrdtHashLastVc(current, getMetaVectorClock(meta));
    if(changed > 0) {
        changeCrdtHash(current, meta);
        if(need_created == CRDT_OK) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, current);
        }
        return changed;
    }
    if(need_created == CRDT_OK)  freeCrdtHash(current);
    return changed;
}
CRDT_Register * hashTypeGetFromHashTable(CRDT_Hash *o, sds field) {
    dictEntry *de;
    de = dictFind(o->map, field);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}
static int addCrdtHashFieldToReply(RedisModuleCtx *ctx, CRDT_Hash *crdtHash, RedisModuleString *field) {
    if (crdtHash == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    sds fld = RedisModule_GetSds(field);
    if(fld == NULL) {
        return RedisModule_ReplyWithError(ctx, "22  Operation against a key holding the wrong kind of value");
    }
    CRDT_Register *crdtRegister = hashTypeGetFromHashTable(crdtHash, fld);
    if (crdtRegister == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        sds val = getCrdtRegisterLastValue(crdtRegister);
        return RedisModule_ReplyWithStringBuffer(ctx, val, sdslen(val));
    }
}
/* --------------------------------------------------------------------------
 * User API for Hash type
 * -------------------------------------------------------------------------- */
const char* crdt_hset_head = "$9\r\nCRDT.HSET\r\n";
const size_t crdt_hset_basic_str_len = 15 + REPLICATION_ARGC_LEN + REPLICATION_MAX_STR_LEN + REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN +  REPLICATION_MAX_LONGLONG_LEN;
size_t replicationFeedCrdtHsetCommand(RedisModuleCtx *ctx,char* cmdbuf,const char* keystr, size_t keylen, CrdtMeta* meta, VectorClock vc,int argc, const char** fieldAndValStr, int* fieldAndValStrLen) {
    size_t cmdlen = 0;
    cmdlen +=  feedArgc(cmdbuf, argc + 6);
    cmdlen += feedBuf(cmdbuf+ cmdlen, crdt_hset_head);
    cmdlen += feedStr2Buf(cmdbuf + cmdlen, keystr, keylen);//$%d\r\n%s\r\n
    cmdlen += feedMeta2Buf(cmdbuf + cmdlen ,getMetaGid(meta),  getMetaTimestamp(meta), vc);
    cmdlen += feedLongLong2Buf(cmdbuf + cmdlen, (long long) (argc));
    for(int i = 0, len = argc; i < len; i+=2) {
        cmdlen += feedKV2Buf(cmdbuf+ cmdlen, fieldAndValStr[i], fieldAndValStrLen[i], fieldAndValStr[i+1], fieldAndValStrLen[i+1]);
    }
    // RedisModule_Debug(logLevel, "len:%d buf:%s", cmdlen, cmdbuf);
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}
//hset key f1 v2 f2 v2 ..
int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    // RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    if ((argc % 2) == 1) {
        return RedisModule_WrongArity(ctx);
    }
    int result = 0;
    CrdtMeta meta = {.gid=0};
    #if defined(HSET_STATISTICS) 
        get_modulekey_start();
    #endif
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtHash, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    
    initIncrMeta(&meta);
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(current != NULL) {
        appendVCForMeta(&meta, getCrdtHashLastVc(current));
    }
    #if defined(HSET_STATISTICS) 
        get_modulekey_end();
    #endif
    if(current == NULL) {
        current = createCrdtHash();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, current);
    } 
    

    size_t keylen = 0;
    const char* keystr = RedisModule_StringPtrLen(argv[1], &keylen);
    const char* fieldAndValStr[argc-2];
    int fieldAndValStrLen[argc-2];
    size_t fieldAndValAllStrLen = 0;
    for(int i = 2; i < argc; i += 2) {
        sds field = RedisModule_GetSds(argv[i]);
        dictEntry* de = dictFind(current->map, field);
        if(de == NULL) {
            #if defined(HSET_STATISTICS) 
                add_val_start();
            #endif
            CRDT_Register* v = createCrdtRegister();
            crdtRegisterSetValue(v, &meta, RedisModule_GetSds(argv[i+1]));
            dictAdd(current->map, sdsdup(field), v);
            result += 1;
            #if defined(HSET_STATISTICS) 
                add_val_end();
            #endif
        } else {
            #if defined(HSET_STATISTICS) 
                update_val_start();
            #endif
            CRDT_Register* v = dictGetVal(de);
            crdtRegisterTryUpdate(v, &meta, RedisModule_GetSds(argv[i+1]), COMPARE_META_VECTORCLOCK_GT);
            #if defined(HSET_STATISTICS) 
                update_val_end();
            #endif
        }
        size_t fieldstrlen = 0;
        const char* fieldstr = RedisModule_StringPtrLen(argv[i], &fieldstrlen);
        size_t valstrlen = 0;
        const char* valstr = RedisModule_StringPtrLen(argv[i+1], &valstrlen);
        fieldAndValStr[i-2] = fieldstr;
        fieldAndValStr[i-1] = valstr;
        fieldAndValStrLen[i-2] = fieldstrlen;
        fieldAndValStrLen[i-1] = valstrlen;
        fieldAndValAllStrLen += fieldstrlen + valstrlen + 2 * REPLICATION_MAX_STR_LEN;
    }
    setCrdtHashLastVc(current, getMetaVectorClock(&meta));
    #if defined(HSET_STATISTICS) 
        send_event_start();
    #endif
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "hset", argv[1]);
    #if defined(HSET_STATISTICS) 
        send_event_end();
    #endif
    
    #if defined(HSET_STATISTICS) 
        write_bakclog_start();
    #endif
        size_t alllen = crdt_hset_basic_str_len + keylen + fieldAndValAllStrLen;
        if(alllen > MAXSTACKSIZE) { 
            char *cmdbuf = RedisModule_Alloc(alllen);
            replicationFeedCrdtHsetCommand(ctx, cmdbuf,  keystr, keylen, &meta, getCrdtHashLastVc(current),argc - 2, fieldAndValStr, fieldAndValStrLen);
            RedisModule_Free(cmdbuf);
        }else {
            char cmdbuf[alllen]; 
            replicationFeedCrdtHsetCommand(ctx, cmdbuf, keystr, keylen, &meta, getCrdtHashLastVc(current),argc - 2, fieldAndValStr, fieldAndValStrLen);
        }
    #if defined(HSET_STATISTICS) 
        write_backlog_end();
    #endif
        //becase setCrdtHashLastVc move vector 
        // freeIncrMeta(&meta);
    
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    sds cmdname = RedisModule_GetSds(argv[0]);
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        return RedisModule_ReplyWithLongLong(ctx, result);
    } else {
        /* HMSET */
        return RedisModule_ReplyWithOk(ctx);
    } 
}

int hgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    CRDT_Hash *crdtHash;
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }

    int result = addCrdtHashFieldToReply(ctx, crdtHash, argv[2]);
    RedisModule_CloseKey(key);
    return result;
}

int hmgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 3) return RedisModule_WrongArity(ctx);
    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    RedisModule_AutoMemory(ctx);
    CRDT_Hash *crdtHash = NULL;
    int i;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, argc-2);

    for (i = 2; i < argc; i++) {
        addCrdtHashFieldToReply(ctx, crdtHash, argv[i]);
    }
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

void addTombstone(CRDT_HashTombstone* tombstone, sds field, CrdtMeta* meta, int* comapre) {
    dictEntry *tde = dictFind(tombstone->map, field);
    CRDT_RegisterTombstone *t;
    if(tde == NULL) {
        t = createCrdtRegisterTombstone();
        dictAdd(tombstone->map, sdsdup(field), t);
    }else{
        t = dictGetVal(tde);
    }
    addRegisterTombstone(t, meta, comapre);
}
int hdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 3) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    int deleted = 0;
    int deleteall = CRDT_NO;
    CrdtMeta hdel_meta = {.gid=0};
    
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_HashTombstone* tombstone = NULL;
    if(t != NULL && isCrdtHashTombstone(t)) {
        tombstone = retrieveCrdtHashTombstone(t);
    }
    if(tombstone == NULL) {
        tombstone = createCrdtHashTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHashTombstone, tombstone);
    }

    initIncrMeta(&hdel_meta);
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
    }
    appendVCForMeta(&hdel_meta, getCrdtHashLastVc(current));
    RedisModuleString** deleted_objs = RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString*) * (argc-2));
    for(int j = 2; j < argc; j++) {
        sds field = RedisModule_GetSds(argv[j]);
        if(dictDelete(current->map, field) == DICT_OK) {
            int compare = 0;
            addTombstone(tombstone, field, &hdel_meta, &compare);
            if(isConflictCommon(compare)) {
                RedisModule_IncrCrdtConflict(DEL_CONFLICT | MODIFYCONFLICT);
            }
            deleted_objs[deleted] = argv[j];
            deleted++;  
        }
    }
    if (crdtHtNeedsResize(current->map)) dictResize(current->map);
    
    if(deleted > 0) {
        changeCrdtHash(current, &hdel_meta);
        changeCrdtHashTombstone(tombstone, &hdel_meta);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH,"hdel", argv[1]);
    }    
    if (dictSize(current->map) == 0) {
        deleteall = CRDT_OK;
        RedisModule_DeleteKey(moduleKey);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
    }
end:
    if(hdel_meta.gid != 0) {
        if(deleted > 0) {
            sds vcStr = vectorClockToSds(getMetaVectorClock(&hdel_meta));
            size_t argc_repl = (size_t) deleted;
            void *argv_repl = (void *) deleted_objs;
            //CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ...
            RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.REM_HASH", "sllcv", argv[1], getMetaGid(&hdel_meta), getMetaTimestamp(&hdel_meta), vcStr, argv_repl, argc_repl);
            sdsfree(vcStr);
        }
        freeIncrMeta(&hdel_meta);
    }

    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted);
    }else{
        return CRDT_ERROR;
    }
    
}
// "CRDT.HSET", <key>, <gid>, <timestamp>, <vclockStr>,  <length> <field> <val> <field> <val> . . .);
//   0           1        2       3           4           5       6
int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 5) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    if (meta == NULL) {
        return 0;
    }
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_HashTombstone* tombstone = NULL;
    if ( t != NULL && isCrdtHashTombstone(t)) {
        tombstone = retrieveCrdtHashTombstone(t);
        int result = isExpireCrdtHashTombstone(tombstone, meta);
        if(isConflictCommon(result)) {
            RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
        }
        if(result > COMPARE_META_EQUAL) {
            goto end;
        }
    }
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(addOrUpdateHash(ctx, argv[1], moduleKey, tombstone, current, meta, argv, 6, argc) == CHANGE_HASH_ERR) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "hset", argv[1]);
end:
    if (meta != NULL) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(meta), ctx);
        freeCrdtMeta(meta);
    }
    if (moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}
// "CRDT.HGET", <key>, <field>
//   0           1        2      
// <value>  <gid>  <timestamp> <vectorClock>
//  0        1      2           3
int CRDT_HGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    CRDT_Hash *crdtHash;

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithNull(ctx);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }
    
    if (crdtHash == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    sds fld = RedisModule_GetSds(argv[2]);
    if(fld == NULL) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return RedisModule_ReplyWithNull(ctx);
    }
    CRDT_Register *crdtRegister = hashTypeGetFromHashTable(crdtHash, fld);     
    if (crdtRegister == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    } 
    sds value =  getCrdtRegisterLastValue(crdtRegister);
    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithStringBuffer(ctx, value, sdslen(value)); 
    RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastGid(crdtRegister));
    RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastTimestamp(crdtRegister));
    sds vclockSds = vectorClockToSds(getCrdtRegisterLastVc(crdtRegister));
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}
//CRDT.DEL_HASH <key> gid timestamp <del-op-vclock> <max-deleted-vclock>
// 0              1    2     3           4                  5
//CRDT.DEL_HASH <key> gid timestamp <del-op-vclock> <max-vclock> <expireVc>  
// 0              1    2     3           4                  5       6           
int CRDT_DelHashCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    if(meta == NULL) {
        return CRDT_ERROR;
    }

    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = 1;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_HashTombstone* tombstone = NULL;
    if(t != NULL) {
        if(isCrdtHashTombstone(t)) {
            tombstone = retrieveCrdtHashTombstone(t);
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtHashTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHashTombstone, tombstone);
    }
    int compare = 0;
    updateMaxDelCrdtHashTombstone(tombstone, meta, &compare);
    if(isConflictCommon(compare)) {
        RedisModule_IncrCrdtConflict(DEL_CONFLICT | MODIFYCONFLICT);
    }
    changeCrdtHashTombstone(tombstone, meta);
    CRDT_Hash* current =  getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
    }
    if(!isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(argv[1], NULL);
        RedisModule_Log(ctx, CRDT_DEBUG_LOG_LEVEL, "[CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr , current->type);
        RedisModule_IncrCrdtConflict(MODIFYCONFLICT | TYPECONFLICT);      
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE); 
        status = CRDT_ERROR;
        goto end;
    }

    
    dictIterator *di = dictGetSafeIterator(current->map);
    dictEntry *de;
    int hasDataTombstoneConflict = 0;
    while ((de = dictNext(di)) != NULL) {
        CRDT_Register *kv = dictGetVal(de);
        int result = compareCrdtRegisterAndDelMeta(kv, meta);
        if(!hasDataTombstoneConflict && isConflictCommon(result)) {
            hasDataTombstoneConflict = 1;
        }
        if(result > COMPARE_META_EQUAL) {
            dictDelete(current->map, dictGetKey(de));
            deleted++;
        }
    }
    if(hasDataTombstoneConflict) {
        RedisModule_IncrCrdtConflict(MODIFYCONFLICT | SET_DEL_CONFLICT);    
    }
    dictReleaseIterator(di);
    /* Always check if the dictionary needs a resize after a delete. */
    if (crdtHtNeedsResize(current->map)) dictResize(current->map);
    if (dictSize(current->map) == 0) {
        RedisModule_DeleteKey(moduleKey);
    }
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end:
    if(meta) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(meta), ctx);
        freeCrdtMeta(meta);
    }
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }   
}
//CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ....
// 0              1    2     3           4             5
int CRDT_RemHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
        CrdtMeta* meta = getMeta(ctx, argv, 2);
    if(meta == NULL) {
        return 0;
    }
    int status = CRDT_OK;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    int deleted = 0;
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_HashTombstone* tombstone = NULL;
    if(t != NULL && isCrdtHashTombstone(t)) {
        tombstone = retrieveCrdtHashTombstone(t);
        int result = isExpireCrdtHashTombstone(tombstone, meta);
        if(isConflictCommon(result)) {
            RedisModule_IncrCrdtConflict(DEL_CONFLICT | MODIFYCONFLICT);
        }
        if(result > COMPARE_META_EQUAL) {
            goto end;
        }
    }
    
    if(tombstone == NULL) {
        tombstone = createCrdtHashTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHashTombstone, tombstone);
    }
    

    CRDT_Hash* current =  getCurrentValue(moduleKey);
    if(current != NULL && !isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(argv[1], NULL);
        RedisModule_Log(ctx, CRDT_DEBUG_LOG_LEVEL, "[CRDT_RemHashCommand][CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr ,current->type);
        RedisModule_IncrCrdtConflict(MODIFYCONFLICT | TYPECONFLICT);      
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE); 
        status = CRDT_ERROR;
        goto end;
    }
    int keyremoved = CRDT_NO;
    int conflict = 0;
    for (int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        if(current != NULL) {
            dictEntry* de = dictFind(current->map, field);
            if(de != NULL) {
                CRDT_Register* value = dictGetVal(de);
                int result = compareCrdtRegisterAndDelMeta(value, meta);
                if(isConflictCommon(result)) {
                    RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
                }
                if(result > COMPARE_META_EQUAL) {
                    dictDelete(current->map, field);
                    deleted++;
                }
            }
        }
        int compare = 0;
        addTombstone(tombstone, field, meta, &compare);
        if(isConflictCommon(compare)) conflict++;
    }
    if(conflict > 0) {
        RedisModule_IncrCrdtConflict(MODIFYCONFLICT | DEL_CONFLICT);
    }
    changeCrdtHashTombstone(tombstone, meta);
    if(current != NULL) {
        if(deleted > 0 && crdtHtNeedsResize(current->map)) {
            changeCrdtHash(current, meta);
            dictResize(current->map);
        }
        if (dictSize(current->map) == 0) {
            RedisModule_DeleteKey(moduleKey);
            keyremoved = CRDT_OK;
        }
    }
    
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "hdel", argv[1]);
    if(keyremoved == CRDT_OK) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
    }
end:
    if(meta) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(meta), ctx);
        freeCrdtMeta(meta);
    }  
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if (status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }   
}

int genericHgetallCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int flags) {
    if(argc != 2) return RedisModule_WrongArity(ctx);
    int multiplier = 0;
    int length, count = 0;

    if (flags & OBJ_HASH_KEY) multiplier++;
    if (flags & OBJ_HASH_VALUE) multiplier++;

    CRDT_Hash *crdtHash;

    RedisModule_AutoMemory(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithArray(ctx, 0);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }

    length = dictSize((const dict*)crdtHash->map);
    length = length * multiplier;

    RedisModule_ReplyWithArray(ctx, length);


    dictIterator *di = dictGetIterator(crdtHash->map);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {

        if (flags & OBJ_HASH_KEY) {
            sds field = dictGetKey(de);
            RedisModule_ReplyWithStringBuffer(ctx, field, sdslen(field));
            count++;
        }
        if (flags & OBJ_HASH_VALUE) {
            CRDT_Register *crdtRegister = dictGetVal(de);
            sds val = getCrdtRegisterLastValue(crdtRegister);
            RedisModule_ReplyWithStringBuffer(ctx, val, sdslen(val));
            count++;
        }
    }
    dictReleaseIterator(di);

    if (count != length) {
        RedisModule_WrongArity(ctx);
    }
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

int hkeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return genericHgetallCommand(ctx, argv, argc, OBJ_HASH_KEY);
}

int hvalsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return genericHgetallCommand(ctx, argv, argc, OBJ_HASH_VALUE);
}

int hgetallCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return genericHgetallCommand(ctx, argv, argc, OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

/*
 * Init Hash Module 
 */
RedisModuleType* getCrdtHash() {
    return CrdtHash;
}
RedisModuleType* getCrdtHashTombstone() {
    return CrdtHashTombstone;
}
int initCrdtHashModule(RedisModuleCtx *ctx) {
    //hash object type
    RedisModuleTypeMethods tm = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtHash,
            .rdb_save = RdbSaveCrdtHash,
            .aof_rewrite = AofRewriteCrdtHash,
            .mem_usage = crdtHashMemUsageFunc,
            .free = freeCrdtHash,
            .digest = crdtHashDigestFunc
    };
    CrdtHash = RedisModule_CreateDataType(ctx, CRDT_HASH_DATATYPE_NAME, 0, &tm);
    if (CrdtHash == NULL) return REDISMODULE_ERR;
    //hash tombstone type
    RedisModuleTypeMethods tbtm = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtHashTombstone,
            .rdb_save = RdbSaveCrdtHashTombstone,
            .aof_rewrite = AofRewriteCrdtHashTombstone,
            .mem_usage = crdtHashTombstoneMemUsageFunc,
            .free = freeCrdtHashTombstone,
            .digest = crdtHashTombstoneDigestFunc
    };
    CrdtHashTombstone = RedisModule_CreateDataType(ctx, CRDT_HASH_TOMBSOTNE_DATATYPE_NAME, 0, &tbtm);
    if (CrdtHashTombstone == NULL) return REDISMODULE_ERR;

    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
    if (RedisModule_CreateCommand(ctx,"HSET",
                                  hsetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HMSET",
                                  hsetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HGET",
                                  hgetCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HMGET",
                                  hmgetCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"HGETALL",
                                  hgetallCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"HKEYS",
                                  hkeysCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"HVALS",
                                  hvalsCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HDEL",
                                  hdelCommand,"write fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.HSET",
                                  CRDT_HSetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.HGET",
                                  CRDT_HGetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_HASH",
                                  CRDT_DelHashCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.REM_HASH",
                                  CRDT_RemHashCommand,"write",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
/**
 *  Basic utils
*/
int RdbLoadDict(RedisModuleIO *rdb, int encver, dict *map, RedisModuleTypeLoadFunc func) {
    uint64_t len;
    sds field;
    CRDT_Register *value;
    size_t strLength;
    len = RedisModule_LoadUnsigned(rdb);
    if (len == RDB_LENERR) return CRDT_NO;
    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        field = sdsnewlen(str, strLength);
        value = func(rdb, encver);
        /* Add pair to hash table */
        dictAdd(map, field, value);
        RedisModule_ZFree(str);
    }
    return CRDT_OK;
}
int RdbLoadCrdtBasicHash(RedisModuleIO *rdb, int encver, void *data) {
    CRDT_Hash* hash = data;
    return RdbLoadDict(rdb, encver, hash->map, RdbLoadCrdtRegister);
}
void RdbSaveDict(RedisModuleIO *rdb, dict* map, RedisModuleTypeSaveFunc func) {
    
    dictIterator *di = dictGetSafeIterator(map);
    dictEntry *de;
    RedisModule_SaveUnsigned(rdb, dictSize(map));
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        void *crdtRegister = dictGetVal(de);

        RedisModule_SaveStringBuffer(rdb, field, sdslen(field));
        func(rdb, crdtRegister);
    }
    dictReleaseIterator(di);
}
void RdbSaveCrdtBasicHash(RedisModuleIO *rdb, void *value) {
    CRDT_Hash* crdtHash = value;
    RdbSaveDict(rdb, crdtHash->map, RdbSaveCrdtRegister);
}
void RdbSaveCrdtBasicHashTombstone(RedisModuleIO *rdb, void *value) {
    CRDT_HashTombstone* crdtHashTombstone = retrieveCrdtHashTombstone(value);
     RdbSaveDict(rdb, crdtHashTombstone->map, RdbSaveCrdtRegisterTombstone);
}
CrdtMeta* appendBasicHash(CRDT_Hash* target, CRDT_Hash* other) {
    CrdtMeta* result = NULL;
    dictIterator *di = dictGetIterator(other->map);
    dictEntry *de, *existDe;
    int conflict = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        existDe = dictFind(target->map, field);
        if (existDe == NULL) {
            dictAdd(target->map, sdsdup(field), dupCrdtRegister(crdtRegister));
        } else {
            CRDT_Register *currentRegister = dictGetVal(existDe);
            int compare = 0;
            CRDT_Register *newRegister = mergeRegister(currentRegister, crdtRegister, &compare);
            if(isConflictCommon(compare)) {
                conflict += 1;
            }
            freeCrdtRegister(currentRegister);
            dictGetVal(existDe) = newRegister;
        }
        CrdtMeta* meta = createCrdtRegisterLastMeta(crdtRegister);
        if(result == NULL) {
            result = meta;
        } else {
            appendCrdtMeta(result, meta);
            freeCrdtMeta(meta);
        }
    }
    if(conflict > 0) {
        //only when merge hash
        RedisModule_IncrCrdtConflict(SET_CONFLICT | MERGECONFLICT);
    }
    dictReleaseIterator(di);
    return result;
}

int RdbLoadCrdtBasicHashTombstone(RedisModuleIO *rdb, int encver, void *data) {
    CRDT_HashTombstone* tombstone = data;
    return RdbLoadDict(rdb, encver, tombstone->map, RdbLoadCrdtRegisterTombstone);
}
size_t crdtBasicHashMemUsageFunc(void* data) {
    CRDT_Hash* result = retrieveCrdtHash(data);
    return dictSize(result->map) ;
}
size_t crdtBasicHashTombstoneMemUsageFunc(void* data) {
    CRDT_HashTombstone* result = retrieveCrdtHashTombstone(data);
    return dictSize(result->map) ;
}
/* --------------------------------------------------------------------------
 * About Dict
 * -------------------------------------------------------------------------- */
uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}
int dictSdsKeyCompare(void *privdata, const void *key1,
                      const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

void dictCrdtRegisterDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    freeCrdtRegister(val);
}
void dictCrdtRegisterTombstoneDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    freeCrdtRegisterTombstone(val);
}

//hash common methods
CrdtObject *crdtHashMerge(CrdtObject *currentVal, CrdtObject *value) {
    CRDT_Hash* target = retrieveCrdtHash(currentVal);
    CRDT_Hash* other = retrieveCrdtHash(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return (CrdtObject*)dupCrdtHash((CRDT_Hash*)value);
    }

    CRDT_Hash *result = dupCrdtHash(target);
    CrdtMeta* meta = appendBasicHash(result, other);
    changeCrdtHash(target, meta);
    freeCrdtMeta(meta);
    return (CrdtObject*)result;
}
int crdtHashDelete(int dbId, void *keyRobj, void *key, void *value) {
    if(value == NULL) {
        return CRDT_ERROR;
    }
    if(!isCrdtHash(value)) {
        return CRDT_ERROR;
    }
    CrdtMeta* meta = createIncrMeta();
    CrdtMeta* del_meta = dupMeta(meta);
    CRDT_Hash* current = (CRDT_Hash*) value;
    appendVCForMeta(del_meta, getCrdtHashLastVc(current));
    RedisModuleKey *moduleKey = (RedisModuleKey *) key;
    CRDT_HashTombstone* tombstone = getTombstone(key);
    if(tombstone == NULL || !isCrdtHashTombstone(tombstone)) {
        tombstone = createCrdtHashTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHashTombstone, tombstone);
    }
    changeCrdtHash(current, del_meta);
    int compare = 0;
    CrdtMeta* result = updateMaxDelCrdtHashTombstone(tombstone, del_meta, &compare);
    changeCrdtHashTombstone(tombstone, del_meta);
    sds vcSds = vectorClockToSds(getMetaVectorClock(result));
        sds maxDeletedVclock = vectorClockToSds(getCrdtHashLastVc(current));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Hash", "sllcc", keyRobj, getMetaGid(meta), getMetaTimestamp(meta), vcSds, vcSds);
    sdsfree(vcSds);
    sdsfree(maxDeletedVclock);
    freeCrdtMeta(meta);
    freeCrdtMeta(del_meta);
    return CRDT_OK;
}

CrdtObject* crdtHashFilter(CrdtObject* common, int gid, long long logic_time) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(common);
    CRDT_Hash* result = createCrdtHash();
    dictIterator *di = dictGetSafeIterator(crdtHash->map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        CRDT_Register *filted = filterRegister(crdtRegister, gid, logic_time);
        if(filted != NULL) {
            dictAdd(result->map, sdsdup(field), filted);
            CrdtMeta* meta = createCrdtRegisterLastMeta(filted);
            changeCrdtHash(result, meta);
            freeCrdtMeta(meta);
        }
    }
    dictReleaseIterator(di);
    if(dictSize(result->map) == 0) {
        freeCrdtHash(result);
        return NULL;
    }
    return (CrdtObject*)result;
}
int crdtHashTombstonePurage( CrdtObject* tombstone, CrdtObject* current) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(current);
    CRDT_HashTombstone* crdtHashTombstone = retrieveCrdtHashTombstone(tombstone);
    dictIterator *di = dictGetSafeIterator(crdtHashTombstone->map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        dictEntry *existDe = dictFind(crdtHash->map, field);
        if(existDe != NULL) {
            CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
            CRDT_Register *crdtRegister = dictGetVal(existDe);
            CrdtMeta* lastMeta = createCrdtRegisterLastMeta(crdtRegister);
            if(isExpireCrdtHashTombstone(crdtHashTombstone, lastMeta) > COMPARE_META_EQUAL
               || purageRegisterTombstone(crdtRegisterTombstone, crdtRegister) == CRDT_OK) {
                dictDelete(crdtHash->map, field);
            }
            freeCrdtMeta(lastMeta);
            
        }
    }
    dictReleaseIterator(di);
    if(dictSize(crdtHash->map) == 0) {
        return CRDT_OK;
    }
    return CRDT_NO;
}

//tombstone common methods
CrdtTombstone *crdtHashTombstoneMerge(CrdtTombstone *currentVal, CrdtTombstone *value) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(currentVal);
    CRDT_HashTombstone* other = retrieveCrdtHashTombstone(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return (CrdtTombstone*)dupCrdtHashTombstone(value);
    }
    CRDT_HashTombstone *result = dupCrdtHashTombstone(target);
    dictIterator *di = dictGetIterator(other->map);
    dictEntry *de, *existDe;
    int conflict = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
        existDe = dictFind(target->map, field);
        if (existDe == NULL) {
            dictAdd(target->map, sdsdup(field), dupCrdtRegisterTombstone(crdtRegisterTombstone));
        } else {
            CRDT_RegisterTombstone *currentRegisterTombstone = dictGetVal(existDe);
            int comp = 0;
            CRDT_RegisterTombstone *newRegisterTombstone = mergeRegisterTombstone(currentRegisterTombstone, crdtRegisterTombstone, &comp);
            if(isConflictCommon(comp)) conflict++;
            freeCrdtRegisterTombstone(currentRegisterTombstone);
            dictGetVal(existDe) = newRegisterTombstone;
            
        }
    }
    dictReleaseIterator(di);
    int compare = 0;
    updateMaxDelCrdtHashTombstone(result,getMaxDelCrdtHashTombstone(other), &compare);
    if(isConflictCommon(compare)) conflict++;
    mergeCrdtHashTombstoneLastVc(result, getCrdtHashTombstoneLastVc(other));
    if(conflict > 0) RedisModule_IncrCrdtConflict(DEL_CONFLICT | MERGECONFLICT);
    return (CrdtTombstone*)result;
}

CrdtTombstone* crdtHashTombstoneFilter(CrdtTombstone* common, int gid, long long logic_time) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(common);
    CRDT_HashTombstone* result = dupCrdtHashTombstone(target);
    dictEmpty(result->map, NULL);
    dictIterator *di = dictGetIterator(target->map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
        CRDT_RegisterTombstone *filter = filterRegisterTombstone(crdtRegisterTombstone, gid, logic_time);
        if(filter != NULL) {
            dictAdd(result->map, sdsdup(field), filter);
        }
    }
    dictReleaseIterator(di);
    if(dictSize(result->map) == 0) {
        freeCrdtHashTombstone(result);
        return NULL;
    }
    return (CrdtTombstone*)result;
}

int crdtHashTombstoneGc(CrdtObject* common, VectorClock clock) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(common);
    return gcCrdtHashTombstone(target, clock);
}

VectorClock crdtHashGetLastVC(void* data) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(data);
    return getCrdtHashLastVc(crdtHash);
}
void crdtHashUpdateLastVC(void* data, VectorClock vc) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(data);
    updateLastVCHash(crdtHash, vc);
}