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
#include "lww/crdt_lww_hashmap.h"
#include "ctrip_swap.h"

dictType crdtHashFileterDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        NULL,          /* key destructor */
        NULL   /* val destructor */
};

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
    crdtAssert(result->map != NULL);
    return result;
}



CRDT_HashTombstone* retrieveCrdtHashTombstone(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_HashTombstone* result = (CRDT_HashTombstone*)t;
    crdtAssert(result->map != NULL);
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
    CrdtMeta* meta_copy = dupMeta(meta);
    if(tombstone) {
        dictEntry* tomDe = dictFind(tombstone->map, field);
        if(tomDe) {
            CRDT_RegisterTombstone* tombstoneValue = dictGetVal(tomDe);
            int result = compareCrdtRegisterTombstone(tombstoneValue, meta_copy);
            if(isConflictCommon(result)) {
                RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
            }
            if(result > COMPARE_META_EQUAL) {
                appendCrdtMeta(getCrdtRegisterTombstoneMeta(tombstoneValue), meta_copy);
                freeCrdtMeta(meta_copy);
                return result_code;
            }
            appendCrdtMeta(meta_copy, getCrdtRegisterTombstoneMeta(tombstoneValue));
            dictDelete(tombstone->map, field);
        }
    }
    dictEntry* de = dictFind(current->map, field);
    if(de == NULL) {
        CRDT_Register* v = createCrdtRegister();
        crdtRegisterSetValue(v, meta_copy, value);
        dictAdd(current->map, sdsdup(field), v);
        result_code = ADD_HASH;
    } else {
        CRDT_Register* v = dictGetVal(de);
        int result = compareCrdtMeta(getCrdtRegisterLastMeta(v), meta_copy);
        if(result == COMPARE_META_VECTORCLOCK_LT) { 
            freeCrdtMeta(meta_copy); 
            return result_code; 
        }
        sds prev = NULL;
        int isConflict = isConflictCommon(result);
        if(isConflict == CRDT_YES) {
            prev = crdtRegisterInfo(v);
        }
        crdtRegisterTryUpdate(v, meta_copy, value, result);
        if(isConflict == CRDT_YES) {
            sds income = crdtRegisterInfoFromMetaAndValue(meta_copy, value);
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
    freeCrdtMeta(meta_copy);
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
    static size_t crdt_hset_head_str_len = 0;
    if(crdt_hset_head_str_len == 0) {
        crdt_hset_head_str_len = strlen(crdt_hset_head);
    }
    cmdlen += feedBuf(cmdbuf+ cmdlen, crdt_hset_head, crdt_hset_head_str_len);
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

#define HSET_NO_FLAGS 0
#define HSET_NX (1<<0)     /* Set if key not exists. */

//hset key f1 v2 f2 v2 ..
int hsetGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int flags) {
    // RedisModule_AutoMemory(ctx);
    
    int result = 0;
    CrdtMeta meta = {.gid=0};
    #if defined(HSET_STATISTICS) 
        get_modulekey_start();
    #endif
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if (moduleKey == NULL) {
        return -1;
    }
    
    CRDT_Hash* current = getCurrentValue(moduleKey);
    #if defined(HSET_STATISTICS) 
        get_modulekey_end();
    #endif
    if(flags & HSET_NX && current != NULL) {
        for(int i = 2; i < argc; i++) {
            sds field  = RedisModule_GetSds(argv[i]);
            dictEntry* de = dictFind(current->map, field);
            if(de != NULL) {
                RedisModule_CloseKey(moduleKey);
                return 0;
            }
        }
    }
    
    initIncrMeta(&meta);
    size_t keylen = 0;
    const char* keystr = RedisModule_StringPtrLen(argv[1], &keylen);
    const char* fieldAndValStr[argc-2];
    int fieldAndValStrLen[argc-2];
    size_t fieldAndValAllStrLen = 0;
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_HashTombstone* tombstone = NULL;
    if(t != NULL && isCrdtHashTombstone(t)) {
        tombstone = retrieveCrdtHashTombstone(t);
    }
    if(current == NULL) {
        current = createCrdtHash();
        if(tombstone) {
            updateLastVCHash(current, getCrdtHashTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            updateLastVCHash(current, LL2VC(vc));
        }
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, current);
    } 
    appendVCForMeta(&meta, getCrdtHashLastVc(current));
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
        if(tombstone) {
            dictDelete(tombstone->map, field);
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
    RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_HASH, "hset", argv[1], moduleKey, NULL);
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
    return result;
    
}

int hsetnxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    int result = hsetGenericCommand(ctx, argv, argc, HSET_NX);
    if(result < 0) { return result; }
    return RedisModule_ReplyWithLongLong(ctx, result);
}
int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4) return RedisModule_WrongArity(ctx);
    if ((argc % 2) == 1) {
        return RedisModule_WrongArity(ctx);
    }
    int result = hsetGenericCommand(ctx, argv, argc, HSET_NO_FLAGS);
    if(result < 0) { return result; }
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
    
    
    if(deleted > 0) {
        changeCrdtHashTombstone(tombstone, &hdel_meta);
        RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_HASH,"hdel", argv[1], moduleKey, NULL);
    }    
    if (dictSize(current->map) == 0) {
        RedisModule_RocksDelete(ctx,argv[1]);
        RedisModule_DeleteKey(moduleKey);
        RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1], moduleKey, NULL);
    } else {
        if (crdtHtNeedsResize(current->map)) dictResize(current->map);
        if (deleted > 0) changeCrdtHash(current, &hdel_meta);
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

void tombstone_add_element(CRDT_HashTombstone* tombstone, sds field, CrdtMeta* meta) {
    dictEntry* de = dictFind(tombstone->map, field);
    CRDT_RegisterTombstone* d = NULL;
    if(de) {
        // dictDelete(tombstone->map, field);
        d =  dictGetVal(de);     
    } else {
        de = dictAddRaw(tombstone->map, sdsdup(field), NULL);
        d = createCrdtRegisterTombstone();
    }
    // appendCrdtMeta(meta, getMaxDelCrdtHashTombstone(tombstone));
    int r = 0;
    addRegisterTombstone(d, meta, &r);
    // dictAdd(tombstone->map, sdsdup(field), d);
    dictSetVal(tombstone->map, de, d);
    
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
        int result = compareCrdtHashTombstone(tombstone, meta);
        if(isConflictCommon(result)) {
            RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
        }
        if(result > COMPARE_META_EQUAL) {
            //to do
            // getCRDTHash(tombstone);
            
            if(result > COMPARE_META_VECTORCLOCK_GT) {
                
                // CRDT_RegisterTombstone* tkv = createCrdtRegisterTombstone();
                // appendCrdtMeta(meta, getMaxDelCrdtHashTombstone(tombstone));
                // int r = 0;
                // addRegisterTombstone(tkv, meta, &r);
                // sds field = RedisModule_GetSds(argv[6]);
                //dupCrdtRegisterTombstone(tkv)
                // add_tombstone(tombstone, field, tkv);
                for (int i = 6; i < argc; i+=2) {
                    sds field = RedisModule_GetSds(argv[i]);
                    tombstone_add_element(tombstone, field, meta);
                }
            } 
            goto end;
        }
    }
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(addOrUpdateHash(ctx, argv[1], moduleKey, tombstone, current, meta, argv, 6, argc) == CHANGE_HASH_ERR) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_HASH, "hset", argv[1], moduleKey, NULL);
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

int CRDT_HDataInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);

    CRDT_Hash *crdtHash = NULL;

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // RedisModule_CloseKey(key);
        // return RedisModule_ReplyWithNull(ctx);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }
    
    sds fld = RedisModule_GetSds(argv[2]);
    if(fld == NULL) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return RedisModule_ReplyWithNull(ctx);
    }
    if(crdtHash) {
        dictEntry* de = dictFind(crdtHash->map, fld);
        if(de) {
            CRDT_Register *crdtRegister = dictGetVal(de);
            sds info = crdtRegisterInfo(crdtRegister);
            RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
            sdsfree(info);
            goto end;
        }
    }

    
    
    CRDT_HashTombstone* tom =  getTombstone(key);
    if(!isCrdtHashTombstone(tom)) {
        RedisModule_ReplyWithNull(ctx);
        goto end;
    }
    if(tom) {
        dictEntry* de = dictFind(tom->map, fld);
        if(de) {
            CRDT_RegisterTombstone *reg = dictGetVal(de);
            sds info = crdtRegisterTombstoneInfo(reg);
            RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
            sdsfree(info);
            goto end;
        }
    }
    RedisModule_ReplyWithNull(ctx);
    // sds value =  getCrdtRegisterLastValue(crdtRegister);
    // RedisModule_ReplyWithArray(ctx, 4);
    // RedisModule_ReplyWithStringBuffer(ctx, value, sdslen(value)); 
    // RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastGid(crdtRegister));
    // RedisModule_ReplyWithLongLong(ctx, getCrdtRegisterLastTimestamp(crdtRegister));
    // sds vclockSds = vectorClockToSds(getCrdtRegisterLastVc(crdtRegister));
    // RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    // sdsfree(vclockSds);
end:
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
            if(result > COMPARE_META_VECTORCLOCK_GT) {
                // CRDT_RegisterTombstone* tkv = createCrdtRegisterTombstone();
                // CrdtMeta* m = dupMeta(getCrdtRegisterTombstoneMeta(kv));
                // appendCrdtMeta(m, meta);
                // int r = 0;
                // addRegisterTombstone(tkv, m, &r);
                // freeCrdtMeta(m);
                // dictAdd(tombstone->map, sdsdup(dictGetKey(de)), tkv);
                tombstone_add_element(tombstone, dictGetKey(de), meta);
            }
            dictDelete(current->map, dictGetKey(de));
            deleted++;
        } else if(result < COMPARE_META_VECTORCLOCK_LT) {
            appendCrdtMeta(getCrdtRegisterLastMeta(kv), meta);
        }
    }
    if(hasDataTombstoneConflict) {
        RedisModule_IncrCrdtConflict(MODIFYCONFLICT | SET_DEL_CONFLICT);    
    }
    dictReleaseIterator(di);

    di = dictGetSafeIterator(tombstone->map);
    while ((de = dictNext(di)) != NULL) {
        CRDT_RegisterTombstone *tkv = dictGetVal(de);
        int result = compareCrdtMeta(getCrdtRegisterTombstoneMeta(tkv), meta);
        if( result < COMPARE_META_EQUAL) {
            dictDelete(tombstone->map, dictGetKey(de));
        } else {
            addRegisterTombstone(tkv, meta, &result);
        }
    }
    dictReleaseIterator(di);
    if (dictSize(current->map) == 0) {
        RedisModule_RocksDelete(ctx,argv[1]);
        RedisModule_DeleteKey(moduleKey);
    } else {
        /* Always check if the dictionary needs a resize after a delete. */
        if (crdtHtNeedsResize(current->map)) dictResize(current->map);
    }
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1], moduleKey, NULL);
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
        int result = compareCrdtHashTombstone(tombstone, meta);
        if(isConflictCommon(result)) {
            RedisModule_IncrCrdtConflict(DEL_CONFLICT | MODIFYCONFLICT);
        }
        if(result > COMPARE_META_VECTORCLOCK_GT) {
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
        CrdtMeta* meta_copy = dupMeta(meta);
        if(current != NULL) {
            dictEntry* de = dictFind(current->map, field);
            if(de != NULL) {
                CRDT_Register* value = dictGetVal(de);
                int result = compareCrdtRegisterAndDelMeta(value, meta_copy);
                if(isConflictCommon(result)) {
                    RedisModule_IncrCrdtConflict(SET_DEL_CONFLICT | MODIFYCONFLICT);
                }
                if(result > COMPARE_META_EQUAL) {
                    if(result > COMPARE_META_VECTORCLOCK_GT) {
                        appendCrdtMeta(meta_copy, getCrdtRegisterLastMeta(value));
                    }
                    dictDelete(current->map, field);
                    deleted++;
                } else {
                    appendCrdtMeta(getCrdtRegisterLastMeta(value), meta_copy);
                    freeCrdtMeta(meta_copy);
                    continue;
                }
            }
        }
        int compare = 0;
        dictEntry* tde = dictFind(tombstone->map, field);
        if(tde == NULL) {
            addTombstone(tombstone, field, meta_copy, &compare);
        } else {
            CRDT_RegisterTombstone* tkv = dictGetVal(tde);
            compare = compareCrdtMeta(getCrdtRegisterTombstoneMeta(tkv), meta_copy);
            appendCrdtMeta(getCrdtRegisterTombstoneMeta(tkv), meta_copy);
        }
        
        if(isConflictCommon(compare)) conflict++;
        freeCrdtMeta(meta_copy);
    }
    if(conflict > 0) {
        RedisModule_IncrCrdtConflict(MODIFYCONFLICT | DEL_CONFLICT);
    }
    changeCrdtHashTombstone(tombstone, meta);
    if(current != NULL) {
        if (dictSize(current->map) == 0) {
            RedisModule_RocksDelete(ctx,argv[1]);
            RedisModule_DeleteKey(moduleKey);
            keyremoved = CRDT_OK;
        } else {
            if(deleted > 0) { changeCrdtHash(current, meta);}
            if(crdtHtNeedsResize(current->map)) dictResize(current->map);
        }
    }
    RedisModule_MergeVectorClock(getMetaGid(meta), getMetaVectorClockToLongLong(meta));
    RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_HASH, "hdel", argv[1], moduleKey, NULL);
    if(keyremoved == CRDT_OK) {
        RedisModule_NotifyKeyspaceEventDirty(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1], moduleKey, NULL);
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

int hlenCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    if(argc != 2) return RedisModule_WrongArity(ctx);
    CRDT_Hash *crdtHash;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else if (RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }
    long long length = dictSize((const dict*)crdtHash->map);
    RedisModule_ReplyWithLongLong(ctx, length);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

void hscanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    list *keys = pd[0];
    sds key = NULL, val = NULL;
    key = sdsdup(dictGetKey(de));
    CRDT_Register *crdtRegister = dictGetVal(de);
    val = sdsdup(getCrdtRegisterLastValue(crdtRegister));
    listAddNodeTail(keys, key);
    listAddNodeTail(keys, val);
}

int hscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtHash, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        replyEmptyScan(ctx);
        return CRDT_ERROR;
    }
    CRDT_Hash* hash = getCurrentValue(moduleKey);
    if(hash == NULL) {
        replyEmptyScan(ctx);
        RedisModule_CloseKey(moduleKey);
        return REDISMODULE_OK;
    }
    unsigned long cursor;

    if (parseScanCursorOrReply(ctx, argv[2], &cursor) == CRDT_ERROR) return 0;

    scanGenericCommand(ctx, argv, argc, hash->map, 1, cursor, hscanCallback);

    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}

int hexistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
     if (argc != 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtHash, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
        return CRDT_ERROR;
    }
    CRDT_Hash* hash = getCurrentValue(moduleKey);
    int result = 0;
    if(hash != NULL) {
        sds field = RedisModule_GetSds(argv[2]);
        dictEntry* entry = dictFind(hash->map, field);
        if(entry != NULL) {
            result = 1;
        } 
    }
    RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}

/*
 * Hash Polymorphism
 */
//create hash
void *createCrdtHash(void) {
    return createCrdtLWWHash();
}
void freeCrdtHash(void *data) {
    freeCrdtLWWHash(data);
}
//create hash tombstone
void *createCrdtHashTombstone(void) {
    return createCrdtLWWHashTombstone();
}
void freeCrdtHashTombstone(void *data) {
    freeCrdtLWWHashTombstone(data);
}
//basic hash module functions
void *sioLoadCrdtHash(sio *io, int encver) {
    long long header = loadCrdtRdbHeader(io);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == LWW_TYPE) {
        return sioLoadCrdtLWWHash(io, version, encver);
    }
    return NULL;
}
void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver) {
    sio *io = rdbStreamCreate(rdb);
    void *res = sioLoadCrdtHash(io, encver);
    rdbStreamRelease(io);
    return res;
}

void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value) {
    sio *io = rdbStreamCreate(rdb);
    sioSaveCrdtLWWHash(io, value);
    rdbStreamRelease(io);
}
void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWHash(aof, key, value);
}

size_t crdtHashMemUsageFunc(const void *value) {
    return crdtLWWHashMemUsageFunc(value);
}
void crdtHashDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWHashDigestFunc(md, value);
}
void *sioLoadCrdtHashTombstone(sio *io, int encver) {
    long long header = loadCrdtRdbHeader(io);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == LWW_TYPE) {
        return sioLoadCrdtLWWHashTombstone(io, version, encver);
    }
    return NULL;
}
//basic hash tombstone module functions
void *RdbLoadCrdtHashTombstone(RedisModuleIO *rdb, int encver) {
    sio *io = rdbStreamCreate(rdb);
    void *res = sioLoadCrdtHashTombstone(io, encver);
    rdbStreamRelease(io);
    return res;
}
void RdbSaveCrdtHashTombstone(RedisModuleIO *rdb, void *value) {
    sio *io = rdbStreamCreate(rdb);
    sioSaveCrdtLWWHashTombstone(io, value);
    rdbStreamRelease(io);
}
void AofRewriteCrdtHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWHashTombstone(aof, key, value);
}

size_t crdtHashTombstoneMemUsageFunc(const void *value) {
    return crdtLWWHashTombstoneMemUsageFunc(value);
}
void crdtHashTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWHashTombstoneDigestFunc(md, value);
}

int changeCrdtHash(CRDT_Hash* hash, CrdtMeta* meta) {
    return changeCrdtLWWHash(retrieveCrdtLWWHash(hash), meta);
}

CRDT_Hash* dupCrdtHash(CRDT_Hash* data) {
    return (CRDT_Hash*)dupCrdtLWWHash(data);
}

void updateLastVCHash(CRDT_Hash* data, VectorClock vc) {
    return updateLastVCLWWHash(data, vc);
}

CrdtMeta* updateMaxDelCrdtHashTombstone(void* data, CrdtMeta* meta, int* comapre) {
    return updateMaxDelCrdtLWWHashTombstone(data, meta, comapre);
}

int compareCrdtHashTombstone(void* data, CrdtMeta* meta) {
    return compareCrdtLWWHashTombstone(data, meta);
}

CRDT_HashTombstone* dupCrdtHashTombstone(void* data) {
    return (CRDT_HashTombstone*)dupCrdtLWWHashTombstone(data);
}

int hash_gc_stats = 1;

int hashStartGc() {
    hash_gc_stats = 1;
    return hash_gc_stats;
}

int hashStopGc() {
    hash_gc_stats = 0;
    return hash_gc_stats;
}

int gcCrdtHashTombstone(void* data, VectorClock clock) {
    if(!hash_gc_stats) {
        return 0;
    }
    return gcCrdtLWWHashTombstone(data, clock);
}

VectorClock getCrdtHashTombstoneLastVc(CRDT_HashTombstone* t) {
    return getCrdtLWWHashTombstoneLastVc(retrieveCrdtLWWHashTombstone(t));
}

void mergeCrdtHashTombstoneLastVc(CRDT_HashTombstone* t, VectorClock vc) {
    CRDT_LWW_HashTombstone* tombstone = retrieveCrdtLWWHashTombstone(t);
    setCrdtLWWHashTombstoneLastVc(tombstone, vectorClockMerge(getCrdtLWWHashTombstoneLastVc(tombstone), vc));
}

CrdtMeta* getMaxDelCrdtHashTombstone(void* data) {
    return getCrdtLWWHashTombstoneMaxDelMeta(retrieveCrdtLWWHashTombstone(data));
}

int changeCrdtHashTombstone(void* data, CrdtMeta* meta) {
    return changeCrdtLWWHashTombstone(data, meta);
}

/**
 *  LWW Hash Get Set Function
 */ 
VectorClock getCrdtHashLastVc(CRDT_Hash* hash) {
    CRDT_LWW_Hash* r = retrieveCrdtLWWHash(hash);
    return getCrdtLWWHashLastVc(r);
}

void setCrdtHashLastVc(CRDT_Hash* hash, VectorClock vc) {
    CRDT_LWW_Hash* r = retrieveCrdtLWWHash(hash);
    setCrdtLWWHashLastVc(r, vc);
}

void mergeCrdtHashLastVc(CRDT_Hash* hash, VectorClock vc) {
    VectorClock old = getCrdtHashLastVc(hash);
    VectorClock now = vectorClockMerge(old, vc);
    setCrdtHashLastVc(hash, now);
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
        .digest = crdtHashDigestFunc,
        .lookup_swapping_clients = lookupSwappingClientsWk,
        .setup_swapping_clients = setupSwappingClientsWk,
        .get_data_swaps = getDataSwapsWk,
        .get_complement_swaps = getComplementSwapsWk,
        .swap_ana = swapAnaWk
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
                                  hsetCommand, NULL,"write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HMSET",
                                  hsetCommand, NULL,"write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HSETNX",
                                  hsetnxCommand, NULL, "write deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HGET",
                                  hgetCommand, NULL,"readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HMGET",
                                  hmgetCommand, NULL,"readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"HGETALL",
                                  hgetallCommand, NULL,"readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"HKEYS",
                                  hkeysCommand, NULL,"readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"HVALS",
                                  hvalsCommand, NULL,"readonly fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"HDEL",
                                  hdelCommand, NULL,"write fast swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.HSET",
                                  CRDT_HSetCommand, NULL,"write deny-oom allow-loading swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.HGET",
                                  CRDT_HGetCommand, NULL,"readonly deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"CRDT.hdatainfo",
                                  CRDT_HDataInfoCommand, NULL,"readonly deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_HASH",
                                  CRDT_DelHashCommand, NULL,"write allow-loading swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"CRDT.REM_HASH",
                                  CRDT_RemHashCommand, NULL,"write allow-loading swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"hlen",
                                  hlenCommand, NULL,"readonly deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"hscan",
                                  hscanCommand, NULL,"readonly deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hexists", 
                                    hexistsCommand, NULL, "readonly deny-oom swap-get",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
/**
 *  Basic utils
*/
int sioLoadDict(sio *io, int encver, dict *map, sioRedisModuleTypeLoadFunc func) {
    uint64_t len;
    sds field;
    CRDT_Register *value;
    size_t strLength;
    len = sioLoadUnsigned(io);
    if (len == RDB_LENERR) return CRDT_NO;
    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = sioLoadStringBuffer(io, &strLength);
        field = sdsnewlen(str, strLength);
        value = func(io, encver);
        /* Add pair to hash table */
        dictAdd(map, field, value);
        RedisModule_ZFree(str);
    }
    return CRDT_OK;
}
int sioLoadCrdtBasicHash(sio *io, int encver, void *data) {
    CRDT_Hash* hash = data;
    return sioLoadDict(io, encver, hash->map, sioLoadCrdtRegister);
}
void sioSaveDict(sio *io, dict* map, sioRedisModuleTypeSaveFunc func) {
    
    dictIterator *di = dictGetSafeIterator(map);
    dictEntry *de;
    sioSaveUnsigned(io, dictSize(map));
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        void *crdtRegister = dictGetVal(de);

        sioSaveStringBuffer(io, field, sdslen(field));
        func(io, crdtRegister);
    }
    dictReleaseIterator(di);
}
void sioSaveCrdtBasicHash(sio *io, void *value) {
    CRDT_Hash* crdtHash = value;
    sioSaveDict(io, crdtHash->map, sioSaveCrdtRegister);
}

void sioSaveLWWCrdtRegisterTombstone(sio *io, void *value);
void sioSaveCrdtBasicHashTombstone(sio *io, void *value) {
    CRDT_HashTombstone* crdtHashTombstone = retrieveCrdtHashTombstone(value);
     sioSaveDict(io, crdtHashTombstone->map, sioSaveLWWCrdtRegisterTombstone);
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

void *sioLoadCrdtRegisterTombstone(sio *io, int encver);
int sioLoadCrdtBasicHashTombstone(sio *io, int encver, void *data) {
    CRDT_HashTombstone* tombstone = data;
    return sioLoadDict(io, encver, tombstone->map, sioLoadCrdtRegisterTombstone);
}
size_t crdtBasicHashMemUsageFunc(void* data) {
    CRDT_Hash* result = retrieveCrdtHash(data);
    return dictSize(result->map) ;
}
size_t crdtBasicHashTombstoneMemUsageFunc(void* data) {
    CRDT_HashTombstone* result = retrieveCrdtHashTombstone(data);
    return dictSize(result->map) ;
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
    dictIterator* di = dictGetSafeIterator(tombstone->map);
    dictEntry* de = NULL;
    while((de = dictNext(di)) != NULL) {
        //if counter  will update lastvc
        // CRDT_RegisterTombstone *tom =  dictGetVal(de);
        dictDelete(tombstone->map, dictGetKey(de));
    }
    dictReleaseIterator(di);
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
void freeHashFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtHash(filters[i]);
    }
    RedisModule_Free(filters);
}
void freeHashTombstoneFilter(CrdtObject** filters, int num) {
    for(int i = 0; i < num; i++) {
        freeCrdtHashTombstone(filters[i]);
    }
    RedisModule_Free(filters);
}

CrdtObject** crdtHashFilter2(CrdtObject* common, int gid, VectorClock min_vc,long long maxsize,int* num) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(common);
    CRDT_Hash** result = NULL;
    dictIterator *di = dictGetSafeIterator(crdtHash->map);
    dictEntry *de;
    CRDT_Hash* hash = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        int length = 0;
        CRDT_Register **filted = crdtRegisterFilter2(crdtRegister, gid, min_vc, maxsize, &length);
        if(length == -1) {
            freeRegisterFilter(filted, length);
            freeHashFilter((CrdtObject**)result, *num);
            *num = -1;
            RedisModule_Debug(logLevel, "[CRDT][FILTER] hash key {%s} value too big", field);
            //clean all
            return NULL;
        }
        if(length != 0) {
            if(current_memory + sdslen(field) + crdtRegisterMemUsageFunc(filted[0]) > maxsize) {
                hash = NULL;
            }
            
            if(hash == NULL) {
                hash = createCrdtHash();
                current_memory = 0;
                hash->map->type = &crdtHashFileterDictType;
                (*num)++;
                if(result) {
                    result = RedisModule_Realloc(result, sizeof(CRDT_Hash*) * (*num));
                }else {
                    result = RedisModule_Alloc(sizeof(CRDT_Hash*));
                }
                result[(*num)-1] = hash;
            }
            current_memory += sdslen(field) + sdslen(getCrdtRegisterSds(filted[0]));
            dictAdd(hash->map, field, filted[0]);
            updateLastVCHash(hash, getCrdtRegisterLastVc(filted[0]));
        }
        freeRegisterFilter(filted, length);
    }
    dictReleaseIterator(di);
    if(hash != NULL) {
        setCrdtHashLastVc(hash, getCrdtHashLastVc(crdtHash));
    }
    return (CrdtObject**)result;
}


CrdtObject** crdtHashFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* num) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(common);
    CRDT_Hash** result = NULL;
    dictIterator *di = dictGetSafeIterator(crdtHash->map);
    dictEntry *de;
    CRDT_Hash* hash = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        int length = 0;
        CRDT_Register **filted = filterRegister(crdtRegister, gid, logic_time, maxsize, &length);
        if(length == -1) {
            freeRegisterFilter(filted, length);
            freeHashFilter((CrdtObject**)result, *num);
            *num = -1;
            RedisModule_Debug(logLevel, "[CRDT][FILTER] hash key {%s} value too big", field);
            //clean all
            return NULL;
        }
        if(length != 0) {
            if(current_memory + sdslen(field) + crdtRegisterMemUsageFunc(filted[0]) > maxsize) {
                hash = NULL;
            }
            
            if(hash == NULL) {
                hash = createCrdtHash();
                current_memory = 0;
                hash->map->type = &crdtHashFileterDictType;
                (*num)++;
                if(result) {
                    result = RedisModule_Realloc(result, sizeof(CRDT_Hash*) * (*num));
                }else {
                    result = RedisModule_Alloc(sizeof(CRDT_Hash*));
                }
                result[(*num)-1] = hash;
            }
            current_memory += sdslen(field) + sdslen(getCrdtRegisterSds(filted[0]));
            dictAdd(hash->map, field, filted[0]);
            updateLastVCHash(hash, getCrdtRegisterLastVc(filted[0]));
        }
        freeRegisterFilter(filted, length);
    }
    dictReleaseIterator(di);
    if(hash != NULL) {
        setCrdtHashLastVc(hash, getCrdtHashLastVc(crdtHash));
    }
    return (CrdtObject**)result;
}
int crdtHashTombstonePurge( CrdtObject* tombstone, CrdtObject* current) {
    if(!isCrdtHash((void*)current)) {
        return 0;
    }
    if(!isCrdtHashTombstone((void*)tombstone)) {
        return 0;
    }
    CRDT_Hash* crdtHash = retrieveCrdtHash(current);
    CRDT_HashTombstone* crdtHashTombstone = retrieveCrdtHashTombstone(tombstone);
    dictIterator *di = dictGetSafeIterator(crdtHash->map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
         sds field = dictGetKey(de);    
        CRDT_Register *crdtRegister = dictGetVal(de);
        CrdtMeta* lastMeta = createCrdtRegisterLastMeta(crdtRegister);
        if(compareCrdtHashTombstone(crdtHashTombstone, lastMeta) > COMPARE_META_EQUAL) {
            dictDelete(crdtHash->map, field);
            freeCrdtMeta(lastMeta);
            continue;
        }
        dictEntry *existDe = dictFind(crdtHashTombstone->map, field);
         if(existDe != NULL) {
           
            CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(existDe);
            int r = purgeRegisterTombstone(crdtRegisterTombstone, crdtRegister) ;
            if(r == PURGE_VAL) {
                dictDelete(crdtHash->map, field);
            } else if(r == PURGE_TOMBSTONE) {
                dictDelete(crdtHashTombstone->map, field);
             }
             
           
         }
         freeCrdtMeta(lastMeta);
     }
     dictReleaseIterator(di);

    if(dictSize(crdtHash->map) == 0) {
        return PURGE_VAL;
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

CrdtObject** crdtHashTombstoneFilter2(CrdtTombstone* common, int gid, VectorClock min_vc,long long maxsize,int* num) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(common);
    // VectorClockUnit unit = getVectorClockUnit(getCrdtHashTombstoneLastVc(target), gid);
    // if(isNullVectorClockUnit(unit)) return NULL;
    // long long vcu = get_logic_clock(unit);
    // if(vcu < logic_time) return NULL;
    if(!not_less_than_vc(min_vc, getCrdtHashTombstoneLastVc(target))) {
        return NULL;
    }
    CRDT_HashTombstone** result = NULL;
    dictIterator *di = dictGetSafeIterator(target->map);
    dictEntry *de;
    CRDT_HashTombstone* tombstone = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        int length = 0;
        CRDT_RegisterTombstone **filted = crdtRegisterTombstoneFilter2(crdtRegister, gid, min_vc, maxsize, &length);
        if(length == -1) {
            freeRegisterTombstoneFilter(filted, length);
            freeHashFilter((CrdtObject**)result, *num);
            *num = -1;
            RedisModule_Debug(logLevel, "[CRDT][FILTER] hash tombstone key {%s} value too big", field);
            //clean all
            return NULL;
        }
        if(length != 0) {
            if(current_memory + sdslen(field) + crdtRegisterTombstoneMemUsageFunc(filted[0]) > maxsize) {
                tombstone = NULL;
            }
            if(tombstone == NULL) {
                tombstone = createCrdtHashFilterTombstone((CRDT_HashTombstone*)target);
                current_memory = 0;
                tombstone->map->type = &crdtHashFileterDictType;
                (*num)++;
                if(result) {
                    result = RedisModule_Realloc(result, sizeof(CRDT_HashTombstone*) * (*num));
                }else {
                    result = RedisModule_Alloc(sizeof(CRDT_HashTombstone*));
                }
                result[(*num)-1] = tombstone;
            }
            current_memory += sdslen(field) ;
            dictAdd(tombstone->map, field, filted[0]);
            mergeCrdtHashTombstoneLastVc(tombstone,  getCrdtRegisterLastVc(filted[0]));
        }
        freeRegisterTombstoneFilter(filted, length);
    }
    dictReleaseIterator(di);
    if(!tombstone) {
        if(isNullVectorClock(target->maxDelvectorClock)) {
            return NULL;
        } else {
            tombstone = createCrdtHashFilterTombstone((CRDT_HashTombstone*)target);
            tombstone->map->type = &crdtHashFileterDictType;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(CRDT_HashTombstone*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(CRDT_HashTombstone*));
            }
            result[(*num)-1] = tombstone;
        }
        
    }
    if(tombstone) {
        mergeCrdtHashTombstoneLastVc(tombstone, getCrdtHashTombstoneLastVc((CRDT_HashTombstone*)target));
    }
    return (CrdtObject**)result;
}

CrdtObject** crdtHashTombstoneFilter(CrdtTombstone* common, int gid, long long logic_time, long long maxsize, int* num) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(common);
    VectorClockUnit unit = getVectorClockUnit(getCrdtHashTombstoneLastVc(target), gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    long long vcu = get_logic_clock(unit);
    if(vcu < logic_time) return NULL;
    CRDT_HashTombstone** result = NULL;
    dictIterator *di = dictGetSafeIterator(target->map);
    dictEntry *de;
    CRDT_HashTombstone* tombstone = NULL;
    int current_memory = 0;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        int length = 0;
        CRDT_RegisterTombstone **filted = filterRegisterTombstone(crdtRegister, gid, logic_time, maxsize, &length);
        if(length == -1) {
            freeRegisterTombstoneFilter(filted, length);
            freeHashFilter((CrdtObject**)result, *num);
            *num = -1;
            RedisModule_Debug(logLevel, "[CRDT][FILTER] hash tombstone key {%s} value too big", field);
            //clean all
            return NULL;
        }
        if(length != 0) {
            if(current_memory + sdslen(field) + crdtRegisterTombstoneMemUsageFunc(filted[0]) > maxsize) {
                tombstone = NULL;
            }
            if(tombstone == NULL) {
                tombstone = createCrdtHashFilterTombstone((CRDT_HashTombstone*)target);
                current_memory = 0;
                tombstone->map->type = &crdtHashFileterDictType;
                (*num)++;
                if(result) {
                    result = RedisModule_Realloc(result, sizeof(CRDT_HashTombstone*) * (*num));
                }else {
                    result = RedisModule_Alloc(sizeof(CRDT_HashTombstone*));
                }
                result[(*num)-1] = tombstone;
            }
            current_memory += sdslen(field) ;
            dictAdd(tombstone->map, field, filted[0]);
            mergeCrdtHashTombstoneLastVc(tombstone,  getCrdtRegisterLastVc(filted[0]));
        }
        freeRegisterTombstoneFilter(filted, length);
    }
    dictReleaseIterator(di);
    if(!tombstone) {
        if(isNullVectorClock(target->maxDelvectorClock)) {
            return NULL;
        } else {
            tombstone = createCrdtHashFilterTombstone((CRDT_HashTombstone*)target);
            tombstone->map->type = &crdtHashFileterDictType;
            (*num)++;
            if(result) {
                result = RedisModule_Realloc(result, sizeof(CRDT_HashTombstone*) * (*num));
            }else {
                result = RedisModule_Alloc(sizeof(CRDT_HashTombstone*));
            }
            result[(*num)-1] = tombstone;
        }
        
    }
    if(tombstone) {
        mergeCrdtHashTombstoneLastVc(tombstone, getCrdtHashTombstoneLastVc((CRDT_HashTombstone*)target));
    }
    return (CrdtObject**)result;
}

VectorClock clone_ht_vc(void* ht) {
    return dupVectorClock(getCrdtHashTombstoneLastVc(ht));
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

CRDT_HashTombstone* createCrdtHashFilterTombstone(CRDT_HashTombstone* target) {
    return (CRDT_HashTombstone*)createCrdtLWWHashFilterTombstone(retrieveCrdtLWWHashTombstone(target));
}
