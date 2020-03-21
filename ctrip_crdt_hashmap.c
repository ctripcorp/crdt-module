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

/*
    util 
*/
int isCrdtHash(void* data) {
    CRDT_Hash* tombstone = (CRDT_Hash*)data;
    if(tombstone != NULL && tombstone->parent.type == CRDT_HASH_TYPE) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
int isCrdtHashTombstone(void *data) {
    CRDT_HashTombstone* tombstone = (CRDT_HashTombstone*)data;
    if(tombstone != NULL && tombstone->parent.type == CRDT_HASH_TOMBSTONE_TYPE) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
CRDT_Hash* retrieveCrdtHash(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_Hash* result = (CRDT_Hash*)t;
    assert(result->parent.type == CRDT_HASH_TYPE);
    assert(result->map != NULL);
    // assert(result->map->type == &crdtHashDictType);
    return result;
}



CRDT_HashTombstone* retrieveCrdtHashTombstone(void* t) {
    if(t == NULL) {
        return NULL;
    }
    CRDT_HashTombstone* result = (CRDT_HashTombstone*)t;
    assert(result->parent.type == CRDT_HASH_TOMBSTONE_TYPE);
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
    CRDT_RegisterTombstone* tombstoneValue = NULL;
    if(tombstone) {
        dictEntry* tomDe = dictFind(tombstone->map, field);
        if(tomDe) {
            tombstoneValue = dictGetVal(tomDe);
        }
    }
    dictEntry* de = dictFind(current->map, field);
    if(de == NULL) {
        CRDT_Register* v = addRegister(tombstoneValue, meta, value);
        if(v != NULL) {
            dictAdd(current->map, sdsdup(field), v);
            result_code = ADD_HASH;
        }
    }else{
        CRDT_Register* v = dictGetVal(de);
        sds prev = v->method->getInfo(v);
        int result = tryUpdateRegister(tombstoneValue, meta, v, value);
        if(isConflictCommon(result)) {
            //add data conflict log
            const char* keyStr = RedisModule_StringPtrLen(key, NULL);
            CRDT_Register* incomeValue = addRegister(NULL, meta, value);
            sds income = incomeValue->method->getInfo(incomeValue);
            sds future = v->method->getInfo(v);
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH] {key: %s, field: %s} [prev] {%s} [income] {%s} [future] {%s}",
                    keyStr, field, prev, income, future);
            freeCrdtRegister(incomeValue);
            sdsfree(income);
            sdsfree(future);
            RedisModule_IncrCrdtConflict();
        }
        sdsfree(prev);
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
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                            keyStr ,current->parent.type);
            RedisModule_IncrCrdtConflict();      
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
        if(result > NO_CHANGE_HASH) changed++;
    }
    if(changed > 0) {
        current->method->change(current, meta);
        if(need_created == CRDT_OK) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, current);
            return ADD_HASH;
        }
        return UPDATE_HASH;
    }
    if(need_created == CRDT_OK)  freeCrdtHash(current);
    return NO_CHANGE_HASH;
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
        RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return RedisModule_ReplyWithNull(ctx);
    }
    CRDT_Register *crdtRegister = hashTypeGetFromHashTable(crdtHash, fld);
    if (crdtRegister == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        sds val = crdtRegister->method->get(crdtRegister);
        return RedisModule_ReplyWithStringBuffer(ctx, val, sdslen(val));
    }
}
/* --------------------------------------------------------------------------
 * User API for Hash type
 * -------------------------------------------------------------------------- */
//hset key f1 v2 f2 v2 ..
int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    if ((argc % 2) == 1) {
        return RedisModule_ReplyWithError(ctx, "wrong number of arguments for HSET/HMSET");
    }
    CrdtMeta* meta =  createIncrMeta();
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if (moduleKey == NULL) {
        return 0;
    }
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(current != NULL) {
        appendVCForMeta(meta, current->method->getLastVC(current));
    }
    int result = addOrUpdateHash(ctx, argv[1], moduleKey, NULL, current,meta, argv, 2, argc);
    if(result == CHANGE_HASH_ERR) {
        goto end;
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "hset", argv[1]);

end:
    if (meta != NULL) {
        //send crdt.hset command peer and slave
        sds vclockStr = vectorClockToSds(meta->vectorClock);
        size_t argc_repl = (size_t) (argc - 2);
        void *argv_repl = (void *) (argv + 2);
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.HSET", "sllclv", argv[1], meta->gid, meta->timestamp, vclockStr, (long long) (argc-2), argv_repl, argc_repl);
        sdsfree(vclockStr);
        freeCrdtMeta(meta);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(result == CHANGE_HASH_ERR) return CRDT_ERROR;
    sds cmdname = RedisModule_GetSds(argv[0]);
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        return RedisModule_ReplyWithLongLong(ctx, result == ADD_HASH? CRDT_OK: CRDT_NO);
    } else {
        /* HMSET */
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
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

void addTombstone(CRDT_HashTombstone* tombstone, sds field, CrdtMeta* meta) {
    dictEntry *tde = dictFind(tombstone->map, field);
    CRDT_RegisterTombstone *t;
    if(tde == NULL) {
        t = createCrdtRegisterTombstone();
        dictAdd(tombstone->map, sdsdup(field), t);
    }else{
        t = dictGetVal(tde);
    }
    t->method->add(t, meta);
}
int hdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 3) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if(moduleKey == NULL) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
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

    CrdtMeta* meta = createIncrMeta();

    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
    }
    appendVCForMeta(meta, current->method->getLastVC(current));
    if(!isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        RedisModule_Log(ctx, logLevel, "[HDELCOMMAND][CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr , current->parent.type);
        RedisModule_IncrCrdtConflict();
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        status = CRDT_ERROR;
        goto end;
    }
    
    RedisModuleString** deleted_objs = RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString*) * (argc-2));
    for(int j = 2; j < argc; j++) {
        sds field = RedisModule_GetSds(argv[j]);
        if(dictDelete(current->map, field) == DICT_OK) {
            addTombstone(tombstone, field, meta);
            deleted_objs[deleted] = argv[j];
            deleted++;  
        }
    }
    if (crdtHtNeedsResize(current->map)) dictResize(current->map);
    
    if(deleted > 0) {
        current->method->change(current, meta);
        tombstone->method->change(tombstone, meta);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH,"hdel", argv[1]);
    }    

    if (dictSize(current->map) == 0) {
        RedisModule_DeleteKey(moduleKey);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
    }
end:
    if(meta != NULL) {
        sds vcStr = vectorClockToSds(meta->vectorClock);
        size_t argc_repl = (size_t) deleted;
        void *argv_repl = (void *) deleted_objs;
        //CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ...
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.REM_HASH", "sllcv", argv[1], meta->gid, meta->timestamp, vcStr, argv_repl, argc_repl);
        sdsfree(vcStr);
        freeCrdtMeta(meta);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }else{
        return CRDT_ERROR;
    }
    
}
// "CRDT.HSET", <key>, <gid>, <timestamp>, <vclockStr>,  <length> <field> <val> <field> <val> . . .);
//   0           1        2       3           4           5       6
int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    if (meta == NULL) {
        return 0;
    }
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if (moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_HashTombstone* tombstone = NULL;
    if ( t != NULL && isCrdtHashTombstone(t)) {
        tombstone = retrieveCrdtHashTombstone(t);
        if(tombstone->method->isExpire(tombstone, meta) == CRDT_OK) {
            goto end;
        }
    }
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(addOrUpdateHash(ctx, argv[1], moduleKey, tombstone, current, meta, argv, 6, argc) == CHANGE_HASH_ERR) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "hset", argv[1]);
end:
    if (meta != NULL) {
        if (meta->gid == RedisModule_CurrentGid()) {
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
        return RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }
    
    if (crdtHash == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }
    sds fld = RedisModule_GetSds(argv[2]);
    if(fld == NULL) {
        RedisModule_ReplyWithError(ctx, "WRONGTYPE Operation against a key holding the wrong kind of value");
        return RedisModule_ReplyWithNull(ctx);
    }
    CRDT_Register *crdtRegister = hashTypeGetFromHashTable(crdtHash, fld);     
    if (crdtRegister == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    } 
    CrdtRegisterValue* value =  crdtRegister->method->getValue(crdtRegister);
    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithStringBuffer(ctx, value->value, sdslen(value->value)); 
    RedisModule_ReplyWithLongLong(ctx, value->meta->gid);
    RedisModule_ReplyWithLongLong(ctx, value->meta->timestamp);
    sds vclockSds = vectorClockToSds(value->meta->vectorClock);
    RedisModule_ReplyWithStringBuffer(ctx, vclockSds, sdslen(vclockSds));
    sdsfree(vclockSds);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}
//CRDT.DEL_HASH <key> gid timestamp <del-op-vclock> <max-deleted-vclock>
// 0              1    2     3           4                  5
int CRDT_DelHashCommand(RedisModuleCtx* ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    CrdtMeta* meta = getMeta(ctx, argv, 2);
    if(meta == NULL) {
        return CRDT_ERROR;
    }
    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if(moduleKey == NULL) {
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
    tombstone->method->updateMaxDel(tombstone, meta);
    tombstone->method->change(tombstone, meta);
    CRDT_Hash* current =  getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
    }
    if(!isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr , current->parent.type);
        RedisModule_IncrCrdtConflict();      
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE); 
        status = CRDT_ERROR;
        goto end;
    }

    
    dictIterator *di = dictGetSafeIterator(current->map);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        CRDT_Register *kv = dictGetVal(de);
        if(kv->method->del(kv, meta)) {
            dictDelete(current->map, dictGetKey(de));
            deleted++;
        }
    }
    dictReleaseIterator(di);
    /* Always check if the dictionary needs a resize after a delete. */
    if (crdtHtNeedsResize(current->map)) dictResize(current->map);
    if (dictSize(current->map) == 0) {
        RedisModule_DeleteKey(moduleKey);
    }
    
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
end:
    if(meta) {
        if (meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
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
        if(tombstone->method->isExpire(tombstone, meta) == CRDT_OK) {
            goto end;
        }
    }
    
    if(tombstone == NULL) {
        tombstone = createCrdtHashTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHashTombstone, tombstone);
    }
    

    CRDT_Hash* current =  getCurrentValue(moduleKey);
    if(current != NULL && !isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        RedisModule_Log(ctx, logLevel, "[CRDT_RemHashCommand][CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr ,current->parent.type);
        RedisModule_IncrCrdtConflict();      
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE); 
        status = CRDT_ERROR;
        goto end;
    }
    int keyremoved = CRDT_NO;
    for (int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        if(current != NULL) {
            dictEntry* de = dictFind(current->map, field);
            if(de != NULL) {
                CRDT_Register* value = dictGetVal(de);
                if(value->method->del(value, meta) == CRDT_OK) {
                    dictDelete(current->map, field);
                    deleted++;
                }
            }
        }
        addTombstone(tombstone, field, meta);
    }
    if(current != NULL) {
        if(deleted > 0 && crdtHtNeedsResize(current->map)) {
            current->method->change(current, meta);
            dictResize(current->map);
        }
        if (dictSize(current->map) == 0) {
            RedisModule_DeleteKey(moduleKey);
            keyremoved = CRDT_OK;
        }
    }
    
    RedisModule_MergeVectorClock(meta->gid, meta->vectorClock);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "hdel", argv[1]);
    if(keyremoved == CRDT_OK) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[1]);
    }
end:
    if(meta) {
        if (meta->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
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
            sds val = crdtRegister->method->get(crdtRegister);
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
int RdbLoadDict(RedisModuleIO *rdb, int encver, void *data, RedisModuleTypeLoadFunc func) {
    CRDT_Hash* crdtHash = data;
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
        dictAdd(crdtHash->map, field, value);
        RedisModule_Free(str);
    }
    return CRDT_OK;
}
int RdbLoadCrdtBasicHash(RedisModuleIO *rdb, int encver, void *data) {
    return RdbLoadDict(rdb, encver, data, RdbLoadCrdtRegister);
}
void RdbSaveCrdtBasicHash(RedisModuleIO *rdb, void *value) {
    CRDT_Hash* crdtHash = value;
    dictIterator *di = dictGetSafeIterator(crdtHash->map);
    dictEntry *de;
    RedisModule_SaveUnsigned(rdb, dictSize(crdtHash->map));
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        void *crdtRegister = dictGetVal(de);

        RedisModule_SaveStringBuffer(rdb, field, sdslen(field));
        RdbSaveCrdtRegister(rdb, crdtRegister);
    }
    dictReleaseIterator(di);
}
void RdbSaveCrdtBasicHashTombstone(RedisModuleIO *rdb, void *value) {
    CRDT_HashTombstone* crdtHashTombstone = retrieveCrdtHashTombstone(value);
    dictIterator *di = dictGetSafeIterator(crdtHashTombstone->map);
    dictEntry *de;
    RedisModule_SaveUnsigned(rdb, dictSize(crdtHashTombstone->map));
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        void *crdtRegisterTombstone = dictGetVal(de);

        RedisModule_SaveStringBuffer(rdb, field, sdslen(field));
        RdbSaveCrdtRegisterTombstone(rdb, crdtRegisterTombstone);
    }
    dictReleaseIterator(di);
}
CrdtMeta* appendBasicHash(CRDT_Hash* target, CRDT_Hash* other) {
    CrdtMeta* result = createMeta(-1, -1, NULL);
    dictIterator *di = dictGetIterator(other->map);
    dictEntry *de, *existDe;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        existDe = dictFind(target->map, field);
        if (existDe == NULL) {
            dictAdd(target->map, sdsdup(field), crdtRegister->method->dup(crdtRegister));
        } else {
            CRDT_Register *currentRegister = dictGetVal(existDe);
            CRDT_Register *newRegister = crdtRegister->method->merge(currentRegister, crdtRegister);
            freeCrdtRegister(currentRegister);
            appendCrdtMeta(result, newRegister->method->getValue(crdtRegister)->meta);
            dictGetVal(existDe) = newRegister;
        }
    }
    dictReleaseIterator(di);
    return result;
}

int RdbLoadCrdtBasicHashTombstone(RedisModuleIO *rdb, int encver, void *data) {
    return RdbLoadDict(rdb, encver, data, RdbLoadCrdtRegisterTombstone);
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
void *crdtHashMerge(void *currentVal, void *value) {
    CRDT_Hash* target = retrieveCrdtHash(currentVal);
    CRDT_Hash* other = retrieveCrdtHash(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return other->method->dup(value);
    }
    CRDT_Hash *result = target->method->dup(target);
    CrdtMeta* meta = appendBasicHash(result, other);
    target->method->change(target, meta);
    freeCrdtMeta(meta);
    return result;
}
int crdtHashDelete(void *ctx, void *keyRobj, void *key, void *value) {
    if(value == NULL) {
        return CRDT_ERROR;
    }
    if(!isCrdtHash(value)) {
        return CRDT_ERROR;
    }
    CrdtMeta* meta = createIncrMeta();
    CRDT_Hash* current = (CRDT_Hash*) value;
    appendVCForMeta(meta, current->method->getLastVC(current));
    RedisModuleKey *moduleKey = (RedisModuleKey *) key;
    CRDT_HashTombstone* tombstone = getTombstone(key);
    if(tombstone == NULL || !isCrdtHashTombstone(tombstone)) {
        tombstone = createCrdtHashTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHashTombstone, tombstone);
    }
    current->method->change(current, meta);
    CrdtMeta* result = tombstone->method->updateMaxDel(tombstone, meta);
    tombstone->method->change(tombstone, meta);
    sds vcSds = vectorClockToSds(result->vectorClock);
    // sds maxDeletedVclock = vectorClockToSds(current->method->lastWriteVc(current));
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_Hash", "sllcc", keyRobj, meta->gid, meta->timestamp, vcSds, vcSds);
    sdsfree(vcSds);
    // sdsfree(maxDeletedVclock);
    freeCrdtMeta(meta);
    return CRDT_OK;
}

void* crdtHashFilter(void* common, long long gid, long long logic_time) {
    CRDT_Hash* crdtHash = retrieveCrdtHash(common);
    CRDT_Hash* result = createCrdtHash();
    dictIterator *di = dictGetSafeIterator(crdtHash->map);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);
        CRDT_Register *filted = crdtRegister->method->filter(crdtRegister, gid, logic_time);
        if(filted != NULL) {
            dictAdd(result->map, sdsdup(field), filted);
            CrdtMeta* meta = filted->method->getValue(filted)->meta;
            result->method->change(result, meta);
        }
    }
    dictReleaseIterator(di);
    if(dictSize(result->map) == 0) {
        freeCrdtHash(result);
        return NULL;
    }
    return result;
}
int crdtHashTombstonePurage( void* tombstone, void* current) {
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
            if(crdtHashTombstone->method->isExpire(crdtHashTombstone, crdtRegister->method->getValue(crdtRegister)->meta) == CRDT_OK
               || crdtRegisterTombstone->method->purage(crdtRegisterTombstone, crdtRegister)) {
                dictDelete(crdtHash->map, field);
            }
            
        }
    }
    dictReleaseIterator(di);
    if(dictSize(crdtHash->map) == 0) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
int crdtHashGc(void* target, VectorClock* clock) {
    return CRDT_NO;
}

//tombstone common methods
void *crdtHashTombstoneMerge(void *currentVal, void *value) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(currentVal);
    CRDT_HashTombstone* other = retrieveCrdtHashTombstone(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return other->method->dup(value);
    }
    CRDT_HashTombstone *result = target->method->dup(target);
    dictIterator *di = dictGetIterator(other->map);
    dictEntry *de, *existDe;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
        existDe = dictFind(target->map, field);
        if (existDe == NULL) {
            dictAdd(target->map, sdsdup(field), crdtRegisterTombstone->method->dup(crdtRegisterTombstone));
        } else {
            CRDT_RegisterTombstone *currentRegisterTombstone = dictGetVal(existDe);
            CRDT_RegisterTombstone *newRegisterTombstone = crdtRegisterTombstone->method->merge(currentRegisterTombstone, crdtRegisterTombstone);
            freeCrdtRegisterTombstone(currentRegisterTombstone);
            dictGetVal(existDe) = newRegisterTombstone;
        }
    }
    dictReleaseIterator(di);
    result->method->updateMaxDel(result,other->method->getMaxDel(other));
    return result;
}

void* crdtHashTombstoneFilter(void* common, long long gid, long long logic_time) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(common);
    CRDT_HashTombstone* result = target->method->dup(target);
    dictEmpty(result->map, NULL);
    dictIterator *di = dictGetIterator(target->map);
    dictEntry *de, *existDe;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_RegisterTombstone *crdtRegisterTombstone = dictGetVal(de);
        CRDT_RegisterTombstone *filter = crdtRegisterTombstone->method->filter(crdtRegisterTombstone, gid, logic_time);
        if(filter != NULL) {
            dictAdd(result->map, sdsdup(field), filter);
        }
    }
    dictReleaseIterator(di);
    return result;
}

int crdtHashTombstoneGc(void* common, VectorClock* clock) {
    CRDT_HashTombstone* target = retrieveCrdtHashTombstone(common);
    return target->method->gc(target, clock);
}