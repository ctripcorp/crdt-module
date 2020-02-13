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

static RedisModuleType *CrdtHash;
/***
 * ==================================== Predefined functions ===========================================*/

int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int hgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int hmgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int hkeysCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int hvalsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int hgetallCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int hdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int CRDT_HGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int CRDT_DelHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int CRDT_RemHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* --------------------------------------------------------------------------
 * Module API for Hash type
 * -------------------------------------------------------------------------- */

void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver);

void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value);

void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value);

size_t crdtHashMemUsageFunc(const void *value);

void crdtHashDigestFunc(RedisModuleDigest *md, void *value);


int isCrdtHash(void* data) {
    CRDT_Hash* tombstone = (CRDT_Hash*)data;
    if(tombstone != NULL && tombstone->common.type == CRDT_HASH_TYPE) {
        return CRDT_OK;
    }
    return CRDT_NO;
}
CRDT_Hash *dupCrdtHash(CRDT_Hash *current) {
    CRDT_Hash *copy = createCrdtHash();
    copy->common.gid = current->common.gid;
    copy->common.vectorClock = dupVectorClock(current->common.vectorClock);

    if (dictSize(current->map)) {
        dictIterator *di = dictGetIterator(current->map);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds field = dictGetKey(de);
            CRDT_Register *crdtRegister = dictGetVal(de);

            dictAdd(copy->map, sdsdup(field), dupCrdtRegister(crdtRegister));
        }
        dictReleaseIterator(di);
    }
    return copy;
}
/* --------------------------------------------------------------------------
 * CrdtCommon API for Hash type
 * -------------------------------------------------------------------------- */

void *crdtHashMerge(void *currentVal, void *value) {

    CRDT_Hash *curHash = currentVal, *targetHash = value;
    if (curHash == NULL) {
        return dupCrdtHash(targetHash);
    }
    CRDT_Hash *result = dupCrdtHash(curHash);

    dictIterator *di = dictGetIterator(targetHash->map);
    dictEntry *de, *existDe;
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        CRDT_Register *crdtRegister = dictGetVal(de);

        existDe = dictFind(result->map, field);
        if (existDe == NULL) {
            dictAdd(result->map, sdsdup(field), dupCrdtRegister(crdtRegister));
        } else {
            CRDT_Register *currentRegister = dictGetVal(existDe);
            CRDT_Register *newRegister = crdtRegisterMerge(currentRegister, crdtRegister);
            freeCrdtRegister(currentRegister);
            dictGetVal(existDe) = newRegister;
        }

    }
    dictReleaseIterator(di);

    VectorClock *toFree = result->common.vectorClock;
    result->common.vectorClock = vectorClockMerge(curHash->common.vectorClock, targetHash->common.vectorClock);
    freeVectorClock(toFree);

    return result;

}

int deleteAll(CRDT_Hash* tombstone, CrdtCommon* common, CRDT_Hash* current) {
    int deleted = 0;
    dictIterator *di = dictGetSafeIterator(current->map);
    dictEntry *de, *exit;
    while ((de = dictNext(di)) != NULL) {
        CRDT_Register *item = dictGetVal(de);
        int result = compareCommon(&item->common, common) ;
        if(result > COMPARE_COMMON_EQUAL) {
            sds field = dictGetKey(de);
            dictEntry *existDe = dictFind(tombstone->map,field);
            if(existDe != NULL) {
                CRDT_Register* t = dictGetVal(existDe);
                crdtCommonMerge(&t->common, common);
            } else {
                CRDT_Register* t = dupCrdtRegister(item);
                if(t->val != NULL) sdsfree(t->val);
                t->val = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);
                crdtCommonMerge(&t->common, common);
                dictAdd(tombstone->map, sdsdup(field), t);
            }
            if (dictDelete(current->map, field) == DICT_OK) {
                deleted++;     
            }
        }
    }
    dictReleaseIterator(di);
    return deleted;
}

int crdtHashDelete(void *ctx, void *keyRobj, void *key, void *value) {
    if(value == NULL) {
        return CRDT_ERROR;
    }
    if(!isCrdtHash(value)) {
        return CRDT_ERROR;
    }
    CrdtCommon* common = createIncrCommon();
    CRDT_Hash* current = (CRDT_Hash*) value;
    RedisModuleKey *moduleKey = (RedisModuleKey *) key;
    CRDT_Hash* tombstone = getTombstone(moduleKey);
    if(tombstone != NULL) {
        if(!isCrdtHash(tombstone)) {
            tombstone = NULL;
        }
    } 
    if(tombstone == NULL) {
        tombstone = createCrdtHash();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHash, tombstone);
    }
    crdtCommonCp(common, tombstone);
    deleteAll(tombstone, common, current);

    sds vcSds = vectorClockToSds(common->vectorClock);
    sds maxDeletedVclock = vectorClockToSds(current->common.vectorClock);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_Hash", "sllcc", keyRobj, common->gid, common->timestamp, vcSds, maxDeletedVclock);
    sdsfree(vcSds);
    sdsfree(maxDeletedVclock);
    freeCommon(common);
    return CRDT_OK;
}
/***
 * ==================================== CRDT Hash Module Init ===========================================*/

static CrdtCommonMethod CrdtHashCommonMethod = {
    merge : crdtHashMerge,
    delFunc : crdtHashDelete
};
int initCrdtHashModule(RedisModuleCtx *ctx) {
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

/* --------------------------------------------------------------------------
 * Dict Functions
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

/* Db->dict, keys are sds strings, vals are CRDT Registers. */
dictType crdtHashDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtRegisterDestructor   /* val destructor */
};

/***
 * ==================================== CRDT Lifecycle functionality ===========================================*/

int crdtHtNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

void *createCrdtHash(void) {
    dict *hash = dictCreate(&crdtHashDictType, NULL);
    CRDT_Hash *crdtHash = RedisModule_Alloc(sizeof(CRDT_Hash));

    crdtHash->common.method = &CrdtHashCommonMethod;
    crdtHash->common.vectorClock = NULL;
    crdtHash->common.timestamp = -1;
    crdtHash->common.gid = (int) RedisModule_CurrentGid();
    crdtHash->common.type = CRDT_HASH_TYPE;

    crdtHash->map = hash;
    return crdtHash;
}

void freeCrdtHash(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_Hash *crdtHash = (CRDT_Hash *)obj;
    if (crdtHash->map) {
        dictRelease(crdtHash->map);
        crdtHash->map = NULL;
    }
    crdtHash->common.method = NULL;

    freeVectorClock(crdtHash->common.vectorClock);

    RedisModule_Free(crdtHash);
}

/* --------------------------------------------------------------------------
 * Internal API for Hash type
 * -------------------------------------------------------------------------- */
CRDT_Hash *crdtHashTypeLookupWriteOrCreate(RedisModuleCtx *ctx, RedisModuleString *key, int gid,
        VectorClock *vclock) {

    RedisModuleKey *moduleKey;
    moduleKey = RedisModule_OpenKey(ctx, key,
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(moduleKey);

    CRDT_Hash *target = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        target = RedisModule_ModuleTypeGetValue(moduleKey);
    }

    if (target == NULL) {
        target = createCrdtHash();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, target);
    } else {
        if (RedisModule_ModuleTypeGetType(moduleKey) != CrdtHash) {
            RedisModule_CloseKey(moduleKey);
            return NULL;
        }
    }
    VectorClock *currentVc = target->common.vectorClock;
    target->common.gid = gid;
    target->common.vectorClock = vectorClockMerge(currentVc, vclock);
    if (currentVc != NULL) {
        freeVectorClock(currentVc);
    }
    return target;
}

#define CRDT_HASH_REM (1<<1)
#define CRDT_HASH_DELETE (1<<0)
/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found.
 * crdtHash could be NULL, as it might be deleted already/or not yet
 * and we need to store the tombstone*/
int crdtHashTypeDelete(CRDT_Hash *crdtHash, sds field, CRDT_Hash *tombstone, int gid,
        VectorClock *vclock, VectorClock *maxDelVclock, int flag) {
    int deleted = 0;
    dictEntry *de;
    CRDT_Register *crdtRegister;
    if (crdtHash != NULL) {
        de = dictFind(crdtHash->map, field);
        if (!de) {
            return deleted;
        }
        crdtRegister = dictGetVal(de);
        if (!isVectorClockMonoIncr(crdtRegister->common.vectorClock, maxDelVclock)) {
            // delete happens-before the hset operation, do nothing
            return deleted;
        }

        if (dictDelete(crdtHash->map, field) == DICT_OK) {
            deleted = 1;
            /* Always check if the dictionary needs a resize after a delete. */
            if (crdtHtNeedsResize(crdtHash->map)) dictResize(crdtHash->map);
        }
    }

    if (flag & CRDT_HASH_REM) {
        crdtRegister = createCrdtRegister();
        crdtRegister->val = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);
        VectorClock *toFree = crdtRegister->common.vectorClock;
        crdtRegister->common.gid = gid;
        crdtRegister->common.vectorClock = dupVectorClock(vclock);
        if (toFree) {
            freeVectorClock(toFree);
        }
        sds fieldKey = sdsdup(field);
        if (!dictReplace(tombstone->map, fieldKey, crdtRegister)) {
            sdsfree(fieldKey);
        }
    }

    return deleted;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found.
 * crdtHash could be NULL, as it might be deleted already/or not yet
 * and we need to store the tombstone*/
static int hashTypeDelete(CRDT_Hash *crdtHash, sds field, CRDT_Hash *tombstone, int gid, VectorClock *vclock) {
    int deleted = 0;
    dictEntry *de;
    CRDT_Register *crdtRegister;
    if (crdtHash != NULL) {
        de = dictFind(crdtHash->map, field);
        if (!de) {
            return deleted;
        }

        if (dictDelete(crdtHash->map, field) == DICT_OK) {
            deleted = 1;
            /* Always check if the dictionary needs a resize after a delete. */
            if (crdtHtNeedsResize(crdtHash->map)) dictResize(crdtHash->map);
        }
    }
    de = dictFind(tombstone->map, field);
    int toCreate = 0;
    if (de) {
        crdtRegister = dictGetVal(de);
    } else {
        crdtRegister = createCrdtRegister();
        toCreate = 1;
    }
    VectorClock *toFree = crdtRegister->common.vectorClock;
    crdtRegister->common.gid = gid;
    crdtRegister->common.vectorClock = dupVectorClock(vclock);
    if(crdtRegister->val != NULL) sdsfree(crdtRegister->val);
    crdtRegister->val = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);
    if (toFree) {
        freeVectorClock(toFree);
    }

    if (toCreate) {
        dictAdd(tombstone->map, sdsdup(field), crdtRegister);
    }
    return deleted;
}

/**
 *  Error 0
 */
#define CHANGE_HASH_ERR -1
#define NO_CHANGE_HASH 0
#define ADD_HASH 1
#define UPDATE_HASH 2
int addOrUpdateItem(RedisModuleCtx* ctx, CRDT_Hash* tombstone, CRDT_Hash* current, CrdtCommon* common,RedisModuleString* key, sds field, sds value) {
    int result_code = NO_CHANGE_HASH;
    CRDT_Register* tombstoneValue = NULL;
    if(tombstone) {
        dictEntry* tomDe = dictFind(tombstone->map, field);
        if(tomDe) {
            tombstoneValue = dictGetVal(tomDe);
        }
    }
    dictEntry* de = dictFind(current->map, field);
    if(de == NULL) {
        CRDT_Register* v = addRegister(tombstoneValue, common, value);
        if(v != NULL) {
            dictAdd(current->map, sdsdup(field), v);
            result_code = ADD_HASH;
        }
    }else{
        CRDT_Register* v = dictGetVal(de);
        sds prev = crdtRegisterInfo(v);
        int result = tryUpdateRegister(tombstoneValue, common, v, value);
        if(isConflictCommon(result)) {
            //add data conflict log
            const char* keyStr = RedisModule_StringPtrLen(key, NULL);
            CRDT_Register* incomeValue = addRegister(NULL, common, value);
            sds income = crdtRegisterInfo(incomeValue);
            sds future = crdtRegisterInfo(v);
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH] {key: %s, field: %s} [prev] {%s} [income] {%s} [future] {%s}",
                    keyStr, field, prev, income, future);
            freeCrdtRegister(incomeValue);
            sdsfree(income);
            sdsfree(future);
            RedisModule_IncrCrdtConflict();
        }
        sdsfree(prev);
        if(result > COMPARE_COMMON_EQUAL) {
            result_code = UPDATE_HASH;
        }
    }
    return result_code;
}
int addOrUpdateHash(RedisModuleCtx* ctx, RedisModuleString* key, RedisModuleKey* moduleKey,CRDT_Hash* tombstone, CRDT_Hash* current, CrdtCommon* common , RedisModuleString** argv, int start_index, int argc) {
    int need_created = CRDT_NO;
    if(current != NULL) {
        if(!isCrdtHash(current)) {
            const char* keyStr = RedisModule_StringPtrLen(key, NULL);
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                            keyStr ,current->common.type);
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
        int result = addOrUpdateItem(ctx, tombstone, current, common, key, field, value);
        if(result > NO_CHANGE_HASH) changed++;
    }
    if(changed > 0) {
        current->common.gid = common->gid;
        current->common.timestamp = common->timestamp;
        freeVectorClock(current->common.vectorClock);
        current->common.vectorClock = dupVectorClock(common->vectorClock);
        if(need_created == CRDT_OK) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, current);
            return ADD_HASH;
        }
        return UPDATE_HASH;
    }
    if(need_created == CRDT_OK)  freeCrdtHash(current);
    return NO_CHANGE_HASH;
}
/* --------------------------------------------------------------------------
 * User API for Hash type
 * -------------------------------------------------------------------------- */
int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
    if ((argc % 2) == 1) {
        return RedisModule_ReplyWithError(ctx, "wrong number of arguments for HSET/HMSET");
    }
    CrdtCommon* common =  createIncrCommon();
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if (moduleKey == NULL) {
        return 0;
    }
    CRDT_Hash* current = getCurrentValue(moduleKey);
    int result = addOrUpdateHash(ctx, argv[1], moduleKey, NULL, current,common, argv, 2, argc);
end:
    if (common != NULL) {
        //send crdt.hset command peer and slave
        sds vclockStr = vectorClockToSds(common->vectorClock);
        size_t argc_repl = (size_t) (argc - 2);
        void *argv_repl = (void *) (argv + 2);
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.HSET", "sllclv", argv[1], common->gid, common->timestamp, vclockStr, (long long) (argc-2), argv_repl, argc_repl);
        sdsfree(vclockStr);
        freeCommon(common);
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
// "CRDT.HSET", <key>, <gid>, <timestamp>, <vclockStr>,  <length> <field> <val> <field> <val> . . .);
//   0           1        2       3           4           5       6
int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    CrdtCommon* common = getCommon(ctx, argv, 2);
    if (common == NULL) {
        return 0;
    }
    RedisModuleKey* moduleKey =  getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if (moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CrdtCommon* t = getTombstone(moduleKey);
    CRDT_Hash* tombstone = NULL;
    if ( t != NULL ) {
        if (!isCrdtHash(t)) {
            if (compareCommon(common, t) > COMPARE_COMMON_EQUAL) {
                goto end;
            }
        }else{
            tombstone = (CrdtCommon*)t;
        }
    }
    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(addOrUpdateHash(ctx, argv[1], moduleKey, tombstone, current, common, argv, 6, argc) == CHANGE_HASH_ERR) {
        status = CRDT_ERROR;
        goto end;
    }
    RedisModule_MergeVectorClock(common->gid, common->vectorClock);
end:
    if (common != NULL) {
        if (common->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCommon(common);
    }
    if (moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithSimpleString(ctx, "OK"); 
    }else{
        return CRDT_ERROR;
    }
}
//hdel key field
int hdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if(argc < 3) return RedisModule_WrongArity(ctx);
    int status = CRDT_OK;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if(moduleKey == NULL) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtCommon* t = getTombstone(moduleKey);
    CRDT_Hash* tombstone = NULL;
    if(t != NULL) {
        if(isCrdtHash(t)) {
            tombstone = (CRDT_Hash*) t;
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtHash();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHash, tombstone);
    }

    CrdtCommon* common = createIncrCommon();
    sds tvalue = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);
    sds field = RedisModule_GetSds(argv[2]);
    
    if(addHash(tombstone, common, field, tvalue) == CRDT_OK) {
        crdtCommonMerge(tombstone, common);
    }
    sdsfree(tvalue);

    CRDT_Hash* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
    }
    if(!isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        RedisModule_Log(ctx, logLevel, "[HDELCOMMAND][CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr , current->common.type);
        RedisModule_IncrCrdtConflict();
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        status = CRDT_ERROR;
        goto end;
    }
    dictDelete(current->map, field);
    
end:
    if(common != NULL) {
        sds vcStr = vectorClockToSds(common->vectorClock);
        size_t argc_repl = (size_t) (argc - 2);
        void *argv_repl = (void *) (argv + 2);
        //CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ...
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.REM_HASH", "sllcv", argv[1], common->gid, common->timestamp, vcStr, argv_repl, argc_repl);
        sdsfree(vcStr);
        freeCommon(common);
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }else{
        return CRDT_ERROR;
    }
    
}


/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
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
        return RedisModule_ReplyWithStringBuffer(ctx, crdtRegister->val, sdslen(crdtRegister->val));
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
    CRDT_Hash *crdtHash = NULL;
    int i;

    RedisModule_AutoMemory(ctx);

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
            RedisModule_ReplyWithStringBuffer(ctx, crdtRegister->val, sdslen(crdtRegister->val));
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



/* --------------------------------------------------------------------------
 * CRDT API for Hash type
 * -------------------------------------------------------------------------- */


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

    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithStringBuffer(ctx, crdtRegister->val, sdslen(crdtRegister->val)); 
    RedisModule_ReplyWithLongLong(ctx, crdtRegister->common.gid);
    RedisModule_ReplyWithLongLong(ctx, crdtRegister->common.timestamp);
    sds vclockSds = vectorClockToSds(crdtRegister->common.vectorClock);
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
    CrdtCommon* common = getCommon(ctx, argv, 2);
    if(common == NULL) {
        return CRDT_ERROR;
    }
    int status = CRDT_OK;
    int deleted = 0;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    if(moduleKey == NULL) {
        status = 1;
        goto end;
    }
    CrdtCommon* t = getTombstone(moduleKey);
    CRDT_Hash* tombstone = NULL;
    if(t != NULL) {
        if(isCrdtHash(t)) {
            tombstone = (CRDT_Hash*)t;
        }else{
            int result = compareCommon(t, common);
            if(result < COMPARE_COMMON_EQUAL) {
                goto end;
            } 
        }
    }
    if(tombstone == NULL) {
        tombstone = createCrdtHash();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHash, tombstone);
    }
    crdtCommonMerge(tombstone, common);
    
    CRDT_Hash* current =  getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
    }
    if(!isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr , current->common.type);
        RedisModule_IncrCrdtConflict();      
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE); 
        status = CRDT_ERROR;
        goto end;
    }

    deleted = deleteAll(tombstone, common, current);
    /* Always check if the dictionary needs a resize after a delete. */
    if (crdtHtNeedsResize(current->map)) dictResize(current->map);
    if (dictSize(current->map) == 0) {
        RedisModule_DeleteKey(moduleKey);
    }
    RedisModule_MergeVectorClock(common->gid, common->vectorClock);
end:
    if(common) {
        if (common->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCommon(common);
    }
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }
    
}


int addHash(CRDT_Hash* hash,CrdtCommon* common, sds field, sds value) {
    dictEntry* tde = dictFind(hash->map, field);
    CRDT_Register* tvalue = NULL;
    if(tde != NULL) {
        tvalue = dictGetVal(tde);
        if(compareCommon(common, &tvalue->common) > 0) {
            return CRDT_NO;
        }
    }else{
        tvalue = createCrdtRegister();
        dictAdd(hash->map, sdsdup(field), tvalue);
    }
    crdtCommonCp(common, tvalue);
    if(tvalue->val) sdsfree(tvalue->val);
    tvalue->val = sdsdup(value);
    return CRDT_OK;
}
//CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ....
// 0              1    2     3           4             5
int CRDT_RemHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);
        CrdtCommon* common = getCommon(ctx, argv, 2);
    if(common == NULL) {
        return 0;
    }
    int status = CRDT_OK;
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtHash);
    int deleted = 0;
    if(moduleKey == NULL) {
        status = CRDT_ERROR;
        goto end;
    }
    CrdtCommon* t = getTombstone(moduleKey);
    CRDT_Hash* tombstone = NULL;
    if(t != NULL) {
        if(isCrdtHash(t)) {
            tombstone = (CRDT_Hash*)t;
        }else{
            if(compareCommon(t, common) < COMPARE_COMMON_EQUAL) {
                goto end;
            }
        }
    }
    
    if(tombstone == NULL) {
        tombstone = createCrdtHash();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHash, tombstone);
    }
    crdtCommonMerge(tombstone, common);

    CRDT_Hash* current =  getCurrentValue(moduleKey);
    if(current != NULL && !isCrdtHash(current)) {
        const char* keyStr = RedisModule_StringPtrLen(moduleKey, NULL);
        RedisModule_Log(ctx, logLevel, "[CRDT_RemHashCommand][CONFLICT][CRDT-HASH][type conflict] key:{%s} prev: {%s} ",
                        keyStr ,current->common.type);
        RedisModule_IncrCrdtConflict();      
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE); 
        status = CRDT_ERROR;
        goto end;
    }
    for (int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        if(current != NULL) {
            dictEntry* de = dictFind(current->map, field);
            CRDT_Register* value = dictGetVal(de);
            int result = compareCommon(&value->common, common);
            if(result > COMPARE_COMMON_EQUAL) {
                dictDelete(current->map, field);
                deleted++;
            }
        }
        sds tvalue = sdsnewlen(DELETED_TAG, DELETED_TAG_SIZE);
        addHash(tombstone, common, field, tvalue);
        sdsfree(tvalue);
    }
    if(current != NULL) {
        if(deleted > 0 && crdtHtNeedsResize(current->map)) {
            dictResize(current->map);
        }
        if (dictSize(current->map) == 0) {
            RedisModule_DeleteKey(moduleKey);
        }
    }
    
    RedisModule_MergeVectorClock(common->gid, common->vectorClock);
end:
    if(common) {
        if (common->gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateVerbatim(ctx);
        } else {
            RedisModule_ReplicateVerbatim(ctx);
        }
        freeCommon(common);
    }  
    if(moduleKey) RedisModule_CloseKey(moduleKey);
    if (status == CRDT_OK) {
        return RedisModule_ReplyWithLongLong(ctx, deleted); 
    }else{
        return CRDT_ERROR;
    }   
}

/* --------------------------------------------------------------------------
 * Module API for Hash type
 * -------------------------------------------------------------------------- */

void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_Hash *crdtHash = createCrdtHash();
    crdtHash->common.gid = RedisModule_LoadSigned(rdb);
    crdtHash->common.timestamp = RedisModule_LoadSigned(rdb);

    size_t vcLength, strLength;
    char* vcStr = RedisModule_LoadStringBuffer(rdb, &vcLength);
    sds vclockSds = sdsnewlen(vcStr, vcLength);
    crdtHash->common.vectorClock = sdsToVectorClock(vclockSds);
    sdsfree(vclockSds);
    RedisModule_Free(vcStr);

    uint64_t len;
    sds field;
    CRDT_Register *value;

    len = RedisModule_LoadUnsigned(rdb);
    if (len == RDB_LENERR) return NULL;

    while (len > 0) {
        len--;
        /* Load encoded strings */
        char* str = RedisModule_LoadStringBuffer(rdb, &strLength);
        field = sdsnewlen(str, strLength);
        value = RdbLoadCrdtRegister(rdb, encver);

        /* Add pair to hash table */
        dictAdd(crdtHash->map, field, value);
        RedisModule_Free(str);
    }

    return crdtHash;
}

void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value) {
    CRDT_Hash *crdtHash = value;
    RedisModule_SaveSigned(rdb, crdtHash->common.gid);
    RedisModule_SaveSigned(rdb, crdtHash->common.timestamp);
    sds vclockStr = vectorClockToSds(crdtHash->common.vectorClock);
    RedisModule_SaveStringBuffer(rdb, vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);

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

void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    //todo: currently do nothing when aof
}

size_t crdtHashMemUsageFunc(const void *value) {
    CRDT_Hash *crdtHash = (CRDT_Hash *) value;
    size_t valSize = sizeof(CRDT_Hash) + dictSize(crdtHash->map);
    int vclcokNum = crdtHash->common.vectorClock->length;
    size_t vclockSize = vclcokNum * sizeof(VectorClockUnit) + sizeof(VectorClock);
    return valSize + vclockSize;
}

void crdtHashDigestFunc(RedisModuleDigest *md, void *value) {
//    CRDT_Hash *crdtHash = (CRDT_Hash *) value;
//    RedisModule_DigestAddLongLong(md, crdtHash->common.gid);
//    RedisModule_DigestAddLongLong(md, crdtHash->common.timestamp);
//    sds vclockStr = vectorClockToSds(crdtHash->common.vectorClock);
//    RedisModule_DigestAddStringBuffer(md, (unsigned char *) vclockStr, sdslen(vclockStr));
//    sdsfree(vclockStr);
////    RedisModule_DigestAddStringBuffer(md, (unsigned char *) crdtRegister->val, sdslen(crdtRegister->val));
//    RedisModule_DigestEndSequence(md);
}
