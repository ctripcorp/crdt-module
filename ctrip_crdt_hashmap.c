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
            CRDT_Register *currentRegister = dictGetVal(de);
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


int crdtHashDelete(void *ctx, void *keyRobj, void *key, void *value) {
    if(value == NULL) {
        return 0;
    }
    RedisModuleKey *moduleKey = (RedisModuleKey *) key;
    CRDT_Hash *crdtHash = (CRDT_Hash *) value;

    long long gid = RedisModule_CurrentGid();
    long long timestamp = mstime();
    RedisModule_IncrLocalVectorClock(1);
    VectorClock *vclock = RedisModule_CurrentVectorClock();

    crdtHash->common.gid = (int) gid;
    crdtHash->common.timestamp = timestamp;
    dictEmpty(crdtHash->map, NULL);

    CRDT_Hash *tombstone = dupCrdtHash(crdtHash);
    tombstone->remvAll = CRDT_YES;
    tombstone->maxdvc = dupVectorClock(crdtHash->common.vectorClock);
    freeVectorClock(tombstone->common.vectorClock);
    tombstone->common.vectorClock = dupVectorClock(vclock);
    RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHash, tombstone);

    sds vcSds = vectorClockToSds(vclock);
    sds maxDeletedVclock = vectorClockToSds(crdtHash->common.vectorClock);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_Hash", "sllcc", keyRobj, gid, timestamp, vcSds, maxDeletedVclock);
    sdsfree(vcSds);
    sdsfree(maxDeletedVclock);

    if (vclock) {
        freeVectorClock(vclock);
    }

    return 1;
}
/***
 * ==================================== CRDT Hash Module Init ===========================================*/

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

    crdtHash->common.merge = crdtHashMerge;
    crdtHash->common.delFunc = crdtHashDelete;
    crdtHash->common.vectorClock = NULL;
    crdtHash->common.timestamp = -1;
    crdtHash->common.gid = (int) RedisModule_CurrentGid();
    crdtHash->common.type = CRDT_HASH_TYPE;

    crdtHash->maxdvc = NULL;
    crdtHash->remvAll = CRDT_NO;
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
    crdtHash->common.merge = NULL;
    crdtHash->common.delFunc = NULL;

    freeVectorClock(crdtHash->common.vectorClock);
    freeVectorClock(crdtHash->maxdvc);

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

//todo: align with redis, pass in sds and flags to decide whether to dup or not
int crdtHashTypeSet(RedisModuleCtx *ctx, RedisModuleString *key, CRDT_Hash *crdtHash, CRDT_Hash *tombstone,
        RedisModuleString *fieldArgv, RedisModuleString *valArgv, int gid, long long timestamp, VectorClock *vclock) {

    int update = 0;
    dictEntry *de;

    sds field = moduleString2Sds(fieldArgv);
    if (tombstone != NULL && (de = dictFind(tombstone->map, field)) != NULL) {
        CRDT_Register *crdtRegister = dictGetVal(de);
        if (isVectorClockMonoIncr(vclock, crdtRegister->common.vectorClock)) {
            // has been deleted already
            return update;
        }
    }
    sds val = moduleString2Sds(valArgv);
    CRDT_Register *value = createCrdtRegister();
    value->common.gid = gid;
    value->common.timestamp = timestamp;
    value->common.vectorClock = dupVectorClock(vclock);
    value->val = val;

    de = dictFind(crdtHash->map, field);

    if (de) {
        CRDT_Register *current = dictGetVal(de);
        dictGetVal(de) = crdtRegisterMerge(current, value);

        //todo: align with redis:  dup each time and free in the end
        if (!isVectorClockMonoIncr(current->common.vectorClock, value->common.vectorClock)) {
            const char* keyStr = RedisModule_StringPtrLen(key, NULL);
            sds prev = crdtRegisterInfo(current);
            sds income = crdtRegisterInfo(value);
            sds future = crdtRegisterInfo(dictGetVal(de));
            RedisModule_Log(ctx, logLevel, "[CONFLICT][CRDT-HASH] {key: %s, field: %s} [prev] {%s} [income] {%s} [future] {%s}",
                    keyStr, field, prev, income, future);
            sdsfree(prev);
            sdsfree(income);
            sdsfree(future);
            RedisModule_IncrCrdtConflict();
        }
        freeCrdtRegister(current);
        freeCrdtRegister(value);
        sdsfree(field);
        update = 1;
    } else {
        dictAdd(crdtHash->map, field, value);
    }

    return update;
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
    if (toFree) {
        freeVectorClock(toFree);
    }

    if (toCreate) {
        dictAdd(tombstone->map, sdsdup(field), crdtRegister);
    }
    return deleted;
}
/* --------------------------------------------------------------------------
 * User API for Hash type
 * -------------------------------------------------------------------------- */

int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);

    long long gid = RedisModule_CurrentGid();
    RedisModule_IncrLocalVectorClock(1);
    // Abstract the logic of local set to match the common process
    // Here, we consider the op vclock we're trying to broadcasting is our vcu wrapped vector clock
    // for example, our gid is 1,  our vector clock is (1:100,2:1,3:100)
    // A set operation will firstly drive the vclock into (1:101,2:1,3:100).
    // and due to the op-based crdt, we do an update or effect, which, we believe the vclock is (1:101), because is locally changed
    // and we will let the under layer logic deal with the real op clock
    VectorClock *currentVectorClock = RedisModule_CurrentVectorClock();
    long long timestamp = RedisModule_Milliseconds();


    int i, created = 0;
    CRDT_Hash *crdtHash = NULL;

    if ((argc % 2) == 1) {
        return RedisModule_ReplyWithError(ctx, "wrong number of arguments for HSET/HMSET");
    }

    if ((crdtHash = crdtHashTypeLookupWriteOrCreate(ctx,argv[1], (int) gid, currentVectorClock)) == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);;
    }

    for (i = 2; i < argc; i += 2) {
        created += !crdtHashTypeSet(ctx, argv[1], crdtHash, NULL, argv[i], argv[i + 1], (int) gid, timestamp, currentVectorClock);
    }

    /*
     * sent to both my slaves and my peer masters */
    // "CRDT.HSET", <key>, <gid>, <timestamp>, <vclockStr>, <argc>, <field> <val> <field> <val> . . .);
    sds vclockStr = vectorClockToSds(currentVectorClock);
    size_t argc_repl = (size_t) (argc - 2);
    void *argv_repl = (void *) (argv + 2);
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.HSET", "sllclv", argv[1], gid, timestamp, vclockStr, (long long) (argc-2), argv_repl, argc_repl);
    sdsfree(vclockStr);

    if (currentVectorClock) {
        freeVectorClock(currentVectorClock);
    }

    /* HMSET (deprecated) and HSET return value is different. */
    char *cmdname = moduleString2Sds(argv[0]);

    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        sdsfree(cmdname);
        return RedisModule_ReplyWithLongLong(ctx, created);
    } else {
        /* HMSET */
        sdsfree(cmdname);
        return RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
}

int hdelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int j, deleted = 0;

    RedisModuleKey *moduleKey;
    moduleKey = RedisModule_OpenKey(ctx, argv[1],
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);

    CRDT_Hash *crdtHash = NULL;
    if (RedisModule_KeyType(moduleKey) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    } else if (RedisModule_ModuleTypeGetType(moduleKey) != CrdtHash) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(moduleKey);
    }

    if (crdtHash == NULL) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    RedisModule_IncrLocalVectorClock(1);
    long long gid = RedisModule_CurrentGid();
    long long timestamp = mstime();
    VectorClock *vclock = RedisModule_CurrentVectorClock();

    CRDT_Hash *tombstone = RedisModule_ModuleTypeGetTombstone(moduleKey);
    if (tombstone == NULL || tombstone->common.type != CRDT_HASH_TYPE) {
        tombstone = createCrdtHash();
        tombstone->common.gid = (int) RedisModule_CurrentGid();
        tombstone->common.vectorClock = dupVectorClock(vclock);
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtHash, tombstone);
    }
    for (j = 2; j < argc; j++) {
        sds field = moduleString2Sds(argv[j]);
        if (hashTypeDelete(crdtHash, field, tombstone, (int) gid, vclock)) {
            deleted++;
            if (dictSize(crdtHash->map) == 0) {
                sdsfree(field);
                RedisModule_DeleteKey(moduleKey);
                break;
            }
        }
        sdsfree(field);
    }
    RedisModule_CloseKey(moduleKey);

    sds vcStr = vectorClockToSds(vclock);
    size_t argc_repl = (size_t) (argc - 2);
    void *argv_repl = (void *) (argv + 2);
    //CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ...
    RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.REM_HASH", "sllcv", argv[1], gid, timestamp, vcStr, argv_repl, argc_repl);
    sdsfree(vcStr);
    freeVectorClock(vclock);

    return RedisModule_ReplyWithLongLong(ctx, deleted);
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
    sds fld = moduleString2Sds(field);
    CRDT_Register *crdtRegister = hashTypeGetFromHashTable(crdtHash, fld);
    sdsfree(fld);
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

// "CRDT.HSET", <key>, <gid>, <timestamp>, <vclockStr>,  <length> <field> <val> <field> <val> . . .);
//   0           1        2       3           4           5       6
int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);
    if (argc < 5) return RedisModule_WrongArity(ctx);

    long long gid;
    if ((RedisModule_StringToLongLong(argv[2],&gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[3],&timestamp) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    long long length;
    if ((RedisModule_StringToLongLong(argv[5],&length) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    RedisModuleKey *moduleKey;
    moduleKey = RedisModule_OpenKey(ctx, argv[1],
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(moduleKey);

    CRDT_Hash *current = NULL, *tombstone = NULL;
    //todo: log here
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(moduleKey) != CrdtHash) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    VectorClock *vclock = getVectorClockFromString(argv[4]);
    RedisModule_MergeVectorClock(gid, vclock);
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        current = RedisModule_ModuleTypeGetValue(moduleKey);
    }
    tombstone = RedisModule_ModuleTypeGetTombstone(moduleKey);

    // has been deleted already
    if (tombstone != NULL) {
        //todo: if delete all
        if (tombstone->common.type == CRDT_HASH_TYPE && isVectorClockMonoIncr(vclock, tombstone->maxdvc)) {
            RedisModule_CloseKey(moduleKey);
            return RedisModule_ReplyWithSimpleString(ctx, "OK");
        } else if (tombstone->common.type != CRDT_HASH_TYPE && isVectorClockMonoIncr(vclock, tombstone->common.vectorClock)) {
            RedisModule_CloseKey(moduleKey);
            return RedisModule_ReplyWithSimpleString(ctx, "OK");
        }
    }
    if (tombstone && tombstone->common.type != CRDT_HASH_TYPE) {
        tombstone = NULL;
    }

    VectorClock *opVectorClock = NULL;
    long long i;
    int replicate = CRDT_YES;
    // newly added element
    if (current == NULL) {
        CRDT_Hash *crdtHash = createCrdtHash();
        opVectorClock = vectorClockMerge(NULL, vclock);;
        crdtHash->common.vectorClock = dupVectorClock(opVectorClock);
        crdtHash->common.timestamp = timestamp;
        crdtHash->common.gid = (int) gid;
        for (i = 6; i < argc; i+=2) {
            crdtHashTypeSet(ctx, argv[1], crdtHash, tombstone, argv[i], argv[i + 1], (int) gid, timestamp, crdtHash->common.vectorClock);
        }
        if (dictSize(crdtHash->map) != 0) {
            RedisModule_ModuleTypeSetValue(moduleKey, CrdtHash, crdtHash);
        } else {
            replicate = CRDT_NO;
            freeCrdtHash(crdtHash);
        }
    } else {
        VectorClock *currentVclock = current->common.vectorClock;
        opVectorClock = vectorClockMerge(currentVclock, vclock);
        freeVectorClock(currentVclock);

        current->common.vectorClock = dupVectorClock(opVectorClock);
        current->common.timestamp = max(current->common.timestamp, timestamp);
        current->common.gid = (int) gid;

        for (i = 6; i < argc; i+=2) {
            crdtHashTypeSet(ctx, argv[1], current, tombstone, argv[i], argv[i + 1], (int) gid, timestamp, vclock);
        }
    }
    RedisModule_CloseKey(moduleKey);

    // "CRDT.HSET", <key>, <gid>, <timestamp>, <vclockStr>,  <length> <field> <val> <field> <val> . . .);
    //   0           1        2       3           4           5       6
    if (replicate) {
        sds vclockStr = vectorClockToSds(opVectorClock);
        size_t argc_repl = (size_t) (argc - 6);
        void *argv_repl = (void *) (argv + 6);
        if (gid == RedisModule_CurrentGid()) {
            RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.HSET", "sllclv", argv[1], gid, timestamp, vclockStr,
                                                       (long long) (argc - 6), argv_repl, argc_repl);
        } else {
            RedisModule_ReplicateStraightForward(ctx, "CRDT.HSET", "sllclv", argv[1], gid, timestamp, vclockStr,
                                                 (long long) (argc - 6), argv_repl, argc_repl);
        }
        sdsfree(vclockStr);
    }

    if (opVectorClock) {
        freeVectorClock(opVectorClock);
    }

    if (vclock) {
        freeVectorClock(vclock);
    }

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int CRDT_HGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return REDISMODULE_OK;
}

//CRDT.DEL_HASH <key> gid timestamp <del-op-vclock> <max-deleted-vclock>
// 0              1    2     3           4                  5
int CRDT_DelHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);

    long long gid;
    if ((RedisModule_StringToLongLong(argv[2], &gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid value: must be a signed 64 bit integer");
    }

    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[3], &timestamp) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid value: must be a signed 64 bit integer");
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
    int type = RedisModule_KeyType(key);

    CRDT_Hash *crdtHash = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }

    VectorClock *vclock = getVectorClockFromString(argv[4]);
    RedisModule_MergeVectorClock(gid, vclock);

    VectorClock *maxDelVclock = getVectorClockFromString(argv[5]);

    CRDT_Hash *tombstone = RedisModule_ModuleTypeGetTombstone(key);
    // This deleted has been deprecated
    if (tombstone != NULL && isVectorClockMonoIncr(vclock, tombstone->common.vectorClock)) {
        freeVectorClock(maxDelVclock);
        freeVectorClock(vclock);
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    if (tombstone == NULL || tombstone->common.type != CRDT_HASH_TYPE) {
        tombstone = createCrdtHash();
        RedisModule_ModuleTombstoneSetValue(key, CrdtHash, tombstone);
    }
    VectorClock *toFree = tombstone->common.vectorClock;
    tombstone->common.vectorClock = vectorClockMerge(toFree, vclock);
    freeVectorClock(toFree);

    toFree = tombstone->maxdvc;
    tombstone->maxdvc = vectorClockMerge(toFree, maxDelVclock);
    freeVectorClock(toFree);

    tombstone->remvAll = CRDT_YES;
    tombstone->common.timestamp = timestamp;
    tombstone->common.gid = (int) gid;

    int deleted = 0;
    if (crdtHash != NULL) {
        dictIterator *di = dictGetSafeIterator(crdtHash->map);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            CRDT_Register *crdtRegister = dictGetVal(de);
            if (isVectorClockMonoIncr(crdtRegister->common.vectorClock, maxDelVclock)) {
                sds field = dictGetKey(de);
                crdtHashTypeDelete(crdtHash, field, tombstone, (int) gid, vclock, maxDelVclock, CRDT_HASH_DELETE);
                deleted++;
            }
        }
        dictReleaseIterator(di);
        if (dictSize(crdtHash->map) == 0) {
            RedisModule_DeleteKey(key);
        }
    }

    //CRDT.DEL_HASH <key> gid timestamp <del-op-vclock> <max-deleted-vclock>
    // 0              1    2     3           4                  5
    sds vclockStr = vectorClockToSds(vclock);
    size_t argc_repl = (size_t) (argc - 1);
    void *argv_repl = (void *) (argv + 1);
    if (gid == RedisModule_CurrentGid()) {
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.DEL_HASH", "v", argv_repl, argc_repl);
    } else {
        RedisModule_ReplicateStraightForward(ctx, "CRDT.DEL_HASH", "v", argv_repl, argc_repl);
    }
    sdsfree(vclockStr);

    RedisModule_CloseKey(key);
    if (vclock) {
        freeVectorClock(vclock);
    }
    if (maxDelVclock) {
        freeVectorClock(maxDelVclock);
    }

    return RedisModule_ReplyWithLongLong(ctx, deleted);
}

//CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ....
// 0              1    2     3           4             5
int CRDT_RemHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) return RedisModule_WrongArity(ctx);

    long long gid;
    if ((RedisModule_StringToLongLong(argv[2],&gid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[3],&timestamp) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    VectorClock *vclock = getVectorClockFromString(argv[4]);
    RedisModule_MergeVectorClock(gid, vclock);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);

    CRDT_Hash *tombstone = RedisModule_ModuleTypeGetTombstone(key);
    if (tombstone == NULL || tombstone->common.type != CRDT_HASH_TYPE) {
        tombstone = createCrdtHash();
        RedisModule_ModuleTombstoneSetValue(key, CrdtHash, tombstone);
    }
    VectorClock *toFree = tombstone->common.vectorClock;
    tombstone->common.vectorClock = vectorClockMerge(toFree, vclock);
    tombstone->common.timestamp = timestamp;
    tombstone->common.gid = (int) gid;
    freeVectorClock(toFree);

    int type = RedisModule_KeyType(key);
    CRDT_Hash *crdtHash = NULL;
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != CrdtHash) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        crdtHash = RedisModule_ModuleTypeGetValue(key);
    }

    int i, deleted = 0;
    for (i = 5; i < argc; i++) {
        sds field = moduleString2Sds(argv[i]);
        if (crdtHashTypeDelete(crdtHash, field, tombstone, (int) gid, vclock, vclock, CRDT_HASH_REM)) {
            deleted++;
            if (dictSize(crdtHash->map) == 0) {
                RedisModule_DeleteKey(key);
            }
        }
        sdsfree(field);
    }
    RedisModule_CloseKey(key);

    //CRDT.REM_HASH <key> gid timestamp <del-op-vclock> <field1> <field2> ....
    // 0              1    2     3           4             5
    sds vclockStr = vectorClockToSds(vclock);
    size_t argc_repl = (size_t) (argc - 1);
    void *argv_repl = (void *) (argv + 1);
    if (gid == RedisModule_CurrentGid()) {
        RedisModule_CrdtReplicateAlsoNormReplicate(ctx, "CRDT.REM_HASH", "v", argv_repl, argc_repl);
    } else {
        RedisModule_ReplicateStraightForward(ctx, "CRDT.REM_HASH", "v", argv_repl, argc_repl);
    }
    sdsfree(vclockStr);

    if (vclock) {
        freeVectorClock(vclock);
    }
    return RedisModule_ReplyWithLongLong(ctx, deleted);
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

    crdtHash->remvAll = RedisModule_LoadSigned(rdb);

    vcStr = RedisModule_LoadStringBuffer(rdb, &vcLength);
    vclockSds = sdsnewlen(vcStr, vcLength);
    crdtHash->maxdvc = sdsToVectorClock(vclockSds);
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
    RedisModule_SaveSigned(rdb, crdtHash->remvAll);
    vclockStr = vectorClockToSds(crdtHash->maxdvc);
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
