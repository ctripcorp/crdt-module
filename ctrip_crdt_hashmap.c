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
#include "util.h"


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

/* --------------------------------------------------------------------------
 * Module API for Hash type
 * -------------------------------------------------------------------------- */

void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver);

void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value);

void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value);

size_t crdtHashMemUsageFunc(const void *value);

void crdtHashDigestFunc(RedisModuleDigest *md, void *value);

/* --------------------------------------------------------------------------
 * CrdtCommon API for Hash type
 * -------------------------------------------------------------------------- */

void *crdtHashMerge(void *currentVal, void *value) {
    return value;
}

int crdtHashDelete(void *ctx, void *keyRobj, void *key, void *value) {
    return 1;
}
/***
 * ==================================== CRDT Hash Module Init ===========================================*/

static RedisModuleType *CrdtHash;

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
    crdtHash->map = hash;
    return crdtHash;
}

void freeCrdtHash(void *obj) {
    CRDT_Hash *crdtHash = (CRDT_Hash *)obj;
    if (crdtHash->map) {
        dictRelease(crdtHash->map);
        crdtHash->map = NULL;
    }
    crdtHash->common.merge = NULL;
    crdtHash->common.delFunc = NULL;
    if(crdtHash->common.vectorClock) {
        freeVectorClock(crdtHash->common.vectorClock);
    }
    RedisModule_Free(crdtHash);
}

/* --------------------------------------------------------------------------
 * Internal API for Hash type
 * -------------------------------------------------------------------------- */
CRDT_Hash *crdtHashTypeLookupWriteOrCreate(RedisModuleCtx *ctx, RedisModuleString *key) {
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
    return target;
}

#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int crdtHashTypeSet(CRDT_Hash *o, RedisModuleString *argv1, RedisModuleString *argv2,
        int gid, long long timestamp, VectorClock *vclock, int flags) {

    int update = 0;

    sds field = moduleString2Sds(argv1);
    sds val = moduleString2Sds(argv2);
    CRDT_Register *value = createCrdtRegister();
    value->common.gid = gid;
    value->common.timestamp = timestamp;
    value->common.vectorClock = dupVectorClock(vclock);
    value->val = val;

    dictEntry *de = dictFind(o->map, field);

    if (de) {
        freeCrdtRegister(dictGetVal(de));
        if (flags & HASH_SET_TAKE_VALUE) {
            dictGetVal(de) = value;
            value = NULL;
        } else {
            dictGetVal(de) = dupCrdtRegister(value);
        }
        update = 1;
    } else {
        sds f;
        CRDT_Register *v;
        if (flags & HASH_SET_TAKE_FIELD) {
            f = field;
            field = NULL;
        } else {
            f = sdsdup(field);
        }
        if (flags & HASH_SET_TAKE_VALUE) {
            v = value;
            value = NULL;
        } else {
            v = dupCrdtRegister(value);
        }
        dictAdd(o->map,f,v);
    }


    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) freeCrdtRegister(value);
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int crdtHashTypeDelete(CRDT_Hash *o, sds field) {
    int deleted = 0;

    if (dictDelete((dict*)o->map, field) == DICT_OK) {
        deleted = 1;

        /* Always check if the dictionary needs a resize after a delete. */
        if (crdtHtNeedsResize(o->map)) dictResize(o->map);
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
    VectorClock *opVectorClock = getUnitVectorClock(currentVectorClock, gid);
    freeVectorClock(currentVectorClock);
    long long timestamp = RedisModule_Milliseconds();


    int i, created = 0;
    CRDT_Hash *crdtHash;

    if ((argc % 2) == 1) {
        return RedisModule_ReplyWithError(ctx, "wrong number of arguments for HSET/HMSET");
    }

    if ((crdtHash = crdtHashTypeLookupWriteOrCreate(ctx,argv[1])) == NULL) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);;
    }

    for (i = 2; i < argc; i += 2)
        created += !crdtHashTypeSet(crdtHash, argv[i], argv[i+1], (int) gid, timestamp, opVectorClock, HASH_SET_COPY);

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
    int j, deleted = 0, keyremoved = 0;

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

    for (j = 2; j < argc; j++) {
        sds field = moduleString2Sds(argv[j]);
        if (crdtHashTypeDelete(crdtHash, field)) {
            deleted++;
            if (dictSize(crdtHash->map) == 0) {
                RedisModule_DeleteKey(moduleKey);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
    }
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
    CRDT_Hash *crdtHash;
    int i;

    RedisModule_AutoMemory(ctx);

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

    RedisModule_ReplyWithArray(ctx, argc-2);
    int result = 1;
    for (i = 2; i < argc; i++) {
        result &= addCrdtHashFieldToReply(ctx, crdtHash, argv[i]);
    }
    RedisModule_CloseKey(key);
    return result;
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
        return RedisModule_ReplyWithNull(ctx);
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


int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return REDISMODULE_OK;
}

int CRDT_HGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return REDISMODULE_OK;
}

int CRDT_DelHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return REDISMODULE_OK;
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
    size_t vcLength;
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
        RedisModuleString *fld = RedisModule_LoadString(rdb);
        field = moduleString2Sds(fld);
        value = RdbLoadCrdtRegister(rdb, encver);

        /* Add pair to hash table */
        dictAdd(crdtHash->map, field, value);
        RedisModule_Free(fld);
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

    dictIterator *di = dictGetIterator(crdtHash->map);
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
