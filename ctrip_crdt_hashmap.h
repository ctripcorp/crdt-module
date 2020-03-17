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

#ifndef XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H
#define XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H

#include <string.h>

#include "include/rmutil/sds.h"
#include "ctrip_crdt_common.h"
#include "include/redismodule.h"
#include "include/rmutil/dict.h"
#include "crdt_util.h"
#include <assert.h>

#define CRDT_HASH_DATATYPE_NAME "crdt_hash"
#define CRDT_HASH_TOMBSOTNE_DATATYPE_NAME "crdt_htom"
#define HASHTABLE_MIN_FILL        10

#define UINT64_MAX        18446744073709551615ULL
#define RDB_LENERR UINT64_MAX

#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2

static RedisModuleType *CrdtHash;
static RedisModuleType *CrdtHashTombstone;
//common methods
void *crdtHashMerge(void *currentVal, void *value);
int crdtHashDelete(void *ctx, void *keyRobj, void *key, void *value);
void* crdtHashFilter(void* common, long long gid, long long logic_time);
int crdtHashGc(void* target, VectorClock* clock);

static CrdtObjectMethod HashCommonMethod = {
    merge: crdtHashMerge,
    del: crdtHashDelete,
    filter: crdtHashFilter,
};

//common methods
void *crdtHashTombstoneMerge(void *currentVal, void *value);
void* crdtHashTombstoneFilter(void* common, long long gid, long long logic_time);
int crdtHashTombstoneGc(void* target, VectorClock* clock);
int crdtHashTombstonePurage(void* obj, void* tombstone);
static CrdtTombstoneMethod HashTombstoneCommonMethod = {
    merge: crdtHashTombstoneMerge,
    filter: crdtHashTombstoneFilter,
    gc: crdtHashTombstoneGc,
    purage: crdtHashTombstonePurage,
};

//hash methods
typedef int (*changeCrdtHashFunc)(struct CRDT_Hash* target, CrdtMeta* meta);
typedef struct CRDT_Hash* (*dupCrdtHashFunc)(struct CRDT_Hash* target);
typedef struct CrdtHashMethod {
    changeCrdtHashFunc change;
    dupCrdtHashFunc dup;
} CrdtHashMethod;
typedef struct CRDT_Hash {
    CrdtObject parent;
    CrdtHashMethod* method;
    dict *map;
} CRDT_Hash;
typedef CrdtMeta* (*updateMaxDelCrdtHashTombstoneFunc)(void* target, CrdtMeta* meta);
typedef int (*lapseCrdtHashTombstoneFunc)(void* target, CrdtMeta* meta);
typedef void* (*dupFunc)(void* target);
typedef int (*gcCrdtHashTombstoneFunc)(void* target, VectorClock* clock);
typedef CrdtMeta* (*getMaxDelCrdtHashTombstoneFunc)(void* target);
typedef int (*changeHashTombstoneFunc)(void* target, CrdtMeta* meta);
typedef int (*purageHashTombstoneFunc)(void* tombstone, void* obj);
typedef struct CrdtHashTombstoneMethod {
    updateMaxDelCrdtHashTombstoneFunc updateMaxDel;
    lapseCrdtHashTombstoneFunc lapse;
    dupFunc dup;
    gcCrdtHashTombstoneFunc gc;
    getMaxDelCrdtHashTombstoneFunc getMaxDel;
    changeHashTombstoneFunc change;
    purageHashTombstoneFunc purage;
} CrdtHashTombstoneMethod;
typedef struct CRDT_HashTombstone {
    CrdtTombstone parent;
    CrdtHashTombstoneMethod* method;
    dict *map;
} CRDT_HashTombstone;


void *createCrdtHash(void);
void *createCrdtHashTombstone(void);


int initCrdtHashModule(RedisModuleCtx *ctx);

int crdtHtNeedsResize(dict *dict);

//hash 
void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value);
void freeCrdtHash(void *crdtHash);
size_t crdtHashMemUsageFunc(const void *value);
void crdtHashDigestFunc(RedisModuleDigest *md, void *value);
//hash tombstone
void *RdbLoadCrdtHashTombstone(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtHashTombstone(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtHashTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
void freeCrdtHashTombstone(void *crdtHash);
size_t crdtHashTombstoneMemUsageFunc(const void *value);
void crdtHashTombstoneDigestFunc(RedisModuleDigest *md, void *value);

//other utils
int RdbLoadCrdtBasicHash(RedisModuleIO *rdb, int encver, void *data);
void RdbSaveCrdtBasicHash(RedisModuleIO *rdb, void *value);
int RdbLoadCrdtBasicHashTombstone(RedisModuleIO *rdb, int encver, void *data);
void RdbSaveCrdtBasicHashTombstone(RedisModuleIO *rdb, void *value);
size_t crdtBasicHashMemUsageFunc(void* data);
size_t crdtBasicHashTombstoneMemUsageFunc(void* data);
/**
 * about dict
 */
uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1,
                      const void *key2);
void dictSdsDestructor(void *privdata, void *val);
void dictCrdtRegisterDestructor(void *privdata, void *val);

void dictCrdtRegisterTombstoneDestructor(void *privdata, void *val);
static dictType crdtHashDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtRegisterDestructor   /* val destructor */
};
static dictType crdtHashTombstoneDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictCrdtRegisterTombstoneDestructor   /* val destructor */
};
#endif //XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H
