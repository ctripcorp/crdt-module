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
// Created by zhuchen(zhuchen at ctrip dot com) on 2019-04-16.
//

#ifndef XREDIS_CRDT_CRDT_REGISTER_H
#define XREDIS_CRDT_CRDT_REGISTER_H

#include "include/rmutil/sds.h"
#include "ctrip_crdt_common.h"
#include "include/redismodule.h"
#include "crdt_util.h"
#include "include/rmutil/adlist.h"

#define CRDT_REGISTER_DATATYPE_NAME "crdt_regr"
#define CRDT_REGISTER_TOMBSTONE_DATATYPE_NAME "crdt_regt"

#define DELETED_TAG "deleted"

#define DELETED_TAG_SIZE 7

// typedef struct CRDT_Register {
//     CrdtCommon common;
//     CrdtMeta meta;
//     sds val;
// } CRDT_Register;
typedef struct CRDT_Register;
typedef struct CRDT_Register* (*dupCrdtRegisterFunc)(struct CRDT_Register*);
typedef int (*delCrdtRegisterFunc)(struct CRDT_Register*, CrdtMeta*);
typedef sds (*getCrdtRegisterFunc)(struct CRDT_Register*);
typedef struct CrdtRegisterValue* (*getValueCrdtRegisterFunc)(struct CRDT_Register*);
typedef int (*setCrdtRegisterFunc)(struct CRDT_Register* target, CrdtMeta* meta, sds value);
typedef sds (*getInfoCrdtRegisterFunc)(struct CRDT_Register* target);
typedef struct CRDT_Register* (*filterCrdtRegisterFunc)(struct CRDT_Register* target,long long gid, long long logic_time);
typedef int (*cleanCrdtRegisterFunc)(struct CRDT_Register* target, struct CRDT_RegisterTombstone* tombstone);
typedef struct CRDT_Register* (*mergeCrdtRegisterFunc)(struct CRDT_Register* target, struct CRDT_Register* other);
typedef struct CrdtRegisterMethod {
    dupCrdtRegisterFunc dup;
    delCrdtRegisterFunc del;
    getCrdtRegisterFunc get;
    getValueCrdtRegisterFunc getValue;
    setCrdtRegisterFunc set;
    getInfoCrdtRegisterFunc getInfo;
    filterCrdtRegisterFunc filter;
    mergeCrdtRegisterFunc merge;
} CrdtRegisterMethod;
typedef struct CRDT_Register {
    CrdtObject parent;
    CrdtRegisterMethod* method;
} CRDT_Register;
typedef struct CRDT_RegisterTombstone;
typedef int (*isExpireCrdtRegisterTombstoneFunc)(struct
 CRDT_RegisterTombstone* target, CrdtMeta* meta);
typedef CrdtMeta* (*addCrdtRegisterTombstoneFunc)(struct CRDT_RegisterTombstone* target,struct CrdtMeta* other);
typedef struct CRDT_RegisterTombstone* (*filterCrdtRegisterTombstoneFunc)(struct CRDT_RegisterTombstone* target, long long gid, long long logic_time);
typedef struct CRDT_RegisterTombstone* (*dupCrdtRegisterTombstoneFunc)(struct CRDT_RegisterTombstone* target);
typedef struct CRDT_RegisterTombstone* (*mergeRegisterTombstoneFunc)(struct CRDT_RegisterTombstone* target, struct CRDT_RegisterTombstone* other);
typedef int (*purageTombstoneFunc)(void* tombstone, void* obj);
typedef struct CrdtRegisterTombstoneMethod {
    isExpireCrdtRegisterTombstoneFunc isExpire;
    addCrdtRegisterTombstoneFunc add;
    filterCrdtRegisterTombstoneFunc filter;
    dupCrdtRegisterTombstoneFunc dup;
    mergeRegisterTombstoneFunc merge;
    purageTombstoneFunc purage;
} CrdtRegisterTombstoneMethod;
typedef struct CRDT_RegisterTombstone {
    CrdtTombstone parent;
    CrdtRegisterTombstoneMethod* method;
}CRDT_RegisterTombstone;


typedef struct CrdtRegisterValue {
    CrdtMeta* meta;
    sds value;
} CrdtRegisterValue;
int mergeCrdtRegisterValue(CrdtRegisterValue* target, CrdtRegisterValue* other);
typedef struct CRDT_ORSET_Register {
    CRDT_Register parent;
    list* data;
} CRDT_ORSET_Register;
void *createCrdtRegister(void);

void freeCrdtRegister(void *crdtRegister);



int initRegisterModule(RedisModuleCtx *ctx);

//register command methods
void *crdtRegisterMerge(void *currentVal, void *value);
int crdtRegisterDelete(void *ctx, void *keyRobj, void *key, void *value);
CrdtObject* crdtRegisterFilter(CrdtObject* common, long long gid, long long logic_time);
int crdtRegisterTombstonePurage( CrdtTombstone* tombstone, CrdtObject* current);
int crdtRegisterGc(void* target, VectorClock* clock);
CRDT_Register* dupCrdtRegister(const struct CRDT_Register *val);
static CrdtObjectMethod RegisterCommonMethod = {
    merge: crdtRegisterMerge,
    del: crdtRegisterDelete,
    filter: crdtRegisterFilter,
};
//register tombstone command methods
void* crdtRegisterTombstoneMerge(void* target, void* other);
void* crdtRegisterTombstoneFilter(void* target, long long gid, long long logic_time) ;
int crdtRegisterTombstoneGc(void* target, VectorClock* clock);
static CrdtTombstoneMethod RegisterTombstoneMethod = {
    merge: crdtRegisterTombstoneMerge,
    filter: crdtRegisterTombstoneFilter,
    gc: crdtRegisterTombstoneGc,
    purage: crdtRegisterTombstonePurage,
};


void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value);

sds crdtRegisterInfo(CRDT_Register *crdtRegister);

CRDT_Register* addRegister(void *tombstone, CrdtMeta* meta, sds value);
int tryUpdateRegister(void* data, CrdtMeta* meta, CRDT_Register* reg, sds value);

void crdtRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value);
size_t crdtRegisterTombstoneMemUsageFunc(const void *value);
void freeCrdtRegisterTombstone(void *obj);
void RdbSaveCrdtRegisterTombstone(RedisModuleIO *rdb, void *value);
void *RdbLoadCrdtRegisterTombstone(RedisModuleIO *rdb, int encver) ;
void AofRewriteCrdtRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);

CRDT_RegisterTombstone* createCrdtRegisterTombstone();



#endif //XREDIS_CRDT_CRDT_REGISTER_H
