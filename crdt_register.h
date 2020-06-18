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
#include "crdt_expire.h"

#define CRDT_REGISTER_DATATYPE_NAME "crdt_regr"
#define CRDT_REGISTER_TOMBSTONE_DATATYPE_NAME "crdt_regt"

#define DELETED_TAG "deleted"

#define DELETED_TAG_SIZE 7

typedef CrdtObject CRDT_Register ;
int getCrdtRegisterLastGid(CRDT_Register* r);
sds getCrdtRegisterLastValue(CRDT_Register* r);
long long getCrdtRegisterLastTimestamp(CRDT_Register* r);
VectorClock getCrdtRegisterLastVc(void* r);
CrdtMeta* createCrdtRegisterLastMeta(CRDT_Register* reg);

typedef CrdtObject CRDT_RegisterTombstone ;
typedef struct CRDT_Register* (*dupCrdtRegisterFunc)(CRDT_Register*);
typedef int (*delCrdtRegisterFunc)(CRDT_Register*, CrdtMeta*);
typedef sds (*getCrdtRegisterFunc)(CRDT_Register*);
typedef struct CrdtRegisterValue* (*getValueCrdtRegisterFunc)(CRDT_Register*);
typedef int (*setCrdtRegisterFunc)(CRDT_Register* target, CrdtMeta* meta, sds value);
typedef sds (*getInfoCrdtRegisterFunc)(CRDT_Register* target);
typedef struct CRDT_Register* (*filterCrdtRegisterFunc)(CRDT_Register* target,int gid, long long logic_time);
typedef int (*cleanCrdtRegisterFunc)(CRDT_Register* target, CRDT_RegisterTombstone* tombstone);
typedef struct CRDT_Register* (*mergeCrdtRegisterFunc)(CRDT_Register* target, struct CRDT_Register* other);
typedef void (*updateLastVCCrdtRegisterFunc)(CRDT_Register* target, VectorClock vc);
typedef struct CrdtRegisterMethod {
    dupCrdtRegisterFunc dup;
    delCrdtRegisterFunc del;
    getCrdtRegisterFunc get;
    getValueCrdtRegisterFunc getValue;
    setCrdtRegisterFunc set;
    getInfoCrdtRegisterFunc getInfo;
    filterCrdtRegisterFunc filter;
    mergeCrdtRegisterFunc merge;
    updateLastVCCrdtRegisterFunc updateLastVC;
} CrdtRegisterMethod;

typedef int (*isExpireCrdtRegisterTombstoneFunc)(CRDT_RegisterTombstone* target, CrdtMeta* meta);
typedef CrdtMeta* (*addCrdtRegisterTombstoneFunc)(CRDT_RegisterTombstone* target,CrdtMeta* other);
typedef CRDT_RegisterTombstone* (*filterCrdtRegisterTombstoneFunc)(CRDT_RegisterTombstone* target, int gid, long long logic_time);
typedef CRDT_RegisterTombstone* (*dupCrdtRegisterTombstoneFunc)(CRDT_RegisterTombstone* target);
typedef CRDT_RegisterTombstone* (*mergeRegisterTombstoneFunc)(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other);
typedef int (*purageTombstoneFunc)(CrdtTombstone* tombstone, CrdtObject* obj);
typedef struct CrdtRegisterTombstoneMethod {
    isExpireCrdtRegisterTombstoneFunc isExpire;
    addCrdtRegisterTombstoneFunc add;
    filterCrdtRegisterTombstoneFunc filter;
    dupCrdtRegisterTombstoneFunc dup;
    mergeRegisterTombstoneFunc merge;
    purageTombstoneFunc purage;
} CrdtRegisterTombstoneMethod;



void *createCrdtRegister(void);
void initRegister(CRDT_Register *crdtRegister);
void freeCrdtRegister(void *crdtRegister);
void setCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) ;
int appendCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value, int compare);
int delCrdtRegister(CRDT_Register* current, CrdtMeta* meta);
int initRegisterModule(RedisModuleCtx *ctx);

//register command methods
CrdtObject *crdtRegisterMerge(CrdtObject *currentVal, CrdtObject *value);
int crdtRegisterDelete(int dbId, void *keyRobj, void *key, void *value);
CrdtObject* crdtRegisterFilter(CrdtObject* common, int gid, long long logic_time);
int crdtRegisterTombstonePurage(CrdtTombstone* tombstone, CrdtObject* current);
CRDT_Register* dupCrdtRegister(CRDT_Register *val);

void crdtRegisterUpdateLastVC(void *data, VectorClock vc);
CRDT_Register* filterRegister(CRDT_Register*  common, int gid, long long logic_time);
static CrdtObjectMethod RegisterCommonMethod = {
    .merge = crdtRegisterMerge,
    .filter = crdtRegisterFilter,
};
static CrdtDataMethod RegisterDataMethod = {
    .propagateDel = crdtRegisterDelete,
    .getLastVC = getCrdtRegisterLastVc,
    .updateLastVC = crdtRegisterUpdateLastVC,
};
//register tombstone command methods
CrdtTombstone* crdtRegisterTombstoneMerge(CrdtTombstone* target, CrdtTombstone* other);
CrdtTombstone* crdtRegisterTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time) ;
int crdtRegisterTombstoneGc(CrdtTombstone* target, VectorClock clock);
static CrdtTombstoneMethod RegisterTombstoneMethod = {
    .merge = crdtRegisterTombstoneMerge,
    .filter =  crdtRegisterTombstoneFilter,
    .gc = crdtRegisterTombstoneGc,
    .purage = crdtRegisterTombstonePurage,
};


void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value);

sds crdtRegisterInfo(CRDT_Register *crdtRegister);
sds crdtRegisterInfoFromMetaAndValue(CrdtMeta* meta, sds value);
CRDT_Register* mergeRegister(CRDT_Register* target, CRDT_Register* other, int* conflict);
sds getCrdtRegisterSds(CRDT_Register* r);
CRDT_Register* addRegister(void *tombstone, CrdtMeta* meta, sds value);
int tryUpdateRegister(void* data, CrdtMeta* meta, CRDT_Register* reg, sds value);
void updateRegister(void* data, CrdtMeta* meta, CRDT_Register* reg, sds value, int compare);
int delCrdtRegister(CRDT_Register* current, CrdtMeta* meta);
CrdtMeta* getCrdtRegisterLastMeta(CRDT_Register* target);
void crdtRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value);
size_t crdtRegisterTombstoneMemUsageFunc(const void *value);
void freeCrdtRegisterTombstone(void *obj);
void RdbSaveCrdtRegisterTombstone(RedisModuleIO *rdb, void *value);
void *RdbLoadCrdtRegisterTombstone(RedisModuleIO *rdb, int encver) ;
void AofRewriteCrdtRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);

CRDT_RegisterTombstone* createCrdtRegisterTombstone();
CRDT_RegisterTombstone* dupCrdtRegisterTombstone(CRDT_RegisterTombstone* target);
CRDT_RegisterTombstone* mergeRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other, int* comapre);
CRDT_RegisterTombstone* filterRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time);
int purageRegisterTombstone(CRDT_RegisterTombstone* tombstone, CRDT_Register* target);
CrdtMeta* addRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta, int* compare);
int isExpireCrdtTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta);
#endif //XREDIS_CRDT_CRDT_REGISTER_H
static RedisModuleType *CrdtRegister;
static RedisModuleType *CrdtRegisterTombstone;
RedisModuleType* getCrdtRegister();
RedisModuleType* getCrdtRegisterTombstone();
