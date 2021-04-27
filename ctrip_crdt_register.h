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
// Created by zhuchen on 2020/9/25.
//

#ifndef CRDT_MODULE_CTRIP_CRDT_REGISTER_H
#define CRDT_MODULE_CTRIP_CRDT_REGISTER_H
#include "ctrip_crdt_common.h"
#include "ctrip_vector_clock.h"

#include "ctrip_crdt_expire.h"
#include "gcounter/g_counter_element.h"
#define CRDT_RC_DATATYPE_NAME "crdt_rc_v"
#define CRDT_RC_TOMBSTONE_DATATYPE_NAME "crdt_rc_t"

#define CRDT_REGISTER_COUNTER (1 << 1)
#define CRDT_REGISTER_LWW_ELE (1 << 2)


#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)
#define OBJ_SET_XX (1<<1)
#define OBJ_SET_EX (1<<2)
#define OBJ_SET_PX (1<<3)
#define OBJ_SET_KEEPTTL (1<<4)
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

typedef CrdtData CRDT_RC;
typedef CrdtTombstone CRDT_RCTombstone;
//========================= Register moduleType functions =======================





//========================= Register moduleType functions =======================
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtRcMemUsageFunc(const void *value);
void freeCrdtRc(void *crdtRegister);
void crdtRcDigestFunc(RedisModuleDigest *md, void *value);

//========================= CRDT Data functions =======================
int crdtRcDelete(int dbId, void *keyRobj, void *key, void *value);
void crdtRcUpdateLastVC(void* rc, VectorClock vc);
void initCrdtRcFromTombstone(CRDT_RC* rc, CRDT_RCTombstone* t);
sds crdtRcInfo(void* rc);
// int crdtRcTrySetValue(CRDT_RC* rc, CrdtMeta* set_meta, int gslen, gcounter_meta** gs, CrdtTombstone* tombstone, int type, void* val);
VectorClock  getCrdtRcLastVc(void* rc);
void freeRcLastVc(VectorClock vc);
CrdtMeta* getCrdtRcLastMeta(CRDT_RC* rc);
static CrdtDataMethod RcDataMethod = {
    .propagateDel = crdtRcDelete,
    .getLastVC = getCrdtRcLastVc,
    .updateLastVC = crdtRcUpdateLastVC,
    .info = crdtRcInfo,
};

CrdtObject *crdtRcMerge(CrdtObject *currentVal, CrdtObject *value);
CrdtObject** crdtRcFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* length);
CrdtObject** crdtRcFilter2(CrdtObject* common, int gid, VectorClock min_vc, long long maxsize, int* length);

void freeRcFilter(CrdtObject** filters, int num);
static CrdtObjectMethod RcCommonMethod = {
    .merge = crdtRcMerge,
    .filterAndSplit = crdtRcFilter,
    .filterAndSplit2 = crdtRcFilter2,
    .freefilter = freeRcFilter,
};
//========================= CRDT Tombstone functions =======================
CRDT_RCTombstone* createCrdtRcTombstone();
CRDT_RCTombstone* dupCrdtRcTombstone(CRDT_RCTombstone* tombstone);
int crdtRcTombstoneGc(CrdtTombstone* target, VectorClock clock);
CrdtTombstone* crdtRcTombstoneMerge(CrdtTombstone* target, CrdtTombstone* src);
CrdtTombstone** crdtRcTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) ;
CrdtTombstone** crdtRcTombstoneFilter2(CrdtTombstone* target, int gid, VectorClock min_vc, long long maxsize,int* length) ;

void freeCrdtRcTombstoneFilter(CrdtTombstone** filters, int num);
int crdtRcTombstonePurge(CRDT_RCTombstone* tombstone, CRDT_RC* r);
sds crdtRcTombstoneInfo(void* tombstone);
// int mergeRcTombstone(CRDT_RCTombstone* tombstone, CrdtMeta* meta, int del_len, gcounter_meta** del_counter);
VectorClock getCrdtRcTombstoneLastVc(CRDT_RCTombstone* rt);
static CrdtTombstoneMethod RcTombstoneCommonMethod = {
    .merge = crdtRcTombstoneMerge,
    .filterAndSplit =  crdtRcTombstoneFilter,
    .filterAndSplit2 =  crdtRcTombstoneFilter2,
    .freefilter = freeCrdtRcTombstoneFilter,
    .gc = crdtRcTombstoneGc,
    .purge = crdtRcTombstonePurge,
    .info = crdtRcTombstoneInfo,
    .getVc = getCrdtRcTombstoneLastVc,
};
void updateRcTombstoneLastVc(CRDT_RCTombstone* rt, VectorClock vc);

//========================= Virtual functions =======================
CRDT_RC* createCrdtRc();
CRDT_RC* dupCrdtRc(CRDT_RC* rc);
int getRcElementLen(CRDT_RC* rc);
int appendCounter(CRDT_RC* rc, int gid, long long value);
int crdtRcSetValue(CRDT_RC* rc, CrdtMeta* set_meta, sds* es, CrdtTombstone* tombstone,int type, void* val);
int crdtRcTryUpdate(CRDT_RC* rc, CrdtMeta* set_meta, CrdtTombstone* tombstone, void** es, int val_type, void* val);
//========================= RegisterTombstone moduleType functions =======================
void *RdbLoadCrdtRcTombstone(RedisModuleIO *rdb, int encver) ;
void RdbSaveCrdtRcTombstone(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtRcTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtRcTombstoneMemUsageFunc(const void *value);
void freeCrdtRcTombstone(void *obj);
void crdtRcTombstoneDigestFunc(RedisModuleDigest *md, void *value);
void* RdbLoadCrdtOrSetRcTombstone(RedisModuleIO *rdb, int version, int encver);

//========================= public functions =======================
int initRcModule(RedisModuleCtx *ctx);

static RedisModuleType *CrdtRC;
static RedisModuleType *CrdtRCT;
RedisModuleType* getCrdtRc();
RedisModuleType* getCrdtRcTombstone();
int rcStopGc();
int rcStartGc();

//========================= counter functions ============================
// gcounter* addOrCreateCounter(CRDT_RC* rc,  CrdtMeta* meta, int type, void* val);
int tryUpdateCounter(CRDT_RC* rc, CRDT_RCTombstone* tom, int gid, long long timestamp, long long start_clock, long long end_clock, int type,  void* val);

//========================= rc tombstone element functions =========================
CRDT_RCTombstone* createRcTombstone();
void freeRcTombstoneElement(void* element);
CRDT_RCTombstone* dupCrdtRcTombstone(CRDT_RCTombstone* v);
sds initRcTombstoneFromRc(CRDT_RCTombstone *tombstone, CrdtMeta* meta, CRDT_RC* rc);

int get_crdt_rc_value(CRDT_RC* rc, ctrip_value* value);
int get_rc_tag_add_value(CRDT_RC* rc, int gid, ctrip_value* value);
int get_rc_tombstone_tag_add_value(CRDT_RCTombstone* rc, int gid, ctrip_value* value);
sds rcIncrby(CRDT_RC* data,  CrdtMeta* meta, int type, union all_type* value);
int rcTryIncrby(CRDT_RC* current, CRDT_RCTombstone* tombstone,  CrdtMeta* meta,  sds value);
sds rcAdd(CRDT_RC* data, CrdtMeta* meta, sds value);
int rcAdd2(CRDT_RC* data, CrdtMeta* meta, sds val, char* buf);
int rcTryAdd(CRDT_RC* data, CRDT_RCTombstone* tombstone, CrdtMeta* meta, sds value);
int rcTryDel(CRDT_RC* current,CRDT_RCTombstone* tombstone, CrdtMeta* meta, sds value);

#endif //CRDT_MODULE_CTRIP_CRDT_REGISTER_H
