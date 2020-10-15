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
#include "gcounter/crdt_g_counter.h"
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

typedef CrdtObject CRDT_RC;
typedef CrdtTombstone CRDT_RCTombstone;
typedef struct {
    unsigned char type;
    long long unit;
    long long timespace;
    union {
        long long i;
        long double f;
    }conv;
} rc_base;

typedef struct {
    char gid; //tag
//    unsigned char flag; //COUNTER, LWW-ELEMENT
    rc_base *base;
    gcounter *counter;
//    void *del_counter;
}rc_element;

typedef struct {
    unsigned char type;
    unsigned char len;
    VectorClock vectorClock;
    rc_element** elements;
} crdt_rc;

//========================= Register moduleType functions =======================
typedef struct {
    long long gid: 4; //tag
//    unsigned char flag; //COUNTER, LWW-ELEMENT
    long long del_unit: 60;
    gcounter *counter;
//    void *del_counter;
} rc_tombstone_element;
typedef struct {
    unsigned char type;
    unsigned char len;
    VectorClock vectorClock;
    //todo: len + pointer
    rc_tombstone_element** elements;
} crdt_rc_tombstone;

CRDT_RCTombstone* createCrdtRcTombstone();


//========================= Register moduleType functions =======================
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtRcMemUsageFunc(const void *value);
void freeCrdtRc(void *crdtRegister);
void crdtRcDigestFunc(RedisModuleDigest *md, void *value);

//========================= CRDT Data functions =======================
int crdtRcDelete(int dbId, void *keyRobj, void *key, void *value);
VectorClock  getCrdtRcLastVc(crdt_rc* rc);
void crdtRcUpdateLastVC(CRDT_RC* rc, VectorClock vc);
void initCrdtRcFromTombstone(CRDT_RC* rc, CRDT_RCTombstone* t);
static CrdtDataMethod RcDataMethod = {
    .propagateDel = crdtRcDelete,
    .getLastVC = getCrdtRcLastVc,
    .updateLastVC = crdtRcUpdateLastVC,
    // .info = crdtRcInfo,
};

CrdtObject *crdtRcMerge(CrdtObject *currentVal, CrdtObject *value);
CrdtObject** crdtRcFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* length);
void freeRcFilter(CrdtObject** filters, int num);
static CrdtObjectMethod RcCommonMethod = {
    .merge = crdtRcMerge,
    .filterAndSplit = crdtRcFilter,
    .freefilter = freeRcFilter,
};
//========================= CRDT Tombstone functions =======================
int rcTombstoneGc(CrdtTombstone* target, VectorClock clock);
static CrdtTombstoneMethod RcTombstoneCommonMethod = {
    // .merge = rcTombstoneMerge,
    // .filterAndSplit =  rcTombstoneFilter,
    // .freefilter = freeRcTombstoneFilter,
    .gc = rcTombstoneGc,
    // .purge = crdtRcTombstonePurge,
    // .info = crdtRcTombstoneInfo,
};

void updateRcTombstoneLastVc(CRDT_RCTombstone* rt, VectorClock vc);
VectorClock getCrdtRcTombstoneLastVc(crdt_rc_tombstone* rt);
//========================= Virtual functions =======================
int getCrdtRcType(CRDT_RC* rc);
long long getCrdtRcIntValue(CRDT_RC* rc);
long double getCrdtRcFloatValue(CRDT_RC* rc);
CRDT_RC* createCrdtRc();
CRDT_RC* dupCrdtRc(CRDT_RC* rc);
int appendCounter(CRDT_RC* rc, int gid, long long value);
int crdtRcSetValue(CRDT_RC* rc, CrdtMeta* set_meta, sds* es, CrdtTombstone* tombstone,int type, void* val);
int crdtRcTryUpdate(CRDT_RC* rc, CrdtMeta* set_meta, CrdtTombstone* tombstone, rc_element** es, int val_type, void* val);
//========================= RegisterTombstone moduleType functions =======================
void *RdbLoadCrdtRcTombstone(RedisModuleIO *rdb, int encver) ;
void RdbSaveCrdtRcTombstone(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtRcTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtRcTombstoneMemUsageFunc(const void *value);
void freeCrdtRcTombstone(void *obj);
void crdtRcTombstoneDigestFunc(RedisModuleDigest *md, void *value);

//========================= public functions =======================
int initRcModule(RedisModuleCtx *ctx);

static RedisModuleType *CrdtRC;
static RedisModuleType *CrdtRCT;
RedisModuleType* getCrdtRc();
RedisModuleType* getCrdtRcTombstone();
//========================= rc element functions =========================
rc_element* createRcElement(int gid);
rc_element* dupRcElement(rc_element* el);
void assign_rc_element(rc_element* target, rc_element* src);
void freeRcElement(void* element);
int setCrdtRcType(CRDT_RC* rc, int type);
rc_element* findRcElement(crdt_rc* rc, int gid);
int updateFloatCounter(CRDT_RC* rc, int gid, long long timestamp, long long start_clock, long long end_clock, long double ld);
//========================= rc base functions ============================
void freeBase(rc_base* base);
rc_base* createRcElementBase();
void assign_max_rc_base(rc_base* target, rc_base* src);
rc_base* dupRcBase(rc_base* base);
int resetElementBase(rc_base* base, CrdtMeta* meta, int val_type, void* v);
int getRcElementLen(crdt_rc* rc);
//========================= counter functions ============================
long long addOrCreateCounter(CRDT_RC* rc,  CrdtMeta* meta, int type, void* val);


//========================= rc tombstone element functions =========================
rc_tombstone_element* createRcTombstoneElement(int gid);
void freeRcTombstoneElement(void* element);
int appendRcTombstoneElement(crdt_rc_tombstone* rt, rc_tombstone_element* el);
rc_tombstone_element* findRcTombstoneElement(crdt_rc_tombstone* rt, int gid);
#endif //CRDT_MODULE_CTRIP_CRDT_REGISTER_H
