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
    VectorClock vc;
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
    //todo: len + pointer
    rc_element** elements;
} crdt_rc;

//========================= Register moduleType functions =======================
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtRc(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtRcMemUsageFunc(const void *value);
void freeCrdtRc(void *crdtRegister);
void crdtRcDigestFunc(RedisModuleDigest *md, void *value);

//========================= Virtual functions =======================
int getCrdtRcType(CRDT_RC* rc);
sds getCrdtRcStringValue(CRDT_RC* rc);
long long getCrdtRcIntValue(CRDT_RC* rc);
long double getCrdtRcFloatValue(CRDT_RC* rc);
int setCrdtRcBaseIntValue(CRDT_RC* rc, CrdtMeta* meta, int gid, long long v);
CRDT_RC* createCrdtRc();
int appendCounter(CRDT_RC* rc, int gid, long long value);
int moveDelCounter(CRDT_RC* rc, CRDT_RCTombstone* tom);
rc_element* crdtRcSetValue(CRDT_RC* rc, CrdtMeta* set_meta, sds v);
rc_element* crdtRcTryUpdate(CRDT_RC* rc, CrdtMeta* set_meta, sds key, CrdtTombstone* tombstone);
int isFloat(sds v);
int isInt(sds v);
int setTypeInt(CRDT_RC* rc);
int setTypeFloat(CRDT_RC* rc);
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
//========================= element functions =========================
rc_element* createElement(int gid);
rc_element* findElement(crdt_rc* rc, int gid);
long long  addOrCreateIntCounter(CRDT_RC* rc,  CrdtMeta* meta, long long value);
#endif //CRDT_MODULE_CTRIP_CRDT_REGISTER_H
