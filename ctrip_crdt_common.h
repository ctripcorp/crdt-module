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
// Created by zhuchen on 2019-06-07.
//

#ifndef REDIS_CTRIP_CRDT_COMMON_H
#define REDIS_CTRIP_CRDT_COMMON_H

#include "include/rmutil/sds.h"
#include "include/rmutil/dict.h"
#include "ctrip_vector_clock.h"
#include "redismodule.h"

#define CRDT_MODULE_OBJECT_PREFIX "crdt"


//type
#define CRDT_DATA 1
#define CRDT_TOMBSTONE 2
#define CRDT_EXPIRE 3
#define CRDT_EXPIRE_TOMBSTONE 4
//data type
#define CRDT_REGISTER_TYPE 1
#define CRDT_HASH_TYPE 2


#define LWW_TYPE 1
#define ORSET_TYPE 2
typedef struct CrdtObject {
    char type;
} CrdtObject;
int getDataType(int type);
int setDataType(CrdtObject *obj, int type);
void setType(CrdtObject *obj, int type);
int getType(int type);
typedef CrdtObject CrdtData;
typedef CrdtObject CrdtTombstone;
typedef CrdtObject CrdtExpire;
typedef CrdtObject CrdtExpireTombstone;
typedef void *(*crdtMergeFunc)(void *curVal, void *value);
typedef int (*crdtPropagateDelFunc)(int db_id, void *keyRobj, void *key, void *crdtObj);
typedef void* (*crdtFilterFunc)(void* common, int gid, long long logic_time);
//typedef void (*crdtGcFunc)(void *crdtObj);
typedef int (*crdtCleanFunc)(struct CrdtObject* value, struct CrdtTombstone* tombstone);
typedef int (*crdtGcFunc)(struct CrdtTombstone* value, VectorClock* clock);
typedef int (*crdtPurageFunc)(struct CrdtTombstone* tombstone, struct CrdtObject* obj);

// typedef void* (*crdtDupFunc)(void *data);
typedef struct CrdtMeta {
    unsigned char gid;
    long long timestamp;
    VectorClock vectorClock;
} __attribute__ ((packed, aligned(1))) CrdtMeta;
int getMetaGid(CrdtMeta* meta);
void setMetaVectorClock(CrdtMeta* meta, VectorClock* vc);
long long getMetaTimestamp(CrdtMeta* meta) ;
VectorClock* getMetaVectorClock(CrdtMeta* meta);
int isNullVectorClock(VectorClock vc);

typedef struct CrdtObjectMethod {
    crdtMergeFunc merge;
    crdtFilterFunc filter;
} CrdtObjectMethod;


typedef VectorClock* (*crdtGetLastVCFunc)(void* value);
typedef void* (*crdtUpdateLastVCFunc)(void* value,VectorClock* data);
typedef struct CrdtDataMethod {
    crdtGetLastVCFunc getLastVC;
    crdtUpdateLastVCFunc updateLastVC;
    crdtPropagateDelFunc propagateDel;
} CrdtDataMethod;
// typedef struct CrdtData {
//     CrdtObject parent;
//     // int dataType;
//     // CrdtDataMethod* method;
// } CrdtData;
typedef struct CrdtExpireObj {
    CrdtMeta* meta;
    long long expireTime;
} CrdtExpireObj; 
CrdtExpireObj* createCrdtExpireObj(CrdtMeta* meta, long long expireTime);
CrdtExpireObj* dupExpireObj(CrdtExpireObj* expire);
void freeCrdtExpireObj(CrdtExpireObj* obj);
typedef int (*crdtExpireAddFunc)(struct CrdtExpire* expire, CrdtExpireObj *obj);
typedef long long (*crdtExpireGetLastExpireTimeFunc)(struct CrdtExpire* expire);
typedef int (*crdtExpireGetLastGid)(struct CrdtExpire* expire);
typedef void (*crdtExpireFreeFunc)(void* value);
typedef struct CrdtExpire* (*crdtExpireDupFunc)(struct CrdtExpire* value);
typedef void (*crdtPersistFunc)(struct CrdtExpire* value, RedisModuleKey* modulekey, int dbId, RedisModuleString* key);
typedef struct CrdtExpireMethod {
    crdtExpireAddFunc add;
    crdtExpireGetLastExpireTimeFunc getLastExpireTime;
    crdtExpireGetLastGid getLastGid;
    crdtExpireFreeFunc free;
    crdtExpireDupFunc dup;
    crdtPersistFunc persist;
} CrdtExpireMethod;

typedef struct CrdtTombstoneMethod {
    crdtMergeFunc merge;
    crdtFilterFunc filter;
    crdtGcFunc gc;
    crdtPurageFunc purage;
} CrdtTombstoneMethod;


typedef int (*crdtIsExpireFunc)(void* target, CrdtMeta* meta);
typedef int (*crdtExpireTombstoneAddFunc)(void* target, CrdtMeta* meta);
typedef struct CrdtExpireTombstoneMethod {
    crdtIsExpireFunc isExpire;
    crdtExpireTombstoneAddFunc add;
} CrdtExpireTombstoneMethod;
// CrdtCommon* createCommon(int gid, long long timestamp, VectorClock* vclock);
// CrdtCommon* createIncrCommon();
CrdtMeta* createMeta(int gid, long long timestamp, VectorClock* vclock);
CrdtMeta* createIncrMeta();
CrdtMeta* dupMeta(CrdtMeta* meta);
void appendVCForMeta(CrdtMeta* target, VectorClock* vc);
void freeCrdtMeta(CrdtMeta* meta);
CrdtDataMethod* getCrdtDataMethod(CrdtObject* data);
// void freeCommon(CrdtCommon* common);

#define COMPARE_META_VECTORCLOCK_GT 1
#define COMPARE_META_VECTORCLOCK_LT -1
#define COMPARE_META_TIMESTAMPE_GT 2
#define COMPARE_META_TIMESTAMPE_LT -2
#define COMPARE_META_GID_GT 3
#define COMPARE_META_GID_LT -3
#define COMPARE_META_EQUAL 0
int compareCrdtMeta(CrdtMeta *a, CrdtMeta *b);
int isConflictMeta(int result);
void crdtMetaCp(CrdtMeta *from, CrdtMeta* to);
int appendCrdtMeta(CrdtMeta *target , CrdtMeta* other);
// int isPartialOrderDeleted(RedisModuleKey *key, VectorClock *vclock);


#endif //REDIS_CTRIP_CRDT_COMMON_H
