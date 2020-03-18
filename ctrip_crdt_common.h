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
#include "ctrip_vector_clock.h"
#include "redismodule.h"

#define CRDT_MODULE_OBJECT_PREFIX "crdt"

#define CRDT_REGISTER_TYPE 1
#define CRDT_REGISTER_TOMBSTONE_TYPE 2
#define CRDT_HASH_TYPE 3
#define CRDT_HASH_TOMBSTONE_TYPE 4
struct CrdtObject;
struct CrdtTombstone;
typedef void *(*crdtMergeFunc)(void *curVal, void *value);
typedef int (*crdtDelFunc)(void *ctx, void *keyRobj, void *key, void *crdtObj);
typedef void* (*crdtFilterFunc)(void* common, long long gid, long long logic_time);
//typedef void (*crdtGcFunc)(void *crdtObj);
typedef int (*crdtCleanFunc)(struct CrdtObject* value, struct CrdtTombstone* tombstone);
typedef int (*crdtGcFunc)(struct CrdtTombstone* value, VectorClock* clock);
typedef int (*crdtPurageFunc)(struct CrdtTombstone* tombstone, struct CrdtObject* obj);
// typedef void* (*crdtDupFunc)(void *data);
typedef struct CrdtMeta {
    int gid;
    long long timestamp;
    VectorClock *vectorClock;
}CrdtMeta;
typedef struct CrdtObjectMethod {
    crdtMergeFunc merge;
    crdtDelFunc del;
    crdtFilterFunc filter;
} CrdtObjectMethod;

typedef struct CrdtObject {
    int type;
    CrdtObjectMethod* method;
} CrdtObject;
typedef struct CrdtTombstoneMethod {
    crdtMergeFunc merge;
    crdtFilterFunc filter;
    crdtGcFunc gc;
    crdtPurageFunc purage;
} CrdtTombstoneMethod;

typedef struct CrdtTombstone {
    int type;
    CrdtTombstoneMethod* method;
} CrdtTombstone;
// CrdtCommon* createCommon(int gid, long long timestamp, VectorClock* vclock);
// CrdtCommon* createIncrCommon();
CrdtMeta* createMeta(int gid, long long timestamp, VectorClock* vclock);
CrdtMeta* createIncrMeta();
CrdtMeta* dupMeta(CrdtMeta* meta);
void appendVCForMeta(CrdtMeta* target, VectorClock* vc);
void freeCrdtMeta(CrdtMeta* meta);
void freeCrdtObject(CrdtObject* object);
void freeCrdtTombstone(CrdtTombstone* tombstone);
// void freeCommon(CrdtCommon* common);

#define COMPARE_COMMON_VECTORCLOCK_GT 1
#define COMPARE_COMMON_VECTORCLOCK_LT -1
#define COMPARE_COMMON_TIMESTAMPE_GT 2
#define COMPARE_COMMON_TIMESTAMPE_LT -2
#define COMPARE_COMMON_GID_GT 3
#define COMPARE_COMMON_GID_LT -3
#define COMPARE_COMMON_EQUAL 0
int compareCrdtMeta(CrdtMeta *a, CrdtMeta *b);
int isConflictMeta(int result);
void crdtMetaCp(CrdtMeta *from, CrdtMeta* to);
int appendCrdtMeta(CrdtMeta *target , CrdtMeta* other);
// int isPartialOrderDeleted(RedisModuleKey *key, VectorClock *vclock);
#endif //REDIS_CTRIP_CRDT_COMMON_H
