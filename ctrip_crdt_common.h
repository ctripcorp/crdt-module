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
#define CRDT_HASH_TYPE 2
typedef void *(*crdtMergeFunc)(void *curVal, void *value);

typedef int (*crdtDelFunc)(void *ctx, void *keyRobj, void *key, void *crdtObj);
typedef struct CrdtCommonMethod {
    //CRDT Merge Function
    crdtMergeFunc merge;
    crdtDelFunc delFunc;
} CrdtCommonMethod;
typedef struct CrdtCommon {
    int gid;
    int type;
    VectorClock *vectorClock;
    long long timestamp;
    CrdtCommonMethod* method;
    //todo: getVectorClock()
} __attribute__((packed, aligned(4))) CrdtCommon;

CrdtCommon* createCommon(int gid, long long timestamp, VectorClock* vclock);
CrdtCommon* createIncrCommon();
void freeCommon(CrdtCommon* common);

#define COMPARE_COMMON_VECTORCLOCK_GT 1
#define COMPARE_COMMON_VECTORCLOCK_LT -1
#define COMPARE_COMMON_TIMESTAMPE_GT 2
#define COMPARE_COMMON_TIMESTAMPE_LT -2
#define COMPARE_COMMON_GID_GT 3
#define COMPARE_COMMON_GID_LT -3
#define COMPARE_COMMON_EQUAL 0
int compareCommon(CrdtCommon *a, CrdtCommon *b);
int isConflictCommon(int result);
void crdtCommonCp(CrdtCommon *from, CrdtCommon* to);
void crdtCommonMerge(CrdtCommon *target , CrdtCommon* other);
int isPartialOrderDeleted(RedisModuleKey *key, VectorClock *vclock);
#endif //REDIS_CTRIP_CRDT_COMMON_H
