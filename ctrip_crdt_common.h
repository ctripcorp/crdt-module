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
#define CRDT_DATA 0
#define CRDT_TOMBSTONE 1
//data type
#define CRDT_REGISTER_TYPE 0
#define CRDT_HASH_TYPE 1


#define LWW_TYPE  (0 << 1)
#define ORSET_TYPE (1 << 1) 



#define setTimestampToLongLong(l, timestamp) do { \
    l &= ((long long)0xFFF) ;\
    l |= ((timestamp << 12) & 0xFFFFFFFFFFFFF000);\
} while(0)
#define getTimestampFromLongLong(l) (l >> 12)

#define getGidFromLongLong(l) ((l >> 8) & 0x00F)
#define setGidToLongLong(l, gid) do { \
    l &= 0xFFFFFFFFFFFFF0FF;\
    l |= (((long long)gid << 8) & 0xF00);\
} while(0)

typedef struct CrdtObject {
    unsigned char reserved:3;
    unsigned char type:2;
    unsigned char dataType:3;
} __attribute__ ((packed, aligned(1))) CrdtObject;
int getDataType(CrdtObject *obj);
void setDataType(CrdtObject *obj, int type);
void setType(CrdtObject *obj, int type);
int getType(CrdtObject *obj);
typedef CrdtObject CrdtData;
typedef CrdtObject CrdtTombstone;
typedef CrdtObject *(*crdtMergeFunc)(CrdtObject *curVal, CrdtObject *value);
typedef int (*crdtPropagateDelFunc)(int db_id, void *keyRobj, void *key, void *crdtObj);
typedef CrdtObject* (*crdtFilterFunc)(CrdtObject* common, int gid, long long logic_time);
typedef int (*crdtCleanFunc)( CrdtObject* value, CrdtTombstone* tombstone);
typedef int (*crdtGcFunc)( CrdtTombstone* value, VectorClock clock);
typedef int (*crdtPurgeFunc)(CrdtTombstone* tombstone,  CrdtObject* obj);

typedef struct CrdtMeta {
    unsigned long long type:8;
    unsigned long long gid:4;
    unsigned long long timestamp:52;
    VectorClock vectorClock;
} __attribute__ ((packed, aligned(1))) CrdtMeta;
int getMetaGid(CrdtMeta* meta);
void setMetaVectorClock(CrdtMeta* meta, VectorClock vc);
long long getMetaTimestamp(CrdtMeta* meta) ;
VectorClock getMetaVectorClock(CrdtMeta* meta);
int isNullVectorClock(VectorClock vc);
long long getMetaVectorClockToLongLong(CrdtMeta* meta);
typedef struct CrdtObjectMethod {
    crdtMergeFunc merge;
    crdtFilterFunc filter;
} CrdtObjectMethod;



typedef VectorClock (*crdtGetLastVCFunc)(void* value);
typedef void (*crdtUpdateLastVCFunc)(void* value,VectorClock data);
typedef sds (*crdtInfoFunc)(void* value);
typedef struct CrdtDataMethod {
    crdtGetLastVCFunc getLastVC;
    crdtUpdateLastVCFunc updateLastVC;
    crdtPropagateDelFunc propagateDel;
    crdtInfoFunc info;
} CrdtDataMethod;

typedef struct CrdtTombstoneMethod {
    crdtMergeFunc merge;
    crdtFilterFunc filter;
    crdtGcFunc gc;
    crdtPurgeFunc purge;
    crdtInfoFunc info;
} CrdtTombstoneMethod;


typedef int (*crdtIsExpireFunc)(void* target, CrdtMeta* meta);

CrdtMeta* createMeta(int gid, long long timestamp, VectorClock vclock);
CrdtMeta* createIncrMeta();
CrdtMeta* dupMeta(CrdtMeta* meta);
void appendVCForMeta(CrdtMeta* target, VectorClock vc);
void freeCrdtMeta(CrdtMeta* meta);
CrdtDataMethod* getCrdtDataMethod(CrdtObject* data);

#define COMPARE_META_VECTORCLOCK_GT 1
#define COMPARE_META_VECTORCLOCK_LT -1
#define COMPARE_META_TIMESTAMPE_GT 2
#define COMPARE_META_TIMESTAMPE_LT -2
#define COMPARE_META_GID_GT 3
#define COMPARE_META_GID_LT -3
#define COMPARE_META_EQUAL 0
int compareCrdtMeta(CrdtMeta *a, CrdtMeta *b);
int isConflictMeta(int result);
int appendCrdtMeta(CrdtMeta *target , CrdtMeta* other);
int isConflictCommon(int result);
void initIncrMeta(CrdtMeta* meta);
void freeIncrMeta(CrdtMeta* meta);
// int isPartialOrderDeleted(RedisModuleKey *key, VectorClock *vclock);


#endif //REDIS_CTRIP_CRDT_COMMON_H
