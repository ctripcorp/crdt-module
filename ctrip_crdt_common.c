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

#include "ctrip_crdt_common.h"
#include "crdt.h"

#include <stdlib.h>

int isPartialOrderDeleted(RedisModuleKey *key, VectorClock *vclock) {
    void *tombstone = RedisModule_ModuleTypeGetTombstone(key);
    if (tombstone == NULL) {
        return CRDT_NO;
    }
    CrdtCommon *common = tombstone;
    if (isVectorClockMonoIncr(vclock, common->vectorClock) == CRDT_OK) {
        return CRDT_YES;
    }
    return CRDT_NO;
}

int isConflictCommon(int result) {
    if(result > COMPARE_COMMON_VECTORCLOCK_GT || result < COMPARE_COMMON_VECTORCLOCK_LT) {
        return CRDT_OK;
    }
    return CRDT_NO;
}

void crdtCommonCp(CrdtCommon* from, CrdtCommon* to) {
    VectorClock* clock =  to->vectorClock;
    to->gid = from->gid;
    to->timestamp = from->timestamp;
    to->vectorClock = dupVectorClock(from->vectorClock);
    freeVectorClock(clock);
}

void crdtCommonMerge(CrdtCommon* target , CrdtCommon* other) {
    VectorClock* clock =  target->vectorClock;
    target->gid = other->gid;
    target->timestamp = other->timestamp;
    target->vectorClock = vectorClockMerge(clock, other->vectorClock);
    freeVectorClock(clock);
}

int compareCommon(CrdtCommon* old_common, CrdtCommon* new_common) {
    if (isVectorClockMonoIncr(old_common->vectorClock, new_common->vectorClock) == CRDT_OK) {
        return COMPARE_COMMON_VECTORCLOCK_GT;
    } else if (isVectorClockMonoIncr(new_common->vectorClock, old_common->vectorClock) == CRDT_OK) {
        return COMPARE_COMMON_VECTORCLOCK_LT;
    } 
    if(old_common->timestamp < new_common->timestamp) {
        return COMPARE_COMMON_TIMESTAMPE_GT;
    }else if (old_common->timestamp > new_common->timestamp) {
        return COMPARE_COMMON_TIMESTAMPE_LT;
    }
    if(old_common->gid > new_common->gid) {
        return COMPARE_COMMON_GID_GT;
    }else if(old_common->gid < new_common->gid) {
        return COMPARE_COMMON_GID_LT;
    }
    return COMPARE_COMMON_EQUAL;
}

int compareCrdtInfo(CrdtCommon* old_common, CrdtInfo* new_info) {
    if (isVectorClockMonoIncr(old_common->vectorClock, new_info->vectorClock) == CRDT_OK) {
        return COMPARE_COMMON_VECTORCLOCK_GT;
    } else if (isVectorClockMonoIncr(new_info->vectorClock, old_common->vectorClock) == CRDT_OK) {
        return COMPARE_COMMON_VECTORCLOCK_LT;
    } 
    if(old_common->timestamp < new_info->timestamp) {
        return COMPARE_COMMON_TIMESTAMPE_GT;
    }else if (old_common->timestamp > new_info->timestamp) {
        return COMPARE_COMMON_TIMESTAMPE_LT;
    }
    if(old_common->gid > new_info->gid) {
        return COMPARE_COMMON_GID_GT;
    }else if(old_common->gid < new_info->gid) {
        return COMPARE_COMMON_GID_LT;
    }
    return COMPARE_COMMON_EQUAL;
}

CrdtCommon* createCommon(int gid, long long timestamp, VectorClock* vclock) {
    CrdtCommon* common = RedisModule_Alloc(sizeof(CrdtCommon));
    common->gid = gid;
    common->timestamp = timestamp;
    common->vectorClock = vclock;
    return common;    
}
CrdtCommon* createIncrCommon() {
    long long gid = RedisModule_CurrentGid();
    RedisModule_IncrLocalVectorClock(1);
    // Abstract the logic of local set to match the common process
    // Here, we consider the op vclock we're trying to broadcasting is our vcu wrapped vector clock
    // for example, our gid is 1,  our vector clock is (1:100,2:1,3:100)
    // A set operation will firstly drive the vclock into (1:101,2:1,3:100).
    VectorClock *currentVectorClock = RedisModule_CurrentVectorClock();
    long long timestamp = RedisModule_Milliseconds();
    return createCommon(gid, timestamp, currentVectorClock);
}
void freeCommon(CrdtCommon* common) {
    freeVectorClock(common->vectorClock);
    RedisModule_Free(common);
}
#if defined(CRDT_COMMON_TEST_MAIN)
#include <stdio.h>
#include "testhelp.h"
#include "limits.h"

#define UNUSED(x) (void)(x)
typedef struct nickObject {
    CrdtCommon common;
    sds content;
}nickObject;

void*
mergeFunc (const void *curVal, const void *value) {
    if(value == NULL) {
        return NULL;
    }
    void *dup = zmalloc(1);
    return dup;
}

nickObject
*createNickObject() {
    nickObject *obj = zmalloc(sizeof(nickObject));
    printf("[nickObject]%lu\r\n", sizeof(nickObject));
    obj->content = sdsnew("hello");

    obj->common.vectorClock = sdsnew("1:200");
    obj->common.merge = mergeFunc;
    return obj;
}

int crdtCommonTest(void) {
    nickObject *obj = createNickObject();
    CrdtCommon *common = (CrdtCommon *) obj;
    test_cond("[crdtCommonTest]", sdscmp(sdsnew("1:200"), common->vectorClock) == 0);
    test_cond("[crdtCommonTest]", sdscmp(sdsnew("hello"), obj->content) == 0);
    test_report();
    return 0;
}

#endif

#ifdef CRDT_COMMON_TEST_MAIN
int main(void) {
    return crdtCommonTest();
}
#endif