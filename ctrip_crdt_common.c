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

int isNullVectorClock(VectorClock vc) {
    return vc == LOGIC_CLOCK_UNDEFINE;
}
void setMetaGid(CrdtMeta* meta, int gid) {
    meta->gid = gid;
} 
int getMetaGid(CrdtMeta* meta) {
    return (int)meta->gid;
}
void setMetaTimestamp(CrdtMeta* meta, long long timestamp) {
    meta->timestamp = timestamp;
}
long long getMetaTimestamp(CrdtMeta* meta) {
    return meta->timestamp;
}
VectorClock getMetaVectorClock(CrdtMeta* meta) {
    if(meta == NULL) return LOGIC_CLOCK_UNDEFINE;
    return meta->vectorClock;
}
int isConflictCommon(int result) {
    if(result > COMPARE_META_VECTORCLOCK_GT || result < COMPARE_META_VECTORCLOCK_LT) {
        return CRDT_OK;
    }
    return CRDT_NO;
}

void crdtMetaCp(CrdtMeta* from, CrdtMeta* to) {
    // VectorClock clock =  to->vectorClock;
    if(to == NULL) return;
    if(isNullVectorClock(getMetaVectorClock(to))) {
        freeInnerClocks(getMetaVectorClock(to));
    }
    setMetaGid(to, getMetaGid(from));
    setMetaTimestamp(to, getMetaTimestamp(from));
    if(!isNullVectorClock(to->vectorClock)) {
        freeInnerClocks(to->vectorClock);
    }
    to->vectorClock = dupVectorClock(getMetaVectorClock(from));
    // cloneVectorClock(&to->vectorClock,getMetaVectorClock(from));
}

int appendCrdtMeta(CrdtMeta* target , CrdtMeta* other) {
    int result = compareCrdtMeta(target, other);
    if(result >COMPARE_META_EQUAL) {
        setMetaGid(target, getMetaGid(other));
        setMetaTimestamp(target, getMetaTimestamp(other));
    }
    
    setMetaVectorClock(target, vectorClockMerge(getMetaVectorClock(target), getMetaVectorClock(other)));
    
    return result;
}

int compareCrdtMeta(CrdtMeta* old_common, CrdtMeta* new_common) {
    if(old_common == NULL || isNullVectorClock(getMetaVectorClock(old_common))) {
        return COMPARE_META_VECTORCLOCK_GT;
    }
    if(new_common == NULL || isNullVectorClock(getMetaVectorClock(new_common))) {
        return COMPARE_META_VECTORCLOCK_LT;
    }
    if (isVectorClockMonoIncr(getMetaVectorClock(old_common), getMetaVectorClock(new_common)) == CRDT_OK) {
        return COMPARE_META_VECTORCLOCK_GT;
    } else if (isVectorClockMonoIncr(getMetaVectorClock(new_common), getMetaVectorClock(old_common)) == CRDT_OK) {
        return COMPARE_META_VECTORCLOCK_LT;
    } 
    long long old_time = getMetaTimestamp(old_common);
    long long new_time = getMetaTimestamp(new_common);
    if(old_time < new_time) {
        return COMPARE_META_TIMESTAMPE_GT;
    }else if (old_time > new_time) {
        return COMPARE_META_TIMESTAMPE_LT;
    }
    int old_gid = getMetaGid(old_common);
    int new_gid = getMetaGid(new_common);
    if(old_gid > new_gid) {
        return COMPARE_META_GID_GT;
    }else if(old_gid < new_gid) {
        return COMPARE_META_GID_LT;
    }
    return COMPARE_META_EQUAL;
}
CrdtMeta* createMeta(int gid, long long timestamp, VectorClock vclock) {
    CrdtMeta* meta = RedisModule_Alloc(sizeof(CrdtMeta));
    meta->type = 0;
    meta->timestamp = 0;
    meta->gid = 1;
    setMetaGid(meta, gid);
    setMetaTimestamp(meta,timestamp);
    meta->vectorClock = vclock;
    return meta;  
};
void appendVCForMeta(CrdtMeta* target, VectorClock vc) {
    if(!isNullVectorClock(getMetaVectorClock(target))) {
        VectorClock v = getMetaVectorClock(target);
        target->vectorClock = vectorClockMerge(getMetaVectorClock(target), vc);
        freeInnerClocks(v);
    }
    
}
CrdtMeta* dupMeta(CrdtMeta* meta) {
    if(meta == NULL) return NULL;
    return createMeta(getMetaGid(meta), getMetaTimestamp(meta), dupVectorClock(getMetaVectorClock(meta)));
}
CrdtMeta* createIncrMeta() {
    long long gid = RedisModule_CurrentGid();
    RedisModule_IncrLocalVectorClock(1);
    VectorClock currentVectorClock = RedisModule_CurrentVectorClock();
    VectorClock result = getMonoVectorClock(currentVectorClock, gid);
    long long timestamp = RedisModule_Milliseconds();
    return createMeta(gid, timestamp, result);
};
void freeCrdtMeta(CrdtMeta* meta) {
    if(meta == NULL) {
        return;
    }
    setMetaVectorClock(meta, LOGIC_CLOCK_UNDEFINE);
    RedisModule_Free(meta);
}

void freeCrdtObject(CrdtObject* object) {
    RedisModule_Free(object);
}
void freeCrdtTombstone(CrdtTombstone* tombstone) {
    RedisModule_Free(tombstone);
}


int getType(CrdtObject* obj) {
    return obj->type;
}
void setType(CrdtObject* obj, int type) {
    obj->type = type;
}
int setDataType(CrdtObject* obj, int type) {
    obj->dataType = type;
}
int getDataType(CrdtObject* obj) {
    return obj->dataType;
}
/**
 * CrdtMeta Get Set Functions
 */ 
void setMetaVectorClock(CrdtMeta* meta, VectorClock vc) {
    if(!isNullVectorClock(getMetaVectorClock(meta))) {
        freeInnerClocks(meta->vectorClock);
    }
    meta->vectorClock = vc;
    
}

CrdtMeta* mergeMeta(CrdtMeta* target, CrdtMeta* other) {
    VectorClock result = vectorClockMerge(getMetaVectorClock(target), getMetaVectorClock(other));
    if(compareCrdtMeta(target, other) > 0) {
        return createMeta(getMetaGid(other), getMetaTimestamp(other), result);
    }else{
        return createMeta(getMetaGid(target), getMetaTimestamp(target), result);
    }
}
CrdtMeta* addOrCreateMeta(CrdtMeta* target, CrdtMeta* other) {
    VectorClock vc = getMetaVectorClock(target);
    if(target == NULL || vc  == NULL) {
        return dupMeta(other);
    }
    if(compareCrdtMeta(target, other) > 0) {
        setMetaGid(target, getMetaGid(other));
        setMetaTimestamp(target, getMetaTimestamp(other));
    }
    vc =  vectorClockMerge(vc, getMetaVectorClock(other));
    target->vectorClock = vc;
    // merge(vc, getMetaVectorClock(other));
    // clone(&target->vectorClock ,vectorClockMerge(vc, getMetaVectorClock(other)));
    return target;
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