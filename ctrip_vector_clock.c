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
// Created by zhuchen on 2019-05-10.
//

#include "ctrip_vector_clock.h"
#include "include/rmutil/sds.h"
#include "include/rmutil/alloc.h"
#include "util.h"
#include "crdt.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

VectorClock*
newVectorClock(int numVcUnits) {
    VectorClock *result = malloc(sizeof(VectorClock));
    result->clocks = malloc(sizeof(VectorClockUnit) * numVcUnits);
    result->length = numVcUnits;
    return result;
}

void
freeVectorClock(VectorClock *vc) {
    if (vc == NULL) {
        return;
    }
    free(vc->clocks);
    free(vc);
}

void
addVectorClockUnit(VectorClock *vc, long long gid, long long logic_time) {
    int newLength = vc->length + 1;
    VectorClockUnit *vcus = malloc(sizeof(VectorClockUnit) * newLength);
    memcpy(vcus, vc->clocks, sizeof(VectorClockUnit) * vc->length);
    free(vc->clocks);
    vc->clocks = vcus;
    vc->clocks[newLength-1].gid = gid;
    vc->clocks[newLength-1].logic_time = logic_time;
    vc->length = newLength;
    sortVectorClock(vc);
}

VectorClock*
dupVectorClock(VectorClock *vc) {
    if (!vc) {
        return NULL;
    }
    VectorClock *dup = newVectorClock(vc->length);
    memcpy(dup->clocks, vc->clocks, vc->length * sizeof(VectorClockUnit));
    return dup;
}

void
sdsToVectorClockUnit(sds vcUnitStr, VectorClockUnit *vcUnit);

// "<gid>:<clock>;<gid>:<clock>"
VectorClock*
sdsToVectorClock(sds vcStr) {
    if (sdslen(vcStr) == 0) {
        return NULL;
    }
    int numVcUnits, clockNum;
    sds *vcUnits = sdssplitlen(vcStr, sdslen(vcStr), VECTOR_CLOCK_SEPARATOR, 1, &numVcUnits);
    if(numVcUnits <= 0 || !vcUnits) {
        return NULL;
    }
    clockNum = numVcUnits;
    sdstrim(vcUnits[numVcUnits-1], "");
    if(sdslen(vcUnits[numVcUnits-1]) < 1) {
        clockNum = numVcUnits - 1;
    }
    VectorClock *result = newVectorClock(clockNum);
    for(int i = 0; i < clockNum; i++) {
        sdsToVectorClockUnit(vcUnits[i], &(result->clocks[i]));
    }

    //clean up
    sdsfreesplitres(vcUnits, numVcUnits);
    return result;
}

void
sdsToVectorClockUnit(sds vcUnitStr, VectorClockUnit *vcUnit) {
    int numElements;
    sds *vcUnits = sdssplitlen(vcUnitStr, sdslen(vcUnitStr), VECTOR_CLOCK_UNIT_SEPARATOR, 1, &numElements);
    if(!vcUnits || numElements != 2) {
        goto cleanup;
    }
    string2ll(vcUnits[0], sdslen(vcUnits[0]), &(vcUnit->gid));
    string2ll(vcUnits[1], sdslen(vcUnits[1]), &(vcUnit->logic_time));

cleanup:
    {
        sdsfreesplitres(vcUnits, numElements);
    }
}

//todo: use redis module auto mem control, to alloc mem (all local/temp variables)
sds
vectorClockToSds(VectorClock *vc) {
    if(!vc || vc->length < 1) {
        return sdsempty();
    }
    int length = vc->length;
    sds vcStr = sdsempty();
    for(int i = 0; i < length; i++) {
        vcStr = sdscatprintf(vcStr, "%lld:%lld", vc->clocks[i].gid, vc->clocks[i].logic_time);
        if(i != length - 1) {
            vcStr = sdscat(vcStr, VECTOR_CLOCK_SEPARATOR);
        }
    }
    return vcStr;
}

/* Sort comparators for qsort() */
static int sort_vector_clock_unit(const void *a, const void *b) {
    const VectorClockUnit *vcu_a = a, *vcu_b = b;
    /* We sort the vector clock unit by gid*/
    if (vcu_a->gid > vcu_b->gid)
        return 1;
    else if (vcu_a->gid == vcu_b->gid)
        return 0;
    else
        return -1;
}

void sortVectorClock(VectorClock *vc) {
    qsort(vc->clocks, vc->length, sizeof(VectorClockUnit), sort_vector_clock_unit);
}

VectorClockUnit*
getVectorClockUnit(VectorClock *vc, long long gid) {
    if (vc == NULL || !vc->length) {
        return NULL;
    }
    for(int i = 0; i < vc->length; i++) {
        if(vc->clocks[i].gid ==  gid) {
            return &(vc->clocks[i]);
        }
    }
    return NULL;
}

VectorClock*
vectorClockMerge(VectorClock *vc1, VectorClock *vc2) {
    if (vc1 == NULL && vc2 == NULL) {
        return NULL;
    }
    if (vc1 == NULL) {
        return dupVectorClock(vc2);
    }
    if (vc2 == NULL) {
        return dupVectorClock(vc1);
    }
    VectorClock *result = dupVectorClock(vc1);
    for (int i = 0; i < vc2->length; i++) {
        VectorClockUnit *target;
        if(!(target = getVectorClockUnit(result, vc2->clocks[i].gid))) {
            addVectorClockUnit(result, vc2->clocks[i].gid, vc2->clocks[i].logic_time);
        } else {
            target->logic_time = max(vc2->clocks[i].logic_time, target->logic_time);
        }
    }
    return result;
}


int
isVectorClockMonoIncr(VectorClock *current, VectorClock *future) {
    if (current == NULL || future == NULL) {
        return CRDT_NO;
    }

    if (current->length > future->length) {
        return CRDT_NO;
    }

    for (int i = 0; i < current->length; i++) {
        VectorClockUnit *currentVcu = &current->clocks[i];
        VectorClockUnit *futureVcu = getVectorClockUnit(future, currentVcu->gid);
        if (futureVcu == NULL || futureVcu->logic_time < currentVcu->logic_time) {
            return CRDT_NO;
        }
    }
    return CRDT_YES;
}

VectorClock*
getUnitVectorClock(VectorClock *vclock, int gid) {
    if(vclock == NULL) {
        return NULL;
    }
    VectorClockUnit *vectorClockUnit = getVectorClockUnit(vclock, gid);
    VectorClock *result = newVectorClock(1);
    result->clocks[0].gid = gid;
    result->clocks[0].logic_time = vectorClockUnit->logic_time;
    return result;
}



#if defined(VECTOR_CLOCK_TEST_MAIN)
#include <stdio.h>
#include <stdlib.h>
#include "testhelp.h"
#include "limits.h"


int testSdsConvert2VectorClockUnit(void) {
    printf("========[testSdsConvert2VectorClockUnit]==========\r\n");
    sds vcStr = sdsnew("1:123");
    VectorClockUnit *unit = malloc(sizeof(VectorClockUnit));
    sdsToVectorClockUnit(vcStr, unit);

    test_cond("sds to vcu", 1 == unit->gid);
    test_cond("sds to vcu", 123 ==unit->logic_time);
    return 0;
}

int testSdsConvert2VectorClock(void) {
    printf("========[testSdsConvert2VectorClock]==========\r\n");
    sds vcStr = sdsnew("1:123;2:234;3:345");
    VectorClock *vc = sdsToVectorClock(vcStr);
    test_cond("[first vc]gid equals", 1 == vc->clocks[0].gid);
    test_cond("[first vc]logic_time equals", 123 == vc->clocks[0].logic_time);

    test_cond("[second vc]gid equals", 2 == vc->clocks[1].gid);
    test_cond("[second vc]logic_time equals", 234 == vc->clocks[1].logic_time);

    test_cond("[third vc]gid equals", 3 == vc->clocks[2].gid);
    test_cond("[third vc]logic_time equals", 345 == vc->clocks[2].logic_time);

    vcStr = sdsnew("1:123");
    vc = sdsToVectorClock(vcStr);
    test_cond("[one clock unit]length", 1 == vc->length);
    test_cond("[one clock unit]", 1 == vc->clocks[0].gid);
    test_cond("[one clock unit]", 123 == vc->clocks[0].logic_time);

    vcStr = sdsnew("1:123;");
    vc = sdsToVectorClock(vcStr);
    test_cond("[one clock unit;]length", 1 == vc->length);
    test_cond("[one clock unit;]gid equals", 1 == vc->clocks[0].gid);
    test_cond("[one clock unit;]logic_time equals", 123 == vc->clocks[0].logic_time);
    return 0;
}

int testFreeVectorClock(void) {
    printf("========[testFreeVectorClock]==========\r\n");
    sds vcStr = sdsnew("1:123;2:234;3:345");
    VectorClock *vc = sdsToVectorClock(vcStr);
    freeVectorClock(vc);
    return 0;
}

int testvectorClockToSds(void) {
    printf("========[testvectorClockToSds]==========\r\n");
    sds vcStr = sdsnew("1:123;2:234;3:345");
    VectorClock *vc = sdsToVectorClock(vcStr);
    sds dup = vectorClockToSds(vc);
    printf("expected: %s, actual: %s \r\n", vcStr, dup);
    test_cond("[testvectorClockToSds]", sdscmp(vcStr, dup) == 0);
    freeVectorClock(vc);
    return 0;
}

int testSortVectorClock(void) {
    printf("========[testSortVectorClock]==========\r\n");
    sds vcStr = sdsnew("1:123;2:234;3:345");
    VectorClock *vc = sdsToVectorClock(vcStr);
    sortVectorClock(vc);
    sds dup = vectorClockToSds(vc);
    test_cond("[testSortVectorClock][positive-case]", sdscmp(vcStr, dup) == 0);

    sds vcStr2 = sdsnew("2:234;3:345;1:123");
    vc = sdsToVectorClock(vcStr2);
    sortVectorClock(vc);
    dup = vectorClockToSds(vc);
    test_cond("[testSortVectorClock][real-sort-case]", sdscmp(vcStr, dup) == 0);

    sds vcStr3 = sdsnew("3:345;1:123;2:234");
    vc = sdsToVectorClock(vcStr3);
    sortVectorClock(vc);
    dup = vectorClockToSds(vc);
    test_cond("[testSortVectorClock][real-sort-case]", sdscmp(sdsnew("1:123;2:234;3:345"), dup) == 0);

    return 0;
}

int testDupVectorClock(void) {
    printf("========[testDupVectorClock]==========\r\n");
    sds vcStr = sdsnew("1:123;2:234;3:345");
    VectorClock *vc = sdsToVectorClock(vcStr);
    VectorClock *dup = dupVectorClock(vc);
    sds dupSds = vectorClockToSds(dup);

    test_cond("[testDupVectorClock]", sdscmp(vcStr, dupSds) == 0);
    return 0;
}

int testAddVectorClockUnit(void) {
    printf("========[testAddVectorClockUnit]==========\r\n");
    sds vcStr = sdsnew("1:123;2:234;3:345");
    VectorClock *vc = sdsToVectorClock(vcStr);
    addVectorClockUnit(vc, 100, 50);

    printf("result: %s\r\n", vectorClockToSds(vc));
    test_cond("[testAddVectorClockUnit]", sdscmp(sdsnew("1:123;2:234;3:345;100:50"), vectorClockToSds(vc)) == 0);
    return 0;
}

int testvectorClockMerge(void) {
    VectorClock *vc = vectorClockMerge(sdsToVectorClock(sdsnew("1:100;2:200;3:300")), sdsToVectorClock(sdsnew("1:200;2:500;3:100")));
    test_cond("[testvectorClockMerge][merge-only]", sdscmp(sdsnew("1:200;2:500;3:300"), vectorClockToSds(vc)) == 0);

    vc = vectorClockMerge(sdsToVectorClock(sdsnew("1:100;2:200;3:300")), sdsToVectorClock(sdsnew("1:99")));
    test_cond("[testvectorClockMerge][add]", sdscmp(sdsnew("1:100;2:200;3:300"), vectorClockToSds(vc)) == 0);

    vc = vectorClockMerge(sdsToVectorClock(sdsnew("1:100;2:200;3:300")), sdsToVectorClock(sdsnew("1:200;3:100;4:400")));
    test_cond("[testvectorClockMerge][add-merge]", sdscmp(sdsnew("1:200;2:200;3:300;4:400"), vectorClockToSds(vc)) == 0);

    return 0;
}

int testStrCmp(void) {
    sds psync = sdsnew("CRDT.PSYNC");
    if (!strcasecmp(psync,"psync")) {
        printf("psync: %d\r\n", strcasecmp(psync,"psync"));
    } else if (!strcasecmp(psync,"crdt.psync")) {
        printf("crdt.psync: %d\r\n", strcasecmp(psync,"crdt.psync"));
    }


    return 0;
}

int vectorClockTest(void) {
    int result = 0;
    {
        result |= testSdsConvert2VectorClockUnit();
        result |= testSdsConvert2VectorClock();
        result |= testFreeVectorClock();
        result |= testvectorClockToSds();
        result |= testSortVectorClock();
        result |= testDupVectorClock();
        result |= testAddVectorClockUnit();
        result |= testvectorClockMerge();
    }
    testStrCmp();
    test_report();
    return result;
}
#endif

#ifdef VECTOR_CLOCK_TEST_MAIN
int main(void) {
    return vectorClockTest();
}
#endif
