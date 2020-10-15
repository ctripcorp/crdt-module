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
// Created by zhuchen on 2020/9/23.
//

#include "crdt_g_counter.h"

#if defined(G_COUNTER_TEST_MAIN)
#include "stdlib.h"
#endif

void* createGcounter(int type) {
#if defined(G_COUNTER_TEST_MAIN)
    gcounter *counter = malloc(sizeof(gcounter));//unit test only
#else
    gcounter *counter = RedisModule_Alloc(sizeof(gcounter));
#endif
    counter->type = type;
    // counter->logic_clock = 0;
    counter->conv.i = 0;
    counter->conv.f = 0;
    counter->start_clock = 0;
    counter->end_clock = 0;
    counter->del_conv.i = 0;
    counter->del_conv.f = 0;
    counter->del_end_clock = 0;
    return counter;
}

gcounter* dupGcounter(gcounter* g) {
    if(g == NULL) { return g; }
    gcounter* dup = createGcounter(g->type);
    dup->start_clock = g->start_clock;
    dup->end_clock = g->end_clock;
    dup->del_end_clock = g->del_end_clock;
    if(g->type == VALUE_TYPE_FLOAT) {
        dup->conv.f = g->conv.f;
        dup->del_conv.f = g->del_conv.f;
    } else if(g->type == VALUE_TYPE_INTEGER) {
        dup->conv.i = g->conv.i;
        dup->del_conv.i = g->del_conv.i;
    }
    return dup;
}

void freeGcounter(void *counter) {
#if defined(G_COUNTER_TEST_MAIN)
    free(counter);//unit test only
#else
    RedisModule_Free(counter);
#endif
}

void assign_max_rc_counter(gcounter* target, gcounter* src) {
    assert(target->start_clock == src->start_clock);
    if(target->end_clock < src->end_clock) {
        target->end_clock = src->end_clock;
        if(target->type == VALUE_TYPE_FLOAT) {
            target->conv.f = src->conv.f;
        } else if(target->type == VALUE_TYPE_INTEGER) {
            target->conv.i = src->conv.i;
        }
    } 
    if(target->del_end_clock < src->del_end_clock) {
        target->del_end_clock = src->del_end_clock;
        if(target->type == VALUE_TYPE_FLOAT) {
            target->del_conv.f = src->del_conv.f;
        } else if(target->type == VALUE_TYPE_INTEGER) {
            target->del_conv.i = src->del_conv.i;
        }
    }
}


void* createGcounterMeta(int type) {
#if defined(G_COUNTER_TEST_MAIN)
    gcounter_meta *counter = malloc(sizeof(gcounter_meta));//unit test only
#else
    gcounter_meta *counter = RedisModule_Alloc(sizeof(gcounter_meta));
#endif
    counter->type = type;
    // counter->logic_clock = 0;
    counter->conv.i = 0;
    counter->conv.f = 0;
    counter->start_clock = 0;
    counter->end_clock = 0;
    counter->gid = 0;
    return counter;
}

void freeGcounterMeta(void *counter) {
#if defined(G_COUNTER_TEST_MAIN)
    free(counter);//unit test only
#else
    RedisModule_Free(counter);
#endif
}

sds gcounterDelToSds(int gid, gcounter* c) {
    sds str = sdsempty();
    if(c->type == VALUE_TYPE_INTEGER) {
        str = sdscatprintf(str, "%d:%lld:%lld:%lld", gid, c->start_clock, c->del_end_clock, c->del_conv.i);
    } else if(c->type == VALUE_TYPE_FLOAT) {
        str = sdscatprintf(str, "%lld:%lld:%lf", gid, c->start_clock, c->del_end_clock, c->del_conv.f);
    }
    return str;
}
gcounter_meta* sdsTogcounterMeta(sds str) {
    gcounter_meta* g = createGcounterMeta(0);
    int result = gcounterMetaFromSds(str, g);
    if(result == 0) {
        freeGcounterMeta(g);
        return NULL;
    }
    return g;
}
int gcounterMetaFromSds(sds str, gcounter_meta* g) {
    int num;
    sds *vals = sdssplitlen(str, sdslen(str), ":", 1, &num);
    if(num != 4) {
        return 0;
    }
    long long ll = 0;
    long double ld= 0;
    int val_type = -1;
    
    if(string2ll(vals[num-1], sdslen(vals[num - 1]), &ll)) {
        val_type = VALUE_TYPE_INTEGER;
    } else if(string2ld(vals[num -1], sdslen(vals[num - 1]), &ld)) {
        val_type = VALUE_TYPE_FLOAT;
    } else {
        return 0;
    } 
    long long gid = 0;
    if(!string2ll(vals[0], sdslen(vals[0]), &gid)) {
        return 0;
    } 
    long long start_clock = 0;
    if(!string2ll(vals[1], sdslen(vals[1]), &start_clock)) {
        return 0;
    } 
    long long end_clock = 0;
    if(!string2ll(vals[2], sdslen(vals[2]), &end_clock)) {
        return 0;
    } 
    g->gid = gid;
    g->type = val_type;
    g->start_clock = start_clock;
    g->end_clock = end_clock;
    if(val_type == VALUE_TYPE_INTEGER) { 
        g->conv.i = ll;
    } else {
        g->conv.f = ld;
    }
    return 1;
}

int update_del_counter(gcounter* target, gcounter* src) {
    if(target->del_end_clock < src->del_end_clock) {
        if(src->type == VALUE_TYPE_FLOAT) {
            target->del_conv.f = src->del_conv.f;
        } else if(src->type == VALUE_TYPE_INTEGER) {
            target->del_conv.i = src->del_conv.i;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    return 1;
}

int update_add_counter(gcounter* target, gcounter* src) {
    if(target->end_clock < src->end_clock) {
        if(src->type == VALUE_TYPE_FLOAT) {
            target->conv.f = src->conv.f;
        } else if(src->type == VALUE_TYPE_INTEGER) {
            target->conv.i = src->conv.i;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    return 1;
}
int update_del_counter_by_meta(gcounter* target, gcounter_meta* src) {
    if(target->del_end_clock < src->end_clock) {
        target->del_end_clock = src->end_clock;
        if(src->type == VALUE_TYPE_FLOAT) {
            target->del_conv.f = src->conv.f;
        } else if(src->type == VALUE_TYPE_INTEGER) {
            target->del_conv.i = src->conv.i;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    
    return 1;
}
int counter_del(gcounter* target, gcounter* src) {
    if(target->del_end_clock < src->end_clock) {
        target->del_end_clock = src->end_clock;
        if(target->type == VALUE_TYPE_FLOAT) {
            target->del_conv.f = src->conv.f;
        } else if(target->type == VALUE_TYPE_INTEGER) {
            target->del_conv.i = src->conv.i;
        }else{
            return 0;
        }
    }
    return 1;
}
#if defined(G_COUNTER_TEST_MAIN)

#include <stdlib.h>

#include <stdio.h>

#include "../tests/testhelp.h"
#include "limits.h"

int testBasicGcounter(void) {
    printf("========[testBasicGcounter]==========\r\n");

    gcounter *counter = createGcounter(0);
    counter->conv.i = 10;
    test_cond("[basic counter int]", 10 == get_int_counter(counter));

    counter->type = VALUE_TYPE_FLOAT;
    counter->conv.f = 1.3f;
    test_cond("[basic counter float]", 1.3f == get_float_counter(counter));
    return 0;
}

int testFreeGcounter(void) {
    printf("========[testFreeGcounter]==========\r\n");

    gcounter *counter = createGcounter(0);
    counter->conv.i = 10;
    test_cond("[basic counter int]", 10 == get_int_counter(counter));
    freeGcounter(counter);

    counter = createGcounter(0);
    counter->type = VALUE_TYPE_FLOAT;
    counter->conv.f = 1.3f;
    test_cond("[basic counter float]", 1.3f == get_float_counter(counter));
    freeGcounter(counter);
    return 0;
}

int testSdsToGCounterMini(void) {
    printf("========[testSdsToGCounterMini]==========\r\n");
    sds str = sdsnew("1:2:2:1");
    gcounter_meta *counter = createGcounterMeta(0);
    gcounterMetaFromSds(str, counter);
    test_cond("[countermini gid]", 1 == counter->gid);
    return 0;
}

int GcounterTest(void) {
    int result = 0;
    {
        result |= testBasicGcounter();
        result |= testFreeGcounter();
        result |= testSdsToGCounterMini();
    }
    test_report();
    return result;
}
#endif

#ifdef G_COUNTER_TEST_MAIN
int main(void) {
    return GcounterTest();
}
#endif
