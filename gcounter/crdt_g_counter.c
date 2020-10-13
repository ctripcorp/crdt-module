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

void* createGcounter() {
#if defined(G_COUNTER_TEST_MAIN)
    gcounter *counter = malloc(sizeof(gcounter));//unit test only
#else
    gcounter *counter = RedisModule_Alloc(sizeof(gcounter));
#endif
    counter->type = VALUE_TYPE_INTEGER;
    // counter->logic_clock = 0;
    counter->conv.i = 0;
    counter->conv.f = 0;
    return counter;
}

void freeGcounter(void *counter) {
#if defined(G_COUNTER_TEST_MAIN)
    free(counter);//unit test only
#else
    RedisModule_Free(counter);
#endif
}



#if defined(G_COUNTER_TEST_MAIN)

#include <stdlib.h>

#include <stdio.h>

#include "../tests/testhelp.h"
#include "limits.h"

int testBasicGcounter(void) {
    printf("========[testBasicGcounter]==========\r\n");

    gcounter *counter = createGcounter();
    counter->conv.i = 10;
    test_cond("[basic counter int]", 10 == get_int_counter(counter));

    counter->type = VALUE_TYPE_FLOAT;
    counter->conv.f = 1.3f;
    test_cond("[basic counter float]", 1.3f == get_float_counter(counter));
    return 0;
}

int testFreeGcounter(void) {
    printf("========[testFreeGcounter]==========\r\n");

    gcounter *counter = createGcounter();
    counter->conv.i = 10;
    test_cond("[basic counter int]", 10 == get_int_counter(counter));
    freeGcounter(counter);

    counter = createGcounter();
    counter->type = VALUE_TYPE_FLOAT;
    counter->conv.f = 1.3f;
    test_cond("[basic counter float]", 1.3f == get_float_counter(counter));
    freeGcounter(counter);
    return 0;
}



int GcounterTest(void) {
    int result = 0;
    {
        result |= testBasicGcounter();
        result |= testFreeGcounter();
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
