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

#ifndef CRDT_MODULE_CRDT_G_COUNTER_H
#define CRDT_MODULE_CRDT_G_COUNTER_H

#include "../include/redismodule.h"
#include "../constans.h"
#include "../include/rmutil/sds.h"
#include "../util.h"

typedef struct {
    long long start_clock: 60;
    long long type: 1; //integer, float
    long long opt: 3; // optional bytes for further use
    long long end_clock;
    long long del_end_clock;
    union {
        long long i;
        long double f;
    }conv;
    union {
        long long i;
        long double f;
    }del_conv;
} gcounter;
typedef struct {
    int gid;
    long long start_clock: 60;
    long long type: 1; //integer, float
    long long opt: 3; // optional bytes for further use
    long long end_clock;
    union {
        long long i;
        long double f;
    }conv;
} gcounter_meta;
#define get_int_counter(gcounter) (counter->conv.i)
#define get_float_counter(gcounter) (counter->conv.f)

void* createGcounter(int type);
void freeGcounter(void *counter);
//about gcounter_meta
void* createGcounterMeta(int type);
void freeGcounterMeta(void *counter);
sds gcounterDelToSds(int gid, gcounter* g);

int gcounterMetaFromSds(sds str, gcounter_meta* g);
gcounter_meta* sdsTogcounterMeta(sds str);
#endif //CRDT_MODULE_CRDT_G_COUNTER_H
