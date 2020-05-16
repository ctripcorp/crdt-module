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

#ifndef REDIS_VECTOR_CLOCK_H
#define REDIS_VECTOR_CLOCK_H

#include "include/rmutil/sds.h"
#include <stdlib.h>
#include <stdio.h>

// "<gid>:<clock>;<gid>:<clock>"
#define VECTOR_CLOCK_SEPARATOR ";"
#define VECTOR_CLOCK_UNIT_SEPARATOR ":"

#define min(x, y) x > y ? y : x

#define max(x, y) x > y ? x : y

/**
 * To shrink down mem usage, an unsigned long long will stand for [gid, clock]
 * where, higher 4 bits is allocated for gid (16 gid in total)
 * and,   lower 60 bits represents the logical clk
 * |0000|0000|xxxxxxxxxxxxxxxxxx|
 * |4bit|4bit|56            bits|
 * |option |gid | logic clock      |
 * */
typedef unsigned long long clk;

/**
 * single one
 * |0000|0000|xxxxxxxxxxxxxxxxxx|
 * |4bit|4bit|56            bits|
 * |len |gid | logic clock      |
 *
 * multi one
 * |0000|0000   |xxxxxxxxxxxxxxxxxx|
 * |4bit|4bit   | 56          bits |
 * |len |option | address          |
 * */
typedef unsigned long long VectorClock;



#define VectorClockUnit clk
#define APPEND(x, y) x ## y
#define ULL(val) (unsigned long long) val

static const size_t       XAddressOffsetBits    = 56; // 65536TB
static const size_t       XLogicClockOffsetBits = 56; // 65536TB
static const size_t       XGidOffsetBits        = 4;
static const size_t       XLengthOffsetBits     = 60;

static const VectorClock  XAddressOffsetMask    = (1ull << XAddressOffsetBits) - 1;
static const clk          XLogicClockOffsetMask = (1ull << XLogicClockOffsetBits) - 1;
static const size_t       XGidMask              = (1 << XGidOffsetBits) - 1;
static const size_t       XLenMask              = (1 << XGidOffsetBits) - 1;

static const clk          GidCleanUpMask        = ~(ULL(XGidMask) << XLogicClockOffsetBits);
static const clk          LogicTimeCleanUpMask  = ~(XLogicClockOffsetMask);


/**-------------------------------------------length utils-------------------------------------------------**/
char get_len(VectorClock vclock);

void set_len(VectorClock *vclock, char length) ;
int ismulti(VectorClock vclock);
/**-------------------------------------------gid utils-------------------------------------------------**/
char get_gid(clk clock);

void set_gid(clk *clock, char gid);

/**-------------------------------------------logic clock utils-----------------------------------------**/
long long get_logic_clock(clk clock) ;

void set_logic_clock(clk *clock, long long logic_time);
/**-------------------------------------------vector clock utils-------------------------------------------------**/
clk* clocks_address(VectorClock value);

clk* get_clock_unit_by_index(VectorClock *vc, char index);

void set_clock_unit_by_index(VectorClock *vclock, char index, clk gid_logic_time);
//
clk init_clock(char gid, clk logic_clk);
//#define null NULL
#define CLOCK_UNIT_MAX 0
#define CLOCK_UNIT_ALIGN 1

#define LOGIC_CLOCK_UNDEFINE ULL(0xFFFFFFFFFFFFFFFF)


/**------------------------Vector Clock Lifecycle--------------------------------------*/
VectorClock
newVectorClock(int numVcUnits);

void
freeVectorClock(VectorClock vc);

void
freeInnerClocks(VectorClock vclock);

VectorClock
addVectorClockUnit(VectorClock vc, int gid, long long logic_time);

VectorClock
dupVectorClock(VectorClock vc);

/**------------------------Vector Clock & sds convertion--------------------------------------*/
VectorClock
sdsToVectorClock(sds vcStr);

sds
vectorClockToSds(VectorClock vc);


/**------------------------Vector Clock Util--------------------------------------*/

clk
getVectorClockUnit(VectorClock vc, int gid);

void
incrLogicClock(VectorClock *vc, int gid, int delta);

void
sortVectorClock(VectorClock vc);

/**------------------------Vector Clock Merge--------------------------------------*/

void
mergeLogicClock(VectorClock *dst, VectorClock *src, int gid);

VectorClock
mergeMinVectorClock(VectorClock vclock1, VectorClock vclock2);

VectorClock
getMonoVectorClock(VectorClock src, int gid);

int
isVectorClockMonoIncr(VectorClock current, VectorClock future);

VectorClock
vectorClockMerge(VectorClock vclock1, VectorClock vclock2);

/**------------------------Replication Usage--------------------------------------*/
void
updateProcessVectorClock(VectorClock *dst, VectorClock *src, int gid, int currentGid);

#endif //REDIS_VECTOR_CLOCK_H
