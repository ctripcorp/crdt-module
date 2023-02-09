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
// Created by zhuchen(zhuchen@ctrip.com) on 2019-04-17.
//

#ifndef XREDIS_CRDT_CRDT_H
#define XREDIS_CRDT_CRDT_H

#include "ctrip_vector_clock.h"
#include <redismodule.h>

#define MODULE_NAME "xredis_crdt"
#define CRDT_OK 1
#define CRDT_ERROR 0

#define CRDT_YES 1
#define CRDT_NO 0

#define CRDT_DEFAULT_LOG_LEVEL "warning"
#define CRDT_DEBUG_LOG_LEVEL "debug"

#define RDB_LENERR 18446744073709551615ULL

char *logLevel;

#define SECOND_HIGHER_PRIORITY(first_gid, sec_gid) (sec_gid <= first_gid ? 1 : 0)

int delCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

VectorClock getVectorClockFromString(RedisModuleString *redisModuleString);

#endif //XREDIS_CRDT_CRDT_H
