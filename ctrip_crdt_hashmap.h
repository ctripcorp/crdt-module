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
// Created by zhuchen on 2019/9/4.
//

#ifndef XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H
#define XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H

#include <string.h>

#include "include/rmutil/sds.h"
#include "ctrip_crdt_common.h"
#include "include/redismodule.h"
#include "include/rmutil/dict.h"
#include "crdt_util.h"

#define CRDT_HASH_DATATYPE_NAME "crdt_hash"

#define HASHTABLE_MIN_FILL        10

#define UINT64_MAX        18446744073709551615ULL
#define RDB_LENERR UINT64_MAX

#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2

typedef struct CRDT_Hash {
    CrdtCommon common;
    int remvAll;
    VectorClock *maxdvc;
    dict *map;
} CRDT_Hash;


void *createCrdtHash(void);

void freeCrdtHash(void *crdtHash);

int initCrdtHashModule(RedisModuleCtx *ctx);

int crdtHtNeedsResize(dict *dict);


#endif //XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H
