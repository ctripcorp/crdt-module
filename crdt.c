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
// Created by zhuchen(zhuchen at ctrip dot com) on 2019-04-17.
//

#include "include/redismodule.h"

#include "crdt.h"
#include "crdt_register.h"
#include "ctrip_crdt_hashmap.h"

#define CRDT_API_VERSION 1


int delCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);

    if (argc < 2) return RedisModule_WrongArity(ctx);

    int numl = 0, dirty, tmp;

    for(int i = 1; i < argc; i++) {
        tmp = numl;
        RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv[i], REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
        // if not found, skip this step
        if (RedisModule_KeyType(moduleKey) != REDISMODULE_KEYTYPE_EMPTY) {
            void *crdtObj = RedisModule_ModuleTypeGetValue(moduleKey);
            if (crdtObj == NULL) {
                continue;
            }
            CrdtCommon *crdtCommon = (CrdtCommon *) crdtObj;
            int result = crdtCommon->method->delFunc(ctx, argv[i], moduleKey, crdtObj);
            if(crdtCommon->method->delFunc(ctx, argv[i], moduleKey, crdtObj)) {
                RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_GENERIC, "del", argv[i]);
                numl ++;
            }
        }
        dirty = numl - tmp;
        if (dirty > 0) {
            RedisModule_DeleteKey(moduleKey);
        }
        RedisModule_CloseKey(moduleKey);
    }

    RedisModule_ReplyWithLongLong(ctx, numl);
    return REDISMODULE_OK;
}

int crdtDebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    RedisModule_AutoMemory(ctx);

    if (argc < 2) return RedisModule_WrongArity(ctx);

    const char *cmd = RedisModule_StringPtrLen(argv[1], NULL);
    if (!strcasecmp(cmd,"loglevel")) {
        sdsfree(logLevel);
        logLevel = sdsnew(RedisModule_StringPtrLen(argv[2], NULL));
    } else {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

VectorClock *getVectorClockFromString(RedisModuleString *vectorClockStr) {
    size_t vcStrLength;
    const char *vcStr = RedisModule_StringPtrLen(vectorClockStr, &vcStrLength);
    sds vclockSds = sdsnewlen(vcStr, vcStrLength);
    VectorClock *vclock = sdsToVectorClock(vclockSds);
    sdsfree(vclockSds);
    return vclock;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    logLevel = sdsnew(CRDT_DEFAULT_LOG_LEVEL);
    if (RedisModule_Init(ctx, MODULE_NAME, CRDT_API_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if(initRegisterModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "register module -- register failed");
        return REDISMODULE_ERR;
    }

    if(initCrdtHashModule(ctx) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "hash module -- register failed");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx,"del",
                                  delCommand,"write",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"crdt.debug",
                                  crdtDebugCommand,"write",1,-1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
