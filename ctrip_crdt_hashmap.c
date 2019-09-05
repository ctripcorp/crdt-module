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

#include "ctrip_crdt_hashmap.h"


///***
// * ==================================== Predefined functions ===========================================*/
//
//int hsetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
//
//int CRDT_HSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
//
//int hgetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
//
//int CRDT_HGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
//
//int CRDT_DelHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
//
///* --------------------------------------------------------------------------
// * Module API for Hash type
// * -------------------------------------------------------------------------- */
//
//void *RdbLoadCrdtHash(RedisModuleIO *rdb, int encver);
//
//void RdbSaveCrdtHash(RedisModuleIO *rdb, void *value);
//
//void AofRewriteCrdtHash(RedisModuleIO *aof, RedisModuleString *key, void *value);
//
//size_t crdtHashMemUsageFunc(const void *value);
//
//void crdtHashDigestFunc(RedisModuleDigest *md, void *value);
//
///* --------------------------------------------------------------------------
// * CrdtCommon API for Hash type
// * -------------------------------------------------------------------------- */
//
//void *crdtHashMerge(void *currentVal, void *value);
//
//int crdtHashDelete(void *ctx, void *keyRobj, void *key, void *value);
///***
// * ==================================== CRDT Hash Module Init ===========================================*/
//
//static RedisModuleType *CrdtHash;
//
int initCrdtMapModule(RedisModuleCtx *ctx) {
//    RedisModuleTypeMethods tm = {
//            .version = REDISMODULE_APIVER_1,
//            .rdb_load = RdbLoadCrdtHash,
//            .rdb_save = RdbSaveCrdtHash,
//            .aof_rewrite = AofRewriteCrdtHash,
//            .mem_usage = crdtHashMemUsageFunc,
//            .free = freeCrdtHash,
//            .digest = crdtHashDigestFunc
//    };
//
//    CrdtHash = RedisModule_CreateDataType(ctx, CRDT_HASH_DATATYPE_NAME, 0, &tm);
//    if (CrdtHash == NULL) return REDISMODULE_ERR;
//
//    // write readonly admin deny-oom deny-script allow-loading pubsub random allow-stale no-monitor fast getkeys-api no-cluster
//    if (RedisModule_CreateCommand(ctx,"HSET",
//                                  hsetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
//        return REDISMODULE_ERR;
//
//    if (RedisModule_CreateCommand(ctx,"CRDT.HSET",
//                                  CRDT_HSetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
//        return REDISMODULE_ERR;
//
//    if (RedisModule_CreateCommand(ctx,"HGET",
//                                  hgetCommand,"readonly fast",1,1,1) == REDISMODULE_ERR)
//        return REDISMODULE_ERR;
//
//    if (RedisModule_CreateCommand(ctx,"CRDT.HGET",
//                                  CRDT_HGetCommand,"readonly deny-oom",1,1,1) == REDISMODULE_ERR)
//        return REDISMODULE_ERR;
//
//    if (RedisModule_CreateCommand(ctx,"CRDT.DEL_HASH",
//                                  CRDT_DelHashCommand,"write",1,1,1) == REDISMODULE_ERR)
//        return REDISMODULE_ERR;
//
    return REDISMODULE_OK;
}
//
//
///***
// * ==================================== CRDT Lifecycle functionality ===========================================*/
//void *createCrdtHash(void) {
////    dict *hash = dictCreate(&hashDictType, NULL);
//}
//
//void freeCrdtHash(void *crdtMap) {
//
//}


