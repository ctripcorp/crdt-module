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

#include "include/rmutil/sds.h"
#include "ctrip_crdt_common.h"
#include "include/redismodule.h"

#define CRDT_HASH_DATATYPE_NAME "crdt_hash"

typedef struct CRDT_Hash {
    CrdtCommon common;
//    dict *map;
} CRDT_Map;


//uint64_t dictSdsHash(const void *key) {
//    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
//}
//
//int dictSdsKeyCompare(void *privdata, const void *key1,
//                      const void *key2)
//{
//    int l1,l2;
//    DICT_NOTUSED(privdata);
//
//    l1 = sdslen((sds)key1);
//    l2 = sdslen((sds)key2);
//    if (l1 != l2) return 0;
//    return 1;
////    return memcmp(key1, key2, l1) == 0;
//}
//
//void dictSdsDestructor(void *privdata, void *val)
//{
//    DICT_NOTUSED(privdata);
//
//    sdsfree(val);
//}
//
/////* Db->dict, keys are sds strings, vals are Redis objects. */
//dictType dbDictType = {
//        dictSdsHash,                /* hash function */
//        NULL,                       /* key dup */
//        NULL,                       /* val dup */
//        dictSdsKeyCompare,          /* key compare */
//        dictSdsDestructor,          /* key destructor */
//        freeCrdtRegister   /* val destructor */
//};
//
//void *createCrdtHash(void);
//
//void freeCrdtHash(void *crdtHash);
//
int initCrdtHashModule(RedisModuleCtx *ctx);


#endif //XREDIS_CRDT_CTRIP_CRDT_HASHMAP_H
