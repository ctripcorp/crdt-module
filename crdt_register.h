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
// Created by zhuchen(zhuchen at ctrip dot com) on 2019-04-16.
//

#ifndef XREDIS_CRDT_CRDT_REGISTER_H
#define XREDIS_CRDT_CRDT_REGISTER_H

#include "include/rmutil/sds.h"
#include "ctrip_crdt_common.h"
#include "include/redismodule.h"
#include "crdt_util.h"

#define CRDT_REGISTER_DATATYPE_NAME "crdt_regr"

#define DELETED_TAG "deleted"

#define DELETED_TAG_SIZE 7

typedef struct CRDT_Register {
    CrdtCommon common;
    sds val;
} CRDT_Register;

void *createCrdtRegister(void);

void freeCrdtRegister(void *crdtRegister);

CRDT_Register* dupCrdtRegister(const CRDT_Register *val);

int initRegisterModule(RedisModuleCtx *ctx);

void *crdtRegisterMerge(void *currentVal, void *value);

void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver);

void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value);

sds crdtRegisterInfo(CRDT_Register *crdtRegister);

CRDT_Register* addRegister(void *tombstone, CrdtCommon* common, sds value);
int tryUpdateRegister(void* data, CrdtCommon* common, CRDT_Register* reg, sds value);
#endif //XREDIS_CRDT_CRDT_REGISTER_H
