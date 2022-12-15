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
// Created by zhuchen on 2019-04-30.
//
#include <rmutil/test.h>
#include "../../ctrip_crdt_register.h"
#include <rmutil/sds.h>

typedef struct crdtRegisterSerial {
    sds vectorClock;
    long long gid;
    long long timestamp;
    sds val;
}crdtRegisterSerial;

void *testSerializeCrdtRegister(void *value) {
    printf("[testSerializeCrdtRegister]\r\n");
    CRDT_Register *crdtRegister = value;
    crdtRegisterSerial *temp = malloc(sizeof(crdtRegisterSerial));
    temp->vectorClock = crdtRegister->common.vectorClock;
    temp->gid = crdtRegister->common.gid;
    temp->timestamp = crdtRegister->common.timestamp;
    temp->val = crdtRegister->val;
    printf("[before map]\r\n");
    tpl_node *tn = tpl_map("S(sIIs)", temp);
    printf("[after map]\r\n");
    sds serialized = sdsempty();
    size_t length = sdslen(temp->val) + sdslen(temp->vectorClock) + sizeof(long long) * 2 + 30;
    sdsMakeRoomFor(serialized, length);
    tpl_pack(tn, 0);
    printf("[before dump]\r\n");
    tpl_dump(tn, TPL_MEM|TPL_PREALLOCD, serialized, length);
    printf("[before free]\r\n");
    tpl_free(tn);
    printf("[before RedisModule_Free]\r\n");
    free(temp);
    return serialized;
}

void *testDeserializeCrdtRegister(sds value) {
    CRDT_Register *crdtRegister = createCrdtRegister();
    crdtRegisterSerial *temp = RedisModule_Alloc(sizeof(crdtRegisterSerial));
    tpl_node *tn = tpl_map("S(sIIs)", temp);
    tpl_load(tn, TPL_MEM|TPL_EXCESS_OK, value, sdslen(value));
    tpl_unpack(tn, 0);
    tpl_free(tn);
    crdtRegister->val = temp->val;
    crdtRegister->common.timestamp = temp->timestamp;
    crdtRegister->common.gid = temp->gid;
    crdtRegister->common.vectorClock = temp->vectorClock;
    return crdtRegister;
}

void *fakeMerge(void *val1, void *val2) {
    if (val1) {
        return val1;
    }
    return val2;
}
int testSerialize() {
    CRDT_Register *crdtRegister = malloc(sizeof(CRDT_Register));
    crdtRegister->val = sdsnew("hello world!");
    crdtRegister->common.merge = fakeMerge;
    crdtRegister->common.gid = 1;
    crdtRegister->common.timestamp = 1560310032445;
    crdtRegister->common.vectorClock = sdsnew("1:100");

    printf("\r\n");
    printf("[before]serialized: %s\r\n", crdtRegister->val);
    sds serialized = testSerializeCrdtRegister(crdtRegister);
    printf("[after]serialized: %s\r\n", serialized);
    return 0;
}

int testDeserialize() {
    CRDT_Register *crdtRegister = malloc(sizeof(CRDT_Register));
    crdtRegister->val = sdsnew("hello world!");
    crdtRegister->common.merge = fakeMerge;
    crdtRegister->common.gid = 1;
    crdtRegister->common.timestamp = 1560310032445;
    crdtRegister->common.vectorClock = sdsnew("1:100");

    sds serialized = testSerializeCrdtRegister(crdtRegister);
    printf("serialized: %s\r\n", serialized);

    CRDT_Register *crdtRegister2 = testDeserializeCrdtRegister(serialized);

    printf("deserialized[val]: %s\r\n", crdtRegister2->val);
    printf("deserialized[vector clock]: %s\r\n", crdtRegister2->common.vectorClock);
    printf("deserialized[gid]: %lld\r\n", crdtRegister2->common.gid);
    printf("deserialized[timestamp]: %lld\r\n", crdtRegister2->common.timestamp);
    ASSERT_EQUAL(crdtRegister->common.timestamp, crdtRegister2->common.timestamp);
    ASSERT_EQUAL(crdtRegister->common.gid, crdtRegister2->common.gid);
    ASSERT_STRING_EQ(crdtRegister->common.vectorClock, crdtRegister2->common.vectorClock);
    ASSERT_STRING_EQ(crdtRegister->val, crdtRegister2->val);
    return 0;
}


TEST_MAIN({
              TESTFUNC(testSerialize);
              TESTFUNC(testDeserialize);
          });


