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
// Created by zhuchen on 2020/9/25.
//

#include "crdt_lww_element.h"

#if defined(LWW_ELEMENT_TEST_MAIN)
#include "stdlib.h"
#endif

void* createLwwElement() {
#if defined(LWW_ELEMENT_TEST_MAIN)
    lww_element *ele = malloc(sizeof(lww_element));//unit test only
#else
    lww_element *ele = RedisModule_Alloc(sizeof(lww_element));
#endif
    ele->type = VALUE_TYPE_INTEGER;
    ele->val.s = NULL;
    return ele;
}

void freeLwwElement(void *target) {
    lww_element *ele = (lww_element *) target;
#if defined(LWW_ELEMENT_TEST_MAIN)
    free(ele);//unit test only
#else
    if(ele->type == VALUE_TYPE_SDS && ele->val.s) {
        sdsfree(ele->val.s);
    }
    RedisModule_Free(ele);
#endif
}














#if defined(LWW_ELEMENT_TEST_MAIN)

#include <stdlib.h>

#include <stdio.h>

#include "../tests/testhelp.h"
#include "limits.h"
#include "../include/rmutil/sds.h"

int testBasicLwwElement(void) {
    printf("========[testBasicLwwElement]==========\r\n");

    lww_element *ele = createLwwElement();
    char *str = malloc(10);
    str[0] = 'h';
    str[1] = 'e';
    str[2] = '\0';
    ele->val.s = str;// sdsnew("hello world!");
    ele->type = VALUE_TYPE_SDS;
//    test_cond("[basic lww sds]", sdscmp(sdsnew("hello world!"), ele->val.s) == 0);
    printf("[basic lww sds] %s\r\n", ele->val.s);
    test_cond("[basic lww sds type]", ele->type == VALUE_TYPE_SDS);

    ele->type = VALUE_TYPE_FLOAT;
    ele->val.f = 1.3f;
    test_cond("[basic lww float]", 1.3f == ele->val.f);

    ele->type = VALUE_TYPE_INTEGER;
    ele->val.i = 10000;
    test_cond("[basic lww float]", 10000 == ele->val.i);
    return 0;
}

int testFreeLwwElement(void) {
    printf("========[testFreeLwwElement]==========\r\n");

    lww_element *ele = createLwwElement();
////    ele->val.s = sdsnew("hello world!");
//    ele->type = VALUE_TYPE_SDS;
////    test_cond("[basic lww sds]", sdscmp(sdsnew("hello world!"), ele->val.s) == 0);
//    test_cond("[basic lww sds type]", ele->type == VALUE_TYPE_SDS);
//    freeLwwElement(ele);

    ele = createLwwElement();
    ele->type = VALUE_TYPE_FLOAT;
    ele->val.f = 1.3f;
    test_cond("[basic lww float]", 1.3f == ele->val.f);
    freeLwwElement(ele);

    ele = createLwwElement();
    ele->type = VALUE_TYPE_INTEGER;
    ele->val.i = 10000;
    test_cond("[basic lww float]", 10000 == ele->val.i);
    freeLwwElement(ele);
    return 0;
}



int LwwElementTest(void) {
    int result = 0;
    {
        result |= testBasicLwwElement();
        result |= testFreeLwwElement();
    }
    test_report();
    return result;
}
#endif

#ifdef LWW_ELEMENT_TEST_MAIN
int main(void) {
    return LwwElementTest();
}
#endif