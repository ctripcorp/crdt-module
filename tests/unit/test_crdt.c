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
#include "../../crdt.h"

int testBasicMacro() {
    ASSERT_EQUAL(CRDT_OK, 1);
    ASSERT_EQUAL(CRDT_ERROR, 0);
    return 0;
}

int testGidPriority() {
    long long base = 2;
    long long target = 1;

    int result = SECOND_HIGHER_PRIORITY(base, target);
    ASSERT(result);

    base = 1, target = 2;
    result = SECOND_HIGHER_PRIORITY(base, target);
    ASSERT_EQUAL(CRDT_ERROR, result);

    base = 1, target = 1;
    result = SECOND_HIGHER_PRIORITY(base, target);
    ASSERT_EQUAL(CRDT_OK, result);

    return 0;
}

TEST_MAIN({
    TESTFUNC(testBasicMacro);
    TESTFUNC(testGidPriority);
});