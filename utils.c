//
// Created by zhuchen on 2019-04-25.
//

#include "utils.h"

static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

long long mstime(void) {
    return ustime()/1000;
}

sds moduleString2Sds(RedisModuleString *argv) {
    size_t sdsLength;
    const char *str = RedisModule_StringPtrLen(argv, &sdsLength);
    return sdsnewlen(str, sdsLength);
}

