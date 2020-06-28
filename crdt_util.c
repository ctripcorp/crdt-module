#include "crdt_util.h"
#include <stdio.h>
#include <string.h>

int ll2str(char* s, long long value, int len) {
    char *p, aux;
    unsigned long long v;

    /* Generate the string representation, this method produces
     * an reversed string. */
    v = (value < 0) ? -value : value;

    p = s + len;
    *p-- = '\0';
    do {
        *p-- = '0'+(v%10);
        v /= 10;
    } while(v);
    if(value < 0) *p-- = '-';

    return len;
}

size_t feedBuf(char* buf, const char* src) {
    strcpy(buf, src);
    return strlen(src);
}
size_t _feedLongLong(char *buf, long long ll) {
    size_t len = 0;
    len += sdsll2str(buf + len, ll);
    buf[len++] = '\r';
    buf[len++] = '\n';
    return len;
}
size_t feedArgc(char* buf, int argc) {
    size_t len = 0;
    buf[len++] = '*';
    len += _feedLongLong(buf + len, (long long)argc);
    return len;
}

size_t feedValStrLen(char *buf, int num) {
    size_t len = 0;
    buf[len++] = '$';
    len += _feedLongLong(buf + len, (long long)num);
    return len;
}
size_t feedValFromString(char *buf, char* str) {
    // return sprintf(buf, "%s\r\n",str);
    size_t len = 0;
    len += feedBuf(buf + len, str);
    buf[len++]='\r';
    buf[len++]='\n';
    return len;
    // return strlen(src) + 2;
}

size_t feedStr2Buf(char *buf, char* str, size_t strlen) {
    size_t len = 0;
    len += feedValStrLen(buf + len, strlen);
    len += feedValFromString(buf + len , str);
    // return sprintf(buf, str_template, strlen, str);
    return len;
}
size_t feedRobj2Buf(char *buf, RedisModuleString* src) {
    size_t srclen = 0;
    const char* srcstr = RedisModule_StringPtrLen(src, &srclen);
    return feedStr2Buf(buf, srcstr, srclen);
}
const char* gidlen1 = "$1\r\n";
const char* gidlen2 = "$2\r\n";
size_t feedGid2Buf(char *buf, int gid) {
    size_t len = 0;
    // len += feedValStrLen(buf + len, gid > 9 ? 2:1);
    if(gid > 9) {
        len += feedBuf(buf +len, gidlen2);
        len += ll2str(buf + len, (long long)gid, 2);
    } else {
        len += feedBuf(buf +len, gidlen1);
        buf[len++] = '0' + gid;
    }
    buf[len++] = '\r';
    buf[len++] = '\n';
    // len += feedBuf(buf + len, gid > 9 ? gidlen2_template: gidlen1_template);
    // len += feedLongLong(buf + len , (long long)gid);
    return len;
}
// const char* kv_template = "$%d\r\n%s\r\n$%d\r\n%s\r\n";
size_t feedKV2Buf(char *buf,char* keystr, size_t keylen, char* valstr, size_t vallen) {
    size_t len = 0;
    len += feedStr2Buf(buf + len, keystr, keylen);
    len += feedStr2Buf(buf + len, valstr, vallen);
    return len;
    // return sprintf(buf, kv_template, keylen, keystr, vallen, valstr );
}
int llstrlen(long long v) {
    int len = 0;
    if(v < 0) {
        v = -v;
        len = 1;
    }
    do {
        len += 1;
        v /= 10;
    } while(v);
    return len;
}

size_t feedLongLong2Buf(char *buf, long long v) {
    size_t len = 0;
    size_t lllen = llstrlen(v);
    len += feedValStrLen(buf + len, lllen);
    // len += _feedLongLong(buf + len, v);
    len += ll2str(buf + len, v, lllen);
    buf[len++] = '\r';
    buf[len++] = '\n';
    return len;
}
size_t feedMeta2Buf(char *buf, int gid, long long time, VectorClock vc) {
    size_t len = 0;
    // len += sprintf(buf, meta_template, gid > 9? 2: 1, gid, llstrlen(time), time, vectorClockToStringLen(vc));
    len += feedGid2Buf(buf + len, gid);
    len += feedLongLong2Buf(buf + len, time);
    len += feedVectorClock2Buf(buf + len, vc);
    // len += vectorClockToString(buf + len, vc);
    return len;
} 
size_t vcunit2buf(char* buf, int gid, long long unit) {
    size_t buflen = 0;
    buflen += ll2str(buf+buflen, (long long)gid, gid > 9? 2:1);
    buf[buflen++] = ':';
    buflen += sdsll2str(buf+buflen, unit);
    return buflen;
}
size_t vc2str(char* buf, VectorClock vc) {
    int length = get_len(vc);
    if(isNullVectorClock(vc) || length < 1) {
        return 0;
    }
    size_t buflen = 0;
    clk *vc_unit = get_clock_unit_by_index(&vc, 0);
    buflen += vcunit2buf(buf + buflen, get_gid(*vc_unit), get_logic_clock(*vc_unit));
    for (int i = 1; i < length; i++) {
        clk* vc_unit1 = get_clock_unit_by_index(&vc, i);
        buf[buflen++]=';';
        buflen += vcunit2buf(buf + buflen, get_gid(*vc_unit1), get_logic_clock(*vc_unit1));
    }
    return buflen;
}
size_t feedVectorClock2Buf(char *buf, VectorClock vc) {
    size_t len = 0;
    len += feedValStrLen(buf + len, vectorClockToStringLen(vc));
    len += vc2str(buf + len, vc);
    buf[len++]='\r';
    buf[len++]='\n';
    return len;
}
int redisModuleStringToGid(RedisModuleCtx *ctx, RedisModuleString *argv, long long *gid) {
    if ((RedisModule_StringToLongLong(argv,gid) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return REDISMODULE_ERR;
    }
    if(RedisModule_CheckGid(*gid) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be < 15");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
int readMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index, CrdtMeta* meta) {
    long long gid;
    if ((redisModuleStringToGid(ctx, argv[start_index],&gid) != REDISMODULE_OK)) {
        return 0;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[start_index+1],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return 0;
    }
    VectorClock vclock = getVectorClockFromString(argv[start_index+2]);
    meta->gid = gid;
    meta->timestamp = timestamp;
    meta->vectorClock = vclock;
    return 1;
}
CrdtMeta* getMeta(RedisModuleCtx *ctx, RedisModuleString **argv, int start_index) {
    long long gid;
    if ((redisModuleStringToGid(ctx, argv[start_index],&gid) != REDISMODULE_OK)) {
        return NULL;
    }
    long long timestamp;
    if ((RedisModule_StringToLongLong(argv[start_index+1],&timestamp) != REDISMODULE_OK)) {
        RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
        return NULL;
    }
    VectorClock vclock = getVectorClockFromString(argv[start_index+2]);
    return createMeta(gid, timestamp, vclock);
}

RedisModuleKey* getRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type, int mode) {
    RedisModuleKey *moduleKey = RedisModule_OpenKey(ctx, argv,
                                    REDISMODULE_TOMBSTONE | REDISMODULE_WRITE);                      
    int type = RedisModule_KeyType(moduleKey);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(moduleKey) != redismodule_type) {
        RedisModule_CloseKey(moduleKey);
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return NULL;
    }
    return moduleKey;
}
RedisModuleKey* getWriteRedisModuleKey(RedisModuleCtx *ctx, RedisModuleString *argv, RedisModuleType* redismodule_type) {
    return getRedisModuleKey(ctx, argv, redismodule_type, REDISMODULE_WRITE | REDISMODULE_TOMBSTONE);
}


void* getCurrentValue(RedisModuleKey *moduleKey) {
    int type = RedisModule_KeyType(moduleKey);
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ModuleTypeGetValue(moduleKey);
    }
    return NULL;
}
void* getTombstone(RedisModuleKey *moduleKey) {
    void* tombstone = RedisModule_ModuleTypeGetTombstone(moduleKey);
    return tombstone;
}

VectorClock rdbLoadVectorClock(RedisModuleIO *rdb) {
    size_t vcLength;
    char* vcStr = RedisModule_LoadStringBuffer(rdb, &vcLength);
    // sds vclockSds = sdsnewlen(vcStr, vcLength);
    // VectorClock result = sdsToVectorClock(vclockSds);
    // sdsfree(vclockSds);
    vcStr[vcLength] = '\0';
    VectorClock result = stringToVectorClock(vcStr);
    zfree(vcStr);
    return result;
}
int rdbSaveVectorClock(RedisModuleIO *rdb, VectorClock vectorClock) {
    // sds vclockStr = vectorClockToSds(vectorClock);
    size_t len = vectorClockToStringLen(vectorClock);
    char buf[len];
    vectorClockToString(&buf, vectorClock);
    RedisModule_SaveStringBuffer(rdb, buf, len);
    // sdsfree(vclockStr);
    return CRDT_OK;
}