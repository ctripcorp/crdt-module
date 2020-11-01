#include "crdt_util.h"
#include "include/redismodule.h"
#include <stdio.h>
#include <string.h>
#include <ctype.h>

int ll2str(char* s, long long value, int len) {
    char *p;
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

// size_t feedBuf(char* buf, const char* src) {
//     strcpy(buf, src);
//     return strlen(src);
// }
size_t feedBuf(char* buf, const char* src, size_t len) {
    memcpy(buf, src, len);
    return len;
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

size_t feedValFromString(char *buf, const char* str, size_t size) {
    // return sprintf(buf, "%s\r\n",str);
    size_t len = 0;
    len += feedBuf(buf + len, str, size);
    buf[len++]='\r';
    buf[len++]='\n';
    return len;
    // return strlen(src) + 2;
}



size_t feedStr2Buf(char *buf, const char* str, size_t strlen) {
    size_t len = 0;
    len += feedValStrLen(buf + len, strlen);
    len += feedValFromString(buf + len , str, strlen);
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
        len += feedBuf(buf +len, gidlen2, strlen(gidlen2));
        len += ll2str(buf + len, (long long)gid, 2);
    } else {
        len += feedBuf(buf +len, gidlen1, strlen(gidlen1));
        buf[len++] = '0' + gid;
    }
    buf[len++] = '\r';
    buf[len++] = '\n';
    // len += feedBuf(buf + len, gid > 9 ? gidlen2_template: gidlen1_template);
    // len += feedLongLong(buf + len , (long long)gid);
    return len;
}
// const char* kv_template = "$%d\r\n%s\r\n$%d\r\n%s\r\n";
size_t feedKV2Buf(char *buf,const char* keystr, size_t keylen,const char* valstr, size_t vallen) {
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
                                    mode);                      
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

//update in rdb version 1: optimize rdb save/load vector clock by saving/load it as long long
// insteadof a string 09/03/2020
VectorClock rdbLoadVectorClock(RedisModuleIO *rdb, int version) {
    if(version == 0) {
        sds vcStr = RedisModule_LoadSds(rdb);
        VectorClock result = stringToVectorClock(vcStr);
        sdsfree(vcStr);
        return result;
    } else if(version == 1) {
        int length = RedisModule_LoadSigned(rdb);
        if (length == 1) {
#if defined(TCL_TEST)
            VectorClock result = newVectorClock(length);
            uint64_t clock = RedisModule_LoadUnsigned(rdb);
            clk clock_unit = VCU(clock);
            set_clock_unit_by_index(&result, (char) 0, clock_unit);
            return result;
#else
            uint64_t vclock = RedisModule_LoadUnsigned(rdb);
            return LL2VC(vclock);
#endif
        } else {
            VectorClock result = newVectorClock(length);
            for (int i = 0; i < length; i++) {
                uint64_t clock = RedisModule_LoadUnsigned(rdb);
                clk clock_unit = VCU(clock);
                set_clock_unit_by_index(&result, (char) i, clock_unit);
            }
            return result;
        }
    } else {
        return newVectorClock(0);
    }
}

//update in rdb version 1: optimize rdb save/load vector clock by saving/load it as long long
// insteadof a string. but saving is not need for compatibility 09/03/2020
int rdbSaveVectorClock(RedisModuleIO *rdb, VectorClock vectorClock, int version) {
    if(version < 1) {
        RedisModule_Debug(CRDT_DEFAULT_LOG_LEVEL, "[rdbSaveVectorClock]end early");
        return CRDT_OK;
    }
    int length = (int) get_len(vectorClock);
    RedisModule_SaveSigned(rdb, length);
    if(length == 1) {
#if defined(TCL_TEST)
        clk *vc_unit = get_clock_unit_by_index(&vectorClock, 0);
        RedisModule_SaveUnsigned(rdb, VC2LL((*vc_unit)));
#else
        uint64_t vclock = VC2LL(vectorClock);
        RedisModule_SaveUnsigned(rdb, vclock);
#endif
    } else {
        for (int i = 0; i < length; i++) {
            clk *vc_unit = get_clock_unit_by_index(&vectorClock, (char)i);
            RedisModule_SaveUnsigned(rdb, VC2LL((*vc_unit)));
        }
    }
    return CRDT_OK;
}

// long double rdbLoadLongDouble(RedisModuleIO *rdb, int version) {
//     size_t ldLength = 0;
//     sds ldstr = RedisModule_LoadSds(rdb);
//     long double ld = 0;
//     RedisModule_Debug(logLevel, "load long double: %s",ldstr);
//     assert(string2ld(ldstr, sdslen(ldstr), &ld) == 1);
//     RedisModule_Debug(logLevel, "load long double over");
//     sdsfree(ldstr);
//     return ld;
// }

// #define MAX_LONG_DOUBLE_CHARS 5*1024
// int rdbSaveLongDouble(RedisModuleIO *rdb, long double ld) {
//     char buf[MAX_LONG_DOUBLE_CHARS];
//     int len = ld2string(buf,sizeof(buf),ld,1);
//     sds s = sdsnewlen(buf, (size_t)len);
//     RedisModule_Debug(logLevel, "save long double %.33Lf %s", ld, s);
//     RedisModule_SaveStringBuffer(rdb, s, sdslen(s));
//     sdsfree(s);
//     return 1;
// }
long double rdbLoadLongDouble(RedisModuleIO *rdb, int version) {
    sds ldstr = RedisModule_LoadSds(rdb);
    // assert(string2ld(ldstr, sdslen(ldstr), &ld) == 1);
    long double ld = *(long double*)(ldstr);
    sdsfree(ldstr);
    return ld;
}

int rdbSaveLongDouble(RedisModuleIO *rdb, long double ld) {
    //  char buf[MAX_LONG_DOUBLE_CHARS];
    // int len = ld2string(buf,sizeof(buf),ld,1);
    sds s = sdsnewlen((char*)&ld, sizeof(long double));
    // RedisModule_Debug(logLevel, "save long double %.33Lf %s", ld, s);
    RedisModule_SaveStringBuffer(rdb, s, sdslen(s));
    sdsfree(s);
    return 1;
}

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
                      const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    sdsfree(val);
}

/*-----------------------------------------------------------------------------
 * DECODE Functionality
 *----------------------------------------------------------------------------*/

int getLongLongFromObjectOrReply(RedisModuleCtx *ctx, RedisModuleString *o, long long *target, const char *msg) {
    long long value;
    if (RedisModule_StringToLongLong(o, &value) == REDISMODULE_ERR) {
        if (msg != NULL) {
            RedisModule_ReplyWithStringBuffer(ctx, (char*)msg, strlen(msg));
        } else {
            RedisModule_ReplyWithError(ctx, "value is not an integer or out of range");
        }
        return CRDT_ERROR;
    }
    *target = value;
    return CRDT_OK;
}

int getLongFromObjectOrReply(RedisModuleCtx *ctx, RedisModuleString *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(ctx, o, &value, msg) != CRDT_OK) return CRDT_ERROR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            RedisModule_ReplyWithStringBuffer(ctx, (char*)msg, strlen(msg));
        } else {
            RedisModule_ReplyWithError(ctx, "value is out of range");
        }
        return CRDT_ERROR;
    }
    *target = value;
    return CRDT_OK;
}

void replySyntaxErr(RedisModuleCtx *ctx) {
    RedisModule_ReplyWithError(ctx, "syntax error");
}

void replyEmptyScan(RedisModuleCtx *ctx) {
    // shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, "0", 1);
    RedisModule_ReplyWithArray(ctx, 0);
}

/*-----------------------------------------------------------------------------
 * SCAN Functionality
 *----------------------------------------------------------------------------*/

/* This callback is used by scanGenericCommand in order to collect elements
 * returned by the dictionary iterator into a list. */
void scanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    list *keys = pd[0];
    int *type = pd[1];
    sds key = NULL, val = NULL;

    if (*type == CRDT_HASH_TYPE) {
        key = dictGetKey(de);
        //todo: hash should take outof the val as sds
        // not sure if it's a or-set or lww-element hash, so just leave it for successor's brilliant
//        val = dictGetVal(de);
//        key = createStringObject(sdskey,sdslen(sdskey));
//        val = createStringObject(sdsval,sdslen(sdsval));
    } else if (*type == CRDT_SET_TYPE) {
        key = dictGetKey(de);
//        key = createStringObject(keysds,sdslen(keysds));
    }

    listAddNodeTail(keys, key);
    if (val) listAddNodeTail(keys, val);
}

/* Try to parse a SCAN cursor stored at object 'o':
 * if the cursor is valid, store it as unsigned integer into *cursor and
 * returns C_OK. Otherwise return C_ERR and send an error to the
 * client. */
int parseScanCursorOrReply(RedisModuleCtx *ctx, RedisModuleString *inputCursor, unsigned long *cursor) {
    char *eptr;

    /* Use strtoul() because we need an *unsigned* long, so
     * getLongLongFromObject() does not cover the whole cursor space. */
    sds inputCursorStr = RedisModule_GetSds(inputCursor);
    *cursor = strtoul(inputCursorStr, &eptr, 10);
    if (isspace(((char*)inputCursorStr)[0]) || eptr[0] != '\0') {
        RedisModule_ReplyWithError(ctx, "invalid cursor");
        return CRDT_ERROR;
    }
    return CRDT_OK;
}

/* This command implements SCAN, HSCAN and SSCAN commands.
 * If object 'o' is passed, then it must be a Hash or Set object, otherwise
 * if 'o' is NULL the command will operate on the dictionary associated with
 * the current database.
 *
 * When 'o' is not NULL the function assumes that the first argument in
 * the client arguments vector is a key so it skips it before iterating
 * in order to parse options.
 *
 * In the case of a Hash object the function returns both the field and value
 * of every element on the Hash. */
void scanGenericCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, dict *ht, int type, unsigned long cursor) {
    int i, j;
    list *keys = listCreate();
    listNode *node, *nextnode;
    long count = 10;
    sds pat = NULL;
    int patlen = 0, use_pattern = 0;

    //copy from redis: i = (o == NULL) ? 2 : 3; /* Skip the key argument if needed. */
    i = 3;
    /* Step 1: Parse options. */
    while (i < argc) {
        j = argc - i;
        if (!strcasecmp(RedisModule_GetSds(argv[i]), "count") && j >= 2) {
            if (getLongFromObjectOrReply(ctx, argv[i+1], &count, NULL) != CRDT_OK) {
                goto cleanup;
            }

            if (count < 1) {
                replySyntaxErr(ctx);
                goto cleanup;
            }

            i += 2;
        } else if (!strcasecmp(RedisModule_GetSds(argv[i]), "match") && j >= 2) {
            pat = RedisModule_GetSds(argv[i+1]);
            patlen = sdslen(pat);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            use_pattern = !(pat[0] == '*' && patlen == 1);

            i += 2;
        } else {
            replySyntaxErr(ctx);
            goto cleanup;
        }
    }

    /* Step 2: Iterate the collection.
     *
     * Note that if the object is encoded with a ziplist, intset, or any other
     * representation that is not a hash table, we are sure that it is also
     * composed of a small number of elements. So to avoid taking state we
     * just return everything inside the object in a single call, setting the
     * cursor to zero to signal the end of the iteration. */

    /* Handle the case of a hash table. */
    if(type == CRDT_HASH_TYPE) {
        count *= 2;
    }

    if (ht) {
        void *privdata[2];
        /* We set the max number of iterations to ten times the specified
         * COUNT, so if the hash table is in a pathological state (very
         * sparsely populated) we avoid to block too much time at the cost
         * of returning no or very few elements. */
        long maxiterations = count*10;

        /* We pass two pointers to the callback: the list to which it will
         * add new elements, and the object containing the dictionary so that
         * it is possible to fetch more data in a type-dependent way. */
        privdata[0] = keys;
        privdata[1] = &type;
        do {
            cursor = dictScan(ht, cursor, scanCallback, NULL, privdata);
        } while (cursor &&
                 maxiterations-- &&
                 listLength(keys) < (unsigned long)count);
    }

    /* Step 3: Filter elements. */
    node = listFirst(keys);
    while (node) {
        sds kobj = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        if (!filter && use_pattern) {
            if (!stringmatchlen(pat, patlen, kobj, sdslen(kobj), 0))
                filter = 1;
        }
        /* Filter element if it is an expired key.
         * no need here, as we won't do a db-scan inside a crdt module */
//        if (!filter && o == NULL && expireIfNeeded(c->db, kobj)) filter = 1;

        /* Remove the element and its associted value if needed. */
        if (filter) {
            listDelNode(keys, node);
        }

        /* If this is a hash or a sorted set, we have a flat list of
         * key-value elements, so if this element was filtered, remove the
         * value, or skip it if it was not filtered: we only match keys. */
        if (type == CRDT_HASH_TYPE) {
            node = nextnode;
            nextnode = listNextNode(node);
            if (filter) {
                kobj = listNodeValue(node);
                listDelNode(keys, node);
            }
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, cursor);

    RedisModule_ReplyWithArray(ctx, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        sds kobj = listNodeValue(node);
        RedisModule_ReplyWithStringBuffer(ctx, kobj, sdslen(kobj));
        listDelNode(keys, node);
    }

cleanup:
    listSetFreeMethod(keys, NULL);
    listRelease(keys);

}
