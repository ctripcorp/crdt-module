#include <stdio.h>
#include "ctrip_swap.h"
#include "include/redismodule.h"
#include "util.h"
#include "ctrip_vector_clock.h"
#include "include/rmutil/sds.h"
#include "ctrip_stream_io.h"
#include "crdt_util.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
#include "ctrip_crdt_hashmap.h"

typedef sds (*encodeKeyFunc)(RedisModuleString *keyobj);
typedef sds (*encodeValFunc)(RedisModuleKey *key);
typedef void *(*decodeValFunc)(sds rawval);
typedef void (*freeValFunc)(void *val);

typedef struct {
    RedisModuleType *mt;
    encodeKeyFunc encode_key;
    encodeValFunc encode_val;
    decodeValFunc decode_val;
    freeValFunc free_val;
} wholeKeySwapType;

typedef struct {
    wholeKeySwapType table[8];
    int used;
}wholeKeySwapTypeTable;

static wholeKeySwapTypeTable wksTable;

int initWholeKeySwapTypeTable() {
    wksTable.used = 0;

    if (getCrdtRegister() == NULL) return REDISMODULE_ERR;
    wksTable.table[wksTable.used++] = (wholeKeySwapType){
        .mt = getCrdtRegister(),
        .encode_key = encodeKeyCrdtString,
        .encode_val = encodeValCrdtRegister,
        .decode_val = decodeValCrdtRegister,
        .free_val = freeCrdtRegister,
    };

    if (getCrdtRc() == NULL) return REDISMODULE_ERR;
    wksTable.table[wksTable.used++] = (wholeKeySwapType){
        .mt = getCrdtRc(),
        .encode_key = encodeKeyCrdtString,
        .encode_val = encodeValCrdtRc,
        .decode_val = decodeValCrdtRC,
        .free_val = freeCrdtRc,
    };

    if (getCrdtHash() == NULL) return REDISMODULE_ERR;
    wksTable.table[wksTable.used++] = (wholeKeySwapType){
        .mt = getCrdtHash(),
        .encode_key = encodeKeyCrdtHash,
        .encode_val = encodeValCrdtHash,
        .decode_val = decodeValCrdtHash,
        .free_val = freeCrdtHash,
    };

    return REDISMODULE_OK;
}

wholeKeySwapType *lookupWholeKeySwapType(RedisModuleType *mt) {
    int i;
    wholeKeySwapType *result = NULL;

    for (i = 0; i < wksTable.used; i++) {
        wholeKeySwapType *wks = wksTable.table+i;
        if (mt == wks->mt) {
            result = wks;
            break;
        }
    }
    crdtAssert(result != NULL);

    return result;
}

int initSwap() {
    return initWholeKeySwapTypeTable();
}

void *lookupSwappingClientsWk(RedisModuleCtx *ctx, RedisModuleString *keyobj, RedisModuleString *subkeyobj) {
    void *scs = NULL;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    if (RedisModule_ModuleTypeEvictExists(key)) scs = RedisModule_ModuleTypeEvictGetSCS(key);
    RedisModule_CloseKey(key);
    return scs;
}
 
void setupSwappingClientsWk(RedisModuleCtx *ctx, RedisModuleString *keyobj, RedisModuleString *subkeyobj, void *scs) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    if (RedisModule_ModuleTypeEvictExists(key)) {
        if (scs != NULL) {
            /* overwrite with new scs */
            crdtAssert(RedisModule_ModuleTypeEvictSetSCS(key, scs) == REDISMODULE_OK);
        } else {
            if (!RedisModule_ModuleTypeEvictEvicted(key)) {
                /* delete key.evict if key not evicted and new scs is NULL */
                crdtAssert(RedisModule_DeleteEvict(key) == REDISMODULE_OK);
            } else {
                /* clear scs and scs flag if key evicted and new scs is NULL */ 
                crdtAssert(RedisModule_ModuleTypeEvictSetSCS(key, scs) == REDISMODULE_OK);
            }
        }
    } else {
        if (scs != NULL) {
            /* create new key.evict if not exists. */
            crdtAssert(RedisModule_ModuleTypeAddEvict(key) == REDISMODULE_OK);
            crdtAssert(RedisModule_ModuleTypeEvictSetSCS(key,scs) == REDISMODULE_OK);
        }
    }
    RedisModule_CloseKey(key);
}

void getDataSwapsWk(RedisModuleCtx *ctx, RedisModuleString *keyobj, int mode, RedisModuleGetSwapsResult *result) {
    RedisModule_RetainString(NULL, keyobj);
    RedisModule_GetSwapsAppendResult(result, keyobj, NULL, NULL);
}

void swapInWk(RedisModuleCtx *ctx, int action, char* _rawkey, char *_rawval, void *pd) {
    CrdtObject *val;
    sds rawkey = _rawkey, rawval = _rawval;
    RedisModuleString *keyobj = pd;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);

    wholeKeySwapType *wks = lookupWholeKeySwapType(mt);
    val = wks->decode_val(rawval);
    crdtAssert(RedisModule_ModuleTypeSwapIn(key, val) == REDISMODULE_OK);

    RedisModule_FreeString(NULL, keyobj);
    RedisModule_CloseKey(key);
}

static void doSwapOutWk(RedisModuleKey *key) {
    CrdtObject *old;
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);
    wholeKeySwapType *wks = lookupWholeKeySwapType(mt);
    crdtAssert(RedisModule_ModuleTypeSwapOut(key, (void**)&old) == REDISMODULE_OK);
    wks->free_val(old);
}

void swapOutWk(RedisModuleCtx *ctx, int action, char* _rawkey, char *_rawval, void *pd) {
    RedisModuleString *keyobj = pd;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    
    doSwapOutWk(key);

    RedisModule_FreeString(NULL, keyobj);
    RedisModule_CloseKey(key);
}

void deleteWk(RedisModuleCtx *ctx, int action, char* _rawkey, char *_rawval, void *pd) {
    RedisModuleString *keyobj = pd;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_WRITE|REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    crdtAssert(RedisModule_DeleteKey(key) == REDISMODULE_OK);
    crdtAssert(RedisModule_DeleteEvict(key) == REDISMODULE_OK);
    RedisModule_FreeString(NULL, keyobj);
    RedisModule_CloseKey(key);
}

/* `HMGET key f1 f2` if key is string(RC or Register), we would swap in key
 * and then reply WRONGTYPE in command proc.
 * NOTE: rawkey and rawval is returned as SDS, but module and server are
 * linking to their own SDS library(which could be different), to solve this:
 * WE NEED TO MAKE SURE MODULE AND SERVER USE THE SAME SDS LIB. */
int swapAnaWk(RedisModuleCtx *ctx, RedisModuleString *keyobj, 
        RedisModuleString *subkeyobj, int *action, char **rawkey,
        char **rawval, RedisModuleSwapFinishedCallback *cb, void **pd) {
    int swapaction = RedisModule_GetSwapAction(ctx);
    RedisModuleKey *key;

    key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);
    wholeKeySwapType *wks = lookupWholeKeySwapType(mt);

    /* NOTE that rawkey/rawval ownership are moved to rocks, which will be
     * freed when rocksSwapFinished. Also NOTE that if CRDT key expired, we
     * should swapin before delete because crdtPropagateExpire needs value. */ 
    if (RedisModule_KeyIsExpired(key) &&
            RedisModule_ModuleTypeEvictEvicted(key)) {
        *action = REDISMODULE_SWAP_GET;
        *rawkey = wks->encode_key(keyobj);
        *rawval = NULL;
        *cb = swapInWk;
        RedisModule_RetainString(NULL, keyobj);
        *pd = keyobj;
    } else if (swapaction == REDISMODULE_SWAP_GET &&
            RedisModule_ModuleTypeEvictEvicted(key)) {
        *action = REDISMODULE_SWAP_GET;
        *rawkey = wks->encode_key(keyobj);
        *rawval = NULL;
        *cb = swapInWk;
        RedisModule_RetainString(NULL, keyobj);
        *pd = keyobj;
    } else if (swapaction == REDISMODULE_SWAP_PUT &&
            RedisModule_ModuleTypeGetValue(key) != NULL) {
        /* Only dirty key needs to swap out to rocksdb, non-dirty key could be
         * freed right away (no rocksdb IO needed). */
        if (!RedisModule_ModuleTypeGetDirty(key)) {
            doSwapOutWk(key);

            *action = REDISMODULE_SWAP_NOP;
            *rawkey = NULL;
            *rawval = NULL;
            *cb = NULL;
            *pd = NULL;
        } else {
            *action = REDISMODULE_SWAP_PUT;
            *rawkey = wks->encode_key(keyobj);
            *rawval = wks->encode_val(key);
            *cb = swapOutWk;
            RedisModule_RetainString(NULL, keyobj);
            *pd = keyobj;
        }
    } else if (swapaction == REDISMODULE_SWAP_DEL) {
        *action = REDISMODULE_SWAP_DEL;
        *rawkey = wks->encode_key(keyobj);
        *rawval = NULL;
        *cb = NULL;
        *pd = NULL;
    } else {
        *action = REDISMODULE_SWAP_NOP;
        *rawkey = NULL;
        *rawval = NULL;
        *cb = NULL;
        *pd = NULL;
    }

    RedisModule_CloseKey(key);
    return 0;
}

int complementWk(void **pdupptr, char* _rawkey, char *_rawval, void *pd) {
    wholeKeySwapType *wks = pd;
    sds rawkey = _rawkey, rawval = _rawval;
    crdtAssert(pdupptr);
    crdtAssert(*pdupptr == NULL);
    *pdupptr = wks->decode_val(rawval);
    return 0;
}

void *getComplementSwapsWk(RedisModuleCtx *ctx,
        RedisModuleString *keyobj,
        RedisModuleGetSwapsResult *result,
        RedisModuleComplementObjectFunc *comp, void **pd) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);
    wholeKeySwapType *wks = lookupWholeKeySwapType(mt);
    crdtAssert(RedisModule_ModuleTypeEvictEvicted(key));

    sds rawkey = wks->encode_key(keyobj);
    RedisModule_GetSwapsAppendResult(result, (RedisModuleString*)rawkey, NULL, NULL);

    *comp = complementWk;
    *pd = wks;

    RedisModule_CloseKey(key);
    return NULL;
}
