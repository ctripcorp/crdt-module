#include <stdio.h>
#include "ctrip_swap.h"
#include "util.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
#include "ctrip_vector_clock.h"
#include "include/rmutil/sds.h"
#include "ctrip_stream_io.h"

/* -------------------------------- String ---------------------------------*/
#define RKS_TYPE_CRDT_STRING 			"MK"
#define RKS_TYPE_CRDT_STRING_LEN 		(sizeof(RKS_TYPE_CRDT_STRING)-1)

void *lookupSwappingClientsString(RedisModuleCtx *ctx, RedisModuleString *keyobj, RedisModuleString *subkeyobj) {
    void *scs = NULL;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    if (RedisModule_ModuleTypeEvictExists(key)) scs = RedisModule_ModuleTypeEvictGetSCS(key);
    RedisModule_CloseKey(key);
    return scs;
}
 
void setupSwappingClientsString(RedisModuleCtx *ctx, RedisModuleString *keyobj, RedisModuleString *subkeyobj, void *scs) {
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

void getDataSwapsString(RedisModuleCtx *ctx, RedisModuleString *keyobj, int mode, RedisModuleGetSwapsResult *result) {
    RedisModule_RetainString(NULL, keyobj);
    RedisModule_GetSwapsAppendResult(result, keyobj, NULL, NULL);
}

sds encodeKeyString(RedisModuleString *keyobj) {
    sds rawkey = sdsempty();
    rawkey = sdscatlen(rawkey, RKS_TYPE_CRDT_STRING, RKS_TYPE_CRDT_STRING_LEN);
    rawkey = sdscatsds(rawkey, RedisModule_GetSds(keyobj));
    return rawkey;
}

sds encodeValCrdtRegister(RedisModuleKey *key) {
    sio *io = sdsStreamCreate(sdsempty());
    sioSaveCrdtRegister(io, RedisModule_ModuleTypeGetValue(key));
    return sdsStreamRelease(io);
}

sds encodeValCrdtRc(RedisModuleKey *key) {
    sio *io = sdsStreamCreate(sdsempty());
    sioSaveCrdtRc(io, RedisModule_ModuleTypeGetValue(key));
    return sdsStreamRelease(io);
}

sds encodeValString(RedisModuleKey *key) {
    sds rawval = NULL;
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);

    if (mt == getCrdtRegister()) {
        rawval = encodeValCrdtRegister(key);
    } else if (mt == getCrdtRc() ) {
        rawval = encodeValCrdtRc(key);
    } else {
        /* unexpected. */
    }

    return rawval;
}

void *sioLoadCrdtRc(sio *io, int encver);
CRDT_RC *decodeValCrdtRC(sds rawval) {
    sio *io = sdsStreamCreate(rawval);
    CRDT_RC *rc = sioLoadCrdtRc(io, 0); /* encver not used */
    sdsStreamRelease(io);
    return rc;
}

CRDT_Register *decodeValCrdtRegister(sds rawval) {
    sio *io = sdsStreamCreate(rawval);
    CRDT_Register *reg = sioLoadCrdtRegister(io, 0);
    sdsStreamRelease(io);
    return reg;
}

void swapInString(RedisModuleCtx *ctx, int action, char* _rawkey, char *_rawval, void *pd) {
    CrdtObject *val;
    sds rawkey = _rawkey, rawval = _rawval;
    RedisModuleString *keyobj = pd;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);

    if (mt == getCrdtRegister()) {
        val = decodeValCrdtRegister(rawval);
        crdtAssert(RedisModule_ModuleTypeSwapIn(key, val) == REDISMODULE_OK);
    } else if (mt == getCrdtRc()) {
        val = decodeValCrdtRC(rawval);
        crdtAssert(RedisModule_ModuleTypeSwapIn(key, val) == REDISMODULE_OK);
    } else {
        /* unexpected. */
    }

    RedisModule_FreeString(NULL, keyobj);
    RedisModule_CloseKey(key);
}

static void swapOutStringKey(RedisModuleKey *key) {
    CrdtObject *old;
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);

    if (mt == getCrdtRegister()) {
        crdtAssert(RedisModule_ModuleTypeSwapOut(key, (void**)&old) == REDISMODULE_OK);
        freeCrdtRegister(old);
    } else if (mt == getCrdtRc()) {
        crdtAssert(RedisModule_ModuleTypeSwapOut(key, (void**)&old) == REDISMODULE_OK);
        freeCrdtRc(old);
    } else {
        /* unexpected. */
    }
}

void swapOutString(RedisModuleCtx *ctx, int action, char* _rawkey, char *_rawval, void *pd) {
    sds rawkey = _rawkey, rawval = _rawval;
    RedisModuleString *keyobj = pd;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    
    swapOutStringKey(key);

    RedisModule_FreeString(NULL, keyobj);
    RedisModule_CloseKey(key);
}

void deleteString(RedisModuleCtx *ctx, int action, char* _rawkey, char *_rawval, void *pd) {
    RedisModuleString *keyobj = pd;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_WRITE|REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    crdtAssert(RedisModule_DeleteKey(key) == REDISMODULE_OK);
    crdtAssert(RedisModule_DeleteEvict(key) == REDISMODULE_OK);
    RedisModule_FreeString(NULL, keyobj);
    RedisModule_CloseKey(key);
}

/* `HMGET key f1 f2` if key is string(RC or Register), we would swap in key
 * and then reply WRONGTYPE in command proc.
 * TODO: rawkey and rawval is returned as SDS, but module and server are
 * linking to their own SDS library(which could be different), to solve this:
 * WE NEED TO MAKE SURE MODULE AND SERVER USE THE SAME SDS LIB. */
int swapAnaString(RedisModuleCtx *ctx, RedisModuleString *keyobj, 
        RedisModuleString *subkeyobj, int *action, char **rawkey,
        char **rawval, RedisModuleSwapFinishedCallback *cb, void **pd) {
    int swapaction = RedisModule_GetSwapAction(ctx);
    RedisModuleKey *key;

    key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);

    /* NOTE that rawkey/rawval ownership are moved to rocks, which will be
     * freed when rocksSwapFinished. Also NOTE that if CRDT key expired, we
     * should swapin before delete because crdtPropagateExpire needs value. */ 
    if (RedisModule_KeyIsExpired(key) &&
            RedisModule_ModuleTypeEvictEvicted(key)) {
        *action = REDISMODULE_SWAP_GET;
        *rawkey = encodeKeyString(keyobj);
        *rawval = NULL;
        *cb = swapInString;
        RedisModule_RetainString(NULL, keyobj);
        *pd = keyobj;
    } else if (swapaction == REDISMODULE_SWAP_GET &&
            RedisModule_ModuleTypeEvictEvicted(key)) {
        *action = REDISMODULE_SWAP_GET;
        *rawkey = encodeKeyString(keyobj);
        *rawval = NULL;
        *cb = swapInString;
        RedisModule_RetainString(NULL, keyobj);
        *pd = keyobj;
    } else if (swapaction == REDISMODULE_SWAP_PUT &&
            RedisModule_ModuleTypeGetValue(key) != NULL) {
        /* Only dirty key needs to swap out to rocksdb, non-dirty key could be
         * freed right away (no rocksdb IO needed). */
        if (!RedisModule_ModuleTypeGetDirty(key)) {
            swapOutStringKey(key);

            *action = REDISMODULE_SWAP_NOP;
            *rawkey = NULL;
            *rawval = NULL;
            *cb = NULL;
            *pd = NULL;
        } else {
            *action = REDISMODULE_SWAP_PUT;
            *rawkey = encodeKeyString(keyobj);
            *rawval = encodeValString(key);
            *cb = swapOutString;
            RedisModule_RetainString(NULL, keyobj);
            *pd = keyobj;
        }
    } else if (swapaction == REDISMODULE_SWAP_DEL) {
        *action = REDISMODULE_SWAP_DEL;
        *rawkey = encodeKeyString(keyobj);
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

int complementCrdtRegister(void **pdupptr, char* _rawkey, char *_rawval, void *pd) {
    sds rawkey = _rawkey, rawval = _rawval;
    crdtAssert(pdupptr);
    crdtAssert(*pdupptr == NULL);
    *pdupptr = decodeValCrdtRegister(rawval);
    return 0;
}

int complementCrdtRc(void **pdupptr, char* _rawkey, char *_rawval, void *pd) {
    sds rawkey = _rawkey, rawval = _rawval;
    crdtAssert(pdupptr);
    crdtAssert(*pdupptr == NULL);
    *pdupptr = decodeValCrdtRC(rawval);
    return 0;
}

void *getComplementSwapsString(RedisModuleCtx *ctx,
        RedisModuleString *keyobj,
        RedisModuleGetSwapsResult *result,
        RedisModuleComplementObjectFunc *comp, void **pd) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyobj, REDISMODULE_EVICT|REDISMODULE_OPEN_KEY_NOEXPIRE);
    RedisModuleType *mt = RedisModule_ModuleTypeGetType(key);
    crdtAssert(RedisModule_ModuleTypeEvictEvicted(key));

    sds rawkey = encodeKeyString(keyobj);
    RedisModule_GetSwapsAppendResult(result, (RedisModuleString*)rawkey, NULL, NULL);
    if (mt == getCrdtRegister()) {
        *comp = complementCrdtRegister;
        *pd = NULL;
    } else if (mt == getCrdtRc()) {
        *comp = complementCrdtRc;
        *pd = NULL;
    } else {
        /* unexpected. */
    }

    RedisModule_CloseKey(key);
    return NULL;
}

