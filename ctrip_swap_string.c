#include <stdio.h>
#include "ctrip_swap.h"
#include "util.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
#include "include/rmutil/sds.h"
#include "ctrip_stream_io.h"

#define RKS_TYPE_CRDT_STRING 			"MK"
#define RKS_TYPE_CRDT_STRING_LEN 		(sizeof(RKS_TYPE_CRDT_STRING)-1)

void *sioLoadCrdtRc(sio *io, int encver);

sds encodeKeyCrdtString(RedisModuleString *keyobj) {
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

void *decodeValCrdtRegister(sds rawval) {
    sio *io = sdsStreamCreate(rawval);
    CRDT_Register *reg = sioLoadCrdtRegister(io, 0); /* encver not used */
    sdsStreamRelease(io);
    return reg;
}

sds encodeValCrdtRc(RedisModuleKey *key) {
    sio *io = sdsStreamCreate(sdsempty());
    sioSaveCrdtRc(io, RedisModule_ModuleTypeGetValue(key));
    return sdsStreamRelease(io);
}

void *decodeValCrdtRC(sds rawval) {
    sio *io = sdsStreamCreate(rawval);
    CRDT_RC *rc = sioLoadCrdtRc(io, 0); /* encver not used */
    sdsStreamRelease(io);
    return rc;
}

