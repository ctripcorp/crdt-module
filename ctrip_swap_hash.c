#include <stdio.h>
#include "ctrip_swap.h"
#include "util.h"
#include "lww/crdt_lww_hashmap.h"
#include "crdt_register.h"
#include "ctrip_crdt_register.h"
#include "include/rmutil/sds.h"
#include "ctrip_stream_io.h"

#define RKS_TYPE_CRDT_HASH 			"MH"
#define RKS_TYPE_CRDT_HASH_LEN 	    (sizeof(RKS_TYPE_CRDT_HASH)-1)

sds encodeKeyCrdtHash(RedisModuleString *keyobj) {
    sds rawkey = sdsempty();
    rawkey = sdscatlen(rawkey, RKS_TYPE_CRDT_HASH, RKS_TYPE_CRDT_HASH_LEN);
    rawkey = sdscatsds(rawkey, RedisModule_GetSds(keyobj));
    return rawkey;
}

sds encodeValCrdtHash(RedisModuleKey *key) {
    sio *io = sdsStreamCreate(sdsempty());
    sioSaveCrdtLWWHash(io, RedisModule_ModuleTypeGetValue(key));
    return sdsStreamRelease(io);
}

void *sioLoadCrdtHash(sio *io, int encver);
void *decodeValCrdtHash(sds rawval) {
    sio *io = sdsStreamCreate(rawval);
    void *reg = sioLoadCrdtHash(io, 0); /* encver not used */
    sdsStreamRelease(io);
    return reg;
}

