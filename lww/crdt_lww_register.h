#ifndef XREDIS_CRDT_CRDT_LWW_REGISTER_H
#define XREDIS_CRDT_CRDT_LWW_REGISTER_H
#include "../crdt_register.h"
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
#define NDEBUG
#include <assert.h>
typedef struct CRDT_LWW_Register {
    CRDT_Register parent;
    CrdtRegisterValue value;
} CRDT_LWW_Register;
typedef struct CRDT_LWW_RegisterTombstone {
    CRDT_RegisterTombstone parent;
    CrdtMeta* meta;
}CRDT_LWW_RegisterTombstone;
CRDT_LWW_RegisterTombstone* retrieveCrdtLWWRegisterTombstone(void *data);
CRDT_LWW_Register* retrieveCrdtLWWRegister(void *data);
sds crdtLWWRegisterInfo(CRDT_LWW_Register *crdtRegister);
sds crdtRegisterInfo(CRDT_Register *crdtRegister) {
    return crdtLWWRegisterInfo(crdtRegister);
}
CRDT_Register* mergeLWWRegister(CRDT_Register* target, CRDT_Register* other);
CRDT_Register* mergeRegister(CRDT_Register* target, CRDT_Register* other) {
    return mergeLWWRegister(target, other);
}
CRDT_Register* filterLWWRegister(CRDT_Register* target, int gid, long long logic_time);
CRDT_Register* filterRegister(CRDT_Register* target, int gid, long long logic_time) {
    return filterLWWRegister(target, gid, logic_time);
}
int delLWWCrdtRegister(CRDT_Register* current, CrdtMeta* meta);
int delCrdtRegister(CRDT_Register* current, CrdtMeta* meta) {
    return delLWWCrdtRegister(current, meta);
}
void RdbSaveLWWCrdtRegister(RedisModuleIO *rdb, void *value);
void *RdbLoadLWWCrdtRegister(RedisModuleIO *rdb, int encver);
void *RdbLoadLWWCrdtRegisterTombstone(RedisModuleIO *rdb, int encver);
CRDT_LWW_RegisterTombstone* dupLWWCrdtRegisterTombstone(CRDT_RegisterTombstone* target);
CRDT_RegisterTombstone* dupCrdtRegisterTombstone(CRDT_RegisterTombstone* target) {
    return dupLWWCrdtRegisterTombstone(target);
}
int setLWWCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value);
int setCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) {
    return setLWWCrdtRegister(r, meta, value);
}
CrdtRegisterValue* getLwwCrdtRegisterValue(CRDT_Register* r);
CrdtRegisterValue* getCrdtRegisterValue(CRDT_Register* r) {
    return getLwwCrdtRegisterValue(r);
}
void RdbSaveLWWCrdtRegisterTombstone(RedisModuleIO *rdb, void *value);
CRDT_RegisterTombstone* filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) ;
CRDT_RegisterTombstone* filterRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) {
    return filterLWWRegisterTombstone(target, gid, logic_time);
}
CrdtMeta* addLWWRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta);
CrdtMeta* addRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta) {
    return addLWWRegisterTombstone(target, meta);
}
CRDT_Register* mergeLWWRegister(CRDT_Register* target, CRDT_Register* other);
int purageLWWRegisterTombstone( CRDT_RegisterTombstone* tombstone, CRDT_Register* target);
int purageRegisterTombstone(CRDT_RegisterTombstone* tombstone, CRDT_Register* target) {
    return purageLWWRegisterTombstone(tombstone, target);
}
CRDT_Register* filterLWWRegister(CRDT_Register* target, int gid, long long logic_time) ;
sds getLWWCrdtRegister(CRDT_Register* r);
sds getCrdtRegisterSds(CRDT_Register* r) {
    return getLWWCrdtRegister(r);
}
CRDT_Register* dupLWWCrdtRegister(const CRDT_Register *val);
CRDT_Register* dupCrdtRegister(const CRDT_Register *val) {
    return dupLWWCrdtRegister(val);
}
void AofRewriteCrdtLWWRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);
void crdtLWWRegisterDigestFunc(RedisModuleDigest *md, void *value);
void crdtLWWRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value);
CRDT_RegisterTombstone* createCrdtLWWRegisterTombstone();
void AofRewriteCrdtLWWRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtLWWRegisterMemUsageFunc(const void *value);
size_t crdtLWWRegisterTombstoneMemUsageFunc(void *value);
CRDT_RegisterTombstone* mergeLWWRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other);
CRDT_RegisterTombstone* mergeRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other) {
    return mergeLWWRegisterTombstone(target, other);
}
void *createLLWCrdtRegister(void);
void* createLLWCrdtRegisterTombstone(void);
void freeLLWCrdtRegisterTombstone(void *obj);
void freeLLWCrdtRegister(void *obj);
//gc
int crdtLWWRegisterTombstoneGc(void* target, VectorClock* clock);
int crdtLWWRegisterGc(void* target, VectorClock* clock);

void updateLastVCLWWRegister(CRDT_Register* r, VectorClock* vc);
void crdtRegisterUpdateLastVC(void *data, VectorClock* vc) {
    CRDT_Register* reg = (CRDT_Register*) data;
    updateLastVCLWWRegister(reg, vc);
}
//create
void *createCrdtRegister(void) {
    return createLLWCrdtRegister();
}
CRDT_RegisterTombstone* createCrdtRegisterTombstone() {
    return createCrdtLWWRegisterTombstone();
}

//CrdtRegister
void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver) {
    int type = RedisModule_LoadSigned(rdb);
    if( type == LWW_TYPE) {
        return RdbLoadLWWCrdtRegister(rdb, encver);
    }
    return NULL;
}
void RdbSaveCrdtRegister(RedisModuleIO *rdb, void *value) {
    RdbSaveLWWCrdtRegister(rdb, value);
} 
void AofRewriteCrdtRegister(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWRegister(aof, key, value);
}
size_t crdtRegisterMemUsageFunc(const void *value) {
    return crdtLWWRegisterMemUsageFunc(value);
}
void freeCrdtRegister(void *obj) {
    freeLLWCrdtRegister(obj);
}
void crdtRegisterDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWRegisterDigestFunc(md, value);
}

//CrdtRegisterTombstone
void *RdbLoadCrdtRegisterTombstone(RedisModuleIO *rdb, int encver) {
    int type = RedisModule_LoadSigned(rdb);
    if( type == LWW_TYPE) {
        return RdbLoadLWWCrdtRegisterTombstone(rdb, encver);
    }
    return NULL;
}

void RdbSaveCrdtRegisterTombstone(RedisModuleIO *rdb, void *value) {
    RdbSaveLWWCrdtRegisterTombstone(rdb, value);
}

void AofRewriteCrdtRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    AofRewriteCrdtLWWRegisterTombstone(aof, key, value);
}

size_t crdtRegisterTombstoneMemUsageFunc(const void *value) {
    return crdtLWWRegisterTombstoneMemUsageFunc(value);
}

void crdtRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    crdtLWWRegisterTombstoneDigestFunc(md, value);
}

void freeCrdtRegisterTombstone(void *obj) {
    freeLLWCrdtRegisterTombstone(obj);
}


//gc
int crdtRegisterTombstoneGc(void* target, VectorClock* clock) {
    return crdtLWWRegisterTombstoneGc(target, clock);
}
int crdtRegisterGc(void* target, VectorClock* clock) {
    crdtLWWRegisterGc(target, clock);
}



int isExpireLWWTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta);
int isExpireTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta) {
    return isExpireLWWTombstone(tombstone, meta);
}

#endif //XREDIS_CRDT_CRDT_LWW_REGISTER_H