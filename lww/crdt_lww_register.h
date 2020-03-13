#ifndef XREDIS_CRDT_CRDT_LWW_REGISTER_H
#define XREDIS_CRDT_CRDT_LWW_REGISTER_H
#include "../crdt_register.h"
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
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
int delLWWCrdtRegister(CRDT_Register* current, CrdtMeta* meta);
void RdbSaveLWWCrdtRegister(RedisModuleIO *rdb, void *value);
void *RdbLoadLWWCrdtRegister(RedisModuleIO *rdb, int encver);
void *RdbLoadLWWCrdtRegisterTombstone(RedisModuleIO *rdb, int encver);
CRDT_LWW_RegisterTombstone* dupLWWCrdtRegisterTombstone(CRDT_RegisterTombstone* target);
int setLWWCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value);
CrdtRegisterValue* getLwwCrdtRegisterValue(CRDT_Register* r);
void RdbSaveLWWCrdtRegisterTombstone(RedisModuleIO *rdb, void *value);
CRDT_RegisterTombstone* filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, long long gid, long long logic_time) ;
CrdtMeta* addLWWRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta);
CRDT_Register* mergeLWWRegister(CRDT_Register* target, CRDT_Register* other);
int cleanLWWRegister(CRDT_Register* target, CRDT_RegisterTombstone* tombstone);
CRDT_Register* filterLWWRegister(CRDT_Register* target, long long gid, long long logic_time) ;
sds getLWWCrdtRegister(CRDT_Register* r);
CRDT_Register* dupLWWCrdtRegister(const CRDT_Register *val);
void AofRewriteCrdtLWWRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);
void crdtLWWRegisterDigestFunc(RedisModuleDigest *md, void *value);
void crdtLWWRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value);
CRDT_RegisterTombstone* createCrdtLWWRegisterTombstone();
void AofRewriteCrdtLWWRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtLWWRegisterMemUsageFunc(const void *value);
size_t crdtLWWRegisterTombstoneMemUsageFunc(void *value);
CRDT_RegisterTombstone* mergeLWWRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other);

void *createLLWCrdtRegister(void);
void* createLLWCrdtRegisterTombstone(void);
void freeLLWCrdtRegisterTombstone(void *obj);
void freeLLWCrdtRegister(void *obj);
//gc
int crdtLWWRegisterTombstoneGc(void* target, VectorClock* clock);
int crdtLWWRegisterGc(void* target, VectorClock* clock);

//create
void *createCrdtRegister(void) {
    return createLLWCrdtRegister();
}
CRDT_RegisterTombstone* createCrdtRegisterTombstone() {
    return createCrdtLWWRegisterTombstone();
}

//CrdtRegister
void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver) {
    return RdbLoadLWWCrdtRegister(rdb, encver);
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
    return RdbLoadLWWCrdtRegisterTombstone(rdb, encver);
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





#endif //XREDIS_CRDT_CRDT_LWW_REGISTER_H