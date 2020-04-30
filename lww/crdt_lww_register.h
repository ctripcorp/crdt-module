#ifndef XREDIS_CRDT_CRDT_LWW_REGISTER_H
#define XREDIS_CRDT_CRDT_LWW_REGISTER_H
#include "../crdt_register.h"
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
#define NDEBUG
#include <assert.h>

typedef struct CRDT_LWW_Register {//26
    unsigned char type;
    unsigned char gid;
    long long timestamp;
    VectorClock vectorClock;
    sds value;
} __attribute__ ((packed, aligned(1))) CRDT_LWW_Register;

long long getCrdtLWWRegisterTimestamp(CRDT_LWW_Register* r);
long long getCrdtRegisterLastTimestamp(CRDT_Register* r) {
    return getCrdtLWWRegisterTimestamp(r);
}
int getCrdtLWWRegisterGid(CRDT_LWW_Register* r);
int getCrdtRegisterLastGid(CRDT_Register* r) {
    return getCrdtLWWRegisterGid(r);
}
sds getCrdtLWWRegisterValue(CRDT_LWW_Register* r);
sds getCrdtRegisterLastValue(CRDT_Register* r) {
    return getCrdtLWWRegisterValue(r);
}
VectorClock* getCrdtLWWRegisterVectorClock(CRDT_LWW_Register* r);
VectorClock* getCrdtRegisterLastVc(CRDT_Register* r) {
    return getCrdtLWWRegisterVectorClock(r);
}
CrdtMeta* createCrdtRegisterLastMeta(CRDT_Register* reg) {
    VectorClock* vc = getCrdtRegisterLastVc(reg);
    if(vc != NULL) {
        return createMeta(
            getCrdtRegisterLastGid(reg), 
            getCrdtRegisterLastTimestamp(reg),
            dupVectorClock(vc)  
        );
    }
    return NULL;
}



/**
 *  CRDT_LWW_Register Functions
*/
CRDT_LWW_Register* retrieveCrdtLWWRegister(void *data);
//register to string
sds crdtLWWRegisterInfo(CRDT_LWW_Register *crdtRegister);
CRDT_Register* mergeLWWRegister(CRDT_Register* target, CRDT_Register* other);


/**
 * CRDT_Register Functions implementation code
*/
sds crdtRegisterInfo(CRDT_Register *crdtRegister) {
    return crdtLWWRegisterInfo(crdtRegister);
}
CRDT_Register* mergeRegister(CRDT_Register* target, CRDT_Register* other) {
    return mergeLWWRegister(target, other);
}

typedef struct  CRDT_LWW_RegisterTombstone {//20
    unsigned char type;
    unsigned char gid;
    long long timestamp;
    VectorClock vectorClock;//8
} __attribute__ ((packed, aligned(1))) CRDT_LWW_RegisterTombstone;
/**
 *  CRDT_LWW_RegisterTombstone Functions
*/
CRDT_LWW_RegisterTombstone* retrieveCrdtLWWRegisterTombstone(void *data);



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

void RdbSaveLWWCrdtRegisterTombstone(RedisModuleIO *rdb, void *value);
CRDT_RegisterTombstone* filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) ;
CRDT_RegisterTombstone* filterRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) {
    return filterLWWRegisterTombstone(target, gid, logic_time);
}
CrdtMeta* addCrdtLWWRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta);
CrdtMeta* addRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta) {
    return addCrdtLWWRegisterTombstone(target, meta);
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
CRDT_LWW_Register* dupCrdtLWWRegister(CRDT_LWW_Register *val);
CRDT_Register* dupCrdtRegister(CRDT_Register *val) {
    return dupCrdtLWWRegister(val);
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
void *createCrdtLWWCrdtRegister(void);
void* createCrdtLWWCrdtRegisterTombstone(void);
void freeCrdtLWWCrdtRegisterTombstone(void *obj);
void freeCrdtLWWCrdtRegister(void *obj);
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
    return createCrdtLWWCrdtRegister();
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
    freeCrdtLWWCrdtRegister(obj);
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
    freeCrdtLWWCrdtRegisterTombstone(obj);
}


//gc
int crdtRegisterTombstoneGc(void* target, VectorClock* clock) {
    return crdtLWWRegisterTombstoneGc(target, clock);
}
int crdtRegisterGc(void* target, VectorClock* clock) {
    crdtLWWRegisterGc(target, clock);
}



int isExpireLWWTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta);
int isExpireCrdtTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta) {
    return isExpireLWWTombstone(tombstone, meta);
}

#endif //XREDIS_CRDT_CRDT_LWW_REGISTER_H