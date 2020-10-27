#ifndef XREDIS_CRDT_CRDT_LWW_REGISTER_H
#define XREDIS_CRDT_CRDT_LWW_REGISTER_H
#include "../crdt_register.h"
#include "../include/rmutil/sds.h"
#include "../crdt_util.h"
#define NDEBUG
#include <assert.h>

typedef struct CRDT_LWW_Register {
    unsigned long long type:8;
    unsigned long long gid:4;
    unsigned long long timestamp:52;
    VectorClock vectorClock;
    sds value;
} __attribute__ ((packed, aligned(1))) CRDT_LWW_Register;

long long getCrdtLWWRegisterTimestamp(CRDT_LWW_Register* r);
long long getCrdtRegisterLastTimestamp(CRDT_Register* r) {
    return getCrdtLWWRegisterTimestamp((CRDT_LWW_Register*)r);
}
int getCrdtLWWRegisterGid(CRDT_LWW_Register* r);
int getCrdtRegisterLastGid(CRDT_Register* r) {
    return getCrdtLWWRegisterGid((CRDT_LWW_Register*)r);
}
sds getCrdtLWWRegisterValue(CRDT_LWW_Register* r);
sds getCrdtRegisterLastValue(CRDT_Register* r) {
    return getCrdtLWWRegisterValue((CRDT_LWW_Register*)r);
}
VectorClock getCrdtLWWRegisterVectorClock(CRDT_LWW_Register* r);
VectorClock getCrdtRegisterLastVc(void* r) {
    return getCrdtLWWRegisterVectorClock((CRDT_LWW_Register*)r);
}
CrdtMeta* createCrdtRegisterLastMeta(CRDT_Register* reg) {
    VectorClock vc = getCrdtRegisterLastVc(reg);
    if(!isNullVectorClock(vc)) {
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
CRDT_LWW_Register* mergeLWWRegister(CRDT_LWW_Register* target, CRDT_LWW_Register* other, int* compare);
CRDT_Register* mergeRegister(CRDT_Register* target, CRDT_Register* other, int* compare) {
    return (CRDT_Register*)mergeLWWRegister((CRDT_LWW_Register*)target, (CRDT_LWW_Register*)other, compare);
}

/**
 * CRDT_Register Functions implementation code
*/
sds crdtRegisterInfo(void *crdtRegister) {
    return crdtLWWRegisterInfo((CRDT_LWW_Register*)crdtRegister);
}




typedef struct  CRDT_LWW_RegisterTombstone {//20
    unsigned long long type:8;
    unsigned long long gid:4;
    unsigned long long timestamp:52;
    VectorClock vectorClock;//8
} __attribute__ ((packed, aligned(1))) CRDT_LWW_RegisterTombstone;
/**
 *  CRDT_LWW_RegisterTombstone Functions
*/
CRDT_LWW_RegisterTombstone* retrieveCrdtLWWRegisterTombstone(void *data);



CRDT_LWW_Register** filterLWWRegister(CRDT_LWW_Register* target, int gid, long long logic_time, long long maxsize, int* length);
CRDT_Register** filterRegister(CRDT_Register* target, int gid, long long logic_time, long long maxsize, int* length) {
    return (CRDT_Register**)filterLWWRegister((CRDT_LWW_Register*)target, gid, logic_time, maxsize, length);
}
int delLWWCrdtRegister(CRDT_Register* current, CrdtMeta* meta);
int compareLWWCrdtRegisterAndDelMeta(CRDT_Register* current, CrdtMeta* meta);
int compareCrdtRegisterAndDelMeta(CRDT_Register* current, CrdtMeta* meta) {
    return compareLWWCrdtRegisterAndDelMeta(current, meta);
}
void RdbSaveLWWCrdtRegister(RedisModuleIO *rdb, void *value);
void *RdbLoadLWWCrdtRegister(RedisModuleIO *rdb, int version, int encver);
void *RdbLoadLWWCrdtRegisterTombstone(RedisModuleIO *rdb, int version, int encver);
CRDT_LWW_RegisterTombstone* dupLWWCrdtRegisterTombstone(CRDT_LWW_RegisterTombstone *target);
CRDT_RegisterTombstone* dupCrdtRegisterTombstone(CRDT_RegisterTombstone *target) {
    return (CRDT_RegisterTombstone*)dupLWWCrdtRegisterTombstone((CRDT_LWW_RegisterTombstone*)target);
}
void initLWWReigster(CRDT_LWW_Register *r);
void initRegister(CRDT_Register *r) {
    return initLWWReigster((CRDT_LWW_Register*)r);
}

void RdbSaveLWWCrdtRegisterTombstone(RedisModuleIO *rdb, void *value);
CRDT_RegisterTombstone** filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time, long long maxsize, int* length);
CRDT_RegisterTombstone** filterRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time, long long maxsize, int* length) {
    return filterLWWRegisterTombstone(target, gid, logic_time, maxsize, length);
}
CrdtMeta* addCrdtLWWRegisterTombstone(CRDT_LWW_RegisterTombstone* target, CrdtMeta* meta, int* compare);
CrdtMeta* addRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta, int* compare) {
    return addCrdtLWWRegisterTombstone((CRDT_LWW_RegisterTombstone*)target, meta, compare);
}
int purgeLWWRegisterTombstone( CRDT_RegisterTombstone* tombstone, CRDT_Register* target);
int purgeRegisterTombstone(CRDT_RegisterTombstone* tombstone, CRDT_Register* target) {
    return purgeLWWRegisterTombstone(tombstone, target);
}
sds getLWWCrdtRegister(CRDT_Register* r);
sds getCrdtRegisterSds(CRDT_Register* r) {
    return getLWWCrdtRegister(r);
}
CrdtMeta* getCrdtLWWRegisterMeta(CRDT_LWW_Register* r);
CrdtMeta* getCrdtRegisterLastMeta(CRDT_Register* r) {
    return getCrdtLWWRegisterMeta((CRDT_LWW_Register*)r);
}
CRDT_LWW_Register* dupCrdtLWWRegister(CRDT_LWW_Register *val);
CRDT_Register* dupCrdtRegister(CRDT_Register *val) {
    return (CRDT_Register*)dupCrdtLWWRegister((CRDT_LWW_Register*)val);
}
void AofRewriteCrdtLWWRegister(RedisModuleIO *aof, RedisModuleString *key, void *value);
void crdtLWWRegisterDigestFunc(RedisModuleDigest *md, void *value);
void crdtLWWRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value);
CRDT_LWW_RegisterTombstone* createCrdtLWWRegisterTombstone();
void AofRewriteCrdtLWWRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtLWWRegisterMemUsageFunc(const void *value);
size_t crdtLWWRegisterTombstoneMemUsageFunc(const void *value);
CRDT_LWW_RegisterTombstone* mergeLWWRegisterTombstone(CRDT_LWW_RegisterTombstone* target, CRDT_LWW_RegisterTombstone* other, int* compare);
CRDT_RegisterTombstone* mergeRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other, int* compare) {
    return (CRDT_RegisterTombstone*)mergeLWWRegisterTombstone((CRDT_LWW_RegisterTombstone*)target, (CRDT_LWW_RegisterTombstone*)other, compare);
}
void* createCrdtLWWCrdtRegisterTombstone(void);
void freeCrdtLWWCrdtRegisterTombstone(void *obj);
void freeCrdtLWWCrdtRegister(void *obj);
//gc
int crdtLWWRegisterTombstoneGc(void* target, VectorClock clock);

void updateLastVCLWWRegister(CRDT_Register* r, VectorClock vc);
void crdtRegisterUpdateLastVC(void *data, VectorClock vc) {
    CRDT_Register* reg = (CRDT_Register*) data;
    updateLastVCLWWRegister(reg, vc);
}

CRDT_RegisterTombstone* createCrdtRegisterTombstone() {
    return (CRDT_RegisterTombstone*)createCrdtLWWRegisterTombstone();
}

//CrdtRegister
void *RdbLoadCrdtRegister(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == LWW_TYPE) {
        return RdbLoadLWWCrdtRegister(rdb, version, encver);
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
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == LWW_TYPE) {
        return RdbLoadLWWCrdtRegisterTombstone(rdb, version, encver);
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
int crdtRegisterTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    return crdtLWWRegisterTombstoneGc(target, clock);
}




int compareCrdtLWWRegisterTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta);
int compareCrdtRegisterTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta) {
    return compareCrdtLWWRegisterTombstone(tombstone, meta);
}
sds crdtRegisterInfoFromMetaAndValue(CrdtMeta* meta, sds value) {
    CRDT_LWW_Register c;
    initLWWReigster(&c);
    c.gid = getMetaGid(meta);
    c.timestamp = getMetaTimestamp(meta);
    c.vectorClock = getMetaVectorClock(meta);
    c.value = value;
    return crdtLWWRegisterInfo(&c);
}

#endif //XREDIS_CRDT_CRDT_LWW_REGISTER_H