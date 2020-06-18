#include "crdt_lww_register.h"

/**
 *  LWW Register Get Set Function
 */ 
int getCrdtLWWRegisterGid(CRDT_LWW_Register* r) {   
    return (int)r->gid;
} 
void setCrdtLWWRegisterGid(CRDT_LWW_Register* r, int gid) {
    r->gid = gid;
}
long long getCrdtLWWRegisterTimestamp(CRDT_LWW_Register* r) {
    return r->timestamp;
}
void setCrdtLWWRegisterTimestamp(CRDT_LWW_Register* r, long long timestamp) {
    r->timestamp = timestamp;
}
VectorClock getCrdtLWWRegisterVectorClock(CRDT_LWW_Register* r) {
    return r->vectorClock;
}

void setCrdtLWWRegisterVectorClock(CRDT_LWW_Register* r, VectorClock vc) {
    if(!isNullVectorClock(getCrdtLWWRegisterVectorClock(r))) {
        freeVectorClock(r->vectorClock);
    }
    r->vectorClock = vc;
}
CrdtMeta* getCrdtLWWRegisterMeta(CRDT_LWW_Register* r) {
    if(isNullVectorClock(r->vectorClock)) return NULL;
    return (CrdtMeta*)r;
}
int appendCrdtLWWRegisterMeta(CRDT_LWW_Register* r, CrdtMeta* other, int compare) {
    if(other == NULL) return COMPARE_META_VECTORCLOCK_LT;
    CrdtMeta* target = getCrdtLWWRegisterMeta(r);
    if(target == NULL) {
        setCrdtLWWRegisterGid(r, getMetaGid(other));
        setCrdtLWWRegisterTimestamp(r, getMetaTimestamp(other));
        setCrdtLWWRegisterVectorClock(r, dupVectorClock(getMetaVectorClock(other)));
        return COMPARE_META_VECTORCLOCK_GT;
    } else {
        if(compare < COMPARE_META_GID_LT) compare = compareCrdtMeta(target, other);
        VectorClock vc = vectorClockMerge(getMetaVectorClock(target), getMetaVectorClock(other));
        setCrdtLWWRegisterVectorClock(r, vc);
        if(compare > COMPARE_META_EQUAL) {
            setCrdtLWWRegisterGid(r, getMetaGid(other));
            setCrdtLWWRegisterTimestamp(r, getMetaTimestamp(other));
        }
        return compare;
    }
    
}
void setCrdtLWWRegisterMeta(CRDT_LWW_Register* r, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWRegisterGid(r, -1);
        setCrdtLWWRegisterTimestamp(r, -1);
        setCrdtLWWRegisterVectorClock(r, newVectorClock(0));
    }else{
        setCrdtLWWRegisterGid(r, getMetaGid(meta));
        setCrdtLWWRegisterTimestamp(r, getMetaTimestamp(meta));
        setCrdtLWWRegisterVectorClock(r, dupVectorClock(getMetaVectorClock(meta)));
    }
    // freeCrdtMeta(meta);
}
sds getCrdtLWWRegisterValue(CRDT_LWW_Register* r) {
    return r->value;
}
void setCrdtLWWRegisterValue(CRDT_LWW_Register* r, sds value) {
    if(r->value != NULL) {
        sdsfree(r->value);
    }
    r->value = value;
}
/**
 *  LWW RegisterTombstone Get Set Function
 */ 
int getCrdtLWWRegisterTombstoneGid(CRDT_LWW_RegisterTombstone* t) {
    return t->gid;
}
 void setCrdtLWWRegisterTombstoneGid(CRDT_LWW_RegisterTombstone* t, int gid) {
    t->gid = gid;
}
 CrdtMeta* getCrdtLWWRegisterTombstoneMeta(CRDT_LWW_RegisterTombstone* t) {
    return (CrdtMeta*)t;
}

VectorClock getCrdtLWWRegisterTombstoneVectorClock(CRDT_LWW_RegisterTombstone* t) {
    return t->vectorClock;
}
 void setCrdtLWWRegisterTombstoneVectorClock(CRDT_LWW_RegisterTombstone* t, VectorClock vc) {
    if(!isNullVectorClock(getCrdtLWWRegisterTombstoneVectorClock(t))) {
        freeVectorClock(t->vectorClock);
    }
    t->vectorClock = vc;
}
 long long getCrdtLWWRegisterTombstoneTimestamp(CRDT_LWW_RegisterTombstone* t) {
    return t->timestamp;
}
void setCrdtLWWRegisterTombstoneTimestamp(CRDT_LWW_RegisterTombstone* t, long long timestamp) {
    t->timestamp = timestamp;
}
CrdtMeta* createCrdtRegisterTombstoneLastMeta(CRDT_RegisterTombstone* t) {
    VectorClock vc = getCrdtLWWRegisterTombstoneVectorClock((CRDT_LWW_RegisterTombstone*)t);
    if(!isNullVectorClock(vc)) {
        return createMeta(
            getCrdtLWWRegisterTombstoneGid((CRDT_LWW_RegisterTombstone*)t), 
            getCrdtLWWRegisterTombstoneTimestamp((CRDT_LWW_RegisterTombstone*)t),
            dupVectorClock(vc)  
        );
    }
    return NULL;
}
void setCrdtLWWRegisterTombstoneMeta(CRDT_LWW_RegisterTombstone* t, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWRegisterTombstoneGid(t, -1);
        setCrdtLWWRegisterTombstoneTimestamp(t, -1);
        setCrdtLWWRegisterTombstoneVectorClock(t, newVectorClock(0));
    }else{
        setCrdtLWWRegisterTombstoneGid(t, getMetaGid(meta));
        setCrdtLWWRegisterTombstoneTimestamp(t, getMetaTimestamp(meta));
        setCrdtLWWRegisterTombstoneVectorClock(t, dupVectorClock(getMetaVectorClock(meta)));
    }
    freeCrdtMeta(meta);
}
void initLWWReigster(CRDT_LWW_Register *crdtRegister) {
    crdtRegister->type = 0;
    setType((CrdtObject*)crdtRegister, CRDT_DATA);
    setDataType((CrdtObject*)crdtRegister, CRDT_REGISTER_TYPE);
    crdtRegister->vectorClock = newVectorClock(0);
    setCrdtLWWRegisterTimestamp(crdtRegister, 0);
    setCrdtLWWRegisterGid(crdtRegister, 0);
    crdtRegister->value = NULL;
}
void* createCrdtRegister(void) {
    CRDT_LWW_Register *crdtRegister = RedisModule_Alloc(sizeof(CRDT_LWW_Register));
    initLWWReigster(crdtRegister);
    return crdtRegister;
}
CRDT_LWW_Register* dupCrdtLWWRegister(CRDT_LWW_Register *target) {
    CRDT_LWW_Register* result = createCrdtRegister();
    setCrdtLWWRegisterMeta(result, getCrdtLWWRegisterMeta(target));
    setCrdtLWWRegisterValue(result, sdsdup(getCrdtLWWRegisterValue(target)));
    return result;
}
CRDT_LWW_Register* mergeLWWRegister(CRDT_LWW_Register* target, CRDT_LWW_Register* other, int* compare) {
    if(target == NULL) {return dupCrdtLWWRegister(other);}
    if(other == NULL) {return dupCrdtLWWRegister(target);}
    CRDT_LWW_Register* result = dupCrdtLWWRegister(target);
    *compare = appendCrdtLWWRegisterMeta(result, getCrdtLWWRegisterMeta(other), COMPARE_META_GID_LT - 1);
    // setCrdtLWWRegisterMeta(result, mergeMeta(getCrdtLWWRegisterMeta(target), getCrdtLWWRegisterMeta(other), compare));
    if(*compare > COMPARE_META_EQUAL) {
        setCrdtLWWRegisterValue(result, sdsdup(getCrdtLWWRegisterValue(other)));
    }
    return result;
}
void updateLastVCLWWRegister(CRDT_Register* r, VectorClock vc) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    setCrdtLWWRegisterVectorClock(data, vectorClockMerge(getCrdtLWWRegisterVectorClock(data), vc));
}
int purageLWWRegisterTombstone(CRDT_RegisterTombstone* tombstone, CRDT_Register* target) {
    CRDT_LWW_Register* current = retrieveCrdtLWWRegister(target);
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(tombstone);
    if(compareCrdtMeta(getCrdtLWWRegisterMeta(current), getCrdtLWWRegisterTombstoneMeta(t)) > 0) {
        return 1;
    }
    return 0;
}
int isExpireLWWTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(tombstone);
    return compareCrdtMeta(meta, getCrdtLWWRegisterTombstoneMeta(t)) > COMPARE_META_EQUAL? CRDT_OK: CRDT_NO;
}




void freeCrdtLWWCrdtRegister(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(obj);
    setCrdtLWWRegisterValue(crdtRegister, NULL);
    setCrdtLWWRegisterVectorClock(crdtRegister, newVectorClock(0));
    RedisModule_Free(crdtRegister);
}

CRDT_LWW_Register* retrieveCrdtLWWRegister(void *data) {
    CRDT_LWW_Register* result = (CRDT_LWW_Register*)data;
    assert(getDataType(result) == CRDT_REGISTER_TYPE);
    return result;
}
CRDT_LWW_RegisterTombstone* retrieveCrdtLWWRegisterTombstone(void *data) {
    CRDT_LWW_RegisterTombstone* result = (CRDT_LWW_RegisterTombstone*)data;
    assert(result->parent.parent.dataType == CRDT_REGISTER_TYPE);
    return result;
}
int delLWWCrdtRegister(CRDT_Register* current, CrdtMeta* meta) {
    CRDT_LWW_Register* target = retrieveCrdtLWWRegister(current);
    int result = compareCrdtMeta(getCrdtLWWRegisterMeta(target), meta);
    if(result > COMPARE_META_EQUAL) {
        
        return 1;
    }
    return 0;
    
}
CRDT_LWW_RegisterTombstone* createCrdtLWWRegisterTombstone() {
    CRDT_LWW_RegisterTombstone *tombstone = RedisModule_Alloc(sizeof(CRDT_LWW_RegisterTombstone));
    tombstone->type = 0;
    tombstone->timestamp = 0;
    tombstone->gid = 0;
    setType((CrdtObject*)tombstone, CRDT_TOMBSTONE);
    setDataType((CrdtObject*)tombstone, CRDT_REGISTER_TYPE);
    tombstone->vectorClock = newVectorClock(0);
    return tombstone;
}

void *RdbLoadLWWCrdtRegister(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_Register *crdtRegister = createCrdtRegister();
    int gid = RedisModule_LoadSigned(rdb);
    if(RedisModule_CheckGid(gid) == REDISMODULE_ERR) {
        return NULL;
    } 
    setCrdtLWWRegisterGid(crdtRegister, gid);
    setCrdtLWWRegisterTimestamp(crdtRegister, RedisModule_LoadSigned(rdb));
    setCrdtLWWRegisterVectorClock(crdtRegister, rdbLoadVectorClock(rdb));

    size_t sdsLength;
    char* str = RedisModule_LoadStringBuffer(rdb, &sdsLength);
    sds val = sdsnewlen(str, sdsLength);
    setCrdtLWWRegisterValue(crdtRegister, val);
    RedisModule_Free(str);

    return crdtRegister;
}


void RdbSaveLWWCrdtRegister(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    RedisModule_SaveSigned(rdb, getCrdtLWWRegisterGid(crdtRegister));
    RedisModule_SaveSigned(rdb, getCrdtLWWRegisterTimestamp(crdtRegister));
    rdbSaveVectorClock(rdb, getCrdtLWWRegisterVectorClock(crdtRegister));
    RedisModule_SaveStringBuffer(rdb, getCrdtLWWRegisterValue(crdtRegister), sdslen(getCrdtLWWRegisterValue(crdtRegister)));
}

sds getLWWCrdtRegister(CRDT_Register* r) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    return getCrdtLWWRegisterValue(data);
}
void setCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) {
// void setLWWCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    // int compare = 0;
    // setCrdtLWWRegisterMeta(data, mergeMeta(getCrdtLWWRegisterMeta(data), meta, &compare));
    setCrdtLWWRegisterMeta(data, meta);
    setCrdtLWWRegisterValue(data, sdsdup(value));
}
// int appendLWWCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) {
int appendCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value, int compare) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    compare = appendCrdtLWWRegisterMeta(data, meta, compare);
    if(compare > COMPARE_META_EQUAL) {
        setCrdtLWWRegisterValue(data, sdsdup(value));
    }
    return compare;
}
CRDT_LWW_Register* filterLWWRegister(CRDT_LWW_Register* target, int gid, long long logic_time) {
    CRDT_LWW_Register* reg = retrieveCrdtLWWRegister(target);
    if(getCrdtLWWRegisterGid(reg) != gid) {
        return NULL;
    }
    VectorClockUnit unit = getVectorClockUnit(getCrdtLWWRegisterVectorClock(reg), gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    
    long long vcu = get_logic_clock(unit);
    if(vcu > logic_time) {
        return dupCrdtLWWRegister(reg);
    }  
    return NULL;
}
sds crdtLWWRegisterInfo(CRDT_LWW_Register *crdtRegister) {
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(getCrdtLWWRegisterVectorClock(crdtRegister));
    result = sdscatprintf(result, "gid: %d, timestamp: %lld, vector-clock: %s, val: %s",
            getCrdtLWWRegisterGid(crdtRegister), getCrdtLWWRegisterTimestamp(crdtRegister), vcStr,getCrdtLWWRegisterValue(crdtRegister));
    sdsfree(vcStr);
    return result;
}
size_t crdtLWWRegisterTombstoneMemUsageFunc(const void *value) {
    //to do 
    return 1;
}
void crdtLWWRegisterDigestFunc(RedisModuleDigest *md, void *value) {
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    RedisModule_DigestAddLongLong(md, getCrdtLWWRegisterGid(crdtRegister));
    RedisModule_DigestAddLongLong(md, getCrdtLWWRegisterTimestamp(crdtRegister));
    sds vclockStr = vectorClockToSds(getCrdtLWWRegisterVectorClock(crdtRegister));
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) (getCrdtLWWRegisterValue(crdtRegister)), sdslen(getCrdtLWWRegisterValue(crdtRegister)));
    RedisModule_DigestEndSequence(md);
}
size_t crdtLWWRegisterMemUsageFunc(const void *value) {
    //to do
    return 1;
}

void AofRewriteCrdtLWWRegister(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    sds vclockSds = vectorClockToSds(getCrdtLWWRegisterVectorClock(crdtRegister));
    RedisModule_EmitAOF(aof, "CRDT.SET", "scllc", key, getCrdtLWWRegisterValue(crdtRegister), getCrdtLWWRegisterGid(crdtRegister), getCrdtLWWRegisterTimestamp(crdtRegister), vclockSds);
    sdsfree(vclockSds);
}


/**
 * tombstone
 */ 
CrdtMeta* addCrdtLWWRegisterTombstone(CRDT_LWW_RegisterTombstone* target, CrdtMeta* meta, int* compare) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    setCrdtLWWRegisterTombstoneMeta(t, mergeMeta(getCrdtLWWRegisterTombstoneMeta(t), meta, compare));
    return meta;
}
CRDT_LWW_RegisterTombstone* mergeLWWRegisterTombstone(CRDT_LWW_RegisterTombstone* target, CRDT_LWW_RegisterTombstone* other, int* comapre) {
    CRDT_LWW_RegisterTombstone* result = NULL;
    if(target == NULL && other == NULL) return NULL;
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    CRDT_LWW_RegisterTombstone* o = retrieveCrdtLWWRegisterTombstone(other);
    if(t == NULL) {return dupLWWCrdtRegisterTombstone(o);}
    if(o == NULL) {return dupLWWCrdtRegisterTombstone(t);}
    result = dupLWWCrdtRegisterTombstone(t);
    addCrdtLWWRegisterTombstone(result, getCrdtLWWRegisterTombstoneMeta(o), comapre);
    return result;
}
CRDT_RegisterTombstone* filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    if(getCrdtLWWRegisterTombstoneGid(t) != gid) return NULL;
    VectorClockUnit unit = getVectorClockUnit(getCrdtLWWRegisterTombstoneVectorClock(t), gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    long long vcu = get_logic_clock(unit);
    if(vcu < logic_time) return NULL;
    return (CRDT_RegisterTombstone*)dupLWWCrdtRegisterTombstone(t);
}
CRDT_LWW_RegisterTombstone* dupLWWCrdtRegisterTombstone(CRDT_LWW_RegisterTombstone* target) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    CRDT_LWW_RegisterTombstone* result = createCrdtLWWRegisterTombstone();
    setCrdtLWWRegisterTombstoneGid(result, getCrdtLWWRegisterTombstoneGid(t));
    setCrdtLWWRegisterTombstoneTimestamp(result, getCrdtLWWRegisterTombstoneTimestamp(t));
    setCrdtLWWRegisterTombstoneVectorClock(result, dupVectorClock(getCrdtLWWRegisterTombstoneVectorClock(t)));
    return result;
}

void freeCrdtLWWCrdtRegisterTombstone(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(obj);
    setCrdtLWWRegisterTombstoneVectorClock(tombstone, newVectorClock(0));
    RedisModule_Free(tombstone);
}
void AofRewriteCrdtLWWRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(value);
    sds vclockSds = vectorClockToSds(getCrdtLWWRegisterTombstoneVectorClock(tombstone));
    //crdt.del key gid time vc
    RedisModule_EmitAOF(aof, "CRDT.DEL", "scllc", key, getCrdtLWWRegisterTombstoneGid(tombstone), getCrdtLWWRegisterTombstoneTimestamp(tombstone), vclockSds);
    sdsfree(vclockSds);
}

void crdtLWWRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(value);
    RedisModule_DigestAddLongLong(md, getCrdtLWWRegisterTombstoneGid(tombstone));
    RedisModule_DigestAddLongLong(md, getCrdtLWWRegisterTombstoneTimestamp(tombstone));
    sds vclockStr = vectorClockToSds(getCrdtLWWRegisterTombstoneVectorClock(tombstone));
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    RedisModule_DigestEndSequence(md);
}
void RdbSaveLWWCrdtRegisterTombstone(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(value);
    RedisModule_SaveSigned(rdb, getCrdtLWWRegisterTombstoneGid(tombstone));
    RedisModule_SaveSigned(rdb, getCrdtLWWRegisterTombstoneTimestamp(tombstone));
    rdbSaveVectorClock(rdb, getCrdtLWWRegisterTombstoneVectorClock(tombstone));
}
void *RdbLoadLWWCrdtRegisterTombstone(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_RegisterTombstone *tombstone = createCrdtLWWRegisterTombstone();
    setCrdtLWWRegisterTombstoneGid(tombstone, RedisModule_LoadSigned(rdb));
    setCrdtLWWRegisterTombstoneTimestamp(tombstone, RedisModule_LoadSigned(rdb));
    setCrdtLWWRegisterTombstoneVectorClock(tombstone, rdbLoadVectorClock(rdb));
    return tombstone;
}

int crdtLWWRegisterTombstoneGc(void* target, VectorClock clock) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    return isVectorClockMonoIncr(getCrdtLWWRegisterTombstoneVectorClock(t), clock);
}
