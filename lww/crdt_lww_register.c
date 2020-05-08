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
    r->timestamp= timestamp;
}
VectorClock* getCrdtLWWRegisterVectorClock(CRDT_LWW_Register* r) {
    if(isNullVectorClock(r->vectorClock)) return NULL;
    return &r->vectorClock;
}

void setCrdtLWWRegisterVectorClock(CRDT_LWW_Register* r, VectorClock* vc) {
    if(getCrdtLWWRegisterVectorClock(r) != NULL) {
        freeInnerClocks(&r->vectorClock);
    }
    if(vc == NULL) {
        r->vectorClock.length = -1;
    } else {
        cloneVectorClock(&r->vectorClock, vc);
        freeVectorClock(vc);
    }
    
}
CrdtMeta* getCrdtLWWRegisterMeta(CRDT_LWW_Register* r) {
    return &r->gid;
}
void setCrdtLWWRegisterMeta(CRDT_LWW_Register* r, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWRegisterGid(r, -1);
        setCrdtLWWRegisterTimestamp(r, -1);
        setCrdtLWWRegisterVectorClock(r, NULL);
    }else{
        setCrdtLWWRegisterGid(r, getMetaGid(meta));
        setCrdtLWWRegisterTimestamp(r, getMetaTimestamp(meta));
        setCrdtLWWRegisterVectorClock(r, dupVectorClock(getMetaVectorClock(meta)));
    }
    freeCrdtMeta(meta);
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
    return (int)t->gid;
}
 int setCrdtLWWRegisterTombstoneGid(CRDT_LWW_RegisterTombstone* t, int gid) {
    t->gid = gid;
}
 CrdtMeta* getCrdtLWWRegisterTombstoneMeta(CRDT_LWW_RegisterTombstone* t) {
    return &t->gid;
}

VectorClock* getCrdtLWWRegisterTombstoneVectorClock(CRDT_LWW_RegisterTombstone* t) {
    if(isNullVectorClock(t->vectorClock)) return NULL;
    return &t->vectorClock;
}
 void setCrdtLWWRegisterTombstoneVectorClock(CRDT_LWW_RegisterTombstone* t, VectorClock* vc) {
    if(getCrdtLWWRegisterTombstoneVectorClock(t) != NULL) {
        freeInnerClocks(&t->vectorClock);
    }
    if(vc == NULL) {
        t->vectorClock.length = -1;
    }else {
        cloneVectorClock(&t->vectorClock, vc);
        freeVectorClock(vc);
    }
}
 long long getCrdtLWWRegisterTombstoneTimestamp(CRDT_LWW_RegisterTombstone* t) {
    return t->timestamp;
}
 long long setCrdtLWWRegisterTombstoneTimestamp(CRDT_LWW_RegisterTombstone* t, long long timestamp) {
    t->timestamp = timestamp;
}
CrdtMeta* createCrdtRegisterTombstoneLastMeta(CRDT_RegisterTombstone* t) {
    VectorClock* vc = getCrdtLWWRegisterTombstoneVectorClock(t);
    if(vc != NULL) {
        return createMeta(
            getCrdtLWWRegisterTombstoneGid(t), 
            getCrdtLWWRegisterTombstoneTimestamp(t),
            dupVectorClock(vc)  
        );
    }
    return NULL;
}
void setCrdtLWWRegisterTombstoneMeta(CRDT_LWW_RegisterTombstone* t, CrdtMeta* meta) {
    if(meta == NULL) {
        setCrdtLWWRegisterTombstoneGid(t, -1);
        setCrdtLWWRegisterTombstoneTimestamp(t, -1);
        setCrdtLWWRegisterTombstoneVectorClock(t, NULL);
    }else{
        setCrdtLWWRegisterTombstoneGid(t, getMetaGid(meta));
        setCrdtLWWRegisterTombstoneTimestamp(t, getMetaTimestamp(meta));
        setCrdtLWWRegisterTombstoneVectorClock(t, dupVectorClock(getMetaVectorClock(meta)));
    }
    freeCrdtMeta(meta);
}
void *createCrdtLWWCrdtRegister(void) {
    CRDT_LWW_Register *crdtRegister = RedisModule_Alloc(sizeof(CRDT_LWW_Register));
    setType((CrdtObject*)crdtRegister, CRDT_DATA);
    setDataType((CrdtObject*)crdtRegister, CRDT_REGISTER_TYPE);
    crdtRegister->vectorClock.length = -1;
    setCrdtLWWRegisterTimestamp(crdtRegister, 0);
    setCrdtLWWRegisterGid(crdtRegister, 0);
    crdtRegister->value = NULL;
    return crdtRegister;
}
CRDT_LWW_Register* dupCrdtLWWRegister(CRDT_LWW_Register *target) {
    CRDT_LWW_Register* result = createCrdtLWWCrdtRegister();
    setCrdtLWWRegisterMeta(result, dupMeta(getCrdtLWWRegisterMeta(target)));
    setCrdtLWWRegisterValue(result, sdsdup(getCrdtLWWRegisterValue(target)));
    return result;
}
CRDT_Register* mergeLWWRegister(CRDT_Register* target, CRDT_Register* other) {
    if(target == NULL) {return dupCrdtLWWRegister(other);}
    if(other == NULL) {return dupCrdtLWWRegister(target);}
    CRDT_Register* result = dupCrdtLWWRegister(target);
    if(compareCrdtMeta(getCrdtLWWRegisterMeta(target), getCrdtLWWRegisterMeta(other)) > COMPARE_META_EQUAL ) {
        setCrdtLWWRegisterValue(result, sdsdup(getCrdtLWWRegisterValue(other)));
    }
    setCrdtLWWRegisterMeta(result, mergeMeta(getCrdtLWWRegisterMeta(target), getCrdtLWWRegisterMeta(other)));
    return result;
}
void updateLastVCLWWRegister(CRDT_Register* r, VectorClock* vc) {
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
    if (crdtRegister->value != NULL) {
        sdsfree(crdtRegister->value);
    }
    setCrdtLWWRegisterVectorClock(crdtRegister, NULL);
    RedisModule_Free(crdtRegister);
}

CRDT_LWW_Register* retrieveCrdtLWWRegister(void *data) {
    CRDT_LWW_Register* result = (CRDT_LWW_Register*)data;
    assert(getDataType(result->type) == CRDT_REGISTER_TYPE);
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
CRDT_RegisterTombstone* createCrdtLWWRegisterTombstone() {
    CRDT_LWW_RegisterTombstone *tombstone = RedisModule_Alloc(sizeof(CRDT_LWW_RegisterTombstone));
    setType((CrdtObject*)tombstone, CRDT_TOMBSTONE);
    setDataType((CrdtObject*)tombstone, CRDT_REGISTER_TYPE);
    tombstone->vectorClock.length = -1;
    tombstone->timestamp = -1;
    tombstone->gid = -1;
    return tombstone;
}

void *RdbLoadLWWCrdtRegister(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_Register *crdtRegister = createCrdtRegister();
    setCrdtLWWRegisterGid(crdtRegister, RedisModule_LoadSigned(rdb));
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

int setLWWCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    int result = compareCrdtMeta(getCrdtLWWRegisterMeta(data), meta);
    if(result > COMPARE_META_EQUAL) {
        setCrdtLWWRegisterValue(data, sdsdup(value));
    }
    setCrdtLWWRegisterMeta(data, mergeMeta(getCrdtLWWRegisterMeta(data), meta));
    return result;
}
CRDT_Register* filterLWWRegister(CRDT_Register* target, int gid, long long logic_time) {
    CRDT_LWW_Register* reg = retrieveCrdtLWWRegister(target);
    if(getCrdtLWWRegisterGid(reg) != gid) {
        return NULL;
    }
    VectorClockUnit* unit = getVectorClockUnit(getCrdtLWWRegisterVectorClock(reg), gid);
    if(unit == NULL) return NULL;
    long long vcu = get_logic_clock(*unit);
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
size_t crdtLWWRegisterTombstoneMemUsageFunc(void *value) {
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
CrdtMeta* addCrdtLWWRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    setCrdtLWWRegisterTombstoneMeta(t, mergeMeta(getCrdtLWWRegisterTombstoneMeta(t), meta));
    return meta;
}
CRDT_RegisterTombstone* mergeLWWRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other) {
    CRDT_LWW_RegisterTombstone* result = NULL;
    if(target == NULL && other == NULL) return NULL;
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    CRDT_LWW_RegisterTombstone* o = retrieveCrdtLWWRegisterTombstone(other);
    if(t == NULL) {return dupLWWCrdtRegisterTombstone(o);}
    if(o == NULL) {return dupLWWCrdtRegisterTombstone(t);}
    result = dupLWWCrdtRegisterTombstone(t);
    addCrdtLWWRegisterTombstone(result, getCrdtLWWRegisterTombstoneMeta(o));
    return result;
}
CRDT_RegisterTombstone* filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    if(getCrdtLWWRegisterGid(t) != gid) return NULL;
    VectorClockUnit* unit = getVectorClockUnit(getCrdtLWWRegisterTombstoneVectorClock(t), gid);
    if(unit == NULL) return NULL;
    long long vcu = get_logic_clock(*unit);
    if(vcu < logic_time) return NULL;
    return dupLWWCrdtRegisterTombstone(t);
}
CRDT_LWW_RegisterTombstone* dupLWWCrdtRegisterTombstone(CRDT_RegisterTombstone* target) {
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
    setCrdtLWWRegisterTombstoneVectorClock(tombstone, NULL);
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

int crdtLWWRegisterTombstoneGc(void* target, VectorClock* clock) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    return isVectorClockMonoIncr(getCrdtLWWRegisterTombstoneVectorClock(t), clock);
}
int crdtLWWRegisterGc(void* target, VectorClock* clock) {
    return CRDT_NO;
}