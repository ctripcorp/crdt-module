#include "crdt_lww_register.h"
static CrdtRegisterMethod Register_LLW_Methods = {
    .dup = dupLWWCrdtRegister,
    .del = delLWWCrdtRegister,
    .get = getLWWCrdtRegister,
    .getValue = getLwwCrdtRegisterValue,
    .set = setLWWCrdtRegister,
    .getInfo = crdtLWWRegisterInfo,
    .filter = filterLWWRegister,
    .merge = mergeLWWRegister,
    .updateLastVC = updateLastVCLWWRegister,
};
void *createLLWCrdtRegister(void) {
    CRDT_LWW_Register *crdtRegister = RedisModule_Alloc(sizeof(CRDT_LWW_Register));
    crdtRegister->parent.parent.parent.type = 0;
    crdtRegister->parent.parent.parent.type |= CRDT_DATA;
    crdtRegister->parent.parent.parent.type |= CRDT_REGISTER_TYPE;
    RedisModule_Debug(logLevel, "register type %lld", crdtRegister->parent.parent.parent.type);
    crdtRegister->value.meta = createMeta(-1, -1, NULL);
    crdtRegister->value.value = NULL;
    return crdtRegister;
}

CRDT_Register* mergeLWWRegister(CRDT_Register* target, CRDT_Register* other) {
    CRDT_LWW_Register* t = retrieveCrdtLWWRegister(target);
    CRDT_LWW_Register* o = retrieveCrdtLWWRegister(other);
    CRDT_LWW_Register* result = dupCrdtRegister(t);
    mergeCrdtRegisterValue(&result->value, &o->value);
    return result;
}
void updateLastVCLWWRegister(CRDT_Register* r, VectorClock* vc) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    VectorClock *old = data->value.meta->vectorClock;
    data->value.meta->vectorClock = vectorClockMerge(old, vc);
    freeVectorClock(old);
}
int purageLWWRegisterTombstone(CRDT_RegisterTombstone* tombstone, CRDT_Register* target) {
    CRDT_LWW_Register* current = retrieveCrdtLWWRegister(target);
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(tombstone);
    if(compareCrdtMeta(current->value.meta, t->meta) > 0) {
        return 1;
    }
    return 0;
}
int isExpireLWWTombstone(CRDT_RegisterTombstone* tombstone, CrdtMeta* meta) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(tombstone);
    return compareCrdtMeta(meta, t->meta) > COMPARE_META_EQUAL? CRDT_OK: CRDT_NO;
}


static CrdtRegisterTombstoneMethod Register_LLW_Tombstone_Methods = {
    .isExpire = isExpireLWWTombstone,
    .add = addLWWRegisterTombstone,
    .filter = filterLWWRegisterTombstone,
    .dup = dupLWWCrdtRegisterTombstone,
    .merge = mergeLWWRegisterTombstone,
    .purage = purageLWWRegisterTombstone,
};

void freeLLWCrdtRegister(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(obj);
    if (crdtRegister->value.value != NULL) {
        sdsfree(crdtRegister->value.value);
    }
    freeCrdtMeta(crdtRegister->value.meta);
    RedisModule_Free(crdtRegister);
}

CRDT_Register* dupLWWCrdtRegister(const CRDT_Register *val) {
    CRDT_LWW_Register *v = retrieveCrdtLWWRegister(val);
    CRDT_LWW_Register *dup = createLLWCrdtRegister();
    freeCrdtMeta(dup->value.meta);
    dup->value.meta = dupMeta(v->value.meta);
    if (v->value.value != NULL) {
        dup->value.value = sdsdup(v->value.value);
    }
    return dup;
}
CRDT_LWW_Register* retrieveCrdtLWWRegister(void *data) {
    CRDT_LWW_Register* result = (CRDT_LWW_Register*)data;
    assert(result->parent.parent.dataType == CRDT_REGISTER_TYPE);
    return result;
}
CRDT_LWW_RegisterTombstone* retrieveCrdtLWWRegisterTombstone(void *data) {
    CRDT_LWW_RegisterTombstone* result = (CRDT_LWW_RegisterTombstone*)data;
    assert(result->parent.parent.dataType == CRDT_REGISTER_TYPE);
    return result;
}
int delLWWCrdtRegister(CRDT_Register* current, CrdtMeta* meta) {
    CRDT_LWW_Register* target = retrieveCrdtLWWRegister(current);
    int result = compareCrdtMeta(target->value.meta, meta);
    if(result > COMPARE_META_EQUAL) {
        
        return 1;
    }
    return 0;
    
}
CRDT_RegisterTombstone* createCrdtLWWRegisterTombstone() {
    CRDT_LWW_RegisterTombstone *tombstone = RedisModule_Alloc(sizeof(CRDT_LWW_RegisterTombstone));
    tombstone->parent.parent.parent.type = 0;
    tombstone->parent.parent.parent.type |= CRDT_REGISTER_TYPE;
    tombstone->parent.parent.parent.type |= CRDT_TOMBSTONE;
    tombstone->parent.parent.parent.type |= CRDT_DATA;
    tombstone->meta = createMeta(-1, -1, NULL);
    return tombstone;
}

void *RdbLoadLWWCrdtRegister(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_Register *crdtRegister = createCrdtRegister();
    crdtRegister->parent.parent.parent.type |= CRDT_REGISTER_TYPE;
    crdtRegister->value.meta->gid = RedisModule_LoadSigned(rdb);
    crdtRegister->value.meta->timestamp = RedisModule_LoadSigned(rdb);
    crdtRegister->value.meta->vectorClock = rdbLoadVectorClock(rdb);

    size_t sdsLength;
    char* str = RedisModule_LoadStringBuffer(rdb, &sdsLength);
    sds val = sdsnewlen(str, sdsLength);
    crdtRegister->value.value = val;
    RedisModule_Free(str);

    return crdtRegister;
}

void RdbSaveLWWCrdtRegisterTombstone(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(value);
    RedisModule_SaveSigned(rdb, tombstone->meta->gid);
    RedisModule_SaveSigned(rdb, tombstone->meta->timestamp);
    rdbSaveVectorClock(rdb, tombstone->meta->vectorClock);
}
void RdbSaveLWWCrdtRegister(RedisModuleIO *rdb, void *value) {
    RedisModule_SaveSigned(rdb, LWW_TYPE);
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    RedisModule_SaveSigned(rdb, crdtRegister->value.meta->gid);
    RedisModule_SaveSigned(rdb, crdtRegister->value.meta->timestamp);
    rdbSaveVectorClock(rdb, crdtRegister->value.meta->vectorClock);
    RedisModule_SaveStringBuffer(rdb, crdtRegister->value.value, sdslen(crdtRegister->value.value));
}

sds getLWWCrdtRegister(CRDT_Register* r) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    return data->value.value;
}
CrdtRegisterValue* getLwwCrdtRegisterValue(CRDT_Register* r) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    return &data->value;
}
int setLWWCrdtRegister(CRDT_Register* r, CrdtMeta* meta, sds value) {
    CRDT_LWW_Register* data = retrieveCrdtLWWRegister(r);
    int result = compareCrdtMeta(data->value.meta, meta);
    if(result > COMPARE_META_EQUAL) {
        if(data->value.value !=NULL ) {sdsfree(data->value.value);}
        data->value.value = sdsdup(value);
    }
    data->value.meta = addOrCreateMeta(data->value.meta, meta);
    return result;
}
CRDT_Register* filterLWWRegister(CRDT_Register* target, int gid, long long logic_time) {
    CRDT_LWW_Register* reg = retrieveCrdtLWWRegister(target);
    if(reg->value.meta->gid != gid) {
        return NULL;
    }
    VectorClockUnit* unit = getVectorClockUnit(reg->value.meta->vectorClock, gid);
    if(unit->logic_time > logic_time) {
        return dupLWWCrdtRegister(reg);
    }  
    return NULL;
}
sds crdtLWWRegisterInfo(CRDT_LWW_Register *crdtRegister) {
    sds result = sdsempty();
    sds vcStr = vectorClockToSds(crdtRegister->value.meta->vectorClock);
    result = sdscatprintf(result, "gid: %d, timestamp: %lld, vector-clock: %s, val: %s",
            crdtRegister->value.meta->gid, crdtRegister->value.meta->timestamp, vcStr, crdtRegister->value.value);
    sdsfree(vcStr);
    return result;
}
size_t crdtLWWRegisterTombstoneMemUsageFunc(void *value) {
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(value);
    size_t valSize = sizeof(CRDT_Register) + sdsAllocSize(tombstone->meta);
    int vclcokNum = tombstone->meta->vectorClock->length;
    size_t vclockSize = vclcokNum * sizeof(VectorClockUnit) + sizeof(VectorClock);
    return valSize + vclockSize;
}
void crdtLWWRegisterDigestFunc(RedisModuleDigest *md, void *value) {
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    RedisModule_DigestAddLongLong(md, crdtRegister->value.meta->gid);
    RedisModule_DigestAddLongLong(md, crdtRegister->value.meta->timestamp);
    sds vclockStr = vectorClockToSds(crdtRegister->value.meta->vectorClock);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) crdtRegister->value.value, sdslen(crdtRegister->value.value));
    RedisModule_DigestEndSequence(md);
}
size_t crdtLWWRegisterMemUsageFunc(const void *value) {
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    size_t valSize = sizeof(CRDT_Register) + sdsAllocSize(crdtRegister->value.value);
    int vclcokNum = crdtRegister->value.meta->vectorClock->length;
    size_t vclockSize = vclcokNum * sizeof(VectorClockUnit) + sizeof(VectorClock);
    return valSize + vclockSize;
}

void AofRewriteCrdtLWWRegister(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CRDT_LWW_Register *crdtRegister = retrieveCrdtLWWRegister(value);
    sds vclockSds = vectorClockToSds(crdtRegister->value.meta->vectorClock);
    RedisModule_EmitAOF(aof, "CRDT.SET", "scllc", key, crdtRegister->value.value, crdtRegister->value.meta->gid, crdtRegister->value.meta->timestamp, vclockSds);
    sdsfree(vclockSds);
}

CrdtMeta* addLWWRegisterTombstone(CRDT_RegisterTombstone* target, CrdtMeta* meta) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    t->meta = addOrCreateMeta(t->meta, meta);
    return t->meta;
}
CRDT_RegisterTombstone* mergeLWWRegisterTombstone(CRDT_RegisterTombstone* target, CRDT_RegisterTombstone* other) {
    CRDT_LWW_RegisterTombstone* result = NULL;
    if(target == NULL && other == NULL) return NULL;
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    CRDT_LWW_RegisterTombstone* o = retrieveCrdtLWWRegisterTombstone(other);
    if(t == NULL) {return dupLWWCrdtRegisterTombstone(o);}
    if(o == NULL) {return dupLWWCrdtRegisterTombstone(o);}
    result = dupLWWCrdtRegisterTombstone(t);
    addLWWRegisterTombstone(result, o->meta);
    return result;
}
CRDT_RegisterTombstone* filterLWWRegisterTombstone(CRDT_RegisterTombstone* target, int gid, long long logic_time) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    if(t->meta->gid != gid) return NULL;
    VectorClockUnit* unit = getVectorClockUnit(t->meta->vectorClock, gid);
    if(unit == NULL) return NULL;
    if(unit->logic_time < logic_time) return NULL;
    return dupLWWCrdtRegisterTombstone(t);
}
CRDT_LWW_RegisterTombstone* dupLWWCrdtRegisterTombstone(CRDT_RegisterTombstone* target) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    CRDT_LWW_RegisterTombstone* result = createCrdtLWWRegisterTombstone();
    crdtMetaCp(t->meta, result->meta);
    return result;
}

void freeLLWCrdtRegisterTombstone(void *obj) {
    if (obj == NULL) {
        return;
    }
    CRDT_LWW_RegisterTombstone *crdtRegister = retrieveCrdtLWWRegisterTombstone(obj);
    freeCrdtMeta(crdtRegister->meta);
    RedisModule_Free(crdtRegister);
}
void AofRewriteCrdtLWWRegisterTombstone(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    CRDT_LWW_RegisterTombstone *tombstone = retrieveCrdtLWWRegisterTombstone(value);
    sds vclockSds = vectorClockToSds(tombstone->meta->vectorClock);
    RedisModule_EmitAOF(aof, "CRDT.DEL", "scllc", key, tombstone->meta->gid, tombstone->meta->timestamp, vclockSds);
    sdsfree(vclockSds);
}

void crdtLWWRegisterTombstoneDigestFunc(RedisModuleDigest *md, void *value) {
    CRDT_LWW_RegisterTombstone *crdtRegister = retrieveCrdtLWWRegisterTombstone(value);
    RedisModule_DigestAddLongLong(md, crdtRegister->meta->gid);
    RedisModule_DigestAddLongLong(md, crdtRegister->meta->timestamp);
    sds vclockStr = vectorClockToSds(crdtRegister->meta->vectorClock);
    RedisModule_DigestAddStringBuffer(md, (unsigned char *) vclockStr, sdslen(vclockStr));
    sdsfree(vclockStr);
    RedisModule_DigestEndSequence(md);
}

void *RdbLoadLWWCrdtRegisterTombstone(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    CRDT_LWW_RegisterTombstone *tombstone = createCrdtLWWRegisterTombstone();
    tombstone->meta->gid = RedisModule_LoadSigned(rdb);
    tombstone->meta->timestamp = RedisModule_LoadSigned(rdb);
    tombstone->meta->vectorClock = rdbLoadVectorClock(rdb);
    return tombstone;
}

int crdtLWWRegisterTombstoneGc(void* target, VectorClock* clock) {
    CRDT_LWW_RegisterTombstone* t = retrieveCrdtLWWRegisterTombstone(target);
    return isVectorClockMonoIncr(t->meta->vectorClock, clock);
}
int crdtLWWRegisterGc(void* target, VectorClock* clock) {
    return CRDT_NO;
}