#include "./crdt_orset_rc.h"
#include "../crdt_util.h"
//  generic function
crdt_rc_tombstone* retrieveCrdtRcTombstone(CRDT_RCTombstone* rt) {
    return (crdt_rc_tombstone*)rt;
}

crdt_orset_rc* retrieveCrdtRc(CRDT_RC* rc) {
    return (crdt_orset_rc*)rc;
}

int getRcElementLen(CRDT_RC* rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    return r->len;
}

void freeCrdtRc(void *value) {
    crdt_orset_rc* rc = retrieveCrdtRc(value);
    freeVectorClock(rc->vectorClock);
    for(int i = 0; i < rc->len; i++) {
        freeRcElement(rc->elements[i]);
        rc->elements[i] = NULL;
    }
    RedisModule_Free(rc->elements);
}

//========================= Virtual functions =======================
int getCrdtRcType(CRDT_RC* rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    for(int i = 0; i< r->len; i++) {
        if(r->elements[i]->base) {
            if(r->elements[i]->base->type == VALUE_TYPE_FLOAT) {
                return VALUE_TYPE_FLOAT;
            }
        }
        if(r->elements[i]->counter) {
            if(r->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                return VALUE_TYPE_FLOAT;
            }
        }
    }
    return VALUE_TYPE_INTEGER;
}

CrdtMeta* getCrdtRcLastMeta(CRDT_RC*rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    long long time = 0;
    int gid = 0;
    for(int i = 0; i < r->len; i++) {
        rc_base* b = r->elements[i]->base;
        if(b && b->timespace > time) {
            time = b->timespace;
            gid = r->elements[i]->gid;
        }
    }
    CrdtMeta* meta =  createMeta(gid, time, dupVectorClock(r->vectorClock));
    return meta;
}

long double getCrdtRcBaseFloatValue(CRDT_RC* rc, CrdtMeta* lastMeta) {
    int gid = getMetaGid(lastMeta);
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    assert(r->type == VALUE_TYPE_FLOAT);
    rc_element* el = findRcElement(r, gid);
    return el->base->conv.f;
}

long long getCrdtRcBaseIntValue(CRDT_RC* rc, CrdtMeta* lastMeta) {
    int gid = getMetaGid(lastMeta);
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    assert(r->type == VALUE_TYPE_INTEGER);
    rc_element* el = findRcElement(r, gid);
    return el->base->conv.i;
}
long double getCrdtRcCouanterFloatValue(CRDT_RC* rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    long double counter = 0; 
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->counter) {
            counter += r->elements[i]->counter->conv.f - r->elements[i]->counter->del_conv.f;
        }
    }
    return counter;
}

long long getCrdtRcCouanterIntValue(CRDT_RC* rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    long long counter = 0; 
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->counter) {
            counter += r->elements[i]->counter->conv.i - r->elements[i]->counter->del_conv.i;
        }
    }
    return counter;
}

long long getCrdtRcIntValue(CRDT_RC* rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    long long counter = 0;                  
    long long base = 0;
    long long base_time = 0;
    for(int i = 0; i < r->len; i++) {
        rc_base* b = r->elements[i]->base;
        if(b && b->timespace > base_time) {
            base_time = b->timespace;
            base = b->conv.i;
        }
        if(r->elements[i]->counter) {
            counter += r->elements[i]->counter->conv.i - r->elements[i]->counter->del_conv.i;
        }
    }
    return base + counter;
}

long double getCrdtRcFloatValue(CRDT_RC* rc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    long double counter = 0;                  
    long double base = 0;
    long long base_time = 0;
    for(int i = 0; i < r->len; i++) {
        rc_base* b = r->elements[i]->base;
        if(b && b->timespace > base_time) {
            base_time = b->timespace;
            if(b->type == VALUE_TYPE_FLOAT) {
                base = b->conv.f;
            } else if(b->type == VALUE_TYPE_INTEGER) {
                base = (long double)b->conv.i;
            }
            
        }
        if(r->elements[i]->counter) {
            if(r->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                counter += r->elements[i]->counter->conv.f - r->elements[i]->counter->del_conv.f;
            } else if(r->elements[i]->counter->type == VALUE_TYPE_INTEGER) {
                long long v = (r->elements[i]->counter->conv.i - r->elements[i]->counter->del_conv.i);
                counter += (long double)v;
            } else {
                assert(1 == 0);
            }
            
        }
    }
    return base + counter;
}


int tryUpdateCounter(CRDT_RC* rc, CRDT_RCTombstone* tom, int gid, long long timestamp, long long start_clock, long long end_clock, int type,  void* val) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tom);
    if(rt != NULL) {
        long long tvcu = get_vcu(rt->vectorClock, gid);
        if(tvcu > end_clock) {
            rc_tombstone_element* el =  findRcTombstoneElement(rt, gid);
            assert(el != NULL && el->counter != NULL);
            assert(el->counter->type == type);
            if(el->counter->end_clock < end_clock) {
                el->counter->end_clock = end_clock;
                if(type == VALUE_TYPE_FLOAT) {
                    el->counter->conv.f = *(long double*)val;
                } else if(type == VALUE_TYPE_INTEGER) {
                    el->counter->conv.i = *(long long*)val;
                }
            }
            return PURGE_VAL;
        } else {
            initCrdtRcFromTombstone(rc, tom);
        }
    }
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    rc_element* e = findRcElement(r, gid);
    if(e == NULL) {
        e = createRcElement(gid);
        appendRcElement(r, e);
    } 
    if(e->counter == NULL) {
        e->counter = createGcounter(type);
        e->counter->start_clock = start_clock;
    } else {
        assert(e->counter->start_clock == start_clock);
    }
    e->counter->end_clock = end_clock;
    if(type == VALUE_TYPE_FLOAT) {
        e->counter->conv.f = *(long double*)val;
    } else if(type == VALUE_TYPE_INTEGER) {
        e->counter->conv.i = *(long long*)val;
    }
    e->counter->type = type;
    VectorClock vc = newVectorClockFromGidAndClock(gid, end_clock);
    crdtRcUpdateLastVC(rc, vc);
    freeVectorClock(vc);
    return PURGE_TOMBSTONE;
}

gcounter*  addOrCreateCounter(CRDT_RC* rc,  CrdtMeta* meta, int type, void* val) {
    int gid = getMetaGid(meta);
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    rc_element* e = findRcElement(r, gid);
    if(e == NULL) {
        e = createRcElement(gid);
        appendRcElement(r, e);
    }
    long long vcu = getVcu(meta);
    if(e->counter == NULL) {
        e->counter = createGcounter(type);
        setCounterType(e->counter, type);
        e->counter->start_clock = vcu;
    }
    e->counter->end_clock = vcu;
    if(e->counter->type != type) {
         setCounterType(e->counter, type);
        // float can't to int
        if(type == VALUE_TYPE_FLOAT) {
            long double v = (*(long double*)val);
            long double f = (long double)(e->counter->conv.i);
            e->counter->conv.f = v + f;
        } else {
            assert(1 == 0);
        }
    } else {
        if(type == VALUE_TYPE_FLOAT) {
            e->counter->conv.f += (*(long double*)val);
        } else if(type == VALUE_TYPE_INTEGER) {
            e->counter->conv.i += (*(long long*)val);
        } else {
            assert(1 == 0);
        }
    }
    
    crdtRcUpdateLastVC(rc, getMetaVectorClock(meta));
    return e->counter;
}

//========================= tombstone function =============================
int crdtRcTombstoneGc(CrdtTombstone* target, VectorClock clock) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(target);
    return isVectorClockMonoIncr(rt->vectorClock, clock);
    // return 0;
}

CrdtTombstone* crdRcTombstoneMerge(CrdtTombstone* currentVal, CrdtTombstone* value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    if(currentVal == NULL) {
        return dupCrdtRcTombstone(value);
    }
    if(value == NULL) {
        return dupCrdtRcTombstone(currentVal);
    }
    crdt_rc_tombstone *current = retrieveCrdtRcTombstone(currentVal);
    crdt_rc_tombstone *other = retrieveCrdtRcTombstone(value);
    crdt_rc_tombstone* result = retrieveCrdtRcTombstone(dupCrdtRcTombstone(currentVal));
    freeVectorClock(result->vectorClock);
    result->vectorClock = vectorClockMerge(current->vectorClock, other->vectorClock);
    for(int i = 0; i < other->len; i++) {
        rc_tombstone_element * el = findRcTombstoneElement(current, other->elements[i]->gid);
        if(el == NULL) {
            el = dupCrdtRcTombstoneElement(other->elements[i]);
            appendRcTombstoneElement(result, el);
        } else {
            assign_max_rc_tombstone_element(el, other->elements[i]);
        }
    }
    return (CrdtTombstone* )result;
}



CrdtObject** crdtRcTombstoneFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(target);
    //value + gid + time + vectorClock
    if (crdtRcTombstoneMemUsageFunc(rt) > maxsize) {
        *length  = -1;
        return NULL;
    }
    VectorClockUnit unit = getVectorClockUnit(rt->vectorClock, gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    long long vcu = get_logic_clock(unit);
    if(vcu > logic_time) {
        *length = 1;
        CrdtObject** re = RedisModule_Alloc(sizeof(crdt_rc_tombstone*));
        re[0] = (CrdtObject*)rt;
        return re;
    }  
    return NULL;
}

void freeCrdtRcTombstoneFilter(CrdtTombstone** filters, int num) {
    RedisModule_ZFree(filters);
}
int comp_rc_tombstone_element(const void* a, const void* b) {
    return (*(rc_tombstone_element**)a)->gid > (*(rc_tombstone_element**)b)->gid? 1: 0;
}
sds crdtRcTombstoneInfo(void* t) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(t);
    sds result = sdsempty();
    sds vc_info = vectorClockToSds(rt->vectorClock);
    result = sdscatprintf(result, "type: crdt_rc_tombstone, vc: %s\r\n", vc_info);
    sdsfree(vc_info);
    for(int i = 0; i < rt->len; i++) {
        rc_tombstone_element* el = rt->elements[i];
        result = sdscatprintf(result, "   %d) gid: %d, unit: %lld\r\n", i, el->gid, el->del_unit);
        if(el->counter) {
            if(el->counter->type == VALUE_TYPE_FLOAT) {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %Lf}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.f);
                result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %Lf}\r\n",el->counter->del_end_clock, el->counter->del_conv.f);
            } else {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %lld}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.i);
                result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %lld}\r\n",el->counter->del_end_clock, el->counter->del_conv.i);
            }
        }
    }
    return result;
}

void updateRcTombstoneLastVc(CRDT_RCTombstone* rt, VectorClock vc) {
    crdt_rc_tombstone* r = retrieveCrdtRcTombstone(rt);
    VectorClock tag = r->vectorClock;
    if(!isNullVectorClock(tag)) {
        r->vectorClock = vectorClockMerge(tag, vc);
        freeVectorClock(tag);
    } else {
        r->vectorClock = dupVectorClock(vc);
    }
}

void crdtRcUpdateLastVC(void* rc, VectorClock vc) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    VectorClock tag = r->vectorClock;
    if(!isNullVectorClock(tag)) {
        r->vectorClock = vectorClockMerge(tag, vc);
        freeVectorClock(tag);
    } else {
        r->vectorClock = dupVectorClock(vc);
    }
}



void initCrdtRcFromTombstone(CRDT_RC* r, CRDT_RCTombstone* t) {
    if(t == NULL) {
        return;
    }
    crdt_orset_rc* rc = retrieveCrdtRc(r);
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(t);
    rc->vectorClock = dupVectorClock(rt->vectorClock);
    for(int i = 0; i < rt->len; i++) {
        rc_tombstone_element* tel =  rt->elements[i];
        rc_element* rel = findRcElement(rc, tel->gid);
        if(rel == NULL) {
            rel = createRcElement(tel->gid);
            rel->counter = tel->counter;
            tel = NULL;
            appendRcElement(rc, rel);
        } else {
            update_add_counter(rel->counter, tel->counter);
            update_del_counter(rel->counter, tel->counter);
        }
    }
}

int comp_rc_element(const void* a, const void* b) {
    return (*(rc_element**)a)->gid > (*(rc_element**)b)->gid? 1: 0;
}

sds crdtRcInfo(void* value) {
    crdt_orset_rc* rc = retrieveCrdtRc(value);
    sds result = sdsempty();
    sds vc_info = vectorClockToSds(rc->vectorClock);
    result = sdscatprintf(result, "type: crdt_orset_rc, vc: %s\r\n", vc_info);
    sdsfree(vc_info);
    for(int i = 0; i < rc->len; i++) {
        rc_element* el = rc->elements[i];
        result = sdscatprintf(result, "  %d) gid: %d\r\n", i, (int)el->gid);
        if(el->base) {
            if(el->base->type) {
                result = sdscatprintf(result, "     base: { clock: %lld, timespace: %lld, value: %Lf} \r\n", el->base->unit, el->base->timespace, el->base->conv.f);
            } else {
                result = sdscatprintf(result, "     base: { clock: %lld, timespace: %lld, value: %lld} \r\n", el->base->unit, el->base->timespace, el->base->conv.i);
            }
        }
        if(el->counter) {
            if(el->counter->type == VALUE_TYPE_FLOAT) {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %Lf}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.f);
                if(el->counter->del_end_clock != 0) result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %Lf}\r\n",el->counter->del_end_clock, el->counter->del_conv.f);
            } else {
                result = sdscatprintf(result, "       counter: { start: %lld, end: %lld, value: %lld}\r\n", el->counter->start_clock, el->counter->end_clock, el->counter->conv.i);
                if(el->counter->del_end_clock != 0) result = sdscatprintf(result, "       counter-del:{ del_end: %lld, value: %lld}\r\n",el->counter->del_end_clock, el->counter->del_conv.i);
            }
        }
    }
    return result;
}

int crdtRcTrySetValue(CRDT_RC* rc, CrdtMeta* set_meta, int gslen, gcounter_meta** gs, CrdtTombstone* tombstone, int type, void* val) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tombstone);
    if(rt != NULL) {
        if(isVectorClockMonoIncr(getMetaVectorClock(set_meta) , rt->vectorClock)) {
            return PURGE_VAL;
        }
    }
    int gid = getMetaGid(set_meta);
    VectorClock vc = getMetaVectorClock(set_meta);
    crdtRcUpdateLastVC(rc, vc);
    int added = 0;
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->gid == gid) {
            if(r->elements[i]->base) {
                long long unit = get_vcu(vc, r->elements[i]->gid);
                if(r->elements[i]->base->unit <= unit) {
                    resetElementBase(r->elements[i]->base, set_meta, type, val);
                }
            } else {
                r->elements[i]->base = createRcElementBase();
                resetElementBase(r->elements[i]->base, set_meta, type, val);
            }
            added = 1;
        } else {
            if(r->elements[i]->base) {
                long long unit = get_vcu(vc, r->elements[i]->gid);
                if(unit >= r->elements[i]->base->unit) {
                    freeBase(r->elements[i]->base);
                    r->elements[i]->base = NULL;
                }
            }
        }
        for(int j = 0; j < gslen; j++) {
            if(gs[j] != NULL && r->elements[i]->gid == gs[j]->gid) {
                if(!r->elements[i]->counter) {
                    r->elements[i]->counter = createGcounter(gs[j]->type);
                }
                assert(r->elements[i]->counter->type == gs[j]->type);
                if(r->elements[i]->counter->del_end_clock < gs[j]->end_clock) {
                    update_del_counter_by_meta(r->elements[i]->counter, gs[j]);
                }
                freeGcounterMeta(gs[j]);
                gs[j] = NULL;
            }
        }
    }
    for(int i = 0; i < gslen; i++) {
        if(gs[i] != NULL) {
            rc_element* e = createRcElement(gs[i]->gid);
            e->counter = createGcounter(gs[i]->type);
            update_del_counter_by_meta(e->counter, gs[i]);
            appendRcElement(r, e);
            if(gs[i]->gid == gid) {
                e->base = createRcElementBase( );
                resetElementBase(e->base, set_meta, type, val);
                added = 1;
            }
            freeGcounterMeta(gs[i]);
            gs[i] = NULL;
        }
    }
    if(added == 0) {
        rc_element* e = createRcElement(gid);
        e->base = createRcElementBase( );
        resetElementBase(e->base, set_meta, type, val);
        appendRcElement(r, e);
    }
    return PURGE_TOMBSTONE;
}
int crdtRcSetValue(CRDT_RC* rc, CrdtMeta* set_meta, sds* gs, CrdtTombstone* tombstone, int type, void* val) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    int gid = getMetaGid(set_meta);
    int index = 0;
    int added = 0;
    VectorClock vc = getMetaVectorClock(set_meta);
    crdtRcUpdateLastVC(rc, vc);
    for(int i = 0; i < r->len; i++) {
        if(r->elements[i]->gid == gid) {
            if(!r->elements[i]->base) {
                r->elements[i]->base = createRcElementBase();
            } 
            resetElementBase(r->elements[i]->base, set_meta, type, val);
            added = 1;
        } else {
            freeBase(r->elements[i]->base);
            r->elements[i]->base = NULL;
        }
        
        if(r->elements[i]->counter) {
            r->elements[i]->counter->del_end_clock = r->elements[i]->counter->end_clock;
            if(r->elements[i]->counter->type == VALUE_TYPE_FLOAT) {
                r->elements[i]->counter->del_conv.f = r->elements[i]->counter->conv.f;
            } else {
                r->elements[i]->counter->del_conv.i = r->elements[i]->counter->conv.i;
            }
            if(gs != NULL) gs[index] = gcounterDelToSds(r->elements[i]->gid, r->elements[i]->counter);
            index++;
        }
    }
    
    if(added == 0) {
        rc_element* e =  createRcElement(gid);
        e->base = createRcElementBase();
        resetElementBase(e->base, set_meta, type, val);
        appendRcElement(r, e);
    }
    return index;
}

CRDT_RC* createCrdtRc() {
    crdt_orset_rc* rc = RedisModule_Alloc(sizeof(crdt_orset_rc));
    rc->vectorClock = newVectorClock(0);
    setDataType((CrdtObject*)rc, CRDT_RC_TYPE);
    setType((CrdtObject*)rc, CRDT_DATA);
    rc->len = 0;
    rc->elements = NULL;
    return (CRDT_RC*)rc;
}
CRDT_RC* dupCrdtRc(CRDT_RC* rc) {
    if(rc == NULL) {return NULL;}
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    crdt_orset_rc* dup = retrieveCrdtRc(createCrdtRc());
    dup->vectorClock = dupVectorClock(r->vectorClock);
    for(int i = 0; i < r->len; i++) {
        rc_element* el = dupRcElement(r->elements[i]);
        appendRcElement(dup, el);
    }
    assert(r->len == dup->len);
    return  (CRDT_RC*)dup;
;
}

CRDT_RCTombstone* createCrdtRcTombstone() {
    crdt_rc_tombstone* rt = RedisModule_Alloc(sizeof(crdt_rc_tombstone));
    rt->type = 0;
    setDataType((CrdtObject*)rt, CRDT_RC_TYPE);
    setType((CrdtObject*)rt, CRDT_TOMBSTONE);
    rt->vectorClock = newVectorClock(0);
    rt->len = 0;
    rt->elements = NULL;
    return (CRDT_RCTombstone*)rt;
}

CRDT_RCTombstone* dupCrdtRcTombstone(CRDT_RCTombstone* tombstone) { 
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tombstone);
    crdt_rc_tombstone* dup = retrieveCrdtRcTombstone(createCrdtRcTombstone());
    dup->vectorClock = dupVectorClock(rt->vectorClock);
    for(int i = 0; i < rt->len; i++) {
        rc_tombstone_element* el = dupCrdtRcTombstoneElement(rt->elements[i]);
        appendRcTombstoneElement(dup, el);
    }
    assert(dup->len == rt->len);
    return (CRDT_RCTombstone*)dup;
}

int appendRcTombstoneElement(crdt_rc_tombstone* rt, rc_tombstone_element* element) {
    rt->len ++;
    if(rt->len != 1) {
        rt->elements = RedisModule_Realloc(rt->elements, sizeof(rc_tombstone_element*) * rt->len);
    } else {
        rt->elements = RedisModule_Alloc(sizeof(rc_tombstone_element*) * 1);
    }
    rt->elements[rt->len-1] = element;
    qsort(rt->elements, rt->len, sizeof(rc_tombstone_element*), comp_rc_tombstone_element);
    return 1;
}
// rc tombstone functions
rc_tombstone_element* createRcTombstoneElement(int gid) {
    rc_tombstone_element* element = RedisModule_Alloc(sizeof(rc_tombstone_element));
    element->gid = gid;
    element->counter = NULL;
    element->del_unit = 0;
    return element;
}

void freeRcTombstoneElement(void* element) {
    RedisModule_Free(element);
}

rc_tombstone_element* dupCrdtRcTombstoneElement(rc_tombstone_element* rt) {
    rc_tombstone_element* dup = createRcTombstoneElement(rt->gid);
    dup->gid = rt->gid;
    dup->del_unit = rt->del_unit;
    dup->counter = dupGcounter(rt->counter);
    return dup;
}

void assign_max_rc_tombstone_element(rc_tombstone_element* target, rc_tombstone_element* src) {
    assert(target->gid == src->gid);
    target->del_unit = max(target->del_unit, src->del_unit);
    assign_max_rc_counter(target->counter, src->counter);
}

rc_tombstone_element* findRcTombstoneElement(crdt_rc_tombstone* rt, int gid) {
    for(int i = 0; i < rt->len; i++) {
        if(rt->elements[i]->gid == gid) {
            return rt->elements[i];
        }
    }
    return NULL;
}

int initRcTombstoneFromRc(CRDT_RCTombstone *tombstone, CrdtMeta* meta, CRDT_RC* rc, sds* del_counters) {
    crdt_orset_rc* r = retrieveCrdtRc(rc);
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(tombstone);
    updateRcTombstoneLastVc(tombstone, getMetaVectorClock(meta));
    int index = 0;
    int added = 0;
    int gid = getMetaGid(meta);
    int vcu = getVcu(meta);
    for(int i = 0; i < r->len; i++) {
        rc_tombstone_element* t = findRcTombstoneElement(rt, r->elements[i]->gid);
        if(t == NULL) {
            t = createRcTombstoneElement(r->elements[i]->gid);
            appendRcTombstoneElement(rt, t);
        }
        if(r->elements[i]->gid == gid) {
            added = 1;
            if(t->del_unit < vcu) {
                t->del_unit = vcu;
            }
        }else if(r->elements[i]->base) {
            if(t->del_unit < r->elements[i]->base->unit) {
                t->del_unit = r->elements[i]->base->unit;
            }
        }
        if(r->elements[i]->counter) {
            if(t->counter == NULL) {
                t->counter = r->elements[i]->counter;
                counter_del(t->counter, t->counter);
            } else {
                rc_element* el = r->elements[i];
                assert(t->counter->start_clock == el->counter->start_clock);
                update_add_counter(t->counter, el->counter);
                if( el->counter->end_clock < el->counter->del_end_clock) {
                    update_del_counter(t->counter, el->counter);
                } else {
                    if(t->counter->del_end_clock < el->counter->end_clock) {
                        counter_del(t->counter, el->counter);
                    }
                }   
                freeGcounter(el->counter);             
            }
            r->elements[i]->counter = NULL;
            if(del_counters) del_counters[index++] = gcounterDelToSds(t->gid,t->counter);
        }
    } 
    if(added == 0) {
        rc_tombstone_element* el = createRcTombstoneElement(gid);
        el->del_unit = vcu;
        appendRcTombstoneElement(rt, el);
    }
    return index;
}



rc_base* createRcElementBase() {
    rc_base* base = RedisModule_Alloc(sizeof(rc_base));
    return base;
}

void assign_max_rc_base(rc_base* target, rc_base* src) {
    if(target->unit < src->unit) {
        target->timespace = src->timespace;
        target->type = src->type;
        if (src->type == VALUE_TYPE_FLOAT) {
            target->conv.f = src->conv.f;
        } else if(src->type == VALUE_TYPE_INTEGER) {
            target->conv.i = src->conv.i;
        }
    }
}

rc_base* dupRcBase(rc_base* base) {
    if(base == NULL) { return NULL;}
    rc_base* dup = createRcElementBase();
    dup->type = base->type;
    dup->timespace = base->timespace;
    dup->unit = base->unit;
    if(dup->type == VALUE_TYPE_FLOAT) {
        dup->conv.f = base->conv.f;
    } else if (dup->type == VALUE_TYPE_INTEGER) {
        dup->conv.i = base->conv.i;
    }
    return dup;
}

int resetElementBase(rc_base* base, CrdtMeta* meta, int val_type, void* v) {
    if (val_type == VALUE_TYPE_FLOAT) {
        base->conv.f = *(long double*)v;
    } else if (val_type == VALUE_TYPE_INTEGER) {
        base->conv.i = *(long long*)v;
    } else {
        assert( 1 == 0);
    }
    base->unit = getVcu(meta);
    base->timespace = getMetaTimestamp(meta);
    base->type = val_type;
    return 1;
}

void freeBase(rc_base* base) {
    RedisModule_Free(base);
}
rc_element* createRcElement(int gid) {
    rc_element* element = RedisModule_Alloc(sizeof(rc_element));
    element->gid = gid;
    element->counter = NULL;
    element->base = NULL;
    return element;
}

void freeRcElement(void* element) {
    RedisModule_Free(element);
}



rc_element* dupRcElement(rc_element* el) {
    rc_element* dup = createRcElement(el->gid);
    dup->base = dupRcBase(el->base);
    dup->counter = dupGcounter(el->counter);
    return dup;
}

void assign_max_rc_element(rc_element* target, rc_element* src) {
    if(target == NULL || src == NULL) return;
    assert(target->gid == src->gid);
    if(src->base) {
        if(!target->base) target->base = createRcElementBase();
        assign_max_rc_base(target->base, src->base);
    }
    if(src->counter) {
        if(!target->counter) target->counter = createGcounter(src->counter->type);
        assign_max_rc_counter(target->counter, src->counter);
    }
    
}

int appendRcElement(crdt_orset_rc* rc, rc_element* element) {
    rc->len ++;
    if(rc->len != 1) {
        rc->elements = RedisModule_Realloc(rc->elements, sizeof(rc_element*) * rc->len);
    } else {
        rc->elements = RedisModule_Alloc(sizeof(rc_element*) * 1);
    }
    rc->elements[rc->len-1] = element;
    qsort(rc->elements, rc->len, sizeof(rc_element*), comp_rc_element);
    return 1;
}

rc_element* findRcElement(crdt_orset_rc* rc, int gid) {
    for(int i = 0; i < rc->len; i++) {
        if(rc->elements[i]->gid == gid) {
            return rc->elements[i];
        }
    }
    return NULL;
}

// gcounter* getRcCounter(CRDT_RC* r, int gid) {
//     rc_element* el = findRcElement(r, gid);
//     if(el == NULL) {return NULL;}
//     return el->counter;
// }

//
VectorClock  getCrdtRcLastVc(void* r) {
    crdt_orset_rc* rc = retrieveCrdtRc(r);
    return rc->vectorClock;
}

VectorClock getCrdtRcTombstoneLastVc(crdt_rc_tombstone* rt) {
    return rt->vectorClock;
}

void freeCrdtRcTombstone(void *obj) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(obj);
    freeVectorClock(rt->vectorClock);
    for(int i = 0; i < rt->len; i++) {
        freeRcTombstoneElement(rt->elements[i]);
        rt->elements[i] = NULL;
    }
    RedisModule_Free(rt->elements);
}
//
int mergeRcTombstone(CRDT_RCTombstone* tombstone, CrdtMeta* meta, int del_len, gcounter_meta** del_counter) {
    crdt_rc_tombstone* t = retrieveCrdtRcTombstone(tombstone);
    VectorClock vc = getMetaVectorClock(meta);
    for(int i = 0; i < t->len; i++) {
        rc_tombstone_element* el = (t->elements[i]);
        long long unit = get_vcu(vc, el->gid);
        if(unit > el->del_unit) {
            el->del_unit = unit;
        }
        for(int j = 0; j < del_len; j++) {
            if(del_counter[j] != NULL && del_counter[j]->gid == el->gid) {
                update_del_counter_by_meta(el->counter, del_counter[j]);
                freeGcounterMeta(del_counter[j]);
                del_counter[j] = NULL;
            }
        }
    }
    for(int j = 0; j < del_len; j++) {
        if(del_counter[j] != NULL) {
            rc_tombstone_element* el = createRcTombstoneElement(del_counter[j]->gid);
            appendRcTombstoneElement(t, el);
            el->del_unit = get_vcu(vc, el->gid);
            el->counter = createGcounter(del_counter[j]->type);
            el->counter->start_clock = del_counter[j]->start_clock;
            update_del_counter_by_meta(el->counter, del_counter[j]);
            freeGcounterMeta(del_counter[j]);
            del_counter[j] = NULL;
        }
    }
    for(int j = 0, len = (int)(get_len(vc)); j < len; j++) {
        clk* c =  get_clock_unit_by_index(&vc, (char)j);
        char gid = get_gid(*c);
        if(!findRcTombstoneElement(t, gid)) {
            rc_tombstone_element* el = createRcTombstoneElement(gid);
            el->del_unit = get_logic_clock(*c);
            appendRcTombstoneElement(t, el);
        }
    }
    updateRcTombstoneLastVc(tombstone, vc);
    return 1;
}

int crdtRcTombstonePurge(CRDT_RCTombstone* tombstone, CRDT_RC* r) {
    crdt_orset_rc* rc = retrieveCrdtRc(r);
    crdt_rc_tombstone* t = retrieveCrdtRcTombstone(tombstone);
    if(isVectorClockMonoIncr(rc->vectorClock, t->vectorClock)) {
        // rc.counter.conv -> tombstone.counter.conv 
        for(int i = 0; i < rc->len; i++) {
            rc_tombstone_element* el = findRcTombstoneElement(t, rc->elements[i]->gid);
            assert(el != NULL);
            if(rc->elements[i]->counter) {
                update_add_counter(el->counter, rc->elements[i]->counter);
            }
        }
        return PURGE_VAL;
    }
    for(int i = 0; i < t->len; i++) {
        rc_element* el = findRcElement(rc, t->elements[i]->gid);
        if(el == NULL) {
            el = createRcElement(t->elements[i]->gid);
            appendRcElement(rc, el);
        }
        if(el->base && t->elements[i]->del_unit >= el->base->unit) {
            freeBase(el->base);
            el->base = NULL;
        }
        if(t->elements[i]->counter != NULL) {
            if(el->counter) {
                update_add_counter(el->counter, t->elements[i]->counter);
                update_del_counter(el->counter, t->elements[i]->counter);
                freeGcounter(t->elements[i]->counter);
            } else {
                el->counter = t->elements[i]->counter;
            }
            t->elements[i] = NULL;
        }
    }
    crdtRcUpdateLastVC(r, t->vectorClock);
    return PURGE_TOMBSTONE;
}
//========================= Rc moduleType functions =======================

#define ADD 1
#define DEL (1<<1)
gcounter* load_counter(RedisModuleIO* rdb) {
    gcounter* counter = createGcounter(0);
    counter->start_clock = RedisModule_LoadSigned(rdb);
    counter->type = RedisModule_LoadSigned(rdb);
    int flags = RedisModule_LoadSigned(rdb);
    if(flags & ADD) {
        counter->end_clock = RedisModule_LoadSigned(rdb);
        if(counter->type == VALUE_TYPE_FLOAT) {
            counter->conv.f = RedisModule_LoadFloat(rdb);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            counter->conv.i = RedisModule_LoadSigned(rdb);
        }  
    }
    if(flags & DEL) {
        counter->del_end_clock = RedisModule_LoadSigned(rdb);
        if(counter->type == VALUE_TYPE_FLOAT) {
            counter->del_conv.f = RedisModule_LoadFloat(rdb);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            counter->del_conv.i = RedisModule_LoadSigned(rdb);
        }  
    }
    return counter;
}

void save_counter(RedisModuleIO* rdb, gcounter* counter) {
    RedisModule_SaveSigned(rdb, counter->start_clock);
    RedisModule_SaveSigned(rdb, counter->type);
    int flags = 0;
    if(counter->end_clock != 0) {flags |= ADD;}
    if(counter->del_end_clock != 0) {flags |= DEL;}
    RedisModule_SaveSigned(rdb, flags);
    if(counter->end_clock != 0) {
        RedisModule_SaveSigned(rdb, counter->end_clock);
        if(counter->type == VALUE_TYPE_FLOAT) {
            RedisModule_SaveFloat(rdb, counter->conv.f);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            RedisModule_SaveSigned(rdb, counter->conv.i);
        }    
    }
    if(counter->del_end_clock != 0) {
        RedisModule_SaveSigned(rdb, counter->del_end_clock);
        if(counter->type == VALUE_TYPE_FLOAT) {
            RedisModule_SaveFloat(rdb, counter->del_conv.f);
        } else if(counter->type == VALUE_TYPE_INTEGER) {
            RedisModule_SaveSigned(rdb, counter->del_conv.i);
        }    
    }
    
}

rc_base* load_base(RedisModuleIO* rdb) {
    rc_base* base = createRcElementBase();
    base->unit = RedisModule_LoadSigned(rdb);
    base->timespace = RedisModule_LoadSigned(rdb);
    base->type = RedisModule_LoadSigned(rdb);
    if(base->type == VALUE_TYPE_FLOAT) {
        base->conv.f = RedisModule_LoadFloat(rdb);
    } else if(base->type == VALUE_TYPE_INTEGER) {
        base->conv.i = RedisModule_LoadSigned(rdb);
    }
    return base;
}

void save_base(RedisModuleIO* rdb, rc_base* base) {
    RedisModule_SaveSigned(rdb, base->unit);
    RedisModule_SaveSigned(rdb, base->timespace);
    RedisModule_SaveSigned(rdb, base->type);
    if(base->type == VALUE_TYPE_FLOAT) {
        RedisModule_SaveFloat(rdb, base->conv.f);
    } else if(base->type == VALUE_TYPE_INTEGER) {
        RedisModule_SaveSigned(rdb, base->conv.i);
    }
}

#define BASE_DATA 1
#define COUNTER_DATA (1<<1)

rc_element* load_rc_element(RedisModuleIO* rdb) {
    rc_element* rc = createRcElement(0);
    rc->gid = RedisModule_LoadSigned(rdb);
    int flags = RedisModule_LoadSigned(rdb);
    if(flags & BASE_DATA) {
        rc->base = load_base(rdb);
    }
    if(flags & COUNTER_DATA) {
        rc->counter = load_counter(rdb);
    }
    return rc;
}

void save_rc_element(RedisModuleIO* rdb, rc_element* el) {
    RedisModule_SaveSigned(rdb, el->gid);
    int flags = 0;
    if(el->base) { flags |= BASE_DATA;}
    if(el->counter) { flags |= COUNTER_DATA;}
    RedisModule_SaveSigned(rdb, flags);
    if(el->base) {save_base(rdb, el->base);}
    if(el->counter) {save_counter(rdb, el->counter);}
}

crdt_orset_rc* RdbLoadCrdtOrSetRc(RedisModuleIO *rdb, long long version, int encver) {
    crdt_orset_rc* rc = retrieveCrdtRc(createCrdtRc());
    rc->vectorClock = rdbLoadVectorClock(rdb, version);
    int len = RedisModule_LoadUnsigned(rdb);
    for(int i = 0; i < len; i++) {
        rc_element* el = load_rc_element(rdb);
        if(el == NULL) { freeCrdtRc(rc); return NULL;}
        appendRcElement(rc, el);
    }
    return rc;
}
void *RdbLoadCrdtRc(RedisModuleIO *rdb, int encver) {
    long long header = loadCrdtRdbHeader(rdb);
    int type = getCrdtRdbType(header);
    int version = getCrdtRdbVersion(header);
    if( type == ORSET_TYPE) {
        return RdbLoadCrdtOrSetRc(rdb, version, encver);
    }
    return NULL;
}



rc_tombstone_element* load_rc_tombstone_element(RedisModuleIO *rdb) {
    rc_tombstone_element* el = createRcTombstoneElement(0);
    el->gid = RedisModule_LoadUnsigned(rdb);
    el->del_unit = RedisModule_LoadUnsigned(rdb);
    int hasCounter = RedisModule_LoadUnsigned(rdb);
    if(hasCounter) {
        el->counter = load_counter(rdb);
    }
    return el;
}

void save_rc_tombstone_element(RedisModuleIO *rdb, rc_tombstone_element* el) {
    RedisModule_SaveUnsigned(rdb, el->gid);
    RedisModule_SaveUnsigned(rdb, el->del_unit);
    if(!el->counter) { 
        RedisModule_SaveUnsigned(rdb, 0);
        return;
    } 
    RedisModule_SaveUnsigned(rdb, 1);
    save_counter(rdb, el->counter);
}

crdt_rc_tombstone* RdbLoadCrdtOrSetRcTombstone(RedisModuleIO *rdb, int version, int encver) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(createCrdtRcTombstone());
    rt->vectorClock = rdbLoadVectorClock(rdb, version);
    int len = RedisModule_LoadUnsigned(rdb);
    for(int i = 0; i < len; i++) {
        rc_tombstone_element* el = load_rc_tombstone_element(rdb);
        if(el == NULL) { freeCrdtRcTombstone(rt); return NULL;}
        appendRcTombstoneElement(rt, el);
    }
    return rt;
}

void RdbSaveCrdtRc(RedisModuleIO *rdb, void *value) {
    crdt_orset_rc* rc = retrieveCrdtRc(value);
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    rdbSaveVectorClock(rdb, rc->vectorClock, CRDT_RDB_VERSION);
    RedisModule_SaveUnsigned(rdb, rc->len);
    for(int i = 0; i < rc->len; i++) {
        save_rc_element(rdb, rc->elements[i]);
    }
}

void RdbSaveCrdtRcTombstone(RedisModuleIO *rdb, void *value) {
    crdt_rc_tombstone* rt = retrieveCrdtRcTombstone(value);
    saveCrdtRdbHeader(rdb, ORSET_TYPE);
    rdbSaveVectorClock(rdb, rt->vectorClock, CRDT_RDB_VERSION);
    RedisModule_SaveUnsigned(rdb, rt->len);
    for(int i = 0; i < rt->len; i++) {
        save_rc_tombstone_element(rdb, rt->elements[i]);
    }
}
//========================= CRDT Data functions =======================
CrdtObject *crdtRcMerge(CrdtObject *currentVal, CrdtObject *value) {
    if(currentVal == NULL && value == NULL) {
        return NULL;
    }
    if(currentVal == NULL) {
        return dupCrdtRc(value);
    }
    if(value == NULL) {
        return dupCrdtRc(currentVal);
    }
    crdt_orset_rc *current = retrieveCrdtRc(currentVal);
    crdt_orset_rc *other = retrieveCrdtRc(value);
    crdt_orset_rc* result = retrieveCrdtRc(dupCrdtRc(currentVal));
    freeVectorClock(result->vectorClock);
    result->vectorClock = vectorClockMerge(current->vectorClock, other->vectorClock);
    for(int i = 0; i < other->len; i++) {
        rc_element* el = findRcElement(current, other->elements[i]->gid);
        if(el == NULL) {
            el = dupRcElement(other->elements[i]);
            appendRcElement(result, el);
        } else {
            assign_max_rc_element(el, other->elements[i]);
        }
    }
    return (CrdtObject*)result;
}

CrdtObject** crdtRcFilter(CrdtObject* target, int gid, long long logic_time, long long maxsize, int* length) {
    crdt_orset_rc* rc = retrieveCrdtRc(target);
    //value + gid + time + vectorClock
    if (crdtRcMemUsageFunc(rc) > maxsize) {
        *length  = -1;
        return NULL;
    }
    VectorClockUnit unit = getVectorClockUnit(rc->vectorClock, gid);
    if(isNullVectorClockUnit(unit)) return NULL;
    long long vcu = get_logic_clock(unit);
    if(vcu > logic_time) {
        *length = 1;
        CrdtObject** re = RedisModule_Alloc(sizeof(crdt_orset_rc*));
        re[0] = target;
        return re;
    }  
    return NULL;
}

void freeRcFilter(CrdtObject** filters, int num) {
    RedisModule_ZFree(filters);
}

//======== info =======
RedisModuleString* element_info(RedisModuleCtx* ctx,rc_element* el) {
    sds base_info = sdsempty();
    sds counter_info = sdsempty();
    if(el->base) {
        if(el->base->type == VALUE_TYPE_FLOAT) {
            base_info = sdscatprintf(base_info, "base: {gid: %d, unit: %lld, time: %lld,value: %Lf}",el->gid, el->base->unit, el->base->timespace, el->base->conv.f);
        } else if(el->base->type == VALUE_TYPE_INTEGER){
            base_info = sdscatprintf(base_info, "base: {gid: %d, unit: %lld, time: %lld,value: %lld}",el->gid, el->base->unit, el->base->timespace, el->base->conv.i);
        }
    }
    if(el->counter) {
        gcounter* g = el->counter;
        if(el->counter->type == VALUE_TYPE_FLOAT) {
            counter_info = sdscatprintf(counter_info, "counter: {start_clock: %lld, end_clock: %lld, value: %Lf, del_clock: %lld, del_value: %Lf}", g->start_clock, g->end_clock, g->conv.f, g->del_end_clock, g->del_conv.f);
        } else {
            counter_info = sdscatprintf(counter_info, "counter: {start_clock: %lld, end_clock: %lld, value: %lld, del_clock: %lld, del_value: %lld}", g->start_clock, g->end_clock, g->conv.i, g->del_end_clock, g->del_conv.i);
        }
    }
    RedisModuleString* result =  RedisModule_CreateStringPrintf(ctx, "gid: %d, %s %s", el->gid, base_info, counter_info);
    sdsfree(base_info);
    sdsfree(counter_info);
    return result;
}