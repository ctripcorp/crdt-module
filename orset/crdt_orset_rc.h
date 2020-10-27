#include "../gcounter/crdt_g_counter.h"
#include "../ctrip_vector_clock.h"
#include "../ctrip_crdt_register.h"
#include "../crdt.h"
#include "../include/redismodule.h"
typedef struct {
    unsigned char type;
    long long unit;
    long long timespace;
    union {
        long long i;
        long double f;
    }conv;
} rc_base;

typedef struct {
    char gid; //tag
//    unsigned char flag; //COUNTER, LWW-ELEMENT
    rc_base *base;
    gcounter *counter;
//    void *del_counter;
}rc_element;

typedef struct crdt_orset_rc
{
    unsigned char type;
    unsigned char len;
    VectorClock vectorClock;
    rc_element** elements;
} crdt_orset_rc;


typedef struct {
    long long gid: 4; //tag
//    unsigned char flag; //COUNTER, LWW-ELEMENT
    long long del_unit: 60;
    gcounter *counter;
//    void *del_counter;
} rc_tombstone_element;
typedef struct {
    unsigned char type;
    unsigned char len;
    VectorClock vectorClock;
    //todo: len + pointer
    rc_tombstone_element** elements;
} crdt_rc_tombstone;


//========================= rc element functions =========================
rc_element* createRcElement(int gid);
rc_element* dupRcElement(rc_element* el);
void assign_max_rc_element(rc_element* target, rc_element* src);
void freeRcElement(void* element);
int setCrdtRcType(CRDT_RC* rc, int type);
rc_element* findRcElement(crdt_orset_rc* rc, int gid);
// gcounter* getRcCounter(CRDT_RC* rc, int gid);
int appendRcElement(crdt_orset_rc* rc, rc_element* element);
//========================= rc base functions ============================
void freeBase(rc_base* base);
rc_base* createRcElementBase();
void assign_max_rc_base(rc_base* target, rc_base* src);
rc_base* dupRcBase(rc_base* base);
int resetElementBase(rc_base* base, CrdtMeta* meta, int val_type, void* v);


// ======================== tombstone elements =============================
rc_tombstone_element* createRcTombstoneElement(int gid);
rc_tombstone_element* dupCrdtRcTombstoneElement(rc_tombstone_element* rt);
void assign_max_rc_tombstone_element(rc_tombstone_element* target, rc_tombstone_element* src);
int appendRcTombstoneElement(crdt_rc_tombstone* rt, rc_tombstone_element* el);
rc_tombstone_element* findRcTombstoneElement(crdt_rc_tombstone* rt, int gid);
