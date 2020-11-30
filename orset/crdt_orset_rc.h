
#ifndef XREDIS_ORSET_RC_H
#define XREDIS_ORSET_RC_H
#include "../ctrip_vector_clock.h"
#include "../ctrip_crdt_register.h"
#include "../crdt.h"
#include "../include/redismodule.h"
// #include "../gcounter/g_counter_element.h"

#if defined(TCL_TEST)
    typedef TestElement crdt_orset_rc;
#else
    typedef struct crdt_orset_rc {
        unsigned long long type:8;
        unsigned long long len:4; 
        unsigned long long tags:52; //crdt_tag** 
    } __attribute__ ((packed, aligned(1))) crdt_orset_rc; 

#endif

typedef  crdt_orset_rc crdt_rc_tombstone;
int rc_gc_stats = 1;
//========================= rc element functions =========================

#endif //XREDIS_ORSET_RC_H