#include "crdt_set.h"
#include "include/rmutil/zmalloc.h"


int crdtSetDelete(int dbId, void* keyRobj, void *key, void *value) {
    
    if(value == NULL) {
        return CRDT_ERROR;
    }
    if(!isCrdtSet(value)) {
        return CRDT_ERROR;
    }
    CrdtMeta* meta = createIncrMeta();
    CrdtMeta* del_meta = dupMeta(meta);
    CRDT_Set* current = (CRDT_Set*) value;
    appendVCForMeta(del_meta, getCrdtSetLastVc(current));
    RedisModuleKey *moduleKey = (RedisModuleKey*) key;
    CRDT_SetTombstone* tombstone = getTombstone(moduleKey);
    RedisModule_Debug(logLevel, "delete %s", RedisModule_GetSds(keyRobj));
    if(tombstone == NULL || !isCrdtSetTombstone(tombstone)) {
        tombstone = createCrdtSetTombstone();
        RedisModule_Debug(logLevel, "delete tombstone");
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, del_meta);
    updateCrdtSetTombstoneMaxDel(tombstone, getMetaVectorClock(del_meta));
    sds vcSds = vectorClockToSds(getMetaVectorClock(del_meta));
    // sds maxDeleteVectorClock = vectorClockToSds(getCrdtSetLastVc(current));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Set", "sllcc", keyRobj, getMetaGid(meta), getMetaTimestamp(meta), vcSds, vcSds);
    sdsfree(vcSds);
    freeCrdtMeta(meta);
    freeCrdtMeta(del_meta);
    return CRDT_OK;
}

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

/**================================== READ COMMANDS ==========================================*/
int sismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0); 
    } 
    sds field = RedisModule_GetSds(argv[2]);
    dictEntry* de = findSetDict(current, field);
    if(de != NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 1); 
    }
    return RedisModule_ReplyWithLongLong(ctx, 0); 
}


int scardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0); 
    } 
    return RedisModule_ReplyWithLongLong(ctx, getSetDictSize(current));
}

int smembersCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        RedisModule_CloseKey(moduleKey);
        return RedisModule_ReplyWithNull(ctx);
    }
    size_t length = getSetDictSize(current);
    RedisModule_ReplyWithArray(ctx, length);
    dictEntry* de = NULL;
    dictIterator* di = getSetDictIterator(current);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        RedisModule_ReplyWithStringBuffer(ctx, field, sdslen(field));
    }
    dictReleaseIterator(di);
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}

int sscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    CRDT_Set* set = getCurrentValue(moduleKey);
    if(set == NULL) {
        replyEmptyScan(ctx);
        RedisModule_CloseKey(moduleKey);
        return REDISMODULE_OK;
    }
    unsigned long cursor;

    if (parseScanCursorOrReply(ctx, argv[2], &cursor) == CRDT_ERROR) return 0;

    scanGenericCommand(ctx, argv, argc, getSetDict(set), CRDT_SET_TYPE, cursor);

    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    CRDT_Set *o1 = *(CRDT_Set**)s1, *o2 = *(CRDT_Set**)s2;
    unsigned long first = o1 ? getSetDictSize(o1) : 0;
    unsigned long second = o2 ? getSetDictSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

static inline int setTypeAdd(CRDT_Set *dstset, sds ele) {
    dict *ht = getSetDict(dstset);
    dictEntry *de = dictAddRaw(ht, ele,NULL);
    if (de) {
        dictSetKey(ht,de,sdsdup(ele));
        dictSetVal(ht,de,NULL);
        return 1;
    }
    return 0;
}

static inline int setTypeRemove(CRDT_Set *setobj, sds value) {
    dict *ht = getSetDict(setobj);
    if (dictDelete(ht, value) == DICT_OK) {
        if (htNeedsResize(ht)) dictResize(ht);
        return 1;
    }
    return 0;
}

int sunionDiffGenericCommand(RedisModuleCtx *ctx, RedisModuleString **setkeys, int setnum,
                              RedisModuleString *dstkey, int op) {

    CRDT_Set **target_sets = zmalloc(sizeof(CRDT_Set *) * setnum);
    dictIterator *di;
    CRDT_Set *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;

    for (j = 0; j < setnum; j++) {
        RedisModuleKey* moduleKey = dstkey ?
                        getRedisModuleKey(ctx, setkeys[j], CrdtSet, REDISMODULE_WRITE) :
                        getRedisModuleKey(ctx, setkeys[j], CrdtSet, REDISMODULE_READ);
        if (moduleKey == NULL) {
            target_sets[j] = NULL;
            continue;
        }
        CRDT_Set *setobj = getCurrentValue(moduleKey);
        if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);

        if (!setobj) {
            target_sets[j] = NULL;
            continue;
        }
        if (setobj->type != CRDT_SET_TYPE) {
            zfree(target_sets);
            return CRDT_ERROR;
        }
        target_sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
    *
    * Algorithm 1 is O(N*M) where N is the size of the element first set
    * and M the total number of sets.
    *
    * Algorithm 2 is O(N) where N is the total number of elements in all
    * the sets.
    *
    * We compute what is the best bet with the current input here. */
    if (op == SET_OP_DIFF && target_sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (target_sets[j] == NULL) continue;

            algo_one_work += getSetDictSize(target_sets[0]);
            algo_two_work += getSetDictSize(target_sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            qsort(target_sets+1,setnum-1,sizeof(CRDT_Set*),
                  qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
    * is not NULL (that is, we are inside an SUNIONSTORE operation) then
    * this set object will be the resulting object to set into the target key*/
    dstset = createCrdtSet();
    dictEntry* de = NULL;
    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        for (j = 0; j < setnum; j++) {
            if (!target_sets[j]) continue; /* non existing keys are like empty sets */

            di = getSetDictIterator(target_sets[j]);
            while((de = dictNext(di)) != NULL) {
                ele = dictGetKey(de);
                if (setTypeAdd(dstset,ele)) cardinality++;
            }
            dictReleaseIterator(di);
        }
    } else if (op == SET_OP_DIFF && target_sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        di = getSetDictIterator(target_sets[0]);
        while((de = dictNext(di)) != NULL) {
            ele = dictGetKey(de);
            for (j = 1; j < setnum; j++) {
                if (!target_sets[j]) continue; /* no key is an empty set. */
                if (target_sets[j] == target_sets[0]) break; /* same set! */
                if (dictFind(getSetDict(target_sets[j]), ele)) break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
        }
        dictReleaseIterator(di);
    } else if (op == SET_OP_DIFF && target_sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        for (j = 0; j < setnum; j++) {
            if (!target_sets[j]) continue; /* non existing keys are like empty sets */

            di = getSetDictIterator(target_sets[j]);
            while((de = dictNext(di)) != NULL) {
                ele = dictGetKey(de);
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
            }
            dictReleaseIterator(di);
            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        RedisModule_ReplyWithArray(ctx, cardinality);
        di = getSetDictIterator(dstset);
        while((de = dictNext(di)) != NULL) {
            ele = dictGetKey(de);
            RedisModule_ReplyWithStringBuffer(ctx, ele, sdslen(ele));
            sdsfree(ele);
        }
        dictReleaseIterator(di);
        freeCrdtSet(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        //ignore
//        int deleted = dbDelete(c->db,dstkey);
//        if (setTypeSize(dstset) > 0) {
//            dbAdd(c->db,dstkey,dstset);
//            addReplyLongLong(c,setTypeSize(dstset));
//            notifyKeyspaceEvent(NOTIFY_SET,
//                                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
//                                dstkey,c->db->id);
//        } else {
//            decrRefCount(dstset);
//            addReply(c,shared.czero);
//            if (deleted)
//                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
//                                    dstkey,c->db->id);
//        }
//        signalModifiedKey(c->db,dstkey);
//        server.dirty++;
    }
    zfree(target_sets);
}


int sunionCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    sunionDiffGenericCommand(ctx, argv+1, argc-1, NULL, SET_OP_UNION);
    return REDISMODULE_OK;
}


/**================================== WRITE COMMANDS ==========================================*/

int sremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int result = 0;
    CrdtMeta meta = {.gid=0}; 
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    initIncrMeta(&meta);
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    
    for(int i = 2; i < argc; i += 1) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        int r = removeSetDict(current, field, &meta);
        if(r) {
            addSetTombstoneDictValue(tombstone, field, &meta);
            result += 1;
        }
    }
    if(getSetDictSize(current) == 0) {
        RedisModule_DeleteKey(moduleKey);
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, &meta);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "srem", argv[1]);
    char buf[100];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Srem", "sllcv", argv[1], getMetaGid(&meta),getMetaTimestamp(&meta), buf, (void *) (argv + 2), (size_t)(argc-2));
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}

//sadd key <field> <field1> ...
//crdt.sadd <key> gid vc <field1> <field1>
int saddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int result = 0;
    CrdtMeta meta = {.gid=0};
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    initIncrMeta(&meta);
    CRDT_Set* current = getCurrentValue(moduleKey);
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    if(current == NULL) {
        current = createCrdtSet();
        if(tombstone) {
            updateCrdtSetLastVc(current, getCrdtSetTombstoneLastVc(tombstone));
        } else {
            long long vc = RedisModule_CurrentVectorClock();
            updateCrdtSetLastVc(current, LL2VC(vc));
        }
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSet, current);
    } 
    appendVCForMeta(&meta, getCrdtSetLastVc(current));
    for(int i = 2; i < argc; i += 1) {
        sds field = RedisModule_GetSds(argv[i]);
        dictEntry* de = findSetDict(current, field);
        if(de == NULL) {
            addSetDict(current, field, &meta);
            result += 1;
        } else {
            updateSetDict(current, de, &meta);
        }
        if(tombstone) {
            removeSetTombstoneDict(tombstone, field);
        }
    }
    
    setCrdtSetLastVc(current, getMetaVectorClock(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "sadd", argv[1]);
    char buf[100];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Sadd", "sllcv", argv[1], getMetaGid(&meta), getMetaTimestamp(&meta), buf, (void *) (argv + 2), (size_t)(argc-2));
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    return RedisModule_ReplyWithLongLong(ctx, result);
}

/*-----------------------------------------------------------------------------
 * CRDT Set Commands
 *----------------------------------------------------------------------------*/

//crdt.sadd <key> gid time vc <field1> <field1>
int crdtSaddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        current = createCrdtSet();
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSet, current);
    } 
    for(int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        setTombstoneIterPurge(current, tombstone, field, &meta);
    }
    updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "sadd", argv[1]);
    
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

//crdt.srem <key>, <gid>, <timestamp>, <vclockStr> k1,k2
int crdtSremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    for(int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        setValueIterPurge(current, tombstone, field, &meta);
    }
    if(current) {
        if(getSetDictSize(current) == 0) {
            RedisModule_DeleteKey(moduleKey);
            current = NULL;
        } else {
            updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
        }
    }
    updateCrdtSetTombstoneLastVc(tombstone, getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "srem", argv[1]);
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    // sds cmdname = RedisModule_GetSds(argv[0]);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }  
}


int crdtSismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_WRITE);
    if (moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    CRDT_SetTombstone* tombstone =  getTombstone(moduleKey);
    sds field = RedisModule_GetSds(argv[2]);
    dictEntry* de = NULL;
    dictEntry* tde = NULL;
    int num = 0;
    if(current != NULL) {
        de = findSetDict(current, field);
        if(de != NULL)  {
            num += 1;
        }
    } 
    if(tombstone != NULL) {
        tde = findSetTombstoneDict(tombstone, field);
        if(tde != NULL)  {
            num += 1;
        }
    }
    
    if(num == 0) {
        return RedisModule_ReplyWithNull(ctx);
    }
    RedisModule_Debug(logLevel, "crdtSismember %d", num);
    RedisModule_ReplyWithArray(ctx, num);
    if(de != NULL) {
        void* data = dictGetVal(de);
        sds info = setIterInfo(data);
        RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
    }
    if(tde != NULL) {
        void* data = dictGetVal(tde);
        sds info = setTombstoneIterInfo(data);
        RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
    }

    
     
}
//crdt.del_Set <key> gid time vc maxvc
int crdtDelSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_Debug(logLevel, "crdt.del");
    if (argc < 6) return RedisModule_WrongArity(ctx);
    CrdtMeta meta = {.gid = 0};
    int status = CRDT_OK;
    if (readMeta(ctx, argv, 2, &meta) != CRDT_OK) {
        return 0;
    }
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if(moduleKey == NULL) {
        RedisModule_IncrCrdtConflict(TYPECONFLICT | MODIFYCONFLICT);
        status = CRDT_ERROR;
        goto end;
    }
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    purgeSetDelMax(current, tombstone, &meta);
    if(getSetDictSize(current) == 0) {
        RedisModule_DeleteKey(moduleKey);
    }
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "srem", argv[1]);
end:
    if (meta.gid != 0) {
        RedisModule_CrdtReplicateVerbatim(getMetaGid(&meta), ctx);
        freeVectorClock(meta.vectorClock);
    }
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(status == CRDT_OK) {
        return RedisModule_ReplyWithOk(ctx); 
    }else{
        return CRDT_ERROR;
    }
}

//hash common methods
CrdtObject *crdtSetMerge(CrdtObject *currentVal, CrdtObject *value) {
    CRDT_Set* target = retrieveCrdtSet(currentVal);
    CRDT_Set* other = retrieveCrdtSet(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    if (target == NULL) {
        return (CrdtObject*)dupCrdtSet(other);
    }
    
    CRDT_Set *result = dupCrdtSet(target);
    appendSet(result, other);
    updateCrdtSetLastVc(result, getCrdtSetLastVc(other));
    return (CrdtObject*)result;
}
CrdtTombstone* crdtSetTombstoneMerge(CrdtTombstone* currentVal, CrdtTombstone* value) {
    CRDT_SetTombstone* target = retrieveCrdtSetTombstone(currentVal);
    CRDT_SetTombstone* other = retrieveCrdtSetTombstone(value);
    if(target == NULL && other == NULL) {
        return NULL;
    }
    RedisModule_Debug(logLevel, "tombstone merge start");
    if (target == NULL) {
        return (CrdtObject*)dupCrdtSetTombstone(other);
    }
     RedisModule_Debug(logLevel, "tombstone merge end");
    CRDT_SetTombstone *result = dupCrdtSetTombstone(target);
    appendSetTombstone(result, other);
    updateCrdtSetTombstoneLastVc(result, getCrdtSetTombstoneLastVc(other));
    updateCrdtSetTombstoneMaxDel(result, getCrdtSetTombstoneMaxDelVc(other));
    
    return (CrdtObject*)result;
}
int initCrdtSetModule(RedisModuleCtx *ctx) {
    //hash object type
    RedisModuleTypeMethods valueTypeMethods = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtSet,
            .rdb_save = RdbSaveCrdtSet,
            .aof_rewrite = AofRewriteCrdtSet,
            .mem_usage = crdtSetMemUsageFunc,
            .free = freeCrdtSet,
            .digest = crdtSetDigestFunc
    };
    CrdtSet = RedisModule_CreateDataType(ctx, CRDT_SET_DATATYPE_NAME, 0, &valueTypeMethods);
    if (CrdtSet == NULL) return REDISMODULE_ERR;
    //set tombstone type
    RedisModuleTypeMethods tombstoneTypeMethods = {
            .version = REDISMODULE_APIVER_1,
            .rdb_load = RdbLoadCrdtSetTombstone,
            .rdb_save = RdbSaveCrdtSetTombstone,
            .aof_rewrite = AofRewriteCrdtSetTombstone,
            .mem_usage = crdtSetTombstoneMemUsageFunc,
            .free = freeCrdtSetTombstone,
            .digest = crdtSetTombstoneDigestFunc
    };
    CrdtSetTombstone = RedisModule_CreateDataType(ctx, CRDT_SET_TOMBSTONE_DATATYPE_NAME, 0, &tombstoneTypeMethods);
    if (CrdtSetTombstone == NULL) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"sadd",
                                  saddCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"crdt.sadd",
                                  crdtSaddCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "sismember", sismemberCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "srem", 
                                sremCommand, "write deny-oom", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx,"crdt.srem",
                                  crdtSremCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR; 
    if (RedisModule_CreateCommand(ctx, "scard",
                                scardCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx, "smembers",
                                smembersCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"sunion",
                                  sunionCommand,"readonly random",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"sscan",
                                  sscanCommand,"readonly random",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "crdt.sismember",
                                crdtSismemberCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx,"crdt.del_set",
                                  crdtDelSetCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;               
    return REDISMODULE_OK;
}
RedisModuleType* getCrdtSet() {
    return CrdtSet;
}
RedisModuleType* getCrdtSetTombstone() {
    return CrdtSetTombstone;
}