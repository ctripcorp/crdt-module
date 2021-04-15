#include "crdt_set.h"
#include "include/rmutil/zmalloc.h"
#include <string.h>

int crdtSetDelete(int dbId, void* keyRobj, void *key, void *value) {
    if(value == NULL) {
        return CRDT_ERROR;
    }
    if(!isCrdtSet(value)) {
        return CRDT_ERROR;
    }
    CrdtMeta* meta = createIncrMeta();
    CRDT_Set* current = (CRDT_Set*) value;
    RedisModuleKey *moduleKey = (RedisModuleKey*) key;
    CRDT_SetTombstone* tombstone = getTombstone(moduleKey);
    if(tombstone == NULL || !isCrdtSetTombstone(tombstone)) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    setDel(current, tombstone, meta);
    sds vcSds = vectorClockToSds(getMetaVectorClock(meta));
    sds maxdelSds = vectorClockToSds(getCrdtSetTombstoneMaxDelVc(tombstone));
    RedisModule_ReplicationFeedAllSlaves(dbId, "CRDT.DEL_Set", "sllcc", keyRobj, getMetaGid(meta), getMetaTimestamp(meta), vcSds, maxdelSds);
    sdsfree(vcSds);
    sdsfree(maxdelSds);
    freeCrdtMeta(meta);
    return CRDT_OK;
}

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

/**================================== READ COMMANDS ==========================================*/

int sismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        RedisModule_ReplyWithLongLong(ctx, 0);
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    sds field = RedisModule_GetSds(argv[2]);
    dictEntry* de = findSetDict(current, field);
    RedisModule_CloseKey(moduleKey);
    if(de != NULL) {
        return RedisModule_ReplyWithLongLong(ctx, 1); 
    }
    return RedisModule_ReplyWithLongLong(ctx, 0); 
}


int scardCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return REDISMODULE_ERR;
        return RedisModule_ReplyWithLongLong(ctx, 0); 
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, getSetSize(current));
}


int smembersCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        RedisModule_ReplyWithArray(ctx, 0);
        return CRDT_ERROR;
    }
    CRDT_Set* current = getCurrentValue(moduleKey);
    size_t length = getSetSize(current);
    RedisModule_ReplyWithArray(ctx, length);
    dictEntry* de = NULL;
    dictIterator* di = getSetIterator(current);
    while((de = dictNext(di)) != NULL) {
        sds field = dictGetKey(de);
        RedisModule_ReplyWithStringBuffer(ctx, field, sdslen(field));
    }
    dictReleaseIterator(di);
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}


void sscanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    list *keys = pd[0];
    sds key = dictGetKey(de);
    listAddNodeTail(keys, sdsdup(key));
}
int sscanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int replyed = 0;
    RedisModuleKey* moduleKey = getRedisModuleKey(ctx, argv[1], CrdtSet, REDISMODULE_READ, &replyed);
    if (moduleKey == NULL) {
        if(replyed) return CRDT_ERROR;
        replyEmptyScan(ctx);
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

    scanGenericCommand(ctx, argv, argc, getSetDict(set), 0, cursor, sscanCallback);

    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return REDISMODULE_OK;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    CRDT_Set *o1 = *(CRDT_Set**)s1, *o2 = *(CRDT_Set**)s2;
    unsigned long first = o1 ? getSetSize(o1) : 0;
    unsigned long second = o2 ? getSetSize(o2) : 0;

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
        int replyed = 0;
        RedisModuleKey* moduleKey = dstkey ?
                        getRedisModuleKey(ctx, setkeys[j], CrdtSet, REDISMODULE_WRITE, NULL) :
                        getRedisModuleKey(ctx, setkeys[j], CrdtSet, REDISMODULE_READ, &replyed);
        if (moduleKey == NULL) {
            if(replyed) {
                zfree(target_sets);
                return CRDT_ERROR;
            }
            target_sets[j] = NULL;
            continue;
        }
        if (RedisModule_ModuleTypeGetType(moduleKey) != CrdtSet) {
            if(replyed) {
                zfree(target_sets);
                return CRDT_ERROR;
            }
            target_sets[j] = NULL;
            continue;
        }
        CRDT_Set *setobj = getCurrentValue(moduleKey);
        if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);

        if (!setobj) {
            target_sets[j] = NULL;
            continue;
        }
        if (setobj->dataType != CRDT_SET_TYPE) {
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

            algo_one_work += getSetSize(target_sets[0]);
            algo_two_work += getSetSize(target_sets[j]);
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

            di = getSetIterator(target_sets[j]);
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
        di = getSetIterator(target_sets[0]);
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

            di = getSetIterator(target_sets[j]);
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
        di = getSetIterator(dstset);
        while((de = dictNext(di)) != NULL) {
            ele = dictGetKey(de);
            RedisModule_ReplyWithStringBuffer(ctx, ele, sdslen(ele));
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
    return 1;
}


int sunionCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);
    sunionDiffGenericCommand(ctx, argv+1, argc-1, NULL, SET_OP_UNION);
    return REDISMODULE_OK;
}


/**================================== WRITE COMMANDS ==========================================*/

int sremCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int result = 0;
    CrdtMeta meta = {.gid=0}; 
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    initIncrMeta(&meta);
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        goto end;
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

    appendVCForMeta(&meta, getCrdtSetLastVc(current));
    for(int i = 2; i < argc; i += 1) {
        sds field = RedisModule_GetSds(argv[i]);
        result += setRem(current, tombstone, field, &meta);
    }
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "srem", argv[1]);
    if(current && getSetSize(current) == 0) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "del", argv[1]);
        RedisModule_DeleteKey(moduleKey);
    } else {
        updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, &meta);
    char buf[256];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Srem", "sllcv", argv[1], getMetaGid(&meta),getMetaTimestamp(&meta), buf, (void *) (argv + 2), (size_t)(argc-2));
end:
    if(meta.gid != 0) freeIncrMeta(&meta);
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    return RedisModule_ReplyWithLongLong(ctx, result);
}


//spop key
int spopCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2) return RedisModule_WrongArity(ctx);
    long long num = 0;
    if (argc == 2) {
        num = 1;
    } else if(argc == 3) {
        if ((RedisModule_StringToLongLong(argv[2],&num) != REDISMODULE_OK)) {
            return RedisModule_ReplyWithError(ctx, "ERR value is not an integer or out of range");
        }
    } else {
        return RedisModule_ReplyWithError(ctx, "ERR syntax error");
    }
    CrdtMeta meta = {.gid=0}; 
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
    if (moduleKey == NULL) {
        return CRDT_ERROR;
    }
    
    CRDT_Set* current = getCurrentValue(moduleKey);
    if(current == NULL) {
        RedisModule_CloseKey(moduleKey);
        RedisModule_ReplyWithNull(ctx);
        return CRDT_OK;
    }
    initIncrMeta(&meta);
    CrdtTombstone* t = getTombstone(moduleKey);
    CRDT_SetTombstone* tombstone = NULL;
    if(t != NULL && isCrdtSetTombstone(t)) {
        tombstone = retrieveCrdtSetTombstone(t);
    }
    if(tombstone == NULL) {
        tombstone = createCrdtSetTombstone();
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    int keylen = getSetSize(current);
    num = min(keylen, num);
    sds fields[num];
    appendVCForMeta(&meta, getCrdtSetLastVc(current));
    if(num < keylen) {
        for(int i = 0; i < num; i += 1) {
            sds field = sdsdup(getRandomSetKey(current));
            // dictEntry* de = findSetDict(current, field);
            // removeSetDict(current, field, &meta);
            // addSetTombstoneDictValue(tombstone, field, &meta);
            setRem(current, tombstone, field, &meta);
            fields[i] = field;
        }
        updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    } else {
        dictIterator* di = getSetIterator(current);
        int i = 0;
        dictEntry* de = NULL;
        while((de = dictNext(di)) != NULL) {
            sds field = sdsdup(dictGetKey(de));
            fields[i++] = field;
            // addSetTombstoneDictValue(tombstone, field, &meta);
            setRem(current, tombstone, field, &meta);
        }
        dictReleaseIterator(di);
        RedisModule_DeleteKey(moduleKey);
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "del", argv[1]);
    }
    updateCrdtSetTombstoneLastVcByMeta(tombstone, &meta);
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, "srem", argv[1]);
    char buf[256];
    vectorClockToString(buf, getMetaVectorClock(&meta));
    RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Srem", "sllca", argv[1], getMetaGid(&meta),getMetaTimestamp(&meta), buf, fields, num);
    if(num == 1) {
        RedisModule_ReplyWithStringBuffer(ctx, fields[0], sdslen(fields[0]));
        sdsfree(fields[0]);
    } else {
        RedisModule_ReplyWithArray(ctx, num);
        for(int i = 0; i < num; i++) {
            sds field = fields[i];
            RedisModule_ReplyWithStringBuffer(ctx, field, sdslen(field));
            sdsfree(field);
        }
    }
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    if(meta.gid != 0) freeIncrMeta(&meta);
    return CRDT_OK;
}

//crdt.sadd key gid timespace vc field....
const char* crdt_sadd_head = "$9\r\nCRDT.SADD\r\n";
size_t crdt_sadd_head_len = 0;
int replicationFeedCrdtSaddCommand(RedisModuleCtx* ctx, char* cmdbuf, sds key, CrdtMeta* meta, sds* fields, size_t* field_lens, int fields_len) {
    size_t cmdlen = 0;
    if(crdt_sadd_head_len == 0) crdt_sadd_head_len = strlen(crdt_sadd_head);
    cmdlen += feedArgc(cmdbuf, fields_len + 5);// 5 = crdt.sadd(1) + key(1) + gid(1) + timespace(1) + vc(1)
    cmdlen +=  feedBuf(cmdbuf + cmdlen, crdt_sadd_head, crdt_sadd_head_len); //crdt.sadd
    cmdlen +=  feedStr2Buf(cmdbuf + cmdlen, key, sdslen(key)); //key
    cmdlen +=  feedMeta2Buf(cmdbuf + cmdlen, getMetaGid(meta), getMetaTimestamp(meta), getMetaVectorClock(meta));
    for (int i = 0; i < fields_len; i++) {
        cmdlen +=  feedStr2Buf(cmdbuf + cmdlen, fields[i], field_lens[i]);
    }
    RedisModule_ReplicationFeedStringToAllSlaves(RedisModule_GetSelectedDb(ctx), cmdbuf, cmdlen);
    return cmdlen;
}

int sendCrdtSaddCommand(struct RedisModuleCtx* ctx, CrdtMeta* meta, RedisModuleString* module_key, RedisModuleString** module_fields, int fields_len ) {
    //char buf[256];
    //vectorClockToString(buf, getMetaVectorClock(&meta));
    //RedisModule_ReplicationFeedAllSlaves(RedisModule_GetSelectedDb(ctx), "CRDT.Sadd", "sllcv", argv[1], getMetaGid(&meta), getMetaTimestamp(&meta), buf, (void *) (argv + 2), (size_t)(argc-2));
    sds key = RedisModule_GetSds(module_key);
    if(crdt_sadd_head_len == 0) crdt_sadd_head_len = strlen(crdt_sadd_head);
    //strlen(crdt_sadd_head) = 15 beca
    size_t bytes_len =  REPLICATION_MAX_LONGLONG_LEN + //args   *<n>\r\n  1 + 21 + 2
                    crdt_sadd_head_len + REPLICATION_MAX_STR_LEN + //head
                    sdslen(key) + REPLICATION_MAX_STR_LEN +  //key
                    REPLICATION_MAX_GID_LEN + REPLICATION_MAX_LONGLONG_LEN + REPLICATION_MAX_VC_LEN; //meta
    sds fields[fields_len];
    size_t field_lens[fields_len];
    for(int i = 0; i < fields_len; i++) {
        fields[i] = RedisModule_GetSds(module_fields[i]);
        field_lens[i] = sdslen(fields[i]);
        bytes_len += field_lens[i] + REPLICATION_MAX_STR_LEN;
    }
    if(bytes_len > MAXSTACKSIZE) {
        char* cmdbuf = RedisModule_Alloc(bytes_len);
        int size =replicationFeedCrdtSaddCommand(ctx, cmdbuf, key, meta, fields, field_lens, fields_len);
        assert(size < bytes_len);
        RedisModule_Free(cmdbuf);
    } else {
        char cmdbuf[bytes_len];
        int size = replicationFeedCrdtSaddCommand(ctx, cmdbuf, key, meta, fields, field_lens, fields_len);
        assert(size < bytes_len);
    }
    return 1;
}

//sadd key <field> <field1> ...
//crdt.sadd <key> gid vc <field1> <field1>
int saddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) return RedisModule_WrongArity(ctx);
    int result = 0;
    CrdtMeta meta = {.gid=0};
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
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
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSet, current);
        if(tombstone) {
            appendVCForMeta(&meta, getCrdtSetTombstoneLastVc(tombstone));
        }
    } else {
        appendVCForMeta(&meta, getCrdtSetLastVc(current));
    }
   
    for(int i = 2; i < argc; i += 1) {
        sds field = RedisModule_GetSds(argv[i]);
        result += setAdd(current, tombstone, field, &meta);
    }
    if(tombstone) {
        if(isNullSetTombstone(tombstone)) {
            RedisModule_DeleteTombstone(moduleKey);
        } else {
            setTombstoneTryResizeDict(tombstone);
        }
        
    }
    updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
    RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "sadd", argv[1]);
    sendCrdtSaddCommand(ctx, &meta, argv[1], argv + 2, argc - 2);
    if(moduleKey != NULL ) RedisModule_CloseKey(moduleKey);
    if(meta.gid != 0) freeIncrMeta(&meta);
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
        if(tombstone) {
            updateCrdtSetLastVc(current, getCrdtSetTombstoneLastVc(tombstone));
        }
        RedisModule_ModuleTypeSetValue(moduleKey, CrdtSet, current);
    } 
    int result = 0;
    for(int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        result += setTryAdd(current, tombstone, field, &meta);
    }
    if(tombstone) {
        if(isNullSetTombstone(tombstone)) {
            RedisModule_DeleteTombstone(moduleKey);
        } else {
            setTombstoneTryResizeDict(tombstone);
        }
    }
    updateCrdtSetLastVcuByVectorClock(current, getMetaGid(&meta), getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    if(result) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "sadd", argv[1]);
    }
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
        if(current) {
            updateCrdtSetTombstoneLastVc(tombstone, getCrdtSetLastVc(current));
        }
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }
    int result = 0;
    for(int i = 5; i < argc; i++) {
        sds field = RedisModule_GetSds(argv[i]);
        // dictEntry* de = findSetDict(current, field);
        result += setTryRem(current, tombstone, field, &meta);
    }
    if(current && result) {
        if(getSetSize(current) == 0) {
            RedisModule_DeleteKey(moduleKey);
            RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "del", argv[1]);
            current = NULL;
        } else {
            updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
            setTryResizeDict(current);
        }
    }
    updateCrdtSetTombstoneLastVc(tombstone, getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));
    if(result) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "srem", argv[1]);
    }
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



int crdtSismemberCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey* moduleKey = getWriteRedisModuleKey(ctx, argv[1], CrdtSet);
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
    if(tombstone != NULL && isCrdtSetTombstone(tombstone)) {
        tde = findSetTombstoneDict(tombstone, field);
        if(tde != NULL)  {
            num += 1;
        }
    }
    
    if(num == 0) {
        RedisModule_ReplyWithNull(ctx);
        goto end;
    }

    RedisModule_ReplyWithArray(ctx, num);
    if(de != NULL) {
        sds info = setIterInfo(de);
        RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
    }
    if(tde != NULL) {
        sds info = setTombstoneIterInfo(tde);
        RedisModule_ReplyWithStringBuffer(ctx, info, sdslen(info));
        sdsfree(info);
    }
end:
    if(moduleKey != NULL) RedisModule_CloseKey(moduleKey);
    return CRDT_OK;
}


//crdt.del_Set <key> gid time vc maxvc
int crdtDelSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
        if(current) {
            updateCrdtSetTombstoneLastVc(tombstone, getCrdtSetLastVc(current));
        }
        RedisModule_ModuleTombstoneSetValue(moduleKey, CrdtSetTombstone, tombstone);
    }

    if(setTryDel(current, tombstone, &meta)) {
        RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "srem", argv[1]);
        if(current) {
            if(getSetSize(current) == 0) {
                RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_SET, "del", argv[1]);
                RedisModule_DeleteKey(moduleKey);
                current = NULL;
            } else {
                updateCrdtSetLastVc(current, getMetaVectorClock(&meta));
                setTryResizeDict(current);
            }
        }
    }
    updateCrdtSetTombstoneLastVc(tombstone, getMetaVectorClock(&meta));
    RedisModule_MergeVectorClock(getMetaGid(&meta), getMetaVectorClockToLongLong(&meta));

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
                                  crdtSaddCommand,"write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "sismember", sismemberCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "srem", 
                                sremCommand, "write deny-oom", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx,"crdt.srem",
                                  crdtSremCommand,"write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR; 
    if (RedisModule_CreateCommand(ctx, "scard",
                                scardCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx, "smembers",
                                smembersCommand, "readonly", 1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"sunion",
                                  sunionCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"sscan",
                                  sscanCommand,"readonly random",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "crdt.sismember",
                                crdtSismemberCommand, "readonly fast", 1,1,1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;   
    if (RedisModule_CreateCommand(ctx,"crdt.del_set",
                                  crdtDelSetCommand,"write deny-oom allow-loading",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;    
    if (RedisModule_CreateCommand(ctx,"spop",
                                  spopCommand,"write deny-oom random fast",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

RedisModuleType* getCrdtSet() {
    return CrdtSet;
}

RedisModuleType* getCrdtSetTombstone() {
    return CrdtSetTombstone;
}

VectorClock clone_st_vc(void* st) {
    return dupVectorClock(getCrdtSetTombstoneLastVc(st));
}