#include "./ctrip_vector_clock.h"
#include "./include/redismodule.h"
#include "./ctrip_crdt_common.h"
#include "./ctrip_zskiplist.h"
#include "./gcounter/crdt_g_counter.h"
#include "./include/rmutil/sds.h"

/* Input flags. */
#define ZADD_NONE 0
#define ZADD_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_XX (1<<2)      /* Only touch elements already exisitng. */

/* Output flags. */
#define ZADD_NOP (1<<3)     /* Operation not performed because of conditionals.*/
#define ZADD_NAN (1<<4)     /* Only touch elements already exisitng. */
#define ZADD_ADDED (1<<5)   /* The element was new and was added. */
#define ZADD_UPDATED (1<<6) /* The element already existed, score updated. */

/* Flags only used by the ZADD command but not by zsetAdd() API: */
#define ZADD_CH (1<<16)      /* Return num of elements added or updated. */



#define CRDT_SS_DATATYPE_NAME "crdt_ss_v"
#define CRDT_SS_TOMBSTONE_DATATYPE_NAME "crdt_ss_t"
typedef CrdtObject CRDT_SS;
typedef CrdtTombstone CRDT_SSTombstone;
/* ZSETs use a specialized version of Skiplists */


struct crdt_sorted_set {
    char type;
    dict* dict;
    struct zskiplist* zsl;
} crdt_sorted_set;


//crdtObject
typedef struct crdt_zset
{
    char type; // data + zset 
    dict* dict;// Map<string, crdt_zset_element>
    zskiplist* zsl;
    VectorClock lastvc; 
} crdt_zset;

typedef struct crdt_zset_tombstone {
    char type;//  tombstone + zset 
    dict* dict;
    VectorClock lastvc;
    VectorClock maxdelvc;
} crdt_zset_tombstone;



CrdtTombstone* crdtSSTMerge(CrdtTombstone* target, CrdtTombstone* src);
CrdtTombstone** crdtSSTFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) ;
void freeSSTFilter(CrdtTombstone** filters, int num);
int crdtZsetTombstonePurge(CrdtTombstone* tombstone, CrdtData* r);
sds crdtZsetTombstoneInfo(void* tombstone);
int crdtZsetTombstoneGc(CrdtTombstone* target, VectorClock clock);
static CrdtTombstoneMethod ZsetTombstoneCommonMethod = {
    .merge = crdtSSTMerge,
    .filterAndSplit =  crdtSSTFilter,
    .freefilter = freeSSTFilter,
    .gc = crdtZsetTombstoneGc,
    .purge = crdtZsetTombstonePurge,
    .info = crdtZsetTombstoneInfo,
};
//about data method
int crdtZSetDelete(int dbId, void* keyRobj, void *key, void *value);
sds crdtZSetInfo(void *data);
static CrdtDataMethod ZSetDataMethod = {
    .propagateDel = crdtZSetDelete,
    .info = crdtZSetInfo,
};


CrdtObject *crdtSSMerge(CrdtObject *currentVal, CrdtObject *value);
CrdtObject** crdtSSFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* length);
void freeSSFilter(CrdtObject** filters, int num);    
static CrdtObjectMethod ZSetCommandMethod = {
    .merge = crdtSSMerge,
    .filterAndSplit = crdtSSFilter,
    .freefilter = freeSSFilter,
};
// moduleType
static RedisModuleType *CrdtSS;
static RedisModuleType *CrdtSST;
RedisModuleType* getCrdtSS();
RedisModuleType* getCrdtSST();


//  init redis module
int initCrdtSSModule(RedisModuleCtx *ctx);
CRDT_SS* create_crdt_zset();
CRDT_SSTombstone* create_crdt_zset_tombstone();
// ===== sorted set ========
void *RdbLoadCrdtSS(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtSS(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtSS(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtSSMemUsageFunc(const void *value);
void freeCrdtSS(void* ss);

void crdtSSDigestFunc(RedisModuleDigest *md, void *value);
// ====== sorted set tombstone ========
void *RdbLoadCrdtSST(RedisModuleIO *rdb, int encver);
void RdbSaveCrdtSST(RedisModuleIO *rdb, void *value);
void AofRewriteCrdtSST(RedisModuleIO *aof, RedisModuleString *key, void *value);
size_t crdtSSTMemUsageFunc(const void *value);
void freeCrdtSST(void* ss);
void crdtSSTDigestFunc(RedisModuleDigest *md, void *value);
// functions
int zsetAdd(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, int* flags, double score, double* newscore, sds* callback_items, int* callback_len, int* callback_byte_size);
double zsetIncr(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double score);
sds zsetDel(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, int* stats);
size_t getZSetSize(CRDT_SS* ss);
size_t getZsetTombstoneSize(CRDT_SSTombstone* sst);
zskiplist* getZSetSkipList(CRDT_SS* ss);
long zsetRank(CRDT_SS* ss, sds ele, int reverse);
int incrTagCounter(CRDT_SS* current, CrdtMeta* zadd_meta, sds field, double score);
double getScore(CRDT_SS* current, sds field);
zskiplistNode* zset_get_zsl_element_by_rank(CRDT_SS* current, int reverse, long start);
VectorClock getCrdtSSLastVc(CRDT_SS* data);
void updateCrdtSSLastVc(CRDT_SS* data, VectorClock vc);
VectorClock getCrdtSSTLastVc(CRDT_SSTombstone* data);
void updateCrdtSSTMaxDel(CRDT_SSTombstone* tombstone, VectorClock vc);
zskiplistNode* zslInRange(CRDT_SS* current, zrangespec* range, int reverse);
zskiplistNode* zslInLexRange(CRDT_SS* current, zrangespec* range, int reverse);
int initSSTombstoneFromSS(CRDT_SSTombstone* tombstone,CrdtMeta* del_meta, CRDT_SS* value, sds* del_counters);
zskiplist* zsetGetZsl(CRDT_SS* current);
int zsetTryAdd(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info);
int zsetTryIncrby(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info);
int zsetTryRem(CRDT_SSTombstone* tombstone,CRDT_SS* current, sds info, CrdtMeta* meta);
int zsetTryDel(CRDT_SS* current,CRDT_SSTombstone* tombstone, CrdtMeta* meta);
void updateCrdtSSTLastVc(CRDT_SSTombstone* data, VectorClock vc);