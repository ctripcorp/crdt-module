#include "./ctrip_vector_clock.h"
#include <redismodule.h>
#include "./ctrip_crdt_common.h"
#include <rmutil/zskiplist.h>
#include <rmutil/sds.h>

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
typedef CrdtData CRDT_SS;
typedef CrdtTombstone CRDT_SSTombstone;
/* ZSETs use a specialized version of Skiplists */


struct crdt_sorted_set {
    char type;
    dict* dict;
    struct zskiplist* zsl;
} crdt_sorted_set;


//crdtObject





CrdtTombstone* crdtSSTMerge(CrdtTombstone* target, CrdtTombstone* src);
CrdtTombstone** crdtSSTFilter(CrdtTombstone* target, int gid, long long logic_time, long long maxsize,int* length) ;
CrdtTombstone** crdtSSTFilter2(CrdtTombstone* target, int gid, VectorClock min_vc, long long maxsize,int* length) ;

void freeSSTFilter(CrdtTombstone** filters, int num);
int crdtZsetTombstonePurge(CrdtTombstone* tombstone, CrdtData* r);
sds crdtZsetTombstoneInfo(void* tombstone);
int crdtZsetTombstoneGc(CrdtTombstone* target, VectorClock clock);
VectorClock getCrdtSSTLastVc(CRDT_SSTombstone* data);
VectorClock clone_sst_vc(void* data);
extern CrdtTombstoneMethod ZsetTombstoneCommonMethod;

//about data method
int crdtZSetDelete(int dbId, void* keyRobj, void *key, void *value, long long deltime);
sds crdtZSetInfo(void *data);
extern CrdtDataMethod ZSetDataMethod;


CrdtObject *crdtSSMerge(CrdtObject *currentVal, CrdtObject *value);
CrdtObject** crdtSSFilter(CrdtObject* common, int gid, long long logic_time, long long maxsize, int* length);
CrdtObject** crdtSSFilter2(CrdtObject* common, int gid, VectorClock min_vc, long long maxsize, int* length);
void freeSSFilter(CrdtObject** filters, int num);  
extern CrdtObjectMethod ZSetCommandMethod;
// moduleType

RedisModuleType* getCrdtSS();
RedisModuleType* getCrdtSST();
int zsetStopGc();
int zsetStartGc();


//  init redis module
int initCrdtSSModule(RedisModuleCtx *ctx);
CRDT_SS* createCrdtZset();
CRDT_SSTombstone* createCrdtZsetTombstone();
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
sds zsetAdd(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field, int* flags, double score, double* newscore);
sds zsetAdd2(CRDT_SS* value, CRDT_SSTombstone* tombstone, CrdtMeta* meta, sds field, int* flags, double score,  double* newscore);
double zsetIncr(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double score);
sds zsetRem(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field);
size_t crdtZsetLength(CRDT_SS* ss);
size_t zsetTombstoneLength(CRDT_SSTombstone* sst);
zskiplist* getZSetSkipList(CRDT_SS* ss);
long zsetRank(CRDT_SS* ss, sds ele, int reverse);
int incrTagCounter(CRDT_SS* current, CrdtMeta* zadd_meta, sds field, double score);
int getScore(CRDT_SS* current, sds field, double* score);
zskiplistNode* zset_get_zsl_element_by_rank(CRDT_SS* current, int reverse, long start);
VectorClock getCrdtSSLastVc(CRDT_SS* data);
void updateCrdtSSLastVc(CRDT_SS* data, VectorClock vc);
VectorClock getCrdtSSTLastVc(CRDT_SSTombstone* data);
void updateCrdtSSTMaxDel(CRDT_SSTombstone* tombstone, VectorClock vc);
VectorClock getCrdtSSTMaxDelVc(CRDT_SSTombstone* data);
zskiplistNode* zslInRange(CRDT_SS* current, zrangespec* range, int reverse);
zskiplistNode* zslInLexRange(CRDT_SS* current, zlexrangespec* range, int reverse);
int initSSTombstoneFromSS(CRDT_SSTombstone* tombstone,CrdtMeta* del_meta, CRDT_SS* value, sds* del_counters);
zskiplist* zsetGetZsl(CRDT_SS* current);
int zsetTryAdd(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info);
int zsetTryIncrby(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field, CrdtMeta* meta, sds info);
int zsetTryRem(CRDT_SSTombstone* tombstone,CRDT_SS* current, sds info, CrdtMeta* meta);
int zsetTryDel(CRDT_SS* current,CRDT_SSTombstone* tombstone, CrdtMeta* meta);
unsigned long  zslDeleteRangeByRank(CRDT_SS* current, CRDT_SSTombstone* tombstone, CrdtMeta* meta, unsigned int start, unsigned int end, sds* callback_items, long long* byte_size);
unsigned long  zslDeleteRangeByScore(CRDT_SS* current, CRDT_SSTombstone* tombstone, CrdtMeta* meta, zrangespec *range, sds* callback_items, long long* byte_size);
unsigned long  zslDeleteRangeByLex(CRDT_SS* current, CRDT_SSTombstone* tombstone, CrdtMeta* meta, zlexrangespec *range, sds* callback_items, long long* byte_size);
void updateCrdtSSTLastVc(CRDT_SSTombstone* data, VectorClock vc);
unsigned long zsetGetRank(CRDT_SS* current, double score, sds ele);
dict* getZsetDict(CRDT_SS* current);
sds getZsetElementInfo(CRDT_SS* current, CRDT_SSTombstone* tombstone, sds field);
int isNullZsetTombstone(CRDT_SSTombstone* tombstone);
double getZScoreByDictEntry(dictEntry* de);
void zsetTombstoneTryResizeDict(CRDT_SSTombstone* tombstone);
void zsetTryResizeDict(CRDT_SS* current);

