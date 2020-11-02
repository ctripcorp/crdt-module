#include "./ctrip_vector_clock.h"
#include "./include/redismodule.h"
#include "./ctrip_crdt_common.h"
#include "./ctrip_zskiplist.h"



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
} crdt_zset_tombstone;

// moduleType
static RedisModuleType *CrdtSS;
static RedisModuleType *CrdtSST;
RedisModuleType* getCrdtSS();
RedisModuleType* getCrdtSST();


//  init redis module
int initCrdtSSModule(RedisModuleCtx *ctx);
CRDT_SS* create_crdt_zset();
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
int zsetAdd(CRDT_SS* ss, CRDT_SSTombstone* sst, CrdtMeta* meta, sds field, double sorted);
size_t getZSetSize(CRDT_SS* ss);
zskiplist* getZSetSkipList(CRDT_SS* ss);
int incrTagCounter(CRDT_SS* current, CrdtMeta* zadd_meta, sds field, double score);
double getScore(CRDT_SS* current, sds field);