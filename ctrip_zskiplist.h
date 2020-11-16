
#include "include/rmutil/ziplist.h"
#include "include/rmutil/zskiplist.h"
#include "include/rmutil/sds.h"
#include "include/redismodule.h"
#include "include/rmutil/util.h"
#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */
typedef struct zskiplistNode {
    sds ele;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned int span;
    } level[];
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

typedef struct {
    sds min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

struct sharedZsetStruct {
    sds minstring, maxstring;
};


struct sharedZsetStruct zset_shared;  
void initZsetShard();

zskiplist *zslCreate(void);
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele);
void zslFreeNode(zskiplistNode *node);
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node);
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update);
int zslParseRange(sds min, sds max, zrangespec *spec);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range);
unsigned long zsetGetRank(zskiplist* zsl, double score, sds ele);
int zslParseLexRange(sds min, sds max, zlexrangespec *spec);
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range);
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range);
void zslFreeLexRange(zlexrangespec *spec);

int zslValueGteMin(double value, zrangespec *spec);
int zslValueLteMax(double value, zrangespec *spec);

int zslLexValueLteMax(sds value, zlexrangespec *spec);
int zslLexValueGteMin(sds value, zlexrangespec *spec);
