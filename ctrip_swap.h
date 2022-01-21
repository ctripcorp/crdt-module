#ifndef __REDIS_SWAP_H
#define __REDIS_SWAP_H

#include "include/redismodule.h"
#include "include/rmutil/sds.h"

/* whole key swap: string, hash */
void* lookupSwappingClientsWk(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey);
void setupSwappingClientsWk(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey, void *scs);
void getDataSwapsWk(RedisModuleCtx *ctx, RedisModuleString *key, int mode, RedisModuleGetSwapsResult *result);
void *getComplementSwapsWk(RedisModuleCtx *ctx, RedisModuleString *keyobj, int mode, int *type, RedisModuleGetSwapsResult *result, RedisModuleComplementObjectFunc *comp, void **pd);
int swapAnaWk(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey, int *action, char **rawkey, char **rawval, RedisModuleSwapFinishedCallback *cb, void **privdata);

#endif
