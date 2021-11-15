#ifndef __REDIS_SWAP_H
#define __REDIS_SWAP_H

#include "include/redismodule.h"


void* lookupSwappingClientsString(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey);
void setupSwappingClientsString(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey, void *scs);
void getDataSwapsString(RedisModuleCtx *ctx, RedisModuleString *key, int mode, RedisModuleGetSwapsResult *result);
void *getComplementSwapsString(RedisModuleCtx *ctx, RedisModuleString *keyobj, RedisModuleGetSwapsResult *result, RedisModuleComplementObjectFunc *comp, void **pd);
int swapAnaString(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey, int *action, char **rawkey, char **rawval, RedisModuleSwapFinishedCallback *cb, void **privdata);

#endif
