#ifndef __REDIS_SWAP_H
#define __REDIS_SWAP_H

#include "include/redismodule.h"
#include "include/rmutil/sds.h"


int initSwap();

/* whole key swap */
void* lookupSwappingClientsWk(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey);
void setupSwappingClientsWk(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey, void *scs);
void getDataSwapsWk(RedisModuleCtx *ctx, RedisModuleString *key, int mode, RedisModuleGetSwapsResult *result);
void *getComplementSwapsWk(RedisModuleCtx *ctx, RedisModuleString *keyobj, RedisModuleGetSwapsResult *result, RedisModuleComplementObjectFunc *comp, void **pd);
int swapAnaWk(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString *subkey, int *action, char **rawkey, char **rawval, RedisModuleSwapFinishedCallback *cb, void **privdata);

/* string */
sds encodeKeyCrdtString(RedisModuleString *keyobj);
sds encodeValCrdtRegister(RedisModuleKey *key);
void *decodeValCrdtRegister(sds rawval);
sds encodeValCrdtRc(RedisModuleKey *key);
void *decodeValCrdtRC(sds rawval);

/* hash */
sds encodeKeyCrdtHash(RedisModuleString *keyobj);
sds encodeValCrdtHash(RedisModuleKey *key);
void *decodeValCrdtHash(sds rawval);

#endif
