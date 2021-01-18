//
// Created by zhuchen on 2019-04-25.
//

#ifndef XREDIS_CRDT_UTILS_H
#define XREDIS_CRDT_UTILS_H

long long mstime();

// sds moduleString2Sds(RedisModuleString *argv);
void _redisAssert(char *estr, char *file, int line);
#define redisAssert(_e) ((_e)?(void)0 : (_redisAssert(#_e,__FILE__,__LINE__),_exit(1)))
#endif //XREDIS_CRDT_UTILS_H
