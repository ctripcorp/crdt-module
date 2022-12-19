#ifndef __RMUTIL_ALLOC__
#define __RMUTIL_ALLOC__

#ifdef REDIS_MODULE_TEST /* Set this when compiling your code as a module */
    #define rmutil_alloc(size) malloc(size)
    #define rmutil_calloc(count, size) calloc(count, size)
    #define rmutil_realloc(ptr, size) realloc(ptr, size)
    #define rmutil_free(ptr) free(ptr)
#else
    #include <redismodule.h>
    #define rmutil_alloc(size) RedisModule_Alloc(size)
    #define rmutil_calloc(count, size) RedisModule_Calloc(count, size)
    #define rmutil_realloc(ptr, size) RedisModule_Realloc(ptr, size)
    #define rmutil_free(ptr) RedisModule_Free(ptr)
#endif /* REDIS_MODULE_TARGET */
#endif /* __RMUTIL_ALLOC__ */