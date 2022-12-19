#ifndef __G_COUNTER_H__
#define __G_COUNTER_H__
#include <stdio.h>

#include "type.h"
#include "../util.h"


#ifdef COUNTER_TEST_MAIN
#include <stdio.h>
#include <stdlib.h> 
//main
#define counter_malloc(size) malloc(size)
#define counter_calloc(count) calloc(count, 1)
#define counter_realloc(ptr, size) realloc(ptr, size)
#define counter_free(ptr) free(ptr)
#else
//build module so
#include <redismodule.h>
#define counter_malloc(size) RedisModule_Alloc(size)
#define counter_calloc(count) RedisModule_Calloc(count, 1)
#define counter_realloc(ptr, size) RedisModule_Realloc(ptr, size)
#define counter_free(ptr) RedisModule_Free(ptr)
#endif
#include <rmutil/sds.h>
typedef struct g_counter_meta {
    unsigned long long gid:4;
    unsigned long long data_type:4;
    unsigned long long vcu: 56;
    union all_type value;
    sds v;
} g_counter_meta;

g_counter_meta* create_g_counter_meta(long long gid, long long vcu);
void free_g_counter_meta(g_counter_meta* meta);
int g_counter_meta_to_str(g_counter_meta* del, char* buf);
typedef int (*GetGMetaFunc)(void* data, int index, g_counter_meta* value);
int get_value_max_len(int type, union all_type value);
int value_to_str(char*buf, int type, union all_type value);
sds value_to_sds(int data_type, union all_type v);
int g_counter_metas_to_str(char* buf, void* data, GetGMetaFunc fun, int size);
int gcounter_meta_set_value(g_counter_meta* meta, int type, void* v, int parse);
int str_to_g_counter_metas(char* buf, int len, g_counter_meta** metas);
sds read_value(char* buf, int len, ctrip_value* value, int* offset);
int str_2_value_and_g_counter_metas(sds info, ctrip_value* value, g_counter_meta** g);

#endif /* __G_COUNTER_H__ */