
#include <stdio.h>
#include "../include/rmutil/sds.h"
#include "../include/redismodule.h"
#include "../include/util.h"
#include "./type.h"
#include "../util.h"
#define VALUE_TYPE_NONE 0
#define VALUE_TYPE_LONGLONG 1
#define VALUE_TYPE_DOUBLE  2
#define VALUE_TYPE_SDS 3
#define VALUE_TYPE_LONGDOUBLE 4

typedef struct g_counter_meta {
    unsigned long long gid:4;
    unsigned long long data_type:4;
    unsigned long long vcu: 56;
    union all_type value;
    sds v;
} g_counter_meta;

g_counter_meta* create_g_counter_meta(long long gid, long long vcu);
void free_g_counater_maeta(g_counter_meta* meta);
int g_counter_meta_to_str(g_counter_meta* del, char* buf);
typedef int (*GetGMetaFunc)(void* data, int index, g_counter_meta* value);
int value_to_str(char*buf, int type, union all_type value);
sds value_to_sds(int data_type, union all_type v);
int g_counter_metas_to_str(char* buf, void* data, GetGMetaFunc fun, int size);
int gcounter_meta_set_value(g_counter_meta* meta, int type, void* v, int parse);
int str_to_g_counter_metas(char* buf, int len, g_counter_meta** metas);
sds read_value(char* buf, int len, long long* type, union all_type* value, int* offset);
int str_2_value_and_g_counter_metas(sds info, long long*type, union all_type* value, g_counter_meta** g);