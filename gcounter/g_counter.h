#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>


#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <float.h>
#include <stdint.h>
#include <errno.h>
#include "../include/rmutil/sds.h"
#include "../include/redismodule.h"
#include "../include/util.h"
#define VALUE_TYPE_INTEGER 0
#define VALUE_TYPE_FLOAT   1
#define VALUE_TYPE_DOUBLE     2
typedef struct g_counter_meta {
    unsigned long long gid:4;
    unsigned long long data_type:4;
    unsigned long long vcu: 56;
    union {
        long long i;
        long double f;
        double d;
    } conv;
} g_counter_meta;
g_counter_meta* create_g_counter_meta(long long gid, long long type, long long vcu);
void free_g_counter_meta(g_counter_meta* meta);
int g_counter_meta_to_string(g_counter_meta* del, char* buf);
typedef int (*GetGMetaFunc)(void* data, int index, g_counter_meta* value);
sds g_counter_metas_to_sds(void* data, GetGMetaFunc fun, int size);
int sds_to_g_counter_meta(sds data, g_counter_meta** ds);
int str_to_g_counter_meta(char* data, int data_size, g_counter_meta** ds);
int array_get_g_counter_meta(void* data, int index, g_counter_meta* value);
