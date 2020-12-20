#include "g_counter.h"
#include <assert.h>
#include <string.h>
typedef int (*parseFunc)(char* buf, int len, void* value);
int read_func(char* buf, int len , char* split_str, void* value, int* is_end, parseFunc func) {
    char* split_index = strstr(buf, split_str);

    int offset = 0;
    if(split_index == NULL) {
        offset = len;
        if(is_end) *is_end = 1;
    } else {
        offset = split_index - buf;
        if(offset > len) {
            printf("strstr find error %d %d\n", offset , len);
            offset = len;
        }
    }
    if(func != NULL) {
        if(!func(buf, offset, value)) {
            return -1;
        }
    }
    if(split_index != NULL) {
        offset += 1;
    } 
    return offset;
}



int read_str_value(char* buf, int len, ctrip_value* value) {
    int offset = 0;
    int o = read_func(buf + offset, len - offset , ":", &value->type , NULL, (parseFunc)string2ll);
    if(o == -1) {return -1;}
    offset += o;
    int is_end = 0;
    
    switch (value->type) {
        case VALUE_TYPE_DOUBLE:
        case VALUE_TYPE_LONGLONG:
        case VALUE_TYPE_LONGDOUBLE: 
            o = read_func(buf + offset, len - offset , ",", NULL , &is_end, NULL);
            if(o == -1) return -1;
            value->value.s = sdsnewlen(buf + offset , is_end? o : o - 1);
            offset += o;
            break;
        case VALUE_TYPE_SDS: {
            long long sds_size = 0;
            o = read_func(buf + offset, len - offset , ":", &sds_size , &is_end, (parseFunc)string2ll);
            if(o == -1) {return -1;}
            offset += o;
            if(sds_size + offset > len) {
                return -1;
            }
            value->value.s = sdsnewlen(buf + offset, sds_size);
            offset += sds_size;
            if(offset < len) {
                if(buf[offset] == ',') {
                    offset += 1;
                }
            }
            break;
        }
            
        default:
            printf("[read_str_value] type error: %lld", value->type);
            return -1;
        break;
    }
    return offset;
}


sds value_sds_to_ld(union all_type* value) {
    long double ld = 0;
    sds result = value->s;
    if(!string2ld(result, sdslen(result), &ld)) {
        return NULL;
    }
    value->f = ld;
    return result;
}
#define LL_STR_MAX 21
sds value_sds_to_ll(union all_type* value) {
    long long ll = 0;
    sds result = value->s;
    if(sdslen(result) > LL_STR_MAX) return NULL;
    if(!string2ll(result, sdslen(result), &ll)) {
        return NULL;
    }
    value->i = ll;
    return result;
}
sds value_sds_to_d(union all_type* value) {
    long double ld = 0;
    sds result = value->s;
    if(!string2ld(result, sdslen(result), &ld)) {
        return NULL;
    }
    value->d = (double)ld;
    return result;
}

sds sds_change_value(int type, union all_type* value) {
    sds result = NULL;
    switch (type)
    {
    case VALUE_TYPE_DOUBLE:
        result = value_sds_to_d(value);
        /* code */
        break;
    case VALUE_TYPE_LONGLONG: {
        result = value_sds_to_ll(value);
        break;
    }
    case VALUE_TYPE_LONGDOUBLE: {
        result = value_sds_to_ld(value);
        break;
    }
    case VALUE_TYPE_SDS: {
        result = value->s;
        break;
    }
    default:
        break;
    }
    return result;
}


sds read_value(char* buf, int len, ctrip_value* value, int* offset) {
    int o = read_str_value(buf, len , value);
    if(o == -1) {return NULL;}
    *offset = o;
    return sds_change_value(value->type, &value->value);
}

sds write_value(int type, union all_type data) {
    switch (type)
    {
    case VALUE_TYPE_LONGLONG:
        return sdscatprintf(sdsempty(), "%d:%lld", type, data.i);
    break;
    case VALUE_TYPE_DOUBLE:
        return sdscatprintf(sdsempty(), "%d:%.17f", type, data.d);
        break;
    case VALUE_TYPE_LONGDOUBLE:
        return sdscatprintf(sdsempty(), "%d:%.17Lf", type, data.f);
    break;
    case VALUE_TYPE_SDS:
        return sdscatprintf(sdsempty(), "%d:%zu:%s", type, sdslen(data.s), data.s);
    break;
    default:
        break;
    }
    return NULL;
}

g_counter_meta* create_g_counter_meta(long long gid, long long vcu) {
    g_counter_meta* meta = RedisModule_Alloc(sizeof(g_counter_meta));
    meta->gid = gid;
    meta->vcu = vcu;
    meta->data_type = VALUE_TYPE_NONE;
    meta->value.i = 0;
    meta->v = NULL;
    return meta;
}

void free_g_counater_maeta(g_counter_meta* meta) {
    sdsfree(meta->v);
    counter_meta_free(meta);
}


int copy_sds_to_buf(char* buf, sds value) {
    memcpy(buf, value, sdslen(value));
    return sdslen(value);
}

#define MAX_LL_SIZE 21
#define MAX_LONG_DOUBLE_CHARS 5*1024
int get_value_max_len(int data_type, union all_type v) {
    switch (data_type)
    {
    case VALUE_TYPE_SDS:
        return sdslen(v.s) + MAX_LL_SIZE + 1;
        break;
    case VALUE_TYPE_DOUBLE:
    case VALUE_TYPE_LONGDOUBLE:
        return MAX_LONG_DOUBLE_CHARS + MAX_LL_SIZE + 1; 
    case VALUE_TYPE_LONGLONG:
        return MAX_LL_SIZE + MAX_LL_SIZE + 1;
    default:
        printf("[get_value_max_len]type error %d\n", data_type);
        assert(1 == 0);
        break;
    }
}
int value_to_str(char* buf, int data_type, union all_type v) {
    int len = 0;
    len += ll2string(buf + len, MAX_LL_SIZE, data_type);
    buf[len++] = ':';
    switch (data_type) {
    case VALUE_TYPE_SDS:
        len += ll2string(buf + len, MAX_LL_SIZE, sdslen(v.s));
        buf[len++] = ':';
        len += copy_sds_to_buf(buf + len , v.s);
    break;
    case VALUE_TYPE_DOUBLE:
        len += d2string(buf + len, MAX_LONG_DOUBLE_CHARS, v.d);
    break;
    case VALUE_TYPE_LONGDOUBLE:
        len += ld2string(buf + len, MAX_LONG_DOUBLE_CHARS, v.f, 1);
    break;
    case VALUE_TYPE_LONGLONG:
        len += ll2string(buf + len, MAX_LL_SIZE, v.i);
    break;
    default:
        printf("[value_to_str]type error:%d", data_type);
        assert(1 == 0);
        break;
    }
    return len;
}

sds value_to_sds(int data_type, union all_type v) {
    int max_len = get_value_max_len(data_type, v);
    char buf[max_len];
    int len = value_to_str(buf, data_type, v);
    return sdsnewlen(buf, len);
}

int g_counter_meta_to_str(g_counter_meta* meta, char* buf) {
    int len = 0;
    //planA
    // len += sprintf(buf + len, "%u:%lld:%u:",meta->gid,  meta->vcu, meta->data_type);
    //planB
    len += ll2string(buf + len, MAX_LL_SIZE, meta->gid);
    buf[len++] = ':';
    len += ll2string(buf + len, MAX_LL_SIZE, meta->vcu);
    buf[len++] = ':';
    len += ll2string(buf + len, MAX_LL_SIZE, meta->data_type);
    buf[len++] = ':';
    if(meta->data_type !=VALUE_TYPE_SDS) {
        len += copy_sds_to_buf(buf + len, meta->v);
    } else {
        len += value_to_str(buf + len, VALUE_TYPE_SDS, meta->value);
    }
    return len;
}



int g_counter_metas_to_str(char*buf, void* data, GetGMetaFunc fun,  int size) {
    int len = 0;
    for(int i = 0; i < size; i++) {
        g_counter_meta del = {
            .gid = 0,
            .data_type = 0,
            .vcu = 0,
            .value.d = 0,
        };
        if(!fun(data, i, &del)) {
            continue;
        }
        len += g_counter_meta_to_str(&del, buf + len);
        if(i + 1 < size) {
            buf[len++] = ',';
        }
        if(del.v != NULL) sdsfree(del.v);
    }
    if(len == 0) {
        return 0;
    }
    return len;
}

int gcounter_meta_set_value(g_counter_meta* meta, int type, void* v, int parse) {
    char buf[256];
    int len = 0;
    long long ll = 0;
    long double ld = 0;
    double d = 0;
    switch (type)
    {
    case VALUE_TYPE_LONGLONG:
        ll = *(long long*)v;
        meta->value.i = ll;
        meta->v = sdsfromlonglong(ll);
        if(parse) {
            if(!string2ll(meta->v, sdslen(meta->v), v)) {
                printf("gcounter_meta_set_value parse long long error\n");
                assert(1 == 0);
            }
        }
        break;
    case VALUE_TYPE_LONGDOUBLE:
        ld = *(long double*)v;
        meta->value.f = ld;
        len = ld2string(buf,sizeof(buf),ld,1);
        meta->v = sdsnewlen(buf, len);
        if(parse) {
            if(!string2ld(meta->v, sdslen(meta->v), v)) {
                printf("gcounter_meta_set_value parse long double error\n");
                assert(1 == 0);
            }
        }
        break;
    case VALUE_TYPE_DOUBLE:
        d = *(double*)v;
        meta->value.d = d;
        len = d2string(buf,sizeof(buf),d);
        meta->v = sdsnewlen(buf, len);
        if(parse) {
            if(!string2ld(meta->v, sdslen(meta->v), &ld)) {
                printf("gcounter_meta_set_value parse double error\n");
                assert(1 == 0);
            }
            *(double*)v = (double)ld;
        }
        break;
    default:
        break;
    }
    meta->data_type = type;
    return 1;
}

int str_to_g_counter_metas(char* buf, int len, g_counter_meta** metas) {
    if(len == 0) return 0;
    int offset = 0;
    g_counter_meta* meta = NULL;
    int meta_len = 0;
    int o = 0;
    do {
        long long gid; 
        o = read_func(buf + offset, len - offset, ":", &gid, NULL, (parseFunc)string2ll);
        if(o == -1) {
            printf("[str_to_g_counter_metas] parse gid error: %s\n",buf + offset);
            goto error;
        } 
        offset += o;
        long long vcu;
        o = read_func(buf + offset, len - offset, ":",  &vcu, NULL, (parseFunc)string2ll);
        if(o == -1) {
            printf("[str_to_g_counter_metas] parse vcu error: %s\n",buf + offset);
            goto error;
        } 
        offset += o;
        
        // union all_type value = {.d = 0};
        // long long type = 0;
        ctrip_value value = {.type = 0, .value.i = 0};
        sds v = read_value(buf + offset, len - offset, &value, &o);
        if(v == NULL)  {
            printf("[str_to_g_counter_metas] parse value error: %s", buf+offset);
            goto error;
        }
        offset += o;
        meta = create_g_counter_meta(gid, vcu);
        meta->data_type = value.type;
        meta->value = value.value;
        meta->v = v;
        metas[meta_len++] = meta;
        meta = NULL;
        if(offset == len) {
            return meta_len;
        }
    } while(1);
    
error:
    for(int i = 0 ; i < meta_len; i++) {
        free_g_counater_maeta(metas[i]);
    }
    return -1;
}

int str_2_value_and_g_counter_metas(sds info, ctrip_value* value,  g_counter_meta** g) {
    int str_len = sdslen(info);
    int offset = 0;
    sds str = read_value(info + offset , str_len - offset, value, &offset);
    if(str == NULL) {return -1;}
    if(value->type != VALUE_TYPE_SDS) {  sdsfree(str); }
    int r = str_to_g_counter_metas(info + offset, str_len - offset, g);
    return r;
}

int copy_tag_data_from_all_type(int type, union tag_data* t, union all_type a) {
    switch (type)
    {
    case VALUE_TYPE_DOUBLE:
        t->f = a.d;
        break;
    case VALUE_TYPE_LONGLONG:
        t->i = a.i;
        break;
    case VALUE_TYPE_SDS:
        t->s = sdsdup(a.s);
        break;
    default:
        printf("[copy_tag_data_from_all_type] type error %d", type);
        assert(1 == 0);
        break;
    }
    return 1;
}

int value_to_ld(ctrip_value* value) {
    switch (value->type)
    {
    case VALUE_TYPE_SDS:
        if(value_sds_to_ld(&value->value)) {
            value->type = VALUE_TYPE_LONGDOUBLE;
        } else {
            return 0;
        }
        break;
    case VALUE_TYPE_LONGLONG:
        value->type = VALUE_TYPE_LONGDOUBLE;
        value->value.f = (long double)value->value.i;
        break;
    case VALUE_TYPE_LONGDOUBLE:
        break;
    case VALUE_TYPE_DOUBLE:
        value->type = VALUE_TYPE_LONGDOUBLE;
        value->value.f = (long double)value->value.d;
        break;
    case VALUE_TYPE_NONE:
        printf("[value_to_ld] type is none");
        return 0;
        break;
    default:
        printf("[value_to_ld] type error: %lld", value->type);
        assert(1 == 0);
        return 0;
        break;
    }
    return 1;
}

int value_to_ll(ctrip_value* value) {
    switch (value->type)
    {
    case VALUE_TYPE_SDS:
        if(value_sds_to_ll(&value->value)) {
            value->type = VALUE_TYPE_LONGLONG;
        } else {
            return 0;
        }
        break;
    case VALUE_TYPE_LONGLONG:
        return 1;
        break;
    case VALUE_TYPE_LONGDOUBLE: {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf, sizeof(buf), value->value.f, 1);
        if(len > MAX_LL_SIZE) {
            return 0;
        }
        long long ll = 0;
        return string2ll(buf, len, &ll);
    }
        break;
    case VALUE_TYPE_DOUBLE:
        printf("[value_to_ll] double to ll Code not implemented");
        return 0;
        break;
    case VALUE_TYPE_NONE:
        printf("[value_to_ll] type is none");
        return 0;
        break;
    default:
        printf("[value_to_ll] type error: %lld", value->type);
        assert(1 == 0);
        return 0;
        break;
    }
    return 1;
}
void free_ctrip_value(ctrip_value value) {
    if(value.type == VALUE_TYPE_SDS) {
        sdsfree(value.value.s);
    }
}

int tag_data_get_ld(int type, union tag_data t, long double* ld) {
    switch (type)
    {
    case VALUE_TYPE_DOUBLE:
        *ld = (long double)t.f;
        break;
    case VALUE_TYPE_LONGLONG:
        *ld = (long double)t.i;
        break;
    case VALUE_TYPE_SDS:
        return string2ld(t.s, sdslen(t.s), ld);
    default:
        return 0;
        break;
    }
    return 1;
}



#if defined(PARSE_TEST_MAIN)
#include <stdio.h>
#include "../libs/testhelp.h"
#include "limits.h"

#define UNUSED(x) (void)(x)

int writeTest(void) {
    {
        printf("========[write Value Long Long]==========\r\n");
        //test parse LONG LONG
        union all_type value = {.i = 0};
        sds x = write_value(VALUE_TYPE_LONGLONG, value);
        sds y = sdsnew("1:0");
        printf("%s \n", x);
        test_cond("write value long long 1",
            !sdscmp(x, y));
        sdsfree(x);
        sdsfree(y);
        value.i = 100;
        x = write_value(VALUE_TYPE_LONGLONG, value);
        y = sdsnew("1:100");
        test_cond("write value long long 2",
            !sdscmp(x, y));
        sdsfree(x);
        sdsfree(y);

        value.d = 1.2;
        x = write_value(VALUE_TYPE_DOUBLE, value);
        y = sdsnew("2:1.2");
        printf("%s\n", x);
        test_cond("write value double 1",
            !sdscmp(x, y));
        sdsfree(x);
        sdsfree(y);

        value.s = sdsnew("abc");
        x = write_value(VALUE_TYPE_SDS, value);
        y = sdsnew("3:3:abc");
        test_cond("write value string 1",
            !sdscmp(x, y));
        sdsfree(x);
        sdsfree(y);

        value.f = 1.234;
        x = write_value(VALUE_TYPE_LONGDOUBLE, value);
        y = sdsnew("4:1.234");
        printf("%s \n",x);
        test_cond("write value long double 1",
            !sdscmp(x, y));
        sdsfree(x);
        sdsfree(y);
    }
    test_report()
    return 0;
}
int readTest(void) {
    {
        printf("========[parse Value Long Long]==========\r\n");
        // //test parse LONG LONG
        sds x = sdsnew("1:12354");
        int offset = 0;
        // int type = 0;
        // union all_type value = {.i = 0};
        ctrip_value value = {.type = VALUE_TYPE_NONE, .value.i = 0};
        // sds s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse value long long 1",
        //     offset == 7 && type == VALUE_TYPE_LONGLONG && value.i == 12354)
        // sdsfree(x);
        // x = sdsnew("1:12354,");
        // s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse value long long 2",
        //     offset == 8 && type == VALUE_TYPE_LONGLONG && value.i == 12354)
        // sdsfree(x);

        // x = sdsnew("1:1.2");
        // s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse type error long",
        //     s == NULL)
        // sdsfree(x);

        // printf("========[parse float ]==========\r\n");
        // x = sdsnew("2:1.2");
        // s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse type error float",
        //     offset == 5 && type == VALUE_TYPE_DOUBLE)
        // sdsfree(x);

        // printf("========[parse long double ]==========\r\n");
        // x = sdsnew("4:1.2");
        // s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse type error float",
        //     offset == 5 && type == VALUE_TYPE_LONGDOUBLE)
        // sdsfree(x);

        // printf("========[parse string ]==========\r\n");
        // x = sdsnew("3:4:1234");
        // s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse string ",
        //     offset == 8 && type == VALUE_TYPE_SDS && sdslen(value.s) == 4 && memcmp(value.s, "1234\0", 5) == 0)
        // sdsfree(x);

        // x = sdsnew("3:5:1234");
        // s = read_value(x, sdslen(x), &type, &value, &offset);
        // test_cond("parse string error",
        //     s == NULL)
        // sdsfree(x);
        printf("libc: %s\n", ZMALLOC_LIB);
        printf("========[parse meta ]==========\r\n");
        g_counter_meta* metas[2];
        x = sdsnew("1:1000:2:1.5,2:2000:1:1000");
        int len = str_to_g_counter_metas(x, sdslen(x), metas);
        test_cond("[meta1] check metas len", len == 2)
        printf("vcu %lld \n", metas[0]->vcu);
        printf("zmalloc: %s\n", ZMALLOC_LIB);
        test_cond("[meta1] check metas value", metas[0]->gid == 1 && metas[0]->vcu == 1000)

        printf("========[parse value and meta ]==========\r\n");
        x = sdsnew("3:3:abc,1:1000:2:1.5,2:2000:1:1000");
        int l = str_2_value_and_g_counter_metas(x, &value, metas);
        test_cond("[meta1] type ", type == 3)
        test_cond("[meta1] check metas len", l == 2)
        printf("vcu %lld \n", metas[0]->vcu);
        printf("zmalloc: %s\n", ZMALLOC_LIB);
        test_cond("[meta1] check metas value", metas[0]->gid == 1 && metas[0]->vcu == 1000)
    }
    test_report()
    return 0;
}
#endif



#ifdef PARSE_TEST_MAIN
#include<stdio.h>
#include<sys/time.h>
typedef int (*TestFunc)(void* value);
void speedTest(TestFunc func, void* value, int num) {
    struct timeval start_time = {0};
    gettimeofday(&start_time,NULL);
    for(int i = 0; i < num; i++) {
        func(value);
    }
    struct timeval end_time = {0};
    gettimeofday(&end_time,NULL);
    long long time_mic = end_time.tv_sec*1000*1000 + end_time.tv_usec - 
            start_time.tv_sec * 1000 * 1000 - start_time.tv_usec;
    printf("run func %d, used %ld(ns)\n",num, time_mic);
}

int meta2StrTest(void* meta) {
    char* buf[1000];
    g_counter_meta_to_str((g_counter_meta*)meta, buf);
    return 1;
}

int array_get_g_counter_meta(void* data, int index, g_counter_meta* value) {
    g_counter_meta* array = (g_counter_meta*)data;
    g_counter_meta a = array[index];
    value->data_type = a.data_type;
    value->gid = a.gid;
    value->vcu = a.vcu;
    switch (value->data_type)
    {
    case VALUE_TYPE_LONGLONG:
        value->value.i = a.value.i;
        break;
    case VALUE_TYPE_LONGDOUBLE:
        value->value.f = a.value.f;
        break;
    case VALUE_TYPE_DOUBLE:
        value->value.d = a.value.d;
        break;
    default:
        break;
    }
    return 1;
}

int main(void) {
    
    g_counter_meta* meta = create_g_counter_meta(1,10000);
    double d = 1.2;
    gcounter_meta_set_value(meta, VALUE_TYPE_DOUBLE, &d, 0);
    char buf[256];
    int len = g_counter_meta_to_str(meta, buf);
    buf[len] = '\0';
    printf("len: %d  str: %s\n", len, buf);
    speedTest(meta2StrTest, meta, 100000);
    return readTest() + writeTest();
}
#endif
