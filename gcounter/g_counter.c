#include "g_counter.h"
#include "../util.h"


g_counter_meta* create_g_counter_meta(long long gid, long long type, long long vcu) {
    g_counter_meta* del = malloc(sizeof(g_counter_meta));
    del->gid = gid;
    del->data_type = type;
    del->vcu = vcu;
    return del;
}

void free_g_counter_meta(g_counter_meta* del) {
    RedisModule_Free(del);
}

int g_counter_meta_to_string(g_counter_meta* del, char* buf) {
    int len = 0;
    
    len += sprintf(buf + len, "%u:%lld:%u:",del->gid,  del->vcu, del->data_type);
    int s = 0;
    switch(del->data_type) {
        case VALUE_TYPE_INTEGER:
            len += sprintf(buf + len, "%lld", del->conv.i);
        break;
        case VALUE_TYPE_FLOAT:
            s = sizeof(del->conv.f);
            memcpy(buf + len, &(del->conv.f), s);
            len += s;
        break;
        case VALUE_TYPE_DOUBLE:
            s = sizeof(del->conv.d);
            memcpy(buf + len, &(del->conv.d), s);
            len += s;
        break;
    }
    return len;
}


sds g_counter_metas_to_sds(void* data, GetGMetaFunc fun, int size) {
    char buf[100 * size];
    int len = 0;
    for(int i = 0; i < size; i++) {
        g_counter_meta del = {
            .gid = 0,
            .data_type = 0,
            .vcu = 0,
            .conv.d = 0,
        };
        if(!fun(data, i, &del)) {
            continue;
        }
        len += g_counter_meta_to_string(&del, buf + len);
        if(i + 1 < size) {
            buf[len++] = ',';
        }
    }
    if(len == 0) {
        return sdsempty();
    }
    return sdsnewlen(buf, len);
}

int str_to_g_counter_meta(char* data, int data_size, g_counter_meta** ds)  {
    int len = 0;
    char* start = data;
    int offset = 0;
    char* split_index = NULL;
    g_counter_meta* v = NULL;
    while((split_index = strstr(start + offset, ":")) != NULL) {
        long long gid = 0;
        if(!string2ll(start + offset, split_index - start - offset , &gid)) {
            goto error;
        }
        offset = split_index - start + 1;

        split_index = strstr(start + offset, ":");
        if(split_index == NULL) goto error;
        long long vcu = 0;
        if(!string2ll(start + offset , split_index - start - offset , &vcu)) {
            goto error;
        }
        offset = split_index - start + 1;

        split_index = strstr(start + offset, ":");
        if(split_index == NULL) goto error;
        long long type = 0;
        if(!string2ll(start + offset , split_index - start - offset, &type)) {
            goto error;
        }

        offset = split_index - start + 1;

        v = create_g_counter_meta(gid, type, vcu);
        double d = 0;
        long long ll = 0;
        long double f = 0;
        int lsize = 0;
        switch (type)
        {
        case VALUE_TYPE_DOUBLE:
            d = *(double*)(start+offset);
            v->conv.d = d;
            offset += sizeof(double);
            break;
        case VALUE_TYPE_FLOAT:
            f = *(long double*)(start+offset);
            v->conv.f = f;
            offset += sizeof(long double);
            break;
        case VALUE_TYPE_INTEGER:
            split_index = strstr(start + offset, ",");
            if(split_index == NULL) {
                lsize = data_size - offset;
            } else {
                lsize = split_index - start - offset;
            }
            if(!string2ll(start + offset , lsize, &ll)) {
                goto error;
            }
            v->conv.i = ll;
            offset += lsize;
            break;
        default:
            goto error;
            break;
        }
        ds[len++] = v;
        v = NULL;
        if(start[offset] == ',') {
            offset++;
        }
    }
    return len;
error:
    if(v != NULL) free_g_counter_meta(v);
    for(int i = 0; i < len; i++) {
        free_g_counter_meta(ds[i]);
        ds[i] = NULL;
    }
    return 0;
}

int sds_to_g_counter_meta(sds data, g_counter_meta** ds) {
    return str_to_g_counter_meta(data, sdslen(data), ds);
}
int array_get_g_counter_meta(void* data, int index, g_counter_meta* value) {
    g_counter_meta* array = (g_counter_meta*)data;
    g_counter_meta a = array[index];
    value->data_type = a.data_type;
    value->gid = a.gid;
    value->vcu = a.vcu;
    switch (value->data_type)
    {
    case VALUE_TYPE_INTEGER:
        value->conv.i = a.conv.i;
        break;
    case VALUE_TYPE_FLOAT:
        value->conv.f = a.conv.f;
        break;
    case VALUE_TYPE_DOUBLE:
        value->conv.d = a.conv.d;
        break;
    default:
        break;
    }
    return 1;
}