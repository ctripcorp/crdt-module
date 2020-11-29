
#include "../include/rmutil/sds.h"
#define VALUE_TYPE_NONE 0
#define VALUE_TYPE_LONGLONG 1
#define VALUE_TYPE_DOUBLE  2
#define VALUE_TYPE_SDS 3
#define VALUE_TYPE_LONGDOUBLE 4
union tag_data {
    long long i;
    double f;
    sds s;
}  __attribute__ ((packed, aligned(1))) tag_data;

union all_type {
    long long i;
    long double f;
    double d;
    sds s;
} __attribute__ ((packed, aligned(1))) all_type;

int copy_tag_data_from_all_type(int type, union tag_data* t, union all_type a);


// int set_double_tag_value(union tag_data *value, double d);
// int set_ll_tag_value(union tag_data *value, long long l);
// int set_sds_tag_value(union tag_data *value, sds s);

// int get_double_tag_value(union tag_data* value, double* d);
// int get_ll_tag_value(union tag_data* value, long long* l);
// int get_sds_tag_value(union tag_data* value, sds* s);
