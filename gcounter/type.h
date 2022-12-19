
#include <limits.h>
#include <rmutil/sds.h>
#define VALUE_TYPE_NONE 0
#define VALUE_TYPE_LONGLONG 1
#define VALUE_TYPE_DOUBLE  2
#define VALUE_TYPE_SDS 3
#define VALUE_TYPE_LONGDOUBLE 4

#define PLUS_ERROR_NAN -1
#define PLUS_ERROR_OVERFLOW -2
#define SDS_PLUS_ERR -3


#define COUNTER_MAX (LLONG_MAX>>4)
#define COUNTER_MIN (LLONG_MIN>>4)


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

typedef struct {
    long long type;
    union all_type value;
}  __attribute__ ((packed, aligned(1))) ctrip_value;

int copy_tag_data_from_all_type(int type, union tag_data* t, union all_type a);
sds sds_change_value(int type, union all_type* value);
int plus_or_minus_ctrip_value(ctrip_value* a, ctrip_value* b, int plus);
int value_to_ld(ctrip_value* value);
int value_to_ll(ctrip_value* value);
void free_ctrip_value(ctrip_value value);

// int set_double_tag_value(union tag_data *value, double d);
// int set_ll_tag_value(union tag_data *value, long long l);
// int set_sds_tag_value(union tag_data *value, sds s);

// int get_double_tag_value(union tag_data* value, double* d);
// int get_ll_tag_value(union tag_data* value, long long* l);
// int get_sds_tag_value(union tag_data* value, sds* s);
int tag_data_get_ld(int type, union tag_data t, long double* ld);
