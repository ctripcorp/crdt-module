#ifndef __SIO_H__
#define __SIO_H__

#include <stdint.h>
#include <stddef.h>
#include "include/redismodule.h"
#include "include/rmutil/sds.h"

struct sio;

typedef void *(*sioRedisModuleTypeLoadFunc)(struct sio *io, int encver);
typedef void (*sioRedisModuleTypeSaveFunc)(struct sio *io, void *value);

typedef struct sioType {
    void (*save_unsigned)(struct sio *io, uint64_t value);
    uint64_t (*load_unsigned)(struct sio *io);
    void (*save_signed)(struct sio *io, int64_t value);
    int64_t (*load_signed)(struct sio *io);
    void (*save_stringbuffer)(struct sio *io, const char *str, size_t len);
    char *(*load_stringbuffer)(struct sio *io, size_t *lenptr);
    void *(*load_sds)(struct sio *io);
    void (*save_double)(struct sio *io, double value);
    double (*load_double)(struct sio *io);
    void (*save_longdouble)(struct sio *io, long double value);
    long double (*load_longdouble)(struct sio *io);
    void (*save_float)(struct sio *io, float value);
    float (*load_float)(struct sio *io);
} sioType;

typedef struct sio {
    sioType *type;
    void *stream;
    void *state;
} sio;

static inline void sioSaveUnsigned(struct sio *io, uint64_t value) {
    io->type->save_unsigned(io, value);
}

static inline uint64_t sioLoadUnsigned(struct sio *io) {
    return io->type->load_unsigned(io);
}

static inline void sioSaveSigned(struct sio *io, int64_t value) {
    io->type->save_signed(io, value);
}

static inline int64_t sioLoadSigned(struct sio *io) {
    return io->type->load_signed(io);
}

static inline void sioSaveStringBuffer(struct sio *io, const char *str, size_t len) {
    io->type->save_stringbuffer(io, str, len);
}

static inline char *sioLoadStringBuffer(struct sio *io, size_t *lenptr) {
    return io->type->load_stringbuffer(io, lenptr);
}

static inline void *sioLoadSds(struct sio *io) {
    return io->type->load_sds(io);
}

static inline void sioSaveDouble(struct sio *io, double value) {
    io->type->save_double(io, value);
}

static inline double sioLoadDouble(struct sio *io) {
    return io->type->load_double(io);
}

static inline void sioSaveLongDouble(struct sio *io, long double value) {
    io->type->save_longdouble(io, value);
}

static inline long double sioLoadLongDouble(struct sio *io) {
    return io->type->load_longdouble(io);
}

static inline void sioSaveFloat(struct sio *io, float value) {
    io->type->save_float(io, value);
}

static inline float sioLoadFloat(struct sio *io) {
    return io->type->load_float(io);
}

sio *rdbStreamCreate(RedisModuleIO *rdb);
void rdbStreamRelease(sio *io);
sio *sdsStreamCreate(sds val);
sds sdsStreamRelease(sio *io);

#endif
