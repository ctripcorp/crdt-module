#include "ctrip_stream_io.h"
#include <string.h>
#include "include/rmutil/zmalloc.h"


/* -------------------------- rdb stream impl ------------------------------ */

static inline void rdbStreamSaveUnsigned(struct sio *io, uint64_t value) {
    RedisModule_SaveUnsigned((RedisModuleIO*)io->stream, value);
}

static inline uint64_t rdbStreamLoadUnsigned(struct sio *io) {
    return RedisModule_LoadUnsigned((RedisModuleIO*)io->stream);
}

static inline void rdbStreamSaveSigned(struct sio *io, int64_t value) {
    RedisModule_SaveSigned((RedisModuleIO*)io->stream, value);
}

static inline int64_t rdbStreamLoadSigned(struct sio *io) {
    return RedisModule_LoadSigned((RedisModuleIO*)io->stream);
}

static inline void rdbStreamSaveStringBuffer(struct sio *io, const char *str, size_t len) {
    RedisModule_SaveStringBuffer((RedisModuleIO*)io->stream, str, len);
}

static inline char *rdbStreamLoadStringBuffer(struct sio *io, size_t *lenptr) {
    return RedisModule_LoadStringBuffer((RedisModuleIO*)io->stream, lenptr);
}

static inline void *rdbStreamLoadSds(struct sio *io) {
    return RedisModule_LoadSds((RedisModuleIO*)io->stream);
}

static inline void rdbStreamSaveDouble(struct sio *io, double value) {
    RedisModule_SaveDouble((RedisModuleIO*)io->stream, value);
}

static inline double rdbStreamLoadDouble(struct sio *io) {
    return RedisModule_LoadDouble((RedisModuleIO*)io->stream);
}

static inline void rdbStreamSaveLongDouble(struct sio *io, long double value) {
    RedisModule_SaveLongDouble((RedisModuleIO*)io->stream, value);
}

static inline long double rdbStreamLoadLongDouble(struct sio *io) {
    return RedisModule_LoadLongDouble((RedisModuleIO*)io->stream);
}

static inline void rdbStreamSaveFloat(struct sio *io, float value) {
    RedisModule_SaveFloat((RedisModuleIO*)io->stream, value);
}

static inline float rdbStreamLoadFloat(struct sio *io) {
    return RedisModule_LoadFloat((RedisModuleIO*)io->stream);
}

sioType rdbStreamType = {
    .save_unsigned = rdbStreamSaveUnsigned,
    .load_unsigned = rdbStreamLoadUnsigned,
    .save_signed = rdbStreamSaveSigned,
    .load_signed = rdbStreamLoadSigned,
    .save_stringbuffer = rdbStreamSaveStringBuffer,
    .load_stringbuffer = rdbStreamLoadStringBuffer,
    .load_sds = rdbStreamLoadSds,
    .save_double = rdbStreamSaveDouble,
    .load_double = rdbStreamLoadDouble,
    .save_longdouble = rdbStreamSaveLongDouble,
    .load_longdouble = rdbStreamLoadLongDouble,
    .save_float = rdbStreamSaveFloat,
    .load_float = rdbStreamLoadFloat,
};

sio *rdbStreamCreate(RedisModuleIO *rdb) {
    sio *io = zmalloc(sizeof(struct sio));
    io->stream = rdb;
    io->state = NULL;
    io->type = &rdbStreamType;
    return io;
}

RedisModuleIO *rdbStreamGetRdb(sio *io) {
    return io->stream;
}

void rdbStreamRelease(sio *io) {
    zfree(io);
}

/* -------------------------- sds stream impl ------------------------------ */

#define SDS_STREAM_SAVE(io, value)  do {                        \
    io->stream = sdscatlen(io->stream, &value, sizeof(value));  \
} while(0)

#define SDS_STREAM_LOAD(io, type) do {                          \
    char *ptr = (char*)io->stream + (size_t)io->state;          \
    io->state = (void*)((size_t)io->state + sizeof(type));      \
    return *(type*)ptr;                                         \
} while(0)

static void sdsStreamSaveUnsigned(struct sio *io, uint64_t value) {
    SDS_STREAM_SAVE(io, value);
}

static uint64_t sdsStreamLoadUnsigned(struct sio *io) {
    SDS_STREAM_LOAD(io, uint64_t);
}

static void sdsStreamSaveSigned(struct sio *io, int64_t value) {
    SDS_STREAM_SAVE(io, value);
}

static int64_t sdsStreamLoadSigned(struct sio *io) {
    SDS_STREAM_LOAD(io, int64_t);
}

static void sdsStreamSaveStringBuffer(struct sio *io, const char *str, size_t len) {
    sdsStreamSaveUnsigned(io, (uint64_t)len);
    io->stream = sdscatlen(io->stream, str, len);
}

static char *sdsStreamLoadStringBuffer(struct sio *io, size_t *lenptr) {
    uint64_t len = sdsStreamLoadUnsigned(io);
    char *ptr = (char*)io->stream + (size_t)io->state;
    char *result = zmalloc(len);
    memcpy(result, ptr, len);
    *lenptr = len;
    io->state = (void*)((size_t)io->state + len);
    return result;
}

static void *sdsStreamLoadSds(struct sio *io) {
    uint64_t len = sdsStreamLoadUnsigned(io);
    char *ptr = (char*)io->stream + (size_t)io->state;
    io->state = (void*)((size_t)io->state + len);
    return sdsnewlen(ptr, len);
}

static void sdsStreamSaveDouble(struct sio *io, double value) {
    SDS_STREAM_SAVE(io, value);
}

static double sdsStreamLoadDouble(struct sio *io) {
    SDS_STREAM_LOAD(io, int64_t);
}

static void sdsStreamSaveLongDouble(struct sio *io, long double value) {
    SDS_STREAM_SAVE(io, value);
}

static long double sdsStreamLoadLongDouble(struct sio *io) {
    SDS_STREAM_LOAD(io, long double);
}

static void sdsStreamSaveFloat(struct sio *io, float value) {
    SDS_STREAM_SAVE(io, value);
}

static float sdsStreamLoadFloat(struct sio *io) {
    SDS_STREAM_LOAD(io, float);
}

sioType sdsStreamType = {
    .save_unsigned = sdsStreamSaveUnsigned,
    .load_unsigned = sdsStreamLoadUnsigned,
    .save_signed = sdsStreamSaveSigned,
    .load_signed = sdsStreamLoadSigned,
    .save_stringbuffer = sdsStreamSaveStringBuffer,
    .load_stringbuffer = sdsStreamLoadStringBuffer,
    .load_sds = sdsStreamLoadSds,
    .save_double = sdsStreamSaveDouble,
    .load_double = sdsStreamLoadDouble,
    .save_longdouble = sdsStreamSaveLongDouble,
    .load_longdouble = sdsStreamLoadLongDouble,
    .save_float = sdsStreamSaveFloat,
    .load_float = sdsStreamLoadFloat,
};

sio *sdsStreamCreate(sds val) {
    sio *io = zmalloc(sizeof(struct sio));
    io->stream = val;
    io->type = &sdsStreamType;
    io->state = 0;
    return io;
}

sds sdsStreamRelease(sio *io) {
    sds result = io->stream;
    zfree(io);
    return result;
}

