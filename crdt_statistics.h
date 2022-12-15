#include <redismodule.h>
#include <rmutil/sds.h>
void parse_start();
void parse_end();

void get_modulekey_start();
void get_modulekey_end();

void add_val_start();
void add_val_end();

void update_val_start();
void update_val_end();

void send_event_start();
void send_event_end();

void write_bakclog_start();
void write_backlog_end();

int statisticsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/** about memory **/
size_t sum_memory();
int memoryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);