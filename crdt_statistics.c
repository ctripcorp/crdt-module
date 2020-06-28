#include "./crdt_statistics.h"
#include <stdio.h>
#include <strings.h>
#include <time.h>

unsigned long long nstime(void) {
   struct timespec ts;
   clock_gettime(CLOCK_REALTIME, &ts);
   return ts.tv_sec*1000000000+ts.tv_nsec;
}


unsigned long long statistics_parse_start_time = 0;
void parse_start() {
   statistics_parse_start_time = nstime();
}
unsigned long long statistics_parse_time = 0;
unsigned long long statistics_parse_num = 0;
void parse_end() {
   statistics_parse_time += nstime() - statistics_parse_start_time;
   statistics_parse_num++;
}
unsigned long long get_modulekey_start_time = 0;
void get_modulekey_start() {
   get_modulekey_start_time = nstime();
}
unsigned long long statistics_get_modulekey_time = 0;
unsigned long long statistics_get_modulekey_num = 0;
void get_modulekey_end() {
   statistics_get_modulekey_time += nstime() - get_modulekey_start_time;
   statistics_get_modulekey_num++;
}
unsigned long long add_val_start_time = 0;
void add_val_start() {
   add_val_start_time = nstime();
}
unsigned long long statistics_add_val_time = 0;
unsigned long long statistics_add_val_num = 0;
void add_val_end() {
   statistics_add_val_time += nstime() - add_val_start_time;
   statistics_add_val_num++;
}
unsigned long long update_val_start_time = 0;
void update_val_start() {
   update_val_start_time = nstime();
}
unsigned long long statistics_update_val_time = 0;
unsigned long long statistics_update_val_num = 0;
void update_val_end() {
   statistics_update_val_time += nstime() - update_val_start_time;
   statistics_update_val_num++;
}
unsigned long long send_event_start_time = 0;
void send_event_start() {
   send_event_start_time = nstime();
}
unsigned long long statistics_send_event_time = 0;
unsigned long long statistics_send_event_num = 0;
void send_event_end() {
   statistics_send_event_time += nstime() - send_event_start_time;
   statistics_send_event_num++;
}
unsigned long long write_bakclog_start_time = 0;
void write_bakclog_start() {
   write_bakclog_start_time = nstime();
}
unsigned long long statistics_write_backlog_time = 0;
unsigned long long statistics_write_backlog_num = 0;
void write_backlog_end() {
   statistics_write_backlog_time += nstime() - write_bakclog_start_time;
   statistics_write_backlog_num++;
}

int statisticsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
   char infobuf[999]; 
    size_t infolen = sprintf(infobuf, 
        "parse: %lld\r\n"
        "get_modulekey: %lld\r\n"
        "add_val: %lld\r\n"
        "update_val: %lld\r\n"
      //   "set_expire: %lld\r\n"
        "send_event: %lld\r\n"
        "write_backlog: %lld\r\n",
        statistics_parse_num == 0? 0: statistics_parse_time/statistics_parse_num,
        statistics_get_modulekey_num == 0? 0: statistics_get_modulekey_time/statistics_get_modulekey_num,
        statistics_add_val_num == 0? 0: statistics_add_val_time/statistics_add_val_num,
        statistics_update_val_num == 0? 0: statistics_update_val_time/statistics_update_val_num,
      //   statistics_set_expire_num == 0? 0: statistics_set_expire_time/statistics_set_expire_num,
        statistics_send_event_num == 0? 0: statistics_send_event_time/statistics_send_event_num,
        statistics_write_backlog_num == 0? 0: statistics_write_backlog_time/statistics_write_backlog_num
    );
    infobuf[infolen] = '\0';
    RedisModule_ReplyWithStringBuffer(ctx, infobuf, infolen);
    return REDISMODULE_OK;
}
/**
 * 
 *       about memory 
 **/
size_t sum_memory() {
   return RedisModule_ModuleMemory() + RedisModule_ModuleAllKeyMemory() + RedisModule_ModuleAllKeySize() * RedisModule_GetModuleValueMemorySize();
}
int memoryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
   char infobuf[999]; 
    size_t infolen = sprintf(infobuf, 
        "module-memory: %lld\r\n"
        "key-memory: %lld\r\n"
        "crdt-key-size: %d\r\n"
        "moduleValue-memory: %lld\r\n",
        RedisModule_ModuleMemory(),
        RedisModule_ModuleAllKeyMemory(),
        RedisModule_ModuleAllKeySize(),
        RedisModule_ModuleAllKeySize() * RedisModule_GetModuleValueMemorySize()
    );
    infobuf[infolen] = '\0';
    RedisModule_ReplyWithStringBuffer(ctx, infobuf, infolen);
    return REDISMODULE_OK;
}