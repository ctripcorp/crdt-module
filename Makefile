#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=./include
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=./include/rmutil
endif

PYTHONTEST=python -m unittest -v -b
ALLMODULES=tests.register_test.CrdtRegisterTest tests.crdt_test.CrdtTest

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
COMPILER := $(shell gcc 2>&1 | head -1 | awk -F ":" '{print $$1}')
$(info USING $(COMPILER))

ifeq ($(COMPILER),clang)
    LIBC ?= 
endif

ifeq ($(COMPILER),gcc)
    LIBC ?= -lc
endif


# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -Wno-psabi -Wno-address-of-packed-member -Wno-int-to-pointer-cast -Wno-int-conversion -Wno-unused-variable -O0 -g -fPIC -std=gnu99  -DREDIS_MODULE_TARGET -DREDISMODULE_EXPERIMENTAL_API $(REDIS_CFLAGS)
ifeq ($(uname_S),Darwin)
	CFLAGS+= -DTCL_TEST -DDEBUG
	# CFLAGS+= -DDEBUG
endif

all: rmutil crdt.so
	@echo "BUILD SUCCESS VIA $(COMPILER)"

rmutil:
	$(MAKE) -C $(RMUTIL_LIBDIR)
crdt.o: crdt.c version.h
	$(CC) $(CFLAGS) -c -o $@ crdt.c
crdt_util.o: crdt.o crdt_util.h
	$(CC) $(CFLAGS) -c -o $@ crdt_util.c
crdt_pubsub.o: crdt_pubsub.c utils.c crdt_util.o
	$(CC) $(CFLAGS) -c -o $@ crdt_pubsub.c
crdt_register.o: crdt_register.c utils.c crdt_util.o crdt_statistics.o
	$(CC) $(CFLAGS) -c -o $@ crdt_register.c
crdt_lww_register.o: lww/crdt_lww_register.c crdt_register.o  crdt_util.o 
	$(CC) $(CFLAGS) -c -o $@ lww/crdt_lww_register.c
ctrip_crdt_hashmap.o: ctrip_crdt_hashmap.c utils.c crdt_util.o crdt_statistics.o
	$(CC) $(CFLAGS) -c -o $@ ctrip_crdt_hashmap.c
crdt_lww_hashmap.o: lww/crdt_lww_hashmap.c ctrip_crdt_hashmap.o  crdt_util.o 
	$(CC) $(CFLAGS) -c -o $@ lww/crdt_lww_hashmap.c
crdt_set.o: crdt_set.c utils.c crdt_util.o crdt_statistics.o
	$(CC) $(CFLAGS) -c -o $@ crdt_set.c
g_counter.o: gcounter/g_counter.c gcounter/g_counter.h
	$(CC) $(CFLAGS) -c -o $@ gcounter/g_counter.c
g_counter_element.o: gcounter/g_counter_element.c gcounter/g_counter_element.h 
	$(CC) $(CFLAGS) -c -o $@ gcounter/g_counter_element.c
crdt_orset_set.o: orset/crdt_orset_set.c crdt_set.o crdt_util.o crdt_statistics.o
	$(CC) $(CFLAGS) -c -o $@ orset/crdt_orset_set.c
ctrip_crdt_register.o: ctrip_crdt_register.c crdt_register.o crdt_util.o crdt_statistics.o  
	$(CC) $(CFLAGS) -c -o $@ ctrip_crdt_register.c
crdt_statistics.o: crdt_statistics.c 
	$(CC) $(CFLAGS) -c -o $@ crdt_statistics.c
ctrip_orset_rc.o: ./orset/crdt_orset_rc.c ctrip_crdt_register.o crdt_util.o g_counter.o g_counter_element.o  
	$(CC) $(CFLAGS) -c -o $@ ./orset/crdt_orset_rc.c
ctrip_crdt_zset.o: ctrip_crdt_zset.c utils.c  crdt_util.o crdt_statistics.o g_counter.o
	$(CC) $(CFLAGS) -c -o $@ ctrip_crdt_zset.c
crdt_orset_zset.o: orset/crdt_orset_zset.c   ctrip_crdt_zset.o crdt_util.o crdt_statistics.o crdt_util.h
	$(CC) $(CFLAGS) -c -o $@ orset/crdt_orset_zset.c
ctrip_rdt_expire.o:  ctrip_crdt_expire.c 
	$(CC) $(CFLAGS) -c -o $@ ctrip_crdt_expire.c
ctrip_stream_io.o: ctrip_stream_io.c ctrip_stream_io.h
	$(CC) $(CFLAGS) -c -o $@ ctrip_stream_io.c
ctirp_swap.o: ctrip_swap.c ctrip_swap.h
	$(CC) $(CFLAGS) -c -o $@ ctrip_swap.c

crdt.so: rmutil  g_counter_element.o g_counter.o  ctrip_orset_rc.o ctrip_crdt_register.o ctrip_crdt_zset.o crdt_orset_zset.o crdt_set.o crdt_orset_set.o crdt_statistics.o ctrip_rdt_expire.o crdt_pubsub.o crdt.o crdt_register.o  ctrip_crdt_hashmap.o ctrip_crdt_common.o ctrip_vector_clock.o util.o crdt_util.o crdt_lww_register.o crdt_lww_hashmap.o ctrip_stream_io.o ctrip_swap.o
	$(LD) -o $@  g_counter_element.o g_counter.o  ctrip_orset_rc.o  ctrip_crdt_register.o ctrip_crdt_zset.o crdt_orset_zset.o crdt_set.o crdt_orset_set.o  crdt_statistics.o ctrip_rdt_expire.o crdt_pubsub.o crdt.o crdt_register.o  ctrip_crdt_hashmap.o ctrip_crdt_common.o ctrip_vector_clock.o util.o crdt_util.o crdt_lww_register.o crdt_lww_hashmap.o ctrip_stream_io.o ctrip_swap.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil $(LIBC)

clean:
	rm -rf *.xo crdt.so *.o *.pyc *.so *.gcno *.gcda
	@(cd ./include && $(MAKE) clean)

# tests

# unit tests
test-lww-element: lww/crdt_lww_element.c lww/crdt_lww_element.h
	$(CC) -Wnullability-extension lww/crdt_lww_element.c -DLWW_ELEMENT_TEST_MAIN -lm -o /tmp/lww_element_test
	/tmp/lww_element_test

test-gcounter: gcounter/crdt_g_counter.c gcounter/crdt_g_counter.h
	$(CC) -Wnullability-extension gcounter/crdt_g_counter.c -DG_COUNTER_TEST_MAIN -lm -o /tmp/gcounter_test
	/tmp/gcounter_test

test_crdt: tests/unit/test_crdt.c
	$(CC) -Wall -o $@ $^ -lc -O0
	@(sh -c ./$@)
.PHONY: test_crdt


test_crdt_register: tests/unit/test_register.c tpl.c include/rmutil/librmutil.a crdt_register.c include/rmutil/sds.c
	$(CC) -Wall -o $@ $^ $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc -O0
	@(sh -c ./$@)
.PHONY: test_crdt_register


# integration tests
integration_test:
	$(PYTHONTEST) $(ALLMODULES)

# all tests
test: test_crdt integration_test
.PHONY: test

gcov:
	$(MAKE) REDIS_CFLAGS="-fprofile-arcs -ftest-coverage -DCOVERAGE_TEST" REDIS_LDFLAGS="-fprofile-arcs -ftest-coverage"
