#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=./deps
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=./deps/rmutil
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
    LIBC ?= -lc -ldl -lm -lrt
endif

ifneq (,$(filter aarch64 armv,$(uname_M)))
        CFLAGS+=-funwind-tables
else
ifneq (,$(findstring armv,$(uname_M)))
        CFLAGS+=-funwind-tables
endif
endif

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Wl,-Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -O0 -g -fPIC -std=gnu99 -Wno-psabi -w -Wno-address-of-packed-member-msse2 -DREDIS_MODULE_TARGET -DREDISMODULE_EXPERIMENTAL_API $(REDIS_CFLAGS)
ifeq ($(uname_S),Darwin)
	CFLAGS+= -DTCL_TEST -DDEBUG
	# CFLAGS+= -DDEBUG
endif

all: rmutil crdt.so
	@echo "BUILD SUCCESS VIA $(COMPILER)"

rmutil:
	$(MAKE) -C $(RMUTIL_LIBDIR)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@ 


CRDT_OBJ = gcounter/g_counter_element.o gcounter/g_counter.o  orset/crdt_orset_rc.o ctrip_crdt_register.o ctrip_crdt_zset.o orset/crdt_orset_zset.o crdt_set.o orset/crdt_orset_set.o crdt_statistics.o ctrip_crdt_expire.o crdt_pubsub.o crdt.o crdt_register.o  ctrip_crdt_hashmap.o ctrip_crdt_common.o ctrip_vector_clock.o util.o crdt_util.o lww/crdt_lww_register.o lww/crdt_lww_hashmap.o 
crdt.so: rmutil  $(CRDT_OBJ)
	$(CC) -o $@  $(CRDT_OBJ) $(SHOBJ_LDFLAGS) $(LIBS) ./deps/rmutil/librmutil.a $(LIBC)

clean:
	rm -rf *.xo crdt.so *.o *.pyc *.so *.gcno *.gcda ./lww/*.o ./orset/*.o ./gcounter/*.o ./tests/*.o
	@(cd ./deps && $(MAKE) clean)

# tests

# unit tests
test-gcounter: clean  gcounter/g_counter.c gcounter/g_counter.h util.o 
	$(MAKE) -C $(RMUTIL_LIBDIR) CRDT_CFLAGS=-DREDIS_MODULE_TEST
	$(CC)  -g gcounter/g_counter.c util.o -DCOUNTER_TEST_MAIN  -lm -o /tmp/gcounter_test -I$(RM_INCLUDE_DIR) -I$(RMUTIL_LIBDIR) ./deps/rmutil/librmutil.a
	/tmp/gcounter_test

test_crdt: tests/unit/test_crdt.c
	$(CC) -Wall -o $@ $^ -lc -O0 -DCOUNTER_TEST_MAIN -I$(RM_INCLUDE_DIR) -I$(RMUTIL_LIBDIR) ./deps/rmutil/librmutil.a
	@(sh -c ./$@)
.PHONY: test_crdt


test_crdt_register: tests/unit/test_register.c  crdt_register.c deps/rmutil/sds.c
	$(CC) -Wall -o $@ $^ $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc -O0 -DCOUNTER_TEST_MAIN  -lm -o /tmp/gcounter_test -I$(RM_INCLUDE_DIR) -I$(RMUTIL_LIBDIR) ./deps/rmutil/librmutil.a
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
