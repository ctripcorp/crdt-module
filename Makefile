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

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -O0 -g -fPIC -lc -lm -std=gnu99  -DREDIS_MODULE_TARGET -DREDISMODULE_EXPERIMENTAL_API

all: rmutil crdt.so

rmutil:
	$(MAKE) -C $(RMUTIL_LIBDIR)

crdt.o: crdt.c version.h
	$(CC) $(CFLAGS) -c -o $@ crdt.c
crdt_util.o: crdt.o crdt_util.h
	$(CC) $(CFLAGS) -c -o $@ crdt_util.c
crdt_register.o: crdt_register.c utils.c crdt_util.o
	$(CC) $(CFLAGS) -c -o $@ crdt_register.c

ctrip_crdt_hashmap.o: ctrip_crdt_hashmap.c utils.c crdt_util.o
	$(CC) $(CFLAGS) -c -o $@ ctrip_crdt_hashmap.c

crdt.so: rmutil crdt.o crdt_register.o ctrip_crdt_hashmap.o ctrip_crdt_common.o ctrip_vector_clock.o util.o crdt_util.o
	$(LD) -o $@ crdt.o crdt_register.o ctrip_crdt_hashmap.o ctrip_crdt_common.o ctrip_vector_clock.o util.o crdt_util.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc

clean:
	rm -rf *.xo *.so *.o *.pyc


# tests

# unit tests
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
