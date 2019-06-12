#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=./include
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=./include/rmutil
endif

PYTHONTEST=python -m unittest -v -b
ALLMODULES=tests.register_test.CrdtRegisterTest

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

crdt_register.o: crdt_register.c utils.h
	$(CC) $(CFLAGS) -c -o $@ crdt_register.c

crdt.so: rmutil crdt.o crdt_register.o ctrip_crdt_common.o
	$(LD) -o $@ crdt.o crdt_register.o ctrip_crdt_common.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc

clean:
	rm -rf *.xo *.so *.o *.pyc


# tests

# unit tests
test_crdt: tests/unit/test_crdt.c
	$(CC) -Wall -o $@ $^ -lc -O0
	@(sh -c ./$@)
.PHONY: test_crdt


# integration tests
integration_test:
	$(PYTHONTEST) $(ALLMODULES)

# all tests
test: test_crdt integration_test
.PHONY: test
