
# set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../../include
endif
CRDT_CFLAGS ?=
CFLAGS ?= -g -fPIC -O3 -std=gnu99 -Wall -Wno-unused-function $(CRDT_CFLAGS)
CFLAGS += -I$(RM_INCLUDE_DIR)


CC=gcc

OBJS=ziplist.o util.o strings.o sds.o vector.o periodic.o dict.o adlist.o zskiplist.o siphash.o

all: librmutil.a

clean:
	rm -rf *.o *.a

librmutil.a: $(OBJS)
	ar rcs $@ $^

test_vector: test_vector.o vector.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	@(sh -c ./$@)
.PHONY: test_vector

test_periodic: test_periodic.o periodic.o
	$(CC) -Wall -o $@ $^ -lc -lpthread -O0
	@(sh -c ./$@)
.PHONY: test_periodic
	
test: test_periodic test_vector
.PHONY: test
