# -*- makefile -*-
#SUBDIRS = stages

# basic flags/libs every arch needs
AM_CFLAGS = -g3 -O2 -Wall -Wcast-align -Wextra -Wfloat-equal \
	-Wconversion -Wno-system-headers -D_REENTRANT 
AM_LDFLAGS = -Wl,$(BDB_HOME)/lib
LDADD = -lnsl -ldl -lm -lpthread -L$(BDB_HOME)/lib
INCLUDES = -I@top_srcdir@/include -I$(BDB_HOME)/include

# now specialize
if SPARC_MACHINE
LDADD += -lsocket -lrt
AM_LDFLAGS += -Wl,-R
else
LDADD += -lboost_regex
AM_LDFLAGS += -Wl,--rpath
endif

# encapsulates all basic classes needed in qpipe
lib_LIBRARIES = libqpipe.a

libqpipe_a_SOURCES = pthread_wrappers.cpp page.cpp tuple.cpp

# define programs here
bin_PROGRAMS = test_hash_join_stage
test_hash_join_stage_SOURCES = hash_join_stage.cpp test


