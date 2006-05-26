# -*- makefile -*-
SUBDIRS = trace stages

INCLUDES = -I$(top_srcdir)/include

# encapsulates all basic classes needed in qpipe
lib_LIBRARIES = libqpipe.a

libqpipe_a_SOURCES = thread.cpp page.cpp tuple.cpp #packet.cpp stage_queue.cpp stage.cpp
libqpipe_a_LIBADD = trace/libtrace.a

# define programs here
#bin_PROGRAMS = test_hash_join_stage
#test_hash_join_stage_SOURCES = hash_join_stage.cpp test


