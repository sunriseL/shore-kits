# -*- makefile -*-
#SUBDIRS = stages

INCLUDES = -I$(top_srcdir)/include

# define programs here
bin_PROGRAMS = test_aggregate_stage test_trace

test_aggregate_stage_SOURCES = test_aggregate_stage.cpp
test_aggregate_stage_LDADD = ../src/libqpipe.a

test_trace_SOURCES = test_trace.cpp
test_trace_LDADD = ../src/libqpipe.a
