# -*- makefile -*-
#SUBDIRS = stages

INCLUDES = -I$(top_srcdir)/include

# define programs here
bin_PROGRAMS = test_trace test_scalar_aggregate_stage test_aggregate_stage test_tscan_stage

test_trace_SOURCES = test_trace.cpp
test_trace_LDADD = ../src/libqpipe.a

test_scalar_aggregate_stage_SOURCES = test_scalar_aggregate_stage.cpp
test_scalar_aggregate_stage_LDADD = ../src/libqpipe.a

test_aggregate_stage_SOURCES = test_aggregate_stage.cpp
test_aggregate_stage_LDADD = ../src/libqpipe.a

test_tscan_stage_SOURCES = test_tscan_stage.cpp
test_tscan_stage_LDADD = ../src/libqpipe.a

