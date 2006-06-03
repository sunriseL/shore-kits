# -*- makefile -*-
#SUBDIRS = stages

INCLUDES = -I$(top_srcdir)/include

LDADD = ../src/libqpipe.a

# define programs here
bin_PROGRAMS = test_trace \
	       test_scalar_aggregate_stage \
	       test_aggregate_stage \
	       test_tscan_stage \
	       test_merge_stage \
	       test_q6

test_trace_SOURCES = test_trace.cpp
test_scalar_aggregate_stage_SOURCES = test_scalar_aggregate_stage.cpp
test_aggregate_stage_SOURCES = test_aggregate_stage.cpp
test_tscan_stage_SOURCES = test_tscan_stage.cpp
test_merge_stage_SOURCES = test_merge_stage.cpp
test_q6_SOURCES = test_q6.cpp

