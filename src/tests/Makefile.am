# -*- makefile -*-
#SUBDIRS = stages

INCLUDES = -I$(top_srcdir)/include

# define programs here
bin_PROGRAMS = test_aggregate_stage
test_aggregate_stage_SOURCES = test_aggregate_stage.cpp
test_aggregate_stage_LDADD = ../libqpipe.a ../stages/libstages.a ../trace/libtrace.a
