# -*- makefile -*-
#SUBDIRS = stages

INCLUDES = -I$(top_srcdir)/include -I$(top_srcdir)/include/trace
# encapsulates all basic classes needed in qpipe
lib_LIBRARIES = libtrace.a

libtrace_a_SOURCES = trace.cpp trace_force.cpp trace_print_pthread.cpp \
	trace_stream.cpp

# define programs here
#bin_PROGRAMS = test_hash_join_stage
#test_hash_join_stage_SOURCES = hash_join_stage.cpp test


