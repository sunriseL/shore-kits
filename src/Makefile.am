# -*- makefile -*-


INCLUDES = -I$(top_srcdir)/include -I$(top_srcdir)/include/util



STAGES = \
   stages/fscan.cpp \
   stages/fdump.cpp \
   stages/tscan.cpp \
   stages/aggregate.cpp \
   stages/func_call.cpp \
   stages/merge.cpp \
   stages/sort.cpp

#  stages/bnl_caching_join.cpp


TRACE = \
   trace/trace.cpp \
   trace/trace_force.cpp \
   trace/trace_print_pthread.cpp \
   trace/trace_stream.cpp

DISPATCHER = \
   dispatcher.cpp

UTIL = \
   util/static_hash_map.cpp \
   util/hash_functions.cpp \
   util/shuffle.cpp \
   util/time_util.cpp




# encapsulates all basic classes needed in qpipe
lib_LIBRARIES = libqpipe.a

libqpipe_a_SOURCES = \
   thread.cpp \
   page.cpp \
   tuple.cpp \
   packet.cpp \
   stage_container.cpp \
   $(TRACE) \
   $(DISPATCHER) \
   $(UTIL)  \
   $(STAGES)
