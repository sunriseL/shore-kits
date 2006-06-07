# -*- makefile -*-


INCLUDES = -I$(top_srcdir)/include



STAGES = \
   stages/sort.cpp \
   stages/merge.cpp \
   stages/fdump.cpp \
   stages/fscan.cpp \
   stages/bnl_caching_join.cpp

#\
   stages/scalar_aggregate.cpp \
   stages/aggregate.cpp \
   stages/merge.cpp \
   stages/tscan.cpp


TRACE = \
   trace/trace.cpp \
   trace/trace_force.cpp \
   trace/trace_print_pthread.cpp \
   trace/trace_stream.cpp

DISPATCHER = \
   dispatcher.cpp

UTIL = \
   util/static_hash_map.cpp \
   util/hash_functions.cpp



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
