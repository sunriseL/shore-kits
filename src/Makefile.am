# -*- makefile -*-


INCLUDES = -I$(top_srcdir)/include



STAGES = \
   stages/scalar_aggregate.cpp \
   stages/aggregate.cpp \
   stages/tscan.cpp \
   stages/fscan.cpp

TRACE = \
   trace/trace.cpp \
   trace/trace_force.cpp \
   trace/trace_print_pthread.cpp \
   trace/trace_stream.cpp

DISPATCHER = \
   dispatcher/dispatcher.cpp

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
   stage.cpp \
   $(STAGES) \
   $(TRACE) \
   $(DISPATCHER) \
   $(UTIL)

# define programs here
#bin_PROGRAMS = test_hash_join_stage
#test_hash_join_stage_SOURCES = hash_join_stage.cpp test
