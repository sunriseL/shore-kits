/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file trace_print_pthread.h
 *
 *  @brief Exports trace_print_pthread().
 *
 *  @bug See trace_print_pthread.cpp.
 */
#ifndef _TRACE_PRINT_PTHREAD_H
#define _TRACE_PRINT_PTHREAD_H

#include <pthread.h> /* for pthread_t */
#include <cstdio>   /* for FILE* */



/* exported functions */

void trace_print_pthread(FILE* out_stream, pthread_t thread);



#endif
