
/** @file trace_stream.h
 *
 *  @brief Exports trace_stream().
 *
 *  @bug See trace_stream.cpp.
 */
#ifndef _TRACE_STREAM_H
#define _TRACE_STREAM_H

#include <cstdio>   /* for FILE* */
#include <cstdarg>  /* for va_list datatype */



/* exported functions */

void trace_stream(FILE* out_stream,
		  const char* filename, int line_num, const char* function_name,
		  char* format, va_list ap);



#endif
