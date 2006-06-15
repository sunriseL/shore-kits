/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file trace.h
 *
 *  @brief Tracing module.
 *
 *  @bug See trace.cpp.
 */
#ifndef _TRACE_H
#define _TRACE_H

#include <cstdarg>      /* for varargs */
#include <stdint.h>      /* for uint32_t */
#include "trace_types.h" /* export trace types */



/* exported functions */

void trace_(int trace_type,
	    const char* filename, int line_num, const char* function_name,
	    char* format, ...);



/* exported data structures */

extern uint32_t trace_current_setting;



/* exported macros */


/**
 *  @def TRACE
 *
 * @brief Other modules in our program use this macro for
 * reporting. We can use preprocessor macros like __FILE__ and
 * __LINE__ to provide more information in the output messages. We can
 * also remove all messages at compile time by changing the
 * definition.
 *
 * @param type The type of the message. This is really a bit vector of
 * all times when this message should be printed. If the current debug
 * setting contains any one of these bits, we print the message.
 *
 * @param format The format string for the printing. Format follows
 * that of printf(3).
 *
 * @param rest Optional arguments that can printed (see printf(3)
 * definition for more details).
 *
 * @return void
 */

#define TRACE(type, format, rest...) trace_(type, __FILE__, __LINE__, __FUNCTION__, format, ##rest)



#endif // _TRACE_H
