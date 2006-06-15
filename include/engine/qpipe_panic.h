/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _QPIPE_PANIC_H
#define _QPIPE_PANIC_H

#include <cstdlib>       /* for abort() */
#include "trace/trace.h" /* for TRACE */



/* exported macros */

#define QPIPE_PANIC() { TRACE(TRACE_ALWAYS, "QPIPE PANIC\n"); abort(); }



#endif
