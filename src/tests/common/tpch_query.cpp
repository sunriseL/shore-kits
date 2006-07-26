/* -*- mode:C++; c-basic-offset:4 -*- */

#include "tests/common/tpch_query.h"

#include "trace.h"
#include "engine/thread.h"
#include "engine/util/stopwatch.h"
#include "engine/dispatcher/dispatcher_policy_os.h"
#include "workload/tpch/tpch_db.h"



query_info_t query_init(int argc, char* argv[]) {

    thread_init();
    db_open();

    // parse command line args
    if ( argc < 2 ) {
	TRACE(TRACE_ALWAYS, "Usage: %s <num iterations>\n", argv[0]);
	exit(-1);
    }

    int num_iterations = atoi(argv[1]);
    if ( num_iterations == 0 ) {
	TRACE(TRACE_ALWAYS, "Invalid iterations per client %s\n", argv[1]);
	exit(-1);
    }

    query_info_t info;
    info.num_iterations = num_iterations;
    info.dispatcher_policy = new dispatcher_policy_os_t();

    return info;
}



void query_main(query_info_t& info, driver_t* driver) {

    TRACE_SET(TRACE_ALWAYS | TRACE_STATISTICS);

    for(int i=0; i < info.num_iterations; i++) {
        stopwatch_t timer;
        driver->submit(info.dispatcher_policy);
        TRACE(TRACE_STATISTICS, "Query executed in %.3lf s\n", timer.time());
    }

    db_close();
}
