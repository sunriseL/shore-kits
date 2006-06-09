// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/aggregate.h"
#include "qpipe_panic.h"
#include "trace/trace.h"
#include "tuple.h"
#include "utils.h"


const char* aggregate_packet_t::PACKET_TYPE = "AGGREGATE";

/** @fn    : process_packet
 *  @brief : 
 */

int aggregate_stage_t::process_packet() {

    adaptor_t* adaptor = _adaptor;
    aggregate_packet_t* packet = (aggregate_packet_t*)adaptor->get_packet();

    // automatically close the input buffer when this function exits
    buffer_guard_t input(packet->input_buffer);
    tuple_aggregate_t* aggregate = packet->aggregate;

    // input buffer owns src
    tuple_t src;

    // "I" own dest, so allocate space for it on the stack
    size_t dest_size = packet->output_buffer->tuple_size;
    char dest_data[dest_size];
    tuple_t dest(dest_data, dest_size);


    // we update the aggregate state by invoking select()
    TRACE(TRACE_TUPLE_FLOW, "Going to call get_tuple()\n");

    while(input->get_tuple(src)) {
	//	TRACE(TRACE_TUPLE_FLOW, "get_tuple() returned a new tuple with size %d\n", src.size);

	if ( aggregate->aggregate(dest, src) ) {
	    
	    stage_t::adaptor_t::output_t output_ret = adaptor->output(src);
	
	    switch (output_ret) {
	    case stage_t::adaptor_t::OUTPUT_RETURN_CONTINUE:
		continue;
	    case stage_t::adaptor_t::OUTPUT_RETURN_STOP:
		return 0;
	    case stage_t::adaptor_t::OUTPUT_RETURN_ERROR:
		return -1;
	    default:
		TRACE(TRACE_ALWAYS, "adaptor->output() return unrecognized value %d\n",
		      output_ret);
		QPIPE_PANIC();
	    }	 
	}
    }
    
    // collect aggregate results
    if ( aggregate->eof(dest) ) {
	    
	stage_t::adaptor_t::output_t output_ret = adaptor->output(src);
	
	switch (output_ret) {
	case stage_t::adaptor_t::OUTPUT_RETURN_CONTINUE:
	    break;
	case stage_t::adaptor_t::OUTPUT_RETURN_STOP:
	    return 0;
	case stage_t::adaptor_t::OUTPUT_RETURN_ERROR:
	    return -1;
	default:
	    TRACE(TRACE_ALWAYS, "adaptor->output() return unrecognized value %d\n",
		  output_ret);
	    QPIPE_PANIC();
	}	 
    }
        
    return 0;
}
