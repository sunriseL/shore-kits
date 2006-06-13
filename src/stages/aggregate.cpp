// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/aggregate.h"
#include "qpipe_panic.h"
#include "dispatcher.h"
#include "trace/trace.h"
#include "tuple.h"
#include "utils.h"



const char* aggregate_packet_t::PACKET_TYPE = "AGGREGATE";



const char* aggregate_stage_t::DEFAULT_STAGE_NAME = "AGGREGATE_STAGE";



stage_t::result_t aggregate_stage_t::process_packet() {


    adaptor_t* adaptor = _adaptor;
    aggregate_packet_t* packet = (aggregate_packet_t*)adaptor->get_packet();

    
    tuple_aggregate_t* aggregate = packet->_aggregator;
    tuple_buffer_t* input_buffer = packet->_input_buffer;
    dispatcher_t::dispatch_packet(packet->_input);


    // input buffer owns src
    tuple_t src;


    // "I" own dest, so allocate space for it on the stack
    size_t dest_size = packet->_output_buffer->tuple_size;
    char dest_data[dest_size];
    tuple_t dest(dest_data, dest_size);


    while(input_buffer->get_tuple(src)) {
	if ( aggregate->aggregate(dest, src) ) {
	    result_t output_ret = adaptor->output(dest);
	    if (output_ret)
		return output_ret;
	}
    }

    
    // collect aggregate results
    if ( aggregate->eof(dest) ) {
	result_t output_ret = adaptor->output(dest);
	if (output_ret)
	    return output_ret;
    }


    return stage_t::RESULT_STOP;
}
