// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/aggregate.h"
#include "trace/trace.h"
#include "utils.h"



void aggregate_packet_t::terminate() {
    input_buffer->close();
}



int aggregate_stage_t::process_packet(packet_t *p) {

    aggregate_packet_t *packet = (aggregate_packet_t *)p;


    // automatically close the input buffer when this function exits
    buffer_guard_t input = packet->input_buffer;
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
	TRACE(TRACE_TUPLE_FLOW, "get_tuple() returned a new tuple with size %d\n", src.size);
	if ( aggregate->aggregate(dest, src) )
	    if ( output(packet, dest) )
		return 1;
    }
    
    // collect aggregate results
    if ( aggregate->eof(dest) )
	if ( output(packet, dest) )
	    return 1;
    
    return 0;
}
