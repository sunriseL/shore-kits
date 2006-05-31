/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/scalar_aggregate.h"
#include "trace/trace.h"



void scalar_aggregate_packet_t::terminate() {
    input_buffer->close();
}



/**
 *
 *
 */

int scalar_aggregate_stage_t::process_packet(packet_t* p)
{
    scalar_aggregate_packet_t* packet = (scalar_aggregate_packet_t*)p;

    // automatically close the input buffer when this function exits
    buffer_closer_t input = packet->input_buffer;
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
	    output(packet, dest);
    }
    
    // collect aggregate results
    if ( aggregate->eof(dest) )
	output(packet, dest);

    return 0;
}
