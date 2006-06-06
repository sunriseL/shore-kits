/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stage.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include "utils.h"
#include "stage_container.h"


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <cstdio>
#include <cstring>



// include me last!!!
#include "namespace.h"






/**
 *  @brief packet should already have been removed from
 *  _staged_packets.
 */
void stage_t::destroy_completed_packet(packet_t* packet) {
    packet->output_buffer->send_eof();
    // TODO check for send_eof() error and delete output
    // buffer
    delete packet;
}



/**
 *  @brief packet should already have been removed from
 *  _staged_packets.
 */
void stage_t::terminate_packet_query(packet_t* packet) {
    packet->output_buffer->send_eof();
    // TODO check for send_eof() error and delete output
    // buffer
    delete packet;
}



void stage_container_t::stage_adaptor_t:: abort_query() {
    // 
    _packet->client_buffer->terminate();
}


	
void stage_t::run() {


    if ( process() ) {
	TRACE(TRACE_ALWAYS, "process() failed!\n");
    }

	
    // destroy local state and close input buffer(s)
    terminate_stage();
    

}



#include "namespace.h"
