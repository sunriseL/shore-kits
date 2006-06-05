/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/fdump.h"
#include "qpipe_panic.h"
#include "trace/trace.h"
#include "tuple.h"



const char* fdump_packet_t::PACKET_TYPE = "FDUMP";



const char* fdump_stage_t::DEFAULT_STAGE_NAME = "FDUMP_STAGE";



fdump_stage_t::fdump_stage_t(packet_list_t* stage_packets,
			     stage_container_t* stage_container,
			     const char* stage_name)
    : stage_t(stage_name, stage_packets, stage_container)
{
    
    // Should not need to synchronize. We are not in the
    // stage_container_t's stage set until we are full
    // constructed. No other threads can touch our data.
    
    // Tail packet still has inputs intact. Move it into the
    // stage.
    fdump_packet_t* packet = (fdump_packet_t*)stage_packets->back();
    _input_buffer = packet->_input_buffer;
 

    if ( asprintf(&_filename, "%s", packet->_filename) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on stage: %s\n",
	      stage_name);
	QPIPE_PANIC();
    }

    
    _file = fopen(_filename, "w+");
    if ( _file == NULL ) {
	TRACE(TRACE_ALWAYS, "fopen() failed on %s\n", _filename);
	QPIPE_PANIC();
    }
}



fdump_stage_t::~fdump_stage_t() {
    free(_filename);
}



/**
 *  @brief Write the file specified by fdump_packet_t p.
 *
 *  @param p An fdump_packet_t.
 *
 *  @return 0 on success. Non-zero on unrecoverable error. The stage
 *  should terminate all queries it is processing.
 */

int fdump_stage_t::process() {

    
    _input_buffer->init_buffer();

    // read the file
    tuple_page_t* tuple_page;
    while ( (tuple_page = _input_buffer->get_page()) != NULL ) {
	if ( !tuple_page->fwrite_full_page(_file) ) {
	    TRACE(TRACE_ALWAYS, "fwrite_full_page() failed\n");
	    free(tuple_page);
	    return -1;
	}

	// When we invoked get_page(), we took ownership of the page
	// from the buffer. It is our responsibility to free it when
	// we're done with it.
	free(tuple_page);
    }

    return 0;
}



void fdump_stage_t::terminate_stage() {

    _input_buffer->close();

    if ( fclose(_file) ) {
	// We have already sent data, so no need to abort the queries.
	// We should log, but ignore fclose() errors
	TRACE(TRACE_ALWAYS, "Error closing %s input file %s\n", _stage_name, _filename);
    }
	      
    free(_filename);    
}
