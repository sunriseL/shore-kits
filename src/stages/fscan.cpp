/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/fscan.h"
#include "qpipe_panic.h"
#include "trace/trace.h"
#include "tuple.h"



const char* fscan_packet_t::PACKET_TYPE = "FSCAN";



const char* fscan_stage_t::DEFAULT_STAGE_NAME = "FSCAN_STAGE";



fscan_stage_t::fscan_stage_t(packet_list_t* stage_packets,
			     stage_container_t* stage_container,
			     const char* stage_name)
    : stage_t(stage_name, stage_packets, stage_container)
{
    
    // Should not need to synchronize. We are not in the
    // stage_container_t's stage set until we are full
    // constructed. No other threads can touch our data.
    
    // Tail packet still has inputs intact. Move it into the
    // stage.
    fscan_packet_t* packet = (fscan_packet_t*)stage_packets->back();
    

    if ( asprintf(&_filename, "%s", packet->_filename) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on stage: %s\n",
	      stage_name);
	QPIPE_PANIC();
    }

    
    _file = fopen(_filename, "r");
    if ( _file == NULL ) {
	TRACE(TRACE_ALWAYS, "fopen() failed on %s\n", _filename);
	QPIPE_PANIC();
    }
    

    _tuple_page =
	tuple_page_t::alloc(packet->output_buffer->tuple_size, malloc);
    if ( _tuple_page == NULL ) {
	TRACE(TRACE_ALWAYS, "tuple_page_t::alloc() failed\n");
	QPIPE_PANIC();
    }
}



fscan_stage_t::~fscan_stage_t() {
    // All resources allocated in the constructor should have been
    // freed in terminate_stage().
}



/**
 *  @brief Read the file specified by fscan_packet_t p.
 *
 *  @return 0 on success. Non-zero on unrecoverable error. The stage
 *  should terminate all queries it is processing.
 */

int fscan_stage_t::process() {

    // If we have FSCAN working sharing enabled, we could be accepting
    // packets now. We would like to avoid doing so when we start
    // sending tuples to output().
    bool accepting_packets = true;

    while (1)
    {
	// read the next page of tuples
	size_t count = _tuple_page->fread(_file);
	if ( count == 0 ) {
	    // done reading the file
	    return 0;
	}
	
	if ( count < _tuple_page->capacity() ) {
	    // short read! treat this as an error!
	    TRACE(TRACE_ALWAYS, "tuple_page.fread() read %z/%z tuples\n",
		  count,
		  _tuple_page->capacity());
	    return -1;
	}


	// We must stop accepting packets as soon as we output() any
	// tuples. Any packet accepted after this point will miss some
	// of the data we are reading.
	if (accepting_packets) {
	    stop_accepting_packets();
	    accepting_packets = false;
	}
	   

	// output() each tuple in the page
	tuple_page_t::iterator it;
	for (it = _tuple_page->begin(); it != _tuple_page->end(); ++it) {
	    tuple_t current_tuple = *it;
	    output_t output_ret = output(current_tuple);
	    switch (output_ret) {
	    case OUTPUT_RETURN_CONTINUE:
		continue;
	    case OUTPUT_RETURN_STOP:
		return 0;
	    case OUTPUT_RETURN_ERROR:
		return -1;
	    default:
		TRACE(TRACE_ALWAYS, "output() return unrecognized value %d\n",
		      output_ret);
		QPIPE_PANIC();
	    }
	}

	// continue to next page
    }

    // control never reaches here
    return -1;
}



void fscan_stage_t::terminate_stage() {

    if ( fclose(_file) ) {
	// We have already sent data, so no need to abort the queries.
	// We should log, but ignore fclose() errors
	TRACE(TRACE_ALWAYS, "Error closing %s input file %s\n", _stage_name, _filename);
    }
	      
    free(_filename);    
    free(_tuple_page);
}
