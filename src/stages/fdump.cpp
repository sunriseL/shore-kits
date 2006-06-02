/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/fdump.h"
#include "qpipe_panic.h"
#include "trace/trace.h"
#include "tuple.h"



/* internal helper functions */

void fdump_packet_t::terminate() {
    
}



/**
 *  @brief Write the file specified by fdump_packet_t p.
 *
 *  @param p An fdump_packet_t.
 */

int fdump_stage_t::process_packet(packet_t* p) {

    fdump_packet_t* packet = (fdump_packet_t*)p;
    tuple_buffer_t* input_buffer = packet->input_buffer;

    
    // acquire resources
    FILE* file = fopen(packet->_filename, "w+");
    if ( file == NULL ) {
	TRACE(TRACE_ALWAYS, "fopen() failed on %s\n",
	      packet->_filename);

	// need to kill query...

	return -1;
    }



    // read the file
    tuple_page_t* tuple_page;
    while ( (tuple_page = input_buffer->get_page()) != NULL ) {
	size_t count = tuple_page->fwrite(file);
	if ( count < tuple_page->capacity() ) {
	    // Short write! This is an error!
	    TRACE(TRACE_ALWAYS, "tuple_page.fwrite() wrote %z/%z tuples\n",
		  count,
		  tuple_page->capacity());
	    free(tuple_page);

	    // need to kill query...
	    
	    return -1;
	}
	free(tuple_page);
    }


    // release resources
    if ( fclose(file) ) {
	TRACE(TRACE_ALWAYS, "fclose() failed for file %s\n",
	      packet->_filename);

	// need to kill query...

	return -1;
    }
    
    
    // just return and let process_next_packet() deliver EOF?
    return 0;
}
