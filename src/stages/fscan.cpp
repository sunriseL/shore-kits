/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/fscan.h"
#include "qpipe_panic.h"
#include "trace/trace.h"
#include "tuple.h"



/* internal helper functions */

void fscan_packet_t::terminate() {
    
}



/**
 *  @brief Read the file specified by fscan_packet_t p.
 *
 *  @param p An fscan_packet_t.
 */

int fscan_stage_t::process_packet(packet_t* p) {

    fscan_packet_t* packet = (fscan_packet_t*)p;
    int return_value = 0;

    
    // acquire resources
    FILE* file = fopen(packet->_filename, "r");
    if ( file == NULL ) {
	TRACE(TRACE_ALWAYS, "fopen() failed on %s\n",
	      packet->_filename);

	// need to kill query...

	return -1;
    }


    tuple_page_t* tuple_page =
	tuple_page_t::alloc(packet->_tuple_size, malloc);
    if ( tuple_page == NULL ) {
	TRACE(TRACE_ALWAYS, "tuple_page_t::alloc() failed\n");
	if ( fclose(file) )
	    TRACE(TRACE_ALWAYS, "fclose() failed\n");
    
	// need to kill query...

	return -1;
    }


    // read the file
    if ( read_file(packet, file, tuple_page) ) {
	TRACE(TRACE_ALWAYS, "process_file() failed for file %s\n",
	      packet->_filename);
	return_value = -1;
    }

  
    // release resources
    free(tuple_page);

    if ( fclose(file) ) {
	TRACE(TRACE_ALWAYS, "fclose() failed for file %s\n",
	      packet->_filename);
	return_value = -1;
    }

    
    // if return_value == -1, need to kill query...
    
    
    return return_value;
}



int fscan_stage_t::read_file(packet_t* packet, FILE* file, tuple_page_t* tuple_page) {

    while (1)
    {
	// read the next page of tuples
	size_t count = tuple_page->fread(file);
	if ( count == 0 ) {
	    // done reading the file
	    return 0;
	}
	
	if ( count < tuple_page->capacity() ) {
	    // short read! treat this as an error!
	    TRACE(TRACE_ALWAYS, "tuple_page.fread() read %z/%z tuples\n",
		  count,
		  tuple_page->capacity());
	    return -1;
	}

	// output() each tuple in the page
	tuple_page_t::iterator it;
	for (it = tuple_page->begin(); it != tuple_page->end(); ++it) {
	    tuple_t current_tuple = *it;
	    if ( output(packet, current_tuple) ) {
		TRACE(TRACE_ALWAYS, "output() failed\n");
		return -1;
	    }
	}
    }
}
