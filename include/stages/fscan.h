/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _FSCAN_H
#define _FSCAN_H

#include <cstdio>

#include "tuple.h"
#include "packet.h"
#include "stage.h"
#include "dispatcher.h"
#include "trace/trace.h"
#include "qpipe_panic.h"



using namespace qpipe;



/* exported datatypes */


/**
 *  @brief
 */
struct fscan_packet_t : public packet_t {
  
    static const char* PACKET_TYPE;
    
    char* _filename;

    /**
     *  @brief FSCAN packet constructor. Raw FSCANS should not be
     *  mergeable based on not mergeable. They should be merged at the
     *  meta-stage.
     */
    fscan_packet_t(char* packet_id,
		   tuple_buffer_t* out_buffer,
		   tuple_filter_t* filt,
		   tuple_buffer_t* client_buffer,
		   const char* filename)
	: packet_t(packet_id, PACKET_TYPE, out_buffer, filt, client_buffer, false)
    {
	if ( asprintf(&_filename, "%s", filename) == -1 ) {
	    TRACE(TRACE_ALWAYS, "asprintf() failed on filename %s\n",
		  filename);
	    QPIPE_PANIC();
	}
    }
    
    virtual ~fscan_packet_t() {
	// may have deleted _filename earlier in terminate_inputs()
	if (_filename != NULL)
	    free(_filename);
    }

    virtual void terminate_inputs() {

	// No input buffers to close. As for the input file, the
	// meta-stage is responsible for deleting the file when it
	// knows we are all done.

	// No longer any need to store filename separately. Set it to
	// NULL so the destructor can know and not do a double-free().
	free(_filename);
	_filename = NULL;
    }

};



/**
 *  @brief FSCAN stage. Reads tuples from a file on the local file
 *  system.
 */
class fscan_stage_t : public stage_t {

public:

    static const char* DEFAULT_STAGE_NAME;

    fscan_stage_t() { }
    
    virtual ~fscan_stage_t() { }
    
protected:

    virtual int process_packet();
    
    int read_file(adaptor_t* adaptor, FILE* file, tuple_page_t* tuple_page);
};



#endif
