/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _FSCAN_H
#define _FSCAN_H

#include <cstdio>

#include "db_cxx.h"
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

    FILE* _file;
    
    /**
     *  @brief FSCAN packet constructor. Raw FSCANS should not be
     *  mergeable based on not mergeable. They should be merged at the
     *  meta-stage.
     */
    fscan_packet_t(char* packet_id,
		   tuple_buffer_t* out_buffer,
		   tuple_filter_t* filt,
		   const char* filename)
	: packet_t(packet_id, PACKET_TYPE, out_buffer, filt, false)
    {
	if ( asprintf(&_filename, "%s", filename) == -1 ) {
	    TRACE(TRACE_ALWAYS, "asprintf() failed on filename %s\n",
		  filename);
	    QPIPE_PANIC();
	}
    }
    
    virtual ~fscan_packet_t() {
	free(_filename);
    }

    virtual void terminate_inputs() {
	// No input buffers to close. As for the input file, the
	// meta-stage is responsible for deleting the file when it
	// knows we are all done.
    }

};



/**
 *  @brief FSCAN stage. Reads tuples from a file on the local file
 *  system.
 */
class fscan_stage_t : public stage_t {

protected:

    static const char* DEFAULT_STAGE_NAME;

    char* _filename;

    tuple_page_t* _tuple_page;

    FILE* _file;

public:
    fscan_stage_t(packet_list_t* stage_packets,
		  stage_container_t* stage_container,
		  const char* stage_name=DEFAULT_STAGE_NAME);
    
    virtual ~fscan_stage_t();
    
protected:
    virtual int  process();
    virtual void terminate_stage();
};



#endif
