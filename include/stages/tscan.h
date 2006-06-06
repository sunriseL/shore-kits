// -*- mode:C++ c-basic-offset:4 -*-
#ifndef __TSCAN_H
#define __TSCAN_H

# include "db_cxx.h"
# include "tuple.h"
# include "packet.h"
# include "stage.h"
# include "dispatcher.h"



using namespace qpipe;



/* exported functions */


/**
 *@brief Packet definition for the Tscan stage
 */
struct tscan_packet_t : public packet_t {

    static const char* PACKET_TYPE;
    

    /**
     *  @brief FSCAN packet constructor. Raw FSCANS should not be
     *  mergeable based on not mergeable. They should be merged at the
     *  meta-stage.
     */
    tscan_packet_t(char* packet_id,
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
    
    virtual ~tscan_packet_t() {
    }

    virtual void terminate_inputs() {

	// No input buffers to close. As for the input file, the
	// meta-stage is responsible for deleting the file when it
	// knows we are all done.

	// No longer any need to store filename separately. Set it to
	// NULL so the destructor can know and not do a double-free().
	free(_filename);
	_filename = NULL;

	// As for the output file, the meta-stage is responsible for
	// deleting the file when it knows we are all done.
    }

};



/**
 *  @brief Table scan stage that reads tuples from the storage manager.
 */

class tscan_stage_t : public stage_t {
public:
    tscan_stage_t()
        : stage_t(TSCAN_STAGE_NAME)
    {
      // register with the dispatcher
      dispatcher_t::register_stage(TSCAN_PACKET_TYPE, this);
    }

protected:
    virtual int process_packet(packet_t *packet);
};



#endif
