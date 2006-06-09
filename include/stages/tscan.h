/* -*- mode:C++; c-basic-offset:4 -*- */
#ifndef __TSCAN_H
#define __TSCAN_H

# include "db_cxx.h"
# include "tuple.h"
# include "packet.h"
# include "stage.h"



using namespace qpipe;

/**
 *@brief Packet definition for the Tscan stage
 */

struct tscan_packet_t : public packet_t {

    static const char* PACKET_TYPE;

    Db* _db;
   
    /**
     *  @brief TSCAN packet constructor. Currently no merging.
     */
    tscan_packet_t(char* packet_id,
		   tuple_buffer_t* out_buffer,
		   tuple_filter_t* filt,
		   tuple_buffer_t* client_buffer,
		   Db* db)
	: packet_t(packet_id, PACKET_TYPE, out_buffer, filt, client_buffer, false),
	  _db(db)
    {
    }
    
    virtual ~tscan_packet_t() {
    }

    virtual void terminate_inputs() {
	// No input buffers to close. We will not be deleting the
	// input table.
    }

};



/**
 *  @brief Table scan stage that reads tuples from the storage manager.
 */

class tscan_stage_t : public stage_t {

public:

    static const char* DEFAULT_STAGE_NAME;

    tscan_stage_t() { }

    virtual ~tscan_stage_t() { }

protected:

    virtual result_t process_packet();
};



#endif
