/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _FDUMP_H
#define _FDUMP_H

#include <cstdio>

#include "db_cxx.h"
#include "tuple.h"
#include "packet.h"
#include "stage.h"
#include "dispatcher/dispatcher.h"



using namespace qpipe;



/* exported constants */

#define FDUMP_STAGE_NAME  "FDUMP_STAGE"
#define FDUMP_PACKET_TYPE "FDUMP"



/* exported functions */


/**
 *  @brief
 */
struct fdump_packet_t : public packet_t {
    
    tuple_buffer_t* input_buffer;
    
    const char* _filename;
    
    bool mergeable;

    /**
     *  @brief FDUMP packet constructor.
     *
     *  @param packet_id The packet identifier.
     *
     *  @param out_buffer No real data will be transmitted through
     *  this buffer. The worker thread processing this packet will
     *  simply close this buffer when the file has been completely
     *  sent to disk.
     *
     *  @param in_buffer 
     *
     *  FDUMP should not really need a tuple filter. All filtering for
     *  the query can be moved to the stage feeding the FDUMP. For
     *  this reason, we simple use the null-filtering capabilities
     *  provided by the tuple_filter_t base class.
     */    
    fdump_packet_t(char* packet_id,
		   tuple_buffer_t* out_buffer,
		   tuple_buffer_t* in_buffer,
		   const char*     filename)
	: packet_t(packet_id, FDUMP_PACKET_TYPE, out_buffer, new tuple_filter_t()),
	  input_buffer(in_buffer),
	  _filename(filename)
    {
    }

    virtual void terminate();
};



/**
 *  @brief FDUMP stage. Creates a (hopefully) temporary file on the
 *  local file system.
 */
class fdump_stage_t : public stage_t {

public:
    fdump_stage_t()
	: stage_t(FDUMP_STAGE_NAME)
    {
	// register with the dispatcher
	dispatcher_t::register_stage(FDUMP_PACKET_TYPE, this);	
    }
    
protected:
  virtual int process_packet(packet_t* packet);
 
  
};



#endif
