/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _FSCAN_H
#define _FSCAN_H

#include <cstdio>

#include "db_cxx.h"
#include "tuple.h"
#include "packet.h"
#include "stage.h"
#include "dispatcher.h"



using namespace qpipe;



/* exported constants */

#define FSCAN_STAGE_NAME  "FSCAN_STAGE"
#define FSCAN_PACKET_TYPE "FSCAN"



/* exported functions */


/**
 *  @brief
 */
struct fscan_packet_t : public packet_t {
  
  const char* _filename;
  int         _tuple_size;
  
  bool mergeable;

  fscan_packet_t(char*  packet_id,
		 tuple_buffer_t* out_buffer,
		 tuple_filter_t* filt,
		 const char* filename,
		 int tuple_size)
    : packet_t(packet_id, FSCAN_PACKET_TYPE, out_buffer, filt),
       _filename(filename),
       _tuple_size(tuple_size)
  {
  }

  virtual void terminate();
};



/**
 *@brief File scan stage that reads tuples from the storage manager.
 */
class fscan_stage_t : public stage_t {

public:
  fscan_stage_t()
    : stage_t(FSCAN_STAGE_NAME)
    {
	// register with the dispatcher
	dispatcher_t::register_stage(FSCAN_PACKET_TYPE, this);	
    }

protected:
  virtual int process_packet(packet_t* packet);
  
private:
  int read_file(packet_t* packet, FILE* file, tuple_page_t* tuple_page);
  
};



#endif
