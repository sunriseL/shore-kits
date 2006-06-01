/* -*- mode:C++ c-basic-offset:4 -*- */

#ifndef __SCALAR_AGGREGATE_H
#define __SCALAR_AGGREGATE_H

#include "tuple.h"
#include "packet.h"
#include "stage.h"
#include "dispatcher/dispatcher.h"



using namespace qpipe;

# define SCALAR_AGGREGATE_STAGE_NAME  "SCALAR_AGGREGATE"
# define SCALAR_AGGREGATE_PACKET_TYPE "SCALAR_AGGREGATE"


/**
 *  @brief Packet definition for the Aggregation stage
 */

struct scalar_aggregate_packet_t : public packet_t {

  // all aggregates receive their inputs from a buffer
  tuple_buffer_t*    input_buffer;

  // the functor we will use to produce our scalar aggregate tuple
  tuple_aggregate_t* aggregate;
  
  bool mergeable;

  scalar_aggregate_packet_t(char*              packet_id,
			    tuple_buffer_t*    out_buffer,
			    tuple_buffer_t*    in_buffer,
			    tuple_aggregate_t* agg,
			    tuple_filter_t*    filt)
    : packet_t(packet_id, SCALAR_AGGREGATE_PACKET_TYPE, out_buffer, filt),
       input_buffer(in_buffer),
       aggregate(agg),
       mergeable(true)
  {
  }

  ~scalar_aggregate_packet_t() { }

  void terminate();
};



/**
 *  @brief Aggregation stage that aggregates over all input tuples to
 *  produce one final output tuple.
 */

class scalar_aggregate_stage_t : public stage_t {

public:
  scalar_aggregate_stage_t()
    : stage_t(SCALAR_AGGREGATE_STAGE_NAME)
    {
	// register with the dispatcher
	dispatcher_t::register_stage(SCALAR_AGGREGATE_PACKET_TYPE, this);
    }

  ~scalar_aggregate_stage_t() { }

protected:
  int process_packet(packet_t *packet);

};



#endif
