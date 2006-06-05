/* -*- mode:C++; c-basic-offset:4 -*- */

#include "thread.h"
#include "stage.h"
#include "trace/trace.h"
#include "qpipe_panic.h"



// include me last!
#include "namespace.h"



/**
 *  @brief packet_t constructor.
 */

packet_t::packet_t(const char* _packet_id,
		   const char* _packet_type,
		   tuple_buffer_t* _output_buffer,
		   tuple_filter_t* _filter,
		   tuple_buffer_t* _client_buffer,
                   bool _merge_enabled)
    : merge_enabled(_merge_enabled),
      output_buffer(_output_buffer),
      filter(_filter),
      client_buffer(_client_buffer),
      _stage_next_tuple_on_merge(0),
      _stage_next_tuple_needed(0)
{

    // need to copy the strings since we don't own them
    if ( asprintf( &packet_id, "%s", _packet_id) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on packet ID %s\n",
	      packet_id);
	QPIPE_PANIC();
    }

    if ( asprintf( &packet_type, "%s", _packet_type) == -1 ) {
	TRACE(TRACE_ALWAYS, "asprintf() failed on packet type %s\n",
	      packet_type);
	QPIPE_PANIC();
    }

    TRACE(TRACE_PACKET_FLOW, "Created a new packet of type %s with ID %s\n",
	  packet_type,
	  packet_id);
}



/**
 *  @brief packet_t destructor.
 */

packet_t::~packet_t(void) {
    
  TRACE(TRACE_PACKET_FLOW, "Destroying packet of type %s with ID %s\n",
	packet_type,
	packet_id);

  free(packet_id);
  free(packet_type);
  delete filter;

  // the output_buffer is destroyed elsewhere since we must account
  // for the possibility that someone is consuming from it
}



/**
 *  @brief Terminate the query that this packet is a part of. This
 *  is invoked when an unrecoverable error occurs when processing
 *  this packet.
 */
void packet_t::notify_client_of_abort() {
    TRACE(TRACE_ALWAYS, "Invoked for packet of type %s with ID %s\n",
	  packet_type,
	  packet_id);
    
    // unimplemented...

    // Should be implemented by setting value at some bool* that is
    // referenced by our packet as well as by closing the client's
    // input buffer on both ends. The double close should be a
    // synchronized operation.
}



#include "namespace.h"
