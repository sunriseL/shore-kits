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

packet_t::packet_t(char* _packet_id,
		   char* _packet_type,
		   tuple_buffer_t* _output_buffer,
		   tuple_filter_t* _filter,
                   bool _mergeable)
    : output_buffer(_output_buffer),
      filter(_filter),
      mergeable(_mergeable)
    
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

    pthread_mutex_init_wrapper( &merge_mutex, NULL );
    merged_packets.push_front(this);
    
    TRACE(TRACE_PACKET_FLOW, "Created a new packet of type %s with ID %s\n",
	  packet_type,
	  packet_id);
}



/**
 *  @brief packet_t destructor.
 */

packet_t::~packet_t(void) {
    
  TRACE(TRACE_PACKET_FLOW, "Destroying packet with ID %s\n", packet_id);

  pthread_mutex_destroy_wrapper(&merge_mutex);

  TRACE(TRACE_ALWAYS, "packet_t destructor not fully implemented... need to destroy packet ID\n");
}



/**
 *  @brief Link the specified packet to this one.
 */

void packet_t::merge(packet_t* packet) {
    
  TRACE(TRACE_PACKET_FLOW, "Adding packet %s to chain of packet %s\n",
	packet->packet_id,
	packet_id);
  
  pthread_mutex_lock_wrapper(&merge_mutex);
  
  // add packet to head of merge set

  merged_packets.push_front(packet);
  
  pthread_mutex_unlock_wrapper(&merge_mutex);
}



#include "namespace.h"
